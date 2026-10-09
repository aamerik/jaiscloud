#!/usr/bin/env python3
"""crypto-medallion demo orchestrator (design §5).

One CLI that walks the demo's stage order against a k3d jaiscloud-gcp
deployment: provision -> ingest -> process -> serve -> observe -> automate,
plus reset and the recording helpers.

    python3 demo/crypto-medallion/scripts/democtl.py <stage> [opts]

Stages
    provision   create the project, apply Terraform, create the data-platform
                resources (Managed Kafka, Metastore, Dataproc, BigQuery,
                Firestore) and write the runtime state (design §5.1).
    bridge      build/push the WS->Kafka bridge image and roll the Deployment
                (design §5.2).
    process     stage the Iceberg/Kafka jars + PySpark driver on GCS and submit
                the always-on medallion streaming job (design §5.3).
    rollup      submit the periodic Spark SQL rollup once (design §5.3/§5.4).
    serve       Cloud Run publisher + leaderboard, Eventarc trigger, Scheduler
                job and the in-cluster submitter (design §5.4).
    observe     custom metric + alert policy + notification channel (design §5.5).
    automate    tick the emulator's Scheduler clock once (design §5.6).
    up          provision + bridge + process + serve (+observe).
    status      print the live state of every demo resource.
    reset       tear the demo down and reset the emulator (design §12).

Everything is idempotent: re-running a stage reconciles, never duplicates.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# ── locations ────────────────────────────────────────────────────────────────
DEMO_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = DEMO_DIR.parent.parent
TF_DIR = DEMO_DIR / "terraform"
SPARK_DIR = DEMO_DIR / "spark"
IMAGES_DIR = DEMO_DIR / "images"
K8S_DIR = DEMO_DIR / "k8s"
STATE_FILE = DEMO_DIR / ".state.json"
JARS_DIR = SPARK_DIR / "jars"

# ── fixed demo config (design §3 locked decisions) ───────────────────────────
PROJECT = os.environ.get("DEMO_PROJECT", "crypto-medallion")
REGION = os.environ.get("DEMO_REGION", "us-central1")
ZONE = os.environ.get("DEMO_ZONE", "us-central1-a")
NS = os.environ.get("K8S_NAMESPACE", "jaiscloud")
KAFKA_CLUSTER = "cm-trades"
KAFKA_TOPIC = "trades"
METASTORE_SERVICE = "cm-hms"
DATAPROC_CLUSTER = "cm-spark"
GKE_TARGET = f"projects/{PROJECT}/locations/{REGION}/clusters/cm-gke"
HMS = f"jaiscloud-gcp.{NS}.svc.cluster.local:9083"
IN_CLUSTER_EMULATOR = f"http://jaiscloud-gcp.{NS}.svc.cluster.local:8080"
ICE_DB = "medallion"
ICE_TABLE = "gold_ohlc"
BQ_DATASET = "crypto_medallion"
BQ_TABLE = "trades_rollup"
FIRESTORE_COLLECTION = "leaderboard"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"]

RUN_PUBLISHER = "cm-publisher"
RUN_LEADERBOARD = "cm-leaderboard"
SUBMITTER = "cm-submitter"
SCHEDULER_JOB = "cm-rollup"
EVENTARC_TRIGGER = "cm-leaderboard-live"
METRIC_TYPE = "custom.googleapis.com/crypto_medallion/trades_per_min"
ALERT_POLICY = "cm-trades-alert"
NOTIFICATION_CHANNEL = "cm-incidents"

DOCKER_REGISTRY = os.environ.get("GCP_REGISTRY", "10.0.100.21:5050")
IMAGE_TAG = os.environ.get("DEMO_IMAGE_TAG", "demo")

# jars staged on GCS for the Spark job (design §3 / SPK1 / SPK2).
ICEBERG_JAR = "iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
KAFKA_JARS = [
    "spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    "kafka-clients-3.4.1.jar",
    "commons-pool2-2.11.1.jar",
]


# ── tiny process helpers ─────────────────────────────────────────────────────
def log(msg: str) -> None:
    print(f"\033[1m==> {msg}\033[0m", flush=True)


def info(msg: str) -> None:
    print(f"    {msg}", flush=True)


def run(cmd, *, check=True, capture=True, env=None, input_text=None):
    """Run a command, returning its stdout (trimmed) when capture is set."""
    e = dict(os.environ)
    if env:
        e.update(env)
    if isinstance(cmd, str):
        cmd = cmd.split()
    proc = subprocess.run(
        cmd, capture_output=capture, text=True, env=e, input=input_text
    )
    if check and proc.returncode != 0:
        if capture:
            sys.stderr.write(proc.stdout or "")
            sys.stderr.write(proc.stderr or "")
        raise SystemExit(f"command failed ({proc.returncode}): {' '.join(cmd)}")
    return (proc.stdout or "").strip() if capture else ""


def have(binary: str) -> bool:
    return shutil.which(binary) is not None


# ── emulator REST client ─────────────────────────────────────────────────────
class Emulator:
    """Thin JSON client for the jaiscloud-gcp REST API (host side)."""

    def __init__(self, base: str):
        self.base = base.rstrip("/")

    def request(self, method: str, path: str, body=None, *, raw=None,
                content_type="application/json", expect=(200, 201, 204, 409, 404)):
        url = self.base + path
        if raw is not None:
            data = raw
        elif body is not None:
            data = json.dumps(body).encode()
        else:
            data = None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", "Bearer demo-token")
        if data is not None:
            req.add_header("Content-Type", content_type)
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                payload = resp.read()
                status = resp.status
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            status = exc.code
        if status not in expect:
            raise SystemExit(
                f"{method} {path} -> HTTP {status}: {payload[:400].decode(errors='replace')}"
            )
        if not payload:
            return None, status
        try:
            return json.loads(payload), status
        except json.JSONDecodeError:
            return payload.decode(errors="replace"), status

    def get(self, path, **kw):
        return self.request("GET", path, **kw)

    def post(self, path, body=None, **kw):
        return self.request("POST", path, body, **kw)

    def patch(self, path, body=None, **kw):
        return self.request("PATCH", path, body, **kw)

    def put(self, path, body=None, **kw):
        return self.request("PUT", path, body, **kw)

    def delete(self, path, **kw):
        return self.request("DELETE", path, **kw)


def wait_healthy(base: str, timeout=60) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(base + "/_jaiscloud/health", timeout=3) as r:
                if r.status == 200:
                    return
        except Exception:
            pass
        time.sleep(1)
    raise SystemExit(f"emulator not healthy at {base}")


def ensure_port_forward(local_port: int, remote_port: int = 8080) -> str:
    """Return a host REST base, starting a port-forward if one is not already up."""
    base = f"http://localhost:{local_port}"
    try:
        with urllib.request.urlopen(base + "/_jaiscloud/health", timeout=3) as r:
            if r.status == 200:
                return base
    except Exception:
        pass
    log(f"port-forward svc/jaiscloud-gcp {local_port}:{remote_port}")
    subprocess.Popen(
        ["kubectl", "-n", NS, "port-forward", f"svc/jaiscloud-gcp",
         f"{local_port}:{remote_port}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    wait_healthy(base, timeout=40)
    return base


# ── gcloud wrapper (endpoint overrides for the emulator) ─────────────────────
def gcloud_env(endpoint: str) -> dict:
    cfg = "/tmp/opencode/gcloud-demo"
    os.makedirs(cfg, exist_ok=True)
    overrides = {
        "CLOUDRESOURCEMANAGER": f"{endpoint}/",
        "SERVICEUSAGE": f"{endpoint}/",
        "DATAPROC": f"{endpoint}/",
        "MANAGEDKAFKA": f"{endpoint}/",
        "METASTORE": f"{endpoint}/",
        "EVENTARC": f"{endpoint}/",
        "CLOUDSCHEDULER": f"{endpoint}/",
        "STORAGE": f"{endpoint}/storage/v1/",
        "PUBSUB": f"{endpoint}/v1/",
        "SECRETMANAGER": f"{endpoint}/v1/",
        "CLOUDKMS": f"{endpoint}/v1/",
        "IAM": f"{endpoint}/",
        "FIRESTORE": f"{endpoint}/",
    }
    env = {
        "CLOUDSDK_CONFIG": cfg,
        "CLOUDSDK_CORE_DISABLE_PROMPTS": "1",
        "CLOUDSDK_CORE_DISABLE_USAGE_REPORTING": "true",
        "CLOUDSDK_AUTH_ACCESS_TOKEN": os.environ.get("GOOGLE_OAUTH_ACCESS_TOKEN", "demo-token"),
        "CLOUDSDK_CORE_PROJECT": PROJECT,
    }
    for k, v in overrides.items():
        env[f"CLOUDSDK_API_ENDPOINT_OVERRIDES_{k}"] = v
    return env


def gcloud(endpoint: str, args, *, check=True, input_text=None):
    return run(["gcloud", *args], env=gcloud_env(endpoint), check=check,
               input_text=input_text)


# ── state ────────────────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")


# ── stage 1: provision ───────────────────────────────────────────────────────
def ensure_project(emu: Emulator) -> None:
    log(f"project {PROJECT}")
    out = gcloud(emu.base, ["projects", "describe", PROJECT], check=False)
    if PROJECT in out or "projectId" in out:
        info(f"{PROJECT} exists")
        return
    gcloud(emu.base, ["projects", "create", PROJECT, "--name", "Crypto Medallion Demo"])


def terraform_apply(emu: Emulator) -> dict:
    log("terraform infra subset (design §5.1)")
    env = {"GOOGLE_OAUTH_ACCESS_TOKEN": "demo-token", "GOOGLE_CLOUD_PROJECT": PROJECT}
    tf = "terraform"
    run([tf, f"-chdir={TF_DIR}", "init", "-input=false", "-no-color"], env=env)
    run([tf, f"-chdir={TF_DIR}", "apply", "-input=false", "-auto-approve",
         "-no-color", f"-var=endpoint={emu.base}", f"-var=project={PROJECT}",
         f"-var=region={REGION}"], env=env)
    out = json.loads(run([tf, f"-chdir={TF_DIR}", "output", "-json"], env=env))
    return {k: v["value"] for k, v in out.items()}


def ensure_metastore(emu: Emulator) -> str:
    name = f"projects/{PROJECT}/locations/{REGION}/services/{METASTORE_SERVICE}"
    log(f"Dataproc Metastore {METASTORE_SERVICE}")
    body = {"hiveMetastoreConfig": {"endpointProtocol": "THRIFT"}}
    emu.post(f"/v1/projects/{PROJECT}/locations/{REGION}/services"
             f"?serviceId={METASTORE_SERVICE}", body)
    info(name)
    return name


def ensure_kafka(emu: Emulator) -> dict:
    log(f"Managed Kafka cluster {KAFKA_CLUSTER} + topic {KAFKA_TOPIC}")
    emu.post(f"/v1/projects/{PROJECT}/locations/{REGION}/clusters"
             f"?clusterId={KAFKA_CLUSTER}", {})
    cluster, _ = emu.get(f"/v1/projects/{PROJECT}/locations/{REGION}/clusters/{KAFKA_CLUSTER}")
    bootstrap = cluster.get("bootstrapAddress", "")
    emu.post(f"/v1/projects/{PROJECT}/locations/{REGION}/clusters/{KAFKA_CLUSTER}"
             f"/topics?topicId={KAFKA_TOPIC}",
             {"partitionCount": 3, "replicationFactor": 1})
    info(f"bootstrapAddress={bootstrap}")
    return {"cluster": KAFKA_CLUSTER, "topic": KAFKA_TOPIC, "bootstrap": bootstrap}


def ensure_bigquery(emu: Emulator) -> None:
    log(f"BigQuery dataset {BQ_DATASET}.{BQ_TABLE}")
    emu.post(f"/bigquery/v2/projects/{PROJECT}/datasets",
             {"datasetReference": {"projectId": PROJECT, "datasetId": BQ_DATASET}})
    schema = {"fields": [
        {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
        {"name": "vwap", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "volume", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "trades", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "window_start", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]}
    emu.post(f"/bigquery/v2/projects/{PROJECT}/datasets/{BQ_DATASET}/tables",
             {"tableReference": {"projectId": PROJECT, "datasetId": BQ_DATASET,
                                 "tableId": BQ_TABLE},
              "schema": schema})


def stage_provision(args) -> None:
    emu = Emulator(ensure_port_forward(args.port))
    ensure_project(emu)
    outputs = terraform_apply(emu)
    ensure_metastore(emu)
    kafka = ensure_kafka(emu)
    ensure_bigquery(emu)
    state = load_state()
    state.update({
        "endpoint": emu.base,
        "project": PROJECT,
        "region": REGION,
        "namespace": NS,
        "buckets": outputs["buckets"],
        "service_account": outputs["service_account_email"],
        "feed_secret": outputs["feed_secret_id"],
        "events_topic": outputs["events_topic"],
        "events_subscription": outputs["events_subscription"],
        "kafka": kafka,
        "hms_service": f"projects/{PROJECT}/locations/{REGION}/services/{METASTORE_SERVICE}",
        "dataproc_cluster": DATAPROC_CLUSTER,
    })
    save_state(state)
    log("provision complete")
    info(f"state -> {STATE_FILE}")


# ── object store helpers ─────────────────────────────────────────────────────
def gcs_upload_object(emu: Emulator, bucket: str, name: str, data: bytes,
                      content_type="application/octet-stream") -> None:
    q = urllib.parse.urlencode({"uploadType": "media", "name": name})
    emu.post(f"/upload/storage/v1/b/{bucket}/o?{q}", raw=data,
             content_type=content_type, expect=(200, 201))


def gcs_read_object(emu: Emulator, bucket: str, name: str):
    obj, _ = emu.get(f"/storage/v1/b/{bucket}/o/{urllib.parse.quote(name, safe='')}?alt=media")
    return obj


# ── stage 3 helpers: stage jars + driver on GCS ──────────────────────────────
def stage_spark_assets(emu: Emulator, state: dict) -> dict:
    jobs_bucket = state["buckets"]["jobs"]
    log("stage Iceberg + Kafka jars and the PySpark driver on GCS")
    jar_names = [ICEBERG_JAR, *KAFKA_JARS]
    jar_uris = []
    for name in jar_names:
        local = JARS_DIR / name
        if not local.exists():
            raise SystemExit(f"missing {local} — run demo/crypto-medallion/spark/download-jars.sh")
        gcs_upload_object(emu, jobs_bucket, f"jars/{name}", local.read_bytes(),
                          "application/java-archive")
        jar_uris.append(f"gs://{jobs_bucket}/jars/{name}")
        info(f"gs://{jobs_bucket}/jars/{name}")
    driver = (SPARK_DIR / "stream_medallion.py").read_bytes()
    gcs_upload_object(emu, jobs_bucket, "jobs/stream_medallion.py", driver, "text/x-python")
    rollup = (SPARK_DIR / "rollup.py").read_bytes()
    gcs_upload_object(emu, jobs_bucket, "jobs/rollup.py", rollup, "text/x-python")
    return {
        "jar_uris": jar_uris,
        "driver_uri": f"gs://{jobs_bucket}/jobs/stream_medallion.py",
        "rollup_uri": f"gs://{jobs_bucket}/jobs/rollup.py",
    }


def ensure_dataproc_cluster(emu: Emulator) -> None:
    path = f"/v1/projects/{PROJECT}/regions/{REGION}/clusters/{DATAPROC_CLUSTER}"
    body, status = emu.get(path, expect=(200, 404))
    if status == 200:
        info(f"cluster {DATAPROC_CLUSTER} exists")
        return
    log(f"Dataproc GKE-virtual cluster {DATAPROC_CLUSTER}")
    payload = {
        "clusterName": DATAPROC_CLUSTER,
        "virtualClusterConfig": {
            "kubernetesClusterConfig": {
                "gkeClusterConfig": {"gkeClusterTarget": GKE_TARGET}
            },
            "auxiliaryServicesConfig": {
                "metastoreConfig": {
                    "dataprocMetastoreService":
                        f"projects/{PROJECT}/locations/{REGION}/services/{METASTORE_SERVICE}"
                }
            },
        },
    }
    emu.post(f"/v1/projects/{PROJECT}/regions/{REGION}/clusters", payload)
    for _ in range(60):
        c, _ = emu.get(path, expect=(200, 404))
        if c and c.get("status", {}).get("state") in ("RUNNING", "ERROR"):
            if c["status"]["state"] == "ERROR":
                raise SystemExit("dataproc cluster entered ERROR")
            return
        time.sleep(2)
    raise SystemExit("dataproc cluster did not reach RUNNING")


def spark_properties(state: dict, extra: dict | None = None) -> dict:
    jobs = state["buckets"]["jobs"]
    props = {
        "spark.sql.catalog.hms": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.hms.catalog-impl": "org.apache.iceberg.hive.HiveCatalog",
        "spark.sql.catalog.hms.uri": f"thrift://{HMS}",
        "spark.sql.catalog.hms.warehouse": f"gs://{jobs}/warehouse/",
        "spark.sql.catalog.hms.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
        "spark.driver.memory": "450m",
        "spark.executor.memory": "450m",
        "spark.executor.instances": "1",
        "spark.sql.shuffle.partitions": "1",
        "spark.sql.streaming.checkpointLocation": f"gs://{jobs}/checkpoints/",
    }
    if extra:
        props.update(extra)
    return props


def submit_pyspark(emu: Emulator, job_id: str, main_uri: str, jar_uris: list,
                   props: dict, args: list | None = None) -> dict:
    payload = {"job": {
        "reference": {"jobId": job_id},
        "placement": {"clusterName": DATAPROC_CLUSTER},
        "pysparkJob": {
            "mainPythonFileUri": main_uri,
            "jarFileUris": jar_uris,
            "properties": props,
            "args": args or [],
        },
    }}
    resp, _ = emu.post(f"/v1/projects/{PROJECT}/regions/{REGION}/jobs:submit", payload)
    return resp


def job_state(emu: Emulator, job_id: str) -> str:
    resp, status = emu.get(f"/v1/projects/{PROJECT}/regions/{REGION}/jobs/{job_id}",
                           expect=(200, 404))
    if status != 200 or not resp:
        return ""
    return resp.get("status", {}).get("state", "")


def stage_process(args) -> None:
    emu = Emulator(load_state()["endpoint"])
    state = load_state()
    assets = stage_spark_assets(emu, state)
    ensure_dataproc_cluster(emu)

    buckets = state["buckets"]
    stream_args = [
        "--bootstrap", state["kafka"]["bootstrap"],
        "--topic", state["kafka"]["topic"],
        "--bronze", f"gs://{buckets['bronze']}/trades",
        "--silver", f"gs://{buckets['silver']}/trades",
        "--quarantine", f"gs://{buckets['silver']}/quarantine",
        "--leaderboard", f"gs://{buckets['leaderboard']}/latest.json",
        "--iceberg-db", ICE_DB,
        "--iceberg-table", ICE_TABLE,
        "--group", "cm-medallion",
    ]
    job_id = "cm-streaming"
    if job_state(emu, job_id) in ("RUNNING", "PENDING", "SETUP_DONE"):
        info(f"streaming job {job_id} already {job_state(emu, job_id)}")
    else:
        log(f"submit streaming job {job_id}")
        submit_pyspark(emu, job_id, assets["driver_uri"], assets["jar_uris"],
                       spark_properties(state), stream_args)
    state["spark"] = assets
    save_state(state)


def stage_rollup(args) -> None:
    emu = Emulator(load_state()["endpoint"])
    state = load_state()
    assets = state.get("spark") or stage_spark_assets(emu, state)
    ensure_dataproc_cluster(emu)
    rollup_args = [
        "--iceberg-db", ICE_DB,
        "--iceberg-table", ICE_TABLE,
        "--rollup", f"gs://{state['buckets']['rollup']}/rollup",
    ]
    log("submit Spark SQL rollup")
    submit_pyspark(emu, f"cm-rollup-{int(time.time())}", assets["rollup_uri"],
                   assets["jar_uris"], spark_properties(state), rollup_args)


# ── image build/push ─────────────────────────────────────────────────────────
def build_image(name: str, dockerfile: Path, context: Path) -> str:
    image = f"{DOCKER_REGISTRY}/{name}:{IMAGE_TAG}"
    log(f"build+push {image}")
    run(["docker", "build", "-t", image, "-f", str(dockerfile), str(context)],
        capture=False)
    run(["docker", "push", image], capture=False)
    return image


def stage_bridge(args) -> None:
    state = load_state()
    emu = Emulator(state["endpoint"])
    image = build_image("crypto-medallion-bridge",
                        IMAGES_DIR / "bridge" / "Dockerfile", DEMO_DIR)
    state["images"] = {**state.get("images", {}), "bridge": image}
    save_state(state)
    render_k8s(emu, "bridge.yaml", state, extra={"BRIDGE_IMAGE": image})
    kubectl(["rollout", "status", "deployment/cm-bridge", "--timeout=180s"])


# ── k8s rendering ────────────────────────────────────────────────────────────
def kubectl(args, *, check=True, input_text=None):
    return run(["kubectl", "-n", NS, *args], check=check, input_text=input_text)


def render_k8s(emu: Emulator, manifest: str, state: dict, extra: dict | None = None) -> None:
    text = (K8S_DIR / manifest).read_text()
    subs = {
        "PROJECT": PROJECT,
        "REGION": REGION,
        "NAMESPACE": NS,
        "EMULATOR": IN_CLUSTER_EMULATOR,
        "KAFKA_BOOTSTRAP": state["kafka"]["bootstrap"],
        "KAFKA_TOPIC": state["kafka"]["topic"],
        "CAPTURE_BUCKET": state["buckets"]["capture"],
        "FEED_SECRET": state["feed_secret"],
        "SERVICE_ACCOUNT": state["service_account"],
    }
    if extra:
        subs.update(extra)
    for key, value in subs.items():
        text = text.replace(f"__{key}__", str(value))
    kubectl(["apply", "-f", "-"], input_text=text)


# ── stage 4: serve ───────────────────────────────────────────────────────────
def deploy_run_service(emu: Emulator, service_id: str, image: str, port: int,
                       env: dict) -> str:
    path = f"/v2/projects/{PROJECT}/locations/{REGION}/services"
    env_list = [{"name": k, "value": str(v)} for k, v in env.items()]
    body = {"template": {"containers": [{
        "image": image, "ports": [{"containerPort": port}], "env": env_list,
    }]}}
    # Reconcile: delete then create keeps the demo deterministic.
    emu.delete(f"{path}/{service_id}", expect=(200, 204, 404))
    resp, _ = emu.post(f"{path}?serviceId={service_id}", body)
    return resp


def run_authority(emu: Emulator) -> str:
    resp, _ = emu.get(f"/v2/projects/{PROJECT}/locations/{REGION}/services/{RUN_LEADERBOARD}")
    return resp.get("uri", "")


def ensure_eventarc_trigger(emu: Emulator) -> None:
    log(f"Eventarc trigger {EVENTARC_TRIGGER} (GCS finalize -> publisher)")
    path = f"/v1/projects/{PROJECT}/locations/{REGION}/triggers"
    emu.delete(f"{path}/{EVENTARC_TRIGGER}", expect=(200, 204, 404))
    body = {
        "destination": {"cloudRun": {
            "service": f"projects/{PROJECT}/locations/{REGION}/services/{RUN_PUBLISHER}",
            "region": REGION,
        }},
        "eventFilters": [
            {"attribute": "type", "value": "google.cloud.storage.object.v1.finalized"},
            {"attribute": "bucket", "value": load_state()["buckets"]["leaderboard"]},
        ],
    }
    emu.post(f"{path}?triggerId={EVENTARC_TRIGGER}", body)


def ensure_scheduler_job(emu: Emulator) -> None:
    log(f"Cloud Scheduler job {SCHEDULER_JOB} (*/2) -> submitter")
    uri = f"http://cm-submitter.{NS}.svc.cluster.local:8080/"
    path = f"/v1/projects/{PROJECT}/locations/{REGION}/jobs"
    emu.delete(f"{path}/{SCHEDULER_JOB}", expect=(200, 204, 404))
    body = {
        "name": f"projects/{PROJECT}/locations/{REGION}/jobs/{SCHEDULER_JOB}",
        "schedule": "*/2 * * * *",
        "timeZone": "UTC",
        "httpTarget": {
            "uri": uri,
            "httpMethod": "POST",
            "body": base64.b64encode(b'{"kind":"rollup"}').decode(),
            "oidcToken": {
                "serviceAccountEmail": f"demo-runner@{PROJECT}.iam.gserviceaccount.com",
                "audience": uri,
            },
        },
        "retryConfig": {"retryCount": 1, "minBackoffDuration": "15s"},
    }
    emu.post(path, body)


def stage_serve(args) -> None:
    state = load_state()
    emu = Emulator(state["endpoint"])
    buckets = state["buckets"]
    publisher_image = build_image("crypto-medallion-publisher",
                                  IMAGES_DIR / "publisher" / "Dockerfile",
                                  IMAGES_DIR / "publisher")
    leaderboard_image = build_image("crypto-medallion-leaderboard",
                                    IMAGES_DIR / "leaderboard" / "Dockerfile",
                                    IMAGES_DIR / "leaderboard")
    submitter_image = build_image("crypto-medallion-submitter",
                                  IMAGES_DIR / "submitter" / "Dockerfile",
                                  IMAGES_DIR / "submitter")
    state["images"] = {**state.get("images", {}),
                       "publisher": publisher_image,
                       "leaderboard": leaderboard_image,
                       "submitter": submitter_image}
    save_state(state)

    log("Cloud Run publisher")
    deploy_run_service(emu, RUN_PUBLISHER, publisher_image, 8080, {
        "PROJECT": PROJECT,
        "EMULATOR": IN_CLUSTER_EMULATOR,
        "BQ_DATASET": BQ_DATASET,
        "BQ_TABLE": BQ_TABLE,
        "FIRESTORE_COLLECTION": FIRESTORE_COLLECTION,
        "LEADERBOARD_BUCKET": buckets["leaderboard"],
        "MONITORING_METRIC": METRIC_TYPE,
    })
    log("Cloud Run leaderboard")
    deploy_run_service(emu, RUN_LEADERBOARD, leaderboard_image, 8080, {
        "PROJECT": PROJECT,
        "EMULATOR": IN_CLUSTER_EMULATOR,
        "FIRESTORE_COLLECTION": FIRESTORE_COLLECTION,
    })

    render_k8s(emu, "submitter.yaml", state, extra={"SUBMITTER_IMAGE": submitter_image})
    ensure_eventarc_trigger(emu)
    ensure_scheduler_job(emu)
    log("serve complete")


# ── stage 5: observe + automate ──────────────────────────────────────────────
def ensure_monitoring(emu: Emulator) -> None:
    state = load_state()
    log("Monitoring notification channel + alert policy")
    topic = state["events_topic"]
    ch, _ = emu.post(f"/v3/projects/{PROJECT}/notificationChannels", {
        "type": "pubsub",
        "displayName": "crypto-medallion incidents",
        "enabled": True,
        "labels": {"topic": topic},
    })
    channel_name = ch.get("name", "")
    policies, _ = emu.get(f"/v3/projects/{PROJECT}/alertPolicies")
    existing = {p.get("displayName") for p in policies.get("alertPolicies", [])}
    if ALERT_POLICY in existing:
        info("alert policy exists")
        return
    emu.post(f"/v3/projects/{PROJECT}/alertPolicies", {
        "displayName": ALERT_POLICY,
        "combiner": "OR",
        "enabled": True,
        "notificationChannels": [channel_name],
        "conditions": [{
            "displayName": "trades per minute high",
            "conditionThreshold": {
                "filter": f'metric.type = "{METRIC_TYPE}" AND resource.type = "global"',
                "comparison": "COMPARISON_GT",
                "thresholdValue": 5,
                "duration": "0s",
            },
        }],
    })


def stage_observe(args) -> None:
    emu = Emulator(load_state()["endpoint"])
    ensure_monitoring(emu)
    log("observe complete (metric is written by the publisher per delivery)")


def stage_automate(args) -> None:
    emu = Emulator(load_state()["endpoint"])
    log("force Cloud Scheduler to fire once")
    emu.post("/_jaiscloud/scheduler-tick")


# ── status / reset ───────────────────────────────────────────────────────────
def stage_status(args) -> None:
    state = load_state()
    if not state:
        print("no state — run provision first")
        return
    emu = Emulator(state["endpoint"])
    print(json.dumps({
        "endpoint": state["endpoint"],
        "kafka": state["kafka"],
        "leaderboard_uri": run_authority(emu) if True else "",
    }, indent=2))


def stage_reset(args) -> None:
    state = load_state()
    endpoint = state.get("endpoint") or f"http://localhost:{args.port}"
    emu = Emulator(endpoint)
    log("delete demo resources")
    for path in (
        f"/v1/projects/{PROJECT}/locations/{REGION}/triggers/{EVENTARC_TRIGGER}",
        f"/v1/projects/{PROJECT}/locations/{REGION}/jobs/{SCHEDULER_JOB}",
        f"/v2/projects/{PROJECT}/locations/{REGION}/services/{RUN_PUBLISHER}",
        f"/v2/projects/{PROJECT}/locations/{REGION}/services/{RUN_LEADERBOARD}",
        f"/v1/projects/{PROJECT}/regions/{REGION}/jobs/cm-streaming",
        f"/v1/projects/{PROJECT}/regions/{REGION}/clusters/{DATAPROC_CLUSTER}",
        f"/v1/projects/{PROJECT}/locations/{REGION}/clusters/{KAFKA_CLUSTER}",
    ):
        emu.delete(path, expect=(200, 204, 404))
    kubectl(["delete", "deployment", "cm-bridge", "cm-submitter", "--ignore-not-found"])
    if args.pp:
        log("terraform destroy")
        env = {"GOOGLE_OAUTH_ACCESS_TOKEN": "demo-token", "GOOGLE_CLOUD_PROJECT": PROJECT}
        run(["terraform", f"-chdir={TF_DIR}", "destroy", "-input=false",
             "-auto-approve", "-no-color", f"-var=endpoint={endpoint}",
             f"-var=project={PROJECT}", f"-var=region={REGION}"], env=env)
    if args.full:
        log("POST /_jaiscloud/reset")
        emu.post("/_jaiscloud/reset")
        if STATE_FILE.exists():
            STATE_FILE.unlink()


def stage_up(args) -> None:
    stage_provision(args)
    stage_bridge(args)
    stage_process(args)
    stage_serve(args)
    if not args.no_observe:
        stage_observe(args)


# ── CLI ──────────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("stage", choices=[
        "provision", "bridge", "process", "rollup", "serve", "observe",
        "automate", "up", "status", "reset"])
    ap.add_argument("--port", type=int, default=18080,
                    help="host port for the emulator REST port-forward")
    ap.add_argument("--no-observe", action="store_true", help="up: skip observe")
    ap.add_argument("--pp", action="store_true", help="reset: also terraform destroy")
    ap.add_argument("--full", action="store_true", help="reset: POST /_jaiscloud/reset")
    args = ap.parse_args()
    {
        "provision": stage_provision, "bridge": stage_bridge,
        "process": stage_process, "rollup": stage_rollup, "serve": stage_serve,
        "observe": stage_observe, "automate": stage_automate, "up": stage_up,
        "status": stage_status, "reset": stage_reset,
    }[args.stage](args)


if __name__ == "__main__":
    main()
