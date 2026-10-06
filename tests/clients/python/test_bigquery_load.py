"""BigQuery REST conformance via the official ``google-cloud-bigquery``.

Wiring: no emulator env hook exists for BigQuery, so the client is pointed at
the emulator with ``client_options.api_endpoint`` and ``AnonymousCredentials``
(see ``harness.bigquery_client``).  A GCS source object is staged with the
storage client (``STORAGE_EMULATOR_HOST``) and loaded from ``gs://`` with
``Client.load_table_from_uri``, exercising the load-job path (BQL1).
"""

from __future__ import annotations

import pytest
from google.cloud import bigquery

from harness import CONFIG, bigquery_client, check, storage_client, unique

SERVICE = "bigquery"

pytestmark = pytest.mark.rest


@pytest.fixture
def dataset(client):
    ds_id = unique("pyc_bq_ds").replace("-", "_")
    ds = client.create_dataset(ds_id)
    yield ds
    try:
        client.delete_dataset(ds, delete_contents=True)
    except Exception:  # noqa: BLE001 - best-effort teardown
        pass


@pytest.fixture
def client():
    return bigquery_client()


def test_load_ndjson_from_uri(client, dataset):
    """The canonical ingestion path: load_table_from_uri of a gs:// NDJSON object."""
    table_id = unique("pyc_bq_tbl").replace("-", "_")
    table = client.create_table(
        bigquery.Table(
            f"{CONFIG.project}.{dataset.dataset_id}.{table_id}",
            schema=[
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING"),
            ],
        )
    )

    # Stage the NDJSON source in the emulated GCS (same emulator origin).
    bucket = storage_client().create_bucket(unique("pyc-bq-load"))
    blob = bucket.blob("rows.json")
    blob.upload_from_string(
        '{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n',
        content_type="application/x-ndjson",
    )

    def op():
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = client.load_table_from_uri(
            f"gs://{bucket.name}/{blob.name}", table, job_config=cfg
        )
        job.result()  # polls jobs.get; the emulator completes synchronously
        assert job.state == "DONE", f"job state {job.state!r}"
        rows = sorted((r["id"], r["name"]) for r in client.list_rows(table))
        assert rows == [(1, "a"), (2, "b")], rows
        return f"{len(rows)} rows"

    try:
        check(SERVICE, "LoadTableFromUri", op)
    finally:
        try:
            blob.delete()
            bucket.delete()
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass
