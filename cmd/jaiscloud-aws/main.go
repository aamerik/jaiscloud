package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	awsadapter "jaiscloud/internal/aws/adapter"
	"jaiscloud/internal/aws/adapter/services"
	keyprovider "jaiscloud/internal/aws/key"
	paramprovider "jaiscloud/internal/aws/parameter"
	apigwprovider "jaiscloud/internal/aws/provider/apigw"
	cacheprovider "jaiscloud/internal/aws/provider/cache"
	"jaiscloud/internal/aws/provider/catalog"
	cloudwatchprovider "jaiscloud/internal/aws/provider/cloudwatch"
	cwlogs "jaiscloud/internal/aws/provider/cloudwatch/logs"
	"jaiscloud/internal/aws/provider/compute"
	containerprovider "jaiscloud/internal/aws/provider/container"
	"jaiscloud/internal/aws/provider/dns"
	eksprovider "jaiscloud/internal/aws/provider/eks"
	emrprovider "jaiscloud/internal/aws/provider/emr"
	emrcontainersprovider "jaiscloud/internal/aws/provider/emroneks"
	eventsprovider "jaiscloud/internal/aws/provider/events"
	functionprovider "jaiscloud/internal/aws/provider/function"
	iamprovider "jaiscloud/internal/aws/provider/iam"
	kinesisprovider "jaiscloud/internal/aws/provider/kinesis"
	ecrprovider "jaiscloud/internal/aws/provider/ecr"
	sfnprovider "jaiscloud/internal/aws/provider/stepfunctions"
	sfndispatcher "jaiscloud/internal/aws/provider/stepfunctions/dispatcher"
	sfnengine "jaiscloud/internal/aws/provider/stepfunctions/engine"
	lambdaesm "jaiscloud/internal/aws/provider/lambda/esm"
	stsprovider "jaiscloud/internal/aws/sts"
	"jaiscloud/internal/aws/provider/events/targets"
	ebscheduler "jaiscloud/internal/aws/provider/events/scheduler"
	"jaiscloud/internal/workers"
	"jaiscloud/internal/logstream"
	kinesisstore "jaiscloud/internal/store/aws/kinesis"
	ecrstore "jaiscloud/internal/store/aws/ecr"
	sfnstore "jaiscloud/internal/store/aws/stepfunctions"
	"jaiscloud/internal/aws/provider/notification"
	objectprovider "jaiscloud/internal/aws/provider/object"
	"jaiscloud/internal/aws/provider/queue"
	rdsprovider "jaiscloud/internal/aws/provider/rds"
	sparkaws "jaiscloud/internal/aws/provider/sparkaws"
	"jaiscloud/internal/aws/provider/stack/handlers"
	// Phase 15 providers
	cognitoprovider "jaiscloud/internal/aws/provider/cognito"
	cognitoidentityprovider "jaiscloud/internal/aws/provider/cognitoidentity"
	acmprovider "jaiscloud/internal/aws/provider/acm"
	sesprovider "jaiscloud/internal/aws/provider/ses"
	firehoseprovider "jaiscloud/internal/aws/provider/firehose"
	cloudfrontprovider "jaiscloud/internal/aws/provider/cloudfront"
	athenaprovider "jaiscloud/internal/aws/provider/athena"
	redshiftprovider "jaiscloud/internal/aws/provider/redshift"
	// G-PENDING new providers
	elbv2provider "jaiscloud/internal/aws/provider/elbv2"
	awsconfigprovider "jaiscloud/internal/aws/provider/awsconfig"
	resourcegroupsprovider "jaiscloud/internal/aws/provider/resourcegroups"
	taggingprovider "jaiscloud/internal/aws/provider/tagging"
	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/aws/provider/table"
	secretprovider "jaiscloud/internal/aws/secret"
	"jaiscloud/internal/adapter"
	"jaiscloud/internal/admin"
	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/certstore"
	"jaiscloud/internal/config"
	"jaiscloud/internal/events"
	ecsexec "jaiscloud/internal/executor/ecs"
	lambdaexec "jaiscloud/internal/executor/lambda"
	"jaiscloud/internal/gateway"
	"jaiscloud/internal/platform"
	"jaiscloud/internal/provider"
	objectstore "jaiscloud/internal/store/object"
	"jaiscloud/internal/store"
	dynamostore "jaiscloud/internal/store/aws/dynamodb"
	s3store "jaiscloud/internal/store/aws/s3"
	sqsstore "jaiscloud/internal/store/aws/sqs"
	streamstore "jaiscloud/internal/store/stream"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	root := &cobra.Command{
		Use:   "jaiscloud-aws",
		Short: "JaisCloud AWS - local AWS emulator",
	}
	root.AddCommand(startCmd())
	root.AddCommand(versionCmd())
	root.AddCommand(envCmd())
	root.AddCommand(doctorCmd())
	root.AddCommand(resetCmd())
	root.AddCommand(exportCmd())
	root.AddCommand(importCmd())
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// ─── start ────────────────────────────────────────────────────────────────────

func startCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the emulator",
		RunE: func(cmd *cobra.Command, args []string) error {
			bindFlags(cmd)

			cfg, err := config.Load()
			if err != nil {
				return err
			}
			cfg.Cloud = "aws"

			ctx := context.Background()
			s, err := initStores(ctx, cfg)
			if err != nil {
				return err
			}

			dek, err := bootstrapDEK(ctx, cfg, s)
			if err != nil {
				return err
			}

			platformCfg, err := platform.LoadFromEnv()
			if err != nil {
				return fmt.Errorf("platform config: %w", err)
			}

			stateDir, _ := config.ResolveStateDir(os.Getenv("JAISCLOUD_STATE_DIR"))
			instanceID, idSource := config.LoadOrCreateInstanceID(stateDir)
			slog.Info("instance id", "id", instanceID, "source", idSource, "state_dir", stateDir)

			ecrP := buildECRProvider(ctx, cfg, s)
			registry, streamStore, bus, keyStore, secretStore, paramStore, lambdaResetter, cleanup, objectP, queueResetter, logsResetter, sfnP, cwResetter, funcP, firehoseP, computeP := buildRegistry(ctx, cfg, s, dek, platformCfg, instanceID, ecrP)
			defer cleanup()

			// Wire Step Functions execution engine — provides real ASL execution.
			sfnDisp := sfndispatcher.New(registry, cfg)
			sfnEng := sfnengine.New(s.sfn, sfnDisp, cfg.Clock)
			sfnP.SetEngine(sfnEng)
			prevCleanup := cleanup
			cleanup = func() {
				shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = sfnEng.Shutdown(shutCtx)
				prevCleanup()
			}

			cloudAdapter := buildAWSAdapter(cfg.S3VirtualHostBases)
			adminHandler := buildAdminHandler(s, streamStore, keyStore, secretStore, paramStore, lambdaResetter, queueResetter, logsResetter, cwResetter, computeP)
			adminHandler.SetLambdaCodeFetcher(funcP)
			adminHandler.SetCWAlarmEvaluator(cwResetter.Evaluator())
			adminHandler.SetFirehoseFlusher(firehoseP)
			adminHandler.SetMeta(admin.HandlerMeta{
				InstanceID: instanceID,
				Cloud:      "aws",
				Region:     cfg.Region,
				AccountID:  cfg.AccountID,
				StateDir:   stateDir,
			})

			var certs certstore.CertStore
			if fsCS, err := certstore.NewFilesystemCertStore(stateDir); err == nil {
				certs = fsCS
			} else {
				slog.Warn("certstore: using in-memory store; TLS cert will regenerate on restart")
				certs = certstore.NewMemoryCertStore()
			}

			var gatewayOpts []func(*gateway.Server)
			if cfg.IMDSEnabled {
				imdsCfg := awsadapter.IMDSConfig{
					Region:    cfg.Region,
					AccountID: cfg.AccountID,
					RoleName:  "jaiscloud-emulator-role",
				}
				gatewayOpts = append(gatewayOpts, gateway.WithExtraRoutes(func(r chi.Router) {
					awsadapter.RegisterIMDSRoutes(r, imdsCfg)
				}))
			}
			gatewayOpts = append(gatewayOpts, gateway.WithCORSLookup(objectP.GetBucketCORSRules))

			// ECR full mode: register OCI Distribution v2 routes before the wildcard.
			if ociHandler := ecrP.OCIHandler(); ociHandler != nil {
				gatewayOpts = append(gatewayOpts, gateway.WithExtraRoutes(func(r chi.Router) {
					r.HandleFunc("/v2/*", ociHandler)
					r.HandleFunc("/v2/", ociHandler)
				}))
			}

			srv := gateway.NewServer(cfg, adminHandler, registry, cloudAdapter, certs, gatewayOpts...)
			_ = bus
			return srv.ListenAndServe()
		},
	}

	cmd.Flags().Int("port", 4566, "Listen port")
	cmd.Flags().String("mode", "lite", "Mode: lite or full")
	cmd.Flags().String("dsn", "", `PostgreSQL connection string (required when --mode full).
	Format:  postgres://USER:PASSWORD@HOST:PORT/DBNAME
	Example: postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud
	Env var: JAISCLOUD_DSN`)
	cmd.Flags().String("log-level", "info", "Log level: debug/info/warn/error")
	cmd.Flags().Bool("metrics", false, "Expose Prometheus metrics at /metrics")
	cmd.Flags().Bool("tracing", false, "Emit OTel trace IDs in response headers")
	cmd.Flags().Bool("deterministic", false, "Enable deterministic mode")
	cmd.Flags().Int64("seed", 0, "Random seed (requires --deterministic)")
	cmd.Flags().String("time", "", "Base time RFC3339 (requires --deterministic)")
	cmd.Flags().String("time-mode", "offset", "Time mode: frozen or offset")
	cmd.Flags().String("blob-dir", "", `Directory for S3 blob bytes (full mode only).
	Defaults to ~/.jaiscloud/blobs.
	Env var: JAISCLOUD_BLOB_DIR`)
	cmd.Flags().String("kms-master-key", "", `32-byte hex KEK for KMS envelope encryption.
	If unset, DEK is stored plaintext (dev only).
	Env var: JAISCLOUD_KMS_MASTER_KEY`)
	cmd.Flags().String("executor-mode", "", `Container orchestrator for all executors (Spark + Lambda): mock, docker, or k8s.
	"" / unset: instant mock completion (no containers).  "mock": explicit mock.
	"docker": Docker daemon.  "k8s": Kubernetes (docker-desktop or in-cluster).
	Env var: JAISCLOUD_EXECUTOR_MODE`)
	cmd.Flags().String("lambda-image", "", `Override default Lambda runtime image.
	Env var: JAISCLOUD_LAMBDA_IMAGE`)
	cmd.Flags().String("lambda-network", "jaiscloud-net", `Docker network for Lambda containers.
	Env var: JAISCLOUD_LAMBDA_NETWORK`)
	cmd.Flags().Int("lambda-keepalive-secs", 300, `Docker warm container idle timeout in seconds.
	Env var: JAISCLOUD_LAMBDA_KEEPALIVE_SECS`)
	cmd.Flags().String("aws-emulator-endpoint", "", `AWS emulator endpoint for Spark driver pods (e.g. http://host:4566).
	Env var: JAISCLOUD_AWS_EMULATOR_ENDPOINT`)
	cmd.Flags().Bool("imds-enabled", false, `Enable the AWS instance-metadata emulator endpoints.
	Env var: JAISCLOUD_IMDS_ENABLED`)
	cmd.Flags().String("k8s-namespace", "jaiscloud", `Kubernetes namespace for Spark and Lambda workloads.
	Env var: JAISCLOUD_K8S_NAMESPACE`)
	cmd.Flags().String("k8s-spark-image", "", `Default container image for spark-submit driver pods.
	Env var: JAISCLOUD_K8S_SPARK_IMAGE`)
	cmd.Flags().String("k8s-spark-sa", "", `Kubernetes service account for Spark driver pods.
	Env var: JAISCLOUD_K8S_SPARK_SA`)
	cmd.Flags().String("spark-emr-image", "", `Container image for EMR on EC2 spark-submit pods (overrides k8s-spark-image).
	Env var: JAISCLOUD_SPARK_EMR_IMAGE`)
	cmd.Flags().String("spark-emreks-image", "", `Container image for EMR on EKS spark-submit pods (overrides k8s-spark-image).
	Env var: JAISCLOUD_SPARK_EMREKS_IMAGE`)
	cmd.Flags().String("s3-virtual-host-bases", "", `Comma-separated host suffixes treated as S3 virtual-hosted bases.
	Env var: JAISCLOUD_S3_VIRTUAL_HOST_BASES`)

	return cmd
}

// bindFlags binds all cobra flags to their viper keys so config.Load() picks
// them up. Must be called at the start of RunE, after flag parsing is complete.
func bindFlags(cmd *cobra.Command) {
	viper.BindPFlag("port", cmd.Flags().Lookup("port"))
	viper.BindPFlag("mode", cmd.Flags().Lookup("mode"))
	viper.BindPFlag("dsn", cmd.Flags().Lookup("dsn"))
	viper.BindPFlag("log_level", cmd.Flags().Lookup("log-level"))
	viper.BindPFlag("metrics", cmd.Flags().Lookup("metrics"))
	viper.BindPFlag("tracing", cmd.Flags().Lookup("tracing"))
	viper.BindPFlag("deterministic", cmd.Flags().Lookup("deterministic"))
	viper.BindPFlag("seed", cmd.Flags().Lookup("seed"))
	viper.BindPFlag("time", cmd.Flags().Lookup("time"))
	viper.BindPFlag("time_mode", cmd.Flags().Lookup("time-mode"))
	viper.BindPFlag("blob_dir", cmd.Flags().Lookup("blob-dir"))
	viper.BindPFlag("kms_master_key", cmd.Flags().Lookup("kms-master-key"))
	viper.BindPFlag("executor_mode", cmd.Flags().Lookup("executor-mode"))
	viper.BindPFlag("lambda_image", cmd.Flags().Lookup("lambda-image"))
	viper.BindPFlag("lambda_network", cmd.Flags().Lookup("lambda-network"))
	viper.BindPFlag("lambda_keepalive_secs", cmd.Flags().Lookup("lambda-keepalive-secs"))
	viper.BindPFlag("aws_emulator_endpoint", cmd.Flags().Lookup("aws-emulator-endpoint"))
	viper.BindPFlag("imds_enabled", cmd.Flags().Lookup("imds-enabled"))
	viper.BindPFlag("k8s_namespace", cmd.Flags().Lookup("k8s-namespace"))
	viper.BindPFlag("k8s_spark_image", cmd.Flags().Lookup("k8s-spark-image"))
	viper.BindPFlag("k8s_spark_sa", cmd.Flags().Lookup("k8s-spark-sa"))
	viper.BindPFlag("spark_emr_image", cmd.Flags().Lookup("spark-emr-image"))
	viper.BindPFlag("spark_emreks_image", cmd.Flags().Lookup("spark-emreks-image"))
	viper.BindPFlag("s3_virtual_host_bases", cmd.Flags().Lookup("s3-virtual-host-bases"))
}

// appStores holds all store instances that the server depends on.
type appStores struct {
	resources   store.ResourceStore
	messages    sqsstore.SQSMessageStore
	dynamo      dynamostore.DynamoDBItemStore
	s3Meta      objectstore.ObjectMetaStore
	blobs       blobfs.BlobStore
	secrets     secretprovider.SecretStore
	parameters  paramprovider.ParameterStore
	stsSession  *stsprovider.MemorySessionStore
	kinesis     *kinesisstore.MemoryKinesisStore
	ecr         *ecrstore.MemoryECRStore
	sfn         *sfnstore.MemoryStepFunctionsStore
}

// initStores constructs the store layer for the chosen mode (lite or full).
func initStores(ctx context.Context, cfg *config.Config) (appStores, error) {
	if cfg.Mode == config.ModeFull {
		if cfg.DSN == "" {
			return appStores{}, fmt.Errorf("--mode full requires --dsn (or JAISCLOUD_DSN)")
		}
		slog.Info("starting in full mode", "dsn", cfg.DSN)
		pgStore, err := store.NewPostgresResourceStore(ctx, cfg.DSN, cfg.Cloud)
		if err != nil {
			return appStores{}, fmt.Errorf("postgres: %w", err)
		}
		pool := pgStore.Pool()
		blobs, err := blobfs.NewLocalFSBlobStore(cfg.BlobDir)
		if err != nil {
			pgStore.Close()
			return appStores{}, fmt.Errorf("blobfs: %w", err)
		}
		slog.Info("blob storage", "dir", cfg.BlobDir)
		return appStores{
			resources:   pgStore,
			messages:    sqsstore.NewPostgresSQSMessageStore(pool),
			dynamo:      dynamostore.NewPostgresDynamoDBItemStore(pool),
			s3Meta:      s3store.NewPostgresS3ObjectMetaStore(pool),
			blobs:       blobs,
			secrets:     secretprovider.NewPostgresSecretStore(pool),
			parameters:  paramprovider.NewPostgresParameterStore(pool),
			stsSession:  stsprovider.NewMemorySessionStore(),
			kinesis:     kinesisstore.NewMemoryKinesisStore(),
			ecr:         ecrstore.NewMemoryECRStore(),
			sfn:         sfnstore.NewMemoryStepFunctionsStore(),
		}, nil
	}

	slog.Info("starting in lite mode")
	return appStores{
		resources:   store.NewMemoryResourceStore(),
		messages:    sqsstore.NewMemoryMessageStore(),
		dynamo:      dynamostore.NewMemoryDynamoDBItemStore(),
		s3Meta:      s3store.NewMemoryS3ObjectMetaStore(),
		blobs:       blobfs.NewMemoryBlobStore(),
		secrets:     secretprovider.NewMemorySecretStore(),
		parameters:  paramprovider.NewMemoryParameterStore(),
		stsSession:  stsprovider.NewMemorySessionStore(),
		kinesis:     kinesisstore.NewMemoryKinesisStore(),
		ecr:         ecrstore.NewMemoryECRStore(),
		sfn:         sfnstore.NewMemoryStepFunctionsStore(),
	}, nil
}

// bootstrapDEK loads or creates the server data-encryption key.
// In full mode it is persisted in PostgreSQL (wrapped by KMSMasterKey if set).
// In lite mode a fresh ephemeral key is generated each startup.
func bootstrapDEK(ctx context.Context, cfg *config.Config, s appStores) ([]byte, error) {
	if cfg.Mode == config.ModeFull {
		pgStore, ok := s.resources.(*store.PostgresResourceStore)
		if !ok {
			return nil, fmt.Errorf("kms bootstrap: expected *store.PostgresResourceStore in full mode")
		}
		keyStore := keyprovider.NewPostgresKeyStore(pgStore.Pool(), nil)
		dek, err := keyprovider.LoadOrCreateDEK(ctx, keyStore, cfg.KMSMasterKey)
		if err != nil {
			return nil, fmt.Errorf("kms bootstrap: %w", err)
		}
		return dek, nil
	}
	dek, err := keyprovider.Generate32()
	if err != nil {
		return nil, fmt.Errorf("kms bootstrap: generate ephemeral DEK: %w", err)
	}
	return dek, nil
}

// buildRegistry wires all providers and returns the populated registry plus a cleanup func.
func buildRegistry(ctx context.Context, cfg *config.Config, s appStores, dek []byte, platformCfg *platform.PlatformConfig, instanceID string, ecrP *ecrprovider.Provider) (*provider.Registry, *streamstore.MemoryStreamStore, *events.EventBus, keyprovider.KeyStore, secretprovider.SecretStore, paramprovider.ParameterStore, admin.Resetter, func(), *objectprovider.ObjectProvider, *queue.QueueProvider, *cwlogs.Provider, *sfnprovider.Provider, *cloudwatchprovider.Provider, *functionprovider.FunctionProvider, *firehoseprovider.Provider, *compute.ComputeProvider) {
	bus := events.NewEventBus()
	streams := streamstore.NewMemoryStreamStore()

	sparkMode, _ := config.ExecutorMode("spark", "mock")

	s3Fetcher := blobfs.NewS3BlobFetcher(s.blobs)
	bootstrapPrefixes := []string{"/etc/pki", "/home/hadoop"}
	if v := os.Getenv("JAISCLOUD_BOOTSTRAP_RELOCATE_PREFIXES"); v != "" {
		bootstrapPrefixes = nil
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				bootstrapPrefixes = append(bootstrapPrefixes, p)
			}
		}
	}
	bootstrapImage := "amazon/aws-cli:2.18"
	if v := os.Getenv("JAISCLOUD_BOOTSTRAP_IMAGE"); v != "" {
		bootstrapImage = v
	}
	bootstrapMaxBytes := int64(1024 * 1024)
	if v := os.Getenv("JAISCLOUD_BOOTSTRAP_SCRIPT_MAX_BYTES"); v != "" {
		if n, parseErr := strconv.ParseInt(v, 10, 64); parseErr == nil {
			bootstrapMaxBytes = n
		} else {
			slog.Warn("invalid JAISCLOUD_BOOTSTRAP_SCRIPT_MAX_BYTES; using default 1 MiB", "value", v, "err", parseErr)
		}
	}
	bootstrapCfg := emrprovider.BootstrapConfig{
		Image:    bootstrapImage,
		MaxBytes: bootstrapMaxBytes,
		Prefixes: bootstrapPrefixes,
	}

	emrImage := cfg.SparkEMRImage
	if emrImage == "" {
		emrImage = cfg.K8sSparkImage
	}
	emrcImage := cfg.SparkEMREKSImage
	if emrcImage == "" {
		emrcImage = cfg.K8sSparkImage
	}

	if sparkMode == "k8s" {
		if emrImage == "" {
			slog.Error("emr: JAISCLOUD_K8S_SPARK_IMAGE (or JAISCLOUD_SPARK_EMR_IMAGE) is required when executor mode is k8s")
			os.Exit(1)
		}
		if emrcImage == "" {
			slog.Error("emroneks: JAISCLOUD_K8S_SPARK_IMAGE (or JAISCLOUD_SPARK_EMREKS_IMAGE) is required when executor mode is k8s")
			os.Exit(1)
		}
	}

	var emrOpts []emrprovider.Option
	var emrcOpts []emrcontainersprovider.Option
	emrOpts = append(emrOpts, emrprovider.WithBootstrap(s3Fetcher, bootstrapCfg))
	if emrImage != "" {
		emrOpts = append(emrOpts, emrprovider.WithSparkImage(emrImage))
	}
	if emrcImage != "" {
		emrcOpts = append(emrcOpts, emrcontainersprovider.WithSparkImage(emrcImage))
	}

	emrOpts = append(emrOpts, emrprovider.WithInstanceID(instanceID))
	emrcOpts = append(emrcOpts, emrcontainersprovider.WithInstanceID(instanceID))

	if cfg.K8sSparkSA != "" {
		emrOpts = append(emrOpts, emrprovider.WithServiceAccountName(cfg.K8sSparkSA))
		emrcOpts = append(emrcOpts, emrcontainersprovider.WithServiceAccountName(cfg.K8sSparkSA))
	}

	if cfg.AWSEmulatorEndpoint != "" {
		imdsEP := ""
		if cfg.IMDSEnabled {
			imdsEP = cfg.AWSEmulatorEndpoint
		}
		emulatorCfg := &sparkaws.AWSEmulatorConfig{
			Region:       cfg.Region,
			AccountID:    cfg.AccountID,
			S3Endpoint:   cfg.AWSEmulatorEndpoint,
			IMDSEndpoint: imdsEP,
		}
		emrOpts = append(emrOpts, emrprovider.WithAWSEmulator(emulatorCfg))
		emrcOpts = append(emrcOpts, emrcontainersprovider.WithAWSEmulator(emulatorCfg))
	}

	if sparkMode == "k8s" {
		k8sNS := cfg.K8sNamespace
		if k8sNS == "" {
			k8sNS = "jaiscloud"
		}
		if k8sClient, err := buildK8sClient(); err != nil {
			slog.Warn("emr: failed to build k8s client; falling back to mock", "err", err)
		} else {
			emrOpts = append(emrOpts, emrprovider.WithK8s(k8sClient, k8sNS, platformCfg))
			emrcOpts = append(emrcOpts, emrcontainersprovider.WithK8s(k8sClient, k8sNS, platformCfg))
		}
	}

	emrP := emrprovider.New(s.resources, bus, emrOpts...)
	emrcP := emrcontainersprovider.New(s.resources, bus, emrcOpts...)

	cleanup := func() {}

	tableProvider := table.NewWithStreams(s.resources, s.dynamo, streams)

	var keyStore keyprovider.KeyStore
	if cfg.Mode == config.ModeFull {
		pgStore := s.resources.(*store.PostgresResourceStore)
		keyStore = keyprovider.NewPostgresKeyStore(pgStore.Pool(), dek)
	} else {
		keyStore = keyprovider.NewMemoryKeyStore()
	}
	keyProv := keyprovider.New(keyStore, nil, dek)
	kmsEncryptor := keyProv.AsKeyEncryptor()
	secretProv := secretprovider.New(s.secrets, kmsEncryptor)
	paramProv := paramprovider.New(s.parameters, kmsEncryptor)

	lambdaMode, lambdaModeSrc := config.ExecutorMode("lambda", "mock")
	lambdaCfg := lambdaexec.DefaultLambdaConfig()
	lambdaCfg.Mode = lambdaMode
	lambdaCfg.Region = cfg.Region
	lambdaCfg.InstanceID = instanceID
	if cfg.LambdaImage != "" {
		lambdaCfg.DefaultImage = cfg.LambdaImage
	}
	if cfg.LambdaNetwork != "" {
		lambdaCfg.Network = cfg.LambdaNetwork
	}
	if cfg.LambdaKeepaliveSecs > 0 {
		lambdaCfg.KeepaliveSecs = cfg.LambdaKeepaliveSecs
	}
	if v := os.Getenv("JAISCLOUD_ENDPOINT"); v != "" {
		lambdaCfg.JaisCloudEndpoint = v
	}
	lambdaCfg = lambdaexec.LambdaConfigFrom(lambdaCfg)
	var lambdaExec lambdaexec.LambdaExecutor
	switch lambdaMode {
	case "docker":
		lambdaExec = lambdaexec.NewDockerExecutor(lambdaCfg, platformCfg)
	case "k8s":
		lambdaExec = lambdaexec.NewK8sExecutor(lambdaCfg, platformCfg)
	default:
		lambdaExec = lambdaexec.NewExecutor(lambdaCfg)
	}
	slog.Info("lambda executor", "mode", lambdaMode, "source", lambdaModeSrc)
	prevCleanup := cleanup
	cleanup = func() { lambdaExec.Close(); prevCleanup() }

	funcP := functionprovider.NewWithLimits(s.resources, lambdaExec, lambdaCfg)
	queueP := queue.New(s.resources, s.messages, cfg.Clock, bus)
	iamP := iamprovider.New(s.resources)
	stsP := stsprovider.NewWithOIDC(s.stsSession, cfg.OIDCIssuers)
	kinesisP := buildKinesisProvider(ctx, cfg, s)
	notifP := notification.New(s.resources, s.messages, bus)
	notifP.SetLambdaInvoker(funcP)
	notifP.SetSQSSender(sqsSenderAdapter{q: queueP})
	objectP := objectprovider.NewWithBus(s.s3Meta, s.blobs, bus).WithResourceStore(s.resources)
	stackP := stackprovider.New(s.resources)

	esmProvider := lambdaesm.New(ctx, s.resources, funcP, queueP, streams, slog.Default())
	esmProvider.SetSQSSender(esmSQSSenderAdapter{q: queueP})
	esmProvider.RehydratePollers(ctx)
	prevCleanup2 := cleanup
	cleanup = func() { esmProvider.Shutdown(ctx); funcP.Shutdown(ctx); prevCleanup2() }

	registry := provider.NewRegistry()
	registry.RegisterAll(keyProv.Routes())
	registry.RegisterAll(secretProv.Routes())
	registry.RegisterAll(paramProv.Routes())
	registry.RegisterAll(funcP.Routes())
	registry.RegisterAll(esmProvider.Routes())
	registry.RegisterAll(queueP.Routes())
	registry.RegisterAll(iamP.Routes())
	registry.RegisterAll(stsP.Routes())
	registry.RegisterAll(kinesisP.Routes())
	registry.RegisterAll(ecrP.Routes())
	sfnP := sfnprovider.New(s.sfn)
	registry.RegisterAll(sfnP.Routes())
	registry.RegisterAll(notifP.Routes())
	registry.RegisterAll(tableProvider.Routes())
	registry.RegisterAll(tableProvider.StreamRoutes())
	registry.RegisterAll(objectP.Routes())
	glueP := catalog.New(s.resources)
	glueP.SetObjectProvider(objectP)
	registry.RegisterAll(glueP.Routes())
	computeP := compute.New(s.resources)
	registry.RegisterAll(computeP.Routes())
	registry.RegisterAll(dns.New(s.resources).Routes())
	registry.RegisterAll(rdsprovider.New(s.resources).Routes())
	registry.RegisterAll(cacheprovider.New(s.resources).Routes())
	ecsP := containerprovider.New(s.resources)
	registry.RegisterAll(ecsP.Routes())
	registry.RegisterAll(stackP.Routes())
	emrP.SetObjectProvider(objectP)
	registry.RegisterAll(emrP.Routes())
	emrcP.SetObjectProvider(objectP)
	registry.RegisterAll(emrcP.Routes())
	registry.RegisterAll(eksprovider.New(s.resources).Routes())
	eventsP := eventsprovider.New(s.resources, s.messages, bus).WithPort(cfg.Port)
	registry.RegisterAll(eventsP.Routes())
	apigwP := apigwprovider.New(s.resources)
	registry.RegisterAll(apigwP.Routes())
	cwP := cloudwatchprovider.New(s.resources, bus)
	registry.RegisterAll(cwP.Routes())

	logsProvider := cwlogs.New()
	registry.RegisterAll(logsProvider.Routes())

	// Wire code loader and CW Logs ingestor into real Lambda executors.
	if dockerExec, ok := lambdaExec.(*lambdaexec.DockerExecutor); ok {
		dockerExec.SetCodeLoader(funcP)
		dockerExec.SetLogsAPI(logsProvider)
	}
	if k8sExec, ok := lambdaExec.(*lambdaexec.K8sExecutor); ok {
		k8sExec.SetCodeLoader(funcP)
		k8sExec.SetLogsAPI(logsProvider)
	}

	// Wire ECS executor.
	ecsMode, _ := config.ExecutorMode("ecs", "mock")
	var ecsExec ecsexec.Executor
	switch ecsMode {
	case "docker":
		ecsExec = ecsexec.New(ecsexec.ModeDocker, logsProvider)
	case "k8s":
		ecsExec = ecsexec.New(ecsexec.ModeK8s, logsProvider)
	default:
		ecsExec = ecsexec.New(ecsexec.ModeMock, nil)
	}
	ecsP.SetExecutor(ecsExec)
	if cfg.AWSEmulatorEndpoint != "" {
		ecsP.SetJaisCloudEndpoint(cfg.AWSEmulatorEndpoint)
	}

	// Phase 15 providers
	registry.RegisterAll(cognitoprovider.New(s.resources).Routes())
	registry.RegisterAll(cognitoidentityprovider.New(s.resources).Routes())
	registry.RegisterAll(acmprovider.New(s.resources).Routes())
	sesP := sesprovider.New(s.resources)
	registry.RegisterAll(sesP.Routes())
	firehoseP := firehoseprovider.New(s.resources).WithS3Meta(s.s3Meta).WithS3Writer(objectP)
	registry.RegisterAll(firehoseP.Routes())
	firehoseP.Start()
	registry.RegisterAll(cloudfrontprovider.New(s.resources).Routes())
	registry.RegisterAll(athenaprovider.New(s.resources).Routes())
	registry.RegisterAll(redshiftprovider.New(s.resources).Routes())
	// G-PENDING new providers
	registry.RegisterAll(elbv2provider.New(s.resources).Routes())
	registry.RegisterAll(awsconfigprovider.New(s.resources).Routes())
	registry.RegisterAll(resourcegroupsprovider.New(s.resources).Routes())
	registry.RegisterAll(taggingprovider.New(s.resources).Routes())

	// Second-pass cross-service wiring.
	objectP.SetFanout(objectprovider.S3FanoutConfig{
		SQS:          queueP,
		SNSPublisher: notifP,
		Lambda:       funcP,
	})
	cwP.SetSNSPublisher(notifP)
	cwP.SetLambdaInvoker(funcP)
	logsProvider.SetSubscriptionDispatcher(funcP)
	logsProvider.SetMetricDataPutter(&cwMetricAdapter{cwP})
	emrcP.SetLogsIngestor(logsProvider)
	secretProv.SetInvoker(funcP)
	paramProv.SetEventPublisher(eventsP)
	paramProv.SetSecretGetter(secretProv)

	// Wire EventBridge target dispatcher and scheduler.
	tgtDisp := targets.New(queueP, funcP, notifP, &logsWriterAdapter{logsProvider}, eventsP)
	eventsP.SetTargetDispatcher(tgtDisp)
	sched := ebscheduler.New(tgtDisp, cfg.Clock)
	eventsP.SetScheduler(sched)

	// Wire SQS DLQ sender into the async queue.
	funcP.SetAsyncSQSSend(func(ctx context.Context, arn string, body string) error {
		return queueP.InternalSend(ctx, arn, body, nil, queue.SourceContext{
			SourceArn:        "lambda.amazonaws.com",
			ServicePrincipal: "lambda.amazonaws.com",
		})
	})

	// Wire workers registry — start all background workers.
	workerReg := workers.New()
	workerReg.Add("eventbridge-scheduler", sched)
	workerReg.Add("cw-alarm-evaluator", cwP.Evaluator())
	workerReg.Add("lambda-async-queue", funcP.AsyncQueue())
	workerReg.Start(ctx)
	prevCleanup3 := cleanup
	cleanup = func() { workerReg.Stop(); prevCleanup3() }

	registerCFNHandlers(stackP, queueP, notifP, objectP, tableProvider, iamP, funcP, keyProv, secretProv, paramProv,
		logsProvider, cwP, eventsP, ecsP, sfnP, esmProvider, apigwP, computeP)

	return registry, streams, bus, keyStore, s.secrets, s.parameters, lambdaExec, cleanup, objectP, queueP, logsProvider, sfnP, cwP, funcP, firehoseP, computeP
}

// cwMetricAdapter bridges cloudwatch.Provider.InternalPutMetricData (uses cloudwatch.MetricDatum)
// to the cwlogs.MetricDataPutter interface (uses logs.MetricDatum). Both types are structurally
// identical; the adapter copies fields to satisfy the distinct type system.
type cwMetricAdapter struct{ p *cloudwatchprovider.Provider }

func (a *cwMetricAdapter) InternalPutMetricData(ctx context.Context, namespace string, data []cwlogs.MetricDatum) error {
	cw := make([]cloudwatchprovider.MetricDatum, len(data))
	for i, d := range data {
		cw[i] = cloudwatchprovider.MetricDatum{Name: d.Name, Value: d.Value, Unit: d.Unit}
	}
	return a.p.InternalPutMetricData(ctx, namespace, cw)
}

// buildK8sClient constructs a kubernetes.Interface using in-cluster config if
// available, falling back to JAISCLOUD_K8S_APISERVER + JAISCLOUD_K8S_TOKEN env vars.
func buildK8sClient() (kubernetes.Interface, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return kubernetes.NewForConfig(cfg)
	}
	apiServer := os.Getenv("JAISCLOUD_K8S_APISERVER")
	if apiServer == "" {
		apiServer = "https://kubernetes.default.svc"
	}
	token := os.Getenv("JAISCLOUD_K8S_TOKEN")
	cfg := &rest.Config{
		Host:        apiServer,
		BearerToken: token,
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: true,
		},
	}
	return kubernetes.NewForConfig(cfg)
}

// registerCFNHandlers wires real resource provisioning for CloudFormation resource types.
func registerCFNHandlers(
	stackP *stackprovider.StackProvider,
	queueP *queue.QueueProvider,
	notifP *notification.SNSProvider,
	objectP *objectprovider.ObjectProvider,
	tableP *table.TableProvider,
	iamP *iamprovider.IAMProvider,
	funcP *functionprovider.FunctionProvider,
	keyP *keyprovider.KeyProvider,
	secretP *secretprovider.SecretProvider,
	paramP *paramprovider.ParameterProvider,
	logsP *cwlogs.Provider,
	cwP *cloudwatchprovider.Provider,
	eventsP *eventsprovider.EventBridgeProvider,
	ecsP *containerprovider.ContainerProvider,
	sfnP *sfnprovider.Provider,
	esmP *lambdaesm.Provider,
	apigwP *apigwprovider.GatewayProvider,
	computeP *compute.ComputeProvider,
) {
	// Existing 27 resource types — delegated to per-file constructors.
	stackP.RegisterHandler("AWS::SQS::Queue", handlers.NewSQSQueueHandler(queueP))
	stackP.RegisterHandler("AWS::SNS::Topic", handlers.NewSNSTopicHandler(notifP))
	stackP.RegisterHandler("AWS::S3::Bucket", handlers.NewS3BucketHandler(objectP))
	stackP.RegisterHandler("AWS::DynamoDB::Table", handlers.NewDynamoDBTableHandler(tableP))
	stackP.RegisterHandler("AWS::IAM::Role", handlers.NewIAMRoleHandler(iamP))
	stackP.RegisterHandler("AWS::Lambda::Function", handlers.NewLambdaFunctionHandler(funcP))
	stackP.RegisterHandler("AWS::SSM::Parameter", handlers.NewSSMParameterHandler(paramP))
	stackP.RegisterHandler("AWS::SecretsManager::Secret", handlers.NewSecretsManagerSecretHandler(secretP))
	stackP.RegisterHandler("AWS::KMS::Key", handlers.NewKMSKeyHandler(keyP))
	stackP.RegisterHandler("AWS::KMS::Alias", handlers.NewKMSAliasHandler(keyP))
	stackP.RegisterHandler("AWS::IAM::Policy", handlers.NewIAMPolicyHandler(iamP))
	stackP.RegisterHandler("AWS::IAM::ManagedPolicy", handlers.NewIAMManagedPolicyHandler(iamP))
	stackP.RegisterHandler("AWS::IAM::InstanceProfile", handlers.NewIAMInstanceProfileHandler(iamP))
	stackP.RegisterHandler("AWS::Lambda::Permission", handlers.NewLambdaPermissionHandler(funcP))
	stackP.RegisterHandler("AWS::Lambda::Alias", handlers.NewLambdaAliasHandler(funcP))
	stackP.RegisterHandler("AWS::Lambda::EventSourceMapping", handlers.NewLambdaEventSourceMappingHandler(esmP))
	stackP.RegisterHandler("AWS::SNS::Subscription", handlers.NewSNSSubscriptionHandler(notifP))
	stackP.RegisterHandler("AWS::Logs::LogGroup", handlers.NewLogsLogGroupHandler(logsP))
	stackP.RegisterHandler("AWS::Logs::LogStream", handlers.NewLogsLogStreamHandler(logsP))
	stackP.RegisterHandler("AWS::CloudWatch::Alarm", handlers.NewCloudWatchAlarmHandler(cwP))
	stackP.RegisterHandler("AWS::Events::Rule", handlers.NewEventsRuleHandler(eventsP))
	stackP.RegisterHandler("AWS::Events::EventBus", handlers.NewEventsEventBusHandler(eventsP))
	stackP.RegisterHandler("AWS::ECS::Cluster", handlers.NewECSClusterHandler(ecsP))
	stackP.RegisterHandler("AWS::ECS::TaskDefinition", handlers.NewECSTaskDefinitionHandler(ecsP))
	stackP.RegisterHandler("AWS::ECS::Service", handlers.NewECSServiceHandler(ecsP))
	stackP.RegisterHandler("AWS::StepFunctions::StateMachine", handlers.NewStepFunctionsStateMachineHandler(sfnP))
	stackP.RegisterHandler("AWS::ApiGateway::RestApi", handlers.NewAPIGatewayRestApiHandler(apigwP))
	stackP.RegisterHandler("AWS::CloudFormation::Stack", handlers.NewCFNStackHandler(stackP))

	// 17 new resource types.
	stackP.RegisterHandler("AWS::EC2::VPC", handlers.NewEC2VPCHandler(computeP))
	stackP.RegisterHandler("AWS::EC2::Subnet", handlers.NewEC2SubnetHandler(computeP))
	stackP.RegisterHandler("AWS::EC2::SecurityGroup", handlers.NewEC2SecurityGroupHandler(computeP))
	stackP.RegisterHandler("AWS::EC2::InternetGateway", handlers.NewEC2InternetGatewayHandler(computeP))
	stackP.RegisterHandler("AWS::EC2::RouteTable", handlers.NewEC2RouteTableHandler(computeP))
	stackP.RegisterHandler("AWS::EC2::Route", handlers.NewEC2RouteHandler(computeP))
	stackP.RegisterHandler("AWS::EC2::SubnetRouteTableAssociation", handlers.NewEC2SubnetRouteTableAssociationHandler(computeP))
	stackP.RegisterHandler("AWS::S3::BucketPolicy", handlers.NewS3BucketPolicyHandler(objectP))
	stackP.RegisterHandler("AWS::Lambda::Version", handlers.NewLambdaVersionHandler(funcP))
	stackP.RegisterHandler("AWS::Lambda::Url", handlers.NewLambdaUrlHandler(funcP))
	stackP.RegisterHandler("AWS::ApiGateway::Resource", handlers.NewAPIGatewayResourceHandler(apigwP))
	stackP.RegisterHandler("AWS::ApiGateway::Method", handlers.NewAPIGatewayMethodHandler(apigwP))
	stackP.RegisterHandler("AWS::ApiGateway::Integration", handlers.NewAPIGatewayIntegrationHandler(apigwP))
	stackP.RegisterHandler("AWS::ApiGateway::Deployment", handlers.NewAPIGatewayDeploymentHandler(apigwP))
	stackP.RegisterHandler("AWS::ApiGateway::Stage", handlers.NewAPIGatewayStageHandler(apigwP))
	stackP.RegisterHandler("AWS::Logs::SubscriptionFilter", handlers.NewLogsSubscriptionFilterHandler(logsP))
	stackP.RegisterHandler("AWS::DynamoDB::GlobalTable", handlers.NewDynamoDBGlobalTableHandler(tableP))
}

// buildAWSAdapter constructs all AWS service codecs and returns the wired adapter.
func buildAWSAdapter(s3VirtualHostBases []string) *awsadapter.AWSAdapter {
	iamCodec := &services.IAMCodec{}
	return awsadapter.NewAdapter(map[string]adapter.Codec{
		"kms":             &services.KMSCodec{},
		"secretsmanager":  &services.SecretsManagerCodec{},
		"ssm":             &services.SSMCodec{},
		"sqs":             &services.SQSCodec{},
		"iam":             iamCodec,
		"sts":             iamCodec,
		"sns":             &services.SNSCodec{},
		"dynamodb":        &services.DynamoDBCodec{},
		"s3":              &services.S3Codec{VirtualHostBases: s3VirtualHostBases},
		"lambda":          &services.LambdaCodec{},
		"glue":            &services.GlueCodec{},
		"ec2":             services.NewEC2Codec("ec2"),
		"route53":         &services.Route53Codec{},
		"rds":             &services.RDSCodec{},
		"elasticache":     &services.ElastiCacheCodec{},
		"ecs":             &services.ECSCodec{},
		"dynamodbstreams": &services.DynamoDBStreamsCodec{},
		"cloudformation":  &services.CloudFormationCodec{},
		"emr":             &services.EMRCodec{},
		"emr-containers":  &services.EMRContainersCodec{},
		"events":          &services.EventBridgeCodec{},
		"eks":             &services.EKSCodec{},
		"apigateway":      &services.APIGatewayCodec{},
		"execute-api":     &services.ExecuteAPICodec{},
		"monitoring":      &services.CloudWatchCodec{},
		"logs":            &services.LogsCodec{},
		"ecr":             &services.ECRCodec{},
		"states":          &services.StepFunctionsCodec{},
		"kinesis":         &services.KinesisCodec{},
		// G-PENDING new codecs
		"elasticloadbalancing": &services.ELBv2Codec{},
		"config":               &services.GenericJSONTargetCodec{Service: "config", TargetPrefix: "StarlingDoveService."},
		"resource-groups":      &services.ResourceGroupsCodec{},
		"tagging":              &services.GenericJSONTargetCodec{Service: "tagging", TargetPrefix: "ResourceGroupsTaggingAPI_20170126."},
		"email":                &services.SESCodec{},
		"acm":                  &services.GenericJSONTargetCodec{Service: "acm", TargetPrefix: "CertificateManager."},
		"cognito-idp":          &services.GenericJSONTargetCodec{Service: "cognito-idp", TargetPrefix: "AWSCognitoIdentityProviderService."},
		"cognito-identity":     &services.GenericJSONTargetCodec{Service: "cognito-identity", TargetPrefix: "AWSCognitoIdentityService."},
		"firehose":             &services.GenericJSONTargetCodec{Service: "firehose", TargetPrefix: "Firehose_20150804."},
	})
}

// buildAdminHandler registers all resetters and snapshotters, then returns the handler.
func buildAdminHandler(s appStores, streams *streamstore.MemoryStreamStore, keyStore keyprovider.KeyStore, secretStore secretprovider.SecretStore, paramStore paramprovider.ParameterStore, extra ...admin.Resetter) *admin.Handler {
	h := admin.NewHandler()
	h.RegisterResetter(s.resources)
	h.RegisterResetter(s.messages)
	h.RegisterResetter(s.dynamo)
	h.RegisterResetter(s.s3Meta)
	h.RegisterResetter(s.blobs)
	h.RegisterResetter(streams)
	h.RegisterResetter(keyStore)
	h.RegisterResetter(secretStore)
	h.RegisterResetter(paramStore)
	h.RegisterResetter(s.stsSession)
	h.RegisterResetter(s.kinesis)
	h.RegisterResetter(s.ecr)
	h.RegisterResetter(s.sfn)
	for _, r := range extra {
		if r != nil {
			h.RegisterResetter(r)
		}
	}
	if snap, ok := s.resources.(admin.Snapshotter); ok {
		h.RegisterSnapshotter("resources", snap)
	}
	if snap, ok := keyStore.(admin.Snapshotter); ok {
		h.RegisterSnapshotter("kms-keys", snap)
	}
	if snap, ok := secretStore.(admin.Snapshotter); ok {
		h.RegisterSnapshotter("secrets", snap)
	}
	if snap, ok := paramStore.(admin.Snapshotter); ok {
		h.RegisterSnapshotter("parameters", snap)
	}
	h.RegisterSnapshotter("sts-sessions", s.stsSession)
	h.RegisterSnapshotter("kinesis", s.kinesis)
	h.RegisterSnapshotter("ecr", s.ecr)
	h.RegisterSnapshotter("stepfunctions", s.sfn)
	return h
}

// ─── ecr provider factory ─────────────────────────────────────────────────────

func buildECRProvider(ctx context.Context, cfg *config.Config, s appStores) *ecrprovider.Provider {
	if cfg.Mode == config.ModeFull && cfg.ExecutorMode == "k8s" {
		k8sNS := cfg.K8sNamespace
		if k8sNS == "" {
			k8sNS = "jaiscloud"
		}
		k8sClient, err := buildK8sClient()
		if err != nil {
			slog.Warn("ecr: cannot reach k8s, falling back to lite mode", "err", err)
			return ecrprovider.New(s.ecr)
		}
		proxy := ecrprovider.NewRegistryProxy(k8sClient, k8sNS, s.ecr)
		if err := proxy.Start(ctx); err != nil {
			slog.Warn("ecr: registry:2 failed to start, falling back to lite mode", "err", err)
			return ecrprovider.New(s.ecr)
		}
		slog.Info("ecr full mode: registry:2 proxy ready")
		return ecrprovider.NewFull(s.ecr, proxy)
	}
	return ecrprovider.New(s.ecr)
}

// ─── kinesis provider factory ─────────────────────────────────────────────────

func buildKinesisProvider(ctx context.Context, cfg *config.Config, s appStores) *kinesisprovider.Provider {
	if cfg.Mode == config.ModeFull {
		dataDir := filepath.Join(cfg.BlobDir, "kinesis")
		mock, err := kinesisprovider.NewMockServer(cfg.AccountID, dataDir)
		if err != nil {
			slog.Warn("kinesis-mock unavailable, falling back to lite mode", "err", err)
			return kinesisprovider.New(s.kinesis)
		}
		if err := mock.Start(ctx); err != nil {
			slog.Warn("kinesis-mock failed to start, falling back to lite mode", "err", err)
			return kinesisprovider.New(s.kinesis)
		}
		slog.Info("kinesis full mode: using kinesis-mock subprocess", "port", mock.Port())
		return kinesisprovider.NewFull(s.kinesis, mock)
	}
	return kinesisprovider.New(s.kinesis)
}

// ─── version ──────────────────────────────────────────────────────────────────

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("jaiscloud-aws %s (commit: %s, built: %s)\n", version, commit, date)
		},
	}
}

// ─── env ──────────────────────────────────────────────────────────────────────

func envCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "env",
		Short: "Print effective configuration as environment variables",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load()
			if err != nil {
				return err
			}
			fmt.Printf("JAISCLOUD_PORT=%d\n", cfg.Port)
			fmt.Printf("JAISCLOUD_MODE=%s\n", cfg.Mode)
			fmt.Printf("JAISCLOUD_CLOUD=aws\n")
			fmt.Printf("JAISCLOUD_REGION=%s\n", cfg.Region)
			fmt.Printf("JAISCLOUD_ACCOUNT_ID=%s\n", cfg.AccountID)
			fmt.Printf("JAISCLOUD_LOG_LEVEL=%s\n", cfg.LogLevel)
			return nil
		},
	}
}

// ─── doctor ───────────────────────────────────────────────────────────────────

func doctorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check that the emulator is reachable",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			resp, err := http.Get(host + "/_jaiscloud/health")
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: cannot reach %s: %v\n", host, err)
				os.Exit(1)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				fmt.Fprintf(os.Stderr, "ERROR: health check returned HTTP %d\n", resp.StatusCode)
				os.Exit(1)
			}
			fmt.Printf("OK: jaiscloud-aws is running at %s\n", host)
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:4566", "Emulator host URL")
	return cmd
}

// ─── reset ────────────────────────────────────────────────────────────────────

func resetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset",
		Short: "Wipe all emulator state",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			resp, err := http.Post(host+"/_jaiscloud/reset", "application/json", nil)
			if err != nil {
				return fmt.Errorf("reset: %w", err)
			}
			defer resp.Body.Close()
			fmt.Println("State reset.")
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:4566", "Emulator host URL")
	return cmd
}

// ─── export ───────────────────────────────────────────────────────────────────

func exportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export emulator state to a JSON file (or stdout)",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			output, _ := cmd.Flags().GetString("output")

			resp, err := http.Get(host + "/_jaiscloud/export")
			if err != nil {
				return fmt.Errorf("export: %w", err)
			}
			defer resp.Body.Close()
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("read export: %w", err)
			}

			var pretty json.RawMessage
			if json.Unmarshal(data, &pretty) == nil {
				if data, err = json.MarshalIndent(pretty, "", "  "); err == nil {
					data = append(data, '\n')
				}
			}

			if output == "" || output == "-" {
				_, err = os.Stdout.Write(data)
				return err
			}
			if err := os.WriteFile(output, data, 0o644); err != nil {
				return fmt.Errorf("write %s: %w", output, err)
			}
			fmt.Fprintf(os.Stderr, "Exported to %s\n", output)
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:4566", "Emulator host URL")
	cmd.Flags().StringP("output", "o", "-", "Output file (default: stdout)")
	return cmd
}

// ─── import ───────────────────────────────────────────────────────────────────

func importCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import emulator state from a JSON file (or stdin)",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			input, _ := cmd.Flags().GetString("input")
			newInstance, _ := cmd.Flags().GetBool("new-instance")

			var data []byte
			var err error
			if input == "" || input == "-" {
				data, err = io.ReadAll(os.Stdin)
			} else {
				data, err = os.ReadFile(input)
			}
			if err != nil {
				return fmt.Errorf("read input: %w", err)
			}

			url := host + "/_jaiscloud/import"
			if newInstance {
				url += "?new_instance=true"
			}
			resp, err := http.Post(url, "application/json", bytes.NewReader(data))
			if err != nil {
				return fmt.Errorf("import: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("import failed (HTTP %d): %s", resp.StatusCode, body)
			}
			fmt.Println("State imported.")
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:4566", "Emulator host URL")
	cmd.Flags().StringP("input", "i", "-", "Input file (default: stdin)")
	cmd.Flags().Bool("new-instance", false, "Assign a fresh instance ID on import (blocks snapshots with KMS key material)")
	return cmd
}

// logsWriterAdapter adapts cwlogs.Provider to targets.LogsWriter ([]string events).
type logsWriterAdapter struct{ p *cwlogs.Provider }

func (a *logsWriterAdapter) InternalPutLogEvents(ctx context.Context, logGroupName, logStreamName string, events []string) error {
	_ = a.p.InternalCreateLogGroup(ctx, logGroupName)
	lsEvents := make([]logstream.Event, len(events))
	now := time.Now().UnixMilli()
	for i, e := range events {
		lsEvents[i] = logstream.Event{Timestamp: now, Message: e}
	}
	return a.p.InternalPutEvents(ctx, logGroupName, logStreamName, lsEvents)
}

// sqsSenderAdapter adapts *queue.QueueProvider to notification.SQSSender.
type sqsSenderAdapter struct{ q *queue.QueueProvider }

func (a sqsSenderAdapter) InternalSend(ctx context.Context, queueARNorURL string, body string, attrs map[string]notification.SQSMessageAttribute, src notification.SQSSourceContext) error {
	queueAttrs := make(map[string]queue.MessageAttribute, len(attrs))
	for k, v := range attrs {
		queueAttrs[k] = queue.MessageAttribute{DataType: v.DataType, StringValue: v.StringValue}
	}
	return a.q.InternalSend(ctx, queueARNorURL, body, queueAttrs, queue.SourceContext{
		SourceArn:        src.SourceArn,
		ServicePrincipal: src.ServicePrincipal,
	})
}

// esmSQSSenderAdapter adapts *queue.QueueProvider to lambdaesm.SQSSenderAPI.
type esmSQSSenderAdapter struct{ q *queue.QueueProvider }

func (a esmSQSSenderAdapter) InternalSend(ctx context.Context, queueARNorURL string, body string, attrs map[string]lambdaesm.SQSMessageAttribute, src lambdaesm.SQSSourceContext) error {
	queueAttrs := make(map[string]queue.MessageAttribute, len(attrs))
	for k, v := range attrs {
		queueAttrs[k] = queue.MessageAttribute{DataType: v.DataType, StringValue: v.StringValue}
	}
	return a.q.InternalSend(ctx, queueARNorURL, body, queueAttrs, queue.SourceContext{
		SourceArn:        src.SourceArn,
		ServicePrincipal: src.ServicePrincipal,
	})
}
