package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"jaiscloud/internal/adapter"
	awsadapter "jaiscloud/internal/adapter/aws"
	"jaiscloud/internal/adapter/aws/services"
	azureadapter "jaiscloud/internal/adapter/azure"
	gcpadapter "jaiscloud/internal/adapter/gcp"
	"jaiscloud/internal/admin"
	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/config"
	"jaiscloud/internal/events"
	"jaiscloud/internal/executor/spark"
	"jaiscloud/internal/gateway"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	cacheprovider "jaiscloud/internal/provider/cache"
	"jaiscloud/internal/provider/catalog"
	"jaiscloud/internal/provider/compute"
	containerprovider "jaiscloud/internal/provider/container"
	"jaiscloud/internal/provider/dns"
	eksprovider "jaiscloud/internal/provider/eks"
	emrprovider "jaiscloud/internal/provider/emr"
	apigwprovider "jaiscloud/internal/provider/apigw"
	keyprovider "jaiscloud/internal/key"
	secretprovider "jaiscloud/internal/secret"
	paramprovider "jaiscloud/internal/parameter"
	lambdaexec "jaiscloud/internal/executor/lambda"
	emrcontainersprovider "jaiscloud/internal/provider/emroneks"
	eventsprovider "jaiscloud/internal/provider/events"
	functionprovider "jaiscloud/internal/provider/function"
	iamprovider "jaiscloud/internal/provider/iam"
	"jaiscloud/internal/provider/notification"
	objectprovider "jaiscloud/internal/provider/object"
	"jaiscloud/internal/provider/queue"
	rdsprovider "jaiscloud/internal/provider/rds"
	stackprovider "jaiscloud/internal/provider/stack"
	"jaiscloud/internal/provider/table"
	"jaiscloud/internal/certstore"
	"jaiscloud/internal/store"
	dynamostore "jaiscloud/internal/store/aws/dynamodb"
	s3store "jaiscloud/internal/store/aws/s3"
	sqsstore "jaiscloud/internal/store/aws/sqs"
	streamstore "jaiscloud/internal/store/stream"
)

const version = "0.2.0"

func main() {
	root := &cobra.Command{
		Use:   "jaiscloud",
		Short: "JaisCloud - local multi-cloud emulator",
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

			ctx := context.Background()
			s, err := initStores(ctx, cfg)
			if err != nil {
				return err
			}

			dek, err := bootstrapDEK(ctx, cfg, s)
			if err != nil {
				return err
			}

			registry, streamStore, bus, keyStore, secretStore, paramStore, lambdaResetter, cleanup := buildRegistry(ctx, cfg, s, dek)
			defer cleanup()

			cloudAdapter, err := buildAdapter(cfg)
			if err != nil {
				return err
			}
			adminHandler := buildAdminHandler(s, streamStore, keyStore, secretStore, paramStore, lambdaResetter)

			var certs certstore.CertStore
			if cfg.Mode == config.ModeFull {
				pgStore := s.resources.(*store.PostgresResourceStore)
				certs = certstore.NewPostgresCertStore(pgStore.Pool())
			} else {
				certs = certstore.NewMemoryCertStore()
			}

			srv := gateway.NewServer(cfg, adminHandler, registry, cloudAdapter, certs)
			_ = bus // bus is used internally by providers
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
	cmd.Flags().String("cloud", "aws", `Cloud provider to emulate: aws (default), azure, gcp.
	Env var: JAISCLOUD_CLOUD`)
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
	viper.BindPFlag("cloud", cmd.Flags().Lookup("cloud"))
	viper.BindPFlag("kms_master_key", cmd.Flags().Lookup("kms-master-key"))
	viper.BindPFlag("executor_mode", cmd.Flags().Lookup("executor-mode"))
	viper.BindPFlag("lambda_image", cmd.Flags().Lookup("lambda-image"))
	viper.BindPFlag("lambda_network", cmd.Flags().Lookup("lambda-network"))
	viper.BindPFlag("lambda_keepalive_secs", cmd.Flags().Lookup("lambda-keepalive-secs"))
}

// appStores holds all store instances that the server depends on.
type appStores struct {
	resources  store.ResourceStore
	messages   sqsstore.SQSMessageStore
	dynamo     dynamostore.DynamoDBItemStore
	s3Meta     s3store.S3ObjectMetaStore
	blobs      blobfs.BlobStore
	secrets    secretprovider.SecretStore
	parameters paramprovider.ParameterStore
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
			resources:  pgStore,
			messages:   sqsstore.NewPostgresSQSMessageStore(pool),
			dynamo:     dynamostore.NewPostgresDynamoDBItemStore(pool),
			s3Meta:     s3store.NewPostgresS3ObjectMetaStore(pool),
			blobs:      blobs,
			secrets:    secretprovider.NewPostgresSecretStore(pool),
			parameters: paramprovider.NewPostgresParameterStore(pool),
		}, nil
	}

	slog.Info("starting in lite mode")
	return appStores{
		resources:  store.NewMemoryResourceStore(),
		messages:   sqsstore.NewMemoryMessageStore(),
		dynamo:     dynamostore.NewMemoryDynamoDBItemStore(),
		s3Meta:     s3store.NewMemoryS3ObjectMetaStore(),
		blobs:      blobfs.NewMemoryBlobStore(),
		secrets:    secretprovider.NewMemorySecretStore(),
		parameters: paramprovider.NewMemoryParameterStore(),
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
		keyStore := keyprovider.NewPostgresKeyStore(pgStore.Pool(), nil) // nil DEK — only loading blob
		dek, err := keyprovider.LoadOrCreateDEK(ctx, keyStore, cfg.KMSMasterKey)
		if err != nil {
			return nil, fmt.Errorf("kms bootstrap: %w", err)
		}
		return dek, nil
	}
	// Lite mode: ephemeral DEK, not persisted.
	dek, err := keyprovider.Generate32()
	if err != nil {
		return nil, fmt.Errorf("kms bootstrap: generate ephemeral DEK: %w", err)
	}
	return dek, nil
}

// buildRegistry wires all providers and returns the populated registry plus a cleanup func.
func buildRegistry(ctx context.Context, cfg *config.Config, s appStores, dek []byte) (*provider.Registry, *streamstore.MemoryStreamStore, *events.EventBus, keyprovider.KeyStore, secretprovider.SecretStore, paramprovider.ParameterStore, admin.Resetter, func()) {
	bus := events.NewEventBus()
	streams := streamstore.NewMemoryStreamStore()

	// Build spark executor. cfg.ExecutorMode drives both Spark and Lambda.
	// "" / "mock" → instant mock completion (nil exec); "docker" / "k8s" → real executor.
	sparkExec, sparkCfg := buildSparkExecutor(cfg.ExecutorMode)
	if sparkExec != nil {
		slog.Info("spark executor enabled", "mode", cfg.ExecutorMode)
	}

	var emrOpts []emrprovider.Option
	var emrcOpts []emrcontainersprovider.Option
	if sparkExec != nil {
		emrOpts = append(emrOpts, emrprovider.WithExecutor(sparkExec, sparkCfg))
		emrcOpts = append(emrcOpts, emrcontainersprovider.WithExecutor(sparkExec, sparkCfg))
	}

	emrP := emrprovider.New(s.resources, bus, emrOpts...)
	emrcP := emrcontainersprovider.New(s.resources, bus, emrcOpts...)

	cleanup := func() {}
	if sparkExec != nil {
		poller := spark.NewStatusPoller(sparkExec, 5*time.Second, func(ev spark.StateChangeEvent) {
			emrP.OnStateChange(ev)
			emrcP.OnStateChange(ev)
		})
		emrP.SetPoller(poller)
		emrcP.SetPoller(poller)
		poller.Start(ctx)
		cleanup = func() {
			emrP.Shutdown(context.Background())
			emrcP.Shutdown(context.Background())
		}
	}

	// tableProvider is created once so both Routes() and StreamRoutes() share state.
	tableProvider := table.NewWithStreams(s.resources, s.dynamo, streams)

	// Build KMS key store and provider.
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

	// Build Lambda executor. Same ExecutorMode drives Lambda and Spark.
	lambdaCfg := lambdaexec.DefaultLambdaConfig()
	lambdaCfg.Mode = cfg.ExecutorMode
	lambdaCfg.Region = cfg.Region
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
	lambdaExec := lambdaexec.NewExecutor(lambdaCfg)
	slog.Info("lambda executor", "mode", cfg.ExecutorMode)
	prevCleanup := cleanup
	cleanup = func() { lambdaExec.Close(); prevCleanup() }

	// Named provider variables so registerCFNHandlers can reference them.
	funcP := functionprovider.NewWithExecutor(s.resources, lambdaExec)
	queueP := queue.New(s.resources, s.messages, cfg.Clock, bus)
	iamP := iamprovider.New(s.resources)
	notifP := notification.New(s.resources, s.messages, bus)
	objectP := objectprovider.New(s.s3Meta, s.blobs)
	stackP := stackprovider.New(s.resources)
	registerCFNHandlers(stackP, queueP, notifP, objectP, tableProvider, iamP, funcP, keyProv, secretProv, paramProv)

	registry := provider.NewRegistry()
	registry.RegisterAll(keyProv.Routes())
	registry.RegisterAll(secretProv.Routes())
	registry.RegisterAll(paramProv.Routes())
	registry.RegisterAll(funcP.Routes())
	registry.RegisterAll(queueP.Routes())
	registry.RegisterAll(iamP.Routes())
	registry.RegisterAll(notifP.Routes())
	registry.RegisterAll(tableProvider.Routes())
	registry.RegisterAll(tableProvider.StreamRoutes())
	registry.RegisterAll(objectP.Routes())
	registry.RegisterAll(catalog.New(s.resources).Routes())
	registry.RegisterAll(compute.New(s.resources).Routes())
	registry.RegisterAll(dns.New(s.resources).Routes())
	registry.RegisterAll(rdsprovider.New(s.resources).Routes())
	registry.RegisterAll(cacheprovider.New(s.resources).Routes())
	registry.RegisterAll(containerprovider.New(s.resources).Routes())
	registry.RegisterAll(stackP.Routes())
	registry.RegisterAll(emrP.Routes())
	registry.RegisterAll(emrcP.Routes())
	registry.RegisterAll(eksprovider.New(s.resources).Routes())
	registry.RegisterAll(eventsprovider.New(s.resources, s.messages, bus).WithPort(cfg.Port).Routes())
	registry.RegisterAll(apigwprovider.New(s.resources).Routes())

	return registry, streams, bus, keyStore, s.secrets, s.parameters, lambdaExec, cleanup
}

// buildSparkExecutor creates a SparkExecutor for the given mode.
// Returns (nil, zero) when mode is "" or "off" — instant completion remains the default.
func buildSparkExecutor(mode string) (spark.SparkExecutor, spark.SparkConfig) {
	if mode == "" || mode == "off" {
		return nil, spark.SparkConfig{}
	}
	cfg := spark.SparkConfigFrom(mode, spark.SizeSmall)
	// Generic image override — applies to both docker and k8s modes.
	// JAISCLOUD_K8S_SPARK_IMAGE (legacy) is read by SparkConfigFrom; this wins if set.
	if v := os.Getenv("JAISCLOUD_SPARK_IMAGE"); v != "" {
		cfg.Image = v
	}
	if mode == "k8s" {
		if v := os.Getenv("JAISCLOUD_K8S_APISERVER"); v != "" {
			cfg.APIServer = v
		}
		if v := os.Getenv("JAISCLOUD_K8S_NAMESPACE"); v != "" {
			cfg.Namespace = v
		}
		if v := os.Getenv("JAISCLOUD_K8S_SA"); v != "" {
			cfg.ServiceAccount = v
		}
	}
	return spark.NewExecutor(mode, cfg), cfg
}

// registerCFNHandlers wires real resource provisioning for the 9 most common
// CloudFormation resource types. Each handler delegates to the relevant provider.
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
) {
	// child builds a minimal NormalizedRequest inheriting context from the CFN
	// request so that providers receive region, accountID, port, clock, and
	// ResourceID without needing separate configuration.
	child := func(nr *model.NormalizedRequest, params map[string]any) *model.NormalizedRequest {
		return &model.NormalizedRequest{
			Region:     nr.Region,
			AccountID:  nr.AccountID,
			Port:       nr.Port,
			Cloud:      nr.Cloud,
			Clock:      nr.Clock,
			ResourceID: nr.ResourceID,
			Params:     params,
		}
	}
	propStr := func(props map[string]any, key, fallback string) string {
		if v, ok := props[key].(string); ok && v != "" {
			return v
		}
		return fallback
	}
	copyProps := func(props map[string]any) map[string]any {
		out := make(map[string]any, len(props))
		for k, v := range props {
			out[k] = v
		}
		return out
	}

	// ── AWS::SQS::Queue ──────────────────────────────────────────────────────
	stackP.RegisterHandler("AWS::SQS::Queue", stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "QueueName", logicalID)
			resp, err := queueP.CreateQueue(ctx, child(nr, map[string]any{"QueueName": name}))
			if err != nil {
				return "", nil, err
			}
			url := resp.Data["QueueUrl"].(string)
			arn := fmt.Sprintf("arn:aws:sqs:%s:%s:%s", nr.Region, nr.AccountID, name)
			return url, map[string]any{"QueueUrl": url, "Arn": arn}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := queueP.DeleteQueue(ctx, &model.NormalizedRequest{Params: map[string]any{"QueueUrl": physicalID}})
			return err
		},
	})

	// ── AWS::SNS::Topic ──────────────────────────────────────────────────────
	stackP.RegisterHandler("AWS::SNS::Topic", stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "TopicName", logicalID)
			resp, err := notifP.CreateTopic(ctx, child(nr, map[string]any{"Name": name}))
			if err != nil {
				return "", nil, err
			}
			arn := resp.Data["TopicArn"].(string)
			return arn, map[string]any{"TopicArn": arn}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := notifP.DeleteTopic(ctx, &model.NormalizedRequest{Params: map[string]any{"TopicArn": physicalID}})
			return err
		},
	})

	// ── AWS::S3::Bucket ──────────────────────────────────────────────────────
	stackP.RegisterHandler("AWS::S3::Bucket", stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			stackName, _ := nr.Params["StackName"].(string)
			defaultName := logicalID
			if stackName != "" {
				defaultName = strings.ToLower(stackName + "-" + logicalID)
			}
			name := propStr(props, "BucketName", defaultName)
			if _, err := objectP.CreateBucket(ctx, child(nr, map[string]any{"_bucket": name})); err != nil {
				return "", nil, err
			}
			arn := "arn:aws:s3:::" + name
			return name, map[string]any{"Arn": arn, "DomainName": name + ".s3.amazonaws.com"}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := objectP.DeleteBucket(ctx, &model.NormalizedRequest{Params: map[string]any{"_bucket": physicalID}})
			return err
		},
	})

	// ── AWS::DynamoDB::Table ─────────────────────────────────────────────────
	stackP.RegisterHandler("AWS::DynamoDB::Table", stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "TableName", logicalID)
			params := copyProps(props)
			params["TableName"] = name
			if _, err := tableP.CreateTable(ctx, child(nr, params)); err != nil {
				return "", nil, err
			}
			arn := fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", nr.Region, nr.AccountID, name)
			return name, map[string]any{"Arn": arn, "StreamArn": ""}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := tableP.DeleteTable(ctx, &model.NormalizedRequest{Params: map[string]any{"TableName": physicalID}})
			return err
		},
	})

	// ── AWS::IAM::Role ───────────────────────────────────────────────────────
	stackP.RegisterHandler("AWS::IAM::Role", stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "RoleName", logicalID)
			params := copyProps(props)
			params["RoleName"] = name
			resp, err := iamP.CreateRole(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			arn := ""
			if rm, ok := resp.Data["Role"].(map[string]any); ok {
				arn, _ = rm["Arn"].(string)
			}
			return name, map[string]any{"Arn": arn}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := iamP.DeleteRole(ctx, &model.NormalizedRequest{
				AccountID: "000000000000",
				Params:    map[string]any{"RoleName": physicalID},
			})
			return err
		},
	})

	// ── AWS::Lambda::Function ────────────────────────────────────────────────
	stackP.RegisterHandler("AWS::Lambda::Function", stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "FunctionName", logicalID)
			params := copyProps(props)
			params["FunctionName"] = name
			resp, err := funcP.CreateFunction(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			arn, _ := resp.Data["FunctionArn"].(string)
			return name, map[string]any{"Arn": arn}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := funcP.DeleteFunction(ctx, &model.NormalizedRequest{Params: map[string]any{"_function_name": physicalID}})
			return err
		},
	})

	// ── AWS::SSM::Parameter ──────────────────────────────────────────────────
	stackP.RegisterHandler("AWS::SSM::Parameter", stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "Name", "/cfn/"+logicalID)
			params := copyProps(props)
			params["Name"] = name
			if _, err := paramP.PutParameter(ctx, child(nr, params)); err != nil {
				return "", nil, err
			}
			return name, map[string]any{}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := paramP.DeleteParameter(ctx, &model.NormalizedRequest{Params: map[string]any{"Name": physicalID}})
			return err
		},
	})

	// ── AWS::SecretsManager::Secret ──────────────────────────────────────────
	stackP.RegisterHandler("AWS::SecretsManager::Secret", stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "Name", logicalID)
			params := copyProps(props)
			params["Name"] = name
			resp, err := secretP.CreateSecret(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			arn, _ := resp.Data["ARN"].(string)
			return arn, map[string]any{"Id": name}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := secretP.DeleteSecret(ctx, &model.NormalizedRequest{
				ResourceID: func(_, _ string) string { return physicalID },
				Params:     map[string]any{"SecretId": physicalID, "ForceDeleteWithoutRecovery": true},
			})
			return err
		},
	})

	// ── AWS::KMS::Key ────────────────────────────────────────────────────────
	stackP.RegisterHandler("AWS::KMS::Key", stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			params := copyProps(props)
			resp, err := keyP.CreateKey(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			keyID, arn := "", ""
			if km, ok := resp.Data["KeyMetadata"].(map[string]any); ok {
				keyID, _ = km["KeyId"].(string)
				arn, _ = km["Arn"].(string)
			}
			return keyID, map[string]any{"Arn": arn, "KeyId": keyID}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := keyP.ScheduleKeyDeletion(ctx, &model.NormalizedRequest{
				Params: map[string]any{"KeyId": physicalID, "PendingWindowInDays": float64(7)},
			})
			return err
		},
	})
}

// buildAdapter selects and constructs the single CloudAdapter for the configured cloud.
func buildAdapter(cfg *config.Config) (adapter.CloudAdapter, error) {
	switch cfg.Cloud {
	case "aws", "":
		return buildAWSAdapter(), nil
	case "azure":
		return azureadapter.New(), nil
	case "gcp":
		return gcpadapter.New(), nil
	default:
		return nil, fmt.Errorf("unknown cloud %q: must be aws, azure, or gcp", cfg.Cloud)
	}
}

// buildAWSAdapter constructs all AWS service codecs and returns the wired adapter.
func buildAWSAdapter() *awsadapter.AWSAdapter {
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
		"s3":              &services.S3Codec{},
		"lambda":          &services.LambdaCodec{},
		"glue":            &services.GlueCodec{},
		"ec2":             services.NewEC2Codec("ec2"),
		"route53":         &services.Route53Codec{},
		"rds":             &services.RDSCodec{},
		"elasticache":     &services.ElastiCacheCodec{},
		"ecs":             &services.ECSCodec{},
		"dynamodbstreams": &services.DynamoDBStreamsCodec{},
		"cloudformation":  &services.CloudFormationCodec{},
		"emr":              &services.EMRCodec{},
		"emr-containers":   &services.EMRContainersCodec{},
		"events":           &services.EventBridgeCodec{},
		"eks":              &services.EKSCodec{},
		"apigateway":       &services.APIGatewayCodec{},
		"execute-api":      &services.ExecuteAPICodec{},
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
	for _, r := range extra {
		if r != nil {
			h.RegisterResetter(r)
		}
	}
	if snap, ok := s.resources.(admin.Snapshotter); ok {
		h.RegisterSnapshotter("resources", snap)
	}
	return h
}

// ─── version ──────────────────────────────────────────────────────────────────

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("jaiscloud %s\n", version)
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
			fmt.Printf("JAISCLOUD_CLOUD=%s\n", cfg.Cloud)
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
			fmt.Printf("OK: jaiscloud is running at %s\n", host)
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

			// Pretty-print
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

			resp, err := http.Post(host+"/_jaiscloud/import", "application/json",
				bytes.NewReader(data))
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
	return cmd
}

