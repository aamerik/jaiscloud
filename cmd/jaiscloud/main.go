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

			registry, streamStore, bus, keyStore, secretStore, paramStore, cleanup := buildRegistry(ctx, cfg, s, dek)
			defer cleanup()

			cloudAdapter, err := buildAdapter(cfg)
			if err != nil {
				return err
			}
			adminHandler := buildAdminHandler(s, streamStore, keyStore, secretStore, paramStore)

			srv := gateway.NewServer(cfg, adminHandler, registry, cloudAdapter)
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
	cmd.Flags().String("lambda-mode", "mock", `Lambda executor mode: mock (default), docker, k8s.
	Env var: JAISCLOUD_LAMBDA_MODE`)
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
	viper.BindPFlag("lambda_mode", cmd.Flags().Lookup("lambda-mode"))
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
func buildRegistry(ctx context.Context, cfg *config.Config, s appStores, dek []byte) (*provider.Registry, *streamstore.MemoryStreamStore, *events.EventBus, keyprovider.KeyStore, secretprovider.SecretStore, paramprovider.ParameterStore, func()) {
	bus := events.NewEventBus()
	streams := streamstore.NewMemoryStreamStore()

	// Build spark executor (nil when JAISCLOUD_SPARK_MODE=off or unset).
	sparkExec, sparkCfg := buildSparkExecutor(cfg.Executors.Spark)
	if sparkExec != nil {
		slog.Info("spark executor enabled", "mode", cfg.Executors.Spark)
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

	// Build Lambda executor.
	lambdaCfg := lambdaexec.LambdaConfig{
		Mode:          cfg.LambdaMode,
		DefaultImage:  cfg.LambdaImage,
		Network:       cfg.LambdaNetwork,
		KeepaliveSecs: cfg.LambdaKeepaliveSecs,
	}
	lambdaExec := lambdaexec.NewExecutor(lambdaCfg)
	slog.Info("lambda executor", "mode", cfg.LambdaMode)
	prevCleanup := cleanup
	cleanup = func() { lambdaExec.Close(); prevCleanup() }

	registry := provider.NewRegistry()
	registry.RegisterAll(keyProv.Routes())
	registry.RegisterAll(secretProv.Routes())
	registry.RegisterAll(paramProv.Routes())
	registry.RegisterAll(functionprovider.NewWithExecutor(s.resources, lambdaExec).Routes())
	registry.RegisterAll(queue.New(s.resources, s.messages, cfg.Clock, bus).Routes())
	registry.RegisterAll(iamprovider.New(s.resources).Routes())
	registry.RegisterAll(notification.New(s.resources, s.messages, bus).Routes())
	registry.RegisterAll(tableProvider.Routes())
	registry.RegisterAll(tableProvider.StreamRoutes())
	registry.RegisterAll(objectprovider.New(s.s3Meta, s.blobs).Routes())
	registry.RegisterAll(catalog.New(s.resources).Routes())
	registry.RegisterAll(compute.New(s.resources).Routes())
	registry.RegisterAll(dns.New(s.resources).Routes())
	registry.RegisterAll(rdsprovider.New(s.resources).Routes())
	registry.RegisterAll(cacheprovider.New(s.resources).Routes())
	registry.RegisterAll(containerprovider.New(s.resources).Routes())
	registry.RegisterAll(stackprovider.New(s.resources).Routes())
	registry.RegisterAll(emrP.Routes())
	registry.RegisterAll(emrcP.Routes())
	registry.RegisterAll(eksprovider.New(s.resources).Routes())
	registry.RegisterAll(eventsprovider.New(s.resources, s.messages, bus).WithPort(cfg.Port).Routes())
	registry.RegisterAll(apigwprovider.New(s.resources).Routes())

	return registry, streams, bus, keyStore, s.secrets, s.parameters, cleanup
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
func buildAdminHandler(s appStores, streams *streamstore.MemoryStreamStore, keyStore keyprovider.KeyStore, secretStore secretprovider.SecretStore, paramStore paramprovider.ParameterStore) *admin.Handler {
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

