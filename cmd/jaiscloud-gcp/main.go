package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"jaiscloud/internal/admin"
	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/certstore"
	"jaiscloud/internal/config"
	"jaiscloud/internal/gateway"
	gcpadapter "jaiscloud/internal/gcp/adapter"
	storageprovider "jaiscloud/internal/gcp/provider/storage"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const version = "0.2.0"

func main() {
	root := &cobra.Command{
		Use:   "jaiscloud-gcp",
		Short: "JaisCloud GCP - local GCP emulator",
	}
	root.AddCommand(startCmd())
	root.AddCommand(versionCmd())
	root.AddCommand(doctorCmd())
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

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
			cfg.Cloud = model.CloudGCP
			// GCP has no account ID; map the project onto the account scope used
			// by the shared store. Region defaults to global (GCP services omit
			// region from most REST paths).
			cfg.AccountID = cfg.ProjectID
			cfg.Region = "global"
			// config.Load defaults the port to 4566 (the AWS default); GCP listens
			// on 8080 unless the user overrides via --port or JAISCLOUD_PORT.
			if !cmd.Flags().Changed("port") && os.Getenv("JAISCLOUD_PORT") == "" {
				cfg.Port = 8080
			}

			ctx := context.Background()

			resources, blobs, closeFn, err := initStores(ctx, cfg)
			if err != nil {
				return err
			}
			defer closeFn()

			storageP := storageprovider.New(resources, blobs)

			reg := provider.NewRegistry().Register(storageP)

			adminHandler := admin.NewHandler()
			adminHandler.RegisterResetter(resources)
			adminHandler.RegisterResetter(blobs)
			adminHandler.RegisterResetter(storageP)
			if snap, ok := resources.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("resources", snap)
			}
			if sb, ok := blobs.(admin.SnapshotBlobStore); ok {
				adminHandler.RegisterBlobStore(sb)
			}
			adminHandler.SetMeta(admin.HandlerMeta{
				Cloud:     "gcp",
				Region:    cfg.Region,
				AccountID: cfg.ProjectID,
				StateDir:  cfg.DataDir,
			})

			cloudAdapter := gcpadapter.NewAdapter(cfg.GCPServiceAccount)

			stateDir, _ := config.ResolveStateDir(os.Getenv("JAISCLOUD_STATE_DIR"))
			var certs certstore.CertStore
			if fsCS, err := certstore.NewFilesystemCertStore(stateDir); err == nil {
				certs = fsCS
			} else {
				certs = certstore.NewMemoryCertStore()
			}

			var gatewayOpts []func(*gateway.Server)
			if cfg.GCPMetadataEnabled {
				metaCfg := gcpadapter.MetadataConfig{
					ProjectID:      cfg.ProjectID,
					ServiceAccount: cfg.GCPServiceAccount,
				}
				gatewayOpts = append(gatewayOpts, gateway.WithExtraRoutes(func(r chi.Router) {
					gcpadapter.RegisterMetadataRoutes(r, metaCfg)
				}))
			}

			srv := gateway.NewServer(cfg, adminHandler, reg, cloudAdapter, certs, gatewayOpts...)
			return srv.ListenAndServe()
		},
	}
	cmd.Flags().Int("port", 8080, "Listen port")
	cmd.Flags().Bool("ephemeral", false, "Run with purely in-memory state (no persistence)")
	cmd.Flags().String("dsn", "", "PostgreSQL connection string (postgres://...)")
	cmd.Flags().String("data-dir", "", "Root data directory for blobs and snapshots")
	cmd.Flags().String("log-level", "info", "Log level: debug/info/warn/error")
	cmd.Flags().Bool("metrics", false, "Expose Prometheus metrics at /metrics")
	cmd.Flags().Bool("gcp-metadata", false, "Enable the GCP metadata-server emulator")
	return cmd
}

func bindFlags(cmd *cobra.Command) {
	viper.BindPFlag("port", cmd.Flags().Lookup("port"))
	viper.BindPFlag("ephemeral", cmd.Flags().Lookup("ephemeral"))
	viper.BindPFlag("dsn", cmd.Flags().Lookup("dsn"))
	viper.BindPFlag("data_dir", cmd.Flags().Lookup("data-dir"))
	viper.BindPFlag("log_level", cmd.Flags().Lookup("log-level"))
	viper.BindPFlag("metrics", cmd.Flags().Lookup("metrics"))
	viper.BindPFlag("gcp_metadata_enabled", cmd.Flags().Lookup("gcp-metadata"))
}

func initStores(ctx context.Context, cfg *config.Config) (store.ResourceStore, blobfs.BlobStore, func(), error) {
	if cfg.DSN != "" {
		pg, err := store.NewPostgresResourceStore(ctx, cfg.DSN, string(cfg.Cloud))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("postgres: %w", err)
		}
		blobs, err := blobfs.NewLocalFSBlobStore(cfg.BlobDir)
		if err != nil {
			pg.Close()
			return nil, nil, nil, fmt.Errorf("blobfs: %w", err)
		}
		return pg, blobs, func() { pg.Close() }, nil
	}
	return store.NewMemoryResourceStore(), blobfs.NewMemoryBlobStore(), func() {}, nil
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("jaiscloud-gcp %s\n", version)
		},
	}
}

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
			fmt.Printf("OK: jaiscloud-gcp is running at %s\n", host)
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:8080", "Emulator host URL")
	return cmd
}
