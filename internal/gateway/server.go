package gateway

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/admin"
	"jaiscloud/internal/certstore"
	"jaiscloud/internal/config"
	"jaiscloud/internal/gateway/middleware"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server is the JaisCloud HTTP server.
type Server struct {
	cfg          *config.Config
	router       chi.Router
	adminHandler *admin.Handler
	registry     *provider.Registry
	cloudAdapter adapter.CloudAdapter
	certs        certstore.CertStore
}

func NewServer(cfg *config.Config, adminHandler *admin.Handler, registry *provider.Registry, cloudAdapter adapter.CloudAdapter, certs certstore.CertStore) *Server {
	s := &Server{
		cfg:          cfg,
		adminHandler: adminHandler,
		registry:     registry,
		cloudAdapter: cloudAdapter,
		certs:        certs,
	}
	s.buildRouter()
	return s
}

func (s *Server) buildRouter() {
	r := chi.NewRouter()

	r.Use(middleware.Recovery)
	r.Use(middleware.RequestID(s.cfg.RandSource))
	r.Use(middleware.Logging(s.cfg.LogLevel))
	if s.cfg.Metrics {
		r.Use(middleware.Metrics)
	}

	r.Route("/_jaiscloud", func(r chi.Router) {
		r.Get("/health", s.adminHandler.Health)
		r.Post("/reset", s.adminHandler.Reset)
		r.Get("/export", s.adminHandler.Export)
		r.Post("/import", s.adminHandler.Import)
	})

	if s.cfg.Metrics {
		r.Handle("/metrics", promhttp.Handler())
	}

	r.HandleFunc("/*", s.handleCloudRequest)

	s.router = r
}

func (s *Server) handleCloudRequest(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("failed to read request body",
			"err", err,
			"method", r.Method,
			"path", r.URL.Path,
			"request_id", middleware.GetRequestID(r.Context()),
		)
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}

	nr, codec, detectErr := s.cloudAdapter.DetectAndDecode(r, body)
	if detectErr != nil {
		if pe, ok := detectErr.(*model.ProviderError); ok {
			slog.Error("service detection error",
				"code", pe.Code,
				"status", pe.HTTPStatus,
				"method", r.Method,
				"path", r.URL.Path,
				"request_id", middleware.GetRequestID(r.Context()),
			)
			status, headers, respBody := encodeErrorFallback(codec, nil, pe)
			writeResponse(w, status, headers, respBody)
			return
		}
		slog.Error("service detection failed",
			"err", detectErr,
			"method", r.Method,
			"path", r.URL.Path,
			"request_id", middleware.GetRequestID(r.Context()),
		)
		http.Error(w, detectErr.Error(), http.StatusBadRequest)
		return
	}

	// Inject gateway context
	nr.Clock = s.cfg.Clock
	nr.Region = s.cfg.Region
	nr.AccountID = s.cfg.AccountID
	nr.Port = s.cfg.Port
	nr.Cloud = s.cloudAdapter.Cloud()
	switch s.cloudAdapter.Cloud() {
	case model.CloudAWS:
		nr.ResourceID = config.AWSResourceID(s.cfg.Region, s.cfg.AccountID)
	case model.CloudAzure:
		nr.ResourceID = config.AzureResourceID(s.cfg.Region, s.cfg.AccountID)
	case model.CloudGCP:
		nr.ResourceID = config.GCPResourceID(s.cfg.AccountID)
	}

	// Attach labels for Prometheus metrics middleware
	r = r.WithContext(middleware.WithRequestLabels(r.Context(), string(nr.Cloud), nr.Service, nr.Action))

	providerKey := s.cloudAdapter.ServiceToProvider(nr.Service) + "." + nr.Action

	resp, dispatchErr := s.registry.Dispatch(r.Context(), providerKey, nr)
	if dispatchErr != nil {
		if pe, ok := dispatchErr.(*model.ProviderError); ok {
			slog.Error("provider error",
				"code", pe.Code,
				"status", pe.HTTPStatus,
				"key", providerKey,
				"request_id", middleware.GetRequestID(r.Context()),
			)
			status, headers, respBody := codec.EncodeError(nr, pe)
			writeResponse(w, status, headers, respBody)
			return
		}
		slog.Error("dispatch error",
			"key", providerKey,
			"err", dispatchErr,
			"request_id", middleware.GetRequestID(r.Context()),
		)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	status, headers, respBody := codec.Encode(nr, resp)
	writeResponse(w, status, headers, respBody)
}

func encodeErrorFallback(codec adapter.Codec, nr *model.NormalizedRequest, pe *model.ProviderError) (int, http.Header, []byte) {
	if codec != nil {
		return codec.EncodeError(nr, pe)
	}
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	body := fmt.Sprintf(`{"__type":%q,"message":%q}`, pe.Code, pe.Message)
	return pe.HTTPStatus, h, []byte(body)
}

func writeResponse(w http.ResponseWriter, status int, headers http.Header, body []byte) {
	for k, vs := range headers {
		for _, v := range vs {
			w.Header().Set(k, v)
		}
	}
	if w.Header().Get("Content-Length") == "" {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	}
	w.WriteHeader(status)
	w.Write(body)
}

func (s *Server) loadOrGenerateCert(ctx context.Context) (tls.Certificate, error) {
	stored, err := s.certs.Load(ctx)
	if err == nil && !stored.NeedsRenewal() {
		cert, parseErr := tls.X509KeyPair(stored.CertPEM, stored.KeyPEM)
		if parseErr == nil {
			return cert, nil
		}
		slog.Warn("https: stored cert parse failed, regenerating", "err", parseErr)
	}

	cert, genErr := generateSelfSignedCert(string(s.cfg.Cloud), s.cfg.Region)
	if genErr != nil {
		return tls.Certificate{}, genErr
	}

	leaf, leafErr := x509.ParseCertificate(cert.Certificate[0])
	if leafErr == nil {
		certPEM := pemEncode("CERTIFICATE", cert.Certificate[0])
		keyDER, _ := x509.MarshalECPrivateKey(cert.PrivateKey.(*ecdsa.PrivateKey))
		keyPEM := pemEncode("EC PRIVATE KEY", keyDER)
		if saveErr := s.certs.Save(ctx, &certstore.StoredCert{
			CertPEM:  certPEM,
			KeyPEM:   keyPEM,
			NotAfter: leaf.NotAfter,
		}); saveErr != nil {
			slog.Warn("https: could not persist TLS cert", "err", saveErr)
		}
	}

	return cert, nil
}

func (s *Server) ListenAndServe() error {
	addr := fmt.Sprintf(":%d", s.cfg.Port)
	srv := &http.Server{Addr: addr, Handler: s.router}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("jaiscloud started", "port", s.cfg.Port, "mode", s.cfg.Mode)
		if serveErr := srv.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
			slog.Error("server error", "err", serveErr)
		}
	}()

	// Start HTTPS listener on :443.
	var tlsSrv *http.Server
	go func() {
		cert, tlsErr := tls.LoadX509KeyPair("/etc/tls/tls.crt", "/etc/tls/tls.key")
		if tlsErr != nil {
			cert, tlsErr = s.loadOrGenerateCert(ctx)
			if tlsErr != nil {
				slog.Warn("https: could not generate TLS cert, HTTPS disabled", "err", tlsErr)
				return
			}
		}
		tlsSrv = &http.Server{
			Addr:    ":443",
			Handler: s.router,
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{cert},
				MinVersion:   tls.VersionTLS12,
			},
		}
		tlsLn, listenErr := net.Listen("tcp", ":443")
		if listenErr != nil {
			slog.Warn("https: could not bind :443, HTTPS disabled", "err", listenErr)
			return
		}
		slog.Info("jaiscloud https started", "port", 443)
		if serveErr := tlsSrv.ServeTLS(tlsLn, "", ""); serveErr != nil && serveErr != http.ErrServerClosed {
			slog.Warn("https server error", "err", serveErr)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if tlsSrv != nil {
		tlsSrv.Shutdown(shutdownCtx) //nolint:errcheck
	}
	return srv.Shutdown(shutdownCtx)
}

// cloudDNSNames returns cloud-specific DNS SANs for the self-signed certificate.
func cloudDNSNames(cloud, region string) []string {
	switch cloud {
	case "azure":
		return []string{
			"*.blob.core.windows.net",
			"*.table.core.windows.net",
			"*.queue.core.windows.net",
			"*.vault.azure.net",
			"management.azure.com",
			"*.management.azure.com",
			"*.documents.azure.com",
			"*.azurewebsites.net",
		}
	case "gcp":
		return []string{
			"*.googleapis.com",
			"storage.googleapis.com",
			"compute.googleapis.com",
			"cloudfunctions.googleapis.com",
			"run.googleapis.com",
			"firestore.googleapis.com",
			"secretmanager.googleapis.com",
			"cloudkms.googleapis.com",
		}
	default: // aws
		regional := func(svc string) string {
			return fmt.Sprintf("%s.%s.amazonaws.com", svc, region)
		}
		return []string{
			"*.amazonaws.com",
			fmt.Sprintf("*.%s.amazonaws.com", region),
			fmt.Sprintf("*.s3.%s.amazonaws.com", region),
			"*.s3.amazonaws.com",
			regional("s3"),
			regional("sqs"),
			regional("sns"),
			regional("dynamodb"),
			regional("lambda"),
			regional("sts"),
			"iam.amazonaws.com",
			regional("kms"),
			regional("secretsmanager"),
			regional("ssm"),
			regional("elasticmapreduce"),
			regional("emr-containers"),
			regional("events"),
			regional("cloudformation"),
			regional("apigateway"),
			regional("execute-api"),
			regional("glue"),
			regional("ec2"),
			"route53.amazonaws.com",
			regional("rds"),
			regional("elasticache"),
			regional("ecs"),
		}
	}
}

// generateSelfSignedCert creates an ECDSA P-256 self-signed certificate with
// cloud-specific DNS SANs valid for 10 years.
func generateSelfSignedCert(cloud, region string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate key: %w", err)
	}

	hostname, _ := os.Hostname()
	dnsNames := append([]string{"localhost"}, cloudDNSNames(cloud, region)...)
	if hostname != "" {
		dnsNames = append(dnsNames, hostname)
	}

	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "jaiscloud"},
		DNSNames:     dnsNames,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create certificate: %w", err)
	}

	return tls.X509KeyPair(
		pemEncode("CERTIFICATE", certDER),
		pemEncodeKey(key),
	)
}

func pemEncode(typ string, der []byte) []byte {
	return []byte(fmt.Sprintf("-----BEGIN %s-----\n%s\n-----END %s-----\n",
		typ, encodeBase64Lines(der), typ))
}

func pemEncodeKey(key *ecdsa.PrivateKey) []byte {
	der, _ := x509.MarshalECPrivateKey(key)
	return pemEncode("EC PRIVATE KEY", der)
}

func encodeBase64Lines(data []byte) string {
	const lineLen = 64
	encoded := make([]byte, ((len(data)+2)/3)*4)
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	n := 0
	for i := 0; i < len(data); i += 3 {
		var b [3]byte
		l := copy(b[:], data[i:])
		encoded[n] = alphabet[b[0]>>2]
		encoded[n+1] = alphabet[(b[0]&0x3)<<4|b[1]>>4]
		if l > 1 {
			encoded[n+2] = alphabet[(b[1]&0xF)<<2|b[2]>>6]
		} else {
			encoded[n+2] = '='
		}
		if l > 2 {
			encoded[n+3] = alphabet[b[2]&0x3F]
		} else {
			encoded[n+3] = '='
		}
		n += 4
	}
	encoded = encoded[:n]

	// Insert newlines every lineLen characters.
	var out []byte
	for len(encoded) > lineLen {
		out = append(out, encoded[:lineLen]...)
		out = append(out, '\n')
		encoded = encoded[lineLen:]
	}
	out = append(out, encoded...)
	return string(out)
}
