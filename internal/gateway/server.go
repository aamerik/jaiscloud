package gateway

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"jaiscloud/internal/adapter"
	awsadapter "jaiscloud/internal/adapter/aws"
	"jaiscloud/internal/admin"
	"jaiscloud/internal/config"
	"jaiscloud/internal/gateway/middleware"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Server is the JaisCloud HTTP server.
type Server struct {
	cfg          *config.Config
	router       chi.Router
	adminHandler *admin.Handler
	registry     *provider.Registry
	awsAdapter   *awsadapter.AWSAdapter
}

func NewServer(cfg *config.Config, adminHandler *admin.Handler, registry *provider.Registry, awsAdapter *awsadapter.AWSAdapter) *Server {
	s := &Server{
		cfg:          cfg,
		adminHandler: adminHandler,
		registry:     registry,
		awsAdapter:   awsAdapter,
	}
	s.buildRouter()
	return s
}

func (s *Server) buildRouter() {
	r := chi.NewRouter()

	r.Use(middleware.Recovery)
	r.Use(middleware.RequestID(s.cfg.RandSource))
	r.Use(middleware.Logging(s.cfg.LogLevel))

	r.Route("/_jaiscloud", func(r chi.Router) {
		r.Get("/health", s.adminHandler.Health)
		r.Post("/reset", s.adminHandler.Reset)
	})

	r.HandleFunc("/*", s.handleCloudRequest)

	s.router = r
}

func (s *Server) handleCloudRequest(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}

	nr, codec, detectErr := s.awsAdapter.DetectAndDecode(r, body)
	if detectErr != nil {
		if pe, ok := detectErr.(*model.ProviderError); ok {
			status, headers, respBody := encodeErrorFallback(codec, nil, pe)
			writeResponse(w, status, headers, respBody)
			return
		}
		http.Error(w, detectErr.Error(), http.StatusBadRequest)
		return
	}

	// Inject gateway context
	nr.Clock = s.cfg.Clock
	nr.Region = s.cfg.Region
	nr.AccountID = s.cfg.AccountID
	nr.Port = s.cfg.Port

	providerKey := serviceToProvider(nr.Service) + "." + nr.Action

	resp, dispatchErr := s.registry.Dispatch(r.Context(), providerKey, nr)
	if dispatchErr != nil {
		if pe, ok := dispatchErr.(*model.ProviderError); ok {
			status, headers, respBody := codec.EncodeError(nr, pe)
			writeResponse(w, status, headers, respBody)
			return
		}
		slog.Error("dispatch error", "key", providerKey, "err", dispatchErr)
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

func serviceToProvider(service string) string {
	switch service {
	case "sqs":
		return "Queue"
	default:
		return service
	}
}

func writeResponse(w http.ResponseWriter, status int, headers http.Header, body []byte) {
	for k, vs := range headers {
		for _, v := range vs {
			w.Header().Set(k, v)
		}
	}
	w.WriteHeader(status)
	w.Write(body)
}

func (s *Server) ListenAndServe() error {
	addr := fmt.Sprintf(":%d", s.cfg.Port)
	srv := &http.Server{Addr: addr, Handler: s.router}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("jaiscloud started", "port", s.cfg.Port, "mode", s.cfg.Mode)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
