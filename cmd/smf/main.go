package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"smf/internal/smf"
	"smf/pkg/config"
	"smf/pkg/logger"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	logger.InitLogger()
	defer logger.Log.Sync()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	logger.Log.Info("Starting SMF Network Function...",
		zap.String("listenAddr", cfg.SMF.ListenAddr),
		zap.String("upfAddr", cfg.SMF.UpfAddr),
		zap.Int("maxWorkers", cfg.SMF.MaxWorkers))

	// 1. Initialize Database Repository (Postgres or In-Memory fallback)
	repo := smf.InitRepository(cfg.SMF.DatabaseUrl)

	// 2. Initialize PFCP client
	pfcpClient, err := smf.NewPFCPClient(cfg.SMF.UpfAddr)
	if err != nil {
		logger.Log.Fatal("SMF: Failed to initialize PFCP UDP client", zap.Error(err))
	}
	defer pfcpClient.Close()

	// 3. Initialize Worker Pool Orchestrator
	smf.InitOrchestrator(repo, pfcpClient, cfg.SMF.MaxWorkers, cfg.SMF.QueueSize)
	defer smf.Orc.Stop()

	// 4. Initialize Dashboard SSE stream hub
	smf.InitDashboardHub()

	// 5. Setup HTTP/2 cleartext server (h2c)
	handler := smf.NewHandler(repo)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.HealthCheck)
	mux.HandleFunc("POST /nsmf-pdusession/v1/sm-contexts", handler.CreateSMContext)
	mux.HandleFunc("POST /nsmf-pdusession/v1/sm-contexts/{smContextRef}/modify", handler.UpdateSMContext)
	mux.HandleFunc("POST /api/trigger", handler.TriggerProxy)
	mux.HandleFunc("GET /api/sessions", handler.GetSessions)
	mux.HandleFunc("GET /api/stats", handler.GetStats)

	// Serve the real-time SSE stream for dashboard
	mux.Handle("GET /dashboard/stream", smf.Hub)

	// Serve static files for dashboard frontend
	fs := http.FileServer(http.Dir(cfg.SMF.WebDir))
	mux.Handle("GET /dashboard/", http.StripPrefix("/dashboard/", fs))

	h2s := &http2.Server{}
	server := &http.Server{
		Addr:    cfg.SMF.ListenAddr,
		Handler: h2c.NewHandler(mux, h2s),
	}

	go func() {
		logger.Log.Info("SMF HTTP/2 h2c server listening", zap.String("addr", cfg.SMF.ListenAddr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log.Fatal("SMF Server failed to start", zap.Error(err))
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Log.Info("Shutting down SMF server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Log.Error("SMF server shutdown failed", zap.Error(err))
	}
	logger.Log.Info("SMF server stopped")
}
