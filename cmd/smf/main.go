package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"smf/internal/smf"
	"smf/pkg/logger"
)

func main() {
	logger.InitLogger()
	defer logger.Log.Sync()

	logger.Log.Info("Starting SMF Network Function...")

	// 1. Initialize Database Repository (Postgres or In-Memory fallback)
	connStr := os.Getenv("DATABASE_URL")
	repo := smf.InitRepository(connStr)

	// 2. Initialize PFCP client (communicates with UPF UDP on 8805)
	upfAddr := os.Getenv("UPF_ADDR")
	if upfAddr == "" {
		upfAddr = "localhost:8805"
	}
	pfcpClient, err := smf.NewPFCPClient(upfAddr)
	if err != nil {
		logger.Log.Fatal("SMF: Failed to initialize PFCP UDP client", zap.Error(err))
	}
	defer pfcpClient.Close()

	// 3. Initialize Worker Pool Orchestrator
	maxWorkers := 20
	if workersStr := os.Getenv("MAX_WORKERS"); workersStr != "" {
		if w, err := strconv.Atoi(workersStr); err == nil {
			maxWorkers = w
		}
	}
	smf.InitOrchestrator(repo, pfcpClient, maxWorkers)
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

	// Serve the real-time SSE stream for dashboard
	mux.Handle("GET /dashboard/stream", smf.Hub)
	
	// Serve static files for dashboard frontend
	fs := http.FileServer(http.Dir("web"))
	mux.Handle("GET /dashboard/", http.StripPrefix("/dashboard/", fs))

	h2s := &http2.Server{}
	server := &http.Server{
		Addr:    ":8081",
		Handler: h2c.NewHandler(mux, h2s),
	}

	go func() {
		logger.Log.Info("SMF HTTP/2 h2c server listening on port 8081")
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
