package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"smf/internal/upf"
	"smf/pkg/logger"
)

func main() {
	logger.InitLogger()
	defer logger.Log.Sync()

	logger.Log.Info("Starting UPF Network Function...")

	// Start UDP Server
	server, err := upf.NewServer(":8805")
	if err != nil {
		logger.Log.Fatal("UPF: Failed to initialize UDP server", zap.Error(err))
	}

	if err := server.Start(); err != nil {
		logger.Log.Fatal("UPF: Failed to start UDP server", zap.Error(err))
	}
	defer server.Stop()

	// Start HTTP health check server on port 8083
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", upf.HealthCheck)
	
	httpServer := &http.Server{
		Addr:    ":8083",
		Handler: mux,
	}

	go func() {
		logger.Log.Info("UPF HTTP health check server listening on port 8083")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log.Error("UPF HTTP Server failed", zap.Error(err))
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Log.Info("Shutting down UPF...")
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Log.Error("UPF HTTP server shutdown failed", zap.Error(err))
	}
	
	logger.Log.Info("UPF stopped")
}
