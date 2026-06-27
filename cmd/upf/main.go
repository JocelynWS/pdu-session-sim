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
	"smf/internal/upf"
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

	logger.Log.Info("Starting UPF Network Function...",
		zap.String("pfcpAddr", cfg.UPF.PfcpAddr),
		zap.String("httpAddr", cfg.UPF.HttpAddr))

	// Start UDP Server for PFCP
	server, err := upf.NewServer(cfg.UPF.PfcpAddr)
	if err != nil {
		logger.Log.Fatal("UPF: Failed to initialize UDP server", zap.Error(err))
	}

	if err := server.Start(); err != nil {
		logger.Log.Fatal("UPF: Failed to start UDP server", zap.Error(err))
	}
	defer server.Stop()

	// Start HTTP health check server
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", upf.HealthCheck)

	httpServer := &http.Server{
		Addr:    cfg.UPF.HttpAddr,
		Handler: mux,
	}

	go func() {
		logger.Log.Info("UPF HTTP health check server listening", zap.String("addr", cfg.UPF.HttpAddr))
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
