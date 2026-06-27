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
	"smf/internal/udm"
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

	logger.Log.Info("Starting UDM Network Function...",
		zap.String("listenAddr", cfg.UDM.ListenAddr))

	repo := udm.InitSubscriberRepository(cfg.UDM.DatabaseUrl)
	handler := udm.NewHandler(repo)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.HealthCheck)
	mux.HandleFunc("GET /nudm-sdm/v2/{imsi}/sm-data", handler.GetSubscriptionData)

	h2s := &http2.Server{}
	server := &http.Server{
		Addr:    cfg.UDM.ListenAddr,
		Handler: h2c.NewHandler(mux, h2s),
	}

	go func() {
		logger.Log.Info("UDM HTTP/2 h2c server listening", zap.String("addr", cfg.UDM.ListenAddr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log.Fatal("UDM Server failed to start", zap.Error(err))
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Log.Info("Shutting down UDM server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Log.Error("UDM server shutdown failed", zap.Error(err))
	}
	logger.Log.Info("UDM server stopped")
}
