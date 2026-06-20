package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"smf/internal/udm"
	"smf/pkg/logger"
)

func main() {
	logger.InitLogger()
	defer logger.Log.Sync()

	logger.Log.Info("Starting UDM Network Function...")

	connStr := os.Getenv("DATABASE_URL")
	repo := udm.InitSubscriberRepository(connStr)
	handler := udm.NewHandler(repo)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.HealthCheck)
	mux.HandleFunc("GET /nudm-sdm/v2/{imsi}/sm-data", handler.GetSubscriptionData)

	h2s := &http2.Server{}
	server := &http.Server{
		Addr:    ":8082",
		Handler: h2c.NewHandler(mux, h2s),
	}

	go func() {
		logger.Log.Info("UDM HTTP/2 h2c server listening on port 8082")
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
