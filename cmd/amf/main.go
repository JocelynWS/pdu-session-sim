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
	"smf/internal/amf"
	"smf/pkg/logger"
)

func main() {
	logger.InitLogger()
	defer logger.Log.Sync()

	logger.Log.Info("Starting AMF Network Function...")

	smfBaseUrl := os.Getenv("SMF_BASE_URL")
	if smfBaseUrl == "" {
		smfBaseUrl = "http://localhost:8081"
	}

	handler := amf.NewHandler(smfBaseUrl)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.HealthCheck)
	mux.HandleFunc("POST /trigger", handler.TriggerEstablishment)
	mux.HandleFunc("POST /namf-comm/v1/ue-context/{imsi}/n1-n2-messages", handler.ReceiveN1N2Callback)

	h2s := &http2.Server{}
	server := &http.Server{
		Addr:    ":8080",
		Handler: h2c.NewHandler(mux, h2s),
	}

	go func() {
		logger.Log.Info("AMF HTTP/2 h2c server listening on port 8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log.Fatal("AMF Server failed to start", zap.Error(err))
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Log.Info("Shutting down AMF server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Log.Error("AMF server shutdown failed", zap.Error(err))
	}
	logger.Log.Info("AMF server stopped")
}
