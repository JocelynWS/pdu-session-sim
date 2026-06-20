package udm

import (
	"encoding/json"
	"net/http"
	"go.uber.org/zap"
	"smf/pkg/logger"
)

type Handler struct {
	repo SubscriberRepository
}

func NewHandler(repo SubscriberRepository) *Handler {
	return &Handler{repo: repo}
}

func (h *Handler) GetSubscriptionData(w http.ResponseWriter, r *http.Request) {
	imsi := r.PathValue("imsi")
	if imsi == "" {
		logger.Log.Warn("UDM: GET subscription request received with empty IMSI")
		http.Error(w, "IMSI is required", http.StatusBadRequest)
		return
	}

	logger.Log.Info("UDM: Received GET subscription request", zap.String("imsi", imsi))

	sub, err := h.repo.GetSubscriber(imsi)
	if err != nil {
		logger.Log.Warn("UDM: Subscriber not found", zap.String("imsi", imsi))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "subscriber not found"})
		return
	}

	logger.Log.Info("UDM: Subscriber found, returning profile data", zap.String("imsi", imsi), zap.String("dnn", sub.Dnn))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sub)
}

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
