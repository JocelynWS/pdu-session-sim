package smf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"smf/pkg/logger"
	"smf/pkg/models"
)

// smContextCounter dùng để sinh smContextRef nội bộ (không cần UUID)
var smContextCounter uint64

type Handler struct {
	repo SessionRepository
}

func NewHandler(repo SessionRepository) *Handler {
	return &Handler{repo: repo}
}

func (h *Handler) CreateSMContext(w http.ResponseWriter, r *http.Request) {
	var req models.CreateSMContextRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		logger.Log.Error("SMF: Failed to decode CreateSMContext request", zap.Error(err))
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	logger.Log.Info("SMF: Received CreateSMContext Request (Step 3)",
		zap.String("supi", req.Supi),
		zap.String("dnn", req.Dnn),
		zap.Int("pduSessionId", req.PduSessionId))

	// Generate internal smContextRef using timestamp to avoid PK collisions across pod restarts
	ref := fmt.Sprintf("smctx-%d-%d", time.Now().UnixNano(), atomic.AddUint64(&smContextCounter, 1))

	// Allocate IP address
	allocatedIP := Orc.AllocateIP()

	// Asynchronously kick off the rest of the flow via worker pool
	enqueued := Orc.EnqueueJob(Job{
		Type:         JobEstablish,
		SMContextRef: ref,
		SUPI:         req.Supi,
		IPAddress:    allocatedIP,
		PduSessionID: req.PduSessionId,
		DNN:          req.Dnn,
		SST:          req.SNssai.Sst,
		SD:           req.SNssai.Sd,
		GPSI:         req.Gpsi,
		ServingNfID:  req.ServingNfId,
		AnType:       req.AnType,
	})

	w.Header().Set("Content-Type", "application/json")
	if !enqueued {
		// 3GPP Standard Load Shedding: Trả về HTTP 503/403 (Congestion) ngay lập tức
		logger.Log.Warn("SMF: Job queue full, rejecting request immediately", zap.String("ref", ref))
		w.WriteHeader(http.StatusServiceUnavailable) // HTTP 503
		json.NewEncoder(w).Encode(models.CreateSMContextResponse{
			Cause: "SMF_CONGESTION",
		})
		return
	}

	// Step 5: Respond 201 Created to AMF immediately!
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(models.CreateSMContextResponse{
		SmContextRef: ref,
		Cause:        "REQUEST_ACCEPTED",
	})
}

func (h *Handler) UpdateSMContext(w http.ResponseWriter, r *http.Request) {
	ref := r.PathValue("smContextRef")
	if ref == "" {
		http.Error(w, "smContextRef is required", http.StatusBadRequest)
		return
	}

	var req models.UpdateSMContextRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Log.Error("SMF: Failed to decode UpdateSMContext request", zap.Error(err))
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Bước 6+ (Update SM Context) chưa cần implement - chỉ ack lại AMF
	logger.Log.Info("SMF: Received UpdateSMContext (stub - not implemented beyond Step 5)",
		zap.String("ref", ref),
		zap.String("upCnxState", req.UpCnxState))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(models.UpdateSMContextResponse{
		UpCnxState: req.UpCnxState,
		Cause:      "REQUEST_ACCEPTED",
	})
}

func (h *Handler) GetSessions(w http.ResponseWriter, r *http.Request) {
	list, err := h.repo.GetAllSessions()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

func (h *Handler) failSession(w http.ResponseWriter, ref string, cause string, httpCode int) {
	h.repo.UpdateSessionStatusWithReason(ref, "FAILED", cause)
	if Hub != nil {
		if sess, err := h.repo.GetSession(ref); err == nil {
			Hub.BroadcastEvent("session_update", sess)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpCode)
	json.NewEncoder(w).Encode(models.CreateSMContextResponse{
		SmContextRef: ref,
		Cause:        cause,
	})
}

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (h *Handler) TriggerProxy(w http.ResponseWriter, r *http.Request) {
	logger.Log.Info("SMF: Proxying trigger request to AMF")

	// Read payload
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Log.Error("SMF Proxy: Failed to read body", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Create request to AMF
	amfReq, err := http.NewRequest("POST", "http://amf:8080/trigger", bytes.NewBuffer(bodyBytes))
	if err != nil {
		logger.Log.Error("SMF Proxy: Failed to create request to AMF", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	amfReq.Header.Set("Content-Type", "application/json")

	// Send to AMF
	resp, err := Orc.h2Client.Do(amfReq)
	if err != nil {
		logger.Log.Error("SMF Proxy: AMF unreachable", zap.Error(err))
		http.Error(w, fmt.Sprintf("AMF is unreachable: %v", err), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	// Return response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (h *Handler) ServeDashboard(w http.ResponseWriter, r *http.Request) {
	// Simple index handler will be served by file server, but fallback is here
	http.ServeFile(w, r, "web/index.html")
}

func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	active, pending, failed := h.repo.CountByStatus()
	failureBreakdown := h.repo.CountByFailureReason()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"active":           active,
		"pending":          pending,
		"failed":           failed,
		"failureBreakdown": failureBreakdown,
	})
}
