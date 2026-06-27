package smf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"

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

	// Generate internal smContextRef (không cần UUID, chỉ dùng nội bộ)
	ref := fmt.Sprintf("smctx-%d", atomic.AddUint64(&smContextCounter, 1))

	// Save session in DB as PENDING
	session := &models.PDUSession{
		SMContextRef: ref,
		SUPI:         req.Supi,
		GPSI:         req.Gpsi,
		PduSessionID: req.PduSessionId,
		DNN:          req.Dnn,
		SST:          req.SNssai.Sst,
		SD:           req.SNssai.Sd,
		ServingNfID:  req.ServingNfId,
		AnType:       req.AnType,
		Status:       "PENDING",
		IPAddress:    "",
	}

	if err := h.repo.SaveSession(session); err != nil {
		logger.Log.Error("SMF: Failed to save session to DB", zap.Error(err))
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	// Broadcast initial PENDING state to dashboard
	if Hub != nil {
		Hub.BroadcastEvent("session_update", session)
	}

	// Step 4: SMF -> UDM: Subscription retrieval
	logger.Log.Info("SMF: Querying UDM for subscriber profile (Step 4)", zap.String("supi", req.Supi))

	udmUrl := fmt.Sprintf("http://localhost:8082/nudm-sdm/v2/%s/sm-data", req.Supi)
	udmReq, err := http.NewRequest("GET", udmUrl, nil)
	if err != nil {
		logger.Log.Error("SMF: Failed to create UDM request", zap.Error(err))
		Orc.NotifyAMFSessionFailure(ref, req.Supi, "INTERNAL_ERROR", err.Error())
		h.failSession(w, ref, "INTERNAL_ERROR", http.StatusInternalServerError)
		return
	}

	udmResp, err := Orc.h2Client.Do(udmReq)
	if err != nil {
		logger.Log.Error("SMF: Failed to communicate with UDM", zap.Error(err))
		Orc.NotifyAMFSessionFailure(ref, req.Supi, "UDM_UNAVAILABLE", err.Error())
		h.failSession(w, ref, "UDM_UNAVAILABLE", http.StatusServiceUnavailable)
		return
	}
	defer udmResp.Body.Close()

	if udmResp.StatusCode == http.StatusNotFound {
		logger.Log.Warn("SMF: UDM returned subscriber not found (404)")
		Orc.NotifyAMFSessionFailure(ref, req.Supi, "SUBSCRIPTION_NOT_FOUND", "UDM returned subscriber not found")
		h.failSession(w, ref, "SUBSCRIPTION_NOT_FOUND", http.StatusNotFound)
		return
	}

	if udmResp.StatusCode != http.StatusOK {
		logger.Log.Error("SMF: UDM returned unexpected status", zap.Int("status", udmResp.StatusCode))
		Orc.NotifyAMFSessionFailure(ref, req.Supi, "UDM_ERROR", fmt.Sprintf("UDM returned HTTP %d", udmResp.StatusCode))
		h.failSession(w, ref, "UDM_ERROR", http.StatusInternalServerError)
		return
	}

	var subData models.SubscriptionData
	if err := json.NewDecoder(udmResp.Body).Decode(&subData); err != nil {
		logger.Log.Error("SMF: Failed to decode UDM response", zap.Error(err))
		Orc.NotifyAMFSessionFailure(ref, req.Supi, "INTERNAL_ERROR", err.Error())
		h.failSession(w, ref, "INTERNAL_ERROR", http.StatusInternalServerError)
		return
	}

	// Validate dnn + sNssai
	if subData.Dnn != req.Dnn || subData.SNssai.Sst != req.SNssai.Sst || subData.SNssai.Sd != req.SNssai.Sd {
		logger.Log.Warn("SMF: Subscription data mismatch",
			zap.String("reqDnn", req.Dnn), zap.String("subDnn", subData.Dnn),
			zap.Int("reqSst", req.SNssai.Sst), zap.Int("subSst", subData.SNssai.Sst),
			zap.String("reqSd", req.SNssai.Sd), zap.String("subSd", subData.SNssai.Sd))
		Orc.NotifyAMFSessionFailure(ref, req.Supi, "SUBSCRIPTION_MISMATCH", "UDM subscription data does not match requested DNN/S-NSSAI")
		h.failSession(w, ref, "SUBSCRIPTION_MISMATCH", http.StatusBadRequest)
		return
	}

	logger.Log.Info("SMF: Subscription validated successfully. Replying to AMF (Step 5)", zap.String("ref", ref))

	// Step 5: Respond 201 Created to AMF
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(models.CreateSMContextResponse{
		SmContextRef: ref,
		Cause:        "REQUEST_ACCEPTED",
	})

	// Allocate IP address
	allocatedIP := Orc.AllocateIP()

	// Asynchronously kick off the rest of the flow via worker pool
	Orc.QueueJob(Job{
		Type:         JobEstablish,
		SMContextRef: ref,
		SUPI:         req.Supi,
		IPAddress:    allocatedIP,
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
	h.repo.UpdateSessionStatus(ref, "FAILED")
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
	amfReq, err := http.NewRequest("POST", "http://localhost:8080/trigger", bytes.NewBuffer(bodyBytes))
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
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"active":  active,
		"pending": pending,
		"failed":  failed,
	})
}
