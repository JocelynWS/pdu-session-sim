package amf

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"smf/pkg/logger"
	"smf/pkg/models"
)

type Handler struct {
	// Maps IMSI (supi) to smContextRef
	sessions   map[string]string
	mu         sync.RWMutex
	h2Client   *http.Client
	smfBaseUrl string
}

func NewHandler(smfBaseUrl string) *Handler {
	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
			return net.Dial(network, addr)
		},
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}

	return &Handler{
		sessions:   make(map[string]string),
		h2Client:   client,
		smfBaseUrl: smfBaseUrl,
	}
}

// TriggerEstablishment initiates PDU Session Establishment (Step 3) from AMF to SMF.
func (h *Handler) TriggerEstablishment(w http.ResponseWriter, r *http.Request) {
	// Accept custom request parameter or use default seed data
	var triggerReq struct {
		Supi         string `json:"supi"`
		Gpsi         string `json:"gpsi"`
		PduSessionId int    `json:"pduSessionId"`
		Dnn          string `json:"dnn"`
		Sst          int    `json:"sst"`
		Sd           string `json:"sd"`
	}

	// Default values
	triggerReq.Supi = "imsi-452040000000001"
	triggerReq.Gpsi = "msisdn-84900000001"
	triggerReq.PduSessionId = 1
	triggerReq.Dnn = "v-internet"
	triggerReq.Sst = 1
	triggerReq.Sd = "000001"

	// Parse body if present
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&triggerReq); err != nil {
			logger.Log.Error("AMF: Failed to decode trigger body", zap.Error(err))
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
	}

	logger.Log.Info("AMF: Triggering PDU Session Establishment (Step 3)",
		zap.String("supi", triggerReq.Supi),
		zap.Int("pduSessionId", triggerReq.PduSessionId))

	createReq := models.CreateSMContextRequest{
		Supi:         triggerReq.Supi,
		Gpsi:         triggerReq.Gpsi,
		PduSessionId: triggerReq.PduSessionId,
		Dnn:          triggerReq.Dnn,
		SNssai: models.SNssai{
			Sst: triggerReq.Sst,
			Sd:  triggerReq.Sd,
		},
		ServingNfId: "2ab2b5a9-68e8-4ee6-b939-024c109b520c",
		AnType:      "3GPP_ACCESS",
	}

	bodyBytes, err := json.Marshal(createReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	url := fmt.Sprintf("%s/nsmf-pdusession/v1/sm-contexts", h.smfBaseUrl)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	// Call SMF
	resp, err := h.h2Client.Do(req)
	if err != nil {
		logger.Log.Error("AMF: Failed to send CreateSMContext request to SMF", zap.Error(err))
		http.Error(w, fmt.Sprintf("SMF communication failed: %v", err), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		var errRes models.CreateSMContextResponse
		json.NewDecoder(resp.Body).Decode(&errRes)
		logger.Log.Warn("AMF: SMF rejected CreateSMContext Request",
			zap.Int("status", resp.StatusCode),
			zap.String("cause", errRes.Cause))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		json.NewEncoder(w).Encode(errRes)
		return
	}

	var createRes models.CreateSMContextResponse
	if err := json.NewDecoder(resp.Body).Decode(&createRes); err != nil {
		http.Error(w, "failed to parse SMF response", http.StatusInternalServerError)
		return
	}

	logger.Log.Info("AMF: Received CreateSMContext Response (Step 5)",
		zap.String("smContextRef", createRes.SmContextRef),
		zap.String("cause", createRes.Cause))

	// Save smContextRef mapping to IMSI
	h.mu.Lock()
	h.sessions[triggerReq.Supi] = createRes.SmContextRef
	h.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(createRes)
}

// ReceiveN1N2Callback handles N1N2 Message Transfer (Step 11).
func (h *Handler) ReceiveN1N2Callback(w http.ResponseWriter, r *http.Request) {
	imsi := r.PathValue("imsi")
	if imsi == "" {
		http.Error(w, "IMSI parameter is required", http.StatusBadRequest)
		return
	}

	var n1n2 models.N1N2MessageTransfer
	if err := json.NewDecoder(r.Body).Decode(&n1n2); err != nil {
		logger.Log.Error("AMF: Failed to decode N1N2 callback request", zap.Error(err))
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	logger.Log.Info("AMF: Received N1N2 Message Transfer Callback (Step 11)",
		zap.String("imsi", imsi),
		zap.Int("pduSessionId", n1n2.PduSessionId))

	h.mu.RLock()
	ref, exists := h.sessions[imsi]
	h.mu.RUnlock()

	if !exists {
		logger.Log.Warn("AMF: Received N1N2 callback for unknown IMSI session", zap.String("imsi", imsi))
		http.Error(w, "session context not found for IMSI", http.StatusNotFound)
		return
	}

	// 1. Respond 200 OK back to SMF first
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))

	// 2. Asynchronously simulate Access Network (AN) setup and trigger Step 15
	go func() {
		// Simulate small delay for AN radio setup (e.g. 50ms)
		time.Sleep(50 * time.Millisecond)
		h.triggerUpdateSMContext(ref, imsi)
	}()
}

func (h *Handler) triggerUpdateSMContext(ref string, imsi string) {
	logger.Log.Info("AMF: Triggering UpdateSMContext Request (Step 15)",
		zap.String("smContextRef", ref),
		zap.String("imsi", imsi))

	modifyReq := models.UpdateSMContextRequest{
		AnType:             "3GPP_ACCESS",
		AnTypeToReactivate: "3GPP_ACCESS",
		UpCnxState:         "ACTIVATED",
	}

	bodyBytes, err := json.Marshal(modifyReq)
	if err != nil {
		logger.Log.Error("AMF: Failed to marshal UpdateSMContext request", zap.Error(err))
		return
	}

	url := fmt.Sprintf("%s/nsmf-pdusession/v1/sm-contexts/%s/modify", h.smfBaseUrl, ref)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		logger.Log.Error("AMF: Failed to create UpdateSMContext HTTP request", zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.h2Client.Do(req)
	if err != nil {
		logger.Log.Error("AMF: UpdateSMContext request failed", zap.Error(err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Log.Error("AMF: SMF responded to UpdateSMContext with error status",
			zap.Int("status", resp.StatusCode))
		return
	}

	var modifyRes models.UpdateSMContextResponse
	if err := json.NewDecoder(resp.Body).Decode(&modifyRes); err != nil {
		logger.Log.Error("AMF: Failed to decode UpdateSMContext response", zap.Error(err))
		return
	}

	logger.Log.Info("AMF: PDU Session established successfully! (Step 17/End)",
		zap.String("smContextRef", ref),
		zap.String("cause", modifyRes.Cause),
		zap.String("upCnxState", modifyRes.UpCnxState))
}

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
