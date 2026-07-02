package smf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"smf/pkg/logger"
	"smf/pkg/models"
)

type JobType string

const (
	JobEstablish JobType = "ESTABLISH"
)

type Job struct {
	Type         JobType
	SMContextRef string
	SUPI         string
	IPAddress    string
	// Session data đi kèm để tránh GetSession round-trip
	PduSessionID int
	DNN          string
	SST          int
	SD           string
	GPSI         string
	ServingNfID  string
	AnType       string
}

type Orchestrator struct {
	repo       SessionRepository
	pfcpClient *PFCPClient
	h2Client   *http.Client
	jobQueue   chan Job
	wg         sync.WaitGroup
	stopChan   chan struct{}
	ipCounter  uint32
	// Performance Metrics
	tpsCounter     uint64
	successCounter uint64
	failCounter    uint64
	latencySum     uint64 // in microseconds
}

var Orc *Orchestrator

func InitOrchestrator(repo SessionRepository, pfcpClient *PFCPClient, maxWorkers int, queueSize int) {
	// Set up HTTP/1.1 client instead of HTTP/2 to prevent connection pinning
	tr := &http.Transport{
		MaxIdleConns:        2000,
		MaxIdleConnsPerHost: 2000,
	}
	h2Client := &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}

	Orc = &Orchestrator{
		repo:       repo,
		pfcpClient: pfcpClient,
		h2Client:   h2Client,
		jobQueue:   make(chan Job, queueSize),
		stopChan:   make(chan struct{}),
		ipCounter:  0x0a0b1601, // 10.11.22.1 (ip binary)
	}

	logger.Log.Info("SMF: Initializing Orchestrator with worker pool", zap.Int("workers", maxWorkers))
	for i := 0; i < maxWorkers; i++ {
		Orc.wg.Add(1)
		go Orc.worker(i)
	}

	// Start a background ticker to report TPS and broadcast metrics
	go Orc.metricsReporter()
}

func (o *Orchestrator) Stop() {
	close(o.stopChan)
	close(o.jobQueue)
	o.wg.Wait()
	logger.Log.Info("SMF: Orchestrator stopped")
}

func (o *Orchestrator) AllocateIP() string {
	val := atomic.AddUint32(&o.ipCounter, 1)
	ip := make(net.IP, 4)
	// Put big-endian uint32
	ip[0] = byte(val >> 24)
	ip[1] = byte(val >> 16)
	ip[2] = byte(val >> 8)
	ip[3] = byte(val)
	return ip.String()
}

// EnqueueJob tries to push a job to the worker pool. Returns false if queue is full.
func (o *Orchestrator) EnqueueJob(job Job) bool {
	select {
	case o.jobQueue <- job:
		return true
	default:
		atomic.AddUint64(&o.failCounter, 1)
		return false
	}
}

func (o *Orchestrator) worker(id int) {
	defer o.wg.Done()
	logger.Log.Debug("SMF Worker: Started", zap.Int("id", id))

	for job := range o.jobQueue {
		start := time.Now()
		logger.Log.Debug("SMF Worker: Processing job", zap.Int("id", id), zap.String("ref", job.SMContextRef))

		switch job.Type {
		case JobEstablish:
			err := o.processEstablishment(job.SMContextRef, job.SUPI, job.IPAddress, job)
			duration := time.Since(start).Microseconds()
			atomic.AddUint64(&o.latencySum, uint64(duration))
			atomic.AddUint64(&o.tpsCounter, 1)

			if err != nil {
				atomic.AddUint64(&o.failCounter, 1)
				errMsg := err.Error()
				reason := "E2E_ESTABLISHMENT_FAILED"
				switch {
				case contains(errMsg, "PFCP") || contains(errMsg, "timeout waiting for PFCP"):
					reason = "PFCP_TIMEOUT"
				case contains(errMsg, "N1N2") || contains(errMsg, "AMF responded"):
					reason = "N1N2_FAILED"
				case contains(errMsg, "database") || contains(errMsg, "db") || contains(errMsg, "scan"):
					reason = "DB_ERROR"
				}
				logger.Log.Error("SMF: Establishment background task failed",
					zap.String("ref", job.SMContextRef), zap.String("reason", reason), zap.Error(err))
				o.repo.UpdateSessionStatusWithReason(job.SMContextRef, "FAILED", reason)
				o.NotifyAMFSessionFailure(job.SMContextRef, job.SUPI, reason, err.Error())
				// Bỏ GetSession cho dashboard ở đây — tiết kiệm 1 DB round-trip
			} else {
				atomic.AddUint64(&o.successCounter, 1)
				logger.Log.Info("SMF: Establishment background task completed successfully",
					zap.String("ref", job.SMContextRef))
			}
		}
	}
}

// contains là helper để kiểm tra substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && indexString(s, substr) >= 0))
}

func indexString(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func (o *Orchestrator) processEstablishment(ref string, supi string, ip string, job Job) error {
	// Async Step 1: Save session to DB
	session := &models.PDUSession{
		SMContextRef: ref,
		SUPI:         job.SUPI,
		GPSI:         job.GPSI,
		PduSessionID: job.PduSessionID,
		DNN:          job.DNN,
		SST:          job.SST,
		SD:           job.SD,
		ServingNfID:  job.ServingNfID,
		AnType:       job.AnType,
		Status:       "CREATING",
		IPAddress:    "",
	}

	if err := o.repo.SaveSession(session); err != nil {
		return fmt.Errorf("database error: %w", err)
	}

	// Async Step 2: Query UDM for subscriber profile
	logger.Log.Info("SMF: Querying UDM for subscriber profile (Async Step 2)", zap.String("supi", supi))
	udmUrl := fmt.Sprintf("http://udm:8082/nudm-sdm/v2/%s/sm-data", supi)
	udmReq, err := http.NewRequest("GET", udmUrl, nil)
	if err != nil {
		return fmt.Errorf("UDM request creation failed: %w", err)
	}

	udmResp, err := o.h2Client.Do(udmReq)
	if err != nil {
		return fmt.Errorf("UDM communication failed: %w", err)
	}
	defer udmResp.Body.Close()

	if udmResp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("UDM returned subscriber not found (404)")
	}
	if udmResp.StatusCode != http.StatusOK {
		return fmt.Errorf("UDM returned unexpected status: %d", udmResp.StatusCode)
	}

	var subData models.SubscriptionData
	if err := json.NewDecoder(udmResp.Body).Decode(&subData); err != nil {
		io.Copy(io.Discard, udmResp.Body)
		return fmt.Errorf("UDM response decode failed: %w", err)
	}
	io.Copy(io.Discard, udmResp.Body)

	if subData.Dnn != job.DNN || subData.SNssai.Sst != job.SST || subData.SNssai.Sd != job.SD {
		return fmt.Errorf("UDM validation failed: subscription mismatch")
	}

	logger.Log.Info("SMF: Subscription validated successfully (Async Step 2)", zap.String("ref", ref))

	// Step 10a: Send PFCP Session Establishment Request to UPF
	logger.Log.Info("SMF: Starting Step 10a (PFCP Establishment to UPF)", zap.String("ref", ref))

	_, err = o.pfcpClient.SendSessionEstablishmentRequest(ip)
	if err != nil {
		return fmt.Errorf("PFCP Establishment Request failed: %w", err)
	}

	// Step 10b: Update status ACTIVE
	logger.Log.Info("SMF: Received Step 10b (PFCP Establishment Response OK)", zap.String("ref", ref))
	o.repo.UpdateSessionStatusAndIP(ref, "ACTIVE", ip)

	// Step 11: SMF -> AMF: N1N2 Message Transfer
	// Dùng dữ liệu từ Job thay vì GetSession — tiết kiệm 1 DB round-trip
	logger.Log.Info("SMF: Starting Step 11 (N1N2 Message Transfer to AMF)", zap.String("ref", ref))

	n1n2 := models.N1N2MessageTransfer{
		PduSessionId: job.PduSessionID,
		SNssai: models.SNssai{
			Sst: job.SST,
			Sd:  job.SD,
		},
		Dnn:          job.DNN,
		SMContextRef: ref,
		Status:       "ACTIVE",
		Cause:        "REQUEST_ACCEPTED",
	}

	bodyBytes, err := json.Marshal(n1n2)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://amf:8080/namf-comm/v1/ue-context/%s/n1-n2-messages", supi)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.h2Client.Do(req)
	if err != nil {
		return fmt.Errorf("N1N2 HTTP request failed: %w", err)
	}
	io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("AMF responded with error: %d %s", resp.StatusCode, resp.Status)
	}

	logger.Log.Info("SMF: N1N2 Callback successful, Step 11 completed", zap.String("ref", ref))
	return nil
}

func (o *Orchestrator) NotifyAMFSessionFailure(ref string, supi string, cause string, message string) {
	session, err := o.repo.GetSession(ref)
	if err != nil {
		logger.Log.Warn("SMF: Cannot notify AMF about failed session because session is missing",
			zap.String("ref", ref),
			zap.String("cause", cause),
			zap.Error(err))
		return
	}

	n1n2 := models.N1N2MessageTransfer{
		PduSessionId: session.PduSessionID,
		SNssai: models.SNssai{
			Sst: session.SST,
			Sd:  session.SD,
		},
		Dnn:          session.DNN,
		SMContextRef: ref,
		Status:       "FAILED",
		Cause:        cause,
		Message:      message,
	}

	bodyBytes, err := json.Marshal(n1n2)
	if err != nil {
		logger.Log.Error("SMF: Failed to marshal AMF failure notification",
			zap.String("ref", ref),
			zap.Error(err))
		return
	}

	url := fmt.Sprintf("http://amf:8080/namf-comm/v1/ue-context/%s/n1-n2-messages", supi)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		logger.Log.Error("SMF: Failed to create AMF failure notification",
			zap.String("ref", ref),
			zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.h2Client.Do(req)
	if err != nil {
		logger.Log.Error("SMF: Failed to notify AMF about failed session",
			zap.String("ref", ref),
			zap.Error(err))
		return
	}
	io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Log.Error("SMF: AMF responded with error for failed session notification",
			zap.String("ref", ref),
			zap.Int("status", resp.StatusCode))
		return
	}
}

func (o *Orchestrator) NotifyAMFJobFailure(job Job, cause string, message string) {
	n1n2 := models.N1N2MessageTransfer{
		PduSessionId: job.PduSessionID,
		SNssai: models.SNssai{
			Sst: job.SST,
			Sd:  job.SD,
		},
		Dnn:          job.DNN,
		SMContextRef: job.SMContextRef,
		Status:       "FAILED",
		Cause:        cause,
		Message:      message,
	}

	bodyBytes, err := json.Marshal(n1n2)
	if err != nil {
		logger.Log.Error("SMF: Failed to marshal AMF failure notification",
			zap.String("ref", job.SMContextRef),
			zap.Error(err))
		return
	}

	url := fmt.Sprintf("http://amf:8080/namf-comm/v1/ue-context/%s/n1-n2-messages", job.SUPI)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		logger.Log.Error("SMF: Failed to create AMF failure notification",
			zap.String("ref", job.SMContextRef),
			zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.h2Client.Do(req)
	if err != nil {
		logger.Log.Error("SMF: AMF failure notification failed",
			zap.String("ref", job.SMContextRef),
			zap.String("cause", cause),
			zap.Error(err))
		return
	}
	io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Log.Error("SMF: AMF rejected failure notification",
			zap.String("ref", job.SMContextRef),
			zap.String("cause", cause),
			zap.Int("status", resp.StatusCode))
		return
	}

	logger.Log.Info("SMF: Notified AMF about failed session",
		zap.String("ref", job.SMContextRef),
		zap.String("cause", cause))
}

func (o *Orchestrator) metricsReporter() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-o.stopChan:
			return
		case <-ticker.C:
			tps := atomic.SwapUint64(&o.tpsCounter, 0)
			success := atomic.LoadUint64(&o.successCounter)
			failed := atomic.LoadUint64(&o.failCounter)
			latSum := atomic.SwapUint64(&o.latencySum, 0)

			avgLatencyMs := 0.0
			if tps > 0 {
				avgLatencyMs = float64(latSum) / float64(tps) / 1000.0 // convert micro to milli
			}

			// Broadcast metrics to UI
			if Hub != nil {
				Hub.BroadcastEvent("metrics", map[string]interface{}{
					"tps":          tps,
					"successCount": success,
					"failCount":    failed,
					"avgLatencyMs": avgLatencyMs,
				})
			}
		}
	}
}
