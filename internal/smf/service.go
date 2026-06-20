package smf

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"golang.org/x/net/http2"
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

func InitOrchestrator(repo SessionRepository, pfcpClient *PFCPClient, maxWorkers int) {
	// Set up cleartext HTTP/2 client (h2c)
	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
			return net.Dial(network, addr)
		},
	}
	h2Client := &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}

	Orc = &Orchestrator{
		repo:       repo,
		pfcpClient: pfcpClient,
		h2Client:   h2Client,
		jobQueue:   make(chan Job, 5000),
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

// QueueJob pushes a job to the worker pool.
func (o *Orchestrator) QueueJob(job Job) {
	select {
	case o.jobQueue <- job:
	default:
		logger.Log.Warn("SMF: Job queue full, dropping job", zap.String("ref", job.SMContextRef))
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
			err := o.processEstablishment(job.SMContextRef, job.SUPI, job.IPAddress)
			duration := time.Since(start).Microseconds()
			atomic.AddUint64(&o.latencySum, uint64(duration))
			atomic.AddUint64(&o.tpsCounter, 1)

			if err != nil {
				atomic.AddUint64(&o.failCounter, 1)
				logger.Log.Error("SMF: Establishment background task failed",
					zap.String("ref", job.SMContextRef), zap.Error(err))
				o.repo.UpdateSessionStatus(job.SMContextRef, "FAILED")
				// Broadcast session failure to dashboard
				if Hub != nil {
					if sess, getErr := o.repo.GetSession(job.SMContextRef); getErr == nil {
						Hub.BroadcastEvent("session_update", sess)
					}
				}
			} else {
				atomic.AddUint64(&o.successCounter, 1)
				logger.Log.Info("SMF: Establishment background task completed successfully",
					zap.String("ref", job.SMContextRef))
			}
		}
	}
}

func (o *Orchestrator) processEstablishment(ref string, supi string, ip string) error {
	// Step 10a: Send PFCP Session Establishment Request to UPF
	logger.Log.Info("SMF: Starting Step 10a (PFCP Establishment to UPF)", zap.String("ref", ref))
	
	// Track state: CREATING
	o.repo.UpdateSessionStatus(ref, "CREATING")
	if Hub != nil {
		if sess, err := o.repo.GetSession(ref); err == nil {
			Hub.BroadcastEvent("session_update", sess)
		}
	}

	_, err := o.pfcpClient.SendSessionEstablishmentRequest(ip)
	if err != nil {
		return fmt.Errorf("PFCP Establishment Request failed: %w", err)
	}

	// Step 10b: Received Response, update status to ACTIVE
	logger.Log.Info("SMF: Received Step 10b (PFCP Establishment Response OK)", zap.String("ref", ref))
	o.repo.UpdateSessionStatusAndIP(ref, "ACTIVE", ip)
	if Hub != nil {
		if sess, err := o.repo.GetSession(ref); err == nil {
			Hub.BroadcastEvent("session_update", sess)
		}
	}

	// Step 11: SMF -> AMF: N1N2 Message Transfer
	logger.Log.Info("SMF: Starting Step 11 (N1N2 Message Transfer to AMF)", zap.String("ref", ref))
	
	session, err := o.repo.GetSession(ref)
	if err != nil {
		return err
	}

	n1n2 := models.N1N2MessageTransfer{
		PduSessionId: session.PduSessionID,
		SNssai: models.SNssai{
			Sst: session.SST,
			Sd:  session.SD,
		},
		Dnn: session.DNN,
	}

	bodyBytes, err := json.Marshal(n1n2)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://localhost:8080/namf-comm/v1/ue-context/%s/n1-n2-messages", supi)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.h2Client.Do(req)
	if err != nil {
		return fmt.Errorf("N1N2 HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("AMF responded with error: %d %s", resp.StatusCode, resp.Status)
	}

	logger.Log.Info("SMF: N1N2 Callback successful, Step 11 completed", zap.String("ref", ref))
	return nil
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
