package upf

import (
	"net"
	"net/http"
	"sync"

	"go.uber.org/zap"
	"smf/pkg/logger"
	"smf/pkg/models"
)

type UPFSession struct {
	SeqNum  uint32
	PdnType uint8
	IP      net.IP
}

type Server struct {
	udpAddr  *net.UDPAddr
	udpConn  *net.UDPConn
	sessions map[uint32]*UPFSession
	mu       sync.RWMutex
	stopChan chan struct{}
}

func NewServer(udpAddrStr string) (*Server, error) {
	addr, err := net.ResolveUDPAddr("udp", udpAddrStr)
	if err != nil {
		return nil, err
	}
	return &Server{
		udpAddr:  addr,
		sessions: make(map[uint32]*UPFSession),
		stopChan: make(chan struct{}),
	}, nil
}

func (s *Server) Start() error {
	conn, err := net.ListenUDP("udp", s.udpAddr)
	if err != nil {
		return err
	}
	s.udpConn = conn

	logger.Log.Info("UPF: UDP Server listening on", zap.String("addr", s.udpAddr.String()))

	go s.listenLoop()

	return nil
}

func (s *Server) Stop() {
	close(s.stopChan)
	if s.udpConn != nil {
		s.udpConn.Close()
	}
	logger.Log.Info("UPF: UDP Server stopped")
}

func (s *Server) listenLoop() {
	buf := make([]byte, 1024)
	for {
		select {
		case <-s.stopChan:
			return
		default:
			n, remoteAddr, err := s.udpConn.ReadFromUDP(buf)
			if err != nil {
				select {
				case <-s.stopChan:
					return
				default:
					logger.Log.Error("UPF: Failed to read from UDP socket", zap.Error(err))
					continue
				}
			}

			data := make([]byte, n)
			copy(data, buf[:n])
			go s.handlePacket(remoteAddr, data)
		}
	}
}

func (s *Server) handlePacket(remoteAddr *net.UDPAddr, data []byte) {
	if len(data) < 2 {
		logger.Log.Warn("UPF: Received packet too small")
		return
	}

	msgType := data[1]
	switch msgType {
	case 50: // PFCP Session Establishment Request
		req, err := models.DecodeRequest(data)
		if err != nil {
			logger.Log.Error("UPF: Failed to decode Session Establishment Request", zap.Error(err))
			return
		}

		logger.Log.Info("UPF: Received PFCP Session Establishment Request",
			zap.Uint32("seqNum", req.Header.SequenceNumber),
			zap.String("ipAddress", req.IP.String()))

		// Save session internally
		s.mu.Lock()
		s.sessions[req.Header.SequenceNumber] = &UPFSession{
			SeqNum:  req.Header.SequenceNumber,
			PdnType: req.PdnType,
			IP:      req.IP,
		}
		s.mu.Unlock()

		// Prepare response (Cause 1 = Success)
		resData, err := models.EncodeResponse(51, req.Header.SequenceNumber, 1)
		if err != nil {
			logger.Log.Error("UPF: Failed to encode Session Establishment Response", zap.Error(err))
			return
		}

		_, err = s.udpConn.WriteToUDP(resData, remoteAddr)
		if err != nil {
			logger.Log.Error("UPF: Failed to send UDP response", zap.Error(err))
		} else {
			logger.Log.Info("UPF: Sent PFCP Session Establishment Response", zap.Uint32("seqNum", req.Header.SequenceNumber))
		}

	case 52: // PFCP Session Modification Request
		req, err := models.DecodeRequest(data)
		if err != nil {
			logger.Log.Error("UPF: Failed to decode Session Modification Request", zap.Error(err))
			return
		}

		logger.Log.Info("UPF: Received PFCP Session Modification Request",
			zap.Uint32("seqNum", req.Header.SequenceNumber),
			zap.String("ipAddress", req.IP.String()))

		// Update session internally
		s.mu.Lock()
		if sess, exists := s.sessions[req.Header.SequenceNumber]; exists {
			sess.IP = req.IP
			sess.PdnType = req.PdnType
		} else {
			// If not found (e.g. simulated reboot or out of order), create one
			s.sessions[req.Header.SequenceNumber] = &UPFSession{
				SeqNum:  req.Header.SequenceNumber,
				PdnType: req.PdnType,
				IP:      req.IP,
			}
		}
		s.mu.Unlock()

		// Prepare response (Cause 1 = Success)
		resData, err := models.EncodeResponse(53, req.Header.SequenceNumber, 1)
		if err != nil {
			logger.Log.Error("UPF: Failed to encode Session Modification Response", zap.Error(err))
			return
		}

		_, err = s.udpConn.WriteToUDP(resData, remoteAddr)
		if err != nil {
			logger.Log.Error("UPF: Failed to send UDP response", zap.Error(err))
		} else {
			logger.Log.Info("UPF: Sent PFCP Session Modification Response", zap.Uint32("seqNum", req.Header.SequenceNumber))
		}

	default:
		logger.Log.Warn("UPF: Received unknown message type", zap.Uint8("type", msgType))
	}
}

// HealthCheck serves HTTP health status.
func HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
