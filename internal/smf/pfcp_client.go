package smf

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"smf/pkg/logger"
	"smf/pkg/models"
)

type PFCPClient struct {
	upfAddr    *net.UDPAddr
	conn       *net.UDPConn
	seqCounter uint32
	pending    map[uint32]chan *models.PFCPSessionResponse
	mu         sync.RWMutex
	stopChan   chan struct{}
}

func NewPFCPClient(upfAddrStr string) (*PFCPClient, error) {
	upfAddr, err := net.ResolveUDPAddr("udp", upfAddrStr)
	if err != nil {
		return nil, err
	}

	// Bind to an ephemeral local port
	localAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		return nil, err
	}

	client := &PFCPClient{
		upfAddr:  upfAddr,
		conn:     conn,
		pending:  make(map[uint32]chan *models.PFCPSessionResponse),
		stopChan: make(chan struct{}),
	}

	logger.Log.Info("SMF PFCP Client: Bound to local UDP port", zap.String("localAddr", conn.LocalAddr().String()))

	go client.receiveLoop()

	return client, nil
}

func (c *PFCPClient) Close() {
	close(c.stopChan)
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *PFCPClient) receiveLoop() {
	buf := make([]byte, 1024)
	for {
		select {
		case <-c.stopChan:
			return
		default:
			n, _, err := c.conn.ReadFrom(buf)
			if err != nil {
				select {
				case <-c.stopChan:
					return
				default:
					logger.Log.Error("SMF PFCP Client: Failed to read from UDP socket", zap.Error(err))
					continue
				}
			}

			data := make([]byte, n)
			copy(data, buf[:n])
			go c.handleResponse(data)
		}
	}
}

func (c *PFCPClient) handleResponse(data []byte) {
	res, err := models.DecodeResponse(data)
	if err != nil {
		logger.Log.Error("SMF PFCP Client: Failed to decode incoming response", zap.Error(err))
		return
	}

	c.mu.RLock()
	ch, exists := c.pending[res.Header.SequenceNumber]
	c.mu.RUnlock()

	if exists {
		select {
		case ch <- res:
		default:
			logger.Log.Warn("SMF PFCP Client: Dropped response due to blocked receiver channel",
				zap.Uint32("seqNum", res.Header.SequenceNumber))
		}
	} else {
		logger.Log.Warn("SMF PFCP Client: Received response for unknown/expired sequence number",
			zap.Uint32("seqNum", res.Header.SequenceNumber))
	}
}

func (c *PFCPClient) SendSessionEstablishmentRequest(ipAddress string) (*models.PFCPSessionResponse, error) {
	seq := atomic.AddUint32(&c.seqCounter, 1)
	data, err := models.EncodeRequest(50, seq, ipAddress)
	if err != nil {
		return nil, err
	}

	resChan := make(chan *models.PFCPSessionResponse, 1)
	c.mu.Lock()
	c.pending[seq] = resChan
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, seq)
		c.mu.Unlock()
	}()

	logger.Log.Info("SMF PFCP Client: Sending Session Establishment Request",
		zap.Uint32("seqNum", seq),
		zap.String("targetIP", ipAddress))

	_, err = c.conn.WriteTo(data, c.upfAddr)
	if err != nil {
		return nil, err
	}

	select {
	case res := <-resChan:
		if res.Cause != 1 {
			return nil, errors.New("PFCP session establishment rejected by UPF")
		}
		return res, nil
	case <-time.After(2 * time.Second):
		return nil, errors.New("timeout waiting for PFCP session establishment response")
	}
}

func (c *PFCPClient) SendSessionModificationRequest(ipAddress string, seqNum uint32) (*models.PFCPSessionResponse, error) {
	data, err := models.EncodeRequest(52, seqNum, ipAddress)
	if err != nil {
		return nil, err
	}

	resChan := make(chan *models.PFCPSessionResponse, 1)
	c.mu.Lock()
	c.pending[seqNum] = resChan
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, seqNum)
		c.mu.Unlock()
	}()

	logger.Log.Info("SMF PFCP Client: Sending Session Modification Request",
		zap.Uint32("seqNum", seqNum),
		zap.String("targetIP", ipAddress))

	_, err = c.conn.WriteTo(data, c.upfAddr)
	if err != nil {
		return nil, err
	}

	select {
	case res := <-resChan:
		if res.Cause != 1 {
			return nil, errors.New("PFCP session modification rejected by UPF")
		}
		return res, nil
	case <-time.After(2 * time.Second):
		return nil, errors.New("timeout waiting for PFCP session modification response")
	}
}
