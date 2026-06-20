package smf

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

type DashboardHub struct {
	clients    map[chan []byte]bool
	register   chan chan []byte
	unregister chan chan []byte
	broadcast  chan []byte
	mu         sync.RWMutex
}

var Hub *DashboardHub

func InitDashboardHub() {
	Hub = &DashboardHub{
		clients:    make(map[chan []byte]bool),
		register:   make(chan chan []byte),
		unregister: make(chan chan []byte),
		broadcast:  make(chan []byte, 200),
	}
	go Hub.start()
}

func (h *DashboardHub) start() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client)
			}
			h.mu.Unlock()
		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client <- message:
				default:
					// Avoid blocking if client channel is full
				}
			}
			h.mu.RUnlock()
		}
	}
}

// BroadcastEvent formats and broadcasts an SSE event.
func (h *DashboardHub) BroadcastEvent(eventType string, data interface{}) {
	payload, err := json.Marshal(data)
	if err != nil {
		return
	}
	// SSE formatted payload
	sseData := fmt.Sprintf("event: %s\ndata: %s\n\n", eventType, string(payload))
	select {
	case h.broadcast <- []byte(sseData):
	default:
		// Queue full, drop event
	}
}

// ServeHTTP implements http.Handler.
func (h *DashboardHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	clientChan := make(chan []byte, 20)
	h.register <- clientChan

	defer func() {
		h.unregister <- clientChan
	}()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Send initial test event to confirm connection
	w.Write([]byte("event: ping\ndata: connected\n\n"))
	flusher.Flush()

	notify := r.Context().Done()
	for {
		select {
		case msg, open := <-clientChan:
			if !open {
				return
			}
			w.Write(msg)
			flusher.Flush()
		case <-notify:
			return
		}
	}
}
