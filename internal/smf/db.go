package smf

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"smf/pkg/logger"
	"smf/pkg/models"
)

type SessionRepository interface {
	SaveSession(session *models.PDUSession) error
	GetSession(ref string) (*models.PDUSession, error)
	UpdateSessionStatus(ref string, status string) error
	UpdateSessionStatusWithReason(ref string, status string, reason string) error
	UpdateSessionStatusAndIP(ref string, status string, ip string) error
	GetAllSessions() ([]*models.PDUSession, error)
	CountByStatus() (active, pending, failed int64)
	CountByFailureReason() map[string]int64
}

// InMemoryRepository represents an in-memory session database.
type InMemoryRepository struct {
	mu       sync.RWMutex
	sessions map[string]*models.PDUSession
}

func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{
		sessions: make(map[string]*models.PDUSession),
	}
}

func (r *InMemoryRepository) SaveSession(session *models.PDUSession) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	now := time.Now()
	session.CreatedAt = now
	session.UpdatedAt = now
	
	// Create a deep copy to prevent mutation outside
	copied := *session
	r.sessions[session.SMContextRef] = &copied
	return nil
}

func (r *InMemoryRepository) GetSession(ref string) (*models.PDUSession, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	session, exists := r.sessions[ref]
	if !exists {
		return nil, errors.New("session not found")
	}
	copied := *session
	return &copied, nil
}

func (r *InMemoryRepository) UpdateSessionStatus(ref string, status string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	session, exists := r.sessions[ref]
	if !exists {
		return errors.New("session not found")
	}
	session.Status = status
	session.UpdatedAt = time.Now()
	return nil
}

func (r *InMemoryRepository) UpdateSessionStatusWithReason(ref string, status string, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	session, exists := r.sessions[ref]
	if !exists {
		return errors.New("session not found")
	}
	session.Status = status
	session.FailureReason = reason
	session.UpdatedAt = time.Now()
	return nil
}

func (r *InMemoryRepository) UpdateSessionStatusAndIP(ref string, status string, ip string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	session, exists := r.sessions[ref]
	if !exists {
		return errors.New("session not found")
	}
	session.Status = status
	session.IPAddress = ip
	session.UpdatedAt = time.Now()
	return nil
}

func (r *InMemoryRepository) GetAllSessions() ([]*models.PDUSession, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]*models.PDUSession, 0, len(r.sessions))
	for _, s := range r.sessions {
		copied := *s
		list = append(list, &copied)
	}
	return list, nil
}

func (r *InMemoryRepository) CountByFailureReason() map[string]int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	counts := make(map[string]int64)
	for _, s := range r.sessions {
		if s.Status == "FAILED" && s.FailureReason != "" {
			counts[s.FailureReason]++
		}
	}
	return counts
}

// PostgresRepository represents a PostgreSQL database repository.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(connStr string) (*PostgresRepository, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection string: %w", err)
	}
	config.MaxConns = 40
	config.MinConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	err = pool.Ping(ctx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	return &PostgresRepository{pool: pool}, nil
}

func (r *PostgresRepository) SaveSession(session *models.PDUSession) error {
	query := `
		INSERT INTO pdu_sessions (sm_context_ref, supi, gpsi, pdu_session_id, dnn, sst, sd, serving_nf_id, an_type, status, ip_address, failure_reason, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	`
	now := time.Now()
	session.CreatedAt = now
	session.UpdatedAt = now

	_, err := r.pool.Exec(context.Background(), query,
		session.SMContextRef, session.SUPI, session.GPSI, session.PduSessionID,
		session.DNN, session.SST, session.SD, session.ServingNfID, session.AnType,
		session.Status, session.IPAddress, session.FailureReason, session.CreatedAt, session.UpdatedAt)

	return err
}

func (r *PostgresRepository) GetSession(ref string) (*models.PDUSession, error) {
	query := `
		SELECT sm_context_ref, supi, gpsi, pdu_session_id, dnn, sst, sd, serving_nf_id, an_type, status, ip_address, failure_reason, created_at, updated_at
		FROM pdu_sessions WHERE sm_context_ref = $1
	`
	row := r.pool.QueryRow(context.Background(), query, ref)

	var s models.PDUSession
	err := row.Scan(&s.SMContextRef, &s.SUPI, &s.GPSI, &s.PduSessionID,
		&s.DNN, &s.SST, &s.SD, &s.ServingNfID, &s.AnType,
		&s.Status, &s.IPAddress, &s.FailureReason, &s.CreatedAt, &s.UpdatedAt)

	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (r *PostgresRepository) UpdateSessionStatus(ref string, status string) error {
	query := `UPDATE pdu_sessions SET status = $1, updated_at = $2 WHERE sm_context_ref = $3`
	_, err := r.pool.Exec(context.Background(), query, status, time.Now(), ref)
	return err
}

func (r *PostgresRepository) UpdateSessionStatusWithReason(ref string, status string, reason string) error {
	query := `UPDATE pdu_sessions SET status = $1, failure_reason = $2, updated_at = $3 WHERE sm_context_ref = $4`
	_, err := r.pool.Exec(context.Background(), query, status, reason, time.Now(), ref)
	return err
}

func (r *PostgresRepository) UpdateSessionStatusAndIP(ref string, status string, ip string) error {
	query := `UPDATE pdu_sessions SET status = $1, ip_address = $2, updated_at = $3 WHERE sm_context_ref = $4`
	_, err := r.pool.Exec(context.Background(), query, status, ip, time.Now(), ref)
	return err
}

func (r *PostgresRepository) GetAllSessions() ([]*models.PDUSession, error) {
	query := `
		SELECT sm_context_ref, supi, gpsi, pdu_session_id, dnn, sst, sd, serving_nf_id, an_type, status, ip_address, failure_reason, created_at, updated_at
		FROM pdu_sessions ORDER BY created_at DESC LIMIT 100
	`
	rows, err := r.pool.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []*models.PDUSession
	for rows.Next() {
		var s models.PDUSession
		err := rows.Scan(&s.SMContextRef, &s.SUPI, &s.GPSI, &s.PduSessionID,
			&s.DNN, &s.SST, &s.SD, &s.ServingNfID, &s.AnType,
			&s.Status, &s.IPAddress, &s.FailureReason, &s.CreatedAt, &s.UpdatedAt)
		if err != nil {
			return nil, err
		}
		list = append(list, &s)
	}
	return list, nil
}

func (r *PostgresRepository) CountByFailureReason() map[string]int64 {
	query := `
		SELECT COALESCE(failure_reason, 'UNKNOWN') as reason, COUNT(*)
		FROM pdu_sessions
		WHERE status = 'FAILED'
		GROUP BY failure_reason
	`
	rows, err := r.pool.Query(context.Background(), query)
	if err != nil {
		return map[string]int64{}
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var reason string
		var count int64
		if err := rows.Scan(&reason, &count); err == nil {
			counts[reason] = count
		}
	}
	return counts
}

// InitRepository attempts to initialize PostgreSQL repo, falling back to In-Memory if connection fails or config says memory.
func InitRepository(connStr string) SessionRepository {
	if connStr == "memory" || connStr == "" {
		logger.Log.Warn("Using IN-MEMORY database repository for SMF")
		return NewInMemoryRepository()
	}

	repo, err := NewPostgresRepository(connStr)
	if err != nil {
		logger.Log.Error("PostgreSQL connection failed. Falling back to IN-MEMORY repository", zap.Error(err))
		return NewInMemoryRepository()
	}

	logger.Log.Info("Successfully connected to PostgreSQL for SMF")
	return repo
}

func (r *InMemoryRepository) CountByStatus() (active, pending, failed int64) {
    r.mu.RLock()
    defer r.mu.RUnlock()
    for _, s := range r.sessions {
        switch s.Status {
        case "ACTIVE":
            active++
        case "PENDING", "CREATING":
            pending++
        case "FAILED":
            failed++
        }
    }
    return
}

func (r *PostgresRepository) CountByStatus() (active, pending, failed int64) {
	query := `
		SELECT status, COUNT(*) 
		FROM pdu_sessions 
		WHERE status IN ('ACTIVE', 'PENDING', 'CREATING', 'FAILED') 
		GROUP BY status
	`
	rows, err := r.pool.Query(context.Background(), query)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err == nil {
			switch status {
			case "ACTIVE":
				active += count
			case "PENDING", "CREATING":
				pending += count
			case "FAILED":
				failed += count
			}
		}
	}
	return
}
