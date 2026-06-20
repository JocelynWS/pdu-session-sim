package udm

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

type SubscriberRepository interface {
	GetSubscriber(imsi string) (*models.SubscriptionData, error)
}

// InMemorySubscriberRepository pre-seeds subscribers for testing.
type InMemorySubscriberRepository struct {
	mu          sync.RWMutex
	subscribers map[string]*models.SubscriptionData
}

func NewInMemorySubscriberRepository() *InMemorySubscriberRepository {
	repo := &InMemorySubscriberRepository{
		subscribers: make(map[string]*models.SubscriptionData),
	}
	// Seed with test data from migrations/001_init.sql
	repo.subscribers["imsi-452040000000001"] = &models.SubscriptionData{
		Imsi:   "imsi-452040000000001",
		Dnn:    "v-internet",
		SNssai: models.SNssai{Sst: 1, Sd: "000001"},
	}
	repo.subscribers["imsi-452040000000002"] = &models.SubscriptionData{
		Imsi:   "imsi-452040000000002",
		Dnn:    "v-internet",
		SNssai: models.SNssai{Sst: 1, Sd: "000002"},
	}
	repo.subscribers["imsi-452040000000003"] = &models.SubscriptionData{
		Imsi:   "imsi-452040000000003",
		Dnn:    "v-internet",
		SNssai: models.SNssai{Sst: 2, Sd: "000003"},
	}
	return repo
}

func (r *InMemorySubscriberRepository) GetSubscriber(imsi string) (*models.SubscriptionData, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	sub, exists := r.subscribers[imsi]
	if !exists {
		return nil, errors.New("subscriber not found")
	}
	copied := *sub
	return &copied, nil
}

// PostgresSubscriberRepository queries PostgreSQL subscribers table.
type PostgresSubscriberRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresSubscriberRepository(connStr string) (*PostgresSubscriberRepository, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection string: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	err = pool.Ping(ctx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	return &PostgresSubscriberRepository{pool: pool}, nil
}

func (r *PostgresSubscriberRepository) GetSubscriber(imsi string) (*models.SubscriptionData, error) {
	query := "SELECT imsi, dnn, sst, sd FROM subscribers WHERE imsi = $1"
	row := r.pool.QueryRow(context.Background(), query, imsi)

	var sub models.SubscriptionData
	err := row.Scan(&sub.Imsi, &sub.Dnn, &sub.SNssai.Sst, &sub.SNssai.Sd)
	if err != nil {
		return nil, err
	}
	return &sub, nil
}

// InitSubscriberRepository initializes either Postgres or InMemory repository.
func InitSubscriberRepository(connStr string) SubscriberRepository {
	if connStr == "memory" || connStr == "" {
		logger.Log.Warn("Using IN-MEMORY subscriber repository for UDM")
		return NewInMemorySubscriberRepository()
	}

	repo, err := NewPostgresSubscriberRepository(connStr)
	if err != nil {
		logger.Log.Error("PostgreSQL connection failed. Falling back to IN-MEMORY subscriber repository for UDM", zap.Error(err))
		return NewInMemorySubscriberRepository()
	}

	logger.Log.Info("Successfully connected to PostgreSQL for UDM")
	return repo
}
