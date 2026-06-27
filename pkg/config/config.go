package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

// Config holds the full configuration for all Network Functions.
type Config struct {
	AMF AMFConfig `yaml:"amf"`
	SMF SMFConfig `yaml:"smf"`
	UDM UDMConfig `yaml:"udm"`
	UPF UPFConfig `yaml:"upf"`
}

type AMFConfig struct {
	// HTTP/2 listen address
	ListenAddr string `yaml:"listenAddr"`
	// Base URL of the SMF to send CreateSMContext to
	SmfBaseUrl string `yaml:"smfBaseUrl"`
}

type SMFConfig struct {
	// HTTP/2 listen address
	ListenAddr string `yaml:"listenAddr"`
	// PostgreSQL connection string. Leave empty or "memory" to use in-memory fallback.
	DatabaseUrl string `yaml:"databaseUrl"`
	// UPF PFCP UDP address
	UpfAddr string `yaml:"upfAddr"`
	// Worker pool size
	MaxWorkers int `yaml:"maxWorkers"`
	// Job queue buffer size
	QueueSize int `yaml:"queueSize"`
	// Path to web dashboard static files
	WebDir string `yaml:"webDir"`
}

type UDMConfig struct {
	// HTTP/2 listen address
	ListenAddr string `yaml:"listenAddr"`
	// PostgreSQL connection string. Leave empty or "memory" to use in-memory fallback.
	DatabaseUrl string `yaml:"databaseUrl"`
}

type UPFConfig struct {
	// UDP listen address for PFCP
	PfcpAddr string `yaml:"pfcpAddr"`
	// HTTP listen address for health check
	HttpAddr string `yaml:"httpAddr"`
}

// DefaultConfig returns a config with sensible default values.
func DefaultConfig() *Config {
	return &Config{
		AMF: AMFConfig{
			ListenAddr: ":8080",
			SmfBaseUrl: "http://localhost:8081",
		},
		SMF: SMFConfig{
			ListenAddr:  ":8081",
			DatabaseUrl: "memory",
			UpfAddr:     "localhost:8805",
			MaxWorkers:  20,
			QueueSize:   5000,
			WebDir:      "web",
		},
		UDM: UDMConfig{
			ListenAddr:  ":8082",
			DatabaseUrl: "memory",
		},
		UPF: UPFConfig{
			PfcpAddr: ":8805",
			HttpAddr: ":8083",
		},
	}
}

// Load loads configuration from a YAML file, then overrides any values
// with environment variables if they are set.
// If the config file does not exist, it falls back to DefaultConfig.
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	// Try to load YAML file
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("[config] File '%s' not found, using defaults.\n", path)
		} else {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	} else {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
		fmt.Printf("[config] Loaded config from '%s'\n", path)
	}

	// Override with environment variables (higher priority than file)
	overrideFromEnv(cfg)

	return cfg, nil
}

// overrideFromEnv reads environment variables and overrides the config values.
func overrideFromEnv(cfg *Config) {
	// AMF
	if v := os.Getenv("AMF_LISTEN_ADDR"); v != "" {
		cfg.AMF.ListenAddr = v
	}
	if v := os.Getenv("SMF_BASE_URL"); v != "" {
		cfg.AMF.SmfBaseUrl = v
	}

	// SMF
	if v := os.Getenv("SMF_LISTEN_ADDR"); v != "" {
		cfg.SMF.ListenAddr = v
	}
	if v := os.Getenv("DATABASE_URL"); v != "" {
		cfg.SMF.DatabaseUrl = v
	}
	if v := os.Getenv("UPF_ADDR"); v != "" {
		cfg.SMF.UpfAddr = v
	}
	if v := os.Getenv("MAX_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.SMF.MaxWorkers = n
		}
	}
	if v := os.Getenv("QUEUE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.SMF.QueueSize = n
		}
	}
	if v := os.Getenv("WEB_DIR"); v != "" {
		cfg.SMF.WebDir = v
	}

	// UDM
	if v := os.Getenv("UDM_LISTEN_ADDR"); v != "" {
		cfg.UDM.ListenAddr = v
	}
	// UDM also reads DATABASE_URL
	if v := os.Getenv("DATABASE_URL"); v != "" {
		cfg.UDM.DatabaseUrl = v
	}

	// UPF
	if v := os.Getenv("UPF_PFCP_ADDR"); v != "" {
		cfg.UPF.PfcpAddr = v
	}
	if v := os.Getenv("UPF_HTTP_ADDR"); v != "" {
		cfg.UPF.HttpAddr = v
	}
}
