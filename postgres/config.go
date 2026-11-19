package postgres

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// DefaultEnvPrefix is the standard OpenWiFi prefix for primary database configuration.
const DefaultEnvPrefix = "STORAGE_TYPE_POSTGRESQL_"

// Config encapsulates configuration parameters for establishing and tuning
// the PostgreSQL connection pool and migration harness.
// Field tags use relative names so consumers can bind arbitrary prefixes
// (e.g., envPrefix:"STORAGE_TYPE_POSTGRESQL_" or envPrefix:"REPLICA_POSTGRESQL_").
type Config struct {
	StorageType        string        `env:"STORAGE_TYPE" envDefault:"postgresql"`
	Host               string        `env:"HOST" envDefault:"localhost"`
	Port               int           `env:"PORT" envDefault:"5432"`
	Username           string        `env:"USERNAME" envDefault:"postgres"`
	Password           string        `env:"PASSWORD" envDefault:"postgres"`
	Database           string        `env:"DATABASE" envDefault:"postgres"`
	SSLMode            string        `env:"SSLMODE" envDefault:"disable"`
	MaxConns           int32         `env:"MAX_CONNS" envDefault:"25"`
	MinConns           int32         `env:"MIN_CONNS" envDefault:"2"`
	MaxConnLifetime    time.Duration `env:"MAX_CONN_LIFETIME" envDefault:"30m"`
	MaxConnIdleTime    time.Duration `env:"MAX_CONN_IDLE_TIME" envDefault:"10m"`
	HealthCheckTimeout time.Duration `env:"HEALTH_CHECK_TIMEOUT" envDefault:"5s"`
	AutoMigrate        bool          `env:"AUTO_MIGRATE" envDefault:"true"`
	MigrationsDir      string        `env:"MIGRATIONS_DIR" envDefault:"db/migrations"`
}

// BuildDSN constructs a properly URL-escaped PostgreSQL connection string.
func BuildDSN(cfg Config) string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.Username, cfg.Password),
		Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Path:   "/" + strings.TrimPrefix(cfg.Database, "/"),
	}
	if cfg.SSLMode != "" {
		q := u.Query()
		q.Set("sslmode", cfg.SSLMode)
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// LoadConfig parses environment variables into a Config struct using the provided prefix.
// If STORAGE_TYPE is set at root level and the prefixed STORAGE_TYPE is empty, it falls back
// to the root STORAGE_TYPE for seamless backward compatibility.
func LoadConfig(prefix string) (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix}); err != nil {
		return Config{}, fmt.Errorf("failed to parse postgres config with prefix %q: %w", prefix, err)
	}

	// Backward compatibility fallback for un-prefixed STORAGE_TYPE
	if cfg.StorageType == "" || cfg.StorageType == "postgresql" {
		if rootStorageType := os.Getenv("STORAGE_TYPE"); rootStorageType != "" {
			cfg.StorageType = rootStorageType
		}
	}

	return cfg, nil
}

// DefaultConfig parses environment variables using the standard OpenWiFi prefix ("STORAGE_TYPE_POSTGRESQL_").
func DefaultConfig() (Config, error) {
	return LoadConfig(DefaultEnvPrefix)
}
