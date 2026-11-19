package postgres_test

import (
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/routerarchitects/ra-common-mods/postgres"
)

func TestBuildDSN_SpecialCharacters(t *testing.T) {
	testCases := []struct {
		name             string
		username         string
		password         string
		host             string
		port             int
		database         string
		sslmode          string
		expectedPassword string
	}{
		{
			name:             "Standard credentials",
			username:         "postgres",
			password:         "postgres",
			host:             "localhost",
			port:             5432,
			database:         "testdb",
			sslmode:          "disable",
			expectedPassword: "postgres",
		},
		{
			name:             "Password with percent sign",
			username:         "postgres",
			password:         "sec%zzret",
			host:             "localhost",
			port:             5432,
			database:         "testdb",
			sslmode:          "disable",
			expectedPassword: "sec%zzret",
		},
		{
			name:             "Password with at-sign",
			username:         "appuser",
			password:         "p@ssw@rd",
			host:             "db.internal",
			port:             5432,
			database:         "test_db",
			sslmode:          "require",
			expectedPassword: "p@ssw@rd",
		},
		{
			name:             "Password with slash and spaces",
			username:         "appuser",
			password:         "p/ss word with spaces",
			host:             "localhost",
			port:             5432,
			database:         "testdb",
			sslmode:          "disable",
			expectedPassword: "p/ss word with spaces",
		},
		{
			name:             "Password with hash and question mark",
			username:         "appuser",
			password:         "p#ss?w&rd=123",
			host:             "localhost",
			port:             5432,
			database:         "testdb",
			sslmode:          "verify-full",
			expectedPassword: "p#ss?w&rd=123",
		},
		{
			name:             "Database with leading slash",
			username:         "appuser",
			password:         "secret",
			host:             "localhost",
			port:             5432,
			database:         "/testdb_with_slash",
			sslmode:          "disable",
			expectedPassword: "secret",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := postgres.Config{
				StorageType: "postgresql",
				Host:        tc.host,
				Port:        tc.port,
				Username:    tc.username,
				Password:    tc.password,
				Database:    tc.database,
				SSLMode:     tc.sslmode,
			}

			dsn := postgres.BuildDSN(cfg)
			t.Logf("Generated DSN: %s", dsn)

			poolConfig, err := pgxpool.ParseConfig(dsn)
			if err != nil {
				t.Fatalf("pgxpool.ParseConfig failed on valid password %q: %v", tc.password, err)
			}

			if poolConfig.ConnConfig.Password != tc.expectedPassword {
				t.Errorf("Password mismatch: got %q, want %q", poolConfig.ConnConfig.Password, tc.expectedPassword)
			}
			if poolConfig.ConnConfig.User != tc.username {
				t.Errorf("Username mismatch: got %q, want %q", poolConfig.ConnConfig.User, tc.username)
			}
			cleanDB := strings.TrimPrefix(tc.database, "/")
			if poolConfig.ConnConfig.Database != cleanDB {
				t.Errorf("Database mismatch: got %q, want %q", poolConfig.ConnConfig.Database, cleanDB)
			}
		})
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	// Clean ambient environment
	t.Setenv("STORAGE_TYPE", "")
	t.Setenv("HOST", "")
	t.Setenv("PORT", "")
	t.Setenv("USERNAME", "")
	t.Setenv("PASSWORD", "")
	t.Setenv("DATABASE", "")
	t.Setenv("SSLMODE", "")

	cfg, err := postgres.LoadConfig("")
	if err != nil {
		t.Fatalf("expected no error loading default config, got: %v", err)
	}

	if cfg.Host != "localhost" {
		t.Errorf("expected default Host 'localhost', got %q", cfg.Host)
	}
	if cfg.Port != 5432 {
		t.Errorf("expected default Port 5432, got %d", cfg.Port)
	}
	if cfg.Database != "postgres" {
		t.Errorf("expected default Database 'postgres', got %q", cfg.Database)
	}
	if cfg.MaxConns != 25 {
		t.Errorf("expected default MaxConns 25, got %d", cfg.MaxConns)
	}
	if cfg.MinConns != 2 {
		t.Errorf("expected default MinConns 2, got %d", cfg.MinConns)
	}
	if cfg.MaxConnLifetime != 30*time.Minute {
		t.Errorf("expected default MaxConnLifetime 30m, got %v", cfg.MaxConnLifetime)
	}
	if !cfg.AutoMigrate {
		t.Errorf("expected default AutoMigrate true, got %v", cfg.AutoMigrate)
	}
}

func TestLoadConfig_CustomPrefix_MultiDatabase(t *testing.T) {
	// Configure Primary DB
	t.Setenv("STORAGE_TYPE_POSTGRESQL_HOST", "primary-db.local")
	t.Setenv("STORAGE_TYPE_POSTGRESQL_PORT", "5432")
	t.Setenv("STORAGE_TYPE_POSTGRESQL_USERNAME", "primary_user")
	t.Setenv("STORAGE_TYPE_POSTGRESQL_PASSWORD", "primary_pass")
	t.Setenv("STORAGE_TYPE_POSTGRESQL_DATABASE", "primary_db")

	// Configure Replica DB
	t.Setenv("REPLICA_POSTGRESQL_HOST", "replica-db.local")
	t.Setenv("REPLICA_POSTGRESQL_PORT", "5433")
	t.Setenv("REPLICA_POSTGRESQL_USERNAME", "replica_user")
	t.Setenv("REPLICA_POSTGRESQL_PASSWORD", "replica_pass")
	t.Setenv("REPLICA_POSTGRESQL_DATABASE", "replica_db")

	primaryCfg, err := postgres.DefaultConfig()
	if err != nil {
		t.Fatalf("expected no error loading primary config, got: %v", err)
	}
	if primaryCfg.Host != "primary-db.local" {
		t.Errorf("expected primary host 'primary-db.local', got %q", primaryCfg.Host)
	}
	if primaryCfg.Database != "primary_db" {
		t.Errorf("expected primary database 'primary_db', got %q", primaryCfg.Database)
	}

	replicaCfg, err := postgres.LoadConfig("REPLICA_POSTGRESQL_")
	if err != nil {
		t.Fatalf("expected no error loading replica config, got: %v", err)
	}
	if replicaCfg.Host != "replica-db.local" {
		t.Errorf("expected replica host 'replica-db.local', got %q", replicaCfg.Host)
	}
	if replicaCfg.Port != 5433 {
		t.Errorf("expected replica port 5433, got %d", replicaCfg.Port)
	}
	if replicaCfg.Database != "replica_db" {
		t.Errorf("expected replica database 'replica_db', got %q", replicaCfg.Database)
	}
}
