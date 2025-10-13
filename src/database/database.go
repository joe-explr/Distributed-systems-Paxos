package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Database interface {
	GetAccount(clientID string) (*Account, error)
	UpdateAccount(clientID string, balance int32) error
	GetAllAccounts() (map[string]*Account, error)
	CreateAccount(clientID string, initialBalance int32) error

	LogTransaction(sequenceNumber int32, sender, receiver string, amount int32, status string, timestamp time.Time) error
	GetTransaction(sequenceNumber int32) (*Transaction, error)
	GetAllTransactions() ([]*Transaction, error)

	SaveSystemState(nodeID int32, sequenceNumber int32, executedSeq int32, committedSeq int32) error
	GetSystemState(nodeID int32) (*SystemState, error)

	ClearAll() error

	Close() error
}

type Account struct {
	ClientID string
	Balance  int32
	Created  time.Time
	Updated  time.Time
}

type Transaction struct {
	SequenceNumber int32
	Sender         string
	Receiver       string
	Amount         int32
	Status         string
	Timestamp      time.Time
}

type SystemState struct {
	NodeID         int32
	SequenceNumber int32
	ExecutedSeq    int32
	CommittedSeq   int32
	LastUpdated    time.Time
}

type SQLiteDatabase struct {
	db   *sql.DB
	mu   sync.RWMutex
	path string
}

func NewSQLiteDatabase(nodeID int32) (*SQLiteDatabase, error) {

	dbDir := "data"
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}
	dbPath := filepath.Join(dbDir, fmt.Sprintf("node-%d.db", nodeID))

	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_synchronous=NORMAL&_cache_size=10000")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	sqliteDB := &SQLiteDatabase{
		db:   db,
		path: dbPath,
	}

	if err := sqliteDB.initializeSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	if err := sqliteDB.SaveSystemState(nodeID, 1, 0, 0); err != nil {
		return nil, fmt.Errorf("failed to initialize system state: %w", err)
	}

	return sqliteDB, nil
}

func (s *SQLiteDatabase) initializeSchema() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	accountsSQL := `
	CREATE TABLE IF NOT EXISTS accounts (
		client_id TEXT PRIMARY KEY,
		balance INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`

	transactionsSQL := `
	CREATE TABLE IF NOT EXISTS transactions (
		sequence_number INTEGER PRIMARY KEY,
		sender TEXT NOT NULL,
		receiver TEXT NOT NULL,
		amount INTEGER NOT NULL,
		status TEXT NOT NULL,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
	);`

	systemStateSQL := `
	CREATE TABLE IF NOT EXISTS system_state (
		node_id INTEGER PRIMARY KEY,
		sequence_number INTEGER NOT NULL DEFAULT 1,
		executed_seq INTEGER NOT NULL DEFAULT 0,
		committed_seq INTEGER NOT NULL DEFAULT 0,
		last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
	);`

	indexesSQL := []string{
		"CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);",
		"CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);",
		"CREATE INDEX IF NOT EXISTS idx_accounts_updated ON accounts(updated_at);",
	}

	schemas := []string{accountsSQL, transactionsSQL, systemStateSQL}
	for _, schema := range schemas {
		if _, err := s.db.Exec(schema); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	for _, index := range indexesSQL {
		if _, err := s.db.Exec(index); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}

func (s *SQLiteDatabase) GetAccount(clientID string) (*Account, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `SELECT client_id, balance, created_at, updated_at FROM accounts WHERE client_id = ?`
	row := s.db.QueryRow(query, clientID)

	var account Account
	var createdAt, updatedAt string

	err := row.Scan(&account.ClientID, &account.Balance, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("account not found: %s", clientID)
		}
		return nil, fmt.Errorf("failed to get account: %w", err)
	}

	if account.Created, err = time.Parse(time.RFC3339, createdAt); err != nil {
		if account.Created, err = time.Parse("2006-01-02 15:04:05", createdAt); err != nil {
			return nil, fmt.Errorf("failed to parse created_at: %w", err)
		}
	}
	if account.Updated, err = time.Parse(time.RFC3339, updatedAt); err != nil {
		if account.Updated, err = time.Parse("2006-01-02 15:04:05", updatedAt); err != nil {
			return nil, fmt.Errorf("failed to parse updated_at: %w", err)
		}
	}

	return &account, nil
}

func (s *SQLiteDatabase) UpdateAccount(clientID string, balance int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `UPDATE accounts SET balance = ?, updated_at = CURRENT_TIMESTAMP WHERE client_id = ?`
	result, err := s.db.Exec(query, balance, clientID)
	if err != nil {
		return fmt.Errorf("failed to update account: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("account not found: %s", clientID)
	}

	return nil
}

func (s *SQLiteDatabase) GetAllAccounts() (map[string]*Account, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `SELECT client_id, balance, created_at, updated_at FROM accounts ORDER BY client_id`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query accounts: %w", err)
	}
	defer rows.Close()

	accounts := make(map[string]*Account)
	for rows.Next() {
		var account Account
		var createdAt, updatedAt string

		err := rows.Scan(&account.ClientID, &account.Balance, &createdAt, &updatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan account: %w", err)
		}

		if account.Created, err = time.Parse(time.RFC3339, createdAt); err != nil {
			if account.Created, err = time.Parse("2006-01-02 15:04:05", createdAt); err != nil {
				return nil, fmt.Errorf("failed to parse created_at: %w", err)
			}
		}
		if account.Updated, err = time.Parse(time.RFC3339, updatedAt); err != nil {
			if account.Updated, err = time.Parse("2006-01-02 15:04:05", updatedAt); err != nil {
				return nil, fmt.Errorf("failed to parse updated_at: %w", err)
			}
		}

		accounts[account.ClientID] = &account
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating accounts: %w", err)
	}

	return accounts, nil
}

func (s *SQLiteDatabase) CreateAccount(clientID string, initialBalance int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `INSERT INTO accounts (client_id, balance) VALUES (?, ?)`
	_, err := s.db.Exec(query, clientID, initialBalance)
	if err != nil {
		return fmt.Errorf("failed to create account: %w", err)
	}

	return nil
}

func (s *SQLiteDatabase) LogTransaction(sequenceNumber int32, sender, receiver string, amount int32, status string, timestamp time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `INSERT OR REPLACE INTO transactions (sequence_number, sender, receiver, amount, status, timestamp) VALUES (?, ?, ?, ?, ?, ?)`
	_, err := s.db.Exec(query, sequenceNumber, sender, receiver, amount, status, timestamp)
	if err != nil {
		return fmt.Errorf("failed to log transaction: %w", err)
	}

	return nil
}

func (s *SQLiteDatabase) GetTransaction(sequenceNumber int32) (*Transaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `SELECT sequence_number, sender, receiver, amount, status, timestamp FROM transactions WHERE sequence_number = ?`
	row := s.db.QueryRow(query, sequenceNumber)

	var transaction Transaction
	var timestamp string

	err := row.Scan(&transaction.SequenceNumber, &transaction.Sender, &transaction.Receiver,
		&transaction.Amount, &transaction.Status, &timestamp)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("transaction not found: %d", sequenceNumber)
		}
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	if transaction.Timestamp, err = time.Parse(time.RFC3339, timestamp); err != nil {
		if transaction.Timestamp, err = time.Parse("2006-01-02 15:04:05", timestamp); err != nil {
			return nil, fmt.Errorf("failed to parse timestamp: %w", err)
		}
	}

	return &transaction, nil
}

func (s *SQLiteDatabase) GetAllTransactions() ([]*Transaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `SELECT sequence_number, sender, receiver, amount, status, timestamp FROM transactions ORDER BY sequence_number`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()

	var transactions []*Transaction
	for rows.Next() {
		var transaction Transaction
		var timestamp string

		err := rows.Scan(&transaction.SequenceNumber, &transaction.Sender, &transaction.Receiver,
			&transaction.Amount, &transaction.Status, &timestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %w", err)
		}

		if transaction.Timestamp, err = time.Parse(time.RFC3339, timestamp); err != nil {
			if transaction.Timestamp, err = time.Parse("2006-01-02 15:04:05", timestamp); err != nil {
				return nil, fmt.Errorf("failed to parse timestamp: %w", err)
			}
		}

		transactions = append(transactions, &transaction)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating transactions: %w", err)
	}

	return transactions, nil
}

func (s *SQLiteDatabase) SaveSystemState(nodeID int32, sequenceNumber int32, executedSeq int32, committedSeq int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `INSERT OR REPLACE INTO system_state (node_id, sequence_number, executed_seq, committed_seq, last_updated) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`
	_, err := s.db.Exec(query, nodeID, sequenceNumber, executedSeq, committedSeq)
	if err != nil {
		return fmt.Errorf("failed to save system state: %w", err)
	}

	return nil
}

func (s *SQLiteDatabase) GetSystemState(nodeID int32) (*SystemState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `SELECT node_id, sequence_number, executed_seq, committed_seq, last_updated FROM system_state WHERE node_id = ?`
	row := s.db.QueryRow(query, nodeID)

	var state SystemState
	var lastUpdated string

	err := row.Scan(&state.NodeID, &state.SequenceNumber, &state.ExecutedSeq, &state.CommittedSeq, &lastUpdated)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("system state not found for node: %d", nodeID)
		}
		return nil, fmt.Errorf("failed to get system state: %w", err)
	}

	if state.LastUpdated, err = time.Parse(time.RFC3339, lastUpdated); err != nil {
		if state.LastUpdated, err = time.Parse("2006-01-02 15:04:05", lastUpdated); err != nil {
			return nil, fmt.Errorf("failed to parse last_updated: %w", err)
		}
	}

	return &state, nil
}

func (s *SQLiteDatabase) ClearAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("failed to checkpoint WAL: %w", err)
	}

	tables := []string{"accounts", "transactions", "system_state"}
	for _, table := range tables {
		query := fmt.Sprintf("DELETE FROM %s", table)
		_, err := s.db.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to clear table %s: %w", table, err)
		}
	}

	if _, err := s.db.Exec("VACUUM"); err != nil {
		return fmt.Errorf("failed to vacuum database: %w", err)
	}

	return nil
}

func (s *SQLiteDatabase) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
