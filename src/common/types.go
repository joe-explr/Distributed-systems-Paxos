package common

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"paxos-banking/proto"
	"sync"
	"time"
)

const (
	defaultClientRetryIntervalMs = 5000
	defaultClientAsyncTimeoutMs  = 30000
	defaultClientMaxRetries      = 0
	defaultNodeElectionMinMs     = 700
	defaultNodeElectionMaxMs     = 2000
	defaultPrepareCooldownMs     = 200
	defaultCatchUpMaxWaitMs      = 7000
	defaultCatchUpPollMs         = 150
	defaultCatchUpLeaderWaitMs   = 2000
)

type NodeType int

const (
	Leader NodeType = iota
	Backup
	Client
)

func (nt NodeType) String() string {
	switch nt {
	case Leader:
		return "leader"
	case Backup:
		return "backup"
	case Client:
		return "client"
	default:
		return "unknown"
	}
}

type NodeState int

const (
	Idle NodeState = iota
	Preparing
	Accepting
	Committing
	Executing
)

type TransactionStatus int

const (
	NoStatus TransactionStatus = iota
	Accepted
	Committed
	Executed
)

type PromiseWithSender struct {
	SenderID int32
	Promise  *proto.Promise
}

type PrepareAckWithSender struct {
	SenderID   int32
	PrepareAck *proto.PrepareAck
}

func (ts TransactionStatus) String() string {
	switch ts {
	case NoStatus:
		return "X"
	case Accepted:
		return "A"
	case Committed:
		return "C"
	case Executed:
		return "E"
	default:
		return "X"
	}
}

type TransactionInfo struct {
	SequenceNumber int32
	Request        *proto.Request
	Status         TransactionStatus
	BallotNumber   *proto.BallotNumber
	NodeID         int32
	Timestamp      time.Time
}

type Transaction struct {
	Sender   string
	Receiver string
	Amount   int32
}

type Account struct {
	ClientID string
	Balance  int32
	mu       sync.RWMutex
}

func (a *Account) GetBalance() int32 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.Balance
}

func (a *Account) UpdateBalance(amount int32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Balance += amount
}

func (a *Account) CanTransfer(amount int32) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.Balance >= amount
}

type LogEntry struct {
	Timestamp time.Time
	Operation string
	Details   string
	NodeID    int32
}

type Logger struct {
	entries []LogEntry
	mu      sync.RWMutex
	file    *os.File
}

func NewLogger() *Logger {
	return &Logger{
		entries: make([]LogEntry, 0),
	}
}

func (l *Logger) LogWithWallTime(operation, details string, nodeID int32) {
	wall := time.Now().UnixNano()
	l.Log(operation, fmt.Sprintf("[wall=%d] %s", wall, details), nodeID)
}

func (l *Logger) LogClientWithWallTime(operation, details string, clientID string) {
	wall := time.Now().UnixNano()
	l.LogClient(operation, fmt.Sprintf("[wall=%d] %s", wall, details), clientID)
}

func (l *Logger) Log(operation, details string, nodeID int32) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := LogEntry{
		Timestamp: time.Now(),
		Operation: operation,
		Details:   details,
		NodeID:    nodeID,
	}
	l.entries = append(l.entries, entry)

	if l.file != nil {

		_, _ = fmt.Fprintf(l.file, "[%s] Node %d: %s - %s\n",
			entry.Timestamp.Format("15:04:05.000"),
			entry.NodeID,
			entry.Operation,
			entry.Details,
		)
	}
}

func (l *Logger) LogClient(operation, details string, clientID string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := LogEntry{
		Timestamp: time.Now(),
		Operation: operation,
		Details:   details,
		NodeID:    0,
	}
	l.entries = append(l.entries, entry)

	if l.file != nil {

		_, _ = fmt.Fprintf(l.file, "[%s] Client %s: %s - %s\n",
			entry.Timestamp.Format("15:04:05.000"),
			clientID,
			entry.Operation,
			entry.Details,
		)
	}
}

func (l *Logger) GetEntries() []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	entries := make([]LogEntry, len(l.entries))
	copy(entries, l.entries)
	return entries
}

func (l *Logger) PrintLog() {
	l.mu.RLock()
	defer l.mu.RUnlock()

	fmt.Println("=== Log ===")
	for _, entry := range l.entries {
		if entry.NodeID == 0 {

			fmt.Printf("[%s] Client: %s - %s\n",
				entry.Timestamp.Format("15:04:05.000"),
				entry.Operation,
				entry.Details)
		} else {

			fmt.Printf("[%s] Node %d: %s - %s\n",
				entry.Timestamp.Format("15:04:05.000"),
				entry.NodeID,
				entry.Operation,
				entry.Details)
		}
	}
}

func (l *Logger) EnableNodeFileLogging(nodeID int32) error {

	logDir := "logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	filePath := filepath.Join(logDir, fmt.Sprintf("node-%d.log", nodeID))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	l.mu.Lock()

	if l.file != nil {
		_ = l.file.Close()
	}
	l.file = f
	l.mu.Unlock()

	_, _ = fmt.Fprintf(f, "===== Logging started at %s for Node %d =====\n",
		time.Now().Format(time.RFC3339Nano), nodeID)

	return nil
}

func (l *Logger) EnableClientFileLogging(clientID string) error {

	logDir := "logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	filePath := filepath.Join(logDir, fmt.Sprintf("client-%s.log", clientID))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	l.mu.Lock()

	if l.file != nil {
		_ = l.file.Close()
	}
	l.file = f
	l.mu.Unlock()

	_, _ = fmt.Fprintf(f, "===== Logging started at %s for Client %s =====\n",
		time.Now().Format(time.RFC3339Nano), clientID)

	return nil
}

func (l *Logger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		_ = l.file.Close()
		l.file = nil
	}
}

type Timer struct {
	duration time.Duration
	timer    *time.Timer
	callback func()
	mu       sync.Mutex
	logger   *Logger
	nodeID   int32
}

func NewTimer(duration time.Duration, callback func()) *Timer {
	return &Timer{
		duration: duration,
		callback: callback,
	}
}

func NewTimerWithLogging(duration time.Duration, callback func(), logger *Logger, nodeID int32) *Timer {
	return &Timer{
		duration: duration,
		callback: callback,
		logger:   logger,
		nodeID:   nodeID,
	}
}

func (t *Timer) Start() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.timer != nil {
		t.timer.Stop()
	}

	var timer *time.Timer
	timer = time.AfterFunc(t.duration, func() {
		t.mu.Lock()
		if t.timer == timer {
			t.timer = nil
		}
		t.mu.Unlock()
		t.callback()
	})
	t.timer = timer

	if t.logger != nil {
		t.logger.Log("TIMER", fmt.Sprintf("Started timer with duration %v", t.duration), t.nodeID)
	}
}

func (t *Timer) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil

		if t.logger != nil {
			t.logger.Log("TIMER", "Stopped timer", t.nodeID)
		}
	}
}

func (t *Timer) IsRunning() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.timer != nil
}

type RandomizedTimer struct {
	minDuration time.Duration
	maxDuration time.Duration
	timer       *time.Timer
	callback    func()
	mu          sync.Mutex
	logger      *Logger
	nodeID      int32
}

func NewRandomizedTimer(minDuration, maxDuration time.Duration, callback func()) *RandomizedTimer {
	return &RandomizedTimer{
		minDuration: minDuration,
		maxDuration: maxDuration,
		callback:    callback,
	}
}

func NewRandomizedTimerWithLogging(minDuration, maxDuration time.Duration, callback func(), logger *Logger, nodeID int32) *RandomizedTimer {
	return &RandomizedTimer{
		minDuration: minDuration,
		maxDuration: maxDuration,
		callback:    callback,
		logger:      logger,
		nodeID:      nodeID,
	}
}

func (rt *RandomizedTimer) Start() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if rt.timer != nil {
		rt.timer.Stop()
	}

	randomDuration := rt.generateRandomDuration()
	var timer *time.Timer
	timer = time.AfterFunc(randomDuration, func() {
		rt.mu.Lock()
		if rt.timer == timer {
			rt.timer = nil
		}
		rt.mu.Unlock()
		rt.callback()
	})
	rt.timer = timer

	if rt.logger != nil {
		rt.logger.Log("TIMER", fmt.Sprintf("Started randomized election timer with duration %v (range: %v - %v)", randomDuration, rt.minDuration, rt.maxDuration), rt.nodeID)
	}
}

func (rt *RandomizedTimer) Stop() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if rt.timer != nil {
		rt.timer.Stop()
		rt.timer = nil

		if rt.logger != nil {
			rt.logger.Log("TIMER", "Stopped randomized election timer", rt.nodeID)
		}
	}
}

func (rt *RandomizedTimer) IsRunning() bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.timer != nil
}

func (rt *RandomizedTimer) generateRandomDuration() time.Duration {

	randomMs := rand.Int63n(int64(rt.maxDuration-rt.minDuration)) + int64(rt.minDuration)
	return time.Duration(randomMs)
}

func GenerateRandomElectionTimer() time.Duration {

	minDuration := 700 * time.Millisecond
	maxDuration := 2000 * time.Millisecond

	randomMs := rand.Int63n(int64(maxDuration-minDuration)) + int64(minDuration)
	return time.Duration(randomMs)
}

type Config struct {
	Nodes   []NodeInfo   `json:"nodes"`
	Clients []ClientInfo `json:"clients"`
	F       int32        `json:"f"`
	Timers  TimingConfig `json:"timers"`
}

type TimingConfig struct {
	Client ClientTimingConfig `json:"client"`
	Node   NodeTimingConfig   `json:"node"`
	Test   TestTimingConfig   `json:"test"`
}

type ClientTimingConfig struct {
	RetryIntervalMs int  `json:"retryIntervalMs"`
	AsyncTimeoutMs  int  `json:"asyncTimeoutMs"`
	MaxRetries      int  `json:"maxRetries"`
	RetryForever    bool `json:"retryForever"`
}

type NodeTimingConfig struct {
	ElectionMinMs     int `json:"electionMinMs"`
	ElectionMaxMs     int `json:"electionMaxMs"`
	PrepareCooldownMs int `json:"prepareCooldownMs"`
}

type TestTimingConfig struct {
	CatchUpMaxWaitMs    int `json:"catchUpMaxWaitMs"`
	CatchUpPollMs       int `json:"catchUpPollMs"`
	CatchUpLeaderWaitMs int `json:"catchUpLeaderWaitMs"`
}

func defaultTimingConfig() TimingConfig {
	return TimingConfig{
		Client: ClientTimingConfig{
			RetryIntervalMs: defaultClientRetryIntervalMs,
			AsyncTimeoutMs:  defaultClientAsyncTimeoutMs,
			MaxRetries:      defaultClientMaxRetries,
			RetryForever:    true,
		},
		Node: NodeTimingConfig{
			ElectionMinMs:     defaultNodeElectionMinMs,
			ElectionMaxMs:     defaultNodeElectionMaxMs,
			PrepareCooldownMs: defaultPrepareCooldownMs,
		},
		Test: TestTimingConfig{
			CatchUpMaxWaitMs:    defaultCatchUpMaxWaitMs,
			CatchUpPollMs:       defaultCatchUpPollMs,
			CatchUpLeaderWaitMs: defaultCatchUpLeaderWaitMs,
		},
	}
}

func (t *TimingConfig) applyDefaults() {
	if t == nil {
		return
	}
	if t.Client.RetryIntervalMs <= 0 {
		t.Client.RetryIntervalMs = defaultClientRetryIntervalMs
	}
	if t.Client.AsyncTimeoutMs <= 0 {
		t.Client.AsyncTimeoutMs = defaultClientAsyncTimeoutMs
	}
	if t.Client.MaxRetries < 0 {
		t.Client.MaxRetries = defaultClientMaxRetries
	}
	if t.Node.ElectionMinMs <= 0 {
		t.Node.ElectionMinMs = defaultNodeElectionMinMs
	}
	if t.Node.ElectionMaxMs <= 0 {
		t.Node.ElectionMaxMs = defaultNodeElectionMaxMs
	}
	if t.Node.ElectionMaxMs < t.Node.ElectionMinMs {
		t.Node.ElectionMaxMs = t.Node.ElectionMinMs
	}
	if t.Node.PrepareCooldownMs <= 0 {
		t.Node.PrepareCooldownMs = defaultPrepareCooldownMs
	}
	if t.Test.CatchUpMaxWaitMs <= 0 {
		t.Test.CatchUpMaxWaitMs = defaultCatchUpMaxWaitMs
	}
	if t.Test.CatchUpPollMs <= 0 {
		t.Test.CatchUpPollMs = defaultCatchUpPollMs
	}
	if t.Test.CatchUpLeaderWaitMs <= 0 {
		t.Test.CatchUpLeaderWaitMs = defaultCatchUpLeaderWaitMs
	}
}

func (c *Config) ApplyDefaults() {
	if c == nil {
		return
	}
	if c.Timers.Client.RetryIntervalMs == 0 && c.Timers.Client.AsyncTimeoutMs == 0 &&
		c.Timers.Node.ElectionMinMs == 0 && c.Timers.Node.ElectionMaxMs == 0 {
		c.Timers = defaultTimingConfig()
	}
	c.Timers.applyDefaults()
}

type NodeInfo struct {
	ID      int32  `json:"id"`
	Address string `json:"address"`
	Port    int32  `json:"port"`
}

type ClientInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Port    int32  `json:"port"`
}

func DefaultConfig() *Config {
	cfg := &Config{
		Nodes: []NodeInfo{
			{ID: 1, Address: "localhost", Port: 50051},
			{ID: 2, Address: "localhost", Port: 50052},
			{ID: 3, Address: "localhost", Port: 50053},
			{ID: 4, Address: "localhost", Port: 50054},
			{ID: 5, Address: "localhost", Port: 50055},
		},
		Clients: []ClientInfo{
			{ID: "A", Address: "localhost", Port: 60001},
			{ID: "B", Address: "localhost", Port: 60002},
			{ID: "C", Address: "localhost", Port: 60003},
			{ID: "D", Address: "localhost", Port: 60004},
			{ID: "E", Address: "localhost", Port: 60005},
			{ID: "F", Address: "localhost", Port: 60006},
			{ID: "G", Address: "localhost", Port: 60007},
			{ID: "H", Address: "localhost", Port: 60008},
			{ID: "I", Address: "localhost", Port: 60009},
			{ID: "J", Address: "localhost", Port: 60010},
		},
		F:      2,
		Timers: defaultTimingConfig(),
	}
	cfg.ApplyDefaults()
	return cfg
}
