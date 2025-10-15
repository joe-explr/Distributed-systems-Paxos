package node

import (
	"fmt"
	"paxos-banking/proto"
	"paxos-banking/src/common"
	"paxos-banking/src/database"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type Node struct {
	ID       int32
	Address  string
	Port     int32
	NodeType common.NodeType
	State    common.NodeState
	Config   *common.Config

	ProposedBallotNumber *proto.BallotNumber
	HighestBallotSeen    *proto.BallotNumber
	AcceptLog            []*proto.AcceptLogEntry
	SequenceNum          int32
	ExecutedSeq          int32
	CommittedSeq         int32

	PromiseAcceptLog           map[int32]*proto.AcceptLogEntry
	minExecutedSeqFromPromises int32

	PromiseCount      int32
	AcceptedBy        map[int32]map[int32]bool
	CommitSent        map[int32]bool
	CommittedSet      map[int32]bool
	LastLeaderMessage time.Time
	LeaderID          int32
	IsLeader          bool

	PendingRequests      []*proto.Request
	AcceptedTransactions map[int32]*proto.Request
	ExecutedTransactions map[int32]bool

	PendingClientReplies map[int32]*proto.Request

	Accounts          map[string]*common.Account
	TransactionStatus map[int32]*common.TransactionInfo

	DB database.Database

	Connections       map[int32]*grpc.ClientConn
	ClientConnections map[string]*grpc.ClientConn
	Server            *grpc.Server

	Logger        *common.Logger
	LeaderTimer   *common.RandomizedTimer
	NewViewMaxSeq int32

	PrepareCooldown tpState

	ReceivedPrepares []*PrepareWithTimestamp

	mu          sync.RWMutex
	requestChan chan *proto.Request
	stopChan    chan bool

	acceptedChan   chan *proto.Accepted
	promiseChan    chan *common.PromiseWithSender
	prepareAckChan chan *common.PrepareAckWithSender

	PromiseAckNodes  map[int32]bool
	PromiseResponded map[int32]bool

	proto.UnimplementedPaxosServiceServer
	proto.UnimplementedNodeServiceServer

	IsActive bool

	NewViewLog []*proto.NewView

	// Deduplication: ballots for which NEW-VIEW has been received (key: "round.node")
	NewViewReceived map[string]bool

	LastClientTimestamp map[string]int64
	LastClientReply     map[string]*proto.Reply

	// Request deduplication: map requestID (clientId:timestamp) -> assigned sequence
	RequestIDToSeq map[string]int32

	RecoveryState RecoveryState

	recoveryMu       sync.Mutex
	recoveryInFlight bool

	// Checkpointing (Bonus)
	LastCheckpointSeq    int32
	LastCheckpointDigest []byte
	LastCheckpointState  []byte
	CheckpointPeriod     int32

	// Promise aggregation for checkpoints (leader election)
	promiseMaxCheckpointSeq    int32
	promiseMaxCheckpointDigest []byte

	// Checkpoint fetch events for observability
	CheckpointFetchLog []CheckpointFetchEvent
}

type RecoveryState int

// Checkpoint fetch observability
type CheckpointFetchEvent struct {
	Seq       int32
	FromNode  int32
	Success   bool
	ErrorMsg  string
	Timestamp time.Time
}

const (
	RecoveryIdle RecoveryState = iota
	RecoveryInProgress
	RecoveryCompleted
	RecoveryFailed
)

func (rs RecoveryState) String() string {
	switch rs {
	case RecoveryIdle:
		return "idle"
	case RecoveryInProgress:
		return "in_progress"
	case RecoveryCompleted:
		return "completed"
	case RecoveryFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func NewNode(id int32, config *common.Config) *Node {
	node := &Node{
		ID:                         id,
		Config:                     config,
		NodeType:                   common.Backup,
		State:                      common.Idle,
		AcceptLog:                  make([]*proto.AcceptLogEntry, 0),
		SequenceNum:                1,
		NewViewMaxSeq:              0,
		ExecutedSeq:                0,
		CommittedSeq:               0,
		PromiseCount:               0,
		AcceptedBy:                 make(map[int32]map[int32]bool),
		CommitSent:                 make(map[int32]bool),
		CommittedSet:               make(map[int32]bool),
		LastLeaderMessage:          time.Now(),
		LeaderID:                   0,
		IsLeader:                   false,
		PendingRequests:            make([]*proto.Request, 0),
		AcceptedTransactions:       make(map[int32]*proto.Request),
		ExecutedTransactions:       make(map[int32]bool),
		PendingClientReplies:       make(map[int32]*proto.Request),
		Accounts:                   make(map[string]*common.Account),
		TransactionStatus:          make(map[int32]*common.TransactionInfo),
		Connections:                make(map[int32]*grpc.ClientConn),
		ClientConnections:          make(map[string]*grpc.ClientConn),
		Logger:                     common.NewLogger(),
		requestChan:                make(chan *proto.Request, 100),
		stopChan:                   make(chan bool),
		acceptedChan:               make(chan *proto.Accepted, 100),
		promiseChan:                make(chan *common.PromiseWithSender, 100),
		prepareAckChan:             make(chan *common.PrepareAckWithSender, 100),
		PromiseAcceptLog:           make(map[int32]*proto.AcceptLogEntry),
		PromiseAckNodes:            make(map[int32]bool),
		PromiseResponded:           make(map[int32]bool),
		IsActive:                   true,
		NewViewLog:                 make([]*proto.NewView, 0),
		LastClientTimestamp:        make(map[string]int64),
		LastClientReply:            make(map[string]*proto.Reply),
		RequestIDToSeq:             make(map[string]int32),
		RecoveryState:              RecoveryIdle,
		LastCheckpointSeq:          0,
		LastCheckpointDigest:       nil,
		LastCheckpointState:        nil,
		CheckpointPeriod:           7,
		promiseMaxCheckpointSeq:    0,
		promiseMaxCheckpointDigest: nil,
		CheckpointFetchLog:         make([]CheckpointFetchEvent, 0),
		NewViewReceived:            make(map[string]bool),
	}

	_ = node.Logger.EnableNodeFileLogging(id)

	node.ProposedBallotNumber = &proto.BallotNumber{
		Round:  1,
		NodeId: node.ID,
	}
	node.HighestBallotSeen = node.ProposedBallotNumber

	db, err := database.NewSQLiteDatabase(id)
	if err != nil {

		fmt.Printf("Warning: Failed to initialize database for node %d: %v\n", id, err)
	} else {
		node.DB = db

		if state, err := db.GetSystemState(id); err == nil {
			node.SequenceNum = state.SequenceNumber
			node.ExecutedSeq = state.ExecutedSeq
			node.CommittedSeq = state.CommittedSeq
			fmt.Printf("Node %d: Loaded system state from database - Seq: %d, Executed: %d, Committed: %d\n",
				id, state.SequenceNumber, state.ExecutedSeq, state.CommittedSeq)
		}
	}

	for _, client := range config.Clients {

		var balance int32 = 10

		if node.DB != nil {
			if account, err := node.DB.GetAccount(client.ID); err == nil {
				balance = account.Balance
			} else {

				if err := node.DB.CreateAccount(client.ID, balance); err != nil {
					fmt.Printf("Warning: Failed to create account %s in database: %v\n", client.ID, err)
				}
			}
		}

		node.Accounts[client.ID] = &common.Account{
			ClientID: client.ID,
			Balance:  balance,
		}
	}

	minTimeout := 700 * time.Millisecond
	maxTimeout := 2000 * time.Millisecond
	node.LeaderTimer = common.NewRandomizedTimerWithLogging(minTimeout, maxTimeout, node.handleLeaderTimeout, node.Logger, node.ID)
	node.Logger.Log("INIT", fmt.Sprintf("Initialized with randomized election timer (range: %v - %v)", minTimeout, maxTimeout), node.ID)

	node.PrepareCooldown = tpState{
		lastPrepareSeen: time.Time{},
		tp:              200 * time.Millisecond,
	}

	node.ReceivedPrepares = make([]*PrepareWithTimestamp, 0)

	return node
}

type PrepareWithTimestamp struct {
	Prepare   *proto.Prepare
	Timestamp time.Time
}

type tpState struct {
	lastPrepareSeen time.Time
	tp              time.Duration
}

func (s *tpState) canSendPrepare(now time.Time) bool {
	if s.lastPrepareSeen.IsZero() {
		return true
	}
	return now.Sub(s.lastPrepareSeen) >= s.tp
}

func (s *tpState) notePrepareSeen(now time.Time) {
	s.lastPrepareSeen = now
}
