package node

import (
	"context"
	"fmt"
	"paxos-banking/proto"
	"paxos-banking/src/common"
	"paxos-banking/src/database"
	"time"
)

func CompareBallots(b1, b2 *proto.BallotNumber) int {
	if b1 == nil && b2 == nil {
		return 0
	}
	if b1 == nil {
		return -1
	}
	if b2 == nil {
		return 1
	}

	if b1.Round < b2.Round {
		return -1
	}
	if b1.Round > b2.Round {
		return 1
	}

	if b1.NodeId < b2.NodeId {
		return -1
	}
	if b1.NodeId > b2.NodeId {
		return 1
	}

	return 0
}

func IsHigherBallot(ballot1, ballot2 *proto.BallotNumber) bool {
	return CompareBallots(ballot1, ballot2) > 0
}

func IsEqualBallot(ballot1, ballot2 *proto.BallotNumber) bool {
	return CompareBallots(ballot1, ballot2) == 0
}

func (n *Node) isValidAccept(req *proto.Accept) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return IsHigherBallot(req.Ballot, n.HighestBallotSeen) || IsEqualBallot(req.Ballot, n.HighestBallotSeen)
}

func (n *Node) isValidPrepare(req *proto.Prepare) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return IsHigherBallot(req.Ballot, n.HighestBallotSeen) || IsEqualBallot(req.Ballot, n.HighestBallotSeen)
}

func (n *Node) GetStatus(ctx context.Context, req *proto.Empty) (*proto.Status, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", "Rejecting GetStatus - node is inactive", n.ID)
		return &proto.Status{
			Status:  "down",
			Message: "Node is down",
		}, nil
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	status := &proto.Status{
		Status:  "running",
		Message: fmt.Sprintf("Node %d is running as %v", n.ID, n.NodeType),
	}

	return status, nil
}

func (n *Node) GetLeader(ctx context.Context, req *proto.Empty) (*proto.NodeInfo, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", "Rejecting GetLeader - node is inactive", n.ID)
		return nil, fmt.Errorf("node is inactive")
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.LeaderID > 0 {

		for _, nodeInfo := range n.Config.Nodes {
			if nodeInfo.ID == n.LeaderID {
				return &proto.NodeInfo{
					NodeId:  nodeInfo.ID,
					Address: nodeInfo.Address,
					Port:    nodeInfo.Port,
				}, nil
			}
		}
	}

	return &proto.NodeInfo{
		NodeId:  0,
		Address: "",
		Port:    0,
	}, nil
}

func (n *Node) PrintLog(ctx context.Context, req *proto.Empty) (*proto.Status, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", "Rejecting PrintLog - node is inactive", n.ID)
		return &proto.Status{
			Status:  "error",
			Message: "Node is down",
		}, nil
	}

	n.Logger.PrintLog()

	status := &proto.Status{
		Status:  "success",
		Message: "Log printed",
	}

	return status, nil
}

func (n *Node) PrintDB(ctx context.Context, req *proto.Empty) (*proto.Status, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", "Rejecting PrintDB - node is inactive", n.ID)
		return &proto.Status{
			Status:  "error",
			Message: "Node is down",
		}, nil
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	fmt.Println("=== Account Balances ===")
	for clientID, account := range n.Accounts {
		fmt.Printf("Client %s: $%d\n", clientID, account.GetBalance())
	}

	status := &proto.Status{
		Status:  "success",
		Message: "Database printed",
	}

	return status, nil
}

func (n *Node) PrintStatus(ctx context.Context, req *proto.PrintStatusRequest) (*proto.Status, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", "Rejecting PrintStatus - node is inactive", n.ID)
		return &proto.Status{
			Status:  "error",
			Message: "Node is down",
		}, nil
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	sequenceNumber := req.SequenceNumber
	fmt.Printf("=== Transaction Status for Sequence %d ===\n", sequenceNumber)

	if transactionInfo, exists := n.TransactionStatus[sequenceNumber]; exists {
		fmt.Printf("Sequence %d: Status = %s\n", sequenceNumber, transactionInfo.Status.String())
		if transactionInfo.Request != nil {
			fmt.Printf("  Transaction: %s->%s $%d\n",
				transactionInfo.Request.Transaction.Sender,
				transactionInfo.Request.Transaction.Receiver,
				transactionInfo.Request.Transaction.Amount)
		}
		if transactionInfo.BallotNumber != nil {
			fmt.Printf("  Ballot: (%d, %d)\n",
				transactionInfo.BallotNumber.Round,
				transactionInfo.BallotNumber.NodeId)
		}
		fmt.Printf("  Node ID: %d\n", transactionInfo.NodeID)
		fmt.Printf("  Timestamp: %s\n", transactionInfo.Timestamp.Format("15:04:05.000"))
	} else {
		fmt.Printf("Sequence %d: Status = X (No Status)\n", sequenceNumber)
		fmt.Println("  Transaction not found or not processed yet")
	}

	status := &proto.Status{
		Status:  "success",
		Message: fmt.Sprintf("Status for sequence %d printed", sequenceNumber),
	}

	return status, nil
}

func (n *Node) PrintView(ctx context.Context, req *proto.Empty) (*proto.Status, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", "Rejecting PrintView - node is inactive", n.ID)
		return &proto.Status{
			Status:  "error",
			Message: "Node is down",
		}, nil
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	fmt.Println("=== New-View Messages & Checkpoints ===")
	if len(n.NewViewLog) == 0 {
		fmt.Println("No new-view messages received yet")
	} else {
		for i, newView := range n.NewViewLog {
			fmt.Printf("NEW-VIEW #%d:\n", i+1)
			fmt.Printf("  Ballot: (%d, %d)\n", newView.Ballot.Round, newView.Ballot.NodeId)
			fmt.Printf("  AcceptLog entries: %d\n", len(newView.AcceptLog))
			if newView.BaseCheckpointSeq > 0 {

				short := ""
				if len(newView.BaseCheckpointDigest) > 0 {
					hexmap := "0123456789abcdef"
					limit := len(newView.BaseCheckpointDigest)
					if limit > 4 {
						limit = 4
					}
					for i := 0; i < limit; i++ {
						b := newView.BaseCheckpointDigest[i]

						fmt.Printf("")
						short += string(hexmap[b>>4]) + string(hexmap[b&0x0f])
					}
				}
				fmt.Printf("  BaseCheckpoint: seq=%d digest=%s\n", newView.BaseCheckpointSeq, short)
			}
			for _, entry := range newView.AcceptLog {
				if entry.AcceptVal.Transaction.Sender == "no-op" {
					fmt.Printf("    Seq %d: NO-OP (ballot %d.%d)\n",
						entry.AcceptSeq, entry.AcceptNum.Round, entry.AcceptNum.NodeId)
				} else {
					fmt.Printf("    Seq %d: %s->%s $%d (ballot %d.%d)\n",
						entry.AcceptSeq, entry.AcceptVal.Transaction.Sender,
						entry.AcceptVal.Transaction.Receiver, entry.AcceptVal.Transaction.Amount,
						entry.AcceptNum.Round, entry.AcceptNum.NodeId)
				}
			}
			fmt.Println()
		}
	}

	if n.LastCheckpointSeq > 0 {
		short := ""
		if len(n.LastCheckpointDigest) > 0 {
			hexmap := "0123456789abcdef"
			limit := len(n.LastCheckpointDigest)
			if limit > 4 {
				limit = 4
			}
			for i := 0; i < limit; i++ {
				b := n.LastCheckpointDigest[i]
				short += string(hexmap[b>>4]) + string(hexmap[b&0x0f])
			}
		}
		fmt.Printf("LocalCheckpoint: seq=%d digest=%s\n", n.LastCheckpointSeq, short)
	}

	if len(n.CheckpointFetchLog) > 0 {
		fmt.Println("Checkpoint Fetch Events:")
		for _, ev := range n.CheckpointFetchLog {
			result := "ok"
			if !ev.Success {
				result = "fail"
			}
			fmt.Printf("  seq=%d from=n%d time=%s result=%s", ev.Seq, ev.FromNode, ev.Timestamp.Format("15:04:05.000"), result)
			if ev.ErrorMsg != "" {
				fmt.Printf(" err=%s", ev.ErrorMsg)
			}
			fmt.Println()
		}
	}

	status := &proto.Status{
		Status:  "success",
		Message: "View printed",
	}
	return status, nil
}

func (n *Node) getTransactionStatus(sequenceNumber int32) common.TransactionStatus {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if transactionInfo, exists := n.TransactionStatus[sequenceNumber]; exists {
		return transactionInfo.Status
	}
	return common.NoStatus
}

func (n *Node) SetNodeActive(active bool) {
	n.mu.Lock()
	wasActive := n.IsActive
	n.IsActive = active
	timer := n.LeaderTimer
	n.mu.Unlock()

	n.Logger.Log("ACTIVITY", fmt.Sprintf("Node %d set to active=%v", n.ID, active), n.ID)

	if !active && timer != nil {
		timer.Stop()
		n.mu.Lock()
		n.LeaderTimer = nil

		if n.RecoveryState == RecoveryInProgress {
			n.RecoveryState = RecoveryFailed
			n.Logger.Log("RECOVERY", "Recovery aborted due to node deactivation", n.ID)
		}
		n.mu.Unlock()
		n.Logger.Log("TIMER", "Stopped and cleared timer on deactivation", n.ID)
	}

	if active && !wasActive {
		// Enter recovery mode immediately to avoid acting on stale leader state.
		n.mu.Lock()
		if n.RecoveryState != RecoveryInProgress {
			n.RecoveryState = RecoveryInProgress
		}
		n.mu.Unlock()

		// Ensure we are not considered leader before recovery finishes.
		n.stepDownLeader("node reactivated, clearing stale leader state", nil)
		go n.simpleRecovery()
	}
}

func (n *Node) IsNodeActive() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.IsActive
}

func (n *Node) IsRecovering() bool {
	n.mu.RLock()
	recovering := n.RecoveryState == RecoveryInProgress
	n.mu.RUnlock()

	if recovering {
		return true
	}

	n.recoveryMu.Lock()
	inFlight := n.recoveryInFlight
	n.recoveryMu.Unlock()

	return inFlight
}

func (n *Node) GetRecoveryState() RecoveryState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.RecoveryState
}

func (n *Node) ClearNewViewLog() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.NewViewLog = make([]*proto.NewView, 0)
	n.Logger.Log("CLEAR", "Cleared NEW-VIEW log for new test case", n.ID)
}

func (n *Node) GetAccountBalancesMap() map[string]int32 {
	n.mu.RLock()
	defer n.mu.RUnlock()

	balances := make(map[string]int32)
	for clientID, account := range n.Accounts {
		balances[clientID] = account.GetBalance()
	}
	return balances
}

func (n *Node) ResetPersistentState() error {

	if n.DB == nil {

		db, err := database.NewSQLiteDatabase(n.ID)
		if err != nil {
			return fmt.Errorf("node %d: failed to open database: %w", n.ID, err)
		}
		n.DB = db
	}

	if err := n.DB.ClearAll(); err != nil {
		return fmt.Errorf("node %d: failed clearing DB: %w", n.ID, err)
	}

	if err := n.DB.SaveSystemState(n.ID, 1, 0, 0); err != nil {
		return fmt.Errorf("node %d: failed to initialize system_state: %w", n.ID, err)
	}

	n.mu.Lock()
	for _, clientInfo := range n.Config.Clients {

		if err := n.DB.CreateAccount(clientInfo.ID, 10); err != nil {
			n.mu.Unlock()
			return fmt.Errorf("node %d: failed to create account %s: %w", n.ID, clientInfo.ID, err)
		}

		if acct, ok := n.Accounts[clientInfo.ID]; ok {
			acct.UpdateBalance(10 - acct.GetBalance())
		} else {
			n.Accounts[clientInfo.ID] = &common.Account{ClientID: clientInfo.ID, Balance: 10}
		}
	}

	n.SequenceNum = 1
	n.ExecutedSeq = 0
	n.CommittedSeq = 0
	n.AcceptLog = make([]*proto.AcceptLogEntry, 0)
	n.ExecutedTransactions = make(map[int32]bool)
	n.TransactionStatus = make(map[int32]*common.TransactionInfo)
	n.PendingClientReplies = make(map[int32]*proto.Request)
	n.PromiseAcceptLog = make(map[int32]*proto.AcceptLogEntry)
	n.CommitSent = make(map[int32]bool)
	n.AcceptedBy = make(map[int32]map[int32]bool)
	n.CommittedSet = make(map[int32]bool)
	n.LastLeaderMessage = time.Now()
	n.mu.Unlock()

	return nil
}

func (n *Node) IsTransactionExecuted(sequenceNumber int32) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.ExecutedTransactions[sequenceNumber]
}

func (n *Node) GetExecutedSequence() int32 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.ExecutedSeq
}

func (n *Node) IsNodeLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.IsLeader
}

func (n *Node) GetTransactionStatus(sequenceNumber int32) common.TransactionStatus {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if transactionInfo, exists := n.TransactionStatus[sequenceNumber]; exists {
		return transactionInfo.Status
	}
	return common.NoStatus
}

func (n *Node) IsTransactionFullyCompleted(sequenceNumber int32) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if !n.ExecutedTransactions[sequenceNumber] {
		return false
	}

	if transactionInfo, exists := n.TransactionStatus[sequenceNumber]; exists {
		return transactionInfo.Status == common.Executed
	}

	return true
}

func (n *Node) GetNodeStateInfo() (isLeader bool, nodeType common.NodeType, lastActivity time.Time) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.IsLeader, n.NodeType, n.LastLeaderMessage
}

func (n *Node) GetTransactionInfo(sequenceNumber int32) (*common.TransactionInfo, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if transactionInfo, exists := n.TransactionStatus[sequenceNumber]; exists {

		return &common.TransactionInfo{
			Request:      transactionInfo.Request,
			Status:       transactionInfo.Status,
			BallotNumber: transactionInfo.BallotNumber,
			NodeID:       transactionInfo.NodeID,
			Timestamp:    transactionInfo.Timestamp,
		}, true
	}
	return nil, false
}

func (n *Node) GetPendingRequests() []*proto.Request {
	n.mu.RLock()
	defer n.mu.RUnlock()

	pending := make([]*proto.Request, len(n.PendingRequests))
	copy(pending, n.PendingRequests)
	return pending
}

func (n *Node) PrintPendingRequests(ctx context.Context, req *proto.Empty) (*proto.Status, error) {
	if !n.IsNodeActive() {
		return &proto.Status{Status: "error", Message: "Node is down"}, nil
	}

	n.mu.RLock()
	pending := n.PendingRequests
	n.mu.RUnlock()

	fmt.Printf("\n=== Node %d Pending Requests ===\n", n.ID)
	if len(pending) == 0 {
		fmt.Println("No pending requests")
	} else {
		for i, req := range pending {
			fmt.Printf("%d. Client %s: %s->%s $%d (timestamp: %d)\n",
				i+1, req.ClientId, req.Transaction.Sender,
				req.Transaction.Receiver, req.Transaction.Amount, req.Timestamp)
		}
	}

	return &proto.Status{Status: "success", Message: "Printed pending requests"}, nil
}

func (n *Node) GetNewViewLog() []*proto.NewView {
	n.mu.RLock()
	defer n.mu.RUnlock()

	logCopy := make([]*proto.NewView, len(n.NewViewLog))
	copy(logCopy, n.NewViewLog)
	return logCopy
}
