package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"paxos-banking/proto"
	"paxos-banking/src/common"
	"sort"
	"strconv"
	"time"

	"google.golang.org/grpc/metadata"
)

func makeRequestID(req *proto.Request) string {
	if req == nil || req.ClientId == "" {
		return ""
	}
	return req.ClientId + ":" + strconv.FormatInt(req.Timestamp, 10)
}

func cloneTransaction(tx *proto.Transaction) *proto.Transaction {
	if tx == nil {
		return nil
	}
	return &proto.Transaction{
		Sender:   tx.Sender,
		Receiver: tx.Receiver,
		Amount:   tx.Amount,
	}
}
func cloneBallotNumber(ballot *proto.BallotNumber) *proto.BallotNumber {
	if ballot == nil {
		return nil
	}
	return &proto.BallotNumber{
		Round:  ballot.Round,
		NodeId: ballot.NodeId,
	}
}
func cloneCommit(c *proto.Commit) *proto.Commit {
	if c == nil {
		return nil
	}
	return &proto.Commit{
		Ballot:   cloneBallotNumber(c.Ballot), // or manually copy fields
		Sequence: c.Sequence,
		Request:  cloneRequest(c.Request),
	}
}
func cloneRequest(req *proto.Request) *proto.Request {
	if req == nil {
		return nil
	}
	return &proto.Request{
		ClientId:    req.ClientId,
		Timestamp:   req.Timestamp,
		Transaction: cloneTransaction(req.Transaction),
	}
}

func (n *Node) rememberRequestLocked(req *proto.Request, seq int32) {
	n.Logger.Log("DEBUG", "rememberRequestLocked called", n.ID)
	rid := makeRequestID(req)
	if rid == "" {
		return
	}
	if n.RequestIDToSeq == nil {
		n.RequestIDToSeq = make(map[string]int32)
	}
	n.RequestIDToSeq[rid] = seq

	n.rememberClientTimestampLocked(req)
}

func (n *Node) rememberClientTimestampLocked(req *proto.Request) {
	n.Logger.Log("DEBUG", "rememberClientTimestampLocked called", n.ID)
	if req == nil || req.ClientId == "" {
		return
	}
	if n.LastClientTimestamp == nil {
		n.LastClientTimestamp = make(map[string]int64)
	}
	if req.Timestamp > n.LastClientTimestamp[req.ClientId] {
		n.LastClientTimestamp[req.ClientId] = req.Timestamp
	}
}

func (n *Node) enqueuePendingRequestLocked(req *proto.Request) {
	n.Logger.Log("DEBUG", "enqueuePendingRequestLocked called", n.ID)
	if req == nil {
		return
	}
	rid := makeRequestID(req)
	if rid != "" {
		if n.PendingRequestIDs == nil {
			n.PendingRequestIDs = make(map[string]bool)
		}
		if n.PendingRequestIDs[rid] {
			n.Logger.Log("DEBUG", fmt.Sprintf("Skipping duplicate pending request %s", rid), n.ID)
			return
		}
		n.PendingRequestIDs[rid] = true
	}
	cloned := cloneRequest(req)
	if cloned == nil {
		return
	}
	n.rememberClientTimestampLocked(cloned)
	n.PendingRequests = append(n.PendingRequests, cloned)
}

func (n *Node) enqueuePendingRequestsLocked(requests []*proto.Request) {
	n.Logger.Log("DEBUG", "enqueuePendingRequestsLocked called", n.ID)
	for _, req := range requests {
		n.enqueuePendingRequestLocked(req)
	}
}

func (n *Node) clearPendingRequestsLocked() {
	n.PendingRequests = n.PendingRequests[:0]
	n.PendingRequestIDs = make(map[string]bool)
}

func (n *Node) broadcastAccept(accept *proto.Accept) {
	n.Logger.Log("BROADCAST", fmt.Sprintf("Broadcasting accept for sequence %d", accept.Sequence), n.ID)

	for _, nodeInfo := range n.Config.Nodes {
		if nodeInfo.ID == n.ID {
			continue
		}

		go func(nodeInfo common.NodeInfo) {
			conn, err := n.getConnection(nodeInfo.ID)
			if err != nil {
				n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to node %d: %v", nodeInfo.ID, err), n.ID)
				return
			}

			client := proto.NewNodeServiceClient(conn)

			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()

				_, err := client.HandleAccept(ctx, accept)
				if err != nil {
					n.Logger.Log("ERROR", fmt.Sprintf("Failed to send accept to node %d: %v", nodeInfo.ID, err), n.ID)
					return
				}

				n.Logger.Log("ACCEPT", fmt.Sprintf("Successfully sent accept to node %d", nodeInfo.ID), n.ID)
			}()
		}(nodeInfo)
	}

}

func (n *Node) processAcceptedMessage(accepted *proto.Accepted) {
	n.Logger.Log("DEBUG", "processAcceptedMessage called", n.ID)
	n.mu.Lock()
	shouldStepDown := false
	shouldSendCommit := false
	var stepDownBallot *proto.BallotNumber
	stepDownReason := ""
	var commitTarget int32
	var commitRequest *proto.Request

	sequence := accepted.Sequence

	defer func() {
		n.mu.Unlock()
		if shouldStepDown {
			n.stepDownLeader(stepDownReason, stepDownBallot)
		}
		if shouldSendCommit {
			go n.sendCommitToNode(commitTarget, sequence, commitRequest)
		}
	}()

	if !n.IsLeader {
		n.Logger.Log("IGNORE", fmt.Sprintf("Ignoring accepted for sequence %d from node %d - not leader", sequence, accepted.NodeId), n.ID)
		return
	}

	n.Logger.Log("DEBUG", fmt.Sprintf("handleAccepted: sequence=%d, ProposedBallotNumber=%v, accepted.Ballot=%v",
		sequence, n.ProposedBallotNumber, accepted.Ballot), n.ID)

	if n.ProposedBallotNumber == nil || accepted.Ballot == nil || !IsEqualBallot(accepted.Ballot, n.ProposedBallotNumber) {

		if accepted.Ballot != nil && (n.ProposedBallotNumber != nil) && IsHigherBallot(accepted.Ballot, n.ProposedBallotNumber) {
			if IsHigherBallot(accepted.Ballot, n.HighestBallotSeen) {
				n.HighestBallotSeen = accepted.Ballot
			}

			shouldStepDown = true
			stepDownBallot = accepted.Ballot
			stepDownReason = fmt.Sprintf("received Accepted from %d with higher ballot %d.%d", accepted.NodeId, accepted.Ballot.Round, accepted.Ballot.NodeId)
		} else if n.ProposedBallotNumber == nil {
			shouldStepDown = true
			stepDownReason = fmt.Sprintf("missing ballot while handling Accepted from %d", accepted.NodeId)
		}
		n.Logger.Log("IGNORE", fmt.Sprintf("Ignoring accepted for sequence %d from node %d due to ballot mismatch", sequence, accepted.NodeId), n.ID)
		return
	}

	if n.AcceptedBy[sequence] == nil {
		n.AcceptedBy[sequence] = make(map[int32]bool)
	}
	n.AcceptedBy[sequence][accepted.NodeId] = true
	count := int32(len(n.AcceptedBy[sequence]))

	n.Logger.Log("ACCEPTED", fmt.Sprintf("Received accepted from node %d for sequence %d (unique: %d)",
		accepted.NodeId, sequence, count), n.ID)

	if count >= n.Config.F+1 {

		n.CommittedSet[sequence] = true

		if !n.CommitSent[sequence] {
			n.Logger.Log("MAJORITY", fmt.Sprintf("Majority reached for sequence %d (%d/%d)",
				sequence, count, n.Config.F+1), n.ID)
			if transaction, exists := n.AcceptedTransactions[sequence]; exists {
				n.CommitSent[sequence] = true
				go n.broadcastCommit(sequence, transaction)
			}
		}

		for {
			next := n.CommittedSeq + 1
			if n.CommittedSet[next] {
				n.CommittedSeq = next
			} else {
				break
			}
		}
	}

	if n.CommittedSet[sequence] {
		if txn, ok := n.AcceptedTransactions[sequence]; ok {
			commitTarget = accepted.NodeId
			commitRequest = txn
			shouldSendCommit = true
			return
		}
	}
}

func (n *Node) sendCommitToNode(targetNodeID int32, sequence int32, request *proto.Request) {
	n.Logger.Log("DEBUG", "sendCommitToNode called", n.ID)
	if targetNodeID == 0 || targetNodeID == n.ID {
		return
	}

	n.mu.Lock()
	if n.ProposedBallotNumber == nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Cannot send commit to node %d for sequence %d - ProposedBallotNumber is nil", targetNodeID, sequence), n.ID)
		n.mu.Unlock()
		return
	}

	ballotCopy := &proto.BallotNumber{
		Round:  n.ProposedBallotNumber.Round,
		NodeId: n.ProposedBallotNumber.NodeId,
	}
	n.mu.Unlock()

	conn, err := n.getConnection(targetNodeID)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to node %d for Commit: %v", targetNodeID, err), n.ID)
		return
	}
	client := proto.NewNodeServiceClient(conn)
	commit := &proto.Commit{Ballot: ballotCopy, Sequence: sequence, Request: request}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := client.HandleCommit(ctx, commit); err != nil {
			n.Logger.Log("ERROR", fmt.Sprintf("Failed to send commit to node %d: %v", targetNodeID, err), n.ID)
		}
	}()
}

func (n *Node) handleLeaderTimeout() {
	n.Logger.Log("DEBUG", "handleLeaderTimeout called", n.ID)

	n.mu.Lock()
	now := time.Now()
	// var highestPrepare *proto.Prepare
	var recentPrepare *proto.Prepare

	for _, prepareWithTime := range n.ReceivedPrepares {
		if now.Sub(prepareWithTime.Timestamp) <= n.PrepareCooldown.tp {
			if recentPrepare == nil || IsHigherBallot(prepareWithTime.Prepare.Ballot, recentPrepare.Ballot) {
				recentPrepare = prepareWithTime.Prepare
			}
		}
	}

	// if recentPrepare == nil {
	// 	for _, prepareWithTime := range n.ReceivedPrepares {
	// 		if highestPrepare == nil || IsHigherBallot(prepareWithTime.Prepare.Ballot, highestPrepare.Ballot) {
	// 			highestPrepare = prepareWithTime.Prepare
	// 		}
	// 	}
	// }

	var validPrepares []*PrepareWithTimestamp
	for _, prepareWithTime := range n.ReceivedPrepares {

		if now.Sub(prepareWithTime.Timestamp) <= n.PrepareCooldown.tp {
			validPrepares = append(validPrepares, prepareWithTime)
		}
	}
	n.ReceivedPrepares = validPrepares
	n.ReceivedPrepares = []*PrepareWithTimestamp{}
	n.mu.Unlock()

	if recentPrepare != nil {
		n.Logger.Log("TIMEOUT", fmt.Sprintf("Found prepare message %d.%d in last tp time, sending ACK",
			recentPrepare.Ballot.Round, recentPrepare.Ballot.NodeId), n.ID)
		n.processPrepareMessageDirectly(recentPrepare)
		return
	}

	n.Logger.Log("TIMEOUT", "No prepare messages in last tp time, starting leader election", n.ID)
	n.Logger.Log("ELECTION", fmt.Sprintf("Leader timeout detected; triggering election (need %d promises)", n.Config.F+1), n.ID)
	n.initiateLeaderElection()
}

func (n *Node) initiateLeaderElection() {
	n.Logger.Log("ELECTION", fmt.Sprintf("Starting election at %v", time.Now()), n.ID)
	n.Logger.Log("DEBUG", fmt.Sprintf("initiateLeaderElection called - current state: isLeader=%v highestBallotSeen=%v",
		n.IsLeader, n.HighestBallotSeen), n.ID)
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.LeaderTimer != nil && n.IsActive && !n.IsLeader {
		n.Logger.Log("DEBUG", "Stopping existing leader timer before starting election", n.ID)
		n.LeaderTimer.Stop()
		n.Logger.Log("TIMER", "Timer Stopped after initiating leader Election, Election timer started", n.ID)
	}

	requiredPromises := n.Config.F + 1
	n.Logger.Log("ELECTION", fmt.Sprintf("Initiating leader election (need %d promises, F=%d)", requiredPromises, n.Config.F), n.ID)

	round := int32(1)
	if n.HighestBallotSeen != nil {
		round = n.HighestBallotSeen.Round + 1
	}

	n.ProposedBallotNumber = &proto.BallotNumber{
		Round:  round,
		NodeId: n.ID,
	}

	n.HighestBallotSeen = n.ProposedBallotNumber
	n.Logger.Log("DEBUG", fmt.Sprintf("Created new ballot: round=%d nodeId=%d", round, n.ID), n.ID)

	n.PromiseCount = 1
	n.PromiseAckNodes = make(map[int32]bool)
	n.PromiseResponded = make(map[int32]bool)
	n.PromiseAckNodes[n.ID] = true

	for k := range n.PromiseAcceptLog {
		delete(n.PromiseAcceptLog, k)
	}

	n.minExecutedSeqFromPromises = n.ExecutedSeq

	for _, entry := range n.AcceptLog {
		if existing, ok := n.PromiseAcceptLog[entry.AcceptSeq]; ok {
			if IsHigherBallot(entry.AcceptNum, existing.AcceptNum) {
				n.PromiseAcceptLog[entry.AcceptSeq] = entry
			}
		} else {
			n.PromiseAcceptLog[entry.AcceptSeq] = entry
		}
	}

	prepare := &proto.Prepare{
		Ballot: n.ProposedBallotNumber,
	}

	n.Logger.Log("ELECTION", fmt.Sprintf("Created ballot %d.%d, counting self as promise (count: %d/%d)",
		n.ProposedBallotNumber.Round, n.ProposedBallotNumber.NodeId, n.PromiseCount, requiredPromises), n.ID)

	n.Logger.Log("DEBUG", fmt.Sprintf("Broadcasting prepare message with ballot %d.%d to %d nodes",
		n.ProposedBallotNumber.Round, n.ProposedBallotNumber.NodeId, len(n.Config.Nodes)-1), n.ID)
	n.broadcastPrepare(prepare)
}

func (n *Node) updateExecutedSequence(sequenceNumber int32) {
	// DEBUG: Log sequence update attempt
	n.Logger.Log("DEBUG", fmt.Sprintf("updateExecutedSequence called for seq %d (current executed=%d)", sequenceNumber, n.ExecutedSeq), n.ID)

	if sequenceNumber == n.ExecutedSeq+1 {
		n.ExecutedSeq = sequenceNumber
		n.Logger.Log("SEQUENCE", fmt.Sprintf("Updated executed sequence to %d", sequenceNumber), n.ID)

		if n.DB != nil {
			if err := n.DB.SaveSystemState(n.ID, n.SequenceNum, n.ExecutedSeq, n.CommittedSeq); err != nil {
				n.Logger.Log("ERROR", fmt.Sprintf("Failed to persist system state: %v", err), n.ID)
			}
		}
	} else if sequenceNumber > n.ExecutedSeq+1 {
		n.Logger.Log("WARN", fmt.Sprintf("Out-of-order execution attempt: %d > %d+1", sequenceNumber, n.ExecutedSeq), n.ID)

	}
}

func (n *Node) getAccountBalances(senderAccount, receiverAccount *common.Account) (senderBalance, receiverBalance int32) {
	n.Logger.Log("DEBUG", "getAccountBalances called", n.ID)
	if senderAccount != nil {
		senderBalance = senderAccount.GetBalance()
	} else {
		senderBalance = -1
	}

	if receiverAccount != nil {
		receiverBalance = receiverAccount.GetBalance()
	} else {
		receiverBalance = -1
	}

	return senderBalance, receiverBalance
}

func (n *Node) handleTransactionFailure(sequenceNumber int32, request *proto.Request, errorMsg string, senderAccount, receiverAccount *common.Account) {
	n.Logger.Log("DEBUG", "handleTransactionFailure called", n.ID)
	n.Logger.Log("ERROR", fmt.Sprintf("%s for transaction %d", errorMsg, sequenceNumber), n.ID)

	n.updateExecutedSequence(sequenceNumber)
	n.ExecutedTransactions[sequenceNumber] = true

	if transactionInfo, exists := n.TransactionStatus[sequenceNumber]; exists {
		transactionInfo.Status = common.Executed
		transactionInfo.Timestamp = time.Now()
	}

	if n.IsLeader {
		if originalRequest, exists := n.PendingClientReplies[sequenceNumber]; exists {
			senderBalance, receiverBalance := n.getAccountBalances(senderAccount, receiverAccount)
			go n.sendReplyToClient(originalRequest, false, errorMsg, request.Transaction, senderBalance, receiverBalance)
			delete(n.PendingClientReplies, sequenceNumber)
		}
	}

	go n.resetTimerOnExecution()

	go n.checkAndRetryWaitingTransactions()
}

func (n *Node) checkAndRetryWaitingTransactions() {
	n.mu.Lock()

	// DEBUG: Log retry attempt with detailed state
	n.Logger.Log("DEBUG", fmt.Sprintf("checkAndRetryWaitingTransactions: executed=%d, committed=%d", n.ExecutedSeq, n.CommittedSeq), n.ID)
	n.Logger.Log("RETRY", fmt.Sprintf("Retrying execution of transactions from sequence %d to %d", n.ExecutedSeq+1, n.CommittedSeq), n.ID)

	for seq := n.ExecutedSeq + 1; seq <= n.CommittedSeq; seq++ {
		n.Logger.Log("DEBUG", fmt.Sprintf("Checking sequence %d: executed_tx=%v", seq, n.ExecutedTransactions[seq]), n.ID)

		if !n.ExecutedTransactions[seq] {
			if request, exists := n.AcceptedTransactions[seq]; exists {
				n.Logger.Log("DEBUG", fmt.Sprintf("Seq %d: Found accepted transaction %v", seq, request != nil), n.ID)

				if transactionInfo, exists := n.TransactionStatus[seq]; exists {
					n.Logger.Log("DEBUG", fmt.Sprintf("Seq %d: Status=%s", seq, transactionInfo.Status.String()), n.ID)

					if transactionInfo.Status == common.Committed {
						n.Logger.Log("RETRY", fmt.Sprintf("Retrying execution of sequence %d", seq), n.ID)

						go n.executeTransaction(seq, request)
						break
					} else {
						n.Logger.Log("WAIT", fmt.Sprintf("Sequence %d not yet committed (status: %s)", seq, transactionInfo.Status), n.ID)
					}
				} else {
					n.Logger.Log("WAIT", fmt.Sprintf("No transaction status found for sequence %d", seq), n.ID)
				}
			} else {

				n.Logger.Log("WARN", fmt.Sprintf("Seq %d missing locally while committed through %d; waiting for recovery",
					seq, n.CommittedSeq), n.ID)
				missingSeq := seq
				n.mu.Unlock()
				n.recoveryMu.Lock()
				recoveryInFlight := n.recoveryInFlight
				n.recoveryMu.Unlock()
				if n.IsNodeActive() && !recoveryInFlight {
					go n.simpleRecovery()
					return
				}
				n.Logger.Log("DEBUG", fmt.Sprintf("Seq %d: No accepted transaction found", missingSeq), n.ID)
				return
			}
		} else {
			n.Logger.Log("DEBUG", fmt.Sprintf("Seq %d: Already executed, skipping", seq), n.ID)
		}
	}
	n.mu.Unlock()
}

func (n *Node) executeTransaction(sequenceNumber int32, request *proto.Request) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// DEBUG: Log execution attempt
	n.Logger.Log("DEBUG", fmt.Sprintf("executeTransaction called for seq %d (current executed=%d, committed=%d, executed_tx=%v)",
		sequenceNumber, n.ExecutedSeq, n.CommittedSeq, n.ExecutedTransactions[sequenceNumber]), n.ID)

	if sequenceNumber <= n.ExecutedSeq {
		n.Logger.Log("EXECUTE", fmt.Sprintf("Skipping already executed/ checkpointed seq %d (executed=%d)", sequenceNumber, n.ExecutedSeq), n.ID)
		return
	}

	if n.ExecutedTransactions[sequenceNumber] {
		n.Logger.Log("EXECUTE", fmt.Sprintf("Transaction %d already executed", sequenceNumber), n.ID)
		return
	}

	if sequenceNumber > n.ExecutedSeq+1 {
		n.Logger.Log("WAIT", fmt.Sprintf("Waiting to execute transaction %d (executed: %d)",
			sequenceNumber, n.ExecutedSeq), n.ID)
		go n.checkAndRetryWaitingTransactions()
		return
	}

	if request.Transaction.Sender == "no-op" || request.Transaction.Receiver == "no-op" {
		n.Logger.Log("NOOP", fmt.Sprintf("Executing no-op for sequence %d", sequenceNumber), n.ID)
		n.updateExecutedSequence(sequenceNumber)
		n.ExecutedTransactions[sequenceNumber] = true

		if transactionInfo, exists := n.TransactionStatus[sequenceNumber]; exists {
			transactionInfo.Status = common.Executed
			transactionInfo.Timestamp = time.Now()
		}

		go n.resetTimerOnExecution()

		go n.checkAndRetryWaitingTransactions()
		return
	}

	transaction := request.Transaction
	senderAccount := n.Accounts[transaction.Sender]
	receiverAccount := n.Accounts[transaction.Receiver]

	// DEBUG: Log account validation
	n.Logger.Log("DEBUG", fmt.Sprintf("Seq %d: Validating accounts - sender=%s (exists=%v), receiver=%s (exists=%v)",
		sequenceNumber, transaction.Sender, senderAccount != nil, transaction.Receiver, receiverAccount != nil), n.ID)

	if senderAccount != nil && receiverAccount != nil {
		n.Logger.Log("DEBUG", fmt.Sprintf("Seq %d: Balances - sender=%s:$%d, receiver=%s:$%d, amount=$%d",
			sequenceNumber, transaction.Sender, senderAccount.GetBalance(), transaction.Receiver, receiverAccount.GetBalance(), transaction.Amount), n.ID)
	}

	if senderAccount == nil || receiverAccount == nil {
		n.Logger.Log("DEBUG", fmt.Sprintf("Seq %d: Account validation failed - calling handleTransactionFailure", sequenceNumber), n.ID)
		n.handleTransactionFailure(sequenceNumber, request, "Invalid accounts", senderAccount, receiverAccount)
		return
	}

	if !senderAccount.CanTransfer(transaction.Amount) {
		n.Logger.Log("DEBUG", fmt.Sprintf("Seq %d: Insufficient balance - sender has $%d, needs $%d", sequenceNumber, senderAccount.GetBalance(), transaction.Amount), n.ID)
		n.handleTransactionFailure(sequenceNumber, request, "Insufficient balance", senderAccount, receiverAccount)
		return
	}

	// DEBUG: Log successful validation
	n.Logger.Log("DEBUG", fmt.Sprintf("Seq %d: Validation passed, proceeding with execution", sequenceNumber), n.ID)

	senderAccount.UpdateBalance(-transaction.Amount)
	receiverAccount.UpdateBalance(transaction.Amount)

	// DEBUG: Log balance updates
	n.Logger.Log("DEBUG", fmt.Sprintf("Seq %d: Updated balances - sender=%s:$%d, receiver=%s:$%d",
		sequenceNumber, transaction.Sender, senderAccount.GetBalance(), transaction.Receiver, receiverAccount.GetBalance()), n.ID)

	if n.DB != nil {
		if err := n.DB.UpdateAccount(transaction.Sender, senderAccount.GetBalance()); err != nil {
			n.Logger.Log("ERROR", fmt.Sprintf("Failed to persist sender balance: %v", err), n.ID)
		}
		if err := n.DB.UpdateAccount(transaction.Receiver, receiverAccount.GetBalance()); err != nil {
			n.Logger.Log("ERROR", fmt.Sprintf("Failed to persist receiver balance: %v", err), n.ID)
		}

		if err := n.DB.LogTransaction(sequenceNumber, transaction.Sender, transaction.Receiver,
			transaction.Amount, "EXECUTED", time.Now()); err != nil {
			n.Logger.Log("ERROR", fmt.Sprintf("Failed to log transaction: %v", err), n.ID)
		}
	}

	n.updateExecutedSequence(sequenceNumber)
	n.ExecutedTransactions[sequenceNumber] = true

	if transactionInfo, exists := n.TransactionStatus[sequenceNumber]; exists {
		transactionInfo.Status = common.Executed
		transactionInfo.Timestamp = time.Now()
	}

	n.Logger.Log("EXECUTE", fmt.Sprintf("Executed transaction %d: %s->%s $%d",
		sequenceNumber, transaction.Sender, transaction.Receiver, transaction.Amount), n.ID)

	if n.IsLeader {
		if originalRequest, exists := n.PendingClientReplies[sequenceNumber]; exists {
			go n.sendReplyToClient(originalRequest, true, "Transaction executed successfully", transaction, senderAccount.GetBalance(), receiverAccount.GetBalance())
			delete(n.PendingClientReplies, sequenceNumber)
		}
	}

	go n.resetTimerOnExecution()
	go n.checkAndRetryWaitingTransactions()
	seqNow := n.ExecutedSeq
	period := n.CheckpointPeriod
	if period > 0 && seqNow > 0 && seqNow%period == 0 {
		go n.emitCheckpoint(seqNow)
	}
}

func (n *Node) isWaitingToExecute() bool {
	n.Logger.Log("DEBUG", "isWaitingToExecute called", n.ID)
	n.mu.RLock()
	defer n.mu.RUnlock()

	if len(n.PendingRequests) > 0 {
		return true
	}
	nextSeq := n.ExecutedSeq + 1

	if _, exists := n.AcceptedTransactions[nextSeq]; exists {
		if !n.ExecutedTransactions[nextSeq] {
			return true
		}
	}

	for seq := n.ExecutedSeq + 1; seq <= n.CommittedSeq; seq++ {
		if !n.ExecutedTransactions[seq] {
			return true
		}
	}

	return false
}

func (n *Node) startTimerOnAccept() {
	n.Logger.Log("DEBUG", "startTimerOnAccept called", n.ID)

	n.mu.RLock()
	isLeader := n.IsLeader
	t := n.LeaderTimer
	nodeID := n.ID
	n.mu.RUnlock()

	if isLeader || t == nil {
		return
	}

	if !t.IsRunning() {
		n.Logger.Log("TIMER", "Starting election timer - received Accept message", nodeID)
		t.Start()
	} else {
		n.Logger.Log("TIMER", "Timer already running - ignoring Accept message", nodeID)
	}
}

func (n *Node) resetTimerOnExecution() {
	n.Logger.Log("DEBUG", "resetTimerOnExecution called", n.ID)

	n.mu.RLock()
	isLeader := n.IsLeader
	t := n.LeaderTimer
	nodeID := n.ID
	n.mu.RUnlock()

	if isLeader || t == nil {
		return
	}

	shouldStart := n.isWaitingToExecute()

	if shouldStart {
		n.Logger.Log("TIMER", "Resetting election timer - more requests pending", nodeID)
		t.Start()
	} else {
		n.Logger.Log("TIMER", "Stopping election timer - no more requests pending", nodeID)
		t.Stop()
	}
}

func (n *Node) resetTimerIfRunning() {
	n.Logger.Log("DEBUG", "resetTimerIfRunning called", n.ID)

	n.mu.RLock()
	isLeader := n.IsLeader
	t := n.LeaderTimer
	nodeID := n.ID
	n.mu.RUnlock()

	if t == nil {
		return
	}
	if isLeader {
		t.Stop()
		return
	}
	if t.IsRunning() {
		n.Logger.Log("TIMER", "Resetting election timer - more requests pending", nodeID)
		t.Start()
	}
}

func (n *Node) stopLeaderTimer() {
	n.Logger.Log("DEBUG", "stopLeaderTimer called", n.ID)

	n.mu.RLock()
	t := n.LeaderTimer
	nodeID := n.ID
	n.mu.RUnlock()

	if t == nil {
		return
	}
	if t.IsRunning() {
		t.Stop()
		n.Logger.Log("TIMER", "Stopping election timer - received NEW-VIEW", nodeID)
	}
}

func (n *Node) restartTimerIfNeeded() {
	n.Logger.Log("DEBUG", "restartTimerIfNeeded called", n.ID)
	n.mu.Lock()
	isActive := n.IsActive
	isLeader := n.IsLeader
	currentTimer := n.LeaderTimer
	n.mu.Unlock()

	if !isActive || isLeader || currentTimer != nil && currentTimer.IsRunning() {
		return
	}

	minTimeout := 900 * time.Millisecond
	maxTimeout := 1500 * time.Millisecond
	timer := common.NewRandomizedTimerWithLogging(minTimeout, maxTimeout, n.handleLeaderTimeout, n.Logger, n.ID)

	n.mu.Lock()
	if n.LeaderTimer == nil {
		n.LeaderTimer = timer
		currentTimer = n.LeaderTimer
	} else {
		currentTimer = n.LeaderTimer
	}
	if !currentTimer.IsRunning() && n.IsActive && !n.IsLeader {
		currentTimer.Start()
		n.Logger.Log("TIMER", fmt.Sprintf("Restarted randomized timer after receiving message (recovery) with range %v - %v", minTimeout, maxTimeout), n.ID)
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()
}

func (n *Node) startTimerIfNotRunning() {
	n.Logger.Log("DEBUG", "startTimerIfNotRunning called", n.ID)
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.IsActive || n.IsLeader {
		return
	}

	hasPendingWork := len(n.PendingRequests) > 0

	if !hasPendingWork {

		hasPendingWork = n.ExecutedSeq < n.CommittedSeq
	}

	if !hasPendingWork {
		return
	}

	if n.LeaderTimer != nil && !n.LeaderTimer.IsRunning() {
		n.Logger.Log("TIMER", "Starting election timer - have pending work", n.ID)
		n.LeaderTimer.Start()
	} else if n.LeaderTimer == nil {

		minTimeout := 700 * time.Millisecond
		maxTimeout := 2000 * time.Millisecond
		timer := common.NewRandomizedTimerWithLogging(minTimeout, maxTimeout, n.handleLeaderTimeout, n.Logger, n.ID)
		n.LeaderTimer = timer
		timer.Start()
		n.Logger.Log("TIMER", fmt.Sprintf("Created and started timer with range %v - %v", minTimeout, maxTimeout), n.ID)
	}
}

func (n *Node) processPrepareMessageDirectly(prepare *proto.Prepare) {
	n.Logger.Log("DEBUG", "processPrepareMessageDirectly called", n.ID)
	n.mu.Lock()

	shouldStepDown := false
	var stepDownBallot *proto.BallotNumber
	shouldSendPromise := false

	// if n.NewViewReceived != nil {
	// 	key := fmt.Sprintf("%d.%d", prepare.Ballot.Round, prepare.Ballot.NodeId)
	// 	if n.NewViewReceived[key] {
	// 		n.Logger.Log("PREPARE", fmt.Sprintf("Ignoring prepare %s - NEW-VIEW already received", key), n.ID)
	// 		return
	// 	}
	// }

	n.Logger.Log("PREPARE", fmt.Sprintf("Processing prepare message %d.%d directly",
		prepare.Ballot.Round, prepare.Ballot.NodeId), n.ID)

	now := time.Now()
	n.PrepareCooldown.notePrepareSeen(now)

	n.ReceivedPrepares = append(n.ReceivedPrepares, &PrepareWithTimestamp{
		Prepare:   prepare,
		Timestamp: now,
	})
	var validPrepares []*PrepareWithTimestamp
	for _, prepareWithTime := range n.ReceivedPrepares {
		if now.Sub(prepareWithTime.Timestamp) <= n.PrepareCooldown.tp {
			validPrepares = append(validPrepares, prepareWithTime)
		}
	}
	n.ReceivedPrepares = validPrepares
	if IsHigherBallot(prepare.Ballot, n.HighestBallotSeen) {
		n.HighestBallotSeen = prepare.Ballot
		n.Logger.Log("STEPDOWN", "Stepping down as leader/candidate due to higher ballot", n.ID)
		if (n.ProposedBallotNumber != nil && !n.IsLeader) || n.IsLeader {
			shouldStepDown = true
			stepDownBallot = prepare.Ballot
		}
		shouldSendPromise = true
	}

	n.mu.Unlock()

	if shouldStepDown {
		n.stepDownLeader("Stepping down as leader due to higher ballot", stepDownBallot)
	}
	if shouldSendPromise {
		go n.sendPromiseToProposer(prepare)
	}
}

func (n *Node) sendPromiseToProposer(prepare *proto.Prepare) {
	n.Logger.Log("DEBUG", "sendPromiseToProposer called", n.ID)
	n.Logger.Log("PROMISE", fmt.Sprintf("Sending promise for prepare %d.%d",
		prepare.Ballot.Round, prepare.Ballot.NodeId), n.ID)

	promise := &proto.Promise{
		Ballot:                 prepare.Ballot,
		AcceptLog:              n.AcceptLog,
		SenderId:               n.ID,
		ExecutedSeq:            n.ExecutedSeq,
		LatestCheckpointSeq:    n.LastCheckpointSeq,
		LatestCheckpointDigest: n.LastCheckpointDigest,
	}

	conn, err := n.getConnection(prepare.Ballot.NodeId)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to proposer %d: %v", prepare.Ballot.NodeId, err), n.ID)
		return
	}

	client := proto.NewNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	md := metadata.Pairs("x-sender-id", fmt.Sprintf("%d", n.ID))
	ctx = metadata.NewOutgoingContext(ctx, md)
	defer cancel()

	_, err = client.HandlePromise(ctx, promise)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to send promise to proposer %d: %v", prepare.Ballot.NodeId, err), n.ID)
	}
}

func (n *Node) updateTransactionStatusLocked(sequenceNumber int32, request *proto.Request, status common.TransactionStatus, ballotNumber *proto.BallotNumber) {
	n.Logger.Log("DEBUG", "updateTransactionStatusLocked called", n.ID)

	if prev, exists := n.TransactionStatus[sequenceNumber]; exists {
		if status > prev.Status {

			transactionInfo := &common.TransactionInfo{
				SequenceNumber: sequenceNumber,
				Request:        request,
				Status:         status,
				BallotNumber:   ballotNumber,
				NodeID:         n.ID,
				Timestamp:      time.Now(),
			}
			n.TransactionStatus[sequenceNumber] = transactionInfo
			n.Logger.Log("STATUS", fmt.Sprintf("Updated transaction %d status to %s", sequenceNumber, status.String()), n.ID)
		} else {

			prev.BallotNumber = ballotNumber
			prev.Request = request
			prev.Timestamp = time.Now()
			n.Logger.Log("STATUS", fmt.Sprintf("Preserved transaction %d status %s (incoming %s)", sequenceNumber, prev.Status.String(), status.String()), n.ID)
		}
		return
	}

	transactionInfo := &common.TransactionInfo{
		SequenceNumber: sequenceNumber,
		Request:        request,
		Status:         status,
		BallotNumber:   ballotNumber,
		NodeID:         n.ID,
		Timestamp:      time.Now(),
	}
	n.TransactionStatus[sequenceNumber] = transactionInfo
	n.Logger.Log("STATUS", fmt.Sprintf("Updated transaction %d status to %s", sequenceNumber, status.String()), n.ID)
}

func (n *Node) broadcastPrepare(prepare *proto.Prepare) {
	n.Logger.Log("BROADCAST", fmt.Sprintf("Broadcasting prepare %d.%d", prepare.Ballot.Round, prepare.Ballot.NodeId), n.ID)

	for _, nodeInfo := range n.Config.Nodes {
		if nodeInfo.ID == n.ID {
			continue
		}

		go func(nodeInfo common.NodeInfo) {
			conn, err := n.getConnection(nodeInfo.ID)
			if err != nil {
				n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to node %d: %v", nodeInfo.ID, err), n.ID)
				return
			}

			client := proto.NewNodeServiceClient(conn)
			n.Logger.Log("ELECTION", fmt.Sprintf("Sending prepare %d.%d to node %d", prepare.Ballot.Round, prepare.Ballot.NodeId, nodeInfo.ID), n.ID)

			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				prepareAck, err := client.HandlePrepare(ctx, prepare)
				if err != nil {
					n.Logger.Log("ERROR", fmt.Sprintf("Failed to send prepare to node %d: %v", nodeInfo.ID, err), n.ID)
					return
				}

				select {
				case n.prepareAckChan <- &common.PrepareAckWithSender{SenderID: nodeInfo.ID, PrepareAck: prepareAck}:
				case <-ctx.Done():
					n.Logger.Log("ERROR", "Context cancelled while sending prepare ack message", n.ID)
				default:

					go n.processPrepareAckMessage(nodeInfo.ID, prepareAck)
				}
			}()
		}(nodeInfo)
	}

}

func (n *Node) processPromiseMessage(senderId int32, promise *proto.Promise) {
	n.Logger.Log("DEBUG", "processPromiseMessage called", n.ID)
	n.mu.Lock()
	shouldStepDown := false
	var stepDownBallot *proto.BallotNumber
	stepDownReason := ""

	defer func() {
		n.mu.Unlock()
		if shouldStepDown {
			n.stepDownLeader(stepDownReason, stepDownBallot)
		}
	}()

	if n.ProposedBallotNumber == nil || (n.IsLeader && n.PromiseCount >= n.Config.F+1) {
		n.Logger.Log("IGNORE", fmt.Sprintf("Ignoring promise from %d - not leader/candidate", senderId), n.ID)
		return
	}

	if promise == nil || promise.Ballot == nil {
		n.Logger.Log("IGNORE", fmt.Sprintf("Promise from %d missing ballot; ignoring", senderId), n.ID)
		return
	}

	if n.PromiseResponded[senderId] {
		return
	}
	n.PromiseResponded[senderId] = true

	if !IsEqualBallot(promise.Ballot, n.ProposedBallotNumber) {

		if IsHigherBallot(promise.Ballot, n.HighestBallotSeen) {

			n.HighestBallotSeen = promise.Ballot
			n.Logger.Log("REJECT", fmt.Sprintf("Higher-ballot NACK from %d; stepping down (their %d.%d > our %d.%d)",
				senderId, promise.Ballot.Round, promise.Ballot.NodeId, n.ProposedBallotNumber.Round, n.ProposedBallotNumber.NodeId), n.ID)

			shouldStepDown = true
			stepDownBallot = promise.Ballot
			stepDownReason = fmt.Sprintf("received promise from %d with higher ballot %d.%d", senderId, promise.Ballot.Round, promise.Ballot.NodeId)
			return
		}

		n.Logger.Log("IGNORE", fmt.Sprintf("Lower-ballot promise from %d; ignoring (their %d.%d < our %d.%d)",
			senderId, promise.Ballot.Round, promise.Ballot.NodeId, n.ProposedBallotNumber.Round, n.ProposedBallotNumber.NodeId), n.ID)
		return
	}

	if promise.ExecutedSeq < n.minExecutedSeqFromPromises {
		n.minExecutedSeqFromPromises = promise.ExecutedSeq
	}

	if promise.LatestCheckpointSeq > n.promiseMaxCheckpointSeq {
		n.promiseMaxCheckpointSeq = promise.LatestCheckpointSeq
		n.promiseMaxCheckpointDigest = promise.LatestCheckpointDigest
	}

	for _, entry := range promise.AcceptLog {
		seq := entry.AcceptSeq
		if existing, ok := n.PromiseAcceptLog[seq]; ok {
			if IsHigherBallot(entry.AcceptNum, existing.AcceptNum) {
				n.PromiseAcceptLog[seq] = entry
			}
		} else {
			n.PromiseAcceptLog[seq] = entry
		}
	}

	if !n.PromiseAckNodes[senderId] {
		n.PromiseAckNodes[senderId] = true
		n.PromiseCount++
	}

	n.Logger.Log("PROMISE", fmt.Sprintf("Received promise for ballot %d.%d (count: %d)",
		promise.Ballot.Round, promise.Ballot.NodeId, n.PromiseCount), n.ID)
	remaining := (n.Config.F + 1) - n.PromiseCount
	if remaining > 0 {
		n.Logger.Log("ELECTION", fmt.Sprintf("Waiting for %d additional promises to win election", remaining), n.ID)
	}

	if n.PromiseCount >= n.Config.F+1 {
		n.Logger.Log("LEADER", fmt.Sprintf("Became leader with ballot %d.%d (%d/%d promises)",
			n.ProposedBallotNumber.Round, n.ProposedBallotNumber.NodeId, n.PromiseCount, n.Config.F+1), n.ID)

		n.IsLeader = true
		n.NodeType = common.Leader
		n.LeaderID = n.ID

		if n.RecoveryState == RecoveryInProgress {
			n.RecoveryState = RecoveryCompleted
			n.Logger.Log("RECOVERY", "Recovery state changed to completed (won election)", n.ID)
		}

		if n.LeaderTimer != nil {
			n.LeaderTimer.Stop()
			n.Logger.Log("TIMER", "Stopped election timer - became leader", n.ID)
		}

		// Process any client work that accumulated while campaigning
		n.mu.Unlock()
		n.createNewViewMessage()
		n.processPendingRequests()
		n.mu.Lock()
		return
	}
}

func (n *Node) resetElectionTrackingLocked() {
	n.Logger.Log("DEBUG", "resetElectionTrackingLocked called", n.ID)
	n.PromiseAckNodes = make(map[int32]bool)
	n.PromiseResponded = make(map[int32]bool)
	n.PromiseCount = 0

	for k := range n.PromiseAcceptLog {
		delete(n.PromiseAcceptLog, k)
	}
}

func (n *Node) stepDownLeader(reason string, higherBallot *proto.BallotNumber) {
	n.Logger.Log("DEBUG", fmt.Sprintf("stepDownLeader called with reason: %s, higherBallot: %v", reason, higherBallot), n.ID)
	n.mu.Lock()

	if higherBallot != nil && (n.HighestBallotSeen == nil || IsHigherBallot(higherBallot, n.HighestBallotSeen)) {
		n.HighestBallotSeen = higherBallot
	}

	wasLeaderOrCandidate := n.IsLeader || n.ProposedBallotNumber != nil

	n.IsLeader = false
	n.NodeType = common.Backup
	n.LeaderID = 0
	n.ProposedBallotNumber = nil

	n.resetElectionTrackingLocked()
	n.mu.Unlock()

	if wasLeaderOrCandidate {
		n.Logger.Log("STEPDOWN", fmt.Sprintf("Stepped down to backup: %s", reason), n.ID)
	}

	n.restartTimerIfNeeded()
	n.startTimerIfNotRunning()
}

func (n *Node) processPendingRequests() {
	n.Logger.Log("DEBUG", "processPendingRequests called", n.ID)
	n.mu.Lock()
	pendingRequests := make([]*proto.Request, len(n.PendingRequests))
	copy(pendingRequests, n.PendingRequests)
	n.clearPendingRequestsLocked()
	n.mu.Unlock()

	n.Logger.Log("LEADER", fmt.Sprintf("Processing %d pending requests", len(pendingRequests)), n.ID)

	for _, request := range pendingRequests {
		n.handleRequest(request)
	}
}

func (n *Node) createNewViewMessage() {
	n.Logger.Log("NEWVIEW", "Creating NEW-VIEW message", n.ID)

	n.mu.Lock()
	if n.ProposedBallotNumber == nil {
		n.Logger.Log("NEWVIEW", "Aborting NEW-VIEW creation - candidacy was abandoned (ProposedBallotNumber is nil)", n.ID)
		n.mu.Unlock()
		return
	}

	ballotCopy := &proto.BallotNumber{
		Round:  n.ProposedBallotNumber.Round,
		NodeId: n.ProposedBallotNumber.NodeId,
	}
	checkpointSeq := n.promiseMaxCheckpointSeq
	checkpointDigest := n.promiseMaxCheckpointDigest
	n.mu.Unlock()

	acceptLog := n.aggregateAcceptLogFromPromises(ballotCopy)

	n.mu.Lock()
	for _, e := range acceptLog {
		n.AcceptedTransactions[e.AcceptSeq] = e.AcceptVal

		if n.AcceptedBy[e.AcceptSeq] == nil {
			n.AcceptedBy[e.AcceptSeq] = make(map[int32]bool)
		}
		n.AcceptedBy[e.AcceptSeq][n.ID] = true

		n.updateTransactionStatusLocked(e.AcceptSeq, e.AcceptVal, common.Accepted, ballotCopy)

		// Mark aggregated accept-log entries as already accepted so pending requests get deduplicated.
		if e.AcceptVal != nil {
			n.rememberRequestLocked(e.AcceptVal, e.AcceptSeq)
		}
	}
	n.mu.Unlock()

	newView := &proto.NewView{
		Ballot:               ballotCopy,
		AcceptLog:            acceptLog,
		BaseCheckpointSeq:    checkpointSeq,
		BaseCheckpointDigest: checkpointDigest,
	}

	n.mu.Lock()
	n.NewViewLog = append(n.NewViewLog, newView)
	n.mu.Unlock()

	n.broadcastNewView(newView)

	var maxSeq int32 = 0
	for _, e := range acceptLog {
		if e.AcceptSeq > maxSeq {
			maxSeq = e.AcceptSeq
		}
	}

	if n.CommittedSeq > maxSeq {
		maxSeq = n.CommittedSeq
	}
	if n.ExecutedSeq > maxSeq {
		maxSeq = n.ExecutedSeq
	}

	n.mu.Lock()
	if n.SequenceNum <= maxSeq {
		n.SequenceNum = maxSeq + 1
		n.Logger.Log("NEWVIEW", fmt.Sprintf("Adjusted SequenceNum to %d after NEW-VIEW (max seen: %d)", n.SequenceNum, maxSeq), n.ID)
	}
	n.mu.Unlock()

	n.mu.Lock()
	for k := range n.PromiseAcceptLog {
		delete(n.PromiseAcceptLog, k)
	}
	n.PromiseAckNodes = make(map[int32]bool)
	n.PromiseResponded = make(map[int32]bool)
	n.PromiseCount = 0
	n.mu.Unlock()

	n.Logger.Log("NEWVIEW", fmt.Sprintf("Per-election state cleared (PromiseAcceptLog=%d, AckNodes=%d, Responded=%d, PromiseCount=%d)",
		len(n.PromiseAcceptLog), len(n.PromiseAckNodes), len(n.PromiseResponded), n.PromiseCount), n.ID)

	go func() {
		time.Sleep(100 * time.Millisecond)
		n.processPendingRequests()
	}()
}

func (n *Node) aggregateAcceptLogFromPromises(ballot *proto.BallotNumber) []*proto.AcceptLogEntry {
	n.Logger.Log("DEBUG", "aggregateAcceptLogFromPromises called", n.ID)
	n.mu.RLock()
	defer n.mu.RUnlock()

	minExecutedSeq := n.minExecutedSeqFromPromises
	n.Logger.Log("NEWVIEW", fmt.Sprintf("Minimum ExecutedSeq from promises: %d", minExecutedSeq), n.ID)

	var maxSeq int32 = minExecutedSeq
	for seq := range n.PromiseAcceptLog {
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	n.Logger.Log("NEWVIEW", fmt.Sprintf("Aggregating from ExecutedSeq %d to maxSeq %d", minExecutedSeq, maxSeq), n.ID)

	var aggregated []*proto.AcceptLogEntry
	for seq := minExecutedSeq + 1; seq <= maxSeq; seq++ {
		if entry, ok := n.PromiseAcceptLog[seq]; ok {

			aggregated = append(aggregated, &proto.AcceptLogEntry{
				AcceptNum: ballot,
				AcceptSeq: seq,
				AcceptVal: entry.AcceptVal,
			})
		} else {

			aggregated = append(aggregated, &proto.AcceptLogEntry{
				AcceptNum: ballot,
				AcceptSeq: seq,
				AcceptVal: &proto.Request{ClientId: "no-op", Timestamp: time.Now().UnixNano(), Transaction: &proto.Transaction{Sender: "no-op", Receiver: "no-op", Amount: 0}},
			})
		}
	}

	n.Logger.Log("NEWVIEW", fmt.Sprintf("Aggregated AcceptLog from promises: %d entries (min executed: %d, max seq: %d)",
		len(aggregated), minExecutedSeq, maxSeq), n.ID)

	return aggregated
}

func (n *Node) broadcastNewView(newView *proto.NewView) {
	n.Logger.Log("BROADCAST", "Broadcasting NEW-VIEW message", n.ID)

	for _, nodeInfo := range n.Config.Nodes {
		if nodeInfo.ID == n.ID {
			continue
		}

		go func(nodeInfo common.NodeInfo) {
			conn, err := n.getConnection(nodeInfo.ID)
			if err != nil {
				n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to node %d: %v", nodeInfo.ID, err), n.ID)
				return
			}

			client := proto.NewNodeServiceClient(conn)

			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				_, err := client.HandleNewView(ctx, newView)
				if err != nil {
					n.Logger.Log("ERROR", fmt.Sprintf("Failed to send NEW-VIEW to node %d: %v", nodeInfo.ID, err), n.ID)
				}
			}()
		}(nodeInfo)
	}

}

func (n *Node) broadcastCommit(sequence int32, request *proto.Request) {
	n.Logger.Log("BROADCAST", fmt.Sprintf("Broadcasting commit for sequence %d", sequence), n.ID)

	n.mu.Lock()
	if transactionInfo, exists := n.TransactionStatus[sequence]; exists {
		if transactionInfo.Status != common.Executed {
			n.updateTransactionStatusLocked(sequence, request, common.Committed, n.ProposedBallotNumber)
		}
	}

	n.AcceptedTransactions[sequence] = request

	if n.ProposedBallotNumber == nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Cannot broadcast commit for sequence %d - ProposedBallotNumber is nil", sequence), n.ID)
		n.mu.Unlock()
		return
	}

	ballotCopy := &proto.BallotNumber{
		Round:  n.ProposedBallotNumber.Round,
		NodeId: n.ProposedBallotNumber.NodeId,
	}
	n.mu.Unlock()

	commit := &proto.Commit{
		Ballot:   ballotCopy,
		Sequence: sequence,
		Request:  request,
	}

	for _, nodeInfo := range n.Config.Nodes {
		if nodeInfo.ID == n.ID {
			continue
		}

		go func(nodeInfo common.NodeInfo) {
			conn, err := n.getConnection(nodeInfo.ID)
			if err != nil {
				n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to node %d: %v", nodeInfo.ID, err), n.ID)
				return
			}

			client := proto.NewNodeServiceClient(conn)

			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				_, err := client.HandleCommit(ctx, commit)
				if err != nil {
					n.Logger.Log("ERROR", fmt.Sprintf("Failed to send commit to node %d: %v", nodeInfo.ID, err), n.ID)
				}
			}()
		}(nodeInfo)
	}
	n.Logger.Log("BROADCAST", fmt.Sprintf("Executing commit for sequence %d locally", sequence), n.ID)

	n.executeTransaction(sequence, request)
}

type accountKV struct {
	ClientID string `json:"client_id"`
	Balance  int32  `json:"balance"`
}

type clientTsKV struct {
	ClientID string `json:"client_id"`
	Ts       int64  `json:"ts"`
}

type snapshotData struct {
	ExecutedSeq      int32        `json:"executed_seq"`
	CommittedSeq     int32        `json:"committed_seq"`
	Accounts         []accountKV  `json:"accounts"`
	ClientTimestamps []clientTsKV `json:"client_timestamps"`
}

func (n *Node) serializeSnapshotLocked() ([]byte, []byte, error) {
	n.Logger.Log("DEBUG", "serializeSnapshotLocked called", n.ID)

	var accs []accountKV
	for id, acc := range n.Accounts {
		accs = append(accs, accountKV{ClientID: id, Balance: acc.GetBalance()})
	}
	sort.Slice(accs, func(i, j int) bool { return accs[i].ClientID < accs[j].ClientID })

	var tsList []clientTsKV
	for id, ts := range n.LastClientTimestamp {
		tsList = append(tsList, clientTsKV{ClientID: id, Ts: ts})
	}
	sort.Slice(tsList, func(i, j int) bool { return tsList[i].ClientID < tsList[j].ClientID })

	snap := snapshotData{
		ExecutedSeq:      n.ExecutedSeq,
		CommittedSeq:     n.CommittedSeq,
		Accounts:         accs,
		ClientTimestamps: tsList,
	}
	bytes, err := json.Marshal(snap)
	if err != nil {
		return nil, nil, err
	}
	sum := sha256.Sum256(bytes)
	digest := sum[:]
	return bytes, digest, nil
}

func (n *Node) deserializeSnapshot(bytes []byte) (*snapshotData, error) {
	n.Logger.Log("DEBUG", "deserializeSnapshot called", n.ID)
	var snap snapshotData
	if err := json.Unmarshal(bytes, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

func (n *Node) installCheckpointStateLocked(seq int32, state []byte, digest []byte, ballot *proto.BallotNumber) error {
	n.Logger.Log("DEBUG", "installCheckpointStateLocked called", n.ID)

	sum := sha256.Sum256(state)
	if digest != nil && hex.EncodeToString(sum[:]) != hex.EncodeToString(digest) {
		return fmt.Errorf("checkpoint digest mismatch")
	}
	snap, err := n.deserializeSnapshot(state)
	if err != nil {
		return err
	}

	for _, kv := range snap.Accounts {
		if acct, ok := n.Accounts[kv.ClientID]; ok {
			acct.UpdateBalance(kv.Balance - acct.GetBalance())
		} else {
			n.Accounts[kv.ClientID] = &common.Account{ClientID: kv.ClientID, Balance: kv.Balance}
		}
		if n.DB != nil {
			_ = n.DB.UpdateAccount(kv.ClientID, kv.Balance)
		}
	}

	n.LastClientTimestamp = make(map[string]int64)
	for _, kv := range snap.ClientTimestamps {
		n.LastClientTimestamp[kv.ClientID] = kv.Ts
	}

	oldExecutedSeq := n.ExecutedSeq
	n.ExecutedSeq = snap.ExecutedSeq

	for seq := oldExecutedSeq + 1; seq <= n.ExecutedSeq; seq++ {
		n.ExecutedTransactions[seq] = true
		n.updateTransactionStatusLocked(seq, nil, common.Executed, ballot)
		n.Logger.Log("DEBUG", fmt.Sprintf("Marked sequence %d as executed (checkpoint)", seq), n.ID)
	}

	if snap.CommittedSeq > n.CommittedSeq {
		n.CommittedSeq = snap.CommittedSeq
	}
	n.LastCheckpointSeq = seq
	n.LastCheckpointState = state
	n.LastCheckpointDigest = digest

	n.pruneLogsLocked(seq)
	if n.DB != nil {
		_ = n.DB.SaveSystemState(n.ID, n.SequenceNum, n.ExecutedSeq, n.CommittedSeq)
	}
	return nil
}

func (n *Node) pruneLogsLocked(seq int32) {
	n.Logger.Log("DEBUG", "pruneLogsLocked called", n.ID)

	var kept []*proto.AcceptLogEntry
	for _, e := range n.AcceptLog {
		if e.AcceptSeq > seq {
			kept = append(kept, e)
		}
	}
	n.AcceptLog = kept

	for s := range n.AcceptedTransactions {
		if s > seq {
			delete(n.AcceptedTransactions, s)
		}
	}
	for s := range n.ExecutedTransactions {
		if s > seq {
			delete(n.ExecutedTransactions, s)
		}
	}
	for s := range n.TransactionStatus {
		if s > seq {
			delete(n.TransactionStatus, s)
		}
	}
	for s := range n.CommittedSet {
		if s > seq {
			delete(n.CommittedSet, s)
		}
	}
	for s := range n.CommitSent {
		if s > seq {
			delete(n.CommitSent, s)
		}
	}
	for s := range n.AcceptedBy {
		if s > seq {
			delete(n.AcceptedBy, s)
		}
	}
}

func (n *Node) emitCheckpoint(seq int32) {
	n.Logger.Log("DEBUG", "emitCheckpoint called", n.ID)
	n.mu.Lock()
	state, digest, err := n.serializeSnapshotLocked()
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to serialize checkpoint: %v", err), n.ID)
		n.mu.Unlock()
		return
	}
	n.LastCheckpointSeq = seq
	n.LastCheckpointDigest = digest
	n.LastCheckpointState = state
	n.mu.Unlock()
}

func (n *Node) ensureCheckpointInstalled(seq int32, digest []byte, leaderID int32, ballot *proto.BallotNumber) {
	n.Logger.Log("DEBUG", "ensureCheckpointInstalled called", n.ID)
	sources := n.selectCheckpointSources(leaderID)
	for _, src := range sources {
		if src == n.ID {
			continue
		}
		if n.tryFetchAndInstallCheckpoint(src, seq, digest, ballot) {
			n.mu.Lock()
			n.CheckpointFetchLog = append(n.CheckpointFetchLog, CheckpointFetchEvent{Seq: seq, FromNode: src, Success: true, ErrorMsg: "", Timestamp: time.Now()})
			n.mu.Unlock()
			n.Logger.Log("CHECKPOINT", fmt.Sprintf("Installed checkpoint %d from node %d", seq, src), n.ID)
			return
		}
		n.mu.Lock()
		n.CheckpointFetchLog = append(n.CheckpointFetchLog, CheckpointFetchEvent{Seq: seq, FromNode: src, Success: false, ErrorMsg: "fetch_or_verify_failed", Timestamp: time.Now()})
		n.mu.Unlock()
	}
	n.Logger.Log("ERROR", fmt.Sprintf("Failed to install checkpoint %d from any peer", seq), n.ID)
}

func (n *Node) selectCheckpointSources(leaderID int32) []int32 {
	n.Logger.Log("DEBUG", "selectCheckpointSources called", n.ID)

	active := n.listActivePeerIDs()
	sort.Slice(active, func(i, j int) bool { return active[i] < active[j] })
	var sources []int32
	if leaderID > 0 {
		sources = append(sources, leaderID)
	}
	for _, id := range active {
		if id != leaderID {
			sources = append(sources, id)
		}
	}
	return sources
}

func (n *Node) tryFetchAndInstallCheckpoint(src int32, seq int32, digest []byte, ballot *proto.BallotNumber) bool {
	n.Logger.Log("DEBUG", "tryFetchAndInstallCheckpoint called", n.ID)
	conn, err := n.getConnection(src)
	if err != nil {
		return false
	}
	client := proto.NewNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	snap, err := client.RequestCheckpoint(ctx, &proto.CheckpointRequest{Seq: seq, Digest: digest})
	if err != nil || snap == nil || len(snap.State) == 0 {
		return false
	}

	n.mu.Lock()
	err = n.installCheckpointStateLocked(seq, snap.State, snap.Digest, ballot)
	n.mu.Unlock()
	return err == nil
}

func (n *Node) sendReplyToClient(originalRequest *proto.Request, success bool, message string, transaction *proto.Transaction, senderBalance, receiverBalance int32) {
	n.Logger.Log("DEBUG", "sendReplyToClient called", n.ID)
	n.Logger.Log("REPLY", fmt.Sprintf("Sending reply to client %s: %s", originalRequest.ClientId, message), n.ID)

	reply := &proto.Reply{
		Ballot:    n.ProposedBallotNumber,
		Timestamp: originalRequest.Timestamp,
		ClientId:  originalRequest.ClientId,
		Result:    success,
		Message:   message,
	}

	n.mu.Lock()
	n.LastClientReply[originalRequest.ClientId] = reply
	n.mu.Unlock()

	conn, err := n.getClientConnection(originalRequest.ClientId)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to client %s: %v", originalRequest.ClientId, err), n.ID)
		return
	}

	client := proto.NewClientServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.SendReply(ctx, reply)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to send reply to client %s: %v", originalRequest.ClientId, err), n.ID)
		return
	}

	n.Logger.Log("REPLY", fmt.Sprintf("Successfully sent reply to client %s: Result=%v, Message=%s",
		originalRequest.ClientId, success, message), n.ID)
}

func (n *Node) createAcceptLogFromLeaderState(fromSeq int32) []*proto.AcceptLogEntry {
	n.Logger.Log("DEBUG", "createAcceptLogFromLeaderState called", n.ID)

	highestAccepted := fromSeq
	for seq := range n.AcceptedTransactions {
		if seq > highestAccepted {
			highestAccepted = seq
		}
	}

	n.Logger.Log("RECOVERY", fmt.Sprintf("Creating AcceptLog from leader state (fromSeq: %d, HighestAccepted: %d)", fromSeq, highestAccepted), n.ID)

	var acceptLog []*proto.AcceptLogEntry
	var stableCheckpoint = n.LastCheckpointSeq
	for seq := stableCheckpoint + 1; seq <= highestAccepted; seq++ {
		if request, exists := n.AcceptedTransactions[seq]; exists {

			acceptLog = append(acceptLog, &proto.AcceptLogEntry{
				AcceptNum: n.ProposedBallotNumber,
				AcceptSeq: seq,
				AcceptVal: request,
			})
		} else {

			acceptLog = append(acceptLog, &proto.AcceptLogEntry{
				AcceptNum: n.ProposedBallotNumber,
				AcceptSeq: seq,
				AcceptVal: &proto.Request{
					ClientId:  "no-op",
					Timestamp: time.Now().UnixNano(),
					Transaction: &proto.Transaction{
						Sender:   "no-op",
						Receiver: "no-op",
						Amount:   0,
					},
				},
			})
		}
	}

	n.Logger.Log("RECOVERY", fmt.Sprintf("Created AcceptLog with %d entries from leader state", len(acceptLog)), n.ID)
	return acceptLog
}

func (n *Node) simpleRecovery() {
	n.Logger.Log("DEBUG", "simpleRecovery called", n.ID)
	startTime := time.Now()

	n.recoveryMu.Lock()
	if n.recoveryInFlight {
		n.recoveryMu.Unlock()
		n.Logger.Log("RECOVERY", "Recovery already in progress, skipping duplicate invocation", n.ID)
		return
	}
	n.recoveryInFlight = true
	n.recoveryMu.Unlock()

	defer func() {
		n.recoveryMu.Lock()
		n.recoveryInFlight = false
		n.recoveryMu.Unlock()
		n.Logger.Log("RECOVERY", fmt.Sprintf("simpleRecovery exiting after %s (final_state=%s)", time.Since(startTime), n.GetRecoveryState().String()), n.ID)
	}()

	n.Logger.Log("RECOVERY", "Node activated - starting simple recovery", n.ID)

	n.mu.Lock()

	if !n.IsActive {
		n.mu.Unlock()
		n.Logger.Log("RECOVERY", "Node deactivated during recovery setup, aborting", n.ID)
		return
	}
	if n.RecoveryState != RecoveryInProgress {
		n.RecoveryState = RecoveryInProgress
	}
	n.mu.Unlock()
	n.Logger.Log("RECOVERY", "Recovery state changed to in_progress", n.ID)

	time.Sleep(500 * time.Millisecond)
	n.Logger.Log("RECOVERY", fmt.Sprintf("simpleRecovery: leader discovery after %s", time.Since(startTime)), n.ID)

	leaderID := n.discoverLeaderFromActiveQuorum()
	if leaderID == 0 {
		n.Logger.Log("RECOVERY", fmt.Sprintf("simpleRecovery: unable to discover leader (elapsed %s)", time.Since(startTime)), n.ID)

		n.mu.Lock()
		n.RecoveryState = RecoveryFailed
		isActive := n.IsActive
		isLeader := n.IsLeader
		timer := n.LeaderTimer
		n.mu.Unlock()

		// Clear any stale leadership if discovery failed.
		if isLeader {
			n.stepDownLeader("recovery failed to discover leader", n.HighestBallotSeen)
		}

		shouldStart := n.isWaitingToExecute()

		if timer == nil {
			n.restartTimerIfNeeded()
		} else if shouldStart && isActive && !isLeader {

			if n.IsNodeActive() {
				n.Logger.Log("TIMER", "Ensuring election timer is active after recovery failure", n.ID)
				timer.Start()
			}
		}
		n.Logger.Log("RECOVERY", "Recovery state changed to failed", n.ID)
		n.Logger.Log("RECOVERY", "No leader found, will wait for messages or timer", n.ID)
		return
	}

	if n.requestNewViewFromLeader(leaderID) {
		n.Logger.Log("RECOVERY", "Recovery initiation completed, waiting for new-View Message", n.ID)
	} else {
		n.Logger.Log("RECOVERY", "Recovery failed, will wait for messages", n.ID)
	}
}

func (n *Node) discoverLeaderFromActiveQuorum() int32 {
	n.Logger.Log("RECOVERY", "Discovering leader from majority of ACTIVE nodes", n.ID)

	activePeers := n.listActivePeerIDs()
	n.Logger.Log("RECOVERY", fmt.Sprintf("Active peer candidates: %v", activePeers), n.ID)
	if len(activePeers) == 0 {
		n.Logger.Log("RECOVERY", "No active peers found for leader discovery", n.ID)
		return 0
	}

	nActiveIncludingSelf := len(activePeers) + 1
	fActive := (nActiveIncludingSelf - 1) / 2
	needed := fActive + 1
	n.Logger.Log("RECOVERY", fmt.Sprintf("Active nodes: %d (including self), need %d votes for quorum", nActiveIncludingSelf, needed), n.ID)

	leaderVotes := make(map[int32]int)
	noLeaderVotes := 0

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type leaderResult struct {
		peerID   int32
		leaderID int32
	}
	resultChan := make(chan leaderResult, len(activePeers))
	for _, pid := range activePeers {
		go func(peerID int32) {
			leaderID := n.queryNodeForLeaderWithContext(ctx, peerID)
			resultChan <- leaderResult{peerID: peerID, leaderID: leaderID}
		}(pid)
	}

	for i := 0; i < len(activePeers); i++ {
		select {
		case result := <-resultChan:
			if result.leaderID > 0 {
				leaderVotes[result.leaderID]++
				n.Logger.Log("RECOVERY", fmt.Sprintf("Node %d reports leader %d (votes: %d)", result.peerID, result.leaderID, leaderVotes[result.leaderID]), n.ID)
				if leaderVotes[result.leaderID] >= needed {
					n.Logger.Log("RECOVERY", fmt.Sprintf("Leader %d has ACTIVE quorum (%d/%d)", result.leaderID, leaderVotes[result.leaderID], needed), n.ID)
					return result.leaderID
				}
			} else {
				noLeaderVotes++
				n.Logger.Log("RECOVERY", fmt.Sprintf("Node %d reports no leader", result.peerID), n.ID)
			}
		case <-ctx.Done():
			n.Logger.Log("RECOVERY", "Leader discovery timeout, proceeding with collected votes", n.ID)
			i = len(activePeers)
		}
	}

	if noLeaderVotes >= needed {
		n.Logger.Log("RECOVERY", fmt.Sprintf("Majority (%d/%d) report no leader - election needed", noLeaderVotes, needed), n.ID)
	} else {
		n.Logger.Log("RECOVERY", "No leader has ACTIVE quorum consensus", n.ID)
	}
	return 0
}

func (n *Node) listActivePeerIDs() []int32 {
	n.Logger.Log("DEBUG", "listActivePeerIDs called", n.ID)
	var active []int32
	for _, nodeInfo := range n.Config.Nodes {
		if nodeInfo.ID == n.ID {
			continue
		}
		conn, err := n.getConnection(nodeInfo.ID)
		if err != nil {
			n.Logger.Log("RECOVERY", fmt.Sprintf("Skipping node %d during active peer discovery: %v", nodeInfo.ID, err), n.ID)
			continue
		}

		client := proto.NewPaxosServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		status, err := client.GetStatus(ctx, &proto.Empty{})
		cancel()
		if err != nil {
			n.Logger.Log("RECOVERY", fmt.Sprintf("GetStatus RPC failed for node %d: %v", nodeInfo.ID, err), n.ID)
			continue
		}
		if status != nil && status.Status == "running" {
			n.Logger.Log("RECOVERY", fmt.Sprintf("Node %d reported status=%s; adding to active list", nodeInfo.ID, status.Status), n.ID)
			active = append(active, nodeInfo.ID)
		}
		if status == nil {
			n.Logger.Log("RECOVERY", fmt.Sprintf("Node %d returned nil status during discovery", nodeInfo.ID), n.ID)
		} else if status.Status != "running" {
			n.Logger.Log("RECOVERY", fmt.Sprintf("Node %d status=%s (not running)", nodeInfo.ID, status.Status), n.ID)
		}
	}
	n.Logger.Log("RECOVERY", fmt.Sprintf("Active peers discovered: %v", active), n.ID)
	return active
}

func (n *Node) queryNodeForLeaderWithContext(ctx context.Context, nodeID int32) int32 {
	n.Logger.Log("DEBUG", "queryNodeForLeaderWithContext called", n.ID)
	conn, err := n.getConnection(nodeID)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to node %d for leader discovery: %v", nodeID, err), n.ID)
		return 0
	}

	client := proto.NewNodeServiceClient(conn)

	n.Logger.Log("RECOVERY", fmt.Sprintf("Requesting leader info from node %d", nodeID), n.ID)
	leaderInfo, err := client.GetLeader(ctx, &proto.Empty{})
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to get leader from node %d: %v", nodeID, err), n.ID)
		return 0
	}
	if leaderInfo == nil {
		n.Logger.Log("RECOVERY", fmt.Sprintf("Node %d responded to GetLeader with nil info", nodeID), n.ID)
		return 0
	}
	n.Logger.Log("RECOVERY", fmt.Sprintf("GetLeader response from node %d: leader=%d", nodeID, leaderInfo.NodeId), n.ID)

	if leaderInfo.NodeId > 0 {
		n.Logger.Log("RECOVERY", fmt.Sprintf("Node %d reports leader %d", nodeID, leaderInfo.NodeId), n.ID)
		return leaderInfo.NodeId
	}

	n.Logger.Log("RECOVERY", fmt.Sprintf("Node %d reports no leader", nodeID), n.ID)
	return 0
}

func (n *Node) requestNewViewFromLeader(leaderID int32) bool {
	n.Logger.Log("RECOVERY", fmt.Sprintf("Requesting NEW-VIEW from leader %d", leaderID), n.ID)

	conn, err := n.getConnection(leaderID)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to leader %d for recovery: %v", leaderID, err), n.ID)

		n.mu.Lock()
		n.RecoveryState = RecoveryFailed
		n.mu.Unlock()
		n.Logger.Log("RECOVERY", "Recovery state changed to failed", n.ID)
		return false
	}

	client := proto.NewNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	n.mu.RLock()
	fromExecuted := n.ExecutedSeq
	n.mu.RUnlock()
	md := metadata.Pairs("x-from-executed-seq", fmt.Sprintf("%d", fromExecuted))
	ctx = metadata.NewOutgoingContext(ctx, md)
	defer cancel()

	newView, err := client.RequestNewView(ctx, &proto.Empty{})
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to request NEW-VIEW from leader %d: %v", leaderID, err), n.ID)

		n.mu.Lock()
		n.RecoveryState = RecoveryFailed
		isActive := n.IsActive
		isLeader := n.IsLeader
		timer := n.LeaderTimer
		n.mu.Unlock()

		shouldStart := n.isWaitingToExecute()
		if shouldStart && timer != nil && isActive && !isLeader {
			n.Logger.Log("TIMER", "Starting election timer - recovery failed and pending work", n.ID)
			timer.Start()
		}
		n.Logger.Log("RECOVERY", "Recovery state changed to failed", n.ID)
		return false
	}

	if newView == nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Received nil NEW-VIEW from leader %d", leaderID), n.ID)

		n.mu.Lock()
		n.RecoveryState = RecoveryFailed
		isActive := n.IsActive
		isLeader := n.IsLeader
		timer := n.LeaderTimer
		n.mu.Unlock()

		shouldStart := n.isWaitingToExecute()
		if shouldStart && timer != nil && isActive && !isLeader {
			n.Logger.Log("TIMER", "Starting election timer - recovery failed and pending work", n.ID)
			timer.Start()
		}
		n.Logger.Log("RECOVERY", "Recovery state changed to failed", n.ID)
		return false
	}

	n.Logger.Log("RECOVERY", fmt.Sprintf("RequestNewView succeeded from leader %d (accept_entries=%d, checkpoint_seq=%d)",
		leaderID, len(newView.AcceptLog), newView.BaseCheckpointSeq), n.ID)
	if newView.Ballot == nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Received NEW-VIEW with nil ballot from leader %d", leaderID), n.ID)

		n.mu.Lock()
		n.RecoveryState = RecoveryFailed
		isActive := n.IsActive
		isLeader := n.IsLeader
		timer := n.LeaderTimer
		n.mu.Unlock()

		shouldStart := n.isWaitingToExecute()
		if shouldStart && timer != nil && isActive && !isLeader {
			n.Logger.Log("TIMER", "Starting election timer - recovery failed and pending work", n.ID)
			timer.Start()
		}
		n.Logger.Log("RECOVERY", "Recovery state changed to failed", n.ID)
		return false
	}

	n.processNewViewAsync(newView)
	n.Logger.Log("RECOVERY", fmt.Sprintf("Successfully received NEW-VIEW from leader %d", leaderID), n.ID)
	return true
}
