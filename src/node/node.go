package node

import (
	"context"
	"fmt"
	"paxos-banking/proto"
	"paxos-banking/src/common"
	"time"

	"google.golang.org/grpc/metadata"
)

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
	n.mu.Lock()
	restart := false

	sequence := accepted.Sequence

	if n.ProposedBallotNumber == nil || accepted.Ballot == nil || !IsEqualBallot(accepted.Ballot, n.ProposedBallotNumber) {

		if n.ProposedBallotNumber != nil && accepted.Ballot != nil && IsHigherBallot(accepted.Ballot, n.ProposedBallotNumber) {
			if IsHigherBallot(accepted.Ballot, n.HighestBallotSeen) {
				n.HighestBallotSeen = accepted.Ballot
			}

			n.resetElectionTrackingLocked()
			restart = true
		}
		n.Logger.Log("IGNORE", fmt.Sprintf("Ignoring accepted for sequence %d from node %d due to ballot mismatch", sequence, accepted.NodeId), n.ID)
		n.mu.Unlock()
		if restart {
			go n.initiateLeaderElection()
		}
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
			target := accepted.NodeId

			n.mu.Unlock()
			go n.sendCommitToNode(target, sequence, txn)
			return
		}
	}

	n.mu.Unlock()
}

func (n *Node) sendCommitToNode(targetNodeID int32, sequence int32, request *proto.Request) {
	if targetNodeID == 0 || targetNodeID == n.ID {
		return
	}
	conn, err := n.getConnection(targetNodeID)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to node %d for Commit: %v", targetNodeID, err), n.ID)
		return
	}
	client := proto.NewNodeServiceClient(conn)
	commit := &proto.Commit{Ballot: n.ProposedBallotNumber, Sequence: sequence, Request: request}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := client.HandleCommit(ctx, commit); err != nil {
			n.Logger.Log("ERROR", fmt.Sprintf("Failed to send commit to node %d: %v", targetNodeID, err), n.ID)
		}
	}()
}

func (n *Node) handleLeaderTimeout() {
	n.Logger.Log("TIMEOUT", "Leader timeout - checking for prepare messages", n.ID)

	if n.GetRecoveryState() == RecoveryInProgress {
		n.Logger.Log("TIMEOUT", "Skipping leader election - recovery in progress", n.ID)
		return
	}

	n.recoveryMu.Lock()
	recoveryOngoing := n.recoveryInFlight
	n.recoveryMu.Unlock()
	if recoveryOngoing {
		n.Logger.Log("TIMEOUT", "Skipping leader election - recovery operation in flight", n.ID)
		return
	}

	n.mu.Lock()
	now := time.Now()
	var highestPrepare *proto.Prepare
	var recentPrepare *proto.Prepare

	// First, look for prepare messages within the tp window (preferred)
	for _, prepareWithTime := range n.ReceivedPrepares {
		if now.Sub(prepareWithTime.Timestamp) <= n.PrepareCooldown.tp {
			if recentPrepare == nil || IsHigherBallot(prepareWithTime.Prepare.Ballot, recentPrepare.Ballot) {
				recentPrepare = prepareWithTime.Prepare
			}
		}
	}

	// If no recent prepare messages, look for ANY prepare message (fallback)
	if recentPrepare == nil {
		for _, prepareWithTime := range n.ReceivedPrepares {
			if highestPrepare == nil || IsHigherBallot(prepareWithTime.Prepare.Ballot, highestPrepare.Ballot) {
				highestPrepare = prepareWithTime.Prepare
			}
		}
	}

	// Clean up old prepare messages to prevent memory leaks
	var validPrepares []*PrepareWithTimestamp
	for _, prepareWithTime := range n.ReceivedPrepares {
		// Keep prepare messages that are either recent or the highest ballot
		if now.Sub(prepareWithTime.Timestamp) <= n.PrepareCooldown.tp ||
			(highestPrepare != nil && prepareWithTime.Prepare.Ballot.Round == highestPrepare.Ballot.Round &&
				prepareWithTime.Prepare.Ballot.NodeId == highestPrepare.Ballot.NodeId) {
			validPrepares = append(validPrepares, prepareWithTime)
		}
	}
	n.ReceivedPrepares = validPrepares
	n.mu.Unlock()

	// Process recent prepare message if available, otherwise process any prepare message
	if recentPrepare != nil {
		n.Logger.Log("TIMEOUT", fmt.Sprintf("Found prepare message %d.%d in last tp time, sending ACK",
			recentPrepare.Ballot.Round, recentPrepare.Ballot.NodeId), n.ID)
		n.processPrepareMessageDirectly(recentPrepare)
		return
	} else if highestPrepare != nil {
		n.Logger.Log("TIMEOUT", fmt.Sprintf("Found older prepare message %d.%d (outside tp window), processing anyway to prevent election deadlock",
			highestPrepare.Ballot.Round, highestPrepare.Ballot.NodeId), n.ID)
		n.processPrepareMessageDirectly(highestPrepare)
		return
	}

	n.Logger.Log("TIMEOUT", "No prepare messages in last tp time, starting leader election", n.ID)

	n.initiateLeaderElection()
}

func (n *Node) initiateLeaderElection() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.RecoveryState == RecoveryInProgress {
		n.Logger.Log("ELECTION", "Skipping leader election - recovery in progress", n.ID)
		return
	}

	n.recoveryMu.Lock()
	recoveryOngoing := n.recoveryInFlight
	n.recoveryMu.Unlock()
	if recoveryOngoing {
		n.Logger.Log("ELECTION", "Skipping leader election - recovery operation in flight", n.ID)
		return
	}

	now := time.Now()
	if !n.PrepareCooldown.canSendPrepare(now) {
		n.Logger.Log("ELECTION", "Skipping leader election - within anti-dueling cooldown period", n.ID)
		return
	}

	n.Logger.Log("ELECTION", "Initiating leader election", n.ID)

	round := int32(1)
	if n.HighestBallotSeen != nil {
		round = n.HighestBallotSeen.Round + 1
	}

	n.ProposedBallotNumber = &proto.BallotNumber{
		Round:  round,
		NodeId: n.ID,
	}

	n.HighestBallotSeen = n.ProposedBallotNumber

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

	n.Logger.Log("ELECTION", fmt.Sprintf("Created ballot %d.%d, counting self as promise (count: %d)",
		n.ProposedBallotNumber.Round, n.ProposedBallotNumber.NodeId, n.PromiseCount), n.ID)

	n.broadcastPrepare(prepare)
}

func (n *Node) updateExecutedSequence(sequenceNumber int32) {

	if sequenceNumber == n.ExecutedSeq+1 {
		n.ExecutedSeq = sequenceNumber
		n.Logger.Log("SEQUENCE", fmt.Sprintf("Updated executed sequence to %d", sequenceNumber), n.ID)
		if sequenceNumber == n.NewViewMaxSeq && n.RecoveryState == RecoveryInProgress {
			n.RecoveryState = RecoveryCompleted
			n.Logger.Log("RECOVERY", "Recovery state changed to completed", n.ID)
		}

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
	defer n.mu.Unlock()

	for seq := n.ExecutedSeq + 1; seq <= n.CommittedSeq; seq++ {
		if !n.ExecutedTransactions[seq] {
			if request, exists := n.AcceptedTransactions[seq]; exists {

				if transactionInfo, exists := n.TransactionStatus[seq]; exists {
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
			}
		}
	}
}

func (n *Node) executeTransaction(sequenceNumber int32, request *proto.Request) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.ExecutedTransactions[sequenceNumber] {
		n.Logger.Log("EXECUTE", fmt.Sprintf("Transaction %d already executed", sequenceNumber), n.ID)
		return
	}

	if sequenceNumber > n.ExecutedSeq+1 {
		n.Logger.Log("WAIT", fmt.Sprintf("Waiting to execute transaction %d (executed: %d)",
			sequenceNumber, n.ExecutedSeq), n.ID)
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

	if senderAccount == nil || receiverAccount == nil {
		n.handleTransactionFailure(sequenceNumber, request, "Invalid accounts", senderAccount, receiverAccount)
		return
	}

	if !senderAccount.CanTransfer(transaction.Amount) {
		n.handleTransactionFailure(sequenceNumber, request, "Insufficient balance", senderAccount, receiverAccount)
		return
	}

	senderAccount.UpdateBalance(-transaction.Amount)
	receiverAccount.UpdateBalance(transaction.Amount)

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
}

func (n *Node) isWaitingToExecute() bool {
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

func (n *Node) restartTimerIfNeeded() {
	n.mu.Lock()
	isActive := n.IsActive
	isLeader := n.IsLeader
	currentTimer := n.LeaderTimer
	n.mu.Unlock()

	if !isActive || isLeader || currentTimer != nil {
		return
	}

	minTimeout := 700 * time.Millisecond
	maxTimeout := 2000 * time.Millisecond
	timer := common.NewRandomizedTimerWithLogging(minTimeout, maxTimeout, n.handleLeaderTimeout, n.Logger, n.ID)

	n.mu.Lock()
	if n.LeaderTimer == nil && n.IsActive && !n.IsLeader {
		n.LeaderTimer = timer
		timer.Start()
		n.Logger.Log("TIMER", fmt.Sprintf("Restarted randomized timer after receiving message (recovery) with range %v - %v", minTimeout, maxTimeout), n.ID)
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()
	timer.Stop()
}

func (n *Node) processPrepareMessageDirectly(prepare *proto.Prepare) {
	n.mu.Lock()
	defer n.mu.Unlock()

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

	n.HighestBallotSeen = prepare.Ballot

	if n.IsLeader {
		n.Logger.Log("STEPDOWN", "Stepping down as leader due to higher ballot", n.ID)
		n.IsLeader = false
		n.NodeType = common.Backup
		n.LeaderID = 0
		n.PromiseCount = 0

		for k := range n.PromiseAcceptLog {
			delete(n.PromiseAcceptLog, k)
		}
	}

	if n.ProposedBallotNumber != nil && !n.IsLeader {
		n.Logger.Log("CANDIDATE", "Abandoning candidacy due to higher ballot", n.ID)
		n.ProposedBallotNumber = nil
		n.PromiseCount = 0
		n.NodeType = common.Backup
		n.LeaderID = 0

		for k := range n.PromiseAcceptLog {
			delete(n.PromiseAcceptLog, k)
		}
	}

	go n.sendPromiseToProposer(prepare)
}

func (n *Node) sendPromiseToProposer(prepare *proto.Prepare) {
	n.Logger.Log("PROMISE", fmt.Sprintf("Sending promise for prepare %d.%d",
		prepare.Ballot.Round, prepare.Ballot.NodeId), n.ID)

	promise := &proto.Promise{
		Ballot:      prepare.Ballot,
		AcceptLog:   n.AcceptLog,
		SenderId:    n.ID,
		ExecutedSeq: n.ExecutedSeq,
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

func (n *Node) updateTransactionStatus(sequenceNumber int32, request *proto.Request, status common.TransactionStatus, ballotNumber *proto.BallotNumber) {
	n.mu.Lock()
	defer n.mu.Unlock()

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
	n.Logger.Log("BROADCAST", "Broadcasting prepare message", n.ID)

	n.PrepareCooldown.notePrepareSeen(time.Now())

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
	n.mu.Lock()
	restart := false
	defer func() {
		n.mu.Unlock()
		if restart {
			go n.initiateLeaderElection()
		}
	}()

	if n.ProposedBallotNumber == nil {
		return
	}

	if n.PromiseResponded[senderId] {
		return
	}
	n.PromiseResponded[senderId] = true

	if !IsEqualBallot(promise.Ballot, n.ProposedBallotNumber) {

		if IsHigherBallot(promise.Ballot, n.HighestBallotSeen) {

			n.HighestBallotSeen = promise.Ballot
			n.Logger.Log("REJECT", fmt.Sprintf("Higher-ballot NACK from %d; restarting (their %d.%d > our %d.%d)",
				senderId, promise.Ballot.Round, promise.Ballot.NodeId, n.ProposedBallotNumber.Round, n.ProposedBallotNumber.NodeId), n.ID)

			n.resetElectionTrackingLocked()
			restart = true
			return
		}

		n.Logger.Log("IGNORE", fmt.Sprintf("Lower-ballot promise from %d; ignoring (their %d.%d < our %d.%d)",
			senderId, promise.Ballot.Round, promise.Ballot.NodeId, n.ProposedBallotNumber.Round, n.ProposedBallotNumber.NodeId), n.ID)
		return
	}

	if promise.ExecutedSeq < n.minExecutedSeqFromPromises {
		n.minExecutedSeqFromPromises = promise.ExecutedSeq
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

	if n.PromiseCount >= n.Config.F+1 {
		n.Logger.Log("LEADER", fmt.Sprintf("Became leader with ballot %d.%d (%d/%d promises)",
			n.ProposedBallotNumber.Round, n.ProposedBallotNumber.NodeId, n.PromiseCount, n.Config.F+1), n.ID)

		n.IsLeader = true
		n.NodeType = common.Leader
		n.LeaderID = n.ID

		if n.LeaderTimer != nil {
			n.LeaderTimer.Stop()
			n.Logger.Log("TIMER", "Stopped election timer - became leader", n.ID)
		}

		go n.createNewViewMessage()

	}
}

func (n *Node) resetElectionTrackingLocked() {
	n.PromiseAckNodes = make(map[int32]bool)
	n.PromiseResponded = make(map[int32]bool)
	n.PromiseCount = 0

	for k := range n.PromiseAcceptLog {
		delete(n.PromiseAcceptLog, k)
	}
}

func (n *Node) processPendingRequests() {
	n.mu.Lock()
	pendingRequests := make([]*proto.Request, len(n.PendingRequests))
	copy(pendingRequests, n.PendingRequests)
	n.PendingRequests = n.PendingRequests[:0]
	n.mu.Unlock()

	n.Logger.Log("LEADER", fmt.Sprintf("Processing %d pending requests", len(pendingRequests)), n.ID)

	for _, request := range pendingRequests {
		n.processAsLeader(request)
	}
}

func (n *Node) createNewViewMessage() {
	n.Logger.Log("NEWVIEW", "Creating NEW-VIEW message", n.ID)

	acceptLog := n.aggregateAcceptLogFromPromises()

	n.mu.Lock()
	for _, e := range acceptLog {

		n.AcceptedTransactions[e.AcceptSeq] = e.AcceptVal

		if n.AcceptedBy[e.AcceptSeq] == nil {
			n.AcceptedBy[e.AcceptSeq] = make(map[int32]bool)
		}
		n.AcceptedBy[e.AcceptSeq][n.ID] = true

		n.updateTransactionStatusLocked(e.AcceptSeq, e.AcceptVal, common.Accepted, n.ProposedBallotNumber)
	}
	n.mu.Unlock()

	newView := &proto.NewView{
		Ballot:    n.ProposedBallotNumber,
		AcceptLog: acceptLog,
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

func (n *Node) aggregateAcceptLogFromPromises() []*proto.AcceptLogEntry {
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
				AcceptNum: n.ProposedBallotNumber,
				AcceptSeq: seq,
				AcceptVal: entry.AcceptVal,
			})
		} else {

			aggregated = append(aggregated, &proto.AcceptLogEntry{
				AcceptNum: n.ProposedBallotNumber,
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

	commit := &proto.Commit{
		Ballot:   n.ProposedBallotNumber,
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

	go n.executeTransaction(sequence, request)

}

func (n *Node) sendReplyToClient(originalRequest *proto.Request, success bool, message string, transaction *proto.Transaction, senderBalance, receiverBalance int32) {
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

	highestAccepted := fromSeq
	for seq := range n.AcceptedTransactions {
		if seq > highestAccepted {
			highestAccepted = seq
		}
	}

	n.Logger.Log("RECOVERY", fmt.Sprintf("Creating AcceptLog from leader state (fromSeq: %d, HighestAccepted: %d)", fromSeq, highestAccepted), n.ID)

	var acceptLog []*proto.AcceptLogEntry

	for seq := fromSeq + 1; seq <= highestAccepted; seq++ {
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

	leaderID := n.discoverLeaderFromActiveQuorum()
	if leaderID == 0 {

		n.mu.Lock()
		n.RecoveryState = RecoveryFailed
		isActive := n.IsActive
		isLeader := n.IsLeader
		timer := n.LeaderTimer
		n.mu.Unlock()

		shouldStart := n.isWaitingToExecute()

		if timer == nil {

			n.restartTimerIfNeeded()
		} else if timer != nil && shouldStart && isActive && !isLeader {

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
		n.Logger.Log("RECOVERY", "Recovery completed successfully", n.ID)
	} else {
		n.Logger.Log("RECOVERY", "Recovery failed, will wait for messages", n.ID)
	}
}

func (n *Node) discoverLeaderFromActiveQuorum() int32 {
	n.Logger.Log("RECOVERY", "Discovering leader from majority of ACTIVE nodes", n.ID)

	activePeers := n.listActivePeerIDs()
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
	var active []int32
	for _, nodeInfo := range n.Config.Nodes {
		if nodeInfo.ID == n.ID {
			continue
		}
		conn, err := n.getConnection(nodeInfo.ID)
		if err != nil {
			continue
		}

		client := proto.NewPaxosServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		status, err := client.GetStatus(ctx, &proto.Empty{})
		cancel()
		if err == nil && status != nil && status.Status == "running" {
			active = append(active, nodeInfo.ID)
		}
	}
	return active
}

func (n *Node) queryNodeForLeaderWithTimeout(nodeID int32, timeout time.Duration) int32 {
	conn, err := n.getConnection(nodeID)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to node %d for leader discovery: %v", nodeID, err), n.ID)
		return 0
	}

	client := proto.NewNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	leaderInfo, err := client.GetLeader(ctx, &proto.Empty{})
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to get leader from node %d: %v", nodeID, err), n.ID)
		return 0
	}

	if leaderInfo.NodeId > 0 {
		n.Logger.Log("RECOVERY", fmt.Sprintf("Node %d reports leader %d", nodeID, leaderInfo.NodeId), n.ID)
		return leaderInfo.NodeId
	}

	n.Logger.Log("RECOVERY", fmt.Sprintf("Node %d reports no leader", nodeID), n.ID)
	return 0
}

func (n *Node) queryNodeForLeaderWithContext(ctx context.Context, nodeID int32) int32 {
	conn, err := n.getConnection(nodeID)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to node %d for leader discovery: %v", nodeID, err), n.ID)
		return 0
	}

	client := proto.NewNodeServiceClient(conn)

	leaderInfo, err := client.GetLeader(ctx, &proto.Empty{})
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to get leader from node %d: %v", nodeID, err), n.ID)
		return 0
	}

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
