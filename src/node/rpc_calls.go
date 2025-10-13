package node

import (
	"context"
	"fmt"
	"paxos-banking/proto"
	"paxos-banking/src/common"
	"time"

	"strconv"

	"google.golang.org/grpc/metadata"
)

func (n *Node) SendRequest(ctx context.Context, req *proto.Request) (*proto.Reply, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", fmt.Sprintf("Rejecting SendRequest from client %s - node is inactive", req.ClientId), n.ID)
		return &proto.Reply{
			Ballot:    n.ProposedBallotNumber,
			Timestamp: req.Timestamp,
			ClientId:  req.ClientId,
			Result:    false,
			Message:   "Node is down",
		}, nil
	}

	n.mu.Lock()
	clientKey := req.ClientId
	lastTimestamp, exists := n.LastClientTimestamp[clientKey]

	if exists && req.Timestamp <= lastTimestamp {

		n.Logger.Log("DUPLICATE", fmt.Sprintf("Duplicate request from client %s (timestamp %d <= %d)",
			req.ClientId, req.Timestamp, lastTimestamp), n.ID)

		if cachedReply, hasReply := n.LastClientReply[clientKey]; hasReply {
			n.mu.Unlock()
			return cachedReply, nil
		}
	}

	n.LastClientTimestamp[clientKey] = req.Timestamp
	n.mu.Unlock()

	n.Logger.Log("RPC", fmt.Sprintf("Received SendRequest from client %s", req.ClientId), n.ID)

	n.requestChan <- req

	var replyBallot *proto.BallotNumber
	n.mu.RLock()
	isLeader := n.IsLeader
	proposed := n.ProposedBallotNumber
	leaderID := n.LeaderID
	highest := n.HighestBallotSeen
	n.mu.RUnlock()
	if isLeader && proposed != nil {
		replyBallot = proposed
	} else if leaderID > 0 && highest != nil {
		replyBallot = &proto.BallotNumber{Round: highest.Round, NodeId: leaderID}
	} else {
		replyBallot = nil
	}

	reply := &proto.Reply{
		Ballot:    replyBallot,
		Timestamp: req.Timestamp,
		ClientId:  req.ClientId,
		Result:    true,
		Message:   "Request queued for processing",
	}

	return reply, nil
}

func (n *Node) HandleRequest(ctx context.Context, req *proto.Request) (*proto.Status, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", fmt.Sprintf("Rejecting HandleRequest from another node for client %s - node is inactive", req.ClientId), n.ID)
		return &proto.Status{
			Status:  "error",
			Message: "Node is down",
		}, nil
	}

	n.Logger.Log("RPC", fmt.Sprintf("Received HandleRequest from another node for client %s", req.ClientId), n.ID)

	n.requestChan <- req

	status := &proto.Status{
		Status:  "success",
		Message: "Request forwarded",
	}

	return status, nil
}

func (n *Node) HandlePrepare(ctx context.Context, req *proto.Prepare) (*proto.PrepareAck, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", fmt.Sprintf("Rejecting HandlePrepare from ballot %d.%d - node is inactive", req.Ballot.Round, req.Ballot.NodeId), n.ID)

		n.mu.RLock()
		highestBallot := n.HighestBallotSeen
		n.mu.RUnlock()
		return &proto.PrepareAck{
			Ballot:  highestBallot,
			Queued:  false,
			Message: "Node is inactive",
		}, nil
	}

	n.Logger.Log("RPC", fmt.Sprintf("Received HandlePrepare from ballot %d.%d", req.Ballot.Round, req.Ballot.NodeId), n.ID)

	if !n.isValidPrepare(req) {
		n.Logger.Log("REJECT", fmt.Sprintf("Rejecting prepare with ballot %d.%d",
			req.Ballot.Round, req.Ballot.NodeId), n.ID)

		n.mu.RLock()
		highestBallot := n.HighestBallotSeen
		n.mu.RUnlock()

		return &proto.PrepareAck{
			Ballot:  highestBallot,
			Queued:  false,
			Message: "Prepare rejected - lower ballot",
		}, nil
	}

	n.mu.RLock()
	timerRunning := n.LeaderTimer != nil && n.LeaderTimer.IsRunning()
	n.mu.RUnlock()

	if timerRunning {

		n.Logger.Log("PREPARE", fmt.Sprintf("Timer running - storing prepare %d.%d for later processing",
			req.Ballot.Round, req.Ballot.NodeId), n.ID)
		go n.processPrepareAsync(req)

		return &proto.PrepareAck{
			Ballot:  req.Ballot,
			Queued:  true,
			Message: "Prepare queued for later processing",
		}, nil
	} else {

		n.Logger.Log("PREPARE", fmt.Sprintf("Timer not running - processing prepare %d.%d immediately",
			req.Ballot.Round, req.Ballot.NodeId), n.ID)
		go n.processPrepareMessageDirectly(req)

		return &proto.PrepareAck{
			Ballot:  req.Ballot,
			Queued:  false,
			Message: "Prepare processed immediately",
		}, nil
	}
}

func (n *Node) processPrepareAsync(req *proto.Prepare) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.PrepareCooldown.notePrepareSeen(time.Now())

	n.ReceivedPrepares = append(n.ReceivedPrepares, &PrepareWithTimestamp{
		Prepare:   req,
		Timestamp: time.Now(),
	})

	now := time.Now()
	var validPrepares []*PrepareWithTimestamp
	for _, prepareWithTime := range n.ReceivedPrepares {

		if now.Sub(prepareWithTime.Timestamp) <= n.PrepareCooldown.tp {
			validPrepares = append(validPrepares, prepareWithTime)
		}
	}
	n.ReceivedPrepares = validPrepares

	n.Logger.Log("PREPARE", fmt.Sprintf("Stored prepare message %d.%d for later processing",
		req.Ballot.Round, req.Ballot.NodeId), n.ID)
}

func (n *Node) HandlePromise(ctx context.Context, req *proto.Promise) (*proto.Status, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", fmt.Sprintf("Rejecting HandlePromise from ballot %d.%d - node is inactive", req.Ballot.Round, req.Ballot.NodeId), n.ID)
		return &proto.Status{
			Status:  "error",
			Message: "Node is inactive",
		}, nil
	}

	var senderID int32 = 0
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-sender-id"); len(vals) > 0 {
			if parsed, err := strconv.Atoi(vals[0]); err == nil {
				senderID = int32(parsed)
			}
		}
	}
	if senderID == 0 && req.SenderId != 0 {
		senderID = req.SenderId
	} else if req.SenderId != 0 && senderID != 0 && senderID != req.SenderId {
		n.Logger.Log("WARN", fmt.Sprintf("Sender ID mismatch: md=%d, body=%d", senderID, req.SenderId), n.ID)
	}
	n.Logger.Log("RPC", fmt.Sprintf("Received HandlePromise from ballot %d.%d (sender=%d)", req.Ballot.Round, req.Ballot.NodeId, senderID), n.ID)

	go n.processPromiseMessage(senderID, req)

	status := &proto.Status{
		Status:  "success",
		Message: "Promise processed",
	}

	return status, nil
}

func (n *Node) HandleAccept(ctx context.Context, req *proto.Accept) (*proto.AcceptAck, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", fmt.Sprintf("Rejecting HandleAccept for sequence %d with ballot %d.%d - node is inactive",
			req.Sequence, req.Ballot.Round, req.Ballot.NodeId), n.ID)

		return &proto.AcceptAck{
			Success: false,
			Message: "Node is inactive",
			NodeId:  n.ID,
		}, nil
	}

	n.Logger.Log("RPC", fmt.Sprintf("Received HandleAccept for sequence %d with ballot %d.%d",
		req.Sequence, req.Ballot.Round, req.Ballot.NodeId), n.ID)

	if !n.isValidAccept(req) {
		n.Logger.Log("REJECT", fmt.Sprintf("Rejecting accept with ballot %d.%d",
			req.Ballot.Round, req.Ballot.NodeId), n.ID)

		return &proto.AcceptAck{
			Success: false,
			Message: "Accept rejected - lower ballot",
			NodeId:  n.ID,
		}, nil
	}

	go n.processAcceptAsync(req)

	acceptAck := &proto.AcceptAck{
		Success: true,
		Message: "Accept processed",
		NodeId:  n.ID,
	}

	n.Logger.Log("ACCEPTED", fmt.Sprintf("Accepted sequence %d with ballot %d.%d",
		req.Sequence, req.Ballot.Round, req.Ballot.NodeId), n.ID)

	return acceptAck, nil
}

func (n *Node) processAcceptAsync(req *proto.Accept) {
	n.mu.Lock()

	recovering := n.RecoveryState == RecoveryInProgress
	followerExecuted := n.ExecutedSeq
	leaderID := req.Ballot.NodeId
	defer n.mu.Unlock()

	if IsHigherBallot(req.Ballot, n.HighestBallotSeen) {
		n.HighestBallotSeen = req.Ballot
	}

	n.updateTransactionStatusLocked(req.Sequence, req.Request, common.Accepted, req.Ballot)

	n.AcceptedTransactions[req.Sequence] = req.Request

	acceptLogEntry := &proto.AcceptLogEntry{
		AcceptNum: req.Ballot,
		AcceptSeq: req.Sequence,
		AcceptVal: req.Request,
	}
	n.AcceptLog = append(n.AcceptLog, acceptLogEntry)

	n.LeaderID = req.Ballot.NodeId
	n.LastLeaderMessage = time.Now()

	go n.sendAcceptedToLeader(req)

	go n.startTimerOnAccept()

	go n.restartTimerIfNeeded()

	if recovering && leaderID > 0 {
		go n.requestNewViewFromLeader(leaderID)
		_ = followerExecuted
	}
}

func (n *Node) sendAcceptedToLeader(accept *proto.Accept) {
	leaderID := accept.Ballot.NodeId
	if leaderID == 0 || leaderID == n.ID {

		return
	}

	conn, err := n.getConnection(leaderID)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to leader %d for Accepted: %v", leaderID, err), n.ID)
		return
	}

	client := proto.NewNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	accepted := &proto.Accepted{
		Ballot:   accept.Ballot,
		Sequence: accept.Sequence,
		Request:  accept.Request,
		NodeId:   n.ID,
	}

	_, err = client.HandleAccepted(ctx, accepted)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to send Accepted to leader %d: %v", leaderID, err), n.ID)
		return
	}

	n.Logger.Log("ACCEPTED", fmt.Sprintf("Sent Accepted for sequence %d to leader %d", accept.Sequence, leaderID), n.ID)
}

func (n *Node) HandleCommit(ctx context.Context, req *proto.Commit) (*proto.Status, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", fmt.Sprintf("Rejecting HandleCommit for sequence %d with ballot %d.%d - node is inactive",
			req.Sequence, req.Ballot.Round, req.Ballot.NodeId), n.ID)
		return &proto.Status{
			Status:  "error",
			Message: "Node is down",
		}, nil
	}

	n.Logger.Log("RPC", fmt.Sprintf("Received HandleCommit for sequence %d with ballot %d.%d",
		req.Sequence, req.Ballot.Round, req.Ballot.NodeId), n.ID)

	go n.processCommitAsync(req)

	status := &proto.Status{
		Status:  "success",
		Message: "Commit processed",
	}

	n.Logger.Log("COMMIT", fmt.Sprintf("Committed sequence %d with ballot %d.%d",
		req.Sequence, req.Ballot.Round, req.Ballot.NodeId), n.ID)

	return status, nil
}

func (n *Node) processCommitAsync(req *proto.Commit) {
	n.mu.Lock()

	recovering := n.RecoveryState == RecoveryInProgress
	leaderID := req.Ballot.NodeId
	defer n.mu.Unlock()

	if transactionInfo, exists := n.TransactionStatus[req.Sequence]; exists {
		if transactionInfo.Status != common.Executed {
			n.updateTransactionStatusLocked(req.Sequence, req.Request, common.Committed, req.Ballot)
		}
	} else {

		n.updateTransactionStatusLocked(req.Sequence, req.Request, common.Committed, req.Ballot)
	}

	n.AcceptedTransactions[req.Sequence] = req.Request

	go n.executeTransaction(req.Sequence, req.Request)

	if req.Sequence > n.CommittedSeq {
		n.CommittedSeq = req.Sequence

		if n.DB != nil {
			if err := n.DB.SaveSystemState(n.ID, n.SequenceNum, n.ExecutedSeq, n.CommittedSeq); err != nil {
				n.Logger.Log("ERROR", fmt.Sprintf("Failed to persist system state: %v", err), n.ID)
			}
		}
	}

	n.LeaderID = req.Ballot.NodeId
	n.LastLeaderMessage = time.Now()

	go n.restartTimerIfNeeded()

	if recovering && leaderID > 0 {
		go n.requestNewViewFromLeader(leaderID)
	}
}

func (n *Node) HandleNewView(ctx context.Context, req *proto.NewView) (*proto.Status, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", fmt.Sprintf("Rejecting HandleNewView from ballot %d.%d - node is inactive", req.Ballot.Round, req.Ballot.NodeId), n.ID)
		return &proto.Status{
			Status:  "error",
			Message: "Node is down",
		}, nil
	}

	n.Logger.Log("RPC", fmt.Sprintf("Received HandleNewView from ballot %d.%d", req.Ballot.Round, req.Ballot.NodeId), n.ID)

	go n.processNewViewAsync(req)

	status := &proto.Status{
		Status:  "success",
		Message: "NewView processed",
	}

	n.Logger.Log("NEWVIEW", fmt.Sprintf("Processed NEW-VIEW from ballot %d.%d",
		req.Ballot.Round, req.Ballot.NodeId), n.ID)

	return status, nil
}

func (n *Node) processNewViewAsync(req *proto.NewView) {

	if req == nil {
		n.Logger.Log("ERROR", "Received nil NEW-VIEW message", n.ID)
		return
	}

	if req.Ballot == nil {
		n.Logger.Log("ERROR", "Received NEW-VIEW message with nil ballot", n.ID)
		return
	}

	n.mu.Lock()

	n.NewViewLog = append(n.NewViewLog, req)
	n.Logger.Log("NEWVIEW", fmt.Sprintf("Received NEW-VIEW from leader %d, ballot %d.%d",
		req.Ballot.NodeId, req.Ballot.Round, req.Ballot.NodeId), n.ID)

	if IsHigherBallot(req.Ballot, n.HighestBallotSeen) {
		n.HighestBallotSeen = req.Ballot
	}

	n.LeaderID = req.Ballot.NodeId
	n.LastLeaderMessage = time.Now()

	acceptedMsgs := make([]*proto.Accepted, 0, len(req.AcceptLog))
	for _, acceptLogEntry := range req.AcceptLog {

		alreadyExecuted := n.ExecutedTransactions[acceptLogEntry.AcceptSeq]

		accept := &proto.Accept{
			Ballot:   req.Ballot,
			Sequence: acceptLogEntry.AcceptSeq,
			Request:  acceptLogEntry.AcceptVal,
		}

		n.processNewViewAcceptMessage(accept, alreadyExecuted)

		acceptedMsgs = append(acceptedMsgs, &proto.Accepted{
			Ballot:   req.Ballot,
			Sequence: acceptLogEntry.AcceptSeq,
			Request:  acceptLogEntry.AcceptVal,
			NodeId:   n.ID,
		})
	}
	var maxSeq int32 = 0
	for _, e := range req.AcceptLog {
		if e.AcceptSeq > maxSeq {
			maxSeq = e.AcceptSeq
		}
	}
	n.NewViewMaxSeq = maxSeq

	leaderID := n.LeaderID
	shouldStartTimer := !n.IsLeader && n.LeaderTimer != nil
	n.mu.Unlock()

	for _, accepted := range acceptedMsgs {
		a := accepted
		go func(leaderID int32) {
			if leaderID == 0 {
				return
			}
			conn, err := n.getConnection(leaderID)
			if err != nil {
				n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to leader %d for HandleAccepted: %v", leaderID, err), n.ID)
				return
			}
			client := proto.NewNodeServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = client.HandleAccepted(ctx, a)
			if err != nil {
				n.Logger.Log("ERROR", fmt.Sprintf("Failed to send Accepted to leader %d: %v", leaderID, err), n.ID)
			}
		}(leaderID)
	}

	if shouldStartTimer {
		go n.startTimerOnAccept()
	}

	go n.restartTimerIfNeeded()

}

func (n *Node) processNewViewAcceptMessage(accept *proto.Accept, alreadyExecuted bool) {

	if !alreadyExecuted {
		if transactionInfo, exists := n.TransactionStatus[accept.Sequence]; exists {
			if transactionInfo.Status == common.Executed || transactionInfo.Status == common.Committed {

				transactionInfo.BallotNumber = accept.Ballot
			} else {
				n.updateTransactionStatusLocked(accept.Sequence, accept.Request, common.Accepted, accept.Ballot)
			}
		} else {
			n.updateTransactionStatusLocked(accept.Sequence, accept.Request, common.Accepted, accept.Ballot)
		}
	} else {

		if transactionInfo, exists := n.TransactionStatus[accept.Sequence]; exists {
			transactionInfo.BallotNumber = accept.Ballot
			n.Logger.Log("PROCESS", fmt.Sprintf("Updated ballot for already-executed sequence %d from NEW-VIEW", accept.Sequence), n.ID)
		}
	}

	acceptLogEntry := &proto.AcceptLogEntry{
		AcceptNum: accept.Ballot,
		AcceptSeq: accept.Sequence,
		AcceptVal: accept.Request,
	}
	n.AcceptLog = append(n.AcceptLog, acceptLogEntry)

	n.AcceptedTransactions[accept.Sequence] = accept.Request

	if !alreadyExecuted {
		n.Logger.Log("PROCESS", fmt.Sprintf("Processed accept for sequence %d from NEW-VIEW", accept.Sequence), n.ID)
	} else {
		n.Logger.Log("PROCESS", fmt.Sprintf("Preserved Executed status for sequence %d from NEW-VIEW", accept.Sequence), n.ID)
	}
}

func (n *Node) HandleAccepted(ctx context.Context, req *proto.Accepted) (*proto.Status, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", fmt.Sprintf("Rejecting HandleAccepted for sequence %d from node %d with ballot %d.%d - node is inactive",
			req.Sequence, req.NodeId, req.Ballot.Round, req.Ballot.NodeId), n.ID)
		return &proto.Status{
			Status:  "error",
			Message: "Node is down",
		}, nil
	}

	n.Logger.Log("RPC", fmt.Sprintf("Received HandleAccepted for sequence %d from node %d with ballot %d.%d",
		req.Sequence, req.NodeId, req.Ballot.Round, req.Ballot.NodeId), n.ID)

	select {
	case n.acceptedChan <- req:
	default:

		go n.processAcceptedMessage(req)
	}

	status := &proto.Status{
		Status:  "success",
		Message: "Accepted received",
	}
	return status, nil
}

func (n *Node) RequestNewView(ctx context.Context, req *proto.Empty) (*proto.NewView, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", "Rejecting RequestNewView - node is inactive", n.ID)
		return nil, fmt.Errorf("node is inactive")
	}

	n.Logger.Log("RPC", "Received RequestNewView for active catch-up", n.ID)

	var fromSeq int32 = 0
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-from-executed-seq"); len(vals) > 0 {
			if parsed, err := strconv.Atoi(vals[0]); err == nil {
				fromSeq = int32(parsed)
			}
		}
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	if !n.IsLeader {
		n.Logger.Log("RPC", fmt.Sprintf("Rejecting RequestNewView - node %d is not a leader", n.ID), n.ID)
		return nil, fmt.Errorf("node %d is not a leader", n.ID)
	}

	if n.ProposedBallotNumber == nil {
		n.Logger.Log("RPC", "Rejecting RequestNewView - no proposed ballot number", n.ID)
		return nil, fmt.Errorf("no proposed ballot number")
	}

	rangedLog := n.createAcceptLogFromLeaderState(fromSeq)
	newView := &proto.NewView{
		Ballot:    n.ProposedBallotNumber,
		AcceptLog: rangedLog,
	}

	n.Logger.Log("RECOVERY", fmt.Sprintf("Created NEW-VIEW for recovery with %d entries", len(newView.AcceptLog)), n.ID)

	return newView, nil
}
