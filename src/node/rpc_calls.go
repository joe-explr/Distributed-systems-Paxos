package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"paxos-banking/proto"
	"paxos-banking/src/common"
	"time"

	"strconv"

	"google.golang.org/grpc/metadata"
)

// Client-Node Request
func (n *Node) SendRequest(ctx context.Context, req *proto.Request) (*proto.Status, error) {
	n.Logger.LogWithWallTime("DEBUG", fmt.Sprintf("SendRequest RPC called - client:%s timestamp:%d amount:%d sender:%s receiver:%s",
		req.ClientId, req.Timestamp, req.Transaction.Amount, req.Transaction.Sender, req.Transaction.Receiver), n.ID)

	if !n.IsNodeActive() {
		n.Logger.LogWithWallTime("RPC", fmt.Sprintf("Rejecting SendRequest from client %s - node is inactive", req.ClientId), n.ID)
		return &proto.Status{Status: "INACTIVE", Message: "Node inactive"}, nil
	}

	rid := makeRequestID(req)
	if rid == "" {
		return &proto.Status{Status: "NIL_REQUEST", Message: "Nil request received", LeaderId: n.ID}, nil
	}
	n.Logger.LogWithWallTime("RPC", fmt.Sprintf("Received SendRequest from client %s", req.ClientId), n.ID)

	n.mu.RLock()
	isLeader := n.IsLeader
	knownLeader := n.LeaderID
	n.mu.RUnlock()

	n.Logger.LogWithWallTime("DEBUG", fmt.Sprintf("SendRequest: isLeader=%v knownLeader=%d", isLeader, knownLeader), n.ID)

	if isLeader {
		select {
		case n.requestChan <- req:
			status := &proto.Status{Status: "OK_ACCEPTED", Message: "Accepted for processing", LeaderId: n.ID}
			return status, nil
		default:
			n.Logger.LogWithWallTime("ERROR", fmt.Sprintf("Request queue full, rejecting client %s", req.ClientId), n.ID)
			status := &proto.Status{Status: "ERROR", Message: "Request queue full", LeaderId: n.ID}
			return status, nil
		}
	}

	if knownLeader != 0 {
		n.Logger.LogWithWallTime("DEBUG", fmt.Sprintf("SendRequest: forwarding to known leader %d", knownLeader), n.ID)
		return n.forwardClientRequestToLeader(req, knownLeader)
	}

	n.mu.Lock()
	n.enqueuePendingRequestLocked(req)
	n.mu.Unlock()

	n.Logger.LogWithWallTime("DEBUG", "SendRequest: no known leader, starting election", n.ID)
	go n.restartTimerIfNeeded()
	status := &proto.Status{Status: "PENDING", Message: "Leader unknown; election started", LeaderId: 0}
	return status, nil
}

func (n *Node) HandleRequest(ctx context.Context, req *proto.Request) (*proto.Status, error) {
	n.Logger.LogWithWallTime("DEBUG", fmt.Sprintf("HandleRequest RPC called - client:%s timestamp:%d amount:%d sender:%s receiver:%s",
		req.ClientId, req.Timestamp, req.Transaction.Amount, req.Transaction.Sender, req.Transaction.Receiver), n.ID)

	if !n.IsNodeActive() {
		n.Logger.LogWithWallTime("RPC", fmt.Sprintf("Rejecting HandleRequest from another node for client %s - node is inactive", req.ClientId), n.ID)
		return nil, fmt.Errorf("node is inactive")
	}

	n.Logger.LogWithWallTime("RPC", fmt.Sprintf("Received HandleRequest from another node for client %s", req.ClientId), n.ID)

	n.mu.RLock()
	isLeader := n.IsLeader
	leaderID := n.LeaderID
	n.mu.RUnlock()

	n.Logger.LogWithWallTime("DEBUG", fmt.Sprintf("HandleRequest: isLeader=%v leaderID=%d", isLeader, leaderID), n.ID)

	if !isLeader {
		message := "Not leader"
		if leaderID != 0 {
			message = fmt.Sprintf("Not leader; forward to %d", leaderID)
		}
		return &proto.Status{
			Status:   "NOT_LEADER",
			Message:  message,
			LeaderId: leaderID,
		}, nil
	}

	rid := makeRequestID(req)
	var cachedReply *proto.Reply
	var seq int32
	var duplicate bool

	n.mu.Lock()
	if rid != "" {
		if existingSeq, ok := n.RequestIDToSeq[rid]; ok {
			duplicate = true
			seq = existingSeq
			cachedReply = n.LastClientReply[req.ClientId]
		}
	}
	n.mu.Unlock()

	if duplicate {
		if cachedReply != nil && cachedReply.Timestamp == req.Timestamp {
			go n.sendReplyToClient(req, cachedReply.Result, cachedReply.Message, req.Transaction, 0, 0)
		}
		msg := "Duplicate request"
		if seq > 0 {
			msg = fmt.Sprintf("Duplicate request; already accepted as seq %d", seq)
		}
		return &proto.Status{
			Status:   "OK_ACCEPTED_DUPLICATE",
			Message:  msg,
			LeaderId: n.ID,
		}, nil
	}

	select {
	case n.requestChan <- req:
		return &proto.Status{
			Status:   "OK_ACCEPTED",
			Message:  "Accepted for processing",
			LeaderId: n.ID,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		n.Logger.Log("ERROR", fmt.Sprintf("Request queue full, rejecting client %s", req.ClientId), n.ID)
		return &proto.Status{
			Status:   "ERROR",
			Message:  "Request queue full",
			LeaderId: n.ID,
		}, nil
	}
}

func (n *Node) forwardClientRequestToLeader(req *proto.Request, leaderID int32) (*proto.Status, error) {
	n.Logger.LogWithWallTime("DEBUG", fmt.Sprintf("forwardClientRequestToLeader called - forwarding client:%s to leader:%d",
		req.ClientId, leaderID), n.ID)

	attempt := leaderID
	for redirects := 0; redirects < 2; redirects++ {
		conn, err := n.getConnection(attempt)
		if err != nil {
			n.Logger.LogClientWithWallTime("FORWARD_ERROR", fmt.Sprintf("Failed to connect to leader %d: %v", attempt, err), fmt.Sprintf("%d", n.ID))
			n.mu.Lock()
			if n.LeaderID == attempt {
				n.LeaderID = 0
			}
			n.enqueuePendingRequestLocked(req)
			n.mu.Unlock()
			go n.restartTimerIfNeeded()
			return &proto.Status{Status: "PENDING", Message: "Leader unreachable; election started", LeaderId: 0}, nil
		}

		client := proto.NewNodeServiceClient(conn)
		n.Logger.Log("FORWARD_DEBUG", fmt.Sprintf("Forwarding request to leader %d for client %s", attempt, req.ClientId), n.ID)
		resp, err := client.HandleRequest(context.Background(), req)
		if err != nil {
			n.Logger.Log("ERROR", fmt.Sprintf("Failed to forward request to leader %d: %v", attempt, err), n.ID)
			n.mu.Lock()
			if n.LeaderID == attempt {
				n.LeaderID = 0
			}
			n.enqueuePendingRequestLocked(req)
			n.mu.Unlock()
			go n.restartTimerIfNeeded()
			return &proto.Status{Status: "PENDING", Message: "Leader forwarding failed; election started", LeaderId: 0}, nil
		}

		if resp == nil {
			n.Logger.Log("ERROR", fmt.Sprintf("Nil response from leader %d while forwarding request", attempt), n.ID)
			n.mu.Lock()
			n.enqueuePendingRequestLocked(req)
			n.LeaderID = 0
			n.mu.Unlock()
			go n.restartTimerIfNeeded()
			return &proto.Status{Status: "PENDING", Message: "Leader response missing; election started", LeaderId: 0}, nil
		}

		if resp.Status == "NOT_LEADER" || resp.Status == "INACTIVE" {
			if resp.LeaderId > 0 && resp.LeaderId != attempt {
				n.Logger.Log("LEADER", fmt.Sprintf("Leader %d redirected to new leader %d after NOT_LEADER", attempt, resp.LeaderId), n.ID)
				n.mu.Lock()
				n.LeaderID = resp.LeaderId
				n.mu.Unlock()
				attempt = resp.LeaderId
				continue
			}

			n.Logger.Log("LEADER", fmt.Sprintf("Leader %d rejected request (%s), triggering election", attempt, resp.Status), n.ID)
			n.mu.Lock()
			n.enqueuePendingRequestLocked(req)
			n.LeaderID = 0
			n.mu.Unlock()
			go n.restartTimerIfNeeded()
			return &proto.Status{Status: "PENDING", Message: "Leader rejected request; election started", LeaderId: resp.LeaderId}, nil
		}

		return resp, nil
	}

	n.mu.Lock()
	n.enqueuePendingRequestLocked(req)
	n.LeaderID = 0
	n.mu.Unlock()
	go n.restartTimerIfNeeded()
	return &proto.Status{Status: "PENDING", Message: "Unable to forward after redirection attempts", LeaderId: 0}, nil
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
		n.Logger.Log("PREPARE", fmt.Sprintf("Node %d acknowledged prepare %d.%d from proposer %d (queued)",
			n.ID, req.Ballot.Round, req.Ballot.NodeId, req.Ballot.NodeId), n.ID)

		return &proto.PrepareAck{
			Ballot:  req.Ballot,
			Queued:  true,
			Message: "Prepare queued for later processing",
		}, nil
	} else {

		n.Logger.Log("PREPARE", fmt.Sprintf("Timer not running - processing prepare %d.%d immediately",
			req.Ballot.Round, req.Ballot.NodeId), n.ID)
		go n.processPrepareMessageDirectly(req)
		n.Logger.Log("PREPARE", fmt.Sprintf("Node %d acknowledged prepare %d.%d from proposer %d (immediate)",
			n.ID, req.Ballot.Round, req.Ballot.NodeId, req.Ballot.NodeId), n.ID)

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

	if req == nil || req.Ballot == nil {
		n.Logger.Log("ERROR", "Received HandlePromise with nil request or ballot - ignoring", n.ID)
		return &proto.Status{
			Status:  "error",
			Message: "Invalid Promise message",
		}, nil
	}

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", fmt.Sprintf("Rejecting HandlePromise from ballot %d.%d - node is inactive", req.Ballot.Round, req.Ballot.NodeId), n.ID)
		return &proto.Status{
			Status:  "error",
			Message: "Node is inactive",
		}, nil
	}

	go n.processPromiseMessage(req.SenderId, req)

	status := &proto.Status{
		Status:  "success",
		Message: "Promise processed",
	}

	return status, nil
}

func (n *Node) HandleAccept(ctx context.Context, req *proto.Accept) (*proto.AcceptAck, error) {

	if req == nil || req.Ballot == nil {
		n.Logger.Log("ERROR", "Received HandleAccept with nil request or ballot - ignoring", n.ID)
		return &proto.AcceptAck{
			Success: false,
			Message: "Invalid Accept message",
			NodeId:  n.ID,
		}, nil
	}

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
	var pendingForTransfer []*proto.Request
	shouldResetTimer := false
	forwardLeaderID := req.Ballot.NodeId

	n.mu.Lock()

	if IsHigherBallot(req.Ballot, n.HighestBallotSeen) {
		n.HighestBallotSeen = req.Ballot
		stepDownReason := fmt.Sprintf("received Accept with higher ballot %d.%d", req.Ballot.Round, req.Ballot.NodeId)
		n.mu.Unlock()
		n.stepDownLeader(stepDownReason, req.Ballot)
		n.mu.Lock()
	}

	if ti, exists := n.TransactionStatus[req.Sequence]; exists {
		if ti.Status == common.Executed || ti.Status == common.Committed {
			n.Logger.Log("ERROR", fmt.Sprintf("Received accept for already executed sequence %d", req.Sequence), n.ID)
			ti.BallotNumber = req.Ballot
		} else {
			n.updateTransactionStatusLocked(req.Sequence, req.Request, common.Accepted, req.Ballot)
		}
	} else {
		n.updateTransactionStatusLocked(req.Sequence, req.Request, common.Accepted, req.Ballot)
	}

	n.AcceptedTransactions[req.Sequence] = req.Request

	acceptLogEntry := &proto.AcceptLogEntry{
		AcceptNum: req.Ballot,
		AcceptSeq: req.Sequence,
		AcceptVal: req.Request,
	}
	n.AcceptLog = append(n.AcceptLog, acceptLogEntry)

	if n.LeaderID != req.Ballot.NodeId {
		n.LeaderID = req.Ballot.NodeId
		shouldResetTimer = true
		forwardLeaderID = req.Ballot.NodeId
		if len(n.PendingRequests) > 0 {
			pendingForTransfer = make([]*proto.Request, len(n.PendingRequests))
			copy(pendingForTransfer, n.PendingRequests)
			n.clearPendingRequestsLocked()
			n.Logger.Log("PENDING", fmt.Sprintf("Captured %d pending requests to forward to new leader %d",
				len(pendingForTransfer), req.Ballot.NodeId), n.ID)
		}
	}

	n.LastLeaderMessage = time.Now()
	n.mu.Unlock()
	if shouldResetTimer {
		n.resetTimerIfRunning()
	}
	if len(pendingForTransfer) > 0 {
		go n.forwardPendingRequestsAfterNewView(forwardLeaderID, pendingForTransfer)
	}
	go n.sendAcceptedToLeader(req)

	go n.startTimerOnAccept()

	go n.restartTimerIfNeeded()
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

	n.Logger.Log("DEBUG", fmt.Sprintf("Processing commit for seq %d (current executed=%d, committed=%d)", req.Sequence, n.ExecutedSeq, n.CommittedSeq), n.ID)
	if n.InNewViewProcessing {
		n.Logger.Log("DEBUG", fmt.Sprintf("Buffering commit %d during NEW-VIEW", req.Sequence), n.ID)
		n.PendingCommitMessages = append(n.PendingCommitMessages, cloneCommit(req))
		n.mu.Unlock()
		return
	}
	if req.Sequence <= n.ExecutedSeq {
		n.Logger.Log("IGNORE", fmt.Sprintf("Ignoring commit for already executed seq %d (executed=%d, committed=%d)", req.Sequence, n.ExecutedSeq, n.CommittedSeq), n.ID)
		n.mu.Unlock()
		return
	}
	defer n.mu.Unlock()

	if transactionInfo, exists := n.TransactionStatus[req.Sequence]; exists {
		if transactionInfo.Status != common.Executed {
			n.updateTransactionStatusLocked(req.Sequence, req.Request, common.Committed, req.Ballot)
		}
	} else {

		n.updateTransactionStatusLocked(req.Sequence, req.Request, common.Committed, req.Ballot)
	}

	n.AcceptedTransactions[req.Sequence] = req.Request

	// DEBUG: Log before calling executeTransaction
	n.Logger.Log("DEBUG", fmt.Sprintf("Calling executeTransaction for seq %d (executed=%d)", req.Sequence, n.ExecutedSeq), n.ID)
	go n.executeTransaction(req.Sequence, req.Request)

	if req.Sequence > n.CommittedSeq {
		n.CommittedSeq = req.Sequence

		if n.DB != nil {
			if err := n.DB.SaveSystemState(n.ID, n.SequenceNum, n.ExecutedSeq, n.CommittedSeq); err != nil {
				n.Logger.Log("ERROR", fmt.Sprintf("Failed to persist system state: %v", err), n.ID)
			}
		}
	}

	if !n.IsLeader {
		nextSeq := req.Sequence + 1
		if n.SequenceNum < nextSeq {
			n.SequenceNum = nextSeq
			n.Logger.Log("DEBUG", fmt.Sprintf("Advanced SequenceNum to %d after commit of seq %d (backup)", n.SequenceNum, req.Sequence), n.ID)
		}
	}

	if n.LeaderID != req.Ballot.NodeId {
		n.Logger.Log("PENDING", fmt.Sprintf("Resetting leader Id to new leader %d",
			req.Ballot.NodeId), n.ID)
		n.LeaderID = req.Ballot.NodeId
		var pendingForTransfer []*proto.Request
		if len(n.PendingRequests) > 0 {
			pendingForTransfer = make([]*proto.Request, len(n.PendingRequests))
			copy(pendingForTransfer, n.PendingRequests)
			n.clearPendingRequestsLocked()
			n.Logger.Log("PENDING", fmt.Sprintf("Captured %d pending requests to forward to new leader %d",
				len(pendingForTransfer), req.Ballot.NodeId), n.ID)
		}
		if len(pendingForTransfer) > 0 {
			go n.forwardPendingRequestsAfterNewView(n.LeaderID, pendingForTransfer)
		}
	}
	n.LastLeaderMessage = time.Now()

	go n.restartTimerIfNeeded()
}

func (n *Node) HandleNewView(ctx context.Context, req *proto.NewView) (*proto.Status, error) {

	if req == nil || req.Ballot == nil {
		n.Logger.Log("ERROR", "Received HandleNewView with nil request or ballot - ignoring", n.ID)
		return &proto.Status{
			Status:  "error",
			Message: "Invalid NewView message",
		}, nil
	}

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

func (n *Node) RequestCheckpoint(ctx context.Context, req *proto.CheckpointRequest) (*proto.CheckpointSnapshot, error) {

	if !n.IsNodeActive() {
		n.Logger.Log("RPC", fmt.Sprintf("Rejecting RequestCheckpoint seq=%d - node is inactive", req.Seq), n.ID)
		return nil, fmt.Errorf("node is inactive")
	}

	n.mu.RLock()
	seq := n.LastCheckpointSeq
	state := n.LastCheckpointState
	digest := n.LastCheckpointDigest
	n.mu.RUnlock()

	if seq != req.Seq || len(state) == 0 || len(digest) == 0 {

		n.mu.Lock()
		if n.ExecutedSeq >= req.Seq {
			bytes, dig, err := n.serializeSnapshotLocked()
			if err == nil {

				state = bytes
				digest = dig
				seq = n.ExecutedSeq
			}
		}
		n.mu.Unlock()
	}

	if len(state) == 0 || len(digest) == 0 {
		return nil, fmt.Errorf("no snapshot available")
	}

	sum := sha256.Sum256(state)
	if hex.EncodeToString(sum[:]) != hex.EncodeToString(digest) {
		return nil, fmt.Errorf("local snapshot digest mismatch")
	}

	return &proto.CheckpointSnapshot{Seq: seq, State: state, Digest: digest}, nil
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

	var pendingForTransfer []*proto.Request
	shouldStopTimer := false

	n.mu.Lock()
	n.InNewViewProcessing = true
	n.NewViewLog = append(n.NewViewLog, req)

	key := fmt.Sprintf("%d.%d", req.Ballot.Round, req.Ballot.NodeId)
	if n.NewViewReceived == nil {
		n.NewViewReceived = make(map[string]bool)
	}
	n.NewViewReceived[key] = true
	n.Logger.Log("NEWVIEW", fmt.Sprintf("Received NEW-VIEW from leader %d, ballot %d.%d",
		req.Ballot.NodeId, req.Ballot.Round, req.Ballot.NodeId), n.ID)

	if IsHigherBallot(req.Ballot, n.HighestBallotSeen) {
		n.HighestBallotSeen = req.Ballot
	}

	if n.IsLeader && req.Ballot.NodeId != n.ID {
		if n.ProposedBallotNumber == nil || IsHigherBallot(req.Ballot, n.ProposedBallotNumber) {
			n.Logger.Log("STEPDOWN",
				fmt.Sprintf("Stepping down as leader - received NEW-VIEW from node %d with ballot %d.%d",
					req.Ballot.NodeId, req.Ballot.Round, req.Ballot.NodeId), n.ID)
			if IsHigherBallot(req.Ballot, n.ProposedBallotNumber) {
				stepDownReason := fmt.Sprintf("received New-View with higher ballot %d.%d", req.Ballot.Round, req.Ballot.NodeId)
				n.mu.Unlock()
				n.stepDownLeader(stepDownReason, req.Ballot)
				n.mu.Lock()
			}
		}
	}

	if n.LeaderID != req.Ballot.NodeId {
		n.LeaderID = req.Ballot.NodeId
		shouldStopTimer = true
	}
	n.LastLeaderMessage = time.Now()

	if len(n.PendingRequests) > 0 {
		pendingForTransfer = make([]*proto.Request, len(n.PendingRequests))
		copy(pendingForTransfer, n.PendingRequests)
		n.clearPendingRequestsLocked()
		n.Logger.Log("PENDING", fmt.Sprintf("Captured %d pending requests to forward to new leader %d",
			len(pendingForTransfer), req.Ballot.NodeId), n.ID)
	}
	if n.LastCheckpointSeq < req.BaseCheckpointSeq {
		n.mu.Unlock()
		n.ensureCheckpointInstalled(req.BaseCheckpointSeq, req.BaseCheckpointDigest, req.Ballot.NodeId, req.Ballot)
		n.mu.Lock()
	}
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

	leaderID := n.LeaderID
	shouldStartTimer := !n.IsLeader && n.LeaderTimer != nil
	bufferedCommits := n.PendingCommitMessages
	n.PendingCommitMessages = nil
	n.InNewViewProcessing = false
	n.mu.Unlock()
	if shouldStopTimer {
		n.stopLeaderTimer()
	}

	if len(pendingForTransfer) > 0 {
		go n.forwardPendingRequestsAfterNewView(leaderID, pendingForTransfer)
	}

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
	n.mu.Lock()
	if n.RecoveryState == RecoveryInProgress {
		n.RecoveryState = RecoveryCompleted
		n.Logger.Log("RECOVERY", "Recovery state changed to completed", n.ID)
	}
	n.mu.Unlock()
	for _, pending := range bufferedCommits {
		go n.processCommitAsync(pending)
	}
	if len(acceptedMsgs) > 0 {
		if shouldStartTimer {
			go n.startTimerOnAccept()
		}
	}
}

func (n *Node) processNewViewAcceptMessage(accept *proto.Accept, alreadyExecuted bool) {

	n.rememberRequestLocked(accept.Request, accept.Sequence)

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

	if req == nil || req.Ballot == nil {
		n.Logger.Log("ERROR", "Received HandleAccepted with nil request or ballot - ignoring", n.ID)
		return &proto.Status{
			Status:  "error",
			Message: "Invalid Accepted message",
		}, nil
	}

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

	n.mu.Lock()
	isLeader := n.IsLeader
	leaderID := n.LeaderID
	n.mu.Unlock()

	if !isLeader {
		n.Logger.Log("IGNORE", fmt.Sprintf("Ignoring HandleAccepted for sequence %d from node %d - not leader", req.Sequence, req.NodeId), n.ID)
		return &proto.Status{
			Status:   "ignored",
			Message:  "Not leader",
			LeaderId: leaderID,
		}, nil
	}

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
		Ballot:               n.ProposedBallotNumber,
		AcceptLog:            rangedLog,
		BaseCheckpointSeq:    n.LastCheckpointSeq,
		BaseCheckpointDigest: n.LastCheckpointDigest,
	}

	n.Logger.Log("RECOVERY", fmt.Sprintf("Created NEW-VIEW for recovery with %d entries", len(newView.AcceptLog)), n.ID)

	return newView, nil
}
