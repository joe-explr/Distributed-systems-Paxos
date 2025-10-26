package node

import (
	"context"
	"fmt"
	"paxos-banking/proto"
	"paxos-banking/src/common"
)

func (n *Node) processMessages() {
	n.Logger.Log("GOROUTINE", "processMessages goroutine started", n.ID)
	for {
		select {
		case request := <-n.requestChan:
			n.Logger.Log("DEQUEUE", fmt.Sprintf("Dequeued request from requestChan: %s->%s $%d",
				request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount), n.ID)
			n.handleRequest(request)

		case accepted := <-n.acceptedChan:
			n.processAcceptedMessage(accepted)

		case promise := <-n.promiseChan:
			n.processPromiseMessage(promise.SenderID, promise.Promise)

		case prepareAck := <-n.prepareAckChan:
			n.processPrepareAckMessage(prepareAck.SenderID, prepareAck.PrepareAck)

		case <-n.stopChan:
			n.Logger.Log("GOROUTINE", "processMessages goroutine exiting via stopChan", n.ID)
			return
		}
	}
}

func (n *Node) handleRequest(request *proto.Request) {
	n.Logger.Log("REQUEST", fmt.Sprintf("Received request from client %s: %s->%s $%d",
		request.ClientId, request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount), n.ID)

	rid := makeRequestID(request)

	if rid != "" {
		n.mu.Lock()
		// De duplication checks only
		lastTs := n.LastClientTimestamp[request.ClientId]
		n.Logger.Log("DEDUP", fmt.Sprintf("Client %s incoming ts=%d last ts=%d (seq map entries=%d)",
			request.ClientId, request.Timestamp, lastTs, len(n.RequestIDToSeq)), n.ID)

		if seq, ok := n.RequestIDToSeq[rid]; ok {
			n.Logger.Log("DEDUP", fmt.Sprintf("Request %s already accepted with sequence %d", rid, seq), n.ID)

			cached := n.LastClientReply[request.ClientId]
			if cached != nil && cached.Timestamp == request.Timestamp {
				original := request
				n.mu.Unlock()

				n.Logger.Log("DEDUP", fmt.Sprintf("Sending cached reply for client %s timestamp %d", request.ClientId, request.Timestamp), n.ID)
				go n.sendReplyToClient(original, cached.Result, cached.Message, original.Transaction, 0, 0)
				return
			}
			n.mu.Unlock()
			n.Logger.Log("DEDUP", fmt.Sprintf("Request is duplicate , not yet found in cache for client %s timestamp %d", request.ClientId, request.Timestamp), n.ID)
			return
		}

		n.Logger.Log("REQUEST", fmt.Sprintf("Request not yet accepted, allowing processing: %s->%s $%d (timestamp: %d, last seen: %d)",
			request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount, request.Timestamp, lastTs), n.ID)
		// Recovery Gate
		if n.RecoveryState == RecoveryInProgress {
			leaderID := n.LeaderID
			isLeader := n.IsLeader
			if leaderID > 0 && !isLeader {
				n.mu.Unlock()
				n.Logger.Log("RECOVERY", fmt.Sprintf("Forwarding request %s during recovery to leader %d", rid, leaderID), n.ID)
				n.forwardToLeader(request)
				return
			}

			n.enqueuePendingRequestLocked(request)
			n.mu.Unlock()
			n.Logger.Log("RECOVERY", fmt.Sprintf("Queuing request %s while recovery is in progress", rid), n.ID)
			return
		}

		n.Logger.Log("REQUEST", fmt.Sprintf("Request is not a duplicate - continuing with processing: %s->%s $%d",
			request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount), n.ID)

		// To check if the node is no longer a leader when processing the queue
		// Enqueue the request to pending and restart election timer if no timer is running.
		if !n.IsLeader && n.LeaderID == 0 {
			n.Logger.Log("ELECTION", fmt.Sprintf("No leader identified; queuing request %s (client %s) and triggering election",
				rid, request.ClientId), n.ID)

			n.Logger.Log("ELECTION", "Not a leader, initiating leader election timer", n.ID)

			n.enqueuePendingRequestLocked(request)
			n.mu.Unlock()
			n.restartTimerIfNeeded()
			return
		}

		if !n.IsLeader {
			n.forwardToLeader(request)
			return
		}
		n.Logger.Log("REQUEST", fmt.Sprintf("Processing request as leader: %s->%s $%d",
			request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount), n.ID)

		n.processAsLeaderLocked(request)
	}
	n.Logger.Log("REQUEST", "Invalid nil Request received", n.ID)
	n.mu.Unlock()
}

func (n *Node) forwardToLeader(request *proto.Request) {
	rid := makeRequestID(request)
	if rid == "" {
		return
	}

	if n.LeaderID == n.ID {
		n.Logger.Log("ERROR", "Cannot forward to self", n.ID)
		return
	}

	conn, err := n.getConnection(n.LeaderID)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to connect to leader %d: %v", n.LeaderID, err), n.ID)

		oldLeaderID := n.LeaderID
		go func() {
			n.mu.Lock()
			n.LeaderID = 0
			n.enqueuePendingRequestLocked(request)
			n.mu.Unlock()
			n.Logger.Log("LEADER", fmt.Sprintf("Cleared LeaderID due to connection failure to node %d", oldLeaderID), n.ID)

			n.Logger.Log("ELECTION", "Initiating leader election due to connection failure", n.ID)
			n.restartTimerIfNeeded()
		}()
		return
	}

	client := proto.NewNodeServiceClient(conn)
	_, err = client.HandleRequest(context.Background(), request)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to forward request: %v", err), n.ID)

		go func() {
			n.mu.Lock()
			n.LeaderID = 0
			n.enqueuePendingRequestLocked(request)
			n.mu.Unlock()
			n.Logger.Log("LEADER", fmt.Sprintf("Cleared LeaderID due to leader failure: %v", err), n.ID)

			n.Logger.Log("ELECTION", "Initiating leader election due to leader processing failure", n.ID)
			n.restartTimerIfNeeded()
		}()
		return
	}

	n.Logger.Log("FORWARD", fmt.Sprintf("Forwarded request %s (client %s) to leader %d",
		rid, request.ClientId, n.LeaderID), n.ID)
}

func (n *Node) forwardPendingRequestsAfterNewView(leaderID int32, pendingRequests []*proto.Request) {
	if len(pendingRequests) == 0 {
		return
	}

	if leaderID == 0 {
		n.Logger.Log("FORWARD", fmt.Sprintf("Leader unknown; re-queuing %d pending requests", len(pendingRequests)), n.ID)
		n.mu.Lock()
		n.enqueuePendingRequestsLocked(pendingRequests)
		n.mu.Unlock()
		return
	}

	if leaderID == n.ID {
		n.Logger.Log("LEADER", fmt.Sprintf("Processing %d pending requests locally after NEW-VIEW", len(pendingRequests)), n.ID)
		for _, req := range pendingRequests {
			n.handleRequest(req)
		}
		return
	}

	n.Logger.Log("FORWARD", fmt.Sprintf("Forwarding %d pending requests to new leader %d", len(pendingRequests), leaderID), n.ID)
	for _, req := range pendingRequests {
		n.forwardToLeader(req)
	}
}

func (n *Node) processPrepareAckMessage(senderId int32, prepareAck *proto.PrepareAck) {
	n.Logger.Log("PREPARE_ACK", fmt.Sprintf("Received prepare ack from node %d: queued=%v, message=%s",
		senderId, prepareAck.Queued, prepareAck.Message), n.ID)

	n.mu.Lock()
	stepDown := false
	if n.ProposedBallotNumber != nil && prepareAck.Ballot != nil {
		if IsHigherBallot(prepareAck.Ballot, n.ProposedBallotNumber) {
			if IsHigherBallot(prepareAck.Ballot, n.HighestBallotSeen) {
				n.HighestBallotSeen = prepareAck.Ballot
			}
			n.resetElectionTrackingLocked()
			stepDown = true
		}
	}
	n.mu.Unlock()
	if stepDown {
		n.stepDownLeader("", n.HighestBallotSeen)
	}
}

func (n *Node) processAsLeaderLocked(request *proto.Request) {

	n.Logger.Log("LEADER", "Processing request as leader", n.ID)

	accept := &proto.Accept{
		Ballot:   n.ProposedBallotNumber,
		Sequence: n.SequenceNum,
		Request:  request,
	}

	acceptLogEntry := &proto.AcceptLogEntry{
		AcceptNum: n.ProposedBallotNumber,
		AcceptSeq: n.SequenceNum,
		AcceptVal: request,
	}
	n.AcceptLog = append(n.AcceptLog, acceptLogEntry)

	n.rememberRequestLocked(request, n.SequenceNum)
	n.updateTransactionStatusLocked(n.SequenceNum, request, common.Accepted, n.ProposedBallotNumber)

	n.AcceptedTransactions[n.SequenceNum] = request

	n.PendingClientReplies[n.SequenceNum] = request

	if n.AcceptedBy[n.SequenceNum] == nil {
		n.AcceptedBy[n.SequenceNum] = make(map[int32]bool)
	}
	n.AcceptedBy[n.SequenceNum][n.ID] = true

	n.broadcastAccept(accept)

	n.SequenceNum++

	n.Logger.Log("LEADER", fmt.Sprintf("Broadcasted accept for sequence %d", accept.Sequence), n.ID)
}
