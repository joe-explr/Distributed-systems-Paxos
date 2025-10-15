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

		lastTs := n.LastClientTimestamp[request.ClientId]
		n.Logger.Log("REQUEST", fmt.Sprintf("Checking for duplicate request from client %s: %s->%s $%d",
			request.ClientId, request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount), n.ID)

		if request.Timestamp < lastTs {
			n.Logger.Log("REQUEST", fmt.Sprintf("Request is a duplicate - client timestamp regressed: %s->%s $%d (timestamp %d < last seen %d)",
				request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount, request.Timestamp, lastTs), n.ID)
			n.mu.Unlock()

			return
		}

		if seq, ok := n.RequestIDToSeq[rid]; ok {
			n.Logger.Log("REQUEST", fmt.Sprintf("Request already accepted with sequence %d: %s->%s $%d",
				seq, request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount), n.ID)

			cached := n.LastClientReply[request.ClientId]
			if cached != nil && cached.Timestamp == request.Timestamp {
				original := request
				n.mu.Unlock()

				go n.sendReplyToClient(original, cached.Result, cached.Message, original.Transaction, 0, 0)
				return
			}
			status := n.TransactionStatus[seq]
			n.mu.Unlock()
			if status != nil {
				if status.Status == common.Executed || status.Status == common.Committed {

					go n.checkAndRetryWaitingTransactions()
					return
				}
			}

			return
		}

		n.Logger.Log("REQUEST", fmt.Sprintf("Request not yet accepted, allowing processing: %s->%s $%d (timestamp: %d, last seen: %d)",
			request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount, request.Timestamp, lastTs), n.ID)
		n.mu.Unlock()
	}
	n.Logger.Log("REQUEST", fmt.Sprintf("Request is not a duplicate - continuing with processing: %s->%s $%d",
		request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount), n.ID)
	if !n.IsLeader && n.LeaderID == 0 {
		n.Logger.Log("REQUEST", fmt.Sprintf("Not a leader, initiating leader election: %s->%s $%d",
			request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount), n.ID)
		n.mu.RLock()
		recovering := n.RecoveryState == RecoveryInProgress
		n.mu.RUnlock()
		if recovering {
			n.Logger.Log("ELECTION", "Skipping election on request - recovery in progress", n.ID)

			n.mu.Lock()
			n.PendingRequests = append(n.PendingRequests, request)
			n.mu.Unlock()
			return
		}
		n.Logger.Log("ELECTION", "Not a leader, initiating leader election", n.ID)

		n.mu.Lock()
		n.PendingRequests = append(n.PendingRequests, request)
		n.mu.Unlock()

		go n.initiateLeaderElection()
		return
	}

	if !n.IsLeader {
		n.forwardToLeader(request)
		return
	}
	n.Logger.Log("REQUEST", fmt.Sprintf("Processing request as leader: %s->%s $%d",
		request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount), n.ID)
	n.processAsLeader(request)
}

func (n *Node) forwardToLeader(request *proto.Request) {

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
			n.mu.Unlock()
			n.Logger.Log("LEADER", fmt.Sprintf("Cleared LeaderID due to connection failure to node %d", oldLeaderID), n.ID)

			n.mu.Lock()
			n.PendingRequests = append(n.PendingRequests, request)
			n.mu.Unlock()
			n.Logger.Log("ELECTION", "Initiating leader election due to connection failure", n.ID)
			n.initiateLeaderElection()
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
			n.mu.Unlock()
			n.Logger.Log("LEADER", fmt.Sprintf("Cleared LeaderID due to leader failure: %v", err), n.ID)

			n.mu.Lock()
			n.PendingRequests = append(n.PendingRequests, request)
			n.mu.Unlock()
			n.Logger.Log("ELECTION", "Initiating leader election due to leader processing failure", n.ID)
			n.initiateLeaderElection()
		}()
	}
}

func (n *Node) processPrepareAckMessage(senderId int32, prepareAck *proto.PrepareAck) {
	n.Logger.Log("PREPARE_ACK", fmt.Sprintf("Received prepare ack from node %d: queued=%v, message=%s",
		senderId, prepareAck.Queued, prepareAck.Message), n.ID)

	n.mu.Lock()
	shouldRestart := false
	if n.ProposedBallotNumber != nil && prepareAck.Ballot != nil {
		if IsHigherBallot(prepareAck.Ballot, n.ProposedBallotNumber) {
			if IsHigherBallot(prepareAck.Ballot, n.HighestBallotSeen) {
				n.HighestBallotSeen = prepareAck.Ballot
			}
			n.resetElectionTrackingLocked()
			shouldRestart = true
		}
	}
	n.mu.Unlock()
	if shouldRestart {
		go n.initiateLeaderElection()
	}
}

func (n *Node) processAsLeader(request *proto.Request) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Logger.Log("LEADER", "Processing request as leader", n.ID)

	if n.ProposedBallotNumber == nil || !n.IsLeader {
		n.Logger.Log("LEADER", fmt.Sprintf("Aborting processAsLeader - no longer leader (ballot nil: %v, IsLeader: %v)",
			n.ProposedBallotNumber == nil, n.IsLeader), n.ID)

		n.PendingRequests = append(n.PendingRequests, request)
		return
	}

	rid := makeRequestID(request)
	if rid != "" {
		if seq, ok := n.RequestIDToSeq[rid]; ok {
			if info, exists := n.TransactionStatus[seq]; exists {
				if info.Status == common.Executed || info.Status == common.Committed {
					go n.checkAndRetryWaitingTransactions()
					return
				}
			}

			return
		}
	}

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
