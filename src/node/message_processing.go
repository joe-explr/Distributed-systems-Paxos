package node

import (
	"context"
	"fmt"
	"paxos-banking/proto"
	"paxos-banking/src/common"
)

func (n *Node) processMessages() {
	for {
		select {
		case request := <-n.requestChan:
			n.handleRequest(request)

		case accepted := <-n.acceptedChan:
			n.processAcceptedMessage(accepted)

		case promise := <-n.promiseChan:
			n.processPromiseMessage(promise.SenderID, promise.Promise)

		case prepareAck := <-n.prepareAckChan:
			n.processPrepareAckMessage(prepareAck.SenderID, prepareAck.PrepareAck)

		case <-n.stopChan:
			return
		}
	}
}

func (n *Node) handleRequest(request *proto.Request) {
	n.Logger.Log("REQUEST", fmt.Sprintf("Received request from client %s: %s->%s $%d",
		request.ClientId, request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount), n.ID)

	if !n.IsLeader && n.LeaderID == 0 {
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
		return
	}

	client := proto.NewNodeServiceClient(conn)
	_, err = client.HandleRequest(context.Background(), request)
	if err != nil {
		n.Logger.Log("ERROR", fmt.Sprintf("Failed to forward request: %v", err), n.ID)
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
