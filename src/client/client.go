package client

import (
	"context"
	"fmt"
	"log"
	"net"
	"paxos-banking/proto"
	"paxos-banking/src/common"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type queuedRequest struct {
	request *proto.Request
	isAsync bool
	onError func(error) 
}

type Client struct {
	ID      string
	Address string
	Port    int32
	Config  *common.Config

	Connections map[int32]*grpc.ClientConn

	CurrentLeaderID int32

	RetryTimer    *time.Timer
	RetryDuration time.Duration

	AsyncTimeout time.Duration

	RequestQueue []*queuedRequest

	ProcessedRequests map[int64]*proto.Reply

	CurrentInFlight *queuedRequest

	IsProcessing bool
	InflightDone chan struct{}

	Logger *common.Logger

	Server *grpc.Server

	mu sync.Mutex

	proto.UnimplementedClientServiceServer
}

func NewClient(id string, config *common.Config) *Client {
	client := &Client{
		ID:                id,
		Config:            config,
		Connections:       make(map[int32]*grpc.ClientConn),
		CurrentLeaderID:   1,
		RetryDuration:     5 * time.Second,
		AsyncTimeout:      7 * time.Second,
		RequestQueue:      make([]*queuedRequest, 0),
		ProcessedRequests: make(map[int64]*proto.Reply),
		Logger:            common.NewLogger(),
		InflightDone:      make(chan struct{}, 1),
	}

	if err := client.Logger.EnableClientFileLogging(id); err != nil {
		fmt.Printf("Warning: Failed to enable file logging for client %s: %v\n", id, err)
	}

	client.Logger.LogClient("INIT", fmt.Sprintf("Client %s initialized", id), id)
	return client
}

func (c *Client) Start() error {
	c.Logger.LogClient("START", fmt.Sprintf("Starting client %s on port %d", c.ID, c.Port), c.ID)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", c.Port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", c.Port, err)
	}

	c.Server = grpc.NewServer()
	proto.RegisterClientServiceServer(c.Server, c)

	go func() {
		if err := c.Server.Serve(lis); err != nil {
			log.Printf("Failed to serve client %s: %v", c.ID, err)
		}
	}()

	c.Logger.LogClient("START", "Client server started successfully", c.ID)
	return nil
}

func (c *Client) Stop() {
	c.Logger.LogClient("STOP", "Stopping client", c.ID)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.RetryTimer != nil {
		c.RetryTimer.Stop()
		c.RetryTimer = nil
	}

	if c.Server != nil {
		c.Server.Stop()
	}

	for _, conn := range c.Connections {
		conn.Close()
	}
}

func (c *Client) SendTransaction(sender, receiver string, amount int32) error {

	timestamp := time.Now().UnixNano()

	request := &proto.Request{
		ClientId:  c.ID,
		Timestamp: timestamp,
		Transaction: &proto.Transaction{
			Sender:   sender,
			Receiver: receiver,
			Amount:   amount,
		},
	}

	c.mu.Lock()

	queued := &queuedRequest{request: request, isAsync: false}
	c.RequestQueue = append(c.RequestQueue, queued)
	queueLength := len(c.RequestQueue)
	shouldStartProcessing := !c.IsProcessing
	c.mu.Unlock()

	c.Logger.LogClient("QUEUE", fmt.Sprintf("Added transaction to queue: %s->%s $%d (timestamp: %d, queue length: %d)",
		sender, receiver, amount, timestamp, queueLength), c.ID)

	if shouldStartProcessing {
		go c.processQueue()
	}

	return nil
}

func (c *Client) SendTransactionAsync(sender, receiver string, amount int32) error {

	timestamp := time.Now().UnixNano()

	request := &proto.Request{
		ClientId:  c.ID,
		Timestamp: timestamp,
		Transaction: &proto.Transaction{
			Sender:   sender,
			Receiver: receiver,
			Amount:   amount,
		},
	}

	c.mu.Lock()

	onErr := func(err error) {
		c.Logger.Log("ERROR", fmt.Sprintf("Async transaction failed: %s->%s $%d - %v",
			sender, receiver, amount, err), 0)
	}
	queued := &queuedRequest{request: request, isAsync: true, onError: onErr}
	c.RequestQueue = append(c.RequestQueue, queued)
	queueLength := len(c.RequestQueue)
	shouldStartProcessing := !c.IsProcessing
	c.mu.Unlock()

	c.Logger.LogClient("QUEUE", fmt.Sprintf("Added transaction to async queue: %s->%s $%d (timestamp: %d, queue length: %d)",
		sender, receiver, amount, timestamp, queueLength), c.ID)

	if shouldStartProcessing {
		go c.processQueue()
	}

	return nil
}

func (c *Client) processQueue() {
	for {
		c.mu.Lock()

		if len(c.RequestQueue) == 0 {
			c.IsProcessing = false
			c.mu.Unlock()
			c.Logger.LogClient("QUEUE", "Request queue empty, stopping processing", c.ID)
			return
		}

		q := c.RequestQueue[0]
		c.RequestQueue = c.RequestQueue[1:]
		c.CurrentInFlight = q
		c.IsProcessing = true

		if q.isAsync {
			c.Logger.LogClient("PROCESS", fmt.Sprintf("Processing async request: %s->%s $%d (timestamp: %d, remaining in queue: %d)",
				q.request.Transaction.Sender, q.request.Transaction.Receiver, q.request.Transaction.Amount,
				q.request.Timestamp, len(c.RequestQueue)), c.ID)
		} else {
			c.Logger.LogClient("PROCESS", fmt.Sprintf("Processing request: %s->%s $%d (timestamp: %d, remaining in queue: %d)",
				q.request.Transaction.Sender, q.request.Transaction.Receiver, q.request.Transaction.Amount,
				q.request.Timestamp, len(c.RequestQueue)), c.ID)
		}

		c.startRetryTimerUnsafe()

		go func(req *proto.Request) {
			err := c.sendRequestToLeader(req)
			if err != nil {
				c.Logger.LogClient("ERROR", fmt.Sprintf("Failed to send request to leader: %v", err), c.ID)

				c.handleRequestTimeout()
			}
		}(q.request)

		c.mu.Unlock()

		if q.isAsync {
			select {
			case <-c.InflightDone:

				c.Logger.LogClient("PROCESS", fmt.Sprintf("Async request completed: %s->%s $%d",
					q.request.Transaction.Sender, q.request.Transaction.Receiver, q.request.Transaction.Amount), c.ID)
			case <-time.After(c.AsyncTimeout):
				c.Logger.LogClient("TIMEOUT", fmt.Sprintf("Async request timeout after %v: %s->%s $%d",
					c.AsyncTimeout, q.request.Transaction.Sender, q.request.Transaction.Receiver, q.request.Transaction.Amount), c.ID)

				c.mu.Lock()

				if c.CurrentInFlight == q {
					c.stopRetryTimerUnsafe()
					c.CurrentInFlight = nil
				}
				c.mu.Unlock()

				if q.onError != nil {
					q.onError(fmt.Errorf("request timeout after %v", c.AsyncTimeout))
				}
			}
		} else {
			<-c.InflightDone
		}
	}
}

func (c *Client) sendRequestToLeader(request *proto.Request) error {

	conn, err := c.getConnection(c.CurrentLeaderID)
	if err != nil {
		return fmt.Errorf("failed to connect to leader node %d: %v", c.CurrentLeaderID, err)
	}

	client := proto.NewPaxosServiceClient(conn)

	c.Logger.LogClient("SEND", fmt.Sprintf("Sending request to leader node %d (timestamp: %d)",
		c.CurrentLeaderID, request.Timestamp), c.ID)

	ctx, cancel := context.WithTimeout(context.Background(), c.RetryDuration)
	defer cancel()

	status, err := client.SendRequest(ctx, request)
	if err != nil {
		return fmt.Errorf("leader node %d failed: %v", c.CurrentLeaderID, err)
	}

	switch status.Status {
	case "OK_ACCEPTED":
		if status.LeaderId != 0 {
			c.CurrentLeaderID = status.LeaderId
			c.Logger.LogClient("LEADER", fmt.Sprintf("Updated current leader to node %d (from status)", c.CurrentLeaderID), c.ID)
		}

		return nil
	case "INACTIVE":
		return fmt.Errorf("leader node %d is inactive", c.CurrentLeaderID)
	case "NOT_LEADER":
		if status.LeaderId != 0 {
			c.CurrentLeaderID = status.LeaderId
			c.Logger.LogClient("NOT_LEADER", fmt.Sprintf("Node %d is not leader; hinted leader is %d", c.CurrentLeaderID, status.LeaderId), c.ID)
		}
		return nil
	case "PENDING":
		c.Logger.LogClient("PENDING", fmt.Sprintf("Leader node %d is pending; leaderId is %d", c.CurrentLeaderID, status.LeaderId), c.ID)
		return nil
	default:
		return fmt.Errorf("unexpected status from leader %d: %s - %s", c.CurrentLeaderID, status.Status, status.Message)
	}
}

func (c *Client) startRetryTimerUnsafe() {

	if c.RetryTimer != nil {
		c.RetryTimer.Stop()
	}

	c.RetryTimer = time.AfterFunc(c.RetryDuration, func() {
		c.handleRequestTimeout()
	})
	c.Logger.LogClient("TIMER", fmt.Sprintf("Retry timer started (%v timeout)", c.RetryDuration), c.ID)
}

func (c *Client) stopRetryTimerUnsafe() {
	if c.RetryTimer != nil {
		c.RetryTimer.Stop()
		c.RetryTimer = nil
		c.Logger.LogClient("TIMER", "Retry timer stopped", c.ID)
	}
}

func (c *Client) handleRequestTimeout() {
	c.mu.Lock()
	current := c.CurrentInFlight
	oldLeader := c.CurrentLeaderID
	c.CurrentLeaderID = 0
	c.mu.Unlock()

	if current == nil {
		c.Logger.LogClient("TIMEOUT", "No current request to retry", c.ID)
		return
	}

	c.Logger.LogClient("TIMEOUT", fmt.Sprintf("Request timeout - leader %d failed, broadcasting to all nodes (timestamp: %d)",
		oldLeader, current.request.Timestamp), c.ID)

	c.broadcastRequestToAllNodes(current.request)

	c.mu.Lock()
	c.startRetryTimerUnsafe()
	c.mu.Unlock()
}

func (c *Client) broadcastRequestToAllNodes(request *proto.Request) {
	c.Logger.LogClient("BROADCAST", fmt.Sprintf("Broadcasting request to all nodes (timestamp: %d)", request.Timestamp), c.ID)

	var wg sync.WaitGroup
	for _, nodeInfo := range c.Config.Nodes {
		wg.Add(1)
		go func(nodeInfo common.NodeInfo) {
			defer wg.Done()
			err := c.sendRequestToNode(request, nodeInfo.ID)
			if err != nil {
				c.Logger.LogClient("ERROR", fmt.Sprintf("Failed to send to node %d: %v", nodeInfo.ID, err), c.ID)
			} else {
				c.Logger.LogClient("BROADCAST", fmt.Sprintf("Successfully sent to node %d", nodeInfo.ID), c.ID)
			}
		}(nodeInfo)
	}

	go func() {
		wg.Wait()
		c.Logger.LogClient("BROADCAST", "Broadcast to all nodes completed", c.ID)
	}()
}

func (c *Client) sendRequestToNode(request *proto.Request, nodeID int32) error {
	conn, err := c.getConnection(nodeID)
	if err != nil {
		return fmt.Errorf("failed to connect to node %d: %v", nodeID, err)
	}

	client := proto.NewPaxosServiceClient(conn)
	status, err := client.SendRequest(context.Background(), request)
	if err != nil {
		return err
	}
	switch status.Status {
	case "OK_ACCEPTED":

		if status.LeaderId != 0 {
			c.mu.Lock()
			c.CurrentLeaderID = status.LeaderId
			c.mu.Unlock()
			c.Logger.LogClient("LEADER", fmt.Sprintf("Adopted leader %d from node %d OK_ACCEPTED", status.LeaderId, nodeID), c.ID)
		} else {
			c.mu.Lock()
			c.CurrentLeaderID = nodeID
			c.mu.Unlock()
			c.Logger.LogClient("LEADER", fmt.Sprintf("Adopted node %d as leader (no hint)", nodeID), c.ID)
		}
		return nil
	case "INACTIVE", "NOT_LEADER":
		return fmt.Errorf("node %d returned %s", nodeID, status.Status)
	default:
		return fmt.Errorf("node %d returned unexpected status: %s - %s", nodeID, status.Status, status.Message)
	}
}

func (c *Client) SendReply(ctx context.Context, reply *proto.Reply) (*proto.Status, error) {
	c.Logger.Log("REPLY", fmt.Sprintf("Received reply: Result=%v, Message=%s, Timestamp=%d",
		reply.Result, reply.Message, reply.Timestamp), 0)

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.ProcessedRequests[reply.Timestamp]; exists {
		c.Logger.Log("DUPLICATE", fmt.Sprintf("Reply for timestamp %d already processed, ignoring", reply.Timestamp), 0)
		return &proto.Status{Status: "success", Message: "Duplicate reply ignored"}, nil
	}

	if c.CurrentInFlight != nil && c.CurrentInFlight.request.Timestamp == reply.Timestamp {
		c.processCurrentReplyUnsafeUnified(reply)
	} else {

		c.Logger.Log("STALE", fmt.Sprintf("Reply for timestamp %d is not for current request", reply.Timestamp), 0)
	}

	return &proto.Status{Status: "success", Message: "Reply processed"}, nil
}

func (c *Client) processCurrentReplyUnsafeUnified(reply *proto.Reply) {
	if c.CurrentInFlight == nil {
		return
	}
	if c.CurrentInFlight.isAsync {
		c.Logger.Log("PROCESS", fmt.Sprintf("Processing reply for current async request (timestamp: %d)", reply.Timestamp), 0)
	} else {
		c.Logger.Log("PROCESS", fmt.Sprintf("Processing reply for current request (timestamp: %d)", reply.Timestamp), 0)
	}

	if reply.Ballot != nil {
		c.CurrentLeaderID = reply.Ballot.NodeId
		if c.CurrentInFlight.isAsync {
			c.Logger.Log("LEADER", fmt.Sprintf("Updated current leader to node %d (async)", c.CurrentLeaderID), 0)
		} else {
			c.Logger.Log("LEADER", fmt.Sprintf("Updated current leader to node %d", c.CurrentLeaderID), 0)
		}
	}

	c.stopRetryTimerUnsafe()

	c.ProcessedRequests[reply.Timestamp] = reply

	c.CurrentInFlight = nil

	if c.CurrentInFlight != nil && c.CurrentInFlight.isAsync {
		c.Logger.Log("COMPLETE", fmt.Sprintf("Async request completed: %s (Result: %v)", reply.Message, reply.Result), 0)
	} else {
		c.Logger.Log("COMPLETE", fmt.Sprintf("Request completed: %s (Result: %v)", reply.Message, reply.Result), 0)
	}

	select {
	case c.InflightDone <- struct{}{}:
	default:
	}
}

func (c *Client) getConnection(nodeID int32) (*grpc.ClientConn, error) {

	if conn, exists := c.Connections[nodeID]; exists {
		return conn, nil
	}

	var nodeInfo *common.NodeInfo
	for _, info := range c.Config.Nodes {
		if info.ID == nodeID {
			nodeInfo = &info
			break
		}
	}

	if nodeInfo == nil {
		return nil, fmt.Errorf("node %d not found in config", nodeID)
	}

	address := fmt.Sprintf("%s:%d", nodeInfo.Address, nodeInfo.Port)
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	c.Connections[nodeID] = conn
	return conn, nil
}

func (c *Client) GetStatus() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return fmt.Sprintf("Client %s: Queue=%d, Processed=%d, Leader=Node%d, Processing=%v",
		c.ID, len(c.RequestQueue), len(c.ProcessedRequests), c.CurrentLeaderID, c.IsProcessing)
}

func (c *Client) PrintLog() {
	c.Logger.LogClient("INFO", "Printing client log", c.ID)
	fmt.Printf("=== Client %s Log ===\n", c.ID)
	c.Logger.PrintLog()
}
