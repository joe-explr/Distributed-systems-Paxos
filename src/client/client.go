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

	RequestQueue      []*proto.Request
	AsyncRequestQueue []*proto.Request

	ProcessedRequests map[int64]*proto.Reply

	CurrentRequest      *proto.Request
	CurrentAsyncRequest *proto.Request

	IsProcessing        bool
	IsAsyncProcessing   bool
	ProcessingDone      chan struct{}
	AsyncProcessingDone chan struct{}

	AsyncErrorCallbacks map[int64]func(error)

	Logger *common.Logger

	Server *grpc.Server

	mu sync.Mutex

	proto.UnimplementedClientServiceServer
}

func NewClient(id string, config *common.Config) *Client {
	client := &Client{
		ID:                  id,
		Config:              config,
		Connections:         make(map[int32]*grpc.ClientConn),
		CurrentLeaderID:     1,
		RetryDuration:       3 * time.Second,
		AsyncTimeout:        7 * time.Second,
		RequestQueue:        make([]*proto.Request, 0),
		AsyncRequestQueue:   make([]*proto.Request, 0),
		ProcessedRequests:   make(map[int64]*proto.Reply),
		AsyncErrorCallbacks: make(map[int64]func(error)),
		Logger:              common.NewLogger(),
		ProcessingDone:      make(chan struct{}, 1),
		AsyncProcessingDone: make(chan struct{}, 1),
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
	c.Logger.Log("STOP", "Stopping client", 0)

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

	c.RequestQueue = append(c.RequestQueue, request)
	queueLength := len(c.RequestQueue)
	shouldStartProcessing := !c.IsProcessing
	c.mu.Unlock()

	c.Logger.Log("QUEUE", fmt.Sprintf("Added transaction to queue: %s->%s $%d (timestamp: %d, queue length: %d)",
		sender, receiver, amount, timestamp, queueLength), 0)

	if shouldStartProcessing {
		go c.processRequestQueue()
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

	c.AsyncRequestQueue = append(c.AsyncRequestQueue, request)
	queueLength := len(c.AsyncRequestQueue)
	shouldStartProcessing := !c.IsAsyncProcessing

	c.AsyncErrorCallbacks[timestamp] = func(err error) {
		c.Logger.Log("ERROR", fmt.Sprintf("Async transaction failed: %s->%s $%d - %v",
			sender, receiver, amount, err), 0)
	}
	c.mu.Unlock()

	c.Logger.Log("QUEUE", fmt.Sprintf("Added transaction to async queue: %s->%s $%d (timestamp: %d, queue length: %d)",
		sender, receiver, amount, timestamp, queueLength), 0)

	if shouldStartProcessing {
		go c.processRequestQueueAsync()
	}

	return nil
}

func (c *Client) processRequestQueue() {
	for {
		c.mu.Lock()

		if len(c.RequestQueue) == 0 {
			c.IsProcessing = false
			c.mu.Unlock()
			c.Logger.Log("QUEUE", "Request queue empty, stopping processing", 0)
			return
		}

		request := c.RequestQueue[0]
		c.RequestQueue = c.RequestQueue[1:]
		c.CurrentRequest = request
		c.IsProcessing = true

		c.Logger.Log("PROCESS", fmt.Sprintf("Processing request: %s->%s $%d (timestamp: %d, remaining in queue: %d)",
			request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount,
			request.Timestamp, len(c.RequestQueue)), 0)

		c.startRetryTimerUnsafe()

		go func(req *proto.Request) {
			err := c.sendRequestToLeader(req)
			if err != nil {
				c.Logger.Log("ERROR", fmt.Sprintf("Failed to send request to leader: %v", err), 0)

				c.handleRequestTimeout()
			}
		}(request)

		c.mu.Unlock()

		<-c.ProcessingDone
	}
}

func (c *Client) processRequestQueueAsync() {
	for {
		c.mu.Lock()

		if len(c.AsyncRequestQueue) == 0 {
			c.IsAsyncProcessing = false
			c.mu.Unlock()
			c.Logger.Log("QUEUE", "Async request queue empty, stopping async processing", 0)
			return
		}

		request := c.AsyncRequestQueue[0]
		c.AsyncRequestQueue = c.AsyncRequestQueue[1:]
		c.CurrentAsyncRequest = request
		c.IsAsyncProcessing = true

		c.Logger.Log("PROCESS", fmt.Sprintf("Processing async request: %s->%s $%d (timestamp: %d, remaining in async queue: %d)",
			request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount,
			request.Timestamp, len(c.AsyncRequestQueue)), 0)

		c.startRetryTimerUnsafe()

		go func(req *proto.Request) {
			err := c.sendRequestToLeader(req)
			if err != nil {
				c.Logger.Log("ERROR", fmt.Sprintf("Failed to send request to leader: %v", err), 0)

				c.handleRequestTimeout()
			}
		}(request)

		c.mu.Unlock()

		select {
		case <-c.AsyncProcessingDone:

			c.Logger.Log("PROCESS", fmt.Sprintf("Async request completed: %s->%s $%d",
				request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount), 0)

			c.cleanupAsyncRequest(request.Timestamp)
		case <-time.After(c.AsyncTimeout):

			c.Logger.Log("TIMEOUT", fmt.Sprintf("Async request timeout after %v: %s->%s $%d",
				c.AsyncTimeout, request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount), 0)

			c.mu.Lock()
			if callback, exists := c.AsyncErrorCallbacks[request.Timestamp]; exists {
				callback(fmt.Errorf("request timeout after %v", c.AsyncTimeout))
			}
			c.CurrentAsyncRequest = nil
			c.mu.Unlock()

			c.cleanupAsyncRequest(request.Timestamp)
		}
	}
}

func (c *Client) cleanupAsyncRequest(timestamp int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.AsyncErrorCallbacks, timestamp)

	c.stopRetryTimerUnsafe()

	c.Logger.Log("CLEANUP", fmt.Sprintf("Cleaned up async request resources (timestamp: %d)", timestamp), 0)
}

func (c *Client) sendRequestToLeader(request *proto.Request) error {

	conn, err := c.getConnection(c.CurrentLeaderID)
	if err != nil {
		return fmt.Errorf("failed to connect to leader node %d: %v", c.CurrentLeaderID, err)
	}

	client := proto.NewPaxosServiceClient(conn)

	c.Logger.Log("SEND", fmt.Sprintf("Sending request to leader node %d (timestamp: %d)",
		c.CurrentLeaderID, request.Timestamp), 0)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	reply, err := client.SendRequest(ctx, request)
	if err != nil {
		return fmt.Errorf("leader node %d failed: %v", c.CurrentLeaderID, err)
	}

	if reply.Message == "Node is down" || reply.Result == false {
		c.Logger.Log("LEADER_DOWN", fmt.Sprintf("Leader %d responded with failure: %s", c.CurrentLeaderID, reply.Message), 0)
		return fmt.Errorf("leader node %d is down: %s", c.CurrentLeaderID, reply.Message)
	}

	return nil
}

func (c *Client) startRetryTimerUnsafe() {

	if c.RetryTimer != nil {
		c.RetryTimer.Stop()
	}

	c.RetryTimer = time.AfterFunc(2*time.Second, func() {
		c.handleRequestTimeout()
	})
	c.Logger.Log("TIMER", "Retry timer started (2s timeout)", 0)
}

func (c *Client) stopRetryTimerUnsafe() {
	if c.RetryTimer != nil {
		c.RetryTimer.Stop()
		c.RetryTimer = nil
		c.Logger.Log("TIMER", "Retry timer stopped", 0)
	}
}

func (c *Client) handleRequestTimeout() {
	c.mu.Lock()
	currentRequest := c.CurrentRequest
	oldLeader := c.CurrentLeaderID
	c.CurrentLeaderID = 0
	c.mu.Unlock()

	if currentRequest == nil {
		c.Logger.Log("TIMEOUT", "No current request to retry", 0)
		return
	}

	c.Logger.Log("TIMEOUT", fmt.Sprintf("Request timeout - leader %d failed, broadcasting to all nodes (timestamp: %d)",
		oldLeader, currentRequest.Timestamp), 0)

	c.broadcastRequestToAllNodes(currentRequest)

	c.mu.Lock()
	c.startRetryTimerUnsafe()
	c.mu.Unlock()
}

func (c *Client) broadcastRequestToAllNodes(request *proto.Request) {
	c.Logger.Log("BROADCAST", fmt.Sprintf("Broadcasting request to all nodes (timestamp: %d)", request.Timestamp), 0)

	var wg sync.WaitGroup
	for _, nodeInfo := range c.Config.Nodes {
		wg.Add(1)
		go func(nodeInfo common.NodeInfo) {
			defer wg.Done()
			err := c.sendRequestToNode(request, nodeInfo.ID)
			if err != nil {
				c.Logger.Log("ERROR", fmt.Sprintf("Failed to send to node %d: %v", nodeInfo.ID, err), 0)
			} else {
				c.Logger.Log("BROADCAST", fmt.Sprintf("Successfully sent to node %d", nodeInfo.ID), 0)
			}
		}(nodeInfo)
	}

	go func() {
		wg.Wait()
		c.Logger.Log("BROADCAST", "Broadcast to all nodes completed", 0)
	}()
}

func (c *Client) sendRequestToNode(request *proto.Request, nodeID int32) error {
	conn, err := c.getConnection(nodeID)
	if err != nil {
		return fmt.Errorf("failed to connect to node %d: %v", nodeID, err)
	}

	client := proto.NewPaxosServiceClient(conn)
	_, err = client.SendRequest(context.Background(), request)
	return err
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

	if c.CurrentRequest != nil && c.CurrentRequest.Timestamp == reply.Timestamp {

		c.processCurrentReplyUnsafe(reply)
	} else if c.CurrentAsyncRequest != nil && c.CurrentAsyncRequest.Timestamp == reply.Timestamp {

		c.processCurrentAsyncReplyUnsafe(reply)
	} else {

		c.Logger.Log("STALE", fmt.Sprintf("Reply for timestamp %d is not for current request", reply.Timestamp), 0)
	}

	return &proto.Status{Status: "success", Message: "Reply processed"}, nil
}

func (c *Client) processCurrentReplyUnsafe(reply *proto.Reply) {
	c.Logger.Log("PROCESS", fmt.Sprintf("Processing reply for current request (timestamp: %d)", reply.Timestamp), 0)

	if reply.Ballot != nil {
		c.CurrentLeaderID = reply.Ballot.NodeId
		c.Logger.Log("LEADER", fmt.Sprintf("Updated current leader to node %d", c.CurrentLeaderID), 0)
	}

	c.stopRetryTimerUnsafe()

	c.ProcessedRequests[reply.Timestamp] = reply

	c.CurrentRequest = nil

	c.Logger.Log("COMPLETE", fmt.Sprintf("Request completed: %s (Result: %v)", reply.Message, reply.Result), 0)

	select {
	case c.ProcessingDone <- struct{}{}:
	default:

	}
}

func (c *Client) processCurrentAsyncReplyUnsafe(reply *proto.Reply) {
	c.Logger.Log("PROCESS", fmt.Sprintf("Processing reply for current async request (timestamp: %d)", reply.Timestamp), 0)

	if reply.Ballot != nil {
		c.CurrentLeaderID = reply.Ballot.NodeId
		c.Logger.Log("LEADER", fmt.Sprintf("Updated current leader to node %d (async)", c.CurrentLeaderID), 0)
	}

	c.stopRetryTimerUnsafe()

	c.ProcessedRequests[reply.Timestamp] = reply

	c.CurrentAsyncRequest = nil

	c.Logger.Log("COMPLETE", fmt.Sprintf("Async request completed: %s (Result: %v)", reply.Message, reply.Result), 0)

	select {
	case c.AsyncProcessingDone <- struct{}{}:
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
