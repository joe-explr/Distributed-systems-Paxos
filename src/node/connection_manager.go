package node

import (
	"fmt"
	"log"
	"net"
	"paxos-banking/proto"
	"paxos-banking/src/common"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (n *Node) Start() error {
	n.Logger.Log("START", fmt.Sprintf("Starting node %d on port %d", n.ID, n.Port), n.ID)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", n.Port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", n.Port, err)
	}

	n.Server = grpc.NewServer()
	proto.RegisterPaxosServiceServer(n.Server, n)
	proto.RegisterNodeServiceServer(n.Server, n)

	go func() {
		if err := n.Server.Serve(lis); err != nil {
			log.Printf("Failed to serve: %v", err)
		}
	}()

	go n.processMessages()

	n.Logger.Log("START", "Node server started successfully", n.ID)
	return nil
}

func (n *Node) Stop() {
	n.Logger.Log("STOP", "Stopping node", n.ID)

	if n.LeaderTimer != nil {
		n.LeaderTimer.Stop()
		n.Logger.Log("TIMER", "Stopped election timer - node stopping", n.ID)
	}

	for _, conn := range n.Connections {
		conn.Close()
	}
	for _, conn := range n.ClientConnections {
		conn.Close()
	}

	if n.Server != nil {
		n.Server.Stop()
	}

	close(n.stopChan)

	if n.Logger != nil {
		n.Logger.Close()
	}

	if n.DB != nil {
		if err := n.DB.Close(); err != nil {
			n.Logger.Log("ERROR", fmt.Sprintf("Failed to close database: %v", err), n.ID)
		} else {
			n.Logger.Log("STOP", "Database connection closed", n.ID)
		}
	}
}

func (n *Node) getConnection(nodeID int32) (*grpc.ClientConn, error) {

	n.mu.RLock()
	if conn, exists := n.Connections[nodeID]; exists {
		n.mu.RUnlock()
		return conn, nil
	}
	n.mu.RUnlock()

	var nodeInfo *common.NodeInfo
	for _, info := range n.Config.Nodes {
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

	n.mu.Lock()
	defer n.mu.Unlock()

	if existingConn, exists := n.Connections[nodeID]; exists {

		conn.Close()
		return existingConn, nil
	}

	n.Connections[nodeID] = conn
	return conn, nil
}

func (n *Node) getClientConnection(clientID string) (*grpc.ClientConn, error) {

	n.mu.RLock()
	if conn, exists := n.ClientConnections[clientID]; exists {
		n.mu.RUnlock()
		return conn, nil
	}
	n.mu.RUnlock()

	var clientInfo *common.ClientInfo
	for _, info := range n.Config.Clients {
		if info.ID == clientID {
			clientInfo = &info
			break
		}
	}

	if clientInfo == nil {
		return nil, fmt.Errorf("client %s not found in config", clientID)
	}

	address := fmt.Sprintf("%s:%d", clientInfo.Address, clientInfo.Port)
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if existingConn, exists := n.ClientConnections[clientID]; exists {

		conn.Close()
		return existingConn, nil
	}

	n.ClientConnections[clientID] = conn
	return conn, nil
}
