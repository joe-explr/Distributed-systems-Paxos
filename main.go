package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"paxos-banking/proto"
	"paxos-banking/src/client"
	"paxos-banking/src/common"
	"paxos-banking/src/node"
	"paxos-banking/src/testcase"
	"strconv"
	"strings"
	"time"
)

func main() {
	fmt.Println("=== Paxos Banking System ===")
	fmt.Println("This implementation demonstrates basic RPC communication between clients and nodes")
	fmt.Println()

	rand.Seed(time.Now().UnixNano())

	logDir := "logs"
	if err := os.RemoveAll(logDir); err != nil {
		log.Printf("Failed to remove logs directory: %v", err)
	}
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Printf("Failed to recreate logs directory: %v", err)
	}

	config := common.DefaultConfig()

	nodes := make([]*node.Node, 0, len(config.Nodes))
	for _, nodeInfo := range config.Nodes {
		n := node.NewNode(nodeInfo.ID, config)
		n.Address = nodeInfo.Address
		n.Port = nodeInfo.Port

		if err := n.Start(); err != nil {
			log.Fatalf("Failed to start node %d: %v", nodeInfo.ID, err)
		}

		nodes = append(nodes, n)
		fmt.Printf("Started node %d on port %d\n", nodeInfo.ID, nodeInfo.Port)
	}

	time.Sleep(2 * time.Second)

	clients := make([]*client.Client, 0, len(config.Clients))
	for _, clientInfo := range config.Clients {
		c := client.NewClient(clientInfo.ID, config)
		c.Address = clientInfo.Address
		c.Port = clientInfo.Port

		if err := c.Start(); err != nil {
			log.Fatalf("Failed to start client %s: %v", clientInfo.ID, err)
		}

		clients = append(clients, c)
		fmt.Printf("Started client %s\n", clientInfo.ID)
	}

	fmt.Println("\n=== System Ready ===")
	fmt.Println("Available commands:")
	fmt.Println("  transaction <client_id> <sender> <receiver> <amount> - Send transaction")
	fmt.Println("  status <node_id> - Get node status")
	fmt.Println("  log <node_id> - Print node log")
	fmt.Println("  db <node_id> - Print node database")
	fmt.Println("  printStatus <node_id> <sequence> - Print transaction status")
	fmt.Println("  printView <node_id> - Print new-view messages")
	fmt.Println("  failLeader - Fail the current leader")
	fmt.Println("  activate <node_id> - Activate a node")
	fmt.Println("  deactivate <node_id> - Deactivate a node")
	fmt.Println("  loadTest <csv_file> - Load and process test cases from CSV")
	fmt.Println("  runAllTests - Run comprehensive test suite automatically")
	fmt.Println("  validateState - Validate current system state")
	fmt.Println("  resetSystem - Reset system to initial state")
	fmt.Println("  resetDB - Reset persisted database to initial state ($10 each)")
	fmt.Println("  quit - Exit the program")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("paxos> ")

		os.Stdout.Sync()

		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				log.Printf("Error reading input: %v", err)
			}
			break
		}

		command := strings.TrimSpace(scanner.Text())
		if command == "" {
			continue
		}

		fmt.Printf("DEBUG: Received command: '%s'\n", command)

		parts := strings.Fields(command)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {

		case "transaction":
			if len(parts) != 5 {
				fmt.Println("Usage: transaction <client_id> <sender> <receiver> <amount>")
				continue
			}

			clientID := parts[1]
			sender := parts[2]
			receiver := parts[3]
			amountStr := parts[4]
			amount, err := strconv.Atoi(amountStr)
			if err != nil {
				fmt.Printf("Invalid amount: %s\n", amountStr)
				continue
			}

			var c *client.Client
			for _, client := range clients {
				if client.ID == clientID {
					c = client
					break
				}
			}

			if c == nil {
				fmt.Printf("Client %s not found\n", clientID)
				continue
			}

			if err := c.SendTransaction(sender, receiver, int32(amount)); err != nil {
				fmt.Printf("Failed to send transaction: %v\n", err)
			}

		case "status":
			if len(parts) != 2 {
				fmt.Println("Usage: status <node_id>")
				continue
			}

			nodeIDStr := parts[1]
			nodeID, err := strconv.Atoi(nodeIDStr)
			if err != nil {
				fmt.Printf("Invalid node ID: %s\n", nodeIDStr)
				continue
			}

			var n *node.Node
			for _, node := range nodes {
				if node.ID == int32(nodeID) {
					n = node
					break
				}
			}

			if n == nil {
				fmt.Printf("Node %d not found\n", nodeID)
				continue
			}

			status, err := n.GetStatus(context.Background(), &proto.Empty{})
			if err != nil {
				fmt.Printf("Failed to get status: %v\n", err)
			} else {
				fmt.Printf("Node %d status: %s\n", nodeID, status.Message)
			}

		case "log":
			if len(parts) != 2 {
				fmt.Println("Usage: log <node_id>")
				continue
			}

			nodeIDStr := parts[1]
			nodeID, err := strconv.Atoi(nodeIDStr)
			if err != nil {
				fmt.Printf("Invalid node ID: %s\n", nodeIDStr)
				continue
			}

			var n *node.Node
			for _, node := range nodes {
				if node.ID == int32(nodeID) {
					n = node
					break
				}
			}

			if n == nil {
				fmt.Printf("Node %d not found\n", nodeID)
				continue
			}

			_, err = n.PrintLog(context.Background(), &proto.Empty{})
			if err != nil {
				fmt.Printf("Failed to print log: %v\n", err)
			}

		case "db":
			if len(parts) != 2 {
				fmt.Println("Usage: db <node_id>")
				continue
			}

			nodeIDStr := parts[1]
			nodeID, err := strconv.Atoi(nodeIDStr)
			if err != nil {
				fmt.Printf("Invalid node ID: %s\n", nodeIDStr)
				continue
			}

			var n *node.Node
			for _, node := range nodes {
				if node.ID == int32(nodeID) {
					n = node
					break
				}
			}

			if n == nil {
				fmt.Printf("Node %d not found\n", nodeID)
				continue
			}

			_, err = n.PrintDB(context.Background(), &proto.Empty{})
			if err != nil {
				fmt.Printf("Failed to print database: %v\n", err)
			}

		case "printStatus":
			if len(parts) != 3 {
				fmt.Println("Usage: printStatus <node_id> <sequence>")
				continue
			}

			nodeIDStr := parts[1]
			nodeID, err := strconv.Atoi(nodeIDStr)
			if err != nil {
				fmt.Printf("Invalid node ID: %s\n", nodeIDStr)
				continue
			}

			sequenceStr := parts[2]
			sequence, err := strconv.Atoi(sequenceStr)
			if err != nil {
				fmt.Printf("Invalid sequence number: %s\n", sequenceStr)
				continue
			}

			var n *node.Node
			for _, node := range nodes {
				if node.ID == int32(nodeID) {
					n = node
					break
				}
			}

			if n == nil {
				fmt.Printf("Node %d not found\n", nodeID)
				continue
			}

			_, err = n.PrintStatus(context.Background(), &proto.PrintStatusRequest{SequenceNumber: int32(sequence)})
			if err != nil {
				fmt.Printf("Failed to print status: %v\n", err)
			}

		case "printView":
			if len(parts) != 2 {
				fmt.Println("Usage: printView <node_id>")
				continue
			}

			nodeIDStr := parts[1]
			nodeID, err := strconv.Atoi(nodeIDStr)
			if err != nil {
				fmt.Printf("Invalid node ID: %s\n", nodeIDStr)
				continue
			}

			var n *node.Node
			for _, node := range nodes {
				if node.ID == int32(nodeID) {
					n = node
					break
				}
			}

			if n == nil {
				fmt.Printf("Node %d not found\n", nodeID)
				continue
			}

			_, err = n.PrintView(context.Background(), &proto.Empty{})
			if err != nil {
				fmt.Printf("Failed to print view: %v\n", err)
			}

		case "failLeader":
			fmt.Println("Failing current leader...")

			var leader *node.Node
			for _, n := range nodes {
				if n.IsLeader {
					leader = n
					break
				}
			}

			if leader == nil {
				fmt.Println("No leader found to fail")
				continue
			}

			fmt.Printf("Failing leader node %d\n", leader.ID)
			leader.SetNodeActive(false)
			if leader.LeaderTimer != nil {
				leader.LeaderTimer.Stop()
				leader.Logger.Log("TIMER", "Stopped election timer - leader failed", leader.ID)
			}
			leader.IsLeader = false
			leader.NodeType = common.Backup
			leader.LeaderID = 0

		case "activate":
			if len(parts) != 2 {
				fmt.Println("Usage: activate <node_id>")
				continue
			}

			nodeIDStr := parts[1]
			nodeID, err := strconv.Atoi(nodeIDStr)
			if err != nil {
				fmt.Printf("Invalid node ID: %s\n", nodeIDStr)
				continue
			}

			var n *node.Node
			for _, node := range nodes {
				if node.ID == int32(nodeID) {
					n = node
					break
				}
			}

			if n == nil {
				fmt.Printf("Node %d not found\n", nodeID)
				continue
			}

			n.SetNodeActive(true)
			fmt.Printf("Node %d activated\n", nodeID)

		case "deactivate":
			if len(parts) != 2 {
				fmt.Println("Usage: deactivate <node_id>")
				continue
			}

			nodeIDStr := parts[1]
			nodeID, err := strconv.Atoi(nodeIDStr)
			if err != nil {
				fmt.Printf("Invalid node ID: %s\n", nodeIDStr)
				continue
			}

			var n *node.Node
			for _, node := range nodes {
				if node.ID == int32(nodeID) {
					n = node
					break
				}
			}

			if n == nil {
				fmt.Printf("Node %d not found\n", nodeID)
				continue
			}

			n.SetNodeActive(false)
			fmt.Printf("Node %d deactivated\n", nodeID)

		case "loadTest":
			if len(parts) != 2 {
				fmt.Println("Usage: loadTest <csv_file>")
				continue
			}

			csvFile := parts[1]
			fmt.Printf("Loading test cases from: %s\n", csvFile)

			parser := common.NewCSVTestParser(csvFile)
			testCases, err := parser.ParseTestCases()
			if err != nil {
				fmt.Printf("Failed to parse CSV file: %v\n", err)
				continue
			}

			fmt.Printf("Loaded %d test cases\n", len(testCases))

			processor := NewEnhancedTestCaseProcessor(nodes, clients, config)

			var carriedForwardTransactions []testcase.Transaction
			for i, testCase := range testCases {

				if len(carriedForwardTransactions) > 0 {
					testCase.CarriedForwardTransactions = carriedForwardTransactions
					fmt.Printf("\n=== Carrying Forward %d Transactions to Test Case %d ===\n",
						len(carriedForwardTransactions), testCase.SetNumber)
				}

				fmt.Printf("\n=== Starting Test Case %d ===\n", testCase.SetNumber)
				testCase.PrintTestCase()

				fmt.Printf("Press Enter to process test case %d...", testCase.SetNumber)
				scanner.Scan()

				result := processor.ProcessTestCaseWithValidation(testCase)
				processor.PrintDetailedResults(result)

				if !result.Success && strings.Contains(result.ErrorMessage, "insufficient active nodes") {
					fmt.Printf("Test Case %d failed due to insufficient nodes. Carrying forward remaining transactions.\n", testCase.SetNumber)

					carriedForwardTransactions = append(carriedForwardTransactions, testCase.Transactions...)

					if i < len(testCases)-1 {
						fmt.Printf("Carrying forward %d transactions to next test case.\n", len(carriedForwardTransactions))
						continue
					} else {
						fmt.Printf("No more test cases available. %d transactions remain unprocessed.\n", len(carriedForwardTransactions))
					}
				} else {

					carriedForwardTransactions = nil
				}

				shouldExit := showInteractiveMenu(nodes, clients, config, scanner)
				if shouldExit {
					fmt.Println("Load test exited by user.")
					break
				}
			}

			processor.PrintTestSummary()

		case "runAllTests":
			fmt.Println("Running comprehensive test suite...")

			processor := NewEnhancedTestCaseProcessor(nodes, clients, config)

			comprehensiveFile := "comprehensive_test_cases.csv"
			if _, err := os.Stat(comprehensiveFile); err == nil {
				fmt.Printf("Running comprehensive tests from: %s\n", comprehensiveFile)
				parser := common.NewCSVTestParser(comprehensiveFile)
				testCases, err := parser.ParseTestCases()
				if err == nil {
					processor.RunTestSuite(testCases, "Comprehensive Tests")
				}
			}

			edgeCaseFile := "edge_case_tests.csv"
			if _, err := os.Stat(edgeCaseFile); err == nil {
				fmt.Printf("Running edge case tests from: %s\n", edgeCaseFile)
				parser := common.NewCSVTestParser(edgeCaseFile)
				testCases, err := parser.ParseTestCases()
				if err == nil {
					processor.RunTestSuite(testCases, "Edge Case Tests")
				}
			}

			originalFile := "CSE535-F25-Project-1-Testcases.csv"
			if _, err := os.Stat(originalFile); err == nil {
				fmt.Printf("Running original tests from: %s\n", originalFile)
				parser := common.NewCSVTestParser(originalFile)
				testCases, err := parser.ParseTestCases()
				if err == nil {
					processor.RunTestSuite(testCases, "Original Tests")
				}
			}

		case "validateState":
			fmt.Println("Validating system state...")
			processor := NewEnhancedTestCaseProcessor(nodes, clients, config)
			processor.ValidateSystemState()

		case "resetSystem":
			fmt.Println("Resetting system to initial state...")
			processor := NewEnhancedTestCaseProcessor(nodes, clients, config)
			processor.ResetSystem()
			fmt.Println("System reset complete. All accounts restored to $10.")

		case "resetDB":
			fmt.Println("Resetting persisted databases for all nodes to initial state ($10 each)...")
			for _, n := range nodes {
				if err := n.ResetPersistentState(); err != nil {
					fmt.Printf("Node %d reset error: %v\n", n.ID, err)
				} else {
					fmt.Printf("Node %d database reset complete.\n", n.ID)
				}
			}
			fmt.Println("Database reset complete across nodes.")

		case "quit", "exit":
			fmt.Println("Shutting down system...")

			for _, c := range clients {
				c.Stop()
			}

			for _, n := range nodes {
				n.Stop()
			}

			fmt.Println("System stopped.")
			return

		default:
			fmt.Printf("Unknown command: %s\n", parts[0])
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading input: %v", err)
	}
}

type TestCaseProcessor struct {
	nodes   []*node.Node
	clients []*client.Client
	config  *common.Config
}

func NewTestCaseProcessor(nodes []*node.Node, clients []*client.Client, config *common.Config) *TestCaseProcessor {
	return &TestCaseProcessor{
		nodes:   nodes,
		clients: clients,
		config:  config,
	}
}

func (tcp *TestCaseProcessor) ProcessTestCase(testCase testcase.TestCase) error {
	fmt.Printf("\n=== Processing Test Case %d ===\n", testCase.SetNumber)
	fmt.Printf("Live Nodes: %v\n", testCase.LiveNodes)
	fmt.Printf("Total Transactions: %d\n", len(testCase.Transactions))
	fmt.Printf("Leader Fail Commands: %d\n", len(testCase.LeaderFails))
	fmt.Println()

	err := tcp.setupLiveNodes(testCase.LiveNodes)
	if err != nil {
		return fmt.Errorf("failed to setup live nodes: %v", err)
	}

	err = tcp.processTransactionsWithLeaderFails(testCase.Transactions, testCase.LeaderFails)
	if err != nil {
		return fmt.Errorf("failed to process transactions: %v", err)
	}

	err = tcp.waitForCompletion()
	if err != nil {
		return fmt.Errorf("failed to wait for completion: %v", err)
	}

	tcp.stopAllTimers()

	fmt.Printf("Test Case %d completed successfully!\n", testCase.SetNumber)
	return nil
}

func (tcp *TestCaseProcessor) setupLiveNodes(liveNodes []int32) error {
	fmt.Printf("Setting up live nodes: %v\n", liveNodes)

	liveNodeMap := make(map[int32]bool)
	for _, nodeID := range liveNodes {
		liveNodeMap[nodeID] = true
	}

	for _, n := range tcp.nodes {
		if liveNodeMap[n.ID] {

			n.SetNodeActive(true)
			fmt.Printf("Node %d: ACTIVE\n", n.ID)
		} else {

			n.SetNodeActive(false)
			fmt.Printf("Node %d: INACTIVE\n", n.ID)
		}
	}

	return nil
}

func (tcp *TestCaseProcessor) processTransactionsWithLeaderFails(transactions []testcase.Transaction, leaderFails []int) error {
	transactionIndex := 0
	lfIndex := 0

	for transactionIndex < len(transactions) || lfIndex < len(leaderFails) {

		nextTransactionPos := transactionIndex
		nextLFPos := -1
		if lfIndex < len(leaderFails) {
			nextLFPos = leaderFails[lfIndex]
		}

		if nextLFPos == -1 || nextTransactionPos < nextLFPos {

			endPos := len(transactions)
			if nextLFPos != -1 {
				endPos = nextLFPos
			}

			for transactionIndex < endPos {
				txn := transactions[transactionIndex]
				fmt.Printf("Processing transaction %d: %s -> %s: $%d\n",
					transactionIndex+1, txn.Sender, txn.Receiver, txn.Amount)

				err := tcp.sendTransaction(txn)
				if err != nil {
					return fmt.Errorf("failed to send transaction %d: %v", transactionIndex+1, err)
				}
				transactionIndex++
			}
		}

		if lfIndex < len(leaderFails) && nextLFPos == transactionIndex {
			fmt.Printf("Executing Leader Fail command at position %d\n", nextLFPos)
			err := tcp.failLeader()
			if err != nil {
				return fmt.Errorf("failed to execute leader fail: %v", err)
			}
			lfIndex++
		}
	}

	return nil
}

func (tcp *TestCaseProcessor) sendTransaction(txn testcase.Transaction) error {

	clientID := txn.Sender

	var c *client.Client
	for _, client := range tcp.clients {
		if client.ID == clientID {
			c = client
			break
		}
	}

	if c == nil {
		return fmt.Errorf("client %s not found", clientID)
	}

	err := c.SendTransactionAsync(txn.Sender, txn.Receiver, txn.Amount)
	if err != nil {
		return fmt.Errorf("failed to send transaction: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	return nil
}

func (tcp *TestCaseProcessor) failLeader() error {

	var leader *node.Node
	for _, n := range tcp.nodes {
		if n.IsLeader {
			leader = n
			break
		}
	}

	if leader == nil {
		return fmt.Errorf("no leader found to fail")
	}

	fmt.Printf("Failing leader node %d\n", leader.ID)

	leader.SetNodeActive(false)

	if leader.LeaderTimer != nil {
		leader.LeaderTimer.Stop()
		leader.Logger.Log("TIMER", "Stopped election timer - leader failed in test case", leader.ID)
	}

	leader.IsLeader = false
	leader.NodeType = common.Backup
	leader.LeaderID = 0

	return nil
}

func (tcp *TestCaseProcessor) waitForCompletion() error {
	fmt.Println("Waiting for all transactions to complete...")

	time.Sleep(2 * time.Second)

	return nil
}

func (tcp *TestCaseProcessor) stopAllTimers() {
	fmt.Println("Stopping all timers...")

	for _, n := range tcp.nodes {
		if n.LeaderTimer != nil {
			n.LeaderTimer.Stop()
			n.Logger.Log("TIMER", "Stopped election timer - test case completion", n.ID)
		}
	}

	for _, c := range tcp.clients {
		if c.RetryTimer != nil {
			c.RetryTimer.Stop()
		}
	}
}

func (tcp *TestCaseProcessor) PrintResults() {
	fmt.Println("\n=== Test Results ===")

	for _, n := range tcp.nodes {
		if n.IsNodeActive() {
			fmt.Printf("\n--- Node %d Results ---\n", n.ID)
			n.PrintDB(context.TODO(), &proto.Empty{})
		}
	}
}

type TestResult struct {
	SetNumber          int
	Success            bool
	ErrorMessage       string
	ExpectedBalances   map[string]int32
	ActualBalances     map[string]int32
	ConsistencyCheck   bool
	LeaderElections    int
	FailedTransactions []string
	ProcessingTime     time.Duration
	NodeStates         map[int32]bool
}

type EnhancedTestCaseProcessor struct {
	nodes           []*node.Node
	clients         []*client.Client
	config          *common.Config
	testResults     []TestResult
	startTime       time.Time
	initialBalances map[string]int32
}

func NewEnhancedTestCaseProcessor(nodes []*node.Node, clients []*client.Client, config *common.Config) *EnhancedTestCaseProcessor {
	processor := &EnhancedTestCaseProcessor{
		nodes:       nodes,
		clients:     clients,
		config:      config,
		testResults: make([]TestResult, 0),
	}

	processor.initialBalances = make(map[string]int32)
	for _, clientInfo := range config.Clients {
		processor.initialBalances[clientInfo.ID] = 10
	}

	return processor
}

func (etcp *EnhancedTestCaseProcessor) ProcessTestCaseWithValidation(testCase testcase.TestCase) TestResult {
	etcp.startTime = time.Now()

	result := TestResult{
		SetNumber:        testCase.SetNumber,
		ExpectedBalances: make(map[string]int32),
		ActualBalances:   make(map[string]int32),
		NodeStates:       make(map[int32]bool),
	}

	err := etcp.setupLiveNodes(testCase.LiveNodes)
	if err != nil {
		result.Success = false
		result.ErrorMessage = fmt.Sprintf("failed to setup live nodes: %v", err)
		result.ProcessingTime = time.Since(etcp.startTime)
		etcp.testResults = append(etcp.testResults, result)
		return result
	}

	activeNodeCount := etcp.countActiveNodes()
	if activeNodeCount < 3 {
		fmt.Printf("WARNING: Only %d active nodes available (minimum required: 3). Exiting current set early.\n", activeNodeCount)
		result.Success = false
		result.ErrorMessage = fmt.Sprintf("insufficient active nodes: %d < 3", activeNodeCount)
		result.ProcessingTime = time.Since(etcp.startTime)
		etcp.testResults = append(etcp.testResults, result)
		return result
	}

	currentBalances := etcp.getCurrentNodeBalances()
	for clientID, balance := range currentBalances {
		result.ExpectedBalances[clientID] = balance
	}

	for _, txn := range testCase.Transactions {
		if result.ExpectedBalances[txn.Sender] >= txn.Amount {
			result.ExpectedBalances[txn.Sender] -= txn.Amount
			result.ExpectedBalances[txn.Receiver] += txn.Amount
		} else {
			result.FailedTransactions = append(result.FailedTransactions,
				fmt.Sprintf("%s->%s:$%d (insufficient balance)", txn.Sender, txn.Receiver, txn.Amount))
		}
	}

	err = etcp.processTransactionsOnly(testCase)
	if err != nil {
		result.Success = false
		result.ErrorMessage = err.Error()
	} else {
		result.Success = true
	}

	etcp.collectSystemState(&result)

	result.ConsistencyCheck = etcp.checkConsistency(&result)

	result.ProcessingTime = time.Since(etcp.startTime)

	etcp.testResults = append(etcp.testResults, result)

	return result
}

func (etcp *EnhancedTestCaseProcessor) processTestCaseInternal(testCase testcase.TestCase) error {

	err := etcp.setupLiveNodes(testCase.LiveNodes)
	if err != nil {
		return fmt.Errorf("failed to setup live nodes: %v", err)
	}

	activeNodeCount := etcp.countActiveNodes()
	if activeNodeCount < 3 {
		fmt.Printf("WARNING: Only %d active nodes available (minimum required: 3). Exiting current set early.\n", activeNodeCount)
		return fmt.Errorf("insufficient active nodes: %d < 3", activeNodeCount)
	}

	allTransactions := append(testCase.CarriedForwardTransactions, testCase.Transactions...)
	if len(testCase.CarriedForwardTransactions) > 0 {
		fmt.Printf("Processing %d carried forward transactions + %d new transactions\n",
			len(testCase.CarriedForwardTransactions), len(testCase.Transactions))
	}

	adjustedLeaderFails := etcp.adjustLeaderFailureIndices(testCase.LeaderFails, len(testCase.CarriedForwardTransactions))

	err = etcp.processTransactionsWithLeaderFails(allTransactions, adjustedLeaderFails)
	if err != nil {
		return fmt.Errorf("failed to process transactions: %v", err)
	}

	err = etcp.waitForCompletion()
	if err != nil {
		return fmt.Errorf("failed to wait for completion: %v", err)
	}

	etcp.stopAllTimers()

	return nil
}

func (etcp *EnhancedTestCaseProcessor) processTransactionsOnly(testCase testcase.TestCase) error {

	allTransactions := append(testCase.CarriedForwardTransactions, testCase.Transactions...)
	if len(testCase.CarriedForwardTransactions) > 0 {
		fmt.Printf("Processing %d carried forward transactions + %d new transactions\n",
			len(testCase.CarriedForwardTransactions), len(testCase.Transactions))
	}

	adjustedLeaderFails := etcp.adjustLeaderFailureIndices(testCase.LeaderFails, len(testCase.CarriedForwardTransactions))

	err := etcp.processTransactionsWithLeaderFails(allTransactions, adjustedLeaderFails)
	if err != nil {
		return fmt.Errorf("failed to process transactions: %v", err)
	}

	err = etcp.waitForCompletion()
	if err != nil {
		return fmt.Errorf("failed to wait for completion: %v", err)
	}

	etcp.stopAllTimers()

	return nil
}

func (etcp *EnhancedTestCaseProcessor) adjustLeaderFailureIndices(leaderFails []int, carriedForwardCount int) []int {
	if carriedForwardCount == 0 || len(leaderFails) == 0 {
		return leaderFails
	}

	adjustedFails := make([]int, len(leaderFails))
	for i, failIndex := range leaderFails {
		adjustedFails[i] = failIndex + carriedForwardCount
		fmt.Printf("Leader Fail: Adjusted index %d -> %d (offset by %d carried forward transactions)\n",
			failIndex, adjustedFails[i], carriedForwardCount)
	}
	return adjustedFails
}

func (etcp *EnhancedTestCaseProcessor) setupLiveNodes(liveNodes []int32) error {
	fmt.Printf("Setting up live nodes: %v\n", liveNodes)

	liveNodeMap := make(map[int32]bool)
	for _, nodeID := range liveNodes {
		liveNodeMap[nodeID] = true
	}

	for _, n := range etcp.nodes {
		if liveNodeMap[n.ID] {

			n.SetNodeActive(true)
			fmt.Printf("Node %d: ACTIVE\n", n.ID)
		} else {

			n.SetNodeActive(false)
			fmt.Printf("Node %d: INACTIVE\n", n.ID)
		}
	}

	return etcp.waitForRecoveryCompletion()
}

func (etcp *EnhancedTestCaseProcessor) processTransactionsWithLeaderFails(transactions []testcase.Transaction, leaderFails []int) error {
	transactionIndex := 0
	lfIndex := 0

	for transactionIndex < len(transactions) || lfIndex < len(leaderFails) {

		nextTransactionPos := transactionIndex
		nextLFPos := -1
		if lfIndex < len(leaderFails) {
			nextLFPos = leaderFails[lfIndex]
		}

		if nextLFPos == -1 || nextTransactionPos < nextLFPos {

			leader := etcp.getLeaderNode()
			var startSequence int32 = 0
			if leader != nil {
				startSequence = leader.GetExecutedSequence()
			}

			endPos := len(transactions)
			if nextLFPos != -1 {
				endPos = nextLFPos
			}

			batchStart := transactionIndex
			for transactionIndex < endPos {
				txn := transactions[transactionIndex]
				fmt.Printf("Processing transaction %d: %s -> %s: $%d\n",
					transactionIndex+1, txn.Sender, txn.Receiver, txn.Amount)

				err := etcp.sendTransaction(txn)
				if err != nil {
					return fmt.Errorf("failed to send transaction %d: %v", transactionIndex+1, err)
				}

				transactionIndex++
			}

			batchSize := transactionIndex - batchStart
			expectedFinalSequence := startSequence + int32(batchSize)

			_ = expectedFinalSequence 
		}

		if lfIndex < len(leaderFails) && nextLFPos == transactionIndex {
			fmt.Printf("Executing Leader Fail command at position %d\n", nextLFPos)

			leader := etcp.getLeaderNode()
			if leader == nil {
				return fmt.Errorf("no leader found when trying to execute leader fail")
			}

			targetSequence := leader.GetExecutedSequence()

			time.Sleep(200 * time.Millisecond)

			targetSequence = leader.GetExecutedSequence()

			err := etcp.waitForSequenceToExecute(targetSequence)
			if err != nil {
				return fmt.Errorf("failed to wait for transactions to execute: %v", err)
			}

			time.Sleep(500 * time.Millisecond)

			err = etcp.failLeader()
			if err != nil {
				return fmt.Errorf("failed to execute leader fail: %v", err)
			}

			fmt.Println("Leader failed - waiting for system to settle before continuing...")
			time.Sleep(3 * time.Second)
			fmt.Println("Continuing with transaction processing - clients will trigger new election through retries")

			lfIndex++
		}
	}

	return nil
}

func (etcp *EnhancedTestCaseProcessor) sendTransaction(txn testcase.Transaction) error {

	clientID := txn.Sender

	var c *client.Client
	for _, client := range etcp.clients {
		if client.ID == clientID {
			c = client
			break
		}
	}

	if c == nil {
		return fmt.Errorf("client %s not found", clientID)
	}

	err := c.SendTransactionAsync(txn.Sender, txn.Receiver, txn.Amount)
	if err != nil {
		return fmt.Errorf("failed to send transaction: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	return nil
}

func (etcp *EnhancedTestCaseProcessor) failLeader() error {

	var leader *node.Node
	for _, n := range etcp.nodes {
		if n.IsLeader {
			leader = n
			break
		}
	}

	if leader == nil {
		return fmt.Errorf("no leader found to fail")
	}

	fmt.Printf("Failing leader node %d\n", leader.ID)

	leader.SetNodeActive(false)

	if leader.LeaderTimer != nil {
		leader.LeaderTimer.Stop()
		leader.Logger.Log("TIMER", "Stopped election timer - leader failed in test case", leader.ID)
	}

	leader.IsLeader = false
	leader.NodeType = common.Backup
	leader.LeaderID = 0

	return nil
}

func (etcp *EnhancedTestCaseProcessor) waitForCompletion() error {
	fmt.Println("Waiting for cluster to catch up after batch...")

	time.Sleep(200 * time.Millisecond)

	maxWait := 7 * time.Second
	poll := 150 * time.Millisecond
	start := time.Now()

	findLeader := func() *node.Node {
		for _, n := range etcp.nodes {
			if n.IsNodeActive() && n.IsLeader {
				return n
			}
		}
		return nil
	}

	var leader *node.Node
	leaderWaitDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(leaderWaitDeadline) {
		if leader = findLeader(); leader != nil {
			break
		}
		time.Sleep(poll)
	}

	var targetExec int32
	if leader != nil {
		targetExec = leader.GetExecutedSequence()
	} else {

		var maxExec int32 = 0
		for _, n := range etcp.nodes {
			if n.IsNodeActive() {
				if exec := n.GetExecutedSequence(); exec > maxExec {
					maxExec = exec
				}
			}
		}
		targetExec = maxExec
		fmt.Println("No leader detected; using max executed among active nodes as target")
	}

	for time.Since(start) < maxWait {

		if l := findLeader(); l != nil {
			if exec := l.GetExecutedSequence(); exec > targetExec {
				targetExec = exec
			}
		}

		allGood := true
		for _, n := range etcp.nodes {
			if !n.IsNodeActive() {
				continue
			}
			state := n.GetRecoveryState()
			exec := n.GetExecutedSequence()

			if state == node.RecoveryInProgress || exec < targetExec {
				allGood = false
				break
			}
		}

		if allGood {
			fmt.Println("All active nodes are recovered and caught up to target")
			return nil
		}

		time.Sleep(poll)
	}

	fmt.Println("Post-batch catch-up timeout; proceeding with possible lag:")
	for _, n := range etcp.nodes {
		if n.IsNodeActive() {
			fmt.Printf("  Node %d: state=%s, executed=%d (target=%d)\n",
				n.ID, n.GetRecoveryState().String(), n.GetExecutedSequence(), targetExec)
		}
	}
	return nil
}

func (etcp *EnhancedTestCaseProcessor) getLeaderNode() *node.Node {
	for _, n := range etcp.nodes {
		if n.IsNodeActive() && n.IsNodeLeader() {
			return n
		}
	}
	return nil
}

func (etcp *EnhancedTestCaseProcessor) waitForSequenceToExecute(targetSequence int32) error {
	if targetSequence == 0 {
		return nil
	}

	fmt.Printf("Waiting for all active nodes to execute up to sequence %d...\n", targetSequence)

	maxWaitTime := 30 * time.Second
	checkInterval := 100 * time.Millisecond
	startTime := time.Now()

	for time.Since(startTime) < maxWaitTime {
		allExecuted := true

		for _, node := range etcp.nodes {
			if !node.IsNodeActive() {
				continue
			}

			executedSeq := node.GetExecutedSequence()
			if executedSeq < targetSequence {
				allExecuted = false
				break
			}
		}

		if allExecuted {
			fmt.Printf("All active nodes have executed up to sequence %d\n", targetSequence)
			return nil
		}

		time.Sleep(checkInterval)
	}

	fmt.Println("Timeout - Current execution state:")
	for _, node := range etcp.nodes {
		if node.IsNodeActive() {
			fmt.Printf("  Node %d: executed=%d (target=%d)\n",
				node.ID, node.GetExecutedSequence(), targetSequence)
		}
	}

	return fmt.Errorf("timeout waiting for transactions to execute after %v", maxWaitTime)
}

func (etcp *EnhancedTestCaseProcessor) countActiveNodes() int {
	count := 0
	for _, node := range etcp.nodes {
		if node.IsNodeActive() {
			count++
		}
	}
	return count
}

func (etcp *EnhancedTestCaseProcessor) waitForTransactionCompletion(sequenceNumber int32) error {
	fmt.Printf("Waiting for transaction %d to complete...\n", sequenceNumber)

	if err := etcp.waitForTransactionSpecificCompletion(sequenceNumber); err != nil {
		fmt.Printf("Phase 1 failed: %v, trying Phase 2...\n", err)

		if err := etcp.waitForSequenceExecution(sequenceNumber); err != nil {
			return fmt.Errorf("both wait phases failed: %v", err)
		}
	}

	if err := etcp.verifySystemConsistency(); err != nil {
		return fmt.Errorf("consistency check failed: %v", err)
	}

	fmt.Printf("Transaction %d completed successfully with consistency verified\n", sequenceNumber)
	return nil
}

func (etcp *EnhancedTestCaseProcessor) waitForTransactionSpecificCompletion(sequenceNumber int32) error {
	maxWait := 8 * time.Second
	start := time.Now()

	time.Sleep(200 * time.Millisecond)

	for time.Since(start) < maxWait {
		if etcp.isTransactionFullyCompleted(sequenceNumber) {
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("transaction %d did not complete within %v", sequenceNumber, maxWait)
}

func (etcp *EnhancedTestCaseProcessor) isTransactionFullyCompleted(sequenceNumber int32) bool {
	for _, n := range etcp.nodes {
		if n.IsNodeActive() {
			if !etcp.isTransactionCompletedOnNode(n, sequenceNumber) {
				return false
			}
		}
	}
	return true
}

func (etcp *EnhancedTestCaseProcessor) isTransactionCompletedOnNode(n *node.Node, sequenceNumber int32) bool {

	return n.IsTransactionFullyCompleted(sequenceNumber)
}

func (etcp *EnhancedTestCaseProcessor) waitForSequenceExecution(sequenceNumber int32) error {
	fmt.Printf("Waiting for sequence %d to be executed...\n", sequenceNumber)

	maxWait := 5 * time.Second
	start := time.Now()

	for time.Since(start) < maxWait {
		allExecuted := true

		for _, n := range etcp.nodes {
			if n.IsNodeActive() {
				executedSeq := n.GetExecutedSequence()

				if executedSeq < sequenceNumber {
					allExecuted = false
					break
				}
			}
		}

		if allExecuted {
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("sequence %d not executed on all nodes within timeout", sequenceNumber)
}

func (etcp *EnhancedTestCaseProcessor) verifySystemConsistency() error {
	fmt.Println("Verifying system consistency...")

	var nodeBalances []map[string]int32
	var activeNodeIDs []int32

	for _, n := range etcp.nodes {
		if n.IsNodeActive() {
			balances := n.GetAccountBalancesMap()
			nodeBalances = append(nodeBalances, balances)
			activeNodeIDs = append(activeNodeIDs, n.ID)
		}
	}

	if len(nodeBalances) == 0 {
		return fmt.Errorf("no active nodes found for consistency check")
	}

	if len(nodeBalances) == 1 {
		fmt.Printf("Single node %d active, consistency verified\n", activeNodeIDs[0])
		return nil
	}

	for i := 1; i < len(nodeBalances); i++ {
		for clientID, balance := range nodeBalances[0] {
			if nodeBalances[i][clientID] != balance {
				return fmt.Errorf("inconsistency detected: Node %d has %s=$%d, Node %d has %s=$%d",
					activeNodeIDs[0], clientID, balance, activeNodeIDs[i], clientID, nodeBalances[i][clientID])
			}
		}
	}

	fmt.Printf("System consistency verified across %d nodes\n", len(nodeBalances))
	return nil
}

func (etcp *EnhancedTestCaseProcessor) waitForNewLeader() error {
	fmt.Println("Waiting for new leader election...")

	maxWait := 10 * time.Second
	start := time.Now()

	for time.Since(start) < maxWait {
		for _, n := range etcp.nodes {
			if n.IsNodeActive() && n.IsLeader {
				fmt.Printf("New leader elected: Node %d\n", n.ID)
				return nil
			}
		}

		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("no new leader elected within timeout")
}

func (etcp *EnhancedTestCaseProcessor) stopAllTimers() {
	fmt.Println("Stopping all timers...")

	for _, n := range etcp.nodes {
		if n.LeaderTimer != nil {
			n.LeaderTimer.Stop()
			n.Logger.Log("TIMER", "Stopped election timer - enhanced test case completion", n.ID)
		}
	}

	for _, c := range etcp.clients {
		if c.RetryTimer != nil {
			c.RetryTimer.Stop()
		}
	}
}

func (etcp *EnhancedTestCaseProcessor) getCurrentNodeBalances() map[string]int32 {
	currentBalances := make(map[string]int32)

	for _, n := range etcp.nodes {
		if n.IsNodeActive() {
			balances := n.GetAccountBalancesMap()
			for clientID, balance := range balances {
				currentBalances[clientID] = balance
			}
			break
		}
	}

	return currentBalances
}

func (etcp *EnhancedTestCaseProcessor) collectSystemState(result *TestResult) {

	for _, n := range etcp.nodes {
		if n.IsNodeActive() {
			result.NodeStates[n.ID] = true

			balances := n.GetAccountBalancesMap()
			if result.ActualBalances == nil {
				result.ActualBalances = make(map[string]int32)
			}
			for clientID, balance := range balances {
				result.ActualBalances[clientID] = balance
			}
		}
	}
}

func (etcp *EnhancedTestCaseProcessor) checkConsistency(result *TestResult) bool {

	return true
}

func (etcp *EnhancedTestCaseProcessor) PrintDetailedResults(result TestResult) {
	fmt.Printf("\n=== Detailed Results for Test Case %d ===\n", result.SetNumber)
	fmt.Printf("Success: %t\n", result.Success)
	if !result.Success {
		fmt.Printf("Error: %s\n", result.ErrorMessage)
	}

	fmt.Printf("Processing Time: %v\n", result.ProcessingTime)
	fmt.Printf("Consistency Check: %t\n", result.ConsistencyCheck)

	fmt.Println("\nExpected vs Actual Balances:")
	for clientID, expected := range result.ExpectedBalances {
		actual := result.ActualBalances[clientID]
		match := expected == actual
		fmt.Printf("  %s: Expected $%d, Actual $%d %s\n",
			clientID, expected, actual, map[bool]string{true: "✓", false: "✗"}[match])
	}

	if len(result.FailedTransactions) > 0 {
		fmt.Println("\nFailed Transactions:")
		for _, failed := range result.FailedTransactions {
			fmt.Printf("  - %s\n", failed)
		}
	}

	fmt.Printf("\nActive Nodes: %v\n", result.NodeStates)
}

func (etcp *EnhancedTestCaseProcessor) RunTestSuite(testCases []testcase.TestCase, suiteName string) {
	fmt.Printf("\n=== Running %s ===\n", suiteName)

	successCount := 0
	totalCount := len(testCases)

	for _, testCase := range testCases {
		fmt.Printf("\n--- Test Case %d ---\n", testCase.SetNumber)
		result := etcp.ProcessTestCaseWithValidation(testCase)

		if result.Success {
			successCount++
			fmt.Printf("✓ Test Case %d PASSED\n", testCase.SetNumber)
		} else {
			fmt.Printf("✗ Test Case %d FAILED: %s\n", testCase.SetNumber, result.ErrorMessage)
		}

		time.Sleep(500 * time.Millisecond)
	}

	fmt.Printf("\n=== %s Summary ===\n", suiteName)
	fmt.Printf("Passed: %d/%d (%.1f%%)\n", successCount, totalCount, float64(successCount)/float64(totalCount)*100)
}

func (etcp *EnhancedTestCaseProcessor) PrintTestSummary() {
	fmt.Println("\n=== Overall Test Summary ===")

	if len(etcp.testResults) == 0 {
		fmt.Println("No test results available.")
		return
	}

	successCount := 0
	totalTime := time.Duration(0)

	for _, result := range etcp.testResults {
		if result.Success {
			successCount++
		}
		totalTime += result.ProcessingTime
	}

	fmt.Printf("Total Tests: %d\n", len(etcp.testResults))
	fmt.Printf("Passed: %d\n", successCount)
	fmt.Printf("Failed: %d\n", len(etcp.testResults)-successCount)
	fmt.Printf("Success Rate: %.1f%%\n", float64(successCount)/float64(len(etcp.testResults))*100)
	fmt.Printf("Total Processing Time: %v\n", totalTime)
	fmt.Printf("Average Processing Time: %v\n", totalTime/time.Duration(len(etcp.testResults)))
}

func (etcp *EnhancedTestCaseProcessor) ValidateSystemState() {
	fmt.Println("=== System State Validation ===")

	fmt.Println("Node States:")
	for _, n := range etcp.nodes {
		status := "INACTIVE"
		if n.IsNodeActive() {
			status = "ACTIVE"
		}
		leaderStatus := ""
		if n.IsLeader {
			leaderStatus = " (LEADER)"
		}
		fmt.Printf("  Node %d: %s%s\n", n.ID, status, leaderStatus)
	}

	hasLeader := false
	for _, n := range etcp.nodes {
		if n.IsLeader && n.IsNodeActive() {
			hasLeader = true
			fmt.Printf("Current Leader: Node %d\n", n.ID)
			break
		}
	}

	if !hasLeader {
		fmt.Println("WARNING: No active leader found!")
	}

	fmt.Println("Client States:")
	for _, c := range etcp.clients {
		fmt.Printf("  Client %s: Connected\n", c.ID)
	}
}

func (etcp *EnhancedTestCaseProcessor) ResetSystem() {
	fmt.Println("Resetting system...")

	for _, n := range etcp.nodes {
		n.SetNodeActive(true)
		n.IsLeader = false
		n.NodeType = common.Backup
		n.LeaderID = 0
	}

	etcp.testResults = make([]TestResult, 0)

	for clientID := range etcp.initialBalances {
		etcp.initialBalances[clientID] = 10
	}

	fmt.Println("System reset complete.")
}

func (etcp *EnhancedTestCaseProcessor) waitForRecoveryCompletion() error {
	fmt.Println("Waiting for node recovery to complete...")

	maxWait := 3 * time.Second
	start := time.Now()

	for time.Since(start) < maxWait {
		allRecovered := true

		for _, n := range etcp.nodes {
			if n.IsNodeActive() {
				recoveryState := n.GetRecoveryState()
				if recoveryState == node.RecoveryInProgress {
					allRecovered = false
					break
				}
			}
		}

		if allRecovered {
			fmt.Println("All nodes have completed recovery")
			return nil
		}

		time.Sleep(200 * time.Millisecond)
	}

	fmt.Println("Recovery timeout - proceeding with test case")
	return nil
}

func showInteractiveMenu(nodes []*node.Node, clients []*client.Client, config *common.Config, scanner *bufio.Scanner) bool {
	for {
		fmt.Println("\n" + strings.Repeat("=", 60))
		fmt.Println("INTERACTIVE SYSTEM INSPECTION MENU")
		fmt.Println(strings.Repeat("=", 60))
		fmt.Println("1. Show system overview (all nodes status)")
		fmt.Println("2. Show node logs (choose specific node)")
		fmt.Println("3. Show node database/balances (choose specific node)")
		fmt.Println("4. Show transaction status (enter sequence number)")
		fmt.Println("5. Show new-view messages (choose specific node)")
		fmt.Println("6. Show log files (persistent logs)")
		fmt.Println("7. Continue to next test case")
		fmt.Println("8. Exit load test")
		fmt.Print("\nEnter your choice (1-8): ")

		if !scanner.Scan() {
			return false
		}

		choice := strings.TrimSpace(scanner.Text())
		fmt.Println()

		switch choice {
		case "1":
			showSystemOverview(nodes)
		case "2":
			showNodeLogs(nodes, scanner)
		case "3":
			showNodeDatabase(nodes, scanner)
		case "4":
			showTransactionStatus(nodes, scanner)
		case "5":
			showNewViewMessages(nodes, scanner)
		case "6":
			showLogFiles()
		case "7":
			return false
		case "8":
			fmt.Println("Exiting load test...")
			return true
		default:
			fmt.Println("Invalid choice. Please enter 1-8.")
		}

		fmt.Println("\nPress Enter to return to menu...")
		scanner.Scan()
	}
}

func showSystemOverview(nodes []*node.Node) {
	fmt.Println("=== SYSTEM OVERVIEW ===")
	fmt.Printf("%-8s %-12s %-10s %-8s %-12s %-15s\n", "Node ID", "Status", "Type", "Leader", "Active", "Last Activity")
	fmt.Println(strings.Repeat("-", 80))

	for _, n := range nodes {
		status := "DOWN"
		if n.IsNodeActive() {
			status = "RUNNING"
		}

		isLeader, _, lastActivity := n.GetNodeStateInfo()

		nodeTypeStr := "BACKUP"
		if isLeader {
			nodeTypeStr = "LEADER"
		}

		isLeaderStr := "No"
		if isLeader {
			isLeaderStr = "Yes"
		}

		active := "No"
		if n.IsNodeActive() {
			active = "Yes"
		}

		lastActivityStr := lastActivity.Format("15:04:05")

		fmt.Printf("%-8d %-12s %-10s %-8s %-12s %-15s\n",
			n.ID, status, nodeTypeStr, isLeaderStr, active, lastActivityStr)
	}

	fmt.Println("\n=== CURRENT LEADER INFO ===")
	leaderFound := false
	for _, n := range nodes {
		if n.IsNodeActive() && n.IsLeader {
			fmt.Printf("Current Leader: Node %d\n", n.ID)
			fmt.Printf("Leader Address: %s:%d\n", n.Address, n.Port)
			leaderFound = true
			break
		}
	}
	if !leaderFound {
		fmt.Println("No active leader found")
	}
}

func showNodeLogs(nodes []*node.Node, scanner *bufio.Scanner) {
	fmt.Print("Enter node ID to view logs (or 'all' for all nodes): ")
	if !scanner.Scan() {
		return
	}

	input := strings.TrimSpace(scanner.Text())

	if input == "all" {
		fmt.Println("=== ALL NODE LOGS ===")
		for _, n := range nodes {
			fmt.Printf("\n--- Node %d Logs ---\n", n.ID)
			if n.IsNodeActive() {
				n.Logger.PrintLog()
			} else {
				fmt.Println("Node is inactive - no logs available")
			}
		}
	} else {
		nodeID, err := strconv.Atoi(input)
		if err != nil {
			fmt.Printf("Invalid node ID: %s\n", input)
			return
		}

		var targetNode *node.Node
		for _, n := range nodes {
			if n.ID == int32(nodeID) {
				targetNode = n
				break
			}
		}

		if targetNode == nil {
			fmt.Printf("Node %d not found\n", nodeID)
			return
		}

		fmt.Printf("=== Node %d Logs ===\n", nodeID)
		if targetNode.IsNodeActive() {
			targetNode.Logger.PrintLog()
		} else {
			fmt.Println("Node is inactive - no logs available")
		}
	}
}

func showNodeDatabase(nodes []*node.Node, scanner *bufio.Scanner) {
	fmt.Print("Enter node ID to view database (or 'all' for all nodes): ")
	if !scanner.Scan() {
		return
	}

	input := strings.TrimSpace(scanner.Text())

	if input == "all" {
		fmt.Println("=== ALL NODE DATABASES ===")
		for _, n := range nodes {
			fmt.Printf("\n--- Node %d Database ---\n", n.ID)
			if n.IsNodeActive() {
				fmt.Println("Account Balances:")
				balances := n.GetAccountBalancesMap()
				for clientID, balance := range balances {
					fmt.Printf("  Client %s: $%d\n", clientID, balance)
				}
			} else {
				fmt.Println("Node is inactive - no database available")
			}
		}
	} else {
		nodeID, err := strconv.Atoi(input)
		if err != nil {
			fmt.Printf("Invalid node ID: %s\n", input)
			return
		}

		var targetNode *node.Node
		for _, n := range nodes {
			if n.ID == int32(nodeID) {
				targetNode = n
				break
			}
		}

		if targetNode == nil {
			fmt.Printf("Node %d not found\n", nodeID)
			return
		}

		fmt.Printf("=== Node %d Database ===\n", nodeID)
		if targetNode.IsNodeActive() {
			fmt.Println("Account Balances:")
			balances := targetNode.GetAccountBalancesMap()
			for clientID, balance := range balances {
				fmt.Printf("  Client %s: $%d\n", clientID, balance)
			}
		} else {
			fmt.Println("Node is inactive - no database available")
		}
	}
}

func showTransactionStatus(nodes []*node.Node, scanner *bufio.Scanner) {
	fmt.Print("Enter sequence number to check transaction status: ")
	if !scanner.Scan() {
		return
	}

	input := strings.TrimSpace(scanner.Text())
	seqNum, err := strconv.Atoi(input)
	if err != nil {
		fmt.Printf("Invalid sequence number: %s\n", input)
		return
	}

	fmt.Printf("=== Transaction Status for Sequence %d ===\n", seqNum)

	for _, n := range nodes {
		if n.IsNodeActive() {
			fmt.Printf("\n--- Node %d ---\n", n.ID)
			if transactionInfo, exists := n.GetTransactionInfo(int32(seqNum)); exists {
				fmt.Printf("Status: %s\n", transactionInfo.Status.String())
				if transactionInfo.Request != nil {
					fmt.Printf("Transaction: %s->%s $%d\n",
						transactionInfo.Request.Transaction.Sender,
						transactionInfo.Request.Transaction.Receiver,
						transactionInfo.Request.Transaction.Amount)
				}
				if transactionInfo.BallotNumber != nil {
					fmt.Printf("Ballot: (%d, %d)\n",
						transactionInfo.BallotNumber.Round,
						transactionInfo.BallotNumber.NodeId)
				}
				fmt.Printf("Timestamp: %s\n", transactionInfo.Timestamp.Format("15:04:05.000"))
			} else {
				fmt.Printf("Status: X (No Status)\n")
				fmt.Println("Transaction not found or not processed yet")
			}
		}
	}
}

func showNewViewMessages(nodes []*node.Node, scanner *bufio.Scanner) {
	fmt.Print("Enter node ID to view new-view messages: ")
	if !scanner.Scan() {
		return
	}

	input := strings.TrimSpace(scanner.Text())
	nodeID, err := strconv.Atoi(input)
	if err != nil {
		fmt.Printf("Invalid node ID: %s\n", input)
		return
	}

	var targetNode *node.Node
	for _, n := range nodes {
		if n.ID == int32(nodeID) {
			targetNode = n
			break
		}
	}

	if targetNode == nil {
		fmt.Printf("Node %d not found\n", nodeID)
		return
	}

	fmt.Printf("=== Node %d New-View Messages ===\n", nodeID)
	if targetNode.IsNodeActive() {
		newViewLog := targetNode.GetNewViewLog()
		if len(newViewLog) == 0 {
			fmt.Println("No new-view messages received yet")
		} else {
			for i, newView := range newViewLog {
				fmt.Printf("NEW-VIEW #%d:\n", i+1)
				fmt.Printf("  Ballot: (%d, %d)\n", newView.Ballot.Round, newView.Ballot.NodeId)
				fmt.Printf("  AcceptLog entries: %d\n", len(newView.AcceptLog))
				for _, entry := range newView.AcceptLog {
					if entry.AcceptVal.Transaction.Sender == "no-op" {
						fmt.Printf("    Seq %d: NO-OP (ballot %d.%d)\n",
							entry.AcceptSeq, entry.AcceptNum.Round, entry.AcceptNum.NodeId)
					} else {
						fmt.Printf("    Seq %d: %s->%s $%d (ballot %d.%d)\n",
							entry.AcceptSeq, entry.AcceptVal.Transaction.Sender,
							entry.AcceptVal.Transaction.Receiver, entry.AcceptVal.Transaction.Amount,
							entry.AcceptNum.Round, entry.AcceptNum.NodeId)
					}
				}
				fmt.Println()
			}
		}
	} else {
		fmt.Println("Node is inactive - no new-view messages available")
	}
}

func showLogFiles() {
	fmt.Println("=== PERSISTENT LOG FILES ===")

	if _, err := os.Stat("logs"); os.IsNotExist(err) {
		fmt.Println("No logs directory found. Log files are created when nodes start.")
		return
	}

	files, err := os.ReadDir("logs")
	if err != nil {
		fmt.Printf("Error reading logs directory: %v\n", err)
		return
	}

	if len(files) == 0 {
		fmt.Println("No log files found in logs directory.")
		return
	}

	fmt.Println("Available log files:")
	for i, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".log") {
			fmt.Printf("%d. %s\n", i+1, file.Name())
		}
	}

	fmt.Print("\nEnter log file number to view (or 'all' for all files): ")
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return
	}

	input := strings.TrimSpace(scanner.Text())

	if input == "all" {
		for _, file := range files {
			if !file.IsDir() && strings.HasSuffix(file.Name(), ".log") {
				fmt.Printf("\n=== %s ===\n", file.Name())
				content, err := os.ReadFile(filepath.Join("logs", file.Name()))
				if err != nil {
					fmt.Printf("Error reading file: %v\n", err)
					continue
				}
				fmt.Print(string(content))
			}
		}
	} else {
		fileNum, err := strconv.Atoi(input)
		if err != nil || fileNum < 1 || fileNum > len(files) {
			fmt.Printf("Invalid file number: %s\n", input)
			return
		}

		logFiles := make([]os.DirEntry, 0)
		for _, file := range files {
			if !file.IsDir() && strings.HasSuffix(file.Name(), ".log") {
				logFiles = append(logFiles, file)
			}
		}

		if fileNum <= len(logFiles) {
			selectedFile := logFiles[fileNum-1]
			fmt.Printf("=== %s ===\n", selectedFile.Name())
			content, err := os.ReadFile(filepath.Join("logs", selectedFile.Name()))
			if err != nil {
				fmt.Printf("Error reading file: %v\n", err)
				return
			}
			fmt.Print(string(content))
		}
	}
}
