package common

import (
	"encoding/csv"
	"fmt"
	"os"
	"paxos-banking/src/testcase"
	"regexp"
	"strconv"
	"strings"
)

// CSVTestParser handles parsing of CSV test files
type CSVTestParser struct {
	filePath string
}

// NewCSVTestParser creates a new CSV test parser
func NewCSVTestParser(filePath string) *CSVTestParser {
	return &CSVTestParser{
		filePath: filePath,
	}
}

// ParseTestCases parses the CSV file and returns all test cases
func (p *CSVTestParser) ParseTestCases() ([]testcase.TestCase, error) {
	file, err := os.Open(p.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV file: %v", err)
	}

	if len(records) < 2 {
		return nil, fmt.Errorf("CSV file must have at least a header and one data row")
	}

	// Skip header row
	records = records[1:]

	var testCases []testcase.TestCase
	var currentTestCase *testcase.TestCase

	for i, record := range records {
		if len(record) < 3 {
			continue // Skip malformed rows
		}

		setNumberStr := strings.TrimSpace(record[0])
		transactionStr := strings.TrimSpace(record[1])
		liveNodesStr := strings.TrimSpace(record[2])

		// Check if this is a new set
		if setNumberStr != "" {
			// Save previous test case if exists
			if currentTestCase != nil {
				testCases = append(testCases, *currentTestCase)
			}

			// Start new test case
			setNumber, err := strconv.Atoi(setNumberStr)
			if err != nil {
				return nil, fmt.Errorf("invalid set number at row %d: %s", i+2, setNumberStr)
			}

			currentTestCase = &testcase.TestCase{
				SetNumber:    setNumber,
				Transactions: []testcase.Transaction{},
				LiveNodes:    []int32{},
				LeaderFails:  []int{},
			}

			// Parse live nodes for this set
			if liveNodesStr != "" {
				nodes, err := p.parseLiveNodes(liveNodesStr)
				if err != nil {
					return nil, fmt.Errorf("failed to parse live nodes at row %d: %v", i+2, err)
				}
				currentTestCase.LiveNodes = nodes
			}
		}

		// Process transaction or LF command
		if transactionStr != "" {
			if transactionStr == "LF" {
				// Leader fail command
				currentTestCase.LeaderFails = append(currentTestCase.LeaderFails, len(currentTestCase.Transactions))
			} else {
				// Parse transaction
				transaction, err := p.parseTransaction(transactionStr)
				if err != nil {
					return nil, fmt.Errorf("failed to parse transaction at row %d: %v", i+2, err)
				}
				currentTestCase.Transactions = append(currentTestCase.Transactions, transaction)
			}
		}
	}

	// Add the last test case
	if currentTestCase != nil {
		testCases = append(testCases, *currentTestCase)
	}

	return testCases, nil
}

// parseTransaction parses a transaction string like "(A, B, 5)"
func (p *CSVTestParser) parseTransaction(transactionStr string) (testcase.Transaction, error) {
	// Remove parentheses and split by comma
	cleanStr := strings.TrimSpace(transactionStr)
	if !strings.HasPrefix(cleanStr, "(") || !strings.HasSuffix(cleanStr, ")") {
		return testcase.Transaction{}, fmt.Errorf("invalid transaction format: %s", transactionStr)
	}

	// Remove parentheses
	cleanStr = cleanStr[1 : len(cleanStr)-1]

	// Split by comma and trim spaces
	parts := strings.Split(cleanStr, ",")
	if len(parts) != 3 {
		return testcase.Transaction{}, fmt.Errorf("transaction must have exactly 3 parts: %s", transactionStr)
	}

	sender := strings.TrimSpace(parts[0])
	receiver := strings.TrimSpace(parts[1])
	amountStr := strings.TrimSpace(parts[2])

	amount, err := strconv.Atoi(amountStr)
	if err != nil {
		return testcase.Transaction{}, fmt.Errorf("invalid amount: %s", amountStr)
	}

	return testcase.Transaction{
		Sender:   sender,
		Receiver: receiver,
		Amount:   int32(amount),
	}, nil
}

// parseLiveNodes parses live nodes string like "[n1, n2, n3, n4, n5]"
func (p *CSVTestParser) parseLiveNodes(liveNodesStr string) ([]int32, error) {
	// Remove brackets
	cleanStr := strings.TrimSpace(liveNodesStr)
	if !strings.HasPrefix(cleanStr, "[") || !strings.HasSuffix(cleanStr, "]") {
		return nil, fmt.Errorf("invalid live nodes format: %s", liveNodesStr)
	}

	cleanStr = cleanStr[1 : len(cleanStr)-1]

	// Split by comma and parse each node
	parts := strings.Split(cleanStr, ",")
	var nodes []int32

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Extract node number from "n1", "n2", etc.
		re := regexp.MustCompile(`n(\d+)`)
		matches := re.FindStringSubmatch(part)
		if len(matches) != 2 {
			return nil, fmt.Errorf("invalid node format: %s", part)
		}

		nodeNum, err := strconv.Atoi(matches[1])
		if err != nil {
			return nil, fmt.Errorf("invalid node number: %s", matches[1])
		}

		nodes = append(nodes, int32(nodeNum))
	}

	return nodes, nil
}
