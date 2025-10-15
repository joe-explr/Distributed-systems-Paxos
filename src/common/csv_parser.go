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

type CSVTestParser struct {
	filePath string
}

func NewCSVTestParser(filePath string) *CSVTestParser {
	return &CSVTestParser{
		filePath: filePath,
	}
}

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

	records = records[1:]

	var testCases []testcase.TestCase
	var currentTestCase *testcase.TestCase

	for i, record := range records {
		if len(record) < 3 {
			continue 
		}

		setNumberStr := strings.TrimSpace(record[0])
		transactionStr := strings.TrimSpace(record[1])
		liveNodesStr := strings.TrimSpace(record[2])

		if setNumberStr != "" {

			if currentTestCase != nil {
				testCases = append(testCases, *currentTestCase)
			}

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

			if liveNodesStr != "" {
				nodes, err := p.parseLiveNodes(liveNodesStr)
				if err != nil {
					return nil, fmt.Errorf("failed to parse live nodes at row %d: %v", i+2, err)
				}
				currentTestCase.LiveNodes = nodes
			}
		}

		if transactionStr != "" {
			if transactionStr == "LF" {

				currentTestCase.LeaderFails = append(currentTestCase.LeaderFails, len(currentTestCase.Transactions))
			} else {

				transaction, err := p.parseTransaction(transactionStr)
				if err != nil {
					return nil, fmt.Errorf("failed to parse transaction at row %d: %v", i+2, err)
				}
				currentTestCase.Transactions = append(currentTestCase.Transactions, transaction)
			}
		}
	}

	if currentTestCase != nil {
		testCases = append(testCases, *currentTestCase)
	}

	return testCases, nil
}

func (p *CSVTestParser) parseTransaction(transactionStr string) (testcase.Transaction, error) {

	cleanStr := strings.TrimSpace(transactionStr)
	if !strings.HasPrefix(cleanStr, "(") || !strings.HasSuffix(cleanStr, ")") {
		return testcase.Transaction{}, fmt.Errorf("invalid transaction format: %s", transactionStr)
	}

	cleanStr = cleanStr[1 : len(cleanStr)-1]

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

func (p *CSVTestParser) parseLiveNodes(liveNodesStr string) ([]int32, error) {

	cleanStr := strings.TrimSpace(liveNodesStr)
	if !strings.HasPrefix(cleanStr, "[") || !strings.HasSuffix(cleanStr, "]") {
		return nil, fmt.Errorf("invalid live nodes format: %s", liveNodesStr)
	}

	cleanStr = cleanStr[1 : len(cleanStr)-1]

	parts := strings.Split(cleanStr, ",")
	var nodes []int32

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

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
