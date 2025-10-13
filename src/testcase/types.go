package testcase

import (
	"fmt"
)

type TestCase struct {
	SetNumber                  int
	Transactions               []Transaction
	LiveNodes                  []int32
	LeaderFails                []int
	CarriedForwardTransactions []Transaction
}

type Transaction struct {
	Sender   string
	Receiver string
	Amount   int32
}

func (tc *TestCase) PrintTestCase() {
	fmt.Printf("=== Test Case %d ===\n", tc.SetNumber)
	fmt.Printf("Live Nodes: %v\n", tc.LiveNodes)
	fmt.Printf("Leader Fails at positions: %v\n", tc.LeaderFails)
	if len(tc.CarriedForwardTransactions) > 0 {
		fmt.Printf("Carried Forward Transactions: %d\n", len(tc.CarriedForwardTransactions))
		for i, txn := range tc.CarriedForwardTransactions {
			fmt.Printf("  CF %d. %s -> %s: $%d\n", i+1, txn.Sender, txn.Receiver, txn.Amount)
		}
	}
	fmt.Println("Transactions:")
	for i, txn := range tc.Transactions {
		fmt.Printf("  %d. %s -> %s: $%d\n", i+1, txn.Sender, txn.Receiver, txn.Amount)
	}
	fmt.Println()
}
