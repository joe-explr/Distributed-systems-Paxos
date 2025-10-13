Name: Joseph B Antony
SBU ID: 116006668


Notes:
1. Used LLMs aid for understanding Go code practices, synchronization and deadlock debugging.
2. Timer 2 strategy is used. 
3. Checkpointing is not done.
4. The application is run from main.go file 
5. the terminal interaction implementation and CSV parser was mostly created using LLM generated code.
6. SQLite3 used for persistent storage.
7. My interactive Grader Marks successful test cases as failure too, cuz the db doesnt match wit hthe inactive nodes too.

What might be breaking currently:


Run cmd: `go build -o main main.go`
to execute the application.

=== System Ready ===
Available commands:

  transaction <client_id> <sender> <receiver> <amount> - Send transaction

  status <node_id> - Get node status

  log <node_id> - Print node log

  db <node_id> - Print node database (PrintDB)

  printStatus <node_id> <sequence> - Print transaction status

  printView <node_id> - Print new-view messages

  failLeader - Fail the current leader

  activate <node_id> - Activate a node

  deactivate <node_id> - Deactivate a node

  loadTest <csv_file> - Load and process test cases from CSV

  runAllTests - Run comprehensive test suite automatically

  validateState - Validate current system state

  resetSystem - Reset system to initial state

  resetDB - Reset persisted database to initial state ($10 each)

  quit - Exit the program




