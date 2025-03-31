package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type RaftNode int

type VoteArguments struct {
	Term        int
	CandidateID int
}

type VoteReply struct {
	Term       int
	ResultVote bool
}

type AppendEntryArgument struct {
	Term     int
	LeaderID int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}

var selfID int
var serverNodes []ServerConnection
var currentTerm int
var votedFor int = -1
var electionTimer *time.Timer
var mutex sync.Mutex
var state string = "follower"
var numVotes int = 0

// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and majority votes
func (node *RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	mutex.Lock()
	defer mutex.Unlock()

	fmt.Println("Received vote request from", arguments.CandidateID, "|| term:", arguments.Term)

	// If the requester's term is higher than our own, we increment our own term and vote yes.
	if arguments.Term > currentTerm {
		currentTerm = arguments.Term // update own term
		votedFor = -1                // New term, so initializing votedFor to be -1
		// Log recency check here - not implemented
		// May want to choose a different leader
		votedFor = arguments.CandidateID
		reply.ResultVote = true
		reply.Term = arguments.Term
		state = "follower" // change our state to follower, in case we were a leader that blipped and came back
		fmt.Println("Voted for", votedFor)
		node.resetElectionTimer()
	} else { // If the requester's term is less than our own, automatic reject.
		reply.ResultVote = false
		reply.Term = currentTerm
	}
	return nil
}

// The AppendEntry RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and heartbeats
func (node *RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	fmt.Println("Received Heartbeat from", arguments.LeaderID, "|| term:", arguments.Term)
	mutex.Lock()
	defer mutex.Unlock()
	if arguments.Term < currentTerm { // not our leader, leader of lower term.
		reply.Success = false
		reply.Term = currentTerm
	} else {
		fmt.Println("Responded to", arguments.LeaderID)
		// respond to our leader
		reply.Success = true
		currentTerm = arguments.Term
		reply.Term = currentTerm
		// reset timer
		node.resetElectionTimer()
	}
	return nil
}

var mutex2 sync.Mutex

func (node *RaftNode) resetElectionTimer() {
	mutex2.Lock()
	defer mutex2.Unlock()

	if electionTimer != nil {
		electionTimer.Stop() // stops existing timer so that only one is running at a time
	}

	// NOTE: RAFT WOULD USE THESE MILLISECONDS, for testing purposes, our timer will take longer so we can see the logs
	// min := 150 * time.Millisecond
	// max := 300 * time.Millisecond
	min := 5 * time.Second
	max := 10 * time.Second
	timerLength := time.Duration(rand.Int63n(max.Nanoseconds()-min.Nanoseconds()) + min.Nanoseconds())
	fmt.Println("Resetting timer to:", timerLength)
	electionTimer = time.AfterFunc(timerLength, func() {
		fmt.Println("Timer Expired. Starting Leader Election ...")
		node.LeaderElection() // start leader election if timer runs out
	})
}

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func (node *RaftNode) LeaderElection() {
	fmt.Println("Starting leader election from node ", selfID)
	node.resetElectionTimer() // set timer for election should be done - allows election to restart if vote is split

	mutex.Lock()
	// increment its current term, turn self into candidate
	currentTerm++
	state = "candidate"
	votedFor = selfID // vote for self
	mutex.Unlock()

	// Issues RequestVote RPCS to all other servers
	for _, server := range serverNodes {
		arguments := VoteArguments{Term: currentTerm, CandidateID: selfID}
		numVotes = 1
		fmt.Println("Requesting vote from ", server.serverID, server.Address)
		go func(server ServerConnection) {
			reply := new(VoteReply)
			err := server.rpcConnection.Call("RaftNode.RequestVote", arguments, &reply)
			if err != nil {
				fmt.Println("Error receiving RequestVoteReply:", err)
				return
			}
			fmt.Println("Received vote from:", server.serverID, "... reply.ResultVote:", reply.ResultVote)
			// Take result of the RPC call, and increment number of votes accordingly
			if reply.ResultVote {
				mutex.Lock()
				numVotes++
				fmt.Println("Received vote from", server.serverID, "... Checking for majority ...")
				// check if there are enough votes for a majority
				if 2*numVotes > len(serverNodes) && state == "candidate" { //majority reached
					fmt.Println("Majority reached")
					// successfully elected as leader
					state = "leader"
					fmt.Println("Successfully elected as leader: ", selfID)
					electionTimer.Stop() // cancel timer, not a split vote, got majority
					heartbeat()          // starts heartbeats, runs forever
				}
				mutex.Unlock()
			}
		}(server)
	}
}

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func heartbeat() {
	// heartbeatInterval := 50 * time.Millisecond
	heartbeatInterval := 2 * time.Second
	for {
		// make sure we are a leader still, if this state has changed, stop sending heartbeats
		if state != "leader" {
			return
		}
		//Send heartbeats to all servers
		for _, server := range serverNodes {
			arguments := AppendEntryArgument{Term: currentTerm, LeaderID: selfID}

			fmt.Println("Sending heartbeat to ", server.serverID, server.Address)
			go func(server ServerConnection) {
				reply := new(AppendEntryReply)
				err := server.rpcConnection.Call("RaftNode.AppendEntry", arguments, &reply)
				if err != nil {
					fmt.Println("Error receiving AppendEntry reply")
					return
				}

				// Step down as leader if it seems that other nodes have a higher term
				if reply.Term > currentTerm {
					state = "follower"
				}
			}(server)
		}
		timer := time.NewTimer(heartbeatInterval)
		<-timer.C
	}
}

func main() {
	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Read the values sent in the command line

	// Get this sever's ID (same as its index for simplicity)
	myID, err := strconv.Atoi(arguments[1])
	if err != nil {
		log.Fatal(err)
	}
	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	myPort := "localhost"

	// Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		log.Printf(text, index)
		if index == myID {
			myPort = text
			index++
			continue
		}
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with readin the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Following lines are to register the RPCs of this object of type RaftNode
	api := new(RaftNode)
	err = rpc.Register(api)
	if err != nil {
		log.Fatal("error registering the RPCs", err)
	}
	rpc.HandleHTTP()
	go http.ListenAndServe(myPort, nil)
	log.Println("serving rpc on port" + myPort)

	// This is a workaround to slow things down until all servers are up and running
	// Idea: wait for user input to indicate that all servers are ready for connections
	// Pros: Guaranteed that all other servers are already alive
	// Cons: Non-realistic work around

	// reader := bufio.NewReader(os.Stdin)
	// fmt.Print("Type anything when ready to connect >> ")
	// text, _ := reader.ReadString('\n')
	// fmt.Println(text)

	// Idea 2: keep trying to connect to other servers even if failure is encountered
	// For fault tolerance, each node will continuously try to connect to other nodes
	// This loop will stop when all servers are connected
	// Pro: Realistic setup
	// Con: If one server is not set up correctly, the rest of the system will halt

	for index, element := range lines {
		// Attemp to connect to the other server node
		client, err := rpc.DialHTTP("tcp", element)
		// If connection is not established
		for err != nil {
			// Record it in log
			log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", element)
		}
		// Once connection is finally established
		// Save that connection information in the servers list
		serverNodes = append(serverNodes, ServerConnection{index, element, client})
		// Record that in log
		fmt.Println("Connected to " + element)
	}

	// Once all the connections are established, we can start the typical operations within Raft
	// Leader election and heartbeats are concurrent and non-stop in Raft

	// HINT 1: You may need to start a thread here (or more, based on your logic)
	// Hint 2: Main process should never stop
	// Hint 3: After this point, the threads should take over
	// Heads up: they never will be done!
	// Hint 4: wg.Wait() might be helpful here
	var wg sync.WaitGroup
	wg.Add(1)
	selfID = myID
	api.resetElectionTimer()
	wg.Wait() // Waits forever, so main process does not stop
}
