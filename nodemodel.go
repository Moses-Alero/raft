//Some what implementatioin of the raft algorithm 
//Guides/Resources 
  //https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/
  //https://raft.github.io/raft.pdf
  //https://github.com/eliben/raft/


package main

import (
  "fmt"
  "time"
  "sync"
  "log"
  "math/rand"
)

type LogEntries struct {
    command interface{} // this is the input from the client
    term    int            // this is term term the input was 
}

type CommitEntries struct{
    command  interface{}
    term     int
    entryIndex int   
}

type NodeState int

const (
    Follower NodeState = iota
    Candidate
    Leader
    Dead //nodes can also be dead
)



//This is the information a node sends to it's peers
//when it is campaigining for votes

type RequestVoteArgs struct{
  term         int
  candidateId  int
  lastLogUndex int 
  lastLogTerm  int  
}

//Response from nodes in the cluster after they have been lobbied for votes
type RequestVoteReplyArgs struct{
  term        int
  voteGranted bool
}

type AppendEntriesArgs struct{
  term     int 
  leaderId int
  prevLogIndex int
  prevLogTerm  int
  entries      []LogEntries

  leaderCommit int
}

type AppendEntriesReplyArgs struct{
  term  int
  success bool
}

// define the node state / model for maintaining consensus

type NodeModel struct{
  mux         sync.Mutex 
  id          int
  //volatile state info
  state       NodeState
  timeToNextElection time.Time

  //persistent state info for nodes
  currentTerm int
  votedFor    int
  log         []LogEntries

  //volatile state
  commitIndex int
  lastApplied int

  peerIds     []int
  server      *Server    //RPC server for communnicating with the other node is in the cluster
}


func NewNode(id int, peerIds []int, server *Server, ready <-chan interface{}) *NodeModel{
    node := new(NodeModel)
    node.state = Follower
    node.id = id
    node.peerIds = peerIds
    node.server = server
    // the voted for value is initiated to -1 because in this implematation 
    // node ids start with 0, real world application might use uuids for node ids
    node.votedFor = -1 
                      
    go func(){
      // the ready channel tell the node that it is connected to all the other nodes in the cluseter
      // thus it can begin its state machine as well as the leader election

      <- ready
      node.mux.Lock()
      node.timeToNextElection = time.Now()
      node.mux.Unlock()
      //start the election timer
      
      node.RunElectionTimer()
    }()

    return node 
}

//submits a client command to a node 
//returns a true or false to the client if the node is a leader node or not,
//note that this is not suffivent and a more intitutive ethod is needed to inform the client
//that the node is nit the leader node.
func (node *NodeModel) Submit(command interface{}) bool{
  node.mux.Lock()
  defer node.mux.Unlock()

  if node.state != Leader{
    node.Logger("Received command: %v from client when not in Leader State", command)
    return false
  }
  node.Logger("Command received from cient, command: %v", command)
  logEntry := LogEntries{
    command: command,
    term:    node.currentTerm,
  }
  node.log =  append(node.log, logEntry)
  return true
  
}

func (node *NodeModel) Logger(message string, args ...interface{}) {
  formattedMessage := fmt.Sprintf("<%d> ", node.id) + message
  log.Printf(formattedMessage, args...) 
}

//generates a random time between 150ms and 300ms
func (node *NodeModel) ElectionTimeout() time.Duration{
  return time.Duration(rand.Intn(150) + 150) * time.Millisecond
}

func (node *NodeModel) ChangeToFollower(term int){
  node.state = Follower
  node.currentTerm = term
  node.votedFor = -1
  node.timeToNextElection = time.Now()


  go node.RunElectionTimer()
}

// this starts a timer that counts down till when a node changes state
//from follower ti candidate
func (node *NodeModel) RunElectionTimer(){
  //get the time to next election and the current term
  timeDurationToElection := node.ElectionTimeout()
  node.mux.Lock()
  termStarted := node.currentTerm
  node.mux.Unlock()

  node.Logger("An election countdown to election has started (%v), term started = %d", timeDurationToElection, termStarted)
  
  //Start a countdown that checks periodically if
  // 1. The node is still in either Follower or Candidate State
  // 2. The term hasn't changed
  // 3.  if the timeToNextElection has elapsed in order to start an election

  ticker := time.NewTicker(10 * time.Millisecond)
  defer ticker.Stop()

  for {
    <- ticker.C //listens for the data from the ticker channel

    node.mux.Lock()

    if node.state != Follower || node.state != Candidate{ 
      node.Logger("Election has already taken place and I've either become Leader :) or I'm Dead :(, state is %s",node.state)
      node.mux.Unlock()
      return
    }

    if termStarted != node.currentTerm && termStarted < node.currentTerm {
      node.Logger("Election has taken place somewhere else and a leader has been chosen. Term changed from %d to %d", termStarted, node.currentTerm)
      node.mux.Unlock()
      return
    }

    elapsed := time.Since(node.timeToNextElection)

    if elapsed >= timeDurationToElection{
      node.StartElection()
      node.mux.Unlock()
      return
    }

    node.mux.Unlock()

  } 

}

//changs node to LeaderState and begins doing Leader activities
//Such as sending hearteats and receiving inputs(commands) from clients
func (node *NodeModel) StartLeader(){
  node.state = Leader

  node.Logger("I have become the Leader of the cluster. Bow to me, currentTerm=%d, logs: %v", node.currentTerm, node.log)

  go func(){
  ticker := time.NewTicker(50 * time.Millisecond)

  defer ticker.Stop()
  for {
    //send heart beats periodically
    node.SendHeartBeats()
    <- ticker.C
    
    node.mux.Lock()
    if node.state != Leader{
      node.mux.Unlock()
      return
    }
    node.mux.Unlock()
  }
    
  }()
}

func (node *NodeModel) SendHeartBeats(){
  node.mux.Lock()
  termOnHeartBeatSend := node.currentTerm
  node.mux.Unlock()
    
  heartBeatArgs := AppendEntriesArgs{
    term:  termOnHeartBeatSend,
    leaderId:  node.id,
  }

  var reply AppendEntriesReplyArgs
  for _, peerId := range node.peerIds{
      
    go func(peerId int){
      
      node.Logger("Sending Heartbeats to peer{%d}, with args=%v", termOnHeartBeatSend, heartBeatArgs)
      err := node.server.Call(peerId, "NodeModel.AppendEntries", heartBeatArgs, &reply)
      if err == nil{
        node.mux.Lock()
        defer node.mux.Unlock()

        node.Logger("Reply received from peer{%d}, with args %v", peerId, reply)

        if reply.term > termOnHeartBeatSend{
          node.Logger("Seems my term is outdated, term has change from %d to %d", termOnHeartBeatSend, reply.term)
          node.ChangeToFollower(reply.term)
          return
        }
      }
      
    }(peerId) 
  }
}


func (node *NodeModel) StartElection(){
  node.state = Candidate
  node.currentTerm += 1
  termElectionStarted :=  node.currentTerm
  node.votedFor = node.id //node votes for itself
  
  node.Logger("I have become a Candidate now currently campaigning and waiting for votes from my peers, currentTerm=%d, log=%v",termElectionStarted, node.log )
  votesReceived := 1
  //Send RequestVote RPCs to all other nodes 
  for _, peerId := range node.peerIds{
     go func(peerId int){
        
      requestVoteArgs := RequestVoteArgs{
        term:     termElectionStarted,
        candidateId:  node.id,
      } 
      
      var reply RequestVoteReplyArgs

      node.Logger("Lobbying peer--> %d for votes with args %v", peerId, requestVoteArgs)
      err := node.server.Call(peerId, "NodeModel.RequestVote", requestVoteArgs, &reply) 
      if err == nil{
        node.mux.Lock()
        defer node.mux.Unlock()

        node.Logger("Peers response to Lobbying: %v", reply)
        
        if node.state != Candidate{
          node.Logger("My state probably changed to %v while I was waitng for a reply", node.state)
          return
        }
        
        if reply.term > termElectionStarted{
          node.Logger("Looks like my term is outdated, current Term is %v", reply.term)
          node.ChangeToFollower(reply.term)
          return
        }else if reply.term == termElectionStarted{
          if reply.voteGranted{
            votesReceived += 1 
            if votesReceived*2 > len(node.peerIds)+1{
              node.Logger("I have won the election by %d votes, I promise it was free and fair :p", votesReceived)
              node.StartLeader()
              return
            }
          } 
        }
      }
     }(peerId) 
  } 
  // if the election is unsuccessful run the elcetion again
  go node.RunElectionTimer()
}

func (node *NodeModel) RequestVote(requestArgs RequestVoteArgs, reply *RequestVoteReplyArgs) error {
  node.mux.Lock()
  defer node.mux.Unlock()
  //check if the node is dead
  if node.state == Dead{
    return nil
  }
  //check if the term from the is candidate is greater
  if requestArgs.term > node.currentTerm{
    node.Logger("Looks like my peer has a higher term of %d than mine %d", requestArgs.term, node.currentTerm)
    node.currentTerm = requestArgs.term
  }
  //check if the terms match
  if requestArgs.term == node.currentTerm && (node.votedFor == -1 || node.votedFor == requestArgs.candidateId){
    reply.term = node.currentTerm 
    node.votedFor = requestArgs.candidateId
    reply.voteGranted = true
  } else{
    reply.voteGranted = false
  }
  node.Logger("Request for vote has been replied to with %v", reply)
  return nil
}

func (node *NodeModel) AppendEntries(entriesArgs AppendEntriesArgs, reply *AppendEntriesReplyArgs) error{
  // check if the node is still alive
  node.mux.Lock()
  defer node.mux.Unlock()

  if node.state == Dead{
    return nil
  }

  node.Logger("AppendEntries to node from Leader: %v", entriesArgs)

  if entriesArgs.term > node.currentTerm{
      node.Logger("My term is outdated needs to updated to new term %d", entriesArgs.term)
      node.ChangeToFollower(entriesArgs.term)
  }

  reply.success = false

  if entriesArgs.term == node.currentTerm{
    if node.state != Follower{
      node.ChangeToFollower(entriesArgs.term)
      node.timeToNextElection = time.Now()
    }
    reply.success = true
  }

  node.Logger("reply sent back to Leader: %v", *reply)
  return nil
}
