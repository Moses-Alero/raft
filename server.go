//implementation of the server logic for sending RPC messages in raft
// implemetation is similar to that used by eli. 
// Tried my best to make sure I understood what was happening here
// 

package main

import (
  "fmt"
  "net"
  "net/rpc"
  "sync"
)


type Server struct{
  mux       sync.Mutex

  serverId  int
  peerIds   []int

  node      *NodeModel

  rpcServer *rpc.Server

  listener  net.Listener

  peerClients map[int]*rpc.Client
  
  ready       <- chan interface{}
  quit        chan interface{}

  wg          sync.WaitGroup
}





func NewServer(serverId int, peerIds []int, ready <-chan interface{}) *Server {
	s := new(Server)
	s.serverId = serverId
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.ready = ready
	s.quit = make(chan interface{})
	return s
}


//sends RPC calls nodes in the cluster
func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
  s.mux.Lock() 
  peer := s.peerClients[id]
  s.mux.Unlock()


  if peer == nil {
    return fmt.Errorf("Client %d  is most likely closed partitioned or dead", id)
  }else {
    return peer.Call(serviceMethod, args, reply)
  }

}




