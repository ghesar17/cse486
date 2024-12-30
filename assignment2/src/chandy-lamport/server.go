package chandy_lamport

import (
	"fmt"
	"log"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	snapped       bool
    snapshot      SyncMap  		   // key = serverIds : value = SnapshotState
	snapshotStarters []string      // serverIds
	// track messages sent to server AFTER its snapped
	afterSnapMessages []SnapshotMessage
	// if the server is listening for messages AFTER its snapped
	listening 	  bool
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		false,
		*NewSyncMap(),
		make([]string,100),
		make([]SnapshotMessage,100),
		false,
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) 
(src string, message interface{}) {
    switch msg := message.(type) {
    case TokenMessage:
		fmt.Println("\nToken Msg Received")
		server.Tokens += msg.numTokens
		if server.listening {
			// if listening for incoming messages, add it to tracking list,

			server.afterSnapMessages = append(server.afterSnapMessages, msg)
		}
    case MarkerMessage:
		// if server is not snapped yet
        if !server.snapped {
			server.snapped = true
			// record state
			snapshot := SnapshotState{
				id:       msg.snapshotId,
				tokens:   make(map[string]int),
				messages: []*SnapshotMessage{},
			}

			fmt.Println("\nMarker Received on", server.Id)
			fmt.Println("Number of tokens", server.Tokens)

			snapshot.id = msg.snapshotId
			snapshot.tokens[server.Id] = server.Tokens

			server.snapshot.Store(server.Id,snapshot)

			server.SendToNeighbors(MarkerMessage{snapshotId: msg.snapshotId})

			server.listening = true
		
        } else {
			// if snapped already
			// record incoming channel as all msgs it received after it snapped

			// update snapshot field, we only want to change the messages field
			origserverSnapshot, _ := server.snapshot.Load(server.Id)
			origServerSnap := origserverSnapshot.(SnapshotState)
			server.snapshot.Delete(server.Id)

			// updated snap
			updatedSnapshot := SnapshotState{
				id:       origServerSnap.id,
				tokens:   make(map[string]int),
				messages: []*SnapshotMessage{},
			}

			updatedSnapshot.id = origServerSnap.id
			updatedSnapshot.tokens[server.Id] = origServerSnap.tokens[server.Id]
			updatedSnapshot.messages = append(updatedSnapshot.messages, server.afterSnapMessages)


			// SnapshotMessage := SnapshotMessage{
			// 	src: src,
			// 	dest: server.Id,
			// 	message: TokenMessage,
			// }

			// serverSnapshot, _ := server.snapshot.Load(server.Id)
			// serverSnap := serverSnapshot.(SnapshotState)
			// serverSnap.messages = append(serverSnap.messages, &SnapshotMessage)

			}

		// check if server is a server where snapshot begun
		// if contains(server.snapshotStarters, server.Id){
		// 	// if received markers on all incoming channels then snapshot is complete
		// 	if len(server.receivedIncoming) == len(server.inboundLinks){
		// 		server.sim.NotifySnapshotComplete(server.Id, msg.snapshotId)
		// 	}
		// }
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {

	server.snapped = true
	server.snapshotStarters = append(server.snapshotStarters, server.Id)

	// record state
	snapshot := SnapshotState{
		id:       snapshotId,
		tokens:   make(map[string]int),
		messages: []*SnapshotMessage{},
	}
	fmt.Println("\nstartSnapshot called")
	// record state
	snapshot.id = snapshotId
	snapshot.tokens[server.Id] = server.Tokens
	server.snapshot.Store(server.Id,snapshot)

	server.SendToNeighbors(MarkerMessage{snapshotId: snapshotId})	
	
	// record for any incoming messages
	server.listening = true

}