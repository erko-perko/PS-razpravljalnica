package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "razpravljalnica/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ServerProcess struct {
	NodeID  string
	Address string
	Port    int
	Cmd     *exec.Cmd
}

type processInfo struct {
	PID       int       `json:"pid"`
	Address   string    `json:"address"`
	StartedAt time.Time `json:"started_at"`
}

type processRegistry struct {
	Version   int                    `json:"version"`
	Processes map[string]processInfo `json:"processes"`
}

// defaultRegistryPath vrne privzeto pot do JSON datoteke, v katero lokalno shranjujemo PID-e zagnanih node-ov.
// Namen je, da jih ob izhodu ali ob ukazu "drop" lahko najdemo in ubijemo (cleanup).
func defaultRegistryPath() string {
	return "control-panel.processes.json"
}

// loadRegistry prebere registry JSON iz diska in ga pretvori v strukturo processRegistry.
// Če datoteka ne obstaja ali je prazna, vrne prazen registry z inicializiranimi mapami, da kasnejše operacije ne padejo.
func loadRegistry(path string) (processRegistry, error) {
	reg := processRegistry{Version: 1, Processes: map[string]processInfo{}}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return reg, nil
		}
		return reg, err
	}
	if len(data) == 0 {
		return reg, nil
	}
	if err := json.Unmarshal(data, &reg); err != nil {
		return processRegistry{Version: 1, Processes: map[string]processInfo{}}, err
	}
	if reg.Processes == nil {
		reg.Processes = map[string]processInfo{}
	}
	if reg.Version == 0 {
		reg.Version = 1
	}
	return reg, nil
}

// saveRegistry varno zapiše processRegistry v JSON datoteko (write-to-temp + rename).
// To zmanjša možnost, da bi v primeru crasha ostala delno zapisana datoteka, in ohrani registry konsistenten.
func saveRegistry(path string, reg processRegistry) error {
	if reg.Version == 0 {
		reg.Version = 1
	}
	if reg.Processes == nil {
		reg.Processes = map[string]processInfo{}
	}
	data, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	_ = os.Remove(path)
	return os.Rename(tmp, path)
}

// registerProcess doda ali posodobi zapis za nodeID v registryju (PID + naslov + timestamp zagona).
// Uporablja se, ko CP lokalno zažene data-plane proces, da ga lahko kasneje zanesljivo ubije.
func registerProcess(registryPath, nodeID, address string, pid int) {
	reg, err := loadRegistry(registryPath)
	if err != nil {
		log.Printf("[CP] warning: failed reading registry %s: %v", registryPath, err)
		return
	}
	reg.Processes[nodeID] = processInfo{PID: pid, Address: address, StartedAt: time.Now()}
	if err := saveRegistry(registryPath, reg); err != nil {
		log.Printf("[CP] warning: failed writing registry %s: %v", registryPath, err)
	}
}

// unregisterProcess odstrani zapis o procesu iz registryja za dani nodeID.
// To se kliče po uspešnem kill-u ali ko ugotovimo, da proces ne obstaja več in želimo pospraviti stanje.
func unregisterProcess(registryPath, nodeID string) {
	reg, err := loadRegistry(registryPath)
	if err != nil {
		return
	}
	delete(reg.Processes, nodeID)
	_ = saveRegistry(registryPath, reg)
}

// killRegisteredProcess poskusi ubiti proces, ki je bil prej registriran za nodeID, in potem počisti registry.
// Vrne status (killed) in opisno sporočilo, da lahko REPL uporabniku jasno pove, kaj se je zgodilo.
func killRegisteredProcess(registryPath, nodeID string) (killed bool, msg string) {
	reg, err := loadRegistry(registryPath)
	if err != nil {
		return false, fmt.Sprintf("failed to read registry: %v", err)
	}
	info, ok := reg.Processes[nodeID]
	if !ok {
		return false, "no local process registered for this node id"
	}
	proc, err := os.FindProcess(info.PID)
	if err != nil {
		unregisterProcess(registryPath, nodeID)
		return false, fmt.Sprintf("failed to find process PID=%d (removed from registry)", info.PID)
	}
	if err := proc.Kill(); err != nil {
		unregisterProcess(registryPath, nodeID)
		return false, fmt.Sprintf("failed to kill PID=%d (removed from registry anyway): %v", info.PID, err)
	}
	unregisterProcess(registryPath, nodeID)
	return true, fmt.Sprintf("killed PID=%d", info.PID)
}

// killAllRegisteredProcesses pobere vse node-e iz registryja in jih best-effort ubije.
// Na koncu odstrani registry datoteko, da naslednji zagon kontrolnega panela začne s čistim stanjem.
func killAllRegisteredProcesses(registryPath string) {
	reg, err := loadRegistry(registryPath)
	if err != nil {
		log.Printf("[CP] warning: failed to read registry for cleanup: %v", err)
		return
	}
	if len(reg.Processes) == 0 {
		_ = os.Remove(registryPath)
		return
	}

	for nodeID, info := range reg.Processes {
		proc, err := os.FindProcess(info.PID)
		if err != nil {
			continue
		}
		if err := proc.Kill(); err != nil {
			continue
		}
		log.Printf("[CP] cleaned up node %s (PID=%d)", nodeID, info.PID)
	}

	_ = os.Remove(registryPath)
}

// parseListenAddress normalizira uporabnikov vnos naslova (port ali host:port) v obliko host, port in "host:port" string.
// Poleg validacije (npr. port je številka) poskrbi, da prazen host privzeto postane "localhost".
func parseListenAddress(input string) (host string, port int, normalized string, err error) {
	if input == "" {
		return "", 0, "", fmt.Errorf("empty address")
	}

	if !strings.Contains(input, ":") {
		p, perr := strconv.Atoi(input)
		if perr != nil {
			return "", 0, "", fmt.Errorf("invalid port/address %q", input)
		}
		return "localhost", p, fmt.Sprintf("localhost:%d", p), nil
	}

	h, pStr, splitErr := net.SplitHostPort(input)
	if splitErr != nil {
		return "", 0, "", fmt.Errorf("invalid address %q (expected host:port)", input)
	}
	p, perr := strconv.Atoi(pStr)
	if perr != nil {
		return "", 0, "", fmt.Errorf("invalid port in %q", input)
	}
	if h == "" {
		h = "localhost"
	}
	return h, p, fmt.Sprintf("%s:%d", h, p), nil
}

// printChain izpiše trenutno stanje verige: kdo je head, kdo je tail in kateri node-i so v chain-u po vrstnem redu.
// Uporablja se v REPL ukazih (state/add/drop), da ima uporabnik hiter pregled nad topologijo.
func printChain(label string, head *pb.NodeInfo, tail *pb.NodeInfo, chain []*pb.NodeInfo) {
	fmt.Printf("%s\n", label)
	fmt.Printf("  head: %s (%s)\n", head.GetNodeId(), head.GetAddress())
	fmt.Printf("  tail: %s (%s)\n", tail.GetNodeId(), tail.GetAddress())
	fmt.Printf("  chain (%d):\n", len(chain))
	for i, n := range chain {
		fmt.Printf("    %d) %s (%s)\n", i+1, n.GetNodeId(), n.GetAddress())
	}
}

// addServerToLocalControlPlane po potrebi lokalno zažene data-plane proces (server.exe) in ga nato registrira v control plane (Join).
// Če Join ne uspe, best-effort ubije pravkar zagnan proces in počisti registry, da ne ostane "ghost" proces v ozadju.
func addServerToLocalControlPlane(cp *controlPlaneServer, nodeID, nodeAddr, controlAddr, serverBin, secret, registryPath string, launch bool, startWait time.Duration) int {
	host, port, normalizedAddr, err := parseListenAddress(nodeAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 2
	}
	if host != "localhost" && host != "127.0.0.1" {
		fmt.Fprintf(os.Stderr, "for this demo, addr host must be localhost/127.0.0.1 (got %q)\n", host)
		return 2
	}

	var startedCmd *exec.Cmd
	if launch {
		cmd := exec.Command(
			serverBin,
			"-n", nodeID,
			"-a", fmt.Sprintf("%d", port),
			"-c", controlAddr,
			"-secret", secret,
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to start server process: %v\n", err)
			return 1
		}
		startedCmd = cmd
		registerProcess(registryPath, nodeID, normalizedAddr, cmd.Process.Pid)
		fmt.Printf("Started server process PID=%d for %s on %s\n", cmd.Process.Pid, nodeID, normalizedAddr)
		time.Sleep(startWait)
	}

	resp, err := cp.Join(context.Background(), &pb.JoinRequest{NodeId: nodeID, Address: normalizedAddr})
	if err != nil {
		if startedCmd != nil && startedCmd.Process != nil {
			_ = startedCmd.Process.Kill()
			unregisterProcess(registryPath, nodeID)
		}
		fmt.Fprintf(os.Stderr, "Join failed: %v\n", err)
		return 1
	}
	printChain("Added server to chain.", resp.GetHead(), resp.GetTail(), resp.GetChain())
	return 0
}

// dropServerFromLocalControlPlane odstrani node iz verige (Leave) in opcijsko ubije tudi lokalni proces, če je bil zagnan prek CP.
// Po spremembi verige počaka kratek čas, da se konfiguracija razširi, nato pa vrne rezultat ukaza REPL.
func dropServerFromLocalControlPlane(cp *controlPlaneServer, nodeID, registryPath string, killLocal bool) int {
	if killLocal {
		_, msg := killRegisteredProcess(registryPath, nodeID)
		fmt.Printf("Local process: %s\n", msg)
	}
	resp, err := cp.Leave(context.Background(), &pb.LeaveRequest{NodeId: nodeID})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Leave failed: %v\n", err)
		return 1
	}
	printChain("Dropped server from chain.", resp.GetHead(), resp.GetTail(), resp.GetChain())
	time.Sleep(500 * time.Millisecond)
	return 0
}

// printReplHelp izpiše seznam podprtih REPL ukazov in kratko razlago njihove uporabe.
// S tem uporabniku omogoča, da brez branja kode hitro vidi, kako upravljati verigo.
func printReplHelp() {
	fmt.Println("Commands:")
	fmt.Println("  help                         Show this help")
	fmt.Println("  state                        Print current chain state")
	fmt.Println("  add <nodeId> <port|host:port> [--no-launch]")
	fmt.Println("  drop <nodeId>                Drop from chain + kill local process")
	fmt.Println("  drop <nodeId> --no-kill      Drop from chain without killing")
	fmt.Println("  exit | quit                  Stop control panel")
}

// runRepl izvaja interaktivno zanko, ki bere ukaze iz stdin in kliče ustrezne metode control-plane strežnika.
// Podpira ogled stanja (state), dodajanje node-a (add) in odstranjevanje node-a (drop), ter omogoča urejen izhod prek stop().
func runRepl(cp *controlPlaneServer, controlAddr, serverBin, secret, registryPath string, startWait time.Duration, stop func()) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Control panel REPL ready. Type 'help' for commands.")
	for {
		fmt.Print("cp> ")
		if !scanner.Scan() {
			stop()
			return
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		cmd := strings.ToLower(fields[0])
		switch cmd {
		case "help", "?":
			printReplHelp()
		case "state":
			resp, err := cp.GetClusterState(context.Background(), &emptypb.Empty{})
			if err != nil {
				fmt.Fprintf(os.Stderr, "GetClusterState failed: %v\n", err)
				continue
			}
			printChain("Cluster state:", resp.GetHead(), resp.GetTail(), resp.GetChain())
		case "add":
			if len(fields) < 3 {
				fmt.Fprintln(os.Stderr, "usage: add <nodeId> <port|host:port> [--no-launch]")
				continue
			}
			nodeID := fields[1]
			nodeAddr := fields[2]
			launch := true
			for _, f := range fields[3:] {
				if f == "--no-launch" {
					launch = false
				}
			}
			_ = addServerToLocalControlPlane(cp, nodeID, nodeAddr, controlAddr, serverBin, secret, registryPath, launch, startWait)
		case "drop", "remove":
			if len(fields) < 2 {
				fmt.Fprintln(os.Stderr, "usage: drop <nodeId> [--no-kill]")
				continue
			}
			nodeID := fields[1]
			killLocal := true
			for _, f := range fields[2:] {
				if f == "--no-kill" {
					killLocal = false
				}
			}
			_ = dropServerFromLocalControlPlane(cp, nodeID, registryPath, killLocal)
		case "exit", "quit":
			stop()
			return
		default:
			fmt.Fprintf(os.Stderr, "unknown command: %s (type 'help')\n", fields[0])
		}
	}
}

type nodeStatus struct {
	info          *pb.NodeInfo
	lastHeartbeat time.Time
	lastApplied   int64
	alive         bool
	subCount      int64
}

type controlPlaneServer struct {
	pb.UnimplementedControlPlaneServer

	lock sync.Mutex

	chain []*pb.NodeInfo
	nodes map[string]*nodeStatus

	heartbeatTimeout time.Duration
}

// newControlPlane inicializira control-plane stanje: prazno verigo, mapo node-ov in timeout za heartbeat failure detection.
// Vse spremembe stanja so kasneje zaščitene z mutexom, da so RPC-ji varni za sočasno izvajanje.
func newControlPlane(timeout time.Duration) *controlPlaneServer {
	return &controlPlaneServer{
		chain:            make([]*pb.NodeInfo, 0),
		nodes:            make(map[string]*nodeStatus),
		heartbeatTimeout: timeout,
	}
}

// cloneNodeInfo naredi plitvo kopijo NodeInfo, da ne vračamo ali delimo mutable pointerjev iz notranjega stanja.
// To zmanjša možnost race conditionov, ko se chain posodablja, medtem ko drug goroutine bere podatke za response.
func cloneNodeInfo(n *pb.NodeInfo) *pb.NodeInfo {
	if n == nil {
		return nil
	}
	return &pb.NodeInfo{NodeId: n.GetNodeId(), Address: n.GetAddress()}
}

// GetClusterState vrne trenutni snapshot verige: head, tail in celoten seznam node-ov po vrstnem redu.
// Odgovor vedno vsebuje kopije NodeInfo, da klient ali REPL ne more nenamerno spreminjati notranjega stanja CP.
func (c *controlPlaneServer) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	head, tail := c.getHeadTailLocked()
	chain := make([]*pb.NodeInfo, 0, len(c.chain))
	for _, n := range c.chain {
		chain = append(chain, cloneNodeInfo(n))
	}
	return &pb.GetClusterStateResponse{
		Head:  head,
		Tail:  tail,
		Chain: chain,
	}, nil
}

// ConfigureChain omogoča, da CP ročno nastavi verigo na seznam node-ov iz requesta.
// Po posodobitvi stanja v ozadju sproži pushChainToAll, da vsi data-plane node-i dobijo novo topologijo prek ConfigureChain RPC-ja.
func (c *controlPlaneServer) ConfigureChain(ctx context.Context, req *pb.ConfigureChainRequest) (*emptypb.Empty, error) {
	c.lock.Lock()
	c.chain = append([]*pb.NodeInfo(nil), req.GetNodes()...)
	for _, n := range c.chain {
		if _, ok := c.nodes[n.GetNodeId()]; !ok {
			c.nodes[n.GetNodeId()] = &nodeStatus{
				info:          &pb.NodeInfo{NodeId: n.GetNodeId(), Address: n.GetAddress()},
				lastHeartbeat: time.Time{},
				lastApplied:   0,
				alive:         false,
			}
		}
	}
	c.lock.Unlock()

	go c.pushChainToAll()
	return &emptypb.Empty{}, nil
}

// GetLeastLoadedNode izbere "najmanj obremenjen" node glede na subCount, pri čemer upošteva samo node-e, ki so alive in v chain vrstnem redu.
// To se uporablja za load-balancing subscription streamov, da se naročnine porazdelijo in ne obremenjujejo enega samega node-a.
func (c *controlPlaneServer) GetLeastLoadedNode(ctx context.Context, _ *emptypb.Empty) (*pb.NodeInfo, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var best *pb.NodeInfo
	var bestCount int64 = -1

	for _, n := range c.chain {
		st, ok := c.nodes[n.GetNodeId()]
		if !ok || !st.alive {
			continue
		}
		if best == nil || st.subCount < bestCount {
			best = st.info
			bestCount = st.subCount
		}
	}

	if best == nil {
		return &pb.NodeInfo{NodeId: "none", Address: ""}, nil
	}
	return &pb.NodeInfo{NodeId: best.GetNodeId(), Address: best.GetAddress()}, nil
}

// Heartbeat posodobi status node-a (alive, lastHeartbeat, lastApplied, subCount) in ga po potrebi doda v verigo.
// Če je to prvič, da CP vidi node (npr. node pošlje heartbeat pred Join), se node avtomatsko doda in topologija se nato razpošlje vsem.
func (c *controlPlaneServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	var changed bool

	c.lock.Lock()
	st, ok := c.nodes[req.GetNodeId()]
	if !ok {
		info := &pb.NodeInfo{NodeId: req.GetNodeId(), Address: req.GetAddress()}
		st = &nodeStatus{
			info:          info,
			lastHeartbeat: time.Now(),
			lastApplied:   req.GetLastAppliedSeq(),
			alive:         true,
			subCount:      0,
		}
		st.subCount = req.GetSubCount()
		c.nodes[req.GetNodeId()] = st
		changed = c.appendToChainIfMissing(info)
	} else {
		st.info.Address = req.GetAddress()
		st.lastHeartbeat = time.Now()
		st.lastApplied = req.GetLastAppliedSeq()
		st.subCount = req.GetSubCount()
		if !st.alive {
			st.alive = true
			if c.appendToChainIfMissing(st.info) {
				changed = true
			} else {
				changed = true
			}
		} else {
			st.alive = true
		}
	}
	c.lock.Unlock()

	if changed {
		go c.pushChainToAll()
	}
	return &pb.HeartbeatResponse{Ok: true}, nil
}

// Join registrira node v CP (ali posodobi njegove podatke) in poskrbi, da je node prisoten v verigi brez duplikatov.
// Če se topologija spremeni, CP asinhrono potisne novo verigo vsem živim node-om, da se uskladijo head/tail/pred/succ.
func (c *controlPlaneServer) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	var changed bool

	c.lock.Lock()
	st, ok := c.nodes[req.GetNodeId()]
	if !ok {
		info := &pb.NodeInfo{NodeId: req.GetNodeId(), Address: req.GetAddress()}
		c.nodes[req.GetNodeId()] = &nodeStatus{
			info:          info,
			lastHeartbeat: time.Now(),
			lastApplied:   0,
			alive:         true,
		}
		changed = c.appendToChainIfMissing(info)
		st = c.nodes[req.GetNodeId()]
	} else {
		st.info.Address = req.GetAddress()
		st.lastHeartbeat = time.Now()
		if !st.alive {
			st.alive = true
		}
		changed = c.appendToChainIfMissing(st.info)
	}

	head, tail := c.getHeadTailLocked()
	chain := make([]*pb.NodeInfo, 0, len(c.chain))
	for _, n := range c.chain {
		chain = append(chain, cloneNodeInfo(n))
	}
	resp := &pb.JoinResponse{Head: head, Tail: tail, Chain: chain}
	c.lock.Unlock()

	if changed {
		go c.pushChainToAll()
	}
	_ = st
	return resp, nil
}

// Leave odstrani node iz CP evidence in ga odstrani tudi iz verige, če je bil prisoten.
// Po odstranitvi CP razpošlje novo topologijo vsem živim node-om, da se veriga stabilizira in se izračunajo novi head/tail.
func (c *controlPlaneServer) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	c.lock.Lock()
	_, ok := c.nodes[req.GetNodeId()]
	if ok {
		delete(c.nodes, req.GetNodeId())
		c.chain = filterChain(c.chain, req.GetNodeId())
	}
	head, tail := c.getHeadTailLocked()
	chain := make([]*pb.NodeInfo, 0, len(c.chain))
	for _, n := range c.chain {
		chain = append(chain, cloneNodeInfo(n))
	}
	c.lock.Unlock()

	go c.pushChainToAll()
	return &pb.LeaveResponse{Head: head, Tail: tail, Chain: chain}, nil
}

// getHeadTailLocked vrne head in tail iz trenutne verige; kliče se samo pod mutexom.
// Če je veriga prazna, vrne sentinel vrednosti ("none"), da se klienti lahko temu prilagodijo.
func (c *controlPlaneServer) getHeadTailLocked() (*pb.NodeInfo, *pb.NodeInfo) {
	if len(c.chain) == 0 {
		return &pb.NodeInfo{NodeId: "none", Address: ""}, &pb.NodeInfo{NodeId: "none", Address: ""}
	}
	return cloneNodeInfo(c.chain[0]), cloneNodeInfo(c.chain[len(c.chain)-1])
}

// filterChain vrne novo verigo brez node-a z danim nodeID, pri čemer ohrani prvotni vrstni red.
// Uporablja se pri Leave in pri failure detectionu, ko CP odstrani node iz topologije.
func filterChain(chain []*pb.NodeInfo, nodeID string) []*pb.NodeInfo {
	out := make([]*pb.NodeInfo, 0, len(chain))
	for _, n := range chain {
		if n.GetNodeId() != nodeID {
			out = append(out, n)
		}
	}
	return out
}

// inChain preveri, ali se nodeID že pojavi v trenutni verigi.
// To preprečuje duplikate, ko node pošlje Join/Heartbeat večkrat ali se ponovno oživi.
func (c *controlPlaneServer) inChain(nodeID string) bool {
	for _, n := range c.chain {
		if n.GetNodeId() == nodeID {
			return true
		}
	}
	return false
}

// appendToChainIfMissing doda node v verigo samo, če še ni prisoten, in vrne ali je prišlo do spremembe.
// CP node-e tipično dodaja na konec (tail), kar poenostavi rast verige in re-konfiguracije.
func (c *controlPlaneServer) appendToChainIfMissing(info *pb.NodeInfo) bool {
	if c.inChain(info.GetNodeId()) {
		return false
	}
	c.chain = append(c.chain, info)
	return true
}

// monitorFailuresLoop periodično preverja, ali so node-i zamudili heartbeat preko heartbeatTimeout.
// Ko node označi kot mrtev, ga odstrani iz aktivne verige (ohrani vrstni red živih) in sproži push nove topologije.
func (c *controlPlaneServer) monitorFailuresLoop(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		c.lock.Lock()
		now := time.Now()

		changed := false
		for id, st := range c.nodes {
			if st.alive && now.Sub(st.lastHeartbeat) > c.heartbeatTimeout {
				log.Printf("[CP] node %s considered dead (last heartbeat %v ago)", id, now.Sub(st.lastHeartbeat))
				st.alive = false
				changed = true
			}
		}

		if changed {
			newChain := make([]*pb.NodeInfo, 0, len(c.chain))
			for _, n := range c.chain {
				st, ok := c.nodes[n.GetNodeId()]
				if ok && st.alive {
					newChain = append(newChain, st.info)
				}
			}
			c.chain = newChain
		}
		c.lock.Unlock()

		if changed {
			go c.pushChainToAll()
		}
	}
}

// pushChainToAll pripravi snapshot trenutne verige in ga pošlje vsem živim node-om preko ConfigureChain RPC-ja.
// Snapshotiranje pod lockom prepreči, da bi goroutine-i delili pointerje, ki se medtem spreminjajo.
func (c *controlPlaneServer) pushChainToAll() {
	c.lock.Lock()
	nodes := make([]*pb.NodeInfo, 0, len(c.chain))
	for _, n := range c.chain {
		nodes = append(nodes, cloneNodeInfo(n))
	}
	aliveTargets := make([]*pb.NodeInfo, 0, len(c.chain))
	for _, n := range c.chain {
		if st, ok := c.nodes[n.GetNodeId()]; ok && st.alive {
			aliveTargets = append(aliveTargets, cloneNodeInfo(st.info))
		}
	}
	c.lock.Unlock()

	for _, n := range aliveTargets {
		c.pushChainToNode(n.GetAddress(), nodes)
	}
}

// pushChainToNode poskuša poslati novo topologijo na en data-plane node z omejenim številom retry-jev.
// To ublaži napake (npr. node se ravno zaganja), brez potrebe po stalnem "spam" pushanju konfiguracije.
func (c *controlPlaneServer) pushChainToNode(addr string, nodes []*pb.NodeInfo) {
	if addr == "" {
		return
	}
	for attempt := 1; attempt <= 3; attempt++ {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
			continue
		}
		client := pb.NewMessageBoardClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = client.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
		cancel()
		_ = conn.Close()
		if err == nil {
			return
		}
		log.Printf("Napaka v konfiguraciji strežnika %s (attempt %d/3): %v", addr, attempt, err)
		time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
	}
}

// startServers lokalno zažene več data-plane server procesov in jih zapiše v registry za kasnejši cleanup.
// Namenjeno je demo načinu (-launch), da lahko z enim ukazom dobiš celo verigo node-ov brez ročnega zaganjanja.
func startServers(numServers int, headPort int, controlAddr string, secret string, serverBinary string, registryPath string) []*ServerProcess {
	servers := make([]*ServerProcess, 0, numServers)
	for i := 0; i < numServers; i++ {
		nodeID := fmt.Sprintf("node-%d", i+1)
		port := headPort + i
		address := fmt.Sprintf("localhost:%d", port)

		log.Printf("Starting %s on %s ...", nodeID, address)

		cmd := exec.Command(
			serverBinary,
			"-n", nodeID,
			"-a", fmt.Sprintf("%d", port),
			"-c", controlAddr,
			"-secret", secret,
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if runtime.GOOS == "windows" {
		}

		if err := cmd.Start(); err != nil {
			log.Printf("Failed to start %s: %v", nodeID, err)
			continue
		}
		if cmd.Process != nil {
			registerProcess(registryPath, nodeID, address, cmd.Process.Pid)
		}

		servers = append(servers, &ServerProcess{
			NodeID:  nodeID,
			Address: address,
			Port:    port,
			Cmd:     cmd,
		})
		time.Sleep(500 * time.Millisecond)
		log.Printf("Started %s (PID=%d)", nodeID, cmd.Process.Pid)
	}
	return servers
}

// main zažene gRPC ControlPlane strežnik, starta failure monitor in odpre REPL za interaktivno upravljanje verige.
// Ob izhodu iz programa naredi cleanup: ubije lokalno zagnane node procese (če obstajajo) in ustavi gRPC server.
func main() {
	numServers := flag.Int("n", 3, "number of servers in the chain")
	headPort := flag.Int("p", 50051, "head server port")
	controlPort := flag.Int("cp", 60000, "control plane port")
	timeoutMs := flag.Int("to", 2000, "heartbeat timeout (ms)")
	secret := flag.String("secret", "dev-secret-change-me", "token signing secret (must match servers)")

	launch := flag.Bool("launch", false, "also launch data-plane servers as OS processes")
	serverBin := flag.String("serverbin", "../server/server.exe", "path to server binary (used only with -launch)")
	registryPath := flag.String("registry", defaultRegistryPath(), "path to local process registry (for drop-server kill)")
	startWait := flag.Duration("start-wait", 600*time.Millisecond, "delay after starting a server before Join (REPL add)")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage:")
		fmt.Fprintln(os.Stderr, "  control-panel [flags]")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Starts the control plane and opens an interactive REPL on stdin.")
		fmt.Fprintln(os.Stderr, "REPL commands: help, state, add <nodeId> <port|host:port>, drop <nodeId>, exit")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Flags:")
		flag.PrintDefaults()
	}

	flag.Parse()

	controlAddr := fmt.Sprintf("localhost:%d", *controlPort)
	log.Printf("Control Plane listening on %s (timeout=%dms)", controlAddr, *timeoutMs)

	lis, err := net.Listen("tcp", controlAddr)
	if err != nil {
		log.Fatal(err)
	}

	cp := newControlPlane(time.Duration(*timeoutMs) * time.Millisecond)
	go cp.monitorFailuresLoop(context.Background())

	grpcServer := grpc.NewServer()
	pb.RegisterControlPlaneServer(grpcServer, cp)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	stopChan := make(chan struct{})
	stopOnce := sync.Once{}
	stop := func() {
		stopOnce.Do(func() { close(stopChan) })
	}

	var servers []*ServerProcess
	if *launch {
		servers = startServers(*numServers, *headPort, controlAddr, *secret, *serverBin, *registryPath)
		if len(servers) == 0 {
			log.Fatal("No servers started")
		}
		log.Println("Servers started. Press Ctrl+C to stop...")
	} else {
		log.Println("Control plane running. (Use -launch to start data-plane nodes automatically.)")
	}

	go runRepl(cp, controlAddr, *serverBin, *secret, *registryPath, *startWait, stop)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sigChan:
	case <-stopChan:
	}

	log.Println("Shutting down control plane...")
	killAllRegisteredProcesses(*registryPath)
	grpcServer.GracefulStop()

	if *launch {
		log.Println("Shutting down servers...")
		for _, s := range servers {
			if s.Cmd != nil && s.Cmd.Process != nil {
				_ = s.Cmd.Process.Kill()
			}
		}
	}
}
