package main

import (
	"context"
	"testing"
	"time"

	pb "razpravljalnica/pb"

	"google.golang.org/protobuf/types/known/emptypb"
)

func eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v", timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Test preveri, da newControlPlane pravilno inicializira vse notranje strukture.
// Preveri timeout, prazno verigo in prazno mapo node-ov.
// S tem potrdi, da je začetno stanje deterministično in pripravljeno na Join/Heartbeat.
func TestNewControlPlane(t *testing.T) {
	timeout := 2 * time.Second
	cp := newControlPlane(timeout)

	if cp == nil {
		t.Fatal("newControlPlane returned nil")
	}
	if cp.heartbeatTimeout != timeout {
		t.Errorf("expected timeout %v, got %v", timeout, cp.heartbeatTimeout)
	}
	if len(cp.chain) != 0 {
		t.Errorf("expected empty chain, got %d nodes", len(cp.chain))
	}
	if len(cp.nodes) != 0 {
		t.Errorf("expected empty nodes map, got %d nodes", len(cp.nodes))
	}
}

// Test preveri, da GetClusterState na praznem clustru vrne "none" za head in tail.
// Preveri tudi, da je vrnjena veriga prazna.
// To je osnovni sanity check za prikaz stanja pred registracijo node-ov.
func TestGetClusterStateEmpty(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	resp, err := cp.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetClusterState failed: %v", err)
	}

	if resp.Head.GetNodeId() != "none" {
		t.Errorf("expected head to be 'none', got %s", resp.Head.GetNodeId())
	}
	if resp.Tail.GetNodeId() != "none" {
		t.Errorf("expected tail to be 'none', got %s", resp.Tail.GetNodeId())
	}
	if len(resp.Chain) != 0 {
		t.Errorf("expected empty chain, got %d nodes", len(resp.Chain))
	}
}

// Test preveri, da ConfigureChain pravilno nastavi verigo in da GetClusterState vrne isto topologijo.
// To simulira “metadata-only” konfiguracijo, brez dejanskega poganjanja strežnikov.
// Preveri pravilno določanje head in tail glede na vrstni red v chain.
func TestGetClusterStateWithNodes(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
		{NodeId: "node-2", Address: "localhost:50052"},
		{NodeId: "node-3", Address: "localhost:50053"},
	}

	_, err := cp.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	if err != nil {
		t.Fatalf("ConfigureChain failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	resp, err := cp.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetClusterState failed: %v", err)
	}

	if resp.Head.GetNodeId() != "node-1" {
		t.Errorf("expected head to be 'node-1', got %s", resp.Head.GetNodeId())
	}
	if resp.Tail.GetNodeId() != "node-3" {
		t.Errorf("expected tail to be 'node-3', got %s", resp.Tail.GetNodeId())
	}
	if len(resp.Chain) != 3 {
		t.Errorf("expected chain length 3, got %d", len(resp.Chain))
	}
}

// Test preveri, da ConfigureChain posodobi chain in nodes mapo, ne da bi node-e označil kot žive.
// Ker ni bilo heartbeat-ov, morajo biti alive=false.
// S tem se potrdi, da “konfiguracija” sama po sebi ne pomeni razpoložljivosti.
func TestConfigureChain(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
		{NodeId: "node-2", Address: "localhost:50052"},
	}

	_, err := cp.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	if err != nil {
		t.Fatalf("ConfigureChain failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.chain) != 2 {
		t.Errorf("expected chain length 2, got %d", len(cp.chain))
	}
	if len(cp.nodes) != 2 {
		t.Errorf("expected nodes map size 2, got %d", len(cp.nodes))
	}
	for id, st := range cp.nodes {
		if st.alive {
			t.Errorf("node %s should be marked dead (no heartbeat yet)", id)
		}
	}
}

// Test preveri, da se ConfigureChain lahko kliče večkrat in da nova konfiguracija zamenja staro.
// Najprej nastavi verigo z enim node-om, nato jo zamenja z dvema.
// Preveri, da je vrstni red v chain enak novemu vhodu.
func TestConfigureChainMultipleTimes(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	nodes1 := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
	}
	_, err := cp.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes1})
	if err != nil {
		t.Fatalf("First ConfigureChain failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	nodes2 := []*pb.NodeInfo{
		{NodeId: "node-2", Address: "localhost:50052"},
		{NodeId: "node-3", Address: "localhost:50053"},
	}
	_, err = cp.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes2})
	if err != nil {
		t.Fatalf("Second ConfigureChain failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.chain) != 2 {
		t.Errorf("expected chain length 2, got %d", len(cp.chain))
	}
	if cp.chain[0].GetNodeId() != "node-2" {
		t.Errorf("expected first node to be 'node-2', got %s", cp.chain[0].GetNodeId())
	}
}

// Test preveri, da Join registrira node, doda node v nodes mapo in ga vključi v chain.
// To simulira delujoč strežnik, ki se prijavi na control plane.
// Preveri tudi, da je JoinResponse skladen z internim stanjem.
func TestJoin(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	req := &pb.JoinRequest{
		NodeId:  "node-1",
		Address: "localhost:50051",
	}

	resp, err := cp.Join(ctx, req)
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.chain) != 1 {
		t.Errorf("expected chain length 1, got %d", len(cp.chain))
	}
	if len(cp.nodes) != 1 {
		t.Errorf("expected nodes map size 1, got %d", len(cp.nodes))
	}

	if resp.Chain[0].GetNodeId() != "node-1" {
		t.Errorf("expected node-1 in chain, got %s", resp.Chain[0].GetNodeId())
	}
}

// Test preveri, da več zaporednih Join klicev sestavi chain v pravilnem vrstnem redu.
// Najprej se pridruži node-1, nato node-2.
// Na koncu mora chain vsebovati oba in ohraniti vrstni red dodajanja.
func TestJoinMultipleNodes(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	_, err := cp.Join(ctx, &pb.JoinRequest{NodeId: "node-1", Address: "localhost:50051"})
	if err != nil {
		t.Fatalf("First Join failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	_, err = cp.Join(ctx, &pb.JoinRequest{NodeId: "node-2", Address: "localhost:50052"})
	if err != nil {
		t.Fatalf("Second Join failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.chain) != 2 {
		t.Errorf("expected chain length 2, got %d", len(cp.chain))
	}
	if cp.chain[0].GetNodeId() != "node-1" {
		t.Errorf("expected first node to be 'node-1', got %s", cp.chain[0].GetNodeId())
	}
	if cp.chain[1].GetNodeId() != "node-2" {
		t.Errorf("expected second node to be 'node-2', got %s", cp.chain[1].GetNodeId())
	}
}

// Test preveri, da Join ne ustvari duplikatov v chain.
// Isti node se prijavi dvakrat z istimi podatki.
// Na koncu mora chain ostati dolžine 1.
func TestJoinDuplicateNode(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	_, err := cp.Join(ctx, &pb.JoinRequest{NodeId: "node-1", Address: "localhost:50051"})
	if err != nil {
		t.Fatalf("First Join failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	_, err = cp.Join(ctx, &pb.JoinRequest{NodeId: "node-1", Address: "localhost:50051"})
	if err != nil {
		t.Fatalf("Second Join failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.chain) != 1 {
		t.Errorf("expected chain length 1 (no duplicates), got %d", len(cp.chain))
	}
}

// Test preveri, da Leave odstrani node iz nodes mape in iz chain.
// Najprej se node pridruži, nato se odstrani.
// Preveri, da je stanje prazno in da tudi response vsebuje prazno verigo.
func TestLeave(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	_, err := cp.Join(ctx, &pb.JoinRequest{NodeId: "node-1", Address: "localhost:50051"})
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	resp, err := cp.Leave(ctx, &pb.LeaveRequest{NodeId: "node-1"})
	if err != nil {
		t.Fatalf("Leave failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.chain) != 0 {
		t.Errorf("expected chain length 0, got %d", len(cp.chain))
	}
	if len(cp.nodes) != 0 {
		t.Errorf("expected nodes map size 0, got %d", len(cp.nodes))
	}
	if len(resp.Chain) != 0 {
		t.Errorf("expected empty chain in response, got %d", len(resp.Chain))
	}
}

// Test preveri, da Leave na neobstoječem node-u ne povzroči napake.
// Klic se mora obnašati idempotentno in ohraniti prazno stanje.
// To je pomembno za robustnost pri ponavljanju ukazov ali nekonsistentnih orchestratorjih.
func TestLeaveNonExistentNode(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	_, err := cp.Leave(ctx, &pb.LeaveRequest{NodeId: "node-999"})
	if err != nil {
		t.Fatalf("Leave failed: %v", err)
	}

	if len(cp.chain) != 0 {
		t.Errorf("expected chain length 0, got %d", len(cp.chain))
	}
}

// Test preveri, da prvi Heartbeat ustvari nodeStatus, nastavi alive=true in auto-join v chain.
// Preveri tudi, da se prenesejo metapodatki lastAppliedSeq in subCount.
// S tem testom pokrijemo scenarij, ko se server nikoli eksplicitno ne Join-a.
func TestHeartbeat(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	req := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 10,
		SubCount:       5,
	}

	resp, err := cp.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	if !resp.Ok {
		t.Error("expected Ok=true")
	}

	time.Sleep(50 * time.Millisecond)

	status, ok := cp.nodes["node-1"]
	if !ok {
		t.Fatal("node-1 not found in nodes map")
	}
	if !status.alive {
		t.Error("expected node to be alive")
	}
	if status.lastApplied != 10 {
		t.Errorf("expected lastApplied=10, got %d", status.lastApplied)
	}
	if status.subCount != 5 {
		t.Errorf("expected subCount=5, got %d", status.subCount)
	}
}

// Test preveri, da Heartbeat na obstoječem node-u posodobi metapodatke.
// Najprej pošlje heartbeat z začetnimi vrednostmi, nato heartbeat z novimi.
// Na koncu mora nodes mapa vsebovati posodobljen lastAppliedSeq in subCount.
func TestHeartbeatUpdate(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	req1 := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 10,
		SubCount:       5,
	}
	_, err := cp.Heartbeat(ctx, req1)
	if err != nil {
		t.Fatalf("First Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	req2 := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 20,
		SubCount:       7,
	}
	_, err = cp.Heartbeat(ctx, req2)
	if err != nil {
		t.Fatalf("Second Heartbeat failed: %v", err)
	}

	status := cp.nodes["node-1"]
	if status.lastApplied != 20 {
		t.Errorf("expected lastApplied=20, got %d", status.lastApplied)
	}
	if status.subCount != 7 {
		t.Errorf("expected subCount=7, got %d", status.subCount)
	}
}

// Test preveri, da GetLeastLoadedNode na praznem clustru vrne "none".
// To je pričakovano, ker ni nobenega alive node-a v chain.
// Test varuje pred nil dereferencami in napačnimi defaulti.
func TestGetLeastLoadedNodeEmpty(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	resp, err := cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}

	if resp.GetNodeId() != "none" {
		t.Errorf("expected 'none', got %s", resp.GetNodeId())
	}
}

// Test preveri, da GetLeastLoadedNode pri enem alive node-u vrne ravno tega node-a.
// Node se registrira prek Heartbeat, zato je alive=true.
// To potrdi osnovno delovanje load-balancing izbire.
func TestGetLeastLoadedNodeSingle(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	req := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 10,
		SubCount:       5,
	}
	_, err := cp.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	resp, err := cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}

	if resp.GetNodeId() != "node-1" {
		t.Errorf("expected 'node-1', got %s", resp.GetNodeId())
	}
}

// Test preveri, da GetLeastLoadedNode izbere alive node z najmanjšim subCount.
// Doda tri node-e prek Heartbeat z različnimi subCount vrednostmi.
// Nato preveri, da je izbran node-2, ker ima najmanjšo obremenitev.
func TestGetLeastLoadedNodeMultiple(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	nodes := []struct {
		id       string
		subCount int64
	}{
		{"node-1", 10},
		{"node-2", 3},
		{"node-3", 7},
	}

	for _, n := range nodes {
		req := &pb.HeartbeatRequest{
			NodeId:         n.id,
			Address:        "localhost:5005" + n.id[len(n.id)-1:],
			LastAppliedSeq: 0,
			SubCount:       n.subCount,
		}
		_, err := cp.Heartbeat(ctx, req)
		if err != nil {
			t.Fatalf("Heartbeat failed for %s: %v", n.id, err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	resp, err := cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}

	if resp.GetNodeId() != "node-2" {
		t.Errorf("expected 'node-2' (lowest subCount), got %s", resp.GetNodeId())
	}
}

// Test preveri, da GetLeastLoadedNode ignorira dead node-e tudi če imajo nižji subCount.
// Ustvari dva node-a in zažene monitorFailuresLoop, nato preneha osveževati node-1.
// Pričakuje, da bo node-1 označen kot dead in da bo izbira padla na node-2.
func TestGetLeastLoadedNodeExcludesDead(t *testing.T) {
	cp := newControlPlane(100 * time.Millisecond)
	ctx := context.Background()

	req1 := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 0,
		SubCount:       1,
	}
	_, err := cp.Heartbeat(ctx, req1)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	req2 := &pb.HeartbeatRequest{
		NodeId:         "node-2",
		Address:        "localhost:50052",
		LastAppliedSeq: 0,
		SubCount:       10,
	}
	_, err = cp.Heartbeat(ctx, req2)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	defer monitorCancel()
	go cp.monitorFailuresLoop(monitorCtx)

	keepAliveCtx, keepAliveCancel := context.WithCancel(context.Background())
	defer keepAliveCancel()
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-keepAliveCtx.Done():
				return
			case <-ticker.C:
				_, _ = cp.Heartbeat(ctx, req2)
			}
		}
	}()

	eventually(t, 2*time.Second, func() bool {
		cp.lock.Lock()
		defer cp.lock.Unlock()
		st := cp.nodes["node-1"]
		return st != nil && !st.alive
	})

	resp, err := cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}

	if resp.GetNodeId() != "node-2" {
		t.Errorf("expected 'node-2' (node-1 is dead), got %s", resp.GetNodeId())
	}
}

// Test preveri, da monitorFailuresLoop zazna mrtve node-e in rebuilda chain brez njih.
// Node se registrira prek Heartbeat, nato se monitor zažene in počaka na timeout.
// Na koncu mora biti node označen dead in chain prazen.
func TestMonitorFailuresDetectsDead(t *testing.T) {
	cp := newControlPlane(200 * time.Millisecond)
	ctx := context.Background()

	req := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 0,
		SubCount:       5,
	}
	_, err := cp.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	defer monitorCancel()
	go cp.monitorFailuresLoop(monitorCtx)

	eventually(t, 2*time.Second, func() bool {
		cp.lock.Lock()
		defer cp.lock.Unlock()
		st := cp.nodes["node-1"]
		return st != nil && !st.alive && len(cp.chain) == 0
	})
}

// Test preveri, da se node po izpadu lahko oživi z novim Heartbeat-om.
// Najprej pusti, da monitor označi node kot dead.
// Nato pošlje heartbeat in pričakuje, da je node spet alive ter da je ponovno v chain.
func TestNodeRevival(t *testing.T) {
	cp := newControlPlane(200 * time.Millisecond)
	ctx := context.Background()

	req := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 0,
		SubCount:       5,
	}
	_, err := cp.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	defer monitorCancel()
	go cp.monitorFailuresLoop(monitorCtx)

	eventually(t, 2*time.Second, func() bool {
		cp.lock.Lock()
		defer cp.lock.Unlock()
		st := cp.nodes["node-1"]
		return st != nil && !st.alive
	})

	_, err = cp.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	eventually(t, 2*time.Second, func() bool {
		cp.lock.Lock()
		defer cp.lock.Unlock()
		st := cp.nodes["node-1"]
		return st != nil && st.alive && len(cp.chain) == 1
	})
}

// Test preveri helper filterChain, ki odstrani node iz verige po nodeID.
// Vhodna chain ima tri node-e, odstranimo srednjega.
// Rezultat mora imeti dolžino 2 in ohranjen vrstni red preostalih node-ov.
func TestFilterChain(t *testing.T) {
	chain := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
		{NodeId: "node-2", Address: "localhost:50052"},
		{NodeId: "node-3", Address: "localhost:50053"},
	}

	filtered := filterChain(chain, "node-2")

	if len(filtered) != 2 {
		t.Errorf("expected length 2, got %d", len(filtered))
	}
	if filtered[0].GetNodeId() != "node-1" {
		t.Errorf("expected node-1, got %s", filtered[0].GetNodeId())
	}
	if filtered[1].GetNodeId() != "node-3" {
		t.Errorf("expected node-3, got %s", filtered[1].GetNodeId())
	}
}

// Test preveri, da filterChain pri neobstoječem nodeID ne spremeni verige.
// Vhod ima en node, filtriramo drugačen ID.
// Rezultat mora ostati dolžine 1 in vsebovati isti node.
func TestFilterChainNonExistent(t *testing.T) {
	chain := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
	}

	filtered := filterChain(chain, "node-999")

	if len(filtered) != 1 {
		t.Errorf("expected length 1, got %d", len(filtered))
	}
}

// Test preveri, da filterChain na prazni verigi vrne prazno verigo.
// S tem se izognemo edge-case napakam pri praznem inputu.
// Pričakujemo dolžino 0.
func TestFilterChainEmpty(t *testing.T) {
	chain := []*pb.NodeInfo{}

	filtered := filterChain(chain, "node-1")

	if len(filtered) != 0 {
		t.Errorf("expected length 0, got %d", len(filtered))
	}
}

// Test preveri helper inChain, ki ugotovi ali nodeID obstaja v chain.
// Ročno nastavi verigo z dvema node-oma in preveri true/false za tri različne ID-je.
// To je osnova za appendToChainIfMissing in preprečevanje duplikatov.
func TestInChain(t *testing.T) {
	cp := newControlPlane(2 * time.Second)

	cp.chain = []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
		{NodeId: "node-2", Address: "localhost:50052"},
	}

	if !cp.inChain("node-1") {
		t.Error("expected node-1 to be in chain")
	}
	if !cp.inChain("node-2") {
		t.Error("expected node-2 to be in chain")
	}
	if cp.inChain("node-3") {
		t.Error("expected node-3 not to be in chain")
	}
}

// Test preveri appendToChainIfMissing, da doda nov node in ignorira duplikat.
// Najprej doda info1 in pričakuje changed=true, nato ponovno doda info1 in pričakuje changed=false.
// Nato doda info2 in potrdi, da se chain podaljša na 2.
func TestAppendToChainIfMissing(t *testing.T) {
	cp := newControlPlane(2 * time.Second)

	info1 := &pb.NodeInfo{NodeId: "node-1", Address: "localhost:50051"}
	info2 := &pb.NodeInfo{NodeId: "node-2", Address: "localhost:50052"}

	changed := cp.appendToChainIfMissing(info1)
	if !changed {
		t.Error("expected changed=true when adding new node")
	}
	if len(cp.chain) != 1 {
		t.Errorf("expected chain length 1, got %d", len(cp.chain))
	}

	changed = cp.appendToChainIfMissing(info1)
	if changed {
		t.Error("expected changed=false when adding duplicate node")
	}
	if len(cp.chain) != 1 {
		t.Errorf("expected chain length 1 (no duplicate), got %d", len(cp.chain))
	}

	changed = cp.appendToChainIfMissing(info2)
	if !changed {
		t.Error("expected changed=true when adding new node")
	}
	if len(cp.chain) != 2 {
		t.Errorf("expected chain length 2, got %d", len(cp.chain))
	}
}

// Test preveri varnost pred race conditions pri sočasnih Heartbeat klicih.
// Zažene več gorutin, ki vse pošiljajo heartbeat za isti node z različnimi vrednostmi.
// Na koncu mora obstajati natanko en node v nodes mapi in v chain ne sme biti več kot en vnos.
func TestConcurrentHeartbeats(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	done := make(chan bool)
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			req := &pb.HeartbeatRequest{
				NodeId:         "node-1",
				Address:        "localhost:50051",
				LastAppliedSeq: int64(id),
				SubCount:       int64(id),
			}
			_, err := cp.Heartbeat(ctx, req)
			if err != nil {
				t.Errorf("Heartbeat failed: %v", err)
			}
			done <- true
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	if len(cp.nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(cp.nodes))
	}
	if len(cp.chain) > 1 {
		t.Errorf("expected at most 1 node in chain, got %d", len(cp.chain))
	}
}

// Test preveri varnost pred race conditions pri sočasnih Join klicih za več različnih node-ov.
// Zažene več gorutin, ki se hkrati prijavljajo z različnimi ID-ji.
// Na koncu mora nodes mapa vsebovati pričakovano število node-ov.
func TestConcurrentJoins(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	done := make(chan bool)
	numNodes := 5

	for i := 1; i <= numNodes; i++ {
		go func(id int) {
			req := &pb.JoinRequest{
				NodeId:  "node-" + string(rune('0'+id)),
				Address: "localhost:5005" + string(rune('0'+id)),
			}
			_, err := cp.Join(ctx, req)
			if err != nil {
				t.Errorf("Join failed: %v", err)
			}
			done <- true
		}(i)
	}

	for i := 0; i < numNodes; i++ {
		<-done
	}

	time.Sleep(100 * time.Millisecond)

	if len(cp.nodes) != numNodes {
		t.Errorf("expected %d nodes, got %d", numNodes, len(cp.nodes))
	}
}

// Test preveri getHeadTailLocked pri verigi z enim node-om.
// Head in tail morata biti isti node.
// S tem pokrijemo edge-case, kjer ima chain dolžino 1.
func TestGetHeadTailLockedSingle(t *testing.T) {
	cp := newControlPlane(2 * time.Second)

	cp.chain = []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
	}

	head, tail := cp.getHeadTailLocked()

	if head.GetNodeId() != "node-1" {
		t.Errorf("expected head 'node-1', got %s", head.GetNodeId())
	}
	if tail.GetNodeId() != "node-1" {
		t.Errorf("expected tail 'node-1', got %s", tail.GetNodeId())
	}
}

// Test preveri getHeadTailLocked pri verigi z več node-i.
// Head mora biti prvi in tail zadnji element.
// To potrdi, da se head/tail vedno računa iz trenutnega vrstnega reda.
func TestGetHeadTailLockedMultiple(t *testing.T) {
	cp := newControlPlane(2 * time.Second)

	cp.chain = []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
		{NodeId: "node-2", Address: "localhost:50052"},
		{NodeId: "node-3", Address: "localhost:50053"},
	}

	head, tail := cp.getHeadTailLocked()

	if head.GetNodeId() != "node-1" {
		t.Errorf("expected head 'node-1', got %s", head.GetNodeId())
	}
	if tail.GetNodeId() != "node-3" {
		t.Errorf("expected tail 'node-3', got %s", tail.GetNodeId())
	}
}

// Test preveri auto-join vedenje, ko node pošlje Heartbeat brez predhodnega Join.
// Heartbeat mora ustvariti nodeStatus, označiti alive=true in dodati node v chain.
// To je pomembno za scenarije, kjer se node ponovno zažene in takoj začne pošiljati heartbeat-e.
func TestAutoJoinViaHeartbeat(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	req := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 0,
		SubCount:       0,
	}

	_, err := cp.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.nodes) != 1 {
		t.Errorf("expected 1 node (auto-joined), got %d", len(cp.nodes))
	}
	if len(cp.chain) != 1 {
		t.Errorf("expected 1 node in chain (auto-joined), got %d", len(cp.chain))
	}
	if !cp.nodes["node-1"].alive {
		t.Error("expected node to be alive")
	}
}

// Test preveri, da Leave posodobi head in tail v response-u, ko odstranimo trenutni head.
// Najprej naredi chain s tremi node-i, nato odstrani node-1.
// Pričakuje, da postane node-2 novi head in node-3 ostane tail.
func TestLeaveUpdatesHeadTail(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	nodes := []string{"node-1", "node-2", "node-3"}
	for i, nodeID := range nodes {
		req := &pb.JoinRequest{
			NodeId:  nodeID,
			Address: "localhost:5005" + string(rune('1'+i)),
		}
		_, err := cp.Join(ctx, req)
		if err != nil {
			t.Fatalf("Join failed for %s: %v", nodeID, err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	resp, err := cp.Leave(ctx, &pb.LeaveRequest{NodeId: "node-1"})
	if err != nil {
		t.Fatalf("Leave failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if resp.Head.GetNodeId() != "node-2" {
		t.Errorf("expected new head 'node-2', got %s", resp.Head.GetNodeId())
	}
	if resp.Tail.GetNodeId() != "node-3" {
		t.Errorf("expected tail 'node-3', got %s", resp.Tail.GetNodeId())
	}
}

// Test preveri, da ConfigureChain ne naredi node-ov razpoložljivih za load balancing brez Heartbeat-a.
// Po ConfigureChain so node-i v chain, vendar morajo ostati alive=false.
// Zato mora GetLeastLoadedNode vrniti "none".
func TestConfigureChainDoesNotMakeServersAvailable(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
		{NodeId: "node-2", Address: "localhost:50052"},
	}

	_, err := cp.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	if err != nil {
		t.Fatalf("ConfigureChain failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	for _, node := range cp.nodes {
		if node.alive {
			t.Error("nodes configured via ConfigureChain should not be alive without heartbeat")
		}
	}

	resp, err := cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}
	if resp.GetNodeId() != "none" {
		t.Errorf("expected 'none' (no alive servers), got %s", resp.GetNodeId())
	}
}

// Test preveri, da ConfigureChain z praznim seznamom node-ov pobriše obstoječo verigo.
// Najprej doda en node prek Join, nato pokliče ConfigureChain z empty list.
// Na koncu mora biti chain prazen.
func TestConfigureChainEmpty(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	_, err := cp.Join(ctx, &pb.JoinRequest{NodeId: "node-1", Address: "localhost:50051"})
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	_, err = cp.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: []*pb.NodeInfo{}})
	if err != nil {
		t.Fatalf("ConfigureChain failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.chain) != 0 {
		t.Errorf("expected empty chain, got %d nodes", len(cp.chain))
	}
}

// Test preveri, da server postane “na voljo” šele po Heartbeat-u, tudi če je v chain prek ConfigureChain.
// Najprej konfigurira en node brez heartbeat-a in pričakuje, da GetLeastLoadedNode vrne "none".
// Nato pošlje heartbeat in pričakuje, da se isti node vrne kot least loaded.
func TestServersAvailableOnlyAfterHeartbeat(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
	}
	_, err := cp.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	if err != nil {
		t.Fatalf("ConfigureChain failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	resp, err := cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}
	if resp.GetNodeId() != "none" {
		t.Errorf("expected 'none' before heartbeat, got %s", resp.GetNodeId())
	}

	heartbeat := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 0,
		SubCount:       5,
	}
	_, err = cp.Heartbeat(ctx, heartbeat)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	resp, err = cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}
	if resp.GetNodeId() != "node-1" {
		t.Errorf("expected 'node-1' after heartbeat, got %s", resp.GetNodeId())
	}
}

// Test preveri, da GetLeastLoadedNode vrne "none", ko so vsi node-i dead.
// Node se najprej registrira prek heartbeat-a, nato monitorFailuresLoop označi node kot dead po timeoutu.
// Po tem ne sme biti nobenega kandidata za load balancing.
func TestGetLeastLoadedNodeAllDead(t *testing.T) {
	cp := newControlPlane(100 * time.Millisecond)
	ctx := context.Background()

	req := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 0,
		SubCount:       1,
	}
	_, err := cp.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	defer monitorCancel()
	go cp.monitorFailuresLoop(monitorCtx)

	eventually(t, 2*time.Second, func() bool {
		cp.lock.Lock()
		defer cp.lock.Unlock()
		st := cp.nodes["node-1"]
		return st != nil && !st.alive
	})

	resp, err := cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}

	if resp.GetNodeId() != "none" {
		t.Errorf("expected 'none' when all nodes are dead, got %s", resp.GetNodeId())
	}
}

// Test preveri, da monitorFailuresLoop rebuilda chain izključno iz alive node-ov in ohrani njihov vrstni red.
// Najprej auto-join-a tri node-e prek heartbeat-a, nato ohranja alive samo node-1 in node-3.
// Ko node-2 postane dead, mora chain vsebovati [node-1, node-3] v tem vrstnem redu.
func TestMonitorFailures_RebuildsChainPreservingOrderOfAliveNodes(t *testing.T) {
	cp := newControlPlane(120 * time.Millisecond)
	ctx := context.Background()

	req1 := &pb.HeartbeatRequest{NodeId: "node-1", Address: "localhost:50051", LastAppliedSeq: 0, SubCount: 1}
	req2 := &pb.HeartbeatRequest{NodeId: "node-2", Address: "localhost:50052", LastAppliedSeq: 0, SubCount: 1}
	req3 := &pb.HeartbeatRequest{NodeId: "node-3", Address: "localhost:50053", LastAppliedSeq: 0, SubCount: 1}

	_, _ = cp.Heartbeat(ctx, req1)
	_, _ = cp.Heartbeat(ctx, req2)
	_, _ = cp.Heartbeat(ctx, req3)

	time.Sleep(30 * time.Millisecond)

	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	defer monitorCancel()
	go cp.monitorFailuresLoop(monitorCtx)

	keepAliveCtx, keepAliveCancel := context.WithCancel(context.Background())
	defer keepAliveCancel()

	go func() {
		ticker := time.NewTicker(40 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-keepAliveCtx.Done():
				return
			case <-ticker.C:
				_, _ = cp.Heartbeat(ctx, req1)
				_, _ = cp.Heartbeat(ctx, req3)
			}
		}
	}()

	eventually(t, 2*time.Second, func() bool {
		cp.lock.Lock()
		defer cp.lock.Unlock()

		st2 := cp.nodes["node-2"]
		if st2 == nil || st2.alive {
			return false
		}
		if len(cp.chain) != 2 {
			return false
		}
		return cp.chain[0].GetNodeId() == "node-1" && cp.chain[1].GetNodeId() == "node-3"
	})
}

// Test preveri, da oživetje prek Heartbeat ne ustvari duplikatov v chain.
// Najprej pusti, da node-1 postane dead in da ga monitor odstrani iz chain.
// Nato pošlje heartbeat, ki mora node ponovno dodati, vendar chain mora ostati dolžine 1.
func TestHeartbeatRevival_DoesNotDuplicateChainEntries(t *testing.T) {
	cp := newControlPlane(120 * time.Millisecond)
	ctx := context.Background()

	req := &pb.HeartbeatRequest{NodeId: "node-1", Address: "localhost:50051", LastAppliedSeq: 0, SubCount: 1}
	_, _ = cp.Heartbeat(ctx, req)
	time.Sleep(20 * time.Millisecond)

	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	defer monitorCancel()
	go cp.monitorFailuresLoop(monitorCtx)

	eventually(t, 2*time.Second, func() bool {
		cp.lock.Lock()
		defer cp.lock.Unlock()
		st := cp.nodes["node-1"]
		return st != nil && !st.alive
	})

	_, _ = cp.Heartbeat(ctx, req)
	time.Sleep(50 * time.Millisecond)

	cp.lock.Lock()
	defer cp.lock.Unlock()

	if len(cp.chain) != 1 {
		t.Fatalf("expected chain length 1 after revival, got %d", len(cp.chain))
	}
	if cp.chain[0].GetNodeId() != "node-1" {
		t.Fatalf("expected node-1 in chain, got %s", cp.chain[0].GetNodeId())
	}
}
