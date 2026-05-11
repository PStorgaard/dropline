package stream

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Hub is the single-drainer demultiplexer that powers multi-session
// `dropline serve`. One Hub owns one *net.UDPConn (caller-supplied,
// caller-closed); its Run goroutine is the only ReadFromUDP caller for
// that socket. Decoded packets are dispatched to per-flow channels
// registered via Register.
//
// The Hub does not manage flow-id lifecycle policy: registering an
// already-registered flow_id panics, since collision implies a session-id
// reuse the server can't safely recover from. Spec: flow_ids are
// crypto/rand-minted client-side, so 32-bit collisions in the
// max-sessions=4 scope are effectively impossible.
type Hub struct {
	conn *net.UDPConn

	mu    sync.RWMutex
	flows map[uint32]*flowQueue
}

// flowQueue is the per-session ring + drop counter living in the hub.
// Receiver pulls observations off ch and reads localDrops once per tick.
type flowQueue struct {
	ch         chan observation
	localDrops atomic.Int64
}

// HubFlow is the per-session handle returned by Hub.Register. Receiver
// uses it to consume observations and bookkeep ring-overflow drops.
type HubFlow struct {
	hub     *Hub
	flowID  uint32
	queue   *flowQueue
	release sync.Once
}

// NewHub constructs a Hub bound to conn. Run must be called for
// observations to flow.
func NewHub(conn *net.UDPConn) *Hub {
	if conn == nil {
		return nil
	}
	return &Hub{
		conn:  conn,
		flows: make(map[uint32]*flowQueue),
	}
}

// Run is the single drainer. It blocks until ctx is canceled or the
// underlying socket returns net.ErrClosed. On exit it closes every
// registered flow's channel so consumers unblock cleanly.
func (h *Hub) Run(ctx context.Context) error {
	defer h.closeAll()

	buf := make([]byte, MaxPacketSize)
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := h.conn.SetReadDeadline(time.Now().Add(drainerShutdownPoll)); err != nil {
			return err
		}
		n, _, err := h.conn.ReadFromUDP(buf)
		if err != nil {
			if isTimeout(err) {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if n < HeaderSize {
			continue
		}
		hdr, derr := DecodeHeader(buf[:HeaderSize])
		if derr != nil {
			continue
		}

		// Hold the read-lock through the non-blocking send so a
		// concurrent Release() can't close fq.ch between lookup and
		// send. The select has a default arm, so RLock duration is
		// bounded by one channel-op + one atomic.
		h.mu.RLock()
		if fq, ok := h.flows[hdr.FlowID]; ok {
			select {
			case fq.ch <- observation{h: hdr, arrival: time.Now()}:
			default:
				fq.localDrops.Add(1)
			}
		}
		h.mu.RUnlock()
	}
}

// Register reserves flow_id on the hub and returns a HubFlow handle. The
// caller MUST Release the handle when the session ends; releasing closes
// the per-flow channel and de-registers from the hub. ringSize=0 falls
// back to DefaultRingSize.
//
// Panics if flow_id is already registered — the server-side serialization
// (the control accept path) is responsible for never letting that happen.
// Callers that handle untrusted flow_ids (e.g. straight from a client
// Hello) should use TryRegister instead.
func (h *Hub) Register(flowID uint32, ringSize int) *HubFlow {
	hf, ok := h.TryRegister(flowID, ringSize)
	if !ok {
		panic("stream: hub flow_id already registered")
	}
	return hf
}

// TryRegister is the non-panicking counterpart of Register: it returns
// (nil, false) when flow_id is already registered, leaving the existing
// registration untouched. Used by the server's control path, where the
// flow_id arrives from an untrusted Hello and a collision must surface
// as a clean Error frame rather than a goroutine panic.
func (h *Hub) TryRegister(flowID uint32, ringSize int) (*HubFlow, bool) {
	if ringSize <= 0 {
		ringSize = DefaultRingSize
	}
	fq := &flowQueue{ch: make(chan observation, ringSize)}

	h.mu.Lock()
	if _, exists := h.flows[flowID]; exists {
		h.mu.Unlock()
		return nil, false
	}
	h.flows[flowID] = fq
	h.mu.Unlock()

	return &HubFlow{hub: h, flowID: flowID, queue: fq}, true
}

// closeAll closes every registered flow's channel and clears the
// registry. Used on Hub shutdown so receivers unblock.
func (h *Hub) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, fq := range h.flows {
		close(fq.ch)
		delete(h.flows, id)
	}
}

// Observations returns the read-only side of the per-flow ring buffer.
// The channel closes when Release is called or when the hub shuts down.
func (f *HubFlow) Observations() <-chan observation { return f.queue.ch }

// PullLocalDrops atomically swaps the per-flow ring-overflow counter to
// zero and returns the previous value. The receiver calls this once per
// snapshot tick and forwards the count to Accumulator.AddLocalDrop.
func (f *HubFlow) PullLocalDrops() int64 { return f.queue.localDrops.Swap(0) }

// Release de-registers the flow from the hub and closes its channel.
// Idempotent.
func (f *HubFlow) Release() {
	f.release.Do(func() {
		f.hub.mu.Lock()
		if cur, ok := f.hub.flows[f.flowID]; ok && cur == f.queue {
			delete(f.hub.flows, f.flowID)
			close(f.queue.ch)
		}
		f.hub.mu.Unlock()
	})
}
