package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newQueueTestServer() *Server {
	return &Server{pushQueues: make(map[string]*deviceQueue)}
}

// drainDwells pops the whole queue and returns each item's dwell, in order.
func drainDwells(s *Server, dev string) []int {
	var got []int
	for {
		_, dwell, ok := s.dequeuePush(dev)
		if !ok {
			return got
		}
		got = append(got, dwell)
	}
}

func TestPushQueue_FIFOOrder(t *testing.T) {
	s := newQueueTestServer()
	dev := "d1"
	s.enqueuePush(dev, []byte("a"), 5, false, "")
	s.enqueuePush(dev, []byte("b"), 9, false, "")
	assert.Equal(t, []int{5, 9}, drainDwells(s, dev))
}

func TestPushQueue_SupersedeFlush(t *testing.T) {
	s := newQueueTestServer()
	dev := "d1"
	s.enqueuePush(dev, []byte("a"), 5, false, "") // queued
	s.enqueuePush(dev, []byte("b"), 9, false, "") // queued
	s.enqueuePush(dev, []byte("c"), 3, true, "")  // supersede: flush + interrupt

	interrupt, pending := s.pushQueueState(dev, false)
	assert.True(t, interrupt, "supersede push should set interrupt")
	assert.True(t, pending)

	// Only the superseding item remains.
	data, dwell, ok := s.dequeuePush(dev)
	require.True(t, ok)
	assert.Equal(t, []byte("c"), data)
	assert.Equal(t, 3, dwell)
	_, _, ok = s.dequeuePush(dev)
	assert.False(t, ok, "queue should hold only the single superseding item")
}

func TestPushQueue_Coalesce(t *testing.T) {
	s := newQueueTestServer()
	dev := "d1"
	s.enqueuePush(dev, []byte("old"), 5, false, "board")
	s.enqueuePush(dev, []byte("mid"), 6, false, "other")
	s.enqueuePush(dev, []byte("new"), 7, false, "board") // coalesces with the first

	// "board" collapses to the latest; "other" is untouched.
	assert.Equal(t, []int{6, 7}, drainDwells(s, dev))
}

func TestPushQueue_InterruptClearedOnPop(t *testing.T) {
	s := newQueueTestServer()
	dev := "d1"
	s.enqueuePush(dev, []byte("drop"), 5, true, "")    // supersede -> interrupt set
	s.enqueuePush(dev, []byte("board"), 15, false, "") // queue:true append after

	// Popping the superseding item must clear the interrupt so the appended
	// board (now at the head) is not cut short.
	_, _, ok := s.dequeuePush(dev)
	require.True(t, ok)
	interrupt, pending := s.pushQueueState(dev, false)
	assert.False(t, interrupt, "interrupt must clear once the superseding item is popped")
	assert.True(t, pending, "the appended queued item should still be pending")
}

func TestPushQueue_StateTakeInterrupt(t *testing.T) {
	s := newQueueTestServer()
	dev := "d1"
	s.enqueuePush(dev, []byte("x"), 5, true, "")
	interrupt, _ := s.pushQueueState(dev, true) // consume it
	assert.True(t, interrupt)
	interrupt2, _ := s.pushQueueState(dev, false)
	assert.False(t, interrupt2, "interrupt should be consumed by takeInterrupt")
}

func TestPushQueue_CapDropsOldest(t *testing.T) {
	s := newQueueTestServer()
	dev := "d1"
	for i := 0; i < maxPushQueueLen+5; i++ {
		s.enqueuePush(dev, []byte{byte(i)}, i, false, "")
	}
	got := drainDwells(s, dev)
	require.Len(t, got, maxPushQueueLen)
	assert.Equal(t, 5, got[0], "the oldest 5 items should have been dropped")
}

func TestPushQueue_EmptyDeletesMapEntry(t *testing.T) {
	s := newQueueTestServer()
	dev := "d1"
	s.enqueuePush(dev, []byte("x"), 5, false, "")
	_, _, ok := s.dequeuePush(dev)
	require.True(t, ok)
	s.pushQueueMutex.Lock()
	_, exists := s.pushQueues[dev]
	s.pushQueueMutex.Unlock()
	assert.False(t, exists, "drained device queue should be removed from the map")
}

func TestDeliverPush_QueueThenSupersede(t *testing.T) {
	s := newTestServerAPI(t)
	dev := "testdevice"
	ctx := context.Background()

	// queue:true then queue:false -> the supersede flushes to just the latest.
	require.NoError(t, s.deliverPush(ctx, dev, "", []byte("q1"), false, true, 5, ""))
	require.NoError(t, s.deliverPush(ctx, dev, "", []byte("q2"), false, false, 9, ""))

	data, dwell, ok := s.dequeuePush(dev)
	require.True(t, ok)
	assert.Equal(t, []byte("q2"), data)
	assert.Equal(t, 9, dwell)
	_, _, ok = s.dequeuePush(dev)
	assert.False(t, ok)
}

func TestDeliverPush_BackgroundNoInterrupt(t *testing.T) {
	s := newTestServerAPI(t)
	dev := "testdevice"
	ctx := context.Background()

	require.NoError(t, s.deliverPush(ctx, dev, "", []byte("bg"), true, false, 5, ""))
	interrupt, pending := s.pushQueueState(dev, false)
	assert.False(t, interrupt, "a background push must not interrupt")
	assert.True(t, pending, "a background push with no installID is still queued for the next cycle")
}
