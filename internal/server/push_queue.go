package server

import "log/slog"

// maxPushQueueLen bounds the per-device transient push queue to keep memory in
// check if a device is offline and never drains it. Oldest items are dropped.
const maxPushQueueLen = 32

// QueueChanged is broadcast to a device's websocket loop to signal that its
// transient push queue changed and should be (re)checked. It carries no payload;
// the queue itself is the source of truth.
type QueueChanged struct{}

// pushItem is a single transient (one-shot) pushed image waiting to be shown.
type pushItem struct {
	Data        []byte
	DisplaySecs int    // 0 => use the device default dwell
	CoalesceID  string // optional; repeated non-interrupt pushes with the same id keep only the latest
}

// deviceQueue holds the pending transient pushes for one device. interrupt is
// set when a foreground, non-queued ("supersede") push has been enqueued and the
// websocket write loop should cut the current display short to show it now.
type deviceQueue struct {
	items     []pushItem
	interrupt bool
}

// enqueuePush adds a transient image to a device's queue.
//
// When interrupt is true (a queue:false foreground push), the pending queue is
// flushed and replaced with just this item ("supersede"), and the interrupt flag
// is raised so the WS loop preempts whatever is currently displayed. When false
// (a queue:true push, or a background push), the item is appended in FIFO order.
//
// coalesceID, when non-empty on a non-interrupt push, drops any pending item that
// shares the same id before appending, so only the latest copy of that id remains
// (mirrors upstream's per-key coalesce). It is ignored for interrupt pushes, which
// already flush the whole queue.
func (s *Server) enqueuePush(deviceID string, data []byte, displaySecs int, interrupt bool, coalesceID string) {
	item := pushItem{Data: data, DisplaySecs: displaySecs, CoalesceID: coalesceID}

	s.pushQueueMutex.Lock()
	defer s.pushQueueMutex.Unlock()

	q := s.pushQueues[deviceID]
	if q == nil {
		q = &deviceQueue{}
		s.pushQueues[deviceID] = q
	}

	if interrupt {
		q.items = []pushItem{item}
		q.interrupt = true
		return
	}

	// Coalesce: drop any pending item with the same id so only the latest remains.
	if coalesceID != "" {
		kept := q.items[:0]
		for _, it := range q.items {
			if it.CoalesceID != coalesceID {
				kept = append(kept, it)
			}
		}
		q.items = kept
	}

	q.items = append(q.items, item)
	if len(q.items) > maxPushQueueLen {
		drop := len(q.items) - maxPushQueueLen
		slog.Warn("Push queue over capacity, dropping oldest images", "device", deviceID, "dropped", drop)
		q.items = q.items[drop:]
	}
}

// dequeuePush pops the next transient image for a device, if any. The boolean is
// false when the queue is empty.
func (s *Server) dequeuePush(deviceID string) ([]byte, int, bool) {
	s.pushQueueMutex.Lock()
	defer s.pushQueueMutex.Unlock()

	q := s.pushQueues[deviceID]
	if q == nil || len(q.items) == 0 {
		return nil, 0, false
	}

	item := q.items[0]
	q.items = q.items[1:]
	// The interrupt flag means "preempt the current display for this superseding
	// head item"; popping it consumes the flag so a later queue:true append (now at
	// the head) does not inherit it and cut short the image currently playing.
	q.interrupt = false
	if len(q.items) == 0 {
		delete(s.pushQueues, deviceID)
	}
	return item.Data, item.DisplaySecs, true
}

// pushQueueState reports, for a device, whether the WS loop should preempt the
// current display: interrupt is true after a supersede push, and pending is true
// when any transient image is waiting. The interrupt flag is cleared if takeInterrupt
// is set (the caller is acting on it now).
func (s *Server) pushQueueState(deviceID string, takeInterrupt bool) (interrupt, pending bool) {
	s.pushQueueMutex.Lock()
	defer s.pushQueueMutex.Unlock()

	q := s.pushQueues[deviceID]
	if q == nil {
		return false, false
	}
	interrupt = q.interrupt
	pending = len(q.items) > 0
	if takeInterrupt && interrupt {
		q.interrupt = false
	}
	return interrupt, pending
}
