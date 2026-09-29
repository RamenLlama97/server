package server

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tronbyt-server/internal/data"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecideQueueChange(t *testing.T) {
	tests := []struct {
		name                                         string
		interrupt, pending, sentQueued, screenQueued bool
		want                                         queueChangeAction
	}{
		{"queue:false push always preempts", true, true, true, true, queueChangePreempt},
		{"queue:false push preempts rotation", true, true, false, false, queueChangePreempt},
		{"nothing pending", false, false, false, false, queueChangeIgnore},
		{"sent image is a push: drains after its ACK", false, true, true, false, queueChangeIgnore},
		{"sent and on-screen images are pushes", false, true, true, true, queueChangeIgnore},
		{"push on screen, rotation buffered: replace buffered", false, true, false, true, queueChangeReplace},
		{"rotation on screen: preempt", false, true, false, false, queueChangePreempt},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, decideQueueChange(tt.interrupt, tt.pending, tt.sentQueued, tt.screenQueued))
		})
	}
}

func TestAckTracker(t *testing.T) {
	displaying := func(n int) WSMessage { return WSMessage{Displaying: &n} }
	queued := func(n int) WSMessage { return WSMessage{Queued: &n} }

	t.Run("accepts any ACK before a counter is known", func(t *testing.T) {
		acks := newAckTracker()
		acks.imageSent()
		assert.True(t, acks.observe(displaying(7)))
	})

	t.Run("queued notices are never ACKs", func(t *testing.T) {
		acks := newAckTracker()
		acks.imageSent()
		assert.False(t, acks.observe(queued(1)))
	})

	t.Run("ignores a late ACK for an older image", func(t *testing.T) {
		acks := newAckTracker()
		acks.imageSent()
		assert.False(t, acks.observe(queued(1)))
		acks.imageSent() // interrupt: a new image is sent before image 1 was ACKed
		assert.False(t, acks.observe(displaying(1)), "image 1 starting is not the new image on screen")
		assert.False(t, acks.observe(queued(2)))
		assert.True(t, acks.observe(displaying(2)))
	})

	t.Run("accepts the alternative counter format", func(t *testing.T) {
		acks := newAckTracker()
		acks.imageSent()
		assert.False(t, acks.observe(queued(3)))
		acks.imageSent()
		n := 4
		assert.True(t, acks.observe(WSMessage{Counter: &n}))
	})
}

// fakeFirmware plays the device side of the WebSocket protocol for tests. Like the
// real firmware it numbers every received image with an increasing counter
// ({"queued":N}), and the test decides when an image starts ({"displaying":N}).
type fakeFirmware struct {
	conn    *websocket.Conn
	msgs    chan wsTestMsg
	held    *wsTestMsg
	counter int
}

type wsTestMsg struct {
	binary bool
	data   []byte
}

// sentImage is one image the server sent, with the firmware counter it was given.
type sentImage struct {
	payload   []byte
	immediate bool
	counter   int
}

func newFakeFirmware(conn *websocket.Conn) *fakeFirmware {
	fw := &fakeFirmware{conn: conn, msgs: make(chan wsTestMsg, 64)}
	go func() {
		defer close(fw.msgs)
		for {
			msgType, msgData, err := conn.ReadMessage()
			if err != nil {
				return
			}
			fw.msgs <- wsTestMsg{binary: msgType == websocket.BinaryMessage, data: msgData}
		}
	}()
	return fw
}

func (fw *fakeFirmware) next(timeout time.Duration) (wsTestMsg, bool) {
	if fw.held != nil {
		m := *fw.held
		fw.held = nil
		return m, true
	}
	select {
	case m, ok := <-fw.msgs:
		return m, ok
	case <-time.After(timeout):
		return wsTestMsg{}, false
	}
}

// expectImage waits for the next image, reports whether "immediate" followed it,
// and acknowledges receipt with {"queued":N} like the firmware's gfx_update.
func (fw *fakeFirmware) expectImage(t *testing.T) sentImage {
	t.Helper()
	for {
		m, ok := fw.next(3 * time.Second)
		require.True(t, ok, "expected an image from the server")
		if !m.binary {
			continue // dwell_secs / brightness metadata
		}
		img := sentImage{payload: m.data}
		// "immediate" is written right after the binary message.
		if follow, ok := fw.next(200 * time.Millisecond); ok {
			if !follow.binary && strings.Contains(string(follow.data), `"immediate"`) {
				img.immediate = true
			} else {
				fw.held = &follow
			}
		}
		fw.counter++
		img.counter = fw.counter
		require.NoError(t, fw.conn.WriteJSON(map[string]int{"queued": img.counter}))
		return img
	}
}

// expectNoImage asserts the server sends no image within d.
func (fw *fakeFirmware) expectNoImage(t *testing.T, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return
		}
		m, ok := fw.next(remaining)
		if !ok {
			return
		}
		require.False(t, m.binary, "server sent an image early: %q", m.data)
	}
}

// displaying reports that the image with this counter is now on screen.
func (fw *fakeFirmware) displaying(t *testing.T, counter int) {
	t.Helper()
	require.NoError(t, fw.conn.WriteJSON(map[string]int{"displaying": counter}))
}

// startQueueTestDevice creates a v1-protocol device with one rotation app and
// connects a fake firmware to it.
func startQueueTestDevice(t *testing.T) (*Server, string, *fakeFirmware) {
	t.Helper()
	s := newTestServerAPI(t)

	deviceID := "wsqueue1"
	protocolVersion := 1
	device := data.Device{
		ID:              deviceID,
		Username:        "testuser",
		Name:            "WS Queue Device",
		APIKey:          "ws_queue_key",
		Brightness:      50,
		DefaultInterval: 10,
		Info:            data.DeviceInfo{ProtocolVersion: &protocolVersion},
	}
	require.NoError(t, s.DB.Create(&device).Error)

	path := "pushed:1"
	app := data.App{
		DeviceID: deviceID, Iname: "1", Name: "Rotation App", Pushed: true, Enabled: true,
		Order: 0, DisplayTime: 15, Path: &path,
	}
	require.NoError(t, s.DB.Create(&app).Error)
	webpDir := filepath.Join(s.DataDir, "webp", deviceID, "pushed")
	require.NoError(t, os.MkdirAll(webpDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(webpDir, "1.webp"), []byte("rotation"), 0644))

	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/"+deviceID+"/ws", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// Like real firmware, report client info (incl. protocol_version) right after
	// connecting. handleWS resets the stored device info to just protocol_type on
	// connect, and this restores it before the write loop next reloads the device.
	require.NoError(t, conn.WriteJSON(WSMessage{ClientInfo: &ClientInfo{
		FirmwareVersion: "test", ProtocolVersion: &protocolVersion,
	}}))
	require.Eventually(t, func() bool {
		var d data.Device
		return s.DB.First(&d, "id = ?", deviceID).Error == nil && d.Info.ProtocolVersion != nil
	}, 2*time.Second, 20*time.Millisecond, "device info was not restored")

	return s, deviceID, newFakeFirmware(conn)
}

// settle gives the server time to read the device's pending "queued" notices, as
// happens in practice long before a buffered image starts.
func settle() {
	time.Sleep(100 * time.Millisecond)
}

// rotationOnScreenWithNextBuffered brings the device to steady rotation: one
// rotation image on screen and the next one buffered behind it.
func rotationOnScreenWithNextBuffered(t *testing.T, fw *fakeFirmware) {
	t.Helper()
	first := fw.expectImage(t)
	require.Equal(t, "rotation", string(first.payload))
	fw.displaying(t, first.counter)
	buffered := fw.expectImage(t)
	require.Equal(t, "rotation", string(buffered.payload))
	require.False(t, buffered.immediate)
}

func push(t *testing.T, s *Server, deviceID, payload string, queue bool) {
	t.Helper()
	require.NoError(t, s.deliverPush(context.Background(), deviceID, "", []byte(payload), false, queue, 1, ""))
}

// Regression: a queue:true push arriving after the show-now push before it is on
// screen (and a rotation image has been buffered behind it) must not cut it off.
func TestWSQueuedPushDoesNotCutOffPushOnScreen(t *testing.T) {
	s, deviceID, fw := startQueueTestDevice(t)
	rotationOnScreenWithNextBuffered(t, fw)

	push(t, s, deviceID, "drop", false)
	drop := fw.expectImage(t)
	require.Equal(t, "drop", string(drop.payload))
	assert.True(t, drop.immediate, "a queue:false push interrupts rotation")
	fw.displaying(t, drop.counter)

	// The queue is empty, so the server buffers the next rotation image behind the drop.
	buffered := fw.expectImage(t)
	require.Equal(t, "rotation", string(buffered.payload))
	assert.False(t, buffered.immediate)

	// The board arrives while the drop is still playing (e.g. ~1.5s render on a Pi).
	push(t, s, deviceID, "board", true)
	board := fw.expectImage(t)
	require.Equal(t, "board", string(board.payload))
	assert.False(t, board.immediate, "the board must replace the buffered rotation image, not interrupt the drop")
}

// A queue:true push that lands before the show-now push is ACKed drains after it.
func TestWSQueuedPushBeforeAckFollowsPush(t *testing.T) {
	s, deviceID, fw := startQueueTestDevice(t)
	rotationOnScreenWithNextBuffered(t, fw)

	push(t, s, deviceID, "drop", false)
	drop := fw.expectImage(t)
	require.Equal(t, "drop", string(drop.payload))
	push(t, s, deviceID, "board", true)
	fw.expectNoImage(t, 300*time.Millisecond) // waits for the drop to be on screen

	fw.displaying(t, drop.counter)
	board := fw.expectImage(t)
	require.Equal(t, "board", string(board.payload))
	assert.False(t, board.immediate)
}

// A queue:true push onto plain rotation still preempts it right away.
func TestWSQueuedPushPreemptsRotation(t *testing.T) {
	s, deviceID, fw := startQueueTestDevice(t)
	rotationOnScreenWithNextBuffered(t, fw)

	push(t, s, deviceID, "board", true)
	board := fw.expectImage(t)
	require.Equal(t, "board", string(board.payload))
	assert.True(t, board.immediate)
}

// A queue:false push still interrupts a pushed image that is playing.
func TestWSShowNowPushInterruptsPushOnScreen(t *testing.T) {
	s, deviceID, fw := startQueueTestDevice(t)
	rotationOnScreenWithNextBuffered(t, fw)

	push(t, s, deviceID, "board-1", true)
	board := fw.expectImage(t)
	fw.displaying(t, board.counter)
	fw.expectImage(t) // rotation buffered behind the board

	push(t, s, deviceID, "drop-2", false)
	drop := fw.expectImage(t)
	require.Equal(t, "drop-2", string(drop.payload))
	assert.True(t, drop.immediate)
}

// A late "displaying" for the image that was buffered when an interrupt was sent
// must not be taken as the interrupting image being on screen.
func TestWSIgnoresLateAckForOlderImage(t *testing.T) {
	s, deviceID, fw := startQueueTestDevice(t)
	first := fw.expectImage(t)
	fw.displaying(t, first.counter)
	buffered := fw.expectImage(t) // rotation image buffered behind the first
	settle()

	push(t, s, deviceID, "drop", false)
	drop := fw.expectImage(t)
	require.Equal(t, "drop", string(drop.payload))

	// The buffered rotation image started just before the interrupt landed.
	fw.displaying(t, buffered.counter)
	fw.expectNoImage(t, 300*time.Millisecond)

	fw.displaying(t, drop.counter)
	next := fw.expectImage(t)
	assert.Equal(t, "rotation", string(next.payload))
}
