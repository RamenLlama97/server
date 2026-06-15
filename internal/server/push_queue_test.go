package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"tronbyt-server/internal/data"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dummyWebpB64 is a small base64 string accepted by base64.StdEncoding (the push
// handler only decodes it; the bytes are never parsed as an image here).
const dummyWebpB64 = "UklGRkXlAAAgAAAAAQABAAHAIwAA//VucG/v/4/f//x8oAA="

func TestPushQueueFIFOAndDrain(t *testing.T) {
	s := newTestServerAPI(t)
	dev := "testdevice"

	s.enqueuePush(dev, []byte("A"), 5, false, "")
	s.enqueuePush(dev, []byte("B"), 15, false, "")

	d1, dwell1, ok1 := s.dequeuePush(dev)
	require.True(t, ok1)
	assert.Equal(t, []byte("A"), d1)
	assert.Equal(t, 5, dwell1)

	d2, dwell2, ok2 := s.dequeuePush(dev)
	require.True(t, ok2)
	assert.Equal(t, []byte("B"), d2)
	assert.Equal(t, 15, dwell2)

	_, _, ok3 := s.dequeuePush(dev)
	assert.False(t, ok3, "queue should be empty after draining both items")
}

func TestPushQueueSupersedeFlush(t *testing.T) {
	s := newTestServerAPI(t)
	dev := "testdevice"

	s.enqueuePush(dev, []byte("A"), 0, false, "") // queued
	s.enqueuePush(dev, []byte("B"), 0, false, "") // queued
	s.enqueuePush(dev, []byte("C"), 0, true, "")  // supersede: flush + interrupt

	interrupt, pending := s.pushQueueState(dev, false)
	assert.True(t, interrupt, "supersede should set the interrupt flag")
	assert.True(t, pending)

	d, _, ok := s.dequeuePush(dev)
	require.True(t, ok)
	assert.Equal(t, []byte("C"), d, "only the superseding image should remain")

	_, _, ok = s.dequeuePush(dev)
	assert.False(t, ok, "pending A and B should have been flushed")
}

func TestPushQueueCoalesce(t *testing.T) {
	s := newTestServerAPI(t)
	dev := "testdevice"

	// Two non-interrupt pushes sharing a coalesceID: only the latest survives.
	s.enqueuePush(dev, []byte("old"), 0, false, "board")
	s.enqueuePush(dev, []byte("new"), 0, false, "board")
	// A different id coexists.
	s.enqueuePush(dev, []byte("other"), 0, false, "drop")

	d1, _, ok := s.dequeuePush(dev)
	require.True(t, ok)
	assert.Equal(t, []byte("new"), d1, "coalesced id should keep only the latest")

	d2, _, ok := s.dequeuePush(dev)
	require.True(t, ok)
	assert.Equal(t, []byte("other"), d2, "a different coalesceID is unaffected")

	_, _, ok = s.dequeuePush(dev)
	assert.False(t, ok)
}

func TestPushQueueStateTakeInterruptClears(t *testing.T) {
	s := newTestServerAPI(t)
	dev := "testdevice"
	s.enqueuePush(dev, []byte("A"), 0, true, "")

	interrupt, _ := s.pushQueueState(dev, true) // act on the interrupt
	assert.True(t, interrupt)

	interrupt2, _ := s.pushQueueState(dev, false)
	assert.False(t, interrupt2, "interrupt flag should be cleared after being taken")
}

func TestDeliverPushOrderingDropThenBoard(t *testing.T) {
	s := newTestServerAPI(t)
	dev := "testdevice"

	// One move: foreground drop (queue:false, 5s) then board (queue:true, 15s).
	require.NoError(t, s.deliverPush(context.Background(), dev, "", []byte("drop"), false, false, 5, ""))
	require.NoError(t, s.deliverPush(context.Background(), dev, "", []byte("board"), false, true, 15, ""))

	d1, dwell1, ok := s.dequeuePush(dev)
	require.True(t, ok)
	assert.Equal(t, []byte("drop"), d1)
	assert.Equal(t, 5, dwell1)

	d2, dwell2, ok := s.dequeuePush(dev)
	require.True(t, ok)
	assert.Equal(t, []byte("board"), d2)
	assert.Equal(t, 15, dwell2)
}

func TestDeliverPushSupersedeDiscardsPendingBoard(t *testing.T) {
	s := newTestServerAPI(t)
	dev := "testdevice"

	require.NoError(t, s.deliverPush(context.Background(), dev, "", []byte("p1drop"), false, false, 5, ""))
	require.NoError(t, s.deliverPush(context.Background(), dev, "", []byte("p1board"), false, true, 15, ""))
	// Player 2 moves before P1's board shows: a new queue:false push supersedes.
	require.NoError(t, s.deliverPush(context.Background(), dev, "", []byte("p2drop"), false, false, 5, ""))

	d, _, ok := s.dequeuePush(dev)
	require.True(t, ok)
	assert.Equal(t, []byte("p2drop"), d)

	_, _, ok = s.dequeuePush(dev)
	assert.False(t, ok, "P1's pending board should be flushed by P2's supersede push")
}

func TestGetNextAppImageDrainsQueueWithDwell(t *testing.T) {
	s := newTestServerAPI(t)
	dev := &data.Device{ID: "testdevice", DefaultInterval: 15}

	s.enqueuePush(dev.ID, []byte("img"), 5, true, "")

	imgData, app, err := s.GetNextAppImage(context.Background(), dev, nil)
	require.NoError(t, err)
	assert.Equal(t, []byte("img"), imgData)
	require.NotNil(t, app)
	assert.True(t, app.Pushed)
	assert.Equal(t, "", app.Iname, "transient pushed image uses an empty Iname")
	assert.Equal(t, 5, dev.GetEffectiveDwellTime(app), "dwell should equal the per-image display time")

	// One-shot: the queue is empty now, so the next call falls through to rotation
	// (the default image, with no app context).
	_, app2, err := s.GetNextAppImage(context.Background(), dev, nil)
	require.NoError(t, err)
	assert.Nil(t, app2, "after draining, no transient app should be returned")
}

func TestHandleNextAppUsesQueueDisplayTime(t *testing.T) {
	s := newTestServerAPI(t)
	dev := "testdevice"

	s.enqueuePush(dev, []byte("img"), 7, true, "")

	req := httptest.NewRequest(http.MethodGet, "/testdevice/next", nil)
	req.SetPathValue("id", dev)
	rr := httptest.NewRecorder()
	s.handleNextApp(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "image/webp", rr.Header().Get("Content-Type"))
	assert.Equal(t, "7", rr.Header().Get("Tronbyt-Dwell-Secs"))
	assert.Equal(t, "img", rr.Body.String())

	// A transient pushed image (empty Iname) must not update displaying_app.
	var d data.Device
	require.NoError(t, s.DB.First(&d, "id = ?", dev).Error)
	assert.True(t, d.DisplayingApp == nil || *d.DisplayingApp == "",
		"transient image should not set displaying_app")
}

func TestHandlePushImageQueueAndDisplayTime(t *testing.T) {
	s := newTestServerAPI(t)
	deviceID := "testdevice"

	pushData := PushData{
		Image:           dummyWebpB64,
		Queue:           true,
		DisplayTimeSecs: 9,
	}
	body, _ := json.Marshal(pushData)
	req := newAPIRequest("POST", fmt.Sprintf("/v0/devices/%s/push", deviceID), "device_api_key", body)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	// No installationID => no rotation app/file; the image is queued transiently
	// with the requested display time.
	_, dwell, ok := s.dequeuePush(deviceID)
	require.True(t, ok)
	assert.Equal(t, 9, dwell)
}
