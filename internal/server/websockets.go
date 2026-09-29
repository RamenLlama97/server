package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"tronbyt-server/internal/data"

	"github.com/gorilla/websocket"
	"gorm.io/gorm"
)

const (
	// maxDisplayingAckTimeoutSeconds is a safety cap for v1+ firmware when displaying
	// ACK never arrives (dropped image, firmware bug). Long enough for very long WebP
	// animations but prevents indefinite rotation stall.
	maxDisplayingAckTimeoutSeconds = 600
)

// queueChangeAction is how the write loop reacts when a device's transient push
// queue changes.
type queueChangeAction int

const (
	// queueChangeIgnore leaves the display alone; the queue drains after the next ACK.
	queueChangeIgnore queueChangeAction = iota
	// queueChangePreempt sends the queue head now with "immediate", interrupting the display.
	queueChangePreempt
	// queueChangeReplace sends the queue head without "immediate". It replaces the
	// image the device has buffered next, and plays once the current image finishes.
	queueChangeReplace
)

func (a queueChangeAction) String() string {
	switch a {
	case queueChangeIgnore:
		return "ignore"
	case queueChangePreempt:
		return "preempt"
	case queueChangeReplace:
		return "replace-buffered"
	default:
		return fmt.Sprintf("queueChangeAction(%d)", int(a))
	}
}

// decideQueueChange picks how the write loop reacts to a push queue change.
//
// The loop sends one image ahead of the display: once the device ACKs that image N
// is displaying, the loop sends image N+1, which the firmware buffers until N has
// finished its dwell. The firmware has a single buffer slot, so a newer image
// replaces a buffered one that has not started. The image on screen
// (onScreenIsQueued) and the image sent and awaiting its ACK (sentIsQueued) can
// therefore differ, and a queue:true push must be judged against what is on screen:
//
//   - interrupt (a queue:false push arrived): preempt whatever is showing.
//   - nothing pending, or the sent image is itself a transient push: ignore; the
//     queue drains in order after that image's ACK.
//   - a transient push is on screen with a rotation image buffered behind it:
//     replace the buffered image without "immediate", so the push on screen plays
//     to the end and the queued push follows it.
//   - rotation/default is on screen: preempt it right away.
func decideQueueChange(interrupt, pending, sentIsQueued, onScreenIsQueued bool) queueChangeAction {
	switch {
	case interrupt:
		return queueChangePreempt
	case !pending || sentIsQueued:
		return queueChangeIgnore
	case onScreenIsQueued:
		return queueChangeReplace
	default:
		return queueChangePreempt
	}
}

// ackTracker matches the device's "displaying" ACKs to the image the write loop
// just sent. The firmware numbers every image it buffers with an increasing
// counter, reported in {"queued":N} and later {"displaying":N}. An image sent now
// gets a counter above every counter already seen, so a "displaying" with a lower
// counter is a late ACK for an older image (e.g. one that started just as an
// interrupt was sent) and must not be taken as the new image being on screen.
type ackTracker struct {
	maxSeen int // highest counter seen in any queued/displaying message; -1 if none yet
	minAck  int // lowest counter accepted as the ACK for the image just sent
}

func newAckTracker() ackTracker {
	return ackTracker{maxSeen: -1}
}

// imageSent records that a new image was just written to the device.
func (t *ackTracker) imageSent() {
	t.minAck = t.maxSeen + 1
}

// observe records a device message and reports whether it is the "displaying" ACK
// for the image just sent. {"queued":N} notices only update the counters.
func (t *ackTracker) observe(msg WSMessage) bool {
	if msg.Queued != nil {
		t.see(*msg.Queued)
	}
	displaying := msg.Displaying
	if displaying == nil {
		displaying = msg.Counter // alternative format: {"status":"displaying","counter":N}
	}
	if displaying == nil {
		return false
	}
	isAck := *displaying >= t.minAck
	t.see(*displaying)
	return isAck
}

func (t *ackTracker) see(counter int) {
	if counter > t.maxSeen {
		t.maxSeen = counter
	}
}

type WSMessage struct {
	Queued     *int        `json:"queued"`
	Displaying *int        `json:"displaying"`
	Counter    *int        `json:"counter"`
	ClientInfo *ClientInfo `json:"client_info"`
}

type ClientInfo struct {
	FirmwareVersion    string  `json:"firmware_version"`
	FirmwareType       string  `json:"firmware_type"`
	ProtocolVersion    *int    `json:"protocol_version"`
	MACAddress         string  `json:"mac"`
	SSID               *string `json:"ssid"`
	WifiPowerSave      *int    `json:"wifi_power_save"`
	SkipDisplayVersion *bool   `json:"skip_display_version"`
	SkipBootAnimation  *bool   `json:"skip_boot_animation"`
	APMode             *bool   `json:"ap_mode"`
	PreferIPv6         *bool   `json:"prefer_ipv6"`
	SwapColors         *bool   `json:"swap_colors"`
	ColorOrder         *string `json:"color_order"`
	DisableTouch       *bool   `json:"disable_touch"`
	TouchBeep          *bool   `json:"touch_beep"`
	ImageURL           *string `json:"image_url"`
	Hostname           *string `json:"hostname"`
	SNTPServer         *string `json:"sntp_server"`
	SyslogAddr         *string `json:"syslog_addr"`
}

type WSEvent struct {
	Type     string `json:"type"`
	DeviceID string `json:"device_id,omitempty"`
	AppID    string `json:"app_id,omitempty"`
	Payload  any    `json:"payload,omitempty"`
}

type DeviceCommandMessage struct {
	Payload []byte
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("id")

	device, err := s.reloadDevice(deviceID)
	if err != nil {
		slog.Warn("WS connection rejected", "id", deviceID, "error", err)
		http.Error(w, "Device not found", http.StatusNotFound)
		return
	}

	if device.RequireAPIKey {
		if key := extractDeviceKey(r); key == "" || key != device.APIKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	user, err := gorm.G[data.User](s.DB).Where("username = ?", device.Username).First(r.Context())
	if err != nil {
		slog.Error("User for device not found in WS handler", "username", device.Username, "error", err)
		http.Error(w, "Internal Server Error: device owner not found", http.StatusInternalServerError)
		return
	}

	conn, err := s.Upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("WS upgrade failed", "error", err)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			slog.Error("Failed to close WS connection", "error", err)
		}
	}()

	s.metrics.wsConnections.Inc()
	defer s.metrics.wsConnections.Dec()

	slog.Info("WS Connected", "device", deviceID)

	// Update protocol type if different from current
	if device.Info.ProtocolType != data.ProtocolWS {
		slog.Info("Updating protocol_type to WS on connect", "device", deviceID)
		device.Info.ProtocolType = data.ProtocolWS
		if _, err := gorm.G[data.Device](s.DB).Where("id = ?", device.ID).Update(r.Context(), "info", data.JSONMap{"protocol_type": data.ProtocolWS}); err != nil {
			slog.Error("Failed to update protocol_type", "error", err)
		}
	}
	ch := s.Broadcaster.Subscribe(deviceID)
	defer s.Broadcaster.Unsubscribe(deviceID, ch)

	ackCh := make(chan WSMessage, 10)
	stopCh := make(chan struct{})

	// Read loop to handle ping/pong/close and client messages
	go func() {
		defer close(stopCh)
		for {
			var msg WSMessage
			if err := conn.ReadJSON(&msg); err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					slog.Info("WS closed unexpectedly", "error", err)
				}
				return
			}

			// Update LastSeen
			if _, err := gorm.G[data.Device](s.DB).Where("id = ?", device.ID).Update(context.Background(), "last_seen", time.Now()); err != nil {
				slog.Error("Failed to update last_seen", "error", err)
			}

			// Handle Message
			if msg.ClientInfo != nil {
				// Update Device Info
				device.Info.FirmwareVersion = msg.ClientInfo.FirmwareVersion
				device.Info.FirmwareType = msg.ClientInfo.FirmwareType
				if msg.ClientInfo.ProtocolVersion != nil {
					device.Info.ProtocolVersion = msg.ClientInfo.ProtocolVersion
				}
				device.Info.MACAddress = msg.ClientInfo.MACAddress

				if msg.ClientInfo.SSID != nil {
					device.Info.SSID = msg.ClientInfo.SSID
				}
				if msg.ClientInfo.WifiPowerSave != nil {
					device.Info.WifiPowerSave = msg.ClientInfo.WifiPowerSave
				}
				if msg.ClientInfo.SkipDisplayVersion != nil {
					device.Info.SkipDisplayVersion = msg.ClientInfo.SkipDisplayVersion
				}
				if msg.ClientInfo.SkipBootAnimation != nil {
					device.Info.SkipBootAnimation = msg.ClientInfo.SkipBootAnimation
				}
				if msg.ClientInfo.APMode != nil {
					device.Info.APMode = msg.ClientInfo.APMode
				}
				if msg.ClientInfo.PreferIPv6 != nil {
					device.Info.PreferIPv6 = msg.ClientInfo.PreferIPv6
				}
				if msg.ClientInfo.SwapColors != nil {
					device.Info.SwapColors = msg.ClientInfo.SwapColors
				}
				if msg.ClientInfo.ColorOrder != nil {
					// Store the canonical lower-case form so the settings page
					// shows what the device is actually using. A value this
					// server does not recognise is ignored rather than stored,
					// since the page could not represent it anyway.
					if order, ok := normalizeColorOrder(*msg.ClientInfo.ColorOrder); ok {
						device.Info.ColorOrder = &order
					}
				}
				if msg.ClientInfo.DisableTouch != nil {
					device.Info.DisableTouch = msg.ClientInfo.DisableTouch
				}
				if msg.ClientInfo.TouchBeep != nil {
					device.Info.TouchBeep = msg.ClientInfo.TouchBeep
				}
				if msg.ClientInfo.ImageURL != nil {
					device.Info.ImageURL = msg.ClientInfo.ImageURL
				}
				if msg.ClientInfo.Hostname != nil {
					device.Info.Hostname = msg.ClientInfo.Hostname
				}
				if msg.ClientInfo.SNTPServer != nil {
					device.Info.SNTPServer = msg.ClientInfo.SNTPServer
				}
				if msg.ClientInfo.SyslogAddr != nil {
					device.Info.SyslogAddr = msg.ClientInfo.SyslogAddr
				}

				if _, err := gorm.G[data.Device](s.DB).Where("id = ?", device.ID).Update(context.Background(), "info", device.Info); err != nil {
					slog.Error("Failed to update device info", "error", err)
				}
			}

			if msg.Queued != nil {
				// If we get a queued message, it's a new firmware device.
				// Update protocol version if not set.
				if device.Info.ProtocolVersion == nil {
					slog.Info("First 'queued' message, setting protocol_version to 1", "device", deviceID)
					newVersion := 1
					device.Info.ProtocolVersion = &newVersion
					if _, err := gorm.G[data.Device](s.DB).Where("id = ?", device.ID).Update(context.Background(), "info", device.Info); err != nil {
						slog.Error("Failed to update device info (protocol version)", "error", err)
					}
				}
			}

			// Forward display ACKs and "queued" notices; the write loop uses the
			// counters to match an ACK to the image it just sent.
			if msg.Displaying != nil || msg.Counter != nil || msg.Queued != nil {
				select {
				case ackCh <- msg:
				default:
				}
			}
		}
	}()

	s.wsWriteLoop(r.Context(), conn, device, &user, ackCh, ch, stopCh, s.GetBaseURL(r))
}

// baseURL is this server's address as the device connected to it, captured
// when the socket opened — the write loop outlives the request that made it.
func (s *Server) wsWriteLoop(ctx context.Context, conn *websocket.Conn, initialDevice *data.Device, user *data.User, ackCh <-chan WSMessage, broadcastCh <-chan any, stopCh <-chan struct{}, baseURL string) {
	device := *initialDevice
	lastSentBrightness := -1
	sendImmediate := false
	// Whether the image the device last ACKed as displaying is a transient push.
	// Not the same as the image last sent: see decideQueueChange.
	onScreenIsQueued := false
	acks := newAckTracker()

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		// Calculate effective brightness
		effectiveBrightness := device.GetEffectiveBrightness()

		// 1. Get Next Image (transient push queue first, then rotation)
		imgData, app, err := s.GetNextAppImage(ctx, &device, user, baseURL)
		if err != nil {
			slog.Error("Failed to get next app", "error", err)

			// Wait for update or timeout before retrying
			timer := time.NewTimer(5 * time.Second)
			select {
			case <-broadcastCh:
				// Update available - reload device
				reloadedDevice, err := s.reloadDevice(initialDevice.ID)
				if err != nil {
					slog.Error("Device gone", "id", initialDevice.ID, "error", err)
					return
				}
				device = *reloadedDevice
			case <-timer.C:
				// Timeout - reload device just in case we missed something
				reloadedDevice, err := s.reloadDevice(initialDevice.ID)
				if err != nil {
					slog.Error("Device gone", "id", initialDevice.ID, "error", err)
					return
				}
				device = *reloadedDevice
			case <-stopCh:
				timer.Stop()
				return
			}
			timer.Stop()
			continue
		}

		// A transient (queued) pushed image has an empty Iname. A queue:true append
		// must not cut such an image short once it is on screen, but a queue:false
		// (supersede) push may. See decideQueueChange.
		sentIsQueued := app != nil && app.Pushed && app.Iname == ""

		dwell := device.GetEffectiveDwellTime(app)

		if err := conn.WriteJSON(map[string]any{"dwell_secs": dwell}); err != nil {
			slog.Error("Failed to write metadata WS message", "error", err)
			return
		}

		// Only send brightness if changed
		if effectiveBrightness != lastSentBrightness {
			if err := conn.WriteJSON(map[string]int{"brightness": effectiveBrightness}); err != nil {
				slog.Error("Failed to write brightness WS message", "error", err)
				return
			}
			lastSentBrightness = effectiveBrightness
		}

		if err := conn.WriteMessage(websocket.BinaryMessage, imgData); err != nil {
			return
		}

		sentImmediate := sendImmediate
		if sendImmediate {
			if err := conn.WriteJSON(map[string]bool{"immediate": true}); err != nil {
				slog.Error("Failed to write immediate WS message", "error", err)
				return
			}
			sendImmediate = false
		}
		acks.imageSent()
		if device.Info.ProtocolVersion == nil {
			// Legacy firmware sends no ACKs; treat the image as on screen once sent.
			onScreenIsQueued = sentIsQueued
		}
		slog.Debug("Sent image to device", "device", device.ID, "transient_push", sentIsQueued,
			"immediate", sentImmediate, "dwell_secs", dwell, "min_ack_counter", acks.minAck)

		// 3. Wait for displaying ACK, safety/legacy timeout, or interrupt.
		// v1+ firmware sends displaying when the image is on screen (including long
		// animations). We wait for that ACK, with a long safety timeout if it never arrives.
		var timer *time.Timer
		var timerC <-chan time.Time
		if device.Info.ProtocolVersion != nil {
			timer = time.NewTimer(time.Duration(maxDisplayingAckTimeoutSeconds) * time.Second)
			timerC = timer.C
		} else {
			timer = time.NewTimer(time.Duration(dwell) * time.Second)
			timerC = timer.C
		}

		interrupted := false
		waiting := true

		for waiting {
			select {
			case msg := <-ackCh:
				// The firmware reports a sequential image counter, not the app we sent, so
				// accept a "displaying" only if its counter belongs to the image just sent.
				if !acks.observe(msg) {
					if msg.Displaying != nil || msg.Counter != nil {
						slog.Debug("Ignoring displaying ACK for an older image", "device", device.ID,
							"min_ack_counter", acks.minAck)
					}
					continue
				}
				waiting = false
				onScreenIsQueued = sentIsQueued

				// Update DisplayingApp confirmation in DB (skip transient pushed
				// images, which use a synthetic app with an empty Iname).
				if app != nil && app.Iname != "" {
					slog.Debug("Received ACK, updating DisplayingApp", "app", app.Iname, "device", device.ID)
					// Only now do we update the database that the device is truly displaying this app.
					if _, err := gorm.G[data.Device](s.DB).Where("id = ?", device.ID).Update(ctx, "displaying_app", app.Iname); err != nil {
						slog.Error("Failed to update displaying_app", "device", device.ID, "error", err)
					}
					// Notify Dashboard
					s.notifyDashboard(user.Username, WSEvent{Type: "image_updated", DeviceID: device.ID})
				} else {
					slog.Debug("Received ACK for default or pushed image (no app context)", "device", device.ID)
				}
			case val := <-broadcastCh:
				// Update available (Reload device first)
				reloaded, err := gorm.G[data.Device](s.DB).Preload("Apps", nil).Where("id = ?", initialDevice.ID).First(ctx)
				if err != nil {
					slog.Error("Device gone", "id", initialDevice.ID)
					return
				}
				device = reloaded

				switch v := val.(type) {
				case DeviceCommandMessage:
					// It's a command, send it directly as JSON (TextMessage)
					if err := conn.WriteMessage(websocket.TextMessage, v.Payload); err != nil {
						slog.Error("Failed to write command to WS", "error", err)
						return
					}
					// Don't interrupt the current app for settings changes, unless it's a reboot (which the device handles)
					continue
				case QueueChanged:
					// The transient push queue changed. Decide against what is on screen, not
					// just what was last sent: the loop sends one image ahead of the display.
					interruptFlag, pending := s.pushQueueState(device.ID, true)
					action := decideQueueChange(interruptFlag, pending, sentIsQueued, onScreenIsQueued)
					slog.Debug("Push queue changed", "device", device.ID, "interrupt", interruptFlag,
						"pending", pending, "sent_transient_push", sentIsQueued,
						"on_screen_transient_push", onScreenIsQueued, "action", action)
					if action != queueChangeIgnore {
						interrupted = true
						waiting = false
						sendImmediate = action == queueChangePreempt
					}
				default:
					// State Change: Update Brightness immediately, but don't interrupt current app
					newBrightness := device.GetEffectiveBrightness()

					if newBrightness != lastSentBrightness {
						if err := conn.WriteJSON(map[string]int{"brightness": newBrightness}); err != nil {
							slog.Error("Failed to write brightness WS message", "error", err)
							return
						}
						lastSentBrightness = newBrightness
					}
				}
			case <-timerC:
				if device.Info.ProtocolVersion != nil {
					appIname := ""
					if app != nil {
						appIname = app.Iname
					}
					slog.Warn(
						"Timed out waiting for displaying ACK, advancing rotation",
						"device", device.ID,
						"app", appIname,
						"timeout_secs", maxDisplayingAckTimeoutSeconds,
					)
				}
				waiting = false
			case <-stopCh:
				if timer != nil {
					timer.Stop()
				}
				return
			}
		}
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}

		if interrupted {
			continue
		}
	}
}

func (s *Server) handleDashboardWS(w http.ResponseWriter, r *http.Request) {
	user := GetUser(r)
	username := user.Username

	conn, err := s.Upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("Dashboard WS upgrade failed", "error", err)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			slog.Error("Failed to close Dashboard WS connection", "error", err)
		}
	}()

	slog.Debug("Dashboard WS Connected", "username", username)

	// Subscribe to user-specific updates
	ch := s.Broadcaster.Subscribe("user:" + username)
	defer s.Broadcaster.Unsubscribe("user:"+username, ch)

	done := make(chan struct{})

	// Read loop (handle ping/pong/close)
	go func() {
		defer close(done)
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					slog.Info("Dashboard WS read error, disconnecting", "username", username, "error", err)
				}
				return
			}
		}
	}()

	// Write loop
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case val := <-ch:
			var data []byte
			if b, ok := val.([]byte); ok {
				data = b
			}

			// Forward the event data (JSON) to the client
			if len(data) == 0 {
				// Fallback for legacy calls sending nil: trigger generic refresh
				data = []byte(`{"type": "refresh"}`)
			}
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				slog.Error("Failed to write message to Dashboard WS", "username", username, "error", err)
				return
			}
		case <-ticker.C:
			// Keep-alive ping
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				slog.Error("Failed to send Dashboard WS ping", "username", username, "error", err)
				return
			}
		}
	}
}

func (s *Server) SetupWebsocketRoutes() {
	s.Router.HandleFunc("GET /{id}/ws", s.handleWS)
	s.Router.HandleFunc("GET /ws", s.RequireLogin(s.handleDashboardWS))
}

func (s *Server) reloadDevice(deviceID string) (*data.Device, error) {
	device, err := gorm.G[data.Device](s.DB).Preload("Apps", nil).Where("id = ?", deviceID).First(context.Background())
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("device not found: %s", deviceID)
		}
		return nil, fmt.Errorf("reload device: %w", err)
	}
	return &device, nil
}
