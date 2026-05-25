package loom

import (
	"testing"

	"github.com/slack-go/slack/socketmode"
)

// Regression test for the 1006 disconnect loop. The bug: readLoop acked
// every event with a non-nil Request, including the `hello` lifecycle
// frame (which slack-go creates WITH a Request). Acking hello sends a
// Socket Mode response with an empty envelope ID; Slack closes the socket
// (1006), reconnects, sends hello, gets acked again — forever, and no
// real event is ever delivered. Two different networks reproduced it
// identically because it is deterministic and code-side, not network.
//
// The rule: ONLY data events are ackable. This test pins that rule.
func TestAckable_OnlyDataEvents(t *testing.T) {
	ack := map[socketmode.EventType]bool{
		// Data events — MUST be acked.
		socketmode.EventTypeEventsAPI:    true,
		socketmode.EventTypeSlashCommand: true,
		socketmode.EventTypeInteractive:  true,

		// Lifecycle frames — MUST NOT be acked. hello is the one that
		// caused the outage: it carries a Request but acking it kills the
		// connection.
		socketmode.EventTypeHello:           false,
		socketmode.EventTypeConnecting:      false,
		socketmode.EventTypeConnected:       false,
		socketmode.EventTypeDisconnect:      false,
		socketmode.EventTypeConnectionError: false,
		socketmode.EventTypeIncomingError:   false,
	}
	for typ, want := range ack {
		if got := ackable(typ); got != want {
			t.Errorf("ackable(%q) = %v, want %v", typ, got, want)
		}
	}
}

// Explicit guard on the specific frame that caused the outage.
func TestAckable_NeverAcksHello(t *testing.T) {
	if ackable(socketmode.EventTypeHello) {
		t.Fatal("REGRESSION: acking hello sends an empty-envelope response and Slack drops the socket (1006)")
	}
}
