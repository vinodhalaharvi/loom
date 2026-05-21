package loom

import (
	"context"
	"strings"
)

// EchoHandler is the PR-1 placeholder Handler. It acknowledges mentions
// and messages with an "eyes" reaction and echoes the text back in-thread.
// It exists to prove the Slack round trip (Socket Mode in → handler →
// render out) before any backend is wired.
//
// In PR-2 this is replaced by a handler whose body submits a Sibyl
// workflow and correlates the thread to the workflow ID; the Handler
// type and the surrounding listener do not change.
func EchoHandler(_ context.Context, e Event) (Reply, error) {
	switch e.Kind {
	case KindMention, KindMessage:
		text := strings.TrimSpace(e.Text)
		if text == "" {
			// Nothing to echo; just acknowledge.
			return Reply{Target: TargetSameThread, React: []string{"eyes"}}, nil
		}
		return Reply{
			Target: TargetSameThread,
			React:  []string{"eyes"},
			Text:   "echo: " + text,
		}, nil

	case KindSlashCommand:
		return Reply{
			Target: TargetEphemeral,
			Text:   "loom received: " + strings.TrimSpace(e.Text),
		}, nil

	default:
		// Reactions, file shares, interactions: ignore for now.
		return Reply{}, nil
	}
}
