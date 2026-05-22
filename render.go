package loom

import (
	"context"
	"fmt"

	"github.com/slack-go/slack"
)

// renderer is the single effectful edge that turns a Reply (data) into
// Slack Web API calls. Everything upstream of here — translate, the
// handler — is pure transformation, which keeps the core testable.
type renderer struct {
	api *slack.Client
}

// Renderer is the exported handle for posting Replies to Slack, used by
// asynchronous result delivery (B1) where a background goroutine posts a
// workflow's answer back to its thread. Build one with NewRenderer from
// the same bot token the Listener uses.
type Renderer = renderer

// NewRenderer builds a Renderer from a bot token. The async runner uses
// it to post results; it shares the same Slack Web API surface the
// Listener renders with.
func NewRenderer(botToken string) *Renderer {
	return &renderer{api: slack.New(botToken)}
}

// render posts the Reply for a given triggering Event. It applies any
// reactions first (they attach to the triggering message), then posts
// text/files to the chosen target.
func (r *renderer) render(ctx context.Context, e Event, reply Reply) error {
	if r == nil || r.api == nil {
		// No Slack client configured (e.g. in tests, or a renderer built
		// without a token). Nothing to post; not an error.
		return nil
	}
	// Reactions attach to the message that triggered the event.
	for _, name := range reply.React {
		if e.Context.Channel == "" || e.Timestamp == "" {
			break // nowhere to attach
		}
		ref := slack.NewRefToMessage(e.Context.Channel, e.Timestamp)
		if err := r.api.AddReactionContext(ctx, name, ref); err != nil {
			// A duplicate reaction ("already_reacted") is not fatal.
			return fmt.Errorf("add reaction %q: %w", name, err)
		}
	}

	if reply.Text == "" && len(reply.Files) == 0 {
		return nil // reaction-only reply
	}

	switch reply.Target {
	case TargetEphemeral:
		_, err := r.api.PostEphemeralContext(ctx, e.Context.Channel, e.Context.User,
			slack.MsgOptionText(reply.Text, false))
		return err

	case TargetDM:
		ch, _, _, err := r.api.OpenConversationContext(ctx, &slack.OpenConversationParameters{
			Users: []string{e.Context.User},
		})
		if err != nil {
			return fmt.Errorf("open DM: %w", err)
		}
		_, _, err = r.api.PostMessageContext(ctx, ch.ID, slack.MsgOptionText(reply.Text, false))
		return err

	case TargetChannel:
		_, _, err := r.api.PostMessageContext(ctx, e.Context.Channel,
			slack.MsgOptionText(reply.Text, false))
		return err

	case TargetSameThread:
		fallthrough
	default:
		// Reply in-thread. Use the event's thread if present, else start
		// a thread on the triggering message.
		threadTS := e.Context.Thread
		if threadTS == "" {
			threadTS = e.Timestamp
		}
		_, _, err := r.api.PostMessageContext(ctx, e.Context.Channel,
			slack.MsgOptionText(reply.Text, false),
			slack.MsgOptionTS(threadTS),
		)
		return err
	}
}
