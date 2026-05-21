package loom

import (
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
)

// translate converts an incoming Socket Mode event into a loom.Event and
// reports whether it is one loom handles. The botUserID is used to fill
// Context.Bot so handlers can recognize the bot's own activity.
//
// Events that don't map to a loom EventKind (connection lifecycle,
// unsupported inner types) return ok=false and are skipped by the caller.
//
// This is the ingress half that Sibyl's channels/slack deliberately
// lacks: Sibyl posts and polls, loom listens.
func translate(evt socketmode.Event, botUserID string) (Event, bool) {
	switch evt.Type {
	case socketmode.EventTypeEventsAPI:
		api, ok := evt.Data.(slackevents.EventsAPIEvent)
		if !ok {
			return Event{}, false
		}
		return translateEventsAPI(api, botUserID)

	case socketmode.EventTypeSlashCommand:
		cmd, ok := evt.Data.(slack.SlashCommand)
		if !ok {
			return Event{}, false
		}
		return Event{
			Context: Context{
				Workspace: cmd.TeamID,
				Channel:   cmd.ChannelID,
				User:      cmd.UserID,
				Bot:       botUserID,
			},
			Kind: KindSlashCommand,
			Text: cmd.Text,
		}, true

	case socketmode.EventTypeInteractive:
		cb, ok := evt.Data.(slack.InteractionCallback)
		if !ok {
			return Event{}, false
		}
		return Event{
			Context: Context{
				Workspace: cb.Team.ID,
				Channel:   cb.Channel.ID,
				User:      cb.User.ID,
				Bot:       botUserID,
			},
			Kind:      KindInteraction,
			Timestamp: cb.MessageTs,
			Raw:       cb,
		}, true

	default:
		// Connection lifecycle (connecting, connected, hello, ...) and
		// anything else loom doesn't model.
		return Event{}, false
	}
}

// translateEventsAPI handles the Events API inner events (app_mention,
// message, reaction_added, file_shared).
func translateEventsAPI(api slackevents.EventsAPIEvent, botUserID string) (Event, bool) {
	switch inner := api.InnerEvent.Data.(type) {
	case *slackevents.AppMentionEvent:
		// Ignore the bot mentioning itself or other bots, to avoid loops.
		if inner.BotID != "" || inner.User == botUserID {
			return Event{}, false
		}
		return Event{
			Context: Context{
				Workspace: api.TeamID,
				Channel:   inner.Channel,
				User:      inner.User,
				Thread:    inner.ThreadTimeStamp,
				Bot:       botUserID,
			},
			Kind:      KindMention,
			Text:      stripMention(inner.Text, botUserID),
			Timestamp: inner.TimeStamp,
		}, true

	case *slackevents.MessageEvent:
		// Skip bot messages (including our own) and message subtypes
		// (edits, deletes, joins) — only react to plain user messages.
		if inner.BotID != "" || inner.User == botUserID || inner.SubType != "" {
			return Event{}, false
		}
		return Event{
			Context: Context{
				Workspace: api.TeamID,
				Channel:   inner.Channel,
				User:      inner.User,
				Thread:    inner.ThreadTimeStamp,
				Bot:       botUserID,
			},
			Kind:      KindMessage,
			Text:      inner.Text,
			Timestamp: inner.TimeStamp,
		}, true

	case *slackevents.ReactionAddedEvent:
		if inner.User == botUserID {
			return Event{}, false
		}
		return Event{
			Context: Context{
				Workspace: api.TeamID,
				Channel:   inner.Item.Channel,
				User:      inner.User,
				Bot:       botUserID,
			},
			Kind:      KindReaction,
			Text:      inner.Reaction, // the emoji name
			Timestamp: inner.Item.Timestamp,
		}, true

	case *slackevents.FileSharedEvent:
		return Event{
			Context: Context{
				Workspace: api.TeamID,
				Channel:   inner.ChannelID,
				User:      inner.UserID,
				Bot:       botUserID,
			},
			Kind: KindFileShared,
			// File bytes are fetched lazily by the handler/render layer
			// using the bot token; PR-1 only records that it happened.
			Files: []Attachment{{Name: inner.File.ID}},
		}, true

	default:
		return Event{}, false
	}
}

// stripMention removes a leading "<@BOTID>" mention token from text so
// handlers see "deploy staging" rather than "<@U07BOT> deploy staging".
func stripMention(text, botUserID string) string {
	token := "<@" + botUserID + ">"
	if len(text) >= len(token) && text[:len(token)] == token {
		return trimLeadingSpace(text[len(token):])
	}
	return text
}

func trimLeadingSpace(s string) string {
	i := 0
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	return s[i:]
}
