// Package loom is a Slack front end for the Sibyl agent execution engine.
//
// It listens to Slack over a Socket Mode WebSocket, turns each incoming
// event into a typed Event, runs a handler that produces a Reply, and
// renders that Reply back to Slack. The spine is one composable shape:
//
//	Handler = func(context.Context, Event) (Reply, error)   // Arrow[Event, Reply]
//
// This file defines the vocabulary — the formal mapping from Slack's
// event-and-entity world into the backend's request/response world. The
// nouns (workspace, channel, user, thread) become Context that rides
// alongside the data; the verbs (message, mention, reaction, ...) become
// a single sealed EventKind so the handler has one input type. Output is
// a Reply value (data, not an action); a single render step at the edge
// turns it into Slack API calls.
//
// PR-1 scope: this vocabulary plus a Socket Mode listener and a trivial
// echo handler. No backend (Sibyl) dependency yet — that arrives in PR-2,
// where the handler's body submits a durable workflow instead of echoing.
package loom

import "context"

// === Layer 1: Context — the Slack nouns as environment ======================

// Context is the who/where of an event. These are Slack's entity nouns;
// in the backend they map to identity and scope (a channel is a role
// boundary, a user is the invoking identity, a thread is the session
// correlation handle). Context is carried alongside the data, never
// composed as an arrow.
type Context struct {
	// Workspace is the Slack team/workspace ID (the tenant).
	Workspace string
	// Channel is the channel ID the event occurred in. Doubles as the
	// role/scope boundary: which agents are available, what identity
	// scope applies.
	Channel string
	// User is the native Slack user ID of the actor (e.g. "U07ABC123").
	// This becomes the backend's invoking identity (after mapping).
	User string
	// Thread is the parent thread timestamp, or "" for a top-level
	// message. Used as the session correlation handle: a durable
	// workflow started from a thread reports back to the same thread.
	Thread string
	// Bot is our own bot user ID, so the handler can recognize (and
	// ignore) its own messages.
	Bot string
}

// === Layer 2: Event — the Slack verbs as one sealed sum ====================

// EventKind enumerates the kinds of Slack activity loom reacts to. It is
// a closed set: a single sum so the handler has one input type (Event)
// and dispatches on Kind, rather than a separate type per Slack event.
type EventKind int

const (
	// KindUnknown is the zero value; events loom doesn't model map here
	// and are typically ignored.
	KindUnknown EventKind = iota
	// KindMessage is a plain message posted in a channel or thread.
	KindMessage
	// KindMention is a message that @-mentions the bot.
	KindMention
	// KindSlashCommand is a slash command invocation (/something ...).
	KindSlashCommand
	// KindReaction is an emoji reaction added to a message. Used as a
	// lightweight signal (acknowledge, approve, trigger).
	KindReaction
	// KindFileShared is a file upload event.
	KindFileShared
	// KindInteraction is a Block Kit interactive component event
	// (button click, menu select).
	KindInteraction
)

// String returns a human-readable name for the kind.
func (k EventKind) String() string {
	switch k {
	case KindMessage:
		return "message"
	case KindMention:
		return "mention"
	case KindSlashCommand:
		return "slash_command"
	case KindReaction:
		return "reaction"
	case KindFileShared:
		return "file_shared"
	case KindInteraction:
		return "interaction"
	default:
		return "unknown"
	}
}

// Attachment is a file moving in either direction: an upload attached to
// an incoming event, or a file to post back in a Reply.
type Attachment struct {
	// Name is the file name.
	Name string
	// MimeType is the content type, if known.
	MimeType string
	// URL is a Slack-hosted URL for incoming files (requires the bot
	// token to download). Empty for outgoing files built in-process.
	URL string
	// Bytes is the raw content for outgoing files, or downloaded
	// content for incoming ones. May be nil if not yet fetched.
	Bytes []byte
}

// Event is the single input type to a handler. Every Slack activity loom
// cares about becomes an Event; Kind says which, and the other fields
// carry whatever that kind provides (Text for messages/commands, Files
// for uploads, etc.). Raw holds the original decoded Slack payload as an
// escape hatch for fields the typed surface doesn't expose yet.
type Event struct {
	Context Context
	Kind    EventKind
	// Text is the message text, command text, or reaction name,
	// depending on Kind. May be empty.
	Text string
	// Files are attachments on the event. May be empty.
	Files []Attachment
	// Timestamp is the Slack message ts of the triggering message, used
	// for threading replies and correlating reactions.
	Timestamp string
	// Raw is the original decoded Slack event value. Type-assert it when
	// the typed fields above aren't enough. Avoid depending on it from
	// handler logic; prefer promoting needed fields to typed ones.
	Raw any
}

// === Layer 3: Reply — output as data, not action ===========================

// ReplyTarget says where a Reply goes. Kept separate from the content so
// the same content can be routed differently (a thread reply vs. an
// ephemeral nudge vs. a DM).
type ReplyTarget int

const (
	// TargetSameThread replies in the thread the event came from (or
	// starts a thread on the triggering message if it was top-level).
	TargetSameThread ReplyTarget = iota
	// TargetChannel posts to the event's channel at top level.
	TargetChannel
	// TargetEphemeral posts a message only the triggering user can see.
	TargetEphemeral
	// TargetDM sends a direct message to the triggering user.
	TargetDM
)

// Reply is the description of a response — what to send, not the act of
// sending. A handler returns a Reply; a single render step at the edge
// turns it into Slack API calls. Keeping Reply as data is what makes
// handlers pure and testable.
//
// An empty Reply (zero value) is valid and means "do nothing" — useful
// for handlers that only need to acknowledge via a reaction, or that
// intentionally ignore an event.
type Reply struct {
	// Target is where the reply goes. The zero value (TargetSameThread)
	// is the common case.
	Target ReplyTarget
	// Text is the message text. Required if Blocks is empty.
	Text string
	// Files are attachments to upload with the reply.
	Files []Attachment
	// React lists emoji names (without colons, e.g. "eyes",
	// "white_check_mark") to add to the triggering message. Independent
	// of Text: a handler may react without posting, or both.
	React []string
}

// IsEmpty reports whether the Reply would send nothing at all.
func (r Reply) IsEmpty() bool {
	return r.Text == "" && len(r.Files) == 0 && len(r.React) == 0
}

// === The handler spine =====================================================

// Handler is the composable unit: Arrow[Event, Reply]. The whole front
// end is a Handler; middleware (logging, recovery, rate limiting) wraps
// Handlers into Handlers. PR-2 replaces the echo handler's body with a
// call into Sibyl, but the type stays exactly this.
type Handler func(ctx context.Context, e Event) (Reply, error)
