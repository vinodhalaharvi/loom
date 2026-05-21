# loom

A Slack front end for [Sibyl](https://github.com/vinodhalaharvi/sibyl), the
agent execution engine. loom listens to Slack over a Socket Mode WebSocket,
turns each event into a typed value, runs a handler, and posts the result
back. It weaves the threads of Slack activity into durable agent execution.

> **Status:** PR-2 — Sibyl-backed handler. A Slack mention/message starts a
> Sibyl `ConvergeWorkflow`; the answer is posted back to the originating
> thread when the workflow completes (the B1 correlation model: start fast,
> await in the background, post on completion). Requires a running Temporal
> cluster and Sibyl worker.

## The idea

Sibyl is the workhorse: durable, Temporal-backed agent execution with
per-agent OAuth. It is unchanged by loom. loom is one *front end* to Sibyl —
a peer of the `agentscript` DSL — that uses Slack as the interface instead of
a command line or a custom language. Slack already solves the hard
interface problems: input and output, file upload/download, identity,
channels as scope boundaries, threads as sessions, mobile, search, history.

The architectural rule (inherited from Sibyl's design): **loom is
presentation and routing only.** Behavior, state, durability, and credential
handling live in Sibyl. loom translates Slack events into Sibyl invocations
and Sibyl results into Slack replies. It never holds a vendor credential.

## How it works

```mermaid
flowchart TD
    subgraph slack["Slack"]
        evt["message · @mention · /command<br/>· reaction · file"]
        reply["reply: text · files · reactions<br/>· 🔑 auth button"]
    end

    subgraph loom["loom — Arrow[Event, Reply]"]
        ws["Socket Mode WebSocket<br/>(outbound; no public URL)"]
        ack["ack within 3s"]
        tr["translate → Event<br/>{Context, Kind, Text, Files}"]
        h["handler"]
        rn["render → Slack API"]
    end

    subgraph sibyl["Sibyl (PR-2+, unchanged)"]
        inv["submit workflow"]
        oauth["WithOAuth → per-user token"]
        work["durable workflow"]
    end

    evt --> ws --> ack --> tr --> h
    h -->|PR-1: echo| rn
    h -.->|PR-2: invoke| inv --> oauth --> work
    work -.->|results / HITL via<br/>Sibyl channels/slack| reply
    rn --> reply
```

Two directions touch Slack, and they live in different repos on purpose:

- **Ingress** (loom): the Socket Mode listener that receives events. Sibyl's
  `channels/slack` deliberately does *not* listen — it posts and polls — so
  there is no overlap.
- **Egress / HITL** (Sibyl's `channels/slack`): when a *running workflow*
  needs to ask a human something, it posts and waits via Sibyl's existing
  Slack channel. loom doesn't mediate that path.

## Design decisions

- **Single-turn, no streams (YAGNI).** Each Slack event is one
  `Arrow[Event, Reply]` invocation. Multi-message-over-time behavior (a
  workflow posting progress) is reconstructed from *correlation* (thread ↔
  workflow) plus Sibyl's durability — not from a streaming arrow. A streaming
  arrow would duplicate the durability that already lives in Temporal. We add
  one only if a genuine live-stream feature (e.g. token-by-token message
  edits with nothing durable behind them) ever demands it.
- **The 3-second ack.** Slack redelivers any event not acknowledged within
  ~3s. A workflow can take minutes, so the loop acks *immediately* and
  dispatches handling to a bounded worker pool. Results come back
  asynchronously via Sibyl's egress, not as the synchronous response to the
  event. This is exactly why the single-turn model fits: the handler's job
  per event is bounded.
- **Reply is data, not action.** Handlers return a `Reply` value; a single
  `render` step at the edge turns it into Slack API calls. Everything before
  `render` is pure and testable.
- **Slack nouns are context, not arrows.** Channel, user, thread become
  `Context` that rides alongside the data (the analog of Sibyl's
  `AgentContext`). Only `Event → Reply` is composed.

## Running

loom needs two tokens and Socket Mode enabled on your Slack app:

```bash
export SLACK_BOT_TOKEN="xoxb-..."   # Web API: post, react, user info
export SLACK_APP_TOKEN="xapp-..."   # Socket Mode: connections:write

go run ./cmd/loom
```

Slack app setup:

1. Enable **Socket Mode**.
2. Create an **App-Level Token** with `connections:write` → `SLACK_APP_TOKEN`.
3. **OAuth & Permissions** bot scopes: `app_mentions:read`, `chat:write`,
   `reactions:write`, `channels:history`, `im:history` (and `commands` if you
   want slash commands).
4. **Event Subscriptions** → bot events: `app_mention`, `message.channels`,
   `message.im`.
5. Install to the workspace → `SLACK_BOT_TOKEN`.

Then `@mention` the bot in a channel it's in. PR-1 reacts 👀 and echoes your
text back in-thread.

## Layout

```
loom/
├── cmd/loom/main.go    # entry point: tokens + Temporal config, wires the handler
├── types.go            # Event, Context, Reply, Handler — the mapping
├── translate.go        # Slack event → loom.Event (the ingress half)
├── listener.go         # Socket Mode connection, ack, bounded dispatch
├── render.go           # Reply → Slack API calls (the one effectful edge)
├── correlation.go      # ThreadID ↔ WorkflowID table (B1)
├── sibyl.go            # the invoke seam: Start / Await + async runner
├── sibyl_handler.go    # SibylHandler: event → Question → workflow
├── handler.go          # EchoHandler (PR-1; kept for reference/testing)
└── *_test.go           # translate / handler / correlation / runner tests
```

## Roadmap

- **PR-1:** Socket Mode listener, typed event mapping, echo handler.
- **PR-2 (this):** depend on Sibyl; `invoke` starts a `ConvergeWorkflow`
  via a Temporal client and posts the answer back to the thread when it
  completes (B1 correlation: `Start` → record in the correlation table →
  background `Await` → post). Sibyl is unchanged.
- **Later:**
  - **Auth button.** Translate a workflow's missing-credential failure into
    a "🔑 Authorize" reply. Deferred because `ConvergeWorkflow` doesn't use
    per-agent OAuth, so nothing raises `MissingCredentialError` on this path
    yet — and that error crosses the Temporal boundary as a generic
    `ApplicationError`, so detecting it cleanly needs a small Sibyl-side
    change (typed `ApplicationError`) paired with the loom translation. A
    focused PR when loom drives an OAuth-using agent.
  - Multi-turn thread continuity (route a human's follow-up reply into the
    existing workflow via the correlation table).
  - Channel-scoped agent availability ("roles via channels").
  - File handling; Slack-ID → canonical identity mapping.
