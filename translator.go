package loom

import (
	"context"
	"fmt"
	"strings"

	"github.com/vinodhalaharvi/agentscript/pkg/script"
	"github.com/vinodhalaharvi/agentscript/pkg/script/registry"
)

// CompleteFunc is the LLM seam: given a system prompt and a user message,
// return the completion. It matches Sibyl's agent.CompleteFunc, so a
// Sibyl LLM client (e.g. agent.NewAnthropicClient(...).Complete) can be
// passed directly.
type CompleteFunc func(ctx context.Context, systemPrompt, userMessage string) (string, error)

// Translator turns natural-language prose into AgentScript DSL using an
// LLM, then leans on the AgentScript compiler to validate it. The LLM is
// the *author* of the DSL; the compiler (Parse → Resolve → Validate) is
// the safety net. A hallucinated command fails at Resolve, not at
// execution.
//
// Composition discipline (the load-bearing prompt rule): the LLM emits
// sequential pipelines (>=>) by default and only uses parallel fan-out
// (<*>) when the user's request is an unambiguous flat list of
// independent items. Inferring parallelism where the user didn't clearly
// express it is the risky case, so the prompt biases hard toward
// sequential. The grammar is permissive; the translation is conservative.
type Translator struct {
	complete CompleteFunc
	reg      *registry.Registry
	// systemPrompt is built once from the registry's builtin names.
	systemPrompt string
}

// NewTranslator builds a Translator over an LLM CompleteFunc and a
// builtin registry. The registry's names are injected into the system
// prompt so the LLM only emits commands that actually exist.
func NewTranslator(complete CompleteFunc, reg *registry.Registry) *Translator {
	return &Translator{
		complete:     complete,
		reg:          reg,
		systemPrompt: buildSystemPrompt(reg),
	}
}

// ToDSL translates prose into an AgentScript DSL string. It does not
// compile or validate — that's Compile's job. The returned string is
// the LLM's raw DSL with surrounding prose/code-fences stripped.
func (t *Translator) ToDSL(ctx context.Context, prose string) (string, error) {
	if t.complete == nil {
		return "", fmt.Errorf("translator: no LLM configured")
	}
	out, err := t.complete(ctx, t.systemPrompt, prose)
	if err != nil {
		return "", fmt.Errorf("translator: LLM call failed: %w", err)
	}
	return cleanDSL(out), nil
}

// Compile translates prose to DSL and compiles it to a validated Sibyl
// Plan. This is the full front-half of the pipeline: prose → DSL →
// Parse → Resolve → Lower → Finalize → Validate. A DSL error (unknown
// builtin, bad arity) surfaces here as a typed compile error, before
// anything executes.
func (t *Translator) Compile(ctx context.Context, prose string) (script.Source, error) {
	dsl, err := t.ToDSL(ctx, prose)
	if err != nil {
		return "", err
	}
	return script.Source(dsl), nil
}

// buildSystemPrompt assembles the instruction the LLM follows. It lists
// the available builtins (so the LLM can't invent commands) and encodes
// the conservative-composition discipline.
func buildSystemPrompt(reg *registry.Registry) string {
	var names []string
	if reg != nil {
		names = reg.Names()
	}
	available := "(none)"
	if len(names) > 0 {
		available = strings.Join(names, ", ")
	}

	return `You translate a user's request into a small pipeline language called AgentScript. Output ONLY the AgentScript program — no prose, no explanation, no code fences.

GRAMMAR
A program is a single block:
  temporal static ( <pipeline> )
A pipeline is one or more commands joined by >=> (sequential, left output feeds right):
  command "arg" >=> command "arg" >=> command
Parallel fan-out exists as <*> inside parentheses, but use it ONLY when the request is an explicit, unambiguous flat list of independent things to do at once. When in doubt, use sequential >=> .

AVAILABLE COMMANDS (you may use ONLY these — never invent a command):
  ` + available + `

RULES
1. Output exactly one block: temporal static ( ... ). Nothing else.
2. Use only commands from the AVAILABLE COMMANDS list. If the request needs a command that does not exist, choose the closest available command; do not invent names.
3. Prefer sequential >=> . Use <*> only for a clear list of independent parallel actions.
4. String arguments are double-quoted. Pass the user's intent as the argument text.
5. Keep it minimal — the smallest pipeline that satisfies the request.

EXAMPLE
Request: say hello to the team
Output: temporal static ( echo "hello to the team" )`
}

// cleanDSL strips common LLM wrapping (code fences, leading/trailing
// prose) so the result is just the AgentScript program. It is
// deliberately conservative: it removes fences and trims whitespace but
// does not try to "fix" the DSL — malformed output should fail loudly at
// Compile, not be silently patched here.
func cleanDSL(s string) string {
	s = strings.TrimSpace(s)
	// Strip a leading ```lang fence and trailing ``` fence if present.
	if strings.HasPrefix(s, "```") {
		if nl := strings.IndexByte(s, '\n'); nl != -1 {
			s = s[nl+1:]
		}
		s = strings.TrimSuffix(strings.TrimSpace(s), "```")
	}
	s = strings.TrimSpace(s)
	// If the model emitted extra lines, keep from the first "temporal" or
	// "memory" block keyword to the last closing paren — the program.
	if i := indexOfBlockStart(s); i > 0 {
		s = s[i:]
	}
	if j := strings.LastIndexByte(s, ')'); j != -1 && j < len(s)-1 {
		s = s[:j+1]
	}
	return strings.TrimSpace(s)
}

func indexOfBlockStart(s string) int {
	t := strings.Index(s, "temporal")
	m := strings.Index(s, "memory")
	switch {
	case t == -1:
		return m
	case m == -1:
		return t
	case t < m:
		return t
	default:
		return m
	}
}
