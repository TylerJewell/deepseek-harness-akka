# Acknowledgements

This project is a port of
**[deepseek-ai/deepseek-harness](https://github.com/deepseek-ai/deepseek-harness)**.

## The original

**Licence: MIT. Copyright (c) 2026 DeepSeek.** Read from the repository's own `LICENSE`
file at commit time, not inferred from a badge. A verbatim copy of it is in this
repository as `LICENSE-deepseek-harness`.

The vendored event system the dispatch half is a port of,
`vendor/cordis/`, is [cordiverse/cordis](https://github.com/cordiverse/cordis), also MIT,
and is redistributed inside the harness under that licence.

## What was copied

**No source was copied.** Not a file, not a function, not a line. Every Java source in this
repository was written for it.

Two categories of text are shared and are named here because "no source" is a claim about
source and not about everything:

- **Event names and rejection codes** — `session/event`, `session/created`,
  `session/disposed`, `session/flush`, `session/end-seed`, `turn/start`, `turn/end`, and
  the fork codes `SESSION_NOT_FOUND`, `SESSION_NOT_LIVE`, `SESSION_ALREADY_EXISTS`,
  `INVALID_BOUNDARY`, `OPEN_TURN`. These are the wire vocabulary of the behaviour being
  reproduced; a port that renamed them would not be a port of it. Short identifiers, taken
  deliberately.
- **The benchmark's workload names**, which are this project's own.

No prompt, fixture, schema, test corpus or test file was copied. The probes under
`deepseek-harness-port/probes/` in the harness repository *import* the original's modules
in order to run them, and are themselves original.

## What this licence forces

Nothing beyond MIT's terms, because nothing MIT-licensed was copied in. This project is
released under **MIT** in any case — see `LICENSE` — which is compatible with the original
either way, and the original's notice is preserved alongside it.

## Behaviour derived even where no text was copied

**Yes, and that is the whole point.** Every rule this project implements was learned by
reading and then *running* `deepseek-ai/deepseek-harness`, and the record of which claim
was established how is in the harness repository at
`deepseek-harness-port/docs/question-log.md`. The behaviour of the five dispatch modes, the
bail stop rule, listener placement and lifetime, sequence assignment, payload refusal,
publication containment, the creation/disposal pairing, the end-of-seed marker and the five
fork refusals are all reproductions of decisions DeepSeek's engineers made. Where this port
does something the original does not, it says so in `README.md` under
`Where it differs from deepseek-ai/deepseek-harness`, and says it as this project's own
decision rather than as a correction of theirs.

## Also used

- **[Akka](https://akka.io)** — the Akka SDK for Java (`io.akka:akka-javasdk-parent`,
  Business Source Licence 1.1), which provides the event-sourced entity, the HTTP endpoint
  and the test kit.
- **Jackson**, **JUnit 5**, **AssertJ**, via the Akka SDK parent, under their own licences.
