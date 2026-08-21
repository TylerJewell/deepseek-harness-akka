# deepseek-harness-akka

Delivers one event to a list of listeners in five different ways, and keeps a numbered,
append-only record of everything that happens in a conversation.

A port of
[deepseek-ai/deepseek-harness](https://github.com/deepseek-ai/deepseek-harness) onto
**Akka**, built with **Akka Specify**.

---

## Where it came from

deepseek-ai/deepseek-harness is a program for running an AI assistant, built so that every
part of it is a plug-in that can be added or removed while it runs. It was ported to derive
a specification format precise enough to regenerate a system on a different stack — the
port is the vehicle, the specification is the deliverable.

Two of its parts are rebuilt here: the thing that hands an event to whoever is listening
for it, and the thing that records what happened in a conversation. Nothing else is —
no assistant, no model, no tools, no screen.

The specifications the port was generated from are in
[TylerJewell/akka-specify-harness](https://github.com/TylerJewell/akka-specify-harness)
under `deepseek-harness-port/`.

---

## What makes this port different from the others

Every earlier port rebuilt something that stores or serves data. This one rebuilds a
**calling convention** — the rules about which listeners get called, in what order, and
what makes the calling stop early. There is no data model at the centre of it. The answer
to every question is a sequence of calls, which is why the comparison against the original
compares traces of who was reached rather than values that came back, and why the one
difference the comparison found was invisible to a suite of thirty checks that only looked
at return values.

---

## deepseek-ai/deepseek-harness → this port

📉 943 TypeScript lines → **998 Java lines**<br>
📁 4 files → **19 files**<br>
⚡ 905,755 nanoseconds → **425** nanoseconds, one dispatch sequence<br>
⏱️ 1,406,146 nanoseconds → **24,030** nanoseconds, one write sequence<br>
⏱️ 2,155,510 nanoseconds → **11,668** nanoseconds, one fork sequence<br>
🎯 7 of 7 → **7 of 7** sequences answered identically<br>
🧪 not measured → **87** checks

Full method and the numbers that did *not* make this list:
[`bench/REPORT.md`](https://github.com/TylerJewell/akka-specify-harness/blob/main/deepseek-harness-port/bench/REPORT.md).

---

## What it took to build

⏱️ **1.3 hours** from the first command to the published repository, **1.1** of them active<br>
💬 **315** exchanges with the model<br>
✍️ **288,346** tokens written by the model, **76,833,534** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **87** tests

```bash
python toolkit/tokens.py --port deepseek-harness
```

The record of every question, and where the time went, is in
[`port-log/`](https://github.com/TylerJewell/akka-specify-harness/tree/main/port-log).

---

## What it does

- **The calling stops at the first listener that answers.** A listener answering with the
  number zero or an empty piece of text has answered; only nothing at all, or a flat no,
  counts as declining.
- **A listener can wrap the ones behind it.** It is handed a way to call the rest of the
  chain, and a listener that never uses it stops everything behind it, including the
  built-in behaviour at the end.
- **Unloading a plug-in removes everything it registered.** No plug-in cleans up after
  itself, and a plug-in that has been unloaded is refused if it tries to register anything
  new.
- **A plug-in waits for what it asked for.** It names the parts of the system it needs, and
  does not run until they are all there; reading one it did not name is refused, with the
  name in the message.
- **Every entry gets its number from the record, not from the caller.** The numbers run
  from zero with no gaps, and a rejected entry does not use up the next number.
- **A rejected entry is rejected at the moment it is written.** A value with no faithful
  way of being written down — a number that is not a number, a value that contains itself —
  is refused before the record changes rather than when it is later saved.
- **What is written down is a copy.** Changing the thing you handed in afterwards does not
  change what was recorded, and nothing already recorded can be altered.
- **A watcher that fails does not undo the entry.** Once the entry is in the record it is
  final: the watchers after the failing one still see it, and the writer is still told it
  worked.
- **A copy of a conversation can only be cut between turns.** Cutting partway through a
  turn is refused, and each of the five ways of asking for the wrong cut has its own name
  so the caller can tell them apart.
- **Reopening a conversation does not make it longer.** A conversation that was put away
  and picked up again has exactly the entries it had before.

---

## Design decisions

**One record per conversation.** Everything that has to stay consistent with a
conversation is the conversation's own list of entries, and nothing else needs to agree
with it. That means two conversations never wait for each other.

**Numbered entries rather than timestamps.** Two entries written in the same millisecond
would be impossible to put back in order, but counting never ties. That means anyone
reading the record can say exactly where they got to with a single number.

**Ask for what happened after a number.** A reader that loses its connection says the last
number it saw and is handed everything after it, from the record itself rather than from a
short-lived memory of recent activity. That means being disconnected for a day costs the
same as being disconnected for a second.

**A refusal carries a name, not just a complaint.** Every way of being turned away has its
own short name the caller can check for. That means a program can react to being told the
cut point is wrong differently from being told the name is taken.

**A limit on how long one conversation can get.** A record that grows forever eventually
becomes too big to copy between machines, and being told so late is worse than being told
early. That means a conversation stops accepting entries with a clear message instead of
quietly failing later.

---

## Running it — the short path

You do not need Java, Maven, or the Akka CLI installed. Akka Specify installs them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/deepseek-harness-akka into a new directory and open
> it. Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3. Open** http://localhost:9040/sessions/demo — after creating one with the command
below.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

### Start the service

```bash
mvn compile
akka local run
```

The service starts on **port 9040**.

### Use it

```bash
curl -X POST http://localhost:9040/sessions/demo
curl -X POST http://localhost:9040/sessions/demo/events \
  -H 'content-type: application/json' \
  -d '{"type":"turn/start","data":{"turn":1}}'
curl -X POST http://localhost:9040/sessions/demo/events \
  -H 'content-type: application/json' \
  -d '{"type":"turn/end","data":{"turn":1}}'
curl http://localhost:9040/sessions/demo
curl http://localhost:9040/sessions/demo/events/since/0
curl -X POST http://localhost:9040/sessions/demo/forks \
  -H 'content-type: application/json' \
  -d '{"childId":"demo-copy","boundary":1}'
```

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| none | — | The service reads no environment variables. The one setting it has, the port it listens on, is in `src/main/resources/application.conf`. |

This project calls no model provider, so it needs no key for one.

---

## Where it differs from deepseek-ai/deepseek-harness

Everything not listed here behaves the same way on purpose, including the parts that look
like mistakes.

- **What a reader misses while disconnected.** deepseek-ai/deepseek-harness hands each
  entry to watchers inside the same program, where there is no connection to lose, so it
  has never had to say what a reader that went away misses. This port serves the record
  over a network, which does lose connections, so it was given a rule: a reader names the
  last number it saw and is handed everything after it, read from the stored record rather
  than from a memory of recent activity. Chosen because the numbers already run from zero
  with no gaps, so a reader's position is a number it already has, and because a
  from-now-on reading would silently skip whatever arrived while it was away.
- **How long one conversation may get.** deepseek-ai/deepseek-harness sets no limit; its
  record lives in a program that eventually exits. This port stores records that outlive
  any program, so it was given a limit of one thousand entries, refused with the name
  `LOG_FULL`. The number comes from the size a stored record can be copied between
  machines at rather than being picked, and the refusal is visible so a caller finds out
  while it can still act.
- **A value with no faithful way of being written down, sent over the network.**
  deepseek-ai/deepseek-harness refuses such a value at the moment it is written, and this
  port does the same when the value reaches it directly. When it arrives over the network
  it cannot: the machinery that carries it turns a number that is not a number into the
  text "NaN" before this port ever sees it, so what arrives is ordinary text and is
  accepted as such. Stated rather than worked around, because the alternative would be
  guessing which pieces of text used not to be text.
- **Where a copy of a conversation is written.** deepseek-ai/deepseek-harness keeps the
  copy beside the original in the same program's memory. This port writes it as a separate
  stored record with its own name, which is what lets a copy outlive the program that made
  it — and means making one costs a write rather than nothing.
- **Which listeners a call reaches.** deepseek-ai/deepseek-harness can restrict a listener
  to events raised in part of the system, using a filter carried by whoever raised the
  event. This port has no such filter: every listener registered for an event receives it.
  Chosen because the filter belongs to the plug-in loading system, which this port does not
  rebuild, and a half-implemented filter is worse than none.
- **What a listener registration hands back.** In deepseek-ai/deepseek-harness the thing
  handed back is described as reporting whether the listener was still registered, and in
  practice reports nothing at all — the listener is removed either way. This port hands
  back something that removes the listener and reports nothing, matching what the original
  does rather than what it says.
- **Waiting for a listener that has not finished.** deepseek-ai/deepseek-harness has
  listeners that finish later, and two of its five ways of calling exist to wait for them.
  In this port a listener is an ordinary call that has already finished when it returns, so
  the two waiting ways and their immediate counterparts behave identically. Both are kept
  separate anyway, because where the calling stops is what a caller depends on, and a
  listener that starts finishing later must not quietly move it.
- **Failures from several listeners at once.** deepseek-ai/deepseek-harness collects them
  into one combined failure. This port attaches every one of them to a single failure it
  raises, in listener order, which is the nearest thing the language it is written in
  offers.
- **How long a conversation stays available.** deepseek-ai/deepseek-harness keeps a
  conversation only while the plug-in that made it is loaded, and forgets it when that
  plug-in is unloaded. This port also stores every conversation permanently, so the stored
  copy survives a restart while the in-memory one does not. Both behaviours are present and
  are reached through different parts of the code.
- **Speed under many callers at once.** `not checked`. Everything measured was one caller
  at a time; neither system was put under load, and nothing here should be read as a claim
  about what happens when they are.
- **What happens when the machine runs out of memory or disk.** `not checked` on both
  sides.

---

## Licence

deepseek-ai/deepseek-harness is MIT, © 2026 DeepSeek. This port reimplements the behaviour
without copied source; see [`ACKNOWLEDGEMENTS.md`](ACKNOWLEDGEMENTS.md).
