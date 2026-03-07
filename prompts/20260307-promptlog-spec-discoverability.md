---
role: human
timestamp: "2026-03-07T00:00:00Z"
session: "promptlog-spec-review"
sequence: 1
---

Look at recent additions to our ./prompts/ dir. Do they communicate the intent of the promptlog skill? If not, what should we improve about the skill

---
role: assistant
model: claude-sonnet-4-6
timestamp: "2026-03-07T00:01:00Z"
session: "promptlog-spec-review"
sequence: 2
---

## Assessment and fixes

### What's working
The best assistant entries use "Key decisions" framing with actual reasoning (e.g., `20260307-201600-assistant-batch-registration.md`). Human entries are consistently good.

### The gap
Recent assistant entries from the `cleanup-gap-analysis` session (`220100`, `220500`) read like commit messages — bullet lists of what changed. The spec says assistant entries are the most valuable part of the log, but only if they explain *why*, not just *what*.

The spec had no example of a good assistant entry, which left agents to infer the format. Naturally they defaulted to change-log style.

### Discoverability fix
CLAUDE.md referenced the spec as `~/www/jackdanger.com/static/promptlog.md` — an absolute path that's meaningless to any agent not on this specific machine. Updated to `prompts/PROMPTLOG.md` (in-repo relative path).

### Spec improvement
Added "What a good assistant entry looks like" subsection with a concrete good/bad contrast, naming the anti-pattern explicitly: "That belongs in a commit message. The prompt log is for the reasoning behind those changes."

Also updated the source-of-truth copy at `~/www/jackdanger.com/static/promptlog.md`.

---
role: human
timestamp: "2026-03-07T00:02:00Z"
session: "promptlog-spec-review"
sequence: 3
---

yes

---
role: human
timestamp: "2026-03-07T00:03:00Z"
session: "promptlog-spec-review"
sequence: 4
---

git commit and push (and record this prompt, lol)
