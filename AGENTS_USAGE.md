# Multi-Agent Development Framework

> Portable multi-agent workflow for Claude Code projects.
> Drop `.claude/agents/` into any project and start working.

---

## Setup

```bash
# Copy agents to your project
cp -r agents/ your-project/.claude/agents/

# Configure your project
# Edit CLAUDE.md with project-specific conventions
# Create docs/BRIEF.md with project requirements

cd your-project
claude
```

---

## 1. The Standard Cycle

Every project follows the same workflow:

```
briefer → architect → data-analyst → [developer → qa-security → validator] × N parts
```

### Step 0 — Define the Project (once)
```
/agents briefer
> "I want to build [describe your project]"
```
The briefer interviews you and generates `docs/BRIEF.md`.

### Step 1 — Plan (once, then revise as needed)
```
/agents architect
> "Read docs/BRIEF.md and generate the plan"
```
The architect creates `PLAN.md` (parts breakdown), `STATE.md` (progress tracking), `CORRECTIONS.md` (empty backlog), and the `REPORTS/` directory.

### Step 1.5 — Explore & Document Data (once data is accessible, update as needed)
```
/agents data-analyst
> "Explore the data and generate DATA_SOURCES.md"
```
The data-analyst profiles all source tables, verifies join keys, generates the ERD, and produces `docs/DATA_SOURCES.md` and `docs/DATA_DICTIONARY.md`.

### Step 2 — Build a Part
```
/agents developer
> "Build part 1"
```
The developer reads the plan, checks dependencies, implements, tests, generates `REPORTS/dev-part-1.md`.

### Step 3 — Audit the Part
```
/agents qa-security
> "Audit part 1"
```
QA writes independent tests, checks security and data quality, generates `REPORTS/qa-part-1.md`.

### Step 4 — Validate the Part
```
/agents validator
> "Validate part 1"
```
The validator runs integration tests, checks regressions, updates `PLAN.md` and `STATE.md`.

### Step 5 — Repeat
```
/agents developer
> "Build part 2"
```

---

## 2. Periodic Maintenance

### Every 3 Validated Parts — Refactoring
```
/agents tech-lead
> "Refactoring after parts 1 through 3"
```

### After Refactoring — Documentation
```
/agents documentalist
> "Update docs"
```

---

## 3. When Things Go Wrong

### QA Rejects a Part
```
/agents developer
> "Fix part 3"
```
Developer reads `CORRECTIONS.md`, fixes, then back to QA:
```
/agents qa-security
> "Re-audit part 3"
```

### 3 Rejections on the Same Part — Escalate
```
/agents architect
> "Part 3 blocked after 3 attempts, revise the plan"
```

### Validator Finds Regressions
```
/agents developer
> "Fix regressions from part 4"
```
Then full cycle: qa-security → validator.

---

## 4. Modify the Plan

```
/agents architect
> "Revise the plan: [add/remove/split/reorder]"
```

---

## 5. Check Project State

```bash
cat STATE.md          # Where are we?
cat CORRECTIONS.md    # Pending bugs?
cat PLAN.md           # Full plan with statuses
ls REPORTS/           # List all reports
cat REPORTS/dev-part-3.md   # Read a specific report
```

---

## 6. Agent Reference

| Situation | Agent | Command |
|-----------|-------|---------|
| Define a new project | `briefer` | "I want to build [X]" |
| Start / revise the plan | `architect` | "Read the brief and generate the plan" |
| Explore & document data | `data-analyst` | "Explore the data and generate DATA_SOURCES.md" |
| Update data docs after changes | `data-analyst` | "Update DATA_SOURCES.md after new tables loaded" |
| Validate data quality | `data-analyst` | "Validate data quality before dashboard" |
| Build a part | `developer` | "Build part N" |
| Fix a part | `developer` | "Fix part N" |
| Test / audit a part | `qa-security` | "Audit part N" |
| Re-test after fix | `qa-security` | "Re-audit part N" |
| Global security audit | `qa-security` | "Global security audit" |
| Validate a part | `validator` | "Validate part N" |
| Refactoring | `tech-lead` | "Refactoring after parts X to Y" |
| Optimize performance | `tech-lead` | "Optimize performance" |
| Update documentation | `documentalist` | "Update docs" |
| Final documentation | `documentalist` | "Full documentation for submission" |
| Part blocked 3+ times | `architect` | "Part N is blocked, revise the plan" |

---

## 7. Model Selection per Agent

| Agent | Recommended Model | Why |
|-------|-------------------|-----|
| `briefer` | Opus | Needs to understand vague requirements deeply |
| `architect` | Opus | Complex reasoning, strategic decomposition |
| `data-analyst` | Sonnet | SQL profiling, schema documentation, ERD generation |
| `developer` | Sonnet | Fast, competent at code/SQL/infra |
| `qa-security` | Sonnet | Systematic tests and checklists |
| `validator` | Sonnet | Verification and integration |
| `tech-lead` | Sonnet | Refactoring, optimization |
| `documentalist` | Sonnet | Technical writing |

**Rule: Opus for thinking, Sonnet for doing.**

---

## 8. Session Management

### Rule: 1 agent = 1 fresh session

```bash
# Session 1: build part 2
claude
> /agents developer
> "Build part 2"
# ... dev finishes ...
# /clear or exit

# Session 2: audit part 2
claude
> /agents qa-security
> "Audit part 2"
# /clear or exit

# Session 3: validate part 2
claude
> /agents validator
> "Validate part 2"
```

**Why?** Each agent reads STATE.md and PLAN.md fresh. A polluted context from a previous agent causes role confusion.

**Exception:** Minor follow-ups within the same agent role — no need to restart.

---

## 9. Coordination Files

| File | Purpose | Created By | Updated By | Read By |
|------|---------|------------|------------|---------|
| `CLAUDE.md` | Project conventions | You (manual) | You (manual) | All agents |
| `docs/BRIEF.md` | Requirements | Briefer | Briefer (revision) | Architect |
| `PLAN.md` | Development plan | Architect | Architect, Validator | All agents |
| `STATE.md` | Live progress | Architect | All agents (after their step) | All agents |
| `CORRECTIONS.md` | Bug backlog | Architect (empty) | QA, Validator (add), Developer (fix), Tech Lead (clear) | Developer, Tech Lead |
| `docs/DATA_SOURCES.md` | Data reference | Data Analyst | Data Analyst (update) | Developer, QA, Documentalist |
| `REPORTS/` | Agent reports | Architect (mkdir) | All agents | Next agent in cycle |

---

## 10. Golden Rules

1. **Always check STATE.md before working** — know where you are
2. **One agent at a time** — never mix roles
3. **Respect the order** — dev → qa → validator, never out of order
4. **Commit after each validated step** — `git commit -m "feat(part-N): [description]"`
5. **3 rejections max** — escalate to architect after 3 failures
6. **Never modify a validated part** — only via tech-lead (refactoring)
7. **Developer only codes what's in PLAN.md** — no freelancing

---

## 11. Commit Convention

```bash
feat(part-1): docker compose + postgresql setup
fix(part-3): missing index on filtered column
refactor: extract shared SQL patterns into macros
docs: update README and DATA_DICTIONARY after part 4
chore: revise PLAN.md — split part 5 into 5a and 5b
```

---

## 12. Adapting to Your Project

To use this framework in a new project:

1. Copy `.claude/agents/` to your project root
2. Write `CLAUDE.md` with your project's conventions (stack, naming, language)
3. Run `/agents briefer` to generate `docs/BRIEF.md`
4. Run `/agents architect` to generate `PLAN.md` and `STATE.md`
5. Start the dev → qa → validate cycle

The agents read project-specific conventions from `CLAUDE.md` and requirements from `docs/BRIEF.md`. The agent files themselves never need editing.
