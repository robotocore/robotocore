# Plan Review Checklist

Use this checklist before executing any multi-phase implementation plan.

## 1. Verify Claims Against Code

- [ ] **Read the actual files** mentioned in the plan (don't trust line number references)
- [ ] **Verify bugs exist** — does the code actually have the problem described?
- [ ] **Check for similar bugs** — if file A has a bug, do files B/C have the same pattern?
- [ ] **Verify assumptions** — are factual claims about behavior correct?

## 2. Validate Impact Estimates

- [ ] **Question percentages** — are "20-30%" claims based on data or speculation?
- [ ] **Measure before fixing** — can we quantify the problem first?
- [ ] **Identify highest-impact items** — which fixes give 80% of the benefit?

## 3. Check for Over-Engineering

- [ ] **Is the problem real?** — does the issue actually occur in practice?
- [ ] **Is simpler sufficient?** — can we solve this with less code?
- [ ] **Who uses this?** — if a tool is rarely used, complex fixes aren't justified
- [ ] **Does existing code already handle it?** — check for timestamp isolation, filters, etc.

## 4. Verify Dependencies

- [ ] **Can phases run independently?** — or does Phase 3 require Phase 1?
- [ ] **What's the minimum viable fix?** — identify the smallest useful change
- [ ] **Are there missing files?** — did the plan miss files with the same bug?

## 5. Reduce Scope

- [ ] **Cut speculative phases** — if impact is uncertain, defer it
- [ ] **Merge similar fixes** — combine changes to related files
- [ ] **Remove meta-tooling** — validators for validators add maintenance burden
- [ ] **Defer "nice to have"** — focus on fixes that unblock real work

## 6. Add Measurement Points

- [ ] **Before/after metrics** — how do we know it worked?
- [ ] **Quick validation commands** — what can we run to verify each phase?
- [ ] **Stop conditions** — when do we know we're done?

## 7. Sequence for Fast Feedback

- [ ] **Highest-value first** — do the most impactful fix early
- [ ] **Measure after each phase** — don't batch all fixes then measure once
- [ ] **Ship incremental commits** — don't wait for all phases to commit

## 8. Final Sanity Check

- [ ] **Can we explain the fix in one sentence?** — if not, it's too complex
- [ ] **Would we write this from scratch?** — or are we just following the plan?
- [ ] **What's the blast radius?** — how many files change, how much risk?
