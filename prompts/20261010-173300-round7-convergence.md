---
session: "round7-convergence"
timestamp: "2026-10-10T17:33:00Z"
model: claude-opus-4-6
sequence: 1
---

## Human

Continue improving robotocore toward the standing goal (adversarial review
cannot find further improvement).

## Assistant

## Key decisions

**Round-7 convergence batch.** A live-verified review of the merged main
(4e743565) confirmed the round-6 capacity-scoping works, but caught four
real defects, two of them in my own round-6 code:

1. **UpdateFunctionCode shape mismatch**: botocore's
   `UpdateFunctionCodeRequest` carries a top-level `ZipFile` (the round-6
   edit read `(spec.get("Code") or {}).get("ZipFile)`), so inline updates
   bypassed size accounting *and* the ledger. Both shapes accepted now, and
   the size swap (remove-prefix → add → record) bookends the backend call,
   so a failed update no longer drops the old tracked size.
2. **Create rollback**: `CreateFunction` ran `backend.create_function`
   before accounting the bytes; a rejected create left a phantom live
   function with the account already pinned. Rollback deletes the phantom.
3. **State import name validation**: `?name=../../x` import previously
   wrote outside the state directory (save/load validated; import was
   missed).
4. **Dashboard snapshot XSS**: `esc()` missed single quotes, breaking out
   of the single-quoted JS strings in inline onclick handlers.

The lambda malformed-input 500→400 mappings deferred from round-5 are
fix-listed for the next cycle.
