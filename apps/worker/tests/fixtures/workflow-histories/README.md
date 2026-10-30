# Workflow Execution History Fixtures & Determinism Replay Gate

This directory contains recorded execution histories for all Temporal workflows in SpaceApp (`apps/worker/src/workflows.ts`, `apps/worker/src/memory-maintenance-workflows.ts`, and `apps/worker/src/memory-mutation-workflows.ts`).

## Purpose

These fixtures gate replay determinism in CI and local test suites (`npm test -w @space/worker`).
When any workflow code is modified, the test runner replays these recorded histories against the updated workflow definitions using `Worker.runReplayHistories`.
If a workflow modification introduces non-deterministic changes (e.g. adding unpatched timers, activities, or changing execution order), the replay suite fails immediately with `DeterminismViolationError`.

## Re-Recording Policy (Strict Invariant)

- **When re-recording is allowed:**
  Re-recording is **ONLY** permitted when introducing backwards-compatible changes guarded by Temporal's `patched()` versioning API, along with a corresponding test verifying the legacy execution path.
- **When re-recording is strictly FORBIDDEN:**
  **NEVER** re-record histories simply to make a failing replay test pass. Doing so breaks backwards-compatibility for existing in-flight workflow executions in production installations.

## How to Re-Record Workflow Histories

To record fresh synthetic execution histories after an approved, `patched()` workflow update:

```bash
npm run temporal:record-histories -w @space/worker
```

This script runs the full workflow matrix on Temporal's `TestWorkflowEnvironment.createTimeSkipping()`, serializes synthetic execution histories into this directory, and validates that all fixtures pass replay verification.
