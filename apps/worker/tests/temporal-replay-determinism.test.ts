import { describe, it, expect, beforeAll } from "vitest";
import { Worker } from "@temporalio/worker";
import { DeterminismViolationError } from "@temporalio/workflow";
import {
  getWorkflowBundles,
  loadHistoryFixture,
  type WorkflowBundles
} from "./support/temporal-test-harness.js";

describe("Temporal Workflow Replay Determinism Gate", { timeout: 60_000 }, () => {
  let bundles: WorkflowBundles;

  beforeAll(async () => {
    bundles = await getWorkflowBundles();
  }, 60_000);

  it("replays dummyTurnWorkflow deterministically", async () => {
    const history = loadHistoryFixture("dummy-turn-workflow.json");
    await expect(
      Worker.runReplayHistory(
        { workflowBundle: bundles.mainBundle },
        history,
        "history:dummy-turn"
      )
    ).resolves.toBeUndefined();
  });

  it("replays codexAppServerTurnWorkflow deterministically", async () => {
    const history = loadHistoryFixture("codex-app-server-turn-workflow.json");
    await expect(
      Worker.runReplayHistory(
        { workflowBundle: bundles.mainBundle },
        history,
        "history:codex-turn"
      )
    ).resolves.toBeUndefined();
  });

  it("replays dotGoalWorkflow deterministically", async () => {
    const history = loadHistoryFixture("dot-goal-workflow.json");
    await expect(
      Worker.runReplayHistory(
        { workflowBundle: bundles.mainBundle },
        history,
        "history:dot-goal"
      )
    ).resolves.toBeUndefined();
  });

  it("replays roomAgentSupervisorWorkflow (current branch) deterministically", async () => {
    const history = loadHistoryFixture("room-agent-supervisor-workflow.json");
    await expect(
      Worker.runReplayHistory(
        { workflowBundle: bundles.mainBundle },
        history,
        "history:room-supervisor"
      )
    ).resolves.toBeUndefined();
  });

  it("replays roomAgentSupervisorWorkflow (legacy pre-patch branch) deterministically", async () => {
    const history = loadHistoryFixture("room-agent-supervisor-workflow-legacy.json");
    // Current workflow code has patched("room-background-continuation-v1").
    // Replaying legacy history (without patch marker) must take legacyRoomAgentSupervisorWorkflow and pass!
    await expect(
      Worker.runReplayHistory(
        { workflowBundle: bundles.mainBundle },
        history,
        "history:room-supervisor-legacy"
      )
    ).resolves.toBeUndefined();
  });

  it("replays roomAgentActionWorkflow deterministically", async () => {
    const history = loadHistoryFixture("room-agent-action-workflow.json");
    await expect(
      Worker.runReplayHistory(
        { workflowBundle: bundles.mainBundle },
        history,
        "history:room-action"
      )
    ).resolves.toBeUndefined();
  });

  it("replays memoryMaintenanceWorkflow deterministically", async () => {
    const history = loadHistoryFixture("memory-maintenance-workflow.json");
    await expect(
      Worker.runReplayHistory(
        { workflowBundle: bundles.memoryMaintenanceBundle },
        history,
        "history:memory-maintenance"
      )
    ).resolves.toBeUndefined();
  });

  it("replays memoryConsolidationWorkflow deterministically", async () => {
    const history = loadHistoryFixture("memory-consolidation-workflow.json");
    await expect(
      Worker.runReplayHistory(
        { workflowBundle: bundles.memoryMaintenanceBundle },
        history,
        "history:memory-consolidation"
      )
    ).resolves.toBeUndefined();
  });

  it("replays memoryMutationWorkflow deterministically", async () => {
    const history = loadHistoryFixture("memory-mutation-workflow.json");
    await expect(
      Worker.runReplayHistory(
        { workflowBundle: bundles.memoryMutationBundle },
        history,
        "history:memory-mutation"
      )
    ).resolves.toBeUndefined();
  });

  it("replays batch histories using Worker.runReplayHistories", async () => {
    const mainHistories = [
      { history: loadHistoryFixture("dummy-turn-workflow.json"), workflowId: "history:dummy-turn" },
      { history: loadHistoryFixture("codex-app-server-turn-workflow.json"), workflowId: "history:codex-turn" },
      { history: loadHistoryFixture("dot-goal-workflow.json"), workflowId: "history:dot-goal" },
      { history: loadHistoryFixture("room-agent-supervisor-workflow.json"), workflowId: "history:room-supervisor" },
      { history: loadHistoryFixture("room-agent-supervisor-workflow-legacy.json"), workflowId: "history:room-supervisor-legacy" },
      { history: loadHistoryFixture("room-agent-action-workflow.json"), workflowId: "history:room-action" }
    ];

    for await (const result of Worker.runReplayHistories(
      { workflowBundle: bundles.mainBundle },
      mainHistories
    )) {
      expect(result.error).toBeUndefined();
    }
  });

  it("negative test: detects determinism violation when workflow code alters activity sequence", async () => {
    const history = loadHistoryFixture("dummy-turn-workflow.json");

    // Alter activity execution sequence so replay encounters unexpected activity
    const mutatedCode = bundles.mainBundle.code.replace(
      "await recordDummyTurnStarted(input);",
      "await recordDummyTurnCompleted(input); await recordDummyTurnStarted(input);"
    );
    const nonDeterministicBundle = { code: mutatedCode };

    let thrown: any = null;
    try {
      await Worker.runReplayHistory(
        { workflowBundle: nonDeterministicBundle },
        history,
        "history:dummy-turn"
      );
    } catch (err) {
      thrown = err;
    }
    expect(thrown).not.toBeNull();
    const isDeterminismError =
      thrown instanceof DeterminismViolationError ||
      thrown?.name === "DeterminismViolationError" ||
      thrown?.message?.includes("nondeterminism") ||
      thrown?.message?.includes("Nondeterminism") ||
      thrown?.message?.includes("DeterminismViolationError") ||
      thrown?.message?.includes("Workflow activation completion failed");
    expect(isDeterminismError).toBe(true);
  });
});
