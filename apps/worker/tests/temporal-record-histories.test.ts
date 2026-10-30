import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import {
  getWorkflowBundles,
  saveHistoryFixture,
  defaultTestWorkerOptions,
  type WorkflowBundles
} from "./support/temporal-test-harness.js";
import {
  MEMORY_MAINTENANCE_TASK_QUEUE
} from "../src/memory-maintenance-config.js";
import {
  MEMORY_MUTATION_TASK_QUEUE
} from "../src/memory-mutation-config.js";

describe("Temporal History Recording Suite", { timeout: 60_000 }, () => {
  let testEnv: TestWorkflowEnvironment;
  let bundles: WorkflowBundles;

  beforeAll(async () => {
    testEnv = await TestWorkflowEnvironment.createTimeSkipping();
    bundles = await getWorkflowBundles();
  }, 120_000);

  afterAll(async () => {
    await new Promise((r) => setTimeout(r, 1000));
    if (testEnv) await testEnv.teardown();
  });

  it("records dummyTurnWorkflow execution history", async () => {
    const taskQueue = "tq-dummy-record";
    const worker = await Worker.create({
      connection: testEnv.nativeConnection,
      taskQueue,
      workflowBundle: bundles.mainBundle,
      ...defaultTestWorkerOptions(),
      activities: {
        recordDummyTurnStarted: async () => {},
        recordDummyTurnCompleted: async () => ({ status: "COMPLETED", message: "Recorded" })
      }
    });

    const workflowId = "history:dummy-turn";
    const input = {
      prompt: "Record dummy turn history",
      roomId: "room:record-1",
      paneId: "pane:record-1",
      agentRunId: "agent_run:record-1",
      traceId: "trace:record-1",
      artifactIds: [],
      collaborationMode: "default",
      permissionMode: null
    };

    const handle = await testEnv.client.workflow.start("dummyTurnWorkflow", {
      workflowId,
      taskQueue,
      args: [input]
    });

    await worker.runUntil(handle.result());
    const history = await handle.fetchHistory();
    saveHistoryFixture("dummy-turn-workflow.json", history);
    expect(history.events?.length).toBeGreaterThan(0);
  });

  it("records codexAppServerTurnWorkflow execution history", async () => {
    const taskQueue = "tq-codex-turn-record";
    const worker = await Worker.create({
      connection: testEnv.nativeConnection,
      taskQueue,
      workflowBundle: bundles.mainBundle,
      ...defaultTestWorkerOptions(),
      activities: {
        runCodexAppServerTurn: async () => ({ status: "COMPLETED", message: "Codex turn finished" })
      }
    });

    const workflowId = "history:codex-turn";
    const input = {
      prompt: "Record codex turn history",
      roomId: "room:record-2",
      paneId: "pane:record-2",
      agentRunId: "agent_run:record-2",
      traceId: "trace:record-2",
      artifactIds: [],
      collaborationMode: "default",
      permissionMode: null
    };

    const handle = await testEnv.client.workflow.start("codexAppServerTurnWorkflow", {
      workflowId,
      taskQueue,
      args: [input]
    });

    await worker.runUntil(handle.result());
    const history = await handle.fetchHistory();
    saveHistoryFixture("codex-app-server-turn-workflow.json", history);
    expect(history.events?.length).toBeGreaterThan(0);
  });

  it("records dotGoalWorkflow execution history", async () => {
    const taskQueue = "tq-dot-goal-record";
    const worker = await Worker.create({
      connection: testEnv.nativeConnection,
      taskQueue,
      workflowBundle: bundles.mainBundle,
      ...defaultTestWorkerOptions(),
      activities: {
        dotGoalStep: async () => ({ state: "DONE", runId: null, goalStatus: "SUCCEEDED" }),
        dotGoalObserve: async () => ({ run: "COMPLETED", goalStatus: "SUCCEEDED" })
      }
    });

    const workflowId = "history:dot-goal";
    const handle = await testEnv.client.workflow.start("dotGoalWorkflow", {
      workflowId,
      taskQueue,
      args: [{ goalId: "goal:record-1" }]
    });

    await worker.runUntil(handle.result());
    const history = await handle.fetchHistory();
    saveHistoryFixture("dot-goal-workflow.json", history);
    expect(history.events?.length).toBeGreaterThan(0);
  });

  it("records roomAgentSupervisorWorkflow current branch execution history", async () => {
    const taskQueue = "tq-room-supervisor-record";
    const worker = await Worker.create({
      connection: testEnv.nativeConnection,
      taskQueue,
      workflowBundle: bundles.mainBundle,
      ...defaultTestWorkerOptions(),
      activities: {
        checkRoomAgentSupervisorTurn: async () => "SKIP",
        finishRoomAgentSupervisorTurn: async () => {},
        discardRoomAgentSupervisorTurn: async () => {}
      }
    });

    const workflowId = "history:room-supervisor";
    const item = {
      missionId: "mission:rec-sup-1",
      turn: {
        roomId: "room:rec-sup-1",
        paneId: "pane:rec-sup-1",
        prompt: "Supervisor record turn",
        agentRunId: "agent_run:rec-sup-1",
        traceId: "trace:rec-sup-1",
        artifactIds: [],
        collaborationMode: "default",
        permissionMode: null
      }
    };

    // Setting processedCount to 50 triggers continueAsNew right after processing
    const handle = await testEnv.client.workflow.start("roomAgentSupervisorWorkflow", {
      workflowId,
      taskQueue,
      args: [{
        roomId: "room:rec-sup-1",
        pending: [item],
        interrupted: [],
        monitoringMissionIds: [],
        processedCount: 49
      }]
    });

    // Run until the continue-as-new transition completes the first run
    await worker.runUntil(async () => {
      await new Promise((r) => setTimeout(r, 2000));
    });

    const history = await handle.fetchHistory();
    saveHistoryFixture("room-agent-supervisor-workflow.json", history);
    expect(history.events?.length).toBeGreaterThan(0);
  });

  it("records roomAgentSupervisorWorkflow legacy branch execution history", async () => {
    const taskQueue = "tq-room-supervisor-legacy-record";
    const legacyBundleCode = bundles.mainBundle.code.replace(
      'patched("room-background-continuation-v1")',
      'false'
    );
    const legacyBundle = { code: legacyBundleCode };

    const worker = await Worker.create({
      connection: testEnv.nativeConnection,
      taskQueue,
      workflowBundle: legacyBundle,
      ...defaultTestWorkerOptions(),
      activities: {
        markRoomAgentMissionStarted: async () => {},
        markRoomAgentMissionFinished: async () => {},
        markRoomAgentMissionContinued: async () => {},
        settleRoomAgentBackgroundMission: async () => true,
        runCodexAppServerTurn: async () => ({ status: "COMPLETED", message: "Legacy supervisor done" })
      }
    });

    const workflowId = "history:room-supervisor-legacy";
    const item = {
      missionId: "mission:rec-leg-1",
      turn: {
        roomId: "room:rec-leg-1",
        paneId: "pane:rec-leg-1",
        prompt: "Legacy supervisor turn",
        agentRunId: "agent_run:rec-leg-1",
        traceId: "trace:rec-leg-1",
        artifactIds: [],
        collaborationMode: "default",
        permissionMode: null
      }
    };

    const handle = await testEnv.client.workflow.start("roomAgentSupervisorWorkflow", {
      workflowId,
      taskQueue,
      args: [{
        roomId: "room:rec-leg-1",
        pending: [item],
        interrupted: [],
        monitoringMissionIds: [],
        processedCount: 49
      }]
    });

    await worker.runUntil(async () => {
      await new Promise((r) => setTimeout(r, 2000));
    });

    const history = await handle.fetchHistory();
    saveHistoryFixture("room-agent-supervisor-workflow-legacy.json", history);
    expect(history.events?.length).toBeGreaterThan(0);
  });

  it("records roomAgentActionWorkflow execution history", async () => {
    const taskQueue = "tq-room-action-record";
    const worker = await Worker.create({
      connection: testEnv.nativeConnection,
      taskQueue,
      workflowBundle: bundles.mainBundle,
      ...defaultTestWorkerOptions(),
      activities: {
        runRoomAgentBackgroundAction: async () => ({ status: "COMPLETED" }),
        failRoomAgentBackgroundAction: async () => {}
      }
    });

    const workflowId = "history:room-action";
    const handle = await testEnv.client.workflow.start("roomAgentActionWorkflow", {
      workflowId,
      taskQueue,
      args: [{
        bridge: {
          roomId: "room:rec-act-1",
          missionId: "mission:rec-act-1"
        },
        traceId: "trace:rec-act-1",
        actionId: "action:rec-act-1"
      }]
    });

    await worker.runUntil(handle.result());
    const history = await handle.fetchHistory();
    saveHistoryFixture("room-agent-action-workflow.json", history);
    expect(history.events?.length).toBeGreaterThan(0);
  });

  it("records memoryMaintenanceWorkflow execution history", async () => {
    const worker = await Worker.create({
      connection: testEnv.nativeConnection,
      taskQueue: MEMORY_MAINTENANCE_TASK_QUEUE,
      workflowBundle: bundles.memoryMaintenanceBundle,
      ...defaultTestWorkerOptions(),
      activities: {
        refreshMemoryGraphSnapshot: async () => ({ sourceHash: "hash-maint-1", status: "COMPLETED" }),
        refreshAllMonthsMemoryGraphSnapshot: async () => {},
        reconcileStaleSpaceAgentSessions: async () => {},
        createScheduledMemoryAudit: async () => ({ id: "audit:maint-1", workflowId: "history:child-consolidation" }),
        prepareMemoryConsolidation: async () => ({ changeSetId: null, run: { id: "run:maint-1", status: "COMPLETED" } })
      }
    });

    const workflowId = "history:memory-maintenance";
    const handle = await testEnv.client.workflow.start("memoryMaintenanceWorkflow", {
      workflowId,
      taskQueue: MEMORY_MAINTENANCE_TASK_QUEUE,
      args: [{
        traceId: "trace:maint-rec-1",
        months: ["2026-10"],
        scope: "all"
      }]
    });

    await worker.runUntil(handle.result());
    const history = await handle.fetchHistory();
    saveHistoryFixture("memory-maintenance-workflow.json", history);
    expect(history.events?.length).toBeGreaterThan(0);
  });

  it("records memoryConsolidationWorkflow execution history", async () => {
    const taskQueue = "tq-memory-consolidation-record";
    const worker = await Worker.create({
      connection: testEnv.nativeConnection,
      taskQueue,
      workflowBundle: bundles.memoryMaintenanceBundle,
      ...defaultTestWorkerOptions(),
      activities: {
        prepareMemoryConsolidation: async () => ({
          changeSetId: null,
          run: { id: "run:rec-cons-1", status: "COMPLETED" }
        })
      }
    });

    const workflowId = "history:memory-consolidation";
    const handle = await testEnv.client.workflow.start("memoryConsolidationWorkflow", {
      workflowId,
      taskQueue,
      args: [{
        runId: "run:rec-cons-1",
        traceId: "trace:rec-cons-1"
      }]
    });

    await worker.runUntil(handle.result());
    const history = await handle.fetchHistory();
    saveHistoryFixture("memory-consolidation-workflow.json", history);
    expect(history.events?.length).toBeGreaterThan(0);
  });

  it("records memoryMutationWorkflow execution history", async () => {
    const taskQueue = MEMORY_MUTATION_TASK_QUEUE;
    const worker = await Worker.create({
      connection: testEnv.nativeConnection,
      taskQueue,
      workflowBundle: bundles.memoryMutationBundle,
      ...defaultTestWorkerOptions(),
      activities: {
        executePersistedMemoryChangeSet: async () => ({ status: "APPLIED", mutationsCount: 1 })
      }
    });

    const workflowId = "history:memory-mutation";
    const handle = await testEnv.client.workflow.start("memoryMutationWorkflow", {
      workflowId,
      taskQueue,
      args: [{
        changeSetId: "cs:rec-mut-1",
        traceId: "trace:rec-mut-1"
      }]
    });

    await worker.runUntil(handle.result());
    const history = await handle.fetchHistory();
    saveHistoryFixture("memory-mutation-workflow.json", history);
    expect(history.events?.length).toBeGreaterThan(0);
  });
});
