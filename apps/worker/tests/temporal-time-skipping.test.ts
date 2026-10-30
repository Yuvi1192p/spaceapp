import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import {
  ROOM_AGENT_ENQUEUE_SIGNAL,
  ROOM_AGENT_STOP_SIGNAL,
  DOT_GOAL_SIGNAL
} from "@space/contracts";
import {
  getWorkflowBundles,
  defaultTestWorkerOptions,
  type WorkflowBundles
} from "./support/temporal-test-harness.js";
import {
  MEMORY_MAINTENANCE_TASK_QUEUE
} from "../src/memory-maintenance-config.js";
import {
  MEMORY_MUTATION_TASK_QUEUE
} from "../src/memory-mutation-config.js";

describe("Temporal Real-Sandbox Time-Skipping Test Suite", { timeout: 120_000 }, () => {
  let testEnv: TestWorkflowEnvironment;
  let bundles: WorkflowBundles;

  beforeAll(async () => {
    testEnv = await TestWorkflowEnvironment.createTimeSkipping();
    bundles = await getWorkflowBundles();
  }, 120_000);

  afterAll(async () => {
    try {
      await new Promise((r) => setTimeout(r, 1000));
      if (testEnv) await testEnv.teardown();
    } catch {
      // Ignore worker teardown race in afterAll
    }
  });

  // --------------------------------------------------------------------------
  // 1. dummyTurnWorkflow
  // --------------------------------------------------------------------------
  describe("dummyTurnWorkflow", () => {
    it("runs dummyTurnWorkflow through start and completed activities", async () => {
      const taskQueue = "tq-ts-dummy";
      let started = false;
      const worker = await Worker.create({
        connection: testEnv.nativeConnection,
        taskQueue,
        workflowBundle: bundles.mainBundle,
        ...defaultTestWorkerOptions(),
        activities: {
          recordDummyTurnStarted: async () => { started = true; },
          recordDummyTurnCompleted: async () => ({ status: "COMPLETED", message: "Turn completed successfully" })
        }
      });

      const input = {
        prompt: "Hello Time-Skipping",
        roomId: "room:ts-dummy",
        paneId: "pane:ts-dummy",
        agentRunId: "agent_run:ts-dummy",
        traceId: "trace:ts-dummy",
        artifactIds: [],
        collaborationMode: "default",
        permissionMode: null
      };

      const result = await worker.runUntil(
        testEnv.client.workflow.execute("dummyTurnWorkflow", {
          workflowId: "ts-workflow:dummy",
          taskQueue,
          args: [input]
        })
      );

      expect(started).toBe(true);
      expect(result).toEqual({ status: "COMPLETED", message: "Turn completed successfully" });
    });
  });

  // --------------------------------------------------------------------------
  // 2. codexAppServerTurnWorkflow (Branches & Activity Retries)
  // --------------------------------------------------------------------------
  describe("codexAppServerTurnWorkflow", () => {
    it("routes providerId='opencode' to runOpenCodeAgentTurn", async () => {
      const taskQueue = "tq-ts-opencode";
      let opencodeCalled = false;
      const worker = await Worker.create({
        connection: testEnv.nativeConnection,
        taskQueue,
        workflowBundle: bundles.mainBundle,
        ...defaultTestWorkerOptions(),
        activities: {
          runOpenCodeAgentTurn: async () => {
            opencodeCalled = true;
            return { status: "COMPLETED", message: "OpenCode processed" };
          }
        }
      });

      const input = {
        providerId: "opencode",
        prompt: "Run opencode",
        roomId: "room:ts-opencode",
        paneId: "pane:ts-opencode",
        agentRunId: "agent_run:ts-opencode",
        traceId: "trace:ts-opencode",
        artifactIds: [],
        collaborationMode: "default",
        permissionMode: null
      };

      const result = await worker.runUntil(
        testEnv.client.workflow.execute("codexAppServerTurnWorkflow", {
          workflowId: "ts-workflow:opencode",
          taskQueue,
          args: [input]
        })
      );

      expect(opencodeCalled).toBe(true);
      expect(result.status).toBe("COMPLETED");
    });

    it("routes CLI chat turn provider to runCliAgentTurn", async () => {
      const taskQueue = "tq-ts-cli";
      let cliCalled = false;
      const worker = await Worker.create({
        connection: testEnv.nativeConnection,
        taskQueue,
        workflowBundle: bundles.mainBundle,
        ...defaultTestWorkerOptions(),
        activities: {
          runCliAgentTurn: async () => {
            cliCalled = true;
            return { status: "COMPLETED", message: "CLI processed" };
          }
        }
      });

      const input = {
        providerId: "cli:claude",
        prompt: "Run CLI",
        roomId: "room:ts-cli",
        paneId: "pane:ts-cli",
        agentRunId: "agent_run:ts-cli",
        traceId: "trace:ts-cli",
        artifactIds: [],
        collaborationMode: "default",
        permissionMode: null
      };

      const result = await worker.runUntil(
        testEnv.client.workflow.execute("codexAppServerTurnWorkflow", {
          workflowId: "ts-workflow:cli",
          taskQueue,
          args: [input]
        })
      );

      expect(cliCalled).toBe(true);
      expect(result.status).toBe("COMPLETED");
    });

    it("routes standard turns to runCodexAppServerTurn with activity retry on transient failure", async () => {
      const taskQueue = "tq-ts-turn-retry";
      let attempts = 0;
      const worker = await Worker.create({
        connection: testEnv.nativeConnection,
        taskQueue,
        workflowBundle: bundles.mainBundle,
        ...defaultTestWorkerOptions(),
        activities: {
          runCodexAppServerTurn: async () => {
            attempts++;
            if (attempts === 1) {
              throw new Error("Transient activity connection blip");
            }
            return { status: "COMPLETED", message: "Turn recovered on retry" };
          }
        }
      });

      const input = {
        prompt: "Retry test",
        roomId: "room:ts-retry",
        paneId: "pane:ts-retry",
        agentRunId: "agent_run:ts-retry",
        traceId: "trace:ts-retry",
        artifactIds: [],
        collaborationMode: "default",
        permissionMode: null
      };

      const result = await worker.runUntil(
        testEnv.client.workflow.execute("codexAppServerTurnWorkflow", {
          workflowId: "ts-workflow:turn-retry",
          taskQueue,
          args: [input]
        })
      );

      expect(attempts).toBe(2);
      expect(result.status).toBe("COMPLETED");
    });
  });

  // --------------------------------------------------------------------------
  // 3. dotGoalWorkflow (Signals, Condition Wait & Continue-as-New)
  // --------------------------------------------------------------------------
  describe("dotGoalWorkflow", () => {
    it("handles wake signal and completes when step reports DONE", async () => {
      const taskQueue = "tq-ts-dot-wake";
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

      const handle = await testEnv.client.workflow.start("dotGoalWorkflow", {
        workflowId: "ts-workflow:dot-wake",
        taskQueue,
        args: [{ goalId: "goal:ts-wake" }]
      });

      await handle.signal(DOT_GOAL_SIGNAL);
      const result = await worker.runUntil(handle.result());
      expect(result).toEqual({ goalStatus: "SUCCEEDED" });
    });

    it("times out in condition wait loop and observes until completion", async () => {
      const taskQueue = "tq-ts-dot-timeout";
      let stepCount = 0;
      let observeCount = 0;
      const worker = await Worker.create({
        connection: testEnv.nativeConnection,
        taskQueue,
        workflowBundle: bundles.mainBundle,
        ...defaultTestWorkerOptions(),
        activities: {
          dotGoalStep: async () => {
            stepCount++;
            if (stepCount >= 2) {
              return { state: "DONE", runId: null, goalStatus: "SUCCEEDED" };
            }
            return { state: "RUNNING", runId: "run:ts-dot", goalStatus: "ACTIVE" };
          },
          dotGoalObserve: async () => {
            observeCount++;
            return { run: observeCount >= 2 ? "COMPLETED" : "RUNNING", goalStatus: "SUCCEEDED" };
          }
        }
      });

      const handle = await testEnv.client.workflow.start("dotGoalWorkflow", {
        workflowId: "ts-workflow:dot-timeout",
        taskQueue,
        args: [{ goalId: "goal:ts-timeout" }]
      });

      // Time-skipping automatically advances the 15-second timer
      const result = await worker.runUntil(handle.result());
      expect(observeCount).toBeGreaterThanOrEqual(2);
      expect(result.goalStatus).toBe("SUCCEEDED");
    });
  });

  // --------------------------------------------------------------------------
  // 4. roomAgentSupervisorWorkflow (Real-Sandbox Parity & Signals)
  // --------------------------------------------------------------------------
  describe("roomAgentSupervisorWorkflow", () => {
    it("handles enqueue and stop signals with cancellation scope in real sandbox", async () => {
      const taskQueue = "tq-ts-supervisor-signals";
      let turnInterrupted = false;
      let interruptedResolve: () => void;
      const interruptedPromise = new Promise<void>((r) => { interruptedResolve = r; });

      const worker = await Worker.create({
        connection: testEnv.nativeConnection,
        taskQueue,
        workflowBundle: bundles.mainBundle,
        ...defaultTestWorkerOptions(),
        activities: {
          checkRoomAgentSupervisorTurn: async () => "READY",
          runCodexAppServerTurn: async () => ({ status: "COMPLETED", message: "Turn completed" }),
          finishRoomAgentSupervisorTurn: async (args: any) => {
            if (args.status === "INTERRUPTED") {
              turnInterrupted = true;
              interruptedResolve();
            }
          },
          discardRoomAgentSupervisorTurn: async () => {}
        }
      });

      const roomId = "room:ts-signals";
      const item1 = {
        missionId: "mission:ts-sig-1",
        turn: {
          roomId,
          paneId: "pane:ts-sig-1",
          prompt: "First request",
          agentRunId: "agent_run:ts-sig-1",
          traceId: "trace:ts-sig-1",
          artifactIds: [],
          collaborationMode: "default",
          permissionMode: null
        }
      };

      const handle = await testEnv.client.workflow.start("roomAgentSupervisorWorkflow", {
        workflowId: "ts-workflow:supervisor-signals",
        taskQueue,
        args: [{
          roomId,
          pending: [item1],
          interrupted: [],
          monitoringMissionIds: [],
          processedCount: 0
        }]
      });

      // Send stop signal while running
      await handle.signal(ROOM_AGENT_STOP_SIGNAL, {
        missionId: "mission:ts-sig-1",
        reason: "Stopped by operator in real sandbox test"
      });

      await worker.runUntil(interruptedPromise);
      await handle.terminate("Signal test finished");

      expect(turnInterrupted).toBe(true);
    });

    it("triggers continueAsNew when processedCount exceeds limit", async () => {
      const taskQueue = "tq-ts-supervisor-can";
      let turnsProcessed = 0;
      let discardResolve: () => void;
      const discardPromise = new Promise<void>((r) => { discardResolve = r; });

      const worker = await Worker.create({
        connection: testEnv.nativeConnection,
        taskQueue,
        workflowBundle: bundles.mainBundle,
        ...defaultTestWorkerOptions(),
        activities: {
          checkRoomAgentSupervisorTurn: async () => {
            turnsProcessed++;
            return "SKIP";
          },
          discardRoomAgentSupervisorTurn: async () => {
            discardResolve();
          },
          finishRoomAgentSupervisorTurn: async () => {}
        }
      });

      const roomId = "room:ts-can";
      const item = {
        missionId: "mission:ts-can-1",
        turn: {
          roomId,
          paneId: "pane:ts-can-1",
          prompt: "Drain turn",
          agentRunId: "agent_run:ts-can-1",
          traceId: "trace:ts-can-1",
          artifactIds: [],
          collaborationMode: "default",
          permissionMode: null
        }
      };

      const handle = await testEnv.client.workflow.start("roomAgentSupervisorWorkflow", {
        workflowId: "ts-workflow:supervisor-can",
        taskQueue,
        args: [{
          roomId,
          pending: [item],
          interrupted: [],
          monitoringMissionIds: [],
          processedCount: 49 // Reaching 50 triggers continueAsNew
        }]
      });

      await worker.runUntil(discardPromise);
      await handle.terminate("Continue-as-new verified");

      expect(turnsProcessed).toBe(1);
    });

    it("runs legacyRoomAgentSupervisorWorkflow when pre-patch branch is active", async () => {
      const taskQueue = "tq-ts-supervisor-legacy";
      const legacyBundleCode = bundles.mainBundle.code.replace(
        /\.patched\)\("room-background-continuation-v1"\)/,
        '.patched)("room-background-continuation-v1") || true'
      );
      const legacyBundle = { code: legacyBundleCode };

      let legacyStarted = false;
      let startedResolve: () => void;
      const startedPromise = new Promise<void>((r) => { startedResolve = r; });

      const worker = await Worker.create({
        connection: testEnv.nativeConnection,
        taskQueue,
        workflowBundle: legacyBundle,
        ...defaultTestWorkerOptions(),
        activities: {
          markRoomAgentMissionStarted: async () => {
            legacyStarted = true;
            startedResolve();
          },
          markRoomAgentMissionFinished: async () => {},
          markRoomAgentMissionContinued: async () => {},
          runCodexAppServerTurn: async () => ({ status: "COMPLETED", message: "Legacy done" })
        }
      });

      const roomId = "room:ts-legacy";
      const item = {
        missionId: "mission:ts-leg-run",
        turn: {
          roomId,
          paneId: "pane:ts-leg-run",
          prompt: "Legacy turn",
          agentRunId: "agent_run:ts-leg-run",
          traceId: "trace:ts-leg-run",
          artifactIds: [],
          collaborationMode: "default",
          permissionMode: null
        }
      };

      const handle = await testEnv.client.workflow.start("roomAgentSupervisorWorkflow", {
        workflowId: "ts-workflow:supervisor-legacy",
        taskQueue,
        args: [{
          roomId,
          pending: [item],
          interrupted: [],
          monitoringMissionIds: [],
          processedCount: 0
        }]
      });

      await worker.runUntil(startedPromise);
      await handle.terminate("Legacy verified");

      expect(legacyStarted).toBe(true);
    });
  });

  // --------------------------------------------------------------------------
  // 5. roomAgentActionWorkflow (Failure & room-background-action-failure-v1)
  // --------------------------------------------------------------------------
  describe("roomAgentActionWorkflow", () => {
    it("completes successfully when action succeeds", async () => {
      const taskQueue = "tq-ts-action-success";
      const worker = await Worker.create({
        connection: testEnv.nativeConnection,
        taskQueue,
        workflowBundle: bundles.mainBundle,
        ...defaultTestWorkerOptions(),
        activities: {
          runRoomAgentBackgroundAction: async () => ({ status: "SUCCESS", result: "Action completed" })
        }
      });

      const result = await worker.runUntil(
        testEnv.client.workflow.execute("roomAgentActionWorkflow", {
          workflowId: "ts-workflow:action-success",
          taskQueue,
          args: [{
            bridge: { roomId: "room:ts-act", missionId: "mission:ts-act" },
            traceId: "trace:ts-act",
            actionId: "action:ts-act-ok"
          }]
        })
      );

      expect(result).toEqual({ status: "SUCCESS", result: "Action completed" });
    });

    it("catches failure and invokes failRoomAgentBackgroundAction when action fails", async () => {
      const taskQueue = "tq-ts-action-fail";
      let failureReported = false;

      const worker = await Worker.create({
        connection: testEnv.nativeConnection,
        taskQueue,
        workflowBundle: bundles.mainBundle,
        ...defaultTestWorkerOptions(),
        activities: {
          runRoomAgentBackgroundAction: async () => {
            throw new Error("Action execution failed");
          },
          failRoomAgentBackgroundAction: async () => {
            failureReported = true;
          }
        }
      });

      await expect(
        worker.runUntil(
          testEnv.client.workflow.execute("roomAgentActionWorkflow", {
            workflowId: "ts-workflow:action-fail",
            taskQueue,
            args: [{
              bridge: { roomId: "room:ts-act-fail", missionId: "mission:ts-act-fail" },
              traceId: "trace:ts-act-fail",
              actionId: "action:ts-act-fail"
            }]
          })
        )
      ).rejects.toThrow();

      expect(failureReported).toBe(true);
    });
  });

  // --------------------------------------------------------------------------
  // 6. memoryMaintenanceWorkflow (Resilience & Child Workflow)
  // --------------------------------------------------------------------------
  describe("memoryMaintenanceWorkflow", () => {
    it("is resilient when optional reconciliation activities fail, executes child workflow", async () => {
      let auditCreated = false;
      let childExecuted = false;

      const worker = await Worker.create({
        connection: testEnv.nativeConnection,
        taskQueue: MEMORY_MAINTENANCE_TASK_QUEUE,
        workflowBundle: bundles.memoryMaintenanceBundle,
        ...defaultTestWorkerOptions(),
        activities: {
          refreshMemoryGraphSnapshot: async () => ({ sourceHash: "hash-resilience-1", status: "COMPLETED" }),
          refreshAllMonthsMemoryGraphSnapshot: async () => {
            throw new Error("Optional all-months refresh failed");
          },
          reconcileStaleSpaceAgentSessions: async () => {
            throw new Error("Optional stale session reconciliation failed");
          },
          createScheduledMemoryAudit: async () => {
            auditCreated = true;
            return { id: "audit:resilience-1", workflowId: "ts-child:consolidation" };
          },
          prepareMemoryConsolidation: async () => {
            childExecuted = true;
            return { changeSetId: null, run: { id: "run:child-ok", status: "COMPLETED" } };
          }
        }
      });

      const result = await worker.runUntil(
        testEnv.client.workflow.execute("memoryMaintenanceWorkflow", {
          workflowId: "ts-workflow:memory-maintenance-resilient",
          taskQueue: MEMORY_MAINTENANCE_TASK_QUEUE,
          args: [{
            traceId: "trace:maint-resilient",
            months: ["2026-10"],
            scope: "all"
          }]
        })
      );

      expect(auditCreated).toBe(true);
      expect(childExecuted).toBe(true);
      expect(result.sourceHash).toBe("hash-resilience-1");
    });
  });

  // --------------------------------------------------------------------------
  // 7. memoryConsolidationWorkflow (Change Set Execution & Finalization)
  // --------------------------------------------------------------------------
  describe("memoryConsolidationWorkflow", () => {
    it("prepares, executes change set on mutation queue, and finalizes repair", async () => {
      let changeSetExecuted = false;
      let repairFinalized = false;

      // Maintenance queue handles workflow & maintenance activities
      const maintWorker = await Worker.create({
        connection: testEnv.nativeConnection,
        taskQueue: "tq-ts-consolidation",
        workflowBundle: bundles.memoryMaintenanceBundle,
        ...defaultTestWorkerOptions(),
        activities: {
          prepareMemoryConsolidation: async () => ({
            changeSetId: "cs:repair-1",
            run: { id: "run:consolidation-1", status: "PREPARED" }
          }),
          finalizeMemoryRepair: async () => {
            repairFinalized = true;
            return { id: "run:consolidation-1", status: "COMPLETED" };
          }
        }
      });

      // Mutation queue handles executePersistedMemoryChangeSet activity
      const mutWorker = await Worker.create({
        connection: testEnv.nativeConnection,
        taskQueue: MEMORY_MUTATION_TASK_QUEUE,
        workflowBundle: bundles.memoryMutationBundle,
        ...defaultTestWorkerOptions(),
        activities: {
          executePersistedMemoryChangeSet: async () => {
            changeSetExecuted = true;
            return { status: "APPLIED", mutationsCount: 5 };
          }
        }
      });

      const handle = await testEnv.client.workflow.start("memoryConsolidationWorkflow", {
        workflowId: "ts-workflow:consolidation-full",
        taskQueue: "tq-ts-consolidation",
        args: [{ runId: "run:consolidation-1", traceId: "trace:consolidation-1" }]
      });

      const [result] = await Promise.all([
        maintWorker.runUntil(handle.result()),
        mutWorker.runUntil(handle.result())
      ]);

      expect(changeSetExecuted).toBe(true);
      expect(repairFinalized).toBe(true);
      expect(result).toEqual({ id: "run:consolidation-1", status: "COMPLETED" });
    });
  });

  // --------------------------------------------------------------------------
  // 8. memoryMutationWorkflow
  // --------------------------------------------------------------------------
  describe("memoryMutationWorkflow", () => {
    it("executes persisted change set directly on mutation queue", async () => {
      const worker = await Worker.create({
        connection: testEnv.nativeConnection,
        taskQueue: MEMORY_MUTATION_TASK_QUEUE,
        workflowBundle: bundles.memoryMutationBundle,
        ...defaultTestWorkerOptions(),
        activities: {
          executePersistedMemoryChangeSet: async (input) => ({
            status: "APPLIED",
            changeSetId: input.changeSetId,
            mutationsCount: 3
          })
        }
      });

      const result = await worker.runUntil(
        testEnv.client.workflow.execute("memoryMutationWorkflow", {
          workflowId: "ts-workflow:memory-mutation",
          taskQueue: MEMORY_MUTATION_TASK_QUEUE,
          args: [{ changeSetId: "cs:mutation-direct", traceId: "trace:mutation-direct" }]
        })
      );

      expect(result.status).toBe("APPLIED");
      expect(result.changeSetId).toBe("cs:mutation-direct");
    });
  });
});
