import { fileURLToPath } from "node:url";
import fs from "node:fs";
import path from "node:path";
import { bundleWorkflowCode, type WorkflowBundle } from "@temporalio/worker";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import proto from "@temporalio/proto";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const FIXTURES_DIR = path.resolve(__dirname, "../fixtures/workflow-histories");

export interface WorkflowBundles {
  mainBundle: WorkflowBundle;
  memoryMaintenanceBundle: WorkflowBundle;
  memoryMutationBundle: WorkflowBundle;
}

let cachedBundles: WorkflowBundles | null = null;

export async function getWorkflowBundles(): Promise<WorkflowBundles> {
  if (cachedBundles) return cachedBundles;

  const mainWorkflowsPath = path.resolve(__dirname, "../../src/workflows.ts");
  const memoryMaintenancePath = path.resolve(__dirname, "../../src/memory-maintenance-workflows.ts");
  const memoryMutationPath = path.resolve(__dirname, "../../src/memory-mutation-workflows.ts");

  const [mainBundle, memoryMaintenanceBundle, memoryMutationBundle] = await Promise.all([
    bundleWorkflowCode({ workflowsPath: mainWorkflowsPath }),
    bundleWorkflowCode({ workflowsPath: memoryMaintenancePath }),
    bundleWorkflowCode({ workflowsPath: memoryMutationPath })
  ]);

  cachedBundles = { mainBundle, memoryMaintenanceBundle, memoryMutationBundle };
  return cachedBundles;
}

export function defaultTestWorkerOptions() {
  return {
    isolateExecutionTimeout: "30s"
  };
}

export function saveHistoryFixture(filename: string, history: unknown): void {
  if (!fs.existsSync(FIXTURES_DIR)) {
    fs.mkdirSync(FIXTURES_DIR, { recursive: true });
  }
  const filePath = path.join(FIXTURES_DIR, filename.endsWith(".json") ? filename : `${filename}.json`);
  fs.writeFileSync(filePath, JSON.stringify(history, null, 2), "utf8");
}

export function loadHistoryFixture(filename: string): any {
  const filePath = path.join(FIXTURES_DIR, filename.endsWith(".json") ? filename : `${filename}.json`);
  if (!fs.existsSync(filePath)) {
    throw new Error(`Fixture file not found: ${filePath}`);
  }
  const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return proto.temporal.api.history.v1.History.fromObject(raw);
}

export function listHistoryFixtures(): string[] {
  if (!fs.existsSync(FIXTURES_DIR)) return [];
  return fs.readdirSync(FIXTURES_DIR).filter((f) => f.endsWith(".json"));
}

