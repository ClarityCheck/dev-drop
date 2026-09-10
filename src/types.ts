/**
 * Shared TypeScript types for the Workflows starter template
 */

export type StepStatus =
	| "pending"
	| "running"
	| "waiting"
	| "completed"
	| "error";
export type WorkflowStatus = "idle" | "running" | "completed" | "error";

export interface StepDefinition {
	id: string;
	name: string;
	description: string;
	lineRange: [number, number];
}

export interface WorkflowState {
	instanceId: string | null;
	currentStep: string | null;
	stepStatuses: Record<string, StepStatus>;
	workflowStatus: WorkflowStatus;
	wsConnected: boolean;
}

export interface WorkflowUpdateMessage {
	type: "workflow_update";
	currentStep: string | null;
	stepStatuses: Record<string, StepStatus>;
	workflowStatus: "running" | "completed" | "error";
	timestamp: number;
}

// Step definitions for the workflow
export const WORKFLOW_STEPS: StepDefinition[] = [
	{
		id: "refresh-view",
		name: "refresh view",
		description: "Recompute ca_drop_combined_search_result",
		lineRange: [1, 1],
	},
	{
		id: "match-keys",
		name: "match keys",
		description: "Page keys from ClickHouse, check each against KV",
		lineRange: [2, 2],
	},
	{
		id: "summary",
		name: "summary",
		description: "Count what landed in ca_drop_match_run",
		lineRange: [3, 3],
	},
];
