# Activity Retry Reported Problems Spike

Local spike note for a potential Temporal Server contribution.

## Motivation

Temporal already has `TemporalReportedProblems` for consecutive workflow task
failures. Activity retries can also keep an execution alive but effectively not
making useful progress. Today that state is visible through per-execution
inspection, but it is not surfaced as a small visibility-level problem signal.

The concept matches a local lab pattern:

- canonical history and mutable state remain the source of truth
- server derives bounded evidence from that truth
- operators and tools can query the evidence without replaying or reading the
  whole history

## Narrow Scope

Reuse the existing `TemporalReportedProblems` search attribute for activity
retry loops.

Initial target behavior:

- add a namespace dynamic config threshold for consecutive activity task
  problems
- when a retryable activity failure reaches the threshold, upsert
  `TemporalReportedProblems`
- encode only low-cardinality symbolic facts, for example:
  - `category=ActivityTaskFailed`
  - `cause=ApplicationFailure`
  - `category=ActivityTaskTimedOut`
  - `cause=ActivityTaskTimedOutCauseStartToClose`
- clear the activity problem signal once the activity succeeds or no pending
  activity still qualifies

## Out Of Scope

- new public API or proto surface
- storing raw activity error messages in visibility
- storing activity IDs or worker identities in visibility
- adding an AI/planner layer to Temporal
- replacing workflow history or mutable state as canonical truth
- broad compensation or saga semantics

## First Code Areas

- `common/searchattribute/sadefs/constants.go`
- `common/dynamicconfig/constants.go`
- `service/history/workflow/mutable_state_impl.go`
- `service/history/workflow/workflow_task_state_machine.go`
- `service/history/api/respondactivitytaskfailed/api.go`
- `service/history/api/respondactivitytaskcompleted/api.go`
- `tests/workflow_task_reported_problems_test.go`

## Spike Questions

- Is `ActivityInfo.Attempt` enough to count consecutive activity task problems,
  or do we need a dedicated counter to avoid counting schedule attempts that are
  not failures?
- Should the signal be set during `RetryActivity`, or after the caller decides
  the retry state and records completion metrics?
- What is the safest clear rule when multiple activities are pending and only
  one recovers?

## Resolved Spike Choices

- Activity timeouts use a distinct `ActivityTaskTimedOut` category in this
  spike because they are operationally different from application failures
  while still having bounded cardinality.
- Multiple qualifying activity problems are reported as a deduplicated token
  set, not as the first pending activity by scheduled event ID.

## Preferred Shape

Keep the first patch small and mechanically similar to the existing workflow
task reported-problems path. The first spike should prove the behavior with a
focused test before deciding whether the implementation belongs inside mutable
state helpers or API handlers.

## Local Spike Result

Branch: `local/activity-retry-reported-problems-spike`

Implemented locally:

- added `system.numConsecutiveActivityTaskProblemsToTriggerSearchAttribute`
  with default `0`, so the feature is off unless a namespace opts in
- made namespace `ReportedProblemsSearchAttribute` capability true when either
  the workflow-task threshold or activity-task threshold is enabled
- reused the existing `TemporalReportedProblems` keyword-list search attribute
  for low-cardinality activity retry evidence
- gave workflow-task problems priority over activity problems when both qualify
- reports all qualifying activity problem categories/causes as a deduplicated
  token set, rather than choosing a single pending activity
- recomputed the search attribute from current mutable state instead of only
  blindly removing it after workflow-task success
- renamed workflow-task clear handling to
  `ClearWorkflowTaskFailureAndRecomputeReportedProblems` so the method name
  reflects the new recompute semantics
- added focused functional tests for application failure, timeout,
  multi-activity deduplication, and workflow-task priority

Verification run locally:

- `go test ./service/history/workflow ./service/frontend ./service/history/configs -run '^$'`
- `go test ./tests -run 'TestWFTFailureReportedProblemsTestSuite'`
- `go test ./service/frontend -run 'TestNamespaceHandlerCommonSuite/TestCapabilitiesAndLimits'`
- `git diff --check`

The functional tests need to run outside the Codex sandbox because the Temporal
test cluster binds local loopback ports.

## Follow-Up Questions

- Is `ActivityInfo.Attempt` the right threshold input, or should activity
  failures maintain an explicit consecutive-problem counter?
- Should activity retry search-attribute updates live in mutable-state retry
  handling, or closer to the activity completion/failure APIs?
- Does upstream prefer a distinct activity category such as
  `ActivityTaskRetrying`, instead of reusing terminal event categories like
  `ActivityTaskFailed` and `ActivityTaskTimedOut`?
