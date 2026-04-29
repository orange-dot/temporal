package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/tests/testcore"
)

type WFTFailureReportedProblemsTestSuite struct {
	testcore.FunctionalTestBase
	shouldFail                atomic.Bool
	shouldBlockActivity       atomic.Bool
	activityFailuresRemaining atomic.Int32
	activityBlockCh           chan struct{}
}

const reportedProblemsActivityID = "reported-problems-activity"

func TestWFTFailureReportedProblemsTestSuite(t *testing.T) {
	s := new(WFTFailureReportedProblemsTestSuite)
	suite.Run(t, s)
}

func (s *WFTFailureReportedProblemsTestSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.activityBlockCh = make(chan struct{})
	s.OverrideDynamicConfig(dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 2)
	s.OverrideDynamicConfig(dynamicconfig.NumConsecutiveActivityTaskProblemsToTriggerSearchAttribute, 2)
}

func (s *WFTFailureReportedProblemsTestSuite) simpleWorkflowWithShouldFail(ctx workflow.Context) (string, error) {
	if s.shouldFail.Load() {
		panic("forced-panic-to-fail-wft")
	}
	return "done!", nil
}

func (s *WFTFailureReportedProblemsTestSuite) simpleActivity() (string, error) {
	return "done!", nil
}

func (s *WFTFailureReportedProblemsTestSuite) activityThatFailsUntilUnblocked(ctx context.Context) (string, error) {
	if s.shouldFail.Load() {
		return "", temporal.NewApplicationError("forced activity failure", "ForcedActivityFailure")
	}
	return "done!", nil
}

func (s *WFTFailureReportedProblemsTestSuite) activityThatFailsOrBlocksUntilReleased(ctx context.Context) (string, error) {
	if s.shouldFail.Load() {
		return "", temporal.NewApplicationError("forced activity failure", "ForcedActivityFailure")
	}
	if s.shouldBlockActivity.Load() {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-s.activityBlockCh:
		}
	}
	return "done!", nil
}

func (s *WFTFailureReportedProblemsTestSuite) activityThatFailsConfiguredTimes(ctx context.Context) (string, error) {
	if s.activityFailuresRemaining.Add(-1) >= 0 {
		return "", temporal.NewApplicationError("forced activity failure", "ForcedActivityFailure")
	}
	return "done!", nil
}

func (s *WFTFailureReportedProblemsTestSuite) activityThatTimesOutUntilUnblocked(ctx context.Context) (string, error) {
	if s.shouldFail.Load() {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return "late result", nil
		}
	}
	return "done!", nil
}

func (s *WFTFailureReportedProblemsTestSuite) workflowWithRetryingActivity(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			MaximumInterval:    100 * time.Millisecond,
			BackoffCoefficient: 1,
		},
	})

	var ret string
	err := workflow.ExecuteActivity(ctx, s.activityThatFailsUntilUnblocked).Get(ctx, &ret)
	return ret, err
}

func (s *WFTFailureReportedProblemsTestSuite) workflowWithRetryingActivityID(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             reportedProblemsActivityID,
		StartToCloseTimeout:    1 * time.Second,
		ScheduleToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			MaximumInterval:    100 * time.Millisecond,
			BackoffCoefficient: 1,
		},
	})

	var ret string
	err := workflow.ExecuteActivity(ctx, s.activityThatFailsOrBlocksUntilReleased).Get(ctx, &ret)
	return ret, err
}

func (s *WFTFailureReportedProblemsTestSuite) workflowWithOneFailureSlowRetryActivity(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			MaximumInterval:    5 * time.Second,
			BackoffCoefficient: 1,
		},
	})

	var ret string
	err := workflow.ExecuteActivity(ctx, s.activityThatFailsConfiguredTimes).Get(ctx, &ret)
	return ret, err
}

func (s *WFTFailureReportedProblemsTestSuite) workflowWithRetryingTimeoutActivity(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 100 * time.Millisecond,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			MaximumInterval:    100 * time.Millisecond,
			BackoffCoefficient: 1,
		},
	})

	var ret string
	err := workflow.ExecuteActivity(ctx, s.activityThatTimesOutUntilUnblocked).Get(ctx, &ret)
	return ret, err
}

func (s *WFTFailureReportedProblemsTestSuite) workflowWithRetryingActivityAndTimeoutActivity(ctx workflow.Context) (string, error) {
	applicationFailureCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			MaximumInterval:    100 * time.Millisecond,
			BackoffCoefficient: 1,
		},
	})
	timeoutCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 100 * time.Millisecond,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			MaximumInterval:    100 * time.Millisecond,
			BackoffCoefficient: 1,
		},
	})

	applicationFailureFuture := workflow.ExecuteActivity(applicationFailureCtx, s.activityThatFailsUntilUnblocked)
	timeoutFuture := workflow.ExecuteActivity(timeoutCtx, s.activityThatTimesOutUntilUnblocked)

	var ret string
	if err := applicationFailureFuture.Get(ctx, &ret); err != nil {
		return "", err
	}
	if err := timeoutFuture.Get(ctx, &ret); err != nil {
		return "", err
	}
	return "done!", nil
}

func (s *WFTFailureReportedProblemsTestSuite) workflowWithRetryingActivityAndFailingSignal(ctx workflow.Context) (string, error) {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			MaximumInterval:    100 * time.Millisecond,
			BackoffCoefficient: 1,
		},
	})

	activityFuture := workflow.ExecuteActivity(activityCtx, s.activityThatFailsUntilUnblocked)
	signalChannel := workflow.GetSignalChannel(ctx, "fail-wft")

	for {
		var activityDone bool
		var ret string
		var activityErr error
		selector := workflow.NewSelector(ctx)
		selector.AddFuture(activityFuture, func(f workflow.Future) {
			activityErr = f.Get(ctx, &ret)
			activityDone = true
		})
		selector.AddReceive(signalChannel, func(c workflow.ReceiveChannel, more bool) {
			var signal string
			c.Receive(ctx, &signal)
			if s.shouldFail.Load() {
				panic("forced-panic-to-fail-wft")
			}
		})
		selector.Select(ctx)
		if activityDone {
			return ret, activityErr
		}
	}
}

// workflowWithSignalsThatFails creates a workflow that listens for signals and fails on each workflow task.
// This is used to test that the TemporalReportedProblems search attribute is not incorrectly removed
// when signals keep coming in despite continuous workflow task failures.
func (s *WFTFailureReportedProblemsTestSuite) workflowWithSignalsThatFails(ctx workflow.Context) (string, error) {
	// Signal ourselves to create buffered events
	err := s.SdkClient().SignalWorkflow(context.Background(), workflow.GetInfo(ctx).WorkflowExecution.ID, "", "test-signal", "self-signal")
	if err != nil {
		return "", err
	}
	panic("forced-panic-after-self-signal")

}

// workflowWithActivity creates a workflow that executes an activity before potentially failing.
// This is used to test workflow task failure scenarios in a more realistic context where the workflow
// has already executed some operations (activities) before encountering a workflow task failure.
// The activity itself succeeds, but the workflow task may fail afterward, which triggers the server
// to clear the sticky task queue and transition to a normal task queue for subsequent workflow tasks.
func (s *WFTFailureReportedProblemsTestSuite) workflowWithActivity(ctx workflow.Context) (string, error) {
	var ret string
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Second,
	}), s.simpleActivity).Get(ctx, &ret)
	if err != nil {
		return "", err
	}

	if s.shouldFail.Load() {
		panic("forced-panic-to-fail-wft")
	}

	return "done!", nil
}

func (s *WFTFailureReportedProblemsTestSuite) TestWFTFailureReportedProblems_SetAndClear() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.shouldFail.Store(true)

	s.SdkWorker().RegisterWorkflow(s.simpleWorkflowWithShouldFail)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.simpleWorkflowWithShouldFail)
	s.NoError(err)

	// Check if the search attributes are not empty and has TemporalReportedProblems
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saVal, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saVal)
		require.Contains(t, saVal, "category=WorkflowTaskFailed")
		require.Contains(t, saVal, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")

		execution, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.GreaterOrEqual(t, execution.PendingWorkflowTask.Attempt, int32(2))
	}, 20*time.Second, 500*time.Millisecond)

	// Unblock the workflow
	s.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))

	description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.NotNil(description.TypedSearchAttributes)
	_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
	s.False(ok)
}

func (s *WFTFailureReportedProblemsTestSuite) TestWFTFailureReportedProblems_NotClearedBySignals() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.SdkWorker().RegisterWorkflow(s.workflowWithSignalsThatFails)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.workflowWithSignalsThatFails)
	s.NoError(err)

	// The workflow will signal itself and panic on each WFT, creating buffered events naturally.
	// Wait for the search attribute to be set due to consecutive failures
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saVal, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saVal)
		require.Contains(t, saVal, "category=WorkflowTaskFailed")
		require.Contains(t, saVal, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")
	}, 20*time.Second, 500*time.Millisecond)

	// Validate the workflow history shows the repeating pattern:
	// signal -> task scheduled -> task started -> task failed
	// This demonstrates that signals are being buffered between workflow task failures.
	s.EventuallyWithT(func(t *assert.CollectT) {
		var events []*historypb.HistoryEvent
		iter := s.SdkClient().GetWorkflowHistory(ctx, workflowRun.GetID(), workflowRun.GetRunID(), false, 0)
		for iter.HasNext() {
			event, err := iter.Next()
			require.NoError(t, err)
			events = append(events, event)
		}

		// Validate the expected pattern structure showing repeated cycles of task failures and signals
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskFailed
  9 WorkflowExecutionSignaled`, events[:9])
	}, 10*time.Second, 500*time.Millisecond)

	// Verify the search attribute persists even as the workflow continues to fail and create buffered events
	// This is the key part of the test - buffered events should not cause the search attribute to be cleared
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		s.NoError(err)
		saVal, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		s.True(ok, "Search attribute should still be present during continued failures")
		s.NotEmpty(saVal, "Search attribute should not be empty during continued failures")
	}, 5*time.Second, 500*time.Millisecond)

	// Terminate the workflow for cleanup
	s.NoError(s.SdkClient().TerminateWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID(), "test cleanup"))
}

func (s *WFTFailureReportedProblemsTestSuite) TestWFTFailureReportedProblems_SetAndClear_FailAfterActivity() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.shouldFail.Store(true)

	s.SdkWorker().RegisterWorkflow(s.workflowWithActivity)
	s.SdkWorker().RegisterActivity(s.simpleActivity)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.workflowWithActivity)
	s.NoError(err)

	// Validate the search attributes are not empty and has TemporalReportedProblems with 2 entries
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saValues)
		require.Len(t, saValues, 2)
		require.Contains(t, saValues, "category=WorkflowTaskFailed")
		require.Contains(t, saValues, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")
	}, 20*time.Second, 500*time.Millisecond)

	// Unblock the workflow
	s.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))

	description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.NotNil(description.TypedSearchAttributes)
	_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
	s.False(ok)
}

func (s *WFTFailureReportedProblemsTestSuite) TestActivityRetryReportedProblems_SetAndClear() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.shouldFail.Store(true)

	s.SdkWorker().RegisterWorkflow(s.workflowWithRetryingActivity)
	s.SdkWorker().RegisterActivity(s.activityThatFailsUntilUnblocked)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.workflowWithRetryingActivity)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saValues)
		require.Len(t, saValues, 2)
		require.Contains(t, saValues, "category=ActivityTaskFailed")
		require.Contains(t, saValues, "cause=ApplicationFailure")

		execution, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotEmpty(t, execution.PendingActivities)
		require.GreaterOrEqual(t, execution.PendingActivities[0].Attempt, int32(3))
	}, 20*time.Second, 500*time.Millisecond)

	s.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))

	description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.NotNil(description.TypedSearchAttributes)
	_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
	s.False(ok)
}

func (s *WFTFailureReportedProblemsTestSuite) TestActivityRetryReportedProblems_DoesNotReportAfterOneFailure() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.activityFailuresRemaining.Store(1)

	s.SdkWorker().RegisterWorkflow(s.workflowWithOneFailureSlowRetryActivity)
	s.SdkWorker().RegisterActivity(s.activityThatFailsConfiguredTimes)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.workflowWithOneFailureSlowRetryActivity)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		execution, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotEmpty(t, execution.PendingActivities)
		require.Equal(t, int32(2), execution.PendingActivities[0].Attempt)

		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.False(t, ok)
	}, 10*time.Second, 100*time.Millisecond)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))
}

func (s *WFTFailureReportedProblemsTestSuite) TestActivityRetryReportedProblems_ResetClearsStaleProblem() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.shouldFail.Store(true)

	s.SdkWorker().RegisterWorkflow(s.workflowWithRetryingActivityID)
	s.SdkWorker().RegisterActivity(s.activityThatFailsOrBlocksUntilReleased)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.workflowWithRetryingActivityID)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.Contains(t, saValues, "category=ActivityTaskFailed")
		require.Contains(t, saValues, "cause=ApplicationFailure")

		execution, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotEmpty(t, execution.PendingActivities)
		require.GreaterOrEqual(t, execution.PendingActivities[0].Attempt, int32(3))
	}, 20*time.Second, 500*time.Millisecond)

	s.shouldFail.Store(false)
	s.shouldBlockActivity.Store(true)

	_, err = s.FrontendClient().ResetActivity(ctx, &workflowservice.ResetActivityRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
			RunId:      workflowRun.GetRunID(),
		},
		Activity: &workflowservice.ResetActivityRequest_Id{Id: reportedProblemsActivityID},
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.False(t, ok)
	}, 10*time.Second, 500*time.Millisecond)

	close(s.activityBlockCh)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))
}

func (s *WFTFailureReportedProblemsTestSuite) TestActivityTimeoutReportedProblems_SetAndClear() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.shouldFail.Store(true)

	s.SdkWorker().RegisterWorkflow(s.workflowWithRetryingTimeoutActivity)
	s.SdkWorker().RegisterActivity(s.activityThatTimesOutUntilUnblocked)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.workflowWithRetryingTimeoutActivity)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saValues)
		require.Len(t, saValues, 2)
		require.Contains(t, saValues, "category=ActivityTaskTimedOut")
		require.Contains(t, saValues, "cause=ActivityTaskTimedOutCauseStartToClose")

		execution, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotEmpty(t, execution.PendingActivities)
		require.GreaterOrEqual(t, execution.PendingActivities[0].Attempt, int32(3))
	}, 20*time.Second, 500*time.Millisecond)

	s.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))

	description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.NotNil(description.TypedSearchAttributes)
	_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
	s.False(ok)
}

func (s *WFTFailureReportedProblemsTestSuite) TestActivityRetryReportedProblems_DeduplicatesMultipleActivities() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.shouldFail.Store(true)

	s.SdkWorker().RegisterWorkflow(s.workflowWithRetryingActivityAndTimeoutActivity)
	s.SdkWorker().RegisterActivity(s.activityThatFailsUntilUnblocked)
	s.SdkWorker().RegisterActivity(s.activityThatTimesOutUntilUnblocked)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.workflowWithRetryingActivityAndTimeoutActivity)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saValues)
		require.Len(t, saValues, 4)
		require.Contains(t, saValues, "category=ActivityTaskFailed")
		require.Contains(t, saValues, "cause=ApplicationFailure")
		require.Contains(t, saValues, "category=ActivityTaskTimedOut")
		require.Contains(t, saValues, "cause=ActivityTaskTimedOutCauseStartToClose")
	}, 20*time.Second, 500*time.Millisecond)

	s.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))
}

func (s *WFTFailureReportedProblemsTestSuite) TestReportedProblemsPrefersWorkflowTaskFailureOverActivityRetry() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.shouldFail.Store(true)

	s.SdkWorker().RegisterWorkflow(s.workflowWithRetryingActivityAndFailingSignal)
	s.SdkWorker().RegisterActivity(s.activityThatFailsUntilUnblocked)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.workflowWithRetryingActivityAndFailingSignal)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.Contains(t, saValues, "category=ActivityTaskFailed")
		require.Contains(t, saValues, "cause=ApplicationFailure")
	}, 20*time.Second, 500*time.Millisecond)

	s.NoError(s.SdkClient().SignalWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID(), "fail-wft", "signal"))

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.Len(t, saValues, 2)
		require.Contains(t, saValues, "category=WorkflowTaskFailed")
		require.Contains(t, saValues, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")
	}, 20*time.Second, 500*time.Millisecond)

	s.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))
}

func (s *WFTFailureReportedProblemsTestSuite) TestWFTFailureReportedProblems_DynamicConfigChanges() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cleanup := s.OverrideDynamicConfig(dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 0)
	defer cleanup()
	s.shouldFail.Store(true)

	s.SdkWorker().RegisterWorkflow(s.simpleWorkflowWithShouldFail)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.simpleWorkflowWithShouldFail)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.False(t, ok)

		exec, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.GreaterOrEqual(t, exec.PendingWorkflowTask.Attempt, int32(2))
	}, 10*time.Second, 500*time.Millisecond)

	cleanup2 := s.OverrideDynamicConfig(dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 2)
	defer cleanup2()

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saValues)
		require.Len(t, saValues, 2)
		require.Contains(t, saValues, "category=WorkflowTaskFailed")
		require.Contains(t, saValues, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")
	}, 15*time.Second, 500*time.Millisecond)

	s.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))

	description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.NotNil(description.TypedSearchAttributes)
	_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
	s.False(ok)
}
