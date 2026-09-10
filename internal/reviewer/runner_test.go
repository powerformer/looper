package reviewer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nexu-io/looper/internal/config"
	"github.com/nexu-io/looper/internal/domain"
	"github.com/nexu-io/looper/internal/eventlog"
	"github.com/nexu-io/looper/internal/forge"
	gitinfra "github.com/nexu-io/looper/internal/infra/git"
	githubinfra "github.com/nexu-io/looper/internal/infra/github"
	"github.com/nexu-io/looper/internal/infra/shell"
	"github.com/nexu-io/looper/internal/infra/specpr"
	"github.com/nexu-io/looper/internal/loops"
	"github.com/nexu-io/looper/internal/loops/failureclass"
	"github.com/nexu-io/looper/internal/networkpolicy"
	"github.com/nexu-io/looper/internal/reviewer/automerge"
	"github.com/nexu-io/looper/internal/reviewer/criteria"
	"github.com/nexu-io/looper/internal/storage"
	"github.com/nexu-io/looper/internal/worktreesafety"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDiscoverPullRequestsCreatesLoopAndQueue(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("len(QueueItems) = %d, want 1", len(result.QueueItems))
	}
	if len(result.CreatedLoopIDs) != 1 {
		t.Fatalf("len(CreatedLoopIDs) = %d, want 1", len(result.CreatedLoopIDs))
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.CreatedLoopIDs[0])
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.Status != "queued" || loop.Repo == nil || *loop.Repo != "acme/looper" {
		t.Fatalf("loop = %#v, want queued reviewer loop", loop)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), result.QueueItems[0].ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "queued" || queue.DedupeKey != "reviewer:project_1:"+result.CreatedLoopIDs[0]+":acme/looper:42" {
		t.Fatalf("queue = %#v, want queued reviewer item", queue)
	}
}

func TestDiscoverPullRequestsSkipsReviewerHoldLabel(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{domain.HoldLabelReviewer}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 || len(result.CreatedLoopIDs) != 0 || result.Skipped == 0 {
		t.Fatalf("result = %#v, want held PR skipped", result)
	}
}

func TestDiscoverPullRequestsAllowsManualReviewerFollowUpWhenHeld(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"alice"}, currentLogin: "bob", labels: []string{domain.HoldLabelReviewer}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"manual":true}`
	loop := storage.LoopRecord{ID: "loop_manual_follow_hold_allowed", Seq: 6, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 || result.QueueItems[0].LoopID == nil || *result.QueueItems[0].LoopID != loop.ID {
		t.Fatalf("result = %#v, want manual held follow-up queued", result)
	}
}

func TestRunPublishStepSkipsWhenReviewerHoldApplied(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{domain.HoldLabelReviewer}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	checkpoint, err := runner.runPublishStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()}, Loop: storage.LoopRecord{}, Repo: "acme/looper", PRNumber: 42, Checkpoint: reviewerCheckpoint{PendingReview: &pendingReviewCheckpoint{HeadSHA: "abc123", Event: ReviewEventComment}}})
	if err == nil || !strings.Contains(err.Error(), "currently held") {
		t.Fatalf("runPublishStep() error = %v, want hold skip", err)
	}
	if checkpoint.SkipReason != "" {
		t.Fatalf("checkpoint = %#v, want unchanged checkpoint before hold handling", checkpoint)
	}
}

func TestProcessClaimedItemSkipsHeldFreshAutomaticReviewerBeforeStepExecution(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := "pr:acme/looper:42"
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_reviewer_runtime_hold", Seq: 2, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	projectID := "project_1"
	loopID := "loop_reviewer_runtime_hold"
	lockKey := loopTarget
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_reviewer_runtime_hold", ProjectID: &projectID, LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "reviewer:runtime-hold", Priority: storage.QueuePriorityReviewer, Status: "running", AvailableAt: nowISO, LockKey: &lockKey, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{labels: []string{domain.HoldLabelReviewer}}
	agent := &fakeAgentExecutor{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.ProcessClaimedItem(context.Background(), storage.QueueItemRecord{ID: "queue_reviewer_runtime_hold", ProjectID: &projectID, LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "running", LockKey: &lockKey})
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !strings.Contains(result.Summary, "currently held") {
		t.Fatalf("result = %#v, want held skipped result", result)
	}
	if len(agent.starts) != 0 {
		t.Fatalf("agent.starts = %#v, want none", agent.starts)
	}
	if len(github.issueCommentCalls) != 0 {
		t.Fatalf("issueCommentCalls = %#v, want none", github.issueCommentCalls)
	}
}

func TestProcessClaimedItemSkipsHeldRoutedReviewerBeforeRoutingDrift(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := "pr:acme/looper:42"
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_reviewer_routed_hold", Seq: 4, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	projectID := "project_1"
	loopID := "loop_reviewer_routed_hold"
	lockKey := loopTarget
	queue := storage.QueueItemRecord{ID: "queue_reviewer_routed_hold", ProjectID: &projectID, LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "reviewer:routed-runtime-hold", Priority: storage.QueuePriorityReviewer, Status: "running", AvailableAt: nowISO, LockKey: &lockKey, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	autoDiscovery := true
	cfg := &config.Config{Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42}, Projects: []config.ProjectRefConfig{{ID: "project_1", RepoPath: t.TempDir(), Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted}, Roles: &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{Discovery: &config.PartialReviewerRoleDiscoveryConfig{AutoDiscovery: &autoDiscovery}}}}}}
	github := &fakeGitHubGateway{currentLogin: "reviewer", author: "author", labels: []string{domain.HoldLabelReviewer}, reviewRequests: []string{}, reviewRequestUsers: []networkpolicy.GitHubUser{}}
	agent := &fakeAgentExecutor{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true}, CustomInstructions: cfg})

	result, err := runner.ProcessClaimedItem(context.Background(), queue)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !strings.Contains(result.Summary, "currently held") {
		t.Fatalf("result = %#v, want held skipped result", result)
	}
	if len(agent.starts) != 0 {
		t.Fatalf("agent.starts = %#v, want none", agent.starts)
	}
}

func TestProcessClaimedItemSkipsHeldFreshAutomaticReviewerBeforeRoutedRevalidation(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := "pr:acme/looper:42"
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_reviewer_fresh_hold", Seq: 3, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	projectID := "project_1"
	loopID := "loop_reviewer_fresh_hold"
	lockKey := loopTarget
	queue := storage.QueueItemRecord{ID: "queue_reviewer_fresh_hold", ProjectID: &projectID, LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "reviewer:fresh-runtime-hold", Priority: storage.QueuePriorityReviewer, Status: "running", AvailableAt: nowISO, LockKey: &lockKey, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{labels: []string{domain.HoldLabelReviewer}}
	agent := &fakeAgentExecutor{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.ProcessClaimedItem(context.Background(), queue)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !strings.Contains(result.Summary, "currently held") {
		t.Fatalf("result = %#v, want held skipped result", result)
	}
	if len(agent.starts) != 0 {
		t.Fatalf("agent.starts = %#v, want none", agent.starts)
	}
	if len(github.issueCommentCalls) != 0 {
		t.Fatalf("issueCommentCalls = %#v, want none", github.issueCommentCalls)
	}
}

func TestShouldRetryQueueFailureRespectsMaxAttempts(t *testing.T) {
	t.Parallel()

	if !shouldRetryQueueFailure(FailureRetryableTransient, 5, -1) {
		t.Fatal("shouldRetryQueueFailure() = false, want true for infinite retries")
	}
	if shouldRetryQueueFailure(FailureNonRetryable, 5, -1) {
		t.Fatal("shouldRetryQueueFailure() = true, want false for infinite non_retryable retries")
	}
	if !shouldRetryQueueFailure(FailureNonRetryable, 1, 3) {
		t.Fatal("shouldRetryQueueFailure() = false, want true for bounded non_retryable retries")
	}
	if shouldRetryQueueFailure(FailureRetryableTransient, 3, 3) {
		t.Fatal("shouldRetryQueueFailure() = true, want false once nextAttempts reaches maxAttempts")
	}
	// Populated hollow worktree parks as MI and must not infinite-retry under max=-1.
	if shouldRetryQueueFailure(FailureManualIntervention, 242, -1) {
		t.Fatal("shouldRetryQueueFailure(MI) = true, want false so hollow storms park")
	}
}

func TestClassifyPreservedUnusableWorktreeAsManualIntervention(t *testing.T) {
	t.Parallel()

	runner := &Runner{}
	err := fmt.Errorf("worktree path /tmp/wt is unusable and not empty; manual intervention required: %w", worktreesafety.ErrUnusableWorktreePreserved)
	got := runner.classifyFailureForProjectAndBoundary("project_1", err, failureclass.BoundaryGitRemote)
	if got == nil || got.kind != FailureManualIntervention {
		t.Fatalf("classify = %#v, want FailureManualIntervention (not transient via worktree step boundary)", got)
	}
	if shouldRetryQueueFailure(got.kind, 1, -1) {
		t.Fatal("preserved unusable worktree must not requeue under unlimited maxAttempts")
	}
}

func TestBackoffDelayCapsBeforeDurationOverflow(t *testing.T) {
	t.Parallel()

	delay := backoffDelay(time.Second, 62, time.Minute)
	if delay != time.Minute {
		t.Fatalf("backoffDelay() = %s, want %s", delay, time.Minute)
	}
}

func TestNewPreservesInfiniteRetryMaxAttempts(t *testing.T) {
	t.Parallel()

	runner := New(Options{RetryMaxAttempts: -1})
	if runner.retryMaxAttempts != -1 {
		t.Fatalf("retryMaxAttempts = %d, want -1", runner.retryMaxAttempts)
	}
}

func TestDiscoverPullRequestCreatesLoopAndQueue(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: "acme/looper", PRNumber: 42})
	if err != nil {
		t.Fatalf("DiscoverPullRequest() error = %v", err)
	}
	if len(result.QueueItems) != 1 || len(result.CreatedLoopIDs) != 1 {
		t.Fatalf("result = %#v, want one queue item and one created loop", result)
	}
	if len(github.listCalls) != 0 {
		t.Fatalf("list calls = %#v, want targeted discovery to avoid repo scan", github.listCalls)
	}
}

func TestDiscoverPullRequestSkipsIneligiblePullRequest(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"someone-else"}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: "acme/looper", PRNumber: 42})
	if err != nil {
		t.Fatalf("DiscoverPullRequest() error = %v", err)
	}
	if len(result.QueueItems) != 0 || len(result.CreatedLoopIDs) != 0 || result.Skipped != 1 {
		t.Fatalf("result = %#v, want skipped targeted discovery with no loop", result)
	}
}

func TestDiscoverPullRequestRoutedModeRefreshesCurrentLoginBeforeSelfAuthoredCheck(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{author: "new-user", currentLogin: "new-user", labels: []string{"looper:target:red"}, reviewRequests: []string{"stale-user"}, reviewRequestUsers: []networkpolicy.GitHubUser{{Login: "new-user", ID: 42}}}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Network = config.NetworkConfig{NodeName: "red", GitHubLogin: "stale-user", GitHubUserID: 42}
	cfg.Projects = []config.ProjectRefConfig{{ID: "project_1", Name: "Looper", RepoPath: t.TempDir(), Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true}, CustomInstructions: &cfg})

	result, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: "acme/looper", PRNumber: 42})
	if err != nil {
		t.Fatalf("DiscoverPullRequest() error = %v", err)
	}
	if len(result.QueueItems) != 0 || len(result.CreatedLoopIDs) != 0 || result.Skipped != 1 {
		t.Fatalf("result = %#v, want self-authored routed PR skipped", result)
	}
	if github.currentLoginCalls != 1 {
		t.Fatalf("GetCurrentUserLogin calls = %d, want 1", github.currentLoginCalls)
	}
}

func TestDiscoverPullRequestRoutedModeRequiresMatchingTargetLabel(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "reviewer", reviewRequests: []string{"reviewer"}, labels: []string{"looper:target:blue"}}
	cfg := config.Config{Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42}, Projects: []config.ProjectRefConfig{{ID: "project_1", Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true}, CustomInstructions: &cfg})

	result, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: "acme/looper", PRNumber: 42})
	if err != nil {
		t.Fatalf("DiscoverPullRequest() error = %v", err)
	}
	if len(result.QueueItems) != 0 || result.Skipped != 1 {
		t.Fatalf("result = %#v, want routed mismatch skipped", result)
	}
}

func TestDiscoverPullRequestRoutedModeAllowsUnknownReviewRequestUsers(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "reviewer", labels: []string{"looper:target:red"}, reviewRequestsUnknown: true}
	autoDiscovery := true
	cfg := config.Config{Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42}, Projects: []config.ProjectRefConfig{{ID: "project_1", Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted}, Roles: &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{Discovery: &config.PartialReviewerRoleDiscoveryConfig{AutoDiscovery: &autoDiscovery}}}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true}, CustomInstructions: &cfg})

	result, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: "acme/looper", PRNumber: 42})
	if err != nil {
		t.Fatalf("DiscoverPullRequest() error = %v", err)
	}
	if len(result.QueueItems) != 1 || len(result.CreatedLoopIDs) != 1 || result.Skipped != 0 {
		t.Fatalf("result = %#v, want routed PR queued when review request users are unknown", result)
	}
}

func TestDiscoverPullRequestsRoutedModeSelfReviewLoginRefreshFailureSkipsWithoutError(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	autoDiscovery := true
	enableSelfReview := true
	requireReviewRequest := false
	github := &fakeGitHubGateway{author: "new-user", currentLoginErr: fmt.Errorf("gh auth failed"), labels: []string{"looper:target:red"}, reviewRequests: []string{}, reviewRequestUsers: []networkpolicy.GitHubUser{}}
	cfg := config.Config{
		Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "stale-user", GitHubUserID: 42},
		Projects: []config.ProjectRefConfig{{
			ID:      "project_1",
			Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted},
			Roles:   &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{Discovery: &config.PartialReviewerRoleDiscoveryConfig{AutoDiscovery: &autoDiscovery, Triggers: &config.PartialReviewerRoleTriggersConfig{EnableSelfReview: &enableSelfReview, RequireReviewRequest: &requireReviewRequest}}}},
		}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true}, CustomInstructions: &cfg})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 || len(result.CreatedLoopIDs) != 0 || result.Skipped != 2 {
		t.Fatalf("result = %#v, want self-review bypass refresh failure skipped without loop", result)
	}
	if github.currentLoginCalls != 1 {
		t.Fatalf("GetCurrentUserLogin calls = %d, want 1", github.currentLoginCalls)
	}
}

func TestRunFilterStepSkipsRoutedPullRequestWhenReviewRequestRemoved(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	cfg := config.Config{Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42}, Projects: []config.ProjectRefConfig{{ID: "project_1", Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{currentLogin: "reviewer"}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})
	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repo"}, Repo: "acme/looper", PRNumber: 42, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", Labels: []string{"looper:target:red"}, ReviewRequests: []string{}, ReviewRequestUsers: []networkpolicy.GitHubUser{}}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind != "routed_claim_ineligible" {
		t.Fatalf("checkpoint = %#v, want routed_claim_ineligible skip", checkpoint)
	}
}

func TestRunFilterStepAllowsRoutedPullRequestWhenReviewRequestUsersUnknown(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	cfg := config.Config{Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42}, Projects: []config.ProjectRefConfig{{ID: "project_1", Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{currentLogin: "reviewer"}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})
	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repo"}, Repo: "acme/looper", PRNumber: 42, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", Labels: []string{"looper:target:red"}, ReviewRequests: nil, ReviewRequestUsers: nil}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind != "" || checkpoint.SkipReason != "" {
		t.Fatalf("checkpoint = %#v, want routed PR allowed when review request users are unknown", checkpoint)
	}
}

func TestRunFilterStepAllowsRoutedSelfReviewWithoutReviewRequest(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	enableSelfReview := true
	requireReviewRequest := false
	cfg := config.Config{
		Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42},
		Projects: []config.ProjectRefConfig{{
			ID:      "project_1",
			Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted},
			Roles:   &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{Discovery: &config.PartialReviewerRoleDiscoveryConfig{Triggers: &config.PartialReviewerRoleTriggersConfig{EnableSelfReview: &enableSelfReview, RequireReviewRequest: &requireReviewRequest}}}},
		}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{currentLogin: "reviewer"}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})
	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repo"}, Repo: "acme/looper", PRNumber: 42, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", Author: "reviewer", HeadSHA: "abc123", Labels: []string{"looper:target:red"}, ReviewRequests: []string{}, ReviewRequestUsers: []networkpolicy.GitHubUser{}}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind != "" || checkpoint.SkipReason != "" {
		t.Fatalf("checkpoint = %#v, want routed self-review allowed without skip", checkpoint)
	}
}

func TestRunFilterStepSkipsRoutedSelfReviewWhenLoginRefreshFails(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	enableSelfReview := true
	requireReviewRequest := false
	github := &fakeGitHubGateway{currentLoginErr: fmt.Errorf("gh auth failed")}
	cfg := config.Config{
		Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42},
		Projects: []config.ProjectRefConfig{{
			ID:      "project_1",
			Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted},
			Roles:   &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{Discovery: &config.PartialReviewerRoleDiscoveryConfig{Triggers: &config.PartialReviewerRoleTriggersConfig{EnableSelfReview: &enableSelfReview, RequireReviewRequest: &requireReviewRequest}}}},
		}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})
	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repo"}, Repo: "acme/looper", PRNumber: 42, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", Author: "reviewer", HeadSHA: "abc123", Labels: []string{"looper:target:red"}, ReviewRequests: []string{}, ReviewRequestUsers: []networkpolicy.GitHubUser{}}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind != "routed_claim_ineligible" || !strings.Contains(checkpoint.SkipReason, "local GitHub identity is not requested for review") {
		t.Fatalf("checkpoint = %#v, want routed denial preserved on login refresh failure", checkpoint)
	}
	if github.currentLoginCalls != 1 {
		t.Fatalf("GetCurrentUserLogin calls = %d, want 1", github.currentLoginCalls)
	}
}

func TestRunFilterStepStillSkipsRoutedSelfReviewWithoutTargetLabel(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	enableSelfReview := true
	requireReviewRequest := false
	cfg := config.Config{
		Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42},
		Projects: []config.ProjectRefConfig{{
			ID:      "project_1",
			Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted},
			Roles:   &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{Discovery: &config.PartialReviewerRoleDiscoveryConfig{Triggers: &config.PartialReviewerRoleTriggersConfig{EnableSelfReview: &enableSelfReview, RequireReviewRequest: &requireReviewRequest}}}},
		}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})
	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repo"}, Repo: "acme/looper", PRNumber: 42, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", Author: "reviewer", HeadSHA: "abc123", Labels: []string{}, ReviewRequests: []string{}, ReviewRequestUsers: []networkpolicy.GitHubUser{}}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind != "routed_claim_ineligible" || !strings.Contains(checkpoint.SkipReason, "missing looper:target:<node_name> label") {
		t.Fatalf("checkpoint = %#v, want routed target-label skip", checkpoint)
	}
}

func TestRevalidateRoutedReviewerClaimAllowsSelfReviewWithoutReviewRequest(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	enableSelfReview := true
	requireReviewRequest := false
	github := &fakeGitHubGateway{author: "reviewer", currentLogin: "reviewer", labels: []string{"looper:target:red"}, reviewRequestUsers: []networkpolicy.GitHubUser{}}
	cfg := config.Config{
		Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42},
		Projects: []config.ProjectRefConfig{{
			ID:       "project_1",
			Name:     "Demo",
			RepoPath: "/tmp/repo",
			Network:  config.ProjectNetworkConfig{Mode: config.NetworkModeRouted},
			Roles:    &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{Discovery: &config.PartialReviewerRoleDiscoveryConfig{Triggers: &config.PartialReviewerRoleTriggersConfig{EnableSelfReview: &enableSelfReview, RequireReviewRequest: &requireReviewRequest}}}},
		}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})
	repo := "acme/looper"
	prNumber := int64(42)
	if err := runner.revalidateRoutedReviewerClaim(context.Background(), storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repo"}, storage.QueueItemRecord{Repo: &repo, PRNumber: &prNumber}); err != nil {
		t.Fatalf("revalidateRoutedReviewerClaim() error = %v", err)
	}
}

func TestRevalidateRoutedReviewerClaimAllowsUnknownReviewRequestUsers(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{"looper:target:red"}, reviewRequestsUnknown: true}
	cfg := config.Config{
		Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42},
		Projects: []config.ProjectRefConfig{{
			ID:       "project_1",
			Name:     "Demo",
			RepoPath: "/tmp/repo",
			Network:  config.ProjectNetworkConfig{Mode: config.NetworkModeRouted},
		}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})
	repo := "acme/looper"
	prNumber := int64(42)
	if err := runner.revalidateRoutedReviewerClaim(context.Background(), storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repo"}, storage.QueueItemRecord{Repo: &repo, PRNumber: &prNumber}); err != nil {
		t.Fatalf("revalidateRoutedReviewerClaim() error = %v", err)
	}
	if github.currentLoginCalls != 0 {
		t.Fatalf("GetCurrentUserLogin calls = %d, want 0 for unknown review request users", github.currentLoginCalls)
	}
}

func TestRevalidateRoutedReviewerClaimRequiresCurrentIdentityForSelfReviewBypass(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	enableSelfReview := true
	requireReviewRequest := false
	github := &fakeGitHubGateway{author: "reviewer", currentLogin: "someone-else", labels: []string{"looper:target:red"}, reviewRequestUsers: []networkpolicy.GitHubUser{}}
	cfg := config.Config{
		Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42},
		Projects: []config.ProjectRefConfig{{
			ID:       "project_1",
			Name:     "Demo",
			RepoPath: "/tmp/repo",
			Network:  config.ProjectNetworkConfig{Mode: config.NetworkModeRouted},
			Roles:    &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{Discovery: &config.PartialReviewerRoleDiscoveryConfig{Triggers: &config.PartialReviewerRoleTriggersConfig{EnableSelfReview: &enableSelfReview, RequireReviewRequest: &requireReviewRequest}}}},
		}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})
	repo := "acme/looper"
	prNumber := int64(42)
	err := runner.revalidateRoutedReviewerClaim(context.Background(), storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repo"}, storage.QueueItemRecord{Repo: &repo, PRNumber: &prNumber})
	if err == nil || !strings.Contains(err.Error(), "local GitHub identity is not requested for review") {
		t.Fatalf("revalidateRoutedReviewerClaim() error = %v, want current-identity review-request failure", err)
	}
}

func TestRevalidateRoutedReviewerClaimTreatsLoginRefreshFailureAsTransient(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	enableSelfReview := true
	requireReviewRequest := false
	github := &fakeGitHubGateway{author: "reviewer", currentLoginErr: fmt.Errorf("gh auth failed"), labels: []string{"looper:target:red"}, reviewRequestUsers: []networkpolicy.GitHubUser{}}
	cfg := config.Config{
		Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42},
		Projects: []config.ProjectRefConfig{{
			ID:       "project_1",
			Name:     "Demo",
			RepoPath: "/tmp/repo",
			Network:  config.ProjectNetworkConfig{Mode: config.NetworkModeRouted},
			Roles:    &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{Discovery: &config.PartialReviewerRoleDiscoveryConfig{Triggers: &config.PartialReviewerRoleTriggersConfig{EnableSelfReview: &enableSelfReview, RequireReviewRequest: &requireReviewRequest}}}},
		}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})
	repo := "acme/looper"
	prNumber := int64(42)
	err := runner.revalidateRoutedReviewerClaim(context.Background(), storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repo"}, storage.QueueItemRecord{Repo: &repo, PRNumber: &prNumber})
	var loopErr *loopError
	if !errors.As(err, &loopErr) || loopErr.kind != FailureRetryableTransient || !strings.Contains(loopErr.message, "gh auth failed") {
		t.Fatalf("revalidateRoutedReviewerClaim() error = %v, want transient login refresh failure", err)
	}
	if github.currentLoginCalls != 1 {
		t.Fatalf("GetCurrentUserLogin calls = %d, want 1", github.currentLoginCalls)
	}
}

func TestRevalidateRoutedReviewerClaimBypassesHoldForManualLoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := "pr:acme/looper:42"
	metadata := `{"manual":true,"followUpdates":true}`
	loopID := "loop_manual_routed_hold"
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: loopID, Seq: 7, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{labels: []string{domain.HoldLabelReviewer}}
	autoDiscovery := true
	cfg := config.Config{Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42}, Projects: []config.ProjectRefConfig{{ID: "project_1", RepoPath: "/tmp/repo", Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted}, Roles: &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{Discovery: &config.PartialReviewerRoleDiscoveryConfig{AutoDiscovery: &autoDiscovery}}}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})
	if err := runner.revalidateRoutedReviewerClaim(context.Background(), storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repo"}, storage.QueueItemRecord{LoopID: &loopID, Repo: &repo, PRNumber: &prNumber}); err != nil {
		t.Fatalf("revalidateRoutedReviewerClaim() error = %v", err)
	}
}

func TestProcessClaimedItemHeldManualRoutedReviewerLoopBypassesHoldAndRoutingGate(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := "pr:acme/looper:42"
	metadata := `{"manual":true,"followUpdates":true}`
	loopID := "loop_manual_routed_process_hold"
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: loopID, Seq: 10, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	projectID := "project_1"
	lockKey := loopTarget
	queue := storage.QueueItemRecord{ID: "queue_manual_routed_process_hold", ProjectID: &projectID, LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "reviewer:manual-routed-process-hold", Priority: storage.QueuePriorityReviewer, Status: "running", AvailableAt: nowISO, LockKey: &lockKey, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}
	autoDiscovery := true
	cfg := &config.Config{Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42}, Projects: []config.ProjectRefConfig{{ID: "project_1", RepoPath: t.TempDir(), Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted}, Roles: &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{Discovery: &config.PartialReviewerRoleDiscoveryConfig{AutoDiscovery: &autoDiscovery}}}}}}
	github := &fakeGitHubGateway{labels: []string{domain.HoldLabelReviewer}, hasConflicts: true}
	agent := &fakeAgentExecutor{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true}, CustomInstructions: cfg})

	result, err := runner.ProcessClaimedItem(context.Background(), queue)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !strings.Contains(result.Summary, "conflicted") || strings.Contains(result.Summary, "currently held") {
		t.Fatalf("result = %#v, want conflicted skip without hold skip", result)
	}
	if len(agent.starts) != 0 {
		t.Fatalf("agent.starts = %#v, want none", agent.starts)
	}
}

func TestProcessClaimedQueueItemRoutedRevalidationFailureDoesNotLeaveRunningRun(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := "pr:acme/looper:42"
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_reviewer_routed_revalidate_fail", Seq: 5, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	projectID := "project_1"
	loopID := "loop_reviewer_routed_revalidate_fail"
	lockKey := loopTarget
	queue := storage.QueueItemRecord{ID: "queue_reviewer_routed_revalidate_fail", ProjectID: &projectID, LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "reviewer:routed-revalidate-fail", Priority: storage.QueuePriorityReviewer, Status: "running", AvailableAt: nowISO, LockKey: &lockKey, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	autoDiscovery := true
	requireReviewRequest := false
	enableSelfReview := true
	cfg := &config.Config{Network: config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42}, Projects: []config.ProjectRefConfig{{ID: "project_1", RepoPath: t.TempDir(), Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted}, Roles: &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{Discovery: &config.PartialReviewerRoleDiscoveryConfig{AutoDiscovery: &autoDiscovery, Triggers: &config.PartialReviewerRoleTriggersConfig{EnableSelfReview: &enableSelfReview, RequireReviewRequest: &requireReviewRequest}}}}}}}
	github := &fakeGitHubGateway{author: "reviewer", currentLoginErr: fmt.Errorf("gh auth failed"), labels: []string{"looper:target:red"}, reviewRequestUsers: []networkpolicy.GitHubUser{}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true}, CustomInstructions: cfg})

	result, err := runner.ProcessClaimedQueueItem(context.Background(), queue)
	if err == nil || !strings.Contains(err.Error(), "gh auth failed") {
		t.Fatalf("ProcessClaimedQueueItem() error = %v, want routed revalidation failure", err)
	}
	if result != nil {
		t.Fatalf("result = %#v, want nil on claim setup failure", result)
	}
	runs, err := fixture.repos.Runs.ListByLoop(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Runs.ListByLoop() error = %v", err)
	}
	for _, run := range runs {
		if run.Status == "running" {
			t.Fatalf("runs = %#v, want no running run", runs)
		}
	}
}

func TestDiscoverPullRequestReusesExistingActiveQueueItem(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	first, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: "acme/looper", PRNumber: 42})
	if err != nil {
		t.Fatalf("first DiscoverPullRequest() error = %v", err)
	}
	second, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: "acme/looper", PRNumber: 42})
	if err != nil {
		t.Fatalf("second DiscoverPullRequest() error = %v", err)
	}
	if len(first.QueueItems) != 1 || len(second.QueueItems) != 0 || second.Skipped != 1 {
		t.Fatalf("first=%#v second=%#v, want duplicate targeted discovery skipped without new queue item", first, second)
	}
	queues, err := fixture.repos.Queue.List(context.Background())
	if err != nil {
		t.Fatalf("Queue.List() error = %v", err)
	}
	if len(queues) != 1 {
		t.Fatalf("len(Queue.List()) = %d, want one active queue item", len(queues))
	}
}

func TestDiscoverPullRequestsRecoversRetryableAfterResumeRestartFromDiscover(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	loopID, queueID := seedFailedReviewerRecoveryLoop(t, fixture, failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish: expected old, got new"})
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 || result.QueueItems[0].ID != queueID {
		t.Fatalf("QueueItems = %#v, want recovered existing queue item %s", result.QueueItems, queueID)
	}
	loop, _ := fixture.repos.Loops.GetByID(context.Background(), loopID)
	queue, _ := fixture.repos.Queue.GetByID(context.Background(), queueID)
	if loop == nil || loop.Status != "queued" || queue == nil || queue.Status != "queued" || queue.LastError != nil || queue.LastErrorKind != nil {
		t.Fatalf("loop=%#v queue=%#v, want queued loop and cleared failed queue metadata", loop, queue)
	}

	again, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("second DiscoverPullRequests() error = %v", err)
	}
	queues, _ := fixture.repos.Queue.List(context.Background())
	if len(again.QueueItems) != 1 || len(queues) != 1 {
		t.Fatalf("second result=%#v queues=%#v, want idempotent single active queue", again, queues)
	}
}

func TestReviewerFailedLoopRecoveryEligibilityWhitelist(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		seed failedReviewerRecoverySeed
		pr   PullRequestSummary
		want bool
	}{
		{name: "retryable rerun review", seed: failedReviewerRecoverySeed{ResumePolicy: "rerun_review", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "marker missing"}, pr: PullRequestSummary{Number: 42, State: "OPEN"}, want: true},
		{name: "retryable restart from discover", seed: failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish"}, pr: PullRequestSummary{Number: 42, State: "OPEN"}, want: true},
		{name: "retryable transient attempts remaining", seed: failedReviewerRecoverySeed{ResumePolicy: "replay_step", QueueErrorKind: string(FailureRetryableTransient), ErrorMessage: "reviewer agent timed out", QueueAttempts: 3, QueueMaxAttempts: 5}, pr: PullRequestSummary{Number: 42, State: "OPEN"}, want: true},
		{name: "historical guardrail non retryable", seed: failedReviewerRecoverySeed{ResumePolicy: "replay_step", QueueErrorKind: string(FailureNonRetryable), ErrorMessage: "review request removed before publish"}, pr: PullRequestSummary{Number: 42, State: "OPEN"}, want: true},
		{name: "retryable transient ignores final allowed run", seed: failedReviewerRecoverySeed{ResumePolicy: "replay_step", QueueErrorKind: string(FailureRetryableTransient), ErrorMessage: "reviewer agent timed out", QueueAttempts: 4, QueueMaxAttempts: 5}, pr: PullRequestSummary{Number: 42, State: "OPEN"}, want: true},
		{name: "manual intervention kind", seed: failedReviewerRecoverySeed{ResumePolicy: "replay_step", QueueErrorKind: string(FailureManualIntervention), ErrorMessage: "operator needed"}, pr: PullRequestSummary{Number: 42, State: "OPEN"}, want: false},
		{name: "manual intervention resume policy", seed: failedReviewerRecoverySeed{ResumePolicy: "manual_intervention", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "operator needed"}, pr: PullRequestSummary{Number: 42, State: "OPEN"}, want: false},
		{name: "closed pr", seed: failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish"}, pr: PullRequestSummary{Number: 42, State: "CLOSED"}, want: false},
		{name: "approved by current user on head", seed: failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish"}, pr: PullRequestSummary{Number: 42, State: "OPEN", ReviewDecision: "APPROVED", HeadSHA: "abc123", Reviews: []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "APPROVED", "commit": map[string]any{"oid": "abc123"}}}}, want: false},
		{name: "approved by another user", seed: failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish"}, pr: PullRequestSummary{Number: 42, State: "OPEN", ReviewDecision: "APPROVED", HeadSHA: "abc123", Reviews: []map[string]any{{"author": map[string]any{"login": "other"}, "state": "APPROVED", "commit": map[string]any{"oid": "abc123"}}}}, want: true},
		{name: "ready label", seed: failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish"}, pr: PullRequestSummary{Number: 42, State: "OPEN", Labels: []string{specpr.ReadyLabel}}, want: false},
		{name: "follow updates disabled", seed: failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish", FollowUpdates: boolPtr(false)}, pr: PullRequestSummary{Number: 42, State: "OPEN"}, want: false},
		{name: "loop disabled", seed: failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish", LoopEnabled: boolPtr(false)}, pr: PullRequestSummary{Number: 42, State: "OPEN"}, want: false},
		{name: "legacy budget termination metadata", seed: failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish", TerminationReason: "max_wall_clock"}, pr: PullRequestSummary{Number: 42, State: "OPEN"}, want: true},
		{name: "attempt cap", seed: failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish", AutoRecoveryAttempts: config.DefaultReviewerAutoRecoveryMaxAttempts}, pr: PullRequestSummary{Number: 42, State: "OPEN"}, want: false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			loopID, _ := seedFailedReviewerRecoveryLoop(t, fixture, tt.seed)
			runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25, StopOnApproved: true, StopOnReadyLabel: true}})
			loop, _ := fixture.repos.Loops.GetByID(context.Background(), loopID)
			eligible, _, _, err := runner.failedReviewerLoopRecoveryEligibility(context.Background(), *loop, tt.pr)
			if err != nil {
				t.Fatalf("failedReviewerLoopRecoveryEligibility() error = %v", err)
			}
			if eligible != tt.want {
				t.Fatalf("eligible = %v, want %v", eligible, tt.want)
			}
		})
	}
}

func TestReviewerFailedLoopRecoveryEligibilityUsesFreshDetailHeadForApproval(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	loopID, _ := seedFailedReviewerRecoveryLoop(t, fixture, failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish"})
	github := &fakeGitHubGateway{
		viewHeadSHA: "fresh-head",
		reviews: []map[string]any{{
			"author": map[string]any{"login": "octocat"},
			"state":  "APPROVED",
			"commit": map[string]any{"oid": "fresh-head"},
		}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25, StopOnApproved: true, StopOnReadyLabel: true}})
	loop, _ := fixture.repos.Loops.GetByID(context.Background(), loopID)

	eligible, _, reason, err := runner.failedReviewerLoopRecoveryEligibility(context.Background(), *loop, PullRequestSummary{Number: 42, State: "OPEN", HeadSHA: "stale-head"})
	if err != nil {
		t.Fatalf("failedReviewerLoopRecoveryEligibility() error = %v", err)
	}
	if eligible || reason != "approved" {
		t.Fatalf("eligible=%v reason=%q, want approved suppression", eligible, reason)
	}
	if github.viewCalls != 1 {
		t.Fatalf("viewCalls = %d, want 1", github.viewCalls)
	}
}

func TestReviewerFailedLoopRecoveryEnhancedTransientIsOptIn(t *testing.T) {
	t.Parallel()
	message := `Post "https://api.github.com/graphql": EOF`
	pr := PullRequestSummary{Number: 42, State: "OPEN"}

	t.Run("default remains non recoverable", func(t *testing.T) {
		t.Parallel()
		fixture := newRunnerFixture(t)
		loopID, _ := seedFailedReviewerRecoveryLoop(t, fixture, failedReviewerRecoverySeed{ResumePolicy: "replay_step", QueueErrorKind: string(FailureNonRetryable), ErrorMessage: message, QueueAttempts: 1, QueueMaxAttempts: 5})
		runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
		loop, _ := fixture.repos.Loops.GetByID(context.Background(), loopID)
		eligible, _, reason, err := runner.failedReviewerLoopRecoveryEligibility(context.Background(), *loop, pr)
		if err != nil {
			t.Fatalf("failedReviewerLoopRecoveryEligibility() error = %v", err)
		}
		if eligible || reason != "not_whitelisted" {
			t.Fatalf("eligible=%v reason=%q, want not_whitelisted", eligible, reason)
		}
	})

	t.Run("enabled recovers and preserves attempt count", func(t *testing.T) {
		t.Parallel()
		fixture := newRunnerFixture(t)
		loopID, queueID := seedFailedReviewerRecoveryLoop(t, fixture, failedReviewerRecoverySeed{ResumePolicy: "replay_step", QueueErrorKind: string(FailureNonRetryable), ErrorMessage: message, QueueAttempts: 1, QueueMaxAttempts: 5})
		runner := New(Options{
			DB:            fixture.coordinator.DB(),
			Repos:         fixture.repos,
			GitHub:        &fakeGitHubGateway{},
			Git:           &fakeGitGateway{},
			AgentExecutor: &fakeAgentExecutor{},
			Logger:        fixture.logger,
			Now:           fixture.now,
			LoopConfig:    config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25},
			RetryPolicy: config.ReviewerRetryConfig{
				EnhancedTransientClassification: true,
				RecoverExistingMatchedFailures:  true,
			},
		})
		loop, _ := fixture.repos.Loops.GetByID(context.Background(), loopID)
		eligible, _, reason, err := runner.failedReviewerLoopRecoveryEligibility(context.Background(), *loop, pr)
		if err != nil {
			t.Fatalf("failedReviewerLoopRecoveryEligibility() error = %v", err)
		}
		if !eligible || reason != "enhanced_transient_match_attempts_remaining" {
			t.Fatalf("eligible=%v reason=%q, want enhanced transient recovery", eligible, reason)
		}
		if _, err := runner.recoverFailedReviewerLoop(context.Background(), *loop, pr); err != nil {
			t.Fatalf("recoverFailedReviewerLoop() error = %v", err)
		}
		queue, err := fixture.repos.Queue.GetByID(context.Background(), queueID)
		if err != nil || queue == nil {
			t.Fatalf("Queue.GetByID() = (%#v, %v), want queue", queue, err)
		}
		if queue.Status != "queued" || queue.Attempts != 1 || queue.LastError != nil || queue.LastErrorKind != nil {
			t.Fatalf("queue = %#v, want requeued with original attempts and cleared error", queue)
		}
	})
}

func TestReviewerEnhancedTransientPersistsExtractedShellStderr(t *testing.T) {
	t.Parallel()

	// Wrapped shell error whose generic exit-code Message hides the EOF text
	// living in Stderr — exactly the shape `gh api ...` produces on a flaky
	// network. Pre-fix, classifyFailureForProject persisted err.Error() ("Command
	// exited with code 1") and failedReviewerLoopRecoveryEligibility could no
	// longer match the persisted message against the enhanced-transient pattern.
	cause := &shell.CommandExecutionError{
		Message: "Command exited with code 1",
		Result: shell.Result{
			ExitCode: 1,
			Stderr:   `Post "https://api.github.com/graphql": EOF`,
		},
	}

	fixture := newRunnerFixture(t)
	runner := New(Options{
		DB:            fixture.coordinator.DB(),
		Repos:         fixture.repos,
		GitHub:        &fakeGitHubGateway{},
		Git:           &fakeGitGateway{},
		AgentExecutor: &fakeAgentExecutor{},
		Logger:        fixture.logger,
		Now:           fixture.now,
		LoopConfig:    config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25},
		RetryPolicy: config.ReviewerRetryConfig{
			EnhancedTransientClassification: true,
			RecoverExistingMatchedFailures:  true,
		},
	})

	classified := runner.classifyFailureForProject("", cause)
	if classified == nil || classified.kind != FailureRetryableTransient {
		t.Fatalf("classifyFailureForProject() = %#v, want retryable transient", classified)
	}
	if !strings.Contains(classified.message, "EOF") || !strings.Contains(classified.message, "/graphql") {
		t.Fatalf("classified.message = %q, want extracted stderr containing graphql EOF", classified.message)
	}
	if !runner.isEnhancedTransientMessageForPolicy(runner.retryPolicyForProject(""), classified.message) {
		t.Fatalf("persisted message %q should re-match enhanced transient pattern", classified.message)
	}

	loopID, queueID := seedFailedReviewerRecoveryLoop(t, fixture, failedReviewerRecoverySeed{
		ResumePolicy:     "replay_step",
		QueueErrorKind:   string(FailureNonRetryable),
		ErrorMessage:     classified.message,
		QueueAttempts:    1,
		QueueMaxAttempts: 5,
	})
	loop, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil || loop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", loop, err)
	}
	eligible, recoveredQueueID, reason, err := runner.failedReviewerLoopRecoveryEligibility(context.Background(), *loop, PullRequestSummary{Number: 42, State: "OPEN"})
	if err != nil {
		t.Fatalf("failedReviewerLoopRecoveryEligibility() error = %v", err)
	}
	if !eligible || reason != "enhanced_transient_match_attempts_remaining" || recoveredQueueID != queueID {
		t.Fatalf("eligible=%v queueID=%q reason=%q, want enhanced transient recovery on queue %q", eligible, recoveredQueueID, reason, queueID)
	}
}

func TestReviewerFailedLoopRecoveryUsesProjectRetryOverride(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	message := "project-only transient transport failure"
	loopID, _ := seedFailedReviewerRecoveryLoop(t, fixture, failedReviewerRecoverySeed{ResumePolicy: "replay_step", QueueErrorKind: string(FailureNonRetryable), ErrorMessage: message, QueueAttempts: 1, QueueMaxAttempts: 5})
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	enhanced := true
	recoverExisting := true
	patterns := []string{"project-only transient transport"}
	cfg.Projects = []config.ProjectRefConfig{{
		ID: "project_1",
		Roles: &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{Behavior: &config.PartialReviewerConfig{Retry: &config.PartialReviewerRetryConfig{
			EnhancedTransientClassification: &enhanced,
			RecoverExistingMatchedFailures:  &recoverExisting,
			ExtraTransientErrorPatterns:     &patterns,
		}}}},
	}}
	runner := New(Options{
		DB:                 fixture.coordinator.DB(),
		Repos:              fixture.repos,
		GitHub:             &fakeGitHubGateway{},
		Git:                &fakeGitGateway{},
		AgentExecutor:      &fakeAgentExecutor{},
		Logger:             fixture.logger,
		Now:                fixture.now,
		LoopConfig:         config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25},
		RetryPolicy:        cfg.Roles.Reviewer.Behavior.Retry,
		CustomInstructions: &cfg,
	})
	loop, _ := fixture.repos.Loops.GetByID(context.Background(), loopID)
	eligible, _, reason, err := runner.failedReviewerLoopRecoveryEligibility(context.Background(), *loop, PullRequestSummary{Number: 42, State: "OPEN"})
	if err != nil {
		t.Fatalf("failedReviewerLoopRecoveryEligibility() error = %v", err)
	}
	if !eligible || reason != "enhanced_transient_match_attempts_remaining" {
		t.Fatalf("eligible=%v reason=%q, want project retry override recovery", eligible, reason)
	}
}

func TestReviewerFailedLoopRecoveryEligibilityHonorsStopOnConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		loopConfig config.ReviewerLoopConfig
		pr         PullRequestSummary
	}{
		{name: "approved allowed", loopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25, StopOnApproved: false, StopOnReadyLabel: true}, pr: PullRequestSummary{Number: 42, State: "OPEN", ReviewDecision: "APPROVED"}},
		{name: "ready label allowed", loopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25, StopOnApproved: true, StopOnReadyLabel: false}, pr: PullRequestSummary{Number: 42, State: "OPEN", Labels: []string{specpr.ReadyLabel}}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			loopID, _ := seedFailedReviewerRecoveryLoop(t, fixture, failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish: expected old, got new"})
			runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: tt.loopConfig})
			loop, _ := fixture.repos.Loops.GetByID(context.Background(), loopID)
			eligible, _, _, err := runner.failedReviewerLoopRecoveryEligibility(context.Background(), *loop, tt.pr)
			if err != nil {
				t.Fatalf("failedReviewerLoopRecoveryEligibility() error = %v", err)
			}
			if !eligible {
				t.Fatalf("eligible = false, want true")
			}
		})
	}
}

func TestReviewerFailedLoopRecoveryEligibilitySkipsCurrentLoginForLocalBlockers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		seed       failedReviewerRecoverySeed
		wantReason string
	}{
		{name: "loop disabled", seed: failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish", LoopEnabled: boolPtr(false)}, wantReason: "loop_disabled"},
		{name: "attempt cap", seed: failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish", AutoRecoveryAttempts: config.DefaultReviewerAutoRecoveryMaxAttempts}, wantReason: "auto_recovery_attempt_cap"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			github := &fakeGitHubGateway{currentLoginErr: fmt.Errorf("gh auth failed")}
			loopID, _ := seedFailedReviewerRecoveryLoop(t, fixture, tt.seed)
			runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25, StopOnApproved: true, StopOnReadyLabel: true}})
			loop, _ := fixture.repos.Loops.GetByID(context.Background(), loopID)
			eligible, _, reason, err := runner.failedReviewerLoopRecoveryEligibility(context.Background(), *loop, PullRequestSummary{Number: 42, State: "OPEN", HeadSHA: "abc123", Reviews: []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "APPROVED", "commit": map[string]any{"oid": "abc123"}}}})
			if err != nil {
				t.Fatalf("failedReviewerLoopRecoveryEligibility() error = %v", err)
			}
			if eligible {
				t.Fatalf("eligible = true, want false")
			}
			if reason != tt.wantReason {
				t.Fatalf("reason = %q, want %q", reason, tt.wantReason)
			}
			if github.currentLoginCalls != 0 {
				t.Fatalf("currentLoginCalls = %d, want 0", github.currentLoginCalls)
			}
		})
	}
}

func TestReviewerFailedLoopRecoveryEligibilitySkipsCurrentLoginForDeterministicBlockers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		seed       failedReviewerRecoverySeed
		mutate     func(t *testing.T, fixture *runnerFixture, loopID string)
		wantReason string
	}{
		{
			name:       "latest queue not failed",
			seed:       failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish"},
			wantReason: "latest_queue_not_failed",
			mutate: func(t *testing.T, fixture *runnerFixture, loopID string) {
				t.Helper()
				queue, err := fixture.repos.Queue.GetLatestByLoopID(context.Background(), loopID)
				if err != nil || queue == nil {
					t.Fatalf("Queue.GetLatestByLoopID() = (%#v, %v), want queue", queue, err)
				}
				queue.Status = "queued"
				if err := fixture.repos.Queue.Upsert(context.Background(), *queue); err != nil {
					t.Fatalf("Queue.Upsert() error = %v", err)
				}
			},
		},
		{
			name:       "latest run not failed",
			seed:       failedReviewerRecoverySeed{ResumePolicy: "restart_from_discover", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "PR head changed before publish"},
			wantReason: "latest_run_not_failed",
			mutate: func(t *testing.T, fixture *runnerFixture, loopID string) {
				t.Helper()
				run, err := fixture.repos.Runs.GetLatestByLoopID(context.Background(), loopID)
				if err != nil || run == nil {
					t.Fatalf("Runs.GetLatestByLoopID() = (%#v, %v), want run", run, err)
				}
				run.Status = "completed"
				if err := fixture.repos.Runs.Upsert(context.Background(), *run); err != nil {
					t.Fatalf("Runs.Upsert() error = %v", err)
				}
			},
		},
		{name: "manual intervention kind", seed: failedReviewerRecoverySeed{ResumePolicy: "replay_step", QueueErrorKind: string(FailureManualIntervention), ErrorMessage: "operator needed"}, wantReason: "manual_intervention"},
		{name: "manual intervention resume policy", seed: failedReviewerRecoverySeed{ResumePolicy: "manual_intervention", QueueErrorKind: string(FailureRetryableAfterResume), ErrorMessage: "operator needed"}, wantReason: "manual_intervention"},
		{name: "not whitelisted", seed: failedReviewerRecoverySeed{ResumePolicy: "replay_step", QueueErrorKind: string(FailureNonRetryable), ErrorMessage: "marker missing"}, wantReason: "not_whitelisted"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			github := &fakeGitHubGateway{currentLoginErr: fmt.Errorf("gh auth failed")}
			loopID, _ := seedFailedReviewerRecoveryLoop(t, fixture, tt.seed)
			if tt.mutate != nil {
				tt.mutate(t, fixture, loopID)
			}
			runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25, StopOnApproved: true, StopOnReadyLabel: true}})
			loop, _ := fixture.repos.Loops.GetByID(context.Background(), loopID)
			eligible, _, reason, err := runner.failedReviewerLoopRecoveryEligibility(context.Background(), *loop, PullRequestSummary{Number: 42, State: "OPEN", HeadSHA: "abc123", Reviews: []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "APPROVED", "commit": map[string]any{"oid": "abc123"}}}})
			if err != nil {
				t.Fatalf("failedReviewerLoopRecoveryEligibility() error = %v", err)
			}
			if eligible {
				t.Fatalf("eligible = true, want false")
			}
			if reason != tt.wantReason {
				t.Fatalf("reason = %q, want %q", reason, tt.wantReason)
			}
			if github.currentLoginCalls != 0 {
				t.Fatalf("currentLoginCalls = %d, want 0", github.currentLoginCalls)
			}
		})
	}
}

func TestSummaryFromDetailPreservesReviewsForApprovalRecovery(t *testing.T) {
	t.Parallel()
	reviews := []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "APPROVED", "commit": map[string]any{"oid": "abc123"}}}

	summary := summaryFromDetail(PullRequestDetail{Number: 42, State: "OPEN", HeadSHA: "abc123", Reviews: reviews})

	if !hasApprovedReviewByAuthorForHead(summary.Reviews, "octocat", "abc123") {
		t.Fatalf("summary reviews = %#v, want current-user approved review preserved", summary.Reviews)
	}
}

func TestReviewDisclosureInstructionPreservesVisibleInlineGuidance(t *testing.T) {
	t.Parallel()
	disclosureCfg := config.DefaultDisclosureConfig()
	disclosureCfg.Channels.InlineCommentVisible = true

	instruction := reviewDisclosureInstruction(disclosureCfg, "opencode", "")
	if !strings.Contains(instruction, "Every inline review comment you post must also use looper's configured visible inline disclosure style") {
		t.Fatalf("instruction = %q, want visible inline disclosure guidance", instruction)
	}
	if strings.Contains(instruction, "Do not add looper disclosure footers or hidden looper stamp markers to inline review comments") {
		t.Fatalf("instruction = %q, should not suppress enabled inline disclosure", instruction)
	}
}

func TestReviewDisclosureInstructionUsesHiddenInlineMarkerWhenVisibleDisabled(t *testing.T) {
	t.Parallel()
	disclosureCfg := config.DefaultDisclosureConfig()
	disclosureCfg.Channels.InlineCommentVisible = false

	instruction := reviewDisclosureInstruction(disclosureCfg, "opencode", "")
	if !strings.Contains(instruction, "Every inline review comment you post must include only the hidden looper stamp marker") {
		t.Fatalf("instruction = %q, want hidden inline marker guidance", instruction)
	}
	if strings.Contains(instruction, "Do not add looper disclosure footers or hidden looper stamp markers to inline review comments") {
		t.Fatalf("instruction = %q, should not suppress enabled inline disclosure", instruction)
	}
}

func TestDiscoverPullRequestsAppliesLabelFilters(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{"needs-review", "spec"}, reviewRequests: []string{"octocat"}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{"needs-review", "spec"}, LabelMode: config.LabelModeAll, IncludeSpecReviewingLabel: false}})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 || *result.QueueItems[0].PRNumber != 42 {
		t.Fatalf("QueueItems = %#v, want only matching PR #42", result.QueueItems)
	}
	if len(github.listCalls) != 1 || strings.Join(github.listCalls[0].Labels, ",") != "needs-review,spec" {
		t.Fatalf("list calls = %#v, want server-side multi-label filter", github.listCalls)
	}
}

func TestListOpenPullRequestsForDiscoveryCapsAnyModeLabelsToLimit(t *testing.T) {
	t.Parallel()
	github := &fakeGitHubGateway{listOpenByLabel: map[string][]PullRequestSummary{
		"needs-review": {
			{Number: 42, State: "OPEN", Labels: []string{"needs-review"}},
			{Number: 43, State: "OPEN", Labels: []string{"needs-review"}},
		},
		"spec": {
			{Number: 44, State: "OPEN", Labels: []string{"spec"}},
			{Number: 45, State: "OPEN", Labels: []string{"spec"}},
		},
	}}
	runner := New(Options{GitHub: github, DiscoveryPolicy: DiscoveryPolicy{Labels: []string{"needs-review", "spec"}, LabelMode: config.LabelModeAny}})

	prs, err := runner.listOpenPullRequestsForDiscovery(context.Background(), "acme/looper", "/tmp/repo", 2)
	if err != nil {
		t.Fatalf("listOpenPullRequestsForDiscovery() error = %v", err)
	}
	if len(prs) != 2 || prs[0].Number != 42 || prs[1].Number != 43 {
		t.Fatalf("prs = %#v, want first two unique PRs capped to limit", prs)
	}
	if len(github.listCalls) != 1 || github.listCalls[0].Label != "needs-review" {
		t.Fatalf("list calls = %#v, want discovery to stop after reaching limit", github.listCalls)
	}
}

func TestListOpenPullRequestsForDiscoveryCapsAnyModeLabelsToDefaultLimit(t *testing.T) {
	t.Parallel()
	firstPage := make([]PullRequestSummary, 30)
	for i := range firstPage {
		firstPage[i] = PullRequestSummary{Number: int64(42 + i), State: "OPEN", Labels: []string{"needs-review"}}
	}
	github := &fakeGitHubGateway{listOpenByLabel: map[string][]PullRequestSummary{
		"needs-review": firstPage,
		"spec":         {{Number: 99, State: "OPEN", Labels: []string{"spec"}}},
	}}
	runner := New(Options{GitHub: github, DiscoveryPolicy: DiscoveryPolicy{Labels: []string{"needs-review", "spec"}, LabelMode: config.LabelModeAny}})

	prs, err := runner.listOpenPullRequestsForDiscovery(context.Background(), "acme/looper", "/tmp/repo", 0)
	if err != nil {
		t.Fatalf("listOpenPullRequestsForDiscovery() error = %v", err)
	}
	if len(prs) != 30 {
		t.Fatalf("len(prs) = %d, want default cap", len(prs))
	}
	if len(github.listCalls) != 1 || github.listCalls[0].Label != "needs-review" || github.listCalls[0].Limit != 30 {
		t.Fatalf("list calls = %#v, want discovery to use and stop at default limit", github.listCalls)
	}
}

func TestDiscoverPullRequestsReturnsCurrentUserLookupError(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLoginErr: fmt.Errorf("gh auth failed")}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err == nil || !strings.Contains(err.Error(), "gh auth failed") {
		t.Fatalf("DiscoverPullRequests() error = %v, want gh auth failed", err)
	}
	if len(result.QueueItems) != 0 || len(result.CreatedLoopIDs) != 0 {
		t.Fatalf("result = %#v, want no discovery results on auth error", result)
	}
}

func TestDiscoverPullRequestsSkipsSelfAuthoredPullRequestsByDefault(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "octocat", listOpenByLabel: map[string][]PullRequestSummary{"": {{Number: 42, Title: "Self review", State: "OPEN", Author: "octocat", HeadSHA: "abc123", ReviewRequests: []string{"octocat"}}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want self-authored PR skipped by default", result.QueueItems)
	}
	if result.Skipped == 0 {
		t.Fatalf("Skipped = %d, want self-authored PR counted as skipped", result.Skipped)
	}
}

func TestDiscoverPullRequestsRoutedModeRefreshesCurrentLoginBeforeSelfAuthoredCheck(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "new-user", listOpenByLabel: map[string][]PullRequestSummary{"": {{Number: 42, Title: "Self review", State: "OPEN", Author: "new-user", HeadSHA: "abc123", Labels: []string{"looper:target:red"}, ReviewRequests: []string{"stale-user"}, ReviewRequestUsers: []networkpolicy.GitHubUser{{Login: "new-user", ID: 42}}}}}}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Network = config.NetworkConfig{NodeName: "red", GitHubLogin: "stale-user", GitHubUserID: 42}
	cfg.Projects = []config.ProjectRefConfig{{ID: "project_1", Name: "Looper", RepoPath: t.TempDir(), Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 || result.Skipped == 0 {
		t.Fatalf("result = %#v, want self-authored routed PR skipped", result)
	}
	if github.currentLoginCalls != 1 {
		t.Fatalf("GetCurrentUserLogin calls = %d, want 1", github.currentLoginCalls)
	}
}

func TestDiscoverPullRequestsAllowsSelfAuthoredPullRequestsWhenEnableSelfReviewTrue(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "octocat", listOpenByLabel: map[string][]PullRequestSummary{"": {{Number: 42, Title: "Self review", State: "OPEN", Author: "octocat", HeadSHA: "abc123", ReviewRequests: []string{"octocat"}}}}}
	cfg := mustLoadReviewerRoleConfig(t, `{"roles":{"reviewer":{"triggers":{"enableSelfReview":true}}}}`)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 || result.QueueItems[0].PRNumber == nil || *result.QueueItems[0].PRNumber != 42 {
		t.Fatalf("QueueItems = %#v, want self-authored PR queued when enableSelfReview=true", result.QueueItems)
	}
}

func TestDiscoverPullRequestsAllowsRoutedSelfAuthoredPullRequestsWhenEnableSelfReviewTrue(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	enableSelfReview := true
	requireReviewRequest := false
	github := &fakeGitHubGateway{currentLogin: "octocat", listOpenByLabel: map[string][]PullRequestSummary{"": {{Number: 42, Title: "Self review", State: "OPEN", Author: "octocat", HeadSHA: "abc123", Labels: []string{"looper:target:red"}, ReviewRequestUsers: []networkpolicy.GitHubUser{}}}}}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Network = config.NetworkConfig{NodeName: "red", GitHubLogin: "octocat", GitHubUserID: 42}
	cfg.Projects = []config.ProjectRefConfig{{
		ID:       "project_1",
		Name:     "Demo",
		RepoPath: t.TempDir(),
		Network:  config.ProjectNetworkConfig{Mode: config.NetworkModeRouted},
		Roles:    &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{Discovery: &config.PartialReviewerRoleDiscoveryConfig{Triggers: &config.PartialReviewerRoleTriggersConfig{EnableSelfReview: &enableSelfReview, RequireReviewRequest: &requireReviewRequest}}}},
	}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 || result.QueueItems[0].PRNumber == nil || *result.QueueItems[0].PRNumber != 42 {
		t.Fatalf("QueueItems = %#v, want routed self-authored PR queued when self-review is enabled", result.QueueItems)
	}
}

func TestDiscoverPullRequestsPreservesPausedLoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_paused", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "paused", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.CreatedLoopIDs) != 0 || len(result.QueueItems) != 0 {
		t.Fatalf("result = %#v, want no created loops or queue items", result)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.Status != "paused" || persisted.NextRunAt != nil {
		t.Fatalf("loop = %#v, want paused loop with nil next run", persisted)
	}
	items, err := fixture.repos.Queue.List(context.Background())
	if err != nil {
		t.Fatalf("Queue.List() error = %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("len(Queue.List()) = %d, want 0", len(items))
	}
}

func TestDiscoverPullRequestsSkipsSpecLabelWhenCurrentUserIsNotRequested(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{specpr.ReviewingLabel}, reviewRequests: []string{"alice"}, currentLogin: "bob"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.CreatedLoopIDs) != 0 || len(result.QueueItems) != 0 {
		t.Fatalf("result = %#v, want no created loops or queue items", result)
	}
	items, err := fixture.repos.Queue.List(context.Background())
	if err != nil {
		t.Fatalf("Queue.List() error = %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("len(Queue.List()) = %d, want 0", len(items))
	}
}

func TestDiscoverPullRequestsAllowsSpecLabelWhenCurrentUserIsRequested(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{specpr.ReviewingLabel}, reviewRequests: []string{"bob"}, currentLogin: "bob"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("len(QueueItems) = %d, want 1", len(result.QueueItems))
	}
}

func TestDiscoverPullRequestsSkipsAutomaticFollowUpWhenCurrentUserIsNotRequested(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"alice"}, currentLogin: "bob"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true}`
	loop := storage.LoopRecord{ID: "loop_follow", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("len(QueueItems) = %d, want 0", len(result.QueueItems))
	}
}

func TestDiscoverPullRequestsAllowsThreadResolutionFollowUpWhenCurrentUserIsNotRequested(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"alice"}, currentLogin: "bob", reviewThreads: []ReviewThread{{ID: "thread_1", Comments: []ReviewThreadComment{{ID: "comment_1", Author: "bob", Body: "Please update this. <!-- looper:stamp v=1 -->", CommitOID: "old-head"}}}}}
	policy := defaultThreadResolutionPolicy(t)
	policy.Enabled = true
	policy.Mode = config.ReviewerThreadResolutionModeResolveObjective
	policy.RequireCurrentReviewRequest = false
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, ThreadResolution: policy})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true}`
	loop := storage.LoopRecord{ID: "loop_follow_thread_resolution", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("len(QueueItems) = %d, want 1", len(result.QueueItems))
	}
}

func TestDiscoverPullRequestsRequiresCurrentReviewRequestBeforeThreadResolutionFollowUp(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"alice"}, currentLogin: "bob", reviewThreads: []ReviewThread{{ID: "thread_1", Comments: []ReviewThreadComment{{ID: "comment_1", Author: "bob", Body: "Please update this. <!-- looper:stamp v=1 -->", CommitOID: "old-head"}}}}}
	policy := defaultThreadResolutionPolicy(t)
	policy.Enabled = true
	policy.Mode = config.ReviewerThreadResolutionModeResolveObjective
	policy.RequireCurrentReviewRequest = true
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, ThreadResolution: policy})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true}`
	loop := storage.LoopRecord{ID: "loop_follow_thread_resolution_requires_request", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("len(QueueItems) = %d, want 0", len(result.QueueItems))
	}
}

func TestDiscoverPullRequestsAllowsAutomaticFollowUpWhenCurrentUserIsRequested(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"bob"}, currentLogin: "bob"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true}`
	loop := storage.LoopRecord{ID: "loop_follow_requested", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("len(QueueItems) = %d, want 1", len(result.QueueItems))
	}
}

func TestDiscoverPullRequestsSkipsFollowUpLoopWhenReviewerHoldAppliedLive(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"bob"}, currentLogin: "bob", labels: []string{domain.HoldLabelReviewer}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true}`
	loop := storage.LoopRecord{ID: "loop_follow_hold", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 || result.Skipped == 0 {
		t.Fatalf("result = %#v, want held follow-up PR skipped", result)
	}
}

func TestDiscoverPullRequestsRequeuesFollowUpOnNewHeadWithoutFreshReviewRequest(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "bob", reviewRequests: []string{}, viewHeadSHA: "new-head"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"lastPublishedHeadSha":"old-head","loop":{"enabled":true,"iterationCount":1,"iterationsByHead":{"old-head":1}}}`
	loop := storage.LoopRecord{ID: "loop_follow_new_head_without_request", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "waiting", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("len(QueueItems) = %d, want 1", len(result.QueueItems))
	}
	if result.QueueItems[0].PayloadJSON == nil || !contains(*result.QueueItems[0].PayloadJSON, `"headSha":"new-head"`) {
		payload := ""
		if result.QueueItems[0].PayloadJSON != nil {
			payload = *result.QueueItems[0].PayloadJSON
		}
		t.Fatalf("queue payload = %q, want new head recorded", payload)
	}
	persistedLoop, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persistedLoop == nil || persistedLoop.Status != "queued" {
		t.Fatalf("loop after requeue = %#v, want queued", persistedLoop)
	}
}

func TestDiscoverPullRequestsSkipsDraftFollowUpWithoutTerminatingLoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"alice"}, currentLogin: "octocat", viewDraft: true}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true}`
	loop := storage.LoopRecord{ID: "loop_draft_follow", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("len(QueueItems) = %d, want 0", len(result.QueueItems))
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.Status != "completed" {
		t.Fatalf("loop = %#v, want completed follow-up loop preserved", persisted)
	}
}

func TestDiscoverPullRequestsDebouncesContinuousFollowUp(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"lastPublishedHeadSha":"old-head","loop":{"enabled":true,"iterationCount":1,"iterationsByHead":{"old-head":1}}}`
	loop := storage.LoopRecord{ID: "loop_debounce", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "waiting", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("len(QueueItems) = %d, want 1", len(result.QueueItems))
	}
	wantAvailableAt := eventlog.FormatJavaScriptISOString(fixture.now().Add(120 * time.Second))
	if result.QueueItems[0].AvailableAt != wantAvailableAt {
		t.Fatalf("AvailableAt = %q, want %q", result.QueueItems[0].AvailableAt, wantAvailableAt)
	}
	persistedLoop, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persistedLoop == nil || persistedLoop.Status != "queued" || persistedLoop.NextRunAt == nil || *persistedLoop.NextRunAt != wantAvailableAt {
		t.Fatalf("loop after debounce = %#v, want queued with delayed next run", persistedLoop)
	}

	result, err = runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() second error = %v", err)
	}
	if len(result.QueueItems) != 1 || result.QueueItems[0].ID == "" {
		t.Fatalf("second result = %#v, want one deduped queued item", result)
	}
	items, err := fixture.repos.Queue.List(context.Background())
	if err != nil {
		t.Fatalf("Queue.List() error = %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("len(Queue.List()) = %d, want deduped single item", len(items))
	}
	if items[0].AvailableAt != wantAvailableAt {
		t.Fatalf("deduped AvailableAt = %q, want original %q", items[0].AvailableAt, wantAvailableAt)
	}

	fixture.advance(30 * time.Second)
	result, err = runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() third error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("third len(QueueItems) = %d, want one deduped queued item", len(result.QueueItems))
	}
	if result.QueueItems[0].AvailableAt != wantAvailableAt {
		t.Fatalf("third AvailableAt = %q, want original %q", result.QueueItems[0].AvailableAt, wantAvailableAt)
	}
}

func TestDiscoverPullRequestsExtendsDebounceWhenQueuedFollowUpSeesNewHead(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, listHeadSHA: "head-one"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"lastPublishedHeadSha":"old-head","loop":{"enabled":true,"iterationCount":1,"iterationsByHead":{"old-head":1}}}`
	loop := storage.LoopRecord{ID: "loop_debounce_new_head", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "waiting", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("len(QueueItems) = %d, want 1", len(result.QueueItems))
	}
	firstAvailableAt := eventlog.FormatJavaScriptISOString(fixture.now().Add(120 * time.Second))
	if result.QueueItems[0].AvailableAt != firstAvailableAt {
		t.Fatalf("AvailableAt = %q, want %q", result.QueueItems[0].AvailableAt, firstAvailableAt)
	}

	fixture.advance(30 * time.Second)
	github.listHeadSHA = "head-two"
	result, err = runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() second error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("second len(QueueItems) = %d, want one deduped queued item", len(result.QueueItems))
	}
	extendedAvailableAt := eventlog.FormatJavaScriptISOString(fixture.now().Add(120 * time.Second))
	if result.QueueItems[0].AvailableAt != extendedAvailableAt {
		t.Fatalf("second AvailableAt = %q, want extended %q", result.QueueItems[0].AvailableAt, extendedAvailableAt)
	}
	items, err := fixture.repos.Queue.List(context.Background())
	if err != nil {
		t.Fatalf("Queue.List() error = %v", err)
	}
	if len(items) != 1 || items[0].AvailableAt != extendedAvailableAt {
		t.Fatalf("queue items = %#v, want single item rescheduled to %q", items, extendedAvailableAt)
	}
	persistedLoop, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persistedLoop == nil || persistedLoop.NextRunAt == nil || *persistedLoop.NextRunAt != extendedAvailableAt {
		t.Fatalf("loop after reschedule = %#v, want next run %q", persistedLoop, extendedAvailableAt)
	}
}

func TestDiscoverPullRequestsHonorsMinimumPublishInterval(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MinPublishIntervalSeconds: 1800, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	lastPublishedAt := eventlog.FormatJavaScriptISOString(fixture.now().Add(-10 * time.Minute))
	metadata := fmt.Sprintf(`{"followUpdates":true,"lastPublishedHeadSha":"old-head","lastPublishedAt":%q,"loop":{"enabled":true,"iterationCount":1,"iterationsByHead":{"old-head":1}}}`, lastPublishedAt)
	loop := storage.LoopRecord{ID: "loop_min_interval", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "waiting", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("len(QueueItems) = %d, want 1", len(result.QueueItems))
	}
	wantAvailableAt := eventlog.FormatJavaScriptISOString(fixture.now().Add(20 * time.Minute))
	if result.QueueItems[0].AvailableAt != wantAvailableAt {
		t.Fatalf("AvailableAt = %q, want min interval %q", result.QueueItems[0].AvailableAt, wantAvailableAt)
	}
}

func TestLoopEnabledTreatsLegacyMissingMetadataAsDisabled(t *testing.T) {
	t.Parallel()
	runner := New(Options{LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove, Blocking: config.ReviewerReviewEventRequestChanges}})

	if runner.loopEnabled(map[string]any{}) {
		t.Fatalf("loopEnabled(empty metadata) = true, want false for legacy persisted loop")
	}

	metadataJSON, err := runner.ensureLoopMetadataJSON(nil, "", "acme/looper", 42)
	if err != nil {
		t.Fatalf("ensureLoopMetadataJSON() error = %v", err)
	}
	meta := parseJSONObject(&metadataJSON)
	if !runner.loopEnabled(meta) {
		t.Fatalf("loopEnabled(ensured metadata) = false, want creation-time default true")
	}
	if enabled, ok := meta["followUpdates"].(bool); !ok || !enabled {
		t.Fatalf("followUpdates = %#v, want true", meta["followUpdates"])
	}
	reviewEvents, _ := meta["reviewEvents"].(map[string]any)
	if reviewEvents["clean"] != string(config.ReviewerReviewEventApprove) || reviewEvents["blocking"] != string(config.ReviewerReviewEventRequestChanges) {
		t.Fatalf("reviewEvents = %#v, want snapshotted decision policy", reviewEvents)
	}
	current := `{"loop":{"enabled":true,"status":"terminated","terminationReason":"max_wall_clock","maxIterationsPerPR":2,"maxIterationsPerHead":1,"maxWallClockSeconds":60,"maxConsecutiveFailures":3,"maxAgentExecutionsPerPR":25}}`
	metadataJSON, err = runner.ensureLoopMetadataJSON(&current, "", "acme/looper", 42)
	if err != nil {
		t.Fatalf("ensureLoopMetadataJSON(legacy budget metadata) error = %v", err)
	}
	loopMeta := reviewerLoopMetadata(parseJSONObject(&metadataJSON))
	for _, key := range deprecatedReviewerLoopBudgetMetadataKeys {
		if _, ok := loopMeta[key]; ok {
			t.Fatalf("loop metadata retained deprecated budget key %q: %#v", key, loopMeta)
		}
	}
	if _, ok := loopMeta["terminationReason"]; ok {
		t.Fatalf("loop metadata retained budget termination reason: %#v", loopMeta)
	}
	if loopMeta["status"] != "active" {
		t.Fatalf("loop metadata status = %#v, want active after removing budget termination", loopMeta["status"])
	}
	current = `{"reviewEvents":{"clean":"BOGUS","blocking":"APPROVE"}}`
	metadataJSON, err = runner.ensureLoopMetadataJSON(&current, "", "acme/looper", 42)
	if err == nil || !strings.Contains(err.Error(), "reviewEvents.clean") {
		t.Fatalf("ensureLoopMetadataJSON(invalid reviewEvents) error = %v, want validation error", err)
	}
	current = `{"reviewEvents":{"clean":123}}`
	metadataJSON, err = runner.ensureLoopMetadataJSON(&current, "", "acme/looper", 42)
	if err == nil || !strings.Contains(err.Error(), "reviewEvents.clean") {
		t.Fatalf("ensureLoopMetadataJSON(malformed reviewEvents) error = %v, want validation error", err)
	}
}

func TestEnsureLoopMetadataJSONUsesProjectReviewEvents(t *testing.T) {
	t.Parallel()
	// Global runner default is COMMENT/COMMENT; the project overrides to APPROVE/REQUEST_CHANGES.
	projectCfg := config.Config{
		Roles: config.RoleConfigs{Reviewer: config.ReviewerRoleConfig{Behavior: config.ReviewerConfig{
			ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment},
		}}},
		Projects: []config.ProjectRefConfig{{
			ID: "forgejo-native", Name: "Forgejo", Repo: "owner/forgejo", RepoPath: "/tmp/forgejo",
			Roles: &config.PartialRoleConfigs{Reviewer: &config.PartialReviewerRoleConfig{
				Behavior: &config.PartialReviewerConfig{ReviewEvents: &config.PartialReviewerReviewEventsConfig{
					Clean:    reviewEventPtr(config.ReviewerReviewEventApprove),
					Blocking: reviewEventPtr(config.ReviewerReviewEventRequestChanges),
				}},
			}},
		}},
	}
	runner := New(Options{
		ReviewEvents:       config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment},
		LoopConfig:         config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25},
		CustomInstructions: &projectCfg,
	})

	metadataJSON, err := runner.ensureLoopMetadataJSON(nil, "forgejo-native", "owner/forgejo", 7)
	if err != nil {
		t.Fatalf("ensureLoopMetadataJSON() error = %v", err)
	}
	meta := parseJSONObject(&metadataJSON)
	reviewEvents, _ := meta["reviewEvents"].(map[string]any)
	if reviewEvents["clean"] != string(config.ReviewerReviewEventApprove) || reviewEvents["blocking"] != string(config.ReviewerReviewEventRequestChanges) {
		t.Fatalf("reviewEvents = %#v, want project-level APPROVE/REQUEST_CHANGES", reviewEvents)
	}
	// effectiveReviewEvents without snapshotted metadata must also resolve project overrides
	// so the trusted proxy policy is not stuck on the global COMMENT default.
	effective := runner.effectiveReviewEvents("forgejo-native", nil)
	if effective.Clean != config.ReviewerReviewEventApprove || effective.Blocking != config.ReviewerReviewEventRequestChanges {
		t.Fatalf("effectiveReviewEvents() = %#v, want project-level APPROVE/REQUEST_CHANGES", effective)
	}
	// Other projects fall back to the runner/global COMMENT default.
	global := runner.effectiveReviewEvents("missing", nil)
	if global.Clean != config.ReviewerReviewEventComment || global.Blocking != config.ReviewerReviewEventComment {
		t.Fatalf("effectiveReviewEvents(missing) = %#v, want global COMMENT defaults", global)
	}
}

func TestRecordLoopRunStartMetadataRefreshesInheritedReviewEventsForNewClaim(t *testing.T) {
	t.Parallel()
	runner := New(Options{ReviewEvents: config.ReviewerReviewEventsConfig{
		Clean:    config.ReviewerReviewEventApprove,
		Blocking: config.ReviewerReviewEventRequestChanges,
	}})
	queuedUnderOldConfig := `{"reviewEvents":{"clean":"COMMENT","blocking":"COMMENT"},"loop":{"status":"queued"}}`

	metadataJSON, err := runner.recordLoopRunStartMetadata(&queuedUnderOldConfig, "project_1")
	if err != nil {
		t.Fatalf("recordLoopRunStartMetadata() error = %v", err)
	}
	got := runner.effectiveReviewEvents("project_1", &metadataJSON)
	if got.Clean != config.ReviewerReviewEventApprove || got.Blocking != config.ReviewerReviewEventRequestChanges {
		t.Fatalf("claimed review events = %#v, want current claim snapshot APPROVE/REQUEST_CHANGES", got)
	}
}

func TestRecordLoopRunStartMetadataPreservesManualReviewEventOverrides(t *testing.T) {
	t.Parallel()
	runner := New(Options{ReviewEvents: config.ReviewerReviewEventsConfig{
		Clean:    config.ReviewerReviewEventApprove,
		Blocking: config.ReviewerReviewEventRequestChanges,
	}})
	manualOverride := `{"manual":true,"reviewEvents":{"clean":"COMMENT","blocking":"COMMENT"},"loop":{"status":"queued"}}`

	metadataJSON, err := runner.recordLoopRunStartMetadata(&manualOverride, "project_1")
	if err != nil {
		t.Fatalf("recordLoopRunStartMetadata() error = %v", err)
	}
	got := runner.effectiveReviewEvents("project_1", &metadataJSON)
	if got.Clean != config.ReviewerReviewEventComment || got.Blocking != config.ReviewerReviewEventComment {
		t.Fatalf("manual review events = %#v, want explicit COMMENT/COMMENT overrides", got)
	}
}

func reviewEventPtr(event config.ReviewerReviewEvent) *config.ReviewerReviewEvent {
	return &event
}

func TestEnsureLoopForPullRequestBackfillsLegacyFollowUpdatesDisabled(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_legacy", Seq: 1, ProjectID: project.ID, Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.ensureLoopForPullRequest(context.Background(), *project, repo, prNumber, &loop)
	if err != nil {
		t.Fatalf("ensureLoopForPullRequest() error = %v", err)
	}
	meta := parseJSONObject(result.record.MetadataJSON)
	if runner.loopEnabled(meta) {
		t.Fatalf("loopEnabled(backfilled legacy metadata) = true, want false")
	}
	if enabled, ok := meta["followUpdates"].(bool); !ok || enabled {
		t.Fatalf("followUpdates = %#v, want false", meta["followUpdates"])
	}
}

func TestEnsureLoopForPullRequestReactivatesLegacyBudgetTerminatedLoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: testReviewerLoopConfig()})
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"loop":{"enabled":true,"status":"terminated","terminationReason":"max_wall_clock","maxIterationsPerPR":2,"maxWallClockSeconds":60}}`
	loop := storage.LoopRecord{ID: "loop_budget_terminated", Seq: 1, ProjectID: project.ID, Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "terminated", MetadataJSON: &metadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.ensureLoopForPullRequest(context.Background(), *project, repo, prNumber, &loop)
	if err != nil {
		t.Fatalf("ensureLoopForPullRequest() error = %v", err)
	}
	if result.record.Status != "queued" || result.record.NextRunAt == nil {
		t.Fatalf("loop = %#v, want queued legacy budget loop", result.record)
	}
	loopMeta := reviewerLoopMetadata(parseJSONObject(result.record.MetadataJSON))
	if _, ok := loopMeta["terminationReason"]; ok {
		t.Fatalf("loop metadata retained budget termination reason: %#v", loopMeta)
	}
	if loopMeta["status"] != "active" {
		t.Fatalf("loop metadata status = %#v, want active", loopMeta["status"])
	}
}

func TestProcessClaimedItemParksExhaustedBudgetBeforeRun(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	projectID := "project_1"
	loopID := "loop_reviewer_budget_entry"
	metadata := `{"loop":{"iterationCount":1}}`
	reviewer := storage.LoopRecord{ID: loopID, Seq: 1, ProjectID: projectID, Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	fixer := storage.LoopRecord{ID: "loop_fixer_budget_entry", Seq: 2, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "waiting", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("Loops.Upsert(reviewer) error = %v", err)
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Loops.Upsert(fixer) error = %v", err)
	}
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_reviewer_budget_entry", ProjectID: &projectID, LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: target, Repo: &repo, PRNumber: &prNumber, DedupeKey: "reviewer:budget-entry", Priority: storage.QueuePriorityReviewer, Status: "running", AvailableAt: nowISO, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 1
	cfg.HITL.Enabled = true
	agent := &fakeAgentExecutor{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg})

	result, err := runner.ProcessClaimedItem(context.Background(), storage.QueueItemRecord{ID: "queue_reviewer_budget_entry", ProjectID: &projectID, LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: target, Repo: &repo, PRNumber: &prNumber, Status: "running"})
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" {
		t.Fatalf("result = %#v, want skipped at claim start", result)
	}
	if len(agent.starts) != 0 {
		t.Fatalf("agent.starts = %#v, want none after lowered-cap claim", agent.starts)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil || updated == nil || updated.Status != "awaiting_human" {
		t.Fatalf("reviewer = (%#v, %v), want awaiting_human", updated, err)
	}
	sibling, err := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
	if err != nil || sibling == nil || sibling.Status != "paused" || !loops.IsSiblingReviewFixBudgetPause(sibling.MetadataJSON) {
		t.Fatalf("fixer sibling = (%#v, %v), want paused sibling hold", sibling, err)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), "queue_reviewer_budget_entry")
	if err != nil || queue == nil || queue.Status != "cancelled" {
		t.Fatalf("queue = (%#v, %v), want cancelled claimed item", queue, err)
	}
	runs, err := fixture.repos.Runs.ListByLoop(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Runs.ListByLoop() error = %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("runs = %#v, want no run after restart/claim park", runs)
	}
}

func TestProcessClaimedQueueItemParksExhaustedBudgetDespiteGitHubTransportFailure(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	projectID := "project_1"
	loopID := "loop_reviewer_budget_transport"
	metadata := `{"loop":{"iterationCount":1}}`
	reviewer := storage.LoopRecord{ID: loopID, Seq: 1, ProjectID: projectID, Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	fixer := storage.LoopRecord{ID: "loop_fixer_budget_transport", Seq: 2, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "waiting", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("Loops.Upsert(reviewer) error = %v", err)
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Loops.Upsert(fixer) error = %v", err)
	}
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_reviewer_budget_transport", ProjectID: &projectID, LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: target, Repo: &repo, PRNumber: &prNumber, DedupeKey: "reviewer:budget-transport", Priority: storage.QueuePriorityReviewer, Status: "running", AvailableAt: nowISO, MaxAttempts: -1, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 1
	cfg.HITL.Enabled = false
	agent := &fakeAgentExecutor{}
	github := &fakeGitHubGateway{viewErrs: []error{&shell.CommandExecutionError{Message: "Command exited with code 1", Result: shell.Result{Stderr: "error connecting to api.github.com\ncheck your internet connection or https://githubstatus.com"}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg})

	result, err := runner.ProcessClaimedQueueItem(context.Background(), storage.QueueItemRecord{ID: "queue_reviewer_budget_transport", ProjectID: &projectID, LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: target, Repo: &repo, PRNumber: &prNumber, Status: "running"})
	if err != nil || result == nil || result.Status != "skipped" {
		t.Fatalf("ProcessClaimedItem() = (%#v, %v), want exhausted-budget skip", result, err)
	}
	if len(agent.starts) != 0 {
		t.Fatalf("agent.starts = %#v, want none", agent.starts)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil || updated == nil {
		t.Fatalf("reviewer = (%#v, %v)", updated, err)
	}
	if updated.Status != "paused" || !loops.IsReviewFixBudgetHold(*updated) {
		t.Fatalf("reviewer = %#v, want exhausted budget held regardless of transport failure", updated)
	}
	sibling, err := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
	if err != nil || sibling == nil || sibling.Status != "paused" || !loops.IsSiblingReviewFixBudgetPause(sibling.MetadataJSON) {
		t.Fatalf("fixer sibling = (%#v, %v), want budget sibling paused", sibling, err)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), "queue_reviewer_budget_transport")
	if err != nil || queue == nil || queue.Status != "cancelled" {
		t.Fatalf("queue = (%#v, %v), want cancelled exhausted-budget queue", queue, err)
	}
	if got := loops.ReviewerPublishCount(updated.MetadataJSON); got != 1 {
		t.Fatalf("publish count = %d, want unchanged successful publish count", got)
	}

}

func TestDiscoverPullRequestsParksExhaustedBudgetDespiteRetryableTransientQueue(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	projectID := "project_1"
	loopID := "loop_reviewer_budget_discover_retry"
	metadata := `{"followUpdates":true,"loop":{"enabled":true,"iterationCount":3}}`
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: loopID, Seq: 7, ProjectID: projectID, Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	kind := string(FailureRetryableTransient)
	lastErr := "Command exited with code 1: error connecting to api.github.com"
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_reviewer_budget_discover_retry", ProjectID: &projectID, LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: target, Repo: &repo, PRNumber: &prNumber, DedupeKey: "reviewer:budget-discover-retry", Priority: storage.QueuePriorityReviewer, Status: "queued", AvailableAt: nowISO, Attempts: 3, MaxAttempts: -1, LastError: &lastErr, LastErrorKind: &kind, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 3
	cfg.HITL.Enabled = false
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{currentLogin: "octocat", reviewRequests: []string{"octocat"}}, Git: &fakeGitGateway{},
		AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now,
		DiscoveryPolicy:    DiscoveryPolicy{AutoDiscovery: true, RequireReviewRequest: false, EnableSelfReview: true},
		LoopConfig:         cfg.Roles.Reviewer.Behavior.Loop,
		CustomInstructions: &cfg,
	})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: projectID, Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil || updated == nil {
		t.Fatalf("GetByID = (%#v, %v)", updated, err)
	}
	if updated.Status != "paused" || !loops.IsReviewFixBudgetHold(*updated) {
		t.Fatalf("loop = %#v discovery=%#v, want exhausted budget held", updated, result)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), "queue_reviewer_budget_discover_retry")
	if err != nil || queue == nil || queue.Status != "cancelled" {
		t.Fatalf("queue = (%#v, %v), want cancelled after budget park", queue, err)
	}
}

func TestProcessClaimedItemDoesNotParkBudgetWhenClaimedAfterPRClosed(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	projectID := "project_1"
	loopID := "loop_reviewer_budget_claim_closed"
	metadata := `{"loop":{"iterationCount":1}}`
	reviewer := storage.LoopRecord{ID: loopID, Seq: 1, ProjectID: projectID, Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	fixer := storage.LoopRecord{ID: "loop_fixer_budget_claim_closed", Seq: 2, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "waiting", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("Loops.Upsert(reviewer) error = %v", err)
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Loops.Upsert(fixer) error = %v", err)
	}
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_reviewer_budget_claim_closed", ProjectID: &projectID, LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: target, Repo: &repo, PRNumber: &prNumber, DedupeKey: "reviewer:budget-claim-closed", Priority: storage.QueuePriorityReviewer, Status: "running", AvailableAt: nowISO, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 1
	cfg.HITL.Enabled = true
	agent := &fakeAgentExecutor{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{viewState: "MERGED"}, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg})

	result, err := runner.ProcessClaimedItem(context.Background(), storage.QueueItemRecord{ID: "queue_reviewer_budget_claim_closed", ProjectID: &projectID, LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: target, Repo: &repo, PRNumber: &prNumber, Status: "running"})
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" {
		t.Fatalf("result = %#v, want skipped after closed-PR claim", result)
	}
	if len(agent.starts) != 0 {
		t.Fatalf("agent.starts = %#v, want none after closed-PR claim", agent.starts)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil || updated == nil || updated.Status != "terminated" {
		t.Fatalf("reviewer = (%#v, %v), want terminated product terminal", updated, err)
	}
	if reason, _ := stringFromAny(reviewerLoopMetadata(parseJSONObject(updated.MetadataJSON))["terminationReason"]); reason != "pr_closed_or_merged" {
		t.Fatalf("terminationReason = %q, want pr_closed_or_merged", reason)
	}
	if ask, ok := loops.ReadHITLAsk(updated.MetadataJSON); ok && loops.IsReviewFixBudgetAsk(ask) {
		t.Fatalf("HITL ask = %#v, want no budget ask on a closed PR", ask)
	}
	sibling, err := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
	if err != nil || sibling == nil || sibling.Status != "waiting" || loops.IsSiblingReviewFixBudgetPause(sibling.MetadataJSON) {
		t.Fatalf("fixer sibling = (%#v, %v), want still waiting without budget pause", sibling, err)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), "queue_reviewer_budget_claim_closed")
	if err != nil || queue == nil || queue.Status != "cancelled" {
		t.Fatalf("queue = (%#v, %v), want cancelled claimed item", queue, err)
	}
	runs, err := fixture.repos.Runs.ListByLoop(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Runs.ListByLoop() error = %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("runs = %#v, want no run after closed-PR claim", runs)
	}
}

func TestProcessClaimedItemDoesNotParkBudgetWhenPublishClosesPR(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	fixer := storage.LoopRecord{ID: "loop_fixer_budget_closed_pr", Seq: 2, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "waiting", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Loops.Upsert(fixer) error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 1
	cfg.HITL.Enabled = true
	github := &fakeGitHubGateway{closeAfterReviewMarker: true, reviewRequests: []string{"octocat"}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Please add tests", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true, LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success after published review", result)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil || loop == nil || loop.Status != "terminated" {
		t.Fatalf("reviewer = (%#v, %v), want terminated product terminal", loop, err)
	}
	if loops.ReviewerPublishCount(loop.MetadataJSON) < 1 {
		t.Fatalf("publish count = %d, want cap-reaching publish before closed-PR terminal", loops.ReviewerPublishCount(loop.MetadataJSON))
	}
	if reason, _ := stringFromAny(reviewerLoopMetadata(parseJSONObject(loop.MetadataJSON))["terminationReason"]); reason != "pr_closed_or_merged" {
		t.Fatalf("terminationReason = %q, want pr_closed_or_merged", reason)
	}
	if ask, ok := loops.ReadHITLAsk(loop.MetadataJSON); ok && loops.IsReviewFixBudgetAsk(ask) {
		t.Fatalf("HITL ask = %#v, want no budget ask on a closed PR", ask)
	}
	sibling, err := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
	if err != nil || sibling == nil || sibling.Status != "waiting" || loops.IsSiblingReviewFixBudgetPause(sibling.MetadataJSON) {
		t.Fatalf("fixer sibling = (%#v, %v), want still waiting without budget pause", sibling, err)
	}
}

func TestProcessClaimedItemTransportRetriesDoNotConsumeBudgetThenThirdPublishParks(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	fixer := storage.LoopRecord{ID: "loop_fixer_budget_closed_pr", Seq: 2, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "waiting", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Loops.Upsert(fixer) error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 3
	cfg.HITL.Enabled = false
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Please add tests", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	options := Options{RetryMaxAttempts: -1, DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true, LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg}
	runner := New(options)

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	seed, err := fixture.repos.Loops.GetByID(context.Background(), *claim.LoopID)
	if err != nil || seed == nil {
		t.Fatalf("seed: %v", err)
	}
	seedMeta := parseJSONObject(seed.MetadataJSON)
	loopMeta := reviewerLoopMetadata(seedMeta)
	loopMeta["iterationCount"] = 2
	seedMeta["loop"] = loopMeta
	encoded, err := json.Marshal(seedMeta)
	if err != nil {
		t.Fatal(err)
	}
	seed.MetadataJSON = stringPtr(string(encoded))
	if err := fixture.repos.Loops.Upsert(context.Background(), *seed); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 7; attempt++ {
		github.viewErrs = []error{&shell.CommandExecutionError{Message: "Command exited with code 1", Result: shell.Result{Stderr: "error connecting to api.github.com"}}}
		failed, err := runner.ProcessClaimedQueueItem(context.Background(), *claim)
		if err != nil || failed == nil || failed.Status != "failed" || failed.FailureKind != FailureRetryableTransient {
			t.Fatalf("attempt %d = (%#v, %v), want transient failure", attempt, failed, err)
		}
		fresh, err := fixture.repos.Loops.GetByID(context.Background(), seed.ID)
		if err != nil || fresh == nil || loops.ReviewerPublishCount(fresh.MetadataJSON) != 2 || loops.IsReviewFixBudgetHold(*fresh) {
			t.Fatalf("attempt %d loop = (%#v, %v), want unchanged 2/3 budget without hold", attempt, fresh, err)
		}
		sibling, err := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
		if err != nil || sibling == nil || sibling.Status != "waiting" {
			t.Fatalf("sibling after failure = (%#v, %v)", sibling, err)
		}
		queue, err := fixture.repos.Queue.GetByID(context.Background(), claim.ID)
		if err != nil || queue == nil || queue.Status != "queued" {
			t.Fatalf("retry queue = (%#v, %v)", queue, err)
		}
		if len(agent.starts) != 0 {
			t.Fatalf("agent started during transport failure")
		}
		fixture.advance(time.Hour)
		claim, err = fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
		if err != nil || claim == nil {
			t.Fatalf("retry claim = (%#v, %v)", claim, err)
		}
	}
	// A new runner must preserve the persisted budget across a daemon restart.
	runner = New(options)
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success after published review", result)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil || loop == nil || loop.Status != "paused" || !loops.IsReviewFixBudgetHold(*loop) {
		t.Fatalf("reviewer = (%#v, %v), want budget hold before successful claim returns", loop, err)
	}
	if got := loops.ReviewerPublishCount(loop.MetadataJSON); got != 3 {
		t.Fatalf("publish count = %d, want exactly 3", got)
	}
	sibling, err := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
	if err != nil || sibling == nil || sibling.Status != "paused" || !loops.IsSiblingReviewFixBudgetPause(sibling.MetadataJSON) {
		t.Fatalf("fixer = (%#v, %v), want paired budget hold", sibling, err)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), claim.ID)
	if err != nil || queue == nil || queue.Status != "cancelled" {
		t.Fatalf("queue = (%#v, %v), want cancelled before claim returns", queue, err)
	}
	events, err := fixture.repos.Events.ListByEntity(context.Background(), "loop", loop.ID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.EventType == "loop.review_fix_budget.exhausted" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("budget handoff events = %d, want 1", count)
	}
}

func TestParkReviewerBudgetIfExhaustedParksSibling(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	reviewerTarget := "pr:acme/looper:42"
	metadata := `{"loop":{"iterationCount":8}}`
	reviewer := storage.LoopRecord{ID: "loop_reviewer_budget", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &reviewerTarget, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	fixer := storage.LoopRecord{ID: "loop_fixer_budget", Seq: 2, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &reviewerTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("Loops.Upsert(reviewer) error = %v", err)
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Loops.Upsert(fixer) error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.HITL.Enabled = true
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{MaxPublishesPerPR: 8}, CustomInstructions: &cfg})
	parked, err := runner.parkReviewerBudgetIfExhausted(context.Background(), reviewer)
	if err != nil || !parked {
		t.Fatalf("parkReviewerBudgetIfExhausted() = (%v, %v), want parked", parked, err)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), reviewer.ID)
	if err != nil || updated == nil || updated.Status != "awaiting_human" {
		t.Fatalf("reviewer = (%#v, %v), want awaiting_human", updated, err)
	}
	sibling, err := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
	if err != nil || sibling == nil || sibling.Status != "paused" || !loops.IsSiblingReviewFixBudgetPause(sibling.MetadataJSON) {
		t.Fatalf("fixer sibling = (%#v, %v), want paused sibling hold", sibling, err)
	}
}

func TestParkReviewerBudgetIfExhaustedDoesNotNotifyHumanAttention(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	reviewerTarget := "pr:acme/looper:42"
	metadata := `{"loop":{"iterationCount":3}}`
	reviewer := storage.LoopRecord{ID: "loop_reviewer_budget_notify", Seq: 11, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &reviewerTarget, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("Loops.Upsert(reviewer) error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 3
	var notified []string
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now,
		LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
		NotifyHumanAttention: func(_ context.Context, loopID string) {
			notified = append(notified, loopID)
		},
	})
	parked, err := runner.parkReviewerBudgetIfExhausted(context.Background(), reviewer)
	if err != nil || !parked {
		t.Fatalf("parkReviewerBudgetIfExhausted() = (%v, %v), want parked", parked, err)
	}
	if len(notified) != 0 {
		t.Fatalf("NotifyHumanAttention = %#v, want none from shared park", notified)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), reviewer.ID)
	if err != nil || updated == nil || !loops.IsReviewFixBudgetHold(*updated) {
		t.Fatalf("reviewer after park = (%#v, %v), want still held", updated, err)
	}
}

func TestReviewerDiscoveryParkNotifiesHumanAttention(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	reviewerTarget := "pr:acme/looper:42"
	// Exhausted automatic loop — discovery enqueue should park and notify.
	metadata := `{"loop":{"iterationCount":3,"enabled":true}}`
	reviewer := storage.LoopRecord{ID: "loop_reviewer_disc_notify", Seq: 12, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &reviewerTarget, Repo: &repo, PRNumber: &prNumber, Status: "waiting", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("Loops.Upsert(reviewer) error = %v", err)
	}
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID = (%v, %v)", project, err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 3
	var notified []string
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{},
		AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now,
		LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
		NotifyHumanAttention: func(_ context.Context, loopID string) {
			notified = append(notified, loopID)
		},
	})
	result := &DiscoveryResult{}
	login := ""
	err = runner.enqueueReviewerDiscoveryCandidate(context.Background(), *project, repo, DiscoveryPolicy{AutoDiscovery: true, RequireReviewRequest: false, EnableSelfReview: true}, &login, PullRequestSummary{Number: prNumber, HeadSHA: "new-head-for-discovery", State: "OPEN"}, &reviewer, false, result)
	if err != nil {
		t.Fatalf("enqueueReviewerDiscoveryCandidate() error = %v", err)
	}
	if result.Skipped == 0 {
		t.Fatalf("discovery result = %#v, want skipped after budget park", result)
	}
	if len(notified) != 1 || notified[0] != reviewer.ID {
		t.Fatalf("NotifyHumanAttention = %#v, want [%q] from discovery park", notified, reviewer.ID)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), reviewer.ID)
	if err != nil || updated == nil || !loops.IsReviewFixBudgetHold(*updated) {
		t.Fatalf("reviewer after discovery = (%#v, %v), want held", updated, err)
	}
}

func TestParkReviewerBudgetIfExhaustedParksWhenHITLDisabled(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	reviewerTarget := "pr:acme/looper:42"
	metadata := `{"loop":{"iterationCount":3}}`
	reviewer := storage.LoopRecord{ID: "loop_reviewer_budget_no_hitl", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &reviewerTarget, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	fixer := storage.LoopRecord{ID: "loop_fixer_budget_no_hitl", Seq: 2, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &reviewerTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("Loops.Upsert(reviewer) error = %v", err)
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Loops.Upsert(fixer) error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	if cfg.HITL.Enabled {
		t.Fatal("DefaultConfig HITL.Enabled = true, want false")
	}
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 3
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg})
	parked, err := runner.parkReviewerBudgetIfExhausted(context.Background(), reviewer)
	if err != nil || !parked {
		t.Fatalf("parkReviewerBudgetIfExhausted() = (%v, %v), want no-HITL paired park", parked, err)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), reviewer.ID)
	if err != nil || updated == nil || updated.Status != "paused" || !loops.IsReviewFixBudgetExhaustedPause(updated.MetadataJSON) {
		t.Fatalf("reviewer = (%#v, %v), want paused review_fix_budget_exhausted", updated, err)
	}
	if _, ok := loops.ReadHITLAsk(updated.MetadataJSON); ok {
		t.Fatal("no-HITL park must not write a HITL ask")
	}
	sibling, err := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
	if err != nil || sibling == nil || sibling.Status != "paused" || !loops.IsSiblingReviewFixBudgetPause(sibling.MetadataJSON) {
		t.Fatalf("fixer sibling = (%#v, %v), want paired budget pause", sibling, err)
	}
}

func TestParkReviewerBudgetIfExhaustedCompletesSiblingAfterPartialPark(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	reviewerTarget := "pr:acme/looper:42"
	reviewer := storage.LoopRecord{ID: "loop_reviewer_budget_retry", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &reviewerTarget, Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	fixer := storage.LoopRecord{ID: "loop_fixer_budget_retry", Seq: 2, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &reviewerTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}
	askMeta, err := loops.WriteHITLAsk(reviewer.MetadataJSON, loops.NewReviewFixBudgetAsk("reviewer", repo, prNumber, 8, 8, nowISO))
	if err != nil {
		t.Fatalf("WriteHITLAsk() error = %v", err)
	}
	reviewer.MetadataJSON = &askMeta
	reviewer.Status = "awaiting_human"
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("Loops.Upsert(reviewer) error = %v", err)
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Loops.Upsert(fixer) error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})
	parked, err := runner.parkReviewerBudgetIfExhausted(context.Background(), reviewer)
	if err != nil || !parked {
		t.Fatalf("parkReviewerBudgetIfExhausted() = (%v, %v), want reconciled park", parked, err)
	}
	sibling, err := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
	if err != nil || sibling == nil || sibling.Status != "paused" || !loops.IsSiblingReviewFixBudgetPause(sibling.MetadataJSON) {
		t.Fatalf("fixer sibling = (%#v, %v), want paused after reconcile", sibling, err)
	}
}

func TestReviewerDiscoveryDoesNotReviveSiblingPausedReviewer(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	// Fixer exhausted; Reviewer sibling-paused with unused budget.
	fixerMeta := `{"reviewFixBudget":{"pushCount":1,"exhaustedBy":"fixer","pauseReason":"review_fix_budget_exhausted"},"pauseReason":"review_fix_budget_exhausted"}`
	reviewerMeta := `{"loop":{"iterationCount":0},"reviewFixBudget":{"siblingOf":"fixer","pauseReason":"sibling_review_fix_budget"},"pauseReason":"sibling_review_fix_budget"}`
	fixer := storage.LoopRecord{ID: "loop_fix_exh_disc", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "paused", MetadataJSON: &fixerMeta, CreatedAt: nowISO, UpdatedAt: nowISO}
	reviewer := storage.LoopRecord{ID: "loop_rev_sib_disc", Seq: 2, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "paused", MetadataJSON: &reviewerMeta, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Upsert fixer: %v", err)
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("Upsert reviewer: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.Roles.Fixer.Behavior.Loop.MaxPushesPerPR = 1
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 3
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg})

	if err := runner.markLoopQueuedForReview(context.Background(), reviewer, nowISO); err != nil {
		t.Fatalf("markLoopQueuedForReview: %v", err)
	}
	after, err := fixture.repos.Loops.GetByID(context.Background(), reviewer.ID)
	if err != nil || after == nil || after.Status != "paused" || !loops.IsSiblingReviewFixBudgetPause(after.MetadataJSON) {
		t.Fatalf("reviewer after mark = (%#v, %v), want still sibling-paused", after, err)
	}
	// Discovery hold check must also skip.
	if !loops.IsReviewFixBudgetHold(*after) {
		t.Fatal("sibling-paused reviewer must be a budget hold")
	}
	parked, err := runner.parkReviewerBudgetIfExhausted(context.Background(), *after)
	if err != nil || !parked {
		t.Fatalf("parkReviewerBudgetIfExhausted sibling = (%v, %v), want held", parked, err)
	}
}

func TestRefusePublishIfBudgetExhaustedAtLiveCap(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	metadata := `{"loop":{"iterationCount":2}}`
	loop := storage.LoopRecord{ID: "loop_pub_refuse", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 2
	cfg.HITL.Enabled = false
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg, GitHub: &fakeGitHubGateway{viewState: "OPEN"}})
	refused, err := runner.refusePublishIfBudgetExhausted(context.Background(), stepInput{
		Project: storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Loop:    loop, Repo: repo, PRNumber: prNumber,
	})
	if !refused {
		t.Fatalf("refused=%v err=%v, want refused at live cap", refused, err)
	}
	if err != nil {
		var hold *holdSkipError
		if !errors.As(err, &hold) {
			t.Fatalf("error = %v, want holdSkipError", err)
		}
	}
	after, _ := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if after == nil || !loops.IsReviewFixBudgetHold(*after) {
		t.Fatalf("after refuse = %#v, want budget hold", after)
	}
}

func TestRefusePublishIfBudgetExhaustedFailsClosedOnLedgerRead(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	// Under the live cap: swallowing GetByID would admit the stale snapshot.
	metadata := `{"loop":{"iterationCount":0}}`
	loop := storage.LoopRecord{ID: "loop_pub_ledger_fail", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 2
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg, GitHub: &fakeGitHubGateway{viewState: "OPEN"}})
	if err := fixture.coordinator.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	refused, err := runner.refusePublishIfBudgetExhausted(context.Background(), stepInput{
		Project: storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Loop:    loop, Repo: repo, PRNumber: prNumber,
	})
	if err == nil {
		t.Fatalf("refused=%v err=%v, want fail closed on ledger read", refused, err)
	}
	if !refused {
		t.Fatalf("refused=%v, want true when ledger refresh fails", refused)
	}
}

func TestFinishHeldReviewerQueueItemPreservesBudgetHold(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	projectID := "project_1"
	holdMeta := `{"loop":{"iterationCount":0},"reviewFixBudget":{"siblingOf":"fixer","pauseReason":"sibling_review_fix_budget"},"pauseReason":"sibling_review_fix_budget"}`
	loop := storage.LoopRecord{ID: "loop_held_finish", Seq: 1, ProjectID: projectID, Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "paused", MetadataJSON: &holdMeta, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue := storage.QueueItemRecord{ID: "queue_held_finish", ProjectID: &projectID, LoopID: &loop.ID, Type: "reviewer", TargetType: "pull_request", TargetID: target, Repo: &repo, PRNumber: &prNumber, DedupeKey: "reviewer:held-finish", Priority: storage.QueuePriorityReviewer, Status: "running", AvailableAt: nowISO, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})
	result, err := runner.finishHeldReviewerQueueItem(context.Background(), loop, nil, queue, reviewerCheckpoint{}, "Reviewer stopped because review-fix budget is held")
	if err != nil {
		t.Fatalf("finishHeldReviewerQueueItem() error = %v", err)
	}
	if result.Status != "skipped" {
		t.Fatalf("result = %#v, want skipped", result)
	}
	after, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || after == nil || after.Status != "paused" || !loops.IsReviewFixBudgetHold(*after) {
		t.Fatalf("reviewer after finish = (%#v, %v), want still sibling-paused hold", after, err)
	}
}

func TestRecordPublishedReviewProgressCountsBeforeClaimComplete(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"loop":{"iterationCount":0}}`
	loop := storage.LoopRecord{ID: "loop_publish_count", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queueID := "queue_publish_count"
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{
		ID: queueID, ProjectID: stringPtr("project_1"), LoopID: &loop.ID, Type: "reviewer",
		TargetType: "pull_request", TargetID: "pr:acme/looper:42", Status: "queued",
		Priority: 1, MaxAttempts: 3, AvailableAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO,
	}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	pending := pendingReviewCheckpoint{HeadSHA: "abc123", Summary: "needs work"}
	if err := runner.recordPublishedReviewProgress(context.Background(), stepInput{
		Project:  storage.ProjectRecord{ID: "project_1"},
		Loop:     loop,
		Run:      storage.RunRecord{ID: "run_publish_count"},
		Repo:     repo,
		PRNumber: prNumber,
	}, pending, ReviewEventComment); err != nil {
		t.Fatalf("recordPublishedReviewProgress() error = %v", err)
	}
	afterPublish, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || afterPublish == nil {
		t.Fatalf("Loops.GetByID() after publish = (%#v, %v)", afterPublish, err)
	}
	if loops.ReviewerPublishCount(afterPublish.MetadataJSON) != 1 {
		t.Fatalf("iterationCount after publish = %d, want 1 before claim complete", loops.ReviewerPublishCount(afterPublish.MetadataJSON))
	}
	if got, _ := stringFromAny(parseJSONObject(afterPublish.MetadataJSON)["lastPublishedHeadSha"]); got != "abc123" {
		t.Fatalf("lastPublishedHeadSha = %q, want abc123", got)
	}

	if err := fixture.repos.Queue.Complete(context.Background(), queueID, nowISO); err != nil {
		t.Fatalf("Queue.Complete() error = %v", err)
	}
	if err := runner.recordPublishedReviewProgress(context.Background(), stepInput{
		Project:  storage.ProjectRecord{ID: "project_1"},
		Loop:     *afterPublish,
		Run:      storage.RunRecord{ID: "run_publish_count"},
		Repo:     repo,
		PRNumber: prNumber,
	}, pending, ReviewEventComment); err != nil {
		t.Fatalf("recordPublishedReviewProgress(retry) error = %v", err)
	}
	successMeta, err := runner.recordLoopSuccessMetadata(afterPublish.MetadataJSON, reviewerCheckpoint{
		Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
		PendingReview: &pendingReviewCheckpoint{HeadSHA: "abc123"},
	}, "Published review")
	if err != nil {
		t.Fatalf("recordLoopSuccessMetadata() error = %v", err)
	}
	if loops.ReviewerPublishCount(&successMeta) != 1 {
		t.Fatalf("iterationCount after success metadata = %d, want 1", loops.ReviewerPublishCount(&successMeta))
	}
	retried, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || retried == nil || loops.ReviewerPublishCount(retried.MetadataJSON) != 1 {
		t.Fatalf("iterationCount after same-head retry = (%#v, %v), want 1", retried, err)
	}
}

func TestRecordPublishedReviewProgressCountsSameHeadConvergence(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	cfg := testReviewerLoopConfig()
	cfg.MaxPublishesPerPR = 3
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, LoopConfig: cfg})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"lastPublishedHeadSha":"abc123","loop":{"iterationCount":2}}`
	loop := storage.LoopRecord{ID: "loop_conv_count", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	fixer := storage.LoopRecord{ID: "loop_conv_count_fix", Seq: 2, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "waiting", MetadataJSON: stringPtr(`{"followUpdates":true}`), CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Loops.Upsert(fixer) error = %v", err)
	}
	pending := pendingReviewCheckpoint{
		HeadSHA:        "abc123",
		IdempotencyKey: agentNativeConvergenceReviewID(loop.ID, "abc123", "sig-1"),
		Summary:        "same-head convergence",
	}
	input := stepInput{
		Project:  storage.ProjectRecord{ID: "project_1"},
		Loop:     loop,
		Run:      storage.RunRecord{ID: "run_conv_count"},
		Repo:     repo,
		PRNumber: prNumber,
	}
	if err := runner.recordPublishedReviewProgress(context.Background(), input, pending, ReviewEventComment); err != nil {
		t.Fatalf("recordPublishedReviewProgress() error = %v", err)
	}
	afterPublish, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || afterPublish == nil {
		t.Fatalf("Loops.GetByID() after publish = (%#v, %v)", afterPublish, err)
	}
	if loops.ReviewerPublishCount(afterPublish.MetadataJSON) != 3 {
		t.Fatalf("iterationCount after same-head convergence = %d, want 3", loops.ReviewerPublishCount(afterPublish.MetadataJSON))
	}
	if got, _ := stringFromAny(parseJSONObject(afterPublish.MetadataJSON)[metadataLastPublishedReviewIDKey]); got != pending.IdempotencyKey {
		t.Fatalf("lastPublishedReviewId = %q, want %q", got, pending.IdempotencyKey)
	}
	if !loops.IsReviewFixBudgetHold(*afterPublish) {
		t.Fatalf("same-head convergence at cap must park: status=%s meta=%s", afterPublish.Status, derefString(afterPublish.MetadataJSON))
	}
	input.Loop = *afterPublish
	if err := runner.recordPublishedReviewProgress(context.Background(), input, pending, ReviewEventComment); err != nil {
		t.Fatalf("recordPublishedReviewProgress(retry) error = %v", err)
	}
	retried, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || retried == nil || loops.ReviewerPublishCount(retried.MetadataJSON) != 3 {
		t.Fatalf("iterationCount after same-identity retry = (%#v, %v), want 3", retried, err)
	}
	next := pending
	next.IdempotencyKey = agentNativeConvergenceReviewID(loop.ID, "abc123", "sig-2")
	input.Loop = *retried
	if err := runner.recordPublishedReviewProgress(context.Background(), input, next, ReviewEventComment); err != nil {
		t.Fatalf("recordPublishedReviewProgress(new signal) error = %v", err)
	}
	second, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || second == nil || loops.ReviewerPublishCount(second.MetadataJSON) != 4 {
		t.Fatalf("iterationCount after new convergence identity = (%#v, %v), want 4", second, err)
	}
}

func TestRecordLoopSuccessMetadataRemovesDeprecatedBudgetMetadata(t *testing.T) {
	t.Parallel()
	runner := New(Options{LoopConfig: testReviewerLoopConfig()})
	current := `{"loop":{"enabled":true,"maxIterationsPerPR":2,"maxIterationsPerHead":1,"maxWallClockSeconds":60,"maxConsecutiveFailures":3,"maxAgentExecutionsPerPR":25}}`

	metadataJSON, err := runner.recordLoopSuccessMetadata(&current, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}, PendingReview: &pendingReviewCheckpoint{}}, "clean")
	if err != nil {
		t.Fatalf("recordLoopSuccessMetadata() error = %v", err)
	}
	loopMeta := reviewerLoopMetadata(parseJSONObject(&metadataJSON))
	for _, key := range deprecatedReviewerLoopBudgetMetadataKeys {
		if _, ok := loopMeta[key]; ok {
			t.Fatalf("loop metadata retained deprecated budget key %q: %#v", key, loopMeta)
		}
	}
}

func TestDiscoverPullRequestsDoesNotMarkSkippedExistingLoopQueued(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	nextRunAt := "2026-04-11T13:00:00.000Z"
	metadata := `{"followUpdates":true,"lastPublishedHeadSha":"abc123"}`
	loop := storage.LoopRecord{ID: "loop_same_head", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", NextRunAt: &nextRunAt, MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("len(QueueItems) = %d, want 0", len(result.QueueItems))
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.Status != "completed" || persisted.NextRunAt == nil || *persisted.NextRunAt != nextRunAt {
		t.Fatalf("loop after skipped discovery = %#v, want unchanged completed loop", persisted)
	}
}

func TestRunFilterStepDoesNotTerminateLongRunningLoopOnBudgetMetadata(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{currentLogin: "octocat"}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 2, MaxIterationsPerHead: 1, MaxWallClockSeconds: 60, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"loop":{"enabled":true,"iterationCount":99,"agentExecutionCount":99,"consecutiveFailures":99,"iterationsByHead":{"abc123":99},"startTime":"2026-04-11T10:00:00.000Z"}}`
	loop := storage.LoopRecord{ID: "loop_stale_budget", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "waiting", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", ReviewRequests: []string{"octocat"}}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipReason != "" {
		t.Fatalf("SkipReason = %q, want no budget skip", checkpoint.SkipReason)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || persisted == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", persisted, err)
	}
	if persisted.Status == "terminated" {
		t.Fatalf("loop status = %q, want not terminated", persisted.Status)
	}
}

func TestRunFilterStepSkipsAlreadyReviewedHeadBeforeBudgetTermination(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"lastPublishedHeadSha":"abc123","loop":{"enabled":true,"iterationCount":1,"iterationsByHead":{"abc123":1},"startTime":"2026-04-11T12:00:00.000Z"}}`
	loop := storage.LoopRecord{ID: "loop_same_head_at_budget", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "waiting", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", ReviewRequests: []string{"octocat"}}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if !strings.Contains(checkpoint.SkipReason, "Skipped already-reviewed head abc123") {
		t.Fatalf("SkipReason = %q, want already-reviewed head skip", checkpoint.SkipReason)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.Status != "waiting" {
		t.Fatalf("loop after filter = %#v, want waiting loop not terminated", persisted)
	}
}

func TestRunFilterStepSkipsConflictedPullRequest(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", BaseRefName: "main", Author: "octocat", HasConflicts: true}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if !strings.Contains(checkpoint.SkipReason, "Skipped conflicted pull request acme/looper#42") {
		t.Fatalf("SkipReason = %q, want conflicted PR skip", checkpoint.SkipReason)
	}
	if len(github.issueCommentCalls) != 1 {
		t.Fatalf("issue comment calls = %d, want 1", len(github.issueCommentCalls))
	}
	body := github.issueCommentCalls[0].Body
	for _, want := range []string{"@octocat", "I'm holding off on generating review comments", "acme/looper#42", "merge conflicts", "resolve the conflicts with main", "I'll take another look"} {
		if !strings.Contains(body, want) {
			t.Fatalf("issue comment body = %q, want to contain %q", body, want)
		}
	}
	if len(github.addThreadReplyCalls) != 0 {
		t.Fatalf("review thread reply calls = %d, want 0", len(github.addThreadReplyCalls))
	}
}

func TestRunFilterStepSkipsConflictedPullRequestWhenNotificationFails(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{issueCommentErr: fmt.Errorf("permission denied")}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", BaseRefName: "main", Author: "octocat", HasConflicts: true}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v, want nil", err)
	}
	if checkpoint.SkipKind != "conflicted" {
		t.Fatalf("SkipKind = %q, want conflicted", checkpoint.SkipKind)
	}
	if !strings.Contains(checkpoint.SkipReason, "Skipped conflicted pull request acme/looper#42") {
		t.Fatalf("SkipReason = %q, want conflicted PR skip", checkpoint.SkipReason)
	}
	if len(github.issueCommentCalls) != 1 {
		t.Fatalf("issue comment calls = %d, want 1", len(github.issueCommentCalls))
	}
}

func TestRunFilterStepDeduplicatesConflictedPullRequestNoticeByHeadSHA(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	input := stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", Author: "octocat", HasConflicts: true}}}

	if _, err := runner.runFilterStep(context.Background(), input); err != nil {
		t.Fatalf("first runFilterStep() error = %v", err)
	}
	if _, err := runner.runFilterStep(context.Background(), input); err != nil {
		t.Fatalf("second runFilterStep() error = %v", err)
	}
	if len(github.issueCommentCalls) != 1 {
		t.Fatalf("issue comment calls = %d, want deduplicated single comment", len(github.issueCommentCalls))
	}

	input.Checkpoint.Detail.HeadSHA = "def456"
	if _, err := runner.runFilterStep(context.Background(), input); err != nil {
		t.Fatalf("new head runFilterStep() error = %v", err)
	}
	if len(github.issueCommentCalls) != 2 {
		t.Fatalf("issue comment calls after new head = %d, want 2", len(github.issueCommentCalls))
	}
}

func TestRunFilterStepDeduplicatesConflictedPullRequestNoticeFromExistingCommentMarker(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	dedupeKey := fmt.Sprintf("reviewer.conflicted_pr:%s:%d:%s", repo, prNumber, "abc123")
	marker := conflictNoticeMarker(dedupeKey)

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", Author: "octocat", HasConflicts: true, IssueComments: []map[string]any{{"body": "previous notice " + marker}}}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind != "conflicted" {
		t.Fatalf("SkipKind = %q, want conflicted", checkpoint.SkipKind)
	}
	if len(github.issueCommentCalls) != 0 {
		t.Fatalf("issue comment calls = %d, want 0", len(github.issueCommentCalls))
	}
	latest, err := fixture.repos.Notifications.GetLatestByDedupe(context.Background(), conflictedPRNotificationChannel, dedupeKey)
	if err != nil {
		t.Fatalf("Notifications.GetLatestByDedupe() error = %v", err)
	}
	if latest == nil || latest.Status != "sent" {
		t.Fatalf("latest notification = %#v, want sent", latest)
	}
}

func TestRunFilterStepSkipsConflictedPullRequestBeforeLoginLookup(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLoginErr: fmt.Errorf("gh auth failed")}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	reviews := []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "APPROVED", "commit": map[string]any{"oid": "abc123"}}}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", HasConflicts: true, ReviewDecision: "APPROVED", Reviews: reviews}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind != "conflicted" {
		t.Fatalf("SkipKind = %q, want conflicted", checkpoint.SkipKind)
	}
	if github.currentLoginCalls != 0 {
		t.Fatalf("GetCurrentUserLogin calls = %d, want 0", github.currentLoginCalls)
	}
}

func TestRunFilterStepSkipsConflictedPullRequestWithAggregateApproval(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_approved_conflicted", ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", HasConflicts: true, ReviewDecision: "APPROVED"}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind != "conflicted" {
		t.Fatalf("SkipKind = %q, want conflicted", checkpoint.SkipKind)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || updated == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updated, err)
	}
	if updated.Status == "terminated" {
		t.Fatalf("loop status = %q, want not terminated", updated.Status)
	}
}

func TestRunFilterStepDoesNotTerminateWhenOnlyAnotherReviewerApproved(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "octocat"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_other_approved", ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	reviews := []map[string]any{{"author": map[string]any{"login": "other"}, "state": "APPROVED", "commit": map[string]any{"oid": "abc123"}}}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", ReviewDecision: "APPROVED", ReviewRequests: []string{"octocat"}, Reviews: reviews}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind == "approved" {
		t.Fatalf("SkipKind = %q, want no approved termination", checkpoint.SkipKind)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || updated == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updated, err)
	}
	if updated.Status == "terminated" {
		t.Fatalf("loop status = %q, want not terminated", updated.Status)
	}
}

func TestRunFilterStepTerminatesReadyBeforeConflictSkip(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_ready_conflicted", ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", HasConflicts: true, Labels: []string{specpr.ReadyLabel}}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if !strings.Contains(checkpoint.SkipReason, "Terminated reviewer loop for ready pull request") {
		t.Fatalf("SkipReason = %q, want ready termination", checkpoint.SkipReason)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || updated == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updated, err)
	}
	if updated.Status != "terminated" {
		t.Fatalf("loop status = %q, want terminated", updated.Status)
	}
}

func TestRunFilterStepTerminatesReadyBeforeLoginLookup(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLoginErr: fmt.Errorf("gh auth failed")}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_ready_login_error", ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	reviews := []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "APPROVED", "commit": map[string]any{"oid": "abc123"}}}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", Labels: []string{specpr.ReadyLabel}, Reviews: reviews}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind != "ready_label" {
		t.Fatalf("SkipKind = %q, want ready_label", checkpoint.SkipKind)
	}
	if github.currentLoginCalls != 0 {
		t.Fatalf("GetCurrentUserLogin calls = %d, want 0", github.currentLoginCalls)
	}
}

func TestRunFilterStepSkipsPullRequestAlreadyReviewedByCurrentUserForHead(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "OctoCat"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	reviews := []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "COMMENTED", "commit": map[string]any{"oid": "abc123"}}}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", Reviews: reviews}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if !strings.Contains(checkpoint.SkipReason, "current user already reviewed head abc123") {
		t.Fatalf("SkipReason = %q, want already-reviewed-by-current-user skip", checkpoint.SkipReason)
	}
}

func TestRunFilterStepSkipsSelfAuthoredPullRequestByDefault(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "octocat"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", Author: "octocat", ReviewRequests: []string{"octocat"}}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind != "self_authored" {
		t.Fatalf("SkipKind = %q, want self_authored", checkpoint.SkipKind)
	}
	if !strings.Contains(strings.ToLower(checkpoint.SkipReason), "self-authored") {
		t.Fatalf("SkipReason = %q, want self-authored skip reason", checkpoint.SkipReason)
	}
}

func TestRunFilterStepRoutedModeRefreshesCurrentLoginBeforeSelfAuthoredCheck(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "new-user"}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Network = config.NetworkConfig{NodeName: "red", GitHubLogin: "stale-user", GitHubUserID: 42}
	cfg.Projects = []config.ProjectRefConfig{{ID: "project_1", Name: "Looper", RepoPath: t.TempDir(), Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})
	repo := "acme/looper"
	prNumber := int64(42)

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", Author: "new-user", Labels: []string{"looper:target:red"}, ReviewRequestUsers: []networkpolicy.GitHubUser{{Login: "new-user", ID: 42}}}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind != "self_authored" {
		t.Fatalf("SkipKind = %q, want self_authored", checkpoint.SkipKind)
	}
	if github.currentLoginCalls != 1 {
		t.Fatalf("GetCurrentUserLogin calls = %d, want 1", github.currentLoginCalls)
	}
}

func TestRunFilterStepRefreshesCurrentLoginBeforeAlreadyReviewedCheck(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "new-user"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	reviews := []map[string]any{{"author": map[string]any{"login": "old-user"}, "state": "COMMENTED", "commit": map[string]any{"oid": "abc123"}}}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", CurrentLogin: "old-user", ReviewRequests: []string{"new-user"}, Reviews: reviews}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipReason != "" {
		t.Fatalf("SkipReason = %q, want no skip for stale checkpoint login", checkpoint.SkipReason)
	}
	if checkpoint.Detail.CurrentLogin != "new-user" {
		t.Fatalf("CurrentLogin = %q, want refreshed login", checkpoint.Detail.CurrentLogin)
	}
	if github.currentLoginCalls != 1 {
		t.Fatalf("GetCurrentUserLogin calls = %d, want 1", github.currentLoginCalls)
	}
}

func TestRunFilterStepSkipsApprovedCurrentHeadWithoutTerminating(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "octocat"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_approved_already_reviewed", ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	reviews := []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "APPROVED", "commit": map[string]any{"oid": "abc123"}}}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", ReviewDecision: "APPROVED", Reviews: reviews}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind != "already_reviewed_by_current_user" {
		t.Fatalf("SkipKind = %q, want already_reviewed_by_current_user", checkpoint.SkipKind)
	}
	if !strings.Contains(checkpoint.SkipReason, "already reviewed head abc123") {
		t.Fatalf("SkipReason = %q, want already-reviewed skip", checkpoint.SkipReason)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || updated == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updated, err)
	}
	if updated.Status == "terminated" {
		t.Fatalf("loop status = %q, want not terminated", updated.Status)
	}
}

func TestRunFilterStepRoutedModeSkipsAlreadyReviewedHeadByCurrentUser(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "reviewer"}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Network = config.NetworkConfig{NodeName: "red", GitHubLogin: "reviewer", GitHubUserID: 42}
	cfg.Projects = []config.ProjectRefConfig{{ID: "project_1", Name: "Looper", RepoPath: t.TempDir(), Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})
	repo := "acme/looper"
	prNumber := int64(42)
	reviews := []map[string]any{{"author": map[string]any{"login": "reviewer"}, "state": "COMMENTED", "commit": map[string]any{"oid": "abc123"}}}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", Author: "octocat", Labels: []string{"looper:target:red"}, ReviewRequestUsers: []networkpolicy.GitHubUser{{Login: "reviewer", ID: 42}}, Reviews: reviews}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind != "already_reviewed_by_current_user" {
		t.Fatalf("SkipKind = %q, want already_reviewed_by_current_user", checkpoint.SkipKind)
	}
	if github.currentLoginCalls != 1 {
		t.Fatalf("GetCurrentUserLogin calls = %d, want 1", github.currentLoginCalls)
	}
}

func TestRunFilterStepAllowsNewHeadAfterCurrentUserApprovedPreviousHead(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "octocat"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_approved_previous_head", ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	reviews := []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "APPROVED", "commit": map[string]any{"oid": "abc123"}}}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "def456", ReviewDecision: "APPROVED", ReviewRequests: []string{"octocat"}, Reviews: reviews}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipReason != "" {
		t.Fatalf("SkipReason = %q, want no skip for new head", checkpoint.SkipReason)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || updated == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updated, err)
	}
	if updated.Status == "terminated" {
		t.Fatalf("loop status = %q, want not terminated", updated.Status)
	}
}

func TestRunFilterStepRefreshesCurrentLoginBeforeApprovedCheck(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "new-user"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_stale_approved", ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	reviews := []map[string]any{{"author": map[string]any{"login": "old-user"}, "state": "APPROVED", "commit": map[string]any{"oid": "abc123"}}}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", CurrentLogin: "old-user", ReviewRequests: []string{"new-user"}, Reviews: reviews}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipReason != "" {
		t.Fatalf("SkipReason = %q, want no termination for stale checkpoint login", checkpoint.SkipReason)
	}
	if checkpoint.Detail.CurrentLogin != "new-user" {
		t.Fatalf("CurrentLogin = %q, want refreshed login", checkpoint.Detail.CurrentLogin)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || updated == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updated, err)
	}
	if updated.Status == "terminated" {
		t.Fatalf("loop status = %q, want not terminated", updated.Status)
	}
}

func TestRunFilterStepTerminatesReadyBeforeAlreadyReviewedSkip(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "octocat"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_ready_already_reviewed", ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	reviews := []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "COMMENTED", "commit": map[string]any{"oid": "abc123"}}}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", Labels: []string{specpr.ReadyLabel}, Reviews: reviews}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if !strings.Contains(checkpoint.SkipReason, "Terminated reviewer loop for ready pull request") {
		t.Fatalf("SkipReason = %q, want ready termination", checkpoint.SkipReason)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || updated == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updated, err)
	}
	if updated.Status != "terminated" {
		t.Fatalf("loop status = %q, want terminated", updated.Status)
	}
}

func TestRunFilterStepAllowsManualReviewAlreadyReviewedByCurrentUserForHead(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "octocat"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"manual":true}`
	loop := storage.LoopRecord{ID: "loop_manual_already_reviewed", ProjectID: "project_1", Type: "reviewer", MetadataJSON: &metadata}
	reviews := []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "COMMENTED", "commit": map[string]any{"oid": "abc123"}}}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", Reviews: reviews}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipReason != "" {
		t.Fatalf("SkipReason = %q, want no skip for manual run", checkpoint.SkipReason)
	}
}

func TestRunFilterStepAllowsReviewWhenOnlyCurrentHeadReviewIsUnsubmitted(t *testing.T) {
	t.Parallel()

	for _, state := range []string{"PENDING", "DISMISSED"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			github := &fakeGitHubGateway{currentLogin: "octocat"}
			runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
			repo := "acme/looper"
			prNumber := int64(42)
			reviews := []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": state, "commit": map[string]any{"oid": "abc123"}}}

			checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", ReviewRequests: []string{"octocat"}, Reviews: reviews}}})
			if err != nil {
				t.Fatalf("runFilterStep() error = %v", err)
			}
			if checkpoint.SkipReason != "" {
				t.Fatalf("SkipReason = %q, want no skip for %s review", checkpoint.SkipReason, state)
			}
		})
	}
}

func TestRunFilterStepAllowsReviewAfterHeadChanges(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "octocat"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	reviews := []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "COMMENTED", "commit": map[string]any{"oid": "old-head"}}}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "new-head", ReviewRequests: []string{"octocat"}, Reviews: reviews}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipReason != "" {
		t.Fatalf("SkipReason = %q, want no skip for old-head review", checkpoint.SkipReason)
	}
}

func TestRunFilterStepAllowsReviewWhenReviewCommitMissing(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "octocat"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	reviews := []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "COMMENTED"}}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", ReviewRequests: []string{"octocat"}, Reviews: reviews}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipReason != "" {
		t.Fatalf("SkipReason = %q, want no skip without review commit", checkpoint.SkipReason)
	}
}

func TestRunFilterStepDoesNotTerminateManualNoLoopReviewOnStaleBudget(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 60, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"manual":true,"followUpdates":false,"loop":{"enabled":false,"iterationCount":99,"agentExecutionCount":99,"consecutiveFailures":99,"iterationsByHead":{"abc123":99},"startTime":"2026-04-11T10:00:00.000Z"}}`
	loop := storage.LoopRecord{ID: "loop_manual_no_loop_stale", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", ReviewRequests: []string{"octocat"}}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipReason != "" {
		t.Fatalf("SkipReason = %q, want no skip so the one-shot review can run", checkpoint.SkipReason)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.Status != "queued" {
		t.Fatalf("loop after filter = %#v, want queued loop not terminated", persisted)
	}
}

func TestDiscoverPullRequestsAllowsManualFollowUpWithoutReviewRequest(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"alice"}, currentLogin: "bob"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"manual":true}`
	loop := storage.LoopRecord{ID: "loop_manual_follow", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("len(QueueItems) = %d, want 1", len(result.QueueItems))
	}
}

func TestDiscoverPullRequestsAllowsManualFollowUpWithoutMatchingLabels(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{"different-label"}, reviewRequests: []string{"alice"}, currentLogin: "bob"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{"needs-review"}, LabelMode: config.LabelModeAll}})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"manual":true}`
	loop := storage.LoopRecord{ID: "loop_manual_follow_labels", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("len(QueueItems) = %d, want 1", len(result.QueueItems))
	}
}

func TestDiscoverPullRequestsPreservesDisabledLoopEnabledWhenFollowUpdatesAbsent(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"loop":{"enabled":false}}`
	loop := storage.LoopRecord{ID: "loop_disabled_legacy", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want none for disabled follow-up loop", result.QueueItems)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || updated == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updated, err)
	}
	updatedMeta := parseJSONObject(updated.MetadataJSON)
	if got, _ := updatedMeta["followUpdates"].(bool); got {
		t.Fatalf("followUpdates = true, want preserved false from loop.enabled")
	}
}

func TestDiscoverPullRequestsAllowsManualFollowUpAfterSkippedAutomaticLoopForSamePR(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"alice"}, currentLogin: "bob"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	automaticMetadata := `{"followUpdates":true}`
	manualMetadata := `{"followUpdates":true,"manual":true}`
	for _, loop := range []storage.LoopRecord{
		{ID: "loop_auto_follow", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &automaticMetadata, CreatedAt: nowISO, UpdatedAt: nowISO},
		{ID: "loop_manual_follow_after_auto", Seq: 2, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &manualMetadata, CreatedAt: nowISO, UpdatedAt: nowISO},
	} {
		if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
			t.Fatalf("Loops.Upsert(%s) error = %v", loop.ID, err)
		}
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("len(QueueItems) = %d, want 1", len(result.QueueItems))
	}
	if result.QueueItems[0].LoopID == nil || *result.QueueItems[0].LoopID != "loop_manual_follow_after_auto" {
		t.Fatalf("queue loopID = %#v, want manual follow-up loop", result.QueueItems[0].LoopID)
	}
}

func TestDiscoverPullRequestAllowsManualFollowUpAfterSkippedAutomaticLoopForSamePR(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"alice"}, currentLogin: "bob"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	automaticMetadata := `{"followUpdates":true}`
	manualMetadata := `{"followUpdates":true,"manual":true}`
	for _, loop := range []storage.LoopRecord{
		{ID: "loop_auto_targeted_follow", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &automaticMetadata, CreatedAt: nowISO, UpdatedAt: nowISO},
		{ID: "loop_manual_targeted_follow", Seq: 2, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &manualMetadata, CreatedAt: nowISO, UpdatedAt: nowISO},
	} {
		if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
			t.Fatalf("Loops.Upsert(%s) error = %v", loop.ID, err)
		}
	}

	result, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("DiscoverPullRequest() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("len(QueueItems) = %d, want 1", len(result.QueueItems))
	}
	if result.QueueItems[0].LoopID == nil || *result.QueueItems[0].LoopID != "loop_manual_targeted_follow" {
		t.Fatalf("queue loopID = %#v, want manual follow-up loop", result.QueueItems[0].LoopID)
	}
}

func TestDiscoverPullRequestAllowsManualFollowUpAfterSkippingHeldAutomaticLoopForSamePR(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"alice"}, currentLogin: "bob", labels: []string{domain.HoldLabelReviewer}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	automaticMetadata := `{"followUpdates":true}`
	manualMetadata := `{"followUpdates":true,"manual":true}`
	for _, loop := range []storage.LoopRecord{
		{ID: "loop_auto_targeted_follow_hold", Seq: 8, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &automaticMetadata, CreatedAt: nowISO, UpdatedAt: nowISO},
		{ID: "loop_manual_targeted_follow_hold", Seq: 9, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &manualMetadata, CreatedAt: nowISO, UpdatedAt: nowISO},
	} {
		if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
			t.Fatalf("Loops.Upsert(%s) error = %v", loop.ID, err)
		}
	}
	result, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("DiscoverPullRequest() error = %v", err)
	}
	if len(result.QueueItems) != 1 || result.QueueItems[0].LoopID == nil || *result.QueueItems[0].LoopID != "loop_manual_targeted_follow_hold" {
		t.Fatalf("result = %#v, want held automatic skipped and manual follow-up queued", result)
	}
}

func TestDiscoverPullRequestsSkipsSelfAuthoredFollowUpLoopByDefault(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{author: "octocat", currentLogin: "octocat", reviewRequests: []string{"octocat"}, listOpenByLabel: map[string][]PullRequestSummary{"": {}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true}`
	loop := storage.LoopRecord{ID: "loop_self_follow", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want self-authored follow-up loop skipped by default", result.QueueItems)
	}
}

func TestDiscoverPullRequestsAllowsProjectOverrideForSelfReview(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "octocat", listOpenByLabel: map[string][]PullRequestSummary{"": {{Number: 42, Title: "Self review", State: "OPEN", Author: "octocat", HeadSHA: "abc123", ReviewRequests: []string{"octocat"}}}}}
	cfg := mustLoadReviewerRoleConfig(t, `{"roles":{"reviewer":{"triggers":{"enableSelfReview":false}}},"projects":[{"id":"project_1","name":"Demo","repoPath":"/tmp/repos/looper","roles":{"reviewer":{"triggers":{"enableSelfReview":true}}}}]}`)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 || result.QueueItems[0].PRNumber == nil || *result.QueueItems[0].PRNumber != 42 {
		t.Fatalf("QueueItems = %#v, want project override to allow self-review", result.QueueItems)
	}
}

func TestDiscoverPullRequestsAllowsSelfReviewAfterEnableSelfReviewFlipForExistingLoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{author: "octocat", currentLogin: "octocat", reviewRequests: []string{"octocat"}}
	cfg := mustLoadReviewerRoleConfig(t, `{"roles":{"reviewer":{"triggers":{"enableSelfReview":true}}}}`)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"lastFilterSkip":{"kind":"self_authored","reason":"Skipped self-authored pull request acme/looper#42 for reviewer octocat","recordedAt":"2026-05-01T00:00:00Z","headSha":"abc123","authorLogin":"octocat","reviewerLogin":"octocat"}}`
	loop := storage.LoopRecord{ID: "loop_self_flip", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 || result.QueueItems[0].PRNumber == nil || *result.QueueItems[0].PRNumber != 42 {
		t.Fatalf("QueueItems = %#v, want existing loop queued after enableSelfReview=true", result.QueueItems)
	}
}

func TestDiscoverPullRequestsDoesNotSuppressStaleSelfAuthoredSkipForDifferentReviewer(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{author: "octocat", currentLogin: "alice", reviewRequests: []string{"alice"}, listOpenByLabel: map[string][]PullRequestSummary{"": {}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"lastFilterSkip":{"kind":"self_authored","reason":"Skipped self-authored pull request acme/looper#42 for reviewer octocat","recordedAt":"2026-05-01T00:00:00Z","headSha":"abc123","authorLogin":"octocat","reviewerLogin":"octocat"}}`
	loop := storage.LoopRecord{ID: "loop_self_other_reviewer", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 || result.QueueItems[0].PRNumber == nil || *result.QueueItems[0].PRNumber != 42 {
		t.Fatalf("QueueItems = %#v, want stale self_authored skip ignored for different reviewer", result.QueueItems)
	}
}

func TestProcessClaimedItemSkipsQueuedAutomaticLoopWhenCurrentUserIsNotRequested(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"alice"}, currentLogin: "bob"}
	agent := &fakeAgentExecutor{}
	git := &fakeGitGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_api", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(context.Background(), enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !strings.Contains(result.Summary, "not requested for review") {
		t.Fatalf("result = %#v, want skipped not requested", result)
	}
	if len(agent.starts) != 0 || len(git.createCalls) != 0 {
		t.Fatalf("agent starts=%d git creates=%d, want no review work", len(agent.starts), len(git.createCalls))
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil || updatedLoop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updatedLoop, err)
	}
	loopMeta := reviewerLoopMetadata(parseJSONObject(updatedLoop.MetadataJSON))
	if got := intFromAny(loopMeta["agentExecutionCount"]); got != 0 {
		t.Fatalf("agentExecutionCount = %d, want 0 for filter-only skip", got)
	}
	if got := intFromAny(loopMeta["iterationCount"]); got != 0 {
		t.Fatalf("iterationCount = %d, want 0 for filter-only skip", got)
	}
}

func TestProcessClaimedItemAllowsQueuedAutomaticLoopWhenReviewRequestsUnknown(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequestsUnknown: true, currentLogin: "bob", reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings; added clean signal", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings; added clean signal"}`, ParseStatus: "parsed"}}}
	git := &fakeGitGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig()})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_unknown_review_requests", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("agent starts=%d, want reviewer work to run when review request state is unknown", len(agent.starts))
	}
}

func TestRunFilterStepAllowsExistingLoopFollowUpOnNewHeadWithoutReviewRequest(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "bob", reviewRequests: []string{}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"lastPublishedHeadSha":"old-head","loop":{"enabled":true,"iterationCount":1,"iterationsByHead":{"old-head":1}}}`
	loop := storage.LoopRecord{ID: "loop_filter_followup_new_head", ProjectID: "project_1", Type: "reviewer", Repo: &repo, PRNumber: &prNumber, MetadataJSON: &metadata}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "new-head", ReviewRequests: []string{}}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind == "not_requested" {
		t.Fatalf("SkipKind = %q, want follow-up new head to stay eligible", checkpoint.SkipKind)
	}
}

func TestRunFilterStepSkipsExistingLoopNewHeadWithoutReviewRequestWhenFollowUpdatesDisabled(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "bob", reviewRequests: []string{}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":false,"lastPublishedHeadSha":"old-head","loop":{"enabled":true,"iterationCount":1,"iterationsByHead":{"old-head":1}}}`
	loop := storage.LoopRecord{ID: "loop_filter_followup_disabled", ProjectID: "project_1", Type: "reviewer", Repo: &repo, PRNumber: &prNumber, MetadataJSON: &metadata}

	checkpoint, err := runner.runFilterStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: reviewerCheckpoint{Detail: &checkpointDetail{State: "OPEN", HeadSHA: "new-head", ReviewRequests: []string{}}}})
	if err != nil {
		t.Fatalf("runFilterStep() error = %v", err)
	}
	if checkpoint.SkipKind != "not_requested" {
		t.Fatalf("SkipKind = %q, want not_requested", checkpoint.SkipKind)
	}
}

func TestProcessClaimedItemRunsExistingLoopFollowUpOnNewHeadWithoutReviewRequest(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "bob", reviewRequests: []string{}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings; added clean signal", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings; added clean signal"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig()})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"lastPublishedHeadSha":"old-head","loop":{"enabled":true,"iterationCount":1,"iterationsByHead":{"old-head":1}}}`
	loop := storage.LoopRecord{ID: "loop_process_followup_new_head", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber, HeadSHA: "abc123"})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("agent starts = %d, want 1", len(agent.starts))
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(ctx, loop.ID)
	if err != nil || updatedLoop == nil || updatedLoop.MetadataJSON == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop metadata", updatedLoop, err)
	}
	if !contains(*updatedLoop.MetadataJSON, `"lastPublishedHeadSha":"abc123"`) {
		t.Fatalf("loop metadata = %s, want new head recorded", *updatedLoop.MetadataJSON)
	}
}

func TestDiscoverPullRequestsSuppressesRepeatedConflictSkipUntilHeadChanges(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{hasConflicts: true, reviewDecision: "REVIEW_REQUIRED", reviewRequests: []string{"octocat"}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: testReviewerLoopConfig()})
	repo := "acme/looper"

	first, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil || len(first.QueueItems) != 1 {
		t.Fatalf("first DiscoverPullRequests() = (%#v, %v), want one queue item", first, err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed queue item", claimed, err)
	}
	processed, err := runner.ProcessClaimedItem(context.Background(), *claimed)
	if err != nil || processed.Status != "skipped" || !strings.Contains(processed.Summary, "conflicted") {
		t.Fatalf("ProcessClaimedItem() = (%#v, %v), want conflicted skip", processed, err)
	}

	github.reviewDecision = "APPROVED"
	second, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("second DiscoverPullRequests() error = %v", err)
	}
	if len(second.QueueItems) != 0 {
		t.Fatalf("second QueueItems = %#v, want no re-enqueue while conflict remains after review decision changes", second.QueueItems)
	}
	items, err := fixture.repos.Queue.List(context.Background())
	if err != nil {
		t.Fatalf("Queue.List() error = %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("len(Queue.List()) = %d, want original completed item only", len(items))
	}

	github.listHeadSHA = "new-head"
	github.viewHeadSHA = "new-head"
	third, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("third DiscoverPullRequests() error = %v", err)
	}
	if len(third.QueueItems) != 1 {
		t.Fatalf("third QueueItems = %#v, want re-enqueue for new head", third.QueueItems)
	}
}

func TestDiscoverPullRequestsSuppressesRepeatedNotRequestedSkipUntilRequested(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "bob", reviewRequests: []string{}, listHeadSHA: "new-head", viewHeadSHA: "new-head"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig()})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	// No prior publication: a cached request miss remains valid until eligibility changes.
	metadata := `{"followUpdates":true,"lastFilterSkip":{"kind":"not_requested","reason":"Skipped pull request acme/looper#42 because current user is not requested for review","recordedAt":"2026-05-01T00:00:00Z","headSha":"new-head","reviewerLogin":"bob"},"loop":{"enabled":true,"iterationCount":1,"iterationsByHead":{"old-head":1}}}`
	loop := storage.LoopRecord{ID: "loop_not_requested_followup", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	first, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("first DiscoverPullRequests() error = %v", err)
	}
	if len(first.QueueItems) != 0 {
		t.Fatalf("first QueueItems = %#v, want suppression for unchanged not_requested head", first.QueueItems)
	}

	second, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("second DiscoverPullRequests() error = %v", err)
	}
	if len(second.QueueItems) != 0 {
		t.Fatalf("second QueueItems = %#v, want no repeated re-enqueue while request remains absent", second.QueueItems)
	}

	github.reviewRequests = []string{"bob"}
	third, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("third DiscoverPullRequests() error = %v", err)
	}
	if len(third.QueueItems) != 1 {
		t.Fatalf("third QueueItems = %#v, want re-enqueue when current reviewer is requested", third.QueueItems)
	}
}

func TestDiscoverPullRequestsDoesNotSuppressNotRequestedSkipAfterHeadChange(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLogin: "bob", reviewRequests: []string{}, listHeadSHA: "new-head", viewHeadSHA: "new-head"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig()})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	// No prior publication: a cached request miss remains valid until eligibility changes.
	metadata := `{"followUpdates":true,"lastFilterSkip":{"kind":"not_requested","reason":"Skipped pull request acme/looper#42 because current user is not requested for review","recordedAt":"2026-05-01T00:00:00Z","headSha":"new-head","reviewerLogin":"bob"},"loop":{"enabled":true,"iterationCount":1,"iterationsByHead":{"old-head":1}}}`
	loop := storage.LoopRecord{ID: "loop_not_requested_head_change", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	first, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("first DiscoverPullRequests() error = %v", err)
	}
	if len(first.QueueItems) != 0 {
		t.Fatalf("first QueueItems = %#v, want suppression for unchanged not_requested head", first.QueueItems)
	}

	second, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("second DiscoverPullRequests() error = %v", err)
	}
	if len(second.QueueItems) != 0 {
		t.Fatalf("second QueueItems = %#v, want no repeated re-enqueue for same head", second.QueueItems)
	}

	if reviewerDiscoverySuppressedByLastSkip(loop, PullRequestSummary{Number: 42, HeadSHA: "newer-head", ReviewRequests: []string{}}, "bob", DiscoveryPolicy{RequireReviewRequest: true}) {
		t.Fatalf("reviewerDiscoverySuppressedByLastSkip() = true, want false after head change")
	}
}

func TestDiscoverPullRequestsAllowsThreadResolutionFollowUpAfterNotRequestedSkip(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		currentLogin:   "bob",
		reviewRequests: []string{},
		listHeadSHA:    "new-head",
		viewHeadSHA:    "new-head",
		reviewThreads:  []ReviewThread{{ID: "thread_1", Comments: []ReviewThreadComment{{ID: "comment_1", Author: "bob", Body: "Please update this. <!-- looper:stamp v=1 -->", CommitOID: "old-head"}}}},
	}
	policy := defaultThreadResolutionPolicy(t)
	policy.Enabled = true
	policy.Mode = config.ReviewerThreadResolutionModeResolveObjective
	policy.RequireCurrentReviewRequest = false
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig(), ThreadResolution: policy})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	metadata := `{"followUpdates":true,"lastPublishedHeadSha":"old-head","lastFilterSkip":{"kind":"not_requested","reason":"Skipped pull request acme/looper#42 because current user is not requested for review","recordedAt":"2026-05-01T00:00:00Z","headSha":"new-head","reviewerLogin":"bob"},"loop":{"enabled":true,"iterationCount":1,"iterationsByHead":{"old-head":1}}}`
	loop := storage.LoopRecord{ID: "loop_not_requested_thread_resolution", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("len(QueueItems) = %d, want 1", len(result.QueueItems))
	}
}

func TestDiscoverPullRequestsChecksNotRequestedThreadResolutionFollowUpOncePerCandidate(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		currentLogin:   "bob",
		reviewRequests: []string{},
		listHeadSHA:    "new-head",
		viewHeadSHA:    "new-head",
		reviewThreads:  []ReviewThread{{ID: "thread_1", Comments: []ReviewThreadComment{{ID: "comment_1", Author: "bob", Body: "Please update this. <!-- looper:stamp v=1 -->", CommitOID: "old-head"}}}},
	}
	policy := defaultThreadResolutionPolicy(t)
	policy.Enabled = true
	policy.Mode = config.ReviewerThreadResolutionModeResolveObjective
	policy.RequireCurrentReviewRequest = false
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig(), ThreadResolution: policy})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	metadata := `{"followUpdates":true,"lastFilterSkip":{"kind":"not_requested","reason":"Skipped pull request acme/looper#42 because current user is not requested for review","recordedAt":"2026-05-01T00:00:00Z","headSha":"new-head","reviewerLogin":"bob"},"loop":{"enabled":true,"iterationCount":1}}`
	loop := storage.LoopRecord{ID: "loop_not_requested_thread_resolution_once", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("len(QueueItems) = %d, want 1", len(result.QueueItems))
	}
	if github.listReviewThreadsCalls != 1 {
		t.Fatalf("ListReviewThreads calls = %d, want 1", github.listReviewThreadsCalls)
	}
}

func TestReviewerDiscoverySuppressedByLastSkipDoesNotUseCurrentReviewRequestsInRoutedMode(t *testing.T) {
	t.Parallel()
	meta := map[string]any{"lastFilterSkip": map[string]any{"kind": "not_requested", "headSha": "new-head", "reviewerLogin": "bob"}}
	pr := PullRequestSummary{
		Number:             42,
		HeadSHA:            "new-head",
		Author:             "alice",
		Labels:             []string{"looper:target:red"},
		ReviewRequests:     []string{},
		ReviewRequestUsers: []networkpolicy.GitHubUser{{Login: "bob", ID: 42}},
	}
	policy := DiscoveryPolicy{
		RequireReviewRequest: true,
		RoutedClaimPolicy: networkpolicy.ProjectPolicy{
			Mode:         config.NetworkModeRouted,
			NodeName:     "red",
			GitHubLogin:  "bob",
			GitHubUserID: 42,
		},
	}
	if reviewerDiscoverySuppressedByLastSkip(storage.LoopRecord{MetadataJSON: stringPtr(mustMarshalJSON(meta))}, pr, "bob", policy) {
		t.Fatalf("reviewerDiscoverySuppressedByLastSkip() = true, want false when routed claim still allows reviewer")
	}
}

func TestFilterSkipMetadataRecordsReviewerForNotRequested(t *testing.T) {
	t.Parallel()
	metadata := filterSkipMetadata(reviewerCheckpoint{
		SkipKind:   "not_requested",
		SkipReason: "Skipped pull request acme/looper#42 because current user is not requested for review",
		Detail:     &checkpointDetail{HeadSHA: "abc123", CurrentLogin: "Bob"},
	}, "2026-05-01T00:00:00Z")
	if metadata == nil {
		t.Fatalf("filterSkipMetadata() = nil, want metadata")
	}
	if got, _ := stringFromAny(metadata["reviewerLogin"]); got != "bob" {
		t.Fatalf("metadata.reviewerLogin = %q, want bob", got)
	}
	if got, _ := stringFromAny(metadata["headSha"]); got != "abc123" {
		t.Fatalf("metadata.headSha = %q, want abc123", got)
	}
}

func TestDiscoverPullRequestsRequeuesConflictedSkipWhenConflictClears(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{hasConflicts: true, reviewRequests: []string{"octocat"}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: testReviewerLoopConfig()})
	repo := "acme/looper"

	first, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil || len(first.QueueItems) != 1 {
		t.Fatalf("first DiscoverPullRequests() = (%#v, %v), want one queue item", first, err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed queue item", claimed, err)
	}
	processed, err := runner.ProcessClaimedItem(context.Background(), *claimed)
	if err != nil || processed.Status != "skipped" || !strings.Contains(processed.Summary, "conflicted") {
		t.Fatalf("ProcessClaimedItem() = (%#v, %v), want conflicted skip", processed, err)
	}

	github.hasConflicts = false
	second, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("second DiscoverPullRequests() error = %v", err)
	}
	if len(second.QueueItems) != 1 {
		t.Fatalf("second QueueItems = %#v, want re-enqueue when conflict clears for same head", second.QueueItems)
	}
}

func TestDiscoverPullRequestsSuppressesRepeatedAlreadyReviewedSkip(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	reviews := []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "COMMENTED", "commit": map[string]any{"oid": "abc123"}}}
	github := &fakeGitHubGateway{currentLogin: "octocat", reviewDecision: "REVIEW_REQUIRED", reviews: reviews, reviewRequests: []string{"octocat"}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: testReviewerLoopConfig()})
	repo := "acme/looper"

	first, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil || len(first.QueueItems) != 1 {
		t.Fatalf("first DiscoverPullRequests() = (%#v, %v), want one queue item", first, err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed queue item", claimed, err)
	}
	processed, err := runner.ProcessClaimedItem(context.Background(), *claimed)
	if err != nil || processed.Status != "skipped" || !strings.Contains(processed.Summary, "already reviewed head abc123") {
		t.Fatalf("ProcessClaimedItem() = (%#v, %v), want already-reviewed skip", processed, err)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), first.CreatedLoopIDs[0])
	if err != nil || loop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", loop, err)
	}
	lastSkip, _ := parseJSONObject(loop.MetadataJSON)["lastFilterSkip"].(map[string]any)
	if got, _ := stringFromAny(lastSkip["reviewerLogin"]); got != "octocat" {
		t.Fatalf("lastFilterSkip.reviewerLogin = %q, want octocat", got)
	}

	github.reviewDecision = "APPROVED"
	second, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("second DiscoverPullRequests() error = %v", err)
	}
	if len(second.QueueItems) != 0 {
		t.Fatalf("second QueueItems = %#v, want no re-enqueue for unchanged already-reviewed head after review decision changes", second.QueueItems)
	}

	github.currentLogin = "looper-bot"
	github.reviewRequests = []string{"looper-bot"}
	third, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("third DiscoverPullRequests() error = %v", err)
	}
	if len(third.QueueItems) != 1 {
		t.Fatalf("third QueueItems = %#v, want re-enqueue when reviewer login changes", third.QueueItems)
	}
}

func TestDiscoverPullRequestsDoesNotRequeueApprovedReadyOrDraftNonActionablePRs(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		github *fakeGitHubGateway
		want   string
		config config.ReviewerLoopConfig
	}{
		{name: "approved", github: &fakeGitHubGateway{reviewDecision: "APPROVED", reviewRequests: []string{"octocat"}, reviews: []map[string]any{{"author": map[string]any{"login": "octocat"}, "state": "APPROVED", "commit": map[string]any{"oid": "abc123"}}}}, want: "approved", config: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 60, MinPublishIntervalSeconds: 300, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25, StopOnApproved: true, StopOnReadyLabel: true, StopOnIdenticalOutput: true}},
		{name: "ready", github: &fakeGitHubGateway{labels: []string{specpr.ReadyLabel}, reviewRequests: []string{"octocat"}}, want: "ready", config: testReviewerLoopConfig()},
		{name: "draft", github: &fakeGitHubGateway{listOpenByLabel: map[string][]PullRequestSummary{"": {{Number: 42, Title: "Draft", State: "OPEN", IsDraft: true, HeadSHA: "draft123", ReviewRequests: []string{"octocat"}}}}}, want: "draft", config: testReviewerLoopConfig()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: tc.github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, LoopConfig: tc.config})
			repo := "acme/looper"

			first, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
			if err != nil {
				t.Fatalf("first DiscoverPullRequests() error = %v", err)
			}
			if tc.want == "draft" {
				if len(first.QueueItems) != 0 {
					t.Fatalf("draft QueueItems = %#v, want no queue items", first.QueueItems)
				}
			} else {
				if len(first.QueueItems) != 1 {
					t.Fatalf("first QueueItems = %#v, want one queue item", first.QueueItems)
				}
				claimed, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
				if err != nil || claimed == nil {
					t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed queue item", claimed, err)
				}
				processed, err := runner.ProcessClaimedItem(context.Background(), *claimed)
				if err != nil || processed.Status != "skipped" || !strings.Contains(processed.Summary, tc.want) {
					t.Fatalf("ProcessClaimedItem() = (%#v, %v), want %s skip", processed, err, tc.want)
				}
			}

			second, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
			if err != nil {
				t.Fatalf("second DiscoverPullRequests() error = %v", err)
			}
			if len(second.QueueItems) != 0 {
				t.Fatalf("second QueueItems = %#v, want no repeated non-actionable requeue", second.QueueItems)
			}
		})
	}
}

func TestProcessClaimedItemSkipsTerminalLoopWithoutStartingRun(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_terminated_queued", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "terminated", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue := storage.QueueItemRecord{ID: "queue_terminal", ProjectID: stringPtr("project_1"), LoopID: stringPtr(loop.ID), Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Priority: 2, Status: "running", AvailableAt: nowISO, Attempts: 1, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), queue)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !strings.Contains(result.Summary, "terminated") {
		t.Fatalf("result = %#v, want skipped terminal loop", result)
	}
	runs, err := fixture.repos.Runs.ListByLoop(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Runs.ListByLoop() error = %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("runs = %#v, want none", runs)
	}
}

func TestProcessClaimedItemRetriesWhenCurrentUserLookupFails(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentLoginErr: fmt.Errorf("gh auth failed")}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_lookup_error", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(context.Background(), enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableTransient {
		t.Fatalf("result = %#v, want retryable transient failure", result)
	}
	queueAfter, err := fixture.repos.Queue.GetByID(context.Background(), queue.ID)
	if err != nil || queueAfter == nil {
		t.Fatalf("Queue.GetByID() = (%#v, %v), want queue", queueAfter, err)
	}
	if queueAfter.Status != "queued" {
		t.Fatalf("queue status = %s, want queued retry", queueAfter.Status)
	}
}

func TestProcessClaimedItemAllowsManualQueuedLoopWithoutReviewRequest(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"alice"}, currentLogin: "bob"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Manual review", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"manual":true}`
	loop := storage.LoopRecord{ID: "loop_manual_api", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(context.Background(), enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("agent starts=%d, want agent-native review to run", len(agent.starts))
	}
	if got, _ := agent.starts[0].Metadata["expectedCommitID"].(string); got != "abc123" {
		t.Fatalf("agent metadata expectedCommitID = %q, want captured head abc123", got)
	}
	if manual, _ := agent.starts[0].Metadata["reviewerManual"].(bool); !manual {
		t.Fatalf("agent metadata reviewerManual = %v, want true for manual loop", agent.starts[0].Metadata["reviewerManual"])
	}
	if got, _ := agent.starts[0].Metadata["reviewerRunID"].(string); got != result.RunID || got != agent.starts[0].RunID {
		t.Fatalf("agent metadata reviewerRunID = %q, want daemon-authored run %q", got, result.RunID)
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil || updatedLoop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updatedLoop, err)
	}
	loopMeta := reviewerLoopMetadata(parseJSONObject(updatedLoop.MetadataJSON))
	if got := intFromAny(loopMeta["agentExecutionCount"]); got != 1 {
		t.Fatalf("agentExecutionCount = %d, want 1 after agent start", got)
	}
}

func TestProcessClaimedItemAllowsManualQueuedLoopWhenApproved(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewDecision: "APPROVED", reviewRequests: []string{"alice"}, currentLogin: "bob"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Manual review", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"manual":true}`
	loop := storage.LoopRecord{ID: "loop_manual_approved", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(context.Background(), enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("agent starts=%d, want manual review to bypass approved termination", len(agent.starts))
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil || updatedLoop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updatedLoop, err)
	}
	if updatedLoop.Status == "terminated" {
		t.Fatalf("loop status = %q, want manual run not terminated", updatedLoop.Status)
	}
}

func TestProcessClaimedItemAllowsManualQueuedLoopWhenReadyLabel(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{specpr.ReadyLabel}, reviewRequests: []string{"alice"}, currentLogin: "bob"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Manual review", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"manual":true}`
	loop := storage.LoopRecord{ID: "loop_manual_ready", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(context.Background(), enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("agent starts=%d, want manual review to bypass ready-label termination", len(agent.starts))
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil || updatedLoop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updatedLoop, err)
	}
	if updatedLoop.Status == "terminated" {
		t.Fatalf("loop status = %q, want manual run not terminated", updatedLoop.Status)
	}
}

func TestProcessClaimedItemAllowsManualQueuedSelfAuthoredLoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"alice"}, currentLogin: "octocat"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Manual self review", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"manual":true}`
	loop := storage.LoopRecord{ID: "loop_manual_self_authored", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(context.Background(), enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("agent starts=%d, want manual self-authored review to bypass skip", len(agent.starts))
	}
}

func TestNormalizedFindingFingerprintIgnoresReviewMarkerMetadata(t *testing.T) {
	t.Parallel()
	oldHead := normalizedFindingFingerprint("same actionable finding <!-- looper:review loop=loop_1 head=old outcome=actionable -->")
	newHead := normalizedFindingFingerprint("same actionable finding <!-- looper:review loop=loop_1 head=new outcome=actionable -->")
	if oldHead == "" || oldHead != newHead {
		t.Fatalf("fingerprints = %q and %q, want equal non-empty values", oldHead, newHead)
	}
}

func TestProcessClaimedItemFingerprintsPublishedReviewBodyForIdenticalOutput(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerBody: "new actionable finding <!-- looper:review loop=loop_identical_output outcome=actionable -->"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Same findings", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25, StopOnIdenticalOutput: true}})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := fmt.Sprintf(`{"followUpdates":true,"loop":{"enabled":true,"lastOutputFingerprint":%q}}`, normalizedFindingFingerprint("previous actionable finding"))
	loop := storage.LoopRecord{ID: "loop_identical_output", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}
	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(ctx, loop.ID)
	if err != nil || updatedLoop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updatedLoop, err)
	}
	if updatedLoop.Status != "waiting" {
		t.Fatalf("loop status = %q, want waiting", updatedLoop.Status)
	}
	if updatedLoop.MetadataJSON == nil || contains(*updatedLoop.MetadataJSON, `"terminationReason":"identical_output"`) {
		t.Fatalf("loop metadata = %#v, want no identical_output termination", updatedLoop.MetadataJSON)
	}
	if want := normalizedFindingFingerprint(github.reviewMarkerBody); !contains(*updatedLoop.MetadataJSON, fmt.Sprintf(`"lastOutputFingerprint":"%s"`, want)) {
		t.Fatalf("loop metadata = %#v, want published review body fingerprint %s", updatedLoop.MetadataJSON, want)
	}
}

func TestProcessClaimedItemFingerprintsPublishedReviewInlineComments(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerBody: "review overview <!-- looper:review loop=loop_inline_fingerprint outcome=actionable -->", reviewMarkerInlineCommentBodies: []string{"different actionable inline finding"}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Same overview", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25, StopOnIdenticalOutput: true}})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := fmt.Sprintf(`{"followUpdates":true,"loop":{"enabled":true,"lastOutputFingerprint":%q}}`, normalizedFindingFingerprint(github.reviewMarkerBody+"\nprevious inline finding"))
	loop := storage.LoopRecord{ID: "loop_inline_fingerprint", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}
	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(ctx, loop.ID)
	if err != nil || updatedLoop == nil || updatedLoop.MetadataJSON == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop metadata", updatedLoop, err)
	}
	if contains(*updatedLoop.MetadataJSON, `"terminationReason":"identical_output"`) {
		t.Fatalf("loop metadata = %#v, want distinct inline comments to avoid identical_output", updatedLoop.MetadataJSON)
	}
	want := normalizedFindingFingerprint(github.reviewMarkerBody + "\n" + strings.Join(github.reviewMarkerInlineCommentBodies, "\n"))
	if !contains(*updatedLoop.MetadataJSON, fmt.Sprintf(`"lastOutputFingerprint":"%s"`, want)) {
		t.Fatalf("loop metadata = %s, want inline comment fingerprint %s", *updatedLoop.MetadataJSON, want)
	}
}

func TestProcessClaimedItemDetectsDuplicatePublishedFindings(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	body := "duplicate actionable finding <!-- looper:review loop=loop_duplicate_finding outcome=actionable -->"
	fingerprint := normalizedFindingFingerprint(body)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerBody: body}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Duplicate findings", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}, DetectDuplicateFindings: true})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := fmt.Sprintf(`{"followUpdates":true,"loop":{"enabled":true,"publishedFindingFingerprints":[%q]}}`, fingerprint)
	loop := storage.LoopRecord{ID: "loop_duplicate_finding", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(ctx, loop.ID)
	if err != nil || updatedLoop == nil || updatedLoop.MetadataJSON == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop metadata", updatedLoop, err)
	}
	if !contains(*updatedLoop.MetadataJSON, `"duplicateFindingsDetected":1`) {
		t.Fatalf("loop metadata = %#v, want duplicateFindingsDetected counter", updatedLoop.MetadataJSON)
	}
	if contains(*updatedLoop.MetadataJSON, "duplicateFindingsSuppressed") {
		t.Fatalf("loop metadata = %#v, want no suppressed counter for detection-only accounting", updatedLoop.MetadataJSON)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("agent starts = %d, want review still executed for detection-only duplicate tracking", len(agent.starts))
	}
}

func TestProcessClaimedItemRecordsCleanNoopWithoutReviewMarkerForCommentPolicy(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings; added clean signal", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings; added clean signal"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"loop":{"enabled":true}}`
	loop := storage.LoopRecord{ID: "loop_clean_noop", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if github.reviewMarkerCalls != 0 {
		t.Fatalf("reviewMarkerCalls = %d, want no review marker lookup for clean no-op", github.reviewMarkerCalls)
	}
	if len(github.addReactionCalls) != 1 {
		t.Fatalf("addReactionCalls = %d, want one clean signal reaction", len(github.addReactionCalls))
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(ctx, loop.ID)
	if err != nil || updatedLoop == nil || updatedLoop.MetadataJSON == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop metadata", updatedLoop, err)
	}
	if !contains(*updatedLoop.MetadataJSON, `"lastPublishedHeadSha":"abc123"`) {
		t.Fatalf("loop metadata = %s, want clean no-op recorded as published for head", *updatedLoop.MetadataJSON)
	}
	if contains(*updatedLoop.MetadataJSON, `"lastOutputFingerprint"`) {
		t.Fatalf("loop metadata = %s, want clean no-op excluded from output fingerprinting", *updatedLoop.MetadataJSON)
	}
}

func TestProcessClaimedItemRejectsCleanNoopWithoutApprovedMarkerForApprovePolicy(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}, LoopConfig: testReviewerLoopConfig()})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "requires an APPROVED review marker") {
		t.Fatalf("result = %#v, want retryable approve-marker-required failure", result)
	}
	if github.reviewMarkerCalls == 0 {
		t.Fatalf("reviewMarkerCalls = %d, want marker lookup before rejecting clean APPROVE summary", github.reviewMarkerCalls)
	}
	if len(github.addReactionCalls) != 0 {
		t.Fatalf("addReactionCalls = %#v, want no reaction for rejected clean no-op", github.addReactionCalls)
	}
}

func TestProcessClaimedItemAcceptsCleanNoopWithApprovedMarkerForApprovePolicy(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{author: "octocat", currentLogin: "reviewer", reviewRequests: []string{"reviewer"}, reviewMarkerOutcome: "clean", reviewMarkerEvent: ReviewEventApprove, reviewMarkerBody: strings.Join([]string{
		"@octocat Thanks for the thoughtful update — the changes are clear and well scoped.",
		"Summary: this keeps the approval flow safe while preserving the intended reviewer behavior.",
		"<!-- looper:review outcome=clean -->",
	}, "\n\n")}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}, LoopConfig: testReviewerLoopConfig()})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if github.reviewMarkerCalls < 2 {
		t.Fatalf("reviewMarkerCalls = %d, want review-step and publish marker verification", github.reviewMarkerCalls)
	}
	if len(github.addReactionCalls) != 1 {
		t.Fatalf("addReactionCalls = %#v, want clean signal reaction", github.addReactionCalls)
	}
	if claim.LoopID == nil {
		t.Fatal("claim.LoopID = nil, want associated loop ID")
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(context.Background(), *claim.LoopID)
	if err != nil || updatedLoop == nil || updatedLoop.MetadataJSON == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop metadata", updatedLoop, err)
	}
	if contains(*updatedLoop.MetadataJSON, `"lastOutputFingerprint"`) {
		t.Fatalf("loop metadata = %s, want accepted clean no-op excluded from output fingerprinting", *updatedLoop.MetadataJSON)
	}
}

func TestValidateCleanApprovedReviewMarkerBodyAcceptsCaseInsensitiveAuthorMention(t *testing.T) {
	t.Parallel()
	checkpoint := reviewerCheckpoint{
		Detail:   &checkpointDetail{Author: "OctoCat"},
		Snapshot: &checkpointSnapshot{Author: "OctoCat"},
	}
	detail := PullRequestDetail{Author: "OctoCat"}
	marker := ReviewMarkerResult{Body: strings.Join([]string{
		"@octocat Thanks for the thoughtful update — the changes are clear and well scoped.",
		"Summary: this keeps the approval flow safe while preserving the intended reviewer behavior.",
		"<!-- looper:review outcome=clean -->",
	}, "\n\n")}

	if err := validateCleanApprovedReviewMarkerBody(marker, cleanReviewAuthorLogin(checkpoint, detail)); err != nil {
		t.Fatalf("validateCleanApprovedReviewMarkerBody() error = %v", err)
	}
}

func TestCleanReviewMarkerSatisfiesCleanPolicyAllowsSelfAuthoredCommentFallback(t *testing.T) {
	t.Parallel()

	marker := ReviewMarkerResult{Found: true, Outcome: "clean", Event: ReviewEventComment, AuthorLogin: "Reviewer"}
	if !cleanReviewMarkerSatisfiesCleanPolicy(marker, "reviewer") {
		t.Fatal("cleanReviewMarkerSatisfiesCleanPolicy() = false, want true for self-authored clean COMMENT fallback")
	}
	if cleanReviewMarkerSatisfiesCleanPolicy(marker, "octocat") {
		t.Fatal("cleanReviewMarkerSatisfiesCleanPolicy() = true, want false for non-self-authored clean COMMENT")
	}
	marker.InlineCommentBodies = []string{"inline"}
	if cleanReviewMarkerSatisfiesCleanPolicy(marker, "reviewer") {
		t.Fatal("cleanReviewMarkerSatisfiesCleanPolicy() = true, want false when clean COMMENT has inline comments")
	}
}

func TestProcessClaimedItemRejectsCleanNoopWithInvalidApprovedMarkerBodyForApprovePolicy(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{author: "octocat", currentLogin: "reviewer", reviewRequests: []string{"reviewer"}, reviewMarkerOutcome: "clean", reviewMarkerEvent: ReviewEventApprove, reviewMarkerBody: "@octocat <!-- hidden filler words should not count toward this approval body -->\n\n[hidden]:https://example.com\n  more hidden filler words that should not count here\n\n<!-- looper:review outcome=clean -->"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}, LoopConfig: testReviewerLoopConfig()})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "short human summary") {
		t.Fatalf("result = %#v, want retryable invalid approval body failure", result)
	}
	if github.reviewMarkerCalls == 0 {
		t.Fatalf("reviewMarkerCalls = %d, want marker lookup before rejecting invalid clean APPROVE body", github.reviewMarkerCalls)
	}
	if len(github.addReactionCalls) != 0 {
		t.Fatalf("addReactionCalls = %#v, want no reaction for rejected clean no-op", github.addReactionCalls)
	}
}

func TestProcessClaimedItemRejectsCleanNoopResumeWithInvalidApprovedMarkerBodyForApprovePolicy(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	ctx := context.Background()
	repo := "acme/looper"
	prNumber := int64(42)
	github := &fakeGitHubGateway{author: "octocat", reviewRequests: []string{"octocat"}, reviewMarkerOutcome: "clean", reviewMarkerEvent: ReviewEventApprove, reviewMarkerBody: "@octocat <!-- hidden filler words should not count toward this approval body --> <!-- looper:review outcome=clean -->"}
	agent := &fakeAgentExecutor{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}, LoopConfig: testReviewerLoopConfig()})
	loopTarget := "pr:42"
	loop := storage.LoopRecord{ID: "loop_clean_approve_resume", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	if _, err := runner.enqueue(ctx, enqueueInput{ProjectID: loop.ProjectID, LoopID: loop.ID, Repo: repo, PRNumber: prNumber}); err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed queue item", claim, err)
	}
	checkpoint := reviewerCheckpoint{
		Detail:        &checkpointDetail{Title: "Review me", State: "OPEN", HeadSHA: "abc123", ReviewRequests: []string{"octocat"}},
		Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
		PendingReview: &pendingReviewCheckpoint{HeadSHA: "abc123", IdempotencyKey: "idem", Event: reviewEventAgentNative, Summary: "No actionable findings"},
		ResumePolicy:  "advance_from_checkpoint",
	}
	checkpointJSON := mustMarshalJSON(checkpoint)
	run := storage.RunRecord{ID: "run_clean_approve_resume", LoopID: loop.ID, Status: "failed", CurrentStep: stringPtr(string(stepPublish)), LastCompletedStep: stringPtr(string(stepReview)), CheckpointJSON: &checkpointJSON, StartedAt: fixture.nowISO(), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Runs.Upsert(ctx, run); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "short human summary") {
		t.Fatalf("result = %#v, want retryable invalid approval body failure", result)
	}
	if len(agent.starts) != 0 {
		t.Fatalf("len(agent.starts) = %d, want no review rerun in failed publish attempt", len(agent.starts))
	}
	if len(github.addReactionCalls) != 0 {
		t.Fatalf("addReactionCalls = %#v, want no reaction for rejected clean no-op resume", github.addReactionCalls)
	}
}

func TestProcessClaimedItemDoesNotStopOnRepeatedCleanNoopSummary(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	cleanSummary := "No actionable findings; added clean signal"
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: cleanSummary, Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings; added clean signal"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25, StopOnIdenticalOutput: true}})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := fmt.Sprintf(`{"followUpdates":true,"loop":{"enabled":true,"lastOutputFingerprint":%q}}`, normalizedFindingFingerprint(cleanSummary))
	loop := storage.LoopRecord{ID: "loop_repeated_clean_noop", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(ctx, loop.ID)
	if err != nil || updatedLoop == nil || updatedLoop.MetadataJSON == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop metadata", updatedLoop, err)
	}
	if updatedLoop.Status != "waiting" {
		t.Fatalf("loop status = %q, want waiting", updatedLoop.Status)
	}
	if contains(*updatedLoop.MetadataJSON, `"terminationReason":"identical_output"`) {
		t.Fatalf("loop metadata = %#v, want repeated clean no-op not to terminate", updatedLoop.MetadataJSON)
	}
	if contains(*updatedLoop.MetadataJSON, `"identicalOutputCount"`) {
		t.Fatalf("loop metadata = %#v, want clean no-op excluded from identical output accounting", updatedLoop.MetadataJSON)
	}
}

func TestProcessClaimedItemSkipsCleanNoopWhenReviewRequestRemovedBeforePublish(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{removeReviewRequestOnSecondView: true, reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings; added clean signal", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings; added clean signal"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"loop":{"enabled":true}}`
	loop := storage.LoopRecord{ID: "loop_clean_noop_request_removed", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !contains(result.Summary, "not requested for review") {
		t.Fatalf("result = %#v, want skipped not requested", result)
	}
	if len(github.addReactionCalls) != 0 {
		t.Fatalf("addReactionCalls = %d, want no clean signal reaction", len(github.addReactionCalls))
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(ctx, loop.ID)
	if err != nil || updatedLoop == nil || updatedLoop.MetadataJSON == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop metadata", updatedLoop, err)
	}
	if contains(*updatedLoop.MetadataJSON, `"lastPublishedHeadSha":"abc123"`) {
		t.Fatalf("loop metadata = %s, want no clean no-op publish progress", *updatedLoop.MetadataJSON)
	}
}

func TestProcessClaimedItemMarksCleanNoopStaleWhenHeadChangesBeforePublish(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{changeHeadOnSecondView: true, reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings; added clean signal", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings; added clean signal"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"loop":{"enabled":true}}`
	loop := storage.LoopRecord{ID: "loop_clean_noop_head_changed", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !contains(result.Summary, "PR head changed before publish") {
		t.Fatalf("result = %#v, want stale head-change skip", result)
	}
	latestRun, err := fixture.repos.Runs.GetLatestByLoopID(ctx, result.LoopID)
	if err != nil || latestRun == nil {
		t.Fatalf("GetLatestByLoopID() = (%#v, %v), want run", latestRun, err)
	}
	if latestRun.Status != "success" {
		t.Fatalf("latestRun.Status = %q, want success for stale skip", latestRun.Status)
	}
	if !contains(derefString(latestRun.Summary), "PR head changed before publish") {
		t.Fatalf("Summary = %q, want standard head-change message", derefString(latestRun.Summary))
	}
	checkpoint := parseCheckpoint(latestRun.CheckpointJSON)
	if checkpoint.SkipKind != "stale" {
		t.Fatalf("SkipKind = %q, want stale", checkpoint.SkipKind)
	}
}

func TestProcessClaimedItemInterruptsRunningReviewerWhenHeadChanges(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{changeHeadOnSecondView: true, reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{
		results: []AgentResult{{Status: "completed", Summary: "stale review", Stdout: `__LOOPER_RESULT__={"summary":"stale review"}`, ParseStatus: "parsed"}},
		wait: func(context.Context) error {
			time.Sleep(25 * time.Millisecond)
			return nil
		},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, HeadChangePollInterval: time.Millisecond, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"loop":{"enabled":true}}`
	loop := storage.LoopRecord{ID: "loop_interrupt_head_changed", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "interrupted" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "PR head changed while reviewer was running") {
		t.Fatalf("result = %#v, want interrupted retryable head change", result)
	}
	if len(agent.killedReasons) != 1 || !contains(agent.killedReasons[0], "new-head") {
		t.Fatalf("killedReasons = %#v, want one head-change kill", agent.killedReasons)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("agent starts = %d, want one reviewer start", len(agent.starts))
	}
	if got, _ := agent.starts[0].Metadata["expectedCommitID"].(string); got != "abc123" {
		t.Fatalf("agent metadata expectedCommitID = %q, want active snapshot head abc123", got)
	}
	if manual, _ := agent.starts[0].Metadata["reviewerManual"].(bool); manual {
		t.Fatalf("agent metadata reviewerManual = %v, want false for automatic loop", agent.starts[0].Metadata["reviewerManual"])
	}
	if got, _ := agent.starts[0].Metadata["reviewerRunID"].(string); got != result.RunID || got != agent.starts[0].RunID {
		t.Fatalf("agent metadata reviewerRunID = %q, want daemon-authored run %q", got, result.RunID)
	}
	interruptedRun, err := fixture.repos.Runs.GetByID(ctx, result.RunID)
	if err != nil || interruptedRun == nil {
		t.Fatalf("Runs.GetByID() = (%#v, %v), want interrupted run", interruptedRun, err)
	}
	if interruptedRun.Status != "interrupted" {
		t.Fatalf("run.Status = %q, want interrupted", interruptedRun.Status)
	}
	checkpoint := parseCheckpoint(interruptedRun.CheckpointJSON)
	if checkpoint.ResumePolicy != "restart_from_discover" || checkpoint.PendingReview != nil {
		t.Fatalf("checkpoint = %#v, want restart_from_discover without stale pending review", checkpoint)
	}
	requeued, err := fixture.repos.Queue.GetByID(ctx, queue.ID)
	if err != nil || requeued == nil {
		t.Fatalf("Queue.GetByID() = (%#v, %v), want queue", requeued, err)
	}
	if requeued.Status != "queued" || requeued.LastErrorKind == nil || *requeued.LastErrorKind != string(FailureRetryableAfterResume) {
		t.Fatalf("queue = %#v, want queued retryable-after-resume", requeued)
	}
}

func TestProcessClaimedItemMarksNativeResumePendingWhenHeadChangesAndFlagEnabled(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	ctx := context.Background()
	github := &fakeGitHubGateway{changeHeadOnSecondView: true, reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{
		results: []AgentResult{{Status: "completed", Summary: "stale review", Stdout: `__LOOPER_RESULT__={"summary":"stale review"}`, ParseStatus: "parsed"}},
		wait: func(context.Context) error {
			time.Sleep(25 * time.Millisecond)
			return nil
		},
	}
	agent.onStart = func(input AgentRunInput) {
		nowISO := fixture.nowISO()
		if err := fixture.repos.AgentExecutions.Upsert(ctx, storage.AgentExecutionRecord{ID: input.ExecutionID, ProjectID: stringPtr(input.ProjectID), LoopID: stringPtr(input.LoopID), RunID: stringPtr(input.RunID), Vendor: string(config.AgentVendorOpenCode), Status: "running", NativeSessionID: stringPtr("session-head-change"), StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
			t.Fatalf("AgentExecutions.Upsert() error = %v", err)
		}
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AgentRuntime: string(config.AgentVendorOpenCode), NativeResume: config.ReviewerNativeResumeConfig{OnHeadChange: true}, HeadChangePollInterval: time.Millisecond, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"loop":{"enabled":true}}`
	loop := storage.LoopRecord{ID: "loop_interrupt_head_changed_native_resume", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "interrupted" {
		t.Fatalf("result.Status = %q, want interrupted", result.Status)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) = %d, want 1", len(agent.starts))
	}
	record, err := fixture.repos.AgentExecutions.GetByID(ctx, agent.starts[0].ExecutionID)
	if err != nil || record == nil {
		t.Fatalf("AgentExecutions.GetByID() = (%#v, %v), want record", record, err)
	}
	if record.NativeResumeMode == nil || *record.NativeResumeMode != "native_resume" || record.NativeResumeStatus == nil || *record.NativeResumeStatus != "pending" {
		t.Fatalf("native resume fields = mode:%v status:%v, want native_resume/pending", record.NativeResumeMode, record.NativeResumeStatus)
	}
	headChange, ok := reviewerNativeResumeHeadChange(record)
	if !ok || headChange.OldHeadSHA != "abc123" || headChange.NewHeadSHA != "new-head" || !headChange.matches(repo, prNumber) {
		t.Fatalf("reviewerNativeResume metadata = (%#v, %v), want head-change abc123 -> new-head", headChange, ok)
	}
}

func TestProcessClaimedItemDoesNotMarkNativeResumePendingWhenHeadChangeFlagDisabled(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	ctx := context.Background()
	github := &fakeGitHubGateway{changeHeadOnSecondView: true, reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{
		results: []AgentResult{{Status: "completed", Summary: "stale review", Stdout: `__LOOPER_RESULT__={"summary":"stale review"}`, ParseStatus: "parsed"}},
		wait: func(context.Context) error {
			time.Sleep(25 * time.Millisecond)
			return nil
		},
	}
	agent.onStart = func(input AgentRunInput) {
		nowISO := fixture.nowISO()
		if err := fixture.repos.AgentExecutions.Upsert(ctx, storage.AgentExecutionRecord{ID: input.ExecutionID, ProjectID: stringPtr(input.ProjectID), LoopID: stringPtr(input.LoopID), RunID: stringPtr(input.RunID), Vendor: string(config.AgentVendorOpenCode), Status: "running", NativeSessionID: stringPtr("session-head-change"), StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
			t.Fatalf("AgentExecutions.Upsert() error = %v", err)
		}
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AgentRuntime: string(config.AgentVendorOpenCode), HeadChangePollInterval: time.Millisecond, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"loop":{"enabled":true}}`
	loop := storage.LoopRecord{ID: "loop_interrupt_head_changed_native_resume_disabled", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	if _, err := runner.ProcessClaimedItem(ctx, *claimed); err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	record, err := fixture.repos.AgentExecutions.GetByID(ctx, agent.starts[0].ExecutionID)
	if err != nil || record == nil {
		t.Fatalf("AgentExecutions.GetByID() = (%#v, %v), want record", record, err)
	}
	if record.NativeResumeStatus != nil && *record.NativeResumeStatus == "pending" {
		t.Fatalf("NativeResumeStatus = %q, want not pending when feature flag is disabled", *record.NativeResumeStatus)
	}
}

func TestProcessClaimedItemMarksStaleWhenPullRequestStateDriftsBeforePublish(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		observed  string
		wantInMsg string
	}{
		{name: "merged", observed: "MERGED", wantInMsg: "observed MERGED"},
		{name: "closed", observed: "CLOSED", wantInMsg: "observed CLOSED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			github := &fakeGitHubGateway{viewStateAfterFirstView: tc.observed, reviewRequests: []string{"octocat"}}
			agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Found actionable issue", Stdout: `__LOOPER_RESULT__={"summary":"Found actionable issue"}`, ParseStatus: "parsed"}}}
			runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})

			if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
				t.Fatalf("DiscoverPullRequests() error = %v", err)
			}
			claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
			if err != nil || claim == nil {
				t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
			}

			result, err := runner.ProcessClaimedItem(context.Background(), *claim)
			if err != nil {
				t.Fatalf("ProcessClaimedItem() error = %v", err)
			}
			if result.Status != "skipped" || !contains(result.Summary, "PR drift detected before publish") || !contains(result.Summary, tc.wantInMsg) {
				t.Fatalf("result = %#v, want stale PR state drift skip", result)
			}
			if github.reviewMarkerCalls != 0 {
				t.Fatalf("reviewMarkerCalls = %d, want no publish verification after state drift", github.reviewMarkerCalls)
			}
			latestRun, err := fixture.repos.Runs.GetLatestByLoopID(context.Background(), result.LoopID)
			if err != nil || latestRun == nil {
				t.Fatalf("GetLatestByLoopID() = (%#v, %v), want run", latestRun, err)
			}
			checkpoint := parseCheckpoint(latestRun.CheckpointJSON)
			if checkpoint.SkipKind != "stale" || !contains(checkpoint.SkipReason, tc.wantInMsg) {
				t.Fatalf("checkpoint = %#v, want stale reason containing %q", checkpoint, tc.wantInMsg)
			}
		})
	}
}

func TestProcessClaimedItemTransitionsSpecLabelsForCleanNoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{specpr.ReviewingLabel}, reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings; added clean signal", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings; added clean signal"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment}, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"loop":{"enabled":true}}`
	loop := storage.LoopRecord{ID: "loop_clean_noop_spec", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(github.removeLabelCalls) != 0 {
		t.Fatalf("removeLabelCalls = %#v, want no spec-reviewing removal for clean COMMENT no-op", github.removeLabelCalls)
	}
	if len(github.addLabelCalls) != 0 {
		t.Fatalf("addLabelCalls = %#v, want no spec-ready add for clean COMMENT no-op", github.addLabelCalls)
	}
	if github.viewCalls < 1 {
		t.Fatalf("viewCalls = %d, want publish detail check", github.viewCalls)
	}
}

func TestProcessClaimedItemDoesNotTreatCleanNoopAsApprovedTransitionForApprovePolicy(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{specpr.ReviewingLabel}, reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings; added clean signal", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings; added clean signal"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: false, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"loop":{"enabled":true}}`
	loop := storage.LoopRecord{ID: "loop_clean_noop_explicit_approve_spec", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "requires an APPROVED review marker") {
		t.Fatalf("result = %#v, want retryable approve-marker-required failure", result)
	}
	if len(github.removeLabelCalls) != 0 {
		t.Fatalf("removeLabelCalls = %#v, want no spec-reviewing removal", github.removeLabelCalls)
	}
	if len(github.addLabelCalls) != 0 {
		t.Fatalf("addLabelCalls = %#v, want no spec-ready add", github.addLabelCalls)
	}
}

func TestProcessClaimedItemDoesNotTransitionSpecLabelsForCleanNoopCommentPolicy(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{specpr.ReviewingLabel}, reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings; added clean signal", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings; added clean signal"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment}, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25}})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"loop":{"enabled":true}}`
	loop := storage.LoopRecord{ID: "loop_clean_noop_comment_policy_spec", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(github.addReactionCalls) != 1 || github.addReactionCalls[0].Content != "+1" {
		t.Fatalf("addReactionCalls = %#v, want one +1 reaction", github.addReactionCalls)
	}
	if len(github.removeLabelCalls) != 0 {
		t.Fatalf("removeLabelCalls = %#v, want no spec-reviewing removal for COMMENT clean policy", github.removeLabelCalls)
	}
	if len(github.addLabelCalls) != 0 {
		t.Fatalf("addLabelCalls = %#v, want no spec-ready add for COMMENT clean policy", github.addLabelCalls)
	}
}

func TestProcessClaimedItemAutoMergeApprovesAndEnablesAutoMergeWhenCriteriaPass(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		author:              "octocat",
		currentLogin:        "reviewer",
		labels:              []string{"looper:worker-ready"},
		reviewMarkerMissing: true,
		reviewRequests:      []string{"reviewer"},
		viewBody:            "Implements feature.\n\nCloses #358",
		viewDiff:            "diff --git a/app.go b/app.go\n@@ -1,1 +1,2 @@\n-old\n+new\n+more\n",
		issueDetail:         githubinfra.IssueDetail{Number: 358, Body: "## Acceptance criteria\n- ship app change\n- add more\n", Labels: []string{"triaged", "dispatch/plan"}},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}, LoopConfig: testReviewerLoopConfig(), CustomInstructions: reviewerAutoMergeTestConfig(t), CriteriaVerifier: stubCriteriaVerifier{responses: map[criteria.AcceptanceCriterion]criteria.CriterionAssessment{
		"ship app change": {Verdict: criteria.VerdictPass, Justification: "present in diff", Evidence: []criteria.Evidence{{FilePath: "app.go", StartLine: 1, EndLine: 2}}},
		"add more":        {Verdict: criteria.VerdictPass, Justification: "present in diff", Evidence: []criteria.Evidence{{FilePath: "app.go", StartLine: 2, EndLine: 2}}},
	}}})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"loop":{"enabled":true}}`
	loop := storage.LoopRecord{ID: "loop_auto_merge_pass", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}
	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(github.submitReviewCalls) != 1 || github.submitReviewCalls[0].Event != string(ReviewEventApprove) {
		t.Fatalf("submitReviewCalls = %#v, want one APPROVE review", github.submitReviewCalls)
	}
	if len(github.enableAutoMergeCalls) != 1 || github.enableAutoMergeCalls[0].Strategy != config.ReviewerAutoMergeStrategySquash {
		t.Fatalf("enableAutoMergeCalls = %#v, want one squash auto-merge call", github.enableAutoMergeCalls)
	}
	if github.enableAutoMergeCalls[0].HeadSHA != "abc123" {
		t.Fatalf("enableAutoMergeCalls[0].HeadSHA = %q, want abc123", github.enableAutoMergeCalls[0].HeadSHA)
	}
	if !strings.Contains(github.submitReviewCalls[0].Body, criteriaVerificationHeading) || !strings.Contains(github.submitReviewCalls[0].Body, "app.go:1-2") {
		t.Fatalf("review body = %q, want acceptance criteria evidence", github.submitReviewCalls[0].Body)
	}
	if len(github.removeIssueLabelCalls) != 0 {
		t.Fatalf("removeIssueLabelCalls = %#v, want none", github.removeIssueLabelCalls)
	}
	if len(github.issueCommentCalls) != 0 {
		t.Fatalf("issueCommentCalls = %#v, want no refusal comment", github.issueCommentCalls)
	}
}

func TestProcessClaimedItemAutoMergeApprovesAndCommentsWhenAutoMergeRefused(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	cfg := reviewerAutoMergeTestConfig(t)
	github := &fakeGitHubGateway{
		author:              "octocat",
		currentLogin:        "reviewer",
		labels:              []string{"looper:worker-ready"},
		reviewMarkerMissing: true,
		reviewRequests:      []string{"reviewer"},
		viewBody:            "Implements feature.\n\nCloses #358",
		viewDiff:            "diff --git a/app.go b/app.go\n@@ -1,1 +1,1 @@\n-old\n+new\n",
		issueDetail:         githubinfra.IssueDetail{Number: 358, Body: "## Acceptance criteria\n- ship app change\n", Labels: []string{"triaged", "dispatch/plan"}},
		repositorySettings:  githubinfra.RepositorySettings{AllowSquashMerge: true, AllowMergeCommit: true, AllowRebaseMerge: true, AllowAutoMerge: false},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}, LoopConfig: testReviewerLoopConfig(), CustomInstructions: cfg, CriteriaVerifier: stubCriteriaVerifier{responses: map[criteria.AcceptanceCriterion]criteria.CriterionAssessment{
		"ship app change": {Verdict: criteria.VerdictPass, Justification: "present in diff", Evidence: []criteria.Evidence{{FilePath: "app.go", StartLine: 1, EndLine: 1}}},
	}}})
	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queue item", claim, err)
	}
	if _, err := runner.ProcessClaimedItem(context.Background(), *claim); err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if len(github.submitReviewCalls) != 1 || github.submitReviewCalls[0].Event != string(ReviewEventApprove) {
		t.Fatalf("submitReviewCalls = %#v, want one APPROVE review", github.submitReviewCalls)
	}
	if len(github.enableAutoMergeCalls) != 0 {
		t.Fatalf("enableAutoMergeCalls = %#v, want none", github.enableAutoMergeCalls)
	}
	if len(github.issueCommentCalls) != 1 || !strings.Contains(github.issueCommentCalls[0].Body, autoMergeRefusedCommentMarker) {
		t.Fatalf("issueCommentCalls = %#v, want stamped refusal comment", github.issueCommentCalls)
	}
}

func TestProcessClaimedItemAutoMergeApprovesWithoutRemoteProbeWhenOutOfScope(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		author:              "octocat",
		currentLogin:        "reviewer",
		labels:              []string{"team:backend"},
		reviewMarkerMissing: true,
		reviewRequests:      []string{"reviewer"},
		viewBody:            "Implements feature.\n\nCloses #358",
		viewDiff:            "diff --git a/app.go b/app.go\n@@ -1,1 +1,1 @@\n-old\n+new\n",
		issueDetail:         githubinfra.IssueDetail{Number: 358, Body: "## Acceptance criteria\n- ship app change\n", Labels: []string{"triaged", "dispatch/plan"}},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}, LoopConfig: testReviewerLoopConfig(), CustomInstructions: reviewerAutoMergeTestConfig(t), CriteriaVerifier: stubCriteriaVerifier{responses: map[criteria.AcceptanceCriterion]criteria.CriterionAssessment{
		"ship app change": {Verdict: criteria.VerdictPass, Justification: "present in diff", Evidence: []criteria.Evidence{{FilePath: "app.go", StartLine: 1, EndLine: 1}}},
	}}})
	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queue item", claim, err)
	}
	if _, err := runner.ProcessClaimedItem(context.Background(), *claim); err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if len(github.submitReviewCalls) != 1 || github.submitReviewCalls[0].Event != string(ReviewEventApprove) {
		t.Fatalf("submitReviewCalls = %#v, want one APPROVE review", github.submitReviewCalls)
	}
	if github.repositorySettingsCalls != 0 {
		t.Fatalf("repositorySettingsCalls = %d, want 0", github.repositorySettingsCalls)
	}
	if github.branchProtectionCalls != 0 {
		t.Fatalf("branchProtectionCalls = %d, want 0", github.branchProtectionCalls)
	}
	if len(github.enableAutoMergeCalls) != 0 {
		t.Fatalf("enableAutoMergeCalls = %#v, want none", github.enableAutoMergeCalls)
	}
	if len(github.issueCommentCalls) != 1 || !strings.Contains(github.issueCommentCalls[0].Body, autoMergeRefusedCommentMarker) {
		t.Fatalf("issueCommentCalls = %#v, want stamped refusal comment", github.issueCommentCalls)
	}
	if !strings.Contains(github.issueCommentCalls[0].Body, string(automerge.RefusalReasonScope)) {
		t.Fatalf("issueCommentCalls[0].Body = %q, want scope refusal reason", github.issueCommentCalls[0].Body)
	}
}

func TestProcessClaimedItemAutoMergeCommentsAndRetriagesWhenCriteriaFail(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		author:              "octocat",
		currentLogin:        "reviewer",
		labels:              []string{"looper:worker-ready"},
		reviewMarkerMissing: true,
		reviewRequests:      []string{"reviewer"},
		viewBody:            "Implements feature.\n\nCloses #358",
		viewDiff:            "diff --git a/app.go b/app.go\n@@ -1,1 +1,1 @@\n-old\n+new\n",
		issueDetail:         githubinfra.IssueDetail{Number: 358, Body: "## Acceptance criteria\n- ship app change\n- add tests\n", Labels: []string{"triaged", "dispatch/plan", "other"}},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}, LoopConfig: testReviewerLoopConfig(), CustomInstructions: reviewerAutoMergeTestConfig(t), CriteriaVerifier: stubCriteriaVerifier{responses: map[criteria.AcceptanceCriterion]criteria.CriterionAssessment{
		"ship app change": {Verdict: criteria.VerdictPass, Justification: "present in diff", Evidence: []criteria.Evidence{{FilePath: "app.go", StartLine: 1, EndLine: 1}}},
		"add tests":       {Verdict: criteria.VerdictFail, Justification: "no test change in diff"},
	}}})
	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queue item", claim, err)
	}
	if _, err := runner.ProcessClaimedItem(context.Background(), *claim); err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if len(github.submitReviewCalls) != 1 || github.submitReviewCalls[0].Event != string(ReviewEventComment) {
		t.Fatalf("submitReviewCalls = %#v, want one COMMENT review", github.submitReviewCalls)
	}
	if !strings.Contains(github.submitReviewCalls[0].Body, criteriaFailCommentMarker) {
		t.Fatalf("review body = %q, want criteria fail marker", github.submitReviewCalls[0].Body)
	}
	if len(github.removeIssueLabelCalls) != 1 {
		t.Fatalf("removeIssueLabelCalls = %#v, want one issue label removal", github.removeIssueLabelCalls)
	}
	if got := github.removeIssueLabelCalls[0].Labels; len(got) != 2 || got[0] != "triaged" || got[1] != "dispatch/plan" {
		t.Fatalf("removed labels = %#v, want triaged + dispatch/plan", got)
	}
	if len(github.enableAutoMergeCalls) != 0 {
		t.Fatalf("enableAutoMergeCalls = %#v, want none", github.enableAutoMergeCalls)
	}
}

func TestProcessClaimedItemSkipsLinkedIssueLabelRemovalWhenReviewerHoldApplied(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		author:              "octocat",
		currentLogin:        "reviewer",
		labels:              []string{"looper:worker-ready"},
		reviewMarkerMissing: true,
		reviewRequests:      []string{"reviewer"},
		viewBody:            "Implements feature.\n\nCloses #358",
		viewDiff:            "diff --git a/app.go b/app.go\n@@ -1,1 +1,1 @@\n-old\n+new\n",
		issueDetail:         githubinfra.IssueDetail{Number: 358, Body: "## Acceptance criteria\n- ship app change\n- add tests\n", Labels: []string{"triaged", domain.HoldLabelReviewer}},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}, LoopConfig: testReviewerLoopConfig(), CustomInstructions: reviewerAutoMergeTestConfig(t), CriteriaVerifier: stubCriteriaVerifier{responses: map[criteria.AcceptanceCriterion]criteria.CriterionAssessment{
		"ship app change": {Verdict: criteria.VerdictPass, Justification: "present in diff", Evidence: []criteria.Evidence{{FilePath: "app.go", StartLine: 1, EndLine: 1}}},
		"add tests":       {Verdict: criteria.VerdictFail, Justification: "no test change in diff"},
	}}})
	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queue item", claim, err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !strings.Contains(result.Summary, "currently held") {
		t.Fatalf("result = %#v, want held linked issue skip", result)
	}
	if len(github.submitReviewCalls) != 1 || github.submitReviewCalls[0].Event != string(ReviewEventComment) {
		t.Fatalf("submitReviewCalls = %#v, want criteria failure review before held side effects", github.submitReviewCalls)
	}
	if len(github.removeIssueLabelCalls) != 0 {
		t.Fatalf("removeIssueLabelCalls = %#v, want no held issue mutation", github.removeIssueLabelCalls)
	}
}

func TestProcessClaimedItemFallsBackWhenLinkedIssueLookupIsUnavailable(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		author:              "octocat",
		currentLogin:        "reviewer",
		labels:              []string{"looper:worker-ready"},
		reviewMarkerMissing: true,
		reviewRequests:      []string{"reviewer"},
		viewBody:            "Implements feature.\n\nCloses owner/private#358",
		viewDiff:            "diff --git a/app.go b/app.go\n@@ -1,1 +1,1 @@\n-old\n+new\n",
		issueDetailErr:      &shell.CommandExecutionError{Message: "Command exited with code 1", Result: shell.Result{Stderr: "HTTP 403: Resource not accessible by integration"}},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}, LoopConfig: testReviewerLoopConfig(), CustomInstructions: reviewerAutoMergeTestConfig(t)})
	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(github.submitReviewCalls) != 1 || github.submitReviewCalls[0].Event != string(ReviewEventApprove) {
		t.Fatalf("submitReviewCalls = %#v, want one APPROVE review", github.submitReviewCalls)
	}
	if !strings.Contains(github.submitReviewCalls[0].Body, "No explicit acceptance criteria were stated on the linked issue") {
		t.Fatalf("review body = %q, want non-criteria fallback explanation", github.submitReviewCalls[0].Body)
	}
	if len(github.enableAutoMergeCalls) != 0 {
		t.Fatalf("enableAutoMergeCalls = %#v, want none", github.enableAutoMergeCalls)
	}
	if len(github.removeIssueLabelCalls) != 0 {
		t.Fatalf("removeIssueLabelCalls = %#v, want none", github.removeIssueLabelCalls)
	}
}

func TestProcessClaimedItemRetriesWhenLinkedIssueLookupIsTransient(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		author:              "octocat",
		currentLogin:        "reviewer",
		labels:              []string{"looper:worker-ready"},
		reviewMarkerMissing: true,
		reviewRequests:      []string{"reviewer"},
		viewBody:            "Implements feature.\n\nCloses #358",
		viewDiff:            "diff --git a/app.go b/app.go\n@@ -1,1 +1,1 @@\n-old\n+new\n",
		issueDetailErr:      &githubinfra.TransientError{Err: &shell.CommandExecutionError{Message: "Command exited with code 1", Result: shell.Result{Stderr: `Post "https://api.github.com/graphql": unexpected EOF`}}},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}, LoopConfig: testReviewerLoopConfig(), CustomInstructions: reviewerAutoMergeTestConfig(t)})
	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume {
		t.Fatalf("result = %#v, want retryable_after_resume failure", result)
	}
	if len(github.submitReviewCalls) != 0 {
		t.Fatalf("submitReviewCalls = %#v, want none", github.submitReviewCalls)
	}
	if len(github.enableAutoMergeCalls) != 0 {
		t.Fatalf("enableAutoMergeCalls = %#v, want none", github.enableAutoMergeCalls)
	}
}

func TestProcessClaimedItemDoesNotTreatActionableSummaryMentioningPriorCleanReviewAsNoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Actionable issue found; prior clean review is outdated", Stdout: `__LOOPER_RESULT__={"summary":"Actionable issue found; prior clean review is outdated"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "no matching GitHub review marker") {
		t.Fatalf("result = %#v, want retryable missing-marker failure", result)
	}
	if github.reviewMarkerCalls == 0 {
		t.Fatalf("reviewMarkerCalls = %d, want marker lookup for actionable summary", github.reviewMarkerCalls)
	}
	if len(github.addReactionCalls) != 0 {
		t.Fatalf("addReactionCalls = %#v, want no clean noop reaction", github.addReactionCalls)
	}
}

func TestProcessClaimedItemStopsOnIdenticalReviewBodyWithDifferentMarkerHead(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	body := "same actionable finding <!-- looper:review loop=loop_identical_marker_head head=new-head outcome=actionable -->"
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerBody: body}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Same findings", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25, StopOnIdenticalOutput: true}})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	previous := normalizedFindingFingerprint("same actionable finding <!-- looper:review loop=loop_identical_marker_head head=old-head outcome=actionable -->")
	metadata := fmt.Sprintf(`{"followUpdates":true,"loop":{"enabled":true,"lastOutputFingerprint":%q}}`, previous)
	loop := storage.LoopRecord{ID: "loop_identical_marker_head", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}
	extraQueue := storage.QueueItemRecord{ID: "queue_identical_marker_head_extra", ProjectID: stringPtr("project_1"), LoopID: stringPtr(loop.ID), Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, DedupeKey: "reviewer:extra", Priority: storage.QueuePriorityReviewer, Status: "queued", AvailableAt: nowISO, Attempts: 0, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(ctx, extraQueue); err != nil {
		t.Fatalf("Queue.Upsert(extra) error = %v", err)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(ctx, loop.ID)
	if err != nil || updatedLoop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updatedLoop, err)
	}
	if updatedLoop.Status != "terminated" {
		t.Fatalf("loop status = %q, want terminated", updatedLoop.Status)
	}
	if updatedLoop.MetadataJSON == nil || !contains(*updatedLoop.MetadataJSON, `"terminationReason":"identical_output"`) {
		t.Fatalf("loop metadata = %#v, want identical_output termination", updatedLoop.MetadataJSON)
	}
	cancelledQueue, err := fixture.repos.Queue.GetByID(ctx, extraQueue.ID)
	if err != nil || cancelledQueue == nil {
		t.Fatalf("Queue.GetByID(extra) = (%#v, %v), want queue", cancelledQueue, err)
	}
	if cancelledQueue.Status != "cancelled" {
		t.Fatalf("extra queue status = %q, want cancelled", cancelledQueue.Status)
	}
}

func TestDiscoverPullRequestsDoesNotReactivateTerminalLoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"loop":{"enabled":true,"status":"terminated","terminationReason":"identical_output"}}`
	loop := storage.LoopRecord{ID: "loop_terminal", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "terminated", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(ctx, DiscoveryInput{ProjectID: "project_1", Repo: repo, Limit: 10})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want none", result.QueueItems)
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(ctx, loop.ID)
	if err != nil || updatedLoop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updatedLoop, err)
	}
	if updatedLoop.Status != "terminated" {
		t.Fatalf("loop status = %q, want terminated", updatedLoop.Status)
	}
}

func TestDiscoverPullRequestsDoesNotReactivateMetadataTerminalLoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	metadata := `{"followUpdates":true,"loop":{"enabled":true,"status":"terminated","terminationReason":"identical_output"}}`
	loop := storage.LoopRecord{ID: "loop_metadata_terminal", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(ctx, DiscoveryInput{ProjectID: "project_1", Repo: repo, Limit: 10})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want none", result.QueueItems)
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(ctx, loop.ID)
	if err != nil || updatedLoop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updatedLoop, err)
	}
	if updatedLoop.Status != "completed" {
		t.Fatalf("loop status = %q, want unchanged completed", updatedLoop.Status)
	}
}

func TestProcessClaimedItemRestartsAutomaticResumeFromDiscoverForFreshReviewRequests(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Resumed review", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_legacy_resume", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	legacyCheckpoint := reviewerCheckpoint{Detail: &checkpointDetail{Title: "Review me", State: "OPEN", HeadSHA: "abc123"}, ResumePolicy: "advance_from_checkpoint"}
	legacyRun := storage.RunRecord{ID: "run_legacy", LoopID: loop.ID, Status: "failed", CurrentStep: stringPtr(string(stepClaim)), LastCompletedStep: stringPtr(string(stepFilter)), CheckpointJSON: stringPtr(mustMarshalJSON(legacyCheckpoint)), StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), legacyRun); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	queue, err := runner.enqueue(context.Background(), enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success after fresh discover", result)
	}
	if github.viewCalls == 0 || len(agent.starts) != 1 {
		t.Fatalf("viewCalls=%d agent starts=%d, want fresh discover and review", github.viewCalls, len(agent.starts))
	}
}

func TestEnqueueScopesReviewerDedupeKeyToLoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	project2ID := "project_2"
	loop1ID := "loop_1"
	loop2ID := "loop_2"
	nowISO := fixture.nowISO()
	baseBranch := "main"
	repoPath2 := filepath.Join(t.TempDir(), "repo-2")
	repo := "acme/looper"
	prNumber := int64(42)

	if err := fixture.repos.Projects.Upsert(context.Background(), storage.ProjectRecord{ID: project2ID, Name: "Looper Two", RepoPath: repoPath2, BaseBranch: &baseBranch, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Projects.Upsert(project_2) error = %v", err)
	}
	for _, loop := range []storage.LoopRecord{
		{ID: loop1ID, Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO},
		{ID: loop2ID, Seq: 2, ProjectID: project2ID, Type: "reviewer", TargetType: "pull_request", Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO},
	} {
		if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
			t.Fatalf("Loops.Upsert(%s) error = %v", loop.ID, err)
		}
	}

	first, err := runner.enqueue(context.Background(), enqueueInput{ProjectID: "project_1", LoopID: loop1ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue(first) error = %v", err)
	}
	second, err := runner.enqueue(context.Background(), enqueueInput{ProjectID: project2ID, LoopID: loop2ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue(second) error = %v", err)
	}
	if first.ID == second.ID {
		t.Fatalf("enqueue(second) reused queue item %q across loops", second.ID)
	}
	if second.LoopID == nil || *second.LoopID != loop2ID {
		t.Fatalf("second loopID = %#v, want %q", second.LoopID, loop2ID)
	}
	if second.DedupeKey != buildReviewerDedupeKey(project2ID, loop2ID, repo, prNumber) {
		t.Fatalf("second dedupe key = %q, want scoped reviewer key", second.DedupeKey)
	}
	items, err := fixture.repos.Queue.List(context.Background())
	if err != nil {
		t.Fatalf("Queue.List() error = %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("len(Queue.List()) = %d, want 2", len(items))
	}
}

func TestProcessClaimedItemCompletesAgentNativeReviewWithoutGoPublish(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Please add tests", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	firstClaim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || firstClaim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", firstClaim, err)
	}
	firstResult, err := runner.ProcessClaimedItem(context.Background(), *firstClaim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem(first) error = %v", err)
	}
	if firstResult.Status != "success" {
		t.Fatalf("first result = %#v, want success", firstResult)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) = %d, want 1", len(agent.starts))
	}
	runs, err := fixture.repos.Runs.ListByLoop(context.Background(), firstResult.LoopID)
	if err != nil {
		t.Fatalf("Runs.ListByLoop() error = %v", err)
	}
	if len(runs) == 0 || runs[0].LastCompletedStep == nil || *runs[0].LastCompletedStep != string(stepPublish) {
		t.Fatalf("runs[0] = %#v, want lastCompletedStep=publish", runs)
	}
	queueAfterSuccess, err := fixture.repos.Queue.GetByID(context.Background(), firstClaim.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queueAfterSuccess == nil || queueAfterSuccess.Status != "completed" {
		t.Fatalf("queue after success = %#v, want completed", queueAfterSuccess)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), firstResult.LoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.Status != "completed" || loop.MetadataJSON == nil || !contains(*loop.MetadataJSON, `"lastPublishedHeadSha":"abc123"`) {
		t.Fatalf("loop after success = %#v, want completed with lastPublishedHeadSha", loop)
	}
}

func TestProcessClaimedItemAgentNativeReviewCompletesWithoutGoPublish(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Please add tests", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	firstClaim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || firstClaim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", firstClaim, err)
	}
	firstResult, err := runner.ProcessClaimedItem(context.Background(), *firstClaim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem(first) error = %v", err)
	}
	if firstResult.Status != "success" {
		t.Fatalf("first result = %#v, want success", firstResult)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("agent starts=%d, want agent review", len(agent.starts))
	}
}

func TestProcessClaimedItemRequiresSideEffectsBeforeRecordingPublishSuccess(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerOutcome: "clean", addReactionErr: fmt.Errorf("reaction failed")}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "LGTM", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "Failed to add clean-review reaction") {
		t.Fatalf("result = %#v, want retryable side-effect failure", result)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || (loop.MetadataJSON != nil && contains(*loop.MetadataJSON, `"lastPublishedHeadSha":"abc123"`)) {
		t.Fatalf("loop after failed side effect = %#v, want no lastPublishedHeadSha", loop)
	}
}

func TestProcessClaimedItemRequiresActionableSideEffectsBeforeRecordingPublishSuccess(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerOutcome: "actionable", removeReactionErr: fmt.Errorf("remove reaction failed")}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Please add tests", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "Failed to remove stale clean-review reaction") {
		t.Fatalf("result = %#v, want retryable side-effect failure", result)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || (loop.MetadataJSON != nil && contains(*loop.MetadataJSON, `"lastPublishedHeadSha":"abc123"`)) {
		t.Fatalf("loop after failed actionable side effect = %#v, want no lastPublishedHeadSha", loop)
	}
}

func TestProcessClaimedItemAppliesCleanSpecSideEffectsBeforePublishSuccess(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{specpr.ReviewingLabel}, reviewRequests: []string{"octocat"}, reviewMarkerOutcome: "clean", reviewMarkerEvent: ReviewEventApprove}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "LGTM", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: false, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(github.addReactionCalls) != 1 || github.addReactionCalls[0].Content != "+1" {
		t.Fatalf("addReactionCalls = %#v, want one +1 reaction", github.addReactionCalls)
	}
	if len(github.removeLabelCalls) != 1 || github.removeLabelCalls[0].Labels[0] != specpr.ReviewingLabel {
		t.Fatalf("removeLabelCalls = %#v, want spec-reviewing removal", github.removeLabelCalls)
	}
	if len(github.addLabelCalls) != 1 || github.addLabelCalls[0].Labels[0] != specpr.ReadyLabel {
		t.Fatalf("addLabelCalls = %#v, want spec-ready add", github.addLabelCalls)
	}
}

func TestProcessClaimedItemAppliesCleanSpecSideEffectsWithConfiguredReviewingLabel(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	const reviewingLabel = "team:spec-reviewing"
	github := &fakeGitHubGateway{labels: []string{reviewingLabel}, reviewRequests: []string{"octocat"}, reviewMarkerOutcome: "clean", reviewMarkerEvent: ReviewEventApprove}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "LGTM", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll, IncludeSpecReviewingLabel: true, SpecReviewingLabel: reviewingLabel}})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(github.removeLabelCalls) != 1 || github.removeLabelCalls[0].Labels[0] != reviewingLabel {
		t.Fatalf("removeLabelCalls = %#v, want configured reviewing label removal", github.removeLabelCalls)
	}
	if len(github.addLabelCalls) != 1 || github.addLabelCalls[0].Labels[0] != specpr.ReadyLabel {
		t.Fatalf("addLabelCalls = %#v, want spec-ready add", github.addLabelCalls)
	}
}

func TestProcessClaimedItemRefreshesReviewStateBeforeSpecReadyTransition(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{specpr.ReviewingLabel}, reviewRequests: []string{"octocat"}, reviewDecision: "CHANGES_REQUESTED", useReviewStateAfterFirstView: true, reviewMarkerOutcome: "clean", reviewMarkerEvent: ReviewEventApprove}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "LGTM", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if github.viewCalls < 2 {
		t.Fatalf("viewCalls = %d, want publish detail refresh before spec-ready transition", github.viewCalls)
	}
	if len(github.removeLabelCalls) != 1 || github.removeLabelCalls[0].Labels[0] != specpr.ReviewingLabel {
		t.Fatalf("removeLabelCalls = %#v, want spec-reviewing removal after refresh", github.removeLabelCalls)
	}
	if len(github.addLabelCalls) != 1 || github.addLabelCalls[0].Labels[0] != specpr.ReadyLabel {
		t.Fatalf("addLabelCalls = %#v, want spec-ready add after refresh", github.addLabelCalls)
	}
}

func TestApplyVerifiedReviewSideEffectsPreservesCheckedHeadForSpecTransition(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		labels:                 []string{specpr.ReviewingLabel},
		reviewDecision:         "APPROVED",
		changeHeadOnSecondView: true,
	}
	runner := New(Options{
		DB:           fixture.coordinator.DB(),
		Repos:        fixture.repos,
		GitHub:       github,
		Logger:       fixture.logger,
		Now:          fixture.now,
		ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove},
	})
	input := stepInput{
		Project:  storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"},
		Loop:     storage.LoopRecord{ProjectID: "project_1", Type: "reviewer"},
		Repo:     "acme/looper",
		PRNumber: 42,
	}
	checkpoint := reviewerCheckpoint{
		Detail: &checkpointDetail{HeadSHA: "abc123", Labels: []string{specpr.ReviewingLabel}},
	}
	detail := PullRequestDetail{HeadSHA: "abc123", Labels: []string{specpr.ReviewingLabel}}

	err := runner.applyVerifiedReviewSideEffects(context.Background(), input, checkpoint, detail, ReviewMarkerResult{Found: true, Outcome: "clean", Event: ReviewEventApprove})
	if err == nil || !contains(err.Error(), "PR head changed before spec-ready transition: expected abc123, got new-head") {
		t.Fatalf("applyVerifiedReviewSideEffects() error = %v, want head drift failure", err)
	}
	if len(github.removeLabelCalls) != 0 || len(github.addLabelCalls) != 0 {
		t.Fatalf("label calls = remove:%#v add:%#v, want none after head drift", github.removeLabelCalls, github.addLabelCalls)
	}
}

func TestApplyCleanNoopReviewSideEffectsPreservesCheckedHeadForSpecTransition(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		labels:                 []string{specpr.ReviewingLabel},
		reviewDecision:         "APPROVED",
		changeHeadOnSecondView: true,
	}
	runner := New(Options{
		DB:           fixture.coordinator.DB(),
		Repos:        fixture.repos,
		GitHub:       github,
		Logger:       fixture.logger,
		Now:          fixture.now,
		ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove},
	})
	input := stepInput{
		Project:  storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repos/looper"},
		Loop:     storage.LoopRecord{ProjectID: "project_1", Type: "reviewer"},
		Repo:     "acme/looper",
		PRNumber: 42,
	}
	checkpoint := reviewerCheckpoint{
		Detail: &checkpointDetail{HeadSHA: "abc123", Labels: []string{specpr.ReviewingLabel}},
	}
	detail := PullRequestDetail{HeadSHA: "abc123", Labels: []string{specpr.ReviewingLabel}}

	err := runner.applyCleanNoopReviewSideEffects(context.Background(), input, checkpoint, detail)
	if err == nil || !contains(err.Error(), "PR head changed before spec-ready transition: expected abc123, got new-head") {
		t.Fatalf("applyCleanNoopReviewSideEffects() error = %v, want head drift failure", err)
	}
	if len(github.removeLabelCalls) != 0 || len(github.addLabelCalls) != 0 {
		t.Fatalf("label calls = remove:%#v add:%#v, want none after head drift", github.removeLabelCalls, github.addLabelCalls)
	}
}

func TestProcessClaimedItemDoesNotTransitionSpecLabelsForCleanCommentReview(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{specpr.ReviewingLabel}, reviewRequests: []string{"octocat"}, reviewMarkerOutcome: "clean", reviewMarkerEvent: ReviewEventComment}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "LGTM", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: false, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(github.addReactionCalls) != 1 || github.addReactionCalls[0].Content != "+1" {
		t.Fatalf("addReactionCalls = %#v, want one +1 reaction", github.addReactionCalls)
	}
	if len(github.removeLabelCalls) != 0 {
		t.Fatalf("removeLabelCalls = %#v, want no spec-reviewing removal for COMMENT review", github.removeLabelCalls)
	}
	if len(github.addLabelCalls) != 0 {
		t.Fatalf("addLabelCalls = %#v, want no spec-ready add for COMMENT review", github.addLabelCalls)
	}
}

func TestProcessClaimedItemDoesNotTransitionSpecLabelsWhenPRReviewStateIsNotClean(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		reviewDecision string
		comments       []map[string]any
	}{
		{name: "changes requested", reviewDecision: "CHANGES_REQUESTED"},
		{name: "unresolved thread", comments: []map[string]any{{"state": "UNRESOLVED"}}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			github := &fakeGitHubGateway{labels: []string{specpr.ReviewingLabel}, reviewRequests: []string{"octocat"}, reviewDecision: tt.reviewDecision, comments: tt.comments, reviewMarkerOutcome: "clean", reviewMarkerEvent: ReviewEventApprove}
			agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "LGTM", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
			runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true})

			if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
				t.Fatalf("DiscoverPullRequests() error = %v", err)
			}
			claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
			if err != nil || claim == nil {
				t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
			}
			result, err := runner.ProcessClaimedItem(context.Background(), *claim)
			if err != nil {
				t.Fatalf("ProcessClaimedItem() error = %v", err)
			}
			if result.Status != "success" {
				t.Fatalf("result = %#v, want success", result)
			}
			if len(github.addReactionCalls) != 1 || github.addReactionCalls[0].Content != "+1" {
				t.Fatalf("addReactionCalls = %#v, want one +1 reaction", github.addReactionCalls)
			}
			if len(github.removeLabelCalls) != 0 {
				t.Fatalf("removeLabelCalls = %#v, want no spec-reviewing removal for unclean PR", github.removeLabelCalls)
			}
			if len(github.addLabelCalls) != 0 {
				t.Fatalf("addLabelCalls = %#v, want no spec-ready add for unclean PR", github.addLabelCalls)
			}
		})
	}
}

func TestProcessClaimedItemFailsWhenAgentMissingCompletionMarker(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "posted maybe", Stdout: "posted maybe"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "valid completion marker") {
		t.Fatalf("result = %#v, want retryable completion marker failure", result)
	}
	latestRun, err := fixture.repos.Runs.GetLatestByLoopID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("GetLatestByLoopID() error = %v", err)
	}
	checkpoint := parseCheckpoint(latestRun.CheckpointJSON)
	if checkpoint.ResumePolicy != "advance_from_checkpoint" || checkpoint.PendingReview == nil || checkpoint.PendingReview.MarkerVerificationMisses != 1 {
		t.Fatalf("checkpoint = %#v, want pending marker verification before retry", checkpoint)
	}
}

func TestProcessClaimedItemRecoversMissingCompletionMarkerWhenReviewMarkerExists(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerOutcome: "actionable"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "posted maybe", Stdout: "posted maybe"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success after marker recovery", result)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.MetadataJSON == nil || !contains(*loop.MetadataJSON, `"lastPublishedHeadSha":"abc123"`) {
		t.Fatalf("loop after recovered publish = %#v, want lastPublishedHeadSha recorded", loop)
	}
	if github.reviewMarkerCalls != 2 {
		t.Fatalf("review marker calls = %d, want parse recovery lookup plus publish verification", github.reviewMarkerCalls)
	}
}

func TestProcessClaimedItemRecoversMissingCompletionMarkerWithLegacyReviewMarkerID(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerExactMissing: true, reviewMarkerBody: "posted review <!-- looper:review id=reviewer:legacy-loop head=abc123 outcome=actionable -->"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "posted maybe", Stdout: "posted maybe"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success after legacy marker recovery", result)
	}
	if github.reviewMarkerCalls < 3 {
		t.Fatalf("review marker calls = %d, want exact miss followed by tolerant lookup", github.reviewMarkerCalls)
	}
	for i, input := range github.reviewMarkerInputs {
		if input.AuthorLogin == "" {
			t.Fatalf("review marker input %d has empty author login; tolerant lookup must stay author-scoped", i)
		}
	}
	wantBareID := fmt.Sprintf("id=reviewer:%s ", result.LoopID)
	if !contains(github.reviewMarkerInputs[1].Marker, wantBareID) {
		t.Fatalf("tolerant marker = %q, want exact bare loop id without prefix collisions", github.reviewMarkerInputs[1].Marker)
	}
}

func TestProcessClaimedItemRecoversFailedAgentRunWhenReviewMarkerExists(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, removeReviewRequestOnSecondView: true, reviewMarkerOutcome: "clean"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "failed", Summary: "posted review, failed to add reaction"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success after failed-run marker recovery", result)
	}
	if github.reviewMarkerCalls != 2 {
		t.Fatalf("review marker calls = %d, want non-completed recovery lookup plus publish verification", github.reviewMarkerCalls)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) = %d, want one review execution", len(agent.starts))
	}
}

func TestProcessClaimedItemRetriesWhenAgentReviewMarkerMissing(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true, reviewMarkerBodyExplicit: true, reviewMarkerInlineCommentBodies: []string{"inline-only retry finding"}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "posted", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}, {Status: "completed", Summary: "posted again", Stdout: `__LOOPER_RESULT__={"summary":"posted review again"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "no matching GitHub review marker") {
		t.Fatalf("result = %#v, want retryable missing marker failure", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) after first attempt = %d, want 1", len(agent.starts))
	}
	github.reviewMarkerMissing = false
	fixture.advance(time.Hour)
	claim, err = fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("retry ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err = runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("retry ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("retry result = %#v, want success", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) after retry = %d, want marker recheck without review rerun", len(agent.starts))
	}
	if github.reviewMarkerCalls != 4 {
		t.Fatalf("review marker calls = %d, want exact, bare-loop, and suffixed-loop initial lookups plus retry", github.reviewMarkerCalls)
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil || updatedLoop == nil || updatedLoop.MetadataJSON == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop metadata", updatedLoop, err)
	}
	want := normalizedFindingFingerprint(strings.Join(github.reviewMarkerInlineCommentBodies, "\n"))
	if !contains(*updatedLoop.MetadataJSON, fmt.Sprintf(`"lastOutputFingerprint":"%s"`, want)) {
		t.Fatalf("loop metadata = %s, want inline-only retry fingerprint %s", *updatedLoop.MetadataJSON, want)
	}
}

func TestProcessClaimedItemPreservesTerminalReviewerFailureMetadata(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "posted", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})
	ctx := context.Background()

	if _, err := runner.DiscoverPullRequests(ctx, DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	agent.onStart = func(AgentRunInput) {
		loop, err := fixture.repos.Loops.GetByID(ctx, *claim.LoopID)
		if err != nil || loop == nil {
			t.Fatalf("Loops.GetByID() = (%#v, %v), want loop during agent start", loop, err)
		}
		metadata := mustMarshalJSON(map[string]any{"loop": map[string]any{"status": "failed"}})
		loop.MetadataJSON = &metadata
		if err := fixture.repos.Loops.Upsert(ctx, *loop); err != nil {
			t.Fatalf("Loops.Upsert() error = %v", err)
		}
	}

	result, err := runner.ProcessClaimedItem(ctx, *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "no matching GitHub review marker") {
		t.Fatalf("result = %#v, want retryable failed marker-miss result", result)
	}
	queue, err := fixture.repos.Queue.GetByID(ctx, claim.ID)
	if err != nil || queue == nil {
		t.Fatalf("Queue.GetByID() = (%#v, %v), want queue", queue, err)
	}
	if queue.Status != "manual_intervention" || queue.FinishedAt == nil || queue.LastErrorKind == nil || *queue.LastErrorKind != string(FailureRetryableAfterResume) {
		t.Fatalf("queue = %#v, want terminal manual_intervention queue preserving retryable failure", queue)
	}
	loop, err := fixture.repos.Loops.GetByID(ctx, result.LoopID)
	if err != nil || loop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", loop, err)
	}
	if loop.Status != "failed" || loop.NextRunAt != nil || terminalReviewerLoopReason(*loop) != "failed" {
		t.Fatalf("loop = %#v, want failed terminal loop with no next run", loop)
	}
}

func TestProcessClaimedItemRerunsReviewAfterRepeatedAgentReviewMarkerMisses(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "posted", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}, {Status: "completed", Summary: "posted again", Stdout: `__LOOPER_RESULT__={"summary":"posted review again"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "retrying marker verification") {
		t.Fatalf("result = %#v, want retryable marker recheck failure", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) after first attempt = %d, want 1", len(agent.starts))
	}

	fixture.advance(time.Hour)
	claim, err = fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("retry ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err = runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("retry ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "no matching GitHub review marker") {
		t.Fatalf("retry result = %#v, want retryable missing marker failure", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) after marker retry = %d, want no review rerun yet", len(agent.starts))
	}
	latestRun, err := fixture.repos.Runs.GetLatestByLoopID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("GetLatestByLoopID() error = %v", err)
	}
	if latestRun == nil {
		t.Fatal("latest run = nil, want failed run")
	}
	checkpoint := parseCheckpoint(latestRun.CheckpointJSON)
	if checkpoint.ResumePolicy != "rerun_review" || checkpoint.PendingReview != nil {
		t.Fatalf("checkpoint = %#v, want cleared pending review with rerun_review after repeated marker misses", checkpoint)
	}
}

func TestProcessClaimedItemRecordsReviewWhenRequestRemovedAfterMarkerAppears(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "posted", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "no matching GitHub review marker") {
		t.Fatalf("result = %#v, want retryable missing marker failure", result)
	}
	github.reviewMarkerMissing = false
	github.reviewRequests = []string{"someoneelse"}
	fixture.advance(time.Hour)
	claim, err = fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("retry ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err = runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("retry ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("retry result = %#v, want publish success after marker appears", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) after retry = %d, want no second review", len(agent.starts))
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.MetadataJSON == nil || !contains(*loop.MetadataJSON, `"lastPublishedHeadSha":"abc123"`) {
		t.Fatalf("loop after marker recovery = %#v, want lastPublishedHeadSha recorded", loop)
	}
}

func TestProcessClaimedItemSkipsRerunReviewWhenRequestRemovedAndMarkerMissing(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "posted", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "no matching GitHub review marker") {
		t.Fatalf("result = %#v, want retryable missing marker failure", result)
	}
	github.reviewRequests = []string{"someoneelse"}
	fixture.advance(time.Hour)
	claim, err = fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("retry ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err = runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("retry ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !contains(result.Summary, "current user is not requested for review") {
		t.Fatalf("retry result = %#v, want eligibility skip when marker is still missing", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) after retry = %d, want no second review", len(agent.starts))
	}
}

func TestProcessClaimedItemRejectsUnverifiableLegacyPendingReview(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})
	ctx := context.Background()

	prNumber := int64(42)
	repo := "acme/looper"
	loopTarget := "pr:42"
	loop := storage.LoopRecord{ID: "loop_legacy", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	if _, err := runner.enqueue(ctx, enqueueInput{ProjectID: loop.ProjectID, LoopID: loop.ID, Repo: repo, PRNumber: prNumber}); err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed queue item", claim, err)
	}

	legacyCheckpoint := reviewerCheckpoint{
		Detail:        &checkpointDetail{Title: "Review me", State: "OPEN", HeadSHA: "abc123", ReviewRequests: []string{"octocat"}},
		Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
		PendingReview: &pendingReviewCheckpoint{HeadSHA: "abc123", Event: ReviewEventComment, Summary: "legacy review already posted"},
		ResumePolicy:  "advance_from_checkpoint",
	}
	checkpointJSON := mustMarshalJSON(legacyCheckpoint)
	run := storage.RunRecord{ID: "run_legacy", LoopID: loop.ID, Status: "failed", CurrentStep: stringPtr(string(stepPublish)), LastCompletedStep: stringPtr(string(stepReview)), CheckpointJSON: &checkpointJSON, StartedAt: fixture.nowISO(), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Runs.Upsert(ctx, run); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	result, err := runner.ProcessClaimedItem(ctx, *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "Legacy pending review checkpoint cannot be verified") {
		t.Fatalf("result = %#v, want retryable legacy verification failure", result)
	}
	if len(agent.starts) != 0 {
		t.Fatalf("len(agent.starts) = %d, want no review rerun in failed publish attempt", len(agent.starts))
	}
	if github.reviewMarkerCalls != 0 {
		t.Fatalf("reviewMarkerCalls = %d, want agent-native marker lookup skipped for legacy pending review", github.reviewMarkerCalls)
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(ctx, loop.ID)
	if err != nil || updatedLoop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", updatedLoop, err)
	}
	if updatedLoop.MetadataJSON != nil && contains(*updatedLoop.MetadataJSON, `"lastPublishedHeadSha":"abc123"`) {
		t.Fatalf("loop metadata = %v, want no legacy publish progress", updatedLoop.MetadataJSON)
	}
}

func TestProcessClaimedItemRetriesWhenAgentNativeReviewApprovesWithoutPermission(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerEvent: ReviewEventApprove}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Looks good", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: false})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "no matching GitHub review marker") {
		t.Fatalf("result = %#v, want retryable disallowed approval marker failure", result)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || (loop.MetadataJSON != nil && contains(*loop.MetadataJSON, `"lastPublishedHeadSha":"abc123"`)) {
		t.Fatalf("loop after failed publish = %#v, want no lastPublishedHeadSha", loop)
	}
}

func TestProcessClaimedItemRecordsAgentNativePublishWhenReviewRequestRemovedAfterPosting(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, removeReviewRequestOnSecondView: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Please add tests", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want publish success after marker verification", result)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.MetadataJSON == nil || !contains(*loop.MetadataJSON, `"lastPublishedHeadSha":"abc123"`) {
		t.Fatalf("loop after verified publish = %#v, want lastPublishedHeadSha recorded", loop)
	}
}

func TestProcessClaimedItemRecordsPublishedHeadForAgentNativeReview(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Please add tests", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	firstClaim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || firstClaim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", firstClaim, err)
	}
	firstResult, err := runner.ProcessClaimedItem(context.Background(), *firstClaim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem(first) error = %v", err)
	}
	if firstResult.Status != "success" {
		t.Fatalf("first result = %#v, want success", firstResult)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("agent starts=%d, want agent-native review", len(agent.starts))
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), firstResult.LoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.MetadataJSON == nil || !contains(*loop.MetadataJSON, `"lastPublishedHeadSha":"abc123"`) {
		t.Fatalf("loop after skipped retry = %#v, want lastPublishedHeadSha recorded", loop)
	}
}

func TestProcessClaimedItemAgentNativeReviewCompletesWithoutPublishRetry(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	fixture.repos.Locks.SetNow(fixture.now)
	github := &fakeGitHubGateway{}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Please add tests", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim1, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim1 == nil {
		t.Fatalf("first ClaimNextOfType() = (%#v, %v), want claimed item", claim1, err)
	}
	first, err := runner.ProcessClaimedItem(context.Background(), *claim1)
	if err != nil {
		t.Fatalf("ProcessClaimedItem(first) error = %v", err)
	}
	if first.Status != "success" {
		t.Fatalf("first = %#v, want success", first)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), claim1.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "completed" {
		t.Fatalf("queue = %#v, want completed item", queue)
	}
}

func TestProcessClaimedItemMarksStaleWhenHeadChangesBeforePublish(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{changeHeadOnSecondView: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Review old head", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}, {Status: "completed", Summary: "Review new head", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	firstClaim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || firstClaim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", firstClaim, err)
	}
	firstResult, err := runner.ProcessClaimedItem(context.Background(), *firstClaim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem(first) error = %v", err)
	}
	if firstResult.Status != "skipped" || !contains(firstResult.Summary, "PR head changed before publish") {
		t.Fatalf("first result = %#v, want stale head-change skip", firstResult)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("agent starts=%d, want 1", len(agent.starts))
	}
}

func TestProcessClaimedItemNotifiesWhenReviewAgentStarts(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Looks good", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	notifications := make([]AgentExecutionStartedInput, 0, 1)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, OnAgentExecutionStarted: func(_ context.Context, input AgentExecutionStartedInput) error {
		notifications = append(notifications, input)
		return nil
	}})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claimed, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(notifications) != 1 {
		t.Fatalf("len(notifications) = %d, want 1", len(notifications))
	}
	if notifications[0].Subtitle != "acme/looper#42" || notifications[0].Body != "Review started" {
		t.Fatalf("notifications[0] = %#v, want review-start payload", notifications[0])
	}
}

func TestProcessClaimedItemRunsReviewerInDedicatedWorktree(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	git := &fakeGitGateway{}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Looks good", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claimed, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(git.createCalls) != 1 {
		t.Fatalf("len(git.createCalls) = %d, want 1", len(git.createCalls))
	}
	if git.createCalls[0].Branch != "pr-42-head" {
		t.Fatalf("create branch = %q, want PR-scoped branch", git.createCalls[0].Branch)
	}
	if git.createCalls[0].PRNumber != 42 {
		t.Fatalf("create PR number = %d, want 42", git.createCalls[0].PRNumber)
	}
	if len(git.prepareCalls) != 1 {
		t.Fatalf("len(git.prepareCalls) = %d, want 1", len(git.prepareCalls))
	}
	if git.prepareCalls[0].Branch != "pr-42-head" {
		t.Fatalf("prepare branch = %q, want PR-scoped branch", git.prepareCalls[0].Branch)
	}
	if git.prepareCalls[0].Ref != "refs/pull/42/head" {
		t.Fatalf("prepare ref = %q, want PR head ref", git.prepareCalls[0].Ref)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) = %d, want 1", len(agent.starts))
	}
	if len(git.cleanupCalls) != 1 {
		t.Fatalf("len(git.cleanupCalls) = %d, want 1", len(git.cleanupCalls))
	}
	if agent.starts[0].WorkingDirectory != git.worktreePath {
		t.Fatalf("agent working dir = %q, want %q", agent.starts[0].WorkingDirectory, git.worktreePath)
	}
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	if agent.starts[0].WorkingDirectory == project.RepoPath {
		t.Fatalf("agent working dir = repo path %q, want dedicated worktree", project.RepoPath)
	}
}

func TestRunPrepareWorktreeStepFallsBackWhenCheckpointLacksHeadRef(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	git := &fakeGitGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: git, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}

	checkpoint, err := runner.runPrepareWorktreeStep(context.Background(), stepInput{
		Project:  *project,
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadSHA: "abc123", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
		},
	})
	if err != nil {
		t.Fatalf("runPrepareWorktreeStep() error = %v", err)
	}
	if len(git.createCalls) != 1 {
		t.Fatalf("len(git.createCalls) = %d, want 1", len(git.createCalls))
	}
	if git.createCalls[0].Branch != "pr-42-head" {
		t.Fatalf("create branch = %q, want fallback branch", git.createCalls[0].Branch)
	}
	if git.createCalls[0].PRNumber != 42 {
		t.Fatalf("create PR number = %d, want 42", git.createCalls[0].PRNumber)
	}
	if len(git.prepareCalls) != 1 {
		t.Fatalf("len(git.prepareCalls) = %d, want 1", len(git.prepareCalls))
	}
	if git.prepareCalls[0].Ref != "refs/pull/42/head" {
		t.Fatalf("prepare ref = %q, want PR head ref", git.prepareCalls[0].Ref)
	}
	if checkpoint.Worktree == nil || checkpoint.Worktree.Branch != "pr-42-head" {
		t.Fatalf("checkpoint worktree = %#v, want fallback branch", checkpoint.Worktree)
	}
}

func TestRunPrepareWorktreeStepClearsFixerOwnerTokenWhenReusingPreparedPath(t *testing.T) {
	t.Parallel()

	// Prepared early return still revokes a race-stamped fixer marker so reviewer
	// dirt cannot be adopted later. Seed marker after PreparedAt would otherwise
	// qualify, then clear via the defensive ClearFixerOwnerToken on reuse.
	// (A marker present before prepared check forces full re-prepare — covered
	// by TestRunPrepareWorktreeStepRePreparesWhenFixerMarkerPresent.)
	fixture := newRunnerFixture(t)
	repoPath := t.TempDir()
	worktreeRoot := filepath.Join(t.TempDir(), "worktrees")
	wtPath := filepath.Join(worktreeRoot, "wt-42")
	if err := os.MkdirAll(wtPath, 0o755); err != nil {
		t.Fatalf("MkdirAll worktree: %v", err)
	}
	ensureFakeUsableGitCheckout(wtPath)
	metadata := fmt.Sprintf(`{"worktreeRoot":%q}`, worktreeRoot)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, Logger: fixture.logger, Now: fixture.now})

	checkpoint, err := runner.runPrepareWorktreeStep(context.Background(), stepInput{
		Project:  storage.ProjectRecord{ID: "project_1", RepoPath: repoPath, MetadataJSON: &metadata},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadSHA: "abc123", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: wtPath, Branch: "pr-42-head", BaseBranch: "main", PreparedAt: fixture.nowISO()},
		},
	})
	if err != nil {
		t.Fatalf("runPrepareWorktreeStep() error = %v", err)
	}
	if checkpoint.Worktree == nil || checkpoint.Worktree.Path != wtPath {
		t.Fatalf("Worktree = %#v, want reused prepared path", checkpoint.Worktree)
	}
	got, err := worktreesafety.ReadFixerOwnerToken(wtPath)
	if err != nil {
		t.Fatalf("ReadFixerOwnerToken() error = %v", err)
	}
	if got != "" {
		t.Fatalf("ReadFixerOwnerToken() = %q, want empty after prepared reuse", got)
	}
}

func TestRunPrepareWorktreeStepRePreparesWhenFixerMarkerPresent(t *testing.T) {
	t.Parallel()

	// Intervening fixer ownership invalidates PreparedAt reuse; Create/Prepare
	// reclaim the path and revoke the marker instead of trusting the stale prepare.
	fixture := newRunnerFixture(t)
	repoPath := t.TempDir()
	worktreeRoot := filepath.Join(t.TempDir(), "worktrees")
	wtPath := filepath.Join(worktreeRoot, "wt-42")
	if err := os.MkdirAll(wtPath, 0o755); err != nil {
		t.Fatalf("MkdirAll worktree: %v", err)
	}
	ensureFakeUsableGitCheckout(wtPath)
	const token = "fixer:loop_x:run_y:must-reprepare"
	if err := worktreesafety.WriteFixerOwnerToken(wtPath, token); err != nil {
		t.Fatalf("WriteFixerOwnerToken: %v", err)
	}
	metadata := fmt.Sprintf(`{"worktreeRoot":%q}`, worktreeRoot)
	git := &fakeGitGateway{worktreePath: wtPath}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: git, Logger: fixture.logger, Now: fixture.now})

	checkpoint, err := runner.runPrepareWorktreeStep(context.Background(), stepInput{
		Project:  storage.ProjectRecord{ID: "project_1", RepoPath: repoPath, MetadataJSON: &metadata},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadSHA: "abc123", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: wtPath, Branch: "pr-42-head", BaseBranch: "main", PreparedAt: fixture.nowISO()},
		},
	})
	if err != nil {
		t.Fatalf("runPrepareWorktreeStep() error = %v", err)
	}
	if len(git.createCalls) == 0 {
		t.Fatal("expected CreateWorktree reclaim when fixer marker present")
	}
	if len(git.prepareCalls) == 0 {
		t.Fatal("expected PrepareWorktree after fixer-marker invalidation")
	}
	if checkpoint.Worktree == nil || checkpoint.Worktree.PreparedAt == "" {
		t.Fatalf("Worktree = %#v, want re-prepared path", checkpoint.Worktree)
	}
	got, err := worktreesafety.ReadFixerOwnerToken(wtPath)
	if err != nil {
		t.Fatalf("ReadFixerOwnerToken() error = %v", err)
	}
	if got != "" {
		t.Fatalf("ReadFixerOwnerToken() = %q, want empty after reclaim", got)
	}
}

func TestRunPrepareWorktreeStepRecreatesUnsafeCheckpointAtRepoPath(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repoPath := t.TempDir()
	git := &fakeGitGateway{worktreePath: filepath.Join(t.TempDir(), "wt")}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: git, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	checkpoint, err := runner.runPrepareWorktreeStep(context.Background(), stepInput{
		Project:  storage.ProjectRecord{ID: "project_1", RepoPath: repoPath},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadSHA: "abc123", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: repoPath, Branch: "stale", BaseBranch: "main"},
		},
	})
	if err != nil {
		t.Fatalf("runPrepareWorktreeStep() error = %v", err)
	}
	if len(git.createCalls) != 1 {
		t.Fatalf("len(git.createCalls) = %d, want 1", len(git.createCalls))
	}
	if checkpoint.Worktree == nil || checkpoint.Worktree.Path != git.worktreePath {
		t.Fatalf("checkpoint.Worktree = %#v, want recreated worktree", checkpoint.Worktree)
	}
	if checkpoint.ResumePolicy != "advance_from_checkpoint" {
		t.Fatalf("ResumePolicy = %q, want advance_from_checkpoint", checkpoint.ResumePolicy)
	}
}

func TestRunPrepareWorktreeStepRecreatesCheckpointOutsideWorktreeRoot(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repoPath := t.TempDir()
	worktreeRoot := filepath.Join(t.TempDir(), "looper-worktrees")
	outsidePath := filepath.Join(t.TempDir(), "outside", "wt")
	git := &fakeGitGateway{worktreePath: filepath.Join(worktreeRoot, "wt")}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: git, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	metadata := fmt.Sprintf(`{"worktreeRoot":%q}`, worktreeRoot)

	checkpoint, err := runner.runPrepareWorktreeStep(context.Background(), stepInput{
		Project:  storage.ProjectRecord{ID: "project_1", RepoPath: repoPath, MetadataJSON: &metadata},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadSHA: "abc123", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: outsidePath, Branch: "stale", BaseBranch: "main", PreparedAt: "stale"},
		},
	})
	if err != nil {
		t.Fatalf("runPrepareWorktreeStep() error = %v", err)
	}
	if len(git.createCalls) != 1 {
		t.Fatalf("len(git.createCalls) = %d, want 1", len(git.createCalls))
	}
	if checkpoint.Worktree == nil || checkpoint.Worktree.Path != git.worktreePath {
		t.Fatalf("checkpoint.Worktree = %#v, want recreated worktree", checkpoint.Worktree)
	}
	if got := git.createCalls[0].WorktreeRoot; got != worktreeRoot {
		t.Fatalf("CreateWorktree().WorktreeRoot = %q, want %q", got, worktreeRoot)
	}
}

func TestReviewerWorktreeBranchIgnoresHeadRefName(t *testing.T) {
	t.Parallel()

	branch := reviewerWorktreeBranch(42, reviewerCheckpoint{
		Detail:   &checkpointDetail{HeadRefName: "patch-1"},
		Worktree: &checkpointWorktree{Branch: "pr-42-head"},
	})
	if branch != "pr-42-head" {
		t.Fatalf("reviewerWorktreeBranch() = %q, want existing PR-scoped branch", branch)
	}

	branch = reviewerWorktreeBranch(42, reviewerCheckpoint{
		Detail: &checkpointDetail{HeadRefName: "main"},
	})
	if branch != "pr-42-head" {
		t.Fatalf("reviewerWorktreeBranch() = %q, want PR-scoped fallback", branch)
	}
}

func TestRunReviewStepRepreparesMissingReviewerWorktree(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	git := &fakeGitGateway{}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Looks good", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: git, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}

	checkpoint, err := runner.runReviewStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_1"},
		Run:      storage.RunRecord{ID: "run_1"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: filepath.Join(t.TempDir(), "deleted-worktree"), Branch: "feature/review-me", PreparedAt: fixture.nowISO()},
		},
	})
	if err != nil {
		t.Fatalf("runReviewStep() error = %v", err)
	}
	if len(git.createCalls) != 1 || len(git.prepareCalls) != 1 {
		t.Fatalf("createCalls=%d prepareCalls=%d, want 1 each", len(git.createCalls), len(git.prepareCalls))
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) = %d, want 1", len(agent.starts))
	}
	if agent.starts[0].WorkingDirectory != git.worktreePath {
		t.Fatalf("agent working dir = %q, want %q", agent.starts[0].WorkingDirectory, git.worktreePath)
	}
	if checkpoint.Worktree == nil || checkpoint.Worktree.Path != git.worktreePath {
		t.Fatalf("checkpoint worktree = %#v, want recreated worktree path", checkpoint.Worktree)
	}
}

func TestRunReviewStepUsesStableIdempotencyKeyAcrossRuns(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	agent := &fakeAgentExecutor{results: []AgentResult{
		{Status: "completed", Summary: "Looks good", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`},
		{Status: "completed", Summary: "Looks good", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`},
	}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	worktree := &checkpointWorktree{Path: t.TempDir(), Branch: "pr-42-head", PreparedAt: fixture.nowISO()}
	input := stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_stable_review"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: worktree,
		},
	}

	input.Run = storage.RunRecord{ID: "run_1"}
	if _, err := runner.runReviewStep(context.Background(), input); err != nil {
		t.Fatalf("runReviewStep(run_1) error = %v", err)
	}
	input.Run = storage.RunRecord{ID: "run_2"}
	if _, err := runner.runReviewStep(context.Background(), input); err != nil {
		t.Fatalf("runReviewStep(run_2) error = %v", err)
	}
	if len(agent.starts) != 2 {
		t.Fatalf("len(agent.starts) = %d, want 2", len(agent.starts))
	}
	want := "reviewer:loop_stable_review:abc123"
	for i, start := range agent.starts {
		if start.IdempotencyKey != want {
			t.Fatalf("start %d idempotency key = %q, want %q", i, start.IdempotencyKey, want)
		}
	}
}

func TestRunReviewStepKeepsFullPromptForPendingNativeResumeFallback(t *testing.T) {
	fixture := newRunnerFixture(t)
	ctx := context.Background()
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Looks good", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AgentRuntime: string(config.AgentVendorOpenCode)})
	project, err := fixture.repos.Projects.GetByID(ctx, "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_native_resume_prompt", Seq: 1, ProjectID: project.ID, Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	run := storage.RunRecord{ID: "run_native_resume_prompt", LoopID: loop.ID, Status: "running", StartedAt: fixture.nowISO(), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Runs.Upsert(ctx, run); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	if err := fixture.repos.AgentExecutions.Upsert(ctx, storage.AgentExecutionRecord{ID: "agent_previous_overload", ProjectID: stringPtr(project.ID), LoopID: stringPtr(loop.ID), RunID: stringPtr(run.ID), Vendor: string(config.AgentVendorOpenCode), Status: "completed", NativeSessionID: stringPtr("session-123"), NativeResumeStatus: stringPtr("pending"), StartedAt: fixture.nowISO(), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("AgentExecutions.Upsert() error = %v", err)
	}

	_, err = runner.runReviewStep(ctx, stepInput{
		Project:  *project,
		Loop:     loop,
		Run:      run,
		Repo:     repo,
		PRNumber: prNumber,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "feature/review-me", PreparedAt: fixture.nowISO()},
		},
	})
	if err != nil {
		t.Fatalf("runReviewStep() error = %v", err)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) = %d, want 1", len(agent.starts))
	}
	prompt := agent.starts[0].Prompt
	if strings.Contains(prompt, "Continue the existing Looper reviewer review task") {
		t.Fatalf("prompt = %q, want full review prompt for checkpoint fallback safety", prompt)
	}
	if !strings.Contains(prompt, "Review pull request") && !strings.Contains(prompt, "Minimal PR seed") {
		t.Fatalf("prompt = %q, want full review prompt for checkpoint fallback safety", prompt)
	}
	nativeResumePrompt := agent.starts[0].NativeResumePrompt
	if !strings.Contains(nativeResumePrompt, "Continue the existing Looper reviewer review task") || !strings.Contains(nativeResumePrompt, "idempotency key: reviewer:loop_native_resume_prompt:abc123") {
		t.Fatalf("native resume prompt = %q, want short native resume continuation prompt", nativeResumePrompt)
	}
}

func TestRunReviewStepUsesReReviewPromptForHeadChangeNativeResume(t *testing.T) {
	fixture := newRunnerFixture(t)
	ctx := context.Background()
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Looks good", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AgentRuntime: string(config.AgentVendorOpenCode), NativeResume: config.ReviewerNativeResumeConfig{ReReviewPromptOnHeadChange: true}})
	project, err := fixture.repos.Projects.GetByID(ctx, "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_native_rereview_prompt", Seq: 1, ProjectID: project.ID, Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	run := storage.RunRecord{ID: "run_native_rereview_prompt", LoopID: loop.ID, Status: "running", StartedAt: fixture.nowISO(), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Runs.Upsert(ctx, run); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	metadata := mustMarshalJSON(map[string]any{reviewerNativeResumeMetadataKey: map[string]any{"reason": reviewerNativeResumeReasonHeadChange, "phase": "review", "repo": repo, "prNumber": prNumber, "oldHeadSha": "old-head", "newHeadSha": "new-head"}})
	if err := fixture.repos.AgentExecutions.Upsert(ctx, storage.AgentExecutionRecord{ID: "agent_previous_head_change", ProjectID: stringPtr(project.ID), LoopID: stringPtr(loop.ID), RunID: stringPtr(run.ID), Vendor: string(config.AgentVendorOpenCode), Status: "killed", NativeSessionID: stringPtr("session-123"), NativeResumeStatus: stringPtr("pending"), MetadataJSON: stringPtr(metadata), StartedAt: fixture.nowISO(), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("AgentExecutions.Upsert() error = %v", err)
	}

	_, err = runner.runReviewStep(ctx, stepInput{
		Project:  *project,
		Loop:     loop,
		Run:      run,
		Repo:     repo,
		PRNumber: prNumber,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "current-head"},
			Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "feature/review-me", PreparedAt: fixture.nowISO()},
		},
	})
	if err != nil {
		t.Fatalf("runReviewStep() error = %v", err)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) = %d, want 1", len(agent.starts))
	}
	prompt := agent.starts[0].Prompt
	if strings.Contains(prompt, "PR update re-review") {
		t.Fatalf("full prompt = %q, want checkpoint fallback full review prompt without re-review continuation", prompt)
	}
	nativeResumePrompt := agent.starts[0].NativeResumePrompt
	for _, want := range []string{"PR update re-review", "previous reviewed head SHA: old-head", "head SHA observed at interruption: new-head", "current expected head SHA for this run: current-head", "idempotency key for this run: reviewer:loop_native_rereview_prompt:current-head", "Discard findings"} {
		if !strings.Contains(nativeResumePrompt, want) {
			t.Fatalf("native resume prompt missing %q:\n%s", want, nativeResumePrompt)
		}
	}
	if strings.Contains(nativeResumePrompt, "transient provider interruption") {
		t.Fatalf("native resume prompt = %q, want re-review prompt instead of transient continuation", nativeResumePrompt)
	}
}

func TestRunReviewStepKeepsGenericPromptWhenReReviewPromptFlagDisabled(t *testing.T) {
	fixture := newRunnerFixture(t)
	ctx := context.Background()
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Looks good", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AgentRuntime: string(config.AgentVendorOpenCode), NativeResume: config.ReviewerNativeResumeConfig{OnHeadChange: true}})
	project, err := fixture.repos.Projects.GetByID(ctx, "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_native_rereview_prompt_disabled", Seq: 1, ProjectID: project.ID, Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	run := storage.RunRecord{ID: "run_native_rereview_prompt_disabled", LoopID: loop.ID, Status: "running", StartedAt: fixture.nowISO(), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Runs.Upsert(ctx, run); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	metadata := mustMarshalJSON(map[string]any{reviewerNativeResumeMetadataKey: map[string]any{"reason": reviewerNativeResumeReasonHeadChange, "phase": "review", "repo": repo, "prNumber": prNumber, "oldHeadSha": "old-head", "newHeadSha": "new-head"}})
	if err := fixture.repos.AgentExecutions.Upsert(ctx, storage.AgentExecutionRecord{ID: "agent_previous_head_change_prompt_disabled", ProjectID: stringPtr(project.ID), LoopID: stringPtr(loop.ID), RunID: stringPtr(run.ID), Vendor: string(config.AgentVendorOpenCode), Status: "killed", NativeSessionID: stringPtr("session-123"), NativeResumeStatus: stringPtr("pending"), MetadataJSON: stringPtr(metadata), StartedAt: fixture.nowISO(), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("AgentExecutions.Upsert() error = %v", err)
	}

	_, err = runner.runReviewStep(ctx, stepInput{
		Project:  *project,
		Loop:     loop,
		Run:      run,
		Repo:     repo,
		PRNumber: prNumber,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "current-head"},
			Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "feature/review-me", PreparedAt: fixture.nowISO()},
		},
	})
	if err != nil {
		t.Fatalf("runReviewStep() error = %v", err)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) = %d, want 1", len(agent.starts))
	}
	nativeResumePrompt := agent.starts[0].NativeResumePrompt
	if strings.Contains(nativeResumePrompt, "PR update re-review") || strings.Contains(nativeResumePrompt, "Discard findings") {
		t.Fatalf("native resume prompt = %q, want generic continuation when re-review prompt flag is disabled", nativeResumePrompt)
	}
	if !strings.Contains(nativeResumePrompt, "transient provider interruption") {
		t.Fatalf("native resume prompt = %q, want generic continuation prompt", nativeResumePrompt)
	}
}

func TestRunReviewStepUsesFullPromptWhenPendingNativeResumeVendorDiffers(t *testing.T) {
	fixture := newRunnerFixture(t)
	ctx := context.Background()
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Looks good", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AgentRuntime: string(config.AgentVendorOpenCode)})
	project, err := fixture.repos.Projects.GetByID(ctx, "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_native_resume_vendor_mismatch", Seq: 1, ProjectID: project.ID, Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	run := storage.RunRecord{ID: "run_native_resume_vendor_mismatch", LoopID: loop.ID, Status: "running", StartedAt: fixture.nowISO(), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Runs.Upsert(ctx, run); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	if err := fixture.repos.AgentExecutions.Upsert(ctx, storage.AgentExecutionRecord{ID: "agent_previous_codex_overload", ProjectID: stringPtr(project.ID), LoopID: stringPtr(loop.ID), RunID: stringPtr(run.ID), Vendor: string(config.AgentVendorCodex), Status: "completed", NativeSessionID: stringPtr("session-123"), NativeResumeStatus: stringPtr("pending"), StartedAt: fixture.nowISO(), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("AgentExecutions.Upsert() error = %v", err)
	}

	_, err = runner.runReviewStep(ctx, stepInput{
		Project:  *project,
		Loop:     loop,
		Run:      run,
		Repo:     repo,
		PRNumber: prNumber,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "feature/review-me", PreparedAt: fixture.nowISO()},
		},
	})
	if err != nil {
		t.Fatalf("runReviewStep() error = %v", err)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) = %d, want 1", len(agent.starts))
	}
	prompt := agent.starts[0].Prompt
	if strings.Contains(prompt, "Continue the existing Looper reviewer review task") {
		t.Fatalf("prompt = %q, want full review prompt when native resume is not compatible", prompt)
	}
	if !strings.Contains(prompt, "Review pull request") && !strings.Contains(prompt, "Minimal PR seed") {
		t.Fatalf("prompt = %q, want full review prompt when native resume is not compatible", prompt)
	}
}

func TestRunReviewStepPersistsRepreparedWorktreeBeforeAgentStart(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	git := &fakeGitGateway{worktreePath: filepath.Join(t.TempDir(), "reviewer-worktree")}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: git, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	prNumber := int64(42)
	loopTarget := "pr:42"
	loop := storage.LoopRecord{ID: "loop_1", Seq: 1, ProjectID: project.ID, Type: "reviewer", TargetType: "pull_request", TargetID: &loopTarget, Repo: stringPtr("acme/looper"), PRNumber: &prNumber, Status: "running", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	initialCheckpoint := reviewerCheckpoint{
		Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
		Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
		Worktree: &checkpointWorktree{Path: filepath.Join(t.TempDir(), "deleted-worktree"), Branch: "feature/review-me", PreparedAt: fixture.nowISO()},
	}
	checkpointJSON := mustMarshalJSON(initialCheckpoint)
	run := storage.RunRecord{ID: "run_1", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepReview)), CheckpointJSON: &checkpointJSON, StartedAt: fixture.nowISO(), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Runs.Upsert(context.Background(), run); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	checkpoint, err := runner.runReviewStep(context.Background(), stepInput{
		Project:    *project,
		Loop:       loop,
		Run:        run,
		Repo:       "acme/looper",
		PRNumber:   prNumber,
		Checkpoint: initialCheckpoint,
	})
	if err == nil || !contains(err.Error(), "no queued agent result") {
		t.Fatalf("runReviewStep() error = %v, want no queued agent result", err)
	}
	if checkpoint.Worktree == nil || checkpoint.Worktree.Path != git.worktreePath {
		t.Fatalf("checkpoint worktree = %#v, want recreated worktree path", checkpoint.Worktree)
	}
	persistedRun, err := fixture.repos.Runs.GetByID(context.Background(), run.ID)
	if err != nil || persistedRun == nil {
		t.Fatalf("Runs.GetByID() = (%#v, %v), want run", persistedRun, err)
	}
	persistedCheckpoint := parseCheckpoint(persistedRun.CheckpointJSON)
	if persistedCheckpoint.Worktree == nil || persistedCheckpoint.Worktree.Path != git.worktreePath {
		t.Fatalf("persisted checkpoint worktree = %#v, want recreated worktree path", persistedCheckpoint.Worktree)
	}
}

func TestRunReviewStepRechecksHoldBeforeAgentStart(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{domain.HoldLabelReviewer}}
	agent := &fakeAgentExecutor{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	_, err = runner.runReviewStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_hold_before_agent", Type: "reviewer"},
		Run:      storage.RunRecord{ID: "run_hold_before_agent"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", Labels: []string{}},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "feature/review-me", PreparedAt: fixture.nowISO()},
		},
	})
	var holdErr *holdSkipError
	if !errors.As(err, &holdErr) {
		t.Fatalf("runReviewStep() error = %v, want hold skip", err)
	}
	if len(agent.starts) != 0 {
		t.Fatalf("agent.starts = %#v, want none", agent.starts)
	}
}

func TestRunReviewStepStopsAfterStaleReprepareDuringReview(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	git := &fakeGitGateway{prepareErr: &gitinfra.RemoteHeadChangedError{Branch: "refs/pull/42/head", ExpectedHeadSHA: "abc123", ActualHeadSHA: "def456"}, worktreePath: filepath.Join(t.TempDir(), "reviewer-worktree")}
	agent := &fakeAgentExecutor{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: git, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	prNumber := int64(42)
	loopTarget := "pr:42"
	loop := storage.LoopRecord{ID: "loop_1", Seq: 1, ProjectID: project.ID, Type: "reviewer", TargetType: "pull_request", TargetID: &loopTarget, Repo: stringPtr("acme/looper"), PRNumber: &prNumber, Status: "running", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	initialCheckpoint := reviewerCheckpoint{
		Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
		Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
		Worktree: &checkpointWorktree{Path: filepath.Join(t.TempDir(), "deleted-worktree"), Branch: "feature/review-me", PreparedAt: fixture.nowISO()},
	}
	checkpointJSON := mustMarshalJSON(initialCheckpoint)
	run := storage.RunRecord{ID: "run_1", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepReview)), CheckpointJSON: &checkpointJSON, StartedAt: fixture.nowISO(), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Runs.Upsert(context.Background(), run); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	checkpoint, err := runner.runReviewStep(context.Background(), stepInput{
		Project:    *project,
		Loop:       loop,
		Run:        run,
		Repo:       "acme/looper",
		PRNumber:   prNumber,
		Checkpoint: initialCheckpoint,
	})
	if err != nil {
		t.Fatalf("runReviewStep() error = %v", err)
	}
	if checkpoint.SkipKind != "stale" || !contains(checkpoint.SkipReason, "Remote head changed for refs/pull/42/head") {
		t.Fatalf("checkpoint = %#v, want stale remote-head skip", checkpoint)
	}
	if len(agent.starts) != 0 {
		t.Fatalf("len(agent.starts) = %d, want 0", len(agent.starts))
	}
	persistedRun, err := fixture.repos.Runs.GetByID(context.Background(), run.ID)
	if err != nil || persistedRun == nil {
		t.Fatalf("Runs.GetByID() = (%#v, %v), want run", persistedRun, err)
	}
	persistedCheckpoint := parseCheckpoint(persistedRun.CheckpointJSON)
	if persistedCheckpoint.SkipKind != "stale" || !contains(persistedCheckpoint.SkipReason, "Remote head changed for refs/pull/42/head") {
		t.Fatalf("persisted checkpoint = %#v, want stale remote-head skip", persistedCheckpoint)
	}
}

func TestProcessClaimedItemRetryAfterReviewFailureRepreparesWorktree(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	git := &fakeGitGateway{worktreePath: filepath.Join(t.TempDir(), "reviewer-worktree")}
	github := &fakeGitHubGateway{reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "failed", Summary: "agent failed"}, {Status: "completed", Summary: "Looks good", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	firstClaim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || firstClaim == nil {
		t.Fatalf("first ClaimNextOfType() = (%#v, %v), want claimed item", firstClaim, err)
	}
	firstResult, err := runner.ProcessClaimedItem(context.Background(), *firstClaim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem(first) error = %v", err)
	}
	if firstResult.Status != "failed" || firstResult.FailureKind != FailureRetryableTransient {
		t.Fatalf("first result = %#v, want retryable_transient failure", firstResult)
	}

	fixture.advance(5 * time.Second)
	github.reviewMarkerMissing = false
	retryClaim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || retryClaim == nil {
		t.Fatalf("retry ClaimNextOfType() = (%#v, %v), want claimed item", retryClaim, err)
	}
	retryResult, err := runner.ProcessClaimedItem(context.Background(), *retryClaim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem(retry) error = %v", err)
	}
	if retryResult.Status != "success" {
		t.Fatalf("retry result = %#v, want success", retryResult)
	}
	if len(git.createCalls) != 3 || len(git.prepareCalls) != 3 {
		t.Fatalf("createCalls=%d prepareCalls=%d, want 3 each", len(git.createCalls), len(git.prepareCalls))
	}
	if len(agent.starts) != 2 {
		t.Fatalf("len(agent.starts) = %d, want 2", len(agent.starts))
	}
}

func TestProcessClaimedItemDoesNotRetryGitHubSelfApprovalFailure(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "failed", Summary: `submit validated PR review: HTTP 422: Review Can not approve your own pull request`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureNonRetryable {
		t.Fatalf("result = %#v, want non_retryable self-approval failure", result)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), claim.ID)
	if err != nil || queue == nil {
		t.Fatalf("Queue.GetByID() = (%#v, %v), want queue", queue, err)
	}
	if queue.Status != "queued" || queue.LastErrorKind == nil || *queue.LastErrorKind != string(FailureNonRetryable) || queue.FinishedAt != nil {
		t.Fatalf("queue = %#v, want requeued non_retryable queue item", queue)
	}
}

func TestProcessClaimedItemRetriesTransientSnapshotFailureInRun(t *testing.T) {
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{captureSnapshotErrs: []error{&shell.CommandExecutionError{Message: "Command exited with code 1", Result: shell.Result{Stderr: "HTTP 504: We couldn't respond to your request in time"}}}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Looks good", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	logger := &testLogger{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{worktreePath: filepath.Join(t.TempDir(), "reviewer-worktree")}, AgentExecutor: agent, Logger: logger, Now: fixture.now, RetryBaseDelay: time.Nanosecond})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claim", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("ProcessClaimedItem() = %#v, want success after snapshot retry", result)
	}
	if github.captureSnapshotCalls != 2 {
		t.Fatalf("captureSnapshotCalls = %d, want 2", github.captureSnapshotCalls)
	}
	if !logger.hasMessage("reviewer transient external failure retrying") || !logger.hasMessage("reviewer transient external retry succeeded") {
		t.Fatalf("logger messages = %#v, want retry attempt and success logs", logger.messages)
	}
}

func TestExecuteStepRetriesTransientDiscoverShellFailure(t *testing.T) {
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{viewErrs: []error{
		&shell.CommandExecutionError{Message: "Command exited with code 1", Result: shell.Result{Stderr: `Post "https://api.github.com/graphql": net/http: TLS handshake timeout`}},
	}}
	logger := &testLogger{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: logger, Now: fixture.now, RetryBaseDelay: time.Nanosecond})
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}

	checkpoint, err := runner.executeStep(context.Background(), stepDiscover, stepInput{Project: *project, Loop: storage.LoopRecord{ID: "loop_1"}, Run: storage.RunRecord{ID: "run_1"}, QueueItem: storage.QueueItemRecord{ID: "queue_1"}, Repo: "acme/looper", PRNumber: 42})
	if err != nil {
		t.Fatalf("executeStep(discover) error = %v, want success after retry", err)
	}
	if checkpoint.Detail == nil || checkpoint.Detail.HeadSHA != "abc123" {
		t.Fatalf("checkpoint.Detail = %#v, want discovered PR detail", checkpoint.Detail)
	}
	if github.viewCalls != 2 {
		t.Fatalf("viewCalls = %d, want 2", github.viewCalls)
	}
	if !logger.hasMessage("reviewer transient external failure retrying") || !logger.hasMessage("reviewer transient external retry succeeded") {
		t.Fatalf("logger messages = %#v, want retry attempt and success logs", logger.messages)
	}
}

func TestProcessClaimedItemRequeuesTransientDiscoverShellFailureAfterMaxAttempts(t *testing.T) {
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, RetryBaseDelay: time.Nanosecond, RetryMaxAttempts: 1})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	github.viewErrs = []error{&shell.CommandExecutionError{Message: "Command exited with code 1", Result: shell.Result{Stderr: `Post "https://api.github.com/graphql": unexpected EOF`}}}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claim", claim, err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableTransient || !strings.Contains(result.Summary, "unexpected EOF") {
		t.Fatalf("result = %#v, want retryable transient failure preserving GitHub error", result)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), claim.ID)
	if err != nil || queue == nil {
		t.Fatalf("Queue.GetByID() = (%#v, %v), want queue", queue, err)
	}
	if queue.Status != "manual_intervention" || queue.LastErrorKind == nil || *queue.LastErrorKind != string(FailureRetryableTransient) || queue.LastError == nil || !strings.Contains(*queue.LastError, "unexpected EOF") || queue.FinishedAt == nil {
		t.Fatalf("queue = %#v, want manual_intervention retryable transient item preserving GitHub error after max attempts", queue)
	}
}

func TestProcessClaimedItemSkipsMissingPullRequestInDiscover(t *testing.T) {
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, RetryBaseDelay: time.Nanosecond})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	github.viewErrs = []error{&shell.CommandExecutionError{Message: "Command exited with code 1", Result: shell.Result{Stderr: "GraphQL: Could not resolve to a PullRequest with the number of 345. (repository.pullRequest)"}}}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claim", claim, err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !strings.Contains(result.Summary, "Could not resolve to a PullRequest") {
		t.Fatalf("result = %#v, want skipped missing PR with original GitHub error", result)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), *claim.LoopID)
	if err != nil || loop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", loop, err)
	}
	if terminalReviewerLoopReason(*loop) != "terminated" {
		t.Fatalf("loop = %#v, want terminal terminated loop", loop)
	}
	loopMeta := reviewerLoopMetadata(parseJSONObject(loop.MetadataJSON))
	if loopMeta["terminationReason"] != "pr_not_found" {
		t.Fatalf("loop metadata = %#v, want pr_not_found termination", loopMeta)
	}
}

func TestProcessClaimedItemTerminatesMissingPullRequestDuringPublishResume(t *testing.T) {
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{viewErrs: []error{&shell.CommandExecutionError{Message: "Command exited with code 1", Result: shell.Result{Stderr: "GraphQL: Could not resolve to a PullRequest with the number of 42. (repository.pullRequest)"}}}}
	git := &fakeGitGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, RetryBaseDelay: time.Nanosecond})
	ctx := context.Background()
	repo := "acme/looper"
	prNumber := int64(42)
	targetID := "pr:acme/looper:42"
	loopID := "loop_publish_pr_not_found"
	queueID := "queue_publish_pr_not_found"
	nowISO := fixture.nowISO()
	worktreePath := filepath.Join(t.TempDir(), "reviewer-worktree")
	metadataJSON, err := runner.ensureLoopMetadataJSON(nil, "project_1", repo, prNumber)
	if err != nil {
		t.Fatalf("ensureLoopMetadataJSON() error = %v", err)
	}
	if err := fixture.repos.Loops.Upsert(ctx, storage.LoopRecord{ID: loopID, Seq: 42, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &targetID, Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadataJSON, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	if err := fixture.repos.Queue.Upsert(ctx, storage.QueueItemRecord{ID: queueID, ProjectID: stringPtr("project_1"), LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: targetID, Repo: &repo, PRNumber: &prNumber, DedupeKey: "reviewer:project_1:loop_publish_pr_not_found:acme/looper:42", Priority: storage.QueuePriorityReviewer, Status: "running", AvailableAt: nowISO, Attempts: 2212, MaxAttempts: 5, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	checkpointJSON := mustMarshalJSON(reviewerCheckpoint{
		ResumePolicy: "advance_from_checkpoint",
		Detail:       &checkpointDetail{Title: "Deploy vela-web test sha-59e6fd8", State: "OPEN", HeadSHA: "abc123", HeadRefName: "feature", BaseRefName: "main", ReviewRequests: []string{"octocat"}, CurrentLogin: "octocat"},
		Snapshot:     &checkpointSnapshot{HeadSHA: "abc123"},
		Worktree:     &checkpointWorktree{Path: worktreePath, Branch: "pr-42-head", BaseBranch: "main", PreparedAt: nowISO},
		PendingReview: &pendingReviewCheckpoint{
			HeadSHA:        "abc123",
			IdempotencyKey: "reviewer:loop_publish_pr_not_found:abc123",
			Event:          reviewEventAgentNative,
			Summary:        "posted review",
		},
	})
	if err := fixture.repos.Runs.Upsert(ctx, storage.RunRecord{ID: "run_publish_pr_not_found_previous", LoopID: loopID, Status: "failed", CurrentStep: stringPtr("publish"), LastCompletedStep: stringPtr("review"), CheckpointJSON: &checkpointJSON, Summary: stringPtr("previous publish failure"), ErrorMessage: stringPtr("previous publish failure"), StartedAt: nowISO, EndedAt: &nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	result, err := runner.ProcessClaimedItem(ctx, storage.QueueItemRecord{ID: queueID, ProjectID: stringPtr("project_1"), LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: targetID, Repo: &repo, PRNumber: &prNumber, Status: "running"})
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !strings.Contains(result.Summary, "Could not resolve to a PullRequest") {
		t.Fatalf("result = %#v, want skipped missing PR publish resume", result)
	}
	loop, err := fixture.repos.Loops.GetByID(ctx, loopID)
	if err != nil || loop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", loop, err)
	}
	if terminalReviewerLoopReason(*loop) != "terminated" {
		t.Fatalf("loop = %#v, want terminal terminated loop", loop)
	}
	loopMeta := reviewerLoopMetadata(parseJSONObject(loop.MetadataJSON))
	if loopMeta["terminationReason"] != "pr_not_found" {
		t.Fatalf("loop metadata = %#v, want pr_not_found termination", loopMeta)
	}
	queue, err := fixture.repos.Queue.GetByID(ctx, queueID)
	if err != nil || queue == nil {
		t.Fatalf("Queue.GetByID() = (%#v, %v), want queue", queue, err)
	}
	if queue.Status == "queued" {
		t.Fatalf("queue = %#v, want terminal queue state instead of requeue", queue)
	}
	if len(git.cleanupCalls) != 1 {
		t.Fatalf("len(git.cleanupCalls) = %d, want 1", len(git.cleanupCalls))
	}
	if git.cleanupCalls[0].WorktreePath != worktreePath {
		t.Fatalf("CleanupWorktree().WorktreePath = %q, want %q", git.cleanupCalls[0].WorktreePath, worktreePath)
	}
}

func TestExecuteStepDoesNotRetryNonTransientSnapshotFailure(t *testing.T) {
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{captureSnapshotErrs: []error{fmt.Errorf("GraphQL: Resource not accessible by integration")}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now, RetryBaseDelay: time.Nanosecond})
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}

	_, err = runner.executeStep(context.Background(), stepSnapshot, stepInput{Project: *project, Loop: storage.LoopRecord{ID: "loop_1"}, Run: storage.RunRecord{ID: "run_1"}, QueueItem: storage.QueueItemRecord{ID: "queue_1"}, Repo: "acme/looper", PRNumber: 42})
	if err == nil || !strings.Contains(err.Error(), "Resource not accessible") {
		t.Fatalf("executeStep(snapshot) error = %v, want non-transient failure", err)
	}
	if github.captureSnapshotCalls != 1 {
		t.Fatalf("captureSnapshotCalls = %d, want 1", github.captureSnapshotCalls)
	}
}

func TestExecuteStepPreservesFinalTransientFailureAfterRetryExhaustion(t *testing.T) {
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{captureSnapshotErrs: []error{
		&githubinfra.TransientError{Err: fmt.Errorf("GitHub GraphQL HTTP 504 first")},
		&githubinfra.TransientError{Err: fmt.Errorf("GitHub GraphQL HTTP 504 final")},
	}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now, RetryBaseDelay: time.Nanosecond, RetryMaxAttempts: 2})
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}

	_, err = runner.executeStep(context.Background(), stepSnapshot, stepInput{Project: *project, Loop: storage.LoopRecord{ID: "loop_1"}, Run: storage.RunRecord{ID: "run_1"}, QueueItem: storage.QueueItemRecord{ID: "queue_1"}, Repo: "acme/looper", PRNumber: 42})
	if err == nil || !strings.Contains(err.Error(), "final") || strings.Contains(err.Error(), "first") {
		t.Fatalf("executeStep(snapshot) error = %v, want final transient failure only", err)
	}
	if github.captureSnapshotCalls != 2 {
		t.Fatalf("captureSnapshotCalls = %d, want 2", github.captureSnapshotCalls)
	}
}

func TestIsTransientExternalFailureDetectsWrappedGitHubStatus(t *testing.T) {
	runner := New(Options{})
	err := &loopError{message: "GraphQL request failed with HTTP 504", kind: FailureRetryableTransient}
	if !runner.isTransientExternalFailure(err) {
		t.Fatal("isTransientExternalFailure(wrapped HTTP 504) = false, want true")
	}
}

func TestUnknownBoundaryDoesNotRetryExternalLookingCommandError(t *testing.T) {
	err := &shell.CommandExecutionError{Message: "Command exited with code 1", Result: shell.Result{Stderr: `Post "https://api.github.com/graphql": EOF`}}

	defaultRunner := New(Options{})
	if got := defaultRunner.classifyFailure(err); got.kind != FailureNonRetryable {
		t.Fatalf("default classifyFailure() kind = %s, want %s", got.kind, FailureNonRetryable)
	}
	if defaultRunner.isTransientExternalFailure(err) {
		t.Fatal("default isTransientExternalFailure() = true, want false")
	}

	boundaryErr := failureclass.WithBoundary(err, failureclass.BoundaryGitHubAPI)
	if got := defaultRunner.classifyFailure(boundaryErr); got.kind != FailureRetryableTransient {
		t.Fatalf("boundary classifyFailure() kind = %s, want %s", got.kind, FailureRetryableTransient)
	}

	enabledRunner := New(Options{RetryPolicy: config.ReviewerRetryConfig{EnhancedTransientClassification: true}})
	if got := enabledRunner.classifyFailure(err); got.kind != FailureRetryableTransient {
		t.Fatalf("enabled classifyFailure() kind = %s, want %s", got.kind, FailureRetryableTransient)
	}
	if !enabledRunner.isTransientExternalFailure(err) {
		t.Fatal("enabled isTransientExternalFailure() = false, want true")
	}
}

func TestEnhancedTransientClassificationHonorsExtraPatterns(t *testing.T) {
	err := fmt.Errorf("custom provider temporarily unavailable")
	runner := New(Options{RetryPolicy: config.ReviewerRetryConfig{
		EnhancedTransientClassification: true,
		ExtraTransientErrorPatterns:     []string{"temporarily unavailable"},
	}})
	if got := runner.classifyFailure(err); got.kind != FailureRetryableTransient {
		t.Fatalf("classifyFailure() kind = %s, want %s", got.kind, FailureRetryableTransient)
	}
}

func TestIsTransientExternalFailureDetectsModelProviderHTTPAndNetworkFailures(t *testing.T) {
	runner := New(Options{})
	for _, message := range []string{
		`Error: {"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded"}}`,
		"anthropic overloaded_error: retry later",
		"POST https://api.anthropic.com/v1/messages: unexpected EOF",
		"net/http: TLS handshake timeout",
		"anthropic request failed with HTTP 529",
		"anthropic request failed with status code: 429",
	} {
		if !runner.isTransientExternalFailure(fmt.Errorf("%s", message)) {
			t.Fatalf("isTransientExternalFailure(%q) = false, want true", message)
		}
	}
}

func TestRetryDelayHonorsRetryAfterAndCapsBackoff(t *testing.T) {
	if got := retryDelay(time.Second, 1, fmt.Errorf("anthropic overloaded; retry-after: 7"), maxRetryDelay); got != 7*time.Second {
		t.Fatalf("retryDelay(retry-after) = %v, want 7s", got)
	}
	if got := retryDelay(time.Minute, 3, fmt.Errorf("anthropic overloaded; retry-after: 120"), maxRetryDelay); got != 120*time.Second {
		t.Fatalf("retryDelay(retry-after below cap) = %v, want 120s", got)
	}
	if got := retryDelay(time.Minute, 3, fmt.Errorf("anthropic overloaded; retry-after: 600"), maxRetryDelay); got != maxRetryDelay {
		t.Fatalf("retryDelay(capped retry-after) = %v, want %v", got, maxRetryDelay)
	}
	if got := retryDelay(time.Minute, 4, fmt.Errorf("anthropic overloaded"), maxRetryDelay); got != maxRetryDelay {
		t.Fatalf("retryDelay(capped exponential) = %v, want %v", got, maxRetryDelay)
	}
	if got := retryDelay(time.Minute, 3, fmt.Errorf("anthropic overloaded; retry-after: 120"), 10*time.Second); got != 10*time.Second {
		t.Fatalf("retryDelay(custom capped retry-after) = %v, want 10s", got)
	}
}

func TestRetryDelayAddsBoundedJitter(t *testing.T) {
	base := time.Second
	wantMin := 2 * base
	wantMax := wantMin + wantMin/retryJitterDivisor
	for i := 0; i < 20; i++ {
		got := retryDelay(base, 2, fmt.Errorf("anthropic overloaded"), maxRetryDelay)
		if got < wantMin || got > wantMax {
			t.Fatalf("retryDelay(jittered) = %v, want between %v and %v", got, wantMin, wantMax)
		}
	}
}

func TestMarkAgentExecutionNativeResumePendingForTransientProvider(t *testing.T) {
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	loopID := "loop_native_resume"
	runID := "run_native_resume"
	repo := "acme/looper"
	prNumber := int64(42)
	if err := fixture.repos.Loops.Upsert(ctx, storage.LoopRecord{ID: loopID, Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	if err := fixture.repos.Runs.Upsert(ctx, storage.RunRecord{ID: runID, LoopID: loopID, Status: "running", StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	if err := fixture.repos.AgentExecutions.Upsert(ctx, storage.AgentExecutionRecord{ID: "agent_native_resume", ProjectID: stringPtr("project_1"), LoopID: stringPtr(loopID), RunID: stringPtr(runID), Vendor: string(config.AgentVendorOpenCode), Status: "completed", NativeSessionID: stringPtr("session-123"), StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("AgentExecutions.Upsert() error = %v", err)
	}

	if !runner.markAgentExecutionNativeResumePendingForTransientProvider(ctx, "agent_native_resume", `{"type":"error","error":{"code":"server_is_overloaded"}}`) {
		t.Fatalf("markAgentExecutionNativeResumePendingForTransientProvider() = false, want true")
	}
	record, err := fixture.repos.AgentExecutions.GetByID(ctx, "agent_native_resume")
	if err != nil {
		t.Fatalf("AgentExecutions.GetByID() error = %v", err)
	}
	if record.NativeResumeMode == nil || *record.NativeResumeMode != "native_resume" || record.NativeResumeStatus == nil || *record.NativeResumeStatus != "pending" {
		t.Fatalf("native resume fields = mode:%v status:%v, want native_resume/pending", record.NativeResumeMode, record.NativeResumeStatus)
	}
	if !runner.hasPendingNativeResume(ctx, loopID) {
		t.Fatalf("hasPendingNativeResume() = false, want true")
	}
}

func TestNativeResumeImmediateRetryRequiresCurrentProviderOverload(t *testing.T) {
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	loopID := "loop_native_resume_delay"
	runID := "run_native_resume_delay"
	repo := "acme/looper"
	prNumber := int64(42)
	if err := fixture.repos.Loops.Upsert(ctx, storage.LoopRecord{ID: loopID, Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	if err := fixture.repos.Runs.Upsert(ctx, storage.RunRecord{ID: runID, LoopID: loopID, Status: "running", StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	if err := fixture.repos.AgentExecutions.Upsert(ctx, storage.AgentExecutionRecord{ID: "agent_native_resume_delay", ProjectID: stringPtr("project_1"), LoopID: stringPtr(loopID), RunID: stringPtr(runID), Vendor: string(config.AgentVendorOpenCode), Status: "completed", NativeSessionID: stringPtr("session-123"), NativeResumeStatus: stringPtr("pending"), StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("AgentExecutions.Upsert() error = %v", err)
	}

	if !runner.shouldSkipTransientRetryDelayForNativeResume(ctx, loopID, fmt.Errorf("service_unavailable_error: server_is_overloaded")) {
		t.Fatalf("provider overload with pending native resume should skip retry delay")
	}
	if runner.shouldSkipTransientRetryDelayForNativeResume(ctx, loopID, &githubinfra.TransientError{Err: fmt.Errorf("GitHub GraphQL HTTP 504")}) {
		t.Fatalf("GitHub transient failure should keep retry delay even with pending native resume")
	}
	if runner.shouldSkipTransientRetryDelayForNativeResume(ctx, loopID, &loopError{message: "GraphQL request failed with HTTP 504", kind: FailureRetryableTransient}) {
		t.Fatalf("non-provider transient loop error should keep retry delay even with pending native resume")
	}
}

func TestMarkAgentExecutionNativeResumePendingRequiresSessionAndProviderError(t *testing.T) {
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})
	ctx := context.Background()
	nowISO := fixture.nowISO()
	loopID := "loop_no_session"
	runID := "run_no_session"
	repo := "acme/looper"
	prNumber := int64(42)
	if err := fixture.repos.Loops.Upsert(ctx, storage.LoopRecord{ID: loopID, Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	if err := fixture.repos.Runs.Upsert(ctx, storage.RunRecord{ID: runID, LoopID: loopID, Status: "running", StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	if err := fixture.repos.AgentExecutions.Upsert(ctx, storage.AgentExecutionRecord{ID: "agent_no_session", ProjectID: stringPtr("project_1"), LoopID: stringPtr(loopID), RunID: stringPtr(runID), Vendor: string(config.AgentVendorOpenCode), Status: "failed", StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("AgentExecutions.Upsert() error = %v", err)
	}
	if runner.markAgentExecutionNativeResumePendingForTransientProvider(ctx, "agent_no_session", "server_is_overloaded") {
		t.Fatalf("mark without native session = true, want false")
	}
	if err := fixture.repos.AgentExecutions.Upsert(ctx, storage.AgentExecutionRecord{ID: "agent_non_provider", ProjectID: stringPtr("project_1"), LoopID: stringPtr(loopID), RunID: stringPtr(runID), Vendor: string(config.AgentVendorOpenCode), Status: "failed", NativeSessionID: stringPtr("session-123"), StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("AgentExecutions.Upsert() error = %v", err)
	}
	if runner.markAgentExecutionNativeResumePendingForTransientProvider(ctx, "agent_non_provider", "permission denied") {
		t.Fatalf("mark non-provider failure = true, want false")
	}
}

func TestProcessClaimedItemRetriesTransientModelOverloadInRun(t *testing.T) {
	fixture := newRunnerFixture(t)
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "failed"}, {Status: "completed", Summary: "Looks good", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}, waitErrs: []error{fmt.Errorf("service_unavailable_error: server_is_overloaded")}}
	logger := &testLogger{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{worktreePath: filepath.Join(t.TempDir(), "reviewer-worktree")}, AgentExecutor: agent, Logger: logger, Now: fixture.now, RetryBaseDelay: time.Nanosecond})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claim", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("ProcessClaimedItem() = %#v, want success after model retry", result)
	}
	if len(agent.starts) != 2 {
		t.Fatalf("len(agent.starts) = %d, want 2", len(agent.starts))
	}
	if !logger.hasMessage("reviewer transient external failure retrying") || !logger.hasMessage("reviewer transient external retry succeeded") {
		t.Fatalf("logger messages = %#v, want retry attempt and success logs", logger.messages)
	}
}

func TestProcessClaimedItemDoesNotTerminateLoopWhenMaxConsecutiveFailuresReached(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "failed", Summary: "agent failed"}}}
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now,
		RetryMaxAttempts: 3,
		LoopConfig:       config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 1, MaxAgentExecutionsPerPR: 25},
	})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableTransient {
		t.Fatalf("result = %#v, want retryable failed result", result)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), *claim.LoopID)
	if err != nil || loop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", loop, err)
	}
	if loop.Status != "queued" || loop.NextRunAt == nil {
		t.Fatalf("loop = %#v, want queued loop with next run", loop)
	}
	loopMeta := reviewerLoopMetadata(parseJSONObject(loop.MetadataJSON))
	if loopMeta["terminationReason"] == "max_consecutive_failures" {
		t.Fatalf("loop metadata = %#v, want no budget termination metadata", loopMeta)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), claim.ID)
	if err != nil || queue == nil {
		t.Fatalf("Queue.GetByID() = (%#v, %v), want queue", queue, err)
	}
	if queue.Status != "queued" || queue.FinishedAt != nil {
		t.Fatalf("queue = %#v, want queued retry item", queue)
	}
}

func TestRunPrepareWorktreeStepPersistsCreatedWorktreeBeforeManualIntervention(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	clean := false
	git := &fakeGitGateway{worktreePath: filepath.Join(t.TempDir(), "reviewer-worktree"), prepareClean: &clean}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: git, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	prNumber := int64(42)
	loopTarget := "pr:42"
	loop := storage.LoopRecord{ID: "loop_1", Seq: 1, ProjectID: project.ID, Type: "reviewer", TargetType: "pull_request", TargetID: &loopTarget, Repo: stringPtr("acme/looper"), PRNumber: &prNumber, Status: "running", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	run := storage.RunRecord{ID: "run_1", LoopID: "loop_1", Status: "running", CurrentStep: stringPtr(string(stepWorktree)), CheckpointJSON: stringPtr(mustMarshalJSON(reviewerCheckpoint{})), StartedAt: fixture.nowISO(), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Runs.Upsert(context.Background(), run); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	_, err = runner.runPrepareWorktreeStep(context.Background(), stepInput{
		Project:  *project,
		Run:      run,
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadSHA: "abc123", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
		},
	})
	if err == nil || !contains(err.Error(), "manual intervention required") {
		t.Fatalf("runPrepareWorktreeStep() error = %v, want manual intervention required", err)
	}
	persistedRun, err := fixture.repos.Runs.GetByID(context.Background(), run.ID)
	if err != nil || persistedRun == nil {
		t.Fatalf("Runs.GetByID() = (%#v, %v), want run", persistedRun, err)
	}
	persistedCheckpoint := parseCheckpoint(persistedRun.CheckpointJSON)
	if persistedCheckpoint.Worktree == nil || persistedCheckpoint.Worktree.Path != git.worktreePath {
		t.Fatalf("persisted checkpoint worktree = %#v, want created worktree", persistedCheckpoint.Worktree)
	}
	if persistedCheckpoint.Worktree.PreparedAt != "" {
		t.Fatalf("persisted checkpoint preparedAt = %q, want empty before failed prepare", persistedCheckpoint.Worktree.PreparedAt)
	}
}

func TestDiscoverPullRequestsUsesReviewRequestedQueryWhenReviewRequestRequired(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		currentLogin: "OctoCat",
		reviewRequestedPullRequests: []PullRequestSummary{{
			Number:             77,
			Title:              "Window-external review request",
			State:              "OPEN",
			HeadSHA:            "head77",
			BaseSHA:            "base77",
			Author:             "contributor",
			ReviewRequests:     []string{"octocat"},
			ReviewRequestUsers: []networkpolicy.GitHubUser{{Login: "octocat"}},
		}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper", Limit: 10})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(github.listCalls) != 0 {
		t.Fatalf("generic list calls = %#v, want targeted review-request query only", github.listCalls)
	}
	if len(github.listReviewRequestedCalls) != 1 {
		t.Fatalf("review-requested calls = %#v, want one call", github.listReviewRequestedCalls)
	}
	call := github.listReviewRequestedCalls[0]
	if call.Reviewer != "octocat" || call.Limit != 10 {
		t.Fatalf("review-requested call = %#v, want normalized reviewer and discovery limit", call)
	}
	if len(result.QueueItems) != 1 || result.QueueItems[0].PRNumber == nil || *result.QueueItems[0].PRNumber != 77 {
		t.Fatalf("queue items = %#v, want PR 77 queued", result.QueueItems)
	}
}

func TestListOpenPullRequestsForDiscoveryCombinesForgejoLabelsAndReviewRequests(t *testing.T) {
	t.Parallel()
	github := &fakeGitHubGateway{
		currentLogin: "reviewer",
		listOpenByLabel: map[string][]PullRequestSummary{
			"needs-review": {
				{Number: 42, State: "OPEN", Labels: []string{"needs-review"}},
				{Number: 43, State: "OPEN", Labels: []string{"needs-review"}},
			},
		},
		reviewRequestedPullRequests: []PullRequestSummary{
			{Number: 43, State: "OPEN", ReviewRequests: []string{"reviewer"}},
			{Number: 44, State: "OPEN", ReviewRequests: []string{"reviewer"}},
		},
	}
	runner := New(Options{GitHub: github})
	policy := DiscoveryPolicy{RequireReviewRequest: true, Labels: []string{"needs-review"}, LabelMode: config.LabelModeAll, MatchAnyTrigger: true}

	pulls, err := runner.listOpenPullRequestsForDiscoveryWithPolicy(context.Background(), "acme/looper", "/tmp/repo", 30, policy, "reviewer")
	if err != nil {
		t.Fatalf("listOpenPullRequestsForDiscoveryWithPolicy() error = %v", err)
	}
	// Requested reviews are reserved first so label pages cannot starve them.
	if len(pulls) != 3 || pulls[0].Number != 43 || pulls[1].Number != 44 || pulls[2].Number != 42 {
		t.Fatalf("pulls = %#v, want requested-first union [43, 44, 42]", pulls)
	}
	if len(github.listCalls) != 1 || len(github.listReviewRequestedCalls) != 1 {
		t.Fatalf("label calls = %#v, review-request calls = %#v, want one of each", github.listCalls, github.listReviewRequestedCalls)
	}
}

func TestListOpenPullRequestsForDiscoveryDoesNotStarveRequestedReviewsWhenLabelsFillLimit(t *testing.T) {
	t.Parallel()
	github := &fakeGitHubGateway{
		currentLogin: "reviewer",
		listOpenByLabel: map[string][]PullRequestSummary{
			"needs-review": {
				{Number: 1, State: "OPEN", Labels: []string{"needs-review"}},
				{Number: 2, State: "OPEN", Labels: []string{"needs-review"}},
				{Number: 3, State: "OPEN", Labels: []string{"needs-review"}},
			},
		},
		reviewRequestedPullRequests: []PullRequestSummary{
			{Number: 99, State: "OPEN", ReviewRequests: []string{"reviewer"}},
		},
	}
	runner := New(Options{GitHub: github})
	policy := DiscoveryPolicy{RequireReviewRequest: true, Labels: []string{"needs-review"}, LabelMode: config.LabelModeAll, MatchAnyTrigger: true}

	pulls, err := runner.listOpenPullRequestsForDiscoveryWithPolicy(context.Background(), "acme/looper", "/tmp/repo", 2, policy, "reviewer")
	if err != nil {
		t.Fatalf("listOpenPullRequestsForDiscoveryWithPolicy() error = %v", err)
	}
	if len(pulls) != 2 || pulls[0].Number != 99 || pulls[1].Number != 1 {
		t.Fatalf("pulls = %#v, want requested PR 99 reserved before label filler", pulls)
	}
}

func TestDiscoverPullRequestsSuppressesConflictedLastFilterSkipFromReviewRequestedQuery(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		currentLogin: "OctoCat",
		reviewRequestedPullRequests: []PullRequestSummary{{
			Number:             77,
			Title:              "Conflicted review request",
			State:              "OPEN",
			HeadSHA:            "head77",
			BaseSHA:            "base77",
			HasConflicts:       true,
			Author:             "contributor",
			ReviewRequests:     []string{"octocat"},
			ReviewRequestUsers: []networkpolicy.GitHubUser{{Login: "octocat"}},
		}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(77)
	metadata := `{"followUpdates":true,"lastFilterSkip":{"kind":"conflicted","reason":"Skipped conflicted pull request acme/looper#77","recordedAt":"2026-06-05T10:00:00Z","headSha":"head77"}}`
	loop := storage.LoopRecord{ID: "loop_conflicted_review_request", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo, Limit: 10})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(github.listReviewRequestedCalls) != 1 {
		t.Fatalf("review-requested calls = %#v, want one call", github.listReviewRequestedCalls)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("queue items = %#v, want conflicted lastFilterSkip preserved", result.QueueItems)
	}
	if result.Skipped != 1 {
		t.Fatalf("Skipped = %d, want conflicted lastFilterSkip suppression", result.Skipped)
	}
}

func TestProcessNextFinalizesClaimedQueueItemOnSetupFailure(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	if _, err := fixture.coordinator.DB().ExecContext(context.Background(), `
		CREATE TRIGGER reviewer_runs_fail_start
		BEFORE INSERT ON runs
		WHEN NEW.status = 'running'
		BEGIN
			SELECT RAISE(FAIL, 'start run blocked');
		END;
	`); err != nil {
		t.Fatalf("create trigger error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	discovery, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}

	result, err := runner.ProcessNext(context.Background(), "reviewer-worker-1")
	if err == nil || !contains(err.Error(), "start run blocked") {
		t.Fatalf("ProcessNext() error = %v, want start run blocked", err)
	}
	if result != nil {
		t.Fatalf("ProcessNext() = %#v, want nil result", result)
	}
	queue, getErr := fixture.repos.Queue.GetByID(context.Background(), discovery.QueueItems[0].ID)
	if getErr != nil {
		t.Fatalf("Queue.GetByID() error = %v", getErr)
	}
	if queue == nil || queue.Status != "queued" || queue.FinishedAt != nil || queue.LastErrorKind == nil || *queue.LastErrorKind != string(FailureNonRetryable) {
		t.Fatalf("queue = %#v, want requeued queue item with non_retryable error kind", queue)
	}
	if queue.LastError == nil || !contains(*queue.LastError, "start run blocked") {
		t.Fatalf("queue.LastError = %#v, want start run blocked", queue.LastError)
	}
}

func TestProcessClaimedItemTreatsReviewerAgentSetupFailureAsRetryableTransient(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "failed", Summary: "OpenAI API error: model gpt-5-reviewer not found for project", Stderr: "OpenAI API error: model gpt-5-reviewer not found for project"}}}
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now,
		RetryMaxAttempts: 3,
		LoopConfig:       config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 1, MaxAgentExecutionsPerPR: 25},
	})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed reviewer queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableTransient {
		t.Fatalf("result = %#v, want retryable transient setup failure", result)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), claim.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "queued" || queue.LastErrorKind == nil || *queue.LastErrorKind != string(FailureRetryableTransient) {
		t.Fatalf("queue = %#v, want queued retryable transient queue kind", queue)
	}
}

func TestProcessClaimedItemReturnsWhenCompleteRunFails(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Please add tests", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoApprove: true})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	if _, err := fixture.coordinator.DB().ExecContext(context.Background(), `
		CREATE TRIGGER reviewer_runs_fail_complete_insert
		BEFORE INSERT ON runs
		WHEN NEW.status != 'running'
		BEGIN
			SELECT RAISE(FAIL, 'complete run blocked');
		END;
	`); err != nil {
		t.Fatalf("create insert trigger error = %v", err)
	}
	if _, err := fixture.coordinator.DB().ExecContext(context.Background(), `
		CREATE TRIGGER reviewer_runs_fail_complete_update
		BEFORE UPDATE ON runs
		WHEN NEW.status != 'running'
		BEGIN
			SELECT RAISE(FAIL, 'complete run blocked');
		END;
	`); err != nil {
		t.Fatalf("create update trigger error = %v", err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err == nil || !contains(err.Error(), "complete run blocked") {
		t.Fatalf("ProcessClaimedItem() error = %v, want complete run blocked", err)
	}
	if result != (ProcessResult{}) {
		t.Fatalf("ProcessClaimedItem() = %#v, want zero result on completeRun failure", result)
	}
	queue, getErr := fixture.repos.Queue.GetByID(context.Background(), claim.ID)
	if getErr != nil {
		t.Fatalf("Queue.GetByID() error = %v", getErr)
	}
	if queue == nil || queue.Status != "running" || queue.FinishedAt != nil {
		t.Fatalf("queue = %#v, want still-running claimed item", queue)
	}
	loop, getErr := fixture.repos.Loops.GetByID(context.Background(), *claim.LoopID)
	if getErr != nil {
		t.Fatalf("Loops.GetByID() error = %v", getErr)
	}
	if loop == nil || loop.Status != "running" {
		t.Fatalf("loop = %#v, want still-running loop", loop)
	}
	runs, getErr := fixture.repos.Runs.ListByLoop(context.Background(), *claim.LoopID)
	if getErr != nil {
		t.Fatalf("Runs.ListByLoop() error = %v", getErr)
	}
	if len(runs) != 1 || runs[0].Status != "running" {
		t.Fatalf("runs = %#v, want single running run", runs)
	}
}

func TestProcessClaimedItemPreservesPausedLoopOnRetryableFailureAfterPause(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "reviewed", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}, wait: func(ctx context.Context) error {
		items, err := fixture.repos.Queue.List(ctx)
		if err != nil {
			return err
		}
		loopID := ""
		for _, item := range items {
			if item.Type == "reviewer" && item.Status == "running" && item.LoopID != nil {
				loopID = *item.LoopID
				break
			}
		}
		if loopID == "" {
			return fmt.Errorf("running reviewer queue item not found")
		}
		loop, err := fixture.repos.Loops.GetByID(ctx, loopID)
		if err != nil {
			return err
		}
		if loop == nil {
			return fmt.Errorf("loop not found: %s", loopID)
		}
		loop.Status = "paused"
		loop.NextRunAt = nil
		loop.UpdatedAt = fixture.nowISO()
		if err := fixture.repos.Loops.Upsert(ctx, *loop); err != nil {
			return err
		}
		reason := "loop paused"
		if _, err := fixture.repos.Queue.CancelByLoop(ctx, loopID, fixture.nowISO(), &reason); err != nil {
			return err
		}
		return context.DeadlineExceeded
	}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableTransient {
		t.Fatalf("result = %#v, want retryable_transient failure", result)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), claim.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "queued" {
		t.Fatalf("queue = %#v, want queued retry", queue)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.Status != "paused" || loop.NextRunAt != nil {
		t.Fatalf("loop = %#v, want paused loop with nil next run", loop)
	}
}

func TestProcessClaimedItemClassifiesReviewerTimeoutWithDiagnostics(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "timeout", Summary: "processed 900 files before timeout", TimeoutType: "max_runtime", ConfiguredMaxRuntimeSeconds: 5400, ConfiguredIdleTimeoutSeconds: 600, ElapsedRuntimeSeconds: 5400, LastProgressAt: "2026-04-11T12:45:00.000Z"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableTransient {
		t.Fatalf("result = %#v, want retryable timeout failure", result)
	}
	for _, want := range []string{"timed out (max_runtime)", "configured max runtime 5400s", "idle timeout 600s", "last progress at 2026-04-11T12:45:00.000Z", "processed 900 files"} {
		if !strings.Contains(result.Summary, want) {
			t.Fatalf("result.Summary = %q, want %q", result.Summary, want)
		}
	}
	events, err := fixture.repos.Events.ListByEntity(context.Background(), "pull_request", "acme/looper#42")
	if err != nil {
		t.Fatalf("Events.ListByEntity() error = %v", err)
	}
	foundTimeoutEvent := false
	for _, event := range events {
		if event.EventType == "reviewer.agent.timed_out" && strings.Contains(event.PayloadJSON, `"timeoutType":"max_runtime"`) && strings.Contains(event.PayloadJSON, `"elapsedRuntimeSeconds":5400`) {
			foundTimeoutEvent = true
		}
	}
	if !foundTimeoutEvent {
		t.Fatalf("events = %#v, want reviewer.agent.timed_out diagnostics", events)
	}
}

func TestProcessClaimedItemEmitsFailedTerminalEventForReviewerAgentFailure(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "failed", Summary: "agent failed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	if _, err := runner.ProcessClaimedItem(context.Background(), *claim); err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	events, err := fixture.repos.Events.ListByEntity(context.Background(), "pull_request", "acme/looper#42")
	if err != nil {
		t.Fatalf("Events.ListByEntity() error = %v", err)
	}
	foundFailedEvent := false
	for _, event := range events {
		switch event.EventType {
		case "reviewer.agent.failed":
			if strings.Contains(event.PayloadJSON, `"status":"failed"`) {
				foundFailedEvent = true
			}
		case "reviewer.agent.completed":
			t.Fatalf("events = %#v, want reviewer.agent.failed instead of reviewer.agent.completed", events)
		}
	}
	if !foundFailedEvent {
		t.Fatalf("events = %#v, want reviewer.agent.failed terminal event", events)
	}
}

func TestProcessClaimedItemDoesNotEmitStartedWhenReviewerAgentStartFails(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	agent := &fakeAgentExecutor{startErr: fmt.Errorf("executor setup failed")}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" {
		t.Fatalf("result = %#v, want failed start result", result)
	}
	events, err := fixture.repos.Events.ListByEntity(context.Background(), "pull_request", "acme/looper#42")
	if err != nil {
		t.Fatalf("Events.ListByEntity() error = %v", err)
	}
	for _, event := range events {
		if event.EventType == "reviewer.agent.started" {
			t.Fatalf("events = %#v, want no reviewer.agent.started on start failure", events)
		}
	}
}

func TestProcessClaimedItemEmitsTerminalEventWhenReviewerAgentWaitFails(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed"}}, waitErr: fmt.Errorf("execution transport failed")}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" {
		t.Fatalf("result = %#v, want failed wait result", result)
	}
	events, err := fixture.repos.Events.ListByEntity(context.Background(), "pull_request", "acme/looper#42")
	if err != nil {
		t.Fatalf("Events.ListByEntity() error = %v", err)
	}
	foundStarted := false
	foundFailed := false
	for _, event := range events {
		switch event.EventType {
		case "reviewer.agent.started":
			foundStarted = true
		case "reviewer.agent.failed":
			if strings.Contains(event.PayloadJSON, `"status":"wait_error"`) && strings.Contains(event.PayloadJSON, "execution transport failed") {
				foundFailed = true
			}
		}
	}
	if !foundStarted || !foundFailed {
		t.Fatalf("events = %#v, want started and terminal wait-error events", events)
	}
}

func TestNewDefaultsReviewerTimeoutToNinetyMinutes(t *testing.T) {
	t.Parallel()
	runner := New(Options{})
	if runner.agentTimeout != 90*time.Minute {
		t.Fatalf("agentTimeout = %s, want 90m", runner.agentTimeout)
	}
}

func TestBuildReviewPromptLaterPassUsesRepairFrontier(t *testing.T) {
	t.Parallel()

	prompt, _ := buildReviewPromptWithInstructions("", config.Config{}, "acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "new-head"}}, "run_1", "reviewer:loop:new-head", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, true, "", config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper", false, false, "old-head")
	for _, want := range []string{
		"Repair frontier contract (later pass)",
		"Last reviewed head SHA: old-head",
		"Current head SHA for this pass: new-head",
		"every unresolved prior must_fix thread",
		"diff from last reviewed head old-head to current head new-head",
		"Do not rescan untouched original diff",
		"late_discovery:",
		"Ordinary P2/P3 robustness",
		"disposition follow_up",
		"disposition needs_human",
		"Review pass contract (later pass / repair frontier)",
		"Fixer decline adjudication",
		"scope dispute, not dismissal authority",
		"Do not open a duplicate thread",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("later-pass prompt missing %q:\n%s", want, prompt)
		}
	}
	for _, forbidden := range []string{
		"complete one full review pass before publishing",
		"scan every changed file/range in scope",
		"rather than deferring it to a later pass",
	} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("later-pass prompt retains first-pass-only language %q:\n%s", forbidden, prompt)
		}
	}
}

func TestBuildReviewPromptFirstPassKeepsExhaustiveContract(t *testing.T) {
	t.Parallel()

	prompt, _ := buildReviewPromptWithInstructions("", config.Config{}, "acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, true, "", config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper", false, false, "")
	for _, want := range []string{
		"Review pass contract: complete one full review pass before publishing",
		"scan every changed file/range in scope",
		"rather than deferring it to a later pass",
		"Fixer decline adjudication",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("first-pass prompt missing %q:\n%s", want, prompt)
		}
	}
	for _, forbidden := range []string{
		"Repair frontier contract (later pass)",
		"Do not rescan untouched original diff",
		"late_discovery:",
		"Review pass contract (later pass / repair frontier)",
	} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("first-pass prompt contains later-pass-only language %q:\n%s", forbidden, prompt)
		}
	}
}

func TestNativeResumeReReviewPromptUsesRepairFrontierWhenLaterPass(t *testing.T) {
	t.Parallel()

	later := nativeResumeReReviewPrompt("acme/looper", 42, "sess-old", "interrupted", "new-head", "reviewer:loop:new-head", "published-old")
	for _, want := range []string{
		"repair-frontier re-review",
		"not a full discard-and-rescan",
		"last published/reviewed head SHA: published-old",
		"diff from last published head published-old to current head new-head",
		"every unresolved prior must_fix thread",
		// Self-contained later-pass contracts (resume replaces the normal prompt).
		"Repair frontier contract (later pass)",
		"late_discovery:",
		"disposition must_fix",
		"Ordinary P2/P3 robustness",
		"disposition follow_up",
		"disposition needs_human",
		"Fixer decline adjudication",
		"Do not open a duplicate thread",
		"Adjudicate on the existing thread only",
	} {
		if !strings.Contains(later, want) {
			t.Fatalf("later-pass native resume prompt missing %q:\n%s", want, later)
		}
	}
	first := nativeResumeReReviewPrompt("acme/looper", 42, "sess-old", "interrupted", "new-head", "reviewer:loop:new-head", "")
	if strings.Contains(first, "repair-frontier re-review") {
		t.Fatalf("first-pass native resume prompt unexpectedly uses frontier language:\n%s", first)
	}
	if !strings.Contains(first, "Discard findings, assumptions, anchors") {
		t.Fatalf("first-pass native resume prompt missing full re-review language:\n%s", first)
	}
	for _, forbidden := range []string{
		"Repair frontier contract (later pass)",
		"late_discovery:",
		"Fixer decline adjudication",
	} {
		if strings.Contains(first, forbidden) {
			t.Fatalf("first-pass native resume prompt contains later-pass-only language %q:\n%s", forbidden, first)
		}
	}
}

func TestBuildReviewPromptIncludesActionableQualityContract(t *testing.T) {
	t.Parallel()

	prompt := buildReviewPrompt("acme/looper", 42, reviewerCheckpoint{Detail: &checkpointDetail{Labels: []string{specpr.ReviewingLabel}}, Snapshot: &checkpointSnapshot{Title: "Spec PR", HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove, Blocking: config.ReviewerReviewEventComment}, false, config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper")
	for _, want := range []string{
		"Every comment MUST include",
		"Bad comment example",
		"Good spec/docs comment example",
		"Spec/docs review rubric",
		"suggestedChange",
		"do not write or publish a bare LGTM review body",
		"Group findings only when they share the same root cause",
		"fixture-matrix tests",
		"'/opt/looper/bin/looper' review submit acme/looper#42 --event COMMENT --commit-id abc123 --reviewer-run-id run_1 --clean-review-event APPROVE --blocking-review-event COMMENT`",
		"wrapper validates inline anchors against the live PR diff before it calls GitHub",
		"Review pass contract",
		"Do not stop after the first issue",
		"include it in this review rather than deferring it to a later pass",
		"Finding disposition contract",
		"disposition must_fix|follow_up|needs_human",
		"Looper parses this JSON",
		"__LOOPER_RESULT__.findings",
		"Finding accumulator contract",
		"group repeated patterns into systemic comments with representative examples only when they share a root cause",
		"Severity rubric",
		"mark a finding as BLOCKING only when",
		"Mark actionable but merge-safe improvements as NON_BLOCKING",
		"NITs must not block merge",
		"Finalization gate before submit",
		"disposition=must_fix with scopeBasis/scopeEvidence",
		"rejects `follow_up`/`needs_human` in actionable comments or the visible review body",
		"review outcome matches the highest published severity",
		"do not use PATH-based `looper`",
		"repository-local `go run ./cmd/looper`",
		"`gh api repos/acme/looper/pulls/42/reviews`, or `gh pr review` directly",
		"gh api repos/acme/looper/pulls/42/reviews",
		"looper:review id=reviewer:loop:abc123 head=abc123 outcome=clean|non_blocking|blocking",
		"before posting anything",
		"existing PR reviews",
		"Idempotency outcome matching is strict",
		"runner will reconcile the clean-signal +1 reaction and any eligible spec label transition",
		"review request removed before publish",
		"PR head changed before publish",
		"looper:spec-reviewing",
		"Do not add or remove the PR main-conversation +1 reaction yourself",
		"Review body style contract",
		"Never post terminal/tool output",
		"never build review JSON inside a double-quoted shell string",
		"Worktree hygiene contract",
		"do not create review payload or scratch files inside the managed worktree",
		"outbound content safety gate rejection",
		"resubmit in the same session",
		"ANSI escape sequences",
		"file-read traces",
		"submit exactly one APPROVE review through the trusted Looper CLI wrapper with `outcome=clean`, no inline `comments`, and no extra PR conversation comment",
		"'/opt/looper/bin/looper' review submit acme/looper#42 --event APPROVE --commit-id abc123 --reviewer-run-id run_1 --clean-review-event APPROVE --blocking-review-event COMMENT`",
		"never use an LGTM, empty, or disclosure-only clean body as a fallback",
		"visible body must start with `@<PR-author-login>`",
		"briefly summarize what changed or what you verified",
		"warm, friendly, encouraging acknowledgement of the author's work",
		"wrapper rejects clean APPROVE reviews that do not start with an @mention",
		"<!-- looper:stamp v=1 -->",
		`<sub>🔁 Powered by <a href="https://github.com/nexu-io/looper">Looper</a> · runner=reviewer · agent=opencode · An autonomous AI dev team for your GitHub repos.</sub>`,
		"Every inline review comment you post must also use looper's configured visible inline disclosure style",
		"Do not write the footer as plain paragraph text",
		"Minimal PR seed",
		"\"repo\": \"acme/looper\"",
		"\"pr_number\": 42",
		"\"head_sha\": \"abc123\"",
		"Agent-side GitHub fetch contract",
		"Local checkout contract: the current working directory is Looper's prepared reviewer worktree for this PR and is the canonical local checkout for verification",
		"Do not run `gh repo clone`, `git clone`, or create any additional checkout for this PR's base or head repository unless the provided worktree is missing or unusable.",
		"gh pr view <pr-url> -R <repo> --json number,title,body,state,isDraft,baseRefName,headRefName,headRefOid,url,labels",
		"gh pr diff <pr-url> -R <repo> --name-only",
		"gh pr diff <pr-url> -R <repo> --patch",
		"gh pr checks <pr-url> -R <repo>",
		"git diff <base>...<head> -- <path>",
		"gh api repos/{owner}/{repo}/pulls/{number}/comments --paginate",
		"gh api repos/{owner}/{repo}/pulls/{number}/reviews --paginate",
		"gh api repos/{owner}/{repo}/issues/{number}/comments --paginate",
		"structured error with `type` set to one of `auth`, `network`, `rate_limit`, or `pr_drift`",
		"validate every inline review comment's `path`, `line`, `side`, `start_line`, and `start_side` against the live PR diff fetched with `gh pr diff`",
		"Preserve exact anchors that fit the live diff",
		"Do not move must_fix findings into the top-level review body",
		"follow-up quality-gating failure",
		"Resolvable inline review comments are required",
		"PR review `comments` array",
		"not as a separate issue/PR conversation comment",
		"create resolvable GitHub review threads",
		"`path`, `line`, `side`",
		"`start_line` and `start_side`",
		"every must_fix finding must live in inline `comments`",
		"Looper **does** parse this findings list",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
	if strings.Contains(prompt, "<!-- looper:review id=reviewer:loop:abc123 head=abc123 run=") {
		t.Fatalf("prompt includes run-scoped idempotency marker:\n%s", prompt)
	}
	if strings.Contains(prompt, "PR conversation comments") {
		t.Fatalf("prompt idempotency source diverges from review marker verification:\n%s", prompt)
	}
	if strings.Contains(prompt, "moving the same actionable feedback into the review body") {
		t.Fatalf("prompt allows weakening resolvable inline comment contract:\n%s", prompt)
	}
	for _, forbidden := range []string{
		"Submit clean reviews as COMMENT",
		"write a concise clean LGTM body",
		"after the APPROVE review is posted",
		"ensure +1 reaction",
		"remove any existing +1 reaction",
		"Prefer 3 deeply specific comments",
		"prefer fewer deep comments",
		"more than 15 blocking findings",
		"more than 25 total comments",
		"safely downgrade it to top-level review body feedback",
		"Use top-level comments without path/line only",
		"downgrade unanchorable feedback to a top-level review-body item",
	} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("prompt contains conflicting no-actionable clean-review instruction %q:\n%s", forbidden, prompt)
		}
	}
}

func TestBuildReviewPromptForgejoNativeKeepsMustFixInline(t *testing.T) {
	t.Parallel()
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.Providers = []config.ProviderConfig{{ID: "forgejo-main", Kind: config.ProviderKindForgejo, BaseURL: "https://forgejo.example.test", TokenEnv: stringPtr("FORGEJO_TOKEN")}}
	cfg.Projects = []config.ProjectRefConfig{{ID: "project_1", Provider: "forgejo-main", Repo: "acme/looper"}}
	prompt, _ := buildReviewPromptWithInstructions("project_1", cfg, "acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove, Blocking: config.ReviewerReviewEventRequestChanges}, false, true, "", config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper", false, false, "")
	if !strings.Contains(prompt, "every must_fix must remain an inline comment") {
		t.Fatalf("forgejo prompt missing inline-only must_fix contract:\n%s", prompt)
	}
	if strings.Contains(prompt, "downgrade unanchorable feedback to a top-level review-body item") {
		t.Fatalf("forgejo prompt still instructs body-only must_fix:\n%s", prompt)
	}
}

func TestBuildReviewerMinimalPRSeedUsesEnterpriseHost(t *testing.T) {
	t.Parallel()

	seed := buildReviewerMinimalPRSeed("ghe.example.com/acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}}, config.ReviewerScopeFullPR, "")
	if !strings.Contains(seed, "\"url\": \"https://ghe.example.com/acme/looper/pull/42\"") {
		t.Fatalf("seed = %q, want enterprise host PR URL", seed)
	}
}

func TestBuildReviewPromptKeepsCommentCleanPolicyWithoutApproveInstruction(t *testing.T) {
	t.Parallel()

	prompt := buildReviewPrompt("acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper")

	if !strings.Contains(prompt, "do not submit a clean COMMENT or APPROVE review") {
		t.Fatalf("prompt missing reaction-only clean instruction:\n%s", prompt)
	}
	for _, forbidden := range []string{
		"submit exactly one APPROVE review through the trusted Looper CLI wrapper",
		"review submit acme/looper#42 --event APPROVE",
	} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("prompt contains unexpected approve clean instruction %q:\n%s", forbidden, prompt)
		}
	}
}

func TestBuildReviewPromptRequiresHumanCleanApproveBodyMentioningAuthor(t *testing.T) {
	t.Parallel()

	prompt := buildReviewPrompt("acme/looper", 42, reviewerCheckpoint{Detail: &checkpointDetail{Author: "octocat"}, Snapshot: &checkpointSnapshot{HeadSHA: "abc123", Author: "octocat"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove, Blocking: config.ReviewerReviewEventComment}, false, config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper")
	for _, want := range []string{
		"visible body must start with `@octocat`",
		"briefly summarize what changed or what you verified",
		"warm, friendly, encouraging acknowledgement of the author's work",
		"Do not use a bare LGTM or marker/disclosure-only body",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestBuildReviewPromptOmitsSubmitPathInstructionWhenTrustedWrapperUnavailable(t *testing.T) {
	t.Parallel()
	for _, provider := range []string{"GitHub", "Forgejo"} {
		t.Run(provider, func(t *testing.T) {
			cfg, err := config.DefaultConfig(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if provider == "Forgejo" {
				cfg.Providers = []config.ProviderConfig{{ID: "fj", Kind: config.ProviderKindForgejo, BaseURL: "https://code.example", TokenEnv: stringPtr("FORGEJO_TOKEN")}}
				cfg.Projects = []config.ProjectRefConfig{{ID: "p", Provider: "fj", Repo: "acme/looper"}}
			}
			prompt, _ := buildReviewPromptWithInstructions("p", cfg, "acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{Title: "Spec PR", HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove, Blocking: config.ReviewerReviewEventComment}, false, true, "", config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "", false, false, "")

			if !strings.Contains(prompt, "trusted looper review submit wrapper unavailable") {
				t.Fatalf("prompt missing trusted wrapper unavailable failure instruction:\n%s", prompt)
			}
			for _, want := range []string{
				"do not publish any " + provider + " review",
				"do not add or remove any " + provider + " reaction",
			} {
				if !strings.Contains(prompt, want) {
					t.Fatalf("prompt missing wrapper-unavailable failure guard %q:\n%s", want, prompt)
				}
			}
			for _, forbidden := range []string{
				"When submitting through",
				"'' review submit",
				" review submit acme/looper#42",
				"You must publish the GitHub review yourself by calling looper's enforced review-submit wrapper",
				"finish successfully with the `No actionable findings` summary only",
				"finish successfully with a summary beginning `No actionable findings`",
			} {
				if strings.Contains(prompt, forbidden) {
					t.Fatalf("prompt contains unavailable submit-path instruction %q:\n%s", forbidden, prompt)
				}
			}
			if provider == "Forgejo" {
				for _, forbidden := range []string{"GitHub operation contract", "gh api", "gh pr"} {
					if strings.Contains(prompt, forbidden) {
						t.Fatalf("Forgejo wrapper-unavailable prompt contains %q", forbidden)
					}
				}
			}
		})
	}
}

func TestBuildReviewPromptUsesMinimalSeedInsteadOfEmbeddedDiff(t *testing.T) {
	t.Parallel()

	diff := "diff --git a/docs/spec.md b/docs/spec.md\n@@ -4,3 +4,4 @@\n # Reviewer anchors\n existing\n+new requirement\n tail\n"
	payload, err := json.Marshal(map[string]string{"diff": diff})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	prompt := buildReviewPrompt("acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{Title: "Spec PR", HeadSHA: "abc123", PayloadJSON: string(payload)}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper")

	for _, want := range []string{
		"Minimal PR seed",
		"\"repo\": \"acme/looper\"",
		"\"pr_number\": 42",
		"\"head_sha\": \"abc123\"",
		"fetch all mutable PR details yourself",
		"gh pr diff <pr-url> -R <repo> --patch",
		"git diff <base>...<head> -- <path>",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
	for _, forbidden := range []string{
		"gh pr diff -- <path>",
		"ANCHORABLE DIFF LOCATIONS",
		"Diff:\n" + diff,
		"docs/spec.md RIGHT lines 4-7",
	} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("prompt includes full snapshot-derived diff content %q:\n%s", forbidden, prompt)
		}
	}
}

func TestBuildReviewPromptRestrictsExistingMarkerSkipWhenApprovalsDisallowed(t *testing.T) {
	t.Parallel()

	prompt := buildReviewPrompt("acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper")
	for _, want := range []string{
		"only treat an existing `outcome=clean` marker as satisfied when it is on a COMMENTED review if clean policy is COMMENT",
		"Treat `outcome=non_blocking` or legacy `outcome=actionable` markers as satisfied only when they are on a COMMENTED review",
		"'/opt/looper/bin/looper' review submit acme/looper#42 --event COMMENT --commit-id abc123 --reviewer-run-id run_1 --clean-review-event COMMENT --blocking-review-event COMMENT",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
	if strings.Contains(prompt, "--event COMMENT|APPROVE") {
		t.Fatalf("prompt advertises approvals while allowApprove=false:\n%s", prompt)
	}
}

func TestBuildReviewPromptUsesOutcomeSensitiveIdempotencyForApproveAndRequestChangesPolicies(t *testing.T) {
	t.Parallel()

	prompt := buildReviewPrompt("acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove, Blocking: config.ReviewerReviewEventRequestChanges}, false, config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper")
	for _, want := range []string{
		"or on an APPROVED review if clean policy is APPROVE",
		"Only treat an existing `outcome=blocking` marker as satisfied when it is on a CHANGES_REQUESTED review if blocking policy is REQUEST_CHANGES",
		"Treat `outcome=non_blocking` or legacy `outcome=actionable` markers as satisfied only when they are on a COMMENTED review",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestBuildReviewPromptIncludesReviewerScopeInstruction(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		scope config.ReviewerScope
		want  string
	}{
		{name: "full", scope: config.ReviewerScopeFullPR, want: "Review scope: full_pr"},
		{name: "files", scope: config.ReviewerScopeChangedFiles, want: "Review scope: changed_files"},
		{name: "ranges", scope: config.ReviewerScopeChangedRanges, want: "Review scope: changed_ranges"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			prompt := buildReviewPrompt("acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, tc.scope, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper")
			if !strings.Contains(prompt, tc.want) {
				t.Fatalf("prompt missing %q:\n%s", tc.want, prompt)
			}
		})
	}
}

func TestBuildReviewPromptFullPRScopeUsesAgentSideFetchContract(t *testing.T) {
	t.Parallel()

	prompt := buildReviewPrompt("acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, config.ReviewerScopeFullPR, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper")
	for _, want := range []string{
		"Review scope: full_pr",
		"complete diff fetched through `gh` according to the agent-side GitHub fetch contract",
		"canonical local checkout",
		"supported by the fetched context",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
	for _, forbidden := range []string{
		"complete diff payload below",
		"supported by the included context",
	} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("prompt still contains stale embedded-context guidance %q:\n%s", forbidden, prompt)
		}
	}
}

func TestBuildThreadResolutionPromptRequiresPreparedWorktreeReuse(t *testing.T) {
	t.Parallel()

	prompt := buildThreadResolutionPrompt("acme/looper", 42, "abc123", nil, reviewerCheckpoint{})
	for _, want := range []string{
		"canonical local checkout",
		"Do not run gh repo clone, git clone, or create any additional checkout for this PR's base or head repository unless the provided worktree is missing or unusable.",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestShouldRestartFromDiscoverForAgentNativePreflightFailures(t *testing.T) {
	t.Parallel()

	for _, summary := range []string{
		"PR head changed before publish",
		"review request removed before publish",
	} {
		if !shouldRestartFromDiscover("failed", stepReview, summary) {
			t.Fatalf("shouldRestartFromDiscover(review, %q) = false, want true", summary)
		}
	}
}

func TestShouldRestartFromDiscoverForThreadResolutionHeadChange(t *testing.T) {
	t.Parallel()

	if !shouldRestartFromDiscover("failed", stepThreadResolution, "PR changed during thread reconciliation") {
		t.Fatalf("shouldRestartFromDiscover(thread_resolution head change) = false, want true")
	}
}

func TestProcessClaimedItemMarksStaleOnHeadChangeSignal(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{changeHeadOnSecondView: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "failed", Summary: "agent reported a generic shell failure"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !contains(result.Summary, "PR head changed before publish") {
		t.Fatalf("result = %#v, want stale head-change skip", result)
	}
	latestRun, err := fixture.repos.Runs.GetLatestByLoopID(context.Background(), result.LoopID)
	if err != nil || latestRun == nil {
		t.Fatalf("GetLatestByLoopID() = (%#v, %v), want run", latestRun, err)
	}
	checkpoint := parseCheckpoint(latestRun.CheckpointJSON)
	if checkpoint.SkipKind != "stale" {
		t.Fatalf("SkipKind = %q, want stale", checkpoint.SkipKind)
	}
}

func TestProcessClaimedItemMarksStaleOnReviewRequestSignal(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{removeReviewRequestOnSecondView: true, reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "failed", Summary: "agent reported a generic shell failure"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !contains(result.Summary, "review request removed before publish") {
		t.Fatalf("result = %#v, want stale review-request skip", result)
	}
	latestRun, err := fixture.repos.Runs.GetLatestByLoopID(context.Background(), result.LoopID)
	if err != nil || latestRun == nil {
		t.Fatalf("GetLatestByLoopID() = (%#v, %v), want run", latestRun, err)
	}
	checkpoint := parseCheckpoint(latestRun.CheckpointJSON)
	if checkpoint.SkipKind != "stale" {
		t.Fatalf("SkipKind = %q, want stale", checkpoint.SkipKind)
	}
}

func TestProcessClaimedItemMarksStaleOnUnparsedReviewRequestGuardrail(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "review request removed before publish"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !contains(result.Summary, "review request removed before publish") {
		t.Fatalf("result = %#v, want stale review-request skip", result)
	}
	latestRun, err := fixture.repos.Runs.GetLatestByLoopID(context.Background(), result.LoopID)
	if err != nil || latestRun == nil {
		t.Fatalf("GetLatestByLoopID() = (%#v, %v), want run", latestRun, err)
	}
	checkpoint := parseCheckpoint(latestRun.CheckpointJSON)
	if checkpoint.SkipKind != "stale" {
		t.Fatalf("SkipKind = %q, want stale", checkpoint.SkipKind)
	}
}

func TestRunReviewStepIgnoresUnparsedReviewRequestGuardrailWhenPolicyDisabled(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "review request removed before publish"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: false, Labels: []string{}, LabelMode: config.LabelModeAll}})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	checkpoint, err := runner.runReviewStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_policy_disabled", ProjectID: project.ID, Type: "reviewer"},
		Run:      storage.RunRecord{ID: "run_policy_disabled", LoopID: "loop_policy_disabled"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "pr-42-head", PreparedAt: fixture.nowISO()},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "valid completion marker") {
		t.Fatalf("runReviewStep() error = %v, want marker failure", err)
	}
	if checkpoint.ResumePolicy == "restart_from_discover" {
		t.Fatalf("ResumePolicy = %q, want no rediscovery restart", checkpoint.ResumePolicy)
	}
}

func TestRunReviewStepIgnoresUnparsedReviewRequestGuardrailForManualLoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "review request removed before publish"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	metadata := `{"manual":true}`
	checkpoint, err := runner.runReviewStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_manual_guardrail", ProjectID: project.ID, Type: "reviewer", MetadataJSON: &metadata},
		Run:      storage.RunRecord{ID: "run_manual_guardrail", LoopID: "loop_manual_guardrail"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "pr-42-head", PreparedAt: fixture.nowISO()},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "valid completion marker") {
		t.Fatalf("runReviewStep() error = %v, want marker failure", err)
	}
	if checkpoint.ResumePolicy == "restart_from_discover" {
		t.Fatalf("ResumePolicy = %q, want no rediscovery restart", checkpoint.ResumePolicy)
	}
}

func TestRunReviewStepIgnoresUnparsedReviewRequestGuardrailForExistingFollowUpNewHead(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "review request removed before publish"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	metadata := `{"followUpdates":true,"lastPublishedHeadSha":"old-head","loop":{"enabled":true,"iterationCount":1,"iterationsByHead":{"old-head":1}}}`
	checkpoint, err := runner.runReviewStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_followup_guardrail", ProjectID: project.ID, Type: "reviewer", MetadataJSON: &metadata},
		Run:      storage.RunRecord{ID: "run_followup_guardrail", LoopID: "loop_followup_guardrail"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "pr-42-head", PreparedAt: fixture.nowISO()},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "valid completion marker") {
		t.Fatalf("runReviewStep() error = %v, want marker failure", err)
	}
	if checkpoint.ResumePolicy == "restart_from_discover" {
		t.Fatalf("ResumePolicy = %q, want no rediscovery restart", checkpoint.ResumePolicy)
	}
}

func TestRunReviewStepSkipsWhenFollowUpLostReviewRequest(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"alice"}, currentLogin: "bob"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "should not run", Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	checkpoint, err := runner.runReviewStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_followup_request_gone", ProjectID: project.ID, Type: "reviewer"},
		Run:      storage.RunRecord{ID: "run_followup_request_gone", LoopID: "loop_followup_request_gone"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:                       &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", ReviewRequests: []string{"alice"}, CurrentLogin: "bob"},
			Snapshot:                     &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree:                     &checkpointWorktree{Path: t.TempDir(), Branch: "pr-42-head", PreparedAt: fixture.nowISO()},
			ThreadResolutionFollowUpOnly: true,
		},
	})
	if err != nil {
		t.Fatalf("runReviewStep() error = %v", err)
	}
	if checkpoint.SkipKind != "not_requested" {
		t.Fatalf("SkipKind = %q, want not_requested", checkpoint.SkipKind)
	}
	if len(agent.starts) != 0 {
		t.Fatalf("len(agent.starts) = %d, want 0", len(agent.starts))
	}
}

func TestRunPublishStepSkipsPendingReviewWhenFollowUpLostReviewRequest(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"alice"}, currentLogin: "bob", reviewMarkerEvent: ReviewEventComment, reviewMarkerOutcome: "non_blocking"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	checkpoint, err := runner.runPublishStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_publish_request_gone", ProjectID: project.ID, Type: "reviewer"},
		Run:      storage.RunRecord{ID: "run_publish_request_gone", LoopID: "loop_publish_request_gone"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:                       &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", ReviewRequests: []string{"bob"}, CurrentLogin: "bob"},
			Snapshot:                     &checkpointSnapshot{HeadSHA: "abc123"},
			ThreadResolutionFollowUpOnly: true,
			PendingReview:                &pendingReviewCheckpoint{HeadSHA: "abc123", IdempotencyKey: "reviewer:loop_publish_request_gone:abc123", Event: reviewEventAgentNative, Summary: "posted review"},
		},
	})
	if err != nil {
		t.Fatalf("runPublishStep() error = %v", err)
	}
	if checkpoint.SkipKind != "not_requested" {
		t.Fatalf("SkipKind = %q, want not_requested", checkpoint.SkipKind)
	}
	if checkpoint.PendingReview != nil {
		t.Fatalf("PendingReview = %#v, want nil", checkpoint.PendingReview)
	}
	if github.reviewMarkerCalls != 0 {
		t.Fatalf("reviewMarkerCalls = %d, want 0", github.reviewMarkerCalls)
	}
}

func TestRunPublishStepKeepsPendingReviewForExistingLoopFollowUpOnNewHeadWithoutReviewRequest(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{}, currentLogin: "bob", reviewMarkerMissing: true}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	metadata := `{"followUpdates":true,"lastPublishedHeadSha":"old-head","loop":{"enabled":true,"iterationCount":1,"iterationsByHead":{"old-head":1}}}`
	checkpoint, err := runner.runPublishStep(context.Background(), stepInput{
		Project: *project,
		Loop:    storage.LoopRecord{ID: "loop_publish_followup_new_head", ProjectID: project.ID, Type: "reviewer", MetadataJSON: &metadata},
		Run:     storage.RunRecord{ID: "run_publish_followup_new_head", LoopID: "loop_publish_followup_new_head"},
		Repo:    "acme/looper", PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:        &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", ReviewRequests: []string{}, CurrentLogin: "bob"},
			Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
			PendingReview: &pendingReviewCheckpoint{HeadSHA: "abc123", IdempotencyKey: "reviewer:loop_publish_followup_new_head:abc123", Event: reviewEventAgentNative, Summary: "posted review"},
		},
	})
	if err == nil || !contains(err.Error(), "no matching GitHub review marker") {
		t.Fatalf("runPublishStep() error = %v, want marker verification retry", err)
	}
	if checkpoint.SkipKind == "not_requested" {
		t.Fatalf("SkipKind = %q, want new-head follow-up authority to continue publish", checkpoint.SkipKind)
	}
	if checkpoint.PendingReview == nil {
		t.Fatalf("PendingReview = nil, want pending review retained for retry")
	}
}

func TestRunPublishStepMarksStaleWhenAgentNativeMarkerMissingAndHeadChanged(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true, viewHeadSHA: "new-head"}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	checkpoint, err := runner.runPublishStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_publish_head_changed", ProjectID: project.ID, Type: "reviewer"},
		Run:      storage.RunRecord{ID: "run_publish_head_changed", LoopID: "loop_publish_head_changed"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:        &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", ReviewRequests: []string{"octocat"}},
			Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
			PendingReview: &pendingReviewCheckpoint{HeadSHA: "abc123", IdempotencyKey: "reviewer:loop_publish_head_changed:abc123", Event: reviewEventAgentNative, Summary: "posted review"},
		},
	})
	if err != nil {
		t.Fatalf("runPublishStep() error = %v, want stale checkpoint", err)
	}
	if checkpoint.SkipKind != "stale" {
		t.Fatalf("SkipKind = %q, want stale", checkpoint.SkipKind)
	}
	if !contains(checkpoint.SkipReason, "PR head changed before publish") {
		t.Fatalf("SkipReason = %q, want head-change stale reason", checkpoint.SkipReason)
	}
	if checkpoint.PendingReview != nil {
		t.Fatalf("PendingReview = %#v, want nil after stale publish", checkpoint.PendingReview)
	}
	if github.reviewMarkerCalls != 0 {
		t.Fatalf("reviewMarkerCalls = %d, want 0 when head drift wins before marker retry", github.reviewMarkerCalls)
	}
}

func TestRunPublishStepMarksStaleWhenAgentNativeMarkerMissingAfterHeadChanges(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true, changeHeadOnSecondView: true}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	checkpoint, err := runner.runPublishStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_publish_marker_missing_then_head_changed", ProjectID: project.ID, Type: "reviewer"},
		Run:      storage.RunRecord{ID: "run_publish_marker_missing_then_head_changed", LoopID: "loop_publish_marker_missing_then_head_changed"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:        &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", ReviewRequests: []string{"octocat"}},
			Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
			PendingReview: &pendingReviewCheckpoint{HeadSHA: "abc123", IdempotencyKey: "reviewer:loop_publish_marker_missing_then_head_changed:abc123", Event: reviewEventAgentNative, Summary: "posted review"},
		},
	})
	if err != nil {
		t.Fatalf("runPublishStep() error = %v, want stale checkpoint", err)
	}
	if checkpoint.SkipKind != "stale" {
		t.Fatalf("SkipKind = %q, want stale", checkpoint.SkipKind)
	}
	if !contains(checkpoint.SkipReason, "PR head changed before publish") {
		t.Fatalf("SkipReason = %q, want head-change stale reason", checkpoint.SkipReason)
	}
	if checkpoint.PendingReview != nil {
		t.Fatalf("PendingReview = %#v, want nil after stale publish", checkpoint.PendingReview)
	}
	if github.reviewMarkerCalls == 0 {
		t.Fatalf("reviewMarkerCalls = %d, want marker lookup before stale recheck", github.reviewMarkerCalls)
	}
	if github.viewCalls < 2 {
		t.Fatalf("viewCalls = %d, want publish drift check plus stale recheck", github.viewCalls)
	}
}

func TestRunPublishStepSkipsWhenCurrentUserAlreadyReviewedPendingHeadWithoutMarker(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		reviewRequests:      []string{},
		currentLogin:        "octocat",
		reviewMarkerMissing: true,
		reviews: []map[string]any{{
			"author": map[string]any{"login": "octocat"},
			"state":  "COMMENTED",
			"commit": map[string]any{"oid": "abc123"},
		}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	checkpoint, err := runner.runPublishStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_publish_already_reviewed_without_marker", ProjectID: project.ID, Type: "reviewer"},
		Run:      storage.RunRecord{ID: "run_publish_already_reviewed_without_marker", LoopID: "loop_publish_already_reviewed_without_marker"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:        &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", ReviewRequests: []string{}, CurrentLogin: "octocat"},
			Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
			PendingReview: &pendingReviewCheckpoint{HeadSHA: "abc123", IdempotencyKey: "reviewer:loop_publish_already_reviewed_without_marker:abc123", Event: reviewEventAgentNative, Summary: "posted review"},
		},
	})
	if err != nil {
		t.Fatalf("runPublishStep() error = %v, want already-reviewed skip", err)
	}
	if checkpoint.SkipKind != "already_reviewed_by_current_user" {
		t.Fatalf("SkipKind = %q, want already_reviewed_by_current_user", checkpoint.SkipKind)
	}
	if !contains(checkpoint.SkipReason, "already reviewed head abc123") || contains(checkpoint.SkipReason, "not requested") {
		t.Fatalf("SkipReason = %q, want already-reviewed skip", checkpoint.SkipReason)
	}
	if checkpoint.PendingReview != nil {
		t.Fatalf("PendingReview = %#v, want nil after already-reviewed skip", checkpoint.PendingReview)
	}
	if github.reviewMarkerCalls == 0 {
		t.Fatalf("reviewMarkerCalls = %d, want marker lookup before fallback", github.reviewMarkerCalls)
	}
	if len(github.addReactionCalls) != 0 || len(github.removeReactionCalls) != 0 || len(github.addLabelCalls) != 0 || len(github.removeLabelCalls) != 0 {
		t.Fatalf("GitHub side effects = addReaction:%d removeReaction:%d addLabel:%d removeLabel:%d, want none", len(github.addReactionCalls), len(github.removeReactionCalls), len(github.addLabelCalls), len(github.removeLabelCalls))
	}
}

func TestRunPublishStepManualLoopKeepsMarkerMissingRetryWhenCurrentUserAlreadyReviewedPendingHead(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		reviewRequests:      []string{},
		currentLogin:        "octocat",
		reviewMarkerMissing: true,
		reviews: []map[string]any{{
			"author": map[string]any{"login": "octocat"},
			"state":  "COMMENTED",
			"commit": map[string]any{"oid": "abc123"},
		}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	metadata := `{"manual":true}`
	checkpoint, err := runner.runPublishStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_publish_manual_already_reviewed", ProjectID: project.ID, Type: "reviewer", MetadataJSON: &metadata},
		Run:      storage.RunRecord{ID: "run_publish_already_reviewed", LoopID: "loop_publish_already_reviewed"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:        &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", ReviewRequests: []string{}, CurrentLogin: "octocat"},
			Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
			PendingReview: &pendingReviewCheckpoint{HeadSHA: "abc123", IdempotencyKey: "reviewer:loop_publish_already_reviewed:abc123", Event: reviewEventAgentNative, Summary: "posted review"},
		},
	})
	if err == nil || !contains(err.Error(), "no matching GitHub review marker") {
		t.Fatalf("runPublishStep() error = %v, want marker verification retry", err)
	}
	if checkpoint.SkipKind == "already_reviewed_by_current_user" {
		t.Fatalf("SkipKind = %q, want manual loop to keep marker-missing recovery", checkpoint.SkipKind)
	}
	if checkpoint.PendingReview == nil {
		t.Fatalf("PendingReview = nil, want pending review retained for retry")
	}
	if checkpoint.ResumePolicy != "advance_from_checkpoint" {
		t.Fatalf("ResumePolicy = %q, want advance_from_checkpoint", checkpoint.ResumePolicy)
	}
	if github.reviewMarkerCalls == 0 {
		t.Fatalf("reviewMarkerCalls = %d, want review marker lookup before retry", github.reviewMarkerCalls)
	}
	if checkpoint.SkipReason != "" {
		t.Fatalf("SkipReason = %q, want no skip reason while marker retry remains active", checkpoint.SkipReason)
	}
}

func TestRunPublishStepMarksStaleWhenMarkerMissingRecoveryFetchSeesHeadChangeBeforeNotRequested(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{changeHeadOnSecondView: true, removeReviewRequestOnSecondView: true, reviewRequests: []string{"octocat"}, currentLogin: "octocat", reviewMarkerMissing: true}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	checkpoint, err := runner.runPublishStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_publish_marker_missing_recovery_stale", ProjectID: project.ID, Type: "reviewer"},
		Run:      storage.RunRecord{ID: "run_publish_marker_missing_recovery_stale", LoopID: "loop_publish_marker_missing_recovery_stale"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:        &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", ReviewRequests: []string{"octocat"}, CurrentLogin: "octocat"},
			Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
			PendingReview: &pendingReviewCheckpoint{HeadSHA: "abc123", IdempotencyKey: "reviewer:loop_publish_marker_missing_recovery_stale:abc123", Event: reviewEventAgentNative, Summary: "posted review"},
		},
	})
	if err != nil {
		t.Fatalf("runPublishStep() error = %v, want stale checkpoint", err)
	}
	if checkpoint.SkipKind != "stale" {
		t.Fatalf("SkipKind = %q, want stale", checkpoint.SkipKind)
	}
	if !contains(checkpoint.SkipReason, "PR head changed before publish") {
		t.Fatalf("SkipReason = %q, want head-change stale reason", checkpoint.SkipReason)
	}
	if checkpoint.PendingReview != nil {
		t.Fatalf("PendingReview = %#v, want nil after stale publish", checkpoint.PendingReview)
	}
}

func TestRunPublishStepMarksStaleWhenMarkerMissingRecoveryFetchSeesClosedPRBeforeAlreadyReviewedSkip(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		currentLogin:            "octocat",
		reviewMarkerMissing:     true,
		viewStateAfterFirstView: "CLOSED",
		reviews: []map[string]any{{
			"author": map[string]any{"login": "octocat"},
			"state":  "COMMENTED",
			"commit": map[string]any{"oid": "abc123"},
		}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{}, LabelMode: config.LabelModeAll}})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	checkpoint, err := runner.runPublishStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_publish_marker_missing_recovery_closed", ProjectID: project.ID, Type: "reviewer"},
		Run:      storage.RunRecord{ID: "run_publish_marker_missing_recovery_closed", LoopID: "loop_publish_marker_missing_recovery_closed"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:        &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", ReviewRequests: []string{}, CurrentLogin: "octocat"},
			Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
			PendingReview: &pendingReviewCheckpoint{HeadSHA: "abc123", IdempotencyKey: "reviewer:loop_publish_marker_missing_recovery_closed:abc123", Event: reviewEventAgentNative, Summary: "posted review"},
		},
	})
	if err != nil {
		t.Fatalf("runPublishStep() error = %v, want stale checkpoint", err)
	}
	if checkpoint.SkipKind != "stale" {
		t.Fatalf("SkipKind = %q, want stale", checkpoint.SkipKind)
	}
	if !contains(checkpoint.SkipReason, "expected PR state OPEN, observed CLOSED") {
		t.Fatalf("SkipReason = %q, want closed PR stale reason", checkpoint.SkipReason)
	}
	if checkpoint.PendingReview != nil {
		t.Fatalf("PendingReview = %#v, want nil after stale publish", checkpoint.PendingReview)
	}
	if checkpoint.SkipReviewerLogin != "" {
		t.Fatalf("SkipReviewerLogin = %q, want empty when stale wins over already-reviewed skip", checkpoint.SkipReviewerLogin)
	}
}

func TestProcessClaimedItemMarksStaleOnUnparsedHeadChangeGuardrail(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Stdout: "PR head changed before publish: expected abc123, got new-head"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !contains(result.Summary, "PR head changed before publish") {
		t.Fatalf("result = %#v, want stale head-change skip", result)
	}
	latestRun, err := fixture.repos.Runs.GetLatestByLoopID(context.Background(), result.LoopID)
	if err != nil || latestRun == nil {
		t.Fatalf("GetLatestByLoopID() = (%#v, %v), want run", latestRun, err)
	}
	checkpoint := parseCheckpoint(latestRun.CheckpointJSON)
	if checkpoint.SkipKind != "stale" {
		t.Fatalf("SkipKind = %q, want stale", checkpoint.SkipKind)
	}
}

func TestProcessClaimedItemMarksStaleOnEmbeddedUnparsedGuardrail(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Stderr: "fatal: publish aborted: review request removed before publish; not posting review"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !contains(result.Summary, "review request removed before publish") {
		t.Fatalf("result = %#v, want stale review-request skip", result)
	}
	latestRun, err := fixture.repos.Runs.GetLatestByLoopID(context.Background(), result.LoopID)
	if err != nil || latestRun == nil {
		t.Fatalf("GetLatestByLoopID() = (%#v, %v), want run", latestRun, err)
	}
	checkpoint := parseCheckpoint(latestRun.CheckpointJSON)
	if checkpoint.SkipKind != "stale" {
		t.Fatalf("SkipKind = %q, want stale", checkpoint.SkipKind)
	}
}

func TestProcessClaimedItemMarksStaleWhenRemoteHeadChangesDuringWorktree(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	git := &fakeGitGateway{prepareErr: &gitinfra.RemoteHeadChangedError{Branch: "refs/pull/42/head", ExpectedHeadSHA: "abc123", ActualHeadSHA: "def456"}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: git, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNext() = (%#v, %v), want claimed queue item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !contains(result.Summary, "Remote head changed for refs/pull/42/head") {
		t.Fatalf("result = %#v, want stale remote-head skip", result)
	}
	latestRun, err := fixture.repos.Runs.GetLatestByLoopID(context.Background(), result.LoopID)
	if err != nil || latestRun == nil {
		t.Fatalf("GetLatestByLoopID() = (%#v, %v), want run", latestRun, err)
	}
	checkpoint := parseCheckpoint(latestRun.CheckpointJSON)
	if checkpoint.SkipKind != "stale" {
		t.Fatalf("SkipKind = %q, want stale", checkpoint.SkipKind)
	}
}

func TestRunReviewStepIgnoresPromptEchoedRediscoveryGuardrail(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Reviewer prompt said to report `PR head changed before publish` before retrying."}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v), want project", project, err)
	}
	checkpoint, err := runner.runReviewStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     storage.LoopRecord{ID: "loop_prompt_echo", ProjectID: project.ID, Type: "reviewer"},
		Run:      storage.RunRecord{ID: "run_prompt_echo", LoopID: "loop_prompt_echo"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "pr-42-head", PreparedAt: fixture.nowISO()},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "valid completion marker") {
		t.Fatalf("runReviewStep() error = %v, want marker failure", err)
	}
	if checkpoint.ResumePolicy == "restart_from_discover" {
		t.Fatalf("ResumePolicy = %q, want no rediscovery restart", checkpoint.ResumePolicy)
	}
}

func TestBuildReviewPromptUsesConfiguredDisclosure(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultDisclosureConfig()
	model := "openai/gpt-5.5"
	prompt := buildReviewPrompt("acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, config.ReviewerScopeChangedRanges, cfg, "claude-code", model, "/opt/looper/bin/looper")
	if !strings.Contains(prompt, `🔁 Powered by <a href="https://github.com/nexu-io/looper">Looper</a> · runner=reviewer · agent=claude-code · An autonomous AI dev team for your GitHub repos.`) {
		t.Fatalf("prompt missing configured linked disclosure:\n%s", prompt)
	}
	if !strings.Contains(prompt, "agent=claude-code") {
		t.Fatalf("prompt missing agent in visible disclosure:\n%s", prompt)
	}
	if strings.Contains(prompt, "model=openai/gpt-5.5") {
		t.Fatalf("prompt exposes model in visible Markdown disclosure:\n%s", prompt)
	}
	if strings.Contains(prompt, "agent=opencode") {
		t.Fatalf("prompt should use configured agent runtime, not hardcoded opencode:\n%s", prompt)
	}

	cfg.Enabled = false
	disabledPrompt := buildReviewPrompt("acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, config.ReviewerScopeChangedRanges, cfg, "claude-code", model, "/opt/looper/bin/looper")
	if !strings.Contains(disabledPrompt, "disclosure stamping is disabled") {
		t.Fatalf("prompt missing disabled disclosure instruction:\n%s", disabledPrompt)
	}
	if strings.Contains(disabledPrompt, "Generated by looper") {
		t.Fatalf("prompt included disclosure footer while disabled:\n%s", disabledPrompt)
	}
}

func TestBuildReviewPromptDoesNotTransitionSpecLabelsWithoutApprove(t *testing.T) {
	t.Parallel()

	prompt := buildReviewPrompt("acme/looper", 42, reviewerCheckpoint{Detail: &checkpointDetail{Labels: []string{specpr.ReviewingLabel}}, Snapshot: &checkpointSnapshot{Title: "Spec PR", HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper")
	if !strings.Contains(prompt, "Do not transition spec-review labels") {
		t.Fatalf("prompt missing no-transition instruction:\n%s", prompt)
	}
	if strings.Contains(prompt, "add `looper:spec-ready`") {
		t.Fatalf("prompt allows spec-ready transition when approve is disabled:\n%s", prompt)
	}
}

func TestBuildReviewPromptOmitsReviewRequestGuardrailWhenDisabled(t *testing.T) {
	t.Parallel()

	prompt, _ := buildReviewPromptWithInstructions("", config.Config{}, "acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, false, "", config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper", false, false, "")

	if strings.Contains(prompt, "review request removed before publish") {
		t.Fatalf("prompt retained review-request guardrail while disabled:\n%s", prompt)
	}
	if !strings.Contains(prompt, "does not require a current-user review request") {
		t.Fatalf("prompt missing disabled review-request instruction:\n%s", prompt)
	}
}

func TestBuildReviewPromptBindsAutomaticReviewerRunID(t *testing.T) {
	t.Parallel()

	prompt, _ := buildReviewPromptWithInstructions("", config.Config{}, "acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}}, "run_auto", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, true, "", config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper", false, false, "")
	if !strings.Contains(prompt, "--reviewer-run-id run_auto") {
		t.Fatalf("automatic prompt missing --reviewer-run-id:\n%s", prompt)
	}
	if strings.Contains(prompt, "--reviewer-manual") {
		t.Fatalf("automatic prompt included --reviewer-manual:\n%s", prompt)
	}
}

func TestBuildReviewPromptNamesFollowUpReviewRequestBypass(t *testing.T) {
	t.Parallel()

	prompt, _ := buildReviewPromptWithInstructions("", config.Config{}, "acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, false, "follow_up_new_head", config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper", false, false, "")

	if strings.Contains(prompt, "review request removed before publish") {
		t.Fatalf("prompt retained review-request guardrail for follow-up bypass:\n%s", prompt)
	}
	if strings.Contains(prompt, "configuration does not require") {
		t.Fatalf("prompt misstates follow-up bypass as configuration-disabled authority:\n%s", prompt)
	}
	if !strings.Contains(prompt, "enabled reviewer follow-up loop") || !strings.Contains(prompt, "differs from the last published review head") {
		t.Fatalf("prompt missing follow-up authority explanation:\n%s", prompt)
	}
}

func TestBuildReviewPromptCommentOnlyOmitsGitHubPublishInstructions(t *testing.T) {
	t.Parallel()

	prompt, _ := buildReviewPromptWithInstructions("", config.Config{}, "acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, false, "", config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper", false, true, "")

	for _, forbidden := range []string{"gh pr view", "gh pr diff", "gh api", "GitHub operation contract", "review request removed before publish", "trusted Looper CLI at", "review-thread resolution"} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("prompt contains %q:\n%s", forbidden, prompt)
		}
	}
	if !strings.Contains(prompt, "comment-only") || !strings.Contains(prompt, "Looper will post your final completion summary") {
		t.Fatalf("prompt missing comment-only publish instructions:\n%s", prompt)
	}
	for _, required := range []string{"`findings`", "`review_item_id`", "`supersedes`", "disposition `must_fix`", "follow_up", "needs_human", "scopeBasis", "scopeEvidence"} {
		if !strings.Contains(prompt, required) {
			t.Fatalf("prompt missing %s contract:\n%s", required, prompt)
		}
	}
	for _, forbidden := range []string{"Prefer 3 deeply specific comments", "prefer fewer deep comments", "more than 15 blocking", "more than 25 total"} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("comment-only prompt retains flood language %q:\n%s", forbidden, prompt)
		}
	}
}

func TestBuildReviewPromptCommentOnlyIncludesExistingReviewerSummaryAuthority(t *testing.T) {
	t.Parallel()

	existing := forge.NewReviewerSummary(2, []forge.ReviewItem{{ReviewItemID: "R-007", Status: forge.ReviewItemStatusOpen, Title: "Keep ID", Body: "Still broken.", Files: []string{"internal/reviewer/runner.go"}, LastSeenRoundID: 2}})
	existingBody, err := renderReviewerSummaryComment(existing, "Previous summary")
	if err != nil {
		t.Fatalf("renderReviewerSummaryComment() error = %v", err)
	}
	prompt, _ := buildReviewPromptWithInstructions("", config.Config{}, "acme/looper", 42, reviewerCheckpoint{Snapshot: &checkpointSnapshot{HeadSHA: "abc123"}, Detail: &checkpointDetail{IssueComments: []map[string]any{{"id": int64(91), "body": existingBody}}}}, "run_1", "reviewer:loop:abc123", config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventComment, Blocking: config.ReviewerReviewEventComment}, false, false, "", config.ReviewerScopeChangedRanges, config.DefaultDisclosureConfig(), "opencode", "", "/opt/looper/bin/looper", false, true, "")

	if !strings.Contains(prompt, "Existing Reviewer Summary authority") {
		t.Fatalf("prompt missing reviewer summary authority:\n%s", prompt)
	}
	for _, required := range []string{"\"review_round_id\": 2", "\"review_item_id\": \"R-007\"", "\"title\": \"Keep ID\""} {
		if !strings.Contains(prompt, required) {
			t.Fatalf("prompt missing %s in reviewer summary context:\n%s", required, prompt)
		}
	}
}

func TestProcessClaimedItemCommentOnlyPublishesOneCommentAndSkipsRediscoveryForSameHead(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{"looper:review"}, reviewRequests: []string{}, currentLogin: "reviewer"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "internal/reviewer/runner.go: add a regression test for Forgejo comment-only publish", Stdout: `__LOOPER_RESULT__={"summary":"internal/reviewer/runner.go: add a regression test for Forgejo comment-only publish","outcome":"non_blocking","findings":[{"title":"Regression coverage missing for comment-only publish","body":"Add a focused reviewer test that proves Forgejo comment-only publish upserts the fixed Reviewer Summary comment instead of treating freeform markdown as authority.","files":["internal/reviewer/runner_test.go"],"disposition":"must_fix","severity":"non_blocking","scopeBasis":"required_invariant","scopeEvidence":"comment-only publish contract"}]}`}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, CommentOnlyPublish: true, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: false, Labels: []string{"looper:review"}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig()})

	discovery, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(github.listReviewRequestedCalls) != 0 {
		t.Fatalf("review-request calls = %#v, want none for comment-only label discovery", github.listReviewRequestedCalls)
	}
	if len(github.listCalls) != 1 || github.listCalls[0].Label != "looper:review" {
		t.Fatalf("list calls = %#v, want label-based discovery", github.listCalls)
	}
	if len(discovery.QueueItems) != 1 {
		t.Fatalf("queue items = %#v, want one queued PR", discovery.QueueItems)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed reviewer item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(github.submitReviewCalls) != 0 {
		t.Fatalf("submitReviewCalls = %#v, want no native review submissions", github.submitReviewCalls)
	}
	if len(github.issueCommentCalls) != 1 {
		t.Fatalf("issueCommentCalls = %#v, want exactly one comment", github.issueCommentCalls)
	}
	if !strings.Contains(github.issueCommentCalls[0].Body, "Forgejo comment-only publish") || !strings.Contains(github.issueCommentCalls[0].Body, forge.ReviewerSummaryMarker) {
		t.Fatalf("comment body = %q, want reviewer summary marker", github.issueCommentCalls[0].Body)
	}
	parsedSummary, err := forge.ParseReviewerSummary(github.issueCommentCalls[0].Body)
	if err != nil {
		t.Fatalf("ParseReviewerSummary() error = %v", err)
	}
	if len(parsedSummary.Items) != 1 || parsedSummary.Items[0].ReviewItemID != "R-001" || parsedSummary.Items[0].Status != forge.ReviewItemStatusOpen {
		t.Fatalf("parsed summary = %#v, want single open item", parsedSummary)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), *claim.LoopID)
	if err != nil || loop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", loop, err)
	}
	meta := parseJSONObject(loop.MetadataJSON)
	if got, _ := stringFromAny(meta["lastPublishedHeadSha"]); got != "abc123" {
		t.Fatalf("lastPublishedHeadSha = %q, want abc123", got)
	}
	rediscovery, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("second DiscoverPullRequests() error = %v", err)
	}
	if len(rediscovery.QueueItems) != 0 {
		t.Fatalf("second discovery queue items = %#v, want none for already-published head", rediscovery.QueueItems)
	}
	if len(github.issueCommentCalls) != 1 {
		t.Fatalf("issueCommentCalls after rediscovery = %#v, want still one comment", github.issueCommentCalls)
	}
}

func TestProcessClaimedItemCommentOnlyPublishesCleanNoopWithoutReactionOrLabelRemoval(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{specpr.ReviewingLabel}, reviewRequests: []string{}, currentLogin: "reviewer"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings","outcome":"clean","findings":[]}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, CommentOnlyPublish: true, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: false, Labels: []string{specpr.ReviewingLabel}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig()})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed reviewer item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(github.issueCommentCalls) != 1 {
		t.Fatalf("issueCommentCalls = %#v, want exactly one comment", github.issueCommentCalls)
	}
	parsedSummary, err := forge.ParseReviewerSummary(github.issueCommentCalls[0].Body)
	if err != nil {
		t.Fatalf("ParseReviewerSummary() error = %v", err)
	}
	if len(parsedSummary.Items) != 0 {
		t.Fatalf("parsed summary items = %#v, want none", parsedSummary.Items)
	}
	if len(github.addReactionCalls) != 0 {
		t.Fatalf("addReactionCalls = %#v, want no GitHub reaction for comment-only clean noop", github.addReactionCalls)
	}
	if len(github.removeLabelCalls) != 0 {
		t.Fatalf("removeLabelCalls = %#v, want no spec-reviewing label removal", github.removeLabelCalls)
	}
}

func TestProcessClaimedItemForgejoProjectInfersCommentOnlyForCleanNoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{"looper:review"}, reviewRequests: []string{}, currentLogin: "reviewer"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings","outcome":"clean","findings":[]}`, ParseStatus: "parsed"}}}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Providers = []config.ProviderConfig{{ID: "forgejo-main", Kind: config.ProviderKindForgejo, BaseURL: "https://forgejo.example.test", TokenEnv: stringPtr("FORGEJO_TOKEN")}}
	cfg.Projects = []config.ProjectRefConfig{{ID: "project_1", Provider: "forgejo-main", Repo: "acme/looper", RepoPath: "/tmp/repos/looper"}}
	cfg.Roles.Reviewer.Discovery.Triggers.RequireReviewRequest = false
	cfg.Roles.Reviewer.Discovery.Triggers.Labels = []string{"looper:review"}
	cfg.Roles.Reviewer.Behavior.ReviewEvents.Clean = config.ReviewerReviewEventComment
	cfg.Roles.Reviewer.Behavior.ReviewEvents.Blocking = config.ReviewerReviewEventComment
	cfg.Roles.Reviewer.Behavior.PublishMode = config.ReviewerPublishModeSummaryComment
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: false, Labels: []string{"looper:review"}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig(), ReviewEvents: cfg.Roles.Reviewer.Behavior.ReviewEvents})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed reviewer item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if github.reviewMarkerCalls != 0 {
		t.Fatalf("reviewMarkerCalls = %d, want none for inferred Forgejo comment-only clean noop", github.reviewMarkerCalls)
	}
	if len(github.issueCommentCalls) != 1 {
		t.Fatalf("issueCommentCalls = %#v, want exactly one comment", github.issueCommentCalls)
	}
	parsedSummary, err := forge.ParseReviewerSummary(github.issueCommentCalls[0].Body)
	if err != nil {
		t.Fatalf("ParseReviewerSummary() error = %v", err)
	}
	if len(parsedSummary.Items) != 0 {
		t.Fatalf("parsed summary items = %#v, want none", parsedSummary.Items)
	}
}

func TestProcessClaimedItemCommentOnlyApprovePolicyPublishesCleanNoopWithoutMarker(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{"looper:review"}, reviewRequests: []string{}, currentLogin: "reviewer"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings","outcome":"clean","findings":[]}`, ParseStatus: "parsed"}}}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Providers = []config.ProviderConfig{{ID: "forgejo-main", Kind: config.ProviderKindForgejo, BaseURL: "https://forgejo.example.test", TokenEnv: stringPtr("FORGEJO_TOKEN")}}
	cfg.Projects = []config.ProjectRefConfig{{ID: "project_1", Provider: "forgejo-main", Repo: "acme/looper", RepoPath: "/tmp/repos/looper"}}
	cfg.Roles.Reviewer.Discovery.Triggers.RequireReviewRequest = false
	cfg.Roles.Reviewer.Discovery.Triggers.Labels = []string{"looper:review"}
	cfg.Roles.Reviewer.Behavior.ReviewEvents.Clean = config.ReviewerReviewEventApprove
	cfg.Roles.Reviewer.Behavior.PublishMode = config.ReviewerPublishModeSummaryComment
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: false, Labels: []string{"looper:review"}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig(), ReviewEvents: cfg.Roles.Reviewer.Behavior.ReviewEvents})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed reviewer item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(github.issueCommentCalls) != 1 {
		t.Fatalf("issueCommentCalls = %#v, want exactly one comment", github.issueCommentCalls)
	}
	if github.reviewMarkerCalls != 0 {
		t.Fatalf("reviewMarkerCalls = %d, want no marker verification for comment-only Forgejo publish", github.reviewMarkerCalls)
	}
}

func TestProcessClaimedItemGlobalCommentOnlyApprovePolicyRequiresCleanMarker(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{"looper:review"}, reviewRequests: []string{}, currentLogin: "reviewer", reviewMarkerMissing: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings","outcome":"clean","findings":[]}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, LooperCLIPath: "/opt/looper/bin/looper", CommentOnlyPublish: true, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: false, Labels: []string{"looper:review"}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig(), ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove}})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed reviewer item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !contains(result.Summary, "requires an APPROVED review marker") {
		t.Fatalf("result = %#v, want retryable approve-marker-required failure", result)
	}
	if github.reviewMarkerCalls == 0 {
		t.Fatalf("reviewMarkerCalls = %d, want marker verification before global comment-only publish", github.reviewMarkerCalls)
	}
	if len(github.issueCommentCalls) != 0 {
		t.Fatalf("issueCommentCalls = %#v, want no comment before APPROVE marker verification succeeds", github.issueCommentCalls)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("agent starts = %d, want one review prompt", len(agent.starts))
	}
	prompt := agent.starts[0].Prompt
	if strings.Contains(prompt, "Comment-only publish contract") || strings.Contains(prompt, "Looper will post your final completion summary") {
		t.Fatalf("prompt = %q, want normal APPROVE review instructions instead of comment-only contract", prompt)
	}
	if !strings.Contains(prompt, "review submit acme/looper#42 --event APPROVE") {
		t.Fatalf("prompt = %q, want clean APPROVE submit instructions", prompt)
	}
}

func TestProcessClaimedItemGlobalCommentOnlyApprovePolicyVerifiesActionableNativeMarker(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{"looper:review"}, reviewRequests: []string{}, currentLogin: "reviewer", reviewMarkerOutcome: "blocking", reviewMarkerEvent: ReviewEventRequestChanges}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "internal/reviewer/runner.go: publish predicate must match review completion mode", Stdout: `__LOOPER_RESULT__={"summary":"internal/reviewer/runner.go: publish predicate must match review completion mode","outcome":"blocking"}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, LooperCLIPath: "/opt/looper/bin/looper", CommentOnlyPublish: true, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: false, Labels: []string{"looper:review"}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig(), ReviewEvents: config.ReviewerReviewEventsConfig{Clean: config.ReviewerReviewEventApprove, Blocking: config.ReviewerReviewEventRequestChanges}})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed reviewer item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success from verified native marker", result)
	}
	if github.reviewMarkerCalls == 0 {
		t.Fatalf("reviewMarkerCalls = %d, want native marker verification", github.reviewMarkerCalls)
	}
	if len(github.issueCommentCalls) != 0 || len(github.updateIssueCommentCalls) != 0 {
		t.Fatalf("issue comment mutations = (%#v, %#v), want no comment-only publish", github.issueCommentCalls, github.updateIssueCommentCalls)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("agent starts = %d, want one review prompt", len(agent.starts))
	}
	if strings.Contains(agent.starts[0].Prompt, "Comment-only publish contract") {
		t.Fatalf("prompt = %q, want native review instructions", agent.starts[0].Prompt)
	}
}

func TestProcessClaimedItemCommentOnlyPublishesBlockingOutcome(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{"looper:review"}, reviewRequests: []string{}, currentLogin: "reviewer"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "internal/reviewer/runner.go: nil worktree cleanup can deadlock reviewer retries", Stdout: `__LOOPER_RESULT__={"summary":"internal/reviewer/runner.go: nil worktree cleanup can deadlock reviewer retries","outcome":"blocking","findings":[{"title":"Nil worktree cleanup can deadlock retries","body":"Guard the cleanup path so retry resume does not block forever when the worktree record is missing.","files":["internal/reviewer/runner.go"],"disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"nil worktree path"}]}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, CommentOnlyPublish: true, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: false, Labels: []string{"looper:review"}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig()})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed reviewer item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(github.issueCommentCalls) != 1 {
		t.Fatalf("issueCommentCalls = %#v, want exactly one comment", github.issueCommentCalls)
	}
	parsedSummary, err := forge.ParseReviewerSummary(github.issueCommentCalls[0].Body)
	if err != nil {
		t.Fatalf("ParseReviewerSummary() error = %v", err)
	}
	if len(parsedSummary.Items) != 1 || parsedSummary.Items[0].Status != forge.ReviewItemStatusOpen {
		t.Fatalf("parsed summary = %#v, want one open item", parsedSummary)
	}
}

func TestProcessClaimedItemCommentOnlyUpdatesSingleReviewerSummaryCommentAndReusesIDs(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	existing := forge.NewReviewerSummary(2, []forge.ReviewItem{
		{ReviewItemID: "R-001", Status: forge.ReviewItemStatusOpen, Title: "Keep ID", Body: "Still broken.", Files: []string{"old.go"}, LastSeenRoundID: 2},
		{ReviewItemID: "R-002", Status: forge.ReviewItemStatusOpen, Title: "Replace me", Body: "Old shape.", Files: []string{"old.go"}, LastSeenRoundID: 2},
	})
	existingBody, err := renderReviewerSummaryComment(existing, "Previous summary")
	if err != nil {
		t.Fatalf("renderReviewerSummaryComment() error = %v", err)
	}
	github := &fakeGitHubGateway{labels: []string{"looper:review"}, reviewRequests: []string{}, currentLogin: "reviewer", issueComments: []map[string]any{{"id": int64(91), "body": existingBody}}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Updated findings", Stdout: `__LOOPER_RESULT__={"summary":"Updated findings","outcome":"blocking","findings":[{"review_item_id":"R-001","title":"Keep ID","body":"Still broken after the latest patch.","files":["internal/reviewer/runner.go"],"disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"still broken"},{"title":"Split replacement","body":"This issue replaces the old broad item with a narrower actionable finding.","files":["internal/reviewer/runner.go"],"supersedes":["R-002"],"disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"narrower split"}]}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, CommentOnlyPublish: true, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: false, Labels: []string{"looper:review"}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig()})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed reviewer item", claim, err)
	}
	if _, err := runner.ProcessClaimedItem(context.Background(), *claim); err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if len(github.issueCommentCalls) != 0 {
		t.Fatalf("issueCommentCalls = %#v, want no new summary comment", github.issueCommentCalls)
	}
	if len(github.updateIssueCommentCalls) != 1 || github.updateIssueCommentCalls[0].CommentID != 91 {
		t.Fatalf("updateIssueCommentCalls = %#v, want existing summary comment updated", github.updateIssueCommentCalls)
	}
	parsed, err := forge.ParseReviewerSummary(github.updateIssueCommentCalls[0].Body)
	if err != nil {
		t.Fatalf("ParseReviewerSummary() error = %v", err)
	}
	if parsed.ReviewRoundID != 3 {
		t.Fatalf("review_round_id = %d, want 3", parsed.ReviewRoundID)
	}
	if len(parsed.Items) != 3 {
		t.Fatalf("items = %#v, want 3", parsed.Items)
	}
	if parsed.Items[0].ReviewItemID != "R-001" || parsed.Items[0].Status != forge.ReviewItemStatusOpen {
		t.Fatalf("item0 = %#v, want reused open R-001", parsed.Items[0])
	}
	if parsed.Items[1].Status != forge.ReviewItemStatusSuperseded || parsed.Items[1].SupersededBy != "R-003" {
		t.Fatalf("item1 = %#v, want superseded by R-003", parsed.Items[1])
	}
	if parsed.Items[2].ReviewItemID != "R-003" || len(parsed.Items[2].Supersedes) != 1 || parsed.Items[2].Supersedes[0] != "R-002" {
		t.Fatalf("item2 = %#v, want new R-003 superseding R-002", parsed.Items[2])
	}
}

func TestBuildReviewerSummaryFromCompletionRejectsSupersededUpdatedReviewItemID(t *testing.T) {
	t.Parallel()

	existing := forge.NewReviewerSummary(2, []forge.ReviewItem{
		{ReviewItemID: "R-001", Status: forge.ReviewItemStatusOpen, Title: "Keep ID", Body: "Still broken.", LastSeenRoundID: 2},
		{ReviewItemID: "R-002", Status: forge.ReviewItemStatusOpen, Title: "Replace me", Body: "Old shape.", LastSeenRoundID: 2},
	})
	completion := reviewerCommentOnlyCompletion{
		Summary: "Updated findings",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{ReviewItemID: "R-001", Title: "Keep ID", Body: "Still broken after the latest patch.", Disposition: "must_fix", ScopeBasis: "introduced_regression", ScopeEvidence: "still broken"},
			{Title: "Replacement", Body: "This narrows the old broad issue.", Supersedes: []string{"R-001"}, Disposition: "must_fix", ScopeBasis: "introduced_regression", ScopeEvidence: "narrower"},
		},
	}

	_, err := buildReviewerSummaryFromCompletion(existing, completion)
	if err == nil || !strings.Contains(err.Error(), `supersedes updated review_item_id "R-001"`) {
		t.Fatalf("buildReviewerSummaryFromCompletion() error = %v, want updated supersedes failure", err)
	}
}

func TestValidateReviewerCommentOnlyCompletionDispositions(t *testing.T) {
	t.Parallel()

	ok, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "Must fix one",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{
				Title: "Bug", Body: "Nil deref", Disposition: "must_fix", Severity: "blocking",
				ScopeBasis: "introduced_regression", ScopeEvidence: "new path",
			},
			{
				Title: "Later", Body: "Nice refactor", Disposition: "follow_up", Severity: "nit",
				ScopeBasis: "independent_improvement", ScopeEvidence: "not required",
			},
		},
	})
	if err != nil {
		t.Fatalf("validate must_fix+follow_up: %v", err)
	}
	if len(ok.Findings) != 2 {
		t.Fatalf("findings = %#v", ok.Findings)
	}

	// follow_up alone is not actionable remote work; outcome must be clean.
	if _, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "No actionable findings",
		Outcome: "clean",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Later", Body: "Nice", Disposition: "follow_up", Severity: "nit", ScopeBasis: "independent_improvement", ScopeEvidence: "x"},
		},
	}); err != nil {
		t.Fatalf("clean with follow_up only: %v", err)
	}

	if _, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "No actionable findings",
		Outcome: "clean",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug", Body: "x", Disposition: "must_fix", Severity: "blocking", ScopeBasis: "introduced_regression", ScopeEvidence: "y"},
		},
	}); err == nil || !strings.Contains(err.Error(), "must_fix") {
		t.Fatalf("clean with must_fix error = %v", err)
	}

	// Fail-closed: missing disposition/scope fields are rejected (no default must_fix).
	if _, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "Missing fields",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug", Body: "x"},
		},
	}); err == nil || !strings.Contains(err.Error(), "requires disposition") {
		t.Fatalf("missing disposition error = %v", err)
	}
	if _, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "Missing scope",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug", Body: "x", Disposition: "must_fix"},
		},
	}); err == nil || !strings.Contains(err.Error(), "requires scopeBasis") {
		t.Fatalf("missing scopeBasis error = %v", err)
	}
	if _, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "Missing evidence",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug", Body: "x", Disposition: "must_fix", ScopeBasis: "introduced_regression"},
		},
	}); err == nil || !strings.Contains(err.Error(), "requires scopeEvidence") {
		t.Fatalf("missing scopeEvidence error = %v", err)
	}
	if _, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "Missing severity",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug", Body: "x", Disposition: "must_fix", ScopeBasis: "introduced_regression", ScopeEvidence: "y"},
		},
	}); err == nil || !strings.Contains(err.Error(), "requires severity") {
		t.Fatalf("missing severity error = %v", err)
	}
	if _, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "Arbitrary severity",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug", Body: "x", Disposition: "must_fix", Severity: "major", ScopeBasis: "introduced_regression", ScopeEvidence: "y"},
		},
	}); err == nil || !strings.Contains(err.Error(), "invalid severity") {
		t.Fatalf("arbitrary severity error = %v", err)
	}
	if _, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "Unknown scopeBasis",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug", Body: "x", Disposition: "must_fix", Severity: "blocking", ScopeBasis: "vibes", ScopeEvidence: "y"},
		},
	}); err == nil || !strings.Contains(err.Error(), "invalid scopeBasis") {
		t.Fatalf("unknown scopeBasis error = %v", err)
	}
}

func TestValidateReviewerCommentOnlyCompletionOutcomeMatchesHighestMustFixSeverity(t *testing.T) {
	t.Parallel()
	mustFix := func(severity string) reviewerCommentOnlyFindingResult {
		return reviewerCommentOnlyFindingResult{
			Title: "Bug", Body: "fix it", Disposition: "must_fix", Severity: severity,
			ScopeBasis: "introduced_regression", ScopeEvidence: "diff",
		}
	}
	if _, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "Blocking issue", Outcome: "non_blocking",
		Findings: []reviewerCommentOnlyFindingResult{mustFix("blocking")},
	}); err == nil || !strings.Contains(err.Error(), "highest must_fix severity") {
		t.Fatalf("blocking finding with non_blocking outcome error = %v", err)
	}
	if _, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "Nit only", Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{mustFix("nit")},
	}); err == nil || !strings.Contains(err.Error(), "highest must_fix severity") {
		t.Fatalf("nit finding with blocking outcome error = %v", err)
	}
	if _, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "Non-blocking only", Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{mustFix("non_blocking")},
	}); err == nil || !strings.Contains(err.Error(), "highest must_fix severity") {
		t.Fatalf("non_blocking finding with blocking outcome error = %v", err)
	}
	if _, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "Blocking issue", Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{mustFix("blocking"), mustFix("nit")},
	}); err != nil {
		t.Fatalf("mixed blocking+nit with blocking outcome: %v", err)
	}
	if _, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "Nit only", Outcome: "non_blocking",
		Findings: []reviewerCommentOnlyFindingResult{mustFix("nit")},
	}); err != nil {
		t.Fatalf("nit with non_blocking outcome: %v", err)
	}
	if _, err := validateReviewerCommentOnlyCompletion(reviewerCommentOnlyCompletion{
		Summary: "Need human", Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{{
			Title: "Ambiguous", Body: "unclear", Disposition: "needs_human", Severity: "blocking",
			ScopeBasis: "ambiguous_intent", ScopeEvidence: "PR non-goals",
		}},
	}); err != nil {
		t.Fatalf("needs_human-only: %v", err)
	}
}

func TestBuildReviewerSummaryFromCompletionFiltersFollowUpAndNeedsHuman(t *testing.T) {
	t.Parallel()

	existing := forge.NewReviewerSummary(1, nil)
	completion := reviewerCommentOnlyCompletion{
		Summary: "Mixed",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Must", Body: "fix", Disposition: "must_fix", ScopeBasis: "introduced_regression", ScopeEvidence: "e1", Files: []string{"a.go"}},
			{Title: "Later", Body: "follow", Disposition: "follow_up", ScopeBasis: "independent_improvement", ScopeEvidence: "e2"},
			{Title: "Human", Body: "ask", Disposition: "needs_human", ScopeBasis: "ambiguous_intent", ScopeEvidence: "e3"},
		},
	}
	summary, err := buildReviewerSummaryFromCompletion(existing, completion)
	if err != nil {
		t.Fatalf("buildReviewerSummaryFromCompletion: %v", err)
	}
	if len(summary.Items) != 1 || summary.Items[0].Title != "Must" {
		t.Fatalf("items = %#v, want only must_fix remote item", summary.Items)
	}
}

func TestBuildReviewerSummaryFromCompletionKeepsNeedsHumanReferencedOpenItems(t *testing.T) {
	t.Parallel()

	existing := forge.NewReviewerSummary(1, []forge.ReviewItem{
		{ReviewItemID: "R-001", Status: forge.ReviewItemStatusOpen, Title: "Ambiguous scope", Body: "Is this in PR scope?", Files: []string{"a.go"}, LastSeenRoundID: 1},
		{ReviewItemID: "R-002", Status: forge.ReviewItemStatusOpen, Title: "Stale leftover", Body: "Truly gone.", LastSeenRoundID: 1},
		{ReviewItemID: "R-003", Status: forge.ReviewItemStatusOpen, Title: "Later rename", Body: "Follow-up only.", LastSeenRoundID: 1},
	})
	completion := reviewerCommentOnlyCompletion{
		Summary: "Mixed publish and park",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Must", Body: "fix", Disposition: "must_fix", ScopeBasis: "introduced_regression", ScopeEvidence: "e1", Files: []string{"b.go"}},
			{ReviewItemID: "R-001", Title: "Ambiguous scope", Body: "Still undecided.", Disposition: "needs_human", ScopeBasis: "ambiguous_intent", ScopeEvidence: "PR non-goals"},
			{ReviewItemID: "R-003", Title: "Later rename", Body: "Not this round.", Disposition: "follow_up", ScopeBasis: "independent_improvement", ScopeEvidence: "style"},
		},
	}
	summary, err := buildReviewerSummaryFromCompletion(existing, completion)
	if err != nil {
		t.Fatalf("buildReviewerSummaryFromCompletion: %v", err)
	}
	byID := map[string]forge.ReviewItem{}
	for _, item := range summary.Items {
		byID[item.ReviewItemID] = item
	}
	if len(byID) != 4 {
		t.Fatalf("items = %#v, want 4", summary.Items)
	}
	kept := byID["R-001"]
	if kept.Status != forge.ReviewItemStatusOpen || kept.Title != "Ambiguous scope" || kept.Body != "Is this in PR scope?" {
		t.Fatalf("R-001 = %#v, want original open needs_human-referenced item", kept)
	}
	if kept.LastSeenRoundID != 2 {
		t.Fatalf("R-001 LastSeenRoundID = %d, want 2", kept.LastSeenRoundID)
	}
	if byID["R-002"].Status != forge.ReviewItemStatusResolved {
		t.Fatalf("R-002 = %#v, want omitted leftover resolved", byID["R-002"])
	}
	if byID["R-003"].Status != forge.ReviewItemStatusResolved {
		t.Fatalf("R-003 = %#v, want follow_up-referenced item resolved", byID["R-003"])
	}
	var publishedOpen []forge.ReviewItem
	for _, item := range summary.Items {
		if item.Status == forge.ReviewItemStatusOpen {
			publishedOpen = append(publishedOpen, item)
		}
	}
	if len(publishedOpen) != 2 {
		t.Fatalf("open items = %#v, want R-001 plus new must_fix", publishedOpen)
	}
	foundMust := false
	for _, item := range publishedOpen {
		if item.Title == "Must" {
			foundMust = true
		}
		if strings.Contains(item.Body, "Still undecided") {
			t.Fatalf("needs_human content leaked into remote item: %#v", item)
		}
	}
	if !foundMust {
		t.Fatalf("open items missing must_fix: %#v", publishedOpen)
	}
}

func TestPublishableCommentOnlyFindingsFiltersNonMustFix(t *testing.T) {
	t.Parallel()
	got := publishableCommentOnlyFindings([]reviewerCommentOnlyFindingResult{
		{Title: "a", Disposition: "must_fix"},
		{Title: "b", Disposition: "follow_up"},
		{Title: "c", Disposition: "needs_human"},
		{Title: "d"}, // empty disposition is not publishable
	})
	if len(got) != 1 || got[0].Title != "a" {
		t.Fatalf("publishable = %#v", got)
	}
}

func TestCommentOnlyPublishVisibleSummaryOmitsSuppressedFindings(t *testing.T) {
	t.Parallel()
	completion := reviewerCommentOnlyCompletion{
		Summary: "Must fix nil deref. Also needs human on scope and a follow-up rename.",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Nil deref", Body: "Guard the pointer", Disposition: "must_fix", ScopeBasis: "introduced_regression", ScopeEvidence: "new path"},
			{Title: "Rename", Body: "later", Disposition: "follow_up", ScopeBasis: "independent_improvement", ScopeEvidence: "style"},
			{Title: "Scope", Body: "unclear", Disposition: "needs_human", ScopeBasis: "ambiguous_intent", ScopeEvidence: "PR non-goals"},
		},
	}
	visible := commentOnlyPublishVisibleSummary(completion)
	if !strings.Contains(visible, "Nil deref") {
		t.Fatalf("visible missing must_fix: %q", visible)
	}
	for _, banned := range []string{"needs human", "follow-up", "Rename", "Scope", "unclear", "PR non-goals"} {
		if strings.Contains(visible, banned) {
			t.Fatalf("visible smuggles %q: %q", banned, visible)
		}
	}
}

func TestCommentOnlyPublishVisibleSummaryRedactsFollowUpFromCleanSummary(t *testing.T) {
	t.Parallel()
	completion := reviewerCommentOnlyCompletion{
		Summary: "No actionable findings. Follow-up: rename the helper later.",
		Outcome: "clean",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Rename helper", Body: "later", Disposition: "follow_up", ScopeBasis: "independent_improvement", ScopeEvidence: "style"},
		},
	}
	visible := commentOnlyPublishVisibleSummary(completion)
	if visible != "No actionable findings" {
		t.Fatalf("visible = %q, want fixed clean summary without follow_up details", visible)
	}
}

func TestParseReviewerNativeCompletionFindingsAndLegacy(t *testing.T) {
	t.Parallel()
	// Legacy summary-only remains valid.
	legacy, err := parseReviewerNativeCompletion(AgentResult{Stdout: `__LOOPER_RESULT__={"summary":"posted review"}`, Summary: "posted review", ParseStatus: "parsed"})
	if err != nil {
		t.Fatalf("legacy native: %v", err)
	}
	if legacy.Summary != "posted review" || len(legacy.Findings) != 0 {
		t.Fatalf("legacy = %#v", legacy)
	}
	// Findings present → fail-closed validation.
	if _, err := parseReviewerNativeCompletion(AgentResult{Stdout: `__LOOPER_RESULT__={"summary":"x","outcome":"blocking","findings":[{"title":"t","body":"b"}]}`}); err == nil {
		t.Fatal("want disposition rejection when findings lack contract fields")
	}
	ok, err := parseReviewerNativeCompletion(AgentResult{Stdout: `__LOOPER_RESULT__={"summary":"Need human","outcome":"blocking","findings":[{"title":"Ambiguous","body":"Unclear","disposition":"needs_human","severity":"blocking","scopeBasis":"ambiguous_intent","scopeEvidence":"AGENTS.md rule X","path":"a.go","line":1}]}`})
	if err != nil {
		t.Fatalf("needs_human native: %v", err)
	}
	if !commentOnlyCompletionHasNeedsHuman(ok) {
		t.Fatalf("want needs_human: %#v", ok)
	}
	if _, err := parseReviewerNativeCompletion(AgentResult{Stdout: `__LOOPER_RESULT__={"summary":"Blocking issue remains","outcome":"non_blocking","findings":[{"title":"Bug","body":"fix it","disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"diff","path":"a.go","line":1}]}`}); err == nil || !strings.Contains(err.Error(), "highest must_fix severity") {
		t.Fatalf("native blocking finding with non_blocking outcome error = %v, want publication-contract rejection", err)
	}

}

func TestCommentOnlyNeedsHumanQuestionNamesAuthority(t *testing.T) {
	t.Parallel()
	q := commentOnlyNeedsHumanQuestion(reviewerCommentOnlyCompletion{
		Findings: []reviewerCommentOnlyFindingResult{
			{
				Title: "Out of scope?", Body: "Expanding API surface", Disposition: "needs_human",
				ScopeBasis: "ambiguous_intent", ScopeEvidence: "PR non-goals exclude API expansion",
			},
		},
	})
	for _, want := range []string{"before unpause", "PR non-goals", "Out of scope?", "Continue resumes against current evidence"} {
		if !strings.Contains(q, want) {
			t.Fatalf("question missing %q: %s", want, q)
		}
	}
	if strings.Contains(q, "Resume against current evidence, or stop") && !strings.Contains(q, "before unpause") {
		t.Fatalf("legacy handoff text without authority: %s", q)
	}
}

func TestNativeMustFixReviewMarkerActionableRequiresInlineComments(t *testing.T) {
	t.Parallel()
	if nativeMustFixReviewMarkerActionable(ReviewMarkerResult{Found: true, Event: ReviewEventComment, Outcome: "actionable"}, 1) {
		t.Fatal("body-only COMMENT must not count as published must_fix")
	}
	if nativeMustFixReviewMarkerActionable(ReviewMarkerResult{Found: true, Event: ReviewEventRequestChanges, Outcome: "blocking"}, 1) {
		t.Fatal("body-only REQUEST_CHANGES must not count as published must_fix")
	}
	if !nativeMustFixReviewMarkerActionable(ReviewMarkerResult{Found: true, Event: ReviewEventComment, Outcome: "actionable", InlineCommentBodies: []string{"fix it"}}, 1) {
		t.Fatal("COMMENT with inline comments must count as published must_fix")
	}
	if nativeMustFixReviewMarkerActionable(ReviewMarkerResult{Found: true, Event: ReviewEventComment, Outcome: "blocking", InlineCommentBodies: []string{"fix first"}}, 2) {
		t.Fatal("one inline comment must not cover two must_fix findings")
	}
	if nativeMustFixReviewMarkerActionable(ReviewMarkerResult{Found: true, Event: ReviewEventComment, Outcome: "blocking", InlineCommentBodies: []string{"fix first", ""}}, 2) {
		t.Fatal("empty inline bodies must not count toward must_fix coverage")
	}
	if !nativeMustFixReviewMarkerActionable(ReviewMarkerResult{Found: true, Event: ReviewEventComment, Outcome: "blocking", InlineCommentBodies: []string{"fix first", "fix second"}}, 2) {
		t.Fatal("two inline comments must cover two must_fix findings")
	}
	if nativeMustFixReviewMarkerActionable(ReviewMarkerResult{}, 1) {
		t.Fatal("missing marker must not count as published must_fix")
	}
}

func TestRunPublishStepNativeMustFixRequiresActionableMarker(t *testing.T) {
	t.Parallel()
	completion := reviewerCommentOnlyCompletion{
		Summary: "Blocking must_fix only",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug", Body: "fix it", Disposition: "must_fix", Severity: "blocking", ScopeBasis: "introduced_regression", ScopeEvidence: "diff", Files: []string{"a.go"}},
		},
	}
	payload, err := json.Marshal(completion)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, tc := range []struct {
		name         string
		markerEvent  ReviewEvent
		markerOut    string
		inlineBodies []string
		wantPublish  bool
	}{
		{name: "body_only_comment", markerEvent: ReviewEventComment, markerOut: "actionable"},
		{name: "body_only_request_changes", markerEvent: ReviewEventRequestChanges, markerOut: "blocking"},
		{name: "inline_comment", markerEvent: ReviewEventComment, markerOut: "blocking", inlineBodies: []string{"fix it"}, wantPublish: true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			repo := "acme/looper"
			prNumber := int64(42)
			nowISO := fixture.nowISO()
			target := "pr:acme/looper:42"
			metadata := `{"loop":{"iterationCount":0}}`
			loop := storage.LoopRecord{
				ID: "loop_native_must_fix_" + tc.name, Seq: 118, ProjectID: "project_1", Type: "reviewer",
				TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
				Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
			}
			if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
				t.Fatalf("upsert loop: %v", err)
			}
			cfg, err := config.DefaultConfig(t.TempDir())
			if err != nil {
				t.Fatalf("DefaultConfig: %v", err)
			}
			runner := New(Options{
				DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{
					reviewMarkerMissing:             false,
					reviewMarkerEvent:               tc.markerEvent,
					reviewMarkerOutcome:             tc.markerOut,
					reviewMarkerInlineCommentBodies: tc.inlineBodies,
				}, Git: &fakeGitGateway{},
				Logger: fixture.logger, Now: fixture.now,
				LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
			})
			project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
			if err != nil || project == nil {
				t.Fatalf("Projects.GetByID() = (%#v, %v)", project, err)
			}
			_, err = runner.runPublishStep(context.Background(), stepInput{
				Project: *project, Loop: loop, Run: storage.RunRecord{ID: "run_native_must_fix_" + tc.name},
				Repo: repo, PRNumber: prNumber,
				Checkpoint: reviewerCheckpoint{
					Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", HeadSHA: "abc123", State: "OPEN"},
					Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
					PendingReview: &pendingReviewCheckpoint{
						HeadSHA: "abc123", IdempotencyKey: "idem-native-must-fix-" + tc.name, Event: reviewEventAgentNative,
						Summary: completion.Summary, Outcome: completion.Outcome, ReviewerSummaryJSON: string(payload),
					},
				},
			})
			updated, getErr := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
			if getErr != nil || updated == nil {
				t.Fatalf("after publish = (%#v, %v)", updated, getErr)
			}
			gotHead, _ := stringFromAny(parseJSONObject(updated.MetadataJSON)["lastPublishedHeadSha"])
			if tc.wantPublish {
				if err != nil {
					t.Fatalf("runPublishStep() error = %v, want published native must_fix", err)
				}
				if loops.ReviewerPublishCount(updated.MetadataJSON) != 1 || gotHead != "abc123" {
					t.Fatalf("published loop = %#v head=%q, want count=1 lastPublishedHeadSha=abc123", updated, gotHead)
				}
				return
			}
			var le *loopError
			if !errors.As(err, &le) || le.kind != FailureRetryableAfterResume {
				t.Fatalf("runPublishStep() error = %v, want retryable loopError", err)
			}
			if !strings.Contains(err.Error(), "actionable review marker") {
				t.Fatalf("error = %q, want actionable-marker publication failure", err)
			}
			if loops.ReviewerPublishCount(updated.MetadataJSON) != 0 || gotHead != "" {
				t.Fatalf("unpublished loop consumed budget/head: count=%d head=%q meta=%v", loops.ReviewerPublishCount(updated.MetadataJSON), gotHead, updated.MetadataJSON)
			}
		})
	}
}

func TestRunPublishStepNativeMustFixRequiresInlineCommentPerFinding(t *testing.T) {
	t.Parallel()
	completion := reviewerCommentOnlyCompletion{
		Summary: "Two blocking must_fix findings",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug one", Body: "fix first", Disposition: "must_fix", Severity: "blocking", ScopeBasis: "introduced_regression", ScopeEvidence: "diff", Files: []string{"a.go"}},
			{Title: "Bug two", Body: "fix second", Disposition: "must_fix", Severity: "blocking", ScopeBasis: "introduced_regression", ScopeEvidence: "diff", Files: []string{"b.go"}},
		},
	}
	payload, err := json.Marshal(completion)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, tc := range []struct {
		name         string
		inlineBodies []string
		wantPublish  bool
	}{
		{name: "one_inline", inlineBodies: []string{"fix first"}},
		{name: "two_inlines", inlineBodies: []string{"fix first", "fix second"}, wantPublish: true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			repo := "acme/looper"
			prNumber := int64(42)
			nowISO := fixture.nowISO()
			target := "pr:acme/looper:42"
			metadata := `{"loop":{"iterationCount":0}}`
			loop := storage.LoopRecord{
				ID: "loop_native_must_fix_multi_" + tc.name, Seq: 119, ProjectID: "project_1", Type: "reviewer",
				TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
				Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
			}
			if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
				t.Fatalf("upsert loop: %v", err)
			}
			cfg, err := config.DefaultConfig(t.TempDir())
			if err != nil {
				t.Fatalf("DefaultConfig: %v", err)
			}
			runner := New(Options{
				DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{
					reviewMarkerMissing:             false,
					reviewMarkerEvent:               ReviewEventComment,
					reviewMarkerOutcome:             "blocking",
					reviewMarkerInlineCommentBodies: tc.inlineBodies,
				}, Git: &fakeGitGateway{},
				Logger: fixture.logger, Now: fixture.now,
				LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
			})
			project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
			if err != nil || project == nil {
				t.Fatalf("Projects.GetByID() = (%#v, %v)", project, err)
			}
			_, err = runner.runPublishStep(context.Background(), stepInput{
				Project: *project, Loop: loop, Run: storage.RunRecord{ID: "run_native_must_fix_multi_" + tc.name},
				Repo: repo, PRNumber: prNumber,
				Checkpoint: reviewerCheckpoint{
					Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", HeadSHA: "abc123", State: "OPEN"},
					Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
					PendingReview: &pendingReviewCheckpoint{
						HeadSHA: "abc123", IdempotencyKey: "idem-native-must-fix-multi-" + tc.name, Event: reviewEventAgentNative,
						Summary: completion.Summary, Outcome: completion.Outcome, ReviewerSummaryJSON: string(payload),
					},
				},
			})
			updated, getErr := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
			if getErr != nil || updated == nil {
				t.Fatalf("after publish = (%#v, %v)", updated, getErr)
			}
			gotHead, _ := stringFromAny(parseJSONObject(updated.MetadataJSON)["lastPublishedHeadSha"])
			if tc.wantPublish {
				if err != nil {
					t.Fatalf("runPublishStep() error = %v, want published native must_fix", err)
				}
				if loops.ReviewerPublishCount(updated.MetadataJSON) != 1 || gotHead != "abc123" {
					t.Fatalf("published loop = %#v head=%q, want count=1 lastPublishedHeadSha=abc123", updated, gotHead)
				}
				return
			}
			var le *loopError
			if !errors.As(err, &le) || le.kind != FailureRetryableAfterResume {
				t.Fatalf("runPublishStep() error = %v, want retryable loopError", err)
			}
			if !strings.Contains(err.Error(), "inline comments for every finding") {
				t.Fatalf("error = %q, want per-finding inline publication failure", err)
			}
			if loops.ReviewerPublishCount(updated.MetadataJSON) != 0 || gotHead != "" {
				t.Fatalf("partial marker consumed budget/head: count=%d head=%q meta=%v", loops.ReviewerPublishCount(updated.MetadataJSON), gotHead, updated.MetadataJSON)
			}
		})
	}
}

func TestRunReviewStepNeedsHumanRecordsPublishedMarkerBeforePark(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		markerMissing bool
		markerEvent   ReviewEvent
		markerOutcome string
		inlineBodies  []string
		wantHold      bool
		wantRetryable bool
		wantPublish   int
		wantLastHead  string
	}{
		{name: "with_marker", markerMissing: false, markerEvent: ReviewEventComment, markerOutcome: "blocking", inlineBodies: []string{"fix it"}, wantHold: true, wantPublish: 1, wantLastHead: "abc123"},
		{name: "without_marker", markerMissing: true, wantRetryable: true, wantPublish: 0},
		{name: "clean_marker", markerMissing: false, markerEvent: ReviewEventApprove, markerOutcome: "clean", wantRetryable: true, wantPublish: 0},
		{name: "body_only_comment", markerMissing: false, markerEvent: ReviewEventComment, markerOutcome: "actionable", wantRetryable: true, wantPublish: 0},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			repo := "acme/looper"
			prNumber := int64(42)
			nowISO := fixture.nowISO()
			target := "pr:acme/looper:42"
			metadata := `{"loop":{"iterationCount":0}}`
			loop := storage.LoopRecord{
				ID: "loop_needs_human_pub_" + tc.name, Seq: 94, ProjectID: "project_1", Type: "reviewer",
				TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
				Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
			}
			if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
				t.Fatalf("upsert loop: %v", err)
			}
			cfg, err := config.DefaultConfig(t.TempDir())
			if err != nil {
				t.Fatalf("DefaultConfig: %v", err)
			}
			cfg.HITL.Enabled = false
			stdout := `__LOOPER_RESULT__={"summary":"Mixed must_fix and needs_human","outcome":"blocking","findings":[{"title":"Bug","body":"fix it","disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"diff","path":"a.go","line":1},{"title":"Ambiguous","body":"unclear","disposition":"needs_human","severity":"blocking","scopeBasis":"ambiguous_intent","scopeEvidence":"PR non-goals","path":"b.go","line":2}]}`
			agent := &fakeAgentExecutor{results: []AgentResult{{
				Status: "completed", Summary: "Mixed must_fix and needs_human", Stdout: stdout, ParseStatus: "parsed",
			}}}
			github := &fakeGitHubGateway{
				reviewMarkerMissing:             tc.markerMissing,
				reviewMarkerEvent:               tc.markerEvent,
				reviewMarkerOutcome:             tc.markerOutcome,
				reviewMarkerInlineCommentBodies: tc.inlineBodies,
			}
			runner := New(Options{
				DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{},
				AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now,
				LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
			})
			project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
			if err != nil || project == nil {
				t.Fatalf("Projects.GetByID() = (%#v, %v)", project, err)
			}
			_, err = runner.runReviewStep(context.Background(), stepInput{
				Project:  *project,
				Loop:     loop,
				Run:      storage.RunRecord{ID: "run_needs_human_pub_" + tc.name},
				Repo:     repo,
				PRNumber: prNumber,
				Checkpoint: reviewerCheckpoint{
					Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
					Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
					Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "feature/review-me", PreparedAt: nowISO},
				},
			})
			updated, getErr := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
			if getErr != nil || updated == nil {
				t.Fatalf("after step = (%#v, %v)", updated, getErr)
			}
			if got := loops.ReviewerPublishCount(updated.MetadataJSON); got != tc.wantPublish {
				t.Fatalf("ReviewerPublishCount = %d, want %d", got, tc.wantPublish)
			}
			gotHead, _ := stringFromAny(parseJSONObject(updated.MetadataJSON)["lastPublishedHeadSha"])
			if gotHead != tc.wantLastHead {
				t.Fatalf("lastPublishedHeadSha = %q, want %q", gotHead, tc.wantLastHead)
			}
			if tc.wantRetryable {
				var le *loopError
				if !errors.As(err, &le) || le.kind != FailureRetryableAfterResume {
					t.Fatalf("runReviewStep() error = %v, want retryable loopError", err)
				}
				if !strings.Contains(err.Error(), "actionable review marker") {
					t.Fatalf("error = %q, want actionable-marker publication failure", err)
				}
				if loops.IsReviewScopeHumanHold(*updated) || loops.HasPendingReviewScopeHuman(*updated) {
					t.Fatalf("must not park mixed must_fix without an actionable marker: meta=%s", derefString(updated.MetadataJSON))
				}
				return
			}
			var hold *holdSkipError
			if !errors.As(err, &hold) {
				t.Fatalf("runReviewStep() error = %v, want holdSkipError", err)
			}
			if !tc.wantHold || !loops.IsReviewScopeHumanHold(*updated) {
				t.Fatalf("after park = (%#v, %v), want scope hold", updated, err)
			}
		})
	}
}

func TestRunReviewStepMixedNativeMustFixRequiresInlineCommentPerFinding(t *testing.T) {
	t.Parallel()
	stdout := `__LOOPER_RESULT__={"summary":"Mixed must_fix and needs_human","outcome":"blocking","findings":[{"title":"Bug one","body":"fix first","disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"diff","path":"a.go","line":1},{"title":"Bug two","body":"fix second","disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"diff","path":"c.go","line":3},{"title":"Ambiguous","body":"unclear","disposition":"needs_human","severity":"blocking","scopeBasis":"ambiguous_intent","scopeEvidence":"PR non-goals","path":"b.go","line":2}]}`
	for _, tc := range []struct {
		name          string
		inlineBodies  []string
		wantHold      bool
		wantRetryable bool
		wantPublish   int
		wantLastHead  string
	}{
		{name: "one_inline", inlineBodies: []string{"fix first"}, wantRetryable: true, wantPublish: 0},
		{name: "two_inlines", inlineBodies: []string{"fix first", "fix second"}, wantHold: true, wantPublish: 1, wantLastHead: "abc123"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			repo := "acme/looper"
			prNumber := int64(42)
			nowISO := fixture.nowISO()
			target := "pr:acme/looper:42"
			metadata := `{"loop":{"iterationCount":0}}`
			loop := storage.LoopRecord{
				ID: "loop_mixed_must_fix_multi_" + tc.name, Seq: 120, ProjectID: "project_1", Type: "reviewer",
				TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
				Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
			}
			if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
				t.Fatalf("upsert loop: %v", err)
			}
			cfg, err := config.DefaultConfig(t.TempDir())
			if err != nil {
				t.Fatalf("DefaultConfig: %v", err)
			}
			cfg.HITL.Enabled = false
			runner := New(Options{
				DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{
					reviewMarkerMissing:             false,
					reviewMarkerEvent:               ReviewEventComment,
					reviewMarkerOutcome:             "blocking",
					reviewMarkerInlineCommentBodies: tc.inlineBodies,
				}, Git: &fakeGitGateway{},
				AgentExecutor: &fakeAgentExecutor{results: []AgentResult{{
					Status: "completed", Summary: "Mixed must_fix and needs_human", Stdout: stdout, ParseStatus: "parsed",
				}}},
				Logger: fixture.logger, Now: fixture.now,
				LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
			})
			project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
			if err != nil || project == nil {
				t.Fatalf("Projects.GetByID() = (%#v, %v)", project, err)
			}
			_, err = runner.runReviewStep(context.Background(), stepInput{
				Project:  *project,
				Loop:     loop,
				Run:      storage.RunRecord{ID: "run_mixed_must_fix_multi_" + tc.name},
				Repo:     repo,
				PRNumber: prNumber,
				Checkpoint: reviewerCheckpoint{
					Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
					Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
					Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "feature/review-me", PreparedAt: nowISO},
				},
			})
			updated, getErr := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
			if getErr != nil || updated == nil {
				t.Fatalf("after step = (%#v, %v)", updated, getErr)
			}
			if got := loops.ReviewerPublishCount(updated.MetadataJSON); got != tc.wantPublish {
				t.Fatalf("ReviewerPublishCount = %d, want %d", got, tc.wantPublish)
			}
			gotHead, _ := stringFromAny(parseJSONObject(updated.MetadataJSON)["lastPublishedHeadSha"])
			if gotHead != tc.wantLastHead {
				t.Fatalf("lastPublishedHeadSha = %q, want %q", gotHead, tc.wantLastHead)
			}
			if tc.wantRetryable {
				var le *loopError
				if !errors.As(err, &le) || le.kind != FailureRetryableAfterResume {
					t.Fatalf("runReviewStep() error = %v, want retryable loopError", err)
				}
				if !strings.Contains(err.Error(), "actionable review marker") {
					t.Fatalf("error = %q, want actionable-marker publication failure", err)
				}
				if loops.IsReviewScopeHumanHold(*updated) || loops.HasPendingReviewScopeHuman(*updated) {
					t.Fatalf("must not park mixed must_fix with a partial marker: meta=%s", derefString(updated.MetadataJSON))
				}
				return
			}
			var hold *holdSkipError
			if !errors.As(err, &hold) {
				t.Fatalf("runReviewStep() error = %v, want holdSkipError", err)
			}
			if !tc.wantHold || !loops.IsReviewScopeHumanHold(*updated) {
				t.Fatalf("after park = (%#v, %v), want scope hold", updated, err)
			}
		})
	}
}

func TestRunReviewStepMixedNativePersistsCompletionBeforePublishedHead(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	metadata := `{"loop":{"iterationCount":0}}`
	loop := storage.LoopRecord{
		ID: "loop_mixed_persist_before_publish", Seq: 97, ProjectID: "project_1", Type: "reviewer",
		TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
		Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("upsert loop: %v", err)
	}
	runID := "run_mixed_persist_before_publish"
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{
		ID: runID, LoopID: loop.ID, Status: "running", StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO,
	}); err != nil {
		t.Fatalf("Runs.Upsert: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.HITL.Enabled = false
	stdout := `__LOOPER_RESULT__={"summary":"Mixed must_fix and needs_human","outcome":"blocking","findings":[{"title":"Bug","body":"fix it","disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"diff","path":"a.go","line":1},{"title":"Ambiguous","body":"unclear","disposition":"needs_human","severity":"blocking","scopeBasis":"ambiguous_intent","scopeEvidence":"PR non-goals","path":"b.go","line":2}]}`
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{
			reviewMarkerMissing: false, reviewMarkerEvent: ReviewEventComment, reviewMarkerOutcome: "blocking",
			reviewMarkerInlineCommentBodies: []string{"fix it"},
		}, Git: &fakeGitGateway{},
		AgentExecutor: &fakeAgentExecutor{results: []AgentResult{{
			Status: "completed", Summary: "Mixed must_fix and needs_human", Stdout: stdout, ParseStatus: "parsed",
		}}},
		Logger: fixture.logger, Now: fixture.now,
		LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
	})
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v)", project, err)
	}
	_, err = runner.runReviewStep(context.Background(), stepInput{
		Project: *project, Loop: loop, Run: storage.RunRecord{ID: runID, LoopID: loop.ID},
		Repo: repo, PRNumber: prNumber,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "feature/review-me", PreparedAt: nowISO},
		},
	})
	var hold *holdSkipError
	if !errors.As(err, &hold) {
		t.Fatalf("runReviewStep() error = %v, want holdSkipError", err)
	}
	updated, getErr := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if getErr != nil || updated == nil {
		t.Fatalf("after step = (%#v, %v)", updated, getErr)
	}
	gotHead, _ := stringFromAny(parseJSONObject(updated.MetadataJSON)["lastPublishedHeadSha"])
	if gotHead != "abc123" {
		t.Fatalf("lastPublishedHeadSha = %q, want abc123", gotHead)
	}
	if !loops.IsReviewScopeHumanHold(*updated) {
		t.Fatalf("after park = %#v, want scope hold", updated)
	}
	persisted, err := fixture.repos.Runs.GetByID(context.Background(), runID)
	if err != nil || persisted == nil {
		t.Fatalf("Runs.GetByID = (%#v, %v)", persisted, err)
	}
	checkpoint := parseCheckpoint(persisted.CheckpointJSON)
	if checkpoint.PendingReview == nil || !strings.Contains(checkpoint.PendingReview.ReviewerSummaryJSON, `"disposition":"needs_human"`) {
		t.Fatalf("checkpoint pending = %#v, want persisted native completion with needs_human before lastPublishedHeadSha", checkpoint.PendingReview)
	}
}

func TestRunReviewStepNeedsHumanDoesNotRecountAlreadyPublishedHead(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	metadata := `{"loop":{"iterationCount":0},"lastPublishedHeadSha":"abc123"}`
	loop := storage.LoopRecord{
		ID: "loop_needs_human_recount", Seq: 96, ProjectID: "project_1", Type: "reviewer",
		TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
		Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("upsert loop: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.HITL.Enabled = false
	stdout := `__LOOPER_RESULT__={"summary":"Mixed must_fix and needs_human","outcome":"blocking","findings":[{"title":"Bug","body":"fix it","disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"diff","path":"a.go","line":1},{"title":"Ambiguous","body":"unclear","disposition":"needs_human","severity":"blocking","scopeBasis":"ambiguous_intent","scopeEvidence":"PR non-goals","path":"b.go","line":2}]}`
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{
			reviewMarkerMissing: false, reviewMarkerEvent: ReviewEventComment, reviewMarkerOutcome: "blocking",
			reviewMarkerInlineCommentBodies: []string{"fix it"},
		}, Git: &fakeGitGateway{},
		AgentExecutor: &fakeAgentExecutor{results: []AgentResult{{
			Status: "completed", Summary: "Mixed must_fix and needs_human", Stdout: stdout, ParseStatus: "parsed",
		}}},
		Logger: fixture.logger, Now: fixture.now,
		LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
	})
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v)", project, err)
	}
	_, err = runner.runReviewStep(context.Background(), stepInput{
		Project: *project, Loop: loop, Run: storage.RunRecord{ID: "run_needs_human_recount"},
		Repo: repo, PRNumber: prNumber,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "feature/review-me", PreparedAt: nowISO},
		},
	})
	var hold *holdSkipError
	if !errors.As(err, &hold) {
		t.Fatalf("runReviewStep() error = %v, want holdSkipError", err)
	}
	updated, getErr := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if getErr != nil || updated == nil {
		t.Fatalf("after retry = (%#v, %v)", updated, getErr)
	}
	if got := loops.ReviewerPublishCount(updated.MetadataJSON); got != 0 {
		t.Fatalf("ReviewerPublishCount = %d, want 0 on already-published head", got)
	}
	if !loops.IsReviewScopeHumanHold(*updated) {
		t.Fatalf("after retry = %#v, want recovered scope hold", updated)
	}
}

func TestRunReviewStepNeedsHumanAtCapBudgetHoldOnlyNoStackedScope(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	// One publish away from cap=1 so recording the marker parks budget.
	metadata := `{"loop":{"iterationCount":0}}`
	loop := storage.LoopRecord{
		ID: "loop_needs_human_at_cap", Seq: 95, ProjectID: "project_1", Type: "reviewer",
		TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
		Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("upsert loop: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.HITL.Enabled = true
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 1
	stdout := `__LOOPER_RESULT__={"summary":"Mixed must_fix and needs_human","outcome":"blocking","findings":[{"title":"Bug","body":"fix it","disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"diff","path":"a.go","line":1},{"title":"Ambiguous","body":"unclear","disposition":"needs_human","severity":"blocking","scopeBasis":"ambiguous_intent","scopeEvidence":"PR non-goals","path":"b.go","line":2}]}`
	agent := &fakeAgentExecutor{results: []AgentResult{{
		Status: "completed", Summary: "Mixed must_fix and needs_human", Stdout: stdout, ParseStatus: "parsed",
	}}}
	github := &fakeGitHubGateway{
		reviewMarkerMissing:             false,
		reviewMarkerEvent:               ReviewEventComment,
		reviewMarkerOutcome:             "blocking",
		reviewMarkerInlineCommentBodies: []string{"fix it"},
	}
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{},
		AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now,
		LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
	})
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v)", project, err)
	}
	_, err = runner.runReviewStep(context.Background(), stepInput{
		Project:  *project,
		Loop:     loop,
		Run:      storage.RunRecord{ID: "run_needs_human_at_cap"},
		Repo:     repo,
		PRNumber: prNumber,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "feature/review-me", PreparedAt: nowISO},
		},
	})
	var hold *holdSkipError
	if !errors.As(err, &hold) {
		t.Fatalf("runReviewStep() error = %v, want holdSkipError", err)
	}
	if !strings.Contains(hold.summary, "budget") {
		t.Fatalf("hold summary = %q, want budget hold (not stacked scope ask)", hold.summary)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || updated == nil {
		t.Fatalf("after park = (%#v, %v)", updated, err)
	}
	if !loops.IsReviewFixBudgetHold(*updated) {
		t.Fatalf("want budget hold only: status=%s meta=%s", updated.Status, derefString(updated.MetadataJSON))
	}
	if loops.IsReviewScopeHumanHold(*updated) {
		t.Fatalf("must not stack active scope hold under budget: %#v", updated)
	}
	if !loops.HasPendingReviewScopeHuman(*updated) {
		t.Fatalf("want pending scope evidence under budget hold: meta=%s", derefString(updated.MetadataJSON))
	}
	ask, ok := loops.ReadHITLAsk(updated.MetadataJSON)
	if !ok || !loops.IsReviewFixBudgetAsk(ask) {
		t.Fatalf("ask = (%#v, %v), want single budget ask", ask, ok)
	}
	if loops.IsReviewScopeHumanAsk(ask) {
		t.Fatal("must not write a second scope HITL ask")
	}
	if got := loops.ReviewerPublishCount(updated.MetadataJSON); got != 1 {
		t.Fatalf("ReviewerPublishCount = %d, want 1", got)
	}
}

func TestRunReviewStepNeedsHumanUnderCapScopeHoldOnly(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	metadata := `{"loop":{"iterationCount":0}}`
	loop := storage.LoopRecord{
		ID: "loop_needs_human_under_cap", Seq: 96, ProjectID: "project_1", Type: "reviewer",
		TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
		Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("upsert loop: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.HITL.Enabled = true
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 8
	stdout := `__LOOPER_RESULT__={"summary":"Mixed must_fix and needs_human","outcome":"blocking","findings":[{"title":"Bug","body":"fix it","disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"diff","path":"a.go","line":1},{"title":"Ambiguous","body":"unclear","disposition":"needs_human","severity":"blocking","scopeBasis":"ambiguous_intent","scopeEvidence":"PR non-goals","path":"b.go","line":2}]}`
	agent := &fakeAgentExecutor{results: []AgentResult{{
		Status: "completed", Summary: "Mixed", Stdout: stdout, ParseStatus: "parsed",
	}}}
	github := &fakeGitHubGateway{
		reviewMarkerMissing:             false,
		reviewMarkerEvent:               ReviewEventComment,
		reviewMarkerOutcome:             "blocking",
		reviewMarkerInlineCommentBodies: []string{"fix it"},
	}
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{},
		AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now,
		LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
	})
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("Projects.GetByID() = (%#v, %v)", project, err)
	}
	_, err = runner.runReviewStep(context.Background(), stepInput{
		Project: *project, Loop: loop, Run: storage.RunRecord{ID: "run_needs_human_under_cap"},
		Repo: repo, PRNumber: prNumber,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main"},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "feature/review-me", PreparedAt: nowISO},
		},
	})
	var hold *holdSkipError
	if !errors.As(err, &hold) {
		t.Fatalf("runReviewStep() error = %v, want holdSkipError", err)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || updated == nil || !loops.IsReviewScopeHumanHold(*updated) {
		t.Fatalf("after park = (%#v, %v), want scope hold", updated, err)
	}
	if loops.IsReviewFixBudgetHold(*updated) {
		t.Fatal("under-cap must not budget-hold")
	}
	if got := loops.ReviewerPublishCount(updated.MetadataJSON); got != 1 {
		t.Fatalf("ReviewerPublishCount = %d, want 1", got)
	}
	ask, ok := loops.ReadHITLAsk(updated.MetadataJSON)
	if !ok || !loops.IsReviewScopeHumanAsk(ask) {
		t.Fatalf("ask = (%#v, %v), want scope ask only", ask, ok)
	}
}

func TestPublishAlreadyPublishedRecoversScopeParkWithoutRepublish(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		cap         int
		budgetHeld  bool
		wantBudget  bool
		wantScope   bool
		wantPending bool
		wantPublish int
	}{
		// lastPublishedHeadSha already set (progress recorded); scope park never ran.
		{name: "under_cap", cap: 8, budgetHeld: false, wantBudget: false, wantScope: true, wantPending: false, wantPublish: 1},
		// At cap: budget hold already applied after publish; recover deferred scope evidence only.
		{name: "at_cap_budget_held", cap: 1, budgetHeld: true, wantBudget: true, wantScope: false, wantPending: true, wantPublish: 1},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			repo := "acme/looper"
			prNumber := int64(42)
			nowISO := fixture.nowISO()
			target := "pr:acme/looper:42"
			// Simulate crash after recordPublishedReviewProgress wrote the head marker.
			metadata := `{"lastPublishedHeadSha":"abc123","loop":{"iterationCount":1}}`
			loop := storage.LoopRecord{
				ID: "loop_already_pub_scope_" + tc.name, Seq: 98, ProjectID: "project_1", Type: "reviewer",
				TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
				Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
			}
			if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
				t.Fatalf("upsert: %v", err)
			}
			if tc.budgetHeld {
				parked, err := loops.ParkReviewFixBudget(context.Background(), fixture.repos, loops.ParkReviewFixBudgetInput{
					Exhausted: loop, Role: "reviewer", Repo: repo, PRNumber: prNumber,
					Count: 1, Cap: tc.cap, NowISO: nowISO, HITLEnabled: true,
					LiveCaps: loops.ReviewFixBudgetLiveCaps{ReviewerMaxPublishes: tc.cap},
					DB:       fixture.coordinator.DB(),
				})
				if err != nil {
					t.Fatalf("ParkReviewFixBudget: %v", err)
				}
				loop = parked
			}
			cfg, err := config.DefaultConfig(t.TempDir())
			if err != nil {
				t.Fatalf("DefaultConfig: %v", err)
			}
			cfg.HITL.Enabled = true
			cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = tc.cap
			cfg.Roles.Reviewer.Behavior.ReviewEvents.Clean = config.ReviewerReviewEventComment
			completion := reviewerCommentOnlyCompletion{
				Summary: "Mixed must_fix and needs_human",
				Outcome: "blocking",
				Findings: []reviewerCommentOnlyFindingResult{
					{Title: "Bug", Body: "fix it", Disposition: "must_fix", Severity: "blocking", ScopeBasis: "introduced_regression", ScopeEvidence: "diff", Files: []string{"a.go"}},
					{Title: "Ambiguous", Body: "unclear", Disposition: "needs_human", Severity: "blocking", ScopeBasis: "ambiguous_intent", ScopeEvidence: "PR non-goals"},
				},
			}
			payload, err := json.Marshal(completion)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			github := &fakeGitHubGateway{}
			runner := New(Options{
				DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{},
				Logger: fixture.logger, Now: fixture.now, CommentOnlyPublish: true,
				LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
				ReviewEvents: cfg.Roles.Reviewer.Behavior.ReviewEvents,
			})
			project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
			if err != nil || project == nil {
				t.Fatalf("project: (%#v, %v)", project, err)
			}
			// Reload loop so stepInput sees post-budget-park status/metadata.
			fresh, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
			if err != nil || fresh == nil {
				t.Fatalf("reload loop: (%#v, %v)", fresh, err)
			}
			pending := pendingReviewCheckpoint{
				HeadSHA: "abc123", IdempotencyKey: "idem-already-pub-" + tc.name, Event: reviewEventAgentNative,
				Summary: completion.Summary, Outcome: completion.Outcome, ReviewerSummaryJSON: string(payload),
			}
			_, err = runner.runPublishStep(context.Background(), stepInput{
				Project: *project, Loop: *fresh, Run: storage.RunRecord{ID: "run_already_pub_" + tc.name},
				Repo: repo, PRNumber: prNumber,
				Checkpoint: reviewerCheckpoint{
					Detail:        &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", HeadSHA: "abc123", State: "OPEN"},
					Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
					PendingReview: &pending,
				},
			})
			var hold *holdSkipError
			if !errors.As(err, &hold) {
				t.Fatalf("runPublishStep() error = %v, want holdSkipError", err)
			}
			if len(github.issueCommentCalls) != 0 {
				t.Fatalf("issueCommentCalls = %d, want 0 (no re-publish)", len(github.issueCommentCalls))
			}
			updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
			if err != nil || updated == nil {
				t.Fatalf("get loop: (%#v, %v)", updated, err)
			}
			if got := loops.ReviewerPublishCount(updated.MetadataJSON); got != tc.wantPublish {
				t.Fatalf("publish count = %d, want %d", got, tc.wantPublish)
			}
			if tc.wantBudget != loops.IsReviewFixBudgetHold(*updated) {
				t.Fatalf("budget hold = %v, want %v (status=%s)", loops.IsReviewFixBudgetHold(*updated), tc.wantBudget, updated.Status)
			}
			if tc.wantScope != loops.IsReviewScopeHumanHold(*updated) {
				t.Fatalf("scope hold = %v, want %v", loops.IsReviewScopeHumanHold(*updated), tc.wantScope)
			}
			if tc.wantPending != loops.HasPendingReviewScopeHuman(*updated) {
				t.Fatalf("pending scope = %v, want %v meta=%s", loops.HasPendingReviewScopeHuman(*updated), tc.wantPending, derefString(updated.MetadataJSON))
			}
			if tc.wantBudget {
				ask, ok := loops.ReadHITLAsk(updated.MetadataJSON)
				if !ok || !loops.IsReviewFixBudgetAsk(ask) || loops.IsReviewScopeHumanAsk(ask) {
					t.Fatalf("at-cap ask = (%#v, %v), want budget only (no stacked scope ask)", ask, ok)
				}
			}
			if tc.wantScope {
				ask, ok := loops.ReadHITLAsk(updated.MetadataJSON)
				if !ok || !loops.IsReviewScopeHumanAsk(ask) {
					t.Fatalf("under-cap ask = (%#v, %v), want scope", ask, ok)
				}
			}
		})
	}
}

func TestProcessClaimedItemAtCapRecoversPendingScopeWithoutAgent(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	projectID := "project_1"
	loopID := "loop_claim_at_cap_scope_recover"
	// Crash after publish + budget park; scope park/defer never ran.
	metadata := `{"lastPublishedHeadSha":"abc123","loop":{"iterationCount":1}}`
	loop := storage.LoopRecord{
		ID: loopID, Seq: 110, ProjectID: projectID, Type: "reviewer",
		TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
		Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("upsert loop: %v", err)
	}
	parked, err := loops.ParkReviewFixBudget(context.Background(), fixture.repos, loops.ParkReviewFixBudgetInput{
		Exhausted: loop, Role: "reviewer", Repo: repo, PRNumber: prNumber,
		Count: 1, Cap: 1, NowISO: nowISO, HITLEnabled: true,
		LiveCaps: loops.ReviewFixBudgetLiveCaps{ReviewerMaxPublishes: 1},
		DB:       fixture.coordinator.DB(),
	})
	if err != nil {
		t.Fatalf("ParkReviewFixBudget: %v", err)
	}
	loop = parked
	if !loops.IsReviewFixBudgetHold(loop) {
		t.Fatalf("precondition: want budget hold, got status=%s meta=%s", loop.Status, derefString(loop.MetadataJSON))
	}
	completion := reviewerCommentOnlyCompletion{
		Summary: "Mixed must_fix and needs_human",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug", Body: "fix it", Disposition: "must_fix", Severity: "blocking", ScopeBasis: "introduced_regression", ScopeEvidence: "diff", Files: []string{"a.go"}},
			{Title: "Ambiguous", Body: "unclear", Disposition: "needs_human", Severity: "blocking", ScopeBasis: "ambiguous_intent", ScopeEvidence: "PR non-goals"},
		},
	}
	payload, err := json.Marshal(completion)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	checkpointJSON := mustMarshalJSON(reviewerCheckpoint{
		Detail:   &checkpointDetail{Title: "Review me", State: "OPEN", HeadSHA: "abc123", BaseRefName: "main", HeadRefName: "feature/review-me"},
		Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
		PendingReview: &pendingReviewCheckpoint{
			HeadSHA: "abc123", IdempotencyKey: "idem-claim-at-cap", Event: reviewEventAgentNative,
			Summary: completion.Summary, Outcome: completion.Outcome, ReviewerSummaryJSON: string(payload),
		},
		ResumePolicy: "advance_from_checkpoint",
	})
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{
		ID: "run_claim_at_cap_scope", LoopID: loopID, Status: "failed",
		CurrentStep: stringPtr(string(stepPublish)), LastCompletedStep: stringPtr(string(stepReview)),
		CheckpointJSON: &checkpointJSON, StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO,
	}); err != nil {
		t.Fatalf("Runs.Upsert: %v", err)
	}
	queueID := "queue_claim_at_cap_scope"
	lockKey := target
	queue := storage.QueueItemRecord{
		ID: queueID, ProjectID: &projectID, LoopID: &loopID, Type: "reviewer",
		TargetType: "pull_request", TargetID: target, Repo: &repo, PRNumber: &prNumber,
		DedupeKey: "reviewer:claim-at-cap-scope", Priority: storage.QueuePriorityReviewer,
		Status: "running", AvailableAt: nowISO, LockKey: &lockKey, MaxAttempts: 3,
		CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.HITL.Enabled = true
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 1
	agent := &fakeAgentExecutor{}
	github := &fakeGitHubGateway{viewHeadSHA: "abc123", viewState: "OPEN"}
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{},
		AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now,
		LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
	})

	result, err := runner.ProcessClaimedItem(context.Background(), queue)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !strings.Contains(result.Summary, "budget") {
		t.Fatalf("result = %#v, want budget skipped", result)
	}
	if len(agent.starts) != 0 {
		t.Fatalf("agent.starts = %#v, want none", agent.starts)
	}
	if len(github.issueCommentCalls) != 0 {
		t.Fatalf("issueCommentCalls = %d, want 0 (no re-publish)", len(github.issueCommentCalls))
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil || updated == nil {
		t.Fatalf("get loop: (%#v, %v)", updated, err)
	}
	if !loops.IsReviewFixBudgetHold(*updated) {
		t.Fatalf("want budget hold retained: status=%s meta=%s", updated.Status, derefString(updated.MetadataJSON))
	}
	if loops.IsReviewScopeHumanHold(*updated) {
		t.Fatalf("must not stack active scope hold under budget: meta=%s", derefString(updated.MetadataJSON))
	}
	if !loops.HasPendingReviewScopeHuman(*updated) {
		t.Fatalf("want pending scope after claim preflight recovery: meta=%s", derefString(updated.MetadataJSON))
	}
	ask, ok := loops.ReadHITLAsk(updated.MetadataJSON)
	if !ok || !loops.IsReviewFixBudgetAsk(ask) || loops.IsReviewScopeHumanAsk(ask) {
		t.Fatalf("ask = (%#v, %v), want budget only", ask, ok)
	}

	// Budget Continue promotes deferred scope to a real scope hold.
	continued, err := loops.ApplyReviewFixBudgetAnswer(context.Background(), fixture.repos, *updated, "Continue", nowISO, loops.ReviewFixBudgetLiveCaps{ReviewerMaxPublishes: 1, FixerMaxPushes: 8})
	if err != nil || !continued.Applied {
		t.Fatalf("ApplyReviewFixBudgetAnswer = (%#v, %v)", continued, err)
	}
	afterContinue, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil || afterContinue == nil {
		t.Fatalf("after continue: (%#v, %v)", afterContinue, err)
	}
	if loops.IsReviewFixBudgetHold(*afterContinue) {
		t.Fatalf("budget hold should clear after Continue: %#v", afterContinue)
	}
	if !loops.IsReviewScopeHumanHold(*afterContinue) {
		t.Fatalf("want scope hold after pending promote: status=%s meta=%s", afterContinue.Status, derefString(afterContinue.MetadataJSON))
	}
	if loops.HasPendingReviewScopeHuman(*afterContinue) {
		t.Fatalf("pending should clear after promote: meta=%s", derefString(afterContinue.MetadataJSON))
	}
}

func TestPersistPendingReviewerScopeHumanDoesNotClobberBudgetContinue(t *testing.T) {
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	metadata := `{"loop":{"iterationCount":1}}`
	loop := storage.LoopRecord{
		ID: "loop_pending_vs_continue", Seq: 111, ProjectID: "project_1", Type: "reviewer",
		TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
		Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("upsert loop: %v", err)
	}
	parked, err := loops.ParkReviewFixBudget(context.Background(), fixture.repos, loops.ParkReviewFixBudgetInput{
		Exhausted: loop, Role: "reviewer", Repo: repo, PRNumber: prNumber,
		Count: 1, Cap: 1, NowISO: nowISO, HITLEnabled: true,
		LiveCaps: loops.ReviewFixBudgetLiveCaps{ReviewerMaxPublishes: 1},
		DB:       fixture.coordinator.DB(),
	})
	if err != nil {
		t.Fatalf("ParkReviewFixBudget: %v", err)
	}
	if !loops.IsReviewFixBudgetHold(parked) {
		t.Fatalf("precondition: want budget hold, got status=%s meta=%s", parked.Status, derefString(parked.MetadataJSON))
	}
	stale := parked
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.HITL.Enabled = true
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 1
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{},
		AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now,
		LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
	})
	reviewScopeHumanPersistBeforeUpsertHook = func(held storage.LoopRecord) error {
		result, contErr := loops.ApplyReviewFixBudgetAnswer(context.Background(), fixture.repos, held, "Continue", nowISO, loops.ReviewFixBudgetLiveCaps{ReviewerMaxPublishes: 1, FixerMaxPushes: 8})
		if contErr != nil {
			return contErr
		}
		if !result.Applied {
			return fmt.Errorf("continue not applied")
		}
		return nil
	}
	t.Cleanup(func() { reviewScopeHumanPersistBeforeUpsertHook = nil })
	completion := reviewerCommentOnlyCompletion{
		Summary: "Mixed must_fix and needs_human",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug", Body: "fix it", Disposition: "must_fix", Severity: "blocking", ScopeBasis: "introduced_regression", ScopeEvidence: "diff", Files: []string{"a.go"}},
			{Title: "Ambiguous", Body: "unclear", Disposition: "needs_human", Severity: "blocking", ScopeBasis: "ambiguous_intent", ScopeEvidence: "PR non-goals"},
		},
	}
	if err := runner.persistPendingReviewerScopeHuman(context.Background(), stale, completion); err != nil {
		t.Fatalf("persistPendingReviewerScopeHuman: %v", err)
	}
	fresh, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || fresh == nil {
		t.Fatalf("get loop: (%#v, %v)", fresh, err)
	}
	if loops.IsReviewFixBudgetHold(*fresh) {
		t.Fatalf("stale persist restored budget hold after Continue: status=%s meta=%s", fresh.Status, derefString(fresh.MetadataJSON))
	}
	if loops.ReviewerPublishCount(fresh.MetadataJSON) != 0 {
		t.Fatalf("Continue meters must stay reset, got publish count %d meta=%s", loops.ReviewerPublishCount(fresh.MetadataJSON), derefString(fresh.MetadataJSON))
	}
	if !loops.IsReviewScopeHumanHold(*fresh) {
		t.Fatalf("want scope hold after Continue-then-persist: status=%s meta=%s", fresh.Status, derefString(fresh.MetadataJSON))
	}
	if loops.HasPendingReviewScopeHuman(*fresh) {
		t.Fatalf("pending must not land on the released loop: meta=%s", derefString(fresh.MetadataJSON))
	}
	ask, ok := loops.ReadHITLAsk(fresh.MetadataJSON)
	if !ok || !loops.IsReviewScopeHumanAsk(ask) || loops.IsReviewFixBudgetAsk(ask) {
		t.Fatalf("ask = (%#v, %v), want scope ask without restored budget ask", ask, ok)
	}

}

func TestPersistLastReviewedSignalDoesNotClobberBudgetContinue(t *testing.T) {
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	metadata := `{"followUpdates":true,"lastPublishedHeadSha":"abc123","loop":{"iterationCount":3}}`
	loop := storage.LoopRecord{
		ID: "loop_signal_vs_continue", Seq: 112, ProjectID: "project_1", Type: "reviewer",
		TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
		Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("upsert loop: %v", err)
	}
	fixer := storage.LoopRecord{
		ID: "loop_signal_vs_continue_fix", Seq: 113, ProjectID: "project_1", Type: "fixer",
		TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
		Status: "queued", MetadataJSON: stringPtr(`{"followUpdates":true}`), CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("upsert fixer: %v", err)
	}
	parked, err := loops.ParkReviewFixBudget(context.Background(), fixture.repos, loops.ParkReviewFixBudgetInput{
		Exhausted: loop, Role: "reviewer", Repo: repo, PRNumber: prNumber,
		Count: 3, Cap: 3, NowISO: nowISO, HITLEnabled: false,
		LiveCaps: loops.ReviewFixBudgetLiveCaps{ReviewerMaxPublishes: 3, FixerMaxPushes: 3},
		DB:       fixture.coordinator.DB(),
	})
	if err != nil {
		t.Fatalf("ParkReviewFixBudget: %v", err)
	}
	if !loops.IsReviewFixBudgetHold(parked) {
		t.Fatalf("precondition: want budget hold, got status=%s meta=%s", parked.Status, derefString(parked.MetadataJSON))
	}
	continued := false
	persistLastReviewedSignalBeforeCASHook = func(held storage.LoopRecord) error {
		if continued {
			return nil
		}
		continued = true
		result, contErr := loops.ApplyReviewFixBudgetAnswer(context.Background(), fixture.repos, held, "Continue", nowISO, loops.ReviewFixBudgetLiveCaps{ReviewerMaxPublishes: 3, FixerMaxPushes: 3})
		if contErr != nil {
			return contErr
		}
		if !result.Applied {
			return fmt.Errorf("continue not applied")
		}
		return nil
	}
	t.Cleanup(func() { persistLastReviewedSignalBeforeCASHook = nil })
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{},
		AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now,
		LoopConfig: testReviewerLoopConfig(),
	})
	if err := runner.persistLastReviewedSignalFingerprint(context.Background(), parked, "sig-after-continue", nil); err != nil {
		t.Fatalf("persistLastReviewedSignalFingerprint: %v", err)
	}
	fresh, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || fresh == nil {
		t.Fatalf("get loop: (%#v, %v)", fresh, err)
	}
	if loops.IsReviewFixBudgetHold(*fresh) {
		t.Fatalf("fingerprint persist restored budget hold after Continue: status=%s meta=%s", fresh.Status, derefString(fresh.MetadataJSON))
	}
	if loops.ReviewerPublishCount(fresh.MetadataJSON) != 0 {
		t.Fatalf("Continue meters must stay reset, got publish count %d meta=%s", loops.ReviewerPublishCount(fresh.MetadataJSON), derefString(fresh.MetadataJSON))
	}
	gotSignal, _ := stringFromAny(parseJSONObject(fresh.MetadataJSON)[metadataLastReviewedSignalFingerprintKey])
	if gotSignal != "sig-after-continue" {
		t.Fatalf("lastReviewedSignalFingerprint = %q, want persisted on released row", gotSignal)
	}
}

func TestBudgetHeldDispositionNeedsHumanCASMissWhileHeldRetriesOrErrors(t *testing.T) {
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	revMeta := `{"followUpdates":true,"lastPublishedHeadSha":"abc123","loop":{"enabled":true,"iterationCount":3}}`
	loop := storage.LoopRecord{
		ID: "loop_nh_budget_cas_miss", Seq: 41, ProjectID: "project_1", Type: "reviewer", Status: "running",
		TargetType: "pull_request", TargetID: stringPtr("pr:acme/looper:42"),
		Repo: &repo, PRNumber: &prNumber, MetadataJSON: &revMeta,
		CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Upsert reviewer: %v", err)
	}
	fixer := storage.LoopRecord{
		ID: "loop_nh_budget_cas_miss_fix", Seq: 42, ProjectID: "project_1", Type: "fixer", Status: "queued",
		TargetType: "pull_request", TargetID: stringPtr("pr:acme/looper:42"),
		Repo: &repo, PRNumber: &prNumber, MetadataJSON: stringPtr(`{"followUpdates":true}`),
		CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Upsert fixer: %v", err)
	}
	parked, err := loops.ParkReviewFixBudget(context.Background(), fixture.repos, loops.ParkReviewFixBudgetInput{
		Exhausted: loop, Role: "reviewer", Repo: repo, PRNumber: prNumber,
		Count: 3, Cap: 3, NowISO: nowISO, HITLEnabled: false,
		LiveCaps: loops.ReviewFixBudgetLiveCaps{ReviewerMaxPublishes: 3, FixerMaxPushes: 3},
		DB:       fixture.coordinator.DB(),
	})
	if err != nil {
		t.Fatalf("ParkReviewFixBudget: %v", err)
	}
	if !loops.IsReviewFixBudgetHold(parked) {
		t.Fatalf("fixture must be budget hold: %#v", parked)
	}
	mutated := false
	reviewScopeHumanPersistBeforeUpsertHook = func(held storage.LoopRecord) error {
		if mutated {
			return nil
		}
		mutated = true
		live, getErr := fixture.repos.Loops.GetByID(context.Background(), held.ID)
		if getErr != nil || live == nil {
			return fmt.Errorf("get held loop: (%#v, %v)", live, getErr)
		}
		meta := parseJSONObject(live.MetadataJSON)
		meta["casNoise"] = "competing-write"
		encoded, marshalErr := json.Marshal(meta)
		if marshalErr != nil {
			return marshalErr
		}
		text := string(encoded)
		live.MetadataJSON = &text
		live.UpdatedAt = nowISO
		return fixture.repos.Loops.Upsert(context.Background(), *live)
	}
	t.Cleanup(func() { reviewScopeHumanPersistBeforeUpsertHook = nil })
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{currentLogin: "looper-bot"},
		Logger: fixture.logger, Now: fixture.now, LoopConfig: testReviewerLoopConfig(),
	})
	input := threadResolutionStepInput()
	input.Loop = parked
	input.Project = storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repo"}
	input.Repo = repo
	input.PRNumber = prNumber
	parkErr := runner.parkDispositionNeedsHuman(context.Background(), input, "thread_1", "ambiguous", "sig-needs-human")
	after, getErr := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if getErr != nil || after == nil {
		t.Fatalf("get reviewer: (%#v, %v)", after, getErr)
	}
	if !loops.IsReviewFixBudgetHold(*after) {
		t.Fatalf("competing write must leave budget hold: status=%s meta=%s", after.Status, derefString(after.MetadataJSON))
	}
	if parkErr != nil {
		return
	}
	if !loops.HasPendingReviewScopeHuman(*after) {
		t.Fatalf("CAS miss while held must persist evidence or error, got nil with meta=%s", derefString(after.MetadataJSON))
	}
}

func TestPublishAlreadyPublishedNeedsHumanSkipsScopeOnNewHead(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	metadata := `{"lastPublishedHeadSha":"abc123","loop":{"iterationCount":1}}`
	loop := storage.LoopRecord{
		ID: "loop_already_pub_new_head", Seq: 111, ProjectID: "project_1", Type: "reviewer",
		TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
		Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.HITL.Enabled = true
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 8
	cfg.Roles.Reviewer.Behavior.ReviewEvents.Clean = config.ReviewerReviewEventComment
	completion := reviewerCommentOnlyCompletion{
		Summary: "Mixed", Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug", Body: "fix", Disposition: "must_fix", Severity: "blocking", ScopeBasis: "introduced_regression", ScopeEvidence: "diff", Files: []string{"a.go"}},
			{Title: "Ambiguous", Body: "unclear", Disposition: "needs_human", Severity: "blocking", ScopeBasis: "ambiguous_intent", ScopeEvidence: "PR non-goals"},
		},
	}
	payload, err := json.Marshal(completion)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	github := &fakeGitHubGateway{viewHeadSHA: "new-head", viewState: "OPEN"}
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{},
		Logger: fixture.logger, Now: fixture.now, CommentOnlyPublish: true,
		LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
		ReviewEvents: cfg.Roles.Reviewer.Behavior.ReviewEvents,
	})
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("project: (%#v, %v)", project, err)
	}
	pending := pendingReviewCheckpoint{
		HeadSHA: "abc123", IdempotencyKey: "idem-new-head", Event: reviewEventAgentNative,
		Summary: completion.Summary, Outcome: completion.Outcome, ReviewerSummaryJSON: string(payload),
	}
	checkpoint, err := runner.runPublishStep(context.Background(), stepInput{
		Project: *project, Loop: loop, Run: storage.RunRecord{ID: "run_already_pub_new_head"},
		Repo: repo, PRNumber: prNumber,
		Checkpoint: reviewerCheckpoint{
			Detail:        &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", HeadSHA: "abc123", State: "OPEN"},
			Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
			PendingReview: &pending,
		},
	})
	if err != nil {
		t.Fatalf("runPublishStep() error = %v, want nil stale skip", err)
	}
	if checkpoint.SkipKind != "stale" || !strings.Contains(checkpoint.SkipReason, "head changed") {
		t.Fatalf("checkpoint = %#v, want stale head-changed skip", checkpoint)
	}
	if len(github.issueCommentCalls) != 0 {
		t.Fatalf("issueCommentCalls = %d, want 0", len(github.issueCommentCalls))
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || updated == nil {
		t.Fatalf("get loop: (%#v, %v)", updated, err)
	}
	if loops.IsReviewScopeHumanHold(*updated) || loops.HasPendingReviewScopeHuman(*updated) {
		t.Fatalf("must not park/defer scope on new head: meta=%s", derefString(updated.MetadataJSON))
	}
}

func TestPublishAlreadyPublishedNeedsHumanSkipsScopeOnClosedPR(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	metadata := `{"lastPublishedHeadSha":"abc123","loop":{"iterationCount":1}}`
	loop := storage.LoopRecord{
		ID: "loop_already_pub_closed", Seq: 112, ProjectID: "project_1", Type: "reviewer",
		TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
		Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.HITL.Enabled = true
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 8
	cfg.Roles.Reviewer.Behavior.ReviewEvents.Clean = config.ReviewerReviewEventComment
	completion := reviewerCommentOnlyCompletion{
		Summary: "Mixed", Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug", Body: "fix", Disposition: "must_fix", Severity: "blocking", ScopeBasis: "introduced_regression", ScopeEvidence: "diff", Files: []string{"a.go"}},
			{Title: "Ambiguous", Body: "unclear", Disposition: "needs_human", Severity: "blocking", ScopeBasis: "ambiguous_intent", ScopeEvidence: "PR non-goals"},
		},
	}
	payload, err := json.Marshal(completion)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	github := &fakeGitHubGateway{viewHeadSHA: "abc123", viewState: "CLOSED"}
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{},
		Logger: fixture.logger, Now: fixture.now, CommentOnlyPublish: true,
		LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
		ReviewEvents: cfg.Roles.Reviewer.Behavior.ReviewEvents,
	})
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("project: (%#v, %v)", project, err)
	}
	pending := pendingReviewCheckpoint{
		HeadSHA: "abc123", IdempotencyKey: "idem-closed", Event: reviewEventAgentNative,
		Summary: completion.Summary, Outcome: completion.Outcome, ReviewerSummaryJSON: string(payload),
	}
	checkpoint, err := runner.runPublishStep(context.Background(), stepInput{
		Project: *project, Loop: loop, Run: storage.RunRecord{ID: "run_already_pub_closed"},
		Repo: repo, PRNumber: prNumber,
		Checkpoint: reviewerCheckpoint{
			Detail:        &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", HeadSHA: "abc123", State: "OPEN"},
			Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
			PendingReview: &pending,
		},
	})
	if err != nil {
		t.Fatalf("runPublishStep() error = %v, want nil closed-PR skip", err)
	}
	if checkpoint.SkipKind != "stale" || !strings.Contains(checkpoint.SkipReason, "CLOSED") {
		t.Fatalf("checkpoint = %#v, want stale closed-PR skip", checkpoint)
	}
	if len(github.issueCommentCalls) != 0 {
		t.Fatalf("issueCommentCalls = %d, want 0", len(github.issueCommentCalls))
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || updated == nil {
		t.Fatalf("get loop: (%#v, %v)", updated, err)
	}
	if loops.IsReviewScopeHumanHold(*updated) || loops.HasPendingReviewScopeHuman(*updated) {
		t.Fatalf("must not park/defer scope on closed PR: meta=%s", derefString(updated.MetadataJSON))
	}
}

func TestRunReviewStepNeedsHumanSkipsParkOnStaleOrClosedPR(t *testing.T) {
	t.Parallel()
	pureStdout := `__LOOPER_RESULT__={"summary":"Need human","outcome":"blocking","findings":[{"title":"Ambiguous","body":"Unclear","disposition":"needs_human","severity":"blocking","scopeBasis":"ambiguous_intent","scopeEvidence":"AGENTS.md rule X","path":"a.go","line":1}]}`
	mixedStdout := `__LOOPER_RESULT__={"summary":"Mixed must_fix and needs_human","outcome":"blocking","findings":[{"title":"Bug","body":"fix it","disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"diff","path":"a.go","line":1},{"title":"Ambiguous","body":"unclear","disposition":"needs_human","severity":"blocking","scopeBasis":"ambiguous_intent","scopeEvidence":"PR non-goals","path":"b.go","line":2}]}`
	for _, tc := range []struct {
		name          string
		commentOnly   bool
		stdout        string
		markerMissing bool
		changeHead    bool
		closeAfter    bool
		wantInReason  string
	}{
		{name: "comment_only_new_head", commentOnly: true, stdout: pureStdout, markerMissing: true, changeHead: true, wantInReason: "head changed"},
		{name: "comment_only_closed", commentOnly: true, stdout: pureStdout, markerMissing: true, closeAfter: true, wantInReason: "CLOSED"},
		{name: "native_mixed_new_head", stdout: mixedStdout, changeHead: true, wantInReason: "head changed"},
		{name: "native_mixed_closed", stdout: mixedStdout, closeAfter: true, wantInReason: "CLOSED"},
		{name: "native_pure_new_head", stdout: pureStdout, markerMissing: true, changeHead: true, wantInReason: "head changed"},
		{name: "native_pure_closed", stdout: pureStdout, markerMissing: true, closeAfter: true, wantInReason: "CLOSED"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			repo := "acme/looper"
			prNumber := int64(42)
			nowISO := fixture.nowISO()
			target := "pr:acme/looper:42"
			metadata := `{"loop":{"iterationCount":0}}`
			loop := storage.LoopRecord{
				ID: "loop_scope_fresh_" + tc.name, Seq: 120, ProjectID: "project_1", Type: "reviewer",
				TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
				Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
			}
			if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
				t.Fatalf("upsert loop: %v", err)
			}
			cfg, err := config.DefaultConfig(t.TempDir())
			if err != nil {
				t.Fatalf("DefaultConfig: %v", err)
			}
			cfg.HITL.Enabled = true
			if tc.commentOnly {
				cfg.Roles.Reviewer.Behavior.ReviewEvents.Clean = config.ReviewerReviewEventComment
			}
			closedAfterFirst := ""
			if tc.closeAfter {
				closedAfterFirst = "CLOSED"
			}
			github := &fakeGitHubGateway{
				changeHeadOnSecondView:  tc.changeHead,
				viewStateAfterFirstView: closedAfterFirst,
				reviewMarkerMissing:     tc.markerMissing,
				reviewMarkerEvent:       ReviewEventComment,
				reviewMarkerOutcome:     "blocking",
			}
			runner := New(Options{
				DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{},
				AgentExecutor: &fakeAgentExecutor{results: []AgentResult{{
					Status: "completed", Summary: "Need human", Stdout: tc.stdout, ParseStatus: "parsed",
				}}},
				Logger: fixture.logger, Now: fixture.now, CommentOnlyPublish: tc.commentOnly,
				LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
				ReviewEvents: cfg.Roles.Reviewer.Behavior.ReviewEvents,
			})
			project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
			if err != nil || project == nil {
				t.Fatalf("Projects.GetByID() = (%#v, %v)", project, err)
			}
			checkpoint, err := runner.runReviewStep(context.Background(), stepInput{
				Project:  *project,
				Loop:     loop,
				Run:      storage.RunRecord{ID: "run_scope_fresh_" + tc.name},
				Repo:     repo,
				PRNumber: prNumber,
				Checkpoint: reviewerCheckpoint{
					Detail:   &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", HeadSHA: "abc123", State: "OPEN"},
					Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
					Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "feature/review-me", PreparedAt: nowISO},
				},
			})
			if err != nil {
				t.Fatalf("runReviewStep() error = %v, want stale skip", err)
			}
			if checkpoint.SkipKind != "stale" || !strings.Contains(checkpoint.SkipReason, tc.wantInReason) {
				t.Fatalf("checkpoint = %#v, want stale %q", checkpoint, tc.wantInReason)
			}
			updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
			if err != nil || updated == nil {
				t.Fatalf("get loop: (%#v, %v)", updated, err)
			}
			if loops.IsReviewScopeHumanHold(*updated) || loops.HasPendingReviewScopeHuman(*updated) {
				t.Fatalf("must not park obsolete needs_human: meta=%s", derefString(updated.MetadataJSON))
			}
			if got := loops.ReviewerPublishCount(updated.MetadataJSON); got != 0 {
				t.Fatalf("ReviewerPublishCount = %d, want 0", got)
			}
		})
	}
}

func TestPublishCommentOnlyMixedMustFixNeedsHumanPublishesThenParksScope(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		cap         int
		wantBudget  bool
		wantScope   bool
		wantPending bool
		wantPublish int
	}{
		{name: "under_cap", cap: 8, wantBudget: false, wantScope: true, wantPending: false, wantPublish: 1},
		{name: "at_cap_after_publish", cap: 1, wantBudget: true, wantScope: false, wantPending: true, wantPublish: 1},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			repo := "acme/looper"
			prNumber := int64(42)
			nowISO := fixture.nowISO()
			target := "pr:acme/looper:42"
			metadata := `{"loop":{"iterationCount":0}}`
			loop := storage.LoopRecord{
				ID: "loop_mixed_comment_" + tc.name, Seq: 97, ProjectID: "project_1", Type: "reviewer",
				TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
				Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
			}
			if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
				t.Fatalf("upsert: %v", err)
			}
			cfg, err := config.DefaultConfig(t.TempDir())
			if err != nil {
				t.Fatalf("DefaultConfig: %v", err)
			}
			cfg.HITL.Enabled = true
			cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = tc.cap
			// Comment-only completion path requires Clean != APPROVE (or Forgejo summary mode).
			cfg.Roles.Reviewer.Behavior.ReviewEvents.Clean = config.ReviewerReviewEventComment
			completion := reviewerCommentOnlyCompletion{
				Summary: "Mixed must_fix and needs_human",
				Outcome: "blocking",
				Findings: []reviewerCommentOnlyFindingResult{
					{Title: "Bug", Body: "fix it", Disposition: "must_fix", Severity: "blocking", ScopeBasis: "introduced_regression", ScopeEvidence: "diff", Files: []string{"a.go"}},
					{Title: "Ambiguous", Body: "unclear", Disposition: "needs_human", Severity: "blocking", ScopeBasis: "ambiguous_intent", ScopeEvidence: "PR non-goals"},
				},
			}
			payload, err := json.Marshal(completion)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			github := &fakeGitHubGateway{}
			runner := New(Options{
				DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{},
				Logger: fixture.logger, Now: fixture.now, CommentOnlyPublish: true,
				LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
				ReviewEvents: cfg.Roles.Reviewer.Behavior.ReviewEvents,
			})
			project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
			if err != nil || project == nil {
				t.Fatalf("project: (%#v, %v)", project, err)
			}
			pending := pendingReviewCheckpoint{
				HeadSHA: "abc123", IdempotencyKey: "idem-mixed-" + tc.name, Event: reviewEventAgentNative,
				Summary: completion.Summary, Outcome: completion.Outcome, ReviewerSummaryJSON: string(payload),
			}
			_, err = runner.runPublishStep(context.Background(), stepInput{
				Project: *project, Loop: loop, Run: storage.RunRecord{ID: "run_mixed_" + tc.name},
				Repo: repo, PRNumber: prNumber,
				Checkpoint: reviewerCheckpoint{
					Detail:        &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", HeadSHA: "abc123", State: "OPEN"},
					Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
					PendingReview: &pending,
				},
			})
			var hold *holdSkipError
			if !errors.As(err, &hold) {
				t.Fatalf("runPublishStep() error = %v, want holdSkipError", err)
			}
			if len(github.issueCommentCalls) != 1 {
				t.Fatalf("issueCommentCalls = %d, want 1 must_fix summary publish", len(github.issueCommentCalls))
			}
			body := github.issueCommentCalls[0].Body
			if strings.Contains(strings.ToLower(body), "ambiguous") || strings.Contains(body, "needs_human") {
				t.Fatalf("published body smuggles needs_human: %s", body)
			}
			updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
			if err != nil || updated == nil {
				t.Fatalf("get loop: (%#v, %v)", updated, err)
			}
			if got := loops.ReviewerPublishCount(updated.MetadataJSON); got != tc.wantPublish {
				t.Fatalf("publish count = %d, want %d", got, tc.wantPublish)
			}
			if tc.wantBudget != loops.IsReviewFixBudgetHold(*updated) {
				t.Fatalf("budget hold = %v, want %v (status=%s)", loops.IsReviewFixBudgetHold(*updated), tc.wantBudget, updated.Status)
			}
			if tc.wantScope != loops.IsReviewScopeHumanHold(*updated) {
				t.Fatalf("scope hold = %v, want %v", loops.IsReviewScopeHumanHold(*updated), tc.wantScope)
			}
			if tc.wantPending != loops.HasPendingReviewScopeHuman(*updated) {
				t.Fatalf("pending scope = %v, want %v meta=%s", loops.HasPendingReviewScopeHuman(*updated), tc.wantPending, derefString(updated.MetadataJSON))
			}
			if tc.wantBudget {
				ask, ok := loops.ReadHITLAsk(updated.MetadataJSON)
				if !ok || !loops.IsReviewFixBudgetAsk(ask) || loops.IsReviewScopeHumanAsk(ask) {
					t.Fatalf("at-cap ask = (%#v, %v), want budget only", ask, ok)
				}
			}
			if tc.wantScope {
				ask, ok := loops.ReadHITLAsk(updated.MetadataJSON)
				if !ok || !loops.IsReviewScopeHumanAsk(ask) {
					t.Fatalf("under-cap ask = (%#v, %v), want scope", ask, ok)
				}
			}
		})
	}
}

// failOnceBudgetHeldLoopGet fails the first loops-by-id read after a budget
// hold is durable, then succeeds. Models a transient storage error between
// recordPublishedReviewProgress and parkOrDeferReviewerScopeHuman.
type failOnceBudgetHeldLoopGet struct {
	db         *sql.DB
	failedOnce bool
}

func (q *failOnceBudgetHeldLoopGet) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return q.db.ExecContext(ctx, query, args...)
}

func (q *failOnceBudgetHeldLoopGet) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return q.db.QueryContext(ctx, query, args...)
}

func (q *failOnceBudgetHeldLoopGet) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if !q.failedOnce && strings.Contains(query, "SELECT * FROM loops WHERE id") && len(args) == 1 {
		id, _ := args[0].(string)
		var status string
		var meta *string
		if err := q.db.QueryRowContext(ctx, `SELECT status, metadata_json FROM loops WHERE id = ?`, id).Scan(&status, &meta); err == nil {
			if loops.IsReviewFixBudgetHold(storage.LoopRecord{Status: status, MetadataJSON: meta}) {
				q.failedOnce = true
				return q.db.QueryRowContext(ctx, `SELECT * FROM loops_refresh_probe_missing WHERE id = ?`, id)
			}
		}
	}
	return q.db.QueryRowContext(ctx, query, args...)
}

func TestPublishCommentOnlyMixedMustFixNeedsHumanRefreshErrorDoesNotStackHolds(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	metadata := `{"loop":{"iterationCount":0}}`
	loop := storage.LoopRecord{
		ID: "loop_mixed_refresh_fail", Seq: 99, ProjectID: "project_1", Type: "reviewer",
		TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
		Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.HITL.Enabled = true
	cfg.Roles.Reviewer.Behavior.Loop.MaxPublishesPerPR = 1
	cfg.Roles.Reviewer.Behavior.ReviewEvents.Clean = config.ReviewerReviewEventComment
	completion := reviewerCommentOnlyCompletion{
		Summary: "Mixed must_fix and needs_human",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug", Body: "fix it", Disposition: "must_fix", Severity: "blocking", ScopeBasis: "introduced_regression", ScopeEvidence: "diff", Files: []string{"a.go"}},
			{Title: "Ambiguous", Body: "unclear", Disposition: "needs_human", Severity: "blocking", ScopeBasis: "ambiguous_intent", ScopeEvidence: "PR non-goals"},
		},
	}
	payload, err := json.Marshal(completion)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	querier := &failOnceBudgetHeldLoopGet{db: fixture.coordinator.DB()}
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: storage.NewRepositories(querier), GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{},
		Logger: fixture.logger, Now: fixture.now, CommentOnlyPublish: true,
		LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
		ReviewEvents: cfg.Roles.Reviewer.Behavior.ReviewEvents,
	})
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("project: (%#v, %v)", project, err)
	}
	pending := pendingReviewCheckpoint{
		HeadSHA: "abc123", IdempotencyKey: "idem-mixed-refresh-fail", Event: reviewEventAgentNative,
		Summary: completion.Summary, Outcome: completion.Outcome, ReviewerSummaryJSON: string(payload),
	}
	_, err = runner.runPublishStep(context.Background(), stepInput{
		Project: *project, Loop: loop, Run: storage.RunRecord{ID: "run_mixed_refresh_fail"},
		Repo: repo, PRNumber: prNumber,
		Checkpoint: reviewerCheckpoint{
			Detail:        &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", HeadSHA: "abc123", State: "OPEN"},
			Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
			PendingReview: &pending,
		},
	})
	if err == nil {
		t.Fatal("runPublishStep() error = nil, want refresh failure")
	}
	var hold *holdSkipError
	if errors.As(err, &hold) {
		t.Fatalf("runPublishStep() = holdSkipError %q, want fail-closed refresh error", hold.summary)
	}
	if !strings.Contains(err.Error(), "refresh loop before review scope park") {
		t.Fatalf("runPublishStep() error = %v, want refresh loop before review scope park", err)
	}
	updated, getErr := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if getErr != nil || updated == nil {
		t.Fatalf("get loop: (%#v, %v)", updated, getErr)
	}
	if !loops.IsReviewFixBudgetHold(*updated) {
		t.Fatalf("want durable budget hold after publish: status=%s meta=%s", updated.Status, derefString(updated.MetadataJSON))
	}
	if loops.IsReviewScopeHumanHold(*updated) {
		t.Fatalf("refresh failure must not stack a scope hold: %#v", updated)
	}
	if loops.HasPendingReviewScopeHuman(*updated) {
		t.Fatalf("refresh failure must not guess pending scope: meta=%s", derefString(updated.MetadataJSON))
	}
	if got := loops.ReviewerPublishCount(updated.MetadataJSON); got != 1 {
		t.Fatalf("ReviewerPublishCount = %d, want 1", got)
	}
}

func TestPublishCommentOnlyMixedMustFixNeedsHumanKeepsReferencedOpenItems(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	metadata := `{"loop":{"iterationCount":0}}`
	loop := storage.LoopRecord{
		ID: "loop_mixed_keep_open", Seq: 98, ProjectID: "project_1", Type: "reviewer",
		TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
		Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	existing := forge.NewReviewerSummary(1, []forge.ReviewItem{
		{ReviewItemID: "R-001", Status: forge.ReviewItemStatusOpen, Title: "Ambiguous scope", Body: "Is this in PR scope?", Files: []string{"a.go"}, LastSeenRoundID: 1},
		{ReviewItemID: "R-002", Status: forge.ReviewItemStatusOpen, Title: "Stale leftover", Body: "Truly gone.", LastSeenRoundID: 1},
	})
	existingBody, err := renderReviewerSummaryComment(existing, "Previous summary")
	if err != nil {
		t.Fatalf("renderReviewerSummaryComment: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.HITL.Enabled = true
	cfg.Roles.Reviewer.Behavior.ReviewEvents.Clean = config.ReviewerReviewEventComment
	completion := reviewerCommentOnlyCompletion{
		Summary: "Mixed must_fix and needs_human",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Bug", Body: "fix it", Disposition: "must_fix", Severity: "blocking", ScopeBasis: "introduced_regression", ScopeEvidence: "diff", Files: []string{"b.go"}},
			{ReviewItemID: "R-001", Title: "Ambiguous scope", Body: "unclear", Disposition: "needs_human", Severity: "blocking", ScopeBasis: "ambiguous_intent", ScopeEvidence: "PR non-goals"},
		},
	}
	payload, err := json.Marshal(completion)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	github := &fakeGitHubGateway{issueComments: []map[string]any{{"id": int64(91), "body": existingBody}}}
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{},
		Logger: fixture.logger, Now: fixture.now, CommentOnlyPublish: true,
		LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
		ReviewEvents: cfg.Roles.Reviewer.Behavior.ReviewEvents,
	})
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || project == nil {
		t.Fatalf("project: (%#v, %v)", project, err)
	}
	pending := pendingReviewCheckpoint{
		HeadSHA: "abc123", IdempotencyKey: "idem-mixed-keep-open", Event: reviewEventAgentNative,
		Summary: completion.Summary, Outcome: completion.Outcome, ReviewerSummaryJSON: string(payload),
	}
	_, err = runner.runPublishStep(context.Background(), stepInput{
		Project: *project, Loop: loop, Run: storage.RunRecord{ID: "run_mixed_keep_open"},
		Repo: repo, PRNumber: prNumber,
		Checkpoint: reviewerCheckpoint{
			Detail:        &checkpointDetail{HeadRefName: "feature/review-me", BaseRefName: "main", HeadSHA: "abc123", State: "OPEN"},
			Snapshot:      &checkpointSnapshot{HeadSHA: "abc123"},
			PendingReview: &pending,
		},
	})
	var hold *holdSkipError
	if !errors.As(err, &hold) {
		t.Fatalf("runPublishStep() error = %v, want holdSkipError", err)
	}
	if len(github.updateIssueCommentCalls) != 1 {
		t.Fatalf("updateIssueCommentCalls = %d, want 1", len(github.updateIssueCommentCalls))
	}
	parsed, err := forge.ParseReviewerSummary(github.updateIssueCommentCalls[0].Body)
	if err != nil {
		t.Fatalf("ParseReviewerSummary: %v", err)
	}
	byID := map[string]forge.ReviewItem{}
	openIDs := map[string]struct{}{}
	for _, item := range parsed.Items {
		byID[item.ReviewItemID] = item
		if item.Status == forge.ReviewItemStatusOpen {
			openIDs[item.ReviewItemID] = struct{}{}
		}
	}
	if byID["R-001"].Status != forge.ReviewItemStatusOpen {
		t.Fatalf("R-001 = %#v, want still open for Forgejo Fixer after mixed publish/park", byID["R-001"])
	}
	if byID["R-002"].Status != forge.ReviewItemStatusResolved {
		t.Fatalf("R-002 = %#v, want omitted leftover resolved", byID["R-002"])
	}
	if _, ok := openIDs["R-001"]; !ok || len(openIDs) != 2 {
		t.Fatalf("open IDs = %#v, want R-001 plus published must_fix", openIDs)
	}
	if strings.Contains(github.updateIssueCommentCalls[0].Body, "unclear") {
		t.Fatalf("published body smuggles needs_human: %s", github.updateIssueCommentCalls[0].Body)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || updated == nil {
		t.Fatalf("get loop: (%#v, %v)", updated, err)
	}
	if !loops.IsReviewScopeHumanHold(*updated) {
		t.Fatalf("scope hold = false, want parked after mixed publish")
	}
}

func TestParkReviewerScopeHumanHITLOffNoAsk(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	metadata := `{"loop":{"iterationCount":1}}`
	reviewer := storage.LoopRecord{ID: "loop_scope_no_hitl", Seq: 91, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	fixer := storage.LoopRecord{ID: "loop_scope_no_hitl_fixer", Seq: 92, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("upsert reviewer: %v", err)
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("upsert fixer: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.HITL.Enabled = false
	var notified []string
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now,
		LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
		NotifyHumanAttention: func(_ context.Context, loopID string) { notified = append(notified, loopID) },
	})
	completion := reviewerCommentOnlyCompletion{
		Summary: "Need human",
		Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Ambiguous", Body: "Unclear scope", Disposition: "needs_human", ScopeBasis: "ambiguous_intent", ScopeEvidence: "PR non-goals conflict"},
		},
	}
	if err := runner.parkReviewerScopeHuman(context.Background(), reviewer, completion); err != nil {
		t.Fatalf("parkReviewerScopeHuman: %v", err)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), reviewer.ID)
	if err != nil || updated == nil || updated.Status != "paused" || !loops.IsReviewScopeHumanHold(*updated) {
		t.Fatalf("reviewer = (%#v, %v), want scope pause", updated, err)
	}
	if loops.IsReviewFixBudgetHold(*updated) {
		t.Fatal("must not be budget hold")
	}
	if ask, ok := loops.ReadHITLAsk(updated.MetadataJSON); ok {
		t.Fatalf("HITL-off must not write ask, got %#v", ask)
	}
	sibling, err := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
	if err != nil || sibling == nil || sibling.Status != "paused" || !loops.IsSiblingReviewScopeHumanPause(sibling.MetadataJSON) {
		t.Fatalf("sibling = (%#v, %v), want scope sibling pause", sibling, err)
	}
	if len(notified) != 1 || notified[0] != reviewer.ID {
		t.Fatalf("NotifyHumanAttention = %#v", notified)
	}
}

func TestParkReviewerScopeHumanHITLOnAskNotBudgetKind(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	metadata := `{"loop":{"iterationCount":1}}`
	reviewer := storage.LoopRecord{ID: "loop_scope_hitl", Seq: 93, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.HITL.Enabled = true
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now,
		LoopConfig: cfg.Roles.Reviewer.Behavior.Loop, CustomInstructions: &cfg,
	})
	if err := runner.parkReviewerScopeHuman(context.Background(), reviewer, reviewerCommentOnlyCompletion{
		Summary: "Need human", Outcome: "blocking",
		Findings: []reviewerCommentOnlyFindingResult{
			{Title: "Ambiguous", Body: "x", Disposition: "needs_human", ScopeBasis: "ambiguous_intent", ScopeEvidence: "e"},
		},
	}); err != nil {
		t.Fatalf("park: %v", err)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), reviewer.ID)
	if err != nil || updated == nil || updated.Status != "awaiting_human" {
		t.Fatalf("updated = (%#v, %v)", updated, err)
	}
	ask, ok := loops.ReadHITLAsk(updated.MetadataJSON)
	if !ok || !loops.IsReviewScopeHumanAsk(ask) || loops.IsReviewFixBudgetAsk(ask) {
		t.Fatalf("ask = (%#v, %v), want scope kind", ask, ok)
	}
	if loops.IsReviewFixBudgetHold(*updated) {
		t.Fatal("scope HITL hold must not be budget hold")
	}
}

func TestProcessClaimedItemPreservesParkedScopeHoldStatus(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		hitl       bool
		wantStatus string
	}{
		{name: "hitl_awaiting", hitl: true, wantStatus: "awaiting_human"},
		{name: "no_hitl_paused", hitl: false, wantStatus: "paused"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			ctx := context.Background()
			nowISO := fixture.nowISO()
			repo := "acme/looper"
			prNumber := int64(42)
			loopID := "loop_scope_finalizer_" + tc.name
			metadata := `{"followUpdates":true,"loop":{"enabled":true}}`
			loop := storage.LoopRecord{
				ID: loopID, Seq: 210, ProjectID: "project_1", Type: "reviewer",
				TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber,
				Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
			}
			if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
				t.Fatalf("Loops.Upsert() error = %v", err)
			}
			cfg, err := config.DefaultConfig(t.TempDir())
			if err != nil {
				t.Fatalf("DefaultConfig: %v", err)
			}
			cfg.HITL.Enabled = tc.hitl
			stdout := `__LOOPER_RESULT__={"summary":"Need human","outcome":"blocking","findings":[{"title":"Ambiguous","body":"Unclear","disposition":"needs_human","severity":"blocking","scopeBasis":"ambiguous_intent","scopeEvidence":"AGENTS.md rule X","path":"a.go","line":1}]}`
			agent := &fakeAgentExecutor{results: []AgentResult{{
				Status: "completed", Summary: "Need human", Stdout: stdout, ParseStatus: "parsed",
			}}}
			github := &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true}
			runner := New(Options{
				DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{},
				AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg,
				LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25},
			})
			queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
			if err != nil {
				t.Fatalf("enqueue() error = %v", err)
			}
			claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
			if err != nil || claimed == nil || claimed.ID != queue.ID {
				t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
			}
			result, err := runner.ProcessClaimedItem(ctx, *claimed)
			if err != nil {
				t.Fatalf("ProcessClaimedItem() error = %v", err)
			}
			if result.Status != "skipped" {
				t.Fatalf("result = %#v, want skipped after in-claim needs_human park", result)
			}
			if !strings.Contains(result.Summary, "review scope requires human judgment") {
				t.Fatalf("result.Summary = %q, want scope hold skip", result.Summary)
			}
			updated, err := fixture.repos.Loops.GetByID(ctx, loopID)
			if err != nil || updated == nil {
				t.Fatalf("Loops.GetByID() = (%#v, %v)", updated, err)
			}
			if updated.Status != tc.wantStatus {
				t.Fatalf("status = %q, want parked %q (not queued)", updated.Status, tc.wantStatus)
			}
			if !loops.IsReviewScopeHumanHold(*updated) {
				t.Fatalf("want scope hold preserved: %#v meta=%v", updated, updated.MetadataJSON)
			}
		})
	}
}

func TestProcessClaimedItemFailedRunNeedsHumanParksBeforeMarkerRecovery(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	loopID := "loop_failed_run_needs_human"
	metadata := `{"followUpdates":true,"loop":{"enabled":true}}`
	loop := storage.LoopRecord{
		ID: loopID, Seq: 212, ProjectID: "project_1", Type: "reviewer",
		TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber,
		Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.HITL.Enabled = false
	stdout := `__LOOPER_RESULT__={"summary":"Mixed must_fix and needs_human","outcome":"blocking","findings":[{"title":"Bug","body":"fix it","disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"diff","path":"a.go","line":1},{"title":"Ambiguous","body":"unclear","disposition":"needs_human","severity":"blocking","scopeBasis":"ambiguous_intent","scopeEvidence":"PR non-goals","path":"b.go","line":2}]}`
	github := &fakeGitHubGateway{
		reviewRequests:                  []string{"octocat"},
		reviewMarkerMissing:             false,
		reviewMarkerEvent:               ReviewEventComment,
		reviewMarkerOutcome:             "blocking",
		reviewMarkerInlineCommentBodies: []string{"fix it"},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{
		Status: "failed", Summary: "posted review, later command failed", Stdout: stdout,
	}}}
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{},
		AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg,
		LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25},
	})
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}
	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" {
		t.Fatalf("result = %#v, want skipped after failed-run needs_human park", result)
	}
	if !strings.Contains(result.Summary, "review scope requires human judgment") {
		t.Fatalf("result.Summary = %q, want scope hold skip", result.Summary)
	}
	updated, err := fixture.repos.Loops.GetByID(ctx, loopID)
	if err != nil || updated == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v)", updated, err)
	}
	if updated.Status != "paused" {
		t.Fatalf("status = %q, want paused scope hold (not published-and-queued)", updated.Status)
	}
	if !loops.IsReviewScopeHumanHold(*updated) {
		t.Fatalf("want scope hold after failed-run needs_human: %#v meta=%v", updated, updated.MetadataJSON)
	}
	gotHead, _ := stringFromAny(parseJSONObject(updated.MetadataJSON)["lastPublishedHeadSha"])
	if gotHead == "" {
		t.Fatalf("lastPublishedHeadSha missing; mixed must_fix should count before park: %s", derefString(updated.MetadataJSON))
	}
	if result.RunID == "" {
		t.Fatal("result.RunID empty, want persisted run checkpoint")
	}
	persisted, err := fixture.repos.Runs.GetByID(ctx, result.RunID)
	if err != nil || persisted == nil {
		t.Fatalf("Runs.GetByID = (%#v, %v)", persisted, err)
	}
	checkpoint := parseCheckpoint(persisted.CheckpointJSON)
	if checkpoint.PendingReview == nil || !strings.Contains(checkpoint.PendingReview.ReviewerSummaryJSON, `"disposition":"needs_human"`) {
		t.Fatalf("checkpoint pending = %#v, want persisted native completion with needs_human before marker recovery", checkpoint.PendingReview)
	}
}

func TestProcessClaimedItemMixedNeedsHumanMissingMarkerFailsRetryable(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	loopID := "loop_mixed_missing_marker"
	metadata := `{"followUpdates":true,"loop":{"enabled":true}}`
	loop := storage.LoopRecord{
		ID: loopID, Seq: 211, ProjectID: "project_1", Type: "reviewer",
		TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber,
		Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.HITL.Enabled = false
	stdout := `__LOOPER_RESULT__={"summary":"Mixed must_fix and needs_human","outcome":"blocking","findings":[{"title":"Bug","body":"fix it","disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"diff","path":"a.go","line":1},{"title":"Ambiguous","body":"unclear","disposition":"needs_human","severity":"blocking","scopeBasis":"ambiguous_intent","scopeEvidence":"PR non-goals","path":"b.go","line":2}]}`
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos,
		GitHub:        &fakeGitHubGateway{reviewRequests: []string{"octocat"}, reviewMarkerMissing: true},
		Git:           &fakeGitGateway{},
		AgentExecutor: &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Mixed", Stdout: stdout, ParseStatus: "parsed"}}},
		Logger:        fixture.logger, Now: fixture.now, CustomInstructions: &cfg,
		LoopConfig: config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 120, MaxIterationsPerPR: 20, MaxIterationsPerHead: 2, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25},
	})
	queue, err := runner.enqueue(ctx, enqueueInput{ProjectID: "project_1", LoopID: loop.ID, Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	claimed, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claimed == nil || claimed.ID != queue.ID {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want queued item %s", claimed, err, queue.ID)
	}
	result, err := runner.ProcessClaimedItem(ctx, *claimed)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume || !strings.Contains(result.Summary, "actionable review marker") {
		t.Fatalf("result = %#v, want retryable mixed-marker publication failure", result)
	}
	updated, err := fixture.repos.Loops.GetByID(ctx, loopID)
	if err != nil || updated == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v)", updated, err)
	}
	if loops.IsReviewScopeHumanHold(*updated) || loops.HasPendingReviewScopeHuman(*updated) {
		t.Fatalf("must not park mixed must_fix without a marker: status=%s meta=%v", updated.Status, updated.MetadataJSON)
	}
}

func TestProcessClaimedItemCommentOnlySkipsWhenReviewRequestRemovedBeforePublish(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		labels:                          []string{"looper:review"},
		reviewRequests:                  []string{"reviewer"},
		currentLogin:                    "reviewer",
		removeReviewRequestOnSecondView: true,
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "Blocking issue remains", Stdout: `__LOOPER_RESULT__={"summary":"Blocking issue remains","outcome":"blocking","findings":[{"title":"Blocking issue remains","body":"Must not publish summary when request was removed.","files":["internal/reviewer/runner.go"],"disposition":"must_fix","severity":"blocking","scopeBasis":"required_invariant","scopeEvidence":"request gate"}]}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, CommentOnlyPublish: true, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: true, Labels: []string{"looper:review"}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig()})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed reviewer item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !contains(result.Summary, "not requested for review") {
		t.Fatalf("result = %#v, want skipped not requested", result)
	}
	if len(github.issueCommentCalls) != 0 {
		t.Fatalf("issueCommentCalls = %#v, want no summary_comment publish after request removal", github.issueCommentCalls)
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(context.Background(), *claim.LoopID)
	if err != nil || updatedLoop == nil || updatedLoop.MetadataJSON == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop metadata", updatedLoop, err)
	}
	if contains(*updatedLoop.MetadataJSON, `"lastPublishedHeadSha":"abc123"`) {
		t.Fatalf("loop metadata = %s, want no comment-only publish progress", *updatedLoop.MetadataJSON)
	}
}

func TestProcessClaimedItemCommentOnlyDoesNotMarkActionableNoActionSummaryAsCleanNoop(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{labels: []string{"looper:review"}, reviewRequests: []string{}, currentLogin: "reviewer"}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings, but this still has a blocking issue", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings, but this still has a blocking issue","outcome":"blocking","findings":[{"title":"Blocking issue remains","body":"The structured outcome is still blocking and must publish an open reviewer item.","files":["internal/reviewer/runner.go"],"disposition":"must_fix","severity":"blocking","scopeBasis":"introduced_regression","scopeEvidence":"structured outcome"}]}`, ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, CommentOnlyPublish: true, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, RequireReviewRequest: false, Labels: []string{"looper:review"}, LabelMode: config.LabelModeAll}, LoopConfig: testReviewerLoopConfig()})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "reviewer-worker-1", "reviewer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed reviewer item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(github.issueCommentCalls) != 1 {
		t.Fatalf("issueCommentCalls = %#v, want exactly one comment", github.issueCommentCalls)
	}
	parsedSummary, err := forge.ParseReviewerSummary(github.issueCommentCalls[0].Body)
	if err != nil {
		t.Fatalf("ParseReviewerSummary() error = %v", err)
	}
	if len(parsedSummary.Items) != 1 || parsedSummary.Items[0].Status != forge.ReviewItemStatusOpen {
		t.Fatalf("parsed summary = %#v, want one open item", parsedSummary)
	}
	if len(github.addReactionCalls) != 0 {
		t.Fatalf("addReactionCalls = %#v, want no clean reaction side effects", github.addReactionCalls)
	}
	if len(github.removeLabelCalls) != 0 {
		t.Fatalf("removeLabelCalls = %#v, want no clean noop label removal", github.removeLabelCalls)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), *claim.LoopID)
	if err != nil || loop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", loop, err)
	}
	if !contains(*loop.MetadataJSON, `"lastOutputFingerprint":"`) {
		t.Fatalf("loop metadata = %s, want actionable output fingerprint recorded", *loop.MetadataJSON)
	}
}

func TestPublishCommentOnlyReviewFailsOnDuplicateReviewerSummaryComments(t *testing.T) {
	t.Parallel()
	existing := forge.NewReviewerSummary(1, []forge.ReviewItem{{ReviewItemID: "R-001", Status: forge.ReviewItemStatusOpen, Title: "Old", Body: "Body", LastSeenRoundID: 1}})
	body, err := renderReviewerSummaryComment(existing, "Existing")
	if err != nil {
		t.Fatalf("renderReviewerSummaryComment() error = %v", err)
	}
	github := &fakeGitHubGateway{labels: []string{"looper:review"}, reviewRequests: []string{}, currentLogin: "reviewer", issueComments: []map[string]any{{"id": int64(1), "body": body}, {"id": int64(2), "body": body}}}
	runner := New(Options{GitHub: github, CommentOnlyPublish: true})
	pending := pendingReviewCheckpoint{HeadSHA: "abc123", Summary: "No actionable findings", Outcome: "clean", ReviewerSummaryJSON: `{"summary":"No actionable findings","outcome":"clean","findings":[]}`}
	input := stepInput{Repo: "acme/looper", PRNumber: 42, Project: storage.ProjectRecord{RepoPath: "/tmp/repo"}}
	if err := runner.publishCommentOnlyReview(context.Background(), input, pending, PullRequestDetail{}); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("publishCommentOnlyReview() error = %v, want duplicate summary failure", err)
	}
}

func TestRequireReviewRequestForLoopIgnoresExistingFollowUpNewHead(t *testing.T) {
	t.Parallel()
	metadata := `{"followUpdates":true,"lastPublishedHeadSha":"old-head","loop":{"enabled":true}}`
	loop := storage.LoopRecord{ID: "loop_followup_authority", Type: "reviewer", MetadataJSON: &metadata}

	if requireReviewRequestForLoop(loop, true, "new-head") {
		t.Fatalf("requireReviewRequestForLoop() = true, want false for enabled follow-up on new head")
	}
}

func TestRunThreadResolutionStepCommentsAndResolvesObjectiveLooperThread(t *testing.T) {
	t.Parallel()
	policy := defaultThreadResolutionPolicy(t)
	policy.Enabled = true
	policy.Mode = config.ReviewerThreadResolutionModeResolveObjective
	github := &fakeGitHubGateway{currentLogin: "looper-bot", reviewRequests: []string{"looper-bot"}, reviewThreads: []ReviewThread{{ID: "thread_1", Comments: []ReviewThreadComment{{ID: "comment_1", Author: "looper-bot", Body: "Please update this. <!-- looper:stamp v=1 -->", CommitOID: "old-head"}}}}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Stdout: `{"decisions":[{"threadId":"thread_1","decision":"OBJECTIVELY_FIXED","evidence":"the nil check is now present","confidence":"HIGH"}]}`}}}
	runner := New(Options{GitHub: github, AgentExecutor: agent, ThreadResolution: policy, Now: func() time.Time { return time.Unix(0, 0).UTC() }})

	checkpoint, err := runner.runThreadResolutionStep(context.Background(), threadResolutionStepInput())
	if err != nil {
		t.Fatalf("runThreadResolutionStep() error = %v", err)
	}
	if checkpoint.ThreadResolution == nil || checkpoint.ThreadResolution.Commented != 1 || checkpoint.ThreadResolution.Resolved != 1 {
		t.Fatalf("ThreadResolution = %#v, want one comment and one resolution", checkpoint.ThreadResolution)
	}
	if len(github.addThreadReplyCalls) != 1 || !strings.Contains(github.addThreadReplyCalls[0].Body, "decision=objectively_fixed") {
		t.Fatalf("addThreadReplyCalls = %#v, want objective audit reply", github.addThreadReplyCalls)
	}
	if len(github.resolveThreadCalls) != 1 || github.resolveThreadCalls[0].ThreadID != "thread_1" {
		t.Fatalf("resolveThreadCalls = %#v, want thread_1 resolved", github.resolveThreadCalls)
	}
	if len(agent.starts) != 1 || !strings.Contains(agent.starts[0].IdempotencyKey, "thread_1") {
		t.Fatalf("agent starts = %#v, want thread id in idempotency key", agent.starts)
	}
}

func TestRunThreadResolutionStepIgnoresProviderWordsInSuccessfulSummaryJSON(t *testing.T) {
	t.Parallel()
	policy := defaultThreadResolutionPolicy(t)
	policy.Enabled = true
	policy.Mode = config.ReviewerThreadResolutionModeCommentOnly
	jsonOutput := `{"decisions":[{"threadId":"thread_1","decision":"needs_human","evidence":"the previous report mentioned service unavailable and overloaded capacity","confidence":"high"}]}`
	github := &fakeGitHubGateway{currentLogin: "looper-bot", reviewRequests: []string{"looper-bot"}, reviewThreads: []ReviewThread{{ID: "thread_1", Comments: []ReviewThreadComment{{ID: "comment_1", Author: "looper-bot", Body: "Please update this. <!-- looper:stamp v=1 -->", CommitOID: "old-head"}}}}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", ParseStatus: "missing", Summary: jsonOutput, Stdout: jsonOutput}}}
	runner := New(Options{GitHub: github, AgentExecutor: agent, ThreadResolution: policy, Now: func() time.Time { return time.Unix(0, 0).UTC() }})

	checkpoint, err := runner.runThreadResolutionStep(context.Background(), threadResolutionStepInput())
	if err != nil {
		t.Fatalf("runThreadResolutionStep() error = %v", err)
	}
	if checkpoint.ThreadResolution == nil || checkpoint.ThreadResolution.Commented != 1 {
		t.Fatalf("ThreadResolution = %#v, want one comment", checkpoint.ThreadResolution)
	}
	if len(github.addThreadReplyCalls) != 1 || !strings.Contains(github.addThreadReplyCalls[0].Body, "service unavailable") {
		t.Fatalf("addThreadReplyCalls = %#v, want evidence comment", github.addThreadReplyCalls)
	}
}

func TestTransientProviderMessageFromAgentResultIgnoresSuccessfulStdout(t *testing.T) {
	t.Parallel()

	result := AgentResult{Status: "completed", Summary: "classified threads", Stdout: `{"decisions":[{"threadId":"thread_1","decision":"needs_human","evidence":"the previous report mentioned service unavailable and overloaded capacity","confidence":"high"}]}`}

	if message := transientProviderMessageFromAgentResult(result); message != "" {
		t.Fatalf("transientProviderMessageFromAgentResult() = %q, want empty", message)
	}
}

func TestTransientProviderMessageFromAgentResultUsesSummaryAndStderr(t *testing.T) {
	t.Parallel()

	if message := transientProviderMessageFromAgentResult(AgentResult{Status: "completed", Summary: "server_is_overloaded"}); message == "" {
		t.Fatalf("transientProviderMessageFromAgentResult(summary overload) = empty, want message")
	}
	if message := transientProviderMessageFromAgentResult(AgentResult{Status: "completed", Stderr: "service unavailable"}); message == "" {
		t.Fatalf("transientProviderMessageFromAgentResult(stderr overload) = empty, want message")
	}
}

func TestRunThreadResolutionStepResolvesLooperThreadWhenCurrentUserRequestNotRequired(t *testing.T) {
	t.Parallel()
	policy := defaultThreadResolutionPolicy(t)
	policy.Enabled = true
	policy.Mode = config.ReviewerThreadResolutionModeResolveObjective
	policy.RequireCurrentReviewRequest = false
	github := &fakeGitHubGateway{currentLogin: "looper-bot", reviewRequests: []string{"alice"}, reviewThreads: []ReviewThread{{ID: "thread_1", Comments: []ReviewThreadComment{{ID: "comment_1", Author: "looper-bot", Body: "Please update this. <!-- looper:stamp v=1 -->", CommitOID: "old-head"}}}}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Stdout: `{"decisions":[{"threadId":"thread_1","decision":"objectively_fixed","evidence":"the nil check is now present","confidence":"high"}]}`}}}
	runner := New(Options{GitHub: github, AgentExecutor: agent, ThreadResolution: policy, Now: func() time.Time { return time.Unix(0, 0).UTC() }})
	input := threadResolutionStepInput()
	input.Checkpoint.Detail.ReviewRequests = []string{"alice"}

	checkpoint, err := runner.runThreadResolutionStep(context.Background(), input)
	if err != nil {
		t.Fatalf("runThreadResolutionStep() error = %v", err)
	}
	if checkpoint.ThreadResolution == nil || checkpoint.ThreadResolution.Commented != 1 || checkpoint.ThreadResolution.Resolved != 1 {
		t.Fatalf("ThreadResolution = %#v, want one comment and one resolution", checkpoint.ThreadResolution)
	}
	if len(github.resolveThreadCalls) != 1 || github.resolveThreadCalls[0].ThreadID != "thread_1" {
		t.Fatalf("resolveThreadCalls = %#v, want thread_1 resolved", github.resolveThreadCalls)
	}
}

func TestRunThreadResolutionStepRechecksCurrentReviewRequestBeforeThreadAction(t *testing.T) {
	t.Parallel()
	policy := defaultThreadResolutionPolicy(t)
	policy.Enabled = true
	policy.Mode = config.ReviewerThreadResolutionModeResolveObjective
	policy.RequireCurrentReviewRequest = true
	github := &fakeGitHubGateway{currentLogin: "looper-bot", reviewRequests: []string{"alice"}, reviewThreads: []ReviewThread{{ID: "thread_1", Comments: []ReviewThreadComment{{ID: "comment_1", Author: "looper-bot", Body: "Please update this. <!-- looper:stamp v=1 -->", CommitOID: "old-head"}}}}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Stdout: `{"decisions":[{"threadId":"thread_1","decision":"objectively_fixed","evidence":"the nil check is now present","confidence":"high"}]}`}}}
	runner := New(Options{GitHub: github, AgentExecutor: agent, ThreadResolution: policy, Now: func() time.Time { return time.Unix(0, 0).UTC() }})
	input := threadResolutionStepInput()
	input.Checkpoint.Detail.ReviewRequests = []string{"looper-bot"}

	checkpoint, err := runner.runThreadResolutionStep(context.Background(), input)
	if err != nil {
		t.Fatalf("runThreadResolutionStep() error = %v", err)
	}
	if checkpoint.ThreadResolution == nil || checkpoint.ThreadResolution.Reported != 1 || checkpoint.ThreadResolution.Processed != 1 || checkpoint.ThreadResolution.Commented != 0 || checkpoint.ThreadResolution.Resolved != 0 {
		t.Fatalf("ThreadResolution = %#v, want candidate processed but no thread action", checkpoint.ThreadResolution)
	}
	if len(github.addThreadReplyCalls) != 0 || len(github.resolveThreadCalls) != 0 {
		t.Fatalf("side effects: replies=%d resolves=%d, want none", len(github.addThreadReplyCalls), len(github.resolveThreadCalls))
	}
}

func TestRunThreadResolutionStepRequiresNewHeadAfterLatestThreadFeedback(t *testing.T) {
	t.Parallel()
	policy := defaultThreadResolutionPolicy(t)
	policy.Enabled = true
	policy.Mode = config.ReviewerThreadResolutionModeResolveObjective
	github := &fakeGitHubGateway{currentLogin: "looper-bot", reviewRequests: []string{"looper-bot"}, reviewThreads: []ReviewThread{{ID: "thread_1", Comments: []ReviewThreadComment{
		{ID: "comment_1", Author: "looper-bot", Body: "Please update this. <!-- looper:stamp v=1 -->", CommitOID: "old-head"},
		{ID: "comment_2", Author: "octocat", Body: "Fixed in the latest push.", CommitOID: "abc123"},
	}}}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Stdout: `{"decisions":[{"threadId":"thread_1","decision":"objectively_fixed","evidence":"the nil check is now present","confidence":"high"}]}`}}}
	runner := New(Options{GitHub: github, AgentExecutor: agent, ThreadResolution: policy, Now: func() time.Time { return time.Unix(0, 0).UTC() }})

	checkpoint, err := runner.runThreadResolutionStep(context.Background(), threadResolutionStepInput())
	if err != nil {
		t.Fatalf("runThreadResolutionStep() error = %v", err)
	}
	if checkpoint.ThreadResolution == nil || checkpoint.ThreadResolution.Reported != 0 || checkpoint.ThreadResolution.Processed != 0 {
		t.Fatalf("ThreadResolution = %#v, want no eligible candidates", checkpoint.ThreadResolution)
	}
	if len(agent.starts) != 0 || len(github.addThreadReplyCalls) != 0 || len(github.resolveThreadCalls) != 0 {
		t.Fatalf("side effects: agent=%d replies=%d resolves=%d, want none", len(agent.starts), len(github.addThreadReplyCalls), len(github.resolveThreadCalls))
	}
}

func TestRunThreadResolutionStepSupersedesNonObjectiveAuditForObjectiveDecision(t *testing.T) {
	t.Parallel()
	policy := defaultThreadResolutionPolicy(t)
	policy.Enabled = true
	policy.Mode = config.ReviewerThreadResolutionModeResolveObjective
	github := &fakeGitHubGateway{currentLogin: "looper-bot", reviewRequests: []string{"looper-bot"}, reviewThreads: []ReviewThread{{ID: "thread_1", Comments: []ReviewThreadComment{
		{ID: "comment_1", Author: "looper-bot", Body: "Please update this. <!-- looper:stamp v=1 -->", CommitOID: "old-head"},
		{ID: "comment_2", Author: "looper-bot", Body: "Looper checked this thread. <!-- looper:thread-resolution thread=thread_1 head=abc123 decision=needs_human -->", CommitOID: "abc123"},
	}}}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Stdout: `{"decisions":[{"threadId":"thread_1","decision":"objectively_fixed","evidence":"the nil check is now present","confidence":"high"}]}`}}}
	runner := New(Options{GitHub: github, AgentExecutor: agent, ThreadResolution: policy, Now: func() time.Time { return time.Unix(0, 0).UTC() }})

	checkpoint, err := runner.runThreadResolutionStep(context.Background(), threadResolutionStepInput())
	if err != nil {
		t.Fatalf("runThreadResolutionStep() error = %v", err)
	}
	if checkpoint.ThreadResolution == nil || checkpoint.ThreadResolution.Commented != 1 || checkpoint.ThreadResolution.Resolved != 1 {
		t.Fatalf("ThreadResolution = %#v, want superseding objective comment and resolution", checkpoint.ThreadResolution)
	}
	if len(github.addThreadReplyCalls) != 1 || !strings.Contains(github.addThreadReplyCalls[0].Body, "decision=objectively_fixed") {
		t.Fatalf("addThreadReplyCalls = %#v, want objective audit reply", github.addThreadReplyCalls)
	}
}

func TestRunThreadResolutionStepRequiresLooperAuthoredMarker(t *testing.T) {
	t.Parallel()
	policy := defaultThreadResolutionPolicy(t)
	policy.Enabled = true
	policy.Mode = config.ReviewerThreadResolutionModeResolveObjective
	github := &fakeGitHubGateway{currentLogin: "looper-bot", reviewRequests: []string{"looper-bot"}, reviewThreads: []ReviewThread{{ID: "thread_1", Comments: []ReviewThreadComment{{ID: "comment_1", Author: "looper-bot", Body: "Manual review comment without Looper marker", CommitOID: "old-head"}}}}}
	agent := &fakeAgentExecutor{}
	runner := New(Options{GitHub: github, AgentExecutor: agent, ThreadResolution: policy, Now: func() time.Time { return time.Unix(0, 0).UTC() }})

	checkpoint, err := runner.runThreadResolutionStep(context.Background(), threadResolutionStepInput())
	if err != nil {
		t.Fatalf("runThreadResolutionStep() error = %v", err)
	}
	if checkpoint.ThreadResolution == nil || checkpoint.ThreadResolution.Reported != 0 || checkpoint.ThreadResolution.Processed != 0 {
		t.Fatalf("ThreadResolution = %#v, want no eligible candidates", checkpoint.ThreadResolution)
	}
	if len(agent.starts) != 0 || len(github.addThreadReplyCalls) != 0 || len(github.resolveThreadCalls) != 0 {
		t.Fatalf("side effects: agent=%d replies=%d resolves=%d, want none", len(agent.starts), len(github.addThreadReplyCalls), len(github.resolveThreadCalls))
	}
}

func TestRunThreadResolutionStepRestartsFromDiscoverOnHeadChange(t *testing.T) {
	t.Parallel()
	policy := defaultThreadResolutionPolicy(t)
	policy.Enabled = true
	policy.Mode = config.ReviewerThreadResolutionModeResolveObjective
	github := &fakeGitHubGateway{currentLogin: "looper-bot", reviewRequests: []string{"looper-bot"}, viewHeadSHA: "new-head", reviewThreads: []ReviewThread{{ID: "thread_1", Comments: []ReviewThreadComment{{ID: "comment_1", Author: "looper-bot", Body: "Please update this. <!-- looper:stamp v=1 -->", CommitOID: "old-head"}}}}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Stdout: `{"decisions":[{"threadId":"thread_1","decision":"objectively_fixed","evidence":"the nil check is now present","confidence":"high"}]}`}}}
	runner := New(Options{GitHub: github, AgentExecutor: agent, ThreadResolution: policy, Now: func() time.Time { return time.Unix(0, 0).UTC() }})

	checkpoint, err := runner.runThreadResolutionStep(context.Background(), threadResolutionStepInput())
	if err == nil || !strings.Contains(err.Error(), "PR changed during thread reconciliation") {
		t.Fatalf("runThreadResolutionStep() error = %v, want head-change reconciliation error", err)
	}
	if checkpoint.ResumePolicy != "restart_from_discover" {
		t.Fatalf("ResumePolicy = %q, want restart_from_discover", checkpoint.ResumePolicy)
	}
	if len(github.addThreadReplyCalls) != 0 || len(github.resolveThreadCalls) != 0 {
		t.Fatalf("side effects: replies=%d resolves=%d, want none", len(github.addThreadReplyCalls), len(github.resolveThreadCalls))
	}
}

func defaultThreadResolutionPolicy(t *testing.T) config.ReviewerThreadResolutionConfig {
	t.Helper()
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	return cfg.Roles.Reviewer.Behavior.ThreadResolution
}

func threadResolutionStepInput() stepInput {
	repo := "acme/looper"
	prNumber := int64(42)
	return stepInput{
		Project:  storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp/repo"},
		Loop:     storage.LoopRecord{ID: "loop_1", ProjectID: "project_1", Type: "reviewer", Repo: &repo, PRNumber: &prNumber},
		Run:      storage.RunRecord{ID: "run_1", LoopID: "loop_1"},
		Repo:     repo,
		PRNumber: prNumber,
		Checkpoint: reviewerCheckpoint{
			Detail:   &checkpointDetail{State: "OPEN", HeadSHA: "abc123", ReviewRequests: []string{"looper-bot"}},
			Snapshot: &checkpointSnapshot{HeadSHA: "abc123"},
			Worktree: &checkpointWorktree{Path: "/tmp/repo", HeadSHA: "abc123"},
		},
	}
}

type runnerFixture struct {
	coordinator *storage.SQLiteCoordinator
	repos       *storage.Repositories
	logger      *testLogger
	current     time.Time
	now         func() time.Time
}

func newRunnerFixture(t *testing.T) *runnerFixture {
	t.Helper()
	coordinator, err := storage.OpenSQLiteCoordinator(context.Background(), filepath.Join(t.TempDir(), "reviewer.sqlite"), storage.SQLiteCoordinatorOptions{BackupDir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenSQLiteCoordinator() error = %v", err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })
	if _, err := coordinator.MigrationRunner().RunPending(context.Background()); err != nil {
		t.Fatalf("RunPending() error = %v", err)
	}
	repos := storage.NewRepositories(coordinator.DB())
	now := time.Date(2026, time.April, 11, 12, 0, 0, 0, time.UTC)
	nowISO := fmt.Sprintf("%s.000Z", now.Format("2006-01-02T15:04:05"))
	baseBranch := "main"
	if err := repos.Projects.Upsert(context.Background(), storage.ProjectRecord{ID: "project_1", Name: "Looper", RepoPath: filepath.Join(t.TempDir(), "repo"), BaseBranch: &baseBranch, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Projects.Upsert() error = %v", err)
	}
	fixture := &runnerFixture{coordinator: coordinator, repos: repos, logger: &testLogger{}, current: now}
	fixture.now = func() time.Time { return fixture.current }
	return fixture
}

func testReviewerLoopConfig() config.ReviewerLoopConfig {
	return config.ReviewerLoopConfig{EnabledByDefault: true, QuietPeriodSeconds: 60, MinPublishIntervalSeconds: 300, MaxIterationsPerPR: 20, MaxIterationsPerHead: 1, MaxWallClockSeconds: 14400, MaxConsecutiveFailures: 3, MaxAgentExecutionsPerPR: 25, StopOnApproved: false, StopOnReadyLabel: true, StopOnIdenticalOutput: true}
}

func (f *runnerFixture) advance(delta time.Duration) { f.current = f.current.Add(delta) }

func (f *runnerFixture) nowISO() string {
	return fmt.Sprintf("%s.000Z", f.current.UTC().Format("2006-01-02T15:04:05"))
}

func boolPtr(value bool) *bool { return &value }

type stubCriteriaVerifier struct {
	responses map[criteria.AcceptanceCriterion]criteria.CriterionAssessment
}

func (s stubCriteriaVerifier) VerifyCriterion(criterion criteria.AcceptanceCriterion, _ criteria.PRDiff) (criteria.CriterionAssessment, error) {
	if assessment, ok := s.responses[criterion]; ok {
		return assessment, nil
	}
	return criteria.CriterionAssessment{Verdict: criteria.VerdictUnverifiable, Justification: "missing test verifier response"}, nil
}

func reviewerAutoMergeTestConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Roles.Reviewer.AutoMerge.Enabled = true
	cfg.Roles.Reviewer.AutoMerge.Strategy = config.ReviewerAutoMergeStrategySquash
	return &cfg
}

type failedReviewerRecoverySeed struct {
	ResumePolicy         string
	QueueErrorKind       string
	ErrorMessage         string
	ConsecutiveFailures  int
	AutoRecoveryAttempts int
	TerminationReason    string
	FollowUpdates        *bool
	LoopEnabled          *bool
	QueueAttempts        int64
	QueueMaxAttempts     int64
}

func seedFailedReviewerRecoveryLoop(t *testing.T, fixture *runnerFixture, seed failedReviewerRecoverySeed) (string, string) {
	t.Helper()
	ctx := context.Background()
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	targetID := "pr:acme/looper:42"
	loopID := "loop_recover_reviewer"
	queueID := "queue_recover_reviewer"
	consecutive := seed.ConsecutiveFailures
	if consecutive == 0 {
		consecutive = 1
	}
	loopEnabled := true
	if seed.LoopEnabled != nil {
		loopEnabled = *seed.LoopEnabled
	}
	loopMeta := map[string]any{"enabled": loopEnabled, "failureCount": consecutive, "consecutiveFailures": consecutive, "lastFailure": seed.ErrorMessage, "autoRecoveryAttempts": seed.AutoRecoveryAttempts}
	if seed.TerminationReason != "" {
		loopMeta["status"] = "terminated"
		loopMeta["terminationReason"] = seed.TerminationReason
	}
	queueAttempts := seed.QueueAttempts
	if queueAttempts == 0 {
		queueAttempts = 3
	}
	queueMaxAttempts := seed.QueueMaxAttempts
	if queueMaxAttempts == 0 {
		queueMaxAttempts = 3
	}
	metadataMap := map[string]any{"loop": loopMeta}
	if seed.FollowUpdates != nil {
		metadataMap["followUpdates"] = *seed.FollowUpdates
	}
	metadata := mustMarshalJSON(metadataMap)
	if err := fixture.repos.Loops.Upsert(ctx, storage.LoopRecord{ID: loopID, Seq: 165, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &targetID, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	checkpoint := mustMarshalJSON(reviewerCheckpoint{ResumePolicy: seed.ResumePolicy, Detail: &checkpointDetail{State: "OPEN", HeadSHA: "abc123", ReviewRequests: []string{"octocat"}}})
	if err := fixture.repos.Runs.Upsert(ctx, storage.RunRecord{ID: "run_recover_reviewer", LoopID: loopID, Status: "failed", CurrentStep: stringPtr(string(stepPublish)), CheckpointJSON: &checkpoint, Summary: &seed.ErrorMessage, ErrorMessage: &seed.ErrorMessage, StartedAt: nowISO, EndedAt: &nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	if err := fixture.repos.Queue.Upsert(ctx, storage.QueueItemRecord{ID: queueID, ProjectID: stringPtr("project_1"), LoopID: &loopID, Type: "reviewer", TargetType: "pull_request", TargetID: targetID, Repo: &repo, PRNumber: &prNumber, DedupeKey: buildReviewerDedupeKey("project_1", loopID, repo, prNumber), Priority: storage.QueuePriorityReviewer, Status: "failed", AvailableAt: nowISO, Attempts: queueAttempts, MaxAttempts: queueMaxAttempts, FinishedAt: &nowISO, LastError: &seed.ErrorMessage, LastErrorKind: &seed.QueueErrorKind, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	return loopID, queueID
}

type fakeGitHubGateway struct {
	changeHeadOnSecondView          bool
	listHeadSHA                     string
	removeReviewRequestOnSecondView bool
	viewCalls                       int
	loadReviewsCalls                int
	// omitReviewsOnView simulates DiscoverySnapshot/fixer-profile views that
	// intentionally omit the reviews field while engagement recovery can still
	// load them via LoadPullRequestReviews.
	omitReviewsOnView               bool
	author                          string
	labels                          []string
	reviewDecision                  string
	comments                        []map[string]any
	issueComments                   []map[string]any
	reviews                         []map[string]any
	hasConflicts                    bool
	useReviewStateAfterFirstView    bool
	reviewDecisionAfterFirstView    string
	commentsAfterFirstView          []map[string]any
	reviewRequests                  []string
	reviewRequestsUnknown           bool
	currentLogin                    string
	currentLoginErr                 error
	currentLoginCalls               int
	reviewRequestUsers              []networkpolicy.GitHubUser
	reviewMarkerMissing             bool
	reviewMarkerExactMissing        bool
	reviewMarkerErr                 error
	reviewMarkerEvent               ReviewEvent
	reviewMarkerOutcome             string
	reviewMarkerBody                string
	reviewMarkerBodyExplicit        bool
	reviewMarkerInlineCommentBodies []string
	reviewMarkerCalls               int
	reviewMarkerInputs              []VerifyReviewMarkerInput
	issueDetail                     githubinfra.IssueDetail
	repositorySettings              githubinfra.RepositorySettings
	repositorySettingsCalls         int
	branchProtection                githubinfra.BranchProtection
	branchProtectionCalls           int
	viewDraft                       bool
	viewBody                        string
	viewDiff                        string
	viewState                       string
	viewStateAfterFirstView         string
	closeAfterReviewMarker          bool
	closeOnNextView                 bool
	viewErrs                        []error
	issueDetailErr                  error
	addReactionErr                  error
	removeReactionErr               error
	addLabelErr                     error
	removeLabelErr                  error
	issueCommentErr                 error
	issueCommentResult              IssueCommentResult
	listIssueCommentsErr            error
	updateIssueCommentErr           error
	reviewThreads                   []ReviewThread
	listReviewThreadsCalls          int
	listReviewThreadsErr            error
	listReviewThreadsErrAfter       int // fail on call N (1-based); 0 = always if err set
	viewHeadSHA                     string
	headSHACalls                    int
	issueCommentCalls               []IssueCommentInput
	updateIssueCommentCalls         []UpdateIssueCommentInput
	submitReviewCalls               []githubinfra.SubmitReviewInput
	enableAutoMergeCalls            []githubinfra.EnableAutoMergeInput
	removeIssueLabelCalls           []githubinfra.IssueLabelsInput
	captureSnapshotErrs             []error
	captureSnapshotCalls            int
	addThreadReplyCalls             []AddReviewThreadReplyInput
	addThreadReplyErr               error
	resolveThreadCalls              []ResolveReviewThreadInput
	resolveThreadErr                error
	resolveThreadErrTimes           int // fail this many times then succeed; 0 with err = always
	resolveThreadFailCount          int
	addReactionCalls                []PullRequestReactionInput
	removeReactionCalls             []PullRequestReactionInput
	addLabelCalls                   []PullRequestLabelsInput
	removeLabelCalls                []PullRequestLabelsInput
	listCalls                       []ListOpenPullRequestsInput
	listReviewRequestedCalls        []ListReviewRequestedPullRequestsInput
	reviewRequestedPullRequests     []PullRequestSummary
	listOpenByLabel                 map[string][]PullRequestSummary
}

func (g *fakeGitHubGateway) ListOpenPullRequests(_ context.Context, input ListOpenPullRequestsInput) ([]PullRequestSummary, error) {
	g.listCalls = append(g.listCalls, input)
	if g.listOpenByLabel != nil {
		return append([]PullRequestSummary(nil), g.listOpenByLabel[input.Label]...), nil
	}
	reviewRequests := g.effectiveReviewRequests()
	headSHA := g.listHeadSHA
	if headSHA == "" {
		headSHA = "abc123"
	}
	author := g.effectiveAuthor()
	users := append([]networkpolicy.GitHubUser(nil), g.reviewRequestUsers...)
	if len(users) == 0 && reviewRequests != nil {
		users = make([]networkpolicy.GitHubUser, 0, len(reviewRequests))
		for _, login := range reviewRequests {
			users = append(users, networkpolicy.GitHubUser{Login: login})
		}
	}
	return []PullRequestSummary{{Number: 42, Title: "Review me", State: "OPEN", ReviewDecision: g.reviewDecision, Labels: append([]string(nil), g.labels...), HeadSHA: headSHA, BaseSHA: "base123", HasConflicts: g.hasConflicts, Author: author, ReviewRequests: reviewRequests, ReviewRequestUsers: users, Reviews: cloneCommentMaps(g.reviews)}, {Number: 99, Title: "Draft", State: "OPEN", IsDraft: true, HeadSHA: "draft123", BaseSHA: "base123", Author: author, ReviewRequests: reviewRequests, ReviewRequestUsers: users}}, nil
}

func (g *fakeGitHubGateway) ListReviewRequestedPullRequests(_ context.Context, input ListReviewRequestedPullRequestsInput) ([]PullRequestSummary, error) {
	g.listReviewRequestedCalls = append(g.listReviewRequestedCalls, input)
	if g.reviewRequestedPullRequests != nil {
		return append([]PullRequestSummary(nil), g.reviewRequestedPullRequests...), nil
	}
	if g.listOpenByLabel != nil {
		return append([]PullRequestSummary(nil), g.listOpenByLabel[""]...), nil
	}
	reviewRequests := g.effectiveReviewRequests()
	headSHA := g.listHeadSHA
	if headSHA == "" {
		headSHA = "abc123"
	}
	author := g.effectiveAuthor()
	users := append([]networkpolicy.GitHubUser(nil), g.reviewRequestUsers...)
	if len(users) == 0 && reviewRequests != nil {
		users = make([]networkpolicy.GitHubUser, 0, len(reviewRequests))
		for _, login := range reviewRequests {
			users = append(users, networkpolicy.GitHubUser{Login: login})
		}
	}
	return []PullRequestSummary{{Number: 42, Title: "Review me", State: "OPEN", ReviewDecision: g.reviewDecision, Labels: append([]string(nil), g.labels...), HeadSHA: headSHA, BaseSHA: "base123", HasConflicts: g.hasConflicts, Author: author, ReviewRequests: reviewRequests, ReviewRequestUsers: users, Reviews: cloneCommentMaps(g.reviews)}, {Number: 99, Title: "Draft", State: "OPEN", IsDraft: true, HeadSHA: "draft123", BaseSHA: "base123", Author: author, ReviewRequests: reviewRequests, ReviewRequestUsers: users}}, nil
}

func (g *fakeGitHubGateway) GetCurrentUserLogin(context.Context, string) (string, error) {
	g.currentLoginCalls++
	if g.currentLoginErr != nil {
		return "", g.currentLoginErr
	}
	if strings.TrimSpace(g.currentLogin) != "" {
		return g.currentLogin, nil
	}
	return "octocat", nil
}

func (g *fakeGitHubGateway) ViewPullRequest(context.Context, ViewPullRequestInput) (PullRequestDetail, error) {
	g.viewCalls++
	if len(g.viewErrs) > 0 {
		err := g.viewErrs[0]
		g.viewErrs = g.viewErrs[1:]
		if err != nil {
			return PullRequestDetail{}, err
		}
	}
	headSHA := "abc123"
	if g.viewHeadSHA != "" {
		headSHA = g.viewHeadSHA
	}
	if g.changeHeadOnSecondView && g.viewCalls >= 2 {
		headSHA = "new-head"
	}
	reviewRequests := g.effectiveReviewRequests()
	if g.removeReviewRequestOnSecondView && g.viewCalls >= 2 {
		reviewRequests = []string{}
	}
	reviewDecision := g.reviewDecision
	comments := g.comments
	if g.useReviewStateAfterFirstView && g.viewCalls > 1 {
		reviewDecision = g.reviewDecisionAfterFirstView
		comments = g.commentsAfterFirstView
	}
	state := g.viewState
	if g.viewStateAfterFirstView != "" && g.viewCalls > 1 {
		state = g.viewStateAfterFirstView
	}
	if g.closeAfterReviewMarker && g.closeOnNextView {
		state = "CLOSED"
	}
	if state == "" {
		state = "OPEN"
	}
	body := g.viewBody
	if body == "" {
		body = "PR body"
	}
	diff := g.viewDiff
	if diff == "" {
		diff = "diff --git a/a.ts b/a.ts"
	}
	users := append([]networkpolicy.GitHubUser(nil), g.reviewRequestUsers...)
	if len(users) == 0 && reviewRequests != nil {
		users = make([]networkpolicy.GitHubUser, 0, len(reviewRequests))
		for _, login := range reviewRequests {
			users = append(users, networkpolicy.GitHubUser{Login: login})
		}
	}
	reviews := cloneCommentMaps(g.reviews)
	if g.omitReviewsOnView {
		reviews = nil
	}
	return PullRequestDetail{Number: 42, Title: "Review me", Body: body, State: state, IsDraft: g.viewDraft, ReviewDecision: reviewDecision, Labels: append([]string(nil), g.labels...), HeadSHA: headSHA, BaseSHA: "base123", HeadRefName: "feature/review-me", BaseRefName: "main", Author: g.effectiveAuthor(), ReviewRequests: reviewRequests, ReviewRequestUsers: users, HasConflicts: g.hasConflicts, ChecksSummary: "SUCCESS", Diff: diff, Comments: cloneCommentMaps(comments), IssueComments: cloneCommentMaps(g.issueComments), Reviews: reviews}, nil
}

func (g *fakeGitHubGateway) LoadPullRequestReviews(context.Context, ViewPullRequestInput) ([]map[string]any, error) {
	g.loadReviewsCalls++
	return cloneCommentMaps(g.reviews), nil
}

func (g *fakeGitHubGateway) ViewIssue(_ context.Context, input githubinfra.ViewIssueInput) (githubinfra.IssueDetail, error) {
	if g.issueDetailErr != nil {
		return githubinfra.IssueDetail{}, g.issueDetailErr
	}
	if g.issueDetail.Number != 0 {
		return g.issueDetail, nil
	}
	return githubinfra.IssueDetail{Number: input.IssueNumber, Title: "Issue", Body: "", State: "open", Labels: []string{"triaged", "dispatch/plan"}}, nil
}

func (g *fakeGitHubGateway) GetRepositorySettings(context.Context, githubinfra.RepositorySettingsInput) (githubinfra.RepositorySettings, error) {
	g.repositorySettingsCalls++
	if g.repositorySettings.AllowSquashMerge || g.repositorySettings.AllowMergeCommit || g.repositorySettings.AllowRebaseMerge || g.repositorySettings.AllowAutoMerge {
		return g.repositorySettings, nil
	}
	return githubinfra.RepositorySettings{AllowSquashMerge: true, AllowMergeCommit: true, AllowRebaseMerge: true, AllowAutoMerge: true}, nil
}

func (g *fakeGitHubGateway) GetBranchProtection(context.Context, githubinfra.BranchProtectionInput) (githubinfra.BranchProtection, error) {
	g.branchProtectionCalls++
	if g.branchProtection.Enabled || g.branchProtection.HasRequiredChecks {
		return g.branchProtection, nil
	}
	return githubinfra.BranchProtection{Enabled: true, HasRequiredChecks: true}, nil
}

func (g *fakeGitHubGateway) GetPullRequestHeadSHA(context.Context, ViewPullRequestInput) (string, error) {
	g.headSHACalls++
	headSHA := "abc123"
	if g.viewHeadSHA != "" {
		headSHA = g.viewHeadSHA
	}
	if g.changeHeadOnSecondView && g.viewCalls+g.headSHACalls >= 2 {
		headSHA = "new-head"
	}
	return headSHA, nil
}

func cloneCommentMaps(comments []map[string]any) []map[string]any {
	if comments == nil {
		return nil
	}
	cloned := make([]map[string]any, 0, len(comments))
	for _, comment := range comments {
		clonedComment := make(map[string]any, len(comment))
		for key, value := range comment {
			clonedComment[key] = value
		}
		cloned = append(cloned, clonedComment)
	}
	return cloned
}

func (g *fakeGitHubGateway) effectiveReviewRequests() []string {
	if g.reviewRequestsUnknown {
		return nil
	}
	if g.reviewRequests != nil {
		reviewRequests := make([]string, len(g.reviewRequests))
		copy(reviewRequests, g.reviewRequests)
		return reviewRequests
	}
	return []string{"octocat"}
}

func (g *fakeGitHubGateway) effectiveAuthor() string {
	if strings.TrimSpace(g.author) != "" {
		return g.author
	}
	return "alice"
}

func (g *fakeGitHubGateway) CapturePullRequestSnapshot(_ context.Context, input CapturePullRequestSnapshotInput) (storage.PullRequestSnapshotRecord, error) {
	g.captureSnapshotCalls++
	if len(g.captureSnapshotErrs) > 0 {
		err := g.captureSnapshotErrs[0]
		g.captureSnapshotErrs = g.captureSnapshotErrs[1:]
		if err != nil {
			return storage.PullRequestSnapshotRecord{}, err
		}
	}
	headSHA := "abc123"
	if g.viewHeadSHA != "" {
		headSHA = g.viewHeadSHA
	}
	if g.changeHeadOnSecondView && g.viewCalls >= 2 {
		headSHA = "new-head"
	}
	return storage.PullRequestSnapshotRecord{ID: fmt.Sprintf("snapshot:%d:%s", input.PRNumber, input.CapturedAt), ProjectID: input.ProjectID, Repo: input.Repo, PRNumber: input.PRNumber, HeadSHA: headSHA, BaseSHA: stringPtr("base123"), Title: stringPtr("Review me"), Body: stringPtr("PR body"), Author: stringPtr(g.effectiveAuthor()), ChecksSummary: stringPtr("SUCCESS"), PayloadJSON: stringPtr(`{"diff":"diff --git a/a.ts b/a.ts"}`), CapturedAt: input.CapturedAt, CreatedAt: input.CapturedAt}, nil
}

func (g *fakeGitHubGateway) FindReviewMarker(_ context.Context, input VerifyReviewMarkerInput) (ReviewMarkerResult, error) {
	g.reviewMarkerCalls++
	g.reviewMarkerInputs = append(g.reviewMarkerInputs, input)
	if g.closeAfterReviewMarker {
		g.closeOnNextView = true
	}
	if g.reviewMarkerErr != nil {
		return ReviewMarkerResult{}, g.reviewMarkerErr
	}
	for i := len(g.reviews) - 1; i >= 0; i-- {
		row := g.reviews[i]
		body, _ := row["body"].(string)
		if !strings.Contains(body, input.Marker) {
			continue
		}
		state, _ := row["state"].(string)
		event := ReviewEventComment
		switch state {
		case "APPROVED":
			event = ReviewEventApprove
		case "CHANGES_REQUESTED":
			event = ReviewEventRequestChanges
		}
		if !reviewEventIn(input.AllowedReviewEvents, event) {
			continue
		}
		outcome := "actionable"
		if strings.Contains(body, "outcome=clean") {
			outcome = "clean"
		} else if strings.Contains(body, "outcome=blocking") {
			outcome = "blocking"
		} else if strings.Contains(body, "outcome=non_blocking") {
			outcome = "non_blocking"
		}
		return ReviewMarkerResult{Found: true, Outcome: outcome, Event: event, Body: body, InlineCommentBodies: append([]string(nil), g.reviewMarkerInlineCommentBodies...)}, nil
	}
	if g.reviewMarkerExactMissing && strings.Contains(input.Marker, " id=") {
		return ReviewMarkerResult{}, nil
	}
	if g.reviewMarkerEvent != "" && !reviewEventIn(input.AllowedReviewEvents, g.reviewMarkerEvent) {
		return ReviewMarkerResult{}, nil
	}
	if g.reviewMarkerMissing {
		return ReviewMarkerResult{}, nil
	}
	outcome := g.reviewMarkerOutcome
	if outcome == "" {
		outcome = "actionable"
	}
	body := g.reviewMarkerBody
	if body == "" && !g.reviewMarkerBodyExplicit {
		if outcome == "clean" && g.reviewMarkerEvent == ReviewEventApprove {
			body = cleanApproveReviewBody(g.effectiveAuthor(), outcome)
		} else {
			body = "review body <!-- looper:review outcome=" + outcome + " -->"
		}
	}
	return ReviewMarkerResult{Found: true, Outcome: outcome, Event: g.reviewMarkerEvent, Body: body, InlineCommentBodies: append([]string(nil), g.reviewMarkerInlineCommentBodies...)}, nil
}

func (g *fakeGitHubGateway) CreateIssueComment(_ context.Context, input IssueCommentInput) (IssueCommentResult, error) {
	g.issueCommentCalls = append(g.issueCommentCalls, input)
	if g.issueCommentErr != nil {
		return IssueCommentResult{}, g.issueCommentErr
	}
	if g.issueCommentResult.ID != 0 || g.issueCommentResult.URL != "" {
		return g.issueCommentResult, nil
	}
	return IssueCommentResult{ID: int64(len(g.issueCommentCalls)), URL: fmt.Sprintf("https://github.com/%s/pull/%d#issuecomment-%d", input.Repo, input.IssueNumber, len(g.issueCommentCalls))}, nil
}

func (g *fakeGitHubGateway) ListIssueComments(_ context.Context, input ViewPullRequestInput) ([]IssueComment, error) {
	if g.listIssueCommentsErr != nil {
		return nil, g.listIssueCommentsErr
	}
	out := make([]IssueComment, 0, len(g.issueComments))
	for i, comment := range g.issueComments {
		body, _ := comment["body"].(string)
		id, _ := comment["id"].(int64)
		if id == 0 {
			id = int64(i + 1)
		}
		out = append(out, IssueComment{ID: id, Body: body})
	}
	_ = input
	return out, nil
}

func (g *fakeGitHubGateway) UpdateIssueComment(_ context.Context, input UpdateIssueCommentInput) error {
	g.updateIssueCommentCalls = append(g.updateIssueCommentCalls, input)
	if g.updateIssueCommentErr != nil {
		return g.updateIssueCommentErr
	}
	for i := range g.issueComments {
		id, _ := g.issueComments[i]["id"].(int64)
		if id == 0 {
			id = int64(i + 1)
			g.issueComments[i]["id"] = id
		}
		if id == input.CommentID {
			g.issueComments[i]["body"] = input.Body
			return nil
		}
	}
	g.issueComments = append(g.issueComments, map[string]any{"id": input.CommentID, "body": input.Body})
	return nil
}

func (g *fakeGitHubGateway) SubmitReview(_ context.Context, input githubinfra.SubmitReviewInput) error {
	g.submitReviewCalls = append(g.submitReviewCalls, input)
	state := "COMMENTED"
	switch input.Event {
	case string(ReviewEventApprove):
		state = "APPROVED"
	case string(ReviewEventRequestChanges):
		state = "CHANGES_REQUESTED"
	}
	body := input.Body
	g.reviews = append(g.reviews, map[string]any{"body": body, "state": state, "user": map[string]any{"login": firstNonEmpty(strings.TrimSpace(g.currentLogin), "octocat")}})
	g.issueComments = append(g.issueComments, map[string]any{"body": body})
	return nil
}

func (g *fakeGitHubGateway) EnableAutoMerge(_ context.Context, input githubinfra.EnableAutoMergeInput) error {
	g.enableAutoMergeCalls = append(g.enableAutoMergeCalls, input)
	return nil
}

func cleanApproveReviewBody(author string, outcome string) string {
	return fmt.Sprintf("@%s Thanks for the thoughtful update — I verified the changes are clear, focused, and safe to approve. Nice work tightening this up; it should be easier to maintain going forward.\n\n<!-- looper:review id=abc head=abc123 outcome=%s -->", author, outcome)
}

func mustLoadReviewerRoleConfig(t *testing.T, contents string) config.Config {
	t.Helper()
	cwd := t.TempDir()
	configPath := filepath.Join(cwd, "config.json")
	if err := os.WriteFile(configPath, []byte(contents), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	loaded, err := config.LoadFile(config.LoadFileOptions{CWD: cwd, ConfigPath: configPath, LookupEnv: func(string) (string, bool) { return "", false }, LookPath: func(file string) (string, error) { return "/usr/bin/" + file, nil }})
	if err != nil {
		t.Fatalf("LoadFile() error = %v", err)
	}
	return loaded.Config
}

func (g *fakeGitHubGateway) AddPullRequestReaction(_ context.Context, input PullRequestReactionInput) error {
	g.addReactionCalls = append(g.addReactionCalls, input)
	return g.addReactionErr
}

func (g *fakeGitHubGateway) RemovePullRequestReaction(_ context.Context, input PullRequestReactionInput) error {
	g.removeReactionCalls = append(g.removeReactionCalls, input)
	return g.removeReactionErr
}

func (g *fakeGitHubGateway) AddPullRequestLabels(_ context.Context, input PullRequestLabelsInput) error {
	g.addLabelCalls = append(g.addLabelCalls, input)
	return g.addLabelErr
}

func (g *fakeGitHubGateway) RemovePullRequestLabels(_ context.Context, input PullRequestLabelsInput) error {
	g.removeLabelCalls = append(g.removeLabelCalls, input)
	return g.removeLabelErr
}

func (g *fakeGitHubGateway) RemoveIssueLabels(_ context.Context, input githubinfra.IssueLabelsInput) error {
	g.removeIssueLabelCalls = append(g.removeIssueLabelCalls, input)
	return nil
}

func (g *fakeGitHubGateway) ListReviewThreads(context.Context, ListReviewThreadsInput) ([]ReviewThread, error) {
	g.listReviewThreadsCalls++
	if g.listReviewThreadsErr != nil {
		if g.listReviewThreadsErrAfter == 0 || g.listReviewThreadsCalls == g.listReviewThreadsErrAfter {
			return nil, g.listReviewThreadsErr
		}
	}
	out := make([]ReviewThread, len(g.reviewThreads))
	copy(out, g.reviewThreads)
	return out, nil
}

func (g *fakeGitHubGateway) AddReviewThreadReply(_ context.Context, input AddReviewThreadReplyInput) error {
	g.addThreadReplyCalls = append(g.addThreadReplyCalls, input)
	if g.addThreadReplyErr != nil {
		return g.addThreadReplyErr
	}
	// Persist reply onto the in-memory thread so retries see audit markers.
	for i := range g.reviewThreads {
		if g.reviewThreads[i].ID == input.ThreadID {
			g.reviewThreads[i].Comments = append(g.reviewThreads[i].Comments, ReviewThreadComment{
				ID:        fmt.Sprintf("reply-%d", len(g.addThreadReplyCalls)),
				Author:    g.currentLogin,
				Body:      input.Body,
				CreatedAt: "t-reply",
				UpdatedAt: "t-reply",
			})
			break
		}
	}
	return nil
}

func (g *fakeGitHubGateway) ResolveReviewThread(_ context.Context, input ResolveReviewThreadInput) error {
	g.resolveThreadCalls = append(g.resolveThreadCalls, input)
	if g.resolveThreadErr != nil {
		if g.resolveThreadErrTimes <= 0 || g.resolveThreadFailCount < g.resolveThreadErrTimes {
			g.resolveThreadFailCount++
			return g.resolveThreadErr
		}
	}
	for i := range g.reviewThreads {
		if g.reviewThreads[i].ID == input.ThreadID {
			g.reviewThreads[i].IsResolved = true
		}
	}
	return nil
}

func reviewEventIn(events []ReviewEvent, want ReviewEvent) bool {
	for _, event := range events {
		if event == want {
			return true
		}
	}
	return false
}

type fakeGitGateway struct {
	worktreePath string
	createCalls  []CreateWorktreeInput
	prepareCalls []PrepareWorktreeInput
	cleanupCalls []CleanupWorktreeInput
	prepareClean *bool
	prepareErr   error
}

func (f *fakeGitGateway) CreateWorktree(_ context.Context, input CreateWorktreeInput) (CreateWorktreeResult, error) {
	f.createCalls = append(f.createCalls, input)
	path := f.worktreePath
	if path == "" {
		path = filepath.Join(input.WorktreeRoot, "reviewer-worktree")
		f.worktreePath = path
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return CreateWorktreeResult{}, err
	}
	// Seed usable local git metadata so reviewerWorktreePrepared (which probes
	// LocalCheckoutUsable) can reuse the path after Prepare sets PreparedAt.
	// Without this, stepReview re-enters prepare and double-counts Create.
	ensureFakeUsableGitCheckout(path)
	// Match real gateway: any CreateWorktree claim revokes prior fixer ownership.
	if err := worktreesafety.ClearFixerOwnerToken(path); err != nil {
		return CreateWorktreeResult{}, err
	}
	return CreateWorktreeResult{WorktreePath: path, Branch: input.Branch, HeadSHA: "abc123"}, nil
}

func ensureFakeUsableGitCheckout(path string) {
	gitDir := filepath.Join(path, ".git")
	if worktreesafety.LocalCheckoutUsable(path) {
		return
	}
	_ = os.MkdirAll(filepath.Join(gitDir, "objects"), 0o755)
	_ = os.MkdirAll(filepath.Join(gitDir, "refs"), 0o755)
	_ = os.WriteFile(filepath.Join(gitDir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644)
}

func (f *fakeGitGateway) PrepareWorktree(_ context.Context, input PrepareWorktreeInput) (PrepareWorktreeResult, error) {
	f.prepareCalls = append(f.prepareCalls, input)
	if f.prepareErr != nil {
		return PrepareWorktreeResult{}, f.prepareErr
	}
	clean := true
	if f.prepareClean != nil {
		clean = *f.prepareClean
	}
	return PrepareWorktreeResult{HeadSHA: input.ExpectedHeadSHA, Clean: clean}, nil
}

func (f *fakeGitGateway) CleanupWorktree(_ context.Context, input CleanupWorktreeInput) error {
	f.cleanupCalls = append(f.cleanupCalls, input)
	return nil
}

func (f *fakeGitGateway) ScrubReservedReviewerScratch(_ context.Context, _ string) error {
	return nil
}

type fakeAgentExecutor struct {
	results       []AgentResult
	starts        []AgentRunInput
	startErr      error
	waitErr       error
	waitErrs      []error
	wait          func(context.Context) error
	onStart       func(AgentRunInput)
	killedReasons []string
}

func (f *fakeAgentExecutor) Start(_ context.Context, input AgentRunInput) (AgentExecution, error) {
	f.starts = append(f.starts, input)
	if f.onStart != nil {
		f.onStart(input)
	}
	if f.startErr != nil {
		return nil, f.startErr
	}
	if len(f.results) == 0 {
		return nil, fmt.Errorf("no queued agent result")
	}
	result := f.results[0]
	f.results = f.results[1:]
	waitErr := f.waitErr
	if len(f.waitErrs) > 0 {
		waitErr = f.waitErrs[0]
		f.waitErrs = f.waitErrs[1:]
	}
	return &fakeAgentExecution{parent: f, result: result, waitErr: waitErr, wait: f.wait, killed: make(chan string, 1)}, nil
}

type fakeAgentExecution struct {
	parent  *fakeAgentExecutor
	result  AgentResult
	waitErr error
	wait    func(context.Context) error
	killed  chan string
}

func (f *fakeAgentExecution) Wait(ctx context.Context) (AgentResult, error) {
	select {
	case reason := <-f.killed:
		return AgentResult{Status: "killed", Summary: reason}, nil
	default:
	}
	if f.wait != nil {
		if err := f.wait(ctx); err != nil {
			return AgentResult{}, err
		}
	}
	select {
	case reason := <-f.killed:
		return AgentResult{Status: "killed", Summary: reason}, nil
	default:
	}
	if f.waitErr != nil {
		return AgentResult{}, f.waitErr
	}
	if f.result.Status == "completed" && f.result.ParseStatus == "" && strings.Contains(f.result.Stdout, "__LOOPER_RESULT__=") {
		f.result.ParseStatus = "parsed"
	}
	return f.result, nil
}

func (f *fakeAgentExecution) Kill(reason string) error {
	if f.parent != nil {
		f.parent.killedReasons = append(f.parent.killedReasons, reason)
	}
	select {
	case f.killed <- reason:
	default:
	}
	return nil
}

type testLogger struct {
	messages []string
}

func (l *testLogger) Debug(message string, _ map[string]any) {
	l.messages = append(l.messages, message)
}
func (l *testLogger) Info(message string, _ map[string]any) { l.messages = append(l.messages, message) }
func (l *testLogger) Warn(message string, _ map[string]any) { l.messages = append(l.messages, message) }
func (l *testLogger) Error(message string, _ map[string]any) {
	l.messages = append(l.messages, message)
}

func (l *testLogger) hasMessage(want string) bool {
	for _, message := range l.messages {
		if message == want {
			return true
		}
	}
	return false
}

func contains(haystack, needle string) bool { return strings.Contains(haystack, needle) }

func TestUpdateLoopPreservesTerminatedLoop(t *testing.T) {
	fixture := newRunnerFixture(t)
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_reviewer_terminated", Seq: 901, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "terminated", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	runner := &Runner{repos: fixture.repos, now: fixture.now}
	updated, err := runner.updateLoop(context.Background(), loop, func(current *storage.LoopRecord) {
		current.Status = "completed"
	})
	if err != nil {
		t.Fatalf("updateLoop() error = %v", err)
	}
	if updated.Status != "terminated" {
		t.Fatalf("updateLoop().Status = %q, want terminated", updated.Status)
	}

	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.Status != "terminated" {
		t.Fatalf("Loops.GetByID() = %#v, want terminated loop", persisted)
	}
}

func TestUpdateLoopDoesNotRestoreReleasedBudgetHold(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*storage.LoopRecord)
	}{
		{
			name: "success-finalization",
			mutate: func(updated *storage.LoopRecord) {
				if loops.IsReviewFixPairHold(*updated) {
					updated.LastRunAt = stringPtr("2026-04-17T12:34:56.000Z")
					updated.NextRunAt = nil
					return
				}
				updated.Status = "completed"
				updated.LastRunAt = stringPtr("2026-04-17T12:34:56.000Z")
				updated.NextRunAt = nil
			},
		},
		{
			name: "claim-start",
			mutate: func(updated *storage.LoopRecord) {
				if !loops.IsReviewFixBudgetHold(*updated) {
					updated.Status = "running"
				}
				updated.LastRunAt = stringPtr("2026-04-17T12:34:56.000Z")
				updated.NextRunAt = nil
			},
		},
		{
			name: "failure-finalization",
			mutate: func(updated *storage.LoopRecord) {
				updated.LastRunAt = stringPtr("2026-04-17T12:34:56.000Z")
				if loops.IsReviewFixPairHold(*updated) {
					updated.NextRunAt = nil
					return
				}
				updated.Status = "paused"
				updated.NextRunAt = nil
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newRunnerFixture(t)
			repo := "acme/looper"
			prNumber := int64(42)
			nowISO := fixture.nowISO()
			target := "pr:acme/looper:42"
			metadata := `{"followUpdates":true,"loop":{"iterationCount":3}}`
			loop := storage.LoopRecord{
				ID: "loop_update_" + tc.name, Seq: 920, ProjectID: "project_1", Type: "reviewer",
				TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
				Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
			}
			if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
				t.Fatalf("upsert loop: %v", err)
			}
			fixer := storage.LoopRecord{
				ID: "loop_update_" + tc.name + "_fix", Seq: 921, ProjectID: "project_1", Type: "fixer",
				TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
				Status: "queued", MetadataJSON: stringPtr(`{"followUpdates":true}`), CreatedAt: nowISO, UpdatedAt: nowISO,
			}
			if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
				t.Fatalf("upsert fixer: %v", err)
			}
			parked, err := loops.ParkReviewFixBudget(context.Background(), fixture.repos, loops.ParkReviewFixBudgetInput{
				Exhausted: loop, Role: "reviewer", Repo: repo, PRNumber: prNumber,
				Count: 3, Cap: 3, NowISO: nowISO, HITLEnabled: false,
				LiveCaps: loops.ReviewFixBudgetLiveCaps{ReviewerMaxPublishes: 3, FixerMaxPushes: 3},
				DB:       fixture.coordinator.DB(),
			})
			if err != nil {
				t.Fatalf("ParkReviewFixBudget: %v", err)
			}
			if !loops.IsReviewFixBudgetHold(parked) {
				t.Fatalf("precondition: want budget hold, got status=%s", parked.Status)
			}
			continued := false
			updateLoopBeforeWriteHook = func(held storage.LoopRecord) error {
				if continued {
					return nil
				}
				continued = true
				result, contErr := loops.ApplyReviewFixBudgetAnswer(context.Background(), fixture.repos, held, "Continue", nowISO, loops.ReviewFixBudgetLiveCaps{ReviewerMaxPublishes: 3, FixerMaxPushes: 3})
				if contErr != nil {
					return contErr
				}
				if !result.Applied {
					return fmt.Errorf("continue not applied")
				}
				return nil
			}
			t.Cleanup(func() { updateLoopBeforeWriteHook = nil })
			runner := &Runner{repos: fixture.repos, now: fixture.now}
			if _, err := runner.updateLoop(context.Background(), parked, tc.mutate); err != nil {
				t.Fatalf("updateLoop: %v", err)
			}
			fresh, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
			if err != nil || fresh == nil {
				t.Fatalf("get loop: (%#v, %v)", fresh, err)
			}
			if loops.IsReviewFixBudgetHold(*fresh) {
				t.Fatalf("%s restored budget hold after Continue: status=%s meta=%s", tc.name, fresh.Status, derefString(fresh.MetadataJSON))
			}
			if loops.ReviewerPublishCount(fresh.MetadataJSON) != 0 {
				t.Fatalf("%s Continue meters must stay reset, got publish count %d meta=%s", tc.name, loops.ReviewerPublishCount(fresh.MetadataJSON), derefString(fresh.MetadataJSON))
			}
		})
	}
}

func TestFinalizeSuccessfulReviewerQueueDoesNotClobberBudgetContinue(t *testing.T) {
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	metadata := `{"followUpdates":true,"lastPublishedHeadSha":"abc123","loop":{"enabled":true,"iterationCount":3}}`
	loop := storage.LoopRecord{
		ID: "loop_finalize_vs_continue", Seq: 930, ProjectID: "project_1", Type: "reviewer",
		TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
		Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("upsert loop: %v", err)
	}
	fixer := storage.LoopRecord{
		ID: "loop_finalize_vs_continue_fix", Seq: 931, ProjectID: "project_1", Type: "fixer",
		TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber,
		Status: "queued", MetadataJSON: stringPtr(`{"followUpdates":true}`), CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("upsert fixer: %v", err)
	}
	parked, err := loops.ParkReviewFixBudget(context.Background(), fixture.repos, loops.ParkReviewFixBudgetInput{
		Exhausted: loop, Role: "reviewer", Repo: repo, PRNumber: prNumber,
		Count: 3, Cap: 3, NowISO: nowISO, HITLEnabled: false,
		LiveCaps: loops.ReviewFixBudgetLiveCaps{ReviewerMaxPublishes: 3, FixerMaxPushes: 3},
		DB:       fixture.coordinator.DB(),
	})
	if err != nil {
		t.Fatalf("ParkReviewFixBudget: %v", err)
	}
	payload := reviewerQueuePayloadJSON("abc123", "sig-done", true)
	item := storage.QueueItemRecord{
		ID: "queue_finalize_vs_continue", ProjectID: stringPtr("project_1"), LoopID: &loop.ID, Type: "reviewer",
		TargetType: "pull_request", TargetID: target, Repo: &repo, PRNumber: &prNumber,
		DedupeKey: buildReviewerDedupeKey("project_1", loop.ID, repo, prNumber),
		Priority:  storage.QueuePriorityReviewer, Status: "running",
		AvailableAt: nowISO, Attempts: 0, MaxAttempts: 5,
		PayloadJSON: &payload, CreatedAt: nowISO, UpdatedAt: nowISO,
	}
	if err := fixture.repos.Queue.Upsert(context.Background(), item); err != nil {
		t.Fatalf("Upsert queue: %v", err)
	}
	continued := false
	updateLoopBeforeWriteHook = func(held storage.LoopRecord) error {
		if continued {
			return nil
		}
		continued = true
		result, contErr := loops.ApplyReviewFixBudgetAnswer(context.Background(), fixture.repos, held, "Continue", nowISO, loops.ReviewFixBudgetLiveCaps{ReviewerMaxPublishes: 3, FixerMaxPushes: 3})
		if contErr != nil {
			return contErr
		}
		if !result.Applied {
			return fmt.Errorf("continue not applied")
		}
		return nil
	}
	t.Cleanup(func() { updateLoopBeforeWriteHook = nil })
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos,
		Logger: fixture.logger, Now: fixture.now,
		LoopConfig: testReviewerLoopConfig(),
	})
	if _, err := runner.finalizeSuccessfulReviewerQueue(context.Background(),
		storage.ProjectRecord{ID: "project_1", RepoPath: "/tmp"}, parked, item, "run_finalize_continue",
		reviewerCheckpoint{DispositionOnly: true}, "success", "disposition done"); err != nil {
		t.Fatalf("finalizeSuccessfulReviewerQueue: %v", err)
	}
	fresh, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || fresh == nil {
		t.Fatalf("get loop: (%#v, %v)", fresh, err)
	}
	if loops.IsReviewFixBudgetHold(*fresh) {
		t.Fatalf("finalization restored budget hold after Continue: status=%s meta=%s", fresh.Status, derefString(fresh.MetadataJSON))
	}
	if loops.ReviewerPublishCount(fresh.MetadataJSON) != 0 {
		t.Fatalf("Continue meters must stay reset, got publish count %d meta=%s", loops.ReviewerPublishCount(fresh.MetadataJSON), derefString(fresh.MetadataJSON))
	}
}
