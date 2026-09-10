package fixer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nexu-io/looper/internal/config"
	"github.com/nexu-io/looper/internal/disclosure"
	"github.com/nexu-io/looper/internal/domain"
	"github.com/nexu-io/looper/internal/eventlog"
	"github.com/nexu-io/looper/internal/forge"
	"github.com/nexu-io/looper/internal/infra/specpr"
	"github.com/nexu-io/looper/internal/lifecycle"
	"github.com/nexu-io/looper/internal/loops"
	"github.com/nexu-io/looper/internal/loops/failureclass"
	"github.com/nexu-io/looper/internal/storage"
)

func TestBuildFixerPromptUsesConcreteDisclosureMetadata(t *testing.T) {
	t.Parallel()

	detail := &checkpointDetail{State: "OPEN", HeadSHA: "abc123", BaseRefName: "main", HeadRefName: "feature"}
	prompt, _ := buildFixerPrompt("project_1", customInstructionConfig(nil), "acme/looper", 42, detail, []FixItem{{ID: "fix-1", Summary: "repair disclosure"}}, true, config.DefaultDisclosureConfig(), "opencode", "openai/gpt-5.5")
	for _, want := range []string{"agent=opencode"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
	for _, unwanted := range []string{"agent=<agent-runtime>", "model=<agent-model>", "model=openai/gpt-5.5", "agent=gpt-5.5", "agent=openai/gpt-5.5"} {
		if strings.Contains(prompt, unwanted) {
			t.Fatalf("prompt contains %q:\n%s", unwanted, prompt)
		}
	}
}

func TestBuildFixerPromptOmitsMissingAgentRuntime(t *testing.T) {
	t.Parallel()

	detail := &checkpointDetail{State: "OPEN", HeadSHA: "abc123", BaseRefName: "main", HeadRefName: "feature"}
	prompt, _ := buildFixerPrompt("project_1", customInstructionConfig(nil), "acme/looper", 42, detail, []FixItem{{ID: "fix-1", Summary: "repair disclosure"}}, true, config.DefaultDisclosureConfig(), "", "openai/gpt-5.5")
	if strings.Contains(prompt, "agent=") {
		t.Fatalf("prompt should omit missing agent runtime:\n%s", prompt)
	}
	if strings.Contains(prompt, "model=") || strings.Contains(prompt, "openai/gpt-5.5") {
		t.Fatalf("prompt should not expose configured model:\n%s", prompt)
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
}

func TestBackoffDelayCapsInfiniteRetryOverflow(t *testing.T) {
	t.Parallel()

	if got := backoffDelay(defaultRetryDelay, 100); got != maxRetryDelay {
		t.Fatalf("backoffDelay(infinite retry overflow) = %v, want %v", got, maxRetryDelay)
	}
}

func TestRefusePushIfBudgetExhaustedDoesNotResolveComments(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	// Already at cap before push.
	meta := `{"reviewFixBudget":{"pushCount":1}}`
	fixer := storage.LoopRecord{ID: "loop_push_refuse", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &meta, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.Roles.Fixer.Behavior.Loop.MaxPushesPerPR = 1
	cfg.HITL.Enabled = false
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg, HITLEnabled: false, GitHub: &fakeGitHubGateway{}})
	refused, err := runner.refusePushIfBudgetExhausted(context.Background(), stepInput{
		Project: storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Loop:    fixer, Repo: repo, PRNumber: prNumber,
	})
	if !refused {
		t.Fatalf("refused=%v err=%v, want refused at live cap", refused, err)
	}
	after, _ := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
	if after == nil || !loops.IsReviewFixBudgetHold(*after) {
		t.Fatalf("after refuse = %#v, want budget hold", after)
	}
}

func TestRefusePushIfBudgetExhaustedFailsClosedOnLedgerRead(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	// Under the live cap: swallowing GetByID would admit the stale snapshot.
	meta := `{"reviewFixBudget":{"pushCount":0}}`
	fixer := storage.LoopRecord{ID: "loop_push_ledger_fail", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &meta, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.Roles.Fixer.Behavior.Loop.MaxPushesPerPR = 1
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg, HITLEnabled: false, GitHub: &fakeGitHubGateway{}})
	if err := fixture.coordinator.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	refused, err := runner.refusePushIfBudgetExhausted(context.Background(), stepInput{
		Project: storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Loop:    fixer, Repo: repo, PRNumber: prNumber,
	})
	if err == nil {
		t.Fatalf("refused=%v err=%v, want fail closed on ledger read", refused, err)
	}
	if !refused {
		t.Fatalf("refused=%v, want true when ledger refresh fails", refused)
	}
}

func TestEnsureFixerPushBudgetCountedParksWhenCapReached(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	// Cap 1, count 0 — after increment should park and return hold skip.
	// Prior head must not win over the just-pushed head on handoff.
	meta := `{"reviewFixBudget":{"pushCount":0},"lastFixHeadSha":"previous-head"}`
	fixer := storage.LoopRecord{ID: "loop_push_count_park", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &meta, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	run := storage.RunRecord{ID: "run_push_count_park", LoopID: fixer.ID, Status: "running", StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), run); err != nil {
		t.Fatalf("Runs.Upsert: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.Roles.Fixer.Behavior.Loop.MaxPushesPerPR = 1
	cfg.HITL.Enabled = false
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg, HITLEnabled: false, GitHub: &fakeGitHubGateway{}})
	pushedHead := "cap-reach-head-sha"
	checkpoint := fixerCheckpoint{Push: &checkpointPush{Pushed: true, BudgetCounted: false, HeadSHA: pushedHead, Evidence: &fixEvidence{HeadSHA: pushedHead}}}
	_, err = runner.ensureFixerPushBudgetCounted(context.Background(), stepInput{
		Project: storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Loop:    fixer, Run: run, Repo: repo, PRNumber: prNumber,
	}, checkpoint)
	var hold *holdSkipError
	if err == nil || !errors.As(err, &hold) {
		t.Fatalf("err = %v, want holdSkipError after cap-reaching push", err)
	}
	after, _ := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
	if after == nil || !loops.IsReviewFixBudgetHold(*after) || loops.FixerPushCount(after.MetadataJSON) != 1 {
		t.Fatalf("after count+park = %#v, want held with pushCount 1", after)
	}
	if after.MetadataJSON == nil || !strings.Contains(*after.MetadataJSON, `"lastFixHeadSha":"`+pushedHead+`"`) {
		t.Fatalf("lastFixHeadSha missing/stale after cap park: %v", after.MetadataJSON)
	}
	events, err := fixture.repos.Events.ListByEntity(context.Background(), "loop", fixer.ID)
	if err != nil {
		t.Fatalf("ListByEntity: %v", err)
	}
	var handoffPayload string
	for i := range events {
		if events[i].EventType == "loop.review_fix_budget.exhausted" {
			handoffPayload = events[i].PayloadJSON
			break
		}
	}
	if handoffPayload == "" {
		t.Fatal("missing review-fix budget handoff event")
	}
	if !strings.Contains(handoffPayload, `"head":"`+pushedHead+`"`) {
		t.Fatalf("handoff payload = %s, want head %q", handoffPayload, pushedHead)
	}
}

func TestIncrementAndParkFixerBudgetParksSibling(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	fixer := storage.LoopRecord{ID: "loop_fixer_budget", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	reviewer := storage.LoopRecord{ID: "loop_reviewer_budget", Seq: 2, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "waiting", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Loops.Upsert(fixer) error = %v", err)
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("Loops.Upsert(reviewer) error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Roles.Fixer.Behavior.Loop.MaxPushesPerPR = 1
	cfg.HITL.Enabled = true
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg, HITLEnabled: true})
	if _, err := runner.incrementFixerPushCount(context.Background(), fixer); err != nil {
		t.Fatalf("incrementFixerPushCount() error = %v", err)
	}
	parked, err := runner.parkFixerBudgetIfExhausted(context.Background(), fixer)
	if err != nil || !parked {
		t.Fatalf("parkFixerBudgetIfExhausted() = (%v, %v), want parked", parked, err)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
	if err != nil || updated == nil || updated.Status != "awaiting_human" {
		t.Fatalf("fixer = (%#v, %v), want awaiting_human", updated, err)
	}
	sibling, err := fixture.repos.Loops.GetByID(context.Background(), reviewer.ID)
	if err != nil || sibling == nil || sibling.Status != "paused" || !loops.IsSiblingReviewFixBudgetPause(sibling.MetadataJSON) {
		t.Fatalf("reviewer sibling = (%#v, %v), want paused sibling hold", sibling, err)
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
	loopID := "loop_fixer_budget_entry"
	fixer := storage.LoopRecord{ID: loopID, Seq: 1, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}
	reviewer := storage.LoopRecord{ID: "loop_reviewer_budget_entry", Seq: 2, ProjectID: projectID, Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "waiting", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Loops.Upsert(fixer) error = %v", err)
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("Loops.Upsert(reviewer) error = %v", err)
	}
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_fixer_budget_entry", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: target, Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:budget-entry", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Roles.Fixer.Behavior.Loop.MaxPushesPerPR = 1
	cfg.HITL.Enabled = true
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg, HITLEnabled: true})
	if _, err := runner.incrementFixerPushCount(context.Background(), fixer); err != nil {
		t.Fatalf("incrementFixerPushCount() error = %v", err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), storage.QueueItemRecord{ID: "queue_fixer_budget_entry", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: target, Repo: &repo, PRNumber: &prNumber, Status: "running"})
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" {
		t.Fatalf("result = %#v, want skipped at claim start", result)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil || updated == nil || updated.Status != "awaiting_human" {
		t.Fatalf("fixer = (%#v, %v), want awaiting_human", updated, err)
	}
	sibling, err := fixture.repos.Loops.GetByID(context.Background(), reviewer.ID)
	if err != nil || sibling == nil || sibling.Status != "paused" || !loops.IsSiblingReviewFixBudgetPause(sibling.MetadataJSON) {
		t.Fatalf("reviewer sibling = (%#v, %v), want paused sibling hold", sibling, err)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), "queue_fixer_budget_entry")
	if err != nil || queue == nil || queue.Status != "cancelled" {
		t.Fatalf("queue = (%#v, %v), want cancelled claimed item", queue, err)
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
	loopID := "loop_fixer_budget_claim_closed"
	fixer := storage.LoopRecord{ID: loopID, Seq: 1, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}
	reviewer := storage.LoopRecord{ID: "loop_reviewer_budget_claim_closed", Seq: 2, ProjectID: projectID, Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "waiting", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Loops.Upsert(fixer) error = %v", err)
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("Loops.Upsert(reviewer) error = %v", err)
	}
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_fixer_budget_claim_closed", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: target, Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:budget-claim-closed", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Roles.Fixer.Behavior.Loop.MaxPushesPerPR = 1
	cfg.HITL.Enabled = true
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{Number: prNumber, State: "MERGED"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg, HITLEnabled: true})
	if _, err := runner.incrementFixerPushCount(context.Background(), fixer); err != nil {
		t.Fatalf("incrementFixerPushCount() error = %v", err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), storage.QueueItemRecord{ID: "queue_fixer_budget_claim_closed", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: target, Repo: &repo, PRNumber: &prNumber, Status: "running"})
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" {
		t.Fatalf("result = %#v, want skipped after closed-PR claim", result)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil || updated == nil || updated.Status != "terminated" {
		t.Fatalf("fixer = (%#v, %v), want terminated product terminal", updated, err)
	}
	reason := ""
	if loopMeta, ok := parseJSONObject(updated.MetadataJSON)["loop"].(map[string]any); ok {
		reason, _ = stringFromAny(loopMeta["terminationReason"])
	}
	if reason != "pr_closed_or_merged" {
		t.Fatalf("terminationReason = %q, want pr_closed_or_merged", reason)
	}
	if ask, ok := loops.ReadHITLAsk(updated.MetadataJSON); ok && loops.IsReviewFixBudgetAsk(ask) {
		t.Fatalf("HITL ask = %#v, want no budget ask on a closed PR", ask)
	}
	sibling, err := fixture.repos.Loops.GetByID(context.Background(), reviewer.ID)
	if err != nil || sibling == nil || sibling.Status != "waiting" || loops.IsSiblingReviewFixBudgetPause(sibling.MetadataJSON) {
		t.Fatalf("reviewer sibling = (%#v, %v), want still waiting without budget pause", sibling, err)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), "queue_fixer_budget_claim_closed")
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

func TestParkFixerBudgetBeforePendingRediscoveryDoesNotEnqueue(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(85)
	nowISO := fixture.nowISO()
	target := buildPullRequestTargetID(repo, prNumber)
	projectID := "project_1"
	loopID := "loop_fixer_budget_rediscovery"
	metadata := mustMarshalJSON(map[string]any{"pendingFixerRediscovery": map[string]any{"headSha": "head-85", "fixItemsStateHash": "state-85", "unresolvedThreadIds": []string{"t1"}, "recordedAt": nowISO}})
	fixer := storage.LoopRecord{ID: loopID, Seq: 1, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Loops.Upsert(fixer) error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Roles.Fixer.Behavior.Loop.MaxPushesPerPR = 1
	cfg.HITL.Enabled = true
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg, HITLEnabled: true})
	if _, err := runner.incrementFixerPushCount(context.Background(), fixer); err != nil {
		t.Fatalf("incrementFixerPushCount() error = %v", err)
	}

	parked, err := runner.parkFixerBudgetIfExhausted(context.Background(), fixer)
	if err != nil || !parked {
		t.Fatalf("parkFixerBudgetIfExhausted() = (%v, %v), want parked before rediscovery", parked, err)
	}
	scheduled, err := runner.schedulePendingRediscoveryAfterRun(context.Background(), fixer, repo, prNumber)
	if err != nil {
		t.Fatalf("schedulePendingRediscoveryAfterRun() error = %v", err)
	}
	if scheduled {
		t.Fatal("schedulePendingRediscoveryAfterRun() = true, want no enqueue after budget park")
	}
	active, err := fixture.repos.Queue.FindActiveByLoopID(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Queue.FindActiveByLoopID() error = %v", err)
	}
	if active != nil {
		t.Fatalf("activeQueue = %#v, want no rediscovery item after cap park", active)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil || updated == nil || updated.Status != "awaiting_human" {
		t.Fatalf("fixer = (%#v, %v), want still awaiting_human", updated, err)
	}
}

func TestParkFixerBudgetIfExhaustedParksWhenHITLDisabled(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	fixer := storage.LoopRecord{ID: "loop_fixer_budget_no_hitl", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	reviewer := storage.LoopRecord{ID: "loop_reviewer_budget_no_hitl", Seq: 2, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "waiting", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Loops.Upsert(fixer) error = %v", err)
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("Loops.Upsert(reviewer) error = %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	if cfg.HITL.Enabled {
		t.Fatal("DefaultConfig HITL.Enabled = true, want false")
	}
	cfg.Roles.Fixer.Behavior.Loop.MaxPushesPerPR = 1
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg})
	if _, err := runner.incrementFixerPushCount(context.Background(), fixer); err != nil {
		t.Fatalf("incrementFixerPushCount() error = %v", err)
	}
	parked, err := runner.parkFixerBudgetIfExhausted(context.Background(), fixer)
	if err != nil || !parked {
		t.Fatalf("parkFixerBudgetIfExhausted() = (%v, %v), want no-HITL paired park", parked, err)
	}
	updated, err := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
	if err != nil || updated == nil || updated.Status != "paused" || !loops.IsReviewFixBudgetExhaustedPause(updated.MetadataJSON) {
		t.Fatalf("fixer = (%#v, %v), want paused review_fix_budget_exhausted", updated, err)
	}
	if _, ok := loops.ReadHITLAsk(updated.MetadataJSON); ok {
		t.Fatal("no-HITL park must not write a HITL ask")
	}
	sibling, err := fixture.repos.Loops.GetByID(context.Background(), reviewer.ID)
	if err != nil || sibling == nil || sibling.Status != "paused" || !loops.IsSiblingReviewFixBudgetPause(sibling.MetadataJSON) {
		t.Fatalf("reviewer sibling = (%#v, %v), want paired budget pause", sibling, err)
	}
}

func TestRunPushStepCountsAlreadyPushedCheckpointOnResume(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	loopMetadata := `{}`
	loopTarget := buildPullRequestTargetID("acme/looper", 42)
	prNumber := int64(42)
	loop := storage.LoopRecord{
		ID: "loop_push_budget_resume", ProjectID: "project_1", Type: "fixer", TargetType: "pull_request",
		TargetID: &loopTarget, Repo: stringPtr("acme/looper"), PRNumber: &prNumber, Status: "running",
		MetadataJSON: &loopMetadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO(),
	}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	run := storage.RunRecord{ID: "run_push_budget_resume", LoopID: loop.ID, Status: "running", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Runs.Upsert(context.Background(), run); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Now: fixture.now, Logger: fixture.logger})
	checkpoint := fixerCheckpoint{Push: &checkpointPush{Pushed: true, Branch: "feature/fix-42", HeadSHA: "fix-head"}}

	updated, err := runner.runPushStep(context.Background(), stepInput{Loop: loop, Run: run, Checkpoint: checkpoint})
	if err != nil {
		t.Fatalf("runPushStep() error = %v", err)
	}
	if updated.Push == nil || !updated.Push.BudgetCounted {
		t.Fatalf("updated.Push = %#v, want budgetCounted after resume", updated.Push)
	}
	fresh, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || fresh == nil || loops.ReadReviewFixBudgetState(fresh.MetadataJSON).PushCount != 1 {
		t.Fatalf("pushCount after resume = (%#v, %v), want 1", fresh, err)
	}

	updated, err = runner.runPushStep(context.Background(), stepInput{Loop: *fresh, Run: run, Checkpoint: updated})
	if err != nil {
		t.Fatalf("runPushStep() second resume error = %v", err)
	}
	fresh, err = fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || fresh == nil || loops.ReadReviewFixBudgetState(fresh.MetadataJSON).PushCount != 1 {
		t.Fatalf("pushCount after second resume = (%#v, %v), want still 1", fresh, err)
	}
}

func TestNewPreservesInfiniteRetryMaxAttempts(t *testing.T) {
	t.Parallel()

	runner := New(Options{RetryMaxAttempts: -1})
	if runner.retryMaxAttempts != -1 {
		t.Fatalf("retryMaxAttempts = %d, want -1", runner.retryMaxAttempts)
	}
}

func TestBuildFixerPromptIncludesMinimalPRSeedFetchContract(t *testing.T) {
	t.Parallel()

	detail := &checkpointDetail{State: "OPEN", HeadSHA: "abc123", BaseRefName: "main", HeadRefName: "feature/fix"}
	prompt, _ := buildFixerPrompt("project_1", customInstructionConfig(nil), "acme/looper", 42, detail, []FixItem{{ID: "fix-1", ThreadID: "thread-1", Summary: "repair disclosure"}}, false, config.DefaultDisclosureConfig(), "opencode", "openai/gpt-5.5")
	for _, want := range []string{
		"Minimal PR seed",
		"\"repo\": \"acme/looper\"",
		"\"pr_number\": 42",
		"\"base_ref\": \"main\"",
		"\"head_ref\": \"feature/fix\"",
		"\"head_sha\": \"abc123\"",
		"\"task_intent\": \"evaluate_in_scope_pull_request_feedback\"",
		"\"review_items\"",
		"\"fix_item_ids\"",
		"gh pr view <pr-url> -R <repo> --json number,title,body,state,isDraft,baseRefName,headRefName,headRefOid,url,labels",
		"gh pr diff <pr-url> -R <repo> --name-only",
		"gh pr diff <pr-url> -R <repo> --patch",
		"gh pr checks <pr-url> -R <repo>",
		"git diff <base>...<head> -- <path>",
		"gh api repos/{owner}/{repo}/pulls/{number}/comments --paginate",
		"gh api repos/{owner}/{repo}/pulls/{number}/reviews --paginate",
		"gh api repos/{owner}/{repo}/issues/{number}/comments --paginate",
		"structured error with `type` set to one of `auth`, `network`, `rate_limit`, or `pr_drift`",
		"Scope authority, in priority order",
		"problem report to evaluate, not as a mandatory change",
		"IN_SCOPE:",
		"OUT_OF_SCOPE:",
		"UNCERTAIN_OR_CONFLICTING:",
		"Make the smallest complete change required by the documented PR intent",
		"Completeness is measured against that intent",
		"Do not broaden the repair into generators or generated artifacts",
		"A failing check is actionable only when current evidence ties the failure to this PR",
		"follow the repository's own instructions for formatting, tests, vetting, builds",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
	for _, unwanted := range []string{
		`"scope":`,
		"Fully address every listed fix item",
		"do not minimize the diff",
		"smallest complete, coherent solution over the smallest diff",
	} {
		if strings.Contains(prompt, unwanted) {
			t.Fatalf("prompt contains obsolete scope contract %q:\n%s", unwanted, prompt)
		}
	}
	// Non-comment fix items must not activate provider reply protocols.
	for _, unwanted := range []string{"review_thread_replies", "repair_results"} {
		if strings.Contains(prompt, unwanted) {
			t.Fatalf("non-comment prompt unexpectedly contains %q:\n%s", unwanted, prompt)
		}
	}
	if strings.Contains(prompt, "gh pr diff -- <path>") {
		t.Fatalf("prompt contains unsupported gh pr diff pathspec instruction:\n%s", prompt)
	}
}

func TestBuildFixerPromptTreatsReviewFeedbackAsProblemReport(t *testing.T) {
	t.Parallel()

	detail := &checkpointDetail{State: "OPEN", HeadSHA: "abc123", BaseRefName: "main", HeadRefName: "feature/fix"}
	prompt, _ := buildFixerPrompt("project_1", customInstructionConfig(nil), "acme/looper", 42, detail, []FixItem{{Type: "comment", ID: "c1", ThreadID: "thread-1", Summary: "replace the documented design"}}, false, config.DefaultDisclosureConfig(), "opencode", "openai/gpt-5.5")
	for _, want := range []string{
		"Treat every listed review item as a problem report to evaluate, not as a mandatory change",
		"Earlier fixer-generated changes are evidence to inspect, not authority",
		"Declining an out-of-scope item is a correct result, not a failure.",
		"give the reviewer a clear, respectful explanation through the structured per-item response",
		"respectfully explain why the suggestion was not adopted",
		"Looper will post it as the thread reply and leave the thread unresolved for Reviewer adjudication",
		"do not create `.looper/dismiss.json` or dismiss an entire review",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing review-feedback boundary %q:\n%s", want, prompt)
		}
	}
	for _, unwanted := range []string{
		"when in doubt, implement the requested change",
		"Fully address every listed fix item",
		"Looper will dismiss that review with your reason",
		"Use this sparingly and only when confident",
	} {
		if strings.Contains(prompt, unwanted) {
			t.Fatalf("prompt contains reviewer-as-command fallback %q:\n%s", unwanted, prompt)
		}
	}
}

func TestFixerRepairScopeInstructionMakesPRIntentAuthoritative(t *testing.T) {
	t.Parallel()

	got := fixerRepairScopeInstruction()
	for _, want := range []string{
		"Scope authority, in priority order",
		"repository instructions",
		"pull request's documented intent",
		"not as a mandatory change",
		"Before editing anything",
		"OUT_OF_SCOPE:",
		"report it as declined with concrete scope evidence",
		"same behavior needs a second repair",
		"use needs_human and stop the entire turn before editing",
		"smallest complete change required by the documented PR intent",
		"Do not broaden the repair into generators or generated artifacts",
		"follow the repository's own instructions for formatting, tests, vetting, builds",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("scope instruction missing %q:\n%s", want, got)
		}
	}
	for _, unwanted := range []string{
		"Fully address every listed fix item",
		"do not minimize the diff",
		"dependency chain",
		"smallest complete, coherent solution over the smallest diff",
		"review_thread_replies",
		"repair_results",
		"change-class",
		"likely force another review round",
	} {
		if strings.Contains(got, unwanted) {
			t.Fatalf("scope instruction contains unwanted %q:\n%s", unwanted, got)
		}
	}
}

func TestBuildFixerPromptGitHubCommentMentionsCollateralOnlyInThreadReplies(t *testing.T) {
	t.Parallel()

	detail := &checkpointDetail{State: "OPEN", HeadSHA: "abc123", BaseRefName: "main", HeadRefName: "feature/fix"}
	prompt, _ := buildFixerPrompt("project_1", customInstructionConfig(nil), "acme/looper", 42, detail, []FixItem{{Type: "comment", ID: "c1", ThreadID: "thread-1", Summary: "repair disclosure"}}, false, config.DefaultDisclosureConfig(), "opencode", "openai/gpt-5.5")
	if !strings.Contains(prompt, "review_thread_replies") {
		t.Fatalf("GitHub comment prompt missing review_thread_replies:\n%s", prompt)
	}
	if !strings.Contains(prompt, "Create structured `review_thread_replies` entries only for listed comment fix items, never for collateral-only changes") {
		t.Fatalf("GitHub comment prompt missing collateral reporting guidance:\n%s", prompt)
	}
	if strings.Contains(prompt, "repair_results") {
		t.Fatalf("GitHub comment prompt unexpectedly contains repair_results:\n%s", prompt)
	}
}

func TestBuildFixerPromptCommentReplyInstructionRequiresVerificationWithoutRemoteMutation(t *testing.T) {
	t.Parallel()

	detail := &checkpointDetail{State: "OPEN", HeadSHA: "abc123", BaseRefName: "main", HeadRefName: "feature/fix"}
	prompt, _ := buildFixerPrompt("project_1", customInstructionConfig(nil), "acme/looper", 42, detail, []FixItem{{Type: "comment", ID: "c1", ThreadID: "thread-1", Summary: "repair disclosure"}}, false, config.DefaultDisclosureConfig(), "opencode", "openai/gpt-5.5")
	for _, want := range []string{
		"Before including an entry, re-read the relevant review thread/comment context",
		"The \"id\" MUST be the GraphQL PullRequestReviewComment node ID.",
		"map REST \"node_id\" to \"id\" and REST \"updated_at\" to \"updatedAt\"",
		"do not use the REST numeric \"id\"",
		"only include items you can confidently confirm are actually addressed by the current branch state",
		"Read-only GitHub fetches are allowed for that verification.",
		"Do not post replies, resolve threads, submit reviews, edit PR metadata, or perform any other mutating GitHub API action",
		"Looper owns those remote review-state changes after validation and push.",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestBuildFixerPromptPrefersRoundSpecificCommitSubject(t *testing.T) {
	t.Parallel()

	detail := &checkpointDetail{State: "OPEN", HeadSHA: "abc123", BaseRefName: "main", HeadRefName: "feature/fix"}
	prompt, _ := buildFixerPrompt("project_1", customInstructionConfig(nil), "acme/looper", 42, detail, []FixItem{{ID: "fix-1", Summary: "repair disclosure"}}, true, config.DefaultDisclosureConfig(), "opencode", "openai/gpt-5.5")
	for _, want := range []string{
		"For fixer commits, prefer a fresh commit subject that precisely summarizes the repair changes from this round.",
		"Do not mechanically reuse the PR title or a previous fixer subject",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
	if strings.Contains(prompt, "For commits, keep commit subjects unchanged and add a commit body trailer") {
		t.Fatalf("prompt still contains old conflicting commit-subject guidance:\n%s", prompt)
	}
}

func TestCollectFixItemsFromForgejoReviewerSummaryOpenItemsOnly(t *testing.T) {
	t.Parallel()

	summary := forge.NewReviewerSummary(3, []forge.ReviewItem{
		{ReviewItemID: "R-001", Status: forge.ReviewItemStatusOpen, Title: "Fix parsing", Body: "Parser must fail fast.", Files: []string{"internal/forge/summary_protocol.go"}, LastSeenRoundID: 3},
		{ReviewItemID: "R-002", Status: forge.ReviewItemStatusResolved, Title: "Old issue", Body: "Already fixed.", LastSeenRoundID: 2},
	})
	marker, err := forge.RenderReviewerSummary(summary)
	if err != nil {
		t.Fatalf("RenderReviewerSummary() error = %v", err)
	}

	items, err := collectFixItemsFromCheckpointForStep(fixerCheckpoint{Detail: &checkpointDetail{IssueComments: []map[string]any{{"id": int64(101), "body": "visible\n" + marker, "url": "https://forgejo.test/comment/101"}}}})
	if err != nil {
		t.Fatalf("collectFixItemsFromCheckpointForStep() error = %v", err)
	}
	if len(items) != 1 || items[0].ID != "R-001" || items[0].ThreadID != "R-001" || items[0].Path != "internal/forge/summary_protocol.go" {
		t.Fatalf("items = %#v, want one open Reviewer Summary item", items)
	}
	if items[0].Source != "forgejo-reviewer-summary" {
		t.Fatalf("item = %#v, want forgejo reviewer summary metadata", items[0])
	}
}

func TestBuildForgejoFixerSummaryValidatesOneResultPerOpenItem(t *testing.T) {
	t.Parallel()

	reviewer := forge.NewReviewerSummary(4, []forge.ReviewItem{
		{ReviewItemID: "R-001", Status: forge.ReviewItemStatusOpen, Title: "Fix parsing", Body: "Parser must fail fast.", LastSeenRoundID: 4},
	})
	checkpoint := fixerCheckpoint{
		Detail:       &checkpointDetail{HeadSHA: "head-1"},
		FixItems:     []FixItem{{Type: "comment", ID: "R-001", ThreadID: "R-001", Summary: "Fix parsing"}},
		FixItemsHash: "hash-1",
		Repair:       &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "R-001", ThreadID: "R-001", Action: "fixed", Explanation: "Added strict parsing."}}},
		ReconcileCommits: &checkpointReconcileCommits{
			FinalHeadSHA:     "head-2",
			WorkingTreeClean: true,
		},
	}

	summary, err := buildForgejoFixerSummary(checkpoint, reviewer, lookupReplyExplanations(checkpoint))
	if err != nil {
		t.Fatalf("buildForgejoFixerSummary() error = %v", err)
	}
	if err := forge.ValidateFixerResultsForReviewerSummary(reviewer, summary); err != nil {
		t.Fatalf("ValidateFixerResultsForReviewerSummary() error = %v", err)
	}
	if summary.ConsumedReviewRoundID != 4 || len(summary.Results) != 1 || summary.Results[0].ReviewItemID != "R-001" || summary.Results[0].Result != forge.FixerItemResultFixed {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestBuildForgejoFixerSummaryFailsWhenAgentOmitsOpenItemResult(t *testing.T) {
	t.Parallel()

	reviewer := forge.NewReviewerSummary(4, []forge.ReviewItem{{ReviewItemID: "R-001", Status: forge.ReviewItemStatusOpen, Title: "Fix parsing", Body: "Parser must fail fast.", LastSeenRoundID: 4}})
	checkpoint := fixerCheckpoint{Detail: &checkpointDetail{HeadSHA: "head-1"}, FixItems: []FixItem{{Type: "comment", ID: "R-001", ThreadID: "R-001", Summary: "Fix parsing"}}, Repair: &checkpointRepair{}}

	_, err := buildForgejoFixerSummary(checkpoint, reviewer, lookupReplyExplanations(checkpoint))
	if err == nil || !strings.Contains(err.Error(), "missing result explanation") {
		t.Fatalf("buildForgejoFixerSummary() error = %v, want missing result explanation", err)
	}
}

func TestRunPrepareWorktreeStepRecreatesUnsafeCheckpointAtRepoPath(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repoPath := t.TempDir()
	git := &fakeGitGateway{createResult: CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt"), Branch: "feature/fix-42", HeadSHA: "base-head"}, prepareResult: PrepareWorktreeResult{HeadSHA: "head-1", Clean: true}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Git: git, Logger: fixture.logger, Now: fixture.now})

	checkpoint, err := runner.runPrepareWorktreeStep(context.Background(), stepInput{
		Project:  storage.ProjectRecord{ID: "project_1", RepoPath: repoPath},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: fixerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/fix-42", BaseRefName: "main", HeadSHA: "head-1"},
			Worktree: &checkpointWorktree{Path: repoPath, Branch: "feature/fix-42", PreparedAt: "stale"},
		},
	})
	if err != nil {
		t.Fatalf("runPrepareWorktreeStep() error = %v", err)
	}
	if len(git.createCalls) != 1 {
		t.Fatalf("len(git.createCalls) = %d, want 1", len(git.createCalls))
	}
	if checkpoint.Worktree == nil || checkpoint.Worktree.Path != git.createResult.WorktreePath {
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
	worktreeRoot := filepath.Join(t.TempDir(), "worktrees")
	legacyPath := filepath.Join(t.TempDir(), "legacy-wt")
	metadata := fmt.Sprintf(`{"worktreeRoot":%q}`, worktreeRoot)
	git := &fakeGitGateway{createResult: CreateWorktreeResult{WorktreePath: filepath.Join(worktreeRoot, "wt"), Branch: "feature/fix-42", HeadSHA: "base-head"}, prepareResult: PrepareWorktreeResult{HeadSHA: "head-1", Clean: true}}
	stdout := `__LOOPER_RESULT__={"summary":"applied fixes","review_thread_replies":[{"fixItemId":"c1","threadId":"t1","explanation":"Applied the requested fix."}]}` + "\n"
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "applied fixes", ParseStatus: "parsed", Stdout: stdout}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Git: git, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	checkpoint, err := runner.runPrepareWorktreeStep(context.Background(), stepInput{
		Project:  storage.ProjectRecord{ID: "project_1", RepoPath: repoPath, MetadataJSON: &metadata},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: fixerCheckpoint{
			Detail:   &checkpointDetail{HeadRefName: "feature/fix-42", BaseRefName: "main", HeadSHA: "head-1"},
			Worktree: &checkpointWorktree{Path: legacyPath, Branch: "feature/fix-42", PreparedAt: "stale"},
		},
	})
	if err != nil {
		t.Fatalf("runPrepareWorktreeStep() error = %v", err)
	}
	if len(git.createCalls) != 1 {
		t.Fatalf("len(git.createCalls) = %d, want 1", len(git.createCalls))
	}
	if checkpoint.Worktree == nil || checkpoint.Worktree.Path != git.createResult.WorktreePath {
		t.Fatalf("checkpoint.Worktree = %#v, want recreated worktree", checkpoint.Worktree)
	}
	if checkpoint.Worktree.Path == legacyPath {
		t.Fatalf("checkpoint.Worktree.Path = %q, want recreated path outside legacy prepared worktree", checkpoint.Worktree.Path)
	}
	if git.createCalls[0].WorktreeRoot != worktreeRoot {
		t.Fatalf("CreateWorktree().WorktreeRoot = %q, want %q", git.createCalls[0].WorktreeRoot, worktreeRoot)
	}
	if len(git.prepareCalls) != 1 {
		t.Fatalf("len(git.prepareCalls) = %d, want 1", len(git.prepareCalls))
	}
}

func TestBuildFixerMinimalPRSeedUsesEnterpriseHost(t *testing.T) {
	t.Parallel()

	seed := buildFixerMinimalPRSeed("ghe.example.com/acme/looper", 42, &checkpointDetail{}, nil)
	if !strings.Contains(seed, "\"url\": \"https://ghe.example.com/acme/looper/pull/42\"") {
		t.Fatalf("seed = %q, want enterprise host PR URL", seed)
	}
}

func TestQueueItemRequiresLabelAuthority(t *testing.T) {
	t.Parallel()

	if queueItemRequiresLabelAuthority(storage.QueueItemRecord{DedupeKey: "fixer:loop_manual"}) {
		t.Fatal("manual fixer queue should not require auto-discovery label authority")
	}
	payload := mustMarshalJSON(map[string]any{"discoveryFingerprint": buildFixerDiscoveryFingerprint("acme/looper", 42, "head-1", "hash-1")})
	if !queueItemRequiresLabelAuthority(storage.QueueItemRecord{DedupeKey: buildFixerDedupeKey("project_1", "loop_1", "acme/looper", 42, "head-1", "hash-1"), PayloadJSON: &payload}) {
		t.Fatal("auto-discovery fixer queue should require label authority")
	}
}

func TestDiscoverPullRequestsCreatesLoopAndQueue(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{
			{Number: 42, State: "OPEN", HeadSHA: "head-42"},
			{Number: 43, State: "OPEN", IsDraft: true, HeadSHA: "head-43"},
			{Number: 44, State: "OPEN", HeadSHA: "head-44"},
		},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-42", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 44, State: "OPEN", HeadSHA: "head-44"},
		},
	}
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
	if loop == nil || loop.Type != "fixer" || loop.Status != "queued" || loop.Repo == nil || *loop.Repo != "acme/looper" || loop.PRNumber == nil || *loop.PRNumber != 42 {
		t.Fatalf("loop = %#v, want queued fixer loop for #42", loop)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), result.QueueItems[0].ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "queued" || !strings.HasPrefix(queue.DedupeKey, "fixer:project_1:"+result.CreatedLoopIDs[0]+":acme/looper:42:") {
		t.Fatalf("queue = %#v, want queued fixer item for #42", queue)
	}
}

func TestDiscoverPullRequestCreatesLoopAndQueue(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "head-42", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}}}
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
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", IsDraft: true, HeadSHA: "head-42", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: "acme/looper", PRNumber: 42})
	if err != nil {
		t.Fatalf("DiscoverPullRequest() error = %v", err)
	}
	if len(result.QueueItems) != 0 || len(result.CreatedLoopIDs) != 0 || result.Skipped != 1 {
		t.Fatalf("result = %#v, want skipped targeted discovery with no loop", result)
	}
}

func TestDiscoverPullRequestSkipsFixerHoldLabel(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "head-42", Labels: []string{domain.HoldLabelFixer}, Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: "acme/looper", PRNumber: 42})
	if err != nil {
		t.Fatalf("DiscoverPullRequest() error = %v", err)
	}
	if len(result.QueueItems) != 0 || len(result.CreatedLoopIDs) != 0 || result.Skipped != 1 {
		t.Fatalf("result = %#v, want held PR skipped", result)
	}
}

func TestProcessClaimedItemSkipsHeldAutomaticFixerPR(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_hold", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	projectID := "project_1"
	loopID := "loop_fixer_hold"
	lockKey := buildPullRequestLockKey(storage.QueueItemRecord{Repo: &repo, PRNumber: &prNumber})
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_fixer_hold", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:hold", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, LockKey: &lockKey, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{Number: prNumber, State: "OPEN", Labels: []string{domain.HoldLabelFixer}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.ProcessClaimedItem(context.Background(), storage.QueueItemRecord{ID: "queue_fixer_hold", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "running"})
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" {
		t.Fatalf("result = %#v, want skipped", result)
	}
}

func TestDiscoverPullRequestPausesExistingLoopWhenLabelsNoLongerMatch(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "head-42", Labels: []string{"bug"}, Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}, {Number: 42, State: "OPEN", HeadSHA: "head-42", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}, {Number: 42, State: "OPEN", HeadSHA: "head-42", Labels: []string{"bug"}, Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, AuthorFilter: config.FixerAuthorFilterCurrentUser, Labels: []string{"bug"}, LabelMode: config.LabelModeAll}})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	payloadJSON := mustMarshalJSON(map[string]any{"discoveryFingerprint": buildFixerDiscoveryFingerprint(repo, prNumber, "head-42", "hash-1")})
	loop := storage.LoopRecord{ID: "loop_label_gate", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", NextRunAt: &nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue := storage.QueueItemRecord{ID: "queue_label_gate", ProjectID: stringPtr("project_1"), LoopID: &loop.ID, Type: "fixer", TargetType: "pull_request", TargetID: "pr:acme/looper:42", Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:label-gate", Priority: storage.QueuePriorityFixer, Status: "queued", AvailableAt: nowISO, Attempts: 0, MaxAttempts: 3, PayloadJSON: &payloadJSON, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	if _, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: repo, PRNumber: prNumber}); err != nil {
		t.Fatalf("first DiscoverPullRequest() error = %v", err)
	}
	result, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("second DiscoverPullRequest() error = %v", err)
	}
	if result.Skipped != 1 {
		t.Fatalf("result = %#v, want skipped targeted discovery", result)
	}
	persistedLoop, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || persistedLoop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", persistedLoop, err)
	}
	if persistedLoop.Status != "paused" || persistedLoop.NextRunAt != nil {
		t.Fatalf("loop = %#v, want paused loop with no next run", persistedLoop)
	}
	persistedQueue, err := fixture.repos.Queue.GetByID(context.Background(), queue.ID)
	if err != nil || persistedQueue == nil {
		t.Fatalf("Queue.GetByID() = (%#v, %v), want queue", persistedQueue, err)
	}
	if persistedQueue.Status != "completed" {
		t.Fatalf("queue = %#v, want completed queued item after label gate removal", persistedQueue)
	}
	resumed, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("third DiscoverPullRequest() error = %v", err)
	}
	if len(resumed.QueueItems) != 1 {
		t.Fatalf("resumed = %#v, want one requeued item after labels are restored", resumed)
	}
	persistedLoop, err = fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || persistedLoop == nil {
		t.Fatalf("Loops.GetByID(after resume) = (%#v, %v), want loop", persistedLoop, err)
	}
	if persistedLoop.Status != "queued" || persistedLoop.NextRunAt == nil {
		t.Fatalf("loop after resume = %#v, want queued loop with next run", persistedLoop)
	}
}

func TestResumePausedLabelMismatchLoopClearsPendingRediscovery(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})

	nowISO := fixture.nowISO()
	metadata := mustMarshalJSON(map[string]any{
		"pauseReason": labelMismatchPauseReason,
		"pendingFixerRediscovery": map[string]any{
			"headSha":             "head-42",
			"fixItemsStateHash":   "state-42",
			"unresolvedThreadIds": []string{"t1"},
			"recordedAt":          nowISO,
		},
	})
	loop := storage.LoopRecord{ID: "loop_label_resume", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Status: "paused", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	resumed, updated, err := runner.resumePausedLabelMismatchLoop(context.Background(), loop)
	if err != nil {
		t.Fatalf("resumePausedLabelMismatchLoop() error = %v", err)
	}
	if !resumed {
		t.Fatal("resumePausedLabelMismatchLoop() = false, want resumed")
	}
	meta := parseJSONObject(updated.MetadataJSON)
	if _, ok := meta["pauseReason"]; ok {
		t.Fatalf("metadata = %#v, want pauseReason cleared", meta)
	}
	if _, ok := meta["pendingFixerRediscovery"]; ok {
		t.Fatalf("metadata = %#v, want pending rediscovery cleared", meta)
	}
}

func TestDiscoverPullRequestsForBaseBranchUpdateTargetsMatchingBaseBranch(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{
			{Number: 42, State: "OPEN", BaseRefName: "main", HeadSHA: "head-42"},
			{Number: 43, State: "OPEN", BaseRefName: "release", HeadSHA: "head-43"},
		},
		viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "head-42", BaseRefName: "main", HasConflicts: true}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequestsForBaseBranchUpdate(context.Background(), BaseBranchDiscoveryInput{ProjectID: "project_1", Repo: "acme/looper", BaseRefName: "main"})
	if err != nil {
		t.Fatalf("DiscoverPullRequestsForBaseBranchUpdate() error = %v", err)
	}
	if len(result.QueueItems) != 1 || *result.QueueItems[0].PRNumber != 42 {
		t.Fatalf("QueueItems = %#v, want only PR #42", result.QueueItems)
	}
	if len(github.listCalls) != 1 || github.listCalls[0].BaseRefName != "main" {
		t.Fatalf("list calls = %#v, want base branch filtered discovery", github.listCalls)
	}
}

func TestDiscoverPullRequestsForBaseBranchUpdateAppliesLabelFilters(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpenByLabel: map[string][]PullRequestSummary{
			"bug":    {{Number: 42, State: "OPEN", BaseRefName: "main", HeadSHA: "head-42", Labels: []string{"bug"}}},
			"urgent": {{Number: 43, State: "OPEN", BaseRefName: "main", HeadSHA: "head-43", Labels: []string{"urgent"}}},
		},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-42", BaseRefName: "main", HasConflicts: true, Labels: []string{"bug"}},
			{Number: 43, State: "OPEN", HeadSHA: "head-43", BaseRefName: "main", HasConflicts: true, Labels: []string{"urgent"}},
		},
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	labels := []string{"bug", "urgent"}
	labelMode := config.LabelModeAny
	autoDiscovery := true
	authorFilter := config.FixerAuthorFilterAny
	cfg.Projects = append(cfg.Projects, config.ProjectRefConfig{ID: "project_1", RepoPath: t.TempDir(), Roles: &config.PartialRoleConfigs{Fixer: &config.PartialFixerRoleConfig{AutoDiscovery: &autoDiscovery, Triggers: &config.PartialFixerRoleTriggersConfig{AuthorFilter: &authorFilter, Labels: &labels, LabelMode: &labelMode}}}})
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg, FixAllPullRequests: true})

	result, err := runner.DiscoverPullRequestsForBaseBranchUpdate(context.Background(), BaseBranchDiscoveryInput{ProjectID: "project_1", Repo: "acme/looper", BaseRefName: "main"})
	if err != nil {
		t.Fatalf("DiscoverPullRequestsForBaseBranchUpdate() error = %v", err)
	}
	if len(result.QueueItems) != 2 {
		t.Fatalf("len(QueueItems) = %d, want 2", len(result.QueueItems))
	}
	for _, call := range github.listCalls {
		if call.BaseRefName != "main" {
			t.Fatalf("list call = %#v, want base branch filter on every query", call)
		}
	}
}

func TestDiscoverPullRequestReusesExistingActiveQueueItem(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "head-42", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}, {Number: 42, State: "OPEN", HeadSHA: "head-42", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	first, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: "acme/looper", PRNumber: 42})
	if err != nil {
		t.Fatalf("first DiscoverPullRequest() error = %v", err)
	}
	second, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: "acme/looper", PRNumber: 42})
	if err != nil {
		t.Fatalf("second DiscoverPullRequest() error = %v", err)
	}
	if len(first.QueueItems) != 1 || len(second.QueueItems) != 1 || first.QueueItems[0].ID != second.QueueItems[0].ID {
		t.Fatalf("first=%#v second=%#v, want same active queue item reused", first, second)
	}
	queues, err := fixture.repos.Queue.List(context.Background())
	if err != nil {
		t.Fatalf("Queue.List() error = %v", err)
	}
	if len(queues) != 1 {
		t.Fatalf("len(Queue.List()) = %d, want one active queue item", len(queues))
	}
}

func TestDiscoverPullRequestsSkipsPRsNotOwnedByCurrentUser(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		currentUser: "looper-bot",
		listOpen: []PullRequestSummary{
			{Number: 42, State: "OPEN", HeadSHA: "head-42", Author: "human"},
			{Number: 43, State: "OPEN", HeadSHA: "head-43", Author: "looper-bot"},
		},
		viewResponses: []PullRequestDetail{
			{Number: 43, State: "OPEN", HeadSHA: "head-43", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
		},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 || result.QueueItems[0].PRNumber == nil || *result.QueueItems[0].PRNumber != 43 {
		t.Fatalf("QueueItems = %#v, want only owned PR #43", result.QueueItems)
	}
	if len(github.listCalls) != 1 || github.listCalls[0].Author != "looper-bot" {
		t.Fatalf("list calls = %#v, want author-filtered discovery", github.listCalls)
	}
	if github.viewIndex != 1 {
		t.Fatalf("view calls = %d, want 1", github.viewIndex)
	}
}

func TestDiscoverPullRequestsFixAllPullRequestsOptIn(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		currentUser: "looper-bot",
		listOpen:    []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "head-42", Author: "human"}},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-42", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
		},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, FixAllPullRequests: true})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 || result.QueueItems[0].PRNumber == nil || *result.QueueItems[0].PRNumber != 42 {
		t.Fatalf("QueueItems = %#v, want opted-in foreign PR #42", result.QueueItems)
	}
}

func TestDiscoverPullRequestsAppliesLabelFilters(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{
			{Number: 42, State: "OPEN", HeadSHA: "head-42", Labels: []string{"bug", "urgent"}},
			{Number: 43, State: "OPEN", HeadSHA: "head-43", Labels: []string{"bug"}},
		},
		viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "head-42", Labels: []string{"bug", "urgent"}, Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, AuthorFilter: config.FixerAuthorFilterCurrentUser, Labels: []string{"bug", "urgent"}, LabelMode: config.LabelModeAll}})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 || *result.QueueItems[0].PRNumber != 42 {
		t.Fatalf("QueueItems = %#v, want only matching PR #42", result.QueueItems)
	}
	if len(github.listCalls) != 1 || strings.Join(github.listCalls[0].Labels, ",") != "bug,urgent" {
		t.Fatalf("list calls = %#v, want server-side multi-label filter", github.listCalls)
	}
}

func TestDiscoverPullRequestsQueriesEachAnyModeLabel(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen:      []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "head-42", Labels: []string{"bug"}}},
		viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "head-42", Labels: []string{"bug"}, Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, AuthorFilter: config.FixerAuthorFilterCurrentUser, Labels: []string{"bug", "urgent"}, LabelMode: config.LabelModeAny}})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 || *result.QueueItems[0].PRNumber != 42 {
		t.Fatalf("QueueItems = %#v, want matching PR #42", result.QueueItems)
	}
	if len(github.listCalls) != 2 || github.listCalls[0].Label != "bug" || github.listCalls[1].Label != "urgent" {
		t.Fatalf("list calls = %#v, want one server-side query per any-mode label", github.listCalls)
	}
}

func TestListOpenPullRequestsForDiscoveryCapsAnyModeLabelsToLimit(t *testing.T) {
	t.Parallel()
	github := &fakeGitHubGateway{listOpenByLabel: map[string][]PullRequestSummary{
		"bug": {
			{Number: 42, State: "OPEN", HeadSHA: "head-42", Labels: []string{"bug"}},
			{Number: 43, State: "OPEN", HeadSHA: "head-43", Labels: []string{"bug"}},
		},
		"urgent": {
			{Number: 44, State: "OPEN", HeadSHA: "head-44", Labels: []string{"urgent"}},
			{Number: 45, State: "OPEN", HeadSHA: "head-45", Labels: []string{"urgent"}},
		},
	}}
	runner := New(Options{GitHub: github, DiscoveryPolicy: DiscoveryPolicy{Labels: []string{"bug", "urgent"}, LabelMode: config.LabelModeAny, AuthorFilter: config.FixerAuthorFilterAny}})

	prs, err := runner.listOpenPullRequestsForDiscovery(context.Background(), "acme/looper", "/tmp/repo", 2, "looper")
	if err != nil {
		t.Fatalf("listOpenPullRequestsForDiscovery() error = %v", err)
	}
	if len(prs) != 2 || prs[0].Number != 42 || prs[1].Number != 43 {
		t.Fatalf("prs = %#v, want first two unique PRs capped to limit", prs)
	}
	if len(github.listCalls) != 1 || github.listCalls[0].Label != "bug" {
		t.Fatalf("list calls = %#v, want discovery to stop after reaching limit", github.listCalls)
	}
}

func TestListOpenPullRequestsForDiscoveryCapsAnyModeLabelsToDefaultLimit(t *testing.T) {
	t.Parallel()
	firstPage := make([]PullRequestSummary, 30)
	for i := range firstPage {
		firstPage[i] = PullRequestSummary{Number: int64(42 + i), State: "OPEN", HeadSHA: fmt.Sprintf("head-%d", 42+i), Labels: []string{"bug"}}
	}
	github := &fakeGitHubGateway{listOpenByLabel: map[string][]PullRequestSummary{
		"bug":    firstPage,
		"urgent": {{Number: 99, State: "OPEN", HeadSHA: "head-99", Labels: []string{"urgent"}}},
	}}
	runner := New(Options{GitHub: github, DiscoveryPolicy: DiscoveryPolicy{Labels: []string{"bug", "urgent"}, LabelMode: config.LabelModeAny, AuthorFilter: config.FixerAuthorFilterAny}})

	prs, err := runner.listOpenPullRequestsForDiscovery(context.Background(), "acme/looper", "/tmp/repo", 0, "looper")
	if err != nil {
		t.Fatalf("listOpenPullRequestsForDiscovery() error = %v", err)
	}
	if len(prs) != 30 {
		t.Fatalf("len(prs) = %d, want default cap", len(prs))
	}
	if len(github.listCalls) != 1 || github.listCalls[0].Label != "bug" || github.listCalls[0].Limit != 30 {
		t.Fatalf("list calls = %#v, want discovery to use and stop at default limit", github.listCalls)
	}
}

func TestDiscoverPullRequestsPreservesPausedLoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	github := &fakeGitHubGateway{
		listOpen:      []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: "head-42"}},
		viewResponses: []PullRequestDetail{{Number: prNumber, State: "OPEN", HeadSHA: "head-42", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	loop := storage.LoopRecord{ID: "loop_paused", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "paused", CreatedAt: nowISO, UpdatedAt: nowISO}
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

func TestDiscoverPullRequestsResumesPausedNoopResolveLoopWhenFixItemsChange(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	previousFixItem := FixItem{Type: "comment", ID: "c1", ThreadID: "t1", ThreadFingerprint: "c1@old"}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{
		ResumePolicy: loops.ResumePolicyManualIntervention,
		Pause:        newCheckpointPause(checkpointPauseReasonNoopResolveNoNewCommits, true, "head-42", hashFixItemsState([]FixItem{previousFixItem}), []string{"t1"}),
		Detail:       &checkpointDetail{State: "OPEN", HeadSHA: "head-42", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "old blocker"}}},
		FixItems:     []FixItem{previousFixItem},
		FixItemsHash: hashFixItemsState([]FixItem{previousFixItem}),
		Push:         &checkpointPush{Pushed: false, SkippedReason: "No new commits to push"},
		Recheck:      &checkpointRecheck{RemainingFixItems: []FixItem{previousFixItem}},
	})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_paused_noop_resolve", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "paused", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	message := "manual hold recorded by structured pause reason"
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{ID: "run_paused_noop_resolve", LoopID: "loop_paused_noop_resolve", Status: "failed", CurrentStep: stringPtr(string(stepRecheck)), LastCompletedStep: stringPtr(string(stepResolveComments)), CheckpointJSON: &checkpointJSON, Summary: stringPtr(message), ErrorMessage: stringPtr(message), StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{
		listOpen:      []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: "head-42"}},
		viewResponses: []PullRequestDetail{{Number: prNumber, State: "OPEN", HeadSHA: "head-42", Comments: []map[string]any{{"id": "c2", "threadId": "t2", "body": "new blocker"}}}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want one requeued fixer item for changed fix items", result.QueueItems)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), "loop_paused_noop_resolve")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.Status != "queued" || persisted.NextRunAt == nil {
		t.Fatalf("loop = %#v, want queued loop after changed fix items", persisted)
	}
	resumed, err := runner.createRunContext(context.Background(), *persisted)
	if err != nil {
		t.Fatalf("createRunContext() error = %v", err)
	}
	if resumed.Resumed || resumed.StartStep != stepDiscoverPR {
		t.Fatalf("resumed = %#v, want fresh discover run after changed fix items", resumed)
	}
	if resumed.Checkpoint.Detail != nil || len(resumed.Checkpoint.FixItems) != 0 || resumed.Checkpoint.Push != nil || resumed.Checkpoint.Recheck != nil {
		t.Fatalf("checkpoint = %#v, want stale no-op resolve checkpoint cleared", resumed.Checkpoint)
	}
}

func TestDiscoverPullRequestsKeepsPausedNoopResolveLoopForSameFixItems(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	fixItem := FixItem{Type: "comment", ID: "c1", ThreadID: "t1", ThreadFingerprint: "legacy:t1:c1", Summary: "same blocker"}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{
		ResumePolicy: loops.ResumePolicyManualIntervention,
		Detail:       &checkpointDetail{State: "OPEN", HeadSHA: "head-42", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "same blocker"}}},
		FixItems:     []FixItem{fixItem},
		FixItemsHash: hashFixItemsState([]FixItem{fixItem}),
		Push:         &checkpointPush{Pushed: false, SkippedReason: "No new commits to push"},
		Recheck:      &checkpointRecheck{RemainingFixItems: []FixItem{fixItem}},
	})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_paused_noop_same", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "paused", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{ID: "run_paused_noop_same", LoopID: "loop_paused_noop_same", Status: "failed", CurrentStep: stringPtr(string(stepRecheck)), LastCompletedStep: stringPtr(string(stepResolveComments)), CheckpointJSON: &checkpointJSON, Summary: stringPtr(noopResolveManualIntervention), ErrorMessage: stringPtr(noopResolveManualIntervention), StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{
		listOpen:      []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: "head-42"}},
		viewResponses: []PullRequestDetail{{Number: prNumber, State: "OPEN", HeadSHA: "head-42", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "same blocker"}}}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want same no-op fingerprint suppressed", result.QueueItems)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), "loop_paused_noop_same")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.Status != "paused" || persisted.NextRunAt != nil {
		t.Fatalf("loop = %#v, want unchanged paused loop", persisted)
	}
}

func TestDiscoverPullRequestsKeepsPausedNoopResolveLoopWhenOnlyDeclinedThreadRemains(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	activeFixItem := FixItem{Type: "comment", ID: "c1", ThreadID: "t1", ThreadFingerprint: "c1@active", Summary: "same blocker"}
	declinedFixItem := FixItem{Type: "comment", ID: "c2", ThreadID: "t2", ThreadFingerprint: "c2@declined", Summary: "declined blocker"}
	allFixItems := []FixItem{activeFixItem, declinedFixItem}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{
		ResumePolicy: loops.ResumePolicyManualIntervention,
		Pause:        newCheckpointPause(checkpointPauseReasonNoopResolveNoNewCommits, true, "head-42", hashFixItemsState(allFixItems), []string{"t1"}),
		Detail:       &checkpointDetail{State: "OPEN", HeadSHA: "head-42", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "threadFingerprint": "c1@active", "body": "same blocker"}, {"id": "c2", "threadId": "t2", "threadFingerprint": "c2@declined", "body": "declined blocker"}}},
		FixItems:     allFixItems,
		FixItemsHash: hashFixItemsState(allFixItems),
		Push:         &checkpointPush{Pushed: false, SkippedReason: "No new commits to push"},
		Recheck:      &checkpointRecheck{RemainingFixItems: allFixItems},
	})
	metadataJSON := mustMarshalJSON(map[string]any{
		"declinedThreads": map[string]any{
			buildDeclinedThreadFingerprint(declinedFixItem, "head-42"): map[string]any{"threadId": declinedFixItem.ThreadID},
		},
	})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_paused_noop_declined", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "paused", MetadataJSON: &metadataJSON, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	message := "structured pause with declined-thread suppression"
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{ID: "run_paused_noop_declined", LoopID: "loop_paused_noop_declined", Status: "failed", CurrentStep: stringPtr(string(stepRecheck)), LastCompletedStep: stringPtr(string(stepResolveComments)), CheckpointJSON: &checkpointJSON, Summary: stringPtr(message), ErrorMessage: stringPtr(message), StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{
		listOpen:      []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: "head-42"}},
		viewResponses: []PullRequestDetail{{Number: prNumber, State: "OPEN", HeadSHA: "head-42", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "threadFingerprint": "c1@active", "body": "same blocker"}, {"id": "c2", "threadId": "t2", "threadFingerprint": "c2@declined", "body": "declined blocker"}}}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want declined-only delta suppressed", result.QueueItems)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), "loop_paused_noop_declined")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.Status != "paused" || persisted.NextRunAt != nil {
		t.Fatalf("loop = %#v, want unchanged paused loop", persisted)
	}
}

func TestDiscoverPullRequestsResumesPausedRiskyConflictLoopWhenFixItemsChange(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	previousFixItem := FixItem{Type: "conflict", Files: []string{}}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{
		ResumePolicy: loops.ResumePolicyManualIntervention,
		Pause:        newCheckpointPause(checkpointPauseReasonRiskyConflict, true, "head-42", hashFixItemsState([]FixItem{previousFixItem}), nil),
		Detail:       &checkpointDetail{State: "OPEN", HeadSHA: "head-42", HasConflicts: true},
		FixItems:     []FixItem{previousFixItem},
		FixItemsHash: hashFixItemsState([]FixItem{previousFixItem}),
	})
	message := "manual hold recorded by structured conflict reason"
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_paused_risky_conflict", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "paused", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{ID: "run_paused_risky_conflict", LoopID: "loop_paused_risky_conflict", Status: "failed", CurrentStep: stringPtr(string(stepRepair)), LastCompletedStep: stringPtr(string(stepPrepareWorktree)), CheckpointJSON: &checkpointJSON, Summary: stringPtr(message), ErrorMessage: stringPtr(message), StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{
		listOpen:      []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: "head-42"}},
		viewResponses: []PullRequestDetail{{Number: prNumber, State: "OPEN", HeadSHA: "head-42", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "now fix this comment"}}}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want one requeued fixer item after conflict signal changed", result.QueueItems)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), "loop_paused_risky_conflict")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	resumed, err := runner.createRunContext(context.Background(), *persisted)
	if err != nil {
		t.Fatalf("createRunContext() error = %v", err)
	}
	if resumed.Resumed || resumed.StartStep != stepDiscoverPR {
		t.Fatalf("resumed = %#v, want fresh discover run after risky-conflict signal changed", resumed)
	}
}

func TestDiscoverPullRequestsKeepsPausedRiskyConflictLoopForSameConflict(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	fixItem := FixItem{Type: "conflict", Files: []string{}}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{ResumePolicy: loops.ResumePolicyManualIntervention, Detail: &checkpointDetail{State: "OPEN", HeadSHA: "head-42", HasConflicts: true}, FixItems: []FixItem{fixItem}, FixItemsHash: hashFixItemsState([]FixItem{fixItem})})
	message := "Skipped acme/looper#42 because risky conflict fixes require manual intervention"
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_paused_risky_same", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "paused", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{ID: "run_paused_risky_same", LoopID: "loop_paused_risky_same", Status: "failed", CurrentStep: stringPtr(string(stepRepair)), LastCompletedStep: stringPtr(string(stepPrepareWorktree)), CheckpointJSON: &checkpointJSON, Summary: stringPtr(message), ErrorMessage: stringPtr(message), StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: "head-42"}}, viewResponses: []PullRequestDetail{{Number: prNumber, State: "OPEN", HeadSHA: "head-42", HasConflicts: true}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want same risky-conflict fingerprint suppressed", result.QueueItems)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), "loop_paused_risky_same")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.Status != "paused" || persisted.NextRunAt != nil {
		t.Fatalf("loop = %#v, want unchanged paused loop", persisted)
	}
}

func TestDiscoverPullRequestsKeepsOtherManualInterventionPausedOnNewSignal(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{ResumePolicy: loops.ResumePolicyManualIntervention, Pause: newCheckpointPause(checkpointPauseReasonAutoPushDisabled, false, "", "", nil), Detail: &checkpointDetail{State: "OPEN", HeadSHA: "old-head"}})
	message := "human must inspect push state"
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_paused_auto_push", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "paused", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{ID: "run_paused_auto_push", LoopID: "loop_paused_auto_push", Status: "failed", CurrentStep: stringPtr(string(stepPush)), LastCompletedStep: stringPtr(string(stepValidate)), CheckpointJSON: &checkpointJSON, Summary: stringPtr(message), ErrorMessage: stringPtr(message), StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: "new-head"}}, viewResponses: []PullRequestDetail{{Number: prNumber, State: "OPEN", HeadSHA: "new-head", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "new blocker"}}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want auto-push hard hold to remain paused", result.QueueItems)
	}
}

func TestClassifyFixerPauseUsesStructuredReason(t *testing.T) {
	t.Parallel()

	fixItem := FixItem{Type: "comment", ID: "c1", ThreadID: "t1"}
	checkpoint := fixerCheckpoint{
		Pause:    newCheckpointPause(checkpointPauseReasonNoopResolveNoNewCommits, true, "head-1", hashFixItemsState([]FixItem{fixItem}), []string{"t1"}),
		FixItems: []FixItem{fixItem},
	}
	message := "totally unrelated human message"
	run := &storage.RunRecord{Status: "failed", CurrentStep: stringPtr(string(stepRecheck)), Summary: &message, ErrorMessage: &message}

	pause, ok := classifyFixerPause(run, checkpoint, nil)
	if !ok {
		t.Fatal("classifyFixerPause() = not found, want structured pause")
	}
	if pause.Reason != string(checkpointPauseReasonNoopResolveNoNewCommits) || !pause.Recoverable {
		t.Fatalf("pause = %#v, want recoverable structured no-op reason", pause)
	}
	if pause.HeadSHA != "head-1" || pause.FixItemsStateHash != hashFixItemsState([]FixItem{fixItem}) || !sameStringSlices(pause.UnresolvedThreadIDs, []string{"t1"}) {
		t.Fatalf("pause = %#v, want structured fingerprint preserved", pause)
	}
}

func TestClassifyFixerPauseFallsBackToLegacyNoopMessage(t *testing.T) {
	t.Parallel()

	fixItem := FixItem{Type: "comment", ID: "c1", ThreadID: "t1"}
	checkpoint := fixerCheckpoint{
		Detail:       &checkpointDetail{HeadSHA: "head-1"},
		FixItems:     []FixItem{fixItem},
		FixItemsHash: hashFixItemsState([]FixItem{fixItem}),
		Recheck:      &checkpointRecheck{RemainingFixItems: []FixItem{fixItem}},
	}
	run := &storage.RunRecord{Status: "failed", CurrentStep: stringPtr(string(stepRecheck)), ErrorMessage: stringPtr(noopResolveManualIntervention)}

	pause, ok := classifyFixerPause(run, checkpoint, nil)
	if !ok {
		t.Fatal("classifyFixerPause() = not found, want legacy compatibility pause")
	}
	if pause.Reason != string(checkpointPauseReasonNoopResolveNoNewCommits) || !pause.Recoverable {
		t.Fatalf("pause = %#v, want recoverable legacy no-op reason", pause)
	}
	if pause.HeadSHA != "head-1" || pause.FixItemsStateHash != hashFixItemsState([]FixItem{fixItem}) || !sameStringSlices(pause.UnresolvedThreadIDs, []string{"t1"}) {
		t.Fatalf("pause = %#v, want legacy fingerprint derived from checkpoint", pause)
	}
}

func TestClassifyFixerPauseUsesLegacyFixItemsHashWhenFixItemsMissing(t *testing.T) {
	t.Parallel()

	checkpoint := fixerCheckpoint{
		Detail:       &checkpointDetail{HeadSHA: "head-1"},
		FixItemsHash: "legacy-fix-items-hash",
		Recheck:      &checkpointRecheck{RemainingFixItems: []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}}},
	}
	run := &storage.RunRecord{Status: "failed", CurrentStep: stringPtr(string(stepRecheck)), ErrorMessage: stringPtr(noopResolveManualIntervention)}

	pause, ok := classifyFixerPause(run, checkpoint, nil)
	if !ok {
		t.Fatal("classifyFixerPause() = not found, want legacy compatibility pause")
	}
	if pause.FixItemsStateHash != "legacy-fix-items-hash" {
		t.Fatalf("pause.FixItemsStateHash = %q, want legacy-fix-items-hash", pause.FixItemsStateHash)
	}
}

func TestClassifyFixerPauseFallsBackToLegacyRiskyConflictErrorMessage(t *testing.T) {
	t.Parallel()

	fixItem := FixItem{Type: "conflict", Files: []string{"a.go"}}
	checkpoint := fixerCheckpoint{
		Detail:       &checkpointDetail{HeadSHA: "head-1"},
		FixItems:     []FixItem{fixItem},
		FixItemsHash: hashFixItemsState([]FixItem{fixItem}),
	}
	summary := "generic manual hold"
	errorMessage := "Skipped acme/looper#42 because risky conflict fixes require manual intervention"
	run := &storage.RunRecord{Status: "failed", CurrentStep: stringPtr(string(stepRepair)), Summary: &summary, ErrorMessage: &errorMessage}

	pause, ok := classifyFixerPause(run, checkpoint, nil)
	if !ok {
		t.Fatal("classifyFixerPause() = not found, want legacy risky-conflict compatibility pause")
	}
	if pause.Reason != string(checkpointPauseReasonRiskyConflict) || !pause.Recoverable {
		t.Fatalf("pause = %#v, want recoverable risky-conflict pause", pause)
	}
}

func TestRunRecheckStepRecordsPauseWithLiveHead(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "new-head", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "still blocked"}}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})
	checkpoint := fixerCheckpoint{
		Detail:           &checkpointDetail{State: "OPEN", HeadSHA: "old-head"},
		FixItems:         []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}},
		FixItemsHash:     hashFixItemsState([]FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}}),
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}
	updated, err := runner.runRecheckStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()}, Loop: storage.LoopRecord{ID: "loop_1", ProjectID: "project_1"}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err == nil {
		t.Fatal("runRecheckStep() error = nil, want manual intervention hold")
	}
	loopErr, ok := err.(*loopError)
	if !ok || loopErr.kind != FailureManualIntervention {
		t.Fatalf("runRecheckStep() error = %#v, want manual_intervention loopError", err)
	}
	if updated.Pause == nil || updated.Pause.HeadSHA != "new-head" {
		t.Fatalf("Pause = %#v, want live recheck head recorded", updated.Pause)
	}
}

func TestEnqueueScopesFixerDedupeKeyToLoop(t *testing.T) {
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
	headSHA := "head-42"
	fixItemsHash := "fix-hash"

	if err := fixture.repos.Projects.Upsert(context.Background(), storage.ProjectRecord{ID: project2ID, Name: "Looper Two", RepoPath: repoPath2, BaseBranch: &baseBranch, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Projects.Upsert(project_2) error = %v", err)
	}
	for _, loop := range []storage.LoopRecord{
		{ID: loop1ID, Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO},
		{ID: loop2ID, Seq: 2, ProjectID: project2ID, Type: "fixer", TargetType: "pull_request", Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO},
	} {
		if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
			t.Fatalf("Loops.Upsert(%s) error = %v", loop.ID, err)
		}
	}

	first, err := runner.enqueue(context.Background(), enqueueInput{ProjectID: "project_1", LoopID: loop1ID, Repo: repo, PRNumber: prNumber, HeadSHA: headSHA, FixItemsHash: fixItemsHash})
	if err != nil {
		t.Fatalf("enqueue(first) error = %v", err)
	}
	second, err := runner.enqueue(context.Background(), enqueueInput{ProjectID: project2ID, LoopID: loop2ID, Repo: repo, PRNumber: prNumber, HeadSHA: headSHA, FixItemsHash: fixItemsHash})
	if err != nil {
		t.Fatalf("enqueue(second) error = %v", err)
	}
	if first.ID == second.ID {
		t.Fatalf("enqueue(second) reused queue item %q across loops", second.ID)
	}
	if second.LoopID == nil || *second.LoopID != loop2ID {
		t.Fatalf("second loopID = %#v, want %q", second.LoopID, loop2ID)
	}
	if second.DedupeKey != buildFixerDedupeKey(project2ID, loop2ID, repo, prNumber, headSHA, fixItemsHash) {
		t.Fatalf("second dedupe key = %q, want scoped fixer key", second.DedupeKey)
	}
	items, err := fixture.repos.Queue.List(context.Background())
	if err != nil {
		t.Fatalf("Queue.List() error = %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("len(Queue.List()) = %d, want 2", len(items))
	}
}

func TestEnqueueKeepsPendingManualFixerQueueUnchanged(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})

	repo := "acme/looper"
	prNumber := int64(42)
	loopID := "loop_manual"
	nowISO := fixture.nowISO()
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: loopID, Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	manual := storage.QueueItemRecord{ID: "queue_manual", ProjectID: stringPtr("project_1"), LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: buildPullRequestTargetID(repo, prNumber), Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:loop_manual", Priority: storage.QueuePriorityFixer, Status: "queued", AvailableAt: nowISO, Attempts: 0, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), manual); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	item, err := runner.enqueue(context.Background(), enqueueInput{ProjectID: "project_1", LoopID: loopID, Repo: repo, PRNumber: prNumber, HeadSHA: "head-1", FixItemsHash: "hash-1"})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	if item.ID != manual.ID || item.DedupeKey != manual.DedupeKey || item.PayloadJSON != nil {
		t.Fatalf("item = %#v, want existing manual queue preserved", item)
	}
}

func TestProcessClaimedItemCompletesSuccessfulFlow(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "head-1"}},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}, Checks: []map[string]any{{"name": "ci", "conclusion": "FAILURE"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}, Checks: []map[string]any{{"name": "ci", "conclusion": "FAILURE"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}, Checks: []map[string]any{{"name": "ci", "conclusion": "FAILURE"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}, Checks: []map[string]any{{"name": "ci", "conclusion": "FAILURE"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}, Checks: []map[string]any{{"name": "ci", "conclusion": "FAILURE"}}},
		},
	}
	git := &fakeGitGateway{
		createResult:  CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
		inspectResults: []InspectHeadResult{
			{HeadSHA: "base-head"},
			{HeadSHA: "new-head", NewCommitSHAs: []string{"new-head"}},
			{HeadSHA: "new-head"},
		},
	}
	stdout := fmt.Sprintf(`__LOOPER_RESULT__={"summary":"applied fixes","review_thread_replies":[{"fixItemId":"c1","threadId":"t1","explanation":"Adjusted the off-by-one handling and verified the fix.","threadCommentsObserved":"%s"}]}`+"\n", hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}}))
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "applied fixes", ParseStatus: "parsed", Stdout: stdout}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(agent.starts) != 1 || len(git.pushCalls) != 1 || len(github.resolveCalls) != 1 {
		t.Fatalf("agent starts=%d push calls=%d resolve calls=%d, want 1/1/1", len(agent.starts), len(git.pushCalls), len(github.resolveCalls))
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), claim.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "completed" {
		t.Fatalf("queue = %#v, want completed", queue)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.Status != "completed" {
		t.Fatalf("loop = %#v, want completed", loop)
	}
	run, err := fixture.repos.Runs.GetByID(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("Runs.GetByID() error = %v", err)
	}
	if run == nil || run.Status != "success" || run.LastCompletedStep == nil || *run.LastCompletedStep != string(stepRecheck) {
		t.Fatalf("run = %#v, want success through recheck", run)
	}
	checkpoint := parseCheckpoint(run.CheckpointJSON)
	if checkpoint.Detail == nil || checkpoint.Detail.HeadSHA != "new-head" {
		t.Fatalf("checkpoint.Detail = %#v, want pushed head new-head", checkpoint.Detail)
	}
	if checkpoint.Push == nil || checkpoint.Push.HeadSHA != "new-head" {
		t.Fatalf("checkpoint.Push = %#v, want recorded pushed head new-head", checkpoint.Push)
	}
	if checkpoint.ReconcileCommits == nil || checkpoint.ReconcileCommits.FinalHeadSHA != "new-head" {
		t.Fatalf("checkpoint.ReconcileCommits = %#v, want refreshed final head new-head", checkpoint.ReconcileCommits)
	}
}

func TestProcessClaimedItemParksBudgetAfterCapReachingPushWithoutResolve(t *testing.T) {
	// Cap-reaching push must park immediately and must not continue into resolve-comments.
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := buildPullRequestTargetID(repo, prNumber)
	reviewer := storage.LoopRecord{ID: "loop_reviewer_budget_cap_push", Seq: 2, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "waiting", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), reviewer); err != nil {
		t.Fatalf("Loops.Upsert(reviewer) error = %v", err)
	}
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "head-1"}},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}, Checks: []map[string]any{{"name": "ci", "conclusion": "FAILURE"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}, Checks: []map[string]any{{"name": "ci", "conclusion": "FAILURE"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}, Checks: []map[string]any{{"name": "ci", "conclusion": "FAILURE"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}, Checks: []map[string]any{{"name": "ci", "conclusion": "FAILURE"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}, Checks: []map[string]any{{"name": "ci", "conclusion": "FAILURE"}}},
		},
	}
	git := &fakeGitGateway{
		createResult:  CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
		inspectResults: []InspectHeadResult{
			{HeadSHA: "base-head"},
			{HeadSHA: "new-head", NewCommitSHAs: []string{"new-head"}},
			{HeadSHA: "new-head"},
		},
	}
	stdout := fmt.Sprintf(`__LOOPER_RESULT__={"summary":"applied fixes","review_thread_replies":[{"fixItemId":"c1","threadId":"t1","explanation":"Adjusted the off-by-one handling and verified the fix.","threadCommentsObserved":"%s"}]}`+"\n", hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}}))
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "applied fixes", ParseStatus: "parsed", Stdout: stdout}}}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Roles.Fixer.Behavior.Loop.MaxPushesPerPR = 1
	cfg.HITL.Enabled = true
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg, HITLEnabled: true})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" {
		t.Fatalf("result = %#v, want skipped after cap-reaching push park", result)
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %d, want 0 (no resolve-comments after budget park)", len(github.resolveCalls))
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil || loop == nil || loop.Status != "awaiting_human" {
		t.Fatalf("fixer = (%#v, %v), want awaiting_human budget hold", loop, err)
	}
	if loops.ReadReviewFixBudgetState(loop.MetadataJSON).PushCount < 1 {
		t.Fatalf("push count = %d, want cap-reaching push counted", loops.ReadReviewFixBudgetState(loop.MetadataJSON).PushCount)
	}
	if ask, ok := loops.ReadHITLAsk(loop.MetadataJSON); !ok || !loops.IsReviewFixBudgetAsk(ask) {
		t.Fatalf("HITL ask = %#v, want budget ask", ask)
	}
	sibling, err := fixture.repos.Loops.GetByID(context.Background(), reviewer.ID)
	if err != nil || sibling == nil || sibling.Status != "paused" || !loops.IsSiblingReviewFixBudgetPause(sibling.MetadataJSON) {
		t.Fatalf("reviewer sibling = (%#v, %v), want paused sibling hold", sibling, err)
	}
}

func TestEnsureFixerPushBudgetCountedTerminatesWhenPRClosed(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	target := "pr:acme/looper:42"
	meta := `{"reviewFixBudget":{"pushCount":0}}`
	fixer := storage.LoopRecord{ID: "loop_push_closed_pr", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &target, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &meta, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), fixer); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	run := storage.RunRecord{ID: "run_push_closed_pr", LoopID: fixer.ID, Status: "running", StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), run); err != nil {
		t.Fatalf("Runs.Upsert: %v", err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.Roles.Fixer.Behavior.Loop.MaxPushesPerPR = 1
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{Number: 42, State: "MERGED", HeadSHA: "new-head"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, CustomInstructions: &cfg, HITLEnabled: true, GitHub: github})
	checkpoint := fixerCheckpoint{Push: &checkpointPush{Pushed: true, BudgetCounted: false}}
	_, err = runner.ensureFixerPushBudgetCounted(context.Background(), stepInput{
		Project: storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Loop:    fixer, Run: run, Repo: repo, PRNumber: prNumber,
	}, checkpoint)
	var hold *holdSkipError
	if err == nil || !errors.As(err, &hold) {
		t.Fatalf("err = %v, want holdSkipError for closed PR", err)
	}
	after, _ := fixture.repos.Loops.GetByID(context.Background(), fixer.ID)
	if after == nil || after.Status != "terminated" {
		t.Fatalf("after = %#v, want terminated (PR-closed wins over budget park)", after)
	}
	if ask, ok := loops.ReadHITLAsk(after.MetadataJSON); ok && loops.IsReviewFixBudgetAsk(ask) {
		t.Fatalf("budget ask = %#v, want none when PR closed", ask)
	}
}

func TestProcessClaimedItemDoesNotResolveCommentsWhenRepairProducesNoCommits(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "base-head"}},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
		},
	}
	git := &fakeGitGateway{
		createResult:  CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
		inspectResults: []InspectHeadResult{
			{HeadSHA: "base-head"},
			{HeadSHA: "base-head"},
			{HeadSHA: "base-head"},
		},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "blocked before editing because gh could not validate PR metadata", ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableAfterResume {
		t.Fatalf("result = %#v, want failed retryable-after-resume completion", result)
	}
	if len(git.commitCalls) != 0 || len(git.pushCalls) != 0 || len(github.resolveCalls) != 0 {
		t.Fatalf("commit calls=%d push calls=%d resolve calls=%d, want 0/0/0 after no-op repair", len(git.commitCalls), len(git.pushCalls), len(github.resolveCalls))
	}
	run, err := fixture.repos.Runs.GetByID(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("Runs.GetByID() error = %v", err)
	}
	if run == nil || run.Status != "failed" {
		t.Fatalf("run = %#v, want failed run", run)
	}
	checkpoint := parseCheckpoint(run.CheckpointJSON)
	if checkpoint.Push == nil || checkpoint.Push.Pushed || checkpoint.Push.SkippedReason == "" {
		t.Fatalf("checkpoint.Push = %#v, want recorded no-op push", checkpoint.Push)
	}
	if checkpoint.ResumePolicy != loops.ResumePolicyRestartFromDiscover {
		t.Fatalf("checkpoint.ResumePolicy = %q, want restart_from_discover", checkpoint.ResumePolicy)
	}
	if checkpoint.ResolvedComments == nil || len(checkpoint.ResolvedComments.Items) == 0 || checkpoint.ResolvedComments.Items[0].Status != "skipped_missing_agent_decision" {
		t.Fatalf("checkpoint.ResolvedComments = %#v, want missing-decision marker", checkpoint.ResolvedComments)
	}
	if len(github.replyCalls) != 0 {
		t.Fatalf("reply calls = %#v, want none for missing agent decision", github.replyCalls)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), claim.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "queued" || queue.LastErrorKind == nil || *queue.LastErrorKind != string(FailureRetryableAfterResume) {
		t.Fatalf("queue = %#v, want queued retryable queue item", queue)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.Status != "queued" || loop.NextRunAt == nil {
		t.Fatalf("loop = %#v, want queued loop with scheduled retry", loop)
	}
	activeFollowup, err := fixture.repos.Queue.FindActiveByLoopID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("Queue.FindActiveByLoopID() error = %v", err)
	}
	if activeFollowup == nil {
		t.Fatalf("active follow-up queue item = %#v, want scheduled retry", activeFollowup)
	}
}

func TestDiscoverPullRequestsSkipsFailedFixerLoopWhenFingerprintUnchanged(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(77)
	nowISO := fixture.nowISO()
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "same-head", HeadRefName: "feature/fix-77", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}
	fingerprint := buildFixerDiscoveryFingerprint(repo, prNumber, detail.HeadSHA, hashFixItems(collectFixItems(detail)))
	metadata := fmt.Sprintf(`{"autonomousRecovery":{"lastFailedDiscoveryFingerprint":%q}}`, fingerprint)
	targetID := buildPullRequestTargetID(repo, prNumber)
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_failed_same_fp", Seq: 90, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &targetID, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: "same-head"}}, viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want none for unchanged failed fingerprint", result.QueueItems)
	}
}

func TestDiscoverPullRequestsRequeuesFailedFixerLoopWhenFingerprintChanges(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(78)
	nowISO := fixture.nowISO()
	oldDetail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "old-head", HeadRefName: "feature/fix-78", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}
	newDetail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-78", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}
	fingerprint := buildFixerDiscoveryFingerprint(repo, prNumber, oldDetail.HeadSHA, hashFixItems(collectFixItems(oldDetail)))
	metadata := fmt.Sprintf(`{"autonomousRecovery":{"lastFailedDiscoveryFingerprint":%q}}`, fingerprint)
	targetID := buildPullRequestTargetID(repo, prNumber)
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_failed_changed_fp", Seq: 91, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &targetID, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: "new-head"}}, viewResponses: []PullRequestDetail{newDetail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want one queue item after fingerprint change", result.QueueItems)
	}
}

func TestDiscoverPullRequestsSkipsDeclinedThreadWhenFingerprintMatches(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(79)
	nowISO := fixture.nowISO()
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "same-head", HeadRefName: "feature/fix-79", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix", "threadFingerprint": "thread-hash-1"}}}
	fixItems := collectFixItems(detail)
	declinedFingerprint := buildDeclinedThreadFingerprint(fixItems[0], detail.HeadSHA)
	metadata := mustMarshalJSON(map[string]any{"declinedThreads": map[string]any{declinedFingerprint: map[string]any{"recordedAt": nowISO, "threadId": "t1", "reason": "already fixed"}}})
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_declined_same_fp", Seq: 92, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: detail.HeadSHA}}, viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want none for declined fingerprint match", result.QueueItems)
	}
}

func TestDiscoverPullRequestsRequeuesDeclinedThreadWhenHeadChanges(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(80)
	nowISO := fixture.nowISO()
	oldDetail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "old-head", HeadRefName: "feature/fix-80", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix", "threadFingerprint": "thread-hash-1"}}}
	oldFixItems := collectFixItems(oldDetail)
	declinedFingerprint := buildDeclinedThreadFingerprint(oldFixItems[0], oldDetail.HeadSHA)
	metadata := mustMarshalJSON(map[string]any{"declinedThreads": map[string]any{declinedFingerprint: map[string]any{"recordedAt": nowISO, "threadId": "t1", "reason": "already fixed"}}})
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_declined_changed_head", Seq: 93, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	newDetail := oldDetail
	newDetail.HeadSHA = "new-head"
	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: newDetail.HeadSHA}}, viewResponses: []PullRequestDetail{newDetail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want one queue item after head change", result.QueueItems)
	}
}

func TestDiscoverPullRequestsKeepsPausedZeroProgressLoopWhenOnlySuppressedDeclinesDiffer(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(80)
	nowISO := fixture.nowISO()
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "same-head", HeadRefName: "feature/fix-80", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix", "threadFingerprint": "thread-hash-1"}, {"id": "c2", "threadId": "t2", "body": "please fix too", "threadFingerprint": "thread-hash-2"}}}
	allFixItems := collectFixItems(detail)
	declinedFingerprint := buildDeclinedThreadFingerprint(allFixItems[0], detail.HeadSHA)
	metadata := mustMarshalJSON(map[string]any{
		"pauseReason": zeroProgressPauseReason,
		"fixerZeroProgress": map[string]any{
			"headSha":           detail.HeadSHA,
			"fixItemsHash":      hashFixItems(allFixItems),
			"fixItemsStateHash": hashFixItemsState(allFixItems),
			"consecutiveCount":  3,
			"recordedAt":        nowISO,
		},
		"declinedThreads": map[string]any{declinedFingerprint: map[string]any{"recordedAt": nowISO, "threadId": "t1", "reason": "already declined"}},
	})
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_paused_declined_mixed", Seq: 94, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "paused", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: detail.HeadSHA}}, viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want none when full fix-item state is unchanged", result.QueueItems)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), "loop_paused_declined_mixed")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.Status != "paused" {
		t.Fatalf("loop = %#v, want paused loop to remain paused", persisted)
	}
}

func TestProcessClaimedItemAllowsNoCommitWhenCommentsAlreadyResolved(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	unresolved := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	resolved := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix", "isResolved": true, "state": "RESOLVED"}
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "base-head"}},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{unresolved}},
			{Number: 42, State: "OPEN", HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{unresolved}},
			{Number: 42, State: "OPEN", HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{resolved}},
			{Number: 42, State: "OPEN", HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{resolved}},
			{Number: 42, State: "OPEN", HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{resolved}},
		},
	}
	git := &fakeGitGateway{
		createResult:  CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
		inspectResults: []InspectHeadResult{
			{HeadSHA: "base-head"},
			{HeadSHA: "base-head"},
			{HeadSHA: "base-head"},
		},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "no edit needed because comment was already resolved", ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success after live comments are already resolved", result)
	}
	if len(git.commitCalls) != 0 || len(git.pushCalls) != 0 || len(github.resolveCalls) != 0 {
		t.Fatalf("commit calls=%d push calls=%d resolve calls=%d, want 0/0/0 after already-resolved live comments", len(git.commitCalls), len(git.pushCalls), len(github.resolveCalls))
	}
}

func TestDiscoverPullRequestsDefersRepeatedNoopResolveRediscoveryBeforeCooldown(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	comment := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{comment}}
	fixItemsStateHash := hashFixItemsState(collectFixItems(detail))
	metadata := mustMarshalJSON(map[string]any{"lastNoopResolveHeadSha": detail.HeadSHA, "lastNoopResolveStateHash": fixItemsStateHash, "lastNoopResolveAt": nowISO})
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_noop", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: detail.HeadSHA}}, viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want delayed retry queue item for unchanged no-op resolve state", result.QueueItems)
	}
	wantAvailableAt := eventlog.FormatJavaScriptISOString(fixture.now().Add(time.Minute))
	if result.QueueItems[0].AvailableAt != wantAvailableAt {
		t.Fatalf("AvailableAt = %q, want %q", result.QueueItems[0].AvailableAt, wantAvailableAt)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), "loop_fixer_noop")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.NextRunAt == nil || *persisted.NextRunAt != wantAvailableAt {
		t.Fatalf("persisted loop = %#v, want NextRunAt %q", persisted, wantAvailableAt)
	}
}

func TestEnqueueMovesExistingDelayedFixerItemEarlierForSameDedupeKey(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})
	projectID := "project_1"
	loopID := "loop_1"
	repo := "acme/looper"
	prNumber := int64(42)
	lockKey := fmt.Sprintf("pr:%s:%d", repo, prNumber)
	dedupeKey := buildFixerDedupeKey(projectID, loopID, repo, prNumber, "head-1", "hash-1")
	later := eventlog.FormatJavaScriptISOString(fixture.now().Add(5 * time.Minute))
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: loopID, Seq: 1, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_1", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: buildPullRequestTargetID(repo, prNumber), Repo: &repo, PRNumber: &prNumber, DedupeKey: dedupeKey, Priority: storage.QueuePriorityFixer, Status: "queued", AvailableAt: later, Attempts: 0, MaxAttempts: 3, LockKey: &lockKey, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	item, err := runner.enqueue(context.Background(), enqueueInput{ProjectID: projectID, LoopID: loopID, Repo: repo, PRNumber: prNumber, HeadSHA: "head-1", FixItemsHash: "hash-1", AvailableAt: fixture.now()})
	if err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	if item.AvailableAt != fixture.nowISO() {
		t.Fatalf("AvailableAt = %q, want %q", item.AvailableAt, fixture.nowISO())
	}
}

func TestDiscoverPullRequestsRequeuesNoopResolveLoopWhenStateChanges(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		detail PullRequestDetail
	}{
		{name: "head changes", detail: PullRequestDetail{Number: 42, State: "OPEN", HeadSHA: "head-2", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}},
		{name: "fix items change", detail: PullRequestDetail{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}, {"id": "c2", "threadId": "t2", "body": "also fix this"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRunnerFixture(t)
			repo := "acme/looper"
			prNumber := int64(42)
			nowISO := fixture.nowISO()
			baseline := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}
			metadata := mustMarshalJSON(map[string]any{"lastNoopResolveHeadSha": baseline.HeadSHA, "lastNoopResolveStateHash": hashFixItemsState(collectFixItems(baseline)), "lastNoopResolveAt": nowISO})
			loopTarget := buildPullRequestTargetID(repo, prNumber)
			if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_noop", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
				t.Fatalf("Loops.Upsert() error = %v", err)
			}

			github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: tc.detail.HeadSHA}}, viewResponses: []PullRequestDetail{tc.detail}}
			runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

			result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
			if err != nil {
				t.Fatalf("DiscoverPullRequests() error = %v", err)
			}
			if len(result.QueueItems) != 1 {
				t.Fatalf("QueueItems = %#v, want one queue item after state change", result.QueueItems)
			}
		})
	}
}

func TestDiscoverPullRequestsRequeuesNoopResolveLoopWhenRecoverableEvidenceExists(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "threadFingerprint": "latest=c1|updated=2026-04-11T12:00:00Z|count=1", "body": "please fix"}}}
	fixItems := collectFixItems(detail)
	metadata := mustJSON(t, map[string]any{
		"lastNoopResolveHeadSha":   detail.HeadSHA,
		"lastNoopResolveStateHash": hashFixItemsState(fixItems),
		"fixEvidenceStoreV2": &fixEvidenceStoreV2{Version: 2, Threads: map[string][]threadFixEvidence{
			"t1": {{ThreadID: "t1", ThreadFingerprint: "latest=c1|updated=2026-04-11T12:00:00Z|count=1", EvidenceHeadSHA: "head-1", ValidationHeadSHA: "head-1", CommitSHA: "head-1", ProducedNewCommits: true, Explanation: "Confirmed fix.", ResolveState: "pending"}},
		}},
	})
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_recoverable_noop", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}

	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: detail.HeadSHA}}, viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want recovery rediscovery to enqueue", result.QueueItems)
	}
}

func TestDecideRediscoveryAfterNoopResolveEnqueuesWhenThreadSetChangesEvenIfHashMatches(t *testing.T) {
	t.Parallel()

	loopMeta := mustMarshalJSON(map[string]any{"fixerFollowup": map[string]any{"reason": "missing_evidence", "headSha": "head-1", "fixItemsStateHash": "same-hash", "unresolvedThreadIds": []string{"t1"}, "attemptsForFingerprint": 2, "lastAttemptAt": "2026-04-11T12:00:00.000Z", "nextEligibleAt": "2026-04-11T12:05:00.000Z"}})
	decision := decideRediscoveryAfterNoopResolve(storage.LoopRecord{ID: "loop_1", MetadataJSON: &loopMeta, UpdatedAt: "2026-04-11T12:00:00.000Z"}, "head-1", "same-hash", "same-hash", []FixItem{{Type: "comment", ID: "c2", ThreadID: "t2", ThreadFingerprint: "latest=c2|updated=2026-04-11T12:01:00Z|count=1"}}, []string{"t2"}, time.Date(2026, time.April, 11, 12, 1, 0, 0, time.UTC))
	if decision.Action != rediscoveryActionEnqueue {
		t.Fatalf("decision = %#v, want enqueue on thread-set change", decision)
	}
}

func TestDecideRediscoveryAfterNoopResolveEnqueuesAfterCooldownExpires(t *testing.T) {
	t.Parallel()

	loopMeta := mustMarshalJSON(map[string]any{"fixerFollowup": map[string]any{"reason": "missing_evidence", "headSha": "head-1", "fixItemsStateHash": "same-hash", "unresolvedThreadIds": []string{"t1"}, "attemptsForFingerprint": 1, "lastAttemptAt": "2026-04-11T12:00:00.000Z", "nextEligibleAt": "2026-04-11T12:01:00.000Z"}})
	decision := decideRediscoveryAfterNoopResolve(storage.LoopRecord{ID: "loop_1", MetadataJSON: &loopMeta, UpdatedAt: "2026-04-11T12:00:00.000Z"}, "head-1", "same-hash", "same-hash", []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", ThreadFingerprint: "latest=c1|updated=2026-04-11T12:00:00Z|count=1"}}, []string{"t1"}, time.Date(2026, time.April, 11, 12, 2, 0, 0, time.UTC))
	if decision.Action != rediscoveryActionEnqueue {
		t.Fatalf("decision = %#v, want enqueue after cooldown expiry", decision)
	}
}

func TestDecideRediscoveryAfterNoopResolveDefersLegacyMetadataDuringCooldown(t *testing.T) {
	t.Parallel()

	loopMeta := mustMarshalJSON(map[string]any{"lastNoopResolveHeadSha": "head-1", "lastNoopResolveFixItemsHash": "legacy-hash", "lastNoopResolveAt": "2026-04-11T12:00:00.000Z"})
	decision := decideRediscoveryAfterNoopResolve(storage.LoopRecord{ID: "loop_1", MetadataJSON: &loopMeta, UpdatedAt: "2026-04-11T12:00:00.000Z"}, "head-1", "legacy-hash", "same-hash", []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", ThreadFingerprint: "latest=c1|updated=2026-04-11T12:00:00Z|count=1"}}, []string{"t1"}, time.Date(2026, time.April, 11, 12, 0, 30, 0, time.UTC))
	if decision.Action != rediscoveryActionDefer {
		t.Fatalf("decision = %#v, want defer for legacy metadata during cooldown", decision)
	}
}

func TestDiscoverPullRequestsPreservesPendingRediscoveryForMidRunReviewComment(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(81)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	loopID := "loop_fixer_midrun_comment"
	projectID := "project_1"
	loop := storage.LoopRecord{ID: loopID, Seq: 95, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{ID: "run_fixer_midrun_comment", LoopID: loopID, Status: "running", CurrentStep: stringPtr(string(stepClaimPR)), LastCompletedStep: stringPtr(string(stepDiscoverPR)), StartedAt: nowISO, LastHeartbeatAt: &nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	lockKey := fmt.Sprintf("pr:%s:%d", repo, prNumber)
	if acquired, err := fixture.repos.Locks.Acquire(context.Background(), storage.LockRecord{Key: lockKey, Owner: loopID, ExpiresAt: eventlog.FormatJavaScriptISOString(fixture.now().Add(time.Minute).UTC()), CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Locks.Acquire() error = %v", err)
	} else if !acquired {
		t.Fatal("Locks.Acquire() = false, want active fixer PR lock")
	}
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_fixer_midrun_comment", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:midrun-comment:active", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, LockKey: &lockKey, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-81", HeadRefName: "feature/fix-81", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}, {"id": "c2", "threadId": "t2", "body": "also fix this"}}}
	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: detail.HeadSHA}}, viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: projectID, Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want no parallel queue item while active run continues", result.QueueItems)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	pending, ok := parsePendingFixerRediscoveryState(parseJSONObject(persisted.MetadataJSON))
	if !ok {
		t.Fatalf("parsePendingFixerRediscoveryState() = false, want pending rediscovery metadata")
	}
	if pending.HeadSHA != detail.HeadSHA || pending.FixItemsStateHash == "" || !sameStringSlices(pending.UnresolvedThreadIDs, []string{"t1", "t2"}) {
		t.Fatalf("pending = %#v, want persisted pending rediscovery state", pending)
	}
	activeQueue, err := fixture.repos.Queue.FindActiveByLoopID(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Queue.FindActiveByLoopID() error = %v", err)
	}
	if activeQueue == nil || activeQueue.ID != "queue_fixer_midrun_comment" || activeQueue.Status != "running" {
		t.Fatalf("activeQueue = %#v, want original running queue item only", activeQueue)
	}
	if err := fixture.repos.Queue.Complete(context.Background(), activeQueue.ID, fixture.nowISO()); err != nil {
		t.Fatalf("Queue.Complete() error = %v", err)
	}
	scheduled, err := runner.schedulePendingRediscoveryAfterRun(context.Background(), *persisted, repo, prNumber)
	if err != nil {
		t.Fatalf("schedulePendingRediscoveryAfterRun() error = %v", err)
	}
	if !scheduled {
		t.Fatal("schedulePendingRediscoveryAfterRun() = false, want queued follow-up rediscovery")
	}
	followupQueue, err := fixture.repos.Queue.FindActiveByLoopID(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Queue.FindActiveByLoopID() after schedule error = %v", err)
	}
	if followupQueue == nil || followupQueue.ID == activeQueue.ID || followupQueue.Status != "queued" {
		t.Fatalf("followupQueue = %#v, want new queued follow-up item", followupQueue)
	}
	persisted, err = fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() after schedule error = %v", err)
	}
	if _, ok := parsePendingFixerRediscoveryState(parseJSONObject(persisted.MetadataJSON)); ok {
		t.Fatalf("pending rediscovery still present in %#v", parseJSONObject(persisted.MetadataJSON))
	}
}

func TestSchedulePendingRediscoveryAfterRunPreservesPausedHardHold(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(84)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	projectID := "project_1"
	metadata := mustMarshalJSON(map[string]any{"pendingFixerRediscovery": map[string]any{"headSha": "head-84", "fixItemsStateHash": "state-84", "unresolvedThreadIds": []string{"t1"}, "recordedAt": nowISO}})
	loop := storage.LoopRecord{ID: "loop_paused_pending_rediscovery", Seq: 98, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "paused", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})

	scheduled, err := runner.schedulePendingRediscoveryAfterRun(context.Background(), loop, repo, prNumber)
	if err != nil {
		t.Fatalf("schedulePendingRediscoveryAfterRun() error = %v", err)
	}
	if scheduled {
		t.Fatal("schedulePendingRediscoveryAfterRun() = true, want paused hard hold to stay pending")
	}
	activeQueue, err := fixture.repos.Queue.FindActiveByLoopID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Queue.FindActiveByLoopID() error = %v", err)
	}
	if activeQueue != nil {
		t.Fatalf("activeQueue = %#v, want no queued rediscovery while paused", activeQueue)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.Status != "paused" || persisted.NextRunAt != nil {
		t.Fatalf("loop = %#v, want unchanged paused loop", persisted)
	}
	pending, ok := parsePendingFixerRediscoveryState(parseJSONObject(persisted.MetadataJSON))
	if !ok {
		t.Fatalf("parsePendingFixerRediscoveryState() = false, want pending rediscovery metadata")
	}
	if pending.HeadSHA != "head-84" || pending.FixItemsStateHash != "state-84" || !sameStringSlices(pending.UnresolvedThreadIDs, []string{"t1"}) || pending.RecordedAt != nowISO {
		t.Fatalf("pending = %#v, want unchanged pending rediscovery metadata", pending)
	}
}

func TestDiscoverPullRequestsPreservesPendingRediscoveryForMidRunCIFailure(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(82)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	loopID := "loop_fixer_midrun_ci"
	projectID := "project_1"
	loop := storage.LoopRecord{ID: loopID, Seq: 96, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{ID: "run_fixer_midrun_ci", LoopID: loopID, Status: "running", CurrentStep: stringPtr(string(stepRepair)), LastCompletedStep: stringPtr(string(stepCollectFixes)), StartedAt: nowISO, LastHeartbeatAt: &nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	lockKey := fmt.Sprintf("pr:%s:%d", repo, prNumber)
	if acquired, err := fixture.repos.Locks.Acquire(context.Background(), storage.LockRecord{Key: lockKey, Owner: loopID, ExpiresAt: eventlog.FormatJavaScriptISOString(fixture.now().Add(time.Minute).UTC()), CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Locks.Acquire() error = %v", err)
	} else if !acquired {
		t.Fatal("Locks.Acquire() = false, want active fixer PR lock")
	}
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_fixer_midrun_ci", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:midrun-ci:active", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, LockKey: &lockKey, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-82", HeadRefName: "feature/fix-82", BaseRefName: "main", BaseSHA: "base-1", Checks: []map[string]any{{"name": "ci", "conclusion": "FAILURE"}}}
	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: detail.HeadSHA}}, viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: projectID, Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want no parallel queue item while active CI run continues", result.QueueItems)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	pending, ok := parsePendingFixerRediscoveryState(parseJSONObject(persisted.MetadataJSON))
	if !ok {
		t.Fatalf("parsePendingFixerRediscoveryState() = false, want pending rediscovery metadata")
	}
	if pending.HeadSHA != detail.HeadSHA || pending.FixItemsStateHash == "" || len(pending.UnresolvedThreadIDs) != 0 {
		t.Fatalf("pending = %#v, want pending CI rediscovery without thread IDs", pending)
	}
	fixture.advance(time.Minute)
	result, err = runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: projectID, Repo: repo})
	if err != nil {
		t.Fatalf("second DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("second QueueItems = %#v, want no new queue item for duplicate CI signal", result.QueueItems)
	}
	persisted, err = fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() after duplicate error = %v", err)
	}
	pending, ok = parsePendingFixerRediscoveryState(parseJSONObject(persisted.MetadataJSON))
	if !ok {
		t.Fatalf("parsePendingFixerRediscoveryState() after duplicate = false, want pending rediscovery metadata")
	}
	if pending.RecordedAt != nowISO {
		t.Fatalf("pending.RecordedAt = %q, want unchanged %q for duplicate CI signal", pending.RecordedAt, nowISO)
	}
}

func TestDiscoverPullRequestsLeavesPendingRediscoveryUnchangedForDuplicateSignal(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(83)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	loopID := "loop_fixer_midrun_duplicate"
	projectID := "project_1"
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-83", HeadRefName: "feature/fix-83", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}, {"id": "c2", "threadId": "t2", "body": "also fix this"}}}
	pendingMeta := mustMarshalJSON(map[string]any{"pendingFixerRediscovery": map[string]any{"headSha": detail.HeadSHA, "fixItemsStateHash": hashFixItemsState(collectFixItems(detail)), "unresolvedThreadIds": []string{"t1", "t2"}, "recordedAt": nowISO}})
	loop := storage.LoopRecord{ID: loopID, Seq: 97, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &pendingMeta, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{ID: "run_fixer_midrun_duplicate", LoopID: loopID, Status: "running", CurrentStep: stringPtr(string(stepValidate)), LastCompletedStep: stringPtr(string(stepCollectFixes)), StartedAt: nowISO, LastHeartbeatAt: &nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	lockKey := fmt.Sprintf("pr:%s:%d", repo, prNumber)
	if acquired, err := fixture.repos.Locks.Acquire(context.Background(), storage.LockRecord{Key: lockKey, Owner: loopID, ExpiresAt: eventlog.FormatJavaScriptISOString(fixture.now().Add(time.Minute).UTC()), CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Locks.Acquire() error = %v", err)
	} else if !acquired {
		t.Fatal("Locks.Acquire() = false, want active fixer PR lock")
	}
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_fixer_midrun_duplicate", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:midrun-duplicate:active", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, LockKey: &lockKey, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: detail.HeadSHA}}, viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: projectID, Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want no new queue item for duplicate signal", result.QueueItems)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	pending, ok := parsePendingFixerRediscoveryState(parseJSONObject(persisted.MetadataJSON))
	if !ok {
		t.Fatalf("parsePendingFixerRediscoveryState() = false, want pending rediscovery metadata")
	}
	if pending.RecordedAt != nowISO {
		t.Fatalf("pending.RecordedAt = %q, want unchanged %q for duplicate signal", pending.RecordedAt, nowISO)
	}
	queueItems, err := fixture.repos.Queue.List(context.Background())
	if err != nil {
		t.Fatalf("Queue.List() error = %v", err)
	}
	if len(queueItems) != 1 {
		t.Fatalf("queue item count = %d, want original active item only", len(queueItems))
	}
}

func TestDiscoverPullRequestsPreservesPendingRediscoveryForRunningAutomaticLoopWithManualFollowup(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(85)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	projectID := "project_1"
	automaticLoopID := "loop_fixer_midrun_automatic"
	automaticLoop := storage.LoopRecord{ID: automaticLoopID, Seq: 99, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), automaticLoop); err != nil {
		t.Fatalf("Loops.Upsert() automatic error = %v", err)
	}
	manualMetadata := `{"manual":true,"followUpdates":true}`
	manualLoop := storage.LoopRecord{ID: "loop_manual_followup_completed", Seq: 100, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &manualMetadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), manualLoop); err != nil {
		t.Fatalf("Loops.Upsert() manual error = %v", err)
	}
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{ID: "run_fixer_midrun_automatic", LoopID: automaticLoopID, Status: "running", CurrentStep: stringPtr(string(stepRepair)), LastCompletedStep: stringPtr(string(stepCollectFixes)), StartedAt: nowISO, LastHeartbeatAt: &nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	lockKey := fmt.Sprintf("pr:%s:%d", repo, prNumber)
	if acquired, err := fixture.repos.Locks.Acquire(context.Background(), storage.LockRecord{Key: lockKey, Owner: automaticLoopID, ExpiresAt: eventlog.FormatJavaScriptISOString(fixture.now().Add(time.Minute).UTC()), CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Locks.Acquire() error = %v", err)
	} else if !acquired {
		t.Fatal("Locks.Acquire() = false, want active fixer PR lock")
	}
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_fixer_midrun_automatic", ProjectID: &projectID, LoopID: &automaticLoopID, Type: "fixer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:midrun-automatic:active", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, LockKey: &lockKey, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-85", HeadRefName: "feature/fix-85", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}
	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: detail.HeadSHA}}, viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: projectID, Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want no parallel queue item while automatic run continues", result.QueueItems)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), automaticLoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	pending, ok := parsePendingFixerRediscoveryState(parseJSONObject(persisted.MetadataJSON))
	if !ok {
		t.Fatalf("parsePendingFixerRediscoveryState() = false, want pending rediscovery metadata on automatic loop")
	}
	if pending.HeadSHA != detail.HeadSHA || pending.FixItemsStateHash == "" || !sameStringSlices(pending.UnresolvedThreadIDs, []string{"t1"}) {
		t.Fatalf("pending = %#v, want persisted pending rediscovery state on automatic loop", pending)
	}
	manualPersisted, err := fixture.repos.Loops.GetByID(context.Background(), manualLoop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() manual error = %v", err)
	}
	if _, ok := parsePendingFixerRediscoveryState(parseJSONObject(manualPersisted.MetadataJSON)); ok {
		t.Fatalf("manual pending rediscovery present in %#v, want automatic loop to own rediscovery state", parseJSONObject(manualPersisted.MetadataJSON))
	}
}

func TestDiscoverPullRequestsSkipsAutoQueueWhileManualSingleShotRunHoldsLock(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(86)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	projectID := "project_1"
	metadata := `{"manual":true,"followUpdates":false}`
	manualLoopID := "loop_manual_single_shot_running"
	manualLoop := storage.LoopRecord{ID: manualLoopID, Seq: 101, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), manualLoop); err != nil {
		t.Fatalf("Loops.Upsert() manual error = %v", err)
	}
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{ID: "run_manual_single_shot_running", LoopID: manualLoopID, Status: "running", CurrentStep: stringPtr(string(stepRepair)), LastCompletedStep: stringPtr(string(stepCollectFixes)), StartedAt: nowISO, LastHeartbeatAt: &nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	lockKey := fmt.Sprintf("pr:%s:%d", repo, prNumber)
	if acquired, err := fixture.repos.Locks.Acquire(context.Background(), storage.LockRecord{Key: lockKey, Owner: manualLoopID, ExpiresAt: eventlog.FormatJavaScriptISOString(fixture.now().Add(time.Minute).UTC()), CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Locks.Acquire() error = %v", err)
	} else if !acquired {
		t.Fatal("Locks.Acquire() = false, want active fixer PR lock")
	}
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_manual_single_shot_running", ProjectID: &projectID, LoopID: &manualLoopID, Type: "fixer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:manual-single-shot:active", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, LockKey: &lockKey, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: "head-86"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: projectID, Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 || len(result.CreatedLoopIDs) != 0 || result.Skipped != 1 {
		t.Fatalf("result = %#v, want active manual single-shot run to suppress auto rediscovery", result)
	}
	if github.viewIndex != 0 {
		t.Fatalf("viewIndex = %d, want discovery to stop before opening PR detail", github.viewIndex)
	}
	queueItems, err := fixture.repos.Queue.List(context.Background())
	if err != nil {
		t.Fatalf("Queue.List() error = %v", err)
	}
	if len(queueItems) != 1 || queueItems[0].ID != "queue_manual_single_shot_running" {
		t.Fatalf("queueItems = %#v, want only the active manual queue item", queueItems)
	}
}

func TestDiscoverPullRequestsRecoversLegacyNoopLoopWithoutOpenPRListing(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	legacyAt := eventlog.FormatJavaScriptISOString(fixture.now().Add(-10 * time.Minute))
	comment := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{comment}}
	metadata := mustMarshalJSON(map[string]any{"lastNoopResolveHeadSha": "head-1", "lastNoopResolveStateHash": hashFixItemsState(collectFixItems(detail)), "lastNoopResolveAt": legacyAt})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_legacy_recovery", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{listOpen: nil, viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want recovered legacy queue item", result.QueueItems)
	}
	if github.viewIndex != 1 {
		t.Fatalf("viewIndex = %d, want direct recovery PR view", github.viewIndex)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), "loop_fixer_legacy_recovery")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.Status != "queued" || persisted.NextRunAt == nil || *persisted.NextRunAt != fixture.nowISO() {
		t.Fatalf("persisted = %#v, want immediately queued recovered loop", persisted)
	}
	followup, ok := parseFixerFollowupState(parseJSONObject(persisted.MetadataJSON))
	if !ok {
		t.Fatalf("parseFixerFollowupState() = false, want migrated follow-up metadata")
	}
	if followup.HeadSHA != "head-1" || followup.FixItemsStateHash == "" || !sameStringSlices(followup.UnresolvedThreadIDs, []string{"t1"}) {
		t.Fatalf("followup = %#v, want migrated live recovery state", followup)
	}
	activeQueue, err := fixture.repos.Queue.FindActiveByLoopID(context.Background(), "loop_fixer_legacy_recovery")
	if err != nil {
		t.Fatalf("Queue.FindActiveByLoopID() error = %v", err)
	}
	if activeQueue == nil || activeQueue.AvailableAt != fixture.nowISO() {
		t.Fatalf("activeQueue = %#v, want immediately runnable recovered queue item", activeQueue)
	}
}

func TestDiscoverPullRequestsRecoversLegacyNoopLoopAfterCooldownWhenPRListingMissesIt(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	legacyAt := fixture.nowISO()
	comment := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{comment}}
	metadata := mustMarshalJSON(map[string]any{"lastNoopResolveHeadSha": "head-1", "lastNoopResolveStateHash": hashFixItemsState(collectFixItems(detail)), "lastNoopResolveAt": legacyAt})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_legacy_cooldown", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want one delayed recovered queue item", result.QueueItems)
	}
	wantAvailableAt := eventlog.FormatJavaScriptISOString(fixture.now().Add(time.Minute))
	if result.QueueItems[0].AvailableAt != wantAvailableAt {
		t.Fatalf("AvailableAt = %q, want %q", result.QueueItems[0].AvailableAt, wantAvailableAt)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), "loop_fixer_legacy_cooldown")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.NextRunAt == nil || *persisted.NextRunAt != wantAvailableAt {
		t.Fatalf("persisted = %#v, want delayed NextRunAt %q", persisted, wantAvailableAt)
	}
}

func TestDiscoverPullRequestsRecoversManualForeignLegacyNoopLoop(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	legacyAt := fixture.nowISO()
	comment := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{comment}}
	metadata := mustMarshalJSON(map[string]any{"manual": true, "followUpdates": true, "lastNoopResolveHeadSha": "head-1", "lastNoopResolveStateHash": hashFixItemsState(collectFixItems(detail)), "lastNoopResolveAt": legacyAt})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_manual_legacy", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{currentUser: "looper-bot", viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want recovered manual foreign queue item", result.QueueItems)
	}
	if result.QueueItems[0].TargetID != loopTarget {
		t.Fatalf("queue item = %#v, want recovered manual foreign PR target", result.QueueItems[0])
	}
}

func TestDiscoverPullRequestsRecoversAutomaticLegacyNoopLoopAfterManualSingleShotHistory(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	manualMetadata := `{"manual":true,"followUpdates":false}`
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_manual_single_shot_history", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &manualMetadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() manual error = %v", err)
	}
	legacyAt := eventlog.FormatJavaScriptISOString(fixture.now().Add(-10 * time.Minute))
	comment := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{comment}}
	automaticMetadata := mustMarshalJSON(map[string]any{"lastNoopResolveHeadSha": "head-1", "lastNoopResolveStateHash": hashFixItemsState(collectFixItems(detail)), "lastNoopResolveAt": legacyAt})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_legacy_after_manual", Seq: 2, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &automaticMetadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() automatic error = %v", err)
	}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want recovered automatic legacy queue item", result.QueueItems)
	}
	if result.QueueItems[0].LoopID == nil || *result.QueueItems[0].LoopID != "loop_fixer_legacy_after_manual" {
		t.Fatalf("queue item = %#v, want automatic legacy loop to be recovered", result.QueueItems[0])
	}
}

func TestDiscoverPullRequestSkipsManualLoopWhenFollowUpdatesDisabled(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	metadata := `{"manual":true,"followUpdates":false}`
	loop := storage.LoopRecord{ID: "loop_manual_single_shot", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	comment := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	github := &fakeGitHubGateway{currentUser: "looper-bot", viewResponses: []PullRequestDetail{{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{comment}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("DiscoverPullRequest() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want no follow-up queue item for disabled manual fixer loop", result.QueueItems)
	}
}

func TestDiscoverPullRequestAllowsManualLoopWhenFollowUpdatesEnabled(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	metadata := `{"manual":true,"followUpdates":true}`
	loop := storage.LoopRecord{ID: "loop_manual_followup", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	comment := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	github := &fakeGitHubGateway{currentUser: "looper-bot", viewResponses: []PullRequestDetail{{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{comment}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("DiscoverPullRequest() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want one manual follow-up queue item", result.QueueItems)
	}
	if result.QueueItems[0].LoopID == nil || *result.QueueItems[0].LoopID != loop.ID {
		t.Fatalf("queue item = %#v, want manual follow-up loop %q", result.QueueItems[0], loop.ID)
	}
}

func TestDiscoverPullRequestAllowsManualFollowUpWhenFixerHoldAppliedLive(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	metadata := `{"manual":true,"followUpdates":true}`
	loop := storage.LoopRecord{ID: "loop_manual_followup_hold", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	comment := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	github := &fakeGitHubGateway{currentUser: "looper-bot", viewResponses: []PullRequestDetail{{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Labels: []string{domain.HoldLabelFixer}, Comments: []map[string]any{comment}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequest(context.Background(), TargetedDiscoveryInput{ProjectID: "project_1", Repo: repo, PRNumber: prNumber})
	if err != nil {
		t.Fatalf("DiscoverPullRequest() error = %v", err)
	}
	if len(result.QueueItems) != 1 || result.QueueItems[0].LoopID == nil || *result.QueueItems[0].LoopID != loop.ID {
		t.Fatalf("result = %#v, want held manual follow-up queued", result)
	}
}

func TestDiscoverPullRequestsDoesNotRecoverLegacyNoopLoopWhenFixerHoldAppliedLive(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	legacyAt := eventlog.FormatJavaScriptISOString(fixture.now().Add(-10 * time.Minute))
	comment := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Labels: []string{domain.HoldLabelFixer}, Comments: []map[string]any{comment}}
	metadata := mustMarshalJSON(map[string]any{"lastNoopResolveHeadSha": "head-1", "lastNoopResolveStateHash": hashFixItemsState(collectFixItems(detail)), "lastNoopResolveAt": legacyAt})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_legacy_hold", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want no recovery while held", result.QueueItems)
	}
	activeQueue, err := fixture.repos.Queue.FindActiveByLoopID(context.Background(), "loop_fixer_legacy_hold")
	if err != nil {
		t.Fatalf("Queue.FindActiveByLoopID() error = %v", err)
	}
	if activeQueue != nil {
		t.Fatalf("activeQueue = %#v, want nil", activeQueue)
	}
}

func TestDiscoverPullRequestsRecoversManualLegacyNoopLoopWhenFixerHoldAppliedLive(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	legacyAt := eventlog.FormatJavaScriptISOString(fixture.now().Add(-10 * time.Minute))
	comment := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Labels: []string{domain.HoldLabelFixer}, Comments: []map[string]any{comment}}
	metadata := mustMarshalJSON(map[string]any{"manual": true, "followUpdates": true, "lastNoopResolveHeadSha": "head-1", "lastNoopResolveStateHash": hashFixItemsState(collectFixItems(detail)), "lastNoopResolveAt": legacyAt})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_manual_legacy_hold", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want recovered manual legacy queue item while held", result.QueueItems)
	}
	if result.QueueItems[0].LoopID == nil || *result.QueueItems[0].LoopID != "loop_fixer_manual_legacy_hold" {
		t.Fatalf("queue item = %#v, want manual legacy loop recovery", result.QueueItems[0])
	}
}

func TestDiscoverPullRequestsSkipsClosedManualFollowUpCandidate(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	metadata := `{"manual":true,"followUpdates":true}`
	loop := storage.LoopRecord{ID: "loop_manual_followup_closed", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{currentUser: "looper-bot", viewResponses: []PullRequestDetail{{Number: prNumber, State: "CLOSED", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want no queue items for closed manual follow-up candidate", result.QueueItems)
	}
}

func TestDiscoverPullRequestsSkipsStoppedManualFollowUpCandidate(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	metadata := `{"manual":true,"followUpdates":true}`
	loop := storage.LoopRecord{ID: "loop_manual_followup_stopped", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "stopped", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	comment := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	github := &fakeGitHubGateway{currentUser: "looper-bot", viewResponses: []PullRequestDetail{{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{comment}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want no queue items for stopped manual follow-up loop", result.QueueItems)
	}
}

func TestDiscoverPullRequestsForBaseBranchUpdateSkipsManualFollowUpOnDifferentBase(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	metadata := `{"manual":true,"followUpdates":true}`
	loop := storage.LoopRecord{ID: "loop_manual_followup_other_base", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	comment := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	github := &fakeGitHubGateway{currentUser: "looper-bot", viewResponses: []PullRequestDetail{{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "release", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{comment}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequestsForBaseBranchUpdate(context.Background(), BaseBranchDiscoveryInput{ProjectID: "project_1", Repo: repo, BaseRefName: "main"})
	if err != nil {
		t.Fatalf("DiscoverPullRequestsForBaseBranchUpdate() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want no queue items for manual follow-up loop on another base branch", result.QueueItems)
	}
}

func TestSchedulePendingRediscoveryAfterRunSkipsDisabledManualLoop(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(84)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	projectID := "project_1"
	metadata := mustMarshalJSON(map[string]any{"manual": true, "followUpdates": false, "pendingFixerRediscovery": map[string]any{"headSha": "head-84", "fixItemsStateHash": "state-84", "unresolvedThreadIds": []string{"t1"}, "recordedAt": nowISO}})
	loop := storage.LoopRecord{ID: "loop_manual_pending_disabled", Seq: 98, ProjectID: projectID, Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})

	scheduled, err := runner.schedulePendingRediscoveryAfterRun(context.Background(), loop, repo, prNumber)
	if err != nil {
		t.Fatalf("schedulePendingRediscoveryAfterRun() error = %v", err)
	}
	if scheduled {
		t.Fatal("schedulePendingRediscoveryAfterRun() = true, want disabled manual loop to stay single-shot")
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if _, ok := parsePendingFixerRediscoveryState(parseJSONObject(persisted.MetadataJSON)); ok {
		t.Fatalf("pending rediscovery still present in %#v", parseJSONObject(persisted.MetadataJSON))
	}
}

func TestDiscoverPullRequestsRecoversManualLegacyNoopLoopDespiteLabelMismatch(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	legacyAt := fixture.nowISO()
	comment := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{comment}}
	metadata := mustMarshalJSON(map[string]any{"manual": true, "followUpdates": true, "lastNoopResolveHeadSha": "head-1", "lastNoopResolveStateHash": hashFixItemsState(collectFixItems(detail)), "lastNoopResolveAt": legacyAt})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_manual_legacy_labels", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{currentUser: "looper-bot", viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, AuthorFilter: config.FixerAuthorFilterCurrentUser, Labels: []string{"bug"}, LabelMode: config.LabelModeAll}})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want recovered manual queue item despite label mismatch", result.QueueItems)
	}
}

func TestDiscoverPullRequestsRecoversManualLegacyNoopDraftLoopDespiteDraftPolicy(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	legacyAt := fixture.nowISO()
	comment := map[string]any{"id": "c1", "threadId": "t1", "body": "please fix"}
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", IsDraft: true, HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{comment}}
	metadata := mustMarshalJSON(map[string]any{"manual": true, "followUpdates": true, "lastNoopResolveHeadSha": "head-1", "lastNoopResolveStateHash": hashFixItemsState(collectFixItems(detail)), "lastNoopResolveAt": legacyAt})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_manual_legacy_draft", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{currentUser: "looper-bot", viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, AuthorFilter: config.FixerAuthorFilterCurrentUser}})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want recovered manual queue item despite draft policy", result.QueueItems)
	}
}

func TestDiscoverPullRequestsRecoverySkipsLegacyNoopLoopWhenPRLocked(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}
	metadata := mustMarshalJSON(map[string]any{"lastNoopResolveHeadSha": "head-1", "lastNoopResolveStateHash": hashFixItemsState(collectFixItems(detail)), "lastNoopResolveAt": fixture.nowISO()})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_legacy_locked", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	lockKey := fmt.Sprintf("pr:%s:%d", repo, prNumber)
	if acquired, err := fixture.repos.Locks.Acquire(context.Background(), storage.LockRecord{Key: lockKey, Owner: "other-fixer", ExpiresAt: fixture.now().Add(time.Minute).UTC().Format("2006-01-02T15:04:05.000Z"), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Locks.Acquire() error = %v", err)
	} else if !acquired {
		t.Fatal("Locks.Acquire() = false, want active competing lock")
	}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want none while PR lock is active", result.QueueItems)
	}
	if github.viewIndex != 0 {
		t.Fatalf("viewIndex = %d, want no recovery PR view while locked", github.viewIndex)
	}
}

func TestDiscoverPullRequestsRecoveryClearsLegacyNoopMetadataWhenFingerprintChanged(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	metadata := mustMarshalJSON(map[string]any{"lastNoopResolveHeadSha": "old-head", "lastNoopResolveStateHash": "old-hash", "lastNoopResolveAt": fixture.nowISO()})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_legacy_changed", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want immediate recovery queue item", result.QueueItems)
	}
	if result.QueueItems[0].AvailableAt != fixture.nowISO() {
		t.Fatalf("AvailableAt = %q, want immediate requeue at %q", result.QueueItems[0].AvailableAt, fixture.nowISO())
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), "loop_fixer_legacy_changed")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	meta := parseJSONObject(persisted.MetadataJSON)
	if _, ok := meta["fixerFollowup"]; ok {
		t.Fatalf("fixerFollowup = %#v, want cleared migrated metadata for changed fingerprint", meta["fixerFollowup"])
	}
	if _, ok := meta["lastNoopResolveHeadSha"]; ok {
		t.Fatalf("legacy noop metadata still present in %#v", meta)
	}
}

func TestDecideRediscoveryAfterNoopResolveEnqueuesWhenTerminalStateGetsNewSignal(t *testing.T) {
	t.Parallel()

	loopMeta := mustMarshalJSON(map[string]any{"fixerFollowup": map[string]any{"reason": "manual_intervention", "headSha": "head-1", "fixItemsStateHash": "same-hash", "unresolvedThreadIds": []string{"t1"}, "attemptsForFingerprint": 6, "lastAttemptAt": "2026-04-11T12:00:00.000Z", "terminal": true}})
	decision := decideRediscoveryAfterNoopResolve(storage.LoopRecord{ID: "loop_1", MetadataJSON: &loopMeta, UpdatedAt: "2026-04-11T12:00:00.000Z"}, "head-2", "same-hash", "same-hash", []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", ThreadFingerprint: "latest=c1|updated=2026-04-11T12:00:00Z|count=1"}}, []string{"t1"}, time.Date(2026, time.April, 11, 12, 1, 0, 0, time.UTC))
	if decision.Action != rediscoveryActionEnqueue {
		t.Fatalf("decision = %#v, want enqueue on new signal despite terminal prior state", decision)
	}
}

func TestDecideRediscoveryAfterNoopResolveIgnoresMismatchedThreadFingerprintEvidence(t *testing.T) {
	t.Parallel()

	loopMeta := mustMarshalJSON(map[string]any{"fixerFollowup": map[string]any{"reason": "missing_evidence", "headSha": "head-1", "fixItemsStateHash": "same-hash", "unresolvedThreadIds": []string{"t1"}, "attemptsForFingerprint": 1, "lastAttemptAt": "2026-04-11T12:00:00.000Z", "nextEligibleAt": "2026-04-11T12:05:00.000Z"}, "fixEvidenceStoreV2": map[string]any{"version": 2, "threads": map[string]any{"t1": []map[string]any{{"threadId": "t1", "threadFingerprint": "latest=old-comment|updated=2026-04-11T11:59:00Z|count=1", "evidenceHeadSHA": "head-1", "validationHeadSHA": "head-1", "commitSHA": "head-1", "producedNewCommits": true, "resolveState": "pending"}}}}})
	decision := decideRediscoveryAfterNoopResolve(storage.LoopRecord{ID: "loop_1", MetadataJSON: &loopMeta, UpdatedAt: "2026-04-11T12:00:00.000Z"}, "head-1", "same-hash", "same-hash", []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", ThreadFingerprint: "latest=c1|updated=2026-04-11T12:00:00Z|count=1"}}, []string{"t1"}, time.Date(2026, time.April, 11, 12, 1, 0, 0, time.UTC))
	if decision.Action != rediscoveryActionDefer {
		t.Fatalf("decision = %#v, want defer when only mismatched thread evidence exists", decision)
	}
	if decision.NextEligibleAt != "2026-04-11T12:05:00.000Z" {
		t.Fatalf("NextEligibleAt = %q, want followup cooldown", decision.NextEligibleAt)
	}
}

func TestDecideRediscoveryAfterNoopResolveDefersMissingConfirmationDuringCooldown(t *testing.T) {
	t.Parallel()

	loopMeta := mustMarshalJSON(map[string]any{"fixerFollowup": map[string]any{"reason": "missing_confirmation", "headSha": "head-1", "fixItemsStateHash": "same-hash", "unresolvedThreadIds": []string{"t1"}, "attemptsForFingerprint": 1, "lastAttemptAt": "2026-04-11T12:00:00.000Z", "nextEligibleAt": "2026-04-11T12:05:00.000Z"}, "fixEvidenceStoreV2": map[string]any{"version": 2, "threads": map[string]any{"t1": []map[string]any{{"threadId": "t1", "threadFingerprint": "latest=c1|updated=2026-04-11T12:00:00Z|count=1", "evidenceHeadSHA": "head-1", "validationHeadSHA": "head-1", "commitSHA": "head-1", "producedNewCommits": true, "resolveState": "skipped_no_confirmation"}}}}})
	decision := decideRediscoveryAfterNoopResolve(storage.LoopRecord{ID: "loop_1", MetadataJSON: &loopMeta, UpdatedAt: "2026-04-11T12:00:00.000Z"}, "head-1", "same-hash", "same-hash", []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", ThreadFingerprint: "latest=c1|updated=2026-04-11T12:00:00Z|count=1"}}, []string{"t1"}, time.Date(2026, time.April, 11, 12, 1, 0, 0, time.UTC))
	if decision.Action != rediscoveryActionDefer {
		t.Fatalf("decision = %#v, want defer for missing confirmation during cooldown", decision)
	}
	if decision.Reason != string(fixerFollowupReasonMissingConfirmation) {
		t.Fatalf("decision.Reason = %q, want %q", decision.Reason, fixerFollowupReasonMissingConfirmation)
	}
}

func TestDiscoverPullRequestsClearsFollowupStateWhenNoUnresolvedCommentsRemain(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	metadata := mustMarshalJSON(map[string]any{"fixerFollowup": map[string]any{"reason": "missing_evidence", "headSha": "head-1", "fixItemsStateHash": "same-hash", "unresolvedThreadIds": []string{"t1"}, "attemptsForFingerprint": 1, "lastAttemptAt": nowISO, "nextEligibleAt": eventlog.FormatJavaScriptISOString(fixture.now().Add(time.Minute))}, "lastNoopResolveHeadSha": "head-1", "lastNoopResolveStateHash": "same-hash", "lastNoopResolveAt": nowISO})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_noop", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "completed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	projectID := "project_1"
	loopID := "loop_fixer_noop"
	lockKey := fmt.Sprintf("pr:%s:%d", repo, prNumber)
	delayedAt := eventlog.FormatJavaScriptISOString(fixture.now().Add(time.Minute))
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_followup", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:followup", Priority: storage.QueuePriorityFixer, Status: "queued", AvailableAt: delayedAt, Attempts: 0, MaxAttempts: 3, LockKey: &lockKey, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	resolvedDetail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "done", "isResolved": true}}}
	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: resolvedDetail.HeadSHA}}, viewResponses: []PullRequestDetail{resolvedDetail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 0 {
		t.Fatalf("QueueItems = %#v, want none when comments are resolved", result.QueueItems)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), "loop_fixer_noop")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	meta := parseJSONObject(persisted.MetadataJSON)
	if _, ok := meta["fixerFollowup"]; ok {
		t.Fatalf("fixerFollowup still present in %#v", meta)
	}
	if _, ok := meta["lastNoopResolveHeadSha"]; ok {
		t.Fatalf("legacy noop metadata still present in %#v", meta)
	}
	activeQueue, err := fixture.repos.Queue.FindActiveByLoopID(context.Background(), "loop_fixer_noop")
	if err != nil {
		t.Fatalf("Queue.FindActiveByLoopID() error = %v", err)
	}
	if activeQueue != nil {
		t.Fatalf("activeQueue = %#v, want cleared delayed queue item", activeQueue)
	}
}

func TestDiscoverPullRequestsKeepsQueuedAttemptsForNonCommentFixItemsWithoutThreads(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	metadata := mustMarshalJSON(map[string]any{"fixerFollowup": map[string]any{"reason": "missing_evidence", "headSha": "head-1", "fixItemsStateHash": "same-hash", "unresolvedThreadIds": []string{"t1"}, "attemptsForFingerprint": 2, "lastAttemptAt": nowISO, "nextEligibleAt": eventlog.FormatJavaScriptISOString(fixture.now().Add(time.Minute))}})
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_checks", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	projectID := "project_1"
	loopID := "loop_fixer_checks"
	lockKey := fmt.Sprintf("pr:%s:%d", repo, prNumber)
	delayedAt := eventlog.FormatJavaScriptISOString(fixture.now().Add(time.Minute))
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_followup", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:followup", Priority: storage.QueuePriorityFixer, Status: "queued", AvailableAt: delayedAt, Attempts: 2, MaxAttempts: 3, LockKey: &lockKey, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	detail := PullRequestDetail{Number: prNumber, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Checks: []map[string]any{{"name": "ci", "conclusion": "FAILURE"}}}
	github := &fakeGitHubGateway{listOpen: []PullRequestSummary{{Number: prNumber, State: "OPEN", HeadSHA: detail.HeadSHA}}, viewResponses: []PullRequestDetail{detail}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: repo})
	if err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	if len(result.QueueItems) != 1 {
		t.Fatalf("QueueItems = %#v, want reused queued item for non-comment fix item", result.QueueItems)
	}
	if result.QueueItems[0].ID != "queue_followup" {
		t.Fatalf("queue item ID = %q, want existing queued fixer item", result.QueueItems[0].ID)
	}
	if result.QueueItems[0].Attempts != 2 {
		t.Fatalf("Attempts = %d, want preserved retry state", result.QueueItems[0].Attempts)
	}
	activeQueue, err := fixture.repos.Queue.FindActiveByLoopID(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Queue.FindActiveByLoopID() error = %v", err)
	}
	if activeQueue == nil || activeQueue.ID != "queue_followup" || activeQueue.Attempts != 2 {
		t.Fatalf("activeQueue = %#v, want preserved queued fixer item with attempts", activeQueue)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	meta := parseJSONObject(persisted.MetadataJSON)
	if _, ok := meta["fixerFollowup"]; ok {
		t.Fatalf("fixerFollowup still present in %#v", meta)
	}
}

func TestRecordFixerFollowupStateTransitionsToManualInterventionAfterMaxAttempts(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	loop := storage.LoopRecord{ID: "loop_fixer_followup", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "completed", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})
	for range fixerFollowupBackoffSchedule {
		fixture.advance(time.Minute)
		if _, err := runner.recordFixerFollowupState(context.Background(), loop, fixerFollowupReasonMissingEvidence, "head-1", "same-hash", []string{"t1"}, fixture.now()); err != nil {
			t.Fatalf("recordFixerFollowupState() error = %v", err)
		}
	}
	fixture.advance(time.Minute)
	if _, err := runner.recordFixerFollowupState(context.Background(), loop, fixerFollowupReasonMissingEvidence, "head-1", "same-hash", []string{"t1"}, fixture.now()); err != nil {
		t.Fatalf("recordFixerFollowupState() terminal error = %v", err)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	followup, ok := parseFixerFollowupState(parseJSONObject(persisted.MetadataJSON))
	if !ok {
		t.Fatalf("parseFixerFollowupState() = false, want follow-up metadata")
	}
	if !followup.Terminal || followup.Reason != string(fixerFollowupReasonManualIntervention) {
		t.Fatalf("followup = %#v, want terminal manual intervention", followup)
	}
}

func TestRunResolveCommentsStepSkipsWithoutVerifiedPushEvidence(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	loopMetadata := `{}`
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID("acme/looper", 42)
	loop := storage.LoopRecord{ID: "loop_1", ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "running", MetadataJSON: &loopMetadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "base-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
		}},
	}}}
	runner := New(Options{Repos: fixture.repos, GitHub: github, Now: fixture.now})
	checkpoint := fixerCheckpoint{
		FixItems:   []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}},
		Validation: &ValidationResult{Passed: true, Summary: "ok"},
		Push:       &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Lifecycle:  &lifecycle.State{Pushed: true},
		ReconcileCommits: &checkpointReconcileCommits{
			BaseHeadSHA:      "base-head",
			FinalHeadSHA:     "base-head",
			NewCommitSHAs:    nil,
			WorkingTreeClean: true,
		},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{
		Project:    storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Loop:       loop,
		Repo:       "acme/looper",
		PRNumber:   42,
		Checkpoint: checkpoint,
	})
	if err == nil || !strings.Contains(err.Error(), "omitted or invalidated thread decisions") {
		t.Fatalf("runResolveCommentsStep() error = %v, want contract-violation retry", err)
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %d, want 0 without agent reply explanation", len(github.resolveCalls))
	}
	if updated.ResolvedComments == nil || len(updated.ResolvedComments.Items) != 1 || updated.ResolvedComments.Items[0].Status != "skipped_missing_agent_decision" {
		t.Fatalf("resolved comments = %#v, want skipped_missing_agent_decision", updated.ResolvedComments)
	}
	if len(github.replyCalls) != 0 {
		t.Fatalf("reply calls = %#v, want no synthetic decline reply", github.replyCalls)
	}
	if updated.ResumePolicy != loops.ResumePolicyRestartFromDiscover {
		t.Fatalf("updated.ResumePolicy = %q, want restart_from_discover", updated.ResumePolicy)
	}
}

func TestRunResolveCommentsStepResolvesUsingRepairReplyExplanations(t *testing.T) {
	t.Parallel()

	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "new-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}}}}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	checkpoint := fixerCheckpoint{
		FixItems:     fixItems,
		FixItemsHash: hashFixItems(fixItems),
		Validation:   &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "new-head"},
		Push:         &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:       &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}})}}},
		ReconcileCommits: &checkpointReconcileCommits{
			BaseHeadSHA:      "base-head",
			FinalHeadSHA:     "base-head",
			NewCommitSHAs:    nil,
			WorkingTreeClean: true,
		},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{
		Project:    storage.ProjectRecord{RepoPath: t.TempDir()},
		Repo:       "acme/looper",
		PRNumber:   42,
		Checkpoint: checkpoint,
	})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v", err)
	}
	if github.viewIndex != 1 {
		t.Fatalf("github.viewIndex = %d, want one live PR refresh", github.viewIndex)
	}
	if len(github.resolveCalls) != 1 {
		t.Fatalf("resolve calls = %d, want 1", len(github.resolveCalls))
	}
	if updated.Detail == nil || updated.Detail.HeadSHA != "new-head" {
		t.Fatalf("updated.Detail = %#v, want refreshed new-head detail", updated.Detail)
	}
	if len(github.replyCalls) != 1 || !contains(github.replyCalls[0].Body, "base-hea") {
		t.Fatalf("reply calls = %#v, want reply referencing resolved commit head", github.replyCalls)
	}
	if updated.ResumePolicy != "advance_from_checkpoint" {
		t.Fatalf("updated.ResumePolicy = %q, want advance_from_checkpoint", updated.ResumePolicy)
	}
}

func TestRunResolveCommentsStepSkipsIdentityLookupWithoutCommentItems(t *testing.T) {
	t.Parallel()
	github := &fakeGitHubGateway{
		currentUserErr: errors.New("identity unavailable"),
		viewResponses: []PullRequestDetail{{
			Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1",
			Checks: []map[string]any{{"name": "ci", "conclusion": "FAILURE"}},
		}},
	}
	runner := New(Options{GitHub: github})
	checkpoint := fixerCheckpoint{
		FixItems:   []FixItem{{Type: "check", Name: "ci", Summary: "FAILURE"}},
		Validation: &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "new-head"},
		Push:       &checkpointPush{Pushed: true, Branch: "feature/fix-42", Remote: "origin", HeadSHA: "new-head"},
		Repair:     &checkpointRepair{Status: "completed"},
		ReconcileCommits: &checkpointReconcileCommits{
			BaseHeadSHA: "base-head", FinalHeadSHA: "new-head", NewCommitSHAs: []string{"new-head"}, WorkingTreeClean: true,
		},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{
		Project:    storage.ProjectRecord{RepoPath: t.TempDir()},
		Repo:       "acme/looper",
		PRNumber:   42,
		Checkpoint: checkpoint,
	})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v, want skip identity lookup when commentItems is empty", err)
	}
	if updated.ResumePolicy != "advance_from_checkpoint" {
		t.Fatalf("updated.ResumePolicy = %q, want advance_from_checkpoint", updated.ResumePolicy)
	}
}

func TestRunResolveCommentsStepPostsDeclinedReplyAndLeavesThreadOpen(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	loop := storage.LoopRecord{ID: "loop_decline_leave_open", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "new-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
			"author":   "alice",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Author: "alice", Summary: "please fix", ThreadFingerprint: "thread-hash-1"}}
	checkpoint := fixerCheckpoint{
		FixItems:         fixItems,
		FixItemsHash:     hashFixItems(fixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "new-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionDeclined), Explanation: "Out of scope for this PR."}}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: checkpoint})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v", err)
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %#v, want none (decline leaves thread open)", github.resolveCalls)
	}
	if len(github.replyCalls) != 1 || !strings.Contains(github.replyCalls[0].Body, "Out of scope for this PR.") {
		t.Fatalf("reply calls = %#v, want declined explanation reply", github.replyCalls)
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != declinePendingAdjudicationStatus {
		t.Fatalf("resolved comments = %#v, want %s", updated.ResolvedComments, declinePendingAdjudicationStatus)
	}
	if !hasProgressed(updated) {
		t.Fatalf("hasProgressed() = false, want decline reply to count as progress")
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if records := parseDeclinedThreadRecords(parseJSONObject(persisted.MetadataJSON)); len(records) != 0 {
		t.Fatalf("declined thread records = %#v, want none (no terminal same-head suppression)", records)
	}
	if got := suppressDeclinedFixItems(persisted.MetadataJSON, "new-head", fixItems); len(got) != 1 || got[0].ID != "c1" {
		t.Fatalf("suppressDeclinedFixItems() = %#v, want original fix item still eligible", got)
	}
}

func TestRunResolveCommentsStepDeclineCrashWindowDoesNotRepost(t *testing.T) {
	t.Parallel()
	// Crash window: AddReviewThreadReply succeeds, resolve-comments checkpoint
	// is not persisted, retry rebuilds FixItems from the live PR whose thread
	// now contains the posted decline. Gateway-excluded threadFingerprint keeps
	// the marker stable so remote idempotency does not post again.
	preFingerprint := "c1@2026-01-01T00:00:00Z"
	headSHA := "new-head"
	item := FixItem{Type: "comment", ID: "c1", ThreadID: "t1", Author: "alice", Summary: "please fix", ThreadFingerprint: preFingerprint}
	fp := buildDeclinedThreadFingerprint(item, headSHA)
	marker := fixerDeclinedReplyMarker("t1", fp)
	github := &fakeGitHubGateway{
		currentUser: testLooperLogin,
		viewResponses: []PullRequestDetail{{
			Number:      42,
			State:       "OPEN",
			HeadSHA:     headSHA,
			HeadRefName: "feature/fix-42",
			BaseRefName: "main",
			BaseSHA:     "base-1",
			Author:      "alice",
			Comments: []map[string]any{{
				"id":                "c1",
				"threadId":          "t1",
				"body":              "Please fix <!-- looper:stamp v=1 -->",
				"author":            testLooperLogin,
				"threadFingerprint": preFingerprint,
			}},
		}},
		threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{
			{ID: "c1", Author: testLooperLogin, Body: "Please fix <!-- looper:stamp v=1 -->", UpdatedAt: "2026-01-01T00:00:00Z"},
		}}},
	}
	runner := New(Options{GitHub: github})
	checkpoint := fixerCheckpoint{
		FixItems:     []FixItem{item},
		FixItemsHash: hashFixItems([]FixItem{item}),
		Validation:   &ValidationResult{Passed: true, Summary: "ok", HeadSHA: headSHA},
		Push:         &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair: &checkpointRepair{
			ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionDeclined), Explanation: "Out of scope for this PR."}},
		},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}
	input := stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint}

	if _, err := runner.runResolveCommentsStep(context.Background(), input); err != nil {
		t.Fatalf("first runResolveCommentsStep() error = %v", err)
	}
	if len(github.replyCalls) != 1 || !strings.Contains(github.replyCalls[0].Body, marker) {
		t.Fatalf("first reply = %#v, want marker %q", github.replyCalls, marker)
	}
	if len(github.threads[0].Comments) < 2 {
		t.Fatalf("live thread comments = %#v, want posted decline present", github.threads[0].Comments)
	}

	retryItems := collectFixItems(github.viewResponses[0])
	if len(retryItems) != 1 {
		t.Fatalf("retry items = %#v, want 1", retryItems)
	}
	retryFP := buildDeclinedThreadFingerprint(retryItems[0], headSHA)
	if retryFP != fp {
		t.Fatalf("retry fingerprint %q != original %q", retryFP, fp)
	}
	naive := retryItems[0]
	naive.ThreadFingerprint = preFingerprint + "|reply-1@"
	if buildDeclinedThreadFingerprint(naive, headSHA) == fp {
		t.Fatal("including the posted decline in the source fingerprint must change the marker")
	}

	// Retry as if persist never recorded the sent decline.
	if _, err := runner.runResolveCommentsStep(context.Background(), input); err != nil {
		t.Fatalf("retry runResolveCommentsStep() error = %v", err)
	}
	if len(github.replyCalls) != 1 {
		t.Fatalf("retry posted %d replies, want 1 (stable marker hit the existing decline)", len(github.replyCalls))
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %#v, want none", github.resolveCalls)
	}
}

func TestRunResolveCommentsStepDoesNotReplayDeclineAfterReject(t *testing.T) {
	t.Parallel()
	repairCompletedAt := "2026-08-25T08:00:00Z"
	rejectAt := "2026-08-25T08:10:00Z"
	item := FixItem{Type: "comment", ID: "c1", ThreadID: "t1", Author: "alice", Summary: "please fix", ThreadFingerprint: "thread-hash-1"}
	fp := buildDeclinedThreadFingerprint(item, "new-head")
	baseMarker := fixerDeclinedReplyMarker("t1", fp)
	github := &fakeGitHubGateway{
		currentUser: "looper",
		viewResponses: []PullRequestDetail{{
			Number:      42,
			State:       "OPEN",
			HeadSHA:     "new-head",
			HeadRefName: "feature/fix-42",
			BaseRefName: "main",
			BaseSHA:     "base-1",
			Comments: []map[string]any{{
				"id":                "c1",
				"threadId":          "t1",
				"body":              "please fix",
				"author":            "alice",
				"threadFingerprint": "thread-hash-1",
			}},
		}},
		threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{
			{ID: "c1", Author: "alice", Body: "please fix"},
			{ID: "c2", Author: "looper", Body: "Out of scope " + baseMarker},
			{ID: "c3", Author: "looper", Body: "rejected <!-- looper:thread-resolution thread=t1 head=h feedback=f decision=reject_wontfix --> <!-- looper:stamp v=1 -->", CreatedAt: rejectAt, UpdatedAt: rejectAt},
		}}},
	}
	runner := New(Options{GitHub: github})
	checkpoint := fixerCheckpoint{
		FixItems:     []FixItem{item},
		FixItemsHash: hashFixItems([]FixItem{item}),
		Validation:   &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "new-head"},
		Push:         &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair: &checkpointRepair{
			CompletedAt:       repairCompletedAt,
			ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionDeclined), Explanation: "Out of scope for this PR."}},
		},
		ResolvedComments: &checkpointResolvedComments{Items: []checkpointResolvedComment{{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionDeclined), Status: declinePendingAdjudicationStatus, ReplyState: "sent"}}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err == nil || !strings.Contains(err.Error(), "review thread content changed") {
		t.Fatalf("runResolveCommentsStep() error = %v, want rediscovery after reject during decline replay", err)
	}
	if len(github.replyCalls) != 0 {
		t.Fatalf("reply calls = %#v, want none (stale pre-reject decline must not post again)", github.replyCalls)
	}
	if updated.ResumePolicy != loops.ResumePolicyRestartFromDiscover {
		t.Fatalf("updated.ResumePolicy = %q, want restart_from_discover", updated.ResumePolicy)
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "skipped_thread_drift" {
		t.Fatalf("resolved comments = %#v, want skipped_thread_drift", updated.ResolvedComments)
	}
}

func TestRunResolveCommentsStepPostsDeclineWhenRepairFollowsReject(t *testing.T) {
	t.Parallel()
	rejectAt := "2026-08-25T08:10:00Z"
	repairCompletedAt := "2026-08-25T08:20:00Z"
	item := FixItem{Type: "comment", ID: "c1", ThreadID: "t1", Author: "alice", Summary: "please fix", ThreadFingerprint: "thread-hash-1"}
	fp := buildDeclinedThreadFingerprint(item, "new-head")
	baseMarker := fixerDeclinedReplyMarker("t1", fp)
	postMarker := fixerDeclinedReplyMarkerPostReject("t1", fp)
	github := &fakeGitHubGateway{
		currentUser: "looper",
		viewResponses: []PullRequestDetail{{
			Number:      42,
			State:       "OPEN",
			HeadSHA:     "new-head",
			HeadRefName: "feature/fix-42",
			BaseRefName: "main",
			BaseSHA:     "base-1",
			Comments: []map[string]any{{
				"id":                "c1",
				"threadId":          "t1",
				"body":              "please fix",
				"author":            "alice",
				"threadFingerprint": "thread-hash-1",
			}},
		}},
		threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{
			{ID: "c1", Author: "alice", Body: "please fix"},
			{ID: "c2", Author: "looper", Body: "first decline " + baseMarker},
			{ID: "c3", Author: "looper", Body: "rejected <!-- looper:thread-resolution thread=t1 head=h feedback=f decision=reject_wontfix --> <!-- looper:stamp v=1 -->", CreatedAt: rejectAt, UpdatedAt: rejectAt},
		}}},
	}
	runner := New(Options{GitHub: github})
	checkpoint := fixerCheckpoint{
		FixItems:     []FixItem{item},
		FixItemsHash: hashFixItems([]FixItem{item}),
		Validation:   &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "new-head"},
		Push:         &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair: &checkpointRepair{
			CompletedAt:       repairCompletedAt,
			ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionDeclined), Explanation: "Still out of scope after reject."}},
		},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v, want post-reject decline from a fresh decision", err)
	}
	if len(github.replyCalls) != 1 || !strings.Contains(github.replyCalls[0].Body, postMarker) {
		t.Fatalf("reply calls = %#v, want one post-reject decline", github.replyCalls)
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != declinePendingAdjudicationStatus {
		t.Fatalf("resolved comments = %#v, want %s", updated.ResolvedComments, declinePendingAdjudicationStatus)
	}
}

func TestRunResolveCommentsStepRechecksLegacyDeclinedThreadState(t *testing.T) {
	t.Parallel()
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "new-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
			"author":   "alice",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}}}}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Author: "alice", Summary: "please fix", ThreadFingerprint: "thread-hash-1"}}
	checkpoint := fixerCheckpoint{
		FixItems:         fixItems,
		FixItemsHash:     hashFixItems(fixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "new-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionDeclined), Explanation: "Out of scope for this PR."}}},
		ResolvedComments: &checkpointResolvedComments{Items: []checkpointResolvedComment{{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionDeclined), Status: "agent_declined", Message: "Out of scope for this PR.", ReplyState: "sent"}}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v", err)
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %#v, want none for legacy open declined thread", github.resolveCalls)
	}
	if len(github.replyCalls) != 0 {
		t.Fatalf("reply calls = %#v, want no duplicate declined reply", github.replyCalls)
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != declinePendingAdjudicationStatus {
		t.Fatalf("resolved comments = %#v, want %s after recheck", updated.ResolvedComments, declinePendingAdjudicationStatus)
	}
}

func TestDeclineOnlyResolveThenRecheckCompletesWithoutNoCommitHold(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	loop := storage.LoopRecord{ID: "loop_decline_only", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	detail := PullRequestDetail{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "same-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
			"author":   "alice",
		}},
	}
	github := &fakeGitHubGateway{
		viewResponses: []PullRequestDetail{detail, detail},
		threads:       []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Author: "alice", Summary: "please fix", ThreadFingerprint: "thread-hash-1"}}
	checkpoint := fixerCheckpoint{
		Detail:           &checkpointDetail{State: "OPEN", HeadSHA: "same-head", HeadRefName: "feature/fix-42", BaseRefName: "main"},
		FixItems:         fixItems,
		FixItemsHash:     hashFixItems(fixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "same-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionDeclined), Explanation: "Out of scope for this PR; leave for Reviewer adjudication."}}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "same-head", FinalHeadSHA: "same-head", WorkingTreeClean: true},
	}
	input := stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: checkpoint}

	afterResolve, err := runner.runResolveCommentsStep(context.Background(), input)
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v", err)
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %#v, want none (decline leaves thread open)", github.resolveCalls)
	}
	if len(github.replyCalls) != 1 || !strings.Contains(github.replyCalls[0].Body, "Out of scope") {
		t.Fatalf("reply calls = %#v, want declined explanation", github.replyCalls)
	}
	if afterResolve.ResolvedComments == nil || afterResolve.ResolvedComments.Items[0].Status != declinePendingAdjudicationStatus {
		t.Fatalf("resolved comments = %#v, want %s", afterResolve.ResolvedComments, declinePendingAdjudicationStatus)
	}

	input.Checkpoint = afterResolve
	afterRecheck, err := runner.runRecheckStep(context.Background(), input)
	if err != nil {
		t.Fatalf("runRecheckStep() error = %v, want success (decline-only handoff, no no-commits hold)", err)
	}
	if afterRecheck.Pause != nil {
		t.Fatalf("Pause = %#v, want nil for decline-only recheck", afterRecheck.Pause)
	}
	if afterRecheck.ResumePolicy != "advance_from_checkpoint" {
		t.Fatalf("ResumePolicy = %q, want advance_from_checkpoint", afterRecheck.ResumePolicy)
	}
	if afterRecheck.Recheck == nil || len(afterRecheck.Recheck.RemainingFixItems) != 1 {
		t.Fatalf("Recheck = %#v, want open declined thread still listed as remaining", afterRecheck.Recheck)
	}
	if shouldBlockResolveWithoutFix(afterRecheck, afterRecheck.Recheck.RemainingFixItems, false) {
		t.Fatal("shouldBlockResolveWithoutFix() = true, want false when only decline_pending_adjudication remains")
	}
}

func TestRunRecheckStepStillBlocksWhenUnresolvedCommentAndNoCommits(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:  42,
		State:   "OPEN",
		HeadSHA: "same-head",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "still open",
			"author":   "alice",
		}},
	}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})
	// Mixed: one decline pending adjudication, one comment with no resolve progress.
	checkpoint := fixerCheckpoint{
		Detail:       &checkpointDetail{State: "OPEN", HeadSHA: "same-head"},
		FixItems:     []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}, {Type: "comment", ID: "c2", ThreadID: "t2"}},
		FixItemsHash: hashFixItemsState([]FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}, {Type: "comment", ID: "c2", ThreadID: "t2"}}),
		Push:         &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		ResolvedComments: &checkpointResolvedComments{Items: []checkpointResolvedComment{
			{FixItemID: "c2", ThreadID: "t2", Action: string(replyActionDeclined), Status: declinePendingAdjudicationStatus, ReplyState: "sent"},
			// c1 never declined or resolved — still a true unresolved comment.
		}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "same-head", FinalHeadSHA: "same-head", WorkingTreeClean: true},
	}
	// Live PR still has c1 open (and optionally c2); only c1 should force the hold.
	github.viewResponses = []PullRequestDetail{{
		Number:  42,
		State:   "OPEN",
		HeadSHA: "same-head",
		Comments: []map[string]any{
			{"id": "c1", "threadId": "t1", "body": "still open", "author": "alice"},
			{"id": "c2", "threadId": "t2", "body": "declined open", "author": "alice"},
		},
	}}

	updated, err := runner.runRecheckStep(context.Background(), stepInput{
		Project:    storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Loop:       storage.LoopRecord{ID: "loop_mixed", ProjectID: "project_1"},
		Repo:       "acme/looper",
		PRNumber:   42,
		Checkpoint: checkpoint,
	})
	if err == nil {
		t.Fatal("runRecheckStep() error = nil, want manual intervention when a non-declined comment remains")
	}
	loopErr, ok := err.(*loopError)
	if !ok || loopErr.kind != FailureManualIntervention {
		t.Fatalf("runRecheckStep() error = %#v, want manual_intervention", err)
	}
	if !strings.Contains(err.Error(), "no new commits to push") {
		t.Fatalf("error = %q, want no-new-commits message", err.Error())
	}
	if updated.Pause == nil || updated.Pause.Reason != string(checkpointPauseReasonNoopResolveNoNewCommits) {
		t.Fatalf("Pause = %#v, want noop_resolve_no_new_commits", updated.Pause)
	}
}

func TestRunResolveCommentsStepLeavesLegacyRemotelyResolvedDeclineResolved(t *testing.T) {
	t.Parallel()
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "new-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
			"author":   "alice",
		}},
	}}, threads: []ReviewThread{{ID: "t1", IsResolved: true, Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}}}}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Author: "alice", Summary: "please fix", ThreadFingerprint: "thread-hash-1"}}
	checkpoint := fixerCheckpoint{
		FixItems:         fixItems,
		FixItemsHash:     hashFixItems(fixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "new-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionDeclined), Explanation: "Out of scope for this PR."}}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v", err)
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %#v, want none for already-resolved legacy decline", github.resolveCalls)
	}
	if len(github.replyCalls) != 0 {
		t.Fatalf("reply calls = %#v, want none for already-resolved legacy decline", github.replyCalls)
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "already_resolved" {
		t.Fatalf("resolved comments = %#v, want already_resolved", updated.ResolvedComments)
	}
}

func TestRunResolveCommentsStepDoesNotPersistDeclinedFingerprintWhenReplyFails(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	loop := storage.LoopRecord{ID: "loop_decline_reply_failure", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "new-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
			"author":   "alice",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}}}, replyErr: errors.New("reply failed")}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Author: "alice", Summary: "please fix", ThreadFingerprint: "thread-hash-1"}}
	checkpoint := fixerCheckpoint{
		FixItems:         fixItems,
		FixItemsHash:     hashFixItems(fixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "new-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionDeclined), Explanation: "Out of scope for this PR."}}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: checkpoint})
	if err == nil || !strings.Contains(err.Error(), "Failed to resolve") {
		t.Fatalf("runResolveCommentsStep() error = %v, want retryable mutation failure error", err)
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "failed_mutation_retry" || updated.ResolvedComments.Items[0].ReplyState != "failed" {
		t.Fatalf("resolved comments = %#v, want failed_mutation_retry with failed reply state", updated.ResolvedComments)
	}
	if updated.ResumePolicy != loops.ResumePolicyReplayStep {
		t.Fatalf("updated.ResumePolicy = %q, want replay_step", updated.ResumePolicy)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if records := parseDeclinedThreadRecords(parseJSONObject(persisted.MetadataJSON)); len(records) != 0 {
		t.Fatalf("declined thread records = %#v, want none after failed reply", records)
	}
	if got := suppressDeclinedFixItems(persisted.MetadataJSON, "new-head", fixItems); len(got) != 1 || got[0].ID != "c1" {
		t.Fatalf("suppressDeclinedFixItems() = %#v, want original fix item after failed reply", got)
	}
}

func TestRunResolveCommentsStepTreatsUnknownActionAsContractViolation(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	loop := storage.LoopRecord{ID: "loop_invalid_action", Seq: 2, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "new-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
			"author":   "alice",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Author: "alice", Summary: "please fix", ThreadFingerprint: "thread-hash-1"}}
	checkpoint := fixerCheckpoint{
		FixItems:         fixItems,
		FixItemsHash:     hashFixItems(fixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "new-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Action: "decliend", Explanation: "Out of scope for this PR."}}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: checkpoint})
	if err == nil || !strings.Contains(err.Error(), "omitted or invalidated thread decisions") {
		t.Fatalf("runResolveCommentsStep() error = %v, want invalid-action contract violation", err)
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %#v, want none for invalid action", github.resolveCalls)
	}
	if len(github.replyCalls) != 0 {
		t.Fatalf("reply calls = %#v, want no invalid-action decline reply", github.replyCalls)
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "skipped_invalid_agent_decision" {
		t.Fatalf("resolved comments = %#v, want skipped_invalid_agent_decision", updated.ResolvedComments)
	}
	if updated.ResumePolicy != loops.ResumePolicyRestartFromDiscover {
		t.Fatalf("updated.ResumePolicy = %q, want restart_from_discover", updated.ResumePolicy)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	meta := parseJSONObject(persisted.MetadataJSON)
	if got := int(int64FromAny(meta["fixerContractViolationCount"])); got != 1 {
		t.Fatalf("fixerContractViolationCount = %d, want 1", got)
	}
}

func TestRunResolveCommentsStepHandlesNewThreadAsContractViolationWithoutSkippingExisting(t *testing.T) {
	t.Parallel()

	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "new-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
		}, {
			"id":       "c2",
			"threadId": "t2",
			"body":     "new feedback",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}}, {ID: "t2", Comments: []ReviewThreadComment{{ID: "c2", Body: "new feedback"}}}}}
	// fixItems mirrors the snapshot the agent saw: only c1/t1.
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	// Live PR has gained thread t2/c2 since the agent ran. The agent's
	// existing decision for c1 must still be honoured (reply + resolve);
	// the unknown thread t2 falls through to the contract-violation path
	// without a synthetic decline reply or resolution.
	checkpoint := fixerCheckpoint{
		FixItems:         fixItems,
		FixItemsHash:     hashFixItems(fixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "new-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}})}}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	loop := storage.LoopRecord{ID: "loop_new_thread_contract", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{
		Project:    storage.ProjectRecord{RepoPath: t.TempDir()},
		Repo:       repo,
		PRNumber:   prNumber,
		Loop:       loop,
		Checkpoint: checkpoint,
	})
	if err == nil || !strings.Contains(err.Error(), "omitted or invalidated thread decisions") {
		t.Fatalf("runResolveCommentsStep() error = %v, want contract-violation retry", err)
	}
	if len(github.resolveCalls) != 1 || github.resolveCalls[0].ThreadID != "t1" {
		t.Fatalf("resolve calls = %#v, want exactly 1 resolve for t1", github.resolveCalls)
	}
	if len(github.replyCalls) != 1 {
		t.Fatalf("reply calls = %d, want 1 fixed reply for t1", len(github.replyCalls))
	}
	var (
		t1Reply *AddReviewThreadReplyInput
	)
	for i, call := range github.replyCalls {
		switch call.ThreadID {
		case "t1":
			t1Reply = &github.replyCalls[i]
		}
	}
	if t1Reply == nil || !strings.Contains(t1Reply.Body, "Applied the requested fix.") {
		t.Fatalf("t1 reply = %#v, want fixed explanation", t1Reply)
	}
	statusByThread := map[string]string{}
	for _, item := range updated.ResolvedComments.Items {
		statusByThread[item.ThreadID] = item.Status
	}
	if statusByThread["t1"] != "resolved" {
		t.Fatalf("t1 status = %q, want resolved", statusByThread["t1"])
	}
	if statusByThread["t2"] != "skipped_missing_agent_decision" {
		t.Fatalf("t2 status = %q, want skipped_missing_agent_decision", statusByThread["t2"])
	}
	if updated.ResumePolicy != loops.ResumePolicyRestartFromDiscover {
		t.Fatalf("updated.ResumePolicy = %q, want restart_from_discover", updated.ResumePolicy)
	}
}

func TestRunResolveCommentsStepRejectsDeclinedReplyWithoutReason(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	loop := storage.LoopRecord{ID: "loop_declined_without_reason", Seq: 3, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "new-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
			"author":   "alice",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Logger: fixture.logger, Now: fixture.now})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Author: "alice", Summary: "please fix", ThreadFingerprint: "thread-hash-1"}}
	checkpoint := fixerCheckpoint{
		FixItems:         fixItems,
		FixItemsHash:     hashFixItems(fixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "new-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionDeclined), Explanation: agentMissingThreadDecisionExplanation}}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Loop: loop, Repo: repo, PRNumber: prNumber, Checkpoint: checkpoint})
	if err == nil || !strings.Contains(err.Error(), "omitted or invalidated thread decisions") {
		t.Fatalf("runResolveCommentsStep() error = %v, want declined-without-reason contract violation", err)
	}
	if len(github.replyCalls) != 0 {
		t.Fatalf("reply calls = %#v, want none for declined-without-reason", github.replyCalls)
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %#v, want none for declined-without-reason", github.resolveCalls)
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "skipped_invalid_agent_decision" || updated.ResolvedComments.Items[0].Message != agentDeclinedThreadWithoutReason {
		t.Fatalf("resolved comments = %#v, want declined-without-reason marker", updated.ResolvedComments)
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if got := int(int64FromAny(parseJSONObject(persisted.MetadataJSON)["fixerContractViolationCount"])); got != 1 {
		t.Fatalf("fixerContractViolationCount = %d, want 1", got)
	}
}

func TestRunResolveCommentsStepSkipsThreadWhenHumanCommentArrivesAfterRunStart(t *testing.T) {
	t.Parallel()

	runStartedAt := "2026-04-11T12:00:00Z"
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "fix-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix", CreatedAt: "2026-04-11T11:59:00Z"}, {ID: "reply-2", Body: "one more thing", CreatedAt: "2026-04-11T12:05:00Z"}}}}}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	checkpoint := fixerCheckpoint{FixItems: fixItems, FixItemsHash: hashFixItems(fixItems), Validation: &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-head"}, Push: &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"}, Repair: &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}})}}}, ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true}}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Run: storage.RunRecord{StartedAt: runStartedAt}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err == nil || !strings.Contains(err.Error(), "review thread content changed") {
		t.Fatalf("runResolveCommentsStep() error = %v, want thread drift retry", err)
	}
	if len(github.replyCalls) != 0 {
		t.Fatalf("reply calls = %d, want 0 after thread drift", len(github.replyCalls))
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %d, want 0 after thread drift", len(github.resolveCalls))
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "skipped_thread_drift" {
		t.Fatalf("resolved comments = %#v, want skipped_thread_drift", updated.ResolvedComments)
	}
	if updated.ResumePolicy != loops.ResumePolicyRestartFromDiscover {
		t.Fatalf("updated.ResumePolicy = %q, want restart_from_discover", updated.ResumePolicy)
	}
}

func TestRunResolveCommentsStepSkipsThreadWhenObservedThreadSnapshotDriftsDuringRepair(t *testing.T) {
	t.Parallel()

	repairCompletedAt := "2026-04-11T12:10:00Z"
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "fix-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
		}, {
			"id":       "c2",
			"threadId": "t1",
			"body":     "also handle edge case",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix", Author: "alice", CreatedAt: "2026-04-11T11:59:00Z"}, {ID: "c2", Body: "also handle edge case", Author: "alice", CreatedAt: "2026-04-11T12:05:00Z"}}}}}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	checkpoint := fixerCheckpoint{
		FixItems:         fixItems,
		FixItemsHash:     hashFixItems(fixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}})}}, CompletedAt: repairCompletedAt},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Run: storage.RunRecord{StartedAt: "2026-04-11T12:30:00Z"}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err == nil || !strings.Contains(err.Error(), "review thread content changed") {
		t.Fatalf("runResolveCommentsStep() error = %v, want retry after observed-thread drift", err)
	}
	if len(github.replyCalls) != 0 {
		t.Fatalf("reply calls = %d, want 0 after observed-thread drift", len(github.replyCalls))
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %d, want 0 after observed-thread drift", len(github.resolveCalls))
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "skipped_thread_drift" {
		t.Fatalf("resolved comments = %#v, want skipped_thread_drift", updated.ResolvedComments)
	}
	if updated.ResumePolicy != loops.ResumePolicyRestartFromDiscover {
		t.Fatalf("updated.ResumePolicy = %q, want restart_from_discover", updated.ResumePolicy)
	}
	if len(updated.FixItems) != 2 || updated.FixItems[1].ID != "c2" {
		t.Fatalf("updated.FixItems = %#v, want live unresolved comments to include c2 for rediscover", updated.FixItems)
	}
}

func TestRunResolveCommentsStepResolvesLooperReviewerStampedThread(t *testing.T) {
	t.Parallel()

	reviewerBody := "please fix\n\n<!-- looper:stamp v=1 -->\n<sub>🔁 Powered by <a href=\"https://github.com/nexu-io/looper\">Looper</a> · runner=reviewer · agent=opencode · model=openai/gpt-5.4 · An autonomous AI dev team for your GitHub repos.</sub>"
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "fix-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     reviewerBody,
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: reviewerBody, Author: "nettee", CreatedAt: "2026-04-11T11:59:00Z", UpdatedAt: "2026-04-11T11:59:00Z"}}}}}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	observedThread := ReviewThread{Comments: []ReviewThreadComment{{ID: "c1", UpdatedAt: "2026-04-11T11:59:00Z"}}}
	checkPoint := fixerCheckpoint{
		FixItems:         fixItems,
		FixItemsHash:     hashFixItems(fixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionFixed), Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(observedThread)}}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkPoint})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v, want Looper reviewer thread resolved", err)
	}
	if len(github.resolveCalls) != 1 || github.resolveCalls[0].ThreadID != "t1" {
		t.Fatalf("resolve calls = %#v, want Looper reviewer thread resolved", github.resolveCalls)
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "resolved" {
		t.Fatalf("resolved comments = %#v, want resolved", updated.ResolvedComments)
	}
}

func TestRunResolveCommentsStepRequiresObservedThreadSnapshotForFixedDecision(t *testing.T) {
	t.Parallel()

	repairCompletedAt := "2026-04-11T12:10:00Z"
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "fix-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
		}, {
			"id":       "c2",
			"threadId": "t1",
			"body":     "also handle edge case",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix", Author: "alice", CreatedAt: "2026-04-11T11:59:00Z"}, {ID: "c2", Body: "also handle edge case", Author: "alice", CreatedAt: "2026-04-11T12:05:00Z"}}}}}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	checkpoint := fixerCheckpoint{
		FixItems:         fixItems,
		FixItemsHash:     hashFixItems(fixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix."}}, CompletedAt: repairCompletedAt},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Run: storage.RunRecord{StartedAt: "2026-04-11T12:30:00Z"}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err == nil || !strings.Contains(err.Error(), "review thread content changed") {
		t.Fatalf("runResolveCommentsStep() error = %v, want retry after missing thread snapshot", err)
	}
	if len(github.replyCalls) != 0 {
		t.Fatalf("reply calls = %d, want 0 after missing thread snapshot", len(github.replyCalls))
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %d, want 0 after missing thread snapshot", len(github.resolveCalls))
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "skipped_thread_drift" {
		t.Fatalf("resolved comments = %#v, want skipped_thread_drift", updated.ResolvedComments)
	}
	if updated.ResumePolicy != loops.ResumePolicyRestartFromDiscover {
		t.Fatalf("updated.ResumePolicy = %q, want restart_from_discover", updated.ResumePolicy)
	}
}

func TestRunResolveCommentsStepAllowsDeclinedDecisionWithoutObservedThreadSnapshot(t *testing.T) {
	t.Parallel()

	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "fix-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix", Author: "alice", CreatedAt: "2026-04-11T11:59:00Z"}}}}}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	checkpoint := fixerCheckpoint{
		FixItems:         fixItems,
		FixItemsHash:     hashFixItems(fixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionDeclined), Explanation: "Out of scope for this PR."}}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v, want declined path without snapshot", err)
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %#v, want none for declined without snapshot", github.resolveCalls)
	}
	if len(github.replyCalls) != 1 || github.replyCalls[0].ThreadID != "t1" {
		t.Fatalf("reply calls = %#v, want one declined reply on t1", github.replyCalls)
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != declinePendingAdjudicationStatus {
		t.Fatalf("resolved comments = %#v, want %s", updated.ResolvedComments, declinePendingAdjudicationStatus)
	}
}

func TestRunResolveCommentsStepDetectsDriftFromEditedComment(t *testing.T) {
	t.Parallel()

	runStartedAt := "2026-04-11T12:00:00Z"
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "fix-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{
		{ID: "c1", Body: "please fix (edited)", Author: "alice", CreatedAt: "2026-04-11T11:59:00Z", UpdatedAt: "2026-04-11T12:05:00Z"},
	}}}}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	checkpoint := fixerCheckpoint{FixItems: fixItems, FixItemsHash: hashFixItems(fixItems), Validation: &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-head"}, Push: &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"}, Repair: &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}})}}}, ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true}}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Run: storage.RunRecord{StartedAt: runStartedAt}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err == nil || !strings.Contains(err.Error(), "review thread content changed") {
		t.Fatalf("runResolveCommentsStep() error = %v, want thread drift retry from edited comment", err)
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %d, want 0 after edited-comment drift", len(github.resolveCalls))
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "skipped_thread_drift" {
		t.Fatalf("resolved comments = %#v, want skipped_thread_drift for edited comment", updated.ResolvedComments)
	}
	if updated.ResumePolicy != loops.ResumePolicyRestartFromDiscover {
		t.Fatalf("updated.ResumePolicy = %q, want restart_from_discover", updated.ResumePolicy)
	}
}

func TestRunResolveCommentsStepFlagsThreadDriftWhenCommentEdited(t *testing.T) {
	t.Parallel()

	runStartedAt := "2026-04-11T12:00:00Z"
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "fix-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix (edited)",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{
		{ID: "c1", Body: "please fix (edited)", Author: "alice", CreatedAt: "2026-04-11T11:59:00Z", UpdatedAt: "2026-04-11T12:05:00Z"},
	}}}}
	runner := New(Options{GitHub: github})
	originalFixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	checkpoint := fixerCheckpoint{
		FixItems:         originalFixItems,
		FixItemsHash:     hashFixItems(originalFixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}})}}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Run: storage.RunRecord{StartedAt: runStartedAt}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err == nil || !strings.Contains(err.Error(), "review thread content changed") {
		t.Fatalf("runResolveCommentsStep() error = %v, want per-thread drift retry", err)
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %d, want 0 when the thread comment was edited after run start", len(github.resolveCalls))
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "skipped_thread_drift" {
		t.Fatalf("resolved comments = %#v, want skipped_thread_drift", updated.ResolvedComments)
	}
	if updated.ResumePolicy != loops.ResumePolicyRestartFromDiscover {
		t.Fatalf("updated.ResumePolicy = %q, want restart_from_discover", updated.ResumePolicy)
	}
}

func TestRunResolveCommentsStepIgnoresBotCommentForDrift(t *testing.T) {
	t.Parallel()

	runStartedAt := "2026-04-11T12:00:00Z"
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "fix-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{
		{ID: "c1", Body: "please fix", CreatedAt: "2026-04-11T11:59:00Z"},
		{ID: "bot-1", Body: "automated nitpick", Author: "chatgpt-codex-connector[bot]", CreatedAt: "2026-04-11T12:05:00Z"},
	}}}}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	checkpoint := fixerCheckpoint{FixItems: fixItems, FixItemsHash: hashFixItems(fixItems), Validation: &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-head"}, Push: &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"}, Repair: &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}, {ID: "bot-1", Author: "chatgpt-codex-connector[bot]"}}})}}}, ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true}}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Run: storage.RunRecord{StartedAt: runStartedAt}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v, want resolve through bot comment", err)
	}
	if len(github.resolveCalls) != 1 {
		t.Fatalf("resolve calls = %d, want 1 (bot comment must not block resolve)", len(github.resolveCalls))
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "resolved" {
		t.Fatalf("resolved comments = %#v, want resolved despite bot comment", updated.ResolvedComments)
	}
}

func TestRunResolveCommentsStepRecordsMutationFailureAsRetryable(t *testing.T) {
	t.Parallel()

	github := &fakeGitHubGateway{
		viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "fix-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}},
		threads:       []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}}},
		resolveErr:    errors.New("graphql mutation failed"),
	}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	checkpoint := fixerCheckpoint{FixItems: fixItems, FixItemsHash: hashFixItems(fixItems), Validation: &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-head"}, Push: &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"}, Repair: &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}})}}}, ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true}}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err == nil || !strings.Contains(err.Error(), "Failed to resolve") {
		t.Fatalf("runResolveCommentsStep() error = %v, want retryable mutation failure error", err)
	}
	if len(github.resolveCalls) != 1 {
		t.Fatalf("resolve calls = %d, want 1", len(github.resolveCalls))
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "failed_mutation_retry" {
		t.Fatalf("resolved comments = %#v, want failed_mutation_retry", updated.ResolvedComments)
	}
	if updated.ResumePolicy != loops.ResumePolicyReplayStep {
		t.Fatalf("updated.ResumePolicy = %q, want replay_step", updated.ResumePolicy)
	}
}

func TestRunResolveCommentsStepRetriesResolveAfterPriorLooperReply(t *testing.T) {
	t.Parallel()

	item := FixItem{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}
	github := &fakeGitHubGateway{
		viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "fix-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}},
		threads:       []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}, {ID: "reply-1", Body: buildFixerReplyBody(item, "fix-head", "Applied the requested fix.")}}}},
	}
	runner := New(Options{GitHub: github})
	checkpoint := fixerCheckpoint{
		FixItems:         []FixItem{item},
		FixItemsHash:     hashFixItems([]FixItem{item}),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}})}}},
		ResolvedComments: &checkpointResolvedComments{Items: []checkpointResolvedComment{{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionFixed), Status: "failed_mutation_retry", ReplyState: "sent"}}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v, want retryable resolve to proceed", err)
	}
	if len(github.replyCalls) != 0 {
		t.Fatalf("reply calls = %#v, want 0 because prior Looper reply should be reused", github.replyCalls)
	}
	if len(github.resolveCalls) != 1 || github.resolveCalls[0].ThreadID != "t1" {
		t.Fatalf("resolve calls = %#v, want 1 retry resolve for t1", github.resolveCalls)
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "resolved" {
		t.Fatalf("resolved comments = %#v, want resolved after retrying with prior Looper reply", updated.ResolvedComments)
	}
}

func TestRunResolveCommentsStepProceedsWhenFixCommitStillReachable(t *testing.T) {
	t.Parallel()

	github := &fakeGitHubGateway{
		viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "live-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}},
		threads:       []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}}},
		compareStatus: "ahead",
	}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	checkpoint := fixerCheckpoint{FixItems: fixItems, FixItemsHash: hashFixItems(fixItems), Validation: &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-commit"}, Push: &checkpointPush{Pushed: true, Branch: "feature/fix-42", Remote: "origin", HeadSHA: "fix-commit"}, Repair: &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}})}}}, ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "fix-commit", WorkingTreeClean: true}}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v, want resolve when fix commit is still reachable", err)
	}
	if len(github.compareCalls) != 1 || github.compareCalls[0].Base != "fix-commit" || github.compareCalls[0].Head != "live-head" {
		t.Fatalf("compare calls = %#v, want one call comparing fix-commit...live-head", github.compareCalls)
	}
	if len(github.resolveCalls) != 1 {
		t.Fatalf("resolve calls = %d, want 1 when fix commit still reachable", len(github.resolveCalls))
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "resolved" {
		t.Fatalf("resolved comments = %#v, want resolved when fix commit still reachable", updated.ResolvedComments)
	}
}

func TestRunResolveCommentsStepAbortsWhenFixCommitNoLongerReachable(t *testing.T) {
	t.Parallel()

	github := &fakeGitHubGateway{
		viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "rebased-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}},
		threads:       []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}}},
		compareStatus: "diverged",
	}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	checkpoint := fixerCheckpoint{FixItems: fixItems, FixItemsHash: hashFixItems(fixItems), Validation: &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-commit"}, Push: &checkpointPush{Pushed: true, Branch: "feature/fix-42", Remote: "origin", HeadSHA: "fix-commit"}, Repair: &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}})}}}, ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "fix-commit", WorkingTreeClean: true}}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err == nil || !strings.Contains(err.Error(), "no longer descends") {
		t.Fatalf("runResolveCommentsStep() error = %v, want abort when fix commit is no longer reachable", err)
	}
	if len(github.compareCalls) != 1 {
		t.Fatalf("compare calls = %d, want 1", len(github.compareCalls))
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %d, want 0 when fix commit no longer reachable", len(github.resolveCalls))
	}
	if len(github.replyCalls) != 0 {
		t.Fatalf("reply calls = %d, want 0 when fix commit no longer reachable", len(github.replyCalls))
	}
	if updated.ResumePolicy != loops.ResumePolicyRestartFromDiscover {
		t.Fatalf("updated.ResumePolicy = %q, want restart_from_discover", updated.ResumePolicy)
	}
}

func TestRunResolveCommentsStepDriftAnchorsToRepairCompletionOnReplay(t *testing.T) {
	t.Parallel()

	repairCompletedAt := "2026-04-11T11:00:00Z"
	humanReplyAt := "2026-04-11T11:30:00Z"
	retryRunStartedAt := "2026-04-11T12:00:00Z"
	github := &fakeGitHubGateway{
		viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "fix-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}},
		threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{
			{ID: "c1", Body: "please fix", CreatedAt: "2026-04-11T10:00:00Z"},
			{ID: "reply-2", Body: "actually also fix this", Author: "alice", CreatedAt: humanReplyAt},
		}}},
	}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	checkpoint := fixerCheckpoint{
		FixItems:         fixItems,
		FixItemsHash:     hashFixItems(fixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}})}}, CompletedAt: repairCompletedAt},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Run: storage.RunRecord{StartedAt: retryRunStartedAt}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err == nil || !strings.Contains(err.Error(), "review thread content changed") {
		t.Fatalf("runResolveCommentsStep() error = %v, want drift retry anchored to Repair.CompletedAt", err)
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("resolve calls = %d, want 0 when reviewer commented after repair completion", len(github.resolveCalls))
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "skipped_thread_drift" {
		t.Fatalf("resolved comments = %#v, want skipped_thread_drift on replay", updated.ResolvedComments)
	}
	if updated.ResumePolicy != loops.ResumePolicyRestartFromDiscover {
		t.Fatalf("updated.ResumePolicy = %q, want restart_from_discover", updated.ResumePolicy)
	}
}

func TestRunResolveCommentsStepOnlySkipsDriftedObservedThread(t *testing.T) {
	t.Parallel()

	repairCompletedAt := "2026-04-11T12:10:00Z"
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "fix-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
		}, {
			"id":       "c2",
			"threadId": "t1",
			"body":     "also handle edge case",
		}, {
			"id":       "c3",
			"threadId": "t2",
			"body":     "rename helper",
		}},
	}}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix", Author: "alice", CreatedAt: "2026-04-11T11:59:00Z"}, {ID: "c2", Body: "also handle edge case", Author: "alice", CreatedAt: "2026-04-11T12:05:00Z"}}}, {ID: "t2", Comments: []ReviewThreadComment{{ID: "c3", Body: "rename helper", Author: "alice", CreatedAt: "2026-04-11T11:58:00Z"}}}}}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}, {Type: "comment", ID: "c3", ThreadID: "t2", Summary: "rename helper"}}
	checkpoint := fixerCheckpoint{
		FixItems:         fixItems,
		FixItemsHash:     hashFixItems(fixItems),
		Validation:       &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-head"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Repair:           &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}})}, {FixItemID: "c3", ThreadID: "t2", Explanation: "Renamed the helper.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c3"}}})}}, CompletedAt: repairCompletedAt},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Run: storage.RunRecord{StartedAt: "2026-04-11T12:30:00Z"}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err == nil || !strings.Contains(err.Error(), "review thread content changed") {
		t.Fatalf("runResolveCommentsStep() error = %v, want retry after one observed thread drifted", err)
	}
	if len(github.resolveCalls) != 1 || github.resolveCalls[0].ThreadID != "t2" {
		t.Fatalf("resolve calls = %#v, want only t2 resolved", github.resolveCalls)
	}
	if len(github.replyCalls) != 1 || github.replyCalls[0].ThreadID != "t2" {
		t.Fatalf("reply calls = %#v, want only t2 replied", github.replyCalls)
	}
	statusByThread := map[string]string{}
	for _, item := range updated.ResolvedComments.Items {
		statusByThread[item.ThreadID] = item.Status
	}
	if statusByThread["t1"] != "skipped_thread_drift" {
		t.Fatalf("t1 status = %q, want skipped_thread_drift", statusByThread["t1"])
	}
	if statusByThread["t2"] != "resolved" {
		t.Fatalf("t2 status = %q, want resolved", statusByThread["t2"])
	}
}

func TestRunResolveCommentsStepReplyFailureDoesNotBlockResolve(t *testing.T) {
	t.Parallel()

	github := &fakeGitHubGateway{
		viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "fix-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}},
		threads:       []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}}},
		replyErr:      errors.New("reply failed"),
	}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	checkpoint := fixerCheckpoint{FixItems: fixItems, FixItemsHash: hashFixItems(fixItems), Validation: &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "fix-head"}, Push: &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"}, Repair: &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}})}}}, ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true}}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v", err)
	}
	if len(github.resolveCalls) != 1 {
		t.Fatalf("resolve calls = %d, want 1 despite reply failure", len(github.resolveCalls))
	}
	if updated.ResolvedComments == nil || updated.ResolvedComments.Items[0].Status != "resolved" || updated.ResolvedComments.Items[0].ReplyState != "failed" {
		t.Fatalf("resolved comments = %#v, want resolved with failed reply state", updated.ResolvedComments)
	}
}

func TestRunResolveCommentsStepUsesRefreshedPushHeadSHA(t *testing.T) {
	t.Parallel()

	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "new-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
		}},
	}}}
	runner := New(Options{GitHub: github})
	fixItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}}
	checkpoint := fixerCheckpoint{
		Detail:       &checkpointDetail{HeadSHA: "old-head", HeadRefName: "feature/fix-42", BaseRefName: "main"},
		FixItems:     fixItems,
		FixItemsHash: hashFixItems(fixItems),
		Validation:   &ValidationResult{Passed: true, Summary: "ok", HeadSHA: "new-head"},
		Push:         &checkpointPush{Pushed: true, Branch: "feature/fix-42", Remote: "origin", HeadSHA: "new-head"},
		Repair:       &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "c1", ThreadID: "t1", Explanation: "Applied the requested fix.", ThreadCommentsObserved: hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}})}}},
		Lifecycle:    &lifecycle.State{Pushed: true},
		ReconcileCommits: &checkpointReconcileCommits{
			BaseHeadSHA:      "base-head",
			FinalHeadSHA:     "old-head",
			NewCommitSHAs:    []string{"new-head"},
			WorkingTreeClean: true,
		},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{
		Project:    storage.ProjectRecord{RepoPath: t.TempDir()},
		Repo:       "acme/looper",
		PRNumber:   42,
		Checkpoint: checkpoint,
	})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v", err)
	}
	if len(github.resolveCalls) != 1 {
		t.Fatalf("resolve calls = %d, want 1", len(github.resolveCalls))
	}
	if len(github.replyCalls) != 1 || !contains(github.replyCalls[0].Body, "new-hea") {
		t.Fatalf("reply calls = %#v, want reply referencing pushed head", github.replyCalls)
	}
	if updated.ResumePolicy != "advance_from_checkpoint" {
		t.Fatalf("updated.ResumePolicy = %q, want advance_from_checkpoint", updated.ResumePolicy)
	}
}

func TestRunResolveCommentsStepPreservesCheckpointLabelSnapshotOnLiveRefresh(t *testing.T) {
	t.Parallel()

	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "base-head",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-1",
		Labels:      nil,
		Comments: []map[string]any{{
			"id":         "c1",
			"threadId":   "t1",
			"body":       "please fix",
			"isResolved": true,
			"state":      "RESOLVED",
		}},
	}}}
	runner := New(Options{GitHub: github, Now: time.Now})
	originalLabels := []string{specpr.ReviewingLabel}
	checkpoint := fixerCheckpoint{
		Detail: &checkpointDetail{
			Labels:      append([]string(nil), originalLabels...),
			HeadSHA:     "base-head",
			HeadRefName: "feature/fix-42",
			BaseRefName: "main",
			Comments: []map[string]any{{
				"id":       "c1",
				"threadId": "t1",
				"body":     "please fix",
			}},
		},
		FixItems:         []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", Summary: "please fix"}},
		Validation:       &ValidationResult{Passed: true, Summary: "ok"},
		Push:             &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{
		Project:    storage.ProjectRecord{RepoPath: t.TempDir()},
		Repo:       "acme/looper",
		PRNumber:   42,
		Checkpoint: checkpoint,
	})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v", err)
	}
	if updated.Detail == nil {
		t.Fatal("updated.Detail = nil, want merged detail")
	}
	if !specpr.HasLabel(updated.Detail.Labels, specpr.ReviewingLabel) {
		t.Fatalf("updated.Detail.Labels = %#v, want preserved %q label", updated.Detail.Labels, specpr.ReviewingLabel)
	}
	if len(updated.Detail.Labels) != 1 {
		t.Fatalf("updated.Detail.Labels = %#v, want preserved snapshot only", updated.Detail.Labels)
	}
}

func TestRunPrepareWorktreeStepPreservesExistingLifecycle(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	git := &fakeGitGateway{createResult: CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"}, prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Git: git, Logger: fixture.logger, Now: fixture.now})
	baseBranch := "main"
	checkpoint := fixerCheckpoint{
		Detail:    &checkpointDetail{HeadRefName: "feature/fix-42", BaseRefName: "main", HeadSHA: "head-1"},
		Lifecycle: &lifecycle.State{Branch: "feature/fix-42", BaseBranch: "main", CommitSHAs: []string{"commit-1"}, Pushed: true, PRNumber: 42, PRURL: "https://example/pr/42", Actions: lifecycle.Actions{Commit: lifecycle.ActionSourceAgent, Push: lifecycle.ActionSourceAgent, PR: lifecycle.ActionSourceFallback}},
	}

	prepared, err := runner.runPrepareWorktreeStep(context.Background(), stepInput{
		Project:    storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir(), BaseBranch: &baseBranch},
		Repo:       "acme/looper",
		PRNumber:   42,
		Checkpoint: checkpoint,
	})
	if err != nil {
		t.Fatalf("runPrepareWorktreeStep() error = %v", err)
	}
	if prepared.Lifecycle == nil || len(prepared.Lifecycle.CommitSHAs) != 1 || prepared.Lifecycle.CommitSHAs[0] != "commit-1" || !prepared.Lifecycle.Pushed || prepared.Lifecycle.PRNumber != 42 || prepared.Lifecycle.PRURL == "" || prepared.Lifecycle.Actions.PR != lifecycle.ActionSourceFallback {
		t.Fatalf("Lifecycle = %#v, want existing lifecycle metadata preserved", prepared.Lifecycle)
	}
}

func TestProcessClaimedItemSkipsPRsNotOwnedByCurrentUser(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		currentUser:   "looper-bot",
		viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}},
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loop := storage.LoopRecord{ID: "loop_foreign", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue := storage.QueueItemRecord{ID: "queue_foreign", ProjectID: stringPtr("project_1"), LoopID: &loop.ID, Type: "fixer", TargetType: "pull_request", TargetID: "pr:acme/looper:42", Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:foreign", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, Attempts: 1, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), queue)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !contains(result.Summary, "does not match fixer owner") {
		t.Fatalf("result = %#v, want skipped foreign PR", result)
	}
}

func TestProcessClaimedItemAllowsManualFixerRunForForeignPR(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		currentUser: "looper-bot",
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
		},
	}
	git := &fakeGitGateway{
		createResult:   CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult:  PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
		inspectResults: []InspectHeadResult{{HeadSHA: "base-head"}, {HeadSHA: "new-head", NewCommitSHAs: []string{"new-head"}}, {HeadSHA: "new-head"}},
	}
	stdout := fmt.Sprintf(`__LOOPER_RESULT__={"summary":"applied fixes","review_thread_replies":[{"fixItemId":"c1","threadId":"t1","explanation":"Adjusted the off-by-one handling and verified the fix.","threadCommentsObserved":"%s"}]}`+"\n", hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}}))
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "applied fixes", ParseStatus: "parsed", Stdout: stdout}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	metadata := `{"manual":true}`
	loop := storage.LoopRecord{ID: "loop_manual_foreign", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue := storage.QueueItemRecord{ID: "queue_manual_foreign", ProjectID: stringPtr("project_1"), LoopID: &loop.ID, Type: "fixer", TargetType: "pull_request", TargetID: "pr:acme/looper:42", Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:manual-foreign", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, Attempts: 1, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), queue)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success for manual foreign PR", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) = %d, want manual fixer run to execute", len(agent.starts))
	}
}

func TestProcessClaimedItemAllowsRecoveredManualFixerRunForForeignPR(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		currentUser: "looper-bot",
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
		},
	}
	git := &fakeGitGateway{
		createResult:   CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult:  PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
		inspectResults: []InspectHeadResult{{HeadSHA: "base-head"}, {HeadSHA: "new-head", NewCommitSHAs: []string{"new-head"}}, {HeadSHA: "new-head"}},
	}
	stdout := fmt.Sprintf(`__LOOPER_RESULT__={"summary":"applied fixes","review_thread_replies":[{"fixItemId":"c1","threadId":"t1","explanation":"Adjusted the off-by-one handling and verified the fix.","threadCommentsObserved":"%s"}]}`+"\n", hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}}))
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "applied fixes", ParseStatus: "parsed", Stdout: stdout}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	metadata := `{"manual":true}`
	loop := storage.LoopRecord{ID: "loop_manual_foreign_recovered", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue := storage.QueueItemRecord{ID: "queue_manual_foreign_recovered", ProjectID: stringPtr("project_1"), LoopID: &loop.ID, Type: "fixer", TargetType: "pull_request", TargetID: "pr:acme/looper:42", Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:manual-foreign-recovered", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, Attempts: 1, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), queue)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success for recovered manual foreign PR", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) = %d, want recovered manual fixer run to execute", len(agent.starts))
	}
}

func TestProcessClaimedItemAllowsRecoveredManualFixerRunDespiteRuntimeLabelMismatch(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		currentUser: "looper-bot",
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Author: "human", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
		},
	}
	git := &fakeGitGateway{
		createResult:   CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult:  PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
		inspectResults: []InspectHeadResult{{HeadSHA: "base-head"}, {HeadSHA: "new-head", NewCommitSHAs: []string{"new-head"}}, {HeadSHA: "new-head"}},
	}
	stdout := fmt.Sprintf(`__LOOPER_RESULT__={"summary":"applied fixes","review_thread_replies":[{"fixItemId":"c1","threadId":"t1","explanation":"Adjusted the off-by-one handling and verified the fix.","threadCommentsObserved":"%s"}]}`+"\n", hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}}))
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "applied fixes", ParseStatus: "parsed", Stdout: stdout}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, AuthorFilter: config.FixerAuthorFilterCurrentUser, Labels: []string{"bug"}, LabelMode: config.LabelModeAll}})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	metadata := `{"manual":true}`
	payloadJSON := mustMarshalJSON(map[string]any{"discoveryFingerprint": buildFixerDiscoveryFingerprint(repo, prNumber, "head-1", "hash-1")})
	loop := storage.LoopRecord{ID: "loop_manual_runtime_label_bypass", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue := storage.QueueItemRecord{ID: "queue_manual_runtime_label_bypass", ProjectID: stringPtr("project_1"), LoopID: &loop.ID, Type: "fixer", TargetType: "pull_request", TargetID: "pr:acme/looper:42", Repo: &repo, PRNumber: &prNumber, DedupeKey: buildFixerDedupeKey("project_1", loop.ID, repo, prNumber, "head-1", "hash-1"), Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, Attempts: 1, MaxAttempts: 3, PayloadJSON: &payloadJSON, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), queue)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success for recovered manual fixer run", result)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) = %d, want recovered manual fixer run to execute", len(agent.starts))
	}
}

func TestProcessClaimedItemPausesWhenLabelsNoLongerMatchAtRuntime(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}, {Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}}}
	agent := &fakeAgentExecutor{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, AuthorFilter: config.FixerAuthorFilterCurrentUser, Labels: []string{"bug"}, LabelMode: config.LabelModeAll}})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	payloadJSON := mustMarshalJSON(map[string]any{"discoveryFingerprint": buildFixerDiscoveryFingerprint(repo, prNumber, "head-1", "hash-1")})
	loop := storage.LoopRecord{ID: "loop_runtime_label_gate", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue := storage.QueueItemRecord{ID: "queue_runtime_label_gate", ProjectID: stringPtr("project_1"), LoopID: &loop.ID, Type: "fixer", TargetType: "pull_request", TargetID: "pr:acme/looper:42", Repo: &repo, PRNumber: &prNumber, DedupeKey: buildFixerDedupeKey("project_1", loop.ID, repo, prNumber, "head-1", "hash-1"), Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, Attempts: 1, MaxAttempts: 3, PayloadJSON: &payloadJSON, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), queue)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "skipped" || !contains(result.Summary, "labels no longer match") {
		t.Fatalf("result = %#v, want skipped label-gated run", result)
	}
	if len(agent.starts) != 0 {
		t.Fatalf("len(agent.starts) = %d, want no agent execution when labels mismatch", len(agent.starts))
	}
	persistedLoop, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || persistedLoop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", persistedLoop, err)
	}
	if persistedLoop.Status != "paused" || persistedLoop.NextRunAt != nil {
		t.Fatalf("loop = %#v, want paused loop after runtime label mismatch", persistedLoop)
	}
	persistedQueue, err := fixture.repos.Queue.GetByID(context.Background(), queue.ID)
	if err != nil || persistedQueue == nil {
		t.Fatalf("Queue.GetByID() = (%#v, %v), want queue", persistedQueue, err)
	}
	if persistedQueue.Status != "completed" {
		t.Fatalf("queue = %#v, want completed queue item after runtime label mismatch", persistedQueue)
	}
}

func TestRunDiscoverPRStepLabelMismatchFinalizerPausesLoop(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Labels: []string{"needs-review"}, Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now, DiscoveryPolicy: DiscoveryPolicy{AutoDiscovery: true, IncludeDrafts: false, AuthorFilter: config.FixerAuthorFilterCurrentUser, Labels: []string{"bug"}, LabelMode: config.LabelModeAll}})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loop := storage.LoopRecord{ID: "loop_discover_step_label_mismatch", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	run := storage.RunRecord{ID: "run_discover_step_label_mismatch", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), run); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	queue := storage.QueueItemRecord{ID: "queue_discover_step_label_mismatch", ProjectID: stringPtr("project_1"), LoopID: &loop.ID, Type: "fixer", TargetType: "pull_request", TargetID: "pr:acme/looper:42", Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:discover-step-label-mismatch", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, Attempts: 1, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	checkpoint, err := runner.runDiscoverPRStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()}, Loop: loop, Run: run, QueueItem: queue, Repo: repo, PRNumber: prNumber, Checkpoint: fixerCheckpoint{}})
	var mismatchErr *labelMismatchSkipError
	if !errors.As(err, &mismatchErr) {
		t.Fatalf("runDiscoverPRStep() error = %v, want labelMismatchSkipError", err)
	}
	result, err := runner.finishLabelMismatchFixerQueueItem(context.Background(), loop, &run, queue, checkpoint, mismatchErr.summary)
	if err != nil {
		t.Fatalf("finishLabelMismatchFixerQueueItem() error = %v", err)
	}
	if result.Status != "skipped" || !contains(result.Summary, "labels no longer match") {
		t.Fatalf("result = %#v, want skipped label mismatch", result)
	}
	persistedLoop, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil || persistedLoop == nil {
		t.Fatalf("Loops.GetByID() = (%#v, %v), want loop", persistedLoop, err)
	}
	if persistedLoop.Status != "paused" || persistedLoop.NextRunAt != nil {
		t.Fatalf("loop = %#v, want paused loop after discover-step label mismatch", persistedLoop)
	}
	metadata := parseJSONObject(persistedLoop.MetadataJSON)
	if metadata["pauseReason"] != labelMismatchPauseReason {
		t.Fatalf("metadata = %#v, want label mismatch pause reason", metadata)
	}
}

func TestRunDiscoverPRStepAcceptsProjectedAutomationCommentsWithoutRetry(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "head-42",
		HeadRefName: "feature/fix-42",
		BaseRefName: "main",
		BaseSHA:     "base-42",
		IssueComments: []map[string]any{
			{"id": float64(101), "body": "<!-- looper:forgejo-reviewer-summary payload -->"},
			{"id": float64(202), "body": "<!-- looper:fixer-round head=head-42 -->"},
		},
	}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	checkpoint, err := runner.runDiscoverPRStep(context.Background(), stepInput{
		Project:  storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Loop:     storage.LoopRecord{Type: "fixer"},
		Repo:     "acme/looper",
		PRNumber: 42,
	})
	if err != nil {
		t.Fatalf("runDiscoverPRStep() error = %v, want discovery to continue without retry", err)
	}
	if checkpoint.ResumePolicy != "replay_step" || checkpoint.Detail == nil || len(checkpoint.Detail.IssueComments) != 2 {
		t.Fatalf("checkpoint = %#v, want successful discovery with projected comments", checkpoint)
	}
	if github.viewIndex != 1 {
		t.Fatalf("ViewPullRequest calls = %d, want one deterministic discovery attempt", github.viewIndex)
	}
}

func TestProcessClaimedItemMarksRunFailedWhenOwnershipCheckErrorsBeforeStart(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentUserErr: errors.New("github timeout")}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loop := storage.LoopRecord{ID: "loop_ownership_error", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "queued", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue := storage.QueueItemRecord{ID: "queue_ownership_error", ProjectID: stringPtr("project_1"), LoopID: &loop.ID, Type: "fixer", TargetType: "pull_request", TargetID: "pr:acme/looper:42", Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:ownership-error", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, Attempts: 1, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	_, err := runner.ProcessClaimedItem(context.Background(), queue)
	if err == nil || !strings.Contains(err.Error(), "github timeout") {
		t.Fatalf("ProcessClaimedItem() error = %v, want github timeout", err)
	}
	latest, err := fixture.repos.Runs.GetLatestByLoopID(context.Background(), loop.ID)
	if err != nil || latest == nil {
		t.Fatalf("Runs.GetLatestByLoopID() = (%#v, %v), want run", latest, err)
	}
	if latest.Status != "failed" || latest.EndedAt == nil {
		t.Fatalf("latest run = %#v, want failed terminal run", latest)
	}
	if latest.CurrentStep == nil || *latest.CurrentStep != string(stepDiscoverPR) {
		t.Fatalf("latest.CurrentStep = %#v, want discover-pr", latest.CurrentStep)
	}
	checkpoint := parseCheckpoint(latest.CheckpointJSON)
	if checkpoint.RunStartedAt != "" || checkpoint.RunStartedRunID != "" {
		t.Fatalf("checkpoint start marker = (%q, %q), want no start marker before ownership failure", checkpoint.RunStartedAt, checkpoint.RunStartedRunID)
	}
	if checkpoint.RunPreStartAt == "" || checkpoint.RunPreStartRunID != latest.ID {
		t.Fatalf("checkpoint pre-start marker = (%q, %q), want current run marker", checkpoint.RunPreStartAt, checkpoint.RunPreStartRunID)
	}
	events, err := fixture.repos.Events.ListByEntity(context.Background(), "run", latest.ID)
	if err != nil {
		t.Fatalf("Events.ListByEntity() error = %v", err)
	}
	for _, event := range events {
		if event.EventType == "run.started" {
			t.Fatalf("events = %#v, want no run.started before ownership failure", events)
		}
	}

	resumed, err := runner.createRunContext(context.Background(), loop)
	if err != nil {
		t.Fatalf("createRunContext() retry error = %v", err)
	}
	if resumed.Run.ID == latest.ID || resumed.Run.Status != "running" {
		t.Fatalf("resumed.Run = %#v, want new running run", resumed.Run)
	}
}

func TestProcessClaimedQueueItemRequeuesNetworkFailureDuringOwnershipCheck(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{currentUserErr: errors.New("Command exited with code 1: error connecting to api.github.com\ncheck your internet connection or https://githubstatus.com")}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loop := storage.LoopRecord{ID: "loop_ownership_network_error", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue := storage.QueueItemRecord{ID: "queue_ownership_network_error", ProjectID: stringPtr("project_1"), LoopID: &loop.ID, Type: "fixer", TargetType: "pull_request", TargetID: "pr:acme/looper:42", Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:ownership-network-error", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, Attempts: 1, MaxAttempts: -1, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	result, err := runner.ProcessClaimedQueueItem(context.Background(), queue)
	if err != nil {
		t.Fatalf("ProcessClaimedQueueItem() error = %v", err)
	}
	if result == nil || result.Status != "failed" || result.FailureKind != FailureRetryableTransient {
		t.Fatalf("result = %#v, want retryable transient ownership-check failure", result)
	}
	requeued, err := fixture.repos.Queue.GetByID(context.Background(), queue.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if requeued == nil || requeued.Status != "queued" || requeued.Attempts != 2 || requeued.LastErrorKind == nil || *requeued.LastErrorKind != string(FailureRetryableTransient) || requeued.FinishedAt != nil {
		t.Fatalf("queue = %#v, want queued retryable transient item", requeued)
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if updatedLoop == nil || updatedLoop.Status != "queued" || updatedLoop.NextRunAt == nil {
		t.Fatalf("loop = %#v, want queued loop with next run", updatedLoop)
	}
}

func TestProcessClaimedQueueItemDoesNotRetryLocalProviderConstructionDuringOwnershipCheck(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	// Simulate Forgejo client construction failure already tagged as local/config.
	// The ownership check must preserve that boundary instead of promoting it to
	// BoundaryGitHubAPI (which would leave unlimited queues retrying forever).
	github := &fakeGitHubGateway{currentUserErr: failureclass.WithBoundary(
		errors.New("forgejo client: environment variable FORGEJO_TOKEN is required"),
		failureclass.BoundaryConfig,
	)}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loop := storage.LoopRecord{ID: "loop_ownership_local_config_error", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue := storage.QueueItemRecord{ID: "queue_ownership_local_config_error", ProjectID: stringPtr("project_1"), LoopID: &loop.ID, Type: "fixer", TargetType: "pull_request", TargetID: "pr:acme/looper:42", Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:ownership-local-config-error", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, Attempts: 1, MaxAttempts: -1, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	result, err := runner.ProcessClaimedQueueItem(context.Background(), queue)
	if err != nil {
		t.Fatalf("ProcessClaimedQueueItem() error = %v", err)
	}
	if result == nil || result.Status != "failed" || result.FailureKind != FailureNonRetryable {
		t.Fatalf("result = %#v, want non-retryable local construction failure", result)
	}
	failed, err := fixture.repos.Queue.GetByID(context.Background(), queue.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	// Queue.Fail parks terminal items as manual_intervention; the kind remains non_retryable.
	if failed == nil || failed.Status != "manual_intervention" || failed.LastErrorKind == nil || *failed.LastErrorKind != string(FailureNonRetryable) || failed.FinishedAt == nil {
		t.Fatalf("queue = %#v, want manual_intervention non-retryable item (not infinite requeue)", failed)
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if updatedLoop == nil || updatedLoop.Status != "paused" {
		t.Fatalf("loop = %#v, want paused loop after non-retryable ownership failure", updatedLoop)
	}
}

func TestProcessClaimedQueueItemRequeuesNetworkFailureDuringAuthorLookup(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	// GetCurrentUserLogin succeeds; GetPullRequestAuthor fails with the observed connectivity error.
	// This covers the second WithBoundary return in pullRequestOwnershipSkipReason.
	github := &fakeGitHubGateway{
		currentUser: "looper",
		authorErr:   errors.New("Command exited with code 1: error connecting to api.github.com\ncheck your internet connection or https://githubstatus.com"),
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loop := storage.LoopRecord{ID: "loop_author_lookup_network_error", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue := storage.QueueItemRecord{ID: "queue_author_lookup_network_error", ProjectID: stringPtr("project_1"), LoopID: &loop.ID, Type: "fixer", TargetType: "pull_request", TargetID: "pr:acme/looper:42", Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:author-lookup-network-error", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, Attempts: 1, MaxAttempts: -1, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	result, err := runner.ProcessClaimedQueueItem(context.Background(), queue)
	if err != nil {
		t.Fatalf("ProcessClaimedQueueItem() error = %v", err)
	}
	if result == nil || result.Status != "failed" || result.FailureKind != FailureRetryableTransient {
		t.Fatalf("result = %#v, want retryable transient author-lookup failure", result)
	}
	requeued, err := fixture.repos.Queue.GetByID(context.Background(), queue.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if requeued == nil || requeued.Status != "queued" || requeued.Attempts != 2 || requeued.LastErrorKind == nil || *requeued.LastErrorKind != string(FailureRetryableTransient) || requeued.FinishedAt != nil {
		t.Fatalf("queue = %#v, want queued retryable transient item", requeued)
	}
	updatedLoop, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if updatedLoop == nil || updatedLoop.Status != "queued" || updatedLoop.NextRunAt == nil {
		t.Fatalf("loop = %#v, want queued loop with next run", updatedLoop)
	}
}

func TestProcessClaimedQueueItemRequeuesNetworkFailureDuringLabelAuthorityCheck(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{viewErr: errors.New("Command exited with code 1: error connecting to api.github.com\ncheck your internet connection or https://githubstatus.com")}
	runner := New(Options{
		DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now,
		DiscoveryPolicy: DiscoveryPolicy{AuthorFilter: config.FixerAuthorFilterAny, Labels: []string{"bug"}, LabelMode: config.LabelModeAll},
	})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loop := storage.LoopRecord{ID: "loop_label_authority_network_error", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	queue := storage.QueueItemRecord{ID: "queue_label_authority_network_error", ProjectID: stringPtr("project_1"), LoopID: &loop.ID, Type: "fixer", TargetType: "pull_request", TargetID: "pr:acme/looper:42", Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:label-authority-network-error", Priority: storage.QueuePriorityFixer, Status: "running", AvailableAt: nowISO, Attempts: 1, MaxAttempts: -1, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Queue.Upsert(context.Background(), queue); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	result, err := runner.ProcessClaimedQueueItem(context.Background(), queue)
	if err != nil {
		t.Fatalf("ProcessClaimedQueueItem() error = %v", err)
	}
	if result == nil || result.Status != "failed" || result.FailureKind != FailureRetryableTransient {
		t.Fatalf("result = %#v, want retryable transient label-authority failure", result)
	}
	requeued, err := fixture.repos.Queue.GetByID(context.Background(), queue.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if requeued == nil || requeued.Status != "queued" || requeued.Attempts != 2 || requeued.LastErrorKind == nil || *requeued.LastErrorKind != string(FailureRetryableTransient) || requeued.FinishedAt != nil {
		t.Fatalf("queue = %#v, want queued retryable transient item", requeued)
	}
}

func TestCreateRunContextTreatsLegacyMarkerlessRunAsRetryable(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, ClaimTTL: time.Minute})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	oldISO := fixture.current.Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	loop := storage.LoopRecord{ID: "loop_legacy_markerless", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{ResumePolicy: "replay_step"})
	active := storage.RunRecord{ID: "run_legacy_markerless", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), CheckpointJSON: &checkpointJSON, StartedAt: oldISO, LastHeartbeatAt: &oldISO, CreatedAt: oldISO, UpdatedAt: oldISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), active); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	_, err := runner.createRunContext(context.Background(), loop)
	if err == nil {
		t.Fatal("createRunContext() error = nil, want retryable legacy markerless error")
	}
	var loopErr *loopError
	if !errors.As(err, &loopErr) || loopErr.kind != FailureRetryableTransient {
		t.Fatalf("createRunContext() error = %#v, want retryable transient loopError", err)
	}
	preservedRun, err := fixture.repos.Runs.GetByID(context.Background(), active.ID)
	if err != nil || preservedRun == nil {
		t.Fatalf("Runs.GetByID(active) = (%#v, %v), want run", preservedRun, err)
	}
	if preservedRun.Status != "running" || preservedRun.EndedAt != nil {
		t.Fatalf("preservedRun = %#v, want still-running legacy markerless run", preservedRun)
	}
}

func TestCreateRunContextInterruptsLegacyMarkerlessRunAfterGrace(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, ClaimTTL: time.Minute})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	staleISO := fixture.current.Add(-25 * time.Hour).UTC().Format(time.RFC3339Nano)
	loop := storage.LoopRecord{ID: "loop_stale_legacy_markerless", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{ResumePolicy: "replay_step"})
	orphan := storage.RunRecord{ID: "run_stale_legacy_markerless", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), CheckpointJSON: &checkpointJSON, StartedAt: staleISO, LastHeartbeatAt: &staleISO, CreatedAt: staleISO, UpdatedAt: staleISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), orphan); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	created, err := runner.createRunContext(context.Background(), loop)
	if err != nil {
		t.Fatalf("createRunContext() error = %v", err)
	}
	if created.Run.ID == orphan.ID || created.Run.Status != "running" {
		t.Fatalf("created.Run = %#v, want replacement running run", created.Run)
	}
	oldRun, err := fixture.repos.Runs.GetByID(context.Background(), orphan.ID)
	if err != nil || oldRun == nil {
		t.Fatalf("Runs.GetByID(orphan) = (%#v, %v), want run", oldRun, err)
	}
	if oldRun.Status != "interrupted" || oldRun.EndedAt == nil {
		t.Fatalf("oldRun = %#v, want interrupted stale legacy markerless run", oldRun)
	}
}

func TestCreateRunContextTreatsFreshMarkerlessRunAsRetryable(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, ClaimTTL: time.Minute})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loop := storage.LoopRecord{ID: "loop_fresh_markerless", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{ResumePolicy: "replay_step"})
	active := storage.RunRecord{ID: "run_fresh_markerless", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), CheckpointJSON: &checkpointJSON, StartedAt: nowISO, LastHeartbeatAt: &nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), active); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	_, err := runner.createRunContext(context.Background(), loop)
	if err == nil {
		t.Fatal("createRunContext() error = nil, want retryable fresh-run error")
	}
	var loopErr *loopError
	if !errors.As(err, &loopErr) || loopErr.kind != FailureRetryableTransient {
		t.Fatalf("createRunContext() error = %#v, want retryable transient loopError", err)
	}
}

func TestCreateRunContextTreatsRunningAgentExecutionAsRetryable(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, ClaimTTL: time.Minute})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	oldISO := fixture.current.Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	loop := storage.LoopRecord{ID: "loop_active_agent_execution", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	orphan := storage.RunRecord{ID: "run_active_agent_execution", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), StartedAt: oldISO, LastHeartbeatAt: &oldISO, CreatedAt: oldISO, UpdatedAt: oldISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), orphan); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	runID := orphan.ID
	loopID := loop.ID
	if err := fixture.repos.AgentExecutions.Upsert(context.Background(), storage.AgentExecutionRecord{ID: "agent_active_execution", LoopID: &loopID, RunID: &runID, Vendor: "opencode", Status: "running", StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("AgentExecutions.Upsert() error = %v", err)
	}

	_, err := runner.createRunContext(context.Background(), loop)
	if err == nil {
		t.Fatal("createRunContext() error = nil, want retryable active-run error")
	}
	var loopErr *loopError
	if !errors.As(err, &loopErr) || loopErr.kind != FailureRetryableTransient {
		t.Fatalf("createRunContext() error = %#v, want retryable transient loopError", err)
	}
	activeRun, err := fixture.repos.Runs.GetByID(context.Background(), orphan.ID)
	if err != nil || activeRun == nil {
		t.Fatalf("Runs.GetByID(active) = (%#v, %v), want run", activeRun, err)
	}
	if activeRun.Status != "running" || activeRun.EndedAt != nil {
		t.Fatalf("activeRun = %#v, want still-running active run", activeRun)
	}
}

func TestCreateRunContextTreatsFreshPreStartRunAsRetryable(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, ClaimTTL: time.Minute})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	oldISO := fixture.current.Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	loop := storage.LoopRecord{ID: "loop_fresh_prestart", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{ResumePolicy: "replay_step", RunPreStartAt: nowISO, RunPreStartRunID: "run_fresh_prestart"})
	preStartRun := storage.RunRecord{ID: "run_fresh_prestart", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), CheckpointJSON: &checkpointJSON, StartedAt: oldISO, LastHeartbeatAt: &oldISO, CreatedAt: oldISO, UpdatedAt: oldISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), preStartRun); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	_, err := runner.createRunContext(context.Background(), loop)
	if err == nil {
		t.Fatal("createRunContext() error = nil, want retryable pre-start error")
	}
	var loopErr *loopError
	if !errors.As(err, &loopErr) || loopErr.kind != FailureRetryableTransient {
		t.Fatalf("createRunContext() error = %#v, want retryable transient loopError", err)
	}
	activeRun, err := fixture.repos.Runs.GetByID(context.Background(), preStartRun.ID)
	if err != nil || activeRun == nil {
		t.Fatalf("Runs.GetByID(preStart) = (%#v, %v), want run", activeRun, err)
	}
	if activeRun.Status != "running" || activeRun.EndedAt != nil {
		t.Fatalf("activeRun = %#v, want still-running pre-start run", activeRun)
	}
}

func TestCreateRunContextInterruptsStalePreStartRun(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, ClaimTTL: time.Minute})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	stalePreStartAt := fixture.current.Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	loop := storage.LoopRecord{ID: "loop_stale_prestart", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{ResumePolicy: "replay_step", RunPreStartAt: stalePreStartAt, RunPreStartRunID: "run_stale_prestart"})
	orphan := storage.RunRecord{ID: "run_stale_prestart", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), CheckpointJSON: &checkpointJSON, StartedAt: stalePreStartAt, LastHeartbeatAt: &stalePreStartAt, CreatedAt: stalePreStartAt, UpdatedAt: stalePreStartAt}
	if err := fixture.repos.Runs.Upsert(context.Background(), orphan); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	created, err := runner.createRunContext(context.Background(), loop)
	if err != nil {
		t.Fatalf("createRunContext() error = %v", err)
	}
	if created.Run.ID == orphan.ID || created.Run.Status != "running" {
		t.Fatalf("created.Run = %#v, want replacement running run", created.Run)
	}
	oldRun, err := fixture.repos.Runs.GetByID(context.Background(), orphan.ID)
	if err != nil || oldRun == nil {
		t.Fatalf("Runs.GetByID(orphan) = (%#v, %v), want run", oldRun, err)
	}
	if oldRun.Status != "interrupted" || oldRun.EndedAt == nil {
		t.Fatalf("oldRun = %#v, want interrupted stale pre-start run", oldRun)
	}
}

func TestCreateRunContextTreatsOlderActiveAgentExecutionAsRetryable(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, ClaimTTL: time.Minute})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	activeStartedAt := fixture.current.Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	loop := storage.LoopRecord{ID: "loop_older_active_agent_execution", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	orphan := storage.RunRecord{ID: "run_older_active_agent_execution", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), StartedAt: activeStartedAt, LastHeartbeatAt: &activeStartedAt, CreatedAt: activeStartedAt, UpdatedAt: activeStartedAt}
	if err := fixture.repos.Runs.Upsert(context.Background(), orphan); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	runID := orphan.ID
	loopID := loop.ID
	if err := fixture.repos.AgentExecutions.Upsert(context.Background(), storage.AgentExecutionRecord{ID: "agent_older_active_execution", LoopID: &loopID, RunID: &runID, Vendor: "opencode", Status: "running", StartedAt: activeStartedAt, CreatedAt: activeStartedAt, UpdatedAt: activeStartedAt}); err != nil {
		t.Fatalf("AgentExecutions.Upsert(active) error = %v", err)
	}
	endedAt := nowISO
	if err := fixture.repos.AgentExecutions.Upsert(context.Background(), storage.AgentExecutionRecord{ID: "agent_newer_terminal_execution", LoopID: &loopID, RunID: &runID, Vendor: "opencode", Status: "completed", StartedAt: nowISO, EndedAt: &endedAt, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("AgentExecutions.Upsert(terminal) error = %v", err)
	}

	_, err := runner.createRunContext(context.Background(), loop)
	if err == nil {
		t.Fatal("createRunContext() error = nil, want retryable active-run error")
	}
	var loopErr *loopError
	if !errors.As(err, &loopErr) || loopErr.kind != FailureRetryableTransient {
		t.Fatalf("createRunContext() error = %#v, want retryable transient loopError", err)
	}
	activeRun, err := fixture.repos.Runs.GetByID(context.Background(), orphan.ID)
	if err != nil || activeRun == nil {
		t.Fatalf("Runs.GetByID(active) = (%#v, %v), want run", activeRun, err)
	}
	if activeRun.Status != "running" || activeRun.EndedAt != nil {
		t.Fatalf("activeRun = %#v, want still-running active run", activeRun)
	}
}

func TestCreateRunContextTreatsMarkerlessRunWithTerminalAgentExecutionAsRetryable(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, ClaimTTL: time.Minute})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	loop := storage.LoopRecord{ID: "loop_terminal_agent_execution", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	oldISO := fixture.current.Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	active := storage.RunRecord{ID: "run_terminal_agent_execution", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), StartedAt: oldISO, LastHeartbeatAt: &oldISO, CreatedAt: oldISO, UpdatedAt: oldISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), active); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	runID := active.ID
	loopID := loop.ID
	endedAt := nowISO
	if err := fixture.repos.AgentExecutions.Upsert(context.Background(), storage.AgentExecutionRecord{ID: "agent_terminal_execution", LoopID: &loopID, RunID: &runID, Vendor: "opencode", Status: "completed", StartedAt: nowISO, EndedAt: &endedAt, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("AgentExecutions.Upsert() error = %v", err)
	}

	_, err := runner.createRunContext(context.Background(), loop)
	if err == nil {
		t.Fatal("createRunContext() error = nil, want retryable markerless active-run error")
	}
	var loopErr *loopError
	if !errors.As(err, &loopErr) || loopErr.kind != FailureRetryableTransient {
		t.Fatalf("createRunContext() error = %#v, want retryable transient loopError", err)
	}
	preservedRun, err := fixture.repos.Runs.GetByID(context.Background(), active.ID)
	if err != nil || preservedRun == nil {
		t.Fatalf("Runs.GetByID(active) = (%#v, %v), want run", preservedRun, err)
	}
	if preservedRun.Status != "running" || preservedRun.EndedAt != nil {
		t.Fatalf("preservedRun = %#v, want still-running markerless run", preservedRun)
	}
}

func TestProcessClaimedQueueItemRequeuesWhenActiveRunHasAgentExecution(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := "pr:acme/looper:42"
	nowISO := fixture.nowISO()
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_active_agent_execution_queue", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	activeRun := storage.RunRecord{ID: "run_active_agent_execution_queue", LoopID: "loop_active_agent_execution_queue", Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), StartedAt: nowISO, LastHeartbeatAt: &nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), activeRun); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	runID := activeRun.ID
	loopID := activeRun.LoopID
	if err := fixture.repos.AgentExecutions.Upsert(context.Background(), storage.AgentExecutionRecord{ID: "agent_active_execution_queue", LoopID: &loopID, RunID: &runID, Vendor: "opencode", Status: "running", StartedAt: nowISO, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("AgentExecutions.Upsert() error = %v", err)
	}
	projectID := "project_1"
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_active_agent_execution", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:active-agent-execution", Priority: 1, Status: "queued", AvailableAt: nowISO, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}

	result, err := runner.ProcessClaimedQueueItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedQueueItem() error = %v", err)
	}
	if result == nil || result.Status != "failed" || result.FailureKind != FailureRetryableTransient || !contains(result.Summary, "already has a running fixer run") {
		t.Fatalf("result = %#v, want retryable_transient active-run recovery", result)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), claim.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "queued" || queue.Attempts != 0 || queue.LastErrorKind == nil || *queue.LastErrorKind != string(FailureRetryableTransient) {
		t.Fatalf("queue = %#v, want requeued retryable_transient active-run failure without consuming attempts", queue)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.Status != "queued" || loop.NextRunAt == nil {
		t.Fatalf("loop = %#v, want loop requeued for retry", loop)
	}
	preservedRun, err := fixture.repos.Runs.GetByID(context.Background(), activeRun.ID)
	if err != nil || preservedRun == nil {
		t.Fatalf("Runs.GetByID() = (%#v, %v), want active run", preservedRun, err)
	}
	if preservedRun.Status != "running" || preservedRun.EndedAt != nil {
		t.Fatalf("preservedRun = %#v, want original run preserved", preservedRun)
	}
}

func TestProcessClaimedQueueItemRequeuesLegacyMarkerlessRunWithoutConsumingAttempts(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, ClaimTTL: time.Minute})
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := "pr:acme/looper:42"
	nowISO := fixture.nowISO()
	oldISO := fixture.current.Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_legacy_markerless_queue", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{ResumePolicy: "replay_step"})
	activeRun := storage.RunRecord{ID: "run_legacy_markerless_queue", LoopID: "loop_legacy_markerless_queue", Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), CheckpointJSON: &checkpointJSON, StartedAt: oldISO, LastHeartbeatAt: &oldISO, CreatedAt: oldISO, UpdatedAt: oldISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), activeRun); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	projectID := "project_1"
	loopID := activeRun.LoopID
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_legacy_markerless", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: loopTarget, Repo: &repo, PRNumber: &prNumber, DedupeKey: "fixer:legacy-markerless", Priority: 1, Status: "queued", AvailableAt: nowISO, Attempts: 1, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}

	result, err := runner.ProcessClaimedQueueItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedQueueItem() error = %v", err)
	}
	if result == nil || result.Status != "failed" || result.FailureKind != FailureRetryableTransient || !contains(result.Summary, "markerless running fixer run") {
		t.Fatalf("result = %#v, want retryable_transient markerless active-run recovery", result)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), claim.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "queued" || queue.Attempts != 1 || queue.LastErrorKind == nil || *queue.LastErrorKind != string(FailureRetryableTransient) {
		t.Fatalf("queue = %#v, want requeued markerless active-run failure without consuming attempts", queue)
	}
	preservedRun, err := fixture.repos.Runs.GetByID(context.Background(), activeRun.ID)
	if err != nil || preservedRun == nil {
		t.Fatalf("Runs.GetByID() = (%#v, %v), want active run", preservedRun, err)
	}
	if preservedRun.Status != "running" || preservedRun.EndedAt != nil {
		t.Fatalf("preservedRun = %#v, want original run preserved", preservedRun)
	}
}

func TestCreateRunContextPreservesMarkerlessRunDespiteStartedEvent(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, ClaimTTL: time.Minute})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	oldISO := fixture.current.Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	loop := storage.LoopRecord{ID: "loop_started_run", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	startedRun := storage.RunRecord{ID: "run_started", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), StartedAt: oldISO, LastHeartbeatAt: &oldISO, CreatedAt: oldISO, UpdatedAt: oldISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), startedRun); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	entityType := "run"
	entityID := startedRun.ID
	if err := fixture.repos.Events.Append(context.Background(), storage.EventLogRecord{ID: "event_started_run", EventType: "run.started", LoopID: &loop.ID, RunID: &startedRun.ID, EntityType: &entityType, EntityID: &entityID, PayloadJSON: "{}", CreatedAt: nowISO}); err != nil {
		t.Fatalf("Events.Append() error = %v", err)
	}

	_, err := runner.createRunContext(context.Background(), loop)
	if err == nil {
		t.Fatal("createRunContext() error = nil, want retryable markerless active-run error")
	}
	var loopErr *loopError
	if !errors.As(err, &loopErr) || loopErr.kind != FailureRetryableTransient {
		t.Fatalf("createRunContext() error = %#v, want retryable transient loopError", err)
	}
	preserved, err := fixture.repos.Runs.GetByID(context.Background(), startedRun.ID)
	if err != nil || preserved == nil {
		t.Fatalf("Runs.GetByID(started) = (%#v, %v), want run", preserved, err)
	}
	if preserved.Status != "running" || preserved.EndedAt != nil {
		t.Fatalf("preserved = %#v, want markerless run preserved despite best-effort event", preserved)
	}
}

func TestCreateRunContextTreatsDurablyStartedRunAsRetryableWhenEventMissing(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, ClaimTTL: time.Minute})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	oldISO := fixture.current.Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	loop := storage.LoopRecord{ID: "loop_started_run_missing_event", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{ResumePolicy: "replay_step", RunStartedAt: nowISO, RunStartedRunID: "run_started_missing_event"})
	startedRun := storage.RunRecord{ID: "run_started_missing_event", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), CheckpointJSON: &checkpointJSON, StartedAt: oldISO, LastHeartbeatAt: &oldISO, CreatedAt: oldISO, UpdatedAt: oldISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), startedRun); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	_, err := runner.createRunContext(context.Background(), loop)
	if err == nil {
		t.Fatal("createRunContext() error = nil, want retryable started-run error")
	}
	var loopErr *loopError
	if !errors.As(err, &loopErr) || loopErr.kind != FailureRetryableTransient {
		t.Fatalf("createRunContext() error = %#v, want retryable transient loopError", err)
	}
	activeRun, err := fixture.repos.Runs.GetByID(context.Background(), startedRun.ID)
	if err != nil || activeRun == nil {
		t.Fatalf("Runs.GetByID(started) = (%#v, %v), want run", activeRun, err)
	}
	if activeRun.Status != "running" || activeRun.EndedAt != nil {
		t.Fatalf("activeRun = %#v, want still-running started run", activeRun)
	}
	latestRun, err := fixture.repos.Runs.GetLatestByLoopID(context.Background(), loop.ID)
	if err != nil || latestRun == nil {
		t.Fatalf("Runs.GetLatestByLoopID() = (%#v, %v), want preserved run", latestRun, err)
	}
	if latestRun.ID != startedRun.ID {
		t.Fatalf("latestRun.ID = %q, want preserved run %q", latestRun.ID, startedRun.ID)
	}
}

func TestCreateRunContextTreatsLegacyStartedRunAsRetryable(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, ClaimTTL: time.Minute})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	oldISO := fixture.current.Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	loop := storage.LoopRecord{ID: "loop_legacy_started_run", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{ResumePolicy: "replay_step", RunStartedAt: nowISO})
	startedRun := storage.RunRecord{ID: "run_legacy_started", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), CheckpointJSON: &checkpointJSON, StartedAt: oldISO, LastHeartbeatAt: &oldISO, CreatedAt: oldISO, UpdatedAt: oldISO}
	if err := fixture.repos.Runs.Upsert(context.Background(), startedRun); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	_, err := runner.createRunContext(context.Background(), loop)
	if err == nil {
		t.Fatal("createRunContext() error = nil, want retryable legacy started-run error")
	}
	var loopErr *loopError
	if !errors.As(err, &loopErr) || loopErr.kind != FailureRetryableTransient {
		t.Fatalf("createRunContext() error = %#v, want retryable transient loopError", err)
	}
}

func TestCreateRunContextInterruptsLegacyStaleStartedMarker(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now, ClaimTTL: time.Minute})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	createdAt := fixture.current.Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	staleStartedAt := fixture.current.Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	loop := storage.LoopRecord{ID: "loop_legacy_stale_started", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{ResumePolicy: "advance_from_checkpoint", RunStartedAt: staleStartedAt})
	orphan := storage.RunRecord{ID: "run_legacy_stale_started", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), CheckpointJSON: &checkpointJSON, StartedAt: createdAt, LastHeartbeatAt: &createdAt, CreatedAt: createdAt, UpdatedAt: createdAt}
	if err := fixture.repos.Runs.Upsert(context.Background(), orphan); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	created, err := runner.createRunContext(context.Background(), loop)
	if err != nil {
		t.Fatalf("createRunContext() error = %v", err)
	}
	if created.Run.ID == orphan.ID || created.Run.Status != "running" {
		t.Fatalf("created.Run = %#v, want replacement running run", created.Run)
	}
	oldRun, err := fixture.repos.Runs.GetByID(context.Background(), orphan.ID)
	if err != nil || oldRun == nil {
		t.Fatalf("Runs.GetByID(orphan) = (%#v, %v), want run", oldRun, err)
	}
	if oldRun.Status != "interrupted" || oldRun.EndedAt == nil {
		t.Fatalf("oldRun = %#v, want interrupted legacy stale-start run", oldRun)
	}
}

func TestCreateRunContextInterruptsResumedRunWithStaleStartMarker(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})
	repo := "acme/looper"
	prNumber := int64(42)
	nowISO := fixture.nowISO()
	staleStartedAt := fixture.current.Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	loop := storage.LoopRecord{ID: "loop_stale_started_run", Seq: 1, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{ResumePolicy: "advance_from_checkpoint", RunStartedAt: staleStartedAt, RunStartedRunID: "previous_run"})
	orphan := storage.RunRecord{ID: "run_stale_started_marker", LoopID: loop.ID, Status: "running", CurrentStep: stringPtr(string(stepDiscoverPR)), CheckpointJSON: &checkpointJSON, StartedAt: staleStartedAt, LastHeartbeatAt: &staleStartedAt, CreatedAt: staleStartedAt, UpdatedAt: staleStartedAt}
	if err := fixture.repos.Runs.Upsert(context.Background(), orphan); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}

	created, err := runner.createRunContext(context.Background(), loop)
	if err != nil {
		t.Fatalf("createRunContext() error = %v", err)
	}
	if created.Run.ID == orphan.ID || created.Run.Status != "running" {
		t.Fatalf("created.Run = %#v, want replacement running run", created.Run)
	}
	if created.Checkpoint.RunStartedAt != "" || created.Checkpoint.RunStartedRunID != "" {
		t.Fatalf("created.Checkpoint = %#v, want cleared start marker", created.Checkpoint)
	}
	if created.Checkpoint.RunPreStartAt == "" || created.Checkpoint.RunPreStartRunID != created.Run.ID {
		t.Fatalf("created.Checkpoint = %#v, want current pre-start marker", created.Checkpoint)
	}
	oldRun, err := fixture.repos.Runs.GetByID(context.Background(), orphan.ID)
	if err != nil || oldRun == nil {
		t.Fatalf("Runs.GetByID(orphan) = (%#v, %v), want run", oldRun, err)
	}
	if oldRun.Status != "interrupted" || oldRun.EndedAt == nil {
		t.Fatalf("oldRun = %#v, want interrupted stale pre-start run", oldRun)
	}
}

func TestProcessClaimedItemFailsWhenRepairCompletionResultMissing(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "head-1"}},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
		},
	}
	validationCalls := 0
	git := &fakeGitGateway{
		createResult:  CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "upstream server_error", ParseStatus: "missing"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: func(context.Context, ValidationInput) (ValidationResult, error) {
		validationCalls++
		return ValidationResult{Passed: true, Summary: "ok"}, nil
	}, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableTransient || !contains(result.Summary, "server_error") {
		t.Fatalf("result = %#v, want retryable failed result with upstream error", result)
	}
	if validationCalls != 0 {
		t.Fatalf("validationCalls = %d, want repair failure to stop before validation", validationCalls)
	}
	if len(git.pushCalls) != 0 || len(github.resolveCalls) != 0 {
		t.Fatalf("push calls=%d resolve calls=%d, want 0/0 after invalid repair completion", len(git.pushCalls), len(github.resolveCalls))
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.Status != "queued" {
		t.Fatalf("loop = %#v, want queued loop for retryable failure", loop)
	}
	run, err := fixture.repos.Runs.GetByID(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("Runs.GetByID() error = %v", err)
	}
	if run == nil || run.Status != "failed" || run.CurrentStep == nil || *run.CurrentStep != string(stepRepair) {
		t.Fatalf("run = %#v, want failed run at repair step", run)
	}
	if run.LastCompletedStep != nil && *run.LastCompletedStep == string(stepRecheck) {
		t.Fatalf("run = %#v, want downstream steps to remain incomplete", run)
	}
}

func TestProcessClaimedItemPersistsCheckpointWhenRepairReturnsNonCompleted(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "head-1"}},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
		},
	}
	git := &fakeGitGateway{
		createResult:  CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "failed", Summary: "upstream server_error"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableTransient || !contains(result.Summary, "server_error") {
		t.Fatalf("result = %#v, want retryable failed result with upstream error", result)
	}
	run, err := fixture.repos.Runs.GetByID(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("Runs.GetByID() error = %v", err)
	}
	checkpoint := parseCheckpoint(run.CheckpointJSON)
	if checkpoint.ResumePolicy != "retry_from_timeout_context" {
		t.Fatalf("checkpoint.ResumePolicy = %q, want retry_from_timeout_context", checkpoint.ResumePolicy)
	}
	if checkpoint.Repair == nil || checkpoint.Repair.Status != "failed" || checkpoint.Repair.Summary != "upstream server_error" {
		t.Fatalf("checkpoint.Repair = %#v, want persisted repair checkpoint", checkpoint.Repair)
	}
}

func TestProcessClaimedItemTreatsAgentSetupFailureAsRetryableTransient(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "head-1"}},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
		},
	}
	git := &fakeGitGateway{createResult: CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"}, prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true}}
	errorMessage := "The 'gpt-5.5' model requires a newer version of Codex. Please upgrade to the latest app or CLI and try again."
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "failed", Stderr: errorMessage}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureRetryableTransient || !contains(result.Summary, "requires a newer version") {
		t.Fatalf("result = %#v, want retryable_transient with real agent error", result)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), claim.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "queued" || queue.LastErrorKind == nil || *queue.LastErrorKind != string(FailureRetryableTransient) {
		t.Fatalf("queue = %#v, want queued retryable_transient failure", queue)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.Status != "queued" || loop.NextRunAt == nil {
		t.Fatalf("loop = %#v, want queued loop awaiting retry", loop)
	}
}

func TestRunRepairStepFailsResumedCompletedCheckpointWithoutParsedResult(t *testing.T) {
	t.Parallel()

	runner := New(Options{})
	checkpoint, err := runner.runRepairStep(context.Background(), stepInput{
		Checkpoint: fixerCheckpoint{
			Repair: &checkpointRepair{
				Summary:     "upstream server_error",
				ParseStatus: "",
			},
		},
	})
	if err == nil {
		t.Fatalf("runRepairStep() error = nil, want parse-status failure")
	}
	if checkpoint.Repair == nil {
		t.Fatal("checkpoint.Repair = nil, want checkpoint preserved")
	}
	var loopErr *loopError
	if !errors.As(err, &loopErr) {
		t.Fatalf("error = %T, want *loopError", err)
	}
	if loopErr.kind != FailureRetryableTransient {
		t.Fatalf("loopErr.kind = %v, want %v", loopErr.kind, FailureRetryableTransient)
	}
	if !contains(err.Error(), "server_error") {
		t.Fatalf("error = %q, want upstream summary", err.Error())
	}
}

func TestCreateRunContextRewindsToPrepareWhenPostRepairResumeCheckpointParseStatusIsInvalid(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := "pr:acme/looper:42"
	nowISO := fixture.nowISO()
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{
		ID:         "loop_fixer_rewind_invalid_repair",
		Seq:        1,
		ProjectID:  "project_1",
		Type:       "fixer",
		TargetType: "pull_request",
		TargetID:   &loopTarget,
		Repo:       &repo,
		PRNumber:   &prNumber,
		Status:     "queued",
		CreatedAt:  nowISO,
		UpdatedAt:  nowISO,
	}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{
		ClaimedLockKey: "pr:acme/looper:42",
		FixItems:       []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}},
		Worktree:       &checkpointWorktree{Path: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "head-1", BaseHeadSHA: "base-1", PreparedAt: nowISO},
		Lifecycle:      &lifecycle.State{Branch: "feature/fix-42", BaseBranch: "main", CommitSHAs: []string{"commit-1"}, Pushed: true, PRNumber: 42, PRURL: "https://example/pr/42", Actions: lifecycle.Actions{Commit: lifecycle.ActionSourceAgent, Push: lifecycle.ActionSourceAgent, PR: lifecycle.ActionSourceFallback}},
		Repair:         &checkpointRepair{Summary: "upstream server_error", ParseStatus: "", CompletedAt: nowISO},
		Validation:     &ValidationResult{Passed: true, Summary: "stale"},
		Push:           &checkpointPush{Pushed: true, Branch: "feature/fix-42", Remote: "origin", PushedAt: nowISO},
		ResolvedComments: &checkpointResolvedComments{
			Items: []checkpointResolvedComment{{FixItemID: "c1", ThreadID: "t1", Status: "resolved", UpdatedAt: nowISO}},
		},
		Recheck: &checkpointRecheck{RemainingFixItems: []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}}},
	})
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{
		ID:                "run_failed_after_recheck",
		LoopID:            "loop_fixer_rewind_invalid_repair",
		Status:            "failed",
		CurrentStep:       stringPtr(string(stepRecheck)),
		LastCompletedStep: stringPtr(string(stepResolveComments)),
		CheckpointJSON:    &checkpointJSON,
		StartedAt:         nowISO,
		CreatedAt:         nowISO,
		UpdatedAt:         nowISO,
	}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), "loop_fixer_rewind_invalid_repair")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil {
		t.Fatal("loop = nil, want fixer loop")
	}

	resumed, err := runner.createRunContext(context.Background(), *loop)
	if err != nil {
		t.Fatalf("createRunContext() error = %v", err)
	}
	if !resumed.Resumed || resumed.StartStep != stepPrepareWorktree {
		t.Fatalf("resumed = %#v, want prepare-worktree rewind", resumed)
	}
	if resumed.Checkpoint.Repair != nil {
		t.Fatalf("Repair = %#v, want cleared repair checkpoint", resumed.Checkpoint.Repair)
	}
	if resumed.Checkpoint.Validation != nil || resumed.Checkpoint.Push != nil || resumed.Checkpoint.ResolvedComments != nil || resumed.Checkpoint.Recheck != nil {
		t.Fatalf("checkpoint = %#v, want post-repair checkpoints cleared", resumed.Checkpoint)
	}
	if resumed.Checkpoint.Worktree == nil || resumed.Checkpoint.Worktree.PreparedAt != "" {
		t.Fatalf("Worktree = %#v, want worktree retained but marked for reprepare", resumed.Checkpoint.Worktree)
	}
	if resumed.Checkpoint.Lifecycle == nil || len(resumed.Checkpoint.Lifecycle.CommitSHAs) != 1 || !resumed.Checkpoint.Lifecycle.Pushed || resumed.Checkpoint.Lifecycle.PRNumber != 42 || resumed.Checkpoint.Lifecycle.Actions.PR != lifecycle.ActionSourceFallback {
		t.Fatalf("Lifecycle = %#v, want lifecycle metadata preserved across prepare rewind", resumed.Checkpoint.Lifecycle)
	}
	if resumed.Run.LastCompletedStep == nil || *resumed.Run.LastCompletedStep != string(stepCollectFixes) {
		t.Fatalf("run.LastCompletedStep = %#v, want collect-fixes", resumed.Run.LastCompletedStep)
	}
}

func TestCreateRunContextRestartsFromDiscoverForRediscoverableCheckpoint(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := "pr:acme/looper:42"
	nowISO := fixture.nowISO()
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{
		ID:         "loop_fixer_manual_restart",
		Seq:        1,
		ProjectID:  "project_1",
		Type:       "fixer",
		TargetType: "pull_request",
		TargetID:   &loopTarget,
		Repo:       &repo,
		PRNumber:   &prNumber,
		Status:     "running",
		CreatedAt:  nowISO,
		UpdatedAt:  nowISO,
	}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{
		ResumePolicy: loops.ResumePolicyManualIntervention,
		Pause:        newCheckpointPause(checkpointPauseReasonNoopResolveNoNewCommits, true, "head-1", hashFixItemsState([]FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}}), []string{"t1"}),
		Detail: &checkpointDetail{
			State:       "OPEN",
			HeadSHA:     "head-1",
			HeadRefName: "feature/fix-42",
			BaseRefName: "main",
			Comments:    []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}},
		},
		FixItems:   []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}},
		Worktree:   &checkpointWorktree{Path: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "head-1", BaseHeadSHA: "head-1", PreparedAt: nowISO},
		Repair:     &checkpointRepair{Status: "completed", Summary: "nothing to change", CompletedAt: nowISO},
		Validation: &ValidationResult{Passed: true, Summary: "passed"},
		Push:       &checkpointPush{Pushed: false, Branch: "feature/fix-42", Remote: "origin", SkippedReason: "No new commits to push"},
		Recheck:    &checkpointRecheck{RemainingFixItems: []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}}},
	})
	manualReason := "manual hold captured by structured pause"
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{
		ID:                "run_manual_intervention",
		LoopID:            "loop_fixer_manual_restart",
		Status:            "failed",
		CurrentStep:       stringPtr(string(stepRecheck)),
		LastCompletedStep: stringPtr(string(stepResolveComments)),
		CheckpointJSON:    &checkpointJSON,
		Summary:           &manualReason,
		ErrorMessage:      &manualReason,
		StartedAt:         nowISO,
		CreatedAt:         nowISO,
		UpdatedAt:         nowISO,
	}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})
	loop, err := fixture.repos.Loops.GetByID(context.Background(), "loop_fixer_manual_restart")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil {
		t.Fatal("loop = nil, want fixer loop")
	}

	resumed, err := runner.createRunContext(context.Background(), *loop)
	if err != nil {
		t.Fatalf("createRunContext() error = %v", err)
	}
	if resumed.Resumed || resumed.StartStep != stepDiscoverPR {
		t.Fatalf("resumed = %#v, want fresh discover run", resumed)
	}
	if resumed.Checkpoint.Detail != nil || len(resumed.Checkpoint.FixItems) != 0 || resumed.Checkpoint.Worktree != nil || resumed.Checkpoint.Push != nil {
		t.Fatalf("checkpoint = %#v, want cleared manual-intervention checkpoint", resumed.Checkpoint)
	}
	if resumed.Checkpoint.ResumePolicy != "replay_step" {
		t.Fatalf("ResumePolicy = %q, want replay_step", resumed.Checkpoint.ResumePolicy)
	}
}

func TestProcessClaimedQueueItemResumeValidationFailureUpdatesLoopState(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	git := &fakeGitGateway{}
	repo := "acme/looper"
	prNumber := int64(42)
	loopTarget := "pr:acme/looper:42"
	nowISO := fixture.nowISO()
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{
		ID:         "loop_fixer_resume_parse_status",
		Seq:        1,
		ProjectID:  "project_1",
		Type:       "fixer",
		TargetType: "pull_request",
		TargetID:   &loopTarget,
		Repo:       &repo,
		PRNumber:   &prNumber,
		Status:     "queued",
		CreatedAt:  nowISO,
		UpdatedAt:  nowISO,
	}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Git: git, Logger: fixture.logger, Now: fixture.now})
	checkpointJSON := mustMarshalJSON(fixerCheckpoint{
		Worktree: &checkpointWorktree{
			Path:   filepath.Join(t.TempDir(), "wt-42"),
			Branch: "feature/fix-42",
		},
		Repair: &checkpointRepair{
			Summary:     "upstream server_error",
			ParseStatus: "",
		},
	})
	if err := fixture.repos.Runs.Upsert(context.Background(), storage.RunRecord{
		ID:                "run_failed_after_repair",
		LoopID:            "loop_fixer_resume_parse_status",
		Status:            "failed",
		LastCompletedStep: stringPtr(string(stepRepair)),
		CheckpointJSON:    &checkpointJSON,
		StartedAt:         nowISO,
		CreatedAt:         nowISO,
		UpdatedAt:         nowISO,
	}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	projectID := "project_1"
	loopID := "loop_fixer_resume_parse_status"
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{
		ID:          "queue_fixer_resume_parse_status",
		ProjectID:   &projectID,
		LoopID:      &loopID,
		Type:        "fixer",
		TargetType:  "pull_request",
		TargetID:    loopTarget,
		Repo:        &repo,
		PRNumber:    &prNumber,
		DedupeKey:   "fixer:acme/looper:42:resume-parse",
		Priority:    1,
		Status:      "queued",
		AvailableAt: nowISO,
		MaxAttempts: 1,
		CreatedAt:   nowISO,
		UpdatedAt:   nowISO,
	}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}

	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil {
		t.Fatalf("ClaimNextOfType() error = %v", err)
	}
	if claim == nil {
		t.Fatal("claim = nil, want claimed queue item")
	}

	result, err := runner.ProcessClaimedQueueItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedQueueItem() error = %v", err)
	}
	if result == nil || result.Status != "failed" || result.FailureKind != FailureRetryableTransient {
		t.Fatalf("result = %#v, want failed retryable_transient result", result)
	}

	queue, err := fixture.repos.Queue.GetByID(context.Background(), "queue_fixer_resume_parse_status")
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "manual_intervention" || queue.LastErrorKind == nil || *queue.LastErrorKind != string(FailureRetryableTransient) || queue.FinishedAt == nil {
		t.Fatalf("queue = %#v, want manual_intervention retryable_transient queue item after max attempts", queue)
	}

	loop, err := fixture.repos.Loops.GetByID(context.Background(), "loop_fixer_resume_parse_status")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.Status != "paused" || loop.NextRunAt != nil {
		t.Fatalf("loop = %#v, want paused loop after max attempts", loop)
	}
	if len(git.cleanupCalls) != 0 {
		t.Fatalf("len(git.cleanupCalls) = %d, want no cleanup for manual intervention", len(git.cleanupCalls))
	}
}

func TestProcessClaimedItemRestartsFromDiscoverAfterRemoteHeadChangeAtPush(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "head-1"}},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-2", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-2", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-2", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-2", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-2", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-2", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head-2", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1"},
			{Number: 42, State: "OPEN", HeadSHA: "new-head-2", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1"},
			{Number: 42, State: "OPEN", HeadSHA: "new-head-2", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1"},
		},
	}
	git := &fakeGitGateway{
		createResult:  CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
		inspectResults: []InspectHeadResult{
			{HeadSHA: "base-head"}, {HeadSHA: "new-head-1", NewCommitSHAs: []string{"new-head-1"}}, {HeadSHA: "new-head-1"},
			{HeadSHA: "base-head"}, {HeadSHA: "new-head-2", NewCommitSHAs: []string{"new-head-2"}}, {HeadSHA: "new-head-2"},
		},
		pushErrors: []error{fmt.Errorf("remote head changed while pushing")},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "first repair", ParseStatus: "parsed"}, {Status: "completed", Summary: "second repair", ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now, Sleep: func(time.Duration) {}})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim1, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim1 == nil {
		t.Fatalf("first ClaimNextOfType() = (%#v, %v), want claimed item", claim1, err)
	}
	first, err := runner.ProcessClaimedItem(context.Background(), *claim1)
	if err != nil {
		t.Fatalf("ProcessClaimedItem(first) error = %v", err)
	}
	if first.Status != "failed" || first.FailureKind != FailureRetryableAfterResume || !contains(first.Summary, "remote head changed") {
		t.Fatalf("first result = %#v, want retryable remote-head failure", first)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) after first run = %d, want 1", len(agent.starts))
	}

	afterFailure, err := fixture.repos.Loops.GetByID(context.Background(), first.LoopID)
	if err != nil || afterFailure == nil || loops.FixerPushCount(afterFailure.MetadataJSON) != 0 || loops.IsReviewFixBudgetHold(*afterFailure) {
		t.Fatalf("after failed push = (%#v, %v), want zero budget consumed and no budget hold", afterFailure, err)
	}

	fixture.advance(5 * time.Second)
	claim2, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim2 == nil {
		t.Fatalf("retry ClaimNextOfType() = (%#v, %v), want claimed item", claim2, err)
	}
	second, err := runner.ProcessClaimedItem(context.Background(), *claim2)
	if err != nil {
		t.Fatalf("ProcessClaimedItem(retry) error = %v", err)
	}
	if second.Status != "success" {
		t.Fatalf("second result = %#v, want success", second)
	}
	afterSuccess, err := fixture.repos.Loops.GetByID(context.Background(), second.LoopID)
	if err != nil || afterSuccess == nil || loops.FixerPushCount(afterSuccess.MetadataJSON) != 1 {
		t.Fatalf("after successful push = (%#v, %v), want exactly one budget unit consumed", afterSuccess, err)
	}
	if len(agent.starts) != 2 {
		t.Fatalf("len(agent.starts) = %d, want 2 (restart from discover)", len(agent.starts))
	}
	if len(git.createCalls) != 2 {
		t.Fatalf("len(git.createCalls) = %d, want 2 (worktree rebuilt)", len(git.createCalls))
	}
}

func TestProcessClaimedItemResumeReacquiresPullRequestLock(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	fixture.repos.Locks.SetNow(fixture.now)
	github := &fakeGitHubGateway{
		listOpen:      []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "head-1"}},
		viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}},
	}
	stdout := fmt.Sprintf(`__LOOPER_RESULT__={"summary":"applied fixes","review_thread_replies":[{"fixItemId":"c1","threadId":"t1","explanation":"Adjusted the off-by-one handling and verified the fix.","threadCommentsObserved":"%s"}]}`+"\n", hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}}))
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "applied fixes", ParseStatus: "parsed", Stdout: stdout}}}
	validationCalls := 0
	git := &fakeGitGateway{createResult: CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"}, prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true}, inspectResults: []InspectHeadResult{{HeadSHA: "base-head"}, {HeadSHA: "new-head", NewCommitSHAs: []string{"new-head"}}, {HeadSHA: "new-head"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now, AllowAutoCommit: true, AllowRiskyFixes: true, ValidationRunner: func(context.Context, ValidationInput) (ValidationResult, error) {
		validationCalls++
		if validationCalls == 1 {
			return ValidationResult{Passed: false, Summary: "Validation failed"}, nil
		}
		return ValidationResult{Passed: true, Summary: "ok"}, nil
	}})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim1, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim1 == nil {
		t.Fatalf("first ClaimNextOfType() = (%#v, %v), want claimed item", claim1, err)
	}
	first, err := runner.ProcessClaimedItem(context.Background(), *claim1)
	if err != nil {
		t.Fatalf("ProcessClaimedItem(first) error = %v", err)
	}
	if first.Status != "failed" || first.FailureKind != FailureRetryableAfterResume {
		t.Fatalf("first = %#v, want retryable-after-resume validation failure", first)
	}
	fixture.advance(5 * time.Second)
	claim2, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim2 == nil {
		t.Fatalf("retry ClaimNextOfType() = (%#v, %v), want claimed item", claim2, err)
	}
	lockKey := buildPullRequestLockKey(*claim2)
	if acquired, err := fixture.repos.Locks.Acquire(context.Background(), storage.LockRecord{Key: lockKey, Owner: "other-fixer", ExpiresAt: fixture.now().Add(time.Minute).UTC().Format("2006-01-02T15:04:05.000Z"), CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Locks.Acquire() error = %v", err)
	} else if !acquired {
		t.Fatal("Locks.Acquire() = false, want competing lock holder")
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claim2)
	if err == nil || !contains(err.Error(), lockKey) {
		t.Fatalf("ProcessClaimedItem(retry) error = %v, want lock reacquire failure", err)
	}
	if result != (ProcessResult{}) {
		t.Fatalf("result = %#v, want zero result on resume lock failure", result)
	}
	if validationCalls != 1 {
		t.Fatalf("validationCalls = %d, want 1 (resume should stop before validate reruns)", validationCalls)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), claim2.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "running" {
		t.Fatalf("queue = %#v, want still-running claimed item after setup failure", queue)
	}
}

func TestProcessNextSetupFailureMarksQueueFailed(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	projectID := "project_1"
	prNumber := int64(99)
	nowISO := fixture.nowISO()
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_missing_fixer", ProjectID: &projectID, Type: "fixer", TargetType: "pull_request", TargetID: "pr:acme/looper:99", Repo: stringPtr("acme/looper"), PRNumber: &prNumber, DedupeKey: "fixer:acme/looper:99:test", Priority: 1, Status: "queued", AvailableAt: nowISO, MaxAttempts: 3, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.ProcessNext(context.Background(), "fixer-worker-1")
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if result == nil || result.Status != "failed" || result.FailureKind != FailureNonRetryable || !contains(result.Summary, "requires loopId") {
		t.Fatalf("result = %#v, want non-retryable missing-loopId failure", result)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), "queue_missing_fixer")
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "queued" || queue.LastErrorKind == nil || *queue.LastErrorKind != string(FailureNonRetryable) || queue.FinishedAt != nil {
		t.Fatalf("queue = %#v, want requeued non_retryable item", queue)
	}
}

func TestRecoverClaimedItemReconcilesRunningLoopState(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	nowISO := fixture.nowISO()
	prNumber := int64(42)
	loopTarget := "pr:acme/looper:42"
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_fixer_running", Seq: 2, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: stringPtr("acme/looper"), PRNumber: &prNumber, Status: "running", CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	projectID := "project_1"
	loopID := "loop_fixer_running"
	if err := fixture.repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_fixer_running", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: loopTarget, Repo: stringPtr("acme/looper"), PRNumber: &prNumber, DedupeKey: "fixer:acme/looper:42:test", Priority: 1, Status: "running", AvailableAt: nowISO, Attempts: 0, MaxAttempts: 1, CreatedAt: nowISO, UpdatedAt: nowISO}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: &fakeGitHubGateway{}, Git: &fakeGitGateway{}, AgentExecutor: &fakeAgentExecutor{}, Logger: fixture.logger, Now: fixture.now})

	result, err := runner.recoverClaimedItem(context.Background(), storage.QueueItemRecord{ID: "queue_fixer_running", ProjectID: &projectID, LoopID: &loopID, Type: "fixer", TargetType: "pull_request", TargetID: loopTarget, Repo: stringPtr("acme/looper"), PRNumber: &prNumber, DedupeKey: "fixer:acme/looper:42:test", Priority: 1, Status: "running", AvailableAt: nowISO, Attempts: 0, MaxAttempts: 1, CreatedAt: nowISO, UpdatedAt: nowISO}, fmt.Errorf("persist step failed"))
	if err != nil {
		t.Fatalf("recoverClaimedItem() error = %v", err)
	}
	if result == nil || result.Status != "failed" || result.FailureKind != FailureNonRetryable {
		t.Fatalf("result = %#v, want failed non-retryable recovery", result)
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), "queue_fixer_running")
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "manual_intervention" || queue.LastErrorKind == nil || *queue.LastErrorKind != string(FailureNonRetryable) || queue.FinishedAt == nil {
		t.Fatalf("queue = %#v, want parked non_retryable recovery item", queue)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), loopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.Status != "paused" || loop.NextRunAt != nil {
		t.Fatalf("loop = %#v, want paused parked loop", loop)
	}
}

func TestProcessClaimedItemAutoPushDisabledSkipsManualIntervention(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "head-1"}},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
		},
	}
	git := &fakeGitGateway{
		createResult:  CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
		inspectResults: []InspectHeadResult{
			{HeadSHA: "base-head"},
			{HeadSHA: "new-head", NewCommitSHAs: []string{"new-head"}},
			{HeadSHA: "new-head"},
		},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "applied fixes", ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: false, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}

	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "failed" || result.FailureKind != FailureManualIntervention || !contains(result.Summary, "Auto push disabled") {
		t.Fatalf("result = %#v, want manual-intervention auto-push summary", result)
	}
	if len(git.pushCalls) != 0 {
		t.Fatalf("len(git.pushCalls) = %d, want 0", len(git.pushCalls))
	}
	queue, err := fixture.repos.Queue.GetByID(context.Background(), claim.ID)
	if err != nil {
		t.Fatalf("Queue.GetByID() error = %v", err)
	}
	if queue == nil || queue.Status != "manual_intervention" || queue.LastErrorKind == nil || *queue.LastErrorKind != string(FailureManualIntervention) || queue.FinishedAt == nil {
		t.Fatalf("queue = %#v, want parked manual_intervention item", queue)
	}
	run, err := fixture.repos.Runs.GetByID(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("Runs.GetByID() error = %v", err)
	}
	if run == nil || run.Status != "failed" || run.CurrentStep == nil || *run.CurrentStep != string(stepPush) {
		t.Fatalf("run = %#v, want failed push step run", run)
	}
	checkpoint := parseCheckpoint(run.CheckpointJSON)
	if checkpoint.ResumePolicy != "manual_intervention" {
		t.Fatalf("checkpoint.ResumePolicy = %q, want manual_intervention", checkpoint.ResumePolicy)
	}
	if checkpoint.Push == nil || checkpoint.Push.SkippedReason != "Auto push disabled" {
		t.Fatalf("checkpoint.Push = %#v, want recorded auto-push-disabled checkpoint", checkpoint.Push)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), result.LoopID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.Status != "paused" || loop.NextRunAt != nil {
		t.Fatalf("loop = %#v, want paused parked loop", loop)
	}
}

func TestRunPrepareWorktreeStepPausesDirtyWorktree(t *testing.T) {
	t.Parallel()

	// Cross-head dirty without fixer provenance → MI (no InspectHead; ownership gate first).
	// Local mismatch with provenance is covered by DirtyAdoptEarlyExitGates.
	git := &fakeGitGateway{
		createResult:   CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult:  PrepareWorktreeResult{HeadSHA: "base-head", Clean: false},
		inspectResults: []InspectHeadResult{{HeadSHA: "other-head", HasUncommittedChanges: true}},
	}
	runner := New(Options{Git: git})
	checkpoint, err := runner.runPrepareWorktreeStep(context.Background(), stepInput{
		Project:    storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Repo:       "acme/looper",
		PRNumber:   42,
		Checkpoint: fixerCheckpoint{Detail: &checkpointDetail{HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main"}},
	})
	assertPrepareDirtyManualIntervention(t, checkpoint, err)
	if len(git.inspectCalls) != 0 {
		t.Fatalf("len(git.inspectCalls) = %d, want 0 (no provenance)", len(git.inspectCalls))
	}
}

func TestRunRepairStepRequiresManualInterventionForRiskyConflictWhenDisabled(t *testing.T) {
	t.Parallel()

	runner := New(Options{AllowRiskyFixes: false})
	checkpoint, err := runner.runRepairStep(context.Background(), stepInput{
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: fixerCheckpoint{
			FixItems: []FixItem{{Type: "conflict", Summary: "merge conflict"}},
		},
	})
	if err == nil {
		t.Fatal("runRepairStep() error = nil, want manual intervention")
	}
	var loopErr *loopError
	if !errors.As(err, &loopErr) {
		t.Fatalf("error = %T, want *loopError", err)
	}
	if loopErr.kind != FailureManualIntervention {
		t.Fatalf("loopErr.kind = %v, want %v", loopErr.kind, FailureManualIntervention)
	}
	if checkpoint.ResumePolicy != "manual_intervention" {
		t.Fatalf("checkpoint.ResumePolicy = %q, want manual_intervention", checkpoint.ResumePolicy)
	}
	if !contains(loopErr.Error(), "risky conflict fixes require manual intervention") {
		t.Fatalf("error = %q, want risky conflict summary", loopErr.Error())
	}
}

func TestRunRepairStepSkipsWhenFixerHoldAppliedBeforeAgentStart(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repoPath := t.TempDir()
	worktreeRoot := filepath.Join(t.TempDir(), "worktrees")
	worktreePath := filepath.Join(worktreeRoot, "fix-42")
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	metadata := fmt.Sprintf(`{"worktreeRoot":%q}`, worktreeRoot)
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", Labels: []string{domain.HoldLabelFixer}}}}
	agent := &fakeAgentExecutor{}
	git := &fakeGitGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})

	_, err := runner.runRepairStep(context.Background(), stepInput{
		Project:  storage.ProjectRecord{ID: "project_1", RepoPath: repoPath, MetadataJSON: &metadata},
		Loop:     storage.LoopRecord{ID: "loop_fixer_hold_before_agent", Type: "fixer", TargetType: "pull_request"},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: fixerCheckpoint{
			Detail:       &checkpointDetail{HeadRefName: "feature/fix-42", BaseRefName: "main", HeadSHA: "head-1"},
			FixItems:     []FixItem{{ID: "fix-1", Type: "conflict", Summary: "merge conflict"}},
			Worktree:     &checkpointWorktree{Path: worktreePath, Branch: "feature/fix-42", HeadSHA: "head-1"},
			FixItemsHash: "hash-1",
		},
	})
	var holdErr *holdSkipError
	if !errors.As(err, &holdErr) {
		t.Fatalf("runRepairStep() error = %v, want hold skip", err)
	}
	if len(git.mergeBaseCalls) != 0 {
		t.Fatalf("len(git.mergeBaseCalls) = %d, want no conflict merge", len(git.mergeBaseCalls))
	}
	if len(agent.starts) != 0 {
		t.Fatalf("len(agent.starts) = %d, want no agent start", len(agent.starts))
	}
}

func TestRunRepairStepRecreatesCheckpointOutsideWorktreeRootAndRunsAgent(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repoPath := t.TempDir()
	worktreeRoot := filepath.Join(t.TempDir(), "worktrees")
	legacyPath := filepath.Join(t.TempDir(), "legacy-wt")
	metadata := fmt.Sprintf(`{"worktreeRoot":%q}`, worktreeRoot)
	git := &fakeGitGateway{createResult: CreateWorktreeResult{WorktreePath: filepath.Join(worktreeRoot, "wt"), Branch: "feature/fix-42", HeadSHA: "base-head"}, prepareResult: PrepareWorktreeResult{HeadSHA: "head-1", Clean: true}}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "applied fixes", ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Git: git, AgentExecutor: agent, Logger: fixture.logger, Now: fixture.now})

	checkpoint, err := runner.runRepairStep(context.Background(), stepInput{
		Project:  storage.ProjectRecord{ID: "project_1", RepoPath: repoPath, MetadataJSON: &metadata},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: fixerCheckpoint{
			Detail:       &checkpointDetail{HeadRefName: "feature/fix-42", BaseRefName: "main", HeadSHA: "head-1"},
			FixItems:     []FixItem{{ID: "fix-1", Summary: "repair disclosure"}},
			Worktree:     &checkpointWorktree{Path: legacyPath, Branch: "feature/fix-42", PreparedAt: "stale"},
			FixItemsHash: "hash-1",
		},
	})
	if err != nil {
		t.Fatalf("runRepairStep() error = %v", err)
	}
	if len(git.createCalls) != 1 {
		t.Fatalf("len(git.createCalls) = %d, want 1", len(git.createCalls))
	}
	if len(git.prepareCalls) != 1 {
		t.Fatalf("len(git.prepareCalls) = %d, want 1", len(git.prepareCalls))
	}
	if checkpoint.Worktree == nil || checkpoint.Worktree.Path != git.createResult.WorktreePath {
		t.Fatalf("checkpoint.Worktree = %#v, want recreated worktree", checkpoint.Worktree)
	}
	if git.createCalls[0].WorktreeRoot != worktreeRoot {
		t.Fatalf("CreateWorktree().WorktreeRoot = %q, want %q", git.createCalls[0].WorktreeRoot, worktreeRoot)
	}
	if len(agent.starts) != 1 {
		t.Fatalf("len(agent.starts) = %d, want 1", len(agent.starts))
	}
	if agent.starts[0].WorkingDirectory != git.createResult.WorktreePath {
		t.Fatalf("agent WorkingDirectory = %q, want rebuilt worktree %q", agent.starts[0].WorkingDirectory, git.createResult.WorktreePath)
	}
	if checkpoint.Repair == nil || checkpoint.Repair.Summary != "applied fixes" {
		t.Fatalf("checkpoint.Repair = %#v, want completed repair after worktree recovery", checkpoint.Repair)
	}
}

func TestReconcileCommitsRequiresManualInterventionWhenAutoCommitDisabledAndDirty(t *testing.T) {
	t.Parallel()

	runner := New(Options{Git: &fakeGitGateway{inspectResults: []InspectHeadResult{{HeadSHA: "head-1", HasUncommittedChanges: true, ChangedFiles: []string{"file.txt"}}}}, AllowAutoCommit: false})
	project := storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()}
	checkpoint, err := runner.reconcileCommits(context.Background(), project, fixerCheckpoint{Worktree: &checkpointWorktree{Path: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "head-1", BaseHeadSHA: "head-1"}}, "fix: test", storage.RunRecord{})
	if err == nil {
		t.Fatal("reconcileCommits() error = nil, want manual intervention")
	}
	var loopErr *loopError
	if !errors.As(err, &loopErr) {
		t.Fatalf("error = %T, want *loopError", err)
	}
	if loopErr.kind != FailureManualIntervention {
		t.Fatalf("loopErr.kind = %v, want %v", loopErr.kind, FailureManualIntervention)
	}
	if checkpoint.ResumePolicy != "manual_intervention" {
		t.Fatalf("checkpoint.ResumePolicy = %q, want manual_intervention", checkpoint.ResumePolicy)
	}
}

func TestProcessClaimedItemUsesDefaultProjectWorktreeRootWhenProjectMetadataOmitsIt(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	project, err := fixture.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil {
		t.Fatalf("Projects.GetByID() error = %v", err)
	}
	if project == nil {
		t.Fatal("project missing")
	}
	project.MetadataJSON = nil
	project.UpdatedAt = fixture.nowISO()
	if err := fixture.repos.Projects.Upsert(context.Background(), *project); err != nil {
		t.Fatalf("Projects.Upsert() error = %v", err)
	}

	github := &fakeGitHubGateway{
		listOpen:      []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "head-1"}},
		viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}}},
	}
	git := &fakeGitGateway{createResult: CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"}, prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "applied fixes", ParseStatus: "parsed"}}}, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: false, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}
	if _, err := runner.ProcessClaimedItem(context.Background(), *claim); err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	wantRoot, err := config.DefaultProjectWorktreeRoot(project.ID, project.RepoPath)
	if err != nil {
		t.Fatalf("DefaultProjectWorktreeRoot() error = %v", err)
	}
	if len(git.createCalls) == 0 || git.createCalls[0].WorktreeRoot != wantRoot {
		t.Fatalf("CreateWorktree().WorktreeRoot = %#v, want %q", git.createCalls, wantRoot)
	}
}

func TestRunValidationUsesShellCommandsByDefault(t *testing.T) {
	t.Parallel()
	runner := New(Options{})
	result, err := runner.runValidation(context.Background(), ValidationInput{
		CWD:      t.TempDir(),
		Commands: []string{"printf 'hello'", "printf 'warn' >&2"},
	})
	if err != nil {
		t.Fatalf("runValidation() error = %v", err)
	}
	if !result.Passed {
		t.Fatalf("result = %#v, want passed", result)
	}
	if result.Summary != "Validation passed" {
		t.Fatalf("Summary = %q, want Validation passed", result.Summary)
	}
	if result.Output != "hello\nwarn" {
		t.Fatalf("Output = %q, want joined shell output", result.Output)
	}
}

func TestRunValidationReturnsCommandFailureOutput(t *testing.T) {
	t.Parallel()
	runner := New(Options{})
	result, err := runner.runValidation(context.Background(), ValidationInput{
		CWD:      t.TempDir(),
		Commands: []string{"printf 'bad' >&2; exit 9"},
	})
	if err != nil {
		t.Fatalf("runValidation() error = %v", err)
	}
	if result.Passed {
		t.Fatalf("result = %#v, want failed validation", result)
	}
	if result.Summary != "Validation failed: printf 'bad' >&2; exit 9" {
		t.Fatalf("Summary = %q, want command-specific failure", result.Summary)
	}
	if result.Output != "bad" {
		t.Fatalf("Output = %q, want stderr output", result.Output)
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
	coordinator, err := storage.OpenSQLiteCoordinator(context.Background(), filepath.Join(t.TempDir(), "fixer.sqlite"), storage.SQLiteCoordinatorOptions{BackupDir: t.TempDir()})
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

func (f *runnerFixture) advance(delta time.Duration) { f.current = f.current.Add(delta) }

func (f *runnerFixture) nowISO() string {
	return fmt.Sprintf("%s.000Z", f.current.UTC().Format("2006-01-02T15:04:05"))
}

type fakeGitHubGateway struct {
	currentUser           string
	currentUserEmpty      bool // when true, GetCurrentUserLogin returns ("", nil) without defaulting
	currentUserErr        error
	authorErr             error
	listOpen              []PullRequestSummary
	listOpenByLabel       map[string][]PullRequestSummary
	listCalls             []ListOpenPullRequestsInput
	viewResponses         []PullRequestDetail
	viewErr               error
	threads               []ReviewThread
	listThreadsErr        error
	listThreadsCalls      []ListReviewThreadsInput
	viewThreadErr         error
	viewThreadCalls       []ViewReviewThreadInput
	viewIndex             int
	resolveCalls          []ResolveReviewThreadInput
	closeAfterResolve     bool
	addLabelCalls         []PullRequestLabelsInput
	removeLabelCalls      []PullRequestLabelsInput
	reviewerRequests      []PullRequestReviewersInput
	reviews               []ReviewSummary
	dismissedReviews      []DismissReviewInput
	replyCalls            []AddReviewThreadReplyInput
	replyErr              error
	resolveErr            error
	createIssueComments   []IssueCommentInput
	updateIssueComments   []UpdateIssueCommentInput
	createIssueCommentErr error
	updateIssueCommentErr error
	nextIssueCommentID    int64
	compareCalls          []CompareCommitsInput
	compareStatus         string
	compareErr            error
	nativeComments        []NativeReviewComment
	nativeCommentBatches  [][]NativeReviewComment
	listNativeContextErr  error
	listNativeErr         error
	listNativeCalls       int
	resolveNativeCalls    []ResolveNativeReviewCommentInput
	resolveNativeErr      error
}

func (f *fakeGitHubGateway) ListOpenPullRequests(_ context.Context, input ListOpenPullRequestsInput) ([]PullRequestSummary, error) {
	f.listCalls = append(f.listCalls, input)
	if f.listOpenByLabel != nil {
		return append([]PullRequestSummary(nil), f.listOpenByLabel[input.Label]...), nil
	}
	result := append([]PullRequestSummary(nil), f.listOpen...)
	if strings.TrimSpace(input.BaseRefName) != "" {
		filtered := result[:0]
		for _, pr := range result {
			if strings.EqualFold(strings.TrimSpace(pr.BaseRefName), strings.TrimSpace(input.BaseRefName)) {
				filtered = append(filtered, pr)
			}
		}
		result = filtered
	}
	for index := range result {
		if result[index].Author == "" {
			result[index].Author = firstNonEmpty(f.currentUser, "looper")
		}
	}
	return result, nil
}

func (f *fakeGitHubGateway) GetCurrentUserLogin(context.Context, string) (string, error) {
	if f.currentUserErr != nil {
		return "", f.currentUserErr
	}
	if f.currentUserEmpty {
		return "", nil
	}
	return firstNonEmpty(f.currentUser, "looper"), nil
}

func (f *fakeGitHubGateway) GetPullRequestAuthor(_ context.Context, input ViewPullRequestInput) (string, error) {
	if f.authorErr != nil {
		return "", f.authorErr
	}
	for _, detail := range f.viewResponses {
		if detail.Number == input.PRNumber && detail.Author != "" {
			return detail.Author, nil
		}
	}
	return firstNonEmpty(f.currentUser, "looper"), nil
}

func (f *fakeGitHubGateway) ViewPullRequest(_ context.Context, input ViewPullRequestInput) (PullRequestDetail, error) {
	if f.viewErr != nil {
		return PullRequestDetail{}, f.viewErr
	}
	if len(f.viewResponses) == 0 {
		return PullRequestDetail{Number: input.PRNumber, State: "OPEN", HeadSHA: "head-default", HeadRefName: "feature/default", BaseRefName: "main", BaseSHA: "base-default"}, nil
	}
	idx := f.viewIndex
	if idx >= len(f.viewResponses) {
		idx = len(f.viewResponses) - 1
	}
	result := f.viewResponses[idx]
	f.viewIndex++
	if f.closeAfterResolve && len(f.resolveCalls) > 0 {
		result.State = "CLOSED"
	}
	if result.Number == 0 {
		result.Number = input.PRNumber
	}
	if result.Author == "" {
		result.Author = firstNonEmpty(f.currentUser, "looper")
	}
	for i := range result.Comments {
		if _, ok := stringFromAny(result.Comments[i]["threadFingerprint"]); ok {
			continue
		}
		threadID, _ := stringFromAny(result.Comments[i]["threadId"])
		commentID, _ := stringFromAny(result.Comments[i]["id"])
		result.Comments[i]["threadFingerprint"] = normalizeThreadFingerprint("", threadID, commentID)
	}
	return result, nil
}

func (f *fakeGitHubGateway) ListReviewThreads(_ context.Context, input ListReviewThreadsInput) ([]ReviewThread, error) {
	f.listThreadsCalls = append(f.listThreadsCalls, input)
	if f.listThreadsErr != nil {
		return nil, f.listThreadsErr
	}
	if f.threads != nil {
		return append([]ReviewThread(nil), f.threads...), nil
	}
	threads := make([]ReviewThread, 0)
	if f.viewIndex == 0 || len(f.viewResponses) == 0 {
		return threads, nil
	}
	idx := f.viewIndex - 1
	if idx >= len(f.viewResponses) {
		idx = len(f.viewResponses) - 1
	}
	for _, comment := range f.viewResponses[idx].Comments {
		threadID, _ := stringFromAny(comment["threadId"])
		commentID, _ := stringFromAny(comment["id"])
		body, _ := stringFromAny(comment["body"])
		if threadID == "" {
			threadID = commentID
		}
		threads = append(threads, ReviewThread{ID: threadID, Comments: []ReviewThreadComment{{ID: commentID, Body: body}}})
	}
	return threads, nil
}

func (f *fakeGitHubGateway) ViewReviewThread(_ context.Context, input ViewReviewThreadInput) (ReviewThread, error) {
	f.viewThreadCalls = append(f.viewThreadCalls, input)
	if f.viewThreadErr != nil {
		return ReviewThread{}, f.viewThreadErr
	}
	threads, err := f.ListReviewThreads(context.Background(), ListReviewThreadsInput{})
	if err != nil {
		return ReviewThread{}, err
	}
	for _, thread := range threads {
		if thread.ID == input.ThreadID {
			return thread, nil
		}
	}
	return ReviewThread{ID: input.ThreadID}, nil
}

func (f *fakeGitHubGateway) ResolveReviewThread(_ context.Context, input ResolveReviewThreadInput) error {
	f.resolveCalls = append(f.resolveCalls, input)
	return f.resolveErr
}

func (f *fakeGitHubGateway) AddReviewThreadReply(_ context.Context, input AddReviewThreadReplyInput) error {
	f.replyCalls = append(f.replyCalls, input)
	for i := range f.threads {
		if f.threads[i].ID == input.ThreadID {
			f.threads[i].Comments = append(f.threads[i].Comments, ReviewThreadComment{
				ID:     fmt.Sprintf("reply-%d", len(f.replyCalls)),
				Author: firstNonEmpty(f.currentUser, "looper"),
				Body:   input.Body,
			})
			return f.replyErr
		}
	}
	return f.replyErr
}

func (f *fakeGitHubGateway) ListNativeReviewComments(ctx context.Context, _ ListNativeReviewCommentsInput) ([]NativeReviewComment, error) {
	f.listNativeCalls++
	f.listNativeContextErr = ctx.Err()
	if f.listNativeErr != nil {
		return nil, f.listNativeErr
	}
	if len(f.nativeCommentBatches) > 0 {
		batch := append([]NativeReviewComment(nil), f.nativeCommentBatches[0]...)
		if len(f.nativeCommentBatches) > 1 {
			f.nativeCommentBatches = f.nativeCommentBatches[1:]
		}
		return batch, nil
	}
	return append([]NativeReviewComment(nil), f.nativeComments...), nil
}

func (f *fakeGitHubGateway) ProbeNativeReviewCommentResolution(context.Context, ListNativeReviewCommentsInput) (forge.ProbeState, error) {
	return forge.ProbeStateSupported, nil
}

func (f *fakeGitHubGateway) ResolveNativeReviewComment(_ context.Context, input ResolveNativeReviewCommentInput) error {
	f.resolveNativeCalls = append(f.resolveNativeCalls, input)
	return f.resolveNativeErr
}

func (f *fakeGitHubGateway) CreateIssueComment(_ context.Context, input IssueCommentInput) (IssueCommentResult, error) {
	f.createIssueComments = append(f.createIssueComments, input)
	if f.createIssueCommentErr != nil {
		return IssueCommentResult{}, f.createIssueCommentErr
	}
	if f.nextIssueCommentID == 0 {
		f.nextIssueCommentID = 9000
	}
	id := f.nextIssueCommentID
	f.nextIssueCommentID++
	return IssueCommentResult{ID: id, URL: fmt.Sprintf("https://example.test/c/%d", id)}, nil
}

func (f *fakeGitHubGateway) UpdateIssueComment(_ context.Context, input UpdateIssueCommentInput) error {
	f.updateIssueComments = append(f.updateIssueComments, input)
	return f.updateIssueCommentErr
}

func (f *fakeGitHubGateway) AddPullRequestLabels(_ context.Context, input PullRequestLabelsInput) error {
	f.addLabelCalls = append(f.addLabelCalls, input)
	return nil
}

func (f *fakeGitHubGateway) RemovePullRequestLabels(_ context.Context, input PullRequestLabelsInput) error {
	f.removeLabelCalls = append(f.removeLabelCalls, input)
	return nil
}

func (f *fakeGitHubGateway) AddPullRequestReviewers(_ context.Context, input PullRequestReviewersInput) error {
	f.reviewerRequests = append(f.reviewerRequests, input)
	return nil
}

func (f *fakeGitHubGateway) ListPullRequestReviews(_ context.Context, _ ViewPullRequestInput) ([]ReviewSummary, error) {
	return f.reviews, nil
}

func (f *fakeGitHubGateway) DismissReview(_ context.Context, input DismissReviewInput) error {
	f.dismissedReviews = append(f.dismissedReviews, input)
	return nil
}

func (f *fakeGitHubGateway) CompareCommits(_ context.Context, input CompareCommitsInput) (CompareCommitsResult, error) {
	f.compareCalls = append(f.compareCalls, input)
	if f.compareErr != nil {
		return CompareCommitsResult{}, f.compareErr
	}
	status := f.compareStatus
	if status == "" {
		status = "identical"
	}
	return CompareCommitsResult{Status: status}, nil
}

type fakeGitGateway struct {
	createResult   CreateWorktreeResult
	prepareResult  PrepareWorktreeResult
	prepareErr     error
	prepareErrors  []error // sequential per-call errors; nil entry falls through to prepareErr/result
	prepareIndex   int
	inspectResults []InspectHeadResult
	inspectIndex   int
	ancestor       map[string]bool
	ancestorErr    error
	fetchErrors    []error
	fetchIndex     int
	fetchErr       error
	pushErrors     []error
	pushIndex      int

	createCalls     []CreateWorktreeInput
	prepareCalls    []PrepareWorktreeInput
	inspectCalls    []InspectHeadInput
	commitCalls     []CommitInput
	pushCalls       []PushInput
	fetchCalls      []string
	mergeBaseCalls  []MergeBaseInput
	mergeBaseResult MergeBaseResult
	mergeBaseErr    error
	cleanupCalls    []CleanupWorktreeInput
}

func (f *fakeGitGateway) CreateWorktree(_ context.Context, input CreateWorktreeInput) (CreateWorktreeResult, error) {
	f.createCalls = append(f.createCalls, input)
	result := f.createResult
	if result.WorktreePath == "" {
		result.WorktreePath = filepath.Join(input.WorktreeRoot, "wt")
	} else if input.WorktreeRoot != "" {
		result.WorktreePath = filepath.Join(input.WorktreeRoot, filepath.Base(result.WorktreePath))
	}
	if result.Branch == "" {
		result.Branch = input.Branch
	}
	if result.HeadSHA == "" {
		result.HeadSHA = "base-head"
	}
	f.createResult = result
	return result, nil
}

func (f *fakeGitGateway) PrepareWorktree(_ context.Context, input PrepareWorktreeInput) (PrepareWorktreeResult, error) {
	f.prepareCalls = append(f.prepareCalls, input)
	if f.prepareIndex < len(f.prepareErrors) {
		err := f.prepareErrors[f.prepareIndex]
		f.prepareIndex++
		if err != nil {
			return PrepareWorktreeResult{}, err
		}
		// nil entry: fall through to success result for this call
	} else if f.prepareErr != nil {
		return PrepareWorktreeResult{}, f.prepareErr
	}
	result := f.prepareResult
	if result.HeadSHA == "" {
		result.HeadSHA = firstNonEmpty(input.ExpectedHeadSHA, "base-head")
	}
	if !result.Clean {
		return result, nil
	}
	return result, nil
}

func (f *fakeGitGateway) InspectHead(_ context.Context, input InspectHeadInput) (InspectHeadResult, error) {
	f.inspectCalls = append(f.inspectCalls, input)
	if len(f.inspectResults) == 0 {
		return InspectHeadResult{HeadSHA: "head"}, nil
	}
	idx := f.inspectIndex
	if idx >= len(f.inspectResults) {
		idx = len(f.inspectResults) - 1
	}
	result := f.inspectResults[idx]
	f.inspectIndex++
	return result, nil
}

func (f *fakeGitGateway) Commit(_ context.Context, input CommitInput) (CommitResult, error) {
	f.commitCalls = append(f.commitCalls, input)
	return CommitResult{CommitSHA: fmt.Sprintf("commit-%d", len(f.commitCalls))}, nil
}

func (f *fakeGitGateway) Push(_ context.Context, input PushInput) error {
	f.pushCalls = append(f.pushCalls, input)
	if f.pushIndex < len(f.pushErrors) && f.pushErrors[f.pushIndex] != nil {
		err := f.pushErrors[f.pushIndex]
		f.pushIndex++
		return err
	}
	f.pushIndex++
	return nil
}

func (f *fakeGitGateway) FetchBranch(_ context.Context, repoPath, remote, branch string) error {
	f.fetchCalls = append(f.fetchCalls, repoPath+"|"+remote+"|"+branch)
	if f.fetchIndex < len(f.fetchErrors) {
		err := f.fetchErrors[f.fetchIndex]
		f.fetchIndex++
		return err
	}
	return f.fetchErr
}

func (f *fakeGitGateway) MergeBaseIntoWorktree(_ context.Context, input MergeBaseInput) (MergeBaseResult, error) {
	f.mergeBaseCalls = append(f.mergeBaseCalls, input)
	return f.mergeBaseResult, f.mergeBaseErr
}

func (f *fakeGitGateway) IsAncestor(_ context.Context, _ string, ancestor, descendant string) (bool, error) {
	if f.ancestorErr != nil {
		return false, f.ancestorErr
	}
	if f.ancestor == nil {
		return ancestor == descendant, nil
	}
	return f.ancestor[ancestor+"->"+descendant], nil
}

func (f *fakeGitGateway) CleanupWorktree(_ context.Context, input CleanupWorktreeInput) error {
	f.cleanupCalls = append(f.cleanupCalls, input)
	return nil
}

type fakeAgentExecutor struct {
	results []AgentResult
	starts  []AgentRunInput
}

func (f *fakeAgentExecutor) Start(_ context.Context, input AgentRunInput) (AgentExecution, error) {
	f.starts = append(f.starts, input)
	if len(f.results) == 0 {
		return nil, fmt.Errorf("no queued agent result")
	}
	result := f.results[0]
	f.results = f.results[1:]
	return fakeAgentExecution{result: result}, nil
}

type fakeAgentExecution struct{ result AgentResult }

func (f fakeAgentExecution) Wait(context.Context) (AgentResult, error) { return f.result, nil }

func passValidation(context.Context, ValidationInput) (ValidationResult, error) {
	return ValidationResult{Passed: true, Summary: "ok"}, nil
}

type testLogger struct{}

func (*testLogger) Debug(string, map[string]any) {}
func (*testLogger) Info(string, map[string]any)  {}
func (*testLogger) Warn(string, map[string]any)  {}
func (*testLogger) Error(string, map[string]any) {}

func contains(haystack, needle string) bool { return strings.Contains(haystack, needle) }

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return string(encoded)
}

func TestBuildFixerReplyBodyMentionsAuthorAndCommit(t *testing.T) {
	t.Parallel()
	got := buildFixerReplyBody(FixItem{Type: "comment", ID: "c1", ThreadID: "t1", Author: "alice", Summary: "Use fmt.Errorf instead of errors.New"}, "abcdef1234567", "")
	if !strings.Contains(got, "@alice") {
		t.Fatalf("body missing author mention:\n%s", got)
	}
	if !strings.Contains(got, "abcdef1") {
		t.Fatalf("body missing short commit SHA:\n%s", got)
	}
	if !strings.Contains(got, "> Use fmt.Errorf") {
		t.Fatalf("body missing original summary quote:\n%s", got)
	}
}

func TestBuildFixerReplyBodyHandlesMissingAuthorAndCommit(t *testing.T) {
	t.Parallel()
	got := buildFixerReplyBody(FixItem{Type: "comment", ID: "c1", ThreadID: "t1"}, "", "")
	if strings.Contains(got, "@") {
		t.Fatalf("body should not include @mention when author missing:\n%s", got)
	}
	if strings.Contains(got, " in ") {
		t.Fatalf("body should not advertise commit when SHA missing:\n%s", got)
	}
	if !strings.Contains(got, "fixed") {
		t.Fatalf("body should still announce fix:\n%s", got)
	}
}

func TestBuildFixerReplyBodyPrefersAgentExplanationOverGenericQuote(t *testing.T) {
	t.Parallel()
	got := buildFixerReplyBody(FixItem{Type: "comment", ID: "c1", ThreadID: "t1", Author: "alice", Summary: "Original review comment that should be hidden"}, "abcdef1", "Replaced strings.Title with cases.Title and added empty-string coverage in foo_test.go.")
	if !strings.Contains(got, "Replaced strings.Title") {
		t.Fatalf("body missing agent explanation:\n%s", got)
	}
	if strings.Contains(got, "Original review comment") {
		t.Fatalf("body should drop original quote when agent explanation present:\n%s", got)
	}
}

func TestProcessClaimedItemRepliesToAuthorBeforeResolving(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "head-1"}},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice"}}},
		},
	}
	git := &fakeGitGateway{
		createResult:  CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
		inspectResults: []InspectHeadResult{
			{HeadSHA: "base-head"},
			{HeadSHA: "new-head", NewCommitSHAs: []string{"new-head"}},
			{HeadSHA: "new-head"},
		},
	}
	stdout := fmt.Sprintf(`__LOOPER_RESULT__={"summary":"applied fixes","review_thread_replies":[{"fixItemId":"c1","threadId":"t1","explanation":"Adjusted the off-by-one handling and verified the fix.","threadCommentsObserved":"%s"}]}`+"\n", hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}}))
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "applied fixes", ParseStatus: "parsed", Stdout: stdout}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(github.replyCalls) != 1 {
		t.Fatalf("reply calls = %d, want 1", len(github.replyCalls))
	}
	reply := github.replyCalls[0]
	if reply.ThreadID != "t1" {
		t.Fatalf("reply thread id = %q, want t1", reply.ThreadID)
	}
	if !strings.Contains(reply.Body, "@alice") {
		t.Fatalf("reply body missing @author mention: %q", reply.Body)
	}
	if len(github.resolveCalls) != 1 {
		t.Fatalf("resolve calls = %d, want 1", len(github.resolveCalls))
	}
}

func TestParseReplyExplanationsValid(t *testing.T) {
	t.Parallel()
	stdout := strings.Join([]string{
		"some agent log line",
		`__LOOPER_RESULT__={"summary":"applied","review_thread_replies":[{"fixItemId":"c1","threadId":"t1","explanation":"Replaced strings.Title with cases.Title.","threadCommentsObserved":"ABC123"}]}`,
	}, "\n")
	got := parseReplyExplanations(stdout, "", []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}})
	if len(got) != 1 || got[0].FixItemID != "c1" || !strings.Contains(got[0].Explanation, "cases.Title") || got[0].ThreadCommentsObserved != "abc123" {
		t.Fatalf("parseReplyExplanations() = %#v", got)
	}
}

func TestParseReplyExplanationsReadsCompletionMarkerFromStderr(t *testing.T) {
	t.Parallel()
	stderr := strings.Join([]string{
		"runtime warning",
		`__LOOPER_RESULT__={"summary":"applied","review_thread_replies":[{"fixItemId":"c1","threadId":"t1","explanation":"Preserved stderr completion markers."}]}`,
	}, "\n")
	got := parseReplyExplanations("", stderr, []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}})
	if len(got) != 1 || got[0].Explanation != "Preserved stderr completion markers." {
		t.Fatalf("parseReplyExplanations() = %#v", got)
	}
}

func TestParseReplyExplanationsSkipsTemplateCompletionMarker(t *testing.T) {
	t.Parallel()
	stdout := strings.Join([]string{
		`__LOOPER_RESULT__={"summary":"applied","review_thread_replies":[{"fixItemId":"c1","threadId":"t1","explanation":"Kept scanning backward to the real completion."}]}`,
		`__LOOPER_RESULT__={"summary":"<one-sentence summary>"}`,
	}, "\n")
	got := parseReplyExplanations(stdout, "", []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}})
	if len(got) != 1 || got[0].Explanation != "Kept scanning backward to the real completion." {
		t.Fatalf("parseReplyExplanations() = %#v", got)
	}
}

func TestParseReplyExplanationsDropsUnknownAndMismatchedThread(t *testing.T) {
	t.Parallel()
	stdout := `__LOOPER_RESULT__={"review_thread_replies":[` +
		`{"fixItemId":"c-unknown","explanation":"hi"},` +
		`{"fixItemId":"c1","threadId":"t-wrong","explanation":"wrong thread"},` +
		`{"fixItemId":"c1","threadId":"t1","explanation":"  "},` +
		`{"fixItemId":"c1","threadId":"t1","explanation":"good"},` +
		`{"fixItemId":"c1","threadId":"t1","explanation":"duplicate, must drop"}` +
		`]}`
	got := parseReplyExplanations(stdout, "", []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}})
	if len(got) != 1 || got[0].Explanation != "good" {
		t.Fatalf("parseReplyExplanations() = %#v, want only the first valid entry", got)
	}
}

func TestParseReplyExplanationsIgnoresNativeReviewComments(t *testing.T) {
	t.Parallel()
	stdout := `__LOOPER_RESULT__={"review_thread_replies":[` +
		`{"fixItemId":"native-101","threadId":"101","action":"fixed","explanation":"generic native reply must not win"},` +
		`{"fixItemId":"c1","threadId":"t1","action":"fixed","explanation":"regular comment reply"}` +
		`]}`
	got := parseReplyExplanations(stdout, "", []FixItem{
		{Type: "comment", ID: "native-101", ThreadID: "101", Source: NativeReviewCommentSource, ProviderCommentID: 101},
		{Type: "comment", ID: "c1", ThreadID: "t1"},
	})
	if len(got) != 1 || got[0].FixItemID != "c1" || got[0].Explanation != "regular comment reply" {
		t.Fatalf("parseReplyExplanations() = %#v, want only non-native reply", got)
	}
}

func TestParseReplyExplanationsDefaultsMissingActionToFixed(t *testing.T) {
	t.Parallel()
	stdout := `__LOOPER_RESULT__={"review_thread_replies":[{"fixItemId":"c1","threadId":"t1","explanation":"good"}]}`
	got := parseReplyExplanations(stdout, "", []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}})
	if len(got) != 1 || got[0].Action != string(replyActionFixed) {
		t.Fatalf("parseReplyExplanations() = %#v, want default fixed action", got)
	}
}

func TestParseReplyExplanationsPreservesDeclinedAction(t *testing.T) {
	t.Parallel()
	stdout := `__LOOPER_RESULT__={"review_thread_replies":[{"fixItemId":"c1","threadId":"t1","action":"declined","explanation":"already implemented"}]}`
	got := parseReplyExplanations(stdout, "", []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}})
	if len(got) != 1 || got[0].Action != string(replyActionDeclined) {
		t.Fatalf("parseReplyExplanations() = %#v, want declined action", got)
	}
}

func TestParseReplyExplanationsPreservesUnknownActionForContractViolationHandling(t *testing.T) {
	t.Parallel()
	stdout := `__LOOPER_RESULT__={"review_thread_replies":[{"fixItemId":"c1","threadId":"t1","action":"decliend","explanation":"already implemented"}]}`
	got := parseReplyExplanations(stdout, "", []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}})
	if len(got) != 1 || got[0].Action != "decliend" {
		t.Fatalf("parseReplyExplanations() = %#v, want preserved unknown action", got)
	}
}

func TestRecordZeroProgressSuccessPausesAfterThreeRunsAndResumesOnStateChange(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(81)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	loop := storage.LoopRecord{ID: "loop_zero_progress", Seq: 94, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "completed", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})
	checkpoint := fixerCheckpoint{Detail: &checkpointDetail{HeadSHA: "head-1"}, FixItems: []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", ThreadFingerprint: "thread-hash-1"}}, FixItemsHash: "fix-hash-1"}
	for i := 0; i < 2; i++ {
		paused, err := runner.recordZeroProgressSuccess(context.Background(), loop, checkpoint)
		if err != nil {
			t.Fatalf("recordZeroProgressSuccess() error = %v", err)
		}
		if paused {
			t.Fatalf("recordZeroProgressSuccess() paused early on run %d", i+1)
		}
	}
	paused, err := runner.recordZeroProgressSuccess(context.Background(), loop, checkpoint)
	if err != nil {
		t.Fatalf("recordZeroProgressSuccess() third run error = %v", err)
	}
	if !paused {
		t.Fatal("recordZeroProgressSuccess() = false, want paused on third zero-progress run")
	}
	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if persisted == nil || persisted.Status != "paused" {
		t.Fatalf("loop = %#v, want paused", persisted)
	}
	resumed, _, err := runner.resumePausedZeroProgressLoop(context.Background(), *persisted, "head-2", hashFixItemsState([]FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", ThreadFingerprint: "thread-hash-1"}}))
	if err != nil {
		t.Fatalf("resumePausedZeroProgressLoop() error = %v", err)
	}
	if !resumed {
		t.Fatal("resumePausedZeroProgressLoop() = false, want resume on head change")
	}
}

func TestClearFixerFollowupMetadataPreservesZeroProgressPauseReason(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(81)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	metadata := mustMarshalJSON(map[string]any{
		"fixerFollowup":            map[string]any{"reason": "missing_evidence"},
		"lastNoopResolveHeadSha":   "head-1",
		"lastNoopResolveStateHash": "same-hash",
		"lastNoopResolveAt":        fixture.nowISO(),
		"pauseReason":              zeroProgressPauseReason,
	})
	loop := storage.LoopRecord{ID: "loop_zero_progress_cleanup", Seq: 95, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "paused", MetadataJSON: &metadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})

	updated, err := runner.clearFixerFollowupMetadata(context.Background(), loop)
	if err != nil {
		t.Fatalf("clearFixerFollowupMetadata() error = %v", err)
	}
	meta := parseJSONObject(updated.MetadataJSON)
	if _, ok := meta["fixerFollowup"]; ok {
		t.Fatalf("fixerFollowup still present in %#v", meta)
	}
	if _, ok := meta["lastNoopResolveHeadSha"]; ok {
		t.Fatalf("lastNoopResolveHeadSha still present in %#v", meta)
	}
	if got, _ := stringFromAny(meta["pauseReason"]); got != zeroProgressPauseReason {
		t.Fatalf("pauseReason = %q, want %q", got, zeroProgressPauseReason)
	}
}

func TestRecordZeroProgressSuccessResetsCountWhenFixItemStateChanges(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	repo := "acme/looper"
	prNumber := int64(82)
	loopTarget := buildPullRequestTargetID(repo, prNumber)
	loop := storage.LoopRecord{ID: "loop_zero_progress_state_reset", Seq: 96, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: &repo, PRNumber: &prNumber, Status: "running", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, Logger: fixture.logger, Now: fixture.now})

	baseItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", ThreadFingerprint: "thread-fingerprint-1"}}
	checkpoint := fixerCheckpoint{Detail: &checkpointDetail{HeadSHA: "head-1"}, FixItems: baseItems, FixItemsHash: hashFixItems(baseItems)}
	for i := 0; i < 2; i++ {
		paused, err := runner.recordZeroProgressSuccess(context.Background(), loop, checkpoint)
		if err != nil {
			t.Fatalf("recordZeroProgressSuccess() error = %v", err)
		}
		if paused {
			t.Fatalf("recordZeroProgressSuccess() paused early on run %d", i+1)
		}
	}

	changedItems := []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", ThreadFingerprint: "thread-fingerprint-2"}}
	changedCheckpoint := fixerCheckpoint{Detail: &checkpointDetail{HeadSHA: "head-1"}, FixItems: changedItems, FixItemsHash: hashFixItems(changedItems)}
	paused, err := runner.recordZeroProgressSuccess(context.Background(), loop, changedCheckpoint)
	if err != nil {
		t.Fatalf("recordZeroProgressSuccess() changed state error = %v", err)
	}
	if paused {
		t.Fatal("recordZeroProgressSuccess() = true, want streak reset when fix-item state changes")
	}

	persisted, err := fixture.repos.Loops.GetByID(context.Background(), loop.ID)
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	state, ok := parseZeroProgressState(parseJSONObject(persisted.MetadataJSON))
	if !ok {
		t.Fatal("parseZeroProgressState() = false, want persisted zero-progress state")
	}
	if state.ConsecutiveCount != 1 {
		t.Fatalf("ConsecutiveCount = %d, want 1 after state change", state.ConsecutiveCount)
	}
	if state.FixItemsStateHash != hashFixItemsState(changedItems) {
		t.Fatalf("FixItemsStateHash = %q, want %q", state.FixItemsStateHash, hashFixItemsState(changedItems))
	}
}

func TestParseReplyExplanationsMalformedFallsBack(t *testing.T) {
	t.Parallel()
	if got := parseReplyExplanations("__LOOPER_RESULT__={bad json}", "", []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}}); got != nil {
		t.Fatalf("parseReplyExplanations() = %#v, want nil on bad JSON", got)
	}
	if got := parseReplyExplanations("no marker here", "", []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1"}}); got != nil {
		t.Fatalf("parseReplyExplanations() = %#v, want nil when marker missing", got)
	}
}

func TestSanitizeReplyExplanationStripsDangerousFragments(t *testing.T) {
	t.Parallel()
	input := "@looper-bot, @another: <!-- looper:stamp v=1 --> <script>alert(1)</script>Replaced foo with bar as requested by @octocat."
	got := sanitizeReplyExplanation(input)
	if strings.Contains(got, "@") || strings.Contains(got, "<script") || strings.Contains(got, "looper:stamp") {
		t.Fatalf("sanitizeReplyExplanation() = %q, did not strip", got)
	}
	if !strings.Contains(got, "Replaced foo with bar") {
		t.Fatalf("sanitizeReplyExplanation() = %q, lost real content", got)
	}
}

func TestSanitizeReplyExplanationStripsVisibleDisclosureFooter(t *testing.T) {
	t.Parallel()
	stamp := disclosure.Stamper{Config: config.DefaultDisclosureConfig(), Agent: "opencode"}.MarkdownStamp("fixer")
	input := "Kept the real fix summary.\n\n" + stamp
	if got := sanitizeReplyExplanation(input); got != "Kept the real fix summary." {
		t.Fatalf("sanitizeReplyExplanation() = %q, want disclosure-free summary", got)
	}
}

func TestSummarizeFixItemSanitizesFallbackSummary(t *testing.T) {
	t.Parallel()
	stamp := disclosure.Stamper{Config: config.DefaultDisclosureConfig(), Agent: "opencode"}.MarkdownStamp("fixer")
	got := summarizeFixItem(FixItem{Summary: "@looper-bot <b>Clean up</b> the fallback text.\n\n" + stamp})
	if strings.Contains(got, "@") || strings.Contains(got, "<b>") || strings.Contains(got, "looper:stamp") {
		t.Fatalf("summarizeFixItem() = %q, did not sanitize fallback summary", got)
	}
	if strings.Contains(got, "Powered by Looper") || strings.Contains(got, disclosure.Slogan) {
		t.Fatalf("summarizeFixItem() = %q, leaked disclosure footer", got)
	}
	if !strings.Contains(got, "Clean up the fallback text.") {
		t.Fatalf("summarizeFixItem() = %q, lost sanitized content", got)
	}
}

func TestLookupReplyExplanationsReturnsExplanationsAndSkipsEmpty(t *testing.T) {
	t.Parallel()
	// Per-thread drift detection (hasNonLooperCommentSince) handles
	// reviewer comments added after the agent's decision; the round
	// summary and evidence records surface the agent's explanations
	// regardless of how the fix-items snapshot has shifted, and entries
	// without a fix item id or explanation text are dropped so summary
	// rendering does not emit blanks.
	checkpoint := fixerCheckpoint{
		Repair: &checkpointRepair{
			ReplyExplanations: []replyExplanationEntry{
				{FixItemID: "c1", Explanation: "good"},
				{FixItemID: "", Explanation: "skip"},
				{FixItemID: "c2", Explanation: ""},
			},
		},
	}
	got := lookupReplyExplanations(checkpoint)
	if len(got) != 1 || got["c1"] != "good" {
		t.Fatalf("lookupReplyExplanations() = %#v, want only c1=good", got)
	}
}

func TestProcessClaimedItemUsesAgentExplanationInReplyBody(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "head-1"}},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice"}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice"}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice"}}},
		},
	}
	git := &fakeGitGateway{
		createResult:  CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
		inspectResults: []InspectHeadResult{
			{HeadSHA: "base-head"},
			{HeadSHA: "new-head", NewCommitSHAs: []string{"new-head"}},
			{HeadSHA: "new-head"},
		},
	}
	stdout := fmt.Sprintf(`__LOOPER_RESULT__={"summary":"done","review_thread_replies":[{"fixItemId":"c1","threadId":"t1","explanation":"Switched the loop bound to len(items)-1 and added a regression test in foo_test.go.","threadCommentsObserved":"%s"}]}`+"\n", hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}}))
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "applied fixes", ParseStatus: "parsed", Stdout: "runtime log", Stderr: stdout}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}
	result, err := runner.ProcessClaimedItem(context.Background(), *claim)
	if err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("result = %#v, want success", result)
	}
	if len(github.replyCalls) != 1 {
		t.Fatalf("reply calls = %d, want 1", len(github.replyCalls))
	}
	reply := github.replyCalls[0]
	if !strings.Contains(reply.Body, "@alice") {
		t.Fatalf("reply body missing @author mention: %q", reply.Body)
	}
	if !strings.Contains(reply.Body, "Switched the loop bound") {
		t.Fatalf("reply body missing agent explanation: %q", reply.Body)
	}
	if strings.Contains(reply.Body, "please fix off-by-one") {
		t.Fatalf("reply body should drop original quote when explanation present: %q", reply.Body)
	}
}

func TestBuildFixerSummaryCommentBodyIncludesMarkerAndItems(t *testing.T) {
	t.Parallel()
	items := []fixerSummaryItem{
		{FixItem: FixItem{Type: "comment", ID: "c1", ThreadID: "t1", Author: "alice", Path: "internal/foo.go", Line: 12, URL: "https://example/threads/t1"}, Status: "resolved", Explanation: "Replaced strings.Title with cases.Title.", ReplyState: "sent"},
		{FixItem: FixItem{Type: "comment", ID: "c2", ThreadID: "t2", Author: "bob", Path: "internal/bar.go", URL: "https://example/threads/t2"}, Status: "failed", ReplyState: "failed"},
		{FixItem: FixItem{Type: "check", Name: "ci"}, Status: "check"},
	}
	got := buildFixerSummaryCommentBody("acme/looper", 42, "abcdef1234567", "abcdef1234567", items)
	if !strings.Contains(got, fixerRoundSummaryMarker("abcdef1234567")) {
		t.Fatalf("body missing round marker:\n%s", got)
	}
	if !strings.Contains(got, "abcdef1") {
		t.Fatalf("body missing short SHA:\n%s", got)
	}
	if !strings.Contains(got, "✅") || !strings.Contains(got, "@alice") {
		t.Fatalf("body missing resolved bullet for alice:\n%s", got)
	}
	if !strings.Contains(got, "⚠️") || !strings.Contains(got, "@bob") {
		t.Fatalf("body missing failed bullet for bob:\n%s", got)
	}
	if !strings.Contains(got, "Replaced strings.Title with cases.Title.") {
		t.Fatalf("body missing explanation:\n%s", got)
	}
	if !strings.Contains(got, "Failing check `ci`") {
		t.Fatalf("body missing failing check item:\n%s", got)
	}
	if !strings.Contains(got, "[thread](https://example/threads/t1)") {
		t.Fatalf("body missing thread link:\n%s", got)
	}
}

func TestFindExistingFixerSummaryCommentIDMatchesTrustedComment(t *testing.T) {
	t.Parallel()
	detail := &checkpointDetail{IssueComments: []map[string]any{
		{"id": float64(101), "body": "unrelated comment"},
		{"id": float64(202), "body": fixerRoundSummaryMarker("abcdef1234567") + "\nLooper fixer round complete\n\n<!-- looper:stamp v=1 -->", "author": map[string]any{"login": "looper"}},
	}}
	id, _ := findExistingFixerSummaryCommentID(detail, "abcdef1234567", "looper")
	if id != 202 {
		t.Fatalf("findExistingFixerSummaryCommentID() = %d, want 202", id)
	}
	if other, _ := findExistingFixerSummaryCommentID(detail, "deadbeef0000000", "looper"); other != 0 {
		t.Fatalf("findExistingFixerSummaryCommentID(other head) = %d, want 0", other)
	}
}

func TestFindExistingFixerSummaryCommentIDSkipsUntrustedMarker(t *testing.T) {
	t.Parallel()
	detail := &checkpointDetail{IssueComments: []map[string]any{
		{"id": float64(101), "body": fixerRoundSummaryMarker("abcdef1234567") + "\nspoofed marker", "author": map[string]any{"login": "someone-else"}},
		{"id": float64(202), "body": fixerRoundSummaryMarker("abcdef1234567") + "\nreal summary", "author": map[string]any{"login": "looper"}},
	}}
	id, _ := findExistingFixerSummaryCommentID(detail, "abcdef1234567", "looper")
	if id != 202 {
		t.Fatalf("findExistingFixerSummaryCommentID() = %d, want trusted comment 202", id)
	}
}

func TestFindExistingFixerSummaryCommentIDSkipsStampedCommentFromOtherAuthor(t *testing.T) {
	t.Parallel()
	detail := &checkpointDetail{IssueComments: []map[string]any{
		{"id": float64(101), "body": fixerRoundSummaryMarker("abcdef1234567") + "\nspoofed marker\n\n<!-- looper:stamp v=1 -->", "author": map[string]any{"login": "someone-else"}},
		{"id": float64(202), "body": fixerRoundSummaryMarker("abcdef1234567") + "\nreal summary\n\n<!-- looper:stamp v=1 -->", "author": map[string]any{"login": "looper"}},
	}}
	id, _ := findExistingFixerSummaryCommentID(detail, "abcdef1234567", "looper")
	if id != 202 {
		t.Fatalf("findExistingFixerSummaryCommentID() = %d, want trusted comment 202", id)
	}
}

func TestFindExistingFixerSummaryCommentIDParsesStringID(t *testing.T) {
	t.Parallel()
	detail := &checkpointDetail{IssueComments: []map[string]any{
		{"id": "202", "body": fixerRoundSummaryMarker("abcdef1234567") + "\nreal summary\n\n<!-- looper:stamp v=1 -->", "author": map[string]any{"login": "looper"}},
	}}
	id, _ := findExistingFixerSummaryCommentID(detail, "abcdef1234567", "looper")
	if id != 202 {
		t.Fatalf("findExistingFixerSummaryCommentID() = %d, want 202 from string id", id)
	}
}

func TestFindExistingFixerSummaryCommentIDParsesGraphQLIDFromURL(t *testing.T) {
	t.Parallel()
	detail := &checkpointDetail{IssueComments: []map[string]any{
		{"id": "IC_kwDOExample", "url": "https://github.com/acme/looper/pull/42#issuecomment-202", "body": fixerRoundSummaryMarker("abcdef1234567") + "\nreal summary\n\n<!-- looper:stamp v=1 -->", "author": map[string]any{"login": "looper"}},
	}}
	id, _ := findExistingFixerSummaryCommentID(detail, "abcdef1234567", "looper")
	if id != 202 {
		t.Fatalf("findExistingFixerSummaryCommentID() = %d, want 202 from issue comment URL", id)
	}
}

func TestFindExistingFixerSummaryCommentIDUsesDatabaseID(t *testing.T) {
	t.Parallel()
	detail := &checkpointDetail{IssueComments: []map[string]any{
		{"id": "IC_kwDOExample", "databaseId": float64(202), "body": fixerRoundSummaryMarker("abcdef1234567") + "\nreal summary\n\n<!-- looper:stamp v=1 -->", "author": map[string]any{"login": "looper"}},
	}}
	id, _ := findExistingFixerSummaryCommentID(detail, "abcdef1234567", "looper")
	if id != 202 {
		t.Fatalf("findExistingFixerSummaryCommentID() = %d, want 202 from databaseId", id)
	}
}

func TestIssueCommentAuthorLoginFallsBackToUser(t *testing.T) {
	t.Parallel()
	comment := map[string]any{"user": map[string]any{"login": "looper"}}
	if got := issueCommentAuthorLogin(comment); got != "looper" {
		t.Fatalf("issueCommentAuthorLogin() = %q, want looper", got)
	}
}

func TestProcessClaimedItemPostsRoundSummaryComment(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "head-1"}},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice", "url": "https://example/threads/t1", "path": "foo.go", "line": float64(7)}}},
			{Number: 42, State: "OPEN", HeadSHA: "head-1", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice", "url": "https://example/threads/t1", "path": "foo.go", "line": float64(7)}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice", "url": "https://example/threads/t1", "path": "foo.go", "line": float64(7)}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice", "url": "https://example/threads/t1", "path": "foo.go", "line": float64(7)}}},
			{Number: 42, State: "OPEN", HeadSHA: "new-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix off-by-one", "author": "alice", "url": "https://example/threads/t1", "path": "foo.go", "line": float64(7)}}},
		},
	}
	git := &fakeGitGateway{
		createResult:  CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
		inspectResults: []InspectHeadResult{
			{HeadSHA: "base-head"},
			{HeadSHA: "new-head", NewCommitSHAs: []string{"new-head"}},
			{HeadSHA: "new-head"},
		},
	}
	stdout := fmt.Sprintf(`__LOOPER_RESULT__={"summary":"done","review_thread_replies":[{"fixItemId":"c1","threadId":"t1","explanation":"Capped loop bound and added regression test.","threadCommentsObserved":"%s"}]}`+"\n", hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}}))
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "applied fixes", ParseStatus: "parsed", Stdout: stdout}}}
	agentModel := "openai/gpt-5.5"
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now, AgentModel: &agentModel})

	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v), want claimed item", claim, err)
	}
	if _, err := runner.ProcessClaimedItem(context.Background(), *claim); err != nil {
		t.Fatalf("ProcessClaimedItem() error = %v", err)
	}
	if len(github.createIssueComments) != 1 {
		t.Fatalf("createIssueComments calls = %d, want 1", len(github.createIssueComments))
	}
	if len(github.updateIssueComments) != 0 {
		t.Fatalf("updateIssueComments calls = %d, want 0 on first round", len(github.updateIssueComments))
	}
	body := github.createIssueComments[0].Body
	if !strings.Contains(body, "Looper fixer round complete") {
		t.Fatalf("summary body missing header:\n%s", body)
	}
	if !strings.Contains(body, "Capped loop bound") {
		t.Fatalf("summary body missing agent explanation:\n%s", body)
	}
	if !strings.Contains(body, fixerRoundSummaryMarker("new-head")) {
		t.Fatalf("summary body missing round marker:\n%s", body)
	}
	if !strings.Contains(body, "@alice") {
		t.Fatalf("summary body missing @author mention:\n%s", body)
	}
	if strings.Contains(body, "model=") {
		t.Fatalf("summary body should not expose agent model:\n%s", body)
	}
}

func TestPublishRoundSummaryCommentUpdatesExistingSummaryFromGraphQLID(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})
	checkpoint := fixerCheckpoint{
		Detail:           &checkpointDetail{IssueComments: []map[string]any{{"id": "IC_kwDOExample", "url": "https://github.com/acme/looper/pull/42#issuecomment-202", "body": fixerRoundSummaryMarker("new-head") + "\nold summary\n\n<!-- looper:stamp v=1 -->", "author": map[string]any{"login": "looper"}}}},
		Push:             &checkpointPush{Pushed: true, HeadSHA: "new-head", Evidence: &fixEvidence{Valid: true, HeadSHA: "new-head", FixItemsHash: "fix-items-hash", Source: "fallback_push", ProducedNewCommits: true}},
		ReconcileCommits: &checkpointReconcileCommits{FinalHeadSHA: "new-head", NewCommitSHAs: []string{"new-head"}},
		ResolvedComments: &checkpointResolvedComments{Items: []checkpointResolvedComment{{FixItemID: "c1", ThreadID: "t1", Status: "resolved", ReplyState: "sent"}}},
		FixItemsHash:     "fix-items-hash",
	}
	fixItems := []FixItem{{ID: "c1", Type: "comment", ThreadID: "t1", Author: "alice", URL: "https://example/threads/t1", Path: "foo.go", Line: 7}}

	runner.publishRoundSummaryComment(context.Background(), stepInput{Repo: "acme/looper", PRNumber: 42, Project: storage.ProjectRecord{RepoPath: t.TempDir()}}, &checkpoint, fixItems, "new-head", map[string]string{"c1": "Capped loop bound and added regression test."})

	if len(github.createIssueComments) != 0 {
		t.Fatalf("createIssueComments calls = %d, want 0 when reusing prior summary", len(github.createIssueComments))
	}
	if len(github.updateIssueComments) != 1 {
		t.Fatalf("updateIssueComments calls = %d, want 1", len(github.updateIssueComments))
	}
	if github.updateIssueComments[0].CommentID != 202 {
		t.Fatalf("updateIssueComments[0].CommentID = %d, want 202", github.updateIssueComments[0].CommentID)
	}
}

func TestHasProgressedCountsAlreadyResolvedComment(t *testing.T) {
	t.Parallel()

	checkpoint := fixerCheckpoint{
		ResolvedComments: &checkpointResolvedComments{Items: []checkpointResolvedComment{{Status: "already_resolved"}}},
	}

	if !hasProgressed(checkpoint) {
		t.Fatalf("hasProgressed() = false, want already_resolved to count as progress")
	}
}

func TestProcessClaimedItemSkipsSummaryWhenNoNewCommits(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{
		listOpen: []PullRequestSummary{{Number: 42, State: "OPEN", HeadSHA: "base-head"}},
		viewResponses: []PullRequestDetail{
			{Number: 42, State: "OPEN", HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
			{Number: 42, State: "OPEN", HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-1", Comments: []map[string]any{{"id": "c1", "threadId": "t1", "body": "please fix"}}},
		},
	}
	git := &fakeGitGateway{
		createResult:  CreateWorktreeResult{WorktreePath: filepath.Join(t.TempDir(), "wt-42"), Branch: "feature/fix-42", HeadSHA: "base-head"},
		prepareResult: PrepareWorktreeResult{HeadSHA: "base-head", Clean: true},
		inspectResults: []InspectHeadResult{
			{HeadSHA: "base-head"},
			{HeadSHA: "base-head"},
			{HeadSHA: "base-head"},
		},
	}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "blocked before editing because gh could not validate PR metadata", ParseStatus: "parsed"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, AgentExecutor: agent, ValidationRunner: passValidation, AllowAutoCommit: true, AllowAutoPush: true, AllowRiskyFixes: true, Logger: fixture.logger, Now: fixture.now})
	if _, err := runner.DiscoverPullRequests(context.Background(), DiscoveryInput{ProjectID: "project_1", Repo: "acme/looper"}); err != nil {
		t.Fatalf("DiscoverPullRequests() error = %v", err)
	}
	claim, err := fixture.repos.Queue.ClaimNextOfType(context.Background(), fixture.nowISO(), "fixer-worker-1", "fixer")
	if err != nil || claim == nil {
		t.Fatalf("ClaimNextOfType() = (%#v, %v)", claim, err)
	}
	if _, err := runner.ProcessClaimedItem(context.Background(), *claim); err != nil {
		// expected to fail in resolve-comments because no new commits; we don't care about the error here.
		_ = err
	}
	if len(github.createIssueComments) != 0 {
		t.Fatalf("createIssueComments calls = %d, want 0 when no new commits", len(github.createIssueComments))
	}
}

func TestRunPushStepAdoptsAgentLifecyclePushEvidence(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	loopMetadata := `{}`
	loopTarget := buildPullRequestTargetID("acme/looper", 42)
	prNumber := int64(42)
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_1", ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: stringPtr("acme/looper"), PRNumber: &prNumber, Status: "running", MetadataJSON: &loopMetadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "agent-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-head"}}}
	git := &fakeGitGateway{prepareResult: PrepareWorktreeResult{HeadSHA: "agent-head", Clean: true}, inspectResults: []InspectHeadResult{{HeadSHA: "agent-head"}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, ValidationRunner: passValidation, AllowAutoPush: true, Now: fixture.now, Logger: fixture.logger})
	checkpoint := fixerCheckpoint{
		Detail:           &checkpointDetail{HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main"},
		Worktree:         &checkpointWorktree{Path: t.TempDir(), Branch: "feature/fix-42", BaseHeadSHA: "base-head"},
		FixItemsHash:     "fix-hash",
		Repair:           &checkpointRepair{Status: "completed", Lifecycle: &lifecycle.State{Branch: "feature/fix-42", CommitSHAs: []string{"agent-head"}, Pushed: true, Actions: lifecycle.Actions{Push: lifecycle.ActionSourceAgent}}},
		Validation:       &ValidationResult{Passed: true, HeadSHA: "base-head"},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}
	updated, err := runner.runPushStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()}, Loop: storage.LoopRecord{ID: "loop_1", MetadataJSON: &loopMetadata}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err != nil {
		t.Fatalf("runPushStep() error = %v", err)
	}
	if updated.Push == nil || !updated.Push.Pushed || updated.Push.HeadSHA != "agent-head" || updated.Push.Evidence == nil || updated.Push.Evidence.Source != "agent_push" {
		t.Fatalf("updated.Push = %#v, want adopted agent evidence", updated.Push)
	}
	if updated.Lifecycle == nil || updated.Lifecycle.Actions.Push != lifecycle.ActionSourceAgent {
		t.Fatalf("updated.Lifecycle = %#v, want preserved agent push source", updated.Lifecycle)
	}
	if github.viewIndex != 1 {
		t.Fatalf("viewIndex = %d, want 1", github.viewIndex)
	}
	if len(git.prepareCalls) != 1 {
		t.Fatalf("prepare calls = %d, want 1 validation-bound reprepare", len(git.prepareCalls))
	}
	if len(git.pushCalls) != 0 {
		t.Fatalf("push calls = %d, want 0 after adoption", len(git.pushCalls))
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), "loop_1")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop != nil && loop.MetadataJSON != nil && !strings.Contains(*loop.MetadataJSON, `"lastFixHeadSha":"agent-head"`) {
		t.Fatalf("loop metadata = %v, want adopted head persisted", *loop.MetadataJSON)
	}
}

func TestRunPushStepRecordsPushEvidenceBeforePostPushHold(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	loopMetadata := `{}`
	loopTarget := buildPullRequestTargetID("acme/looper", 42)
	prNumber := int64(42)
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_post_push_hold", ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: stringPtr("acme/looper"), PRNumber: &prNumber, Status: "running", MetadataJSON: &loopMetadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	run := storage.RunRecord{ID: "run_post_push_hold", LoopID: "loop_post_push_hold", Status: "running", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Runs.Upsert(context.Background(), run); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{
		{Number: 42, State: "OPEN", HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-head"},
		{Number: 42, State: "OPEN", HeadSHA: "fix-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-head"},
		{Number: 42, State: "OPEN", HeadSHA: "fix-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-head", Labels: []string{domain.HoldLabelFixer}},
	}}
	git := &fakeGitGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, ValidationRunner: passValidation, AllowAutoPush: true, Now: fixture.now, Logger: fixture.logger})
	checkpoint := fixerCheckpoint{
		Detail:           &checkpointDetail{HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main"},
		Worktree:         &checkpointWorktree{Path: t.TempDir(), Branch: "feature/fix-42", BaseHeadSHA: "base-head"},
		FixItems:         []FixItem{{ID: "c1", ThreadID: "t1", ThreadFingerprint: "fp1"}},
		FixItemsHash:     "fix-hash",
		Repair:           &checkpointRepair{Status: "completed"},
		Validation:       &ValidationResult{Passed: true, HeadSHA: "fix-head"},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "fix-head", NewCommitSHAs: []string{"fix-head"}, WorkingTreeClean: true},
	}

	updated, err := runner.runPushStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()}, Loop: storage.LoopRecord{ID: "loop_post_push_hold", MetadataJSON: &loopMetadata}, Run: run, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})

	var holdErr *holdSkipError
	if !errors.As(err, &holdErr) {
		t.Fatalf("runPushStep() error = %v, want hold skip", err)
	}
	if len(git.pushCalls) != 1 {
		t.Fatalf("push calls = %d, want 1", len(git.pushCalls))
	}
	if updated.Push == nil || !updated.Push.Pushed || updated.Push.HeadSHA != "fix-head" || updated.Push.Evidence == nil || updated.Push.Evidence.HeadSHA != "fix-head" {
		t.Fatalf("updated.Push = %#v, want recorded pushed head and evidence", updated.Push)
	}
	if updated.Lifecycle == nil || !updated.Lifecycle.Pushed || updated.Lifecycle.Actions.Push != lifecycle.ActionSourceFallback {
		t.Fatalf("updated.Lifecycle = %#v, want pushed fallback lifecycle before hold skip", updated.Lifecycle)
	}
	if len(github.reviewerRequests) != 0 {
		t.Fatalf("reviewerRequests = %#v, want no post-hold reviewer requests", github.reviewerRequests)
	}
	loop, err := fixture.repos.Loops.GetByID(context.Background(), "loop_post_push_hold")
	if err != nil {
		t.Fatalf("Loops.GetByID() error = %v", err)
	}
	if loop == nil || loop.MetadataJSON == nil || !strings.Contains(*loop.MetadataJSON, `"lastFixHeadSha":"fix-head"`) {
		t.Fatalf("loop metadata = %#v, want pushed head persisted before hold skip", loop)
	}
	if updated.Push == nil || !updated.Push.BudgetCounted {
		t.Fatalf("updated.Push = %#v, want budget counted before hold skip", updated.Push)
	}
	if loops.ReadReviewFixBudgetState(loop.MetadataJSON).PushCount != 1 {
		t.Fatalf("pushCount = %d, want 1 before hold skip", loops.ReadReviewFixBudgetState(loop.MetadataJSON).PushCount)
	}
}

func TestRunPushStepSkipsWhenWontfixArrivesAfterCollectFixes(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	loopMetadata := `{}`
	loopTarget := buildPullRequestTargetID("acme/looper", 42)
	prNumber := int64(42)
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_collect_to_push_wontfix", ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: stringPtr("acme/looper"), PRNumber: &prNumber, Status: "running", MetadataJSON: &loopMetadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	run := storage.RunRecord{ID: "run_collect_to_push_wontfix", LoopID: "loop_collect_to_push_wontfix", Status: "running", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Runs.Upsert(context.Background(), run); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{
		currentUser: testLooperLogin,
		viewResponses: []PullRequestDetail{{
			Number: 42, State: "OPEN", HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-head", Author: "alice",
		}},
		threads: []ReviewThread{{
			ID: "t1",
			Comments: []ReviewThreadComment{
				{ID: "c1", Author: testLooperLogin, Body: "Please fix <!-- looper:stamp v=1 -->"},
				{ID: "c2", Author: "alice", AuthorAssociation: "OWNER", Body: "/looper wontfix out of scope"},
			},
		}},
	}
	git := &fakeGitGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, ValidationRunner: passValidation, AllowAutoPush: true, Now: fixture.now, Logger: fixture.logger})
	checkpoint := fixerCheckpoint{
		Detail:           &checkpointDetail{HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main"},
		Worktree:         &checkpointWorktree{Path: t.TempDir(), Branch: "feature/fix-42", BaseHeadSHA: "base-head"},
		FixItems:         []FixItem{{Type: "comment", ID: "c1", ThreadID: "t1", ThreadFingerprint: "fp1"}},
		FixItemsHash:     "fix-hash",
		Repair:           &checkpointRepair{Status: "completed"},
		Validation:       &ValidationResult{Passed: true, HeadSHA: "fix-head"},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "fix-head", NewCommitSHAs: []string{"fix-head"}, WorkingTreeClean: true},
	}

	updated, err := runner.runPushStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()}, Loop: storage.LoopRecord{ID: "loop_collect_to_push_wontfix", MetadataJSON: &loopMetadata}, Run: run, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err != nil {
		t.Fatalf("runPushStep() error = %v", err)
	}
	if len(git.pushCalls) != 0 {
		t.Fatalf("push calls = %d, want 0 after mid-run wontfix", len(git.pushCalls))
	}
	if updated.Push == nil || updated.Push.Pushed || !strings.Contains(updated.Push.SkippedReason, "withheld") {
		t.Fatalf("updated.Push = %#v, want skipped withheld push", updated.Push)
	}
	if len(github.listThreadsCalls) == 0 {
		t.Fatal("want live withhold recheck before push")
	}
}

func TestCollectToPushAbortsWhenOneRepairedThreadBecomesWithheld(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	loopMetadata := `{}`
	loopTarget := buildPullRequestTargetID("acme/looper", 42)
	prNumber := int64(42)
	if err := fixture.repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_partial_withhold_push", ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", TargetID: &loopTarget, Repo: stringPtr("acme/looper"), PRNumber: &prNumber, Status: "running", MetadataJSON: &loopMetadata, CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	run := storage.RunRecord{ID: "run_partial_withhold_push", LoopID: "loop_partial_withhold_push", Status: "running", CreatedAt: fixture.nowISO(), UpdatedAt: fixture.nowISO()}
	if err := fixture.repos.Runs.Upsert(context.Background(), run); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	github := &fakeGitHubGateway{
		currentUser: testLooperLogin,
		viewResponses: []PullRequestDetail{{
			Number: 42, State: "OPEN", HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-head", Author: "alice",
		}},
		threads: []ReviewThread{
			{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Author: testLooperLogin, Body: "Please fix t1 <!-- looper:stamp v=1 -->"}}},
			{ID: "t2", Comments: []ReviewThreadComment{{ID: "c2", Author: testLooperLogin, Body: "Please fix t2 <!-- looper:stamp v=1 -->"}}},
		},
	}
	git := &fakeGitGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Git: git, ValidationRunner: passValidation, AllowAutoPush: true, Now: fixture.now, Logger: fixture.logger})
	collectCheckpoint := fixerCheckpoint{
		Detail: &checkpointDetail{
			State: "OPEN", HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main",
			Comments: []map[string]any{
				{"id": "c1", "threadId": "t1", "body": "Please fix t1 <!-- looper:stamp v=1 -->", "author": testLooperLogin},
				{"id": "c2", "threadId": "t2", "body": "Please fix t2 <!-- looper:stamp v=1 -->", "author": testLooperLogin},
			},
		},
	}
	collected, err := runner.runCollectFixesStep(context.Background(), stepInput{
		Project:    storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Loop:       storage.LoopRecord{ID: "loop_partial_withhold_push", MetadataJSON: &loopMetadata},
		Run:        run,
		Repo:       "acme/looper",
		PRNumber:   42,
		Checkpoint: collectCheckpoint,
	})
	if err != nil {
		t.Fatalf("runCollectFixesStep() error = %v", err)
	}
	if collected.SkipReason != "" || len(collected.FixItems) != 2 {
		t.Fatalf("collect FixItems=%#v skip=%q, want both comment items admitted", collected.FixItems, collected.SkipReason)
	}

	github.threads[0].Comments = append(github.threads[0].Comments, ReviewThreadComment{
		ID: "c1-wontfix", Author: "alice", AuthorAssociation: "OWNER", Body: "/looper wontfix out of scope",
	})

	pushCheckpoint := collected
	pushCheckpoint.Worktree = &checkpointWorktree{Path: t.TempDir(), Branch: "feature/fix-42", BaseHeadSHA: "base-head"}
	pushCheckpoint.Repair = &checkpointRepair{Status: "completed"}
	pushCheckpoint.Validation = &ValidationResult{Passed: true, HeadSHA: "fix-head"}
	pushCheckpoint.ReconcileCommits = &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "fix-head", NewCommitSHAs: []string{"fix-head"}, WorkingTreeClean: true}

	updated, err := runner.runPushStep(context.Background(), stepInput{
		Project:    storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Loop:       storage.LoopRecord{ID: "loop_partial_withhold_push", MetadataJSON: &loopMetadata},
		Run:        run,
		Repo:       "acme/looper",
		PRNumber:   42,
		Checkpoint: pushCheckpoint,
	})
	if err == nil || !strings.Contains(err.Error(), "will rediscover remaining items") {
		t.Fatalf("runPushStep() error = %v, want rediscover after partial withhold", err)
	}
	var loopErr *loopError
	if !errors.As(err, &loopErr) || loopErr.kind != FailureRetryableAfterResume {
		t.Fatalf("runPushStep() error = %v, want retryable-after-resume loopError", err)
	}
	if len(git.pushCalls) != 0 {
		t.Fatalf("push calls = %d, want 0 when one repaired thread is withheld", len(git.pushCalls))
	}
	if updated.Push == nil || updated.Push.Pushed || !strings.Contains(updated.Push.SkippedReason, "withheld") {
		t.Fatalf("updated.Push = %#v, want skipped withheld push", updated.Push)
	}
	if updated.ResumePolicy != loops.ResumePolicyRestartFromDiscover {
		t.Fatalf("updated.ResumePolicy = %q, want restart_from_discover", updated.ResumePolicy)
	}
}

func TestRunPushStepDoesNotAdoptAgentLifecyclePushEvidenceOnLiveHeadMismatch(t *testing.T) {
	t.Parallel()
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{{Number: 42, State: "OPEN", HeadSHA: "other-head", HeadRefName: "feature/fix-42", BaseRefName: "main", BaseSHA: "base-head"}}}
	git := &fakeGitGateway{}
	runner := New(Options{GitHub: github, Git: git, AllowAutoPush: true})
	checkpoint := fixerCheckpoint{
		Detail:           &checkpointDetail{HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main"},
		Worktree:         &checkpointWorktree{Path: t.TempDir(), Branch: "feature/fix-42", BaseHeadSHA: "base-head"},
		FixItemsHash:     "fix-hash",
		Repair:           &checkpointRepair{Status: "completed", Lifecycle: &lifecycle.State{Branch: "feature/fix-42", CommitSHAs: []string{"agent-head"}, Pushed: true, Actions: lifecycle.Actions{Push: lifecycle.ActionSourceAgent}}},
		Validation:       &ValidationResult{Passed: true, HeadSHA: "agent-head"},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true},
	}
	updated, err := runner.runPushStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
	if err != nil {
		t.Fatalf("runPushStep() error = %v", err)
	}
	if updated.Push == nil || updated.Push.Pushed {
		t.Fatalf("updated.Push = %#v, want no adoption", updated.Push)
	}
	if len(git.pushCalls) != 0 {
		t.Fatalf("push calls = %d, want 0", len(git.pushCalls))
	}
}

func TestRunPushStepDoesNotAdoptAgentLifecyclePushEvidenceOnBranchOrPRMismatch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		lifecycle *lifecycle.State
	}{
		{name: "branch mismatch", lifecycle: &lifecycle.State{Branch: "other-branch", CommitSHAs: []string{"agent-head"}, Pushed: true, Actions: lifecycle.Actions{Push: lifecycle.ActionSourceAgent}}},
		{name: "pr mismatch", lifecycle: &lifecycle.State{Branch: "feature/fix-42", PRNumber: 41, CommitSHAs: []string{"agent-head"}, Pushed: true, Actions: lifecycle.Actions{Push: lifecycle.ActionSourceAgent}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			github := &fakeGitHubGateway{}
			runner := New(Options{GitHub: github, Git: &fakeGitGateway{}, AllowAutoPush: true})
			checkpoint := fixerCheckpoint{Detail: &checkpointDetail{HeadSHA: "base-head", HeadRefName: "feature/fix-42", BaseRefName: "main"}, Worktree: &checkpointWorktree{Path: t.TempDir(), Branch: "feature/fix-42", BaseHeadSHA: "base-head"}, FixItemsHash: "fix-hash", Repair: &checkpointRepair{Status: "completed", Lifecycle: tc.lifecycle}, Validation: &ValidationResult{Passed: true, HeadSHA: "agent-head"}, ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head", WorkingTreeClean: true}}
			updated, err := runner.runPushStep(context.Background(), stepInput{Project: storage.ProjectRecord{RepoPath: t.TempDir()}, Repo: "acme/looper", PRNumber: 42, Checkpoint: checkpoint})
			if err != nil {
				t.Fatalf("runPushStep() error = %v", err)
			}
			if updated.Push == nil || updated.Push.Pushed {
				t.Fatalf("updated.Push = %#v, want no adoption", updated.Push)
			}
			if github.viewIndex != 0 {
				t.Fatalf("viewIndex = %d, want 0", github.viewIndex)
			}
		})
	}
}

func TestPublishRoundSummaryCommentPostsForAgentEvidenceWithoutLocalNewCommits(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	github := &fakeGitHubGateway{}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Now: fixture.now, Logger: fixture.logger})
	checkpoint := fixerCheckpoint{
		Validation:       &ValidationResult{Passed: true, HeadSHA: "agent-head"},
		Push:             &checkpointPush{Pushed: true, HeadSHA: "agent-head", Evidence: &fixEvidence{Valid: true, HeadSHA: "agent-head", FixItemsHash: "fix-hash", Source: "agent_push", ProducedNewCommits: true}},
		ReconcileCommits: &checkpointReconcileCommits{BaseHeadSHA: "base-head", FinalHeadSHA: "base-head"},
		ResolvedComments: &checkpointResolvedComments{Items: []checkpointResolvedComment{{FixItemID: "c1", ThreadID: "t1", Status: "resolved", ReplyState: "sent"}}},
		FixItemsHash:     "fix-hash",
	}
	runner.publishRoundSummaryComment(context.Background(), stepInput{Repo: "acme/looper", PRNumber: 42, Project: storage.ProjectRecord{RepoPath: t.TempDir()}}, &checkpoint, []FixItem{{ID: "c1", Type: "comment", ThreadID: "t1", Author: "alice", URL: "https://example/threads/t1"}}, "base-head", map[string]string{"c1": "Applied the reviewer-requested fix."})
	if len(github.createIssueComments) != 1 {
		t.Fatalf("createIssueComments calls = %d, want 1", len(github.createIssueComments))
	}
	if !strings.Contains(github.createIssueComments[0].Body, fixerRoundSummaryMarker("agent-head")) {
		t.Fatalf("summary body = %q, want adopted evidence head marker", github.createIssueComments[0].Body)
	}
}

func TestRunForgejoFixerSummaryStepRefreshesLiveSummaryComment(t *testing.T) {
	t.Parallel()
	fixture := newRunnerFixture(t)
	reviewerSummary := forge.NewReviewerSummary(2, []forge.ReviewItem{{
		ReviewItemID:    "R-001",
		Status:          forge.ReviewItemStatusOpen,
		Title:           "Keep fixer summary idempotent",
		Body:            "Refresh live comments before choosing create vs update.",
		LastSeenRoundID: 2,
	}})
	reviewerMarker, err := forge.RenderReviewerSummary(reviewerSummary)
	if err != nil {
		t.Fatalf("RenderReviewerSummary() error = %v", err)
	}
	existingFixerBody, err := renderForgejoFixerSummaryComment(forge.NewFixerSummary(3, reviewerSummary.ReviewRoundID, []forge.FixerResult{{
		ReviewItemID: "R-001",
		Result:       forge.FixerItemResultFixed,
		Explanation:  "Earlier fixer run result.",
	}}))
	if err != nil {
		t.Fatalf("renderForgejoFixerSummaryComment() error = %v", err)
	}
	staleDetail := &checkpointDetail{
		HeadSHA:       "head-sha",
		HeadRefName:   "reviewer-fixer",
		BaseRefName:   "main",
		IssueComments: []map[string]any{{"id": int64(101), "body": reviewerMarker, "url": "https://example.test/comments/101", "author": map[string]any{"login": "looper"}}},
	}
	liveDetail := PullRequestDetail{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "head-sha",
		HeadRefName: "reviewer-fixer",
		BaseRefName: "main",
		IssueComments: []map[string]any{
			{"id": int64(101), "body": reviewerMarker, "url": "https://example.test/comments/101", "author": map[string]any{"login": "looper"}},
			{"id": int64(201), "body": existingFixerBody, "url": "https://example.test/comments/201", "author": map[string]any{"login": "mallory"}},
			{"id": int64(202), "body": existingFixerBody, "url": "https://example.test/comments/202", "author": map[string]any{"login": "looper"}},
		},
	}
	github := &fakeGitHubGateway{currentUser: "looper", viewResponses: []PullRequestDetail{liveDetail}}
	cfg := forgejoFixerDiscoveryConfig(t, fixture)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Now: fixture.now, Logger: fixture.logger, CustomInstructions: cfg})
	checkpoint := fixerCheckpoint{
		Detail:       staleDetail,
		FixItemsHash: "fix-items-hash",
		FixItems:     []FixItem{{ID: "R-001", Type: "comment"}},
		Validation:   &ValidationResult{Passed: true},
		Push:         &checkpointPush{Pushed: true, HeadSHA: "head-sha"},
		Repair:       &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "R-001", Action: "fixed", Explanation: "Refetched live PR comments before upserting the Forgejo fixer summary."}}},
	}

	got, err := runner.runForgejoFixerSummaryStep(context.Background(), stepInput{
		Repo:       "acme/looper",
		PRNumber:   42,
		Project:    storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Checkpoint: checkpoint,
	})
	if err != nil {
		t.Fatalf("runForgejoFixerSummaryStep() error = %v", err)
	}
	if len(github.createIssueComments) != 0 {
		t.Fatalf("createIssueComments calls = %d, want 0 when live summary already exists", len(github.createIssueComments))
	}
	if len(github.updateIssueComments) != 1 {
		t.Fatalf("updateIssueComments calls = %d, want 1", len(github.updateIssueComments))
	}
	if github.updateIssueComments[0].CommentID != 202 {
		t.Fatalf("updateIssueComments[0].CommentID = %d, want 202", github.updateIssueComments[0].CommentID)
	}
	parsedFixer, err := forge.ParseFixerSummary(github.updateIssueComments[0].Body)
	if err != nil {
		t.Fatalf("ParseFixerSummary(updated body) error = %v", err)
	}
	if parsedFixer.FixRoundID != 4 {
		t.Fatalf("updated fixer summary round = %d, want 4 from live existing summary", parsedFixer.FixRoundID)
	}
	if got.SummaryComment == nil || got.SummaryComment.CommentID != 202 || got.SummaryComment.State != "updated" {
		t.Fatalf("SummaryComment = %#v, want updated live comment 202", got.SummaryComment)
	}
}

func TestRunResolveCommentsStepForgejoSummaryOnlySkipsNativeThreadLogicAndPostsSummary(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	reviewerSummary := forge.NewReviewerSummary(2, []forge.ReviewItem{{
		ReviewItemID:    "R-001",
		Status:          forge.ReviewItemStatusOpen,
		Title:           "Fix parsing",
		Body:            "Parser must fail fast.",
		LastSeenRoundID: 2,
	}})
	reviewerMarker, err := forge.RenderReviewerSummary(reviewerSummary)
	if err != nil {
		t.Fatalf("RenderReviewerSummary() error = %v", err)
	}
	liveDetail := PullRequestDetail{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "head-sha",
		HeadRefName: "reviewer-fixer",
		BaseRefName: "main",
		IssueComments: []map[string]any{{
			"id":     int64(101),
			"body":   reviewerMarker,
			"url":    "https://example.test/comments/101",
			"author": map[string]any{"login": "looper"},
		}, {
			"id":     int64(102),
			"body":   reviewerMarker,
			"url":    "https://example.test/comments/102",
			"author": map[string]any{"login": "mallory"},
		}},
	}
	github := &fakeGitHubGateway{currentUser: "looper", viewResponses: []PullRequestDetail{liveDetail, liveDetail}}
	cfg := forgejoFixerDiscoveryConfig(t, fixture)
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Now: fixture.now, Logger: fixture.logger, CustomInstructions: cfg})
	fixItems := []FixItem{{ID: "R-001", Type: "comment", Source: "forgejo-reviewer-summary", ThreadID: "R-001", Summary: "Fix parsing"}}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{
		Project:  storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: fixerCheckpoint{
			Detail:       &checkpointDetail{HeadSHA: "head-sha", HeadRefName: "reviewer-fixer", BaseRefName: "main", IssueComments: cloneObjectSlice(liveDetail.IssueComments)},
			FixItems:     fixItems,
			FixItemsHash: hashFixItems(fixItems),
			Validation:   &ValidationResult{Passed: true, HeadSHA: "head-sha"},
			Push:         &checkpointPush{Pushed: true, HeadSHA: "head-sha"},
			Repair:       &checkpointRepair{ReplyExplanations: []replyExplanationEntry{{FixItemID: "R-001", Action: "fixed", Explanation: "Applied the requested fix."}}},
		},
	})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v", err)
	}
	if len(github.viewThreadCalls) != 0 {
		t.Fatalf("ViewReviewThread calls = %#v, want none for forgejo summary items", github.viewThreadCalls)
	}
	if len(github.resolveCalls) != 0 {
		t.Fatalf("ResolveReviewThread calls = %#v, want none for forgejo summary items", github.resolveCalls)
	}
	if len(github.createIssueComments) != 1 {
		t.Fatalf("createIssueComments calls = %d, want 1", len(github.createIssueComments))
	}
	if updated.SummaryComment == nil || updated.SummaryComment.State != "created" {
		t.Fatalf("SummaryComment = %#v, want created summary comment", updated.SummaryComment)
	}
	if len(updated.FixItems) != 1 || updated.FixItems[0].Source != "forgejo-reviewer-summary" {
		t.Fatalf("FixItems = %#v, want forgejo summary item preserved", updated.FixItems)
	}
}

func TestRunResolveCommentsStepNativeAndSummaryCoexistWithoutRoutingSummaryThroughThreadLogic(t *testing.T) {
	t.Parallel()

	fixture := newRunnerFixture(t)
	reviewerSummary := forge.NewReviewerSummary(2, []forge.ReviewItem{{
		ReviewItemID:    "R-001",
		Status:          forge.ReviewItemStatusOpen,
		Title:           "Fix parsing",
		Body:            "Parser must fail fast.",
		LastSeenRoundID: 2,
	}})
	reviewerMarker, err := forge.RenderReviewerSummary(reviewerSummary)
	if err != nil {
		t.Fatalf("RenderReviewerSummary() error = %v", err)
	}
	liveDetail := PullRequestDetail{
		Number:      42,
		State:       "OPEN",
		HeadSHA:     "head-sha",
		HeadRefName: "reviewer-fixer",
		BaseRefName: "main",
		IssueComments: []map[string]any{{
			"id":   int64(101),
			"body": reviewerMarker,
			"url":  "https://example.test/comments/101",
		}},
		Comments: []map[string]any{{
			"id":       "c1",
			"threadId": "t1",
			"body":     "please fix",
		}},
	}
	github := &fakeGitHubGateway{viewResponses: []PullRequestDetail{liveDetail, liveDetail}, threads: []ReviewThread{{ID: "t1", Comments: []ReviewThreadComment{{ID: "c1", Body: "please fix"}}}}}
	runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: github, Now: fixture.now, Logger: fixture.logger})
	observed := hashReviewThreadComments(ReviewThread{Comments: []ReviewThreadComment{{ID: "c1"}}})
	fixItems := []FixItem{{ID: "c1", Type: "comment", ThreadID: "t1", Summary: "please fix"}, {ID: "R-001", Type: "comment", Source: "forgejo-reviewer-summary", ThreadID: "R-001", Summary: "Fix parsing"}}

	updated, err := runner.runResolveCommentsStep(context.Background(), stepInput{
		Project:  storage.ProjectRecord{ID: "project_1", RepoPath: t.TempDir()},
		Repo:     "acme/looper",
		PRNumber: 42,
		Checkpoint: fixerCheckpoint{
			Detail:       &checkpointDetail{HeadSHA: "head-sha", HeadRefName: "reviewer-fixer", BaseRefName: "main", Comments: liveDetail.Comments, IssueComments: liveDetail.IssueComments},
			FixItems:     fixItems,
			FixItemsHash: hashFixItems(fixItems),
			Validation:   &ValidationResult{Passed: true, HeadSHA: "head-sha"},
			Push:         &checkpointPush{Pushed: true, HeadSHA: "head-sha"},
			Repair: &checkpointRepair{ReplyExplanations: []replyExplanationEntry{
				{FixItemID: "c1", ThreadID: "t1", Action: string(replyActionFixed), Explanation: "Applied the requested fix.", ThreadCommentsObserved: observed},
				{FixItemID: "R-001", Action: "fixed", Explanation: "Applied the requested fix."},
			}},
		},
	})
	if err != nil {
		t.Fatalf("runResolveCommentsStep() error = %v", err)
	}
	if len(github.viewThreadCalls) == 0 {
		t.Fatalf("ViewReviewThread calls = %#v, want calls only for native thread t1", github.viewThreadCalls)
	}
	for _, call := range github.viewThreadCalls {
		if call.ThreadID != "t1" {
			t.Fatalf("ViewReviewThread calls = %#v, want no forgejo summary thread lookups", github.viewThreadCalls)
		}
	}
	if len(github.resolveCalls) != 1 || github.resolveCalls[0].ThreadID != "t1" {
		t.Fatalf("ResolveReviewThread calls = %#v, want only native thread t1", github.resolveCalls)
	}
	if len(github.createIssueComments) != 1 {
		t.Fatalf("createIssueComments calls = %d, want 1", len(github.createIssueComments))
	}
	if updated.SummaryComment == nil || updated.SummaryComment.State != "created" {
		t.Fatalf("SummaryComment = %#v, want created forgejo summary comment", updated.SummaryComment)
	}
}

func TestSkippedNoEvidenceThreadIDs(t *testing.T) {
	t.Parallel()
	fixItems := []FixItem{
		{ID: "c1", Type: "comment", ThreadID: "t1"},
		{ID: "c2", Type: "comment", ThreadID: "t2"},
		{ID: "c3", Type: "task", ThreadID: "t3"},
		{ID: "c4", Type: "comment", ThreadID: "t1"},
	}
	resolved := []checkpointResolvedComment{
		{FixItemID: "c1", ThreadID: "t1", Status: "skipped_no_evidence"},
		{FixItemID: "c2", ThreadID: "t2", Status: "resolved"},
		{FixItemID: "c4", ThreadID: "t1", Status: "skipped_no_evidence"},
	}
	got := skippedNoEvidenceThreadIDs(fixItems, resolved)
	want := []string{"t1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("skippedNoEvidenceThreadIDs() = %v, want %v", got, want)
	}
}

func TestUpdateLoopPreservesTerminatedLoop(t *testing.T) {
	fixture := newRunnerFixture(t)
	nowISO := fixture.nowISO()
	repo := "acme/looper"
	prNumber := int64(42)
	loop := storage.LoopRecord{ID: "loop_fixer_terminated", Seq: 901, ProjectID: "project_1", Type: "fixer", TargetType: "pull_request", Repo: &repo, PRNumber: &prNumber, Status: "terminated", CreatedAt: nowISO, UpdatedAt: nowISO}
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

func TestReRequestReviewersAfterFix(t *testing.T) {
	github := &fakeGitHubGateway{
		currentUser: "looper-bot",
		reviews: []ReviewSummary{
			{ID: 1, State: "CHANGES_REQUESTED", Author: "reviewer-a"},
			{ID: 2, State: "COMMENTED", Author: "reviewer-b"},
			{ID: 3, State: "APPROVED", Author: "reviewer-c"},          // approved -> not re-requested
			{ID: 4, State: "CHANGES_REQUESTED", Author: "looper-bot"}, // self -> excluded
			{ID: 5, State: "CHANGES_REQUESTED", Author: "reviewer-a"}, // duplicate -> once
		},
	}
	runner := New(Options{GitHub: github, RetryMaxAttempts: -1})
	runner.reRequestReviewersAfterFix(context.Background(), stepInput{Repo: "acme/x", PRNumber: 7, Project: storage.ProjectRecord{RepoPath: "/tmp"}})

	if len(github.reviewerRequests) != 1 {
		t.Fatalf("reviewerRequests = %d, want 1 (one re-request call)", len(github.reviewerRequests))
	}
	got := github.reviewerRequests[0].Reviewers
	if len(got) != 2 {
		t.Fatalf("re-requested %v, want [reviewer-a reviewer-b] (approved/self/dup excluded)", got)
	}
	set := map[string]bool{got[0]: true, got[1]: true}
	if !set["reviewer-a"] || !set["reviewer-b"] {
		t.Fatalf("re-requested %v, want reviewer-a + reviewer-b", got)
	}
}

func TestApplyReviewDismissals(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".looper"), 0o755); err != nil {
		t.Fatalf("MkdirAll error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".looper", "dismiss.json"), []byte(`{"dismissals":[{"reviewer":"reviewer-x","reason":"this behavior is intentional"}]}`), 0o644); err != nil {
		t.Fatalf("WriteFile error = %v", err)
	}
	github := &fakeGitHubGateway{
		reviews: []ReviewSummary{
			{ID: 10, State: "CHANGES_REQUESTED", Author: "reviewer-x"}, // named + changes-requested -> dismiss
			{ID: 11, State: "CHANGES_REQUESTED", Author: "reviewer-y"}, // not named -> keep
			{ID: 12, State: "APPROVED", Author: "reviewer-x"},          // not changes-requested -> keep
		},
	}
	runner := New(Options{GitHub: github, RetryMaxAttempts: -1})
	runner.applyReviewDismissals(context.Background(), stepInput{Repo: "acme/x", PRNumber: 9, Project: storage.ProjectRecord{RepoPath: "/tmp"}}, dir)

	if len(github.dismissedReviews) != 1 {
		t.Fatalf("dismissed = %d, want exactly 1", len(github.dismissedReviews))
	}
	d := github.dismissedReviews[0]
	if d.ReviewID != 10 || d.Message != "this behavior is intentional" {
		t.Fatalf("dismissed = %#v, want review 10 with the agent's reason", d)
	}
	if _, err := os.Stat(filepath.Join(dir, ".looper", "dismiss.json")); !os.IsNotExist(err) {
		t.Fatal("dismiss.json must be consumed (removed) after processing")
	}
}
