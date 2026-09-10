package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nexu-io/looper/internal/storage"
	pkgapi "github.com/nexu-io/looper/pkg/api"
)

func TestLoopInspectAcceptsRunIDAndClassifiesFailure(t *testing.T) {
	t.Parallel()

	configPath := writeLoopDiagnosticsFixture(t, "")
	exitCode, stdout, stderr := runApp(t, "loop", "inspect", "run_reviewer_failed", "--json", "--config", configPath)
	if exitCode != 0 {
		t.Fatalf("Run([loop inspect]) exit code = %d, want 0; stderr=%q", exitCode, stderr)
	}
	var decoded struct {
		SelectorKind string `json:"selectorKind"`
		Loop         struct {
			Seq    int64  `json:"seq"`
			Status string `json:"status"`
			Target struct {
				Label string `json:"label"`
			} `json:"target"`
		} `json:"loop"`
		Run struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"run"`
		LatestQueueItem struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"latestQueueItem"`
		Diagnosis struct {
			FailureClass      string `json:"failureClass"`
			Retryable         *bool  `json:"retryable"`
			RecommendedAction string `json:"recommendedAction"`
		} `json:"diagnosis"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v\noutput=%q", err, stdout)
	}
	if decoded.SelectorKind != "runId" || decoded.Loop.Seq != 7 || decoded.Loop.Target.Label != "acme/looper#42" || decoded.Run.ID != "run_reviewer_failed" {
		t.Fatalf("inspect output = %#v, want run selector resolved to loop #7", decoded)
	}
	if decoded.LatestQueueItem.ID != "queue_failed" || decoded.LatestQueueItem.Status != "failed" {
		t.Fatalf("latest queue item = %#v, want failed queue", decoded.LatestQueueItem)
	}
	if decoded.Diagnosis.FailureClass != "github_transient" || decoded.Diagnosis.Retryable == nil || !*decoded.Diagnosis.Retryable || decoded.Diagnosis.RecommendedAction == "" {
		t.Fatalf("diagnosis = %#v, want retryable github_transient with action", decoded.Diagnosis)
	}
}

func TestClassifyDiagnosticMessageSuggestsRepairForRetryableConfigFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		message   string
		wantClass string
		wantHint  string
	}{
		{name: "github auth", message: "GitHub API failed: HTTP 403 Forbidden", wantClass: "github_auth_or_scope", wantHint: "GitHub auth"},
		{name: "repository access", message: "GraphQL: Could not resolve to a Repository", wantClass: "github_repository_access", wantHint: "repo slug"},
		{name: "repo path", message: "start command: chdir /tmp/missing-repo: no such file or directory", wantClass: "project_repo_path", wantHint: "repoPath"},
		{name: "config", message: "invalid model", wantClass: "configuration", wantHint: "configuration"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyDiagnosticMessage(tt.message, "retryable_transient")
			if got.FailureClass != tt.wantClass || got.Retryable == nil || !*got.Retryable {
				t.Fatalf("diagnosis = %#v, want retryable %s", got, tt.wantClass)
			}
			if !strings.Contains(got.RecommendedAction, tt.wantHint) {
				t.Fatalf("RecommendedAction = %q, want hint %q", got.RecommendedAction, tt.wantHint)
			}
		})
	}
}

func TestClassifyDiagnosticMessagePreservesTerminalGitHubDenials(t *testing.T) {
	t.Parallel()

	got := classifyDiagnosticMessage("GitHub API failed: HTTP 403 Forbidden: policy denied by ruleset", "non_retryable")
	if got.FailureClass != "non_retryable" || got.Retryable == nil || *got.Retryable {
		t.Fatalf("diagnosis = %#v, want non_retryable terminal denial", got)
	}
	if got.RecommendedAction != "inspect before manual recovery" {
		t.Fatalf("RecommendedAction = %q, want manual recovery hint", got.RecommendedAction)
	}
}

func TestClassifyDiagnosticMessagePreservesTerminalConfigValidationFailures(t *testing.T) {
	t.Parallel()

	got := classifyDiagnosticMessage("config validation failed: notifications.osascript.enabled requires osascript", "non_retryable")
	if got.FailureClass != "non_retryable" || got.Retryable == nil || *got.Retryable {
		t.Fatalf("diagnosis = %#v, want non_retryable terminal config failure", got)
	}
	if got.RecommendedAction != "fix the Looper configuration value before manual recovery" {
		t.Fatalf("RecommendedAction = %q, want config manual recovery hint", got.RecommendedAction)
	}
}

func TestLoopFailuresListsFailedLoops(t *testing.T) {
	t.Parallel()

	configPath := writeLoopDiagnosticsFixture(t, "")
	exitCode, stdout, stderr := runApp(t, "loop", "failures", "--type", "reviewer", "--json", "--config", configPath)
	if exitCode != 0 {
		t.Fatalf("Run([loop failures]) exit code = %d, want 0; stderr=%q", exitCode, stderr)
	}
	var decoded struct {
		Count int `json:"count"`
		Items []struct {
			Loop struct {
				Seq int64 `json:"seq"`
			} `json:"loop"`
			Diagnosis struct {
				FailureClass string `json:"failureClass"`
			} `json:"diagnosis"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v\noutput=%q", err, stdout)
	}
	if decoded.Count != 1 || len(decoded.Items) != 1 || decoded.Items[0].Loop.Seq != 7 || decoded.Items[0].Diagnosis.FailureClass != "github_transient" {
		t.Fatalf("loop failures output = %#v, want one failed reviewer loop", decoded)
	}
}

func TestLoopFailuresIncludesPausedManualInterventionLoops(t *testing.T) {
	t.Parallel()

	configPath := writeLoopDiagnosticsFixture(t, "")
	exitCode, stdout, stderr := runApp(t, "loop", "failures", "--type", "worker", "--json", "--config", configPath)
	if exitCode != 0 {
		t.Fatalf("Run([loop failures]) exit code = %d, want 0; stderr=%q", exitCode, stderr)
	}
	var decoded struct {
		Count int `json:"count"`
		Items []struct {
			Loop struct {
				Seq    int64  `json:"seq"`
				Status string `json:"status"`
			} `json:"loop"`
			LatestQueueItem struct {
				Status string `json:"status"`
			} `json:"latestQueueItem"`
			Diagnosis struct {
				FailureClass string `json:"failureClass"`
			} `json:"diagnosis"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v\noutput=%q", err, stdout)
	}
	// Queue status is manual_intervention (operator hold), but LastErrorKind is
	// non_retryable — FailureClass must preserve the structured kind.
	if decoded.Count != 1 || len(decoded.Items) != 1 || decoded.Items[0].Loop.Seq != 8 || decoded.Items[0].Loop.Status != "paused" || decoded.Items[0].LatestQueueItem.Status != "manual_intervention" || decoded.Items[0].Diagnosis.FailureClass != "non_retryable" {
		t.Fatalf("loop failures output = %#v, want one paused manual-hold worker loop with non_retryable class", decoded)
	}
}

func TestDescribeAliasesLoopInspect(t *testing.T) {
	t.Parallel()

	configPath := writeLoopDiagnosticsFixture(t, "")
	exitCode, stdout, stderr := runApp(t, "describe", "run_reviewer_failed", "--json", "--config", configPath)
	if exitCode != 0 {
		t.Fatalf("Run([describe]) exit code = %d, want 0; stderr=%q", exitCode, stderr)
	}
	var decoded struct {
		SelectorKind string `json:"selectorKind"`
		Loop         struct {
			Seq int64 `json:"seq"`
		} `json:"loop"`
		Run struct {
			ID string `json:"id"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v\noutput=%q", err, stdout)
	}
	if decoded.SelectorKind != "runId" || decoded.Loop.Seq != 7 || decoded.Run.ID != "run_reviewer_failed" {
		t.Fatalf("describe output = %#v, want same resolution as loop inspect", decoded)
	}
}

func TestDescribeHumanShowsManualInterventionReason(t *testing.T) {
	t.Parallel()

	configPath := writeLoopDiagnosticsFixture(t, "")
	exitCode, stdout, stderr := runApp(t, "describe", "8", "--config", configPath)
	if exitCode != 0 {
		t.Fatalf("Run([describe 8]) exit code = %d, want 0; stderr=%q", exitCode, stderr)
	}
	for _, want := range []string{"worktree is locked", "manual_intervention", "retry"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("describe human output = %q, want to contain %q", stdout, want)
		}
	}
}

func TestClassifyDiagnosticMessageManualIntervention(t *testing.T) {
	t.Parallel()

	got := classifyDiagnosticMessage("dirty worktree: uncommitted changes", "manual_intervention")
	if got.FailureClass != "manual_intervention" || got.Retryable == nil || *got.Retryable {
		t.Fatalf("diagnosis = %#v, want non-retryable manual_intervention", got)
	}
	if !strings.Contains(got.RecommendedAction, "discard") || !strings.Contains(got.RecommendedAction, "retry") {
		t.Fatalf("RecommendedAction = %q, want dirty-worktree discard/retry guidance", got.RecommendedAction)
	}

	locked := classifyDiagnosticMessage("fatal: worktree is locked", "manual_intervention")
	if strings.Contains(locked.RecommendedAction, "discard") {
		t.Fatalf("RecommendedAction = %q, must not recommend discard for locked worktree", locked.RecommendedAction)
	}
	if !strings.Contains(locked.RecommendedAction, "unlock") {
		t.Fatalf("RecommendedAction = %q, want unlock guidance for locked worktree", locked.RecommendedAction)
	}

	// Production stores ErrUnusableWorktreePreserved as LastErrorKind=manual_intervention;
	// message-specific integrity class must win over the generic MI branch.
	preserved := classifyDiagnosticMessage(
		"worktree path /tmp/wt is unusable and not empty; manual intervention required: unusable worktree path preserved",
		"manual_intervention",
	)
	if preserved.FailureClass != "managed_worktree_integrity" || preserved.Retryable == nil || *preserved.Retryable {
		t.Fatalf("preserved diagnosis = %#v, want non-retryable managed_worktree_integrity", preserved)
	}
	if !strings.Contains(preserved.RecommendedAction, "preserve any agent output") {
		t.Fatalf("RecommendedAction = %q, want managed-worktree cleanup guidance", preserved.RecommendedAction)
	}
}

func TestDiagnoseLoopExpandsRetrySeqPlaceholder(t *testing.T) {
	t.Parallel()

	kind := "manual_intervention"
	msg := "dirty worktree: uncommitted changes"
	queue := &storage.QueueItemRecord{Status: "manual_intervention", LastError: &msg, LastErrorKind: &kind}
	run := &storage.RunRecord{Status: "failed", ErrorMessage: &msg}
	got := diagnoseLoop(storage.LoopRecord{Seq: 42, Status: "paused"}, run, queue, loopDiagnosticMetadata{}, true)
	if strings.Contains(got.RecommendedAction, "<seq>") {
		t.Fatalf("RecommendedAction = %q, want expanded loop seq not literal <seq>", got.RecommendedAction)
	}
	if !strings.Contains(got.RecommendedAction, "looper retry 42") {
		t.Fatalf("RecommendedAction = %q, want looper retry 42", got.RecommendedAction)
	}

	paused := diagnoseLoop(storage.LoopRecord{Seq: 7, Status: "paused"}, nil, nil, loopDiagnosticMetadata{}, false)
	if strings.Contains(paused.RecommendedAction, "<seq>") {
		t.Fatalf("paused RecommendedAction = %q, want expanded seq", paused.RecommendedAction)
	}
	if !strings.Contains(paused.RecommendedAction, "looper unpause 7") || !strings.Contains(paused.RecommendedAction, "looper describe 7") {
		t.Fatalf("paused RecommendedAction = %q, want unpause/describe with seq 7", paused.RecommendedAction)
	}
}

func TestDiagnoseQueueItemDoesNotEmitSeqPlaceholder(t *testing.T) {
	t.Parallel()

	kind := "manual_intervention"
	msg := "dirty worktree: uncommitted changes"
	item := storage.QueueItemRecord{Status: "manual_intervention", LastError: &msg, LastErrorKind: &kind}

	withSeq := diagnoseQueueItem(item, 3)
	if strings.Contains(withSeq.RecommendedAction, "<seq>") {
		t.Fatalf("RecommendedAction = %q, want expanded seq not literal <seq>", withSeq.RecommendedAction)
	}
	if !strings.Contains(withSeq.RecommendedAction, "looper retry 3") {
		t.Fatalf("RecommendedAction = %q, want looper retry 3", withSeq.RecommendedAction)
	}

	withoutSeq := diagnoseQueueItem(item, 0)
	if strings.Contains(withoutSeq.RecommendedAction, "<seq>") {
		t.Fatalf("RecommendedAction = %q, must not leak unresolved <seq> when loop seq is unknown", withoutSeq.RecommendedAction)
	}
	if !strings.Contains(withoutSeq.RecommendedAction, "retry the owning loop") {
		t.Fatalf("RecommendedAction = %q, want owning-loop retry guidance without placeholder", withoutSeq.RecommendedAction)
	}
}

func TestDiagnoseLoopPreservesErrorKindWhenQueueIsManualHold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		kind      string
		message   string
		wantClass string
		retryable bool
	}{
		{name: "retryable_transient", kind: "retryable_transient", message: `Post "https://api.github.com/graphql": EOF`, wantClass: "github_transient", retryable: true},
		{name: "retryable_after_resume", kind: "retryable_after_resume", message: "agent interrupted", wantClass: "retryable_after_resume", retryable: true},
		{name: "non_retryable", kind: "non_retryable", message: "fatal: worktree is locked", wantClass: "non_retryable", retryable: false},
		{name: "manual_intervention kind", kind: "manual_intervention", message: "dirty worktree: uncommitted changes", wantClass: "manual_intervention", retryable: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind := tt.kind
			msg := tt.message
			queue := &storage.QueueItemRecord{Status: "manual_intervention", LastError: &msg, LastErrorKind: &kind}
			run := &storage.RunRecord{Status: "failed", ErrorMessage: &msg}
			got := diagnoseLoop(storage.LoopRecord{Status: "paused"}, run, queue, loopDiagnosticMetadata{}, true)
			if got.FailureClass != tt.wantClass {
				t.Fatalf("FailureClass = %q, want %q", got.FailureClass, tt.wantClass)
			}
			if got.Retryable == nil || *got.Retryable != tt.retryable {
				t.Fatalf("Retryable = %#v, want %v", got.Retryable, tt.retryable)
			}
		})
	}
}

func TestDiagnoseLoopCheckpointOnlyManualHoldUsesResumePolicy(t *testing.T) {
	t.Parallel()

	// Paused loops included via checkpoint resumePolicy with no parked queue
	// must classify as manual_intervention, not unknown.
	msg := "dirty worktree: uncommitted changes"
	checkpoint := `{"resumePolicy":"manual_intervention"}`
	run := &storage.RunRecord{Status: "failed", ErrorMessage: &msg, CheckpointJSON: &checkpoint}

	got := diagnoseLoop(storage.LoopRecord{Seq: 9, Status: "paused"}, run, nil, loopDiagnosticMetadata{}, true)
	if got.FailureClass != "manual_intervention" {
		t.Fatalf("FailureClass = %q, want manual_intervention for checkpoint-only hold", got.FailureClass)
	}
	if got.Retryable == nil || *got.Retryable {
		t.Fatalf("Retryable = %#v, want false for manual_intervention", got.Retryable)
	}
	if !strings.Contains(got.RecommendedAction, "looper retry 9") {
		t.Fatalf("RecommendedAction = %q, want operator-hold retry guidance for seq 9", got.RecommendedAction)
	}
	if got.Source != "run" || !strings.Contains(got.Message, "dirty worktree") {
		t.Fatalf("diagnosis = %#v, want run-sourced dirty worktree message", got)
	}

	// Queue LastErrorKind still wins when present (do not overwrite with policy).
	queueKind := "non_retryable"
	queue := &storage.QueueItemRecord{Status: "manual_intervention", LastError: &msg, LastErrorKind: &queueKind}
	withQueue := diagnoseLoop(storage.LoopRecord{Seq: 9, Status: "paused"}, run, queue, loopDiagnosticMetadata{}, true)
	if withQueue.FailureClass != "non_retryable" {
		t.Fatalf("FailureClass = %q, want non_retryable from queue kind over resumePolicy", withQueue.FailureClass)
	}
}

func TestDiagnoseLoopRunSelectorIgnoresLatestQueueKind(t *testing.T) {
	t.Parallel()

	runMsg := `Post "https://api.github.com/graphql": EOF`
	queueMsg := "dirty worktree: uncommitted changes"
	queueKind := "manual_intervention"
	run := &storage.RunRecord{Status: "failed", ErrorMessage: &runMsg}
	queue := &storage.QueueItemRecord{Status: "manual_intervention", LastError: &queueMsg, LastErrorKind: &queueKind}

	got := diagnoseLoop(storage.LoopRecord{Status: "paused"}, run, queue, loopDiagnosticMetadata{}, false)
	if got.FailureClass != "github_transient" {
		t.Fatalf("FailureClass = %q, want github_transient from historical run only", got.FailureClass)
	}
	if got.Source != "run" || !strings.Contains(got.Message, "api.github.com") {
		t.Fatalf("diagnosis = %#v, want run-sourced github message", got)
	}
}

func TestDiagnoseLoopRunSelectorIgnoresLoopMetadataLastFailure(t *testing.T) {
	t.Parallel()

	// Historical run succeeded (or has no error); loop metadata still carries a
	// later run's lastFailure. Run-id diagnosis must not adopt that signal.
	laterFailure := "dirty worktree: uncommitted changes from a later run"
	metadata := loopDiagnosticMetadata{
		Loop: &loopDiagnosticLoopMetadata{LastFailure: &laterFailure},
	}
	run := &storage.RunRecord{Status: "succeeded"}
	queueMsg := "queue error from current hold"
	queueKind := "manual_intervention"
	queue := &storage.QueueItemRecord{Status: "manual_intervention", LastError: &queueMsg, LastErrorKind: &queueKind}

	got := diagnoseLoop(storage.LoopRecord{Status: "paused"}, run, queue, metadata, false)
	if got.Source == "loopMetadata" || strings.Contains(got.Message, laterFailure) {
		t.Fatalf("diagnosis = %#v, want no loop metadata lastFailure for run-id selector", got)
	}
	if got.Source == "queueItem" || strings.Contains(got.Message, queueMsg) {
		t.Fatalf("diagnosis = %#v, want no latest queue error for run-id selector", got)
	}
	if got.Message != "" && got.Source != "run" {
		t.Fatalf("diagnosis = %#v, want empty or run-only diagnosis for successful historical run", got)
	}
}

func TestRecommendedActionForPausedIsNotAlwaysRetry(t *testing.T) {
	t.Parallel()

	got := recommendedActionForState("paused")
	if strings.Contains(got, "retry") && !strings.Contains(got, "unpause") {
		t.Fatalf("paused action = %q, want unpause/describe guidance not forced retry", got)
	}
	if !strings.Contains(got, "unpause") {
		t.Fatalf("paused action = %q, want unpause guidance", got)
	}
}

func TestDiagnoseLoopBudgetHoldRecommendsUnpauseStop(t *testing.T) {
	t.Parallel()
	meta := `{"pauseReason":"review_fix_budget_exhausted","reviewFixBudget":{"exhaustedBy":"reviewer","pauseReason":"review_fix_budget_exhausted"}}`
	loop := storage.LoopRecord{ID: "loop_budget", Seq: 12, Type: "reviewer", Status: "paused", MetadataJSON: &meta}
	parsed := parseLoopDiagnosticMetadata(loop.MetadataJSON)
	if parsed.PauseReason == nil || *parsed.PauseReason != "review_fix_budget_exhausted" {
		t.Fatalf("PauseReason = %#v, want review_fix_budget_exhausted", parsed.PauseReason)
	}
	diagnosis := diagnoseLoop(loop, nil, nil, parsed, true)
	if !strings.Contains(diagnosis.RecommendedAction, "looper unpause 12") || !strings.Contains(diagnosis.RecommendedAction, "looper stop 12") {
		t.Fatalf("RecommendedAction = %q, want unpause/stop", diagnosis.RecommendedAction)
	}
	if strings.Contains(diagnosis.RecommendedAction, "retry") {
		t.Fatalf("RecommendedAction = %q, must not recommend retry for budget hold", diagnosis.RecommendedAction)
	}
	handoff := reviewFixInspectHandoff(loop, parsed)
	if handoff == nil || handoff.Kind != "review_fix_budget" {
		t.Fatalf("handoff = %#v, want review_fix_budget", handoff)
	}
	var buf bytes.Buffer
	if err := writeHumanLoopInspect(&buf, loopInspectOutput{
		Loop:      diagnosticLoopOutput(loop),
		Metadata:  parsed,
		Handoff:   handoff,
		Diagnosis: diagnosis,
	}); err != nil {
		t.Fatalf("writeHumanLoopInspect: %v", err)
	}
	got := buf.String()
	if !strings.Contains(got, "Hold: review_fix_budget_exhausted") {
		t.Fatalf("human inspect = %q, want Hold reason", got)
	}
	if !strings.Contains(got, "Release: looper unpause 12 / looper stop 12") {
		t.Fatalf("human inspect = %q, want Release commands", got)
	}
	if strings.Contains(got, "looper retry 12") {
		t.Fatalf("human inspect = %q, must not suggest retry", got)
	}
}

func TestDiagnoseLoopBudgetHoldDoesNotClassifyHistoricalGitHubTransient(t *testing.T) {
	t.Parallel()
	meta := `{"pauseReason":"review_fix_budget_exhausted","reviewFixBudget":{"exhaustedBy":"reviewer","pauseReason":"review_fix_budget_exhausted"}}`
	loop := storage.LoopRecord{ID: "loop_budget", Seq: 12, Type: "reviewer", Status: "paused", MetadataJSON: &meta}
	runMsg := "Command exited with code 1: error connecting to api.github.com\ncheck your internet connection or https://githubstatus.com"
	run := &storage.RunRecord{Status: "failed", ErrorMessage: &runMsg}
	got := diagnoseLoop(loop, run, nil, parseLoopDiagnosticMetadata(loop.MetadataJSON), true)
	if got.FailureClass != "review_fix_budget" {
		t.Fatalf("FailureClass = %q, want review_fix_budget not historical github_transient", got.FailureClass)
	}
	if got.Source != "loop" || got.Message != "review-fix budget exhausted" {
		t.Fatalf("diagnosis = %#v, want current budget hold source and message", got)
	}
	if !strings.Contains(got.RecommendedAction, "looper unpause 12") || !strings.Contains(got.RecommendedAction, "looper stop 12") {
		t.Fatalf("RecommendedAction = %q, want unpause/stop", got.RecommendedAction)
	}
	if strings.Contains(strings.ToLower(got.RecommendedAction), "github") {
		t.Fatalf("RecommendedAction = %q, must not send operators to GitHub recovery", got.RecommendedAction)
	}
}

func TestDiagnoseLoopBudgetHoldMessageIsRoleNeutralForFixerExhaustion(t *testing.T) {
	t.Parallel()
	meta := `{"pauseReason":"sibling_review_fix_budget","reviewFixBudget":{"siblingOf":"fixer","pauseReason":"sibling_review_fix_budget"}}`
	loop := storage.LoopRecord{ID: "loop_budget_reviewer_sibling", Seq: 4, Type: "reviewer", Status: "paused", MetadataJSON: &meta}
	got := diagnoseLoop(loop, nil, nil, parseLoopDiagnosticMetadata(loop.MetadataJSON), true)
	if got.FailureClass != "review_fix_budget" {
		t.Fatalf("FailureClass = %q, want review_fix_budget", got.FailureClass)
	}
	if got.Message != "review-fix budget exhausted" || strings.Contains(got.Message, "publish") {
		t.Fatalf("Message = %q, want role-neutral review-fix budget exhausted", got.Message)
	}
}

func TestWriteHumanLoopInspectScopeHoldUsesRelease(t *testing.T) {
	t.Parallel()
	meta := `{"pauseReason":"review_scope_human_required","reviewScopeHuman":{"heldBy":"reviewer","pauseReason":"review_scope_human_required"}}`
	loop := storage.LoopRecord{ID: "loop_scope", Seq: 9, Type: "reviewer", Status: "paused", MetadataJSON: &meta}
	parsed := parseLoopDiagnosticMetadata(loop.MetadataJSON)
	diagnosis := diagnoseLoop(loop, nil, nil, parsed, true)
	handoff := reviewFixInspectHandoff(loop, parsed)
	if handoff == nil || handoff.Kind != "review_scope_human" {
		t.Fatalf("handoff = %#v, want review_scope_human", handoff)
	}
	var buf bytes.Buffer
	if err := writeHumanLoopInspect(&buf, loopInspectOutput{
		Loop:      diagnosticLoopOutput(loop),
		Metadata:  parsed,
		Handoff:   handoff,
		Diagnosis: diagnosis,
	}); err != nil {
		t.Fatalf("writeHumanLoopInspect: %v", err)
	}
	got := buf.String()
	if !strings.Contains(got, "Hold: review_scope_human_required") {
		t.Fatalf("human inspect = %q, want Hold reason", got)
	}
	if !strings.Contains(got, "Release: looper unpause 9 / looper stop 9") {
		t.Fatalf("human inspect = %q, want Release commands", got)
	}
	if !strings.Contains(got, "unpause releases the scope hold; other blockers remain, such as unanswered agent/HITL asks, failed or interrupted runs, human takeover, or a manual pause.") {
		t.Fatalf("human inspect = %q, want independent-blocker note", got)
	}
	if strings.Contains(got, "Resume:") {
		t.Fatalf("human inspect = %q, must use Release not Resume", got)
	}
}

func TestDiagnoseLoopHITLBudgetAskPrefixesContinueStop(t *testing.T) {
	t.Parallel()
	meta := `{"hitl":{"kind":"review_fix_budget","status":"awaiting","options":["Continue","Stop"]}}`
	loop := storage.LoopRecord{ID: "loop_hitl", Seq: 15, Type: "reviewer", Status: "awaiting_human", MetadataJSON: &meta}
	got := diagnoseLoop(loop, nil, nil, parseLoopDiagnosticMetadata(loop.MetadataJSON), true)
	if !strings.Contains(got.RecommendedAction, "Continue / Stop") {
		t.Fatalf("RecommendedAction = %q, want Continue/Stop prefix", got.RecommendedAction)
	}
	if !strings.Contains(got.RecommendedAction, "looper unpause 15") || !strings.Contains(got.RecommendedAction, "looper stop 15") {
		t.Fatalf("RecommendedAction = %q, want unpause/stop commands", got.RecommendedAction)
	}
}

func TestDiagnoseLoopOrdinaryPauseIsNotPairStop(t *testing.T) {
	t.Parallel()
	loop := storage.LoopRecord{ID: "loop_paused", Seq: 4, Type: "worker", Status: "paused"}
	got := diagnoseLoop(loop, nil, nil, loopDiagnosticMetadata{}, true)
	if !strings.Contains(got.RecommendedAction, "looper unpause 4") || !strings.Contains(got.RecommendedAction, "looper describe 4") {
		t.Fatalf("RecommendedAction = %q, want unpause/describe", got.RecommendedAction)
	}
	if strings.Contains(got.RecommendedAction, "looper stop") {
		t.Fatalf("RecommendedAction = %q, must not use pair stop", got.RecommendedAction)
	}
	if reviewFixInspectHandoff(loop, loopDiagnosticMetadata{}) != nil {
		t.Fatal("handoff must be nil for ordinary pause")
	}
}

func TestDiagnoseLoopRunSelectorDoesNotRewritePairHoldAction(t *testing.T) {
	t.Parallel()
	meta := `{"pauseReason":"review_fix_budget_exhausted","reviewFixBudget":{"exhaustedBy":"reviewer","pauseReason":"review_fix_budget_exhausted"}}`
	loop := storage.LoopRecord{ID: "loop_budget", Seq: 12, Type: "reviewer", Status: "paused", MetadataJSON: &meta}
	runMsg := `Post "https://api.github.com/graphql": EOF`
	run := &storage.RunRecord{Status: "failed", ErrorMessage: &runMsg}
	got := diagnoseLoop(loop, run, nil, parseLoopDiagnosticMetadata(loop.MetadataJSON), false)
	if got.FailureClass != "github_transient" {
		t.Fatalf("FailureClass = %q, want github_transient from historical run", got.FailureClass)
	}
	if strings.Contains(got.RecommendedAction, "unpause") || strings.Contains(got.RecommendedAction, "looper stop") {
		t.Fatalf("RecommendedAction = %q, must not rewrite historical-run action", got.RecommendedAction)
	}
}

func TestTruncateCLITextIsRuneSafeAndSingleLine(t *testing.T) {
	t.Parallel()

	// 10 runes of multi-byte text + control/newline collapse.
	got := truncateCLIText("你好世界测试文本更长\nline2\twith\ttabs", 8)
	if strings.Contains(got, "\n") || strings.Contains(got, "\t") {
		t.Fatalf("truncateCLIText = %q, want single-line sanitized text", got)
	}
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("truncateCLIText = %q, want ellipsis suffix", got)
	}
	if len([]rune(got)) != 8 {
		t.Fatalf("truncateCLIText rune len = %d, want 8 (including ...)", len([]rune(got)))
	}
}

func TestLogsAcceptsRunIDFromPSOutput(t *testing.T) {
	t.Parallel()

	var requestedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		writeEnvelope(t, w, pkgapi.Success("req_logs", map[string]any{
			"seq":        7,
			"loopId":     "loop_failed",
			"loopType":   "reviewer",
			"loopStatus": "failed",
			"run":        map[string]any{"runId": "run_reviewer_failed", "status": "failed", "currentStep": "snapshot"},
			"agent":      map[string]any{"executionId": "agent_failed", "vendor": "claude-code", "status": "failed", "stdout": "review output", "stderr": ""},
		}))
	}))
	defer server.Close()

	configPath := writeLoopDiagnosticsFixture(t, server.URL)
	exitCode, stdout, stderr := runApp(t, "logs", "run_reviewer_failed", "--config", configPath)
	if exitCode != 0 {
		t.Fatalf("Run([logs run_reviewer_failed]) exit code = %d, want 0; stderr=%q", exitCode, stderr)
	}
	if requestedPath != "/api/v1/runs/run_reviewer_failed/logs" {
		t.Fatalf("requested path = %q, want run-scoped logs path", requestedPath)
	}
	for _, want := range []string{"Loop #7 · reviewer · failed", "Run run_reviewer_failed · step: snapshot", "review output"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout = %q, want to contain %q", stdout, want)
		}
	}
}

func TestLogsRejectsRunIDFollow(t *testing.T) {
	t.Parallel()

	configPath := writeLoopDiagnosticsFixture(t, "")
	exitCode, _, stderr := runApp(t, "logs", "run_reviewer_failed", "--follow", "--config", configPath)
	if exitCode == 0 {
		t.Fatal("Run([logs run_reviewer_failed --follow]) exit code = 0, want non-zero")
	}
	if !strings.Contains(stderr, "run-scoped logs cannot be followed") {
		t.Fatalf("stderr = %q, want run-scoped follow error", stderr)
	}
}

func writeLoopDiagnosticsFixture(t *testing.T, serverURL string) string {
	t.Helper()
	root := t.TempDir()
	dbPath := filepath.Join(root, "looper.sqlite")
	coordinator, err := storage.OpenSQLiteCoordinator(context.Background(), dbPath, storage.SQLiteCoordinatorOptions{Migrations: storage.EmbeddedMigrations, BackupDir: filepath.Join(root, "backups")})
	if err != nil {
		t.Fatalf("OpenSQLiteCoordinator() error = %v", err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })
	if _, err := coordinator.MigrationRunner().RunPending(context.Background()); err != nil {
		t.Fatalf("RunPending() error = %v", err)
	}

	repos := storage.NewRepositories(coordinator.DB())
	now := "2026-04-11T12:00:00.000Z"
	projectID := "project_loop_diagnostics"
	if err := repos.Projects.Upsert(context.Background(), storage.ProjectRecord{ID: projectID, Name: "Looper", RepoPath: "/tmp/looper", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("Projects.Upsert() error = %v", err)
	}
	prNumber := int64(42)
	targetID := "pr:acme/looper:42"
	metadata := `{"followUpdates":true,"lastPublishedAt":"2026-04-11T11:00:00.000Z","lastReviewSummary":"previous review","loop":{"status":"active","lastStatus":"failed","consecutiveFailures":2,"failureCount":2,"lastFailure":"Command exited with code 1: Post \"https://api.github.com/graphql\": EOF"}}`
	if err := repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_failed", Seq: 7, ProjectID: projectID, Type: "reviewer", TargetType: "pull_request", TargetID: &targetID, Repo: stringPtr("acme/looper"), PRNumber: &prNumber, Status: "failed", MetadataJSON: &metadata, LastRunAt: &now, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	errorMessage := `Command exited with code 1: Post "https://api.github.com/graphql": EOF`
	if err := repos.Runs.Upsert(context.Background(), storage.RunRecord{ID: "run_reviewer_failed", LoopID: "loop_failed", Status: "failed", CurrentStep: stringPtr("snapshot"), ErrorMessage: &errorMessage, StartedAt: now, LastHeartbeatAt: &now, EndedAt: &now, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	lastErrorKind := "retryable_transient"
	if err := repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_failed", ProjectID: &projectID, LoopID: stringPtr("loop_failed"), Type: "reviewer", TargetType: "pull_request", TargetID: targetID, Repo: stringPtr("acme/looper"), PRNumber: &prNumber, DedupeKey: "reviewer:failed", Priority: 1, Status: "failed", AvailableAt: now, Attempts: 2, MaxAttempts: 5, LastError: &errorMessage, LastErrorKind: &lastErrorKind, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	workerTargetID := "project:acme/looper"
	workerMetadata := `{"loop":{"lastFailure":"fatal: worktree is locked"}}`
	if err := repos.Loops.Upsert(context.Background(), storage.LoopRecord{ID: "loop_paused_action_required", Seq: 8, ProjectID: projectID, Type: "worker", TargetType: "project", TargetID: &workerTargetID, Repo: stringPtr("acme/looper"), Status: "paused", MetadataJSON: &workerMetadata, LastRunAt: &now, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("Loops.Upsert() error = %v", err)
	}
	workerErrorMessage := "fatal: worktree is locked"
	if err := repos.Runs.Upsert(context.Background(), storage.RunRecord{ID: "run_worker_paused", LoopID: "loop_paused_action_required", Status: "failed", CurrentStep: stringPtr("prepare_work"), ErrorMessage: &workerErrorMessage, StartedAt: now, LastHeartbeatAt: &now, EndedAt: &now, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("Runs.Upsert() error = %v", err)
	}
	workerErrorKind := "non_retryable"
	if err := repos.Queue.Upsert(context.Background(), storage.QueueItemRecord{ID: "queue_worker_manual_intervention", ProjectID: &projectID, LoopID: stringPtr("loop_paused_action_required"), Type: "worker", TargetType: "project", TargetID: workerTargetID, Repo: stringPtr("acme/looper"), DedupeKey: "worker:manual", Priority: 1, Status: "manual_intervention", AvailableAt: now, Attempts: 3, MaxAttempts: 3, LastError: &workerErrorMessage, LastErrorKind: &workerErrorKind, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("Queue.Upsert() error = %v", err)
	}
	pid := int64(1234)
	if err := repos.AgentExecutions.Upsert(context.Background(), storage.AgentExecutionRecord{ID: "agent_failed", ProjectID: &projectID, LoopID: stringPtr("loop_failed"), RunID: stringPtr("run_reviewer_failed"), Vendor: "claude-code", Status: "failed", PID: &pid, ErrorMessage: &errorMessage, StartedAt: now, EndedAt: &now, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("AgentExecutions.Upsert() error = %v", err)
	}

	configPayload := map[string]any{"storage": map[string]any{"dbPath": dbPath}}
	if serverURL != "" {
		configPayload["server"] = map[string]any{"baseUrl": serverURL, "authMode": "none"}
	}
	configPath := filepath.Join(root, "config.json")
	raw, err := json.Marshal(configPayload)
	if err != nil {
		t.Fatalf("json.Marshal(config) error = %v", err)
	}
	if err := os.WriteFile(configPath, raw, 0o644); err != nil {
		t.Fatalf("WriteFile(config) error = %v", err)
	}
	return configPath
}
