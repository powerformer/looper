package cliapp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/nexu-io/looper/internal/eventlog"
	"github.com/nexu-io/looper/internal/loops"
	"github.com/nexu-io/looper/internal/storage"
	"github.com/spf13/cobra"
)

const defaultDiagnosticsLimit int64 = 100

type loopInspectOutput struct {
	NowISO          string                  `json:"nowIso"`
	Selector        string                  `json:"selector"`
	SelectorKind    string                  `json:"selectorKind"`
	Loop            loopDiagnosticLoop      `json:"loop"`
	Metadata        loopDiagnosticMetadata  `json:"metadata"`
	Handoff         *loopInspectHandoff     `json:"handoff,omitempty"`
	Run             *loopDiagnosticRun      `json:"run,omitempty"`
	LatestQueueItem *queueItemCommandOutput `json:"latestQueueItem,omitempty"`
	Agent           *loopDiagnosticAgent    `json:"agent,omitempty"`
	Diagnosis       loopDiagnosis           `json:"diagnosis"`
}

type loopInspectHandoff struct {
	Kind        string  `json:"kind"`
	PauseReason *string `json:"pauseReason,omitempty"`
	Resume      string  `json:"resume"`
}

type loopFailuresOutput struct {
	NowISO    string              `json:"nowIso"`
	Type      string              `json:"type,omitempty"`
	ProjectID string              `json:"projectId,omitempty"`
	Limit     int64               `json:"limit"`
	Count     int                 `json:"count"`
	Items     []loopInspectOutput `json:"items"`
}

type queueFailedOutput struct {
	NowISO    string                  `json:"nowIso"`
	Type      string                  `json:"type,omitempty"`
	ProjectID string                  `json:"projectId,omitempty"`
	Limit     int64                   `json:"limit"`
	Count     int                     `json:"count"`
	Items     []queueFailedItemOutput `json:"items"`
}

type queueFailedItemOutput struct {
	QueueItem queueItemCommandOutput `json:"queueItem"`
	Diagnosis loopDiagnosis          `json:"diagnosis"`
}

type loopDiagnosticLoop struct {
	ID        string               `json:"id"`
	Seq       int64                `json:"seq"`
	ProjectID string               `json:"projectId"`
	Type      string               `json:"type"`
	Status    string               `json:"status"`
	Target    loopDiagnosticTarget `json:"target"`
	LastRunAt *string              `json:"lastRunAt,omitempty"`
	NextRunAt *string              `json:"nextRunAt,omitempty"`
	CreatedAt string               `json:"createdAt"`
	UpdatedAt string               `json:"updatedAt"`
}

type loopDiagnosticTarget struct {
	Type     string  `json:"type"`
	ID       *string `json:"id,omitempty"`
	Repo     *string `json:"repo,omitempty"`
	PRNumber *int64  `json:"prNumber,omitempty"`
	Label    string  `json:"label"`
}

type loopDiagnosticMetadata struct {
	DecodeError          *string                     `json:"decodeError,omitempty"`
	FollowUpdates        *bool                       `json:"followUpdates,omitempty"`
	LastPublishedAt      *string                     `json:"lastPublishedAt,omitempty"`
	LastPublishedHeadSHA *string                     `json:"lastPublishedHeadSha,omitempty"`
	LastReviewEvent      *string                     `json:"lastReviewEvent,omitempty"`
	LastReviewSummary    *string                     `json:"lastReviewSummary,omitempty"`
	PauseReason          *string                     `json:"pauseReason,omitempty"`
	LastFilterSkip       map[string]any              `json:"lastFilterSkip,omitempty"`
	Loop                 *loopDiagnosticLoopMetadata `json:"loop,omitempty"`
}

type loopDiagnosticLoopMetadata struct {
	Status                 *string `json:"status,omitempty"`
	LastStatus             *string `json:"lastStatus,omitempty"`
	ConsecutiveFailures    *int64  `json:"consecutiveFailures,omitempty"`
	FailureCount           *int64  `json:"failureCount,omitempty"`
	AutoRecoveryAttempts   *int64  `json:"autoRecoveryAttempts,omitempty"`
	LastAutoRecoveryReason *string `json:"lastAutoRecoveryReason,omitempty"`
	LastFailure            *string `json:"lastFailure,omitempty"`
	LastReviewedHeadSHA    *string `json:"lastReviewedHeadSha,omitempty"`
	LastOutputFingerprint  *string `json:"lastOutputFingerprint,omitempty"`
	IterationCount         *int64  `json:"iterationCount,omitempty"`
	AgentExecutionCount    *int64  `json:"agentExecutionCount,omitempty"`
	TerminationReason      *string `json:"terminationReason,omitempty"`
	MinPublishIntervalSecs *int64  `json:"minPublishIntervalSeconds,omitempty"`
	QuietPeriodSeconds     *int64  `json:"quietPeriodSeconds,omitempty"`
}

type loopDiagnosticRun struct {
	ID                  string  `json:"id"`
	LoopID              string  `json:"loopId"`
	Status              string  `json:"status"`
	CurrentStep         *string `json:"currentStep,omitempty"`
	LastCompletedStep   *string `json:"lastCompletedStep,omitempty"`
	Summary             *string `json:"summary,omitempty"`
	ErrorMessage        *string `json:"errorMessage,omitempty"`
	ResumePolicy        *string `json:"resumePolicy,omitempty"`
	StartedAt           string  `json:"startedAt"`
	LastHeartbeatAt     *string `json:"lastHeartbeatAt,omitempty"`
	EndedAt             *string `json:"endedAt,omitempty"`
	ElapsedSeconds      *int64  `json:"elapsedSeconds,omitempty"`
	HeartbeatAgeSeconds *int64  `json:"heartbeatAgeSeconds,omitempty"`
	CreatedAt           string  `json:"createdAt"`
	UpdatedAt           string  `json:"updatedAt"`
}

type loopDiagnosticAgent struct {
	ID                  string  `json:"id"`
	Vendor              string  `json:"vendor"`
	Status              string  `json:"status"`
	PID                 *int64  `json:"pid,omitempty"`
	Summary             *string `json:"summary,omitempty"`
	ParseStatus         *string `json:"parseStatus,omitempty"`
	CompletionSignal    *string `json:"completionSignal,omitempty"`
	HeartbeatCount      int64   `json:"heartbeatCount"`
	LastHeartbeatAt     *string `json:"lastHeartbeatAt,omitempty"`
	HeartbeatAgeSeconds *int64  `json:"heartbeatAgeSeconds,omitempty"`
	ErrorMessage        *string `json:"errorMessage,omitempty"`
	NativeSessionID     *string `json:"nativeSessionId,omitempty"`
	NativeResumeMode    *string `json:"nativeResumeMode,omitempty"`
	NativeResumeStatus  *string `json:"nativeResumeStatus,omitempty"`
	NativeResumeError   *string `json:"nativeResumeError,omitempty"`
	StartedAt           string  `json:"startedAt"`
	EndedAt             *string `json:"endedAt,omitempty"`
	CreatedAt           string  `json:"createdAt"`
	UpdatedAt           string  `json:"updatedAt"`
}

type loopDiagnosis struct {
	State             string `json:"state"`
	Source            string `json:"source,omitempty"`
	FailureClass      string `json:"failureClass,omitempty"`
	Retryable         *bool  `json:"retryable,omitempty"`
	Message           string `json:"message,omitempty"`
	RecommendedAction string `json:"recommendedAction,omitempty"`
}

type loopSelectorResult struct {
	Loop         storage.LoopRecord
	Run          *storage.RunRecord
	SelectorKind string
}

func (r *commandRuntime) queueFailed(cmd *cobra.Command, args []string) error {
	_ = args
	limit, err := diagnosticsLimit(cmd)
	if err != nil {
		return err
	}
	typeFilter := strings.TrimSpace(getStringFlag(cmd, "type"))
	projectFilter := strings.TrimSpace(getStringFlag(cmd, "project"))
	return r.withLocalRepositories(cmd.Context(), func(repos *storage.Repositories) error {
		items, err := repos.Queue.List(cmd.Context())
		if err != nil {
			return err
		}
		output := queueFailedOutput{NowISO: eventlog.FormatJavaScriptISOString(time.Now().UTC()), Type: typeFilter, ProjectID: projectFilter, Limit: limit}
		loopSeqByID := map[string]int64{}
		for _, item := range items {
			if item.Status != "failed" && item.Status != "manual_intervention" {
				continue
			}
			if typeFilter != "" && item.Type != typeFilter {
				continue
			}
			if projectFilter != "" && (item.ProjectID == nil || *item.ProjectID != projectFilter) {
				continue
			}
			loopSeq := resolveQueueItemLoopSeq(cmd.Context(), repos, item, loopSeqByID)
			output.Items = append(output.Items, queueFailedItemOutput{QueueItem: queueItemOutput(item), Diagnosis: diagnoseQueueItem(item, loopSeq)})
			if int64(len(output.Items)) >= limit {
				break
			}
		}
		output.Count = len(output.Items)
		if getBoolFlag(cmd, "json") {
			return writeJSON(cmd.OutOrStdout(), output)
		}
		return writeHumanQueueFailed(cmd.OutOrStdout(), output)
	})
}

func (r *commandRuntime) loopInspect(cmd *cobra.Command, args []string) error {
	selector := strings.TrimSpace(args[0])
	return r.withLocalRepositories(cmd.Context(), func(repos *storage.Repositories) error {
		resolved, err := resolveLoopSelector(cmd.Context(), repos, selector)
		if err != nil {
			return err
		}
		output, err := buildLoopInspectOutput(cmd.Context(), repos, selector, resolved, time.Now().UTC())
		if err != nil {
			return err
		}
		if getBoolFlag(cmd, "json") {
			return writeJSON(cmd.OutOrStdout(), output)
		}
		return writeHumanLoopInspect(cmd.OutOrStdout(), output)
	})
}

func (r *commandRuntime) loopFailures(cmd *cobra.Command, args []string) error {
	_ = args
	limit, err := diagnosticsLimit(cmd)
	if err != nil {
		return err
	}
	typeFilter := strings.TrimSpace(getStringFlag(cmd, "type"))
	projectFilter := strings.TrimSpace(getStringFlag(cmd, "project"))
	return r.withLocalRepositories(cmd.Context(), func(repos *storage.Repositories) error {
		loops, err := repos.Loops.List(cmd.Context())
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		output := loopFailuresOutput{NowISO: eventlog.FormatJavaScriptISOString(now), Type: typeFilter, ProjectID: projectFilter, Limit: limit}
		for _, loop := range loops {
			if typeFilter != "" && loop.Type != typeFilter {
				continue
			}
			if projectFilter != "" && loop.ProjectID != projectFilter {
				continue
			}
			queueItem, err := repos.Queue.GetLatestByLoopID(cmd.Context(), loop.ID)
			if err != nil {
				return err
			}
			run, err := repos.Runs.GetLatestByLoopID(cmd.Context(), loop.ID)
			if err != nil {
				return err
			}
			if !includeLoopInFailures(loop, queueItem, run) {
				continue
			}
			item, err := buildLoopInspectOutput(cmd.Context(), repos, fmt.Sprintf("%d", loop.Seq), loopSelectorResult{Loop: loop, Run: run, SelectorKind: "loopId"}, now)
			if err != nil {
				return err
			}
			output.Items = append(output.Items, item)
			if int64(len(output.Items)) >= limit {
				break
			}
		}
		output.Count = len(output.Items)
		if getBoolFlag(cmd, "json") {
			return writeJSON(cmd.OutOrStdout(), output)
		}
		return writeHumanLoopFailures(cmd.OutOrStdout(), output)
	})
}

func diagnosticsLimit(cmd *cobra.Command) (int64, error) {
	raw := strings.TrimSpace(getStringFlag(cmd, "limit"))
	if raw == "" {
		return defaultDiagnosticsLimit, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("--limit must be a positive integer")
	}
	return value, nil
}

func resolveLoopSelector(ctx context.Context, repos *storage.Repositories, selector string) (loopSelectorResult, error) {
	if selector == "" {
		return loopSelectorResult{}, fmt.Errorf("loop selector is required")
	}
	if seq, err := strconv.ParseInt(selector, 10, 64); err == nil {
		loop, err := repos.Loops.GetBySeq(ctx, seq)
		if err != nil {
			return loopSelectorResult{}, err
		}
		if loop == nil {
			return loopSelectorResult{}, fmt.Errorf("loop not found: %s", selector)
		}
		run, err := repos.Runs.GetLatestByLoopID(ctx, loop.ID)
		if err != nil {
			return loopSelectorResult{}, err
		}
		return loopSelectorResult{Loop: *loop, Run: run, SelectorKind: "seq"}, nil
	}
	if strings.HasPrefix(selector, "run_") {
		run, err := repos.Runs.GetByID(ctx, selector)
		if err != nil {
			return loopSelectorResult{}, err
		}
		if run == nil {
			return loopSelectorResult{}, fmt.Errorf("run not found: %s", selector)
		}
		loop, err := repos.Loops.GetByID(ctx, run.LoopID)
		if err != nil {
			return loopSelectorResult{}, err
		}
		if loop == nil {
			return loopSelectorResult{}, fmt.Errorf("loop not found for run: %s", selector)
		}
		return loopSelectorResult{Loop: *loop, Run: run, SelectorKind: "runId"}, nil
	}
	loop, err := repos.Loops.GetByID(ctx, selector)
	if err != nil {
		return loopSelectorResult{}, err
	}
	if loop == nil {
		return loopSelectorResult{}, fmt.Errorf("loop not found: %s", selector)
	}
	run, err := repos.Runs.GetLatestByLoopID(ctx, loop.ID)
	if err != nil {
		return loopSelectorResult{}, err
	}
	return loopSelectorResult{Loop: *loop, Run: run, SelectorKind: "loopId"}, nil
}

func (r *commandRuntime) resolveLogLoopSelector(ctx context.Context, selector string) (string, error) {
	if !strings.HasPrefix(selector, "run_") {
		return selector, nil
	}
	var loopID string
	err := r.withLocalRepositories(ctx, func(repos *storage.Repositories) error {
		run, err := repos.Runs.GetByID(ctx, selector)
		if err != nil {
			return err
		}
		if run == nil {
			return fmt.Errorf("run not found: %s", selector)
		}
		loopID = run.LoopID
		return nil
	})
	if err != nil {
		return "", err
	}
	return loopID, nil
}

func buildLoopInspectOutput(ctx context.Context, repos *storage.Repositories, selector string, resolved loopSelectorResult, now time.Time) (loopInspectOutput, error) {
	metadata := parseLoopDiagnosticMetadata(resolved.Loop.MetadataJSON)
	queueItem, err := repos.Queue.GetLatestByLoopID(ctx, resolved.Loop.ID)
	if err != nil {
		return loopInspectOutput{}, err
	}
	var queueOutput *queueItemCommandOutput
	if queueItem != nil {
		item := queueItemOutput(*queueItem)
		queueOutput = &item
	}

	var agent *storage.AgentExecutionRecord
	if resolved.Run != nil {
		agent, err = repos.AgentExecutions.GetLatestByRunID(ctx, resolved.Run.ID)
	} else {
		agent, err = repos.AgentExecutions.GetLatestByLoopID(ctx, resolved.Loop.ID)
	}
	if err != nil {
		return loopInspectOutput{}, err
	}

	// When the operator selected a historical run, do not let the loop's latest
	// queue item rewrite that run's failure class/kind. Still surface the latest
	// queue item as current loop state in LatestQueueItem.
	associateQueueWithDiagnosis := resolved.SelectorKind != "runId"
	output := loopInspectOutput{
		NowISO:          eventlog.FormatJavaScriptISOString(now),
		Selector:        selector,
		SelectorKind:    resolved.SelectorKind,
		Loop:            diagnosticLoopOutput(resolved.Loop),
		Metadata:        metadata,
		LatestQueueItem: queueOutput,
		Diagnosis:       diagnoseLoop(resolved.Loop, resolved.Run, queueItem, metadata, associateQueueWithDiagnosis),
	}
	if resolved.Run != nil {
		run := diagnosticRunOutput(*resolved.Run, now)
		output.Run = &run
	}
	if agent != nil {
		agentOutput := diagnosticAgentOutput(*agent, now)
		output.Agent = &agentOutput
	}
	if handoff := reviewFixInspectHandoff(resolved.Loop, metadata); handoff != nil {
		output.Handoff = handoff
	}
	return output, nil
}

func includeLoopInFailures(loop storage.LoopRecord, queueItem *storage.QueueItemRecord, run *storage.RunRecord) bool {
	if loop.Status == "failed" {
		return true
	}
	if loop.Status != "paused" {
		return false
	}
	if isManualInterventionQueueItem(queueItem) {
		return true
	}
	policy := resumePolicyFromCheckpoint(nil)
	if run != nil {
		policy = resumePolicyFromCheckpoint(run.CheckpointJSON)
	}
	return policy != nil && *policy == "manual_intervention"
}

func isManualInterventionQueueItem(item *storage.QueueItemRecord) bool {
	if item == nil {
		return false
	}
	if item.Status == "manual_intervention" {
		return true
	}
	return item.LastErrorKind != nil && strings.TrimSpace(*item.LastErrorKind) == "manual_intervention"
}

func parseLoopDiagnosticMetadata(raw *string) loopDiagnosticMetadata {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return loopDiagnosticMetadata{}
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(*raw), &doc); err != nil {
		msg := err.Error()
		return loopDiagnosticMetadata{DecodeError: &msg}
	}
	output := loopDiagnosticMetadata{
		FollowUpdates:        boolPtrFromMap(doc, "followUpdates"),
		LastPublishedAt:      stringPtrFromMap(doc, "lastPublishedAt"),
		LastPublishedHeadSHA: stringPtrFromMap(doc, "lastPublishedHeadSha"),
		LastReviewEvent:      stringPtrFromMap(doc, "lastReviewEvent"),
		LastReviewSummary:    stringPtrFromMap(doc, "lastReviewSummary"),
		PauseReason:          stringPtrFromMap(doc, "pauseReason"),
	}
	if skip, ok := doc["lastFilterSkip"].(map[string]any); ok {
		output.LastFilterSkip = skip
	}
	if loopDoc, ok := doc["loop"].(map[string]any); ok {
		output.Loop = &loopDiagnosticLoopMetadata{
			Status:                 stringPtrFromMap(loopDoc, "status"),
			LastStatus:             stringPtrFromMap(loopDoc, "lastStatus"),
			ConsecutiveFailures:    int64PtrFromMap(loopDoc, "consecutiveFailures"),
			FailureCount:           int64PtrFromMap(loopDoc, "failureCount"),
			AutoRecoveryAttempts:   int64PtrFromMap(loopDoc, "autoRecoveryAttempts"),
			LastAutoRecoveryReason: stringPtrFromMap(loopDoc, "lastAutoRecoveryReason"),
			LastFailure:            stringPtrFromMap(loopDoc, "lastFailure"),
			LastReviewedHeadSHA:    stringPtrFromMap(loopDoc, "lastReviewedHeadSha"),
			LastOutputFingerprint:  stringPtrFromMap(loopDoc, "lastOutputFingerprint"),
			IterationCount:         int64PtrFromMap(loopDoc, "iterationCount"),
			AgentExecutionCount:    int64PtrFromMap(loopDoc, "agentExecutionCount"),
			TerminationReason:      stringPtrFromMap(loopDoc, "terminationReason"),
			MinPublishIntervalSecs: int64PtrFromMap(loopDoc, "minPublishIntervalSeconds"),
			QuietPeriodSeconds:     int64PtrFromMap(loopDoc, "quietPeriodSeconds"),
		}
	}
	return output
}

func diagnosticLoopOutput(loop storage.LoopRecord) loopDiagnosticLoop {
	return loopDiagnosticLoop{
		ID:        loop.ID,
		Seq:       loop.Seq,
		ProjectID: loop.ProjectID,
		Type:      loop.Type,
		Status:    loop.Status,
		Target:    diagnosticTargetOutput(loop),
		LastRunAt: loop.LastRunAt,
		NextRunAt: loop.NextRunAt,
		CreatedAt: loop.CreatedAt,
		UpdatedAt: loop.UpdatedAt,
	}
}

func diagnosticTargetOutput(loop storage.LoopRecord) loopDiagnosticTarget {
	label := strings.TrimSpace(diagnosticString(loop.TargetID))
	if loop.Repo != nil && loop.PRNumber != nil {
		label = fmt.Sprintf("%s#%d", *loop.Repo, *loop.PRNumber)
	}
	return loopDiagnosticTarget{Type: loop.TargetType, ID: loop.TargetID, Repo: loop.Repo, PRNumber: loop.PRNumber, Label: label}
}

func diagnosticRunOutput(run storage.RunRecord, now time.Time) loopDiagnosticRun {
	output := loopDiagnosticRun{
		ID:                run.ID,
		LoopID:            run.LoopID,
		Status:            run.Status,
		CurrentStep:       run.CurrentStep,
		LastCompletedStep: run.LastCompletedStep,
		Summary:           run.Summary,
		ErrorMessage:      run.ErrorMessage,
		ResumePolicy:      resumePolicyFromCheckpoint(run.CheckpointJSON),
		StartedAt:         run.StartedAt,
		LastHeartbeatAt:   run.LastHeartbeatAt,
		EndedAt:           run.EndedAt,
		CreatedAt:         run.CreatedAt,
		UpdatedAt:         run.UpdatedAt,
	}
	endISO := eventlog.FormatJavaScriptISOString(now)
	if run.EndedAt != nil && strings.TrimSpace(*run.EndedAt) != "" {
		endISO = *run.EndedAt
	}
	output.ElapsedSeconds = elapsedSecondsPtr(run.StartedAt, endISO)
	if run.LastHeartbeatAt != nil && run.EndedAt == nil {
		output.HeartbeatAgeSeconds = elapsedSecondsPtr(*run.LastHeartbeatAt, eventlog.FormatJavaScriptISOString(now))
	}
	return output
}

func resumePolicyFromCheckpoint(checkpointJSON *string) *string {
	if checkpointJSON == nil || strings.TrimSpace(*checkpointJSON) == "" {
		return nil
	}
	var doc struct {
		ResumePolicy string `json:"resumePolicy"`
	}
	if err := json.Unmarshal([]byte(*checkpointJSON), &doc); err != nil {
		return nil
	}
	policy := strings.TrimSpace(doc.ResumePolicy)
	if policy == "" {
		return nil
	}
	return &policy
}

func diagnosticAgentOutput(agent storage.AgentExecutionRecord, now time.Time) loopDiagnosticAgent {
	output := loopDiagnosticAgent{
		ID:                 agent.ID,
		Vendor:             agent.Vendor,
		Status:             agent.Status,
		PID:                agent.PID,
		Summary:            agent.Summary,
		ParseStatus:        agent.ParseStatus,
		CompletionSignal:   agent.CompletionSignal,
		HeartbeatCount:     agent.HeartbeatCount,
		LastHeartbeatAt:    agent.LastHeartbeatAt,
		ErrorMessage:       agent.ErrorMessage,
		NativeSessionID:    agent.NativeSessionID,
		NativeResumeMode:   agent.NativeResumeMode,
		NativeResumeStatus: agent.NativeResumeStatus,
		NativeResumeError:  agent.NativeResumeError,
		StartedAt:          agent.StartedAt,
		EndedAt:            agent.EndedAt,
		CreatedAt:          agent.CreatedAt,
		UpdatedAt:          agent.UpdatedAt,
	}
	if agent.LastHeartbeatAt != nil && agent.EndedAt == nil {
		output.HeartbeatAgeSeconds = elapsedSecondsPtr(*agent.LastHeartbeatAt, eventlog.FormatJavaScriptISOString(now))
	}
	return output
}

func diagnoseLoop(loop storage.LoopRecord, run *storage.RunRecord, queue *storage.QueueItemRecord, metadata loopDiagnosticMetadata, associateQueue bool) loopDiagnosis {
	state := loop.Status
	// Run-id selectors diagnose only the selected run. Drop latest-queue and
	// loop-level metadata signals (e.g. lastFailure from a later run).
	var diagnosisQueue *storage.QueueItemRecord
	diagnosisMetadata := loopDiagnosticMetadata{}
	if associateQueue {
		diagnosisQueue = queue
		diagnosisMetadata = metadata
	}
	message, source := loopDiagnosticMessage(run, diagnosisQueue, diagnosisMetadata)
	// FailureClass/Retryable come from the structured error kind + message.
	// Queue status "manual_intervention" is an operator-hold signal, not the
	// underlying failure class — keep them separate.
	// Checkpoint-only holds have no parked queue item, so queueErrorKind is
	// empty; synthesize manual_intervention from the run resume policy so
	// failures listing does not surface class=unknown for operator holds.
	errorKind := queueErrorKind(diagnosisQueue)
	if errorKind == "" && run != nil {
		if policy := resumePolicyFromCheckpoint(run.CheckpointJSON); policy != nil && strings.TrimSpace(*policy) == "manual_intervention" {
			errorKind = "manual_intervention"
		}
	}
	diagnosis := classifyDiagnosticMessage(message, errorKind)
	diagnosis.State = state
	diagnosis.Source = source
	if diagnosis.Message == "" {
		diagnosis.Message = message
	}
	if diagnosis.RecommendedAction == "" {
		diagnosis.RecommendedAction = recommendedActionForState(state)
	}
	if associateQueue && loops.IsReviewFixPairHold(loop) {
		release := reviewFixInspectResume(loop)
		if loops.IsReviewScopeHumanHold(loop) && !loops.IsReviewFixBudgetHold(loop) {
			diagnosis.RecommendedAction = release + "; unpause releases only the scope hold; then " + diagnosis.RecommendedAction
		} else {
			diagnosis.RecommendedAction = release
		}
		if loops.IsReviewFixBudgetHold(loop) {
			retryable := true
			diagnosis.FailureClass = "review_fix_budget"
			diagnosis.Retryable = &retryable
			diagnosis.Source = "loop"
			diagnosis.Message = "review-fix budget exhausted"
		}
	}
	// Expand <seq> before emitting JSON/human output so operators and scripts
	// never see the literal placeholder outside writeHumanLoopInspect.
	diagnosis.RecommendedAction = formatActionWithSeq(diagnosis.RecommendedAction, loop.Seq)
	return diagnosis
}

func diagnoseQueueItem(item storage.QueueItemRecord, loopSeq int64) loopDiagnosis {
	// Preserve LastErrorKind as the failure cause. Queue status remains in
	// diagnosis.State so operators can see a parked manual hold separately.
	diagnosis := classifyDiagnosticMessage(diagnosticString(item.LastError), diagnosticString(item.LastErrorKind))
	diagnosis.State = item.Status
	diagnosis.Source = "queueItem"
	if diagnosis.RecommendedAction == "" {
		diagnosis.RecommendedAction = "inspect the owning loop before requeueing"
	}
	// Same contract as diagnoseLoop: never serialize the literal <seq>
	// placeholder. Expand when the owning loop seq is known; otherwise rewrite
	// guidance so queue-failed JSON does not leak unresolved tokens.
	if loopSeq > 0 {
		diagnosis.RecommendedAction = formatActionWithSeq(diagnosis.RecommendedAction, loopSeq)
	} else {
		diagnosis.RecommendedAction = actionWithoutSeqPlaceholder(diagnosis.RecommendedAction)
	}
	return diagnosis
}

// resolveQueueItemLoopSeq looks up the owning loop sequence for a queue item,
// caching results so queue-failed listing does not repeat GetByID per item.
func resolveQueueItemLoopSeq(ctx context.Context, repos *storage.Repositories, item storage.QueueItemRecord, cache map[string]int64) int64 {
	if item.LoopID == nil {
		return 0
	}
	loopID := strings.TrimSpace(*item.LoopID)
	if loopID == "" {
		return 0
	}
	if seq, ok := cache[loopID]; ok {
		return seq
	}
	loop, err := repos.Loops.GetByID(ctx, loopID)
	if err != nil || loop == nil {
		cache[loopID] = 0
		return 0
	}
	cache[loopID] = loop.Seq
	return loop.Seq
}

// actionWithoutSeqPlaceholder rewrites retry/unpause/describe guidance when no
// loop sequence is available, so consumers never see a literal <seq> token.
func actionWithoutSeqPlaceholder(action string) string {
	rewritten := action
	for _, pair := range [][2]string{
		{"looper retry <seq>", "retry the owning loop"},
		{"looper unpause <seq>", "unpause the owning loop"},
		{"looper describe <seq>", "describe the owning loop"},
	} {
		rewritten = strings.ReplaceAll(rewritten, pair[0], pair[1])
	}
	return strings.ReplaceAll(rewritten, "<seq>", "the owning loop seq")
}

func loopDiagnosticMessage(run *storage.RunRecord, queue *storage.QueueItemRecord, metadata loopDiagnosticMetadata) (string, string) {
	if run != nil && (run.Status == "failed" || run.Status == "interrupted") {
		if msg := firstNonEmpty(diagnosticString(run.ErrorMessage), diagnosticString(run.Summary)); msg != "" {
			return msg, "run"
		}
	}
	if queue != nil && strings.TrimSpace(diagnosticString(queue.LastError)) != "" {
		return strings.TrimSpace(diagnosticString(queue.LastError)), "queueItem"
	}
	if metadata.Loop != nil && metadata.Loop.LastFailure != nil && strings.TrimSpace(*metadata.Loop.LastFailure) != "" {
		return strings.TrimSpace(*metadata.Loop.LastFailure), "loopMetadata"
	}
	if run != nil {
		if msg := firstNonEmpty(diagnosticString(run.ErrorMessage), diagnosticString(run.Summary)); msg != "" {
			return msg, "run"
		}
	}
	return "", ""
}

func classifyDiagnosticMessage(message string, errorKind string) loopDiagnosis {
	msg := strings.TrimSpace(message)
	lower := strings.ToLower(msg)
	kind := strings.ToLower(strings.TrimSpace(errorKind))
	if msg == "" && kind == "" {
		return loopDiagnosis{}
	}
	// Preserved unusable managed paths are stored as LastErrorKind=manual_intervention.
	// Classify them before the generic MI branch so operators get integrity-specific
	// cleanup guidance instead of generic manual_intervention actions.
	if strings.Contains(lower, "unusable and not empty") || strings.Contains(lower, "unusable worktree path preserved") {
		retryable := false
		return loopDiagnosis{FailureClass: "managed_worktree_integrity", Retryable: &retryable, Message: msg, RecommendedAction: "inspect the managed worktree path, preserve any agent output, clear or move the leftover directory, then retry the loop"}
	}
	if kind == "manual_intervention" {
		retryable := false
		return loopDiagnosis{
			FailureClass:      "manual_intervention",
			Retryable:         &retryable,
			Message:           msg,
			RecommendedAction: recommendedActionForManualIntervention(msg),
		}
	}
	if strings.Contains(lower, "could not resolve to a pullrequest") {
		retryable := false
		return loopDiagnosis{FailureClass: "pull_request_unresolved", Retryable: &retryable, Message: msg, RecommendedAction: "confirm the PR still exists or terminate the stale loop"}
	}
	if strings.Contains(lower, "pull request lock is already held") {
		retryable := false
		return loopDiagnosis{FailureClass: "lock_held", Retryable: &retryable, Message: msg, RecommendedAction: "inspect the active run or lock holder before retrying"}
	}
	if strings.Contains(lower, "no matching github review marker") {
		retryable := true
		return loopDiagnosis{FailureClass: "review_marker_missing", Retryable: &retryable, Message: msg, RecommendedAction: "rerun publish verification or re-review after checking GitHub state"}
	}
	if kind == "non_retryable" && isTerminalGitHubDenial(lower) {
		retryable := false
		return loopDiagnosis{FailureClass: kind, Retryable: &retryable, Message: msg, RecommendedAction: "inspect before manual recovery"}
	}
	if strings.Contains(lower, "bad credentials") || strings.Contains(lower, "authentication failed") || strings.Contains(lower, "not authorized") || strings.Contains(lower, "permission denied") || strings.Contains(lower, "http 401") || strings.Contains(lower, "http 403") {
		retryable := true
		return loopDiagnosis{FailureClass: "github_auth_or_scope", Retryable: &retryable, Message: msg, RecommendedAction: "fix GitHub auth or token scopes, then allow the queued retry to continue"}
	}
	if strings.Contains(lower, "could not resolve to a repository") || strings.Contains(lower, "repository not found") {
		retryable := true
		return loopDiagnosis{FailureClass: "github_repository_access", Retryable: &retryable, Message: msg, RecommendedAction: "check the configured repo slug and GitHub access, then allow the queued retry to continue"}
	}
	if strings.Contains(lower, "start command: chdir") || strings.Contains(lower, "not a git repository") || strings.Contains(lower, "not in a git directory") {
		retryable := true
		return loopDiagnosis{FailureClass: "project_repo_path", Retryable: &retryable, Message: msg, RecommendedAction: "fix the project repoPath or restore the local checkout, then allow the queued retry to continue"}
	}
	if strings.Contains(lower, "invalid model") || strings.Contains(lower, "unsupported model") || strings.Contains(lower, "config validation") {
		if kind == "non_retryable" {
			retryable := false
			return loopDiagnosis{FailureClass: kind, Retryable: &retryable, Message: msg, RecommendedAction: "fix the Looper configuration value before manual recovery"}
		}
		retryable := true
		return loopDiagnosis{FailureClass: "configuration", Retryable: &retryable, Message: msg, RecommendedAction: "fix the Looper configuration value, then allow the queued retry to continue"}
	}
	if strings.Contains(lower, "timed out (idle)") || strings.Contains(lower, "idle timed out") {
		retryable := true
		return loopDiagnosis{FailureClass: "agent_idle_timeout", Retryable: &retryable, Message: msg, RecommendedAction: "retry the loop; inspect agent output if the timeout repeats"}
	}
	if strings.Contains(lower, "api.github.com") || strings.Contains(lower, "http 502") || strings.Contains(lower, "http 504") || strings.Contains(lower, "gateway timeout") || strings.Contains(lower, "eof") {
		retryable := true
		return loopDiagnosis{FailureClass: "github_transient", Retryable: &retryable, Message: msg, RecommendedAction: "retry or allow auto-recovery after GitHub transport stabilizes"}
	}
	if strings.Contains(lower, "connection closed") || strings.Contains(lower, "could not read from remote repository") || strings.Contains(lower, "git ") {
		retryable := true
		return loopDiagnosis{FailureClass: "git_transient", Retryable: &retryable, Message: msg, RecommendedAction: "retry after checking network access to the remote"}
	}
	if kind == "retryable" || strings.HasPrefix(kind, "retryable_") {
		retryable := true
		return loopDiagnosis{FailureClass: kind, Retryable: &retryable, Message: msg, RecommendedAction: "retry according to queue policy"}
	}
	if kind == "non_retryable" {
		retryable := false
		return loopDiagnosis{FailureClass: kind, Retryable: &retryable, Message: msg, RecommendedAction: "inspect before manual recovery"}
	}
	return loopDiagnosis{FailureClass: "unknown", Message: msg, RecommendedAction: "inspect the loop, run, and queue item details"}
}

func isTerminalGitHubDenial(message string) bool {
	for _, fragment := range []string{
		"http 400",
		"http 403",
		"http 422",
		"400 bad request",
		"403 forbidden",
		"422 unprocessable",
		"protected branch",
		"branch protection",
		"policy denied",
	} {
		if strings.Contains(message, fragment) {
			return true
		}
	}
	return false
}

func recommendedActionForManualIntervention(message string) string {
	lower := strings.ToLower(message)
	// Match the narrower dirty-worktree classifier used by failureclass.
	if strings.Contains(lower, "dirty worktree") || strings.Contains(lower, "worktree is dirty") || strings.Contains(lower, "uncommitted changes") {
		return "inspect with looper jump <seq>, or discard via looper retry <seq> --discard-worktree-changes --confirm"
	}
	if strings.Contains(lower, "unusable and not empty") || strings.Contains(lower, "unusable worktree path preserved") {
		return "inspect with looper jump <seq>, or clear leftovers via looper retry <seq> --clear-unusable-worktree --confirm"
	}
	if strings.Contains(lower, "worktree is locked") {
		return "unlock or remove the locked worktree, then looper retry <seq>"
	}
	if strings.Contains(lower, "worktree") {
		return "inspect the worktree path/state, then looper retry <seq>"
	}
	return "resolve the blocker, then looper retry <seq>"
}

func recommendedActionForState(state string) string {
	switch state {
	case "running":
		return "monitor active run progress"
	case "waiting":
		return "no immediate action; loop is waiting for follow-up work"
	case "paused":
		return "use looper unpause <seq> if intentionally paused; otherwise looper describe <seq>"
	case "failed":
		return "inspect failure fields before requeueing"
	case "terminated", "completed", "stopped":
		return "no action expected for terminal loop"
	default:
		return "inspect loop details"
	}
}

func formatActionWithSeq(action string, seq int64) string {
	return strings.ReplaceAll(action, "<seq>", strconv.FormatInt(seq, 10))
}

func reviewFixInspectHandoff(loop storage.LoopRecord, metadata loopDiagnosticMetadata) *loopInspectHandoff {
	if !loops.IsReviewFixPairHold(loop) {
		return nil
	}
	kind := loops.HITLKindReviewFixBudget
	if loops.IsReviewScopeHumanHold(loop) && !loops.IsReviewFixBudgetHold(loop) {
		kind = loops.HITLKindReviewScopeHuman
	}
	return &loopInspectHandoff{
		Kind:        kind,
		PauseReason: reviewFixInspectPauseReason(metadata, loop, kind),
		Resume:      reviewFixInspectResume(loop),
	}
}

func reviewFixInspectResume(loop storage.LoopRecord) string {
	selector := strings.TrimSpace(loop.ID)
	if loop.Seq > 0 {
		selector = strconv.FormatInt(loop.Seq, 10)
	}
	cli := fmt.Sprintf("looper unpause %s / looper stop %s", selector, selector)
	ask, ok := loops.ReadHITLAsk(loop.MetadataJSON)
	if ok && strings.EqualFold(strings.TrimSpace(ask.Status), "awaiting") &&
		(loops.IsReviewFixBudgetAsk(ask) || loops.IsReviewScopeHumanAsk(ask)) {
		return "Continue / Stop; " + cli
	}
	return cli
}

func reviewFixInspectPauseReason(metadata loopDiagnosticMetadata, loop storage.LoopRecord, kind string) *string {
	if metadata.PauseReason != nil {
		if reason := strings.TrimSpace(*metadata.PauseReason); reason != "" {
			return &reason
		}
	}
	nested := ""
	if kind == loops.HITLKindReviewScopeHuman {
		nested = strings.TrimSpace(loops.ReadReviewScopeHumanState(loop.MetadataJSON).PauseReason)
	} else {
		nested = strings.TrimSpace(loops.ReadReviewFixBudgetState(loop.MetadataJSON).PauseReason)
	}
	if nested == "" {
		return nil
	}
	return &nested
}

func humanInspectHoldReason(output loopInspectOutput) string {
	if output.Metadata.PauseReason != nil {
		if reason := strings.TrimSpace(*output.Metadata.PauseReason); reason != "" {
			return reason
		}
	}
	if output.Handoff != nil && output.Handoff.PauseReason != nil {
		return strings.TrimSpace(*output.Handoff.PauseReason)
	}
	return ""
}

func requiresOperatorHold(output loopInspectOutput) bool {
	if output.Diagnosis.FailureClass == "manual_intervention" {
		return true
	}
	if output.LatestQueueItem != nil && output.LatestQueueItem.Status == "manual_intervention" {
		return true
	}
	if output.LatestQueueItem != nil && output.LatestQueueItem.LastErrorKind != nil && strings.TrimSpace(*output.LatestQueueItem.LastErrorKind) == "manual_intervention" {
		return true
	}
	if output.Run != nil && output.Run.ResumePolicy != nil && strings.TrimSpace(*output.Run.ResumePolicy) == "manual_intervention" {
		return true
	}
	return false
}

func queueErrorKind(queue *storage.QueueItemRecord) string {
	if queue == nil {
		return ""
	}
	return diagnosticString(queue.LastErrorKind)
}

func writeHumanQueueFailed(w io.Writer, output queueFailedOutput) error {
	if len(output.Items) == 0 {
		_, err := fmt.Fprintln(w, "No failed queue items.")
		return err
	}
	for _, item := range output.Items {
		target := strings.TrimSpace(item.QueueItem.TargetID)
		if item.QueueItem.Repo != nil && item.QueueItem.PRNumber != nil {
			target = fmt.Sprintf("%s#%d", *item.QueueItem.Repo, *item.QueueItem.PRNumber)
		}
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\tattempts=%d/%d\tclass=%s\ttarget=%s\n", item.QueueItem.ID, item.QueueItem.Type, item.QueueItem.Status, item.QueueItem.Attempts, item.QueueItem.MaxAttempts, item.Diagnosis.FailureClass, target); err != nil {
			return err
		}
	}
	return nil
}

func writeHumanLoopInspect(w io.Writer, output loopInspectOutput) error {
	if _, err := fmt.Fprintf(w, "Loop #%d · %s · %s · %s\n", output.Loop.Seq, output.Loop.Type, output.Loop.Status, output.Loop.Target.Label); err != nil {
		return err
	}
	if output.Run != nil {
		if _, err := fmt.Fprintf(w, "Run %s · %s", output.Run.ID, output.Run.Status); err != nil {
			return err
		}
		if output.Run.CurrentStep != nil {
			if _, err := fmt.Fprintf(w, " · step: %s", *output.Run.CurrentStep); err != nil {
				return err
			}
		}
		if output.Run.ResumePolicy != nil && strings.TrimSpace(*output.Run.ResumePolicy) != "" {
			if _, err := fmt.Fprintf(w, " · resumePolicy: %s", *output.Run.ResumePolicy); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}
	queueError := ""
	if output.LatestQueueItem != nil {
		if _, err := fmt.Fprintf(w, "Queue %s · attempts %d/%d", output.LatestQueueItem.Status, output.LatestQueueItem.Attempts, output.LatestQueueItem.MaxAttempts); err != nil {
			return err
		}
		if output.LatestQueueItem.LastErrorKind != nil && strings.TrimSpace(*output.LatestQueueItem.LastErrorKind) != "" {
			if _, err := fmt.Fprintf(w, " · kind %s", *output.LatestQueueItem.LastErrorKind); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if output.LatestQueueItem.LastError != nil {
			queueError = strings.TrimSpace(*output.LatestQueueItem.LastError)
		}
		if queueError != "" {
			if _, err := fmt.Fprintf(w, "Error: %s\n", queueError); err != nil {
				return err
			}
		}
	}
	if output.Agent != nil {
		if _, err := fmt.Fprintf(w, "Agent %s · %s · %s", output.Agent.ID, output.Agent.Vendor, output.Agent.Status); err != nil {
			return err
		}
		if output.Agent.PID != nil {
			if _, err := fmt.Fprintf(w, " · pid %d", *output.Agent.PID); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}
	if output.Diagnosis.FailureClass != "" || output.Diagnosis.Message != "" {
		if _, err := fmt.Fprintf(w, "Diagnosis: state=%s class=%s retryable=%s source=%s\n", output.Diagnosis.State, output.Diagnosis.FailureClass, humanBoolPtr(output.Diagnosis.Retryable), output.Diagnosis.Source); err != nil {
			return err
		}
		// Avoid printing the same failure text twice when queue Error already showed it.
		if output.Diagnosis.Message != "" && strings.TrimSpace(output.Diagnosis.Message) != queueError {
			if _, err := fmt.Fprintf(w, "Message: %s\n", output.Diagnosis.Message); err != nil {
				return err
			}
		}
	}
	if reason := humanInspectHoldReason(output); reason != "" {
		if _, err := fmt.Fprintf(w, "Hold: %s\n", reason); err != nil {
			return err
		}
	}
	if output.Handoff != nil {
		if _, err := fmt.Fprintf(w, "Release: %s\n", output.Handoff.Resume); err != nil {
			return err
		}
		if output.Handoff.Kind == loops.HITLKindReviewFixBudget {
			if _, err := fmt.Fprintln(w, "unpause releases the budget hold and refills only exhausted meters; other holds, including scope holds, may still prevent the pair from running."); err != nil {
				return err
			}
		}
		if output.Handoff.Kind == loops.HITLKindReviewScopeHuman {
			if _, err := fmt.Fprintln(w, "unpause releases the scope hold; other blockers remain, such as unanswered agent/HITL asks, failed or interrupted runs, human takeover, or a manual pause."); err != nil {
				return err
			}
		}
	}
	if output.Diagnosis.RecommendedAction != "" {
		if _, err := fmt.Fprintf(w, "Action: %s\n", formatActionWithSeq(output.Diagnosis.RecommendedAction, output.Loop.Seq)); err != nil {
			return err
		}
	}
	if requiresOperatorHold(output) && output.Handoff == nil {
		if _, err := fmt.Fprintf(w, "Next: after resolving the blocker, looper retry %d (see also looper logs %d)\n", output.Loop.Seq, output.Loop.Seq); err != nil {
			return err
		}
	}
	return nil
}

func writeHumanLoopFailures(w io.Writer, output loopFailuresOutput) error {
	if len(output.Items) == 0 {
		_, err := fmt.Fprintln(w, "No failed loops.")
		return err
	}
	for _, item := range output.Items {
		action := formatActionWithSeq(item.Diagnosis.RecommendedAction, item.Loop.Seq)
		if _, err := fmt.Fprintf(w, "#%d\t%s\t%s\tclass=%s\tretryable=%s\t%s\n", item.Loop.Seq, item.Loop.Type, item.Loop.Target.Label, item.Diagnosis.FailureClass, humanBoolPtr(item.Diagnosis.Retryable), action); err != nil {
			return err
		}
	}
	return nil
}

func humanBoolPtr(value *bool) string {
	if value == nil {
		return "unknown"
	}
	if *value {
		return "true"
	}
	return "false"
}

func stringPtrFromMap(values map[string]any, key string) *string {
	value, ok := values[key]
	if !ok {
		return nil
	}
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		return nil
	}
	return &text
}

func boolPtrFromMap(values map[string]any, key string) *bool {
	value, ok := values[key]
	if !ok {
		return nil
	}
	typed, ok := value.(bool)
	if !ok {
		return nil
	}
	return &typed
}

func int64PtrFromMap(values map[string]any, key string) *int64 {
	value, ok := values[key]
	if !ok {
		return nil
	}
	switch typed := value.(type) {
	case float64:
		next := int64(typed)
		return &next
	case int64:
		return &typed
	case int:
		next := int64(typed)
		return &next
	default:
		return nil
	}
}

func elapsedSecondsPtr(startISO string, endISO string) *int64 {
	start, err := time.Parse(time.RFC3339Nano, startISO)
	if err != nil {
		return nil
	}
	end, err := time.Parse(time.RFC3339Nano, endISO)
	if err != nil {
		return nil
	}
	elapsed := int64(end.Sub(start).Seconds())
	if elapsed < 0 {
		elapsed = 0
	}
	return &elapsed
}

func diagnosticString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
