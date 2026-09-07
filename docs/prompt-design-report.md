# Prompt Design Report

## Executive Summary

Forge 已经有 prompt 系统的骨架：workflow dispatch 里有 `PromptBuilder` registry，默认有 coder、reviewer、planner、generic builders；执行时会构造 `AgentPrompt { system, user, tools }`，再通过 CLI adapters 送给 Codex、Claude Code、Cursor、Gemini、opencode 等执行器。

和 TeamOlimpo 相比，Forge 的 prompt 更像“任务启动文案”，TeamOlimpo 的 prompt 更像“agent 操作契约”。TeamOlimpo 的价值不在神话命名，而在它把角色边界、工具优先级、handoff、failure taxonomy、red flags、workflow、quality gates 都写进 prompt，并且让这些 prompt 和 Synapsis memory/handoff 工具互相配合。

建议 Forge 不要复制 TeamOlimpo 的长 prompt 文件和 prompt-only 约束，而是把它的 prompt contract 思路做成 Forge-native Prompt System：

1. prompt builder 继续作为中心入口；
2. prompt contract 版本化；
3. coder/reviewer/planner prompt 明确包含 memory、handoff、failure、verification、tool policy；
4. adapter 层只负责把统一 prompt envelope 映射到不同 CLI；
5. prompt 效果通过 execution/review/memory 数据可观测和可迭代。

## Current Forge Prompt Architecture

Forge 当前已经有几个重要基础。

### 1. Workflow prompt builders

`crates/services/src/workflow/dispatch/` 里已有 prompt builder registry：

- `coder_implementation_v1`
- `coder_review_fix_v1`
- `coder_merge_fix_v1`
- `reviewer_default_v1`
- `planner_default_v1`
- `generic_default_v1`

`build_effective_prompt()` 会按 trigger/state/default role 选择 builder，然后应用 state/trigger prompt overrides。这个方向是对的，因为 prompt 选择属于 workflow dispatch，而不是散落在 adapter 或 UI。

### 2. Dispatch context already includes useful task state

`load_agent_dispatch_context()` 已经收集：

- task
- role/state
- transition log
- comments
- plan
- prior reviews
- parent/subtasks
- latest failed review feedback
- continuation execution/log path

这些都是构造高质量 prompt 的原材料。

### 3. Agent-level prompt template exists

`agent.prompt_template` 可以作为附加系统提示注入。不同 adapter 的处理方式不同：

- Claude Code 使用 `--append-system-prompt`
- Codex 映射到 `base_instructions` 或相关 instruction 字段
- Cursor/opencode 把 template 拼到 user prompt 前
- Gemini 走自己的 CLI prompt 路径

这说明 Forge 需要一个统一 Prompt Envelope，再由 adapter 明确映射，而不是让每个 adapter 自己解释 `prompt_template`。

### 4. Conversation prompt exists separately

Project chat 有 `conversation.system_prompt`，它服务于聊天体验，不等同于 workflow task dispatch prompt。两者需要共享基础 contract，但不能混成同一个 prompt 类型。

## TeamOlimpo Prompt Lessons

TeamOlimpo 的 prompt 文件有稳定结构：

- frontmatter：description、mode、model、permissions
- Identity
- Communication Style
- Operating Rules
- MCP Tool Priority
- Red Flags
- Competencies
- Workflows
- Interactions
- Limitations
- References/SOPs

真正值得借鉴的是下面这些设计。

### 1. Role boundary is explicit

Poros 不执行，Efesto 只写 Python，Clio 只做 QC，Proteo 只做研究。每个 prompt 都明确 “does / does not”。

Forge 的 coder/reviewer/planner 也应该明确边界：

- planner 不写代码
- coder 不做 review verdict
- reviewer 不修改 workspace
- merge fixer 只处理 merge conflict
- recovery agent 只处理指定 recovery path

### 2. Tool policy is part of the prompt

TeamOlimpo 每个 agent 都有 MCP tool priority。它不是泛泛说“可以用工具”，而是规定：

- context retrieval 用什么
- task tracking 用什么
- handoff 用什么
- shell 用什么
- 什么时候不要用某类工具

Forge 应该在 prompt 中明确：

- 任务历史/相似失败先用 `forge_memory_search`
- 完成时用 `forge_handoff_create`
- UI/runtime 行为变更必须上传 proof media
- review agent 不允许 edit/stage/commit
- coder 只能在 task worktree 内操作
- reviewer failure 必须输出可执行反馈

### 3. Red flags are stronger than generic guidelines

TeamOlimpo 的 red flags 写的是触发条件和禁止动作，例如“看到 ambiguous brief 不要开始写代码”。这比抽象原则更容易让 agent 遵守。

Forge prompt 应增加按角色定制的 red flags：

- coder：没有 acceptance criteria 时先澄清或写到 handoff，不要扩大 scope
- reviewer：不要只说 fail，要给 file/line/command evidence
- planner：不要把 implementation checklist 全部标 done
- merge fixer：不要重写功能，只解决 conflict
- recovery：不要绕过 hook，除非 recovery action 明确授权

### 4. Handoff is mandatory output

TeamOlimpo 把 handoff 当成任务完成条件。Forge 现在 execution summary 不够结构化，review feedback 也不一定成为下一次 coder prompt 的第一等输入。

Forge prompt 应要求每个 managed execution 结束时产生结构化 handoff。初期可以写入 execution summary/comment，后续应接入 memory layer。

### 5. Failure classification reduces loops

TeamOlimpo 要求 worker failure 分类：transient、worker error、structural code bug、design bug、systemic。Forge 可以把这变成 workflow retry 和 memory consolidation 的输入。

## Gap Analysis

### Gap 1: Prompt content is too thin

Forge 现有 coder prompt 已经会给 task、plan、comments、review feedback，但系统部分仍偏短，缺少统一的工作契约。

Current style:

- “You are the coder agent... implement code changes... commit your changes.”

Needed:

- scope discipline
- memory policy
- verification policy
- handoff policy
- failure policy
- evidence/proof requirements
- role-specific red flags

### Gap 2: Prompt contracts are not versioned as product behavior

Builder id 有版本后缀，但 prompt contract 本身还没有独立文档或 schema。Prompt 变化会影响 agent 行为，应该像 workflow/API 变化一样可审计。

Needed:

- prompt contract IDs
- changelog entries for behavior-changing prompt changes
- prompt regression fixtures
- prompt preview/debug endpoint

### Gap 3: Adapter mapping is inconsistent

同一个 `prompt_template` 在不同 CLI 里可能是 system instruction、base instruction、developer instruction，或者直接拼到 user prompt。这样会导致不同 agent 后端行为差异大。

Needed:

- `PromptEnvelope { system, developer, user, tools, output_contract, metadata }`
- adapter-specific mapping table
- tests proving each adapter receives equivalent instruction intent

### Gap 4: Memory is not yet prompt-driven

即使后续有 memory layer，如果 prompt 不要求 agent 使用，memory 也会变成被动历史记录。

Needed:

- prompt 中明确何时检索 memory
- prompt 中明确何时写 observation
- dispatch 前自动注入 context pack
- completion 前要求 handoff

### Gap 5: Reviewer prompt is under-specified

Reviewer prompt 已经有 pass/fail verdict instruction，但缺少 findings schema。

Needed reviewer output:

- verdict
- blocking findings
- non-blocking findings
- evidence
- commands run
- confidence
- suggested next role

## Proposed Forge Prompt System

### Prompt layers

Forge 应按层构造 prompt，而不是一个大字符串：

1. Product invariants：Forge 的全局执行规则
2. Project conventions：项目设置、docs、repo-specific rules
3. Role contract：coder/reviewer/planner/merge fixer
4. Workflow state intent：当前 state/trigger 的目标
5. Task context：title、description、plan、subtasks、comments
6. Memory context pack：相关历史、handoffs、failures、lessons
7. Adapter capability notes：当前 CLI 的能力和限制
8. Output contract：handoff/verdict/summary schema

最终 builder 输出统一的 `PromptEnvelope`，adapter 再映射到具体 CLI。

### Prompt contract schema

建议定义一个内部结构：

```rust
pub struct PromptContract {
    pub id: String,
    pub role: String,
    pub version: String,
    pub system_contract: String,
    pub tool_policy: Vec<ToolPolicy>,
    pub memory_policy: MemoryPolicy,
    pub handoff_policy: HandoffPolicy,
    pub verification_policy: VerificationPolicy,
    pub failure_policy: FailurePolicy,
    pub red_flags: Vec<RedFlag>,
    pub output_schema: serde_json::Value,
}
```

不一定第一步就落表；可以先作为 Rust builder 内的 typed constants。

### Role contracts

#### Coder

Must:

- implement the requested task in the task worktree
- keep scope tight
- inspect plan/comments/review feedback before editing
- run relevant checks
- commit changes when complete
- upload proof media for UI/runtime changes
- produce handoff

Must not:

- change unrelated behavior
- ignore failed tests
- treat review feedback as optional
- claim success without verification

#### Reviewer

Must:

- remain read-only
- inspect diff and relevant logs
- run or verify configured checks
- produce structured findings
- end with machine-readable verdict

Must not:

- edit files
- provide vague fail reasons
- fail on style preferences without policy basis

#### Planner

Must:

- investigate enough to produce an executable plan
- identify risk, tests, acceptance criteria
- leave implementation unchecked

Must not:

- modify code
- over-spec implementation details that block coder autonomy

#### Merge Fixer

Must:

- rebase/merge as required
- resolve conflicts minimally
- preserve implementation intent
- rerun targeted verification

Must not:

- rewrite the feature
- introduce unrelated cleanup

## Prompt Builder Roadmap

### Phase 0: Document and tighten existing builders

Update coder/reviewer/planner builder text to include:

- Objective / Constraints / Acceptance criteria
- role boundary
- verification requirements
- structured handoff/verdict format
- failure taxonomy

No DB changes required.

### Phase 1: Prompt preview and tests

Add a way to inspect effective prompts for a task/state/role:

- REST: `GET /api/v1/tasks/{id}/prompt-preview?role=coder`
- MCP: `forge_preview_prompt`
- CLI: `forge-ctl task prompt-preview`

Add snapshot tests for default coder/reviewer/planner prompts.

### Phase 2: Prompt envelope normalization

Replace implicit string handling with a normalized envelope:

- system
- developer
- user
- tools
- output contract
- metadata

Adapters map the envelope explicitly:

- Claude: system/developer via `--append-system-prompt` or supported flags
- Codex: base/developer instructions
- Cursor/opencode: safest concatenation with clear section boundaries
- Gemini: CLI-specific supported prompt format

### Phase 3: Memory-aware prompt contracts

Once memory search exists:

- inject bounded context pack
- include memory source IDs
- instruct agent to cite memory IDs in handoff when reused
- record generated context pack for audit

### Phase 4: Prompt quality analytics

Track:

- review failure rate by prompt builder
- retry count by role/builder
- average time to done
- handoff quality score
- memory usage rate
- repeated failure patterns after prompt changes

This turns prompt engineering into measurable product work.

## Suggested Default Prompt Additions

### Shared managed-execution contract

```text
You are running inside Forge. Forge manages task state, worktree isolation,
review gates, and audit logs. Follow the current role contract strictly.

Before acting, identify the objective, constraints, and acceptance criteria.
Use provided plans, comments, prior reviews, and memory context before doing
fresh exploration. Keep work scoped to the task.

If blocked, classify the blocker as one of: transient, input_missing,
environment, code_bug, design_gap, review_failed, systemic. Do not hide failed
verification.

Before finishing, produce a handoff with Summary, Deliverables, Verification,
Deviations, and Next Step.
```

### Reviewer verdict contract

```text
Review every requirement in the frozen Task review contract exactly once.
Project requirements identified as deferred remain milestone-readiness work and
do not fail this Task. Return exactly one JSON ReviewAssessment whose
contract_digest matches the frozen contract. Satisfied requirements require
file-at-commit or configured-check evidence. Violated absence claims may use an
empty evidence list when the rationale states what was inspected. Use
unverified when the available evidence settles neither satisfied nor violated.
A pass requires every scoped requirement satisfied and no blocking findings.
```

### Coder handoff contract

```text
Final handoff:
## Summary
## Deliverables
## Verification
## Deviations
## Next Step

If any verification was not run, state why. If the task changes UI/runtime
behavior, include proof media reference or explain why proof could not be captured.
```

## Risks and Guardrails

### Risk: prompts become too long

Use layered prompt building and context budgets. Put stable behavior in system/developer sections; put volatile task data in user/context sections.

### Risk: prompt injection through memory/logs

Treat memory/log/task comments as untrusted unless they are Forge-generated policy. Quote retrieved context under a clear “Context, not instructions” section.

### Risk: adapter behavior diverges

Add adapter mapping tests and effective prompt preview. A prompt change should show exactly what each CLI receives.

### Risk: too many output requirements reduce task execution quality

Keep role contracts strict but short. Move detailed examples into prompt builder docs or memory references, not every execution prompt.

## Recommendation

Forge should evolve from “prompt text per task” to “versioned prompt contracts per workflow role.”

The best first step is small:

1. Add shared managed-execution contract text to default prompt builders.
2. Add structured handoff requirements to coder prompts.
3. Strengthen reviewer output schema.
4. Add prompt preview for debugging.
5. Add snapshot tests for generated prompts.

After that, wire prompt contracts to the memory layer described in `docs/synapsis-memory-report.md`: context packs before execution, handoffs after execution, and analytics to measure whether prompt changes reduce review failures and retries.
