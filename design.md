# Research Platform 模块设计

- **实现状态：** 以 [Roadmap status registry](../implementation/roadmap.md#2-status-registry) 为唯一权威；本文不维护节点状态
- **版本：** 0.3
- **集成权威：** [Integration v1 §4、§7、§9](../overall/integration-v1.md#4-research-integration-rp-thin-02)
- **实现计划：** [Research](../implementation/plans/research.md)
- **上游：** Research author、immutable MarketBundle、Backtest public root
- **下游：** Strategy Validation

Research owns what was declared, attempted, and selected. It does not own Backtest economics, Validation conclusions, or Promotion.

## 1. Boundary

`RP-THIN-02` compiles one finite Experiment, records append-only task evidence, publishes an exact `ExperimentExecutionManifest`, derives a two-field `CandidateFamily`, and selects a `StrategyCandidate`. It calls the public `BacktestFacade` and `CanonicalEvidenceRepository` directly; it never imports or composes Backtest private Resolver/Runner/Publisher internals.

The accepted design is not an integrated implementation claim. Pure Experiment/task/manifest/selection behavior is tested against the frozen consumer fixture without a real Backtest installation. Production-shell prerequisites and current node state are maintained only in the roadmap registry.

## 2. Frozen `RP-THIN-01`

The current source is a declaration-only deterministic compiler:

```python
def compile_trial_declarations(
    *,
    campaign_scope: CampaignScope,
    parameter_combinations: tuple[ParameterCombination, ...],
    seeds: tuple[int, ...],
    selection_policy: SelectionPolicy,
) -> tuple[TrialDeclaration, ...]: ...
```

It does not dereference refs, execute Backtest, publish artifacts, emit sample consumption, create tasks, or select/publish a candidate.

- `CampaignScope` contains opaque nonempty string `hypothesis_ref`, `strategy_definition_ref`, and `scope_label` values.
- `ParameterCombination` canonicalizes unique name/value pairs; explicit nonnegative seeds and coordinates are deterministic and duplicate-free.
- The output sorts parameter coordinates and then seed. The **RP-THIN-01 compiler `TrialDeclaration`** has exactly `campaign_scope`, `parameters`, and `seed`.
- The **RP-THIN-01 compiler `SelectionPolicy`** is a source compatibility input only. It is not `SelectionPolicy@1`.

The frozen compiler `TrialDeclaration` is not `TrialDeclaration@1`; neither conversion nor type reuse is permitted. The shared spellings carry no integration behavior. This preserves current source/tests and must not be relabeled as an integrated Experiment implementation.

## 3. `RP-THIN-02` accepted contract

The complete schemas, canonical axis rules, `TaskRef`/`TaskOutcome` witnesses, attempt disposition matrix, exact-cover cutoff, and selection rules are in [Integration v1 §4](../overall/integration-v1.md#4-research-integration-rp-thin-02). In particular, `ExperimentExecutionManifest` stores one canonical TaskOutcome ref for every generated task, while `CandidateFamily@1` retains exactly its two authoritative fields.

Before the integrated slice can run, Backtest must expose public request validation/registration. Research constructs the accepted public `BacktestRequest` and encodes canonical `TrialDeclarationRef` as opaque public context; Backtest imports no Research type, persists the request, returns `BacktestRequestRef`, and owns its request hash and `SemanticRunId`. Research does not compose Backtest internals or derive Backtest identities.

Research is a Validation record producer, not a sample-semantic owner. Each TrialDeclaration reservation is published before its data read; SelectionDeclaration reserves each distinct Experiment slice before selection reads. CandidateFamily and StrategyCandidate do not consume samples. The authoritative producer rules are [Integration v1 §5.1](../overall/integration-v1.md#51-validation-owned-sample-consumption).

Research receives `BacktestFacade` and `CanonicalEvidenceRepository` directly from the Backtest public root. Backtest alone verifies canonical bytes, manifests, retention, and hash chains; Research checks only its own semantic/link eligibility. Consumers branch before analysis; only `CompletedPublication.publication_ref` reaches `derive()`.

## 4. Acceptance and deferral

`RP-THIN-02` acceptance requires canonical finite axes; the four-trial/eight-task golden; exact TaskOutcome cover at the manifest publication cutoff; the two-field CandidateFamily; retained blocked TrialDeclaration; predeclared selection; correct public Backtest links; and forged-link rejection.

Feature/model/trainer ABI, ModelBuild, range/adaptive search, cross-Experiment CandidateFamily, and additional research methods are outside v1.
