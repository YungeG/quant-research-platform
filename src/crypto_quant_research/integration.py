from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any

RESEARCH_EXECUTION_LOG = "research.execution.v1"
FAILURE_PRECEDENCE = (
    "MODEL_BUILD_PLAN_INVALID",
    "TASK_AXIS_DUPLICATE",
    "MODEL_TRAINING_BLOCKED",
    "MODEL_TRAINING_FAILED",
    "MODEL_BINDING_INVALID",
    "EXPERIMENT_SPEC_INVALID",
    "TASK_REF_FOREIGN",
    "ATTEMPT_CHAIN_INVALID",
    "TASK_OUTCOME_INVALID",
    "TASK_OUTCOME_MISSING_OR_DUPLICATE",
    "MANIFEST_CUTOFF_INVALID",
    "EXPERIMENT_REOPENED_AFTER_CLOSE",
    "SELECTION_PRECOMMIT_MISSING",
    "SELECTION_INPUT_INCOMPLETE",
    "SELECTION_POLICY_MISMATCH",
)

_HASH = re.compile(r"sha256:[0-9a-f]{64}")
_UTC = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z")
_DECIMAL = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?")
_LOCAL_REF_TYPES = {
    "experiment_spec",
    "trial_declaration",
    "analysis_task",
    "task_attempt_started",
    "task_attempt_closed",
    "task_outcome",
    "experiment_execution_manifest",
    "selection_policy",
    "selection_declaration",
    "feature_recipe",
    "trainer_recipe",
    "model_build_plan",
    "feature_build_task",
    "model_training_task",
    "feature_dataset_manifest",
    "model_build_evidence",
}
_LOCAL_REF = re.compile(
    r"rp-core:(?P<artifact_type>[a-z][a-z0-9_]*)@1:sha256:[0-9a-f]{64}"
)
_TASK_KINDS = {"FEATURE_BUILD", "MODEL_TRAINING", "TRIAL", "ANALYSIS"}
_OUTCOME_STATES = {"COMPLETED", "BLOCKED", "FAILED", "CANCELLED"}
_TERMINAL_STATES = {"BLOCKED", "FAILED", "CANCELLED"}


class ResearchCoreError(ValueError):
    """Stable RP-CORE-02 failure with the plan-defined code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        if code not in FAILURE_PRECEDENCE:
            raise ValueError(f"unknown Research core failure: {code}")
        self.code = code
        super().__init__(message or code)


def _fail(code: str, message: str | None = None) -> None:
    raise ResearchCoreError(code, message)


def _nonempty_string(value: object, name: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{name} must be a nonempty string")
    return value


def _freeze_json(value: object) -> object:
    if isinstance(value, Mapping):
        frozen: dict[str, object] = {}
        for key in sorted(value):
            if type(key) is not str:
                raise ValueError("canonical JSON object keys must be strings")
            frozen[key] = _freeze_json(value[key])
        return MappingProxyType(frozen)
    if type(value) in {tuple, list}:
        return tuple(_freeze_json(item) for item in value)  # type: ignore[arg-type]
    if value is None or type(value) in {str, int, bool}:
        return value
    raise ValueError("value must be canonical JSON data without floats")


def _plain_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _plain_json(item) for key, item in value.items()}
    if type(value) is tuple:
        return [_plain_json(item) for item in value]
    return value


def _canonical_json(value: object) -> str:
    return json.dumps(
        _plain_json(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _same_wire(left: object, right: object) -> bool:
    return _canonical_json(left) == _canonical_json(right)


def _canonical_ref(
    value: object,
    name: str,
    *,
    expected_type: str | None = None,
    expected_artifact_type: str | None = None,
) -> object:
    if expected_artifact_type in _LOCAL_REF_TYPES:
        if type(value) is not str:
            raise ValueError(f"{name} must use the RP-CORE local reference wire")
        match = _LOCAL_REF.fullmatch(value)
        if match is None or match["artifact_type"] != expected_artifact_type:
            raise ValueError(f"{name} must address {expected_artifact_type}")
        return value
    if expected_type is None and expected_artifact_type == "backtest_metric_profile":
        expected_type = "artifact_ref"
    if type(value) is str:
        if expected_type is not None or expected_artifact_type is not None:
            raise ValueError(f"{name} must use the canonical reference wire")
        return _nonempty_string(value, name)
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a canonical reference mapping")

    plain = _plain_json(value)
    if type(plain) is not dict or type(plain.get("type")) is not str:
        raise ValueError(f"{name} must include a reference type")
    ref_type = plain["type"]
    artifact = plain
    if ref_type == "artifact_ref":
        if set(plain) != {"type", "artifact_type", "schema_version", "content_hash"}:
            raise ValueError(f"{name} artifact_ref must be canonical")
    elif "artifact_ref" in plain:
        if (
            set(plain) != {"type", "artifact_ref"}
            or type(plain["artifact_ref"]) is not dict
        ):
            raise ValueError(f"{name} tagged reference must be canonical")
        artifact = plain["artifact_ref"]
        if (
            set(artifact) != {"type", "artifact_type", "schema_version", "content_hash"}
            or artifact.get("type") != "artifact_ref"
        ):
            raise ValueError(f"{name} tagged artifact_ref must be canonical")
    elif expected_type is not None or expected_artifact_type is not None:
        raise ValueError(f"{name} has the wrong reference shape")

    if artifact.get("type") == "artifact_ref":
        if (
            type(artifact.get("artifact_type")) is not str
            or not artifact["artifact_type"]
        ):
            raise ValueError(f"{name} artifact_type must be nonempty")
        if (
            type(artifact.get("schema_version")) is not int
            or artifact["schema_version"] != 1
        ):
            raise ValueError(f"{name} schema_version must be 1")
        content_hash = artifact.get("content_hash")
        if type(content_hash) is not str or _HASH.fullmatch(content_hash) is None:
            raise ValueError(f"{name} content_hash must be canonical")

    if expected_type is not None and ref_type != expected_type:
        raise ValueError(f"{name} must have type {expected_type}")
    if (
        expected_artifact_type is not None
        and artifact.get("artifact_type") != expected_artifact_type
    ):
        raise ValueError(f"{name} must address {expected_artifact_type}")
    return _freeze_json(plain)


def _local_ref(artifact_type: str, payload: object) -> str:
    """Deterministic pre-seam handle; deliberately not a Domain ArtifactRef."""

    preimage = ("rp-core-local-ref-v1", artifact_type, payload)
    digest = hashlib.sha256(_canonical_json(preimage).encode("utf-8")).hexdigest()
    return f"rp-core:{artifact_type}@1:sha256:{digest}"


def _canonical_utc(value: object, name: str) -> str:
    value = _nonempty_string(value, name)
    if _UTC.fullmatch(value) is None:
        raise ValueError(f"{name} must be canonical UTC")
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError as error:
        raise ValueError(f"{name} must be canonical UTC") from error
    return value


def _canonical_decimal(value: object, name: str) -> str:
    value = _nonempty_string(value, name)
    if value == "-0" or _DECIMAL.fullmatch(value) is None:
        raise ValueError(f"{name} must be a canonical ordinary decimal string")
    try:
        Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"{name} must be a decimal string") from error
    return value


def _ref_tuple(value: object, name: str) -> tuple[object, ...]:
    if type(value) is not tuple:
        raise ValueError(f"{name} must be a tuple")
    return tuple(_canonical_ref(item, name) for item in value)


def _content_hash(value: object, name: str) -> str:
    value = _nonempty_string(value, name)
    if _HASH.fullmatch(value) is None:
        raise ValueError(f"{name} must be a canonical sha256 hash")
    return value


def _utc_epoch_nanoseconds(value: str) -> int:
    instant = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    delta = instant - datetime(1970, 1, 1, tzinfo=UTC)
    return ((delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds) * 1_000


def _utc_instant_nanoseconds(value: object, name: str) -> int:
    if not isinstance(value, Mapping) or set(value) != {
        "type",
        "epoch_nanoseconds",
    }:
        raise ValueError(f"{name} must be a canonical UtcInstant")
    if value.get("type") != "utc_instant" or type(value.get("epoch_nanoseconds")) is not int:
        raise ValueError(f"{name} must be a canonical UtcInstant")
    return value["epoch_nanoseconds"]  # type: ignore[return-value]


def _canonical_model_artifact(value: object) -> Mapping[str, object]:
    keys = {
        "type",
        "schema_version",
        "model_key",
        "model_hash",
        "training_data_hash",
        "training_start",
        "training_end",
        "training_code_hash",
        "feature_schema_hash",
        "available_at",
        "revision_id",
        "supersedes_revision_id",
        "artifact_ref_hash",
    }
    if not isinstance(value, Mapping) or set(value) != keys:
        raise ValueError("model_artifact must be the canonical Backtest value")
    plain = _plain_json(value)
    if type(plain) is not dict:
        raise ValueError("model_artifact must be canonical")
    if (
        plain["type"] != "model_artifact_ref"
        or type(plain["schema_version"]) is not int
        or plain["schema_version"] != 1
    ):
        raise ValueError("model_artifact has the wrong schema")
    _nonempty_string(plain["model_key"], "model_artifact.model_key")
    for name in (
        "model_hash",
        "training_data_hash",
        "training_code_hash",
        "feature_schema_hash",
        "artifact_ref_hash",
    ):
        _content_hash(plain[name], f"model_artifact.{name}")
    _nonempty_string(plain["revision_id"], "model_artifact.revision_id")
    if plain["supersedes_revision_id"] is not None:
        _nonempty_string(
            plain["supersedes_revision_id"],
            "model_artifact.supersedes_revision_id",
        )
    training_start = _utc_instant_nanoseconds(
        plain["training_start"], "model_artifact.training_start"
    )
    training_end = _utc_instant_nanoseconds(
        plain["training_end"], "model_artifact.training_end"
    )
    if training_start >= training_end:
        raise ValueError("model_artifact training interval is invalid")
    available_at = plain["available_at"]
    if not isinstance(available_at, Mapping) or set(available_at) != {
        "type",
        "instant",
        "phase",
        "source_sequence",
    } or available_at.get("type") != "simulation_instant":
        raise ValueError("model_artifact.available_at must be a SimulationInstant")
    if training_end > _utc_instant_nanoseconds(
        available_at["instant"], "model_artifact.available_at.instant"
    ):
        raise ValueError("model_artifact is available before training ends")
    body = {key: item for key, item in plain.items() if key != "artifact_ref_hash"}
    expected_hash = "sha256:" + hashlib.sha256(
        _canonical_json(body).encode("utf-8")
    ).hexdigest()
    if plain["artifact_ref_hash"] != expected_hash:
        raise ValueError("model_artifact.artifact_ref_hash does not match its body")
    return _freeze_json(plain)  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class DataSlice:
    market_bundle_ref: object
    dataset_revision: str
    interval_start: str
    interval_end: str

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "market_bundle_ref",
                _canonical_ref(self.market_bundle_ref, "market_bundle_ref"),
            )
            _nonempty_string(self.dataset_revision, "dataset_revision")
            object.__setattr__(
                self,
                "interval_start",
                _canonical_utc(self.interval_start, "interval_start"),
            )
            object.__setattr__(
                self, "interval_end", _canonical_utc(self.interval_end, "interval_end")
            )
            if self.interval_start >= self.interval_end:
                raise ValueError("interval_start must precede interval_end")
        except ValueError as error:
            _fail("EXPERIMENT_SPEC_INVALID", str(error))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "market_bundle_ref": self.market_bundle_ref,
                "dataset_revision": self.dataset_revision,
                "interval_start": self.interval_start,
                "interval_end": self.interval_end,
            }
        )

    @property
    def canonical_wire(self) -> str:
        return _canonical_json(self.payload)


@dataclass(frozen=True, slots=True)
class FeatureRecipe:
    feature_key: str
    feature_code_hash: str
    feature_schema_hash: str
    input_names: tuple[str, ...]

    def __post_init__(self) -> None:
        try:
            _nonempty_string(self.feature_key, "feature_key")
            _content_hash(self.feature_code_hash, "feature_code_hash")
            _content_hash(self.feature_schema_hash, "feature_schema_hash")
            if type(self.input_names) is not tuple or not self.input_names:
                raise ValueError("input_names must be a nonempty tuple")
            names = tuple(_nonempty_string(item, "input_name") for item in self.input_names)
            if names != tuple(sorted(names)) or len(names) != len(set(names)):
                raise ValueError("input_names must be unique and sorted")
            object.__setattr__(self, "input_names", names)
        except ValueError as error:
            _fail("MODEL_BUILD_PLAN_INVALID", str(error))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "feature_key": self.feature_key,
                "feature_code_hash": self.feature_code_hash,
                "feature_schema_hash": self.feature_schema_hash,
                "input_names": self.input_names,
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("feature_recipe", self.payload)


@dataclass(frozen=True, slots=True)
class TrainerRecipe:
    trainer_key: str
    training_code_hash: str
    model_key: str
    hyperparameters: object

    def __post_init__(self) -> None:
        if not isinstance(self.hyperparameters, Mapping):
            _fail(
                "MODEL_BUILD_PLAN_INVALID",
                "hyperparameters must be an explicit canonical object",
            )
        try:
            _nonempty_string(self.trainer_key, "trainer_key")
            _content_hash(self.training_code_hash, "training_code_hash")
            _nonempty_string(self.model_key, "model_key")
            object.__setattr__(self, "hyperparameters", _freeze_json(self.hyperparameters))
        except ValueError as error:
            _fail("MODEL_BUILD_PLAN_INVALID", str(error))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "trainer_key": self.trainer_key,
                "training_code_hash": self.training_code_hash,
                "model_key": self.model_key,
                "hyperparameters": self.hyperparameters,
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("trainer_recipe", self.payload)


@dataclass(frozen=True, slots=True)
class ModelBuildPlan:
    feature_recipe_ref: object
    trainer_recipe_ref: object
    training_slice: DataSlice
    seed: int

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "feature_recipe_ref",
                _canonical_ref(
                    self.feature_recipe_ref,
                    "feature_recipe_ref",
                    expected_artifact_type="feature_recipe",
                ),
            )
            object.__setattr__(
                self,
                "trainer_recipe_ref",
                _canonical_ref(
                    self.trainer_recipe_ref,
                    "trainer_recipe_ref",
                    expected_artifact_type="trainer_recipe",
                ),
            )
            if type(self.training_slice) is not DataSlice:
                raise ValueError("training_slice must be a DataSlice")
            if type(self.seed) is not int or self.seed < 0:
                raise ValueError("seed must be a nonnegative integer")
        except ValueError as error:
            _fail("MODEL_BUILD_PLAN_INVALID", str(error))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "feature_recipe_ref": self.feature_recipe_ref,
                "trainer_recipe_ref": self.trainer_recipe_ref,
                "training_slice": self.training_slice.payload,
                "seed": self.seed,
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("model_build_plan", self.payload)


@dataclass(frozen=True, slots=True)
class FeatureDatasetManifest:
    model_build_plan_ref: object
    dataset_revision: str
    interval_start: str
    interval_end: str
    feature_schema_hash: str
    training_data_hash: str
    row_count: int

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "model_build_plan_ref",
                _canonical_ref(
                    self.model_build_plan_ref,
                    "model_build_plan_ref",
                    expected_artifact_type="model_build_plan",
                ),
            )
            _nonempty_string(self.dataset_revision, "dataset_revision")
            object.__setattr__(
                self, "interval_start", _canonical_utc(self.interval_start, "interval_start")
            )
            object.__setattr__(
                self, "interval_end", _canonical_utc(self.interval_end, "interval_end")
            )
            if self.interval_start >= self.interval_end:
                raise ValueError("interval_start must precede interval_end")
            _content_hash(self.feature_schema_hash, "feature_schema_hash")
            _content_hash(self.training_data_hash, "training_data_hash")
            if type(self.row_count) is not int or self.row_count <= 0:
                raise ValueError("row_count must be a positive integer")
        except ValueError as error:
            _fail("MODEL_BINDING_INVALID", str(error))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "model_build_plan_ref": self.model_build_plan_ref,
                "dataset_revision": self.dataset_revision,
                "interval_start": self.interval_start,
                "interval_end": self.interval_end,
                "feature_schema_hash": self.feature_schema_hash,
                "training_data_hash": self.training_data_hash,
                "row_count": self.row_count,
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("feature_dataset_manifest", self.payload)


@dataclass(frozen=True, slots=True)
class ModelBuildEvidence:
    model_build_plan_ref: object
    feature_dataset_manifest_ref: object
    model_artifact: object

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "model_build_plan_ref",
                _canonical_ref(
                    self.model_build_plan_ref,
                    "model_build_plan_ref",
                    expected_artifact_type="model_build_plan",
                ),
            )
            object.__setattr__(
                self,
                "feature_dataset_manifest_ref",
                _canonical_ref(
                    self.feature_dataset_manifest_ref,
                    "feature_dataset_manifest_ref",
                    expected_artifact_type="feature_dataset_manifest",
                ),
            )
            object.__setattr__(
                self, "model_artifact", _canonical_model_artifact(self.model_artifact)
            )
        except ValueError as error:
            _fail("MODEL_BINDING_INVALID", str(error))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "model_build_plan_ref": self.model_build_plan_ref,
                "feature_dataset_manifest_ref": self.feature_dataset_manifest_ref,
                "model_artifact": self.model_artifact,
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("model_build_evidence", self.payload)


@dataclass(frozen=True, slots=True)
class ParameterCombination:
    values: tuple[tuple[str, object], ...]

    def __post_init__(self) -> None:
        if type(self.values) is not tuple or not self.values:
            _fail(
                "EXPERIMENT_SPEC_INVALID", "parameter values must be a nonempty tuple"
            )
        canonical: list[tuple[str, object]] = []
        try:
            for pair in self.values:
                if type(pair) is not tuple or len(pair) != 2:
                    raise ValueError("parameter values must contain name/value tuples")
                canonical.append(
                    (_nonempty_string(pair[0], "parameter name"), _freeze_json(pair[1]))
                )
        except ValueError as error:
            _fail("EXPERIMENT_SPEC_INVALID", str(error))
        names = tuple(name for name, _ in canonical)
        if names != tuple(sorted(names)):
            _fail(
                "EXPERIMENT_SPEC_INVALID",
                "parameter names must be lexicographically sorted",
            )
        if len(names) != len(set(names)):
            _fail("TASK_AXIS_DUPLICATE", "parameter names must be unique")
        object.__setattr__(self, "values", tuple(canonical))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType({name: value for name, value in self.values})

    @property
    def canonical_wire(self) -> str:
        return _canonical_json(self.payload)


@dataclass(frozen=True, slots=True)
class ExperimentSpec:
    hypothesis_ref: object
    strategy_definition_ref: object
    data_slices: tuple[DataSlice, ...]
    parameter_combinations: tuple[ParameterCombination, ...]
    seeds: tuple[int, ...]
    scenario_refs: tuple[object, ...]
    backtest_template_ref: object
    model_build_plan: object | None
    metric_profile_refs: tuple[object, ...]
    budget: object

    def __post_init__(self) -> None:
        plan = self.model_build_plan
        if plan is not None and type(plan) is not ModelBuildPlan:
            _fail("MODEL_BUILD_PLAN_INVALID", "model_build_plan must be a ModelBuildPlan")
        try:
            object.__setattr__(
                self,
                "hypothesis_ref",
                _canonical_ref(self.hypothesis_ref, "hypothesis_ref"),
            )
            object.__setattr__(
                self,
                "strategy_definition_ref",
                _canonical_ref(self.strategy_definition_ref, "strategy_definition_ref"),
            )
            object.__setattr__(
                self,
                "backtest_template_ref",
                _canonical_ref(self.backtest_template_ref, "backtest_template_ref"),
            )
            if self.budget is None or type(self.budget) is bool:
                raise ValueError("budget must be an explicit canonical value")
            object.__setattr__(self, "budget", _freeze_json(self.budget))
        except ValueError as error:
            _fail("EXPERIMENT_SPEC_INVALID", str(error))

        self._validate_object_axis(
            "data_slices", self.data_slices, DataSlice, lambda item: item.canonical_wire
        )
        self._validate_object_axis(
            "parameter_combinations",
            self.parameter_combinations,
            ParameterCombination,
            lambda item: item.canonical_wire,
        )
        if plan is not None:
            if plan.training_slice.canonical_wire not in {
                item.canonical_wire for item in self.data_slices
            }:
                _fail(
                    "MODEL_BUILD_PLAN_INVALID",
                    "training_slice must be one of the Experiment data_slices",
                )
            object.__setattr__(self, "model_build_plan", plan.ref)

        if type(self.seeds) is not tuple or not self.seeds:
            _fail("EXPERIMENT_SPEC_INVALID", "seeds must be a nonempty tuple")
        if any(type(seed) is not int or seed < 0 for seed in self.seeds):
            _fail("EXPERIMENT_SPEC_INVALID", "seeds must be nonnegative integers")
        if self.seeds != tuple(sorted(self.seeds)):
            _fail("EXPERIMENT_SPEC_INVALID", "seeds must be ascending")
        if len(self.seeds) != len(set(self.seeds)):
            _fail("TASK_AXIS_DUPLICATE", "seeds must be unique")

        self._validate_ref_axis("scenario_refs", self.scenario_refs)
        self._validate_ref_axis(
            "metric_profile_refs",
            self.metric_profile_refs,
            expected_artifact_type="backtest_metric_profile",
        )

    def _validate_object_axis(
        self, name: str, value: object, item_type: type, key: Any
    ) -> None:
        if type(value) is not tuple or not value:
            _fail("EXPERIMENT_SPEC_INVALID", f"{name} must be a nonempty tuple")
        if any(type(item) is not item_type for item in value):
            _fail("EXPERIMENT_SPEC_INVALID", f"{name} contains the wrong value type")
        keys = tuple(key(item) for item in value)
        if keys != tuple(sorted(keys)):
            _fail("EXPERIMENT_SPEC_INVALID", f"{name} must be canonically sorted")
        if len(keys) != len(set(keys)):
            _fail("TASK_AXIS_DUPLICATE", f"{name} must be unique")

    def _validate_ref_axis(
        self,
        name: str,
        value: object,
        *,
        expected_artifact_type: str | None = None,
    ) -> None:
        if type(value) is not tuple or not value:
            _fail("EXPERIMENT_SPEC_INVALID", f"{name} must be a nonempty tuple")
        try:
            refs = tuple(
                _canonical_ref(
                    item, name, expected_artifact_type=expected_artifact_type
                )
                for item in value
            )
        except ValueError as error:
            _fail("EXPERIMENT_SPEC_INVALID", str(error))
        wires = tuple(_canonical_json(ref) for ref in refs)
        if wires != tuple(sorted(wires)):
            _fail("EXPERIMENT_SPEC_INVALID", f"{name} must be canonically sorted")
        if len(wires) != len(set(wires)):
            _fail("TASK_AXIS_DUPLICATE", f"{name} must be unique")
        object.__setattr__(self, name, refs)

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "hypothesis_ref": self.hypothesis_ref,
                "strategy_definition_ref": self.strategy_definition_ref,
                "data_slices": tuple(item.payload for item in self.data_slices),
                "parameter_combinations": tuple(
                    item.payload for item in self.parameter_combinations
                ),
                "seeds": self.seeds,
                "scenario_refs": self.scenario_refs,
                "backtest_template_ref": self.backtest_template_ref,
                "model_build_plan": self.model_build_plan,
                "metric_profile_refs": self.metric_profile_refs,
                "budget": self.budget,
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("experiment_spec", self.payload)

    @property
    def experiment_ref(self) -> object:
        return self.ref


@dataclass(frozen=True, slots=True)
class TrialDeclaration:
    experiment_ref: object
    parameter_values: ParameterCombination
    data_slice: DataSlice
    scenario_ref: object
    seed: int
    backtest_template_ref: object
    model_input_bindings: tuple[tuple[str, object], ...] = ()

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "experiment_ref",
                _canonical_ref(
                    self.experiment_ref,
                    "experiment_ref",
                    expected_artifact_type="experiment_spec",
                ),
            )
            if type(self.parameter_values) is not ParameterCombination:
                raise ValueError("parameter_values must be a ParameterCombination")
            if type(self.data_slice) is not DataSlice:
                raise ValueError("data_slice must be a DataSlice")
            object.__setattr__(
                self, "scenario_ref", _canonical_ref(self.scenario_ref, "scenario_ref")
            )
            if type(self.seed) is not int or self.seed < 0:
                raise ValueError("seed must be a nonnegative integer")
            object.__setattr__(
                self,
                "backtest_template_ref",
                _canonical_ref(self.backtest_template_ref, "backtest_template_ref"),
            )
            if type(self.model_input_bindings) is not tuple:
                raise ValueError("model_input_bindings must be a tuple")
            bindings: list[tuple[str, object]] = []
            for pair in self.model_input_bindings:
                if type(pair) is not tuple or len(pair) != 2:
                    raise ValueError("model_input_bindings must contain pairs")
                bindings.append(
                    (
                        _nonempty_string(pair[0], "model binding name"),
                        _freeze_json(pair[1]),
                    )
                )
            names = tuple(name for name, _ in bindings)
            if len(names) != len(set(names)) or names != tuple(sorted(names)):
                raise ValueError("model_input_bindings must have unique sorted names")
            object.__setattr__(self, "model_input_bindings", tuple(bindings))
        except ValueError as error:
            _fail("EXPERIMENT_SPEC_INVALID", str(error))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "experiment_ref": self.experiment_ref,
                "parameter_values": self.parameter_values.payload,
                "data_slice": self.data_slice.payload,
                "scenario_ref": self.scenario_ref,
                "seed": self.seed,
                "backtest_template_ref": self.backtest_template_ref,
                "model_input_bindings": MappingProxyType(
                    dict(self.model_input_bindings)
                ),
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("trial_declaration", self.payload)


@dataclass(frozen=True, slots=True)
class AnalysisTask:
    experiment_ref: object
    trial_declaration_ref: object
    metric_profile_ref: object

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "experiment_ref",
                _canonical_ref(
                    self.experiment_ref,
                    "experiment_ref",
                    expected_artifact_type="experiment_spec",
                ),
            )
            object.__setattr__(
                self,
                "trial_declaration_ref",
                _canonical_ref(
                    self.trial_declaration_ref,
                    "trial_declaration_ref",
                    expected_artifact_type="trial_declaration",
                ),
            )
            object.__setattr__(
                self,
                "metric_profile_ref",
                _canonical_ref(
                    self.metric_profile_ref,
                    "metric_profile_ref",
                    expected_artifact_type="backtest_metric_profile",
                ),
            )
        except ValueError as error:
            _fail("EXPERIMENT_SPEC_INVALID", str(error))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "experiment_ref": self.experiment_ref,
                "trial_declaration_ref": self.trial_declaration_ref,
                "metric_profile_ref": self.metric_profile_ref,
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("analysis_task", self.payload)


@dataclass(frozen=True, slots=True)
class FeatureBuildTask:
    experiment_ref: object
    model_build_plan_ref: object

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "experiment_ref",
                _canonical_ref(
                    self.experiment_ref,
                    "experiment_ref",
                    expected_artifact_type="experiment_spec",
                ),
            )
            object.__setattr__(
                self,
                "model_build_plan_ref",
                _canonical_ref(
                    self.model_build_plan_ref,
                    "model_build_plan_ref",
                    expected_artifact_type="model_build_plan",
                ),
            )
        except ValueError as error:
            _fail("MODEL_BUILD_PLAN_INVALID", str(error))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "experiment_ref": self.experiment_ref,
                "model_build_plan_ref": self.model_build_plan_ref,
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("feature_build_task", self.payload)


@dataclass(frozen=True, slots=True)
class ModelTrainingTask:
    experiment_ref: object
    model_build_plan_ref: object
    feature_build_task_ref: object

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "experiment_ref",
                _canonical_ref(
                    self.experiment_ref,
                    "experiment_ref",
                    expected_artifact_type="experiment_spec",
                ),
            )
            object.__setattr__(
                self,
                "model_build_plan_ref",
                _canonical_ref(
                    self.model_build_plan_ref,
                    "model_build_plan_ref",
                    expected_artifact_type="model_build_plan",
                ),
            )
            object.__setattr__(
                self,
                "feature_build_task_ref",
                _canonical_ref(
                    self.feature_build_task_ref,
                    "feature_build_task_ref",
                    expected_artifact_type="feature_build_task",
                ),
            )
        except ValueError as error:
            _fail("MODEL_BUILD_PLAN_INVALID", str(error))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "experiment_ref": self.experiment_ref,
                "model_build_plan_ref": self.model_build_plan_ref,
                "feature_build_task_ref": self.feature_build_task_ref,
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("model_training_task", self.payload)


TaskArtifact = TrialDeclaration | AnalysisTask | FeatureBuildTask | ModelTrainingTask
_TASK_ARTIFACTS: Mapping[str, tuple[str, type]] = MappingProxyType(
    {
        "FEATURE_BUILD": ("feature_build_task", FeatureBuildTask),
        "MODEL_TRAINING": ("model_training_task", ModelTrainingTask),
        "TRIAL": ("trial_declaration", TrialDeclaration),
        "ANALYSIS": ("analysis_task", AnalysisTask),
    }
)


@dataclass(frozen=True, slots=True, eq=False)
class TaskRef:
    kind: str
    task_artifact_ref: object
    _artifact: TaskArtifact | None = field(default=None, repr=False, compare=False)
    _experiment_ref: object | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.kind not in _TASK_KINDS:
            raise ValueError("TaskRef.kind is not supported")
        expected, expected_class = _TASK_ARTIFACTS[self.kind]
        object.__setattr__(
            self,
            "task_artifact_ref",
            _canonical_ref(
                self.task_artifact_ref,
                "task_artifact_ref",
                expected_artifact_type=expected,
            ),
        )
        if self._artifact is not None:
            if type(self._artifact) is not expected_class or not _same_wire(
                self._artifact.ref, self.task_artifact_ref
            ):
                raise ValueError("TaskRef artifact does not match its reference")
            object.__setattr__(self, "_experiment_ref", self._artifact.experiment_ref)
        elif self._experiment_ref is not None:
            object.__setattr__(
                self,
                "_experiment_ref",
                _canonical_ref(
                    self._experiment_ref,
                    "experiment_ref",
                    expected_artifact_type="experiment_spec",
                ),
            )

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {"kind": self.kind, "task_artifact_ref": self.task_artifact_ref}
        )

    @property
    def canonical_wire(self) -> str:
        return _canonical_json(self.payload)

    @property
    def experiment_ref(self) -> object | None:
        return self._experiment_ref

    @property
    def artifact(self) -> TaskArtifact | None:
        return self._artifact

    def __hash__(self) -> int:
        return hash(self.canonical_wire)

    def __eq__(self, other: object) -> bool:
        return type(other) is TaskRef and self.canonical_wire == other.canonical_wire


def validate_model_build(
    plan: ModelBuildPlan,
    feature_recipe: FeatureRecipe,
    trainer_recipe: TrainerRecipe,
    feature_manifest: FeatureDatasetManifest,
    model_artifact: object,
) -> ModelBuildEvidence:
    if (
        type(plan) is not ModelBuildPlan
        or type(feature_recipe) is not FeatureRecipe
        or type(trainer_recipe) is not TrainerRecipe
        or type(feature_manifest) is not FeatureDatasetManifest
    ):
        _fail("MODEL_BINDING_INVALID")
    try:
        artifact = _canonical_model_artifact(model_artifact)
    except ValueError as error:
        _fail("MODEL_BINDING_INVALID", str(error))
    expected_start = _utc_epoch_nanoseconds(plan.training_slice.interval_start)
    expected_end = _utc_epoch_nanoseconds(plan.training_slice.interval_end)
    if not (
        _same_wire(plan.feature_recipe_ref, feature_recipe.ref)
        and _same_wire(plan.trainer_recipe_ref, trainer_recipe.ref)
        and _same_wire(feature_manifest.model_build_plan_ref, plan.ref)
        and feature_manifest.dataset_revision == plan.training_slice.dataset_revision
        and feature_manifest.interval_start == plan.training_slice.interval_start
        and feature_manifest.interval_end == plan.training_slice.interval_end
        and feature_manifest.feature_schema_hash == feature_recipe.feature_schema_hash
        and artifact["model_key"] == trainer_recipe.model_key
        and artifact["training_data_hash"] == feature_manifest.training_data_hash
        and artifact["training_code_hash"] == trainer_recipe.training_code_hash
        and artifact["feature_schema_hash"] == feature_recipe.feature_schema_hash
        and _utc_instant_nanoseconds(artifact["training_start"], "training_start")
        == expected_start
        and _utc_instant_nanoseconds(artifact["training_end"], "training_end")
        == expected_end
        and artifact["supersedes_revision_id"] is None
    ):
        _fail("MODEL_BINDING_INVALID")
    return ModelBuildEvidence(plan.ref, feature_manifest.ref, artifact)


def build_trial_declarations(
    experiment_spec: ExperimentSpec,
) -> tuple[TrialDeclaration, ...]:
    if type(experiment_spec) is not ExperimentSpec:
        _fail("EXPERIMENT_SPEC_INVALID")
    declarations = tuple(
        TrialDeclaration(
            experiment_ref=experiment_spec.ref,
            parameter_values=parameters,
            data_slice=data_slice,
            scenario_ref=scenario_ref,
            seed=seed,
            backtest_template_ref=experiment_spec.backtest_template_ref,
            model_input_bindings=(
                ()
                if experiment_spec.model_build_plan is None
                else (("primary_model", experiment_spec.model_build_plan),)
            ),
        )
        for parameters in experiment_spec.parameter_combinations
        for data_slice in experiment_spec.data_slices
        for scenario_ref in experiment_spec.scenario_refs
        for seed in experiment_spec.seeds
    )
    return tuple(sorted(declarations, key=lambda item: _canonical_json(item.ref)))


def build_analysis_tasks(
    experiment_spec: ExperimentSpec,
    trials: tuple[TrialDeclaration, ...] | None = None,
) -> tuple[AnalysisTask, ...]:
    if type(experiment_spec) is not ExperimentSpec:
        _fail("EXPERIMENT_SPEC_INVALID")
    trials = build_trial_declarations(experiment_spec) if trials is None else trials
    if type(trials) is not tuple or any(
        type(trial) is not TrialDeclaration for trial in trials
    ):
        _fail("EXPERIMENT_SPEC_INVALID")
    tasks = tuple(
        AnalysisTask(experiment_spec.ref, trial.ref, metric_profile_ref)
        for trial in trials
        for metric_profile_ref in experiment_spec.metric_profile_refs
    )
    return tuple(sorted(tasks, key=lambda item: _canonical_json(item.ref)))


def build_task_universe(experiment_spec: ExperimentSpec) -> tuple[TaskRef, ...]:
    trials = build_trial_declarations(experiment_spec)
    analyses = build_analysis_tasks(experiment_spec, trials)
    existing = tuple(
        sorted(
            [TaskRef("TRIAL", trial.ref, trial) for trial in trials]
            + [TaskRef("ANALYSIS", analysis.ref, analysis) for analysis in analyses],
            key=lambda item: (item.kind, _canonical_json(item.task_artifact_ref)),
        )
    )
    build: tuple[TaskRef, ...] = ()
    if experiment_spec.model_build_plan is not None:
        feature = FeatureBuildTask(experiment_spec.ref, experiment_spec.model_build_plan)
        training = ModelTrainingTask(
            experiment_spec.ref, experiment_spec.model_build_plan, feature.ref
        )
        build = (
            TaskRef("FEATURE_BUILD", feature.ref, feature),
            TaskRef("MODEL_TRAINING", training.ref, training),
        )
    tasks = build + existing
    if len(tasks) != len(set(tasks)):
        _fail("TASK_AXIS_DUPLICATE")
    return tasks


@dataclass(frozen=True, slots=True)
class TrialCompletedPublication:
    publication_ref: object

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "publication_ref",
                _canonical_ref(
                    self.publication_ref,
                    "publication_ref",
                    expected_type="backtest_canonical_publication_ref",
                    expected_artifact_type="canonical_publication_manifest",
                ),
            )
        except ValueError as error:
            _fail("TASK_OUTCOME_INVALID", str(error))


@dataclass(frozen=True, slots=True)
class AnalysisDerivation:
    analysis_ref: object
    source_publication_ref: object

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "analysis_ref",
                _canonical_ref(
                    self.analysis_ref,
                    "analysis_ref",
                    expected_type="analysis_artifact_ref",
                    expected_artifact_type="backtest_analysis",
                ),
            )
            object.__setattr__(
                self,
                "source_publication_ref",
                _canonical_ref(
                    self.source_publication_ref,
                    "source_publication_ref",
                    expected_type="backtest_canonical_publication_ref",
                    expected_artifact_type="canonical_publication_manifest",
                ),
            )
        except ValueError as error:
            _fail("TASK_OUTCOME_INVALID", str(error))


@dataclass(frozen=True, slots=True)
class FeatureDatasetPublication:
    feature_dataset_manifest_ref: object

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "feature_dataset_manifest_ref",
                _canonical_ref(
                    self.feature_dataset_manifest_ref,
                    "feature_dataset_manifest_ref",
                    expected_artifact_type="feature_dataset_manifest",
                ),
            )
        except ValueError as error:
            _fail("TASK_OUTCOME_INVALID", str(error))


@dataclass(frozen=True, slots=True)
class ModelBuildPublication:
    model_build_evidence_ref: object

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "model_build_evidence_ref",
                _canonical_ref(
                    self.model_build_evidence_ref,
                    "model_build_evidence_ref",
                    expected_artifact_type="model_build_evidence",
                ),
            )
        except ValueError as error:
            _fail("TASK_OUTCOME_INVALID", str(error))


@dataclass(frozen=True, slots=True)
class BacktestTerminal:
    status: str
    durable_evidence_ref: object

    def __post_init__(self) -> None:
        if self.status not in _TERMINAL_STATES:
            _fail("TASK_OUTCOME_INVALID", "invalid Backtest terminal status")
        try:
            object.__setattr__(
                self,
                "durable_evidence_ref",
                _canonical_ref(
                    self.durable_evidence_ref,
                    "durable_evidence_ref",
                    expected_type="artifact_ref",
                ),
            )
        except ValueError as error:
            _fail("TASK_OUTCOME_INVALID", str(error))


@dataclass(frozen=True, slots=True)
class UpstreamTaskOutcome:
    task_outcome_ref: object

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "task_outcome_ref",
                _canonical_ref(
                    self.task_outcome_ref,
                    "task_outcome_ref",
                    expected_type="artifact_ref",
                    expected_artifact_type="task_outcome",
                ),
            )
        except ValueError as error:
            _fail("TASK_OUTCOME_INVALID", str(error))


@dataclass(frozen=True, slots=True)
class DependencyBlock:
    reason_code: str
    dependency_ref: object | None = None

    def __post_init__(self) -> None:
        try:
            _nonempty_string(self.reason_code, "reason_code")
            if self.dependency_ref is not None:
                object.__setattr__(
                    self,
                    "dependency_ref",
                    _canonical_ref(self.dependency_ref, "dependency_ref"),
                )
        except ValueError as error:
            _fail("TASK_OUTCOME_INVALID", str(error))


@dataclass(frozen=True, slots=True)
class LocalFailure:
    failure_code: str

    def __post_init__(self) -> None:
        try:
            _nonempty_string(self.failure_code, "failure_code")
        except ValueError as error:
            _fail("TASK_OUTCOME_INVALID", str(error))


Witness = (
    TrialCompletedPublication
    | AnalysisDerivation
    | FeatureDatasetPublication
    | ModelBuildPublication
    | BacktestTerminal
    | UpstreamTaskOutcome
    | DependencyBlock
    | LocalFailure
)


def _witness_payload(witness: Witness) -> Mapping[str, object]:
    if type(witness) is TrialCompletedPublication:
        return MappingProxyType(
            {
                "trial_completed_publication": MappingProxyType(
                    {"publication_ref": witness.publication_ref}
                )
            }
        )
    if type(witness) is AnalysisDerivation:
        return MappingProxyType(
            {
                "analysis_derivation": MappingProxyType(
                    {
                        "analysis_ref": witness.analysis_ref,
                        "source_publication_ref": witness.source_publication_ref,
                    }
                )
            }
        )
    if type(witness) is FeatureDatasetPublication:
        return MappingProxyType(
            {
                "feature_dataset_manifest": MappingProxyType(
                    {
                        "feature_dataset_manifest_ref": witness.feature_dataset_manifest_ref
                    }
                )
            }
        )
    if type(witness) is ModelBuildPublication:
        return MappingProxyType(
            {
                "model_build_evidence": MappingProxyType(
                    {"model_build_evidence_ref": witness.model_build_evidence_ref}
                )
            }
        )
    if type(witness) is BacktestTerminal:
        return MappingProxyType(
            {
                "backtest_terminal": MappingProxyType(
                    {
                        "status": witness.status,
                        "durable_evidence_ref": witness.durable_evidence_ref,
                    }
                )
            }
        )
    if type(witness) is UpstreamTaskOutcome:
        return MappingProxyType(
            {
                "upstream_task_outcome": MappingProxyType(
                    {"task_outcome_ref": witness.task_outcome_ref}
                )
            }
        )
    if type(witness) is DependencyBlock:
        return MappingProxyType(
            {
                "dependency_block": MappingProxyType(
                    {
                        "reason_code": witness.reason_code,
                        "dependency_ref": witness.dependency_ref,
                    }
                )
            }
        )
    if type(witness) is LocalFailure:
        return MappingProxyType(
            {"local_failure": MappingProxyType({"failure_code": witness.failure_code})}
        )
    raise AssertionError("unreachable witness")


def _coerce_witness(value: object) -> Witness:
    if type(value) in {
        TrialCompletedPublication,
        AnalysisDerivation,
        FeatureDatasetPublication,
        ModelBuildPublication,
        BacktestTerminal,
        UpstreamTaskOutcome,
        DependencyBlock,
        LocalFailure,
    }:
        return value  # type: ignore[return-value]
    if not isinstance(value, Mapping) or len(value) != 1:
        _fail("TASK_OUTCOME_INVALID", "witness must be one tagged value")
    tag, payload = next(iter(value.items()))
    if not isinstance(payload, Mapping):
        _fail("TASK_OUTCOME_INVALID")
    try:
        if tag == "trial_completed_publication" and set(payload) == {"publication_ref"}:
            return TrialCompletedPublication(payload["publication_ref"])
        if tag == "analysis_derivation" and set(payload) == {
            "analysis_ref",
            "source_publication_ref",
        }:
            return AnalysisDerivation(
                payload["analysis_ref"], payload["source_publication_ref"]
            )
        if tag == "feature_dataset_manifest" and set(payload) == {
            "feature_dataset_manifest_ref"
        }:
            return FeatureDatasetPublication(payload["feature_dataset_manifest_ref"])
        if tag == "model_build_evidence" and set(payload) == {
            "model_build_evidence_ref"
        }:
            return ModelBuildPublication(payload["model_build_evidence_ref"])
        if tag == "backtest_terminal" and set(payload) == {
            "status",
            "durable_evidence_ref",
        }:
            return BacktestTerminal(payload["status"], payload["durable_evidence_ref"])  # type: ignore[arg-type]
        if tag == "upstream_task_outcome" and set(payload) == {"task_outcome_ref"}:
            return UpstreamTaskOutcome(payload["task_outcome_ref"])
        if tag == "dependency_block" and set(payload) == {
            "reason_code",
            "dependency_ref",
        }:
            return DependencyBlock(payload["reason_code"], payload["dependency_ref"])  # type: ignore[arg-type]
        if tag == "local_failure" and set(payload) == {"failure_code"}:
            return LocalFailure(payload["failure_code"])  # type: ignore[arg-type]
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, ResearchCoreError):
            raise
        _fail("TASK_OUTCOME_INVALID", str(error))
    _fail("TASK_OUTCOME_INVALID", "unknown or malformed witness")


@dataclass(frozen=True, slots=True)
class TaskOutcome:
    task_ref: TaskRef
    state: str
    witness: Witness | Mapping[str, object]

    def __post_init__(self) -> None:
        if type(self.task_ref) is not TaskRef or self.state not in _OUTCOME_STATES:
            _fail("TASK_OUTCOME_INVALID")
        witness = _coerce_witness(self.witness)
        object.__setattr__(self, "witness", witness)
        valid = False
        if self.task_ref.kind == "FEATURE_BUILD":
            valid = (
                self.state == "COMPLETED"
                and type(witness) is FeatureDatasetPublication
            ) or (
                self.state == "BLOCKED" and type(witness) is DependencyBlock
            ) or (self.state == "FAILED" and type(witness) is LocalFailure)
        elif self.task_ref.kind == "MODEL_TRAINING":
            valid = (
                self.state == "COMPLETED" and type(witness) is ModelBuildPublication
            ) or (
                self.state == "BLOCKED" and type(witness) is UpstreamTaskOutcome
            ) or (self.state == "FAILED" and type(witness) is LocalFailure)
        elif self.task_ref.kind == "TRIAL":
            valid = (
                (
                    self.state == "COMPLETED"
                    and type(witness) is TrialCompletedPublication
                )
                or (
                    self.state == "BLOCKED"
                    and type(witness)
                    in {BacktestTerminal, DependencyBlock, UpstreamTaskOutcome}
                )
                or (
                    self.state == "FAILED"
                    and type(witness) in {BacktestTerminal, LocalFailure}
                )
                or (self.state == "CANCELLED" and type(witness) is BacktestTerminal)
            )
        elif self.task_ref.kind == "ANALYSIS":
            valid = (
                (self.state == "COMPLETED" and type(witness) is AnalysisDerivation)
                or (self.state == "BLOCKED" and type(witness) is UpstreamTaskOutcome)
                or (self.state == "FAILED" and type(witness) is LocalFailure)
            )
        if not valid or (
            type(witness) is BacktestTerminal and witness.status != self.state
        ):
            _fail("TASK_OUTCOME_INVALID")

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "task_ref": self.task_ref.payload,
                "state": self.state,
                "witness": _witness_payload(self.witness),
            }  # type: ignore[arg-type]
        )

    @property
    def ref(self) -> object:
        return _local_ref("task_outcome", self.payload)


def map_backtest_observation(task_ref: TaskRef, observation: object) -> TaskOutcome:
    """Map one successful BT-PORT observation or stable port failure."""

    if type(task_ref) is not TaskRef:
        _fail("TASK_OUTCOME_INVALID")
    if not isinstance(observation, Mapping):
        code = getattr(observation, "code", None)
        if type(code) is not str or not code:
            _fail("TASK_OUTCOME_INVALID", "port failures must expose a stable code")
        return TaskOutcome(task_ref, "FAILED", LocalFailure(code))

    keys = set(observation)
    completed_fields = {
        "publication_ref",
        "semantic_run_id",
        "execution_result_hash",
        "result_grade",
    }
    if frozenset(keys) in {
        frozenset(completed_fields),
        frozenset(completed_fields | {"model_binding"}),
    }:
        if task_ref.kind != "TRIAL":
            _fail("TASK_OUTCOME_INVALID")
        return TaskOutcome(
            task_ref,
            "COMPLETED",
            TrialCompletedPublication(observation["publication_ref"]),
        )
    if keys == {"status", "durable_evidence_ref"}:
        if task_ref.kind != "TRIAL":
            _fail("TASK_OUTCOME_INVALID")
        status = observation["status"]
        if type(status) is not str:
            _fail("TASK_OUTCOME_INVALID")
        return TaskOutcome(
            task_ref,
            status,
            BacktestTerminal(status, observation["durable_evidence_ref"]),
        )
    if keys == {
        "analysis_ref",
        "metric_profile_ref",
        "source_publication_ref",
        "source_execution_result_hash",
        "simple_period_return",
        "trade_count",
        "result_grade",
    }:
        if task_ref.kind != "ANALYSIS":
            _fail("TASK_OUTCOME_INVALID")
        return TaskOutcome(
            task_ref,
            "COMPLETED",
            AnalysisDerivation(
                observation["analysis_ref"], observation["source_publication_ref"]
            ),
        )
    _fail("TASK_OUTCOME_INVALID", "observation is not a verified BT-PORT record")


def block_analysis_from_upstream(
    analysis_task_ref: TaskRef, trial_outcome: TaskOutcome
) -> TaskOutcome:
    if (
        type(analysis_task_ref) is not TaskRef
        or analysis_task_ref.kind != "ANALYSIS"
        or type(trial_outcome) is not TaskOutcome
        or trial_outcome.task_ref.kind != "TRIAL"
        or trial_outcome.state == "COMPLETED"
    ):
        _fail("TASK_OUTCOME_INVALID")
    analysis = analysis_task_ref.artifact
    trial = trial_outcome.task_ref.artifact
    if (
        type(analysis) is not AnalysisTask
        or type(trial) is not TrialDeclaration
        or not _same_wire(analysis.trial_declaration_ref, trial.ref)
    ):
        _fail("TASK_OUTCOME_INVALID")
    return TaskOutcome(
        analysis_task_ref, "BLOCKED", UpstreamTaskOutcome(trial_outcome.ref)
    )


@dataclass(frozen=True, slots=True)
class TaskAttemptStarted:
    task_ref: TaskRef
    ordinal: int
    parent_closed_attempt_ref: object | None
    selection_declaration_refs: tuple[object, ...]
    dispatch_ref: object | None = None

    def __post_init__(self) -> None:
        if (
            type(self.task_ref) is not TaskRef
            or type(self.ordinal) is not int
            or self.ordinal <= 0
        ):
            _fail("ATTEMPT_CHAIN_INVALID")
        try:
            if self.parent_closed_attempt_ref is not None:
                object.__setattr__(
                    self,
                    "parent_closed_attempt_ref",
                    _canonical_ref(
                        self.parent_closed_attempt_ref,
                        "parent_closed_attempt_ref",
                        expected_type="artifact_ref",
                        expected_artifact_type="task_attempt_closed",
                    ),
                )
            refs = _ref_tuple(
                self.selection_declaration_refs, "selection_declaration_refs"
            )
            if len(refs) != len({_canonical_json(ref) for ref in refs}):
                raise ValueError("selection_declaration_refs must be unique")
            object.__setattr__(
                self,
                "selection_declaration_refs",
                tuple(sorted(refs, key=_canonical_json)),
            )
            if self.dispatch_ref is not None:
                object.__setattr__(
                    self,
                    "dispatch_ref",
                    _canonical_ref(self.dispatch_ref, "dispatch_ref"),
                )
        except ValueError as error:
            _fail("ATTEMPT_CHAIN_INVALID", str(error))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "task_ref": self.task_ref.payload,
                "ordinal": self.ordinal,
                "parent_closed_attempt_ref": self.parent_closed_attempt_ref,
                "selection_declaration_refs": self.selection_declaration_refs,
                "dispatch_ref": self.dispatch_ref,
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("task_attempt_started", self.payload)


@dataclass(frozen=True, slots=True)
class TaskAttemptClosed:
    started_attempt_ref: object
    disposition: str
    task_outcome_ref: object | None
    failure_code: str | None

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "started_attempt_ref",
                _canonical_ref(
                    self.started_attempt_ref,
                    "started_attempt_ref",
                    expected_type="artifact_ref",
                    expected_artifact_type="task_attempt_started",
                ),
            )
            if self.disposition not in {"RETRYABLE_FAILURE", "ABANDONED", "TERMINAL"}:
                raise ValueError("invalid disposition")
            if self.disposition == "TERMINAL":
                if self.task_outcome_ref is None or self.failure_code is not None:
                    raise ValueError(
                        "TERMINAL requires one task outcome and no failure code"
                    )
                object.__setattr__(
                    self,
                    "task_outcome_ref",
                    _canonical_ref(
                        self.task_outcome_ref,
                        "task_outcome_ref",
                        expected_type="artifact_ref",
                        expected_artifact_type="task_outcome",
                    ),
                )
            elif (
                self.task_outcome_ref is not None
                or type(self.failure_code) is not str
                or not self.failure_code
            ):
                raise ValueError("retry/abandon close requires only a failure code")
        except ValueError as error:
            _fail("ATTEMPT_CHAIN_INVALID", str(error))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "started_attempt_ref": self.started_attempt_ref,
                "disposition": self.disposition,
                "task_outcome_ref": self.task_outcome_ref,
                "failure_code": self.failure_code,
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("task_attempt_closed", self.payload)


@dataclass(frozen=True, slots=True)
class OrderingCriterion:
    field_name: str
    direction: str

    def __post_init__(self) -> None:
        if self.field_name not in {
            "simple_period_return",
            "trade_count",
        } or self.direction not in {"ascending", "descending"}:
            _fail("SELECTION_POLICY_MISMATCH")

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {"field_name": self.field_name, "direction": self.direction}
        )


@dataclass(frozen=True, slots=True)
class HardFilter:
    field_name: str
    operator: str
    threshold: str | int

    def __post_init__(self) -> None:
        if self.field_name not in {
            "simple_period_return",
            "trade_count",
        } or self.operator not in {"gt", "gte", "lt", "lte", "eq"}:
            _fail("SELECTION_POLICY_MISMATCH")
        if self.field_name == "simple_period_return":
            try:
                object.__setattr__(
                    self, "threshold", _canonical_decimal(self.threshold, "threshold")
                )
            except ValueError as error:
                _fail("SELECTION_POLICY_MISMATCH", str(error))
        elif type(self.threshold) is not int or self.threshold < 0:
            _fail("SELECTION_POLICY_MISMATCH")

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "field_name": self.field_name,
                "operator": self.operator,
                "threshold": self.threshold,
            }
        )


def _ordering(value: object) -> OrderingCriterion:
    if type(value) is OrderingCriterion:
        return value
    if type(value) is tuple and len(value) == 2:
        return OrderingCriterion(value[0], value[1])  # type: ignore[arg-type]
    if isinstance(value, Mapping) and set(value) == {"field_name", "direction"}:
        return OrderingCriterion(value["field_name"], value["direction"])  # type: ignore[arg-type]
    _fail("SELECTION_POLICY_MISMATCH")


def _hard_filter(value: object) -> HardFilter:
    if type(value) is HardFilter:
        return value
    if type(value) is tuple and len(value) == 3:
        return HardFilter(value[0], value[1], value[2])  # type: ignore[arg-type]
    if isinstance(value, Mapping) and set(value) == {
        "field_name",
        "operator",
        "threshold",
    }:
        return HardFilter(value["field_name"], value["operator"], value["threshold"])  # type: ignore[arg-type]
    _fail("SELECTION_POLICY_MISMATCH")


@dataclass(frozen=True, slots=True)
class SelectionPolicy:
    metric_profile_ref: object
    eligible_trial_statuses: tuple[str, ...]
    accepted_backtest_grades: tuple[str, ...]
    hard_filters: tuple[HardFilter | tuple[object, ...] | Mapping[str, object], ...]
    ordering: tuple[OrderingCriterion | tuple[object, ...] | Mapping[str, object], ...]
    max_selections: int
    tie_break: str

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "metric_profile_ref",
                _canonical_ref(
                    self.metric_profile_ref,
                    "metric_profile_ref",
                    expected_artifact_type="backtest_metric_profile",
                ),
            )
        except ValueError as error:
            _fail("SELECTION_POLICY_MISMATCH", str(error))
        if self.eligible_trial_statuses != ("COMPLETED",):
            _fail("SELECTION_POLICY_MISMATCH")
        if (
            type(self.accepted_backtest_grades) is not tuple
            or not self.accepted_backtest_grades
            or any(
                type(grade) is not str or not grade
                for grade in self.accepted_backtest_grades
            )
            or len(self.accepted_backtest_grades)
            != len(set(self.accepted_backtest_grades))
            or self.accepted_backtest_grades
            != tuple(sorted(self.accepted_backtest_grades))
        ):
            _fail("SELECTION_POLICY_MISMATCH")
        if (
            type(self.hard_filters) is not tuple
            or type(self.ordering) is not tuple
            or not self.ordering
        ):
            _fail("SELECTION_POLICY_MISMATCH")
        object.__setattr__(
            self,
            "hard_filters",
            tuple(_hard_filter(item) for item in self.hard_filters),
        )
        object.__setattr__(
            self, "ordering", tuple(_ordering(item) for item in self.ordering)
        )
        if (
            type(self.max_selections) is not int
            or self.max_selections <= 0
            or self.tie_break != "trial_declaration_ref_ascending"
        ):
            _fail("SELECTION_POLICY_MISMATCH")

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "metric_profile_ref": self.metric_profile_ref,
                "eligible_trial_statuses": self.eligible_trial_statuses,
                "accepted_backtest_grades": self.accepted_backtest_grades,
                "hard_filters": tuple(item.payload for item in self.hard_filters),  # type: ignore[union-attr]
                "ordering": tuple(item.payload for item in self.ordering),  # type: ignore[union-attr]
                "max_selections": self.max_selections,
                "tie_break": self.tie_break,
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("selection_policy", self.payload)


@dataclass(frozen=True, slots=True)
class SelectionDeclaration:
    experiment_ref: object
    selection_policy_ref: object
    universe_kind: str
    declared_by_ref: object

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "experiment_ref",
                _canonical_ref(
                    self.experiment_ref,
                    "experiment_ref",
                    expected_artifact_type="experiment_spec",
                ),
            )
            object.__setattr__(
                self,
                "selection_policy_ref",
                _canonical_ref(
                    self.selection_policy_ref,
                    "selection_policy_ref",
                    expected_artifact_type="selection_policy",
                ),
            )
            if self.universe_kind != "candidate_trial_declarations_v1":
                raise ValueError("invalid selection universe")
            object.__setattr__(
                self,
                "declared_by_ref",
                _canonical_ref(self.declared_by_ref, "declared_by_ref"),
            )
        except ValueError as error:
            _fail("SELECTION_POLICY_MISMATCH", str(error))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "experiment_ref": self.experiment_ref,
                "selection_policy_ref": self.selection_policy_ref,
                "universe_kind": self.universe_kind,
                "declared_by_ref": self.declared_by_ref,
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("selection_declaration", self.payload)


@dataclass(frozen=True, slots=True)
class VerifiedAnalysis:
    analysis_ref: object
    trial_publication_ref: object
    metric_profile_ref: object
    simple_period_return: str
    trade_count: int
    result_grade: str

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "analysis_ref",
                _canonical_ref(
                    self.analysis_ref,
                    "analysis_ref",
                    expected_type="analysis_artifact_ref",
                    expected_artifact_type="backtest_analysis",
                ),
            )
            object.__setattr__(
                self,
                "trial_publication_ref",
                _canonical_ref(
                    self.trial_publication_ref,
                    "trial_publication_ref",
                    expected_type="backtest_canonical_publication_ref",
                    expected_artifact_type="canonical_publication_manifest",
                ),
            )
            object.__setattr__(
                self,
                "metric_profile_ref",
                _canonical_ref(
                    self.metric_profile_ref,
                    "metric_profile_ref",
                    expected_artifact_type="backtest_metric_profile",
                ),
            )
            object.__setattr__(
                self,
                "simple_period_return",
                _canonical_decimal(self.simple_period_return, "simple_period_return"),
            )
            if type(self.trade_count) is not int or self.trade_count < 0:
                raise ValueError("trade_count must be a nonnegative integer")
            _nonempty_string(self.result_grade, "result_grade")
        except ValueError as error:
            _fail("SELECTION_INPUT_INCOMPLETE", str(error))

    @classmethod
    def from_record(cls, record: Mapping[str, object]) -> VerifiedAnalysis:
        expected = {
            "analysis_ref",
            "metric_profile_ref",
            "source_publication_ref",
            "source_execution_result_hash",
            "simple_period_return",
            "trade_count",
            "result_grade",
        }
        if set(record) != expected:
            _fail("SELECTION_INPUT_INCOMPLETE")
        return cls(
            analysis_ref=record["analysis_ref"],
            trial_publication_ref=record["source_publication_ref"],
            metric_profile_ref=record["metric_profile_ref"],
            simple_period_return=record["simple_period_return"],  # type: ignore[arg-type]
            trade_count=record["trade_count"],  # type: ignore[arg-type]
            result_grade=record["result_grade"],  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class Selected:
    trial_declaration_ref: object
    analysis_ref: object
    trial_publication_ref: object
    selection_rank: int = 1

    @property
    def selected_trial_declaration_ref(self) -> object:
        return self.trial_declaration_ref

    @property
    def selected_analysis_ref(self) -> object:
        return self.analysis_ref

    @property
    def selected_publication_ref(self) -> object:
        return self.trial_publication_ref


@dataclass(frozen=True, slots=True)
class NoSelection:
    reason_code: str = "NO_ELIGIBLE_TRIAL"


@dataclass(frozen=True, slots=True)
class ExecutionEntry:
    log_sequence: int
    payload: object

    def __post_init__(self) -> None:
        if type(self.log_sequence) is not int or self.log_sequence <= 0:
            raise ValueError("log_sequence must be a positive integer")


class ExecutionProjection(Mapping[TaskRef, TaskOutcome]):
    __slots__ = (
        "_items",
        "selection_declarations",
        "selection_refs_complete",
        "cutoff_ref",
    )

    def __init__(
        self,
        items: Iterable[tuple[TaskRef, TaskOutcome]],
        *,
        selection_declarations: tuple[SelectionDeclaration, ...],
        selection_refs_complete: bool,
        cutoff_ref: object,
    ) -> None:
        self._items = tuple(items)
        self.selection_declarations = selection_declarations
        self.selection_refs_complete = selection_refs_complete
        self.cutoff_ref = cutoff_ref

    def __iter__(self) -> Iterator[TaskRef]:
        for task_ref, _ in self._items:
            yield task_ref

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, key: TaskRef) -> TaskOutcome:
        for task_ref, outcome in self._items:
            if task_ref == key:
                return outcome
        raise KeyError(key)


@dataclass(frozen=True, slots=True)
class ExperimentExecutionManifest:
    experiment_ref: object
    task_outcome_refs: tuple[object, ...]

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "experiment_ref",
                _canonical_ref(
                    self.experiment_ref,
                    "experiment_ref",
                    expected_artifact_type="experiment_spec",
                ),
            )
            if type(self.task_outcome_refs) is not tuple:
                raise ValueError("task_outcome_refs must be a tuple")
            refs = tuple(
                _canonical_ref(
                    ref,
                    "task_outcome_ref",
                    expected_type="artifact_ref",
                    expected_artifact_type="task_outcome",
                )
                for ref in self.task_outcome_refs
            )
            if len(refs) != len({_canonical_json(ref) for ref in refs}):
                _fail("TASK_OUTCOME_MISSING_OR_DUPLICATE")
            object.__setattr__(self, "task_outcome_refs", refs)
        except ResearchCoreError:
            raise
        except ValueError as error:
            _fail("TASK_OUTCOME_INVALID", str(error))

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "experiment_ref": self.experiment_ref,
                "task_outcome_refs": self.task_outcome_refs,
            }
        )

    @property
    def ref(self) -> object:
        return _local_ref("experiment_execution_manifest", self.payload)

    @property
    def outcome_map(self) -> Mapping[TaskRef, TaskOutcome]:
        outcomes = getattr(self, "_outcomes", ())
        if not outcomes:
            _fail("SELECTION_INPUT_INCOMPLETE")
        return MappingProxyType({outcome.task_ref: outcome for outcome in outcomes})


class _VerifiedExecutionManifest(ExperimentExecutionManifest):
    __slots__ = (
        "_outcomes",
        "_selection_declarations",
        "_selection_refs_complete",
        "_cutoff_ref",
    )

    def __init__(
        self,
        experiment_ref: object,
        task_outcome_refs: tuple[object, ...],
        outcomes: tuple[TaskOutcome, ...],
        selection_declarations: tuple[SelectionDeclaration, ...],
        selection_refs_complete: bool,
        cutoff_ref: object,
    ) -> None:
        super().__init__(experiment_ref, task_outcome_refs)
        object.__setattr__(self, "_outcomes", outcomes)
        object.__setattr__(self, "_selection_declarations", selection_declarations)
        object.__setattr__(self, "_selection_refs_complete", selection_refs_complete)
        object.__setattr__(self, "_cutoff_ref", cutoff_ref)


@dataclass(frozen=True, slots=True)
class CandidateFamily:
    experiment_ref: object
    execution_manifest_ref: object

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "experiment_ref",
                _canonical_ref(
                    self.experiment_ref,
                    "experiment_ref",
                    expected_artifact_type="experiment_spec",
                ),
            )
            object.__setattr__(
                self,
                "execution_manifest_ref",
                _canonical_ref(
                    self.execution_manifest_ref,
                    "execution_manifest_ref",
                    expected_artifact_type="experiment_execution_manifest",
                ),
            )
        except ValueError as error:
            _fail("TASK_OUTCOME_INVALID", str(error))


@dataclass(frozen=True, slots=True)
class _ParsedEntry:
    sequence: int
    payload: object


def _cutoff_ref(value: object) -> object:
    if not isinstance(value, Mapping):
        _fail("MANIFEST_CUTOFF_INVALID")
    plain = _plain_json(value)
    if type(plain) is not dict or not {
        "log_name",
        "log_sequence",
        "receipt_hash",
    }.issubset(plain):
        _fail("MANIFEST_CUTOFF_INVALID")
    if (
        plain["log_name"] != RESEARCH_EXECUTION_LOG
        or type(plain["log_sequence"]) is not int
        or plain["log_sequence"] <= 0
    ):
        _fail("MANIFEST_CUTOFF_INVALID")
    receipt_hash = plain["receipt_hash"]
    if type(receipt_hash) is not str or _HASH.fullmatch(receipt_hash) is None:
        _fail("MANIFEST_CUTOFF_INVALID")
    return _freeze_json(
        {
            "log_name": plain["log_name"],
            "log_sequence": plain["log_sequence"],
            "receipt_hash": receipt_hash,
        }
    )


def _entry(value: object) -> _ParsedEntry:
    if type(value) is ExecutionEntry:
        return _ParsedEntry(value.log_sequence, value.payload)
    if type(value) is tuple and len(value) == 2 and type(value[0]) is int:
        return _ParsedEntry(value[0], value[1])
    if isinstance(value, Mapping):
        sequence = value.get("log_sequence")
        if type(sequence) is not int and isinstance(value.get("entry_ref"), Mapping):
            sequence = value["entry_ref"].get("log_sequence")  # type: ignore[index]
        if type(sequence) is int and "payload" in value:
            return _ParsedEntry(sequence, value["payload"])
    sequence = getattr(value, "log_sequence", None)
    if type(sequence) is int and hasattr(value, "payload"):
        return _ParsedEntry(sequence, getattr(value, "payload"))
    _fail("MANIFEST_CUTOFF_INVALID", "entries must expose log_sequence and payload")


def _task_ref(value: object, universe: Mapping[str, TaskRef] | None = None) -> TaskRef:
    if type(value) is TaskRef:
        candidate = value
    elif isinstance(value, Mapping) and set(value) == {"kind", "task_artifact_ref"}:
        try:
            candidate = TaskRef(value["kind"], value["task_artifact_ref"])  # type: ignore[arg-type]
        except ValueError as error:
            _fail("TASK_REF_FOREIGN", str(error))
    else:
        _fail("TASK_REF_FOREIGN")
    if universe is not None and candidate.canonical_wire in universe:
        return universe[candidate.canonical_wire]
    return candidate


def _outcome(value: object, universe: Mapping[str, TaskRef]) -> TaskOutcome:
    if type(value) is TaskOutcome:
        task = _task_ref(value.task_ref, universe)
        return (
            value
            if task is value.task_ref
            else TaskOutcome(task, value.state, value.witness)
        )
    if not isinstance(value, Mapping) or set(value) != {"task_ref", "state", "witness"}:
        _fail("TASK_OUTCOME_INVALID")
    return TaskOutcome(
        _task_ref(value["task_ref"], universe), value["state"], value["witness"]
    )  # type: ignore[arg-type]


def _start(value: object, universe: Mapping[str, TaskRef]) -> TaskAttemptStarted:
    if type(value) is TaskAttemptStarted:
        task = _task_ref(value.task_ref, universe)
        return (
            value
            if task is value.task_ref
            else TaskAttemptStarted(
                task,
                value.ordinal,
                value.parent_closed_attempt_ref,
                value.selection_declaration_refs,
                value.dispatch_ref,
            )
        )
    if not isinstance(value, Mapping) or set(value) != {
        "task_ref",
        "ordinal",
        "parent_closed_attempt_ref",
        "selection_declaration_refs",
        "dispatch_ref",
    }:
        _fail("ATTEMPT_CHAIN_INVALID")
    return TaskAttemptStarted(
        _task_ref(value["task_ref"], universe),
        value["ordinal"],  # type: ignore[arg-type]
        value["parent_closed_attempt_ref"],
        tuple(value["selection_declaration_refs"]),  # type: ignore[arg-type]
        value["dispatch_ref"],
    )


def _close(value: object) -> TaskAttemptClosed:
    if type(value) is TaskAttemptClosed:
        return value
    if not isinstance(value, Mapping) or set(value) != {
        "started_attempt_ref",
        "disposition",
        "task_outcome_ref",
        "failure_code",
    }:
        _fail("ATTEMPT_CHAIN_INVALID")
    return TaskAttemptClosed(
        value["started_attempt_ref"],
        value["disposition"],  # type: ignore[arg-type]
        value["task_outcome_ref"],
        value["failure_code"],  # type: ignore[arg-type]
    )


def _selection_declaration(value: object) -> SelectionDeclaration:
    if type(value) is SelectionDeclaration:
        return value
    if not isinstance(value, Mapping) or set(value) != {
        "experiment_ref",
        "selection_policy_ref",
        "universe_kind",
        "declared_by_ref",
    }:
        _fail("SELECTION_POLICY_MISMATCH")
    return SelectionDeclaration(
        value["experiment_ref"],
        value["selection_policy_ref"],
        value["universe_kind"],  # type: ignore[arg-type]
        value["declared_by_ref"],
    )


def _manifest_payload(value: object) -> tuple[object, tuple[object, ...]]:
    if isinstance(value, ExperimentExecutionManifest):
        return value.experiment_ref, value.task_outcome_refs
    if not isinstance(value, Mapping) or set(value) != {
        "experiment_ref",
        "task_outcome_refs",
    }:
        _fail("MANIFEST_CUTOFF_INVALID")
    try:
        experiment_ref = _canonical_ref(
            value["experiment_ref"],
            "experiment_ref",
            expected_artifact_type="experiment_spec",
        )
        refs = tuple(
            _canonical_ref(
                ref,
                "task_outcome_ref",
                expected_type="artifact_ref",
                expected_artifact_type="task_outcome",
            )
            for ref in value["task_outcome_refs"]
        )  # type: ignore[union-attr]
    except (TypeError, ValueError) as error:
        _fail("MANIFEST_CUTOFF_INVALID", str(error))
    return experiment_ref, refs


def _payload_kind(value: object) -> str | None:
    for value_type, kind in (
        (TaskOutcome, "outcome"),
        (TaskAttemptStarted, "start"),
        (TaskAttemptClosed, "close"),
        (ExperimentExecutionManifest, "manifest"),
        (SelectionDeclaration, "selection"),
    ):
        if isinstance(value, value_type):
            return kind
    if not isinstance(value, Mapping):
        return None
    keys = set(value)
    if keys == {"task_ref", "state", "witness"}:
        return "outcome"
    if keys == {
        "task_ref",
        "ordinal",
        "parent_closed_attempt_ref",
        "selection_declaration_refs",
        "dispatch_ref",
    }:
        return "start"
    if keys == {
        "started_attempt_ref",
        "disposition",
        "task_outcome_ref",
        "failure_code",
    }:
        return "close"
    if keys == {"experiment_ref", "task_outcome_refs"}:
        return "manifest"
    if keys == {
        "experiment_ref",
        "selection_policy_ref",
        "universe_kind",
        "declared_by_ref",
    }:
        return "selection"
    return None


def _belongs_to_experiment(task_ref: TaskRef, experiment_ref: object) -> bool | None:
    if task_ref.experiment_ref is None:
        return None
    return _same_wire(task_ref.experiment_ref, experiment_ref)


def _validate_outcome_links(
    universe: tuple[TaskRef, ...],
    outcomes: Mapping[TaskRef, TaskOutcome],
) -> None:
    feature: tuple[FeatureBuildTask, TaskOutcome] | None = None
    training: tuple[ModelTrainingTask, TaskOutcome] | None = None
    trials: dict[str, tuple[TaskRef, TaskOutcome]] = {}
    for task in universe:
        artifact = task.artifact
        if task.kind == "FEATURE_BUILD":
            if feature is not None or type(artifact) is not FeatureBuildTask:
                _fail("TASK_OUTCOME_INVALID")
            feature = (artifact, outcomes[task])
        elif task.kind == "MODEL_TRAINING":
            if training is not None or type(artifact) is not ModelTrainingTask:
                _fail("TASK_OUTCOME_INVALID")
            training = (artifact, outcomes[task])
        elif task.kind == "TRIAL":
            if type(artifact) is not TrialDeclaration:
                _fail("TASK_OUTCOME_INVALID")
            trials[_canonical_json(artifact.ref)] = (task, outcomes[task])

    if (feature is None) != (training is None):
        _fail("TASK_OUTCOME_INVALID")
    training_outcome: TaskOutcome | None = None
    if feature is not None and training is not None:
        feature_task, feature_outcome = feature
        training_task, training_outcome = training
        if not _same_wire(training_task.feature_build_task_ref, feature_task.ref):
            _fail("TASK_OUTCOME_INVALID")
        if feature_outcome.state != "COMPLETED":
            if not (
                training_outcome.state == "BLOCKED"
                and type(training_outcome.witness) is UpstreamTaskOutcome
                and _same_wire(
                    training_outcome.witness.task_outcome_ref, feature_outcome.ref
                )
            ):
                _fail("TASK_OUTCOME_INVALID")
        elif training_outcome.state == "BLOCKED":
            _fail("TASK_OUTCOME_INVALID")

    for _, trial_outcome in trials.values():
        if (
            training_outcome is not None
            and training_outcome.state != "COMPLETED"
            and not (
                trial_outcome.state == "BLOCKED"
                and type(trial_outcome.witness) is UpstreamTaskOutcome
                and _same_wire(
                    trial_outcome.witness.task_outcome_ref, training_outcome.ref
                )
            )
        ):
            _fail("TASK_OUTCOME_INVALID")

    for task in universe:
        if task.kind != "ANALYSIS":
            continue
        outcome = outcomes[task]
        analysis = task.artifact
        if type(analysis) is not AnalysisTask:
            _fail("TASK_OUTCOME_INVALID")
        upstream = trials.get(_canonical_json(analysis.trial_declaration_ref))
        if upstream is None:
            _fail("TASK_OUTCOME_INVALID")
        _, trial_outcome = upstream
        if trial_outcome.state == "COMPLETED":
            if outcome.state == "COMPLETED":
                trial_witness = trial_outcome.witness
                analysis_witness = outcome.witness
                if (
                    type(trial_witness) is not TrialCompletedPublication
                    or type(analysis_witness) is not AnalysisDerivation
                    or not _same_wire(
                        analysis_witness.source_publication_ref,
                        trial_witness.publication_ref,
                    )
                ):
                    _fail("TASK_OUTCOME_INVALID")
            elif not (
                outcome.state == "FAILED" and type(outcome.witness) is LocalFailure
            ):
                _fail("TASK_OUTCOME_INVALID")
        elif not (
            outcome.state == "BLOCKED"
            and type(outcome.witness) is UpstreamTaskOutcome
            and _same_wire(outcome.witness.task_outcome_ref, trial_outcome.ref)
        ):
            _fail("TASK_OUTCOME_INVALID")


def _outcome_sequence(
    experiment_spec: ExperimentSpec,
    outcomes: Mapping[TaskRef, TaskOutcome] | Iterable[TaskOutcome],
) -> tuple[tuple[TaskRef, ...], tuple[TaskOutcome, ...]]:
    universe = build_task_universe(experiment_spec)
    by_wire = {task.canonical_wire: task for task in universe}
    supplied: Iterable[object]
    if isinstance(outcomes, Mapping):
        supplied = outcomes.values()
    elif type(outcomes) in {tuple, list}:
        supplied = outcomes
    else:
        _fail("TASK_OUTCOME_INVALID")

    normalized: dict[TaskRef, TaskOutcome] = {}
    for value in supplied:
        outcome = _outcome(value, by_wire)
        if outcome.task_ref.canonical_wire not in by_wire:
            _fail("TASK_REF_FOREIGN")
        task = by_wire[outcome.task_ref.canonical_wire]
        if task in normalized:
            _fail("TASK_OUTCOME_MISSING_OR_DUPLICATE")
        normalized[task] = (
            outcome
            if outcome.task_ref is task
            else TaskOutcome(task, outcome.state, outcome.witness)
        )
    if set(normalized) != set(universe):
        _fail("TASK_OUTCOME_MISSING_OR_DUPLICATE")
    _validate_outcome_links(universe, normalized)
    return universe, tuple(normalized[task] for task in universe)


def build_execution_manifest(
    experiment_spec: ExperimentSpec,
    outcomes: Mapping[TaskRef, TaskOutcome] | Iterable[TaskOutcome],
    cutoff_ref: object,
) -> ExperimentExecutionManifest:
    _, ordered = _outcome_sequence(experiment_spec, outcomes)
    cutoff = _cutoff_ref(cutoff_ref)
    declarations: tuple[SelectionDeclaration, ...] = ()
    refs_complete = False
    if type(outcomes) is ExecutionProjection:
        if not _same_wire(outcomes.cutoff_ref, cutoff):
            _fail("MANIFEST_CUTOFF_INVALID")
        declarations = outcomes.selection_declarations
        refs_complete = outcomes.selection_refs_complete
    return _VerifiedExecutionManifest(
        experiment_spec.ref,
        tuple(outcome.ref for outcome in ordered),
        ordered,
        declarations,
        refs_complete,
        cutoff,
    )


def validate_execution_prefix(
    experiment_spec: ExperimentSpec,
    entries: tuple[object, ...] | list[object],
    manifest_cutoff: object,
) -> ExecutionProjection:
    if type(experiment_spec) is not ExperimentSpec:
        _fail("EXPERIMENT_SPEC_INVALID")
    if type(entries) not in {tuple, list}:
        _fail("MANIFEST_CUTOFF_INVALID")
    cutoff = _cutoff_ref(manifest_cutoff)
    cutoff_sequence = _plain_json(cutoff)["log_sequence"]  # type: ignore[index]
    parsed = tuple(
        sorted((_entry(entry) for entry in entries), key=lambda item: item.sequence)
    )

    universe = build_task_universe(experiment_spec)
    by_wire = {task.canonical_wire: task for task in universe}
    outcomes: dict[TaskRef, TaskOutcome] = {}
    starts_by_ref: dict[str, tuple[TaskAttemptStarted, int]] = {}
    open_by_task: dict[TaskRef, TaskAttemptStarted] = {}
    last_close_by_task: dict[TaskRef, TaskAttemptClosed] = {}
    terminal_close_by_task: dict[TaskRef, TaskAttemptClosed] = {}
    target_selection_declarations: list[tuple[int, SelectionDeclaration]] = []
    selection_ref_sets: list[set[str]] = []
    cutoff_manifest: tuple[object, tuple[object, ...]] | None = None

    for entry in parsed:
        if entry.sequence > cutoff_sequence:
            continue
        kind = _payload_kind(entry.payload)
        if kind == "selection":
            declaration = _selection_declaration(entry.payload)
            if _same_wire(declaration.experiment_ref, experiment_spec.ref):
                target_selection_declarations.append((entry.sequence, declaration))
            continue
        if kind == "manifest":
            manifest = _manifest_payload(entry.payload)
            if entry.sequence == cutoff_sequence:
                cutoff_manifest = manifest
            continue
        if kind == "outcome":
            outcome = _outcome(entry.payload, by_wire)
            canonical = by_wire.get(outcome.task_ref.canonical_wire)
            if canonical is None:
                belongs = _belongs_to_experiment(
                    outcome.task_ref, experiment_spec.ref
                )
                if belongs is None or belongs:
                    _fail("TASK_REF_FOREIGN")
                continue
            outcome = (
                outcome
                if outcome.task_ref is canonical
                else TaskOutcome(canonical, outcome.state, outcome.witness)
            )
            if canonical in outcomes:
                _fail("TASK_OUTCOME_MISSING_OR_DUPLICATE")
            outcomes[canonical] = outcome
            continue
        if kind == "start":
            started = _start(entry.payload, by_wire)
            canonical = by_wire.get(started.task_ref.canonical_wire)
            if canonical is None:
                belongs = _belongs_to_experiment(
                    started.task_ref, experiment_spec.ref
                )
                if belongs is None or belongs:
                    _fail("TASK_REF_FOREIGN")
                continue
            if started.task_ref is not canonical:
                started = TaskAttemptStarted(
                    canonical,
                    started.ordinal,
                    started.parent_closed_attempt_ref,
                    started.selection_declaration_refs,
                    started.dispatch_ref,
                )
            if canonical in open_by_task or canonical in terminal_close_by_task:
                _fail("ATTEMPT_CHAIN_INVALID")
            previous = last_close_by_task.get(canonical)
            expected_ordinal = (
                1
                if previous is None
                else starts_by_ref[_canonical_json(previous.started_attempt_ref)][
                    0
                ].ordinal
                + 1
            )
            if started.ordinal != expected_ordinal:
                _fail("ATTEMPT_CHAIN_INVALID")
            if previous is None:
                if started.parent_closed_attempt_ref is not None:
                    _fail("ATTEMPT_CHAIN_INVALID")
            elif started.parent_closed_attempt_ref is None or not _same_wire(
                started.parent_closed_attempt_ref, previous.ref
            ):
                _fail("ATTEMPT_CHAIN_INVALID")
            wire = _canonical_json(started.ref)
            if wire in starts_by_ref:
                _fail("ATTEMPT_CHAIN_INVALID")
            starts_by_ref[wire] = (started, entry.sequence)
            open_by_task[canonical] = started
            selection_ref_sets.append(
                {_canonical_json(ref) for ref in started.selection_declaration_refs}
            )
            continue
        if kind == "close":
            closed = _close(entry.payload)
            started_record = starts_by_ref.get(
                _canonical_json(closed.started_attempt_ref)
            )
            if started_record is None:
                continue
            started, _ = started_record
            current = open_by_task.get(started.task_ref)
            if current is None or not _same_wire(
                current.ref, closed.started_attempt_ref
            ):
                _fail("ATTEMPT_CHAIN_INVALID")
            del open_by_task[started.task_ref]
            last_close_by_task[started.task_ref] = closed
            if closed.disposition == "TERMINAL":
                if started.task_ref in terminal_close_by_task:
                    _fail("ATTEMPT_CHAIN_INVALID")
                terminal_close_by_task[started.task_ref] = closed

    if open_by_task:
        _fail("ATTEMPT_CHAIN_INVALID")
    for task, closed in terminal_close_by_task.items():
        outcome = outcomes.get(task)
        if outcome is not None and not _same_wire(closed.task_outcome_ref, outcome.ref):
            _fail("ATTEMPT_CHAIN_INVALID")

    if set(outcomes) == set(universe):
        _validate_outcome_links(universe, outcomes)
    if set(outcomes) != set(universe) or set(terminal_close_by_task) != set(universe):
        _fail("TASK_OUTCOME_MISSING_OR_DUPLICATE")
    for task in universe:
        if not _same_wire(
            terminal_close_by_task[task].task_outcome_ref, outcomes[task].ref
        ):
            _fail("TASK_OUTCOME_MISSING_OR_DUPLICATE")

    expected_refs = tuple(outcomes[task].ref for task in universe)
    if cutoff_manifest is None:
        _fail("MANIFEST_CUTOFF_INVALID")
    manifest_experiment, manifest_refs = cutoff_manifest
    if not _same_wire(manifest_experiment, experiment_spec.ref):
        _fail("MANIFEST_CUTOFF_INVALID")
    if tuple(_canonical_json(ref) for ref in manifest_refs) != tuple(
        _canonical_json(ref) for ref in expected_refs
    ):
        _fail("TASK_OUTCOME_MISSING_OR_DUPLICATE")

    for entry in parsed:
        if entry.sequence <= cutoff_sequence:
            continue
        kind = _payload_kind(entry.payload)
        if kind in {"outcome", "start"}:
            task = (
                _outcome(entry.payload, by_wire).task_ref
                if kind == "outcome"
                else _start(entry.payload, by_wire).task_ref
            )
            canonical = by_wire.get(task.canonical_wire)
            belongs = _belongs_to_experiment(task, experiment_spec.ref)
            if canonical is not None or belongs is None or belongs:
                _fail("EXPERIMENT_REOPENED_AFTER_CLOSE")
        elif (
            kind == "close"
            and _canonical_json(_close(entry.payload).started_attempt_ref)
            in starts_by_ref
        ):
            _fail("EXPERIMENT_REOPENED_AFTER_CLOSE")
        elif kind == "manifest":
            later_experiment, _ = _manifest_payload(entry.payload)
            if _same_wire(later_experiment, experiment_spec.ref):
                _fail("EXPERIMENT_REOPENED_AFTER_CLOSE")

    first_start_sequence = min(
        (sequence for _, sequence in starts_by_ref.values()), default=cutoff_sequence
    )
    declarations_by_ref = {
        _canonical_json(declaration.ref): declaration
        for sequence, declaration in target_selection_declarations
        if sequence < first_start_sequence
    }
    common_refs = set.intersection(*selection_ref_sets) if selection_ref_sets else set()
    refs_complete = bool(selection_ref_sets) and all(
        refs == common_refs and refs for refs in selection_ref_sets
    )
    selected_declarations = tuple(
        declarations_by_ref[wire]
        for wire in sorted(common_refs)
        if wire in declarations_by_ref
    )
    refs_complete = refs_complete and len(selected_declarations) == len(common_refs)

    return ExecutionProjection(
        ((task, outcomes[task]) for task in universe),
        selection_declarations=selected_declarations,
        selection_refs_complete=refs_complete,
        cutoff_ref=cutoff,
    )


def build_candidate_family(
    experiment_ref: object,
    manifest_ref: ExperimentExecutionManifest | object,
) -> CandidateFamily:
    try:
        experiment = _canonical_ref(
            experiment_ref, "experiment_ref", expected_artifact_type="experiment_spec"
        )
    except ValueError as error:
        _fail("TASK_OUTCOME_INVALID", str(error))
    if not isinstance(manifest_ref, ExperimentExecutionManifest):
        _fail("TASK_OUTCOME_INVALID", "a verified manifest is required")
    if not _same_wire(manifest_ref.experiment_ref, experiment):
        _fail("TASK_OUTCOME_INVALID", "manifest belongs to another Experiment")
    return CandidateFamily(experiment, manifest_ref.ref)


def _matches_filter(analysis: VerifiedAnalysis, hard_filter: HardFilter) -> bool:
    left: Decimal | int
    right: Decimal | int
    if hard_filter.field_name == "simple_period_return":
        left = Decimal(analysis.simple_period_return)
        right = Decimal(hard_filter.threshold)  # type: ignore[arg-type]
    else:
        left = analysis.trade_count
        right = hard_filter.threshold  # type: ignore[assignment]
    return {
        "gt": left > right,
        "gte": left >= right,
        "lt": left < right,
        "lte": left <= right,
        "eq": left == right,
    }[hard_filter.operator]


def select_candidate(
    family: CandidateFamily,
    manifest: ExperimentExecutionManifest,
    policy: SelectionPolicy,
    verified_analyses: tuple[VerifiedAnalysis, ...] | list[VerifiedAnalysis],
) -> Selected | NoSelection:
    if (
        type(family) is not CandidateFamily
        or not isinstance(manifest, ExperimentExecutionManifest)
        or type(policy) is not SelectionPolicy
    ):
        _fail("SELECTION_POLICY_MISMATCH")
    declarations = getattr(manifest, "_selection_declarations", ())
    if not getattr(manifest, "_selection_refs_complete", False) or not declarations:
        _fail("SELECTION_PRECOMMIT_MISSING")
    if not _same_wire(family.experiment_ref, manifest.experiment_ref) or not _same_wire(
        family.execution_manifest_ref, manifest.ref
    ):
        _fail("SELECTION_POLICY_MISMATCH")
    if not any(
        _same_wire(declaration.experiment_ref, family.experiment_ref)
        and _same_wire(declaration.selection_policy_ref, policy.ref)
        for declaration in declarations
    ):
        _fail("SELECTION_POLICY_MISMATCH")
    if type(verified_analyses) not in {tuple, list} or any(
        type(item) is not VerifiedAnalysis for item in verified_analyses
    ):
        _fail("SELECTION_INPUT_INCOMPLETE")

    outcome_map = manifest.outcome_map
    trials: dict[str, tuple[TaskRef, TaskOutcome]] = {}
    analyses: dict[tuple[str, str], tuple[TaskRef, TaskOutcome]] = {}
    completed_analysis_refs: dict[str, tuple[AnalysisTask, AnalysisDerivation]] = {}
    for task, outcome in outcome_map.items():
        if task.kind == "TRIAL":
            trial = task.artifact
            if type(trial) is not TrialDeclaration:
                _fail("SELECTION_INPUT_INCOMPLETE")
            trials[_canonical_json(trial.ref)] = (task, outcome)
        elif task.kind == "ANALYSIS":
            analysis_task = task.artifact
            if type(analysis_task) is not AnalysisTask:
                _fail("SELECTION_INPUT_INCOMPLETE")
            analyses[
                (
                    _canonical_json(analysis_task.trial_declaration_ref),
                    _canonical_json(analysis_task.metric_profile_ref),
                )
            ] = (task, outcome)
            if (
                outcome.state == "COMPLETED"
                and type(outcome.witness) is AnalysisDerivation
            ):
                completed_analysis_refs[
                    _canonical_json(outcome.witness.analysis_ref)
                ] = (analysis_task, outcome.witness)

    verified_by_ref: dict[str, VerifiedAnalysis] = {}
    for analysis in verified_analyses:
        wire = _canonical_json(analysis.analysis_ref)
        if wire in verified_by_ref or wire not in completed_analysis_refs:
            _fail("SELECTION_INPUT_INCOMPLETE")
        task, witness = completed_analysis_refs[wire]
        if not _same_wire(
            analysis.metric_profile_ref, task.metric_profile_ref
        ) or not _same_wire(
            analysis.trial_publication_ref, witness.source_publication_ref
        ):
            _fail("SELECTION_INPUT_INCOMPLETE")
        verified_by_ref[wire] = analysis

    ranked: list[
        tuple[tuple[Decimal | int | str, ...], TrialDeclaration, VerifiedAnalysis]
    ] = []
    profile_wire = _canonical_json(policy.metric_profile_ref)
    for trial_wire, (_, trial_outcome) in trials.items():
        if trial_outcome.state != "COMPLETED":
            continue
        if type(trial_outcome.witness) is not TrialCompletedPublication:
            _fail("SELECTION_INPUT_INCOMPLETE")
        analysis_pair = analyses.get((trial_wire, profile_wire))
        if analysis_pair is None:
            _fail("SELECTION_INPUT_INCOMPLETE")
        _, analysis_outcome = analysis_pair
        if (
            analysis_outcome.state != "COMPLETED"
            or type(analysis_outcome.witness) is not AnalysisDerivation
        ):
            _fail("SELECTION_INPUT_INCOMPLETE")
        verified = verified_by_ref.get(
            _canonical_json(analysis_outcome.witness.analysis_ref)
        )
        if (
            verified is None
            or not _same_wire(
                verified.trial_publication_ref, trial_outcome.witness.publication_ref
            )
            or not _same_wire(verified.metric_profile_ref, policy.metric_profile_ref)
        ):
            _fail("SELECTION_INPUT_INCOMPLETE")
        if verified.result_grade not in policy.accepted_backtest_grades:
            continue
        if not all(_matches_filter(verified, item) for item in policy.hard_filters):
            continue
        trial = trial_outcome.task_ref.artifact
        if type(trial) is not TrialDeclaration:
            _fail("SELECTION_INPUT_INCOMPLETE")
        rank_key: list[Decimal | int | str] = []
        for criterion in policy.ordering:
            value: Decimal | int = (
                Decimal(verified.simple_period_return)
                if criterion.field_name == "simple_period_return"
                else verified.trade_count
            )
            rank_key.append(-value if criterion.direction == "descending" else value)
        rank_key.append(_canonical_json(trial.ref))
        ranked.append((tuple(rank_key), trial, verified))

    if not ranked:
        return NoSelection()
    ranked.sort(key=lambda item: item[0])
    _, trial, verified = ranked[0]
    return Selected(trial.ref, verified.analysis_ref, verified.trial_publication_ref, 1)


__all__ = [
    "FAILURE_PRECEDENCE",
    "RESEARCH_EXECUTION_LOG",
    "AnalysisDerivation",
    "AnalysisTask",
    "BacktestTerminal",
    "CandidateFamily",
    "DataSlice",
    "DependencyBlock",
    "ExecutionEntry",
    "ExecutionProjection",
    "ExperimentExecutionManifest",
    "ExperimentSpec",
    "FeatureBuildTask",
    "FeatureDatasetManifest",
    "FeatureDatasetPublication",
    "FeatureRecipe",
    "HardFilter",
    "LocalFailure",
    "ModelBuildEvidence",
    "ModelBuildPlan",
    "ModelBuildPublication",
    "ModelTrainingTask",
    "NoSelection",
    "OrderingCriterion",
    "ParameterCombination",
    "ResearchCoreError",
    "Selected",
    "SelectionDeclaration",
    "SelectionPolicy",
    "TaskAttemptClosed",
    "TaskAttemptStarted",
    "TaskOutcome",
    "TaskRef",
    "TrainerRecipe",
    "TrialCompletedPublication",
    "TrialDeclaration",
    "UpstreamTaskOutcome",
    "VerifiedAnalysis",
    "block_analysis_from_upstream",
    "build_analysis_tasks",
    "build_candidate_family",
    "build_execution_manifest",
    "build_task_universe",
    "build_trial_declarations",
    "map_backtest_observation",
    "select_candidate",
    "validate_execution_prefix",
    "validate_model_build",
]
