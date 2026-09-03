from importlib import import_module

from .declarations import (
    CampaignScope,
    ParameterCombination,
    SelectionPolicy,
    TrialDeclaration,
    compile_trial_declarations,
)
from .raw_blob_snapshots import (
    RAW_BLOB_SNAPSHOTS_LOG,
    RawBlobSnapshotFoundation,
    RawBlobSnapshotPublication,
    RawBlobSnapshotPublicationFact,
    open_verified_raw_blob_snapshot,
    publish_raw_blob_snapshot,
)
from .integration import (
    DataSlice,
    DataSlice as IntegratedDataSlice,
    ExperimentSpec as IntegratedExperimentSpec,
    FeatureBuildTask,
    FeatureDatasetManifest,
    FeatureRecipe,
    HardFilter as IntegratedHardFilter,
    ModelBuildEvidence,
    ModelBuildPlan,
    ModelTrainingTask,
    OrderingCriterion as IntegratedOrderingCriterion,
    ParameterCombination as IntegratedParameterCombination,
    SelectionPolicy as IntegratedSelectionPolicy,
    TrainerRecipe,
    TrialDeclaration as IntegratedTrialDeclaration,
    build_trial_declarations as build_integrated_trial_declarations,
    validate_model_build,
)

_RUNTIME_EXPORTS = frozenset(
    {
        "DeferredTrialExecution",
        "FrozenExperimentInputs",
        "FrozenModelExperimentInputs",
        "PublishedNoSelection",
        "PublishedStrategyCandidate",
        "TrialExecution",
        "execute_experiment",
        "execute_model_experiment",
    }
)


def __getattr__(name: str):
    if name in _RUNTIME_EXPORTS:
        return getattr(import_module(".runtime", __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CampaignScope",
    "RAW_BLOB_SNAPSHOTS_LOG",
    "RawBlobSnapshotFoundation",
    "RawBlobSnapshotPublication",
    "RawBlobSnapshotPublicationFact",
    "DataSlice",
    "DeferredTrialExecution",  # pyright: ignore[reportUnsupportedDunderAll]
    "FeatureBuildTask",
    "FeatureDatasetManifest",
    "FeatureRecipe",
    "FrozenExperimentInputs",
    "FrozenModelExperimentInputs",
    "IntegratedDataSlice",
    "IntegratedExperimentSpec",
    "IntegratedHardFilter",
    "IntegratedOrderingCriterion",
    "IntegratedParameterCombination",
    "IntegratedSelectionPolicy",
    "IntegratedTrialDeclaration",
    "ModelBuildEvidence",
    "ModelBuildPlan",
    "ModelTrainingTask",
    "ParameterCombination",
    "PublishedNoSelection",
    "PublishedStrategyCandidate",
    "SelectionPolicy",
    "TrainerRecipe",
    "TrialDeclaration",
    "TrialExecution",
    "build_integrated_trial_declarations",
    "compile_trial_declarations",
    "execute_experiment",
    "execute_model_experiment",
    "open_verified_raw_blob_snapshot",
    "publish_raw_blob_snapshot",
    "validate_model_build",
]
