from importlib import import_module

from .declarations import (
    CampaignScope,
    ParameterCombination,
    SelectionPolicy,
    TrialDeclaration,
    compile_trial_declarations,
)
from .integration import (
    DataSlice,
    FeatureBuildTask,
    FeatureDatasetManifest,
    FeatureRecipe,
    ModelBuildEvidence,
    ModelBuildPlan,
    ModelTrainingTask,
    TrainerRecipe,
    validate_model_build,
)

_RUNTIME_EXPORTS = frozenset(
    {
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
    "DataSlice",
    "FeatureBuildTask",
    "FeatureDatasetManifest",
    "FeatureRecipe",
    "FrozenExperimentInputs",
    "FrozenModelExperimentInputs",
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
    "compile_trial_declarations",
    "execute_experiment",
    "execute_model_experiment",
    "validate_model_build",
]
