from importlib import import_module

from .declarations import (
    CampaignScope,
    ParameterCombination as CampaignParameterCombination,
    SelectionPolicy as CampaignSelectionPolicy,
    TrialDeclaration,
    compile_trial_declarations,
)
from .integration import (
    DataSlice,
    ExperimentSpec,
    FeatureBuildTask,
    FeatureDatasetManifest,
    FeatureRecipe,
    HardFilter,
    ModelBuildEvidence,
    ModelBuildPlan,
    ModelTrainingTask,
    OrderingCriterion,
    ParameterCombination as ExperimentParameterCombination,
    SelectionPolicy as ExperimentSelectionPolicy,
    TargetBuildTask,
    TargetMaterializationEvidence,
    TargetRecipe,
    TrainerRecipe,
    validate_model_build,
)

ParameterCombination = CampaignParameterCombination
SelectionPolicy = CampaignSelectionPolicy

_RUNTIME_EXPORTS = frozenset(
    {
        "FrozenExperimentInputs",
        "FrozenModelExperimentInputs",
        "FrozenTargetExperimentInputs",
        "PublishedNoSelection",
        "PublishedStrategyCandidate",
        "TrialExecution",
        "execute_experiment",
        "execute_model_experiment",
        "execute_target_experiment",
    }
)


def __getattr__(name: str):
    if name in _RUNTIME_EXPORTS:
        return getattr(import_module(".runtime", __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CampaignScope",
    "DataSlice",
    "ExperimentParameterCombination",
    "ExperimentSelectionPolicy",
    "ExperimentSpec",
    "FeatureBuildTask",
    "FeatureDatasetManifest",
    "FeatureRecipe",
    "FrozenExperimentInputs",
    "HardFilter",
    "FrozenModelExperimentInputs",
    "FrozenTargetExperimentInputs",
    "ModelBuildEvidence",
    "ModelBuildPlan",
    "ModelTrainingTask",
    "OrderingCriterion",
    "ParameterCombination",
    "PublishedNoSelection",
    "PublishedStrategyCandidate",
    "SelectionPolicy",
    "TargetBuildTask",
    "TargetMaterializationEvidence",
    "TargetRecipe",
    "TrainerRecipe",
    "TrialDeclaration",
    "TrialExecution",
    "compile_trial_declarations",
    "execute_experiment",
    "execute_model_experiment",
    "execute_target_experiment",
    "validate_model_build",
]
