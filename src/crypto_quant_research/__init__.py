from importlib import import_module
from typing import TYPE_CHECKING

from .declarations import (
    CampaignScope,
    TrialDeclaration,
    compile_trial_declarations,
)
from .declarations import (
    ParameterCombination as CampaignParameterCombination,
)
from .declarations import (
    SelectionPolicy as CampaignSelectionPolicy,
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
    TargetBuildTask,
    TargetMaterializationEvidence,
    TargetRecipe,
    TrainerRecipe,
    validate_model_build,
)
from .integration import (
    ParameterCombination as ExperimentParameterCombination,
)
from .integration import (
    SelectionPolicy as ExperimentSelectionPolicy,
)
from .retrospective_study import (
    PublishedRetrospectiveArtifact,
    RetrospectiveReadTask,
    RetrospectiveSourceSlice,
    RetrospectiveStudyDeclaration,
    RetrospectiveStudyProtocol,
    publish_retrospective_protocol,
    publish_retrospective_read_task,
    publish_retrospective_study,
)
from .statistical_study import (
    PublishedStatisticalReadTask,
    PublishedStatisticalStudy,
    StatisticalReadTask,
    StatisticalStudyDeclaration,
    publish_statistical_read_task,
    publish_statistical_study,
)

if TYPE_CHECKING:
    from .provenance import verify_candidate_provenance
    from .runtime import (
        FrozenExperimentInputs,
        FrozenModelExperimentInputs,
        FrozenTargetExperimentInputs,
        PublishedNoSelection,
        PublishedStrategyCandidate,
        PublishedTargetExperimentRegistration,
        TrialExecution,
        execute_experiment,
        execute_model_experiment,
        execute_target_experiment,
        preregister_target_experiment,
    )

ParameterCombination = CampaignParameterCombination
SelectionPolicy = CampaignSelectionPolicy

_RUNTIME_EXPORTS = frozenset(
    {
        "FrozenExperimentInputs",
        "FrozenModelExperimentInputs",
        "FrozenTargetExperimentInputs",
        "PublishedNoSelection",
        "PublishedTargetExperimentRegistration",
        "PublishedStrategyCandidate",
        "TrialExecution",
        "execute_experiment",
        "execute_model_experiment",
        "execute_target_experiment",
        "preregister_target_experiment",
    }
)


def __getattr__(name: str):
    if name == "verify_candidate_provenance":
        return getattr(import_module(".provenance", __name__), name)
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
    "FrozenModelExperimentInputs",
    "FrozenTargetExperimentInputs",
    "HardFilter",
    "ModelBuildEvidence",
    "ModelBuildPlan",
    "ModelTrainingTask",
    "OrderingCriterion",
    "ParameterCombination",
    "PublishedNoSelection",
    "PublishedRetrospectiveArtifact",
    "PublishedStatisticalReadTask",
    "PublishedStatisticalStudy",
    "PublishedStrategyCandidate",
    "PublishedTargetExperimentRegistration",
    "RetrospectiveReadTask",
    "RetrospectiveSourceSlice",
    "RetrospectiveStudyDeclaration",
    "RetrospectiveStudyProtocol",
    "SelectionPolicy",
    "StatisticalReadTask",
    "StatisticalStudyDeclaration",
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
    "preregister_target_experiment",
    "publish_retrospective_protocol",
    "publish_retrospective_read_task",
    "publish_retrospective_study",
    "publish_statistical_read_task",
    "publish_statistical_study",
    "validate_model_build",
    "verify_candidate_provenance",
]
