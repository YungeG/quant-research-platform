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
from .integration import (
    DataSlice as IntegratedDataSlice,
)
from .integration import (
    ExperimentSpec as IntegratedExperimentSpec,
)
from .integration import (
    HardFilter as IntegratedHardFilter,
)
from .integration import (
    OrderingCriterion as IntegratedOrderingCriterion,
)
from .integration import (
    ParameterCombination as IntegratedParameterCombination,
)
from .integration import (
    SelectionPolicy as IntegratedSelectionPolicy,
)
from .integration import (
    TrialDeclaration as IntegratedTrialDeclaration,
)
from .integration import (
    build_trial_declarations as build_integrated_trial_declarations,
)
from .koru_boundary_indexes import (
    BOUNDARY_INDEXES_LOG,
    BoundaryIndexPublication,
    BoundaryIndexPublicationFact,
    open_published_koru_aggregate_trade_boundary_index_authority_v3,
    publish_koru_aggregate_trade_boundary_index_authority_v3,
)
from .koru_premium_preflight_authority import (
    KORU_PREMIUM_DISCOVERY_SCOPE_V1,
    KORU_PREMIUM_PREFLIGHT_FAILURE_PRECEDENCE_V1,
    KoruPremiumPreflightAuthorityErrorV1,
    KoruPremiumPreflightAuthorityFailureCodeV1,
    KoruPremiumPreflightAuthorityFoundationV1,
    KoruPremiumPreflightAuthorityV1,
    KoruPremiumPreflightStageKindV1,
    admit_koru_aggregate_trade_boundary_index_publication_fact_v1,
    admit_raw_blob_snapshot_publication_fact_v1,
    construct_koru_premium_preflight_authority_v1,
    create_koru_premium_preflight_stage_publication_fact_v1,
    open_koru_premium_preflight_authority_v1,
    verify_koru_premium_preflight_authority_v1,
)
from .koru_source_projections import (
    SOURCE_PROJECTIONS_LOG_V3,
    KoruTradifiSourceProjectionScopeV3,
    SourceProjectionPublicationFactV3,
    SourceProjectionPublicationV3,
    open_published_koru_tradifi_source_projection_authority_v3,
    publish_koru_tradifi_source_projection_authority_v3,
)
from .raw_blob_snapshots import (
    RAW_BLOB_SNAPSHOTS_LOG,
    RawBlobSnapshotFoundation,
    RawBlobSnapshotPublication,
    RawBlobSnapshotPublicationFact,
    open_verified_raw_blob_snapshot,
    publish_raw_blob_snapshot,
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
    "BOUNDARY_INDEXES_LOG",
    "KORU_PREMIUM_DISCOVERY_SCOPE_V1",
    "KORU_PREMIUM_PREFLIGHT_FAILURE_PRECEDENCE_V1",
    "RAW_BLOB_SNAPSHOTS_LOG",
    "SOURCE_PROJECTIONS_LOG_V3",
    "BoundaryIndexPublication",
    "BoundaryIndexPublicationFact",
    "CampaignScope",
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
    "KoruPremiumPreflightAuthorityErrorV1",
    "KoruPremiumPreflightAuthorityFailureCodeV1",
    "KoruPremiumPreflightAuthorityFoundationV1",
    "KoruPremiumPreflightAuthorityV1",
    "KoruPremiumPreflightStageKindV1",
    "KoruTradifiSourceProjectionScopeV3",
    "ModelBuildEvidence",
    "ModelBuildPlan",
    "ModelTrainingTask",
    "ParameterCombination",
    "PublishedNoSelection",
    "PublishedStrategyCandidate",
    "RawBlobSnapshotFoundation",
    "RawBlobSnapshotPublication",
    "RawBlobSnapshotPublicationFact",
    "SelectionPolicy",
    "SourceProjectionPublicationFactV3",
    "SourceProjectionPublicationV3",
    "TrainerRecipe",
    "TrialDeclaration",
    "TrialExecution",
    "admit_koru_aggregate_trade_boundary_index_publication_fact_v1",
    "admit_raw_blob_snapshot_publication_fact_v1",
    "build_integrated_trial_declarations",
    "compile_trial_declarations",
    "construct_koru_premium_preflight_authority_v1",
    "create_koru_premium_preflight_stage_publication_fact_v1",
    "execute_experiment",
    "execute_model_experiment",
    "open_koru_premium_preflight_authority_v1",
    "open_published_koru_aggregate_trade_boundary_index_authority_v3",
    "open_published_koru_tradifi_source_projection_authority_v3",
    "open_verified_raw_blob_snapshot",
    "publish_koru_aggregate_trade_boundary_index_authority_v3",
    "publish_koru_tradifi_source_projection_authority_v3",
    "publish_raw_blob_snapshot",
    "validate_model_build",
    "verify_koru_premium_preflight_authority_v1",
]
