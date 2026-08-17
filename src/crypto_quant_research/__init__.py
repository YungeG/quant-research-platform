from importlib import import_module

from .declarations import (
    CampaignScope,
    ParameterCombination,
    SelectionPolicy,
    TrialDeclaration,
    compile_trial_declarations,
)

_RUNTIME_EXPORTS = frozenset(
    {
        "FrozenExperimentInputs",
        "PublishedNoSelection",
        "PublishedStrategyCandidate",
        "TrialExecution",
        "execute_experiment",
    }
)


def __getattr__(name: str):
    if name in _RUNTIME_EXPORTS:
        return getattr(import_module(".runtime", __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CampaignScope",
    "FrozenExperimentInputs",
    "ParameterCombination",
    "PublishedNoSelection",
    "PublishedStrategyCandidate",
    "SelectionPolicy",
    "TrialDeclaration",
    "TrialExecution",
    "compile_trial_declarations",
    "execute_experiment",
]
