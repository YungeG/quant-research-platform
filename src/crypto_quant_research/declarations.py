from __future__ import annotations

from dataclasses import dataclass

_DIRECTIONS = frozenset({"ascending", "descending"})


def _require_nonempty_str(value: object, name: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{name} must be a nonempty string")
    return value


def _validate_seed(seed: object) -> int:
    if type(seed) is not int or seed < 0:
        raise ValueError("seed must be a nonnegative integer")
    return seed


def _canonical_parameter_values(values: object) -> tuple[tuple[str, str], ...]:
    if type(values) is not tuple:
        raise ValueError("parameter values must be a tuple")
    if not values:
        raise ValueError("parameter values must not be empty")

    canonical_values: list[tuple[str, str]] = []
    for pair in values:
        if type(pair) is not tuple or len(pair) != 2:
            raise ValueError("parameter values must contain name/value tuples")
        name, value = pair
        canonical_values.append(
            (
                _require_nonempty_str(name, "parameter name"),
                _require_nonempty_str(value, "parameter value"),
            )
        )

    names = [name for name, _ in canonical_values]
    if len(names) != len(set(names)):
        raise ValueError("parameter names must be unique")
    return tuple(sorted(canonical_values))


@dataclass(frozen=True, slots=True)
class CampaignScope:
    hypothesis_ref: str
    strategy_definition_ref: str
    scope_label: str

    def __post_init__(self) -> None:
        _require_nonempty_str(self.hypothesis_ref, "hypothesis_ref")
        _require_nonempty_str(self.strategy_definition_ref, "strategy_definition_ref")
        _require_nonempty_str(self.scope_label, "scope_label")


@dataclass(frozen=True, slots=True)
class ParameterCombination:
    values: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "values", _canonical_parameter_values(self.values))

    @property
    def sort_key(self) -> tuple[tuple[str, str], ...]:
        return self.values


@dataclass(frozen=True, slots=True)
class SelectionPolicy:
    metric_label: str
    direction: str
    max_selections: int

    def __post_init__(self) -> None:
        _require_nonempty_str(self.metric_label, "metric_label")
        if type(self.direction) is not str or self.direction not in _DIRECTIONS:
            raise ValueError("direction must be ascending or descending")
        if type(self.max_selections) is not int or self.max_selections <= 0:
            raise ValueError("max_selections must be a positive integer")


@dataclass(frozen=True, slots=True)
class TrialDeclaration:
    campaign_scope: CampaignScope
    parameters: ParameterCombination
    seed: int

    def __post_init__(self) -> None:
        _validate_campaign_scope(self.campaign_scope)
        _validate_parameter_combination(self.parameters)
        _validate_seed(self.seed)


def _validate_campaign_scope(value: object) -> CampaignScope:
    if type(value) is not CampaignScope:
        raise ValueError("campaign_scope must be a CampaignScope")
    try:
        _require_nonempty_str(value.hypothesis_ref, "hypothesis_ref")
        _require_nonempty_str(value.strategy_definition_ref, "strategy_definition_ref")
        _require_nonempty_str(value.scope_label, "scope_label")
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("campaign_scope must contain canonical fields") from error
    return value


def _validate_parameter_combination(value: object) -> ParameterCombination:
    if type(value) is not ParameterCombination:
        raise ValueError("parameters must be a ParameterCombination")
    try:
        canonical_values = _canonical_parameter_values(value.values)
        if value.values != canonical_values:
            raise ValueError("parameter values must be canonical")
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("parameters must contain canonical values") from error
    return value


def _validate_selection_policy(value: object) -> SelectionPolicy:
    if type(value) is not SelectionPolicy:
        raise ValueError("selection_policy must be a SelectionPolicy")
    try:
        _require_nonempty_str(value.metric_label, "metric_label")
        if type(value.direction) is not str or value.direction not in _DIRECTIONS:
            raise ValueError("direction must be ascending or descending")
        if type(value.max_selections) is not int or value.max_selections <= 0:
            raise ValueError("max_selections must be a positive integer")
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("selection_policy must contain canonical fields") from error
    return value


def compile_trial_declarations(
    *,
    campaign_scope: CampaignScope,
    parameter_combinations: tuple[ParameterCombination, ...],
    seeds: tuple[int, ...],
    selection_policy: SelectionPolicy,
) -> tuple[TrialDeclaration, ...]:
    if type(parameter_combinations) is not tuple:
        raise ValueError("parameter_combinations must be a tuple")
    if type(seeds) is not tuple:
        raise ValueError("seeds must be a tuple")
    _validate_campaign_scope(campaign_scope)
    _validate_selection_policy(selection_policy)
    if not parameter_combinations or not seeds:
        raise ValueError("parameter combinations and seeds must not be empty")

    validated_parameters = tuple(
        _validate_parameter_combination(parameters)
        for parameters in parameter_combinations
    )
    validated_seeds = tuple(_validate_seed(seed) for seed in seeds)
    coordinates = tuple(
        (parameters, seed)
        for parameters in validated_parameters
        for seed in validated_seeds
    )
    if len(coordinates) != len(
        {(parameters.values, seed) for parameters, seed in coordinates}
    ):
        raise ValueError("duplicate trial declaration")

    # The frozen compatibility input is validated, but selection lineage is not
    # an execution coordinate and is deliberately absent from declarations.
    return tuple(
        TrialDeclaration(campaign_scope, parameters, seed)
        for parameters, seed in sorted(
            coordinates,
            key=lambda item: (item[0].sort_key, item[1]),
        )
    )
