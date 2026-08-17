import pytest

from crypto_quant_research import (
    CampaignScope,
    ParameterCombination,
    SelectionPolicy,
    TrialDeclaration,
    compile_trial_declarations,
)

SCOPE = CampaignScope("sha256:hypothesis", "sha256:strategy", "momentum-v1")
POLICY = SelectionPolicy("return", "descending", 1)


class _Truthy:
    def __bool__(self) -> bool:
        return True


def _parameters(**values: str) -> ParameterCombination:
    return ParameterCombination(tuple(values.items()))


def _untraversable():
    raise AssertionError("the rejected iterable must not be traversed")
    yield None


def test_declaration_order_is_deterministic_and_selection_independent() -> None:
    first = compile_trial_declarations(
        campaign_scope=SCOPE,
        parameter_combinations=(
            _parameters(window="20", threshold="0.2"),
            _parameters(threshold="0.1", window="10"),
        ),
        seeds=(2, 1),
        selection_policy=POLICY,
    )
    second = compile_trial_declarations(
        campaign_scope=SCOPE,
        parameter_combinations=(
            _parameters(window="10", threshold="0.1"),
            _parameters(threshold="0.2", window="20"),
        ),
        seeds=(1, 2),
        selection_policy=SelectionPolicy("drawdown", "ascending", 2),
    )

    assert first == second
    assert [(item.parameters.values, item.seed) for item in first] == [
        ((('threshold', '0.1'), ('window', '10')), 1),
        ((('threshold', '0.1'), ('window', '10')), 2),
        ((('threshold', '0.2'), ('window', '20')), 1),
        ((('threshold', '0.2'), ('window', '20')), 2),
    ]
    assert set(TrialDeclaration.__dataclass_fields__) == {
        "campaign_scope",
        "parameters",
        "seed",
    }


def test_duplicate_trial_coordinate_fails_closed() -> None:
    parameters = _parameters(window="10")
    with pytest.raises(ValueError, match="duplicate"):
        compile_trial_declarations(
            campaign_scope=SCOPE,
            parameter_combinations=(parameters, parameters),
            seeds=(1,),
            selection_policy=POLICY,
        )

    with pytest.raises(ValueError, match="duplicate"):
        compile_trial_declarations(
            campaign_scope=SCOPE,
            parameter_combinations=(parameters,),
            seeds=(1, 1),
            selection_policy=POLICY,
        )


def test_inputs_are_declarations_not_ref_dereferences() -> None:
    declarations = compile_trial_declarations(
        campaign_scope=CampaignScope("opaque-h", "opaque-s", "label-only"),
        parameter_combinations=(_parameters(window="10"),),
        seeds=(0,),
        selection_policy=SelectionPolicy("metric-label", "ascending", 1),
    )
    assert declarations[0].campaign_scope.scope_label == "label-only"


@pytest.mark.parametrize("direction", ["ASCENDING", "unknown", "", True, _Truthy()])
def test_selection_direction_is_closed(direction: object) -> None:
    with pytest.raises(ValueError, match="direction"):
        SelectionPolicy("return", direction, 1)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("hypothesis_ref", ""),
        ("hypothesis_ref", True),
        ("hypothesis_ref", _Truthy()),
        ("strategy_definition_ref", 1),
        ("scope_label", object()),
        ("metric_label", ""),
        ("metric_label", True),
        ("metric_label", _Truthy()),
        ("max_selections", True),
        ("max_selections", 0),
        ("max_selections", _Truthy()),
    ],
)
def test_scalar_inputs_require_exact_nonempty_strings_and_ints(
    field: str, value: object
) -> None:
    if field in {"hypothesis_ref", "strategy_definition_ref", "scope_label"}:
        values: dict[str, object] = {
            "hypothesis_ref": "h",
            "strategy_definition_ref": "s",
            "scope_label": "scope",
        }
        values[field] = value
        with pytest.raises(ValueError):
            CampaignScope(**values)  # type: ignore[arg-type]
    else:
        values = {
            "metric_label": "return",
            "direction": "ascending",
            "max_selections": 1,
        }
        values[field] = value
        with pytest.raises(ValueError):
            SelectionPolicy(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "values",
    [
        [("window", "10")],
        ((["window", "10"],)),
        (("window", ""),),
        (("", "10"),),
        (("window", True),),
        ((True, "10"),),
        (("window", object()),),
        (("window", _Truthy()),),
        ((_Truthy(), "10"),),
        _Truthy(),
    ],
)
def test_parameter_values_require_exact_immutable_string_pairs(values: object) -> None:
    with pytest.raises(ValueError):
        ParameterCombination(values)  # type: ignore[arg-type]


def test_generators_and_mutable_sequences_are_rejected_before_traversal() -> None:
    parameters = _parameters(window="10")
    for parameter_combinations, seeds, expected in (
        (_untraversable(), (1,), "parameter_combinations"),
        ([parameters], (1,), "parameter_combinations"),
        ((parameters,), _untraversable(), "seeds"),
        ((parameters,), [1], "seeds"),
    ):
        with pytest.raises(ValueError, match=expected):
            compile_trial_declarations(
                campaign_scope=SCOPE,
                parameter_combinations=parameter_combinations,  # type: ignore[arg-type]
                seeds=seeds,  # type: ignore[arg-type]
                selection_policy=POLICY,
            )

    with pytest.raises(ValueError, match="parameter values"):
        ParameterCombination(_untraversable())  # type: ignore[arg-type]


def test_bool_and_truthy_seeds_fail_closed() -> None:
    parameters = _parameters(window="10")
    for seed in (True, _Truthy()):
        with pytest.raises(ValueError, match="seed"):
            compile_trial_declarations(
                campaign_scope=SCOPE,
                parameter_combinations=(parameters,),
                seeds=(seed,),  # type: ignore[arg-type]
                selection_policy=POLICY,
            )


def test_forged_frozen_fields_fail_closed_at_compile_boundaries() -> None:
    forged_parameters = object.__new__(ParameterCombination)
    object.__setattr__(forged_parameters, "values", (("window", "10"), ["mutable"]))
    with pytest.raises(ValueError):
        TrialDeclaration(SCOPE, forged_parameters, 1)

    forged_scope = object.__new__(CampaignScope)
    object.__setattr__(forged_scope, "hypothesis_ref", _Truthy())
    object.__setattr__(forged_scope, "strategy_definition_ref", "s")
    object.__setattr__(forged_scope, "scope_label", "scope")
    with pytest.raises(ValueError):
        TrialDeclaration(forged_scope, _parameters(window="10"), 1)

    forged_policy = object.__new__(SelectionPolicy)
    object.__setattr__(forged_policy, "metric_label", _Truthy())
    object.__setattr__(forged_policy, "direction", "ascending")
    object.__setattr__(forged_policy, "max_selections", True)
    with pytest.raises(ValueError):
        compile_trial_declarations(
            campaign_scope=SCOPE,
            parameter_combinations=(_parameters(window="10"),),
            seeds=(1,),
            selection_policy=forged_policy,
        )


def test_direct_trial_construction_revalidates_forged_coordinates() -> None:
    forged_parameters = object.__new__(ParameterCombination)
    object.__setattr__(forged_parameters, "values", _Truthy())
    with pytest.raises(ValueError):
        TrialDeclaration(SCOPE, forged_parameters, 1)
    with pytest.raises(ValueError):
        TrialDeclaration(SCOPE, _parameters(window="10"), True)

    missing_scope = object.__new__(CampaignScope)
    missing_parameters = object.__new__(ParameterCombination)
    with pytest.raises(ValueError):
        TrialDeclaration(missing_scope, _parameters(window="10"), 1)
    with pytest.raises(ValueError):
        TrialDeclaration(SCOPE, missing_parameters, 1)
