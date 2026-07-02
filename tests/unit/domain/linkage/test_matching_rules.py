import pytest

from cidacsrl.domain.linkage.matching_rules import BlockingPhase, ComparisonRule
from cidacsrl.domain.linkage.linkage_specification import SequentialLinkageSpecification

pytestmark = pytest.mark.unit

def _build_rule(source_column: str, target_column: str) -> ComparisonRule:
	return ComparisonRule(
		source_column=source_column,
		target_column=target_column,
		similarity="exact",
		weight=1.0,
		es_clause_type="must",
	)


def test_blocking_phase_exposes_comparison_target_fields():
	phase = BlockingPhase(
		phase_name="exact",
		rules=[
			_build_rule("nome_1", "nome_completo"),
			_build_rule("nome_2", "nome_completo"),
			_build_rule("municipio", "municipio_nascimento"),
		],
	)

	assert phase.comparison_target_fields == [
		"nome_completo",
		"municipio_nascimento",
	]


def test_workflow_builds_blocking_phase_context_with_required_and_output_fields():
	phase = BlockingPhase(
		phase_name="exact",
		rules=[_build_rule("nome", "nome_completo")],
	)
	workflow = SequentialLinkageSpecification(
		source_table="source_table",
		id_source_table="source_id",
		target_es_index="target_index",
		id_target_table="target_id",
		blocking_phases=[phase],
	)

	context = workflow.build_blocking_phase_context(phase)

	assert context.target_fields.comparison_fields == ["nome_completo"]
	assert context.target_fields.required_fields == ["target_id"]
	assert context.target_fields.extra_fields == []
	assert context.target_fields.fetch_fields == ["target_id", "nome_completo"]
	assert context.target_fields.result_fields == ["target_id", "nome_completo"]


def test_workflow_uses_comparison_fields_as_default_target_output():
	phase = BlockingPhase(
		phase_name="exact",
		rules=[_build_rule("nome", "nome_completo")],
	)
	workflow = SequentialLinkageSpecification(
		source_table="source_table",
		id_source_table="source_id",
		target_es_index="target_index",
		id_target_table="target_id",
		blocking_phases=[phase],
	)

	context = workflow.build_blocking_phase_context(phase)

	assert context.target_fields.comparison_fields == ["nome_completo"]
	assert context.target_fields.result_fields == ["target_id", "nome_completo"]
