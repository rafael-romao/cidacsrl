from cidacsrl_rlp.cidacsrl.domain.models.rules import ComparisonRule
from cidacsrl_rlp.cidacsrl.infra.configs.models.indexed_dataset_filter import IndexedDatasetFilterItem
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.query_builder import ElasticsearchQueryBuilder

def test_build_query_with_single_rule():
    # Arrange

    rules = [ComparisonRule(
        source_column="municipio_id", 
        target_column="municipio_residencia", 
        similarity="exact", 
        weight=1.0,
        query_type="term", 
        es_clause_type="must"
    ),
    ComparisonRule(
        source_column="nome", 
        target_column="nome_completo", 
        similarity="fuzzy", 
        weight=2.0,
        query_type="match", 
        is_fuzzy=True, 
        boost=2.0, 
        es_clause_type="should"
    )]

    filters = [
        IndexedDatasetFilterItem(term={"status": "active"}),
        IndexedDatasetFilterItem(column="uf"),
    ]


    query_builder = ElasticsearchQueryBuilder(
        phase_rules=rules,
        fetch_fields=["municipio_residencia", "nome_completo"],
        candidate_limit=100,
        static_filter=filters
    )

    source_record = {
        "nome": "João Silva",
        "municipio_id": "12345",
        "uf": "SP"
    }

    # Act
    query = query_builder.build_search_body_for_record(source_record)

    # Assert
    expected_query = {
        '_source': ['municipio_residencia', 'nome_completo'],
        'query': {
            'bool': {
                'filter': [
                    {'term': {'status': 'active'}},
                    {'term': {'uf': 'SP'}}
                ],
                'must': [
                    {'term': {'municipio_residencia': {'value': '12345'}}}
                ],
                'should': [
                    {'match': {'nome_completo': {'query': 'João Silva', 'fuzziness': 'AUTO', 'boost': 2.0}}}
                ]
            }
        },
        "size": 100
    }
    assert query == expected_query