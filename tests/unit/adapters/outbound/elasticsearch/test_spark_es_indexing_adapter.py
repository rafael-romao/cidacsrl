from cidacsrl.adapters.outbound.elasticsearch.spark_es_indexing_adapter import (
    SparkESIndexingAdapter,
)
from cidacsrl.domain.indexing.indexing_specification import (
    DatasetIndexingSpecification,
)


def _adapter():
    return SparkESIndexingAdapter(es_config={"es_connection_url": "http://elasticsearch:9200"})


def _mapping_for(columns, index_config=None):
    spec = DatasetIndexingSpecification.from_dict({
        "source_config": {"source_table": "t", "id_field": "id"},
        "index_config": {"name": "demo", **(index_config or {})},
        "index_columns": columns,
    })
    return _adapter()._build_es_mapping_payload(spec)


def test_mapping_text_index_as_both_creates_keyword_subfield_with_default_ignore_above():
    props = _mapping_for([{"name": "nome", "type": "text", "index_as": "both"}])["mappings"]["properties"]
    assert props["nome"] == {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
    }


def test_mapping_text_index_as_both_honors_custom_ignore_above():
    props = _mapping_for([
        {"name": "nome", "type": "text", "index_as": "both", "ignore_above": 64}
    ])["mappings"]["properties"]
    assert props["nome"]["fields"]["keyword"]["ignore_above"] == 64


def test_mapping_text_index_as_keyword_becomes_pure_keyword():
    props = _mapping_for([{"name": "cpf", "type": "text", "index_as": "keyword"}])["mappings"]["properties"]
    assert props["cpf"] == {"type": "keyword"}


def test_mapping_text_carries_analyzer():
    props = _mapping_for([
        {"name": "obs", "type": "text", "analyzer": "brazilian"}
    ])["mappings"]["properties"]
    assert props["obs"] == {"type": "text", "analyzer": "brazilian"}


def test_mapping_keyword_honors_ignore_above():
    props = _mapping_for([{"name": "tags", "type": "keyword", "ignore_above": 32}])["mappings"]["properties"]
    assert props["tags"] == {"type": "keyword", "ignore_above": 32}


def test_mapping_date_carries_format():
    props = _mapping_for([
        {"name": "dt", "type": "date", "format": "yyyy-MM-dd"}
    ])["mappings"]["properties"]
    assert props["dt"] == {"type": "date", "format": "yyyy-MM-dd"}


def test_mapping_date_without_format_omits_it():
    props = _mapping_for([{"name": "dt", "type": "date"}])["mappings"]["properties"]
    assert props["dt"] == {"type": "date"}


def test_mapping_scalar_types_pass_through():
    props = _mapping_for([
        {"name": "idade", "type": "integer"},
        {"name": "fatal", "type": "boolean"},
    ])["mappings"]["properties"]
    assert props["idade"] == {"type": "integer"}
    assert props["fatal"] == {"type": "boolean"}


def test_mapping_string_alias_is_normalized_to_text():
    props = _mapping_for([{"name": "apelido", "type": "string"}])["mappings"]["properties"]
    assert props["apelido"] == {"type": "text"}


def test_mapping_settings_include_shards_replicas_refresh():
    settings = _mapping_for(
        [{"name": "id", "type": "keyword"}],
        index_config={"number_of_shards": 3, "number_of_replicas": 2, "refresh_interval": "5s"},
    )["settings"]["index"]
    assert settings["number_of_shards"] == 3
    assert settings["number_of_replicas"] == 2
    assert settings["refresh_interval"] == "5s"
    assert "analysis" not in settings


def test_mapping_settings_include_analysis_when_provided():
    analysis = {
        "analyzer": {"folding": {"tokenizer": "standard", "filter": ["lowercase", "asciifolding"]}}
    }
    settings = _mapping_for(
        [{"name": "nome", "type": "text", "analyzer": "folding"}],
        index_config={"analysis": analysis},
    )["settings"]["index"]
    assert settings["analysis"] == analysis


def test_build_es_write_options_defaults_to_wan_only_and_no_ssl():
    adapter = SparkESIndexingAdapter(es_config={"es_connection_url": "http://elasticsearch:9200"})

    options = adapter._build_es_write_options(id_field=None)

    assert options["es.nodes"] == "elasticsearch"
    assert options["es.port"] == "9200"
    assert options["es.nodes.wan.only"] == "true"
    assert "es.net.ssl" not in options
    assert options["es.write.operation"] == "index"


def test_build_es_write_options_enables_ssl_for_https_url():
    adapter = SparkESIndexingAdapter(es_config={"es_connection_url": "https://elasticsearch.exemplo.com:9243"})

    options = adapter._build_es_write_options(id_field="codigo")

    assert options["es.net.ssl"] == "true"
    assert options["es.write.operation"] == "upsert"
    assert options["es.mapping.id"] == "codigo"


def test_build_es_write_options_allows_self_signed_when_verify_certs_false():
    adapter = SparkESIndexingAdapter(
        es_config={
            "es_connection_url": "https://elasticsearch.exemplo.com:9243",
            "verify_certs": False,
        }
    )

    options = adapter._build_es_write_options(id_field=None)

    assert options["es.net.ssl.cert.allow.self.signed"] == "true"


def test_build_es_write_options_forwards_basic_auth():
    adapter = SparkESIndexingAdapter(
        es_config={
            "es_connection_url": "http://elasticsearch:9200",
            "es_user": "elastic",
            "es_password": "senha",
        }
    )

    options = adapter._build_es_write_options(id_field=None)

    assert options["es.net.http.auth.user"] == "elastic"
    assert options["es.net.http.auth.pass"] == "senha"


def test_build_es_write_options_raw_es_keys_take_precedence():
    adapter = SparkESIndexingAdapter(
        es_config={
            "es_connection_url": "http://elasticsearch:9200",
            "wan_only": True,
            "es.nodes.wan.only": "false",
            "es.net.ssl.truststore.location": "/certs/truststore.jks",
        }
    )

    options = adapter._build_es_write_options(id_field=None)

    assert options["es.nodes.wan.only"] == "false"
    assert options["es.net.ssl.truststore.location"] == "/certs/truststore.jks"
