from cidacsrl.adapters.outbound.elasticsearch.spark_es_indexing_adapter import (
    SparkESIndexingAdapter,
)


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
