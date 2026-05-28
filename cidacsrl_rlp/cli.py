import argparse
from cidacsrl_rlp.shared.infra.spark.spark_factory import create_spark_session
from cidacsrl_rlp.shared.infra.config_loader import load_yaml
from cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper import bootstrap_sequential_linkage
from cidacsrl_cleaning.infra.adapters.inbound.cleaning_runner import run_cleaning
from cidacsrl_indexing.infra.adapters.inbound.indexing_runner import run_indexing


def main():
    parser = argparse.ArgumentParser(description="Motor CIDACS-RL")
    parser.add_argument("pipeline", choices=["cleaning", "indexing", "linkage"])
    parser.add_argument("--config", required=True, help="Caminho do YAML de configuração")
    
    args = parser.parse_args()

    pipeline_config = load_yaml(args.config)
    spark_config = load_yaml(pipeline_config.get("spark_config_path"), {})

    if args.pipeline == "linkage":
        spark = create_spark_session(app_name="CIDACS-RL Linkage Pipeline", **spark_config)
        bootstrap_sequential_linkage(pipeline_config, spark)
    if args.pipeline == "cleaning":
        spark = create_spark_session(app_name="CIDACS-RL Cleaning Pipeline", **spark_config)
        run_cleaning(pipeline_config, spark)
    if args.pipeline == "indexing":
        spark = create_spark_session(app_name="CIDACS-RL Indexing Pipeline", **spark_config)
        run_indexing(pipeline_config, spark)

if __name__ == "__main__":
    main()
