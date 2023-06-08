from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse



path_countries = Path(f"carbon_data/countries.csv")
path = Path(f"carbon_data/emissions.parquet.gz")

@task()
def write_gcs_c(path: Path) -> None:
    """Upload local file to GCS"""
    gcs_block = GcsBucket.load("echarging-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return



@task()
def write_gcs(path_countries: Path) -> None:
    """Upload local file to GCS"""
    gcs_block = GcsBucket.load("echarging-gcs")
    gcs_block.upload_from_path(from_path=path_countries, to_path=path_countries)
    return

@task()
def load_bq(uris):
    """Load to BQ"""
    
    with BigQueryWarehouse.load("echarging-bigquery") as warehouse:
        operation = """
            CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-380308.carbon.emissions`
            OPTIONS (
                format = 'parquet',
                uris = %(uris)s
            );
        """
        warehouse.execute(operation, parameters={"uris": uris})

@flow()
def parent_etl() -> None:
    """The main ETL function"""
    write_gcs(path)
    #write_gcs_c(path_countries)
    #uris = ['gs://dtc_data_lake_zoomcamp-380308/carbon_data/emissions.parquet.gz']
    #load_bq(uris)
        
if __name__ == "__main__":
    parent_etl()