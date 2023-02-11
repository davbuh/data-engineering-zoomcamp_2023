from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import os
import requests as re

@task()
def write_local(dataset_url:str, dataset_file: str) -> Path:
    dirpath  = Path(f"../../data/fhv/")
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    filepath = Path(f"../../data/fhv/{dataset_file}.parquet")
    df.to_parquet(filepath, compression="gzip")
    return filepath


@task()
def write_gcs(from_path: Path, to_path:Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("de-zoomcamp-gcs-bucket")
    gcs_block.upload_from_path(from_path, to_path.as_posix())
    return


@flow(log_prints=True)
def etl_web_to_gcs(month:int =11) -> None:
    """The main ETL function"""    
    dirpath  = Path(f"./data/fhv/")
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)

    for month in range(1,13):        
        dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month:02}.csv.gz'
        print(f'reading file #{month} from web')
        response = re.get(dataset_url)
        with open(f"./data/fhv/{month}.csv.gz",'wb') as handle:
            handle.write(response.content)
        #path = write_local(dataset_url, dataset_file)
        #print(f'Total rows processed: {len(df_clean)}')
        
        from_path = Path(f'./data/fhv/{month}.csv.gz')
        to_path = Path(f'data/fhv/{month}.csv.gz')
        print(f'writing to gcs: {from_path}')
        write_gcs(from_path, to_path)


if __name__ == "__main__":
    etl_web_to_gcs()
    
