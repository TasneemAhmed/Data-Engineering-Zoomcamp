import pyarrow as pa 
import pyarrow.parquet as pq
import os

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/mage-demo-412911-3c063964aa6c.json"

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    #call global variable
    bucket_name = kwargs['bucket_name']
    object_name = "green_data_hw"
    
    #authorize environment automatically
    gcs = pa.fs.GcsFileSystem()

    root_path = f'{bucket_name}/{object_name}'
    
    #reading dataframe into pyarrow table, to write this table into parquet file
    table = pa.Table.from_pandas(df)


    """
        write pyarrow table into root_path(which exists in GCS bucket)
        partition dataframe by 'lpep_pickup_date' into parquet files
    """
    
    pq.write_to_dataset(
        table,
        root_path = root_path,
        partition_cols = ['lpep_pickup_date'],
        filesystem = gcs
    )

