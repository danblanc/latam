from typing import List, Tuple
from datetime import datetime
import polars as pl

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    df_scan = pl.scan_ndjson(file_path, infer_schema_length=None)

    df_output = df_scan \
        .select(
            [
                pl.col('date').str.strptime(pl.Datetime).dt.date().alias('datetime'),
                pl.col('user').struct.field('username').alias('username')
            ]
        ) \
        .group_by(['datetime', 'username']) \
        .agg(
            twits_count = pl.count('username')
        ) \
        .group_by('datetime') \
        .agg([
            pl.col('username').gather(pl.col('twits_count').arg_max()),
            pl.col('twits_count').gather(pl.col('twits_count').arg_max())
        ]) \
        .with_columns(
            pl.col('username').list.first(),
            pl.col('twits_count').list.first()
        ) \
        .sort('twits_count', descending=True) \
        .select(['datetime', 'username']) \
        .head(10)
    
    return df_output.collect()