import click
import boto3
from botocore.exceptions import ClientError
import os
import json
import h5py
from pathlib import Path

from deon.s3sync import SmartS3Sync
import logging

import pandas as pd

"""S3 link"""

def sync_s3(local, s3path, fromS3, interval, force, **kwargs):
    s3_sync = SmartS3Sync(
        local = local,
        s3path = s3path,
        **kwargs
    )

    s3_sync.sync(interval = interval, force = force, fromS3 = fromS3)

def sync_files_s3(list_of_files, force=False, **kwargs):
    # s3 prefix to search is the minimum shared prefix of list of files
    s_files = sorted(list_of_files)
    first, last = s_files[0], s_files[-1]
    for i in range(min(len(first), len(last))):
        if first[i] != last[i]:
            break
    result = first[:i]
    prefix = result[:-result[::-1].index('/')]

    s3_sync = SmartS3Sync(
        local = list(list_of_files),
        s3path = prefix,
        **kwargs
    )
    s3_sync.sync_files_fromS3(force=force)

"""Google cloud link: TODO"""

"""Azure blob link: TODO"""

"""CLI"""

@click.group()
def cli():
    pass

@cli.command()
@click.argument("local_path")
@click.option('--interval', is_flag=True)
@click.option('--force', is_flag=True)
@click.option('--metadata', is_flag=True)
@click.option('--profile', is_flag=True)
@click.option('--log', default=20) # 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL
def up(local_path, interval, force, **kwargs):
    """Sync data up: local -> remote"""
    local = local_path
    s3path = local_path
    fromS3 = False

    sync_s3(local, s3path, fromS3, interval, force, **kwargs)


@cli.command()
@click.argument("local_path")
@click.option('--force', is_flag=True)
@click.option('--interval', is_flag=True)
@click.option('--metadata', is_flag=True)
@click.option('--profile', is_flag=True)
@click.option('--log', default=20) # 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL
def down(local_path, force, interval, **kwargs):
    """Sync data down: remote -> local"""
    local = local_path
    s3path = local_path
    fromS3 = True

    sync_s3(local, s3path, fromS3, interval, force, **kwargs)


@cli.group()
def metadata():
    """Metadata management CLI"""
    pass


@metadata.command()
@click.argument("local_dir")
@click.option('--force', is_flag=True)
@click.option('--interval', is_flag=True)
@click.option('--metadata', is_flag=True)
@click.option('--profile', is_flag=True)
@click.option('--log', default=20) # 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL
def syncdown(local_dir, force, interval, **kwargs):
    """Sync metadata of data path <local_dir> down"""
    local = local_dir
    s3path = local_dir

    s3_sync = SmartS3Sync(local = local, s3path = s3path, **kwargs)
    s3_sync.sync_metadata_fromS3(force = force, show_progress = False)


@metadata.command()
@click.argument("local_dir")
def load(local_dir):
    """Load metadata and put it in a data frame, then start a ipdb session"""
    path = Path("metadata") / local_dir
    metadatas = []
    paths = list(path.glob('**/*.json'))
    for metadata_path in paths:
        with metadata_path.open("r") as metadata_file:
            metadata = json.load(metadata_file)
        metadatas.append(metadata)

    filenames = [str(path)[len("metadata/"):-5] for path in paths]
    df = pd.DataFrame(metadatas, index=filenames)
    print("loaded data frame df")
    print("rows", len(df))
    print("keys", df.keys())
    print("""
example usage:
filtered_df = df[df['robot'] == "sawyer"]
print(filtered_df.index)
sync_files_s3(filtered_df.index, force=False)
"""
    )
    import ipdb; ipdb.set_trace()


@cli.command()
@click.argument("local_file")
def show(local_dir):
    """Visualize hdf5 file (TODO)"""
    pass


@cli.command()
def buckets():
    """Print list of accessible buckets from S3"""
    s3client = boto3.client('s3')
    response = s3client.list_buckets()
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(bucket["Name"])


if __name__ == "__main__":
    cli()
