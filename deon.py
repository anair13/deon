import click
import boto3
from botocore.exceptions import ClientError
import os
import json
import h5py
from pathlib import Path

from s3sync import SmartS3Sync
import logging

def sync_s3(local, s3path, fromS3, interval, force, **kwargs):
    s3_sync = SmartS3Sync(
        local = local,
        s3path = s3path,
        **kwargs
    )

    s3_sync.sync(interval = interval, force = force, fromS3 = fromS3)

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
    local = local_path
    s3path = local_path
    fromS3 = True

    sync_s3(local, s3path, fromS3, interval, force, **kwargs)


@cli.group()
def metadata():
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
