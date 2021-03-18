import click
import boto3
from botocore.exceptions import ClientError
import os
import json
import h5py
from pathlib import Path

"""S3 utils"""

s3_client = boto3.client('s3')

def upload_to_s3(file_name, bucket, object_name):
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def split_s3_path(s3_path):
    """
    Split "s3://foo/bar/baz" into "foo" and "bar/baz"
    """
    bucket_name_and_directories = s3_path.split('//')[1]
    bucket_name, *directories = bucket_name_and_directories.split('/')
    directory_path = '/'.join(directories)
    return bucket_name, directory_path

def join_s3_path(bucket_name, directory_path):
    """
    Join bucket name "foo" and path "bar/baz" into an S3 URL: "s3://foo/bar/baz"
    """
    return ["s3:/", bucket_name, directory_path].join("/")

def sync_down(path, check_exists=True):
    is_docker = os.path.isfile("/.dockerenv")
    if is_docker:
        local_path = "/tmp/%s" % (path)
    else:
        local_path = "%s/%s" % (LOCAL_LOG_DIR, path)

    if check_exists and os.path.isfile(local_path):
        return local_path

    local_dir = os.path.dirname(local_path)
    os.makedirs(local_dir, exist_ok=True)

    if is_docker:
        from doodad.ec2.autoconfig import AUTOCONFIG
        os.environ["AWS_ACCESS_KEY_ID"] = AUTOCONFIG.aws_access_key()
        os.environ["AWS_SECRET_ACCESS_KEY"] = AUTOCONFIG.aws_access_secret()

    full_s3_path = os.path.join(AWS_S3_PATH, path)
    bucket_name, bucket_relative_path = split_s3_full_path(full_s3_path)
    try:
        bucket = boto3.resource('s3').Bucket(bucket_name)
        bucket.download_file(bucket_relative_path, local_path)
    except Exception as e:
        local_path = None
        print("Failed to sync! path: ", path)
        print("Exception: ", e)
    return local_path


def sync_down_folder(path):
    is_docker = os.path.isfile("/.dockerenv")
    if is_docker:
        local_path = "/tmp/%s" % (path)
    else:
        local_path = "%s/%s" % (LOCAL_LOG_DIR, path)

    local_dir = os.path.dirname(local_path)
    os.makedirs(local_dir, exist_ok=True)

    if is_docker:
        from doodad.ec2.autoconfig import AUTOCONFIG
        os.environ["AWS_ACCESS_KEY_ID"] = AUTOCONFIG.aws_access_key()
        os.environ["AWS_SECRET_ACCESS_KEY"] = AUTOCONFIG.aws_access_secret()

    full_s3_path = os.path.join(AWS_S3_PATH, path)
    bucket_name, bucket_relative_path = split_s3_full_path(full_s3_path)
    command = "aws s3 sync s3://%s/%s %s" % (bucket_name, bucket_relative_path, local_path)
    print(command)
    stream = os.popen(command)
    output = stream.read()
    print(output)
    return local_path


"""Utils"""

def get_metadata(filepath):
    hf = h5py.File(filepath, "r")
    return hf.attrs['metadata']

def resolve_deon_path(deon_dir):
    return Path(deon_dir)

def read_file(path):
    with path.open("r") as file:
        return file.read()

def write_file(path, contents):
    with path.open("w") as file:
        file.write(contents)

def write_json(path, contents, *args, **kwargs):
    with path.open("w") as file:
        json.dump(contents, file, *args, **kwargs)

def parse_config(config):
    metadata_bucket = config["metadata_bucket"]
    data_buckets = []
    for x in config["data_buckets"]:
        data_buckets.append(x['bucket'])
    return metadata_bucket, data_buckets

def get_config(deon_path):
    config_path = deon_path / "config.json"
    with config_path.open("r") as config_file:
        deon_config = json.load(config_file)
    return deon_config


"""CLI"""

@click.group()
def cli():
    pass

@cli.command()
@click.argument("path")
@click.option("--metadata_bucket", default="rail-robot-data-sharing-metadata-v1", help="Shared metadata bucket")
@click.option("--data_buckets", default="rail-robot-data-sharing-v1", help="Data bucket, comma separated")
def init(path, metadata_bucket, data_buckets):
    """Initialize a local dataset at <path>"""
    data_buckets_list = data_buckets.split(",")
    data_buckets_dict = [dict(bucket=bucket_name) for bucket_name in data_buckets_list]
    config = dict(
        metadata_bucket=metadata_bucket,
        data_buckets=data_buckets_dict,
    )

    # set up directory structure
    base_path = Path(path)
    metadata_path = Path(path) / metadata_bucket
    metadata_path.mkdir(parents=True, exist_ok=True)
    for data_bucket in data_buckets_list:
        data_bucket_path = Path(path) / data_bucket
        data_bucket_path.mkdir(parents=True, exist_ok=True)

    config_path = base_path / "config.json"
    write_json(config_path, config, indent=4)


@cli.command()
@click.argument("data_path")
@click.option("--deon_path", default="")
def down(data_path, deon_path, ):
    """Sync data down: remote -> local"""
    base_path = resolve_deon_path(deon_path)
    deon_config = get_config(base_path)

    print(deon_config)


@cli.command()
@click.argument("data_dir")
@click.option("--deon_dir", default="")
def up(data_dir, deon_dir, ):
    """Sync data up: local -> remote"""
    deon_path = resolve_deon_path(deon_dir)
    deon_config = get_config(deon_path)

    metadata_bucket, data_buckets = parse_config(deon_config)
    metadata_path = deon_path / metadata_bucket

    data_path = deon_path / data_dir

    print(deon_config)

    for path in data_path.glob("**/*.hdf5"):
        metadata_str = get_metadata(path)
        # TODO: convert to json and make sure its valid

        rel_path = path.relative_to(deon_path)
        bucket_name = rel_path.parts[0]
        bucket_key = "/".join(rel_path.parts[1:])
        assert bucket_name in data_buckets

        # import ipdb; ipdb.set_trace()

        metadata_key_suffix = str((rel_path.parent / rel_path.stem).with_suffix(".json"))
        metadata_filepath = metadata_path / metadata_key_suffix
        metadata_key = metadata_bucket + "/" + metadata_key_suffix
        if False: # metadata_filepath.exists():
            metadata_contents = read_file(metadata_filepath)
            if metadata_str != metadata_contents:
                write_file(metadata_filepath, metadata_str)
                upload_to_s3(str(metadata_filepath), metadata_bucket, metadata_key)
        else:
            # make sure metadata parent path exists
            metadata_filepath.parent.mkdir(parents=True, exist_ok=True)
            write_file(metadata_filepath, metadata_str)
            upload_to_s3(str(metadata_filepath), metadata_bucket, metadata_key)


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
