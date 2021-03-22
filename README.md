# deon
A lightweight utility for managing DEcentrailized ONline datasets

The aim of this utility is to enable data sharing among several users, where
each user may be continuously collecting new data. The command line interface
enables: (1) uploading your data to AWS S3 and (2) managing metadata to
selectively filter and download data from other users.

## Installation
Source python > 3.5

`pip install -r requirements.txt`

`pip install -e .`

## Data Format
This package (so far) works with hdf5 files with metadata stored as valid JSON
in `hf.attrs['metadata']`. When you use `deoncli up` to upload this file, the
metadata is added to S3 header metadata and can be used for filtering data
without downloading the entire dataset.

## Usage
Print list of accessible buckets from S3

`deoncli buckets`

Sync data down: remote -> local

`deoncli down <bucket>/<prefix>`

Sync data up: local -> remote

`deoncli up <bucket>/<prefix>`

Sync metadata down

`deoncli metadata down <bucket>/<prefix>`

Sync specific files down

```
$ deoncli metadata load rail-robot-data-sharing-v1/mini-robonet
loaded data frame df
rows 703
keys Index(['ETag', 'action_T', 'action_space', 'adim', 'background', 'bin_insert', 'bin_type', 'camera_configuration', 'camera_type', 'contains_annotation', 'environment_size', 'file_version', 'frame_dim', 'gripper', 'high_bound', 'image_format', 'img_T', 'img_encoding', 'low_bound', 'ncam', 'object_batch', 'object_classes', 'policy_desc', 'primitives', 'robot', 'sdim', 'sha256', 'state_T', 'term_t', 'traj_ok'], dtype='object')
example usage:
filtered_df = df[df['robot'] == "sawyer"]
print(filtered_df.index)
sync_files_s3(filtered_df.index, force=False)
ipdb> filtered_df = df[df['robot'] == "sawyer"]
ipdb> sync_files_s3(filtered_df.index, force=False)
INFO:SmartS3Sync:local files are up to date
```

Visualize hdf5 file (TODO)

`deoncli show <filename>.hdf5`

## Access Management

The following snippet in S3 > Permissions > Bucket Policy shares the bucket with an organization:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowOrganizationToReadBucket",
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::rail-robot-data-sharing-v1",
                "arn:aws:s3:::rail-robot-data-sharing-v1/*"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:PrincipalOrgID": "o-<organization_id>"
                }
            }
        }
    ]
}
```

The organization ID is available at https://console.aws.amazon.com/organizations/home. You can also share with individual users or have different access controls.
