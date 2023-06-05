"""
Python CLI script that:
- Connects via SFTP to the BALTRAD server
- For each vp file (pvol gets ignored), download the file from the server and
  upload it to the "aloft" S3 bucket
- If file already exists at destination => do nothing

Designed to be executed daily via a simple scheduled job like cron (files disappear after a few
days on the BALTRAD server)

Configuration is loaded from environmental variables:
- FTP_HOST: Baltrad FTP host ip address
- FTP_PORT: Baltrad FTP host port
- FTP_USERNAME: Baltrad FTP user name
- FTP_PWD: Baltrad FTP password
- FTP_DATADIR: Baltrad FTP directory to load data files from
- DESTINATION_BUCKET: AWS S3 bucket to write data to
- SNS_TOPIC: AWS SNS topic to report when routine fails
- AWS_PROFILE: AWS profile (mainly for local development)
"""

import os
from functools import partial
import tempfile

import boto3
import click
from dotenv import load_dotenv
import paramiko

from vptstools.bin.click_exception import catch_all_exceptions, report_exception_to_sns

# Load environmental variables from file in dev (load_dotenv doesn't override existing environment variables)
load_dotenv()

AWS_SNS_TOPIC = os.environ.get("SNS_TOPIC")
AWS_PROFILE = os.environ.get("AWS_PROFILE", None)
AWS_REGION = os.environ.get("AWS_REGION", None)
DESTINATION_BUCKET = os.environ.get("DESTINATION_BUCKET", "aloft")

# Update reporting to SNS functionality
report_sns = partial(report_exception_to_sns,
                     aws_sns_topic=AWS_SNS_TOPIC,
                     subject=f"Transfer from Baltrad FTP to s3 bucket {DESTINATION_BUCKET} failed.",
                     profile_name=AWS_PROFILE,
                     region_name=AWS_REGION
                     )


def s3_key_exists(key: str, bucket: str, s3_client) -> bool:
    """Check if an S3 key is existing in a bucket

    Parameters
    ----------
    key : str
        Key of the object
    bucket : str
        Bucket name to search for key
    s3_client : boto3.Session
        Boto3 session

    Returns
    -------
    bool
    """
    results = s3_client.list_objects(Bucket=bucket, Prefix=key)
    return "Contents" in results


def extract_metadata_from_filename(filename: str) -> tuple:
    """Extract the metadata from the filename (format
    such as 'fropo_vp_20220809T051000Z_0xb')

    All returned values are strings, month and days are 0-prefixed if
    they are single-digit.

    Parameters
    ----------
    filename : str
        Filename of a h5 incoming file from FTP
    """
    elems = filename.split("_")
    radar_code = elems[0]
    timestamp = elems[2]

    year = timestamp[0:4]
    month_str = timestamp[4:6]
    day_str = timestamp[6:8]

    return radar_code, year, month_str, day_str


@click.command(cls=catch_all_exceptions(click.Command, handler=report_sns))  # Add SNS-reporting to exception
def cli():

    click.echo("1. Read configuration from environmental variables")
    baltrad_server_host = os.environ.get("FTP_HOST")
    baltrad_server_port = int(os.environ.get("FTP_PORT"))
    baltrad_server_username = os.environ.get("FTP_USERNAME")
    baltrad_server_password = os.environ.get("FTP_PWD")
    baltrad_server_datadir = os.environ.get("FTP_DATADIR", "data")
    destination_bucket = DESTINATION_BUCKET

    click.echo("2. Establish SFTP connection")
    paramiko.util.log_to_file("paramiko.log")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(
        paramiko.AutoAddPolicy()
    )  # necessary to avoid "Server 'hostname' not found in known_hosts error
    ssh.connect(
        baltrad_server_host,
        port=baltrad_server_port,
        username=baltrad_server_username,
        password=baltrad_server_password,
    )
    sftp = ssh.open_sftp()
    sftp.chdir(baltrad_server_datadir)

    click.echo("3. Initialize S3/Boto3 client")
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3_client = session.client("s3")

    click.echo("Initialization complete, we can loop on files on the SFTP server")
    for entry in sftp.listdir_iter():
        if "_vp_" in entry.filename:  # PVOLs and other files are ignored
            click.echo(
                f"{entry.filename} is a vp file, we need to consider it... "
            )

            radar_code, year, month_str, day_str = extract_metadata_from_filename(
                entry.filename
            )
            destination_key = (
                f"baltrad/hdf5/{radar_code}/{year}/"
                f"{month_str}/{day_str}/{entry.filename}"
            )
            if not s3_key_exists(destination_key, destination_bucket, s3_client):
                click.echo(
                    f"{destination_key} does not exist at {destination_bucket}, "
                    f"transfer it..."
                )
                with tempfile.TemporaryDirectory() as tmpdirname:
                    tmp_file_path = os.path.join(tmpdirname, entry.filename)
                    sftp.get(entry.filename, tmp_file_path)
                    click.echo("SFTP download completed. ")
                    s3_client.upload_file(
                        tmp_file_path, destination_bucket, destination_key
                    )
                    click.echo("Upload to S3 completed!")
            else:
                click.echo(
                    f"{destination_key} already exists at {destination_bucket}, skip it."
                )
    click.echo("File transfer from Baltrad finished.")


if __name__ == "__main__":
    cli()
