import datetime
import os
from functools import partial
import tempfile

import boto3
import click
from dotenv import load_dotenv
import paramiko

from vptstools.bin.click_exception import catch_all_exceptions, report_click_exception_to_sns

# Load environmental variables from file in dev (load_dotenv doesn't override existing environment variables)
load_dotenv()

AWS_SNS_TOPIC = os.environ.get("SNS_TOPIC")
AWS_PROFILE = os.environ.get("AWS_PROFILE", None)
AWS_REGION = os.environ.get("AWS_REGION", None)
DESTINATION_BUCKET = os.environ.get("DESTINATION_BUCKET", "aloft")

# Update reporting to SNS functionality
report_sns = partial(report_click_exception_to_sns,
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
    """Extract the metadata from the filename (format such as 'fropo_vp_20220809T051000Z_0xb')

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
    """Sync files from Baltrad FTP server to the aloft s3 bucket.

    This function connects via SFTP to the BALTRAD server, downloads the available ``vp`` files (``pvol`` gets ignored),
    from the FTP server and upload the h5 file to the 'aloft' S3 bucket according to the defined folder path name
    convention. Existing files are ignored.

    Designed to be executed via a simple scheduled job like cron or scheduled cloud function. Remark that
    files disappear after a few days on the BALTRAD server.

    Configuration is loaded from the following environmental variables:

    - ``FTP_HOST``: Baltrad FTP host ip address
    - ``FTP_PORT``: Baltrad FTP host port
    - ``FTP_USERNAME``: Baltrad FTP user name
    - ``FTP_PWD``: Baltrad FTP password
    - ``FTP_DATADIR``: Baltrad FTP directory to load data files from
    - ``DESTINATION_BUCKET``: AWS S3 bucket to write data to
    - ``SNS_TOPIC``: AWS SNS topic to report when routine fails
    - ``AWS_REGION``: AWS region where the SNS alerting is defined
    - ``AWS_PROFILE``: AWS profile (mainly useful for local development when working with multiple AWS profiles)
    """
    cli_start_time = datetime.datetime.now()
    click.echo(f"Start transfer Baltrad FTP sync at {cli_start_time}")
    click.echo("Read configuration from environmental variables.")
    baltrad_server_host = os.environ.get("FTP_HOST")
    baltrad_server_port = int(os.environ.get("FTP_PORT"))
    baltrad_server_username = os.environ.get("FTP_USERNAME")
    baltrad_server_password = os.environ.get("FTP_PWD")
    baltrad_server_datadir = os.environ.get("FTP_DATADIR", "data")
    destination_bucket = DESTINATION_BUCKET

    click.echo("Establish SFTP connection.")
    paramiko.util.log_to_file("paramiko.log")
    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(
            paramiko.AutoAddPolicy()
        )  # necessary to avoid "Server 'hostname' not found in known_hosts error
        ssh.connect(
            baltrad_server_host,
            port=baltrad_server_port,
            username=baltrad_server_username,
            password=baltrad_server_password,
        )
        with ssh.open_sftp() as sftp:
            sftp.chdir(baltrad_server_datadir)

            click.echo("Initialize S3/Boto3 client")
            session = boto3.Session(profile_name=AWS_PROFILE)
            s3_client = session.client("s3")
            click.echo("Initialization complete, we can loop on files on the SFTP server")

            # listdir_attr is not a generator like listdir_iter which introduced BUG, but as the time between enlisting
            # and the effective download is now larger, the risk of a removed file in between requires us to
            # double check on the existence (should be edge case when running daily).
            for entry in sftp.listdir_attr():
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
                            try:
                                sftp.get(entry.filename, tmp_file_path)
                                click.echo(f"SFTP download of file {entry.filename} completed.")
                                s3_client.upload_file(
                                    tmp_file_path, destination_bucket, destination_key
                                )
                                click.echo(f"Upload of file {entry.filename} to S3 completed!")
                            except FileNotFoundError:
                                click.echo(
                                    f"{entry.filename} file could not longer be found on sFTP, skipping file."
                                )

                    else:
                        click.echo(
                            f"{destination_key} already exists at {destination_bucket}, skip it."
                        )
    cli_duration = datetime.datetime.now() - cli_start_time
    click.echo(f"File transfer from Baltrad finished, the syncrhonization took {cli_duration}.")


if __name__ == "__main__":
    cli()
