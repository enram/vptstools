"""Utility to support SNS topic publishing on failed CLI commands"""

import boto3
import click


def catch_all_exceptions(cls, handler):  # noqa
    """Catch exceptions raised by a click CLI command to override exception handler.

    Credits to https://stackoverflow.com/questions/52213375/python-click-exception-handling-under-setuptools
    """
    class Cls(cls):
        """Override Click Class (can be either Command or Group) with custom invoke and context handlers

        Inside the invoke/make_context the original CLI command is called and a custom error can be added when the
        CLI command raises an error
        """

        _original_args = None

        def make_context(self, info_name, args, parent=None, **extra):
            """Add custom exception handler to the click CLI context."""
            # grab the original command line arguments
            self._original_args = ' '.join(args)
            try:
                return super(Cls, self).make_context(info_name, args, parent=parent, **extra)
            except Exception as exc:
                # call the Exception handler if error code is non-zero
                if isinstance(exc, click.exceptions.Exit):
                    if not exc.exit_code == 0:
                        handler(self, info_name, exc)
                else:
                    handler(self, info_name, exc)
                # let the user see the original error
                raise

        def invoke(self, ctx):
            """Add custom exception handler to the click CLI invoke."""
            try:
                return super(Cls, self).invoke(ctx)
            except Exception as exc:
                # call the Exception handler if error code is non-zero
                if isinstance(exc, click.exceptions.Exit):
                    if not exc.exit_code == 0:
                        handler(self, ctx.info_name, exc)
                else:
                    handler(self, ctx.info_name, exc)
                # let the user see the original error
                raise

    return Cls


def report_message_to_sns(subject, message, aws_sns_topic,
                          profile_name=None, region_name=None):
    """Push exceptions from click to SNS topic, used as handler for click applications

    Parameters
    ----------
    subject : str
        Mail subject when sending notifications
    message : str
        Mail message when sending notifications
    aws_sns_topic : str
        arn of the AWS SNS topic
    profile_name : aws profile (optional)
        AWS profile
    region_name : aws region (optional)
        AWS region
    """
    session = boto3.Session(profile_name=profile_name, region_name=region_name)
    sns_client = session.client('sns')
    click.echo(message)
    sns_client.publish(TopicArn=aws_sns_topic,
                       Message=message,
                       Subject=subject)
    click.echo("Sent error message to SNS topic.")


def report_click_exception_to_sns(cmd, info_name, exc, aws_sns_topic, subject, profile_name=None, region_name=None):
    """Push exceptions from click to SNS topic, used as handler for click applications

    Parameters
    ----------
    cmd, info_name, exc : Click handler arguments
    aws_sns_topic : str
        arn of the AWS SNS topic
    subject : str
        Mail subject when sending notifications,
    profile_name : aws profile (optional)
        AWS profile
    region_name : aws region (optional)
        AWS region
    """
    sns_message = f"CLI routine '{info_name} {cmd._original_args}' failed raising error: '{type(exc)}: {exc}'."  # noqa
    report_message_to_sns(subject, sns_message, aws_sns_topic,
                          profile_name=profile_name, region_name=region_name)

