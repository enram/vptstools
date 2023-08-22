# Changelog

## Version 0.2.2

- Hotfix for Docker deployment continued
- Handle wrong function import

## Version 0.2.1

- Hotfix for Docker deployment

## Version 0.2.0

- Add support for uppercase radarcode in file names (#53)
- Improve logs and alert handling to AWS cloudwatch/SNS and make sure routine does not stop on single file failure (#54)
- Improve documentation (#55)

## Version 0.1.0

- Integrate functions from [odimh5](https://pypi.org/project/odimh5) to read odim5 files
- Support for converting ODIm hdf5 files to the vpts-csv data standard
- s3 data storage integration
- CLI endpoint for the transfer of ODIM hdf5 files from Baltrad to the aloft S3 bucket
- CLI endpoint for the conversion from ODIM hdf5 files to daily/monthly aggregates as vpts-csv format
- Setup CI with Github Actions
