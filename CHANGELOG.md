# Changelog

## Version 0.3.0

- Add getting started examples to README (#55, #65)
- Simplify regex to look for valid file names (#60)
- Use HDF5, PVOL, VP, VPTS, VPTS CSV, ODIM consistently in documentation (#58)
- Make use of production, uat or dummy S3 bucket where appropriate (#59)
- Update contributors and copyright holder (to INBO)

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

- Integrate functions from [odimh5](https://pypi.org/project/odimh5) to read ODIM HDF5 files
- Support for converting ODIM HDF5 files to the VPTS CSV data standard
- S3 data storage integration
- CLI endpoint for the transfer of ODIM HDF5 files from Baltrad to the Aloft S3 bucket
- CLI endpoint for the conversion from ODIM HDF5 files to daily/monthly aggregates as VPTS CSV format
- Setup CI with Github Actions
