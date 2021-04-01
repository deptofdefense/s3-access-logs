# S3 Access Logs

## Description

This project take AWS S3 access logs and transforms them to parquet format. This can be indexed by AWS Glue
and searched using AWS Athena.

## Setup

```sh
make venv
source .venv/bin/activate
make requirements editable
```

## Usage

```shell
make export
```

## Contributing

We'd love to have your contributions!  Please see [CONTRIBUTING.md](CONTRIBUTING.md) for more info.

## Security

Please see [SECURITY.md](SECURITY.md) for more info.

## Authors

See [AUTHORS](AUTHORS.md) for a list of contributors.  When making a contribution, please add yourself to the list if you like.

## Contact

Please direct any further questions about contributing to <code@dds.mil>.

## Licensing

This project constitutes a work of the United States Government and is not subject to domestic copyright protection under 17 USC § 105.  However, because the project utilizes code licensed from contributors and other third parties, it therefore is licensed under the MIT License.  See LICENSE file for more information.
