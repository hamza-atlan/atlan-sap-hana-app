# SAP HANA Metadata Extractor

An Application SDK implementation for extracting metadata from SAP HANA databases, including calculation views and lineage.

## Features

- Extracts standard database objects (tables, views, columns, schemas, procedures)
- Extracts SAP HANA calculation views and their columns
- Extracts lineage between calculation views and their source tables/views
- Extracts column-level lineage for calculation views

## Requirements

- Python 3.11+
- SAP HANA database (version 2.0+)
- SAP HANA Python driver (hdbcli)
- SQLAlchemy HANA dialect
- Application SDK

## Setup

1. Install dependencies:

```bash
uv pip install -e .
```

2. Configure environment variables:

```bash
cp .env.example .env
# Edit .env with your configuration
```

3. Run the application:

```bash
python main.py
```

## Development

To set up a development environment:

```bash
uv venv
source .venv/bin/activate
uv pip install -e ".[dev,test]"
```

## License

Apache 2.0
```

</rewritten_file>