# SAP HANA Metadata Extraction Connector

This application extracts metadata from SAP HANA databases including:

- Databases
- Schemas
- Tables
- Views
- Columns
- Stored Procedures
- Calculation Views (with lineage)

## Features

- Extracts standard database objects (tables, views, columns, etc.)
- Extracts SAP HANA specific objects like Calculation Views
- Generates lineage information from calculation views
- Supports SAP HANA 2.0 and above

## Prerequisites

- Python 3.11 or higher
- SAP HANA ODBC/JDBC drivers
- Access to a SAP HANA database

## Installation

1. Clone the repository:

```bash
git clone https://github.com/your-org/sap-hana-connector.git
cd sap-hana-connector
```

2. Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -e .
```

## Configuration

You can configure the application using environment variables or through the API:

```bash
# Required
export DATABASE_HOST=your-sap-hana-host
export DATABASE_PORT=30015
export DATABASE_USERNAME=your-username
export DATABASE_PASSWORD=your-password

# Optional
export DATABASE_ENCRYPT=true
export DATABASE_SSL_VALIDATE_CERT=false
```

## Running the Application

Start the application with:

```bash
python main.py
```

By default, the server starts on http://localhost:8000.

## API Endpoints

The following API endpoints are available:

- `/workflows/v1/auth` - Test authentication
- `/workflows/v1/check` - Run preflight checks
- `/workflows/v1/start` - Start extraction workflow

## Development

For development, install additional dependencies:

```bash
pip install -e ".[dev,test]"
```

Run tests with:

```bash
pytest
```

## License

Apache 2.0 