# Store Monitoring System

A FastAPI-based store monitoring application that provides data upload capabilities, report generation, and download functionality with PostgreSQL database integration.

## Features

- **Data Upload**: Upload store monitoring data via REST API
- **Report Generation**: Trigger asynchronous report generation
- **Report Retrieval**: Get report status and metadata
- **Report Download**: Download generated reports
- **Database Management**: PostgreSQL with Alembic migrations

## Tech Stack

- **Backend**: FastAPI
- **Database**: PostgreSQL
- **Migrations**: Alembic
- **Python**: 3.8+

## Installation

### Prerequisites

- Python 3.8 or higher
- PostgreSQL
- pip or poetry

### Setup

1. **Clone the repository**
```bash
git clone https://github.com/CaptainRedCodes/Rahul_28Aug2025
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Environment Configuration**
Create a `.env` file in the root directory:
```env
DB_USER=
DB_PASSWORD=
DB_HOST=localhost
DB_PORT=5432
DB_NAME=store_monitoring  
```

5. **Database Setup**
```bash
# Create database (if not exists)
createdb store_monitoring_db

# Run migrations
alembic upgrade head

**Make sure you have created a database of DB_NAME in PostgreSQL**
```

## Running the Application

### Development
```bash
uvicorn app.main:app --reload
```

The API will be available at `http://127.0.0.1:8000/`

## API Documentation

Once the application is running, visit:
- **Interactive API docs**: `http://127.0.0.1:8000/docs#/`

## API Endpoints

### 1. Upload Data
```http
POST /upload-data/
```
Upload store monitoring data to the system.

**Request Body:**
- Multipart form data with file upload
- Or JSON payload with store data

**Response:**
```json
{
  "message": "Data uploaded successfully",
  "timestamp": "2025-08-28T10:30:00Z"
}
```

### 2. Trigger Report Generation
```http
POST /trigger_report
```
Initiate asynchronous report generation process.

**Request Body:**
```json
{
"report_id": "UUID"
}
```


### 3. Get Report Status
```http
GET /get_report/{report_id}
```
Retrieve report generation status and metadata.

**Parameters:**
- `report_id`: UUID of the report

**Response:**
```json
{
  "report_id": "UUID",
  "status": "string",
  "file_path": "string"
}
```

### 4. Download Report
```http
GET /download_report/{report_id}
```
Download the generated report file.

**Parameters:**
- `report_id`: UUID of the completed report

**Response:**
- File download (CSV based on report type)
- Content-Type: application/octet-stream

## Database Schema

### Migration Commands

```bash
# Create a new migration
alembic revision --autogenerate -m "Description of changes"

# Apply migrations
alembic upgrade head

```

## Project Structure

```
store-monitoring/
├── alembic/                 # Database migrations
│   ├── versions/           # Migration files
│   └── env.py             # Alembic configuration
├── app/
│   ├── utils/             # Data Upload utils
│   ├── db/                # Database models and connection
│   ├── models/            # SQLAlchemy models
│   ├── schemas/           # Pydantic schemas
│   ├──  service/          # Business logic
    ├──  settings.py       # db settings
    └── main.py            # FastAPI application entry point
├── data/                  # csv data
├── reports/               # final reports
├── requirements.txt       # Python dependencies
├── alembic.ini           # Alembic configuration

└── README.md             # This file
```
## Ideas of Improving the project

1.Caching frequently accesed report
2.Generating reports based on only required restaurants
3.Using ETL pipelines for data ingestion and validation
4.Make better data ingestion for Bulk operation
5.Use ASynchronous methods for report generation
6.Making sure of not overloading API requests by adding some rate limiting mechanism

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes 
4. Push to the branch
5. Open a Pull Request


## This Project is a Take home interview Assignment for LOOP. Contact svmrahul15@gmail.com for more info.