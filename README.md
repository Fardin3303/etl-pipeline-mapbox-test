# ETL Pipeline with Docker and PostgreSQL

This project is a sample ETL (Extract, Transform, Load) pipeline built in Python.  
It fetches road data from an API, transforms it, and loads it into a PostgreSQL database.  
Everything is fully containerized using Docker and Docker Compose.

## Features
- Modular Python ETL (Extract, Transform, Load)
- API retries and timeout handling
- Secure environment variables management
- Automatic PostgreSQL table creation
- Dockerized services (Python app + PostgreSQL)
- Full deployment with Docker Compose

## How to Run
```bash
# Build and start services
docker-compose up --build

## Tech Stack
- Python 3.10
- PostgreSQL 14
- Docker & Docker Compose
- psycopg2, requests, python-dotenv

## Notes
- `.env` file is excluded for security reasons.
- API URL and database credentials are configured via environment variables.
