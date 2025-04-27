import os
import logging
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import overpass

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Database connection parameters
db_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5432),
}

# Overpass API query to fetch roads for a specific area (e.g., Helsinki)
city_name = os.getenv("CITY_NAME", "Helsinki")
overpass_query = f"""
area["name"="{city_name}"]->.a;
(
  way(area.a)[highway];
);
out geom;
"""

# SQL Query
INSERT_ROAD_QUERY = """
INSERT INTO roads (road_id, road_name, road_type, geom)
VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326))
ON CONFLICT (road_id) DO NOTHING
"""

CREATE_ROADS_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS roads (
    road_id VARCHAR PRIMARY KEY,
    road_name TEXT NOT NULL,
    road_type TEXT NOT NULL,
    geom geometry(LineString, 4326) 
);
"""


def create_table_if_not_exists():
    try:
        conn = psycopg2.connect(
            dbname=db_params["dbname"],
            user=db_params["user"],
            password=db_params["password"],
            host=db_params["host"],
            port=db_params["port"],
        )
        cur = conn.cursor()

        # First ensure PostGIS extension is enabled
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        conn.commit()

        # Now create the roads table
        cur.execute(CREATE_ROADS_TABLE_QUERY)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Ensured 'roads' table exists in the database.")
    except psycopg2.Error as e:
        logger.exception(f"Database error during table creation: {e}")


def extract():
    api = overpass.API()
    try:
        response = api.get(overpass_query, responseformat="json")
        logger.info("Successfully fetched data using Overpass API.")
        return response
    except Exception as e:
        logger.exception(f"Error fetching data from Overpass API: {e}")
        return {}


def transform(data):
    elements = data.get("elements", [])
    transformed_data = []

    for element in elements:
        road_id = str(element.get("id"))
        tags = element.get("tags", {})
        road_name = tags.get("name", f"road_{road_id}")  # fallback if no name
        road_type = tags.get("highway", "unknown")
        
        geometry = element.get("geometry")
        if geometry:
            # Build WKT LineString
            coords = ", ".join(f"{point['lon']} {point['lat']}" for point in geometry)
            linestring_wkt = f"LINESTRING({coords})"
        else:
            linestring_wkt = None

        transformed_data.append((road_id, road_name, road_type, linestring_wkt))

    logger.info(f"Transformed {len(transformed_data)} roads.")
    return transformed_data


def load(transformed_data):
    try:
        conn = psycopg2.connect(
            dbname=db_params["dbname"],
            user=db_params["user"],
            password=db_params["password"],
            host=db_params["host"],
            port=db_params["port"],
        )
        cur = conn.cursor()
        execute_batch(cur, INSERT_ROAD_QUERY, transformed_data)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Data loaded successfully into the database.")
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")


def main():
    # First ensure the table exists
    create_table_if_not_exists()

    raw_data = extract()
    if raw_data:
        main_roads = transform(raw_data)
        if main_roads:
            load(main_roads)
    else:
        logger.warning("No data to process.")


if __name__ == "__main__":
    main()
