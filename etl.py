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
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE TABLE IF NOT EXISTS roads (
    road_id VARCHAR PRIMARY KEY,
    road_name TEXT NOT NULL,
    road_type TEXT NOT NULL,
    geom geometry(LineString, 4326) 
);
"""


def create_table_if_not_exists() -> None:
    """
    Ensures that the 'roads' table exists in the database by creating it if it does not already exist.

    This function connects to a PostgreSQL database using the provided connection parameters,
    executes the SQL query defined in `CREATE_ROADS_TABLE_QUERY` to create the 'roads' table,
    and logs the operation's success or failure.

    Input:
        - Relies on the global `db_params` dictionary for database connection parameters:
            - dbname: Name of the database.
            - user: Username for authentication.
            - password: Password for authentication.
            - host: Host address of the database.
            - port: Port number for the database connection.
        - Relies on the global `CREATE_ROADS_TABLE_QUERY` for the SQL query to create the table.
        - Relies on the global `logger` for logging.

    Output:
        - Logs a success message if the table creation is ensured.
        - Logs an exception message if a database error occurs.

    Raises:
        - psycopg2.Error: If there is an error during the database operation.
    """
    try:
        conn = psycopg2.connect(
            dbname=db_params["dbname"],
            user=db_params["user"],
            password=db_params["password"],
            host=db_params["host"],
            port=db_params["port"],
        )
        cur = conn.cursor()

        # Now create the roads table
        cur.execute(CREATE_ROADS_TABLE_QUERY)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Ensured 'roads' table exists in the database.")
    except psycopg2.Error as e:
        logger.exception(f"Database error during table creation: {e}")


def extract() -> dict:
    """
    Fetches data from the Overpass API using a predefined query.

    This function uses the Overpass API to execute a query and retrieve data
    in JSON format. If an error occurs during the API call, it logs the exception
    and returns an empty dictionary.

    Returns:
        dict: The response data from the Overpass API in JSON format if successful,
              otherwise an empty dictionary.
    """
    api = overpass.API()
    try:
        response = api.get(overpass_query, responseformat="json")
        logger.info("Successfully fetched data using Overpass API.")
        return response
    except Exception as e:
        logger.exception(f"Error fetching data from Overpass API: {e}")
        return {}


def transform(data: dict) -> list:
    """
    Transforms raw road data into a structured format.
    Args:
        data (dict): A dictionary containing road data. Expected to have a key "elements",
                     which is a list of dictionaries. Each dictionary represents a road element
                     with the following structure:
                     - "id" (int or str): The unique identifier of the road.
                     - "tags" (dict): A dictionary of metadata about the road, including:
                         - "name" (str): The name of the road (optional).
                         - "highway" (str): The type of road (optional).
                     - "geometry" (list): A list of dictionaries representing the geometry of the road,
                                         where each dictionary contains:
                                         - "lon" (float): Longitude of a point.
                                         - "lat" (float): Latitude of a point.
    Returns:
        list: A list of tuples, where each tuple represents a transformed road with the following structure:
              - road_id (str): The unique identifier of the road as a string.
              - road_name (str): The name of the road, or a fallback name if not provided.
              - road_type (str): The type of road, or "unknown" if not provided.
              - linestring_wkt (str or None): The geometry of the road in WKT LineString format,
                                              or None if geometry is not provided.
    Logs:
        Logs the number of roads transformed using the logger.
    Example:
        Input:
        {
            "elements": [
                {
                    "id": 1,
                    "tags": {"name": "Main Street", "highway": "residential"},
                    "geometry": [{"lon": 10.0, "lat": 20.0}, {"lon": 11.0, "lat": 21.0}]
                }
            ]
        }
        Output:
        [
            ("1", "Main Street", "residential", "LINESTRING(10.0 20.0, 11.0 21.0)")
        ]
    """
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


def load(transformed_data: list) -> None:
    """
    Loads transformed data into the database.

    This function establishes a connection to the database using the provided
    connection parameters, executes a batch insert query to load the transformed
    data, and commits the transaction. If an error occurs during the process,
    it logs the error.

    Args:
        transformed_data (list[tuple]): A list of tuples containing the transformed
            data to be inserted into the database.

    Returns:
        None

    Raises:
        psycopg2.Error: If there is an error during the database operation.

    Logs:
        - Info: Logs a success message when data is loaded successfully.
        - Error: Logs an error message if a database error occurs.
    """
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
