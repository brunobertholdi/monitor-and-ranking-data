"""
[DESCRIPTION]
This module contains a wrapper class for saving flight data to a SQLite database.

[CHANGELOG] - Version - Author - Date - Changes
v0.0.1 - Bruno Bertholdi - 2025-04-19 - Initializes database module.
v0.0.2 - Bruno Bertholdi - 2025-04-19 - Enhances insertion logic, adds indices, includes example usage.
v0.0.3 - Bruno Bertholdi - 2025-04-19 - Adds cycle_timestamp column for grouping snapshots.
v0.0.4 - Bruno Bertholdi - 2025-04-19 - Adds flight_changes table and insertion logic.
"""
# --- Imports --- #
import os
import sqlite3
from datetime import datetime
import logfire
from typing import Optional, Dict, Any

# --- Constants --- #
DATABASE_DIR = 'data'
DATABASE_NAME = 'flights_monitor_final.db'
DATABASE_PATH = os.path.join(DATABASE_DIR, DATABASE_NAME)
LOGFIRE_TOKEN = os.getenv('LOGFIRE_TOKEN')

# --- Ensure data dir. exists --- #
os.makedirs(DATABASE_DIR, exist_ok=True)

# --- Database wrapper --- #
def get_db_connection():
    """
    Establishes and returns a connection to the SQLite database.
    """
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        logfire.debug(f'Successfully connected to SQLite database: {DATABASE_PATH}')
        return conn
    except sqlite3.Error as e:
        logfire.error(f'Error connecting to SQLite database: {e}', exc_info=True)
        return None


def create_table(conn: sqlite3.Connection):
    """Creates the flight_snapshots table if it doesn't exist."""
    try:
        cursor = conn.cursor()
        # Added UNIQUE constraint for snapshot_id for clarity, although AUTOINCREMENT implies it
        # Added indices directly in CREATE TABLE for SQLite compatibility
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS flight_snapshots (
                snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                unique_flight_id TEXT NOT NULL,
                cycle_timestamp TEXT NOT NULL, -- ISO format timestamp for the monitoring cycle
                workspace_timestamp TEXT NOT NULL, -- ISO format timestamp
                flight_number TEXT,
                airline_iata TEXT,
                airline_name TEXT,
                scheduled_departure_utc TEXT, -- ISO format timestamp
                estimated_departure_utc TEXT, -- ISO format timestamp
                departure_terminal TEXT,
                departure_gate TEXT,
                status TEXT,
                destination_iata TEXT,
                destination_name TEXT,
                codeshare_status TEXT,
                is_operator INTEGER, -- Store boolean as 0 or 1
                aircraft_model TEXT,
                aircraft_reg TEXT
            );
        """)
        # Create indices separately for clarity and potentially better performance management
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_unique_flight_id ON flight_snapshots (unique_flight_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_workspace_timestamp ON flight_snapshots (workspace_timestamp);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cycle_timestamp ON flight_snapshots (cycle_timestamp);")

        # --- Create flight_changes table --- #
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS flight_changes (
                change_id INTEGER PRIMARY KEY AUTOINCREMENT,
                unique_flight_id TEXT NOT NULL,
                change_detected_cycle_timestamp TEXT NOT NULL, -- Cycle timestamp when change was noticed
                previous_cycle_timestamp TEXT NOT NULL,      -- Cycle timestamp of the snapshot *before* the change
                attribute_changed TEXT NOT NULL,             -- 'scheduled_departure_utc', 'estimated_departure_utc', 'departure_gate'
                previous_value TEXT,                         -- Value before the change
                new_value TEXT,                              -- Value after the change
                change_logged_at TEXT NOT NULL               -- Timestamp when this record was created
            );
        """)
        # Add indices for flight_changes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_changes_flight_id ON flight_changes (unique_flight_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_changes_detected_ts ON flight_changes (change_detected_cycle_timestamp);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_changes_attribute ON flight_changes (attribute_changed);")

        conn.commit()
        logfire.info("Tables ('flight_snapshots', 'flight_changes') and indices checked/created successfully.")
    except sqlite3.Error as e:
        logfire.error(f"Error creating tables or indices: {e}", exc_info=True)


def insert_snapshot(conn: sqlite3.Connection, snapshot_data: Dict[str, Any], cycle_timestamp: str):
    """Inserts a single flight snapshot record into the database."""
    prepared_data = snapshot_data.copy() # Avoid modifying the original dict
    prepared_data['cycle_timestamp'] = cycle_timestamp # Add cycle timestamp

    # Convert datetime objects to ISO format strings if they exist
    for key in ['workspace_timestamp', 'scheduled_departure_utc', 'estimated_departure_utc']:
        if isinstance(prepared_data.get(key), datetime):
            prepared_data[key] = prepared_data[key].isoformat()
        elif prepared_data.get(key) is None:
             prepared_data[key] = None # Ensure None is passed explicitly

    # Convert boolean to integer
    if 'is_operator' in prepared_data:
        prepared_data['is_operator'] = 1 if prepared_data['is_operator'] else 0

    # Ensure all columns defined in the table exist in the data, adding None if missing
    # This makes the insertion more robust if the input dict sometimes lacks optional keys
    all_columns = [
        'unique_flight_id', 'cycle_timestamp', 'workspace_timestamp', 'flight_number', 'airline_iata',
        'airline_name', 'scheduled_departure_utc', 'estimated_departure_utc',
        'departure_terminal', 'departure_gate', 'status', 'destination_iata',
        'destination_name', 'codeshare_status', 'is_operator', 'aircraft_model',
        'aircraft_reg'
    ]
    final_data = {col: prepared_data.get(col) for col in all_columns}

    columns = ', '.join(final_data.keys())
    placeholders = ', '.join('?' * len(final_data))
    sql = f"INSERT INTO flight_snapshots ({columns}) VALUES ({placeholders})"

    try:
        cursor = conn.cursor()
        cursor.execute(sql, list(final_data.values()))
        conn.commit()
        logfire.debug(f"Inserted snapshot for flight: {final_data.get('unique_flight_id')}")
        return cursor.lastrowid
    except sqlite3.Error as e:
        # Include data keys in error for easier debugging
        logfire.error(f"Error inserting snapshot data (keys: {list(final_data.keys())}): {e}", exc_info=True)
        return None


def insert_change_record(
    conn: sqlite3.Connection,
    unique_flight_id: str,
    change_detected_cycle_timestamp: str,
    previous_cycle_timestamp: str,
    attribute_changed: str,
    previous_value: Any,
    new_value: Any
):
    """Inserts a record into the flight_changes table."""
    change_logged_at = datetime.now().isoformat()
    sql = """
        INSERT INTO flight_changes (
            unique_flight_id, change_detected_cycle_timestamp, previous_cycle_timestamp,
            attribute_changed, previous_value, new_value, change_logged_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (
            unique_flight_id, change_detected_cycle_timestamp, previous_cycle_timestamp,
            attribute_changed, str(previous_value), str(new_value), change_logged_at
        ))
        # Removed conn.commit() here to avoid premature commits - let caller manage transaction
        logfire.debug(f"Change record inserted for {unique_flight_id}, attribute: {attribute_changed}")
    except sqlite3.Error as e:
        logfire.error(f"Failed to insert change record for {unique_flight_id}: {e}", exc_info=True)
        conn.rollback()


def get_latest_snapshot(conn: sqlite3.Connection, unique_flight_id: str) -> Optional[sqlite3.Row]:
    """Retrieves the most recent snapshot for a given unique_flight_id."""
    sql = """
        SELECT *
        FROM flight_snapshots
        WHERE unique_flight_id = ?
        ORDER BY workspace_timestamp DESC
        LIMIT 1
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (unique_flight_id,))
        result = cursor.fetchone()
        if result:
            logfire.debug(f"Retrieved latest snapshot for flight: {unique_flight_id}")
        else:
            logfire.debug(f"No previous snapshot found for flight: {unique_flight_id}")
        return result
    except sqlite3.Error as e:
        logfire.error(f"Error retrieving latest snapshot for {unique_flight_id}: {e}", exc_info=True)
        return None

# --- Example Usage (for testing) --- #
if __name__ == "__main__":

    logfire.configure(token=LOGFIRE_TOKEN)

    logfire.info("Testing database module...")
    connection = get_db_connection()
    if connection:
        create_table(connection)

        # Example data (mimicking structure from request.py's processing)
        ts_now = datetime.now()
        test_data_1 = {
            'unique_flight_id': 'AA-123-20250420-JFK',
            'workspace_timestamp': ts_now,
            'flight_number': '123',
            'airline_iata': 'AA',
            'airline_name': 'American Airlines',
            'scheduled_departure_utc': ts_now, # Using now for simplicity
            'estimated_departure_utc': ts_now,
            'departure_terminal': 'D',
            'departure_gate': 'D20',
            'status': 'Scheduled',
            'destination_iata': 'JFK',
            'destination_name': 'New York JFK',
            'codeshare_status': 'IsOperator',
            'is_operator': True,
            'aircraft_model': 'B738',
            'aircraft_reg': 'N123AA'
        }
        test_cycle_ts = datetime.now().isoformat() # Example cycle timestamp
        insert_snapshot(connection, test_data_1, test_cycle_ts)

        # Test retrieval
        latest = get_latest_snapshot(connection, 'AA-123-20250420-JFK')
        if latest:
            logfire.info(f"Latest snapshot retrieved: {dict(latest)}")
            # Verify data types and values
            assert latest['unique_flight_id'] == 'AA-123-20250420-JFK'
            assert latest['is_operator'] == 1 # Check boolean conversion
            assert isinstance(latest['workspace_timestamp'], str) # Check timestamp conversion
            logfire.info("Basic data verification passed.")
        else:
            logfire.warning("Could not retrieve latest snapshot for testing.")

        # --- Test flight_changes insertion --- #
        insert_change_record(
            conn=connection,
            unique_flight_id='AA-123-20250420-JFK',
            change_detected_cycle_timestamp=datetime.now().isoformat(),
            previous_cycle_timestamp=test_cycle_ts, # From snapshot insert
            attribute_changed='departure_gate',
            previous_value='D20',
            new_value='D22'
        )
        logfire.info("Test change record inserted.")

        # Simple query to check changes table (replace with more specific if needed)
        cur = connection.cursor()
        cur.execute("SELECT * FROM flight_changes LIMIT 5")
        changes = cur.fetchall()
        logfire.info(f"Sample change records: {changes}")

        connection.close()
        logfire.info("Database connection closed.")
    else:
        logfire.error("Failed to establish database connection for testing.")