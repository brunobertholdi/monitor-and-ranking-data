"""
[DESCRIPTION]
This module contains a wrapper class for making requests to aerodatabox API and handling its responses.

[CHANGELOG] - Version - Author - Date - Changes
v0.0.1 - Bruno Bertholdi - 2025-04-19 - Initializes GetData class (a wrapper to data requesting logic).
v0.0.2 - Bruno Bertholdi - 2025-04-19 - Adds logging and error handling. Logs are sent to logfire: https://logfire-us.pydantic.dev/bertbert/monitor-and-rank-data
v0.0.3 - Bruno Bertholdi - 2025-04-19 - Integrates Pydantic models for data validation.
v0.0.4 - Bruno Bertholdi - 2025-04-19 - Adds method to save gathered data to .csv file.
"""

# --- Imports --- #
import os
import json
import http.client
from dotenv import load_dotenv
import logfire
import pandas as pd
from datetime import datetime, timezone
from pydantic import ValidationError

from models import FlightDataResponse

# --- Environment variables --- #
load_dotenv()
API_KEY = os.getenv('API_KEY')
API_HOST = os.getenv('API_HOST')
LOGFIRE_TOKEN = os.getenv('LOGFIRE_TOKEN')

# --- Logfire setup --- #
logfire.configure(token=LOGFIRE_TOKEN)

# --- Request wrapper --- #
class GetData():
    """
    This class is used to make requests to aerodatabox API.
    Main reason is to wrap and organize requesting logic.
    """
    def __init__(self):
        self.api_key = API_KEY
        self.api_host = API_HOST
        # --- Initialize connection --- #
        try:
            self.CONN = http.client.HTTPSConnection(self.api_host)
        except Exception as e:
            logfire.error(f"Failed to initialize HTTPS connection to {self.api_host}: {e}")
            self.CONN = None # Mark connection as failed

        self.headers = {
            'X-RapidAPI-Key': self.api_key,
            'X-RapidAPI-Host': self.api_host
        }
        logfire.info(f"GetData initialized with host: {self.api_host}")

    def make_request(self):
        conn = http.client.HTTPSConnection(self.api_host)
        # Reverted endpoint and added parameters based on previous version/common practice
        endpoint = "/flights/airports/iata/DFW"
        params = "?direction=Departure&durationMinutes=720&withCodeshared=true&withCargo=false&withPrivate=false" # Simplified params
        full_url = endpoint + params
        logfire.info(f"Making request to endpoint: {self.api_host}{full_url}")
        try:
            conn.request("GET", full_url, headers=self.headers)
            res = conn.getresponse()
            data = res.read()
            logfire.info(f"Request completed, status code: {res.status}")

            # Check status code before proceeding
            if res.status == 200:
                # Decode and load JSON data
                json_data = data.decode("utf-8")
                # logfire.debug(f"Received raw data: {json_data[:500]}...") # Optional: Log snippet of raw data

                # Validate data using Pydantic models
                try:
                    validated_data = FlightDataResponse.model_validate_json(json_data)
                    logfire.info("API response successfully validated against Pydantic models.")
                    # Pass the validated Pydantic object to save_data
                    self.save_data(validated_data)
                except ValidationError as e:
                    logfire.error(f"Data validation failed: {e}", payload={'raw_json': json_data[:1000]}) # Log part of raw data on validation error
                    return None # Or raise an exception
            else:
                logfire.error(f"API request failed with status {res.status}. Response: {data.decode('utf-8', errors='ignore')[:500]}")
                return None # Indicate failure

        except Exception as e:
            logfire.error(f"An error occurred during the request: {e}", exc_info=True)
        finally:
            conn.close()
            logfire.info("Connection closed.")

    def save_data(self, flight_data: FlightDataResponse):
        """Processes and saves flight data into a structured format."""
        logfire.info("Processing and saving flight data.")
        records = []
        workspace_timestamp = datetime.now(timezone.utc)

        for flight in flight_data.departures:
            # Using attribute access on Pydantic models
            movement = flight.movement
            airline = flight.airline
            aircraft = flight.aircraft
            status = flight.status # status might not exist directly under flight, check model
            codeshare_status = flight.codeshareStatus
            is_operator = True if flight.codeshareStatus == 'IsOperator' else False # Just a boolean flag, might be useful for filtering in the future.
            # potentially missing nested attributes #
            dest_iata = movement.airport.iata if movement and movement.airport else None
            dest_name = movement.airport.name if movement and movement.airport else None
            scheduled_utc = movement.scheduledTime.utc if movement and movement.scheduledTime else None
            estimated_utc = movement.revisedTime.utc if movement and movement.revisedTime else scheduled_utc # Use scheduled if revised is missing
            terminal = movement.terminal if movement else None
            gate = movement.gate if movement else None # Assuming gate was added to MovementInfo
            airline_iata = airline.iata if airline else None
            airline_name = airline.name if airline else None
            ac_model = aircraft.model if aircraft else None
            ac_reg = aircraft.reg if aircraft else None

            record = {
                'workspace_timestamp': workspace_timestamp,
                'flight_number': flight.number,
                'airline_iata': airline_iata,
                'airline_name': airline_name,
                'scheduled_departure_utc': scheduled_utc,
                'estimated_departure_utc': estimated_utc,
                'departure_terminal': terminal,
                'departure_gate': gate, # Added gate. TODO: check docs to confirm if gate is actually available. hasn't been delivered in response during development.
                'status': status,
                'destination_iata': dest_iata,
                'destination_name': dest_name,
                'codeshare_status': codeshare_status,
                'is_operator': is_operator,
                'aircraft_model': ac_model,
                'aircraft_reg': ac_reg,
                'unique_flight_id': f"{airline_iata or 'UNK'}-{flight.number or '000'}-"
                                  f"{scheduled_utc.strftime('%Y%m%d') if scheduled_utc else 'NODATE'}-"
                                  f"{dest_iata or 'UNK'}"
            }
            records.append(record)

        if records:
            df = pd.DataFrame(records)
            data_dir = 'data'
            os.makedirs(data_dir, exist_ok=True)
            # Save to CSV for now
            output_path = os.path.join(data_dir, 'flight_data.csv')
            df.to_csv(output_path, index=False)
            logfire.info(f"Data saved successfully to {output_path}. Total records: {len(df)}")
        else:
            logfire.warning("No flight records to save.")

# --- Test Execution --- #
if __name__ == "__main__":
    logfire.info("Starting test execution of GetData.")
    getter = GetData()
    getter.make_request() # save_data is now called within make_request
    logfire.info("Test execution finished.")
