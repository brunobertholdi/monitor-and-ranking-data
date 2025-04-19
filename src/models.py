"""
[DESCRIPTION]
This module contains Pydantic models for validating and processing flight data, being captured from aerodatabox API.

[CHANGELOG] - Version - Author - Date - Changes
v0.0.1 - Bruno Bertholdi - 2025-04-19 - Initializes multiple models
"""
# --- Imports --- #
from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime

# --- Nested Models --- #

class AirportInfo(BaseModel):
    icao: Optional[str] = None
    iata: Optional[str] = None
    name: Optional[str] = None
    timeZone: Optional[str] = Field(None, alias='timeZone')

class TimeInfo(BaseModel):
    utc: Optional[datetime] = None
    local: Optional[str] = None

class RunwayTimeInfo(TimeInfo):
    # Inherits utc and local from TimeInfo
    pass

class MovementInfo(BaseModel):
    airport: Optional[AirportInfo] = None
    scheduledTime: Optional[TimeInfo] = Field(None, alias='scheduledTime')
    revisedTime: Optional[TimeInfo] = Field(None, alias='revisedTime') # can be missing. TODO: if missing, assign scheduledTime's value (?)
    runwayTime: Optional[RunwayTimeInfo] = Field(None, alias='runwayTime') # can be missing.
    terminal: Optional[str] = None
    gate: Optional[str] = None # Added based on save_data logic
    runway: Optional[str] = None
    quality: Optional[List[str]] = None

class AircraftInfo(BaseModel):
    reg: Optional[str] = None
    modeS: Optional[str] = Field(None, alias='modeS')
    model: Optional[str] = None

class AirlineInfo(BaseModel):
    name: Optional[str] = None
    iata: Optional[str] = None
    icao: Optional[str] = None

# --- Main Departure Model --- #

class Departure(BaseModel):
    movement: Optional[MovementInfo] = None
    number: Optional[str] = None # Flight number
    callSign: Optional[str] = Field(None, alias='callSign')
    status: Optional[str] = None
    codeshareStatus: Optional[str] = Field(None, alias='codeshareStatus')
    isCargo: Optional[bool] = Field(None, alias='isCargo')
    aircraft: Optional[AircraftInfo] = None
    airline: Optional[AirlineInfo] = None

# --- Top-Level API Response Model --- #

class FlightDataResponse(BaseModel):
    departures: List[Departure] = []
