import json
import os

import requests
from datetime import datetime, timezone, timedelta
from pymongo import errors
from app.core.database import db_manager
from app.core.config import settings

JAEGER_BASE_URL = settings.JAEGER_URL


def fetch_services():
    """
    Fetch all available services from Jaeger.
    Returns:
        List of service names.
    """
    url = f"{JAEGER_BASE_URL}/services"
    try:
        response = requests.get(url)
        response.raise_for_status()
        services = response.json().get("data", [])
        return sorted([service for service in services if service != "jaeger-all-in-one"])
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch services: {e}")
        return []


def fetch_traces(service_name, start_us, end_us, limit=100):
    """
    Fetch traces for a specific service within a time range.
    """
    url = f"{JAEGER_BASE_URL}/traces"
    params = {
        "service": service_name,
        "start": int(start_us),  # Ensure integer timestamps
        "end": int(end_us),      # Ensure integer timestamps
        "limit": limit,
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json().get("data", [])
        if not isinstance(data, list):
            print(f"Unexpected API response for {service_name}: {data}")
            return []
        return data
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch traces for service '{service_name}': {e}")
        return []


def get_traces_from_files_within_timerange(start_us, end_us):
    """
    Retrieve traces from the MongoDB traces collection within a given time range and with pagination.
    Args:
        start_us (int): Start time in microseconds.
        end_us (int): End time in microseconds.
    Returns:
        list: List of trace documents within the specified time range.
    """
    print(f"Retrieving traces from {start_us} to {end_us}")
    try:
        trace_files = [f for f in os.listdir(settings.TRACES_DIR) if f.endswith('.json') and f != 'offset.json']

        # Filter files within the specified time range based on their names
        filtered_files = [
            f for f in trace_files
            if int(f.split('_')[0]) >= int(start_us) and int(f.split('_')[1].split('.')[0]) <= int(end_us)
        ]

        # Initialize an empty list to store all traces
        all_traces = []

        for trace_file in filtered_files:
            with open(os.path.join(settings.TRACES_DIR, trace_file), 'r') as file:
                traces = json.load(file)
                all_traces.extend(traces)

        # Filter traces within the specified time range
        filtered_traces = [
            trace for trace in all_traces
            if any(int(start_us) <= span["startTime"] <= int(end_us) for span in trace["spans"])
        ]
        print(f"Retrieved {len(filtered_traces)} traces from the JSON files.")

        return filtered_traces
    except Exception as e:
        print(f"Error fetching traces: {e}")
        return None