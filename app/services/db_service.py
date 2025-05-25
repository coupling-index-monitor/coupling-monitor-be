from datetime import datetime
from pymongo.collection import Collection
from app.core.database import db_manager

def get_metrics_within_time_range(start_time, end_time):
    """
    Fetch data from a MongoDB collection within the given time range.
    """

    if isinstance(start_time, str):
        start_time = datetime.fromisoformat(start_time).replace(microsecond=0)
    if isinstance(end_time, str):
        end_time = datetime.fromisoformat(end_time).replace(microsecond=0)

    print(f"Fetching metrics between {start_time} and {end_time}...")

    query = {
        "$or": [
            {"start_time": {"$lte": start_time}},
            {"end_time": {"$gte": end_time}},
        ]
    }
    metric_col: Collection = db_manager.get_metrics_collection()
    
    metrics = list(metric_col.find(query, {"_id": 0}))
    print(f"Retrieved {len(metrics)} traces.")

    return metrics