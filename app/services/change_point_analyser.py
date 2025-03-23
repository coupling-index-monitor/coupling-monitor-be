from fastapi.responses import JSONResponse
import numpy as np
import pandas as pd
from app.services.db_service import get_metrics_within_time_range

# Function to Detect Gradual Shifts (CUSUM)
def detect_cusum(df, column, threshold=0.5):
    """Detects gradual changes using CUSUM."""
    df["cusum"] = np.cumsum(df[column] - df[column].mean())
    change_points = df[df["cusum"].abs() > threshold * df["cusum"].std()]
    return change_points


def get_change_points(service_data, column, threshold):
    print("Service Data: {}".format(service_data))
    df = pd.DataFrame(service_data)

    cusum_changes = detect_cusum(df, column, threshold)

    print("\nCUSUM Changes:")
    print(cusum_changes)
    print(cusum_changes[column].values)

    return cusum_changes[column].values

def detect_change_points(start_time: int = 0, end_time: int = 0, metric: str = None, threshold: float = 0.5):
    """
    Endpoint to detect change points in the dependency graph.
    """
    try:
        node_metrics = ["absolute_importance", "absolute_dependence"]
        edge_metrics = ["latency", "frequency", "coexecution"]
        if metric in node_metrics:
            type = "nodes"
        elif metric in edge_metrics:
            type = "edges"
        else:
            return JSONResponse(status_code=400, content= {"status": "error", "message": "Invalid metric type provided."})
        
        metrics = get_metrics_within_time_range(start_time, end_time, type)
        change_points = get_change_points(metrics, metric, threshold)

        return {"status": "success", "change_points": change_points}
    except Exception as e:
        print(f"ERROR: Failed to detect change points: {str(e)}")
        return JSONResponse(status_code=500, content= {"status": "error", "message": f"Failed to detect change points: {str(e)}"})