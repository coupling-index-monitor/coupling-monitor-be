from fastapi import APIRouter
from datetime import datetime, timedelta

from fastapi.responses import JSONResponse
from app.services import (
    generate_graph_with_edge_weights, 
    get_traces_from_files_within_timerange, 
    get_graph_data_as_json,
)
from app.utils import WEIGHT_TYPES, get_gap_time_str

router = APIRouter()

@router.get("/edge-weight")
async def get_weighted_dependency_graph_from_files(weight_type: str = "CO", start_time: int = 0, end_time: int = 0):
    """
    Endpoint to generate and return the weighted dependency graph from the traces of a given time range.
    """
    try:
        weight_type_values = [wt.value for wt in WEIGHT_TYPES]
        if weight_type not in weight_type_values:
            return JSONResponse(status_code=400, content={
                "status": "error", 
                "message": f"Invalid weight_type parameter. Must be one of: {weight_type_values}"
            })
        if start_time != 0 and end_time != 0 and start_time >= end_time:
            return JSONResponse(status_code=400, content={
                "status": "error", 
                "message": "Invalid time range. start_time must be less than end_time."
            })
        if start_time == 0:
            start_time = int((datetime.now() - timedelta(hours=24)).timestamp() * 1_000_000)
        if end_time == 0:
            end_time = int(datetime.now().timestamp() * 1_000_000)
            
        print(f"Generating weighted dependency graph with weight_type={weight_type}, "
              f"start_time={datetime.fromtimestamp(start_time / 1_000_000)}, end_time={datetime.fromtimestamp(end_time / 1_000_000)}")
        
        gap_time = None
        try:
            gap_time = get_gap_time_str(start_time, end_time)
        except ValueError as e:
            return JSONResponse(status_code=400, content={
                "status": "error", 
                "message": f"Invalid time range. {str(e)}"
            })
        
        traces = get_traces_from_files_within_timerange(start_time, end_time)
        if not traces:
            return {"status": "success", "message": "No traces to process."}

        graph_data = generate_graph_with_edge_weights(traces, WEIGHT_TYPES(weight_type).value)

        return JSONResponse(status_code=200, content={
            "status": "success", 
            "message": "Weighted Dependency graph generated successfully.", 
            "weight_type": WEIGHT_TYPES(weight_type).name,
            "gap_time": gap_time,
            "data": graph_data
        })
    except Exception as e:
        print(f"ERROR: Failed to generate weighted graph: {str(e)}")
        return JSONResponse(status_code=500, content={"status": "error", "message": f"Failed to generate graph: {str(e)}"})

@router.get("/")
async def fetch_dependency_graph():
    """
    Endpoint to fetch the dependency graph as JSON data.
    """
    try:
        graph_data = get_graph_data_as_json()
        return {"status": "success", "graph": graph_data}
    except Exception as e:
        return {"status": "error", "message": f"Failed to fetch graph: {str(e)}"}


