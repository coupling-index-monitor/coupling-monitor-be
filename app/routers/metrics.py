from fastapi import APIRouter, Request

from fastapi.responses import JSONResponse
from app.services import (
    detect_change_points
)

router = APIRouter()

@router.get("/change-points")
async def get_change_points(start_time: int, end_time: int, metric: str, threshold: float):
    """
    Endpoint to get change points in the given time range.
    """
    try:
        response = detect_change_points(start_time, end_time, metric, threshold)
        return JSONResponse(status_code=200, content={
            "status": "success", 
            "message": "Weighted Dependency graph generated successfully.", 
        })
    except Exception as e:
        print(f"ERROR: Failed to generate weighted graph: {str(e)}")
        return JSONResponse(status_code=500, content={"status": "error", "message": f"Failed to generate graph: {str(e)}"})
