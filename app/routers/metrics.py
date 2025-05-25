from fastapi import APIRouter, Request

from fastapi.responses import JSONResponse
from app.services import (
    handle_detect_change_points
)

router = APIRouter()

@router.get("/change-points")
async def get_change_points(start_time: int, end_time: int, metric: str):
    """
    Endpoint to get change points in the given time range.
    """
    try:
        response = handle_detect_change_points(start_time, end_time, metric)
        return response
    except Exception as e:
        print(f"ERROR: Failed to generate weighted graph: {str(e)}")
        return JSONResponse(status_code=500, content={"status": "error", "message": f"Failed to generate graph: {str(e)}"})
