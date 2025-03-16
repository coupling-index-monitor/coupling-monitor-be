from fastapi import APIRouter

from app.services import fetch_services
from app.services.graph_processor import fetch_unique_services_from_neo4j

router = APIRouter()


@router.get("/active")
async def get_active_services():
    services = fetch_services()
    return {"status": "success", "services": services}

@router.get("/recorded")
async def get_recorded_services():
    services  = fetch_unique_services_from_neo4j()
    return {"status": "success", "services": services}