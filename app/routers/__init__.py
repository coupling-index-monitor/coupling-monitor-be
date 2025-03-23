from app.routers.graphs import router as graphs_router
from app.routers.services import router as services_router
from app.routers.coupling import router as coupling_router
from app.routers.metrics import router as metrics_router

__all__ = ["graphs_router", "services_router", "coupling_router", "metrics_router"]
