from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.database import db_manager
from app.routers import graphs_router, services_router, coupling_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up: ")
    print("Initializing database connections...")
    db_manager.initialize_neo4j()
    
    print("\nAvailable API Endpoints:")
    for route in app.router.routes:
        methods = ", ".join(route.methods)
        print(f"{methods} - {route.path}")
    
    yield 

    print("Shutting down: ")
    print("Closing database connections...")
    db_manager.close_neo4j()

app = FastAPI(title="Coupling Monitor API", version="0.1.0", lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(graphs_router, prefix="/api/graphs", tags=["Graphs"])
app.include_router(services_router, prefix="/api/services", tags=["Graphs"])
app.include_router(coupling_router, prefix="/api/coupling", tags=["Coupling"])

@app.get("/")
async def root():
    return {"message": "Coupling Monitor API is running"}
