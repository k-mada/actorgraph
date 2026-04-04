"""
Actor Graph API
FastAPI application with cache-through data ingestion.
"""

import os
import logging
from contextlib import asynccontextmanager
from dotenv import load_dotenv

load_dotenv()
from fastapi import FastAPI, HTTPException, Query
from fastapi.security import APIKeyHeader

from .middleware.auth import APIKeyMiddleware
from .middleware.cors import add_cors_middleware
from .services.tmdb_service import TMDBService
from .services.neo4j_service import Neo4jService
from .services.ingestion_service import DataIngestionService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  App lifecycle — initialize services on startup, clean up on shutdown
# ------------------------------------------------------------------ #

ingestion: DataIngestionService = None  # set during lifespan


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ingestion

    # Initialize services
    tmdb = TMDBService(api_key=os.getenv("TMDB_READ_API_TOKEN"))
    neo4j = Neo4jService(
        uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        user=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", "password"),
    )

    # Set up database schema (idempotent — safe to run every startup)
    await neo4j.setup_schema()

    ingestion = DataIngestionService(tmdb=tmdb, neo4j=neo4j)
    logger.info("All services initialized")

    yield  # app runs here

    # Cleanup
    await tmdb.close()
    await neo4j.close()
    logger.info("Services shut down")


# Security scheme for Swagger UI "Authorize" button
api_key_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)

app = FastAPI(
    title="Actor Graph API",
    description="Explore actor relationships through shared movies",
    version="1.0.0",
    lifespan=lifespan,
    swagger_ui_parameters={"persistAuthorization": True},
)

# Middleware order matters: CORS runs first (handles preflight), then auth
add_cors_middleware(app)
app.add_middleware(APIKeyMiddleware)


# ------------------------------------------------------------------ #
#  Search
# ------------------------------------------------------------------ #

@app.get("/api/search")
async def search(q: str = Query(..., min_length=2, description="Search for actors or movies")):
    """
    Combined search for actors and movies.
    Searches local database and TMDB concurrently, returns results grouped by type.
    """
    return await ingestion.search(q)


@app.get("/api/search/actors")
async def search_actors(q: str = Query(..., min_length=2, description="Actor name to search")):
    """
    Search for actors by name.
    Checks local database first, then supplements with TMDB results.
    """
    results = await ingestion.search_actors(q)
    return {"results": results}


# ------------------------------------------------------------------ #
#  Actor details
# ------------------------------------------------------------------ #

@app.get("/api/actors/{tmdb_id}")
async def get_actor(tmdb_id: int):
    """
    Get an actor's profile. If not in the database, fetches from TMDB
    and stores the actor + their full filmography.
    """
    actor = await ingestion.ensure_actor(tmdb_id)
    if not actor:
        raise HTTPException(status_code=404, detail="Actor not found")
    return actor


# ------------------------------------------------------------------ #
#  Movie details
# ------------------------------------------------------------------ #

@app.get("/api/movies/{tmdb_id}")
async def get_movie(tmdb_id: int):
    """
    Get a movie's details and principal cast. If not in the database,
    fetches from TMDB and stores the movie + lightweight actor nodes.
    """
    movie = await ingestion.ensure_movie_with_cast(tmdb_id)
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found")
    return movie


# ------------------------------------------------------------------ #
#  Use Case 1: Co-stars
# ------------------------------------------------------------------ #

@app.get("/api/actors/{tmdb_id}/costars")
async def get_costars(tmdb_id: int):
    """
    Get all actors who have appeared in a movie with this actor,
    sorted by number of shared movies.
    """
    costars = await ingestion.get_costars(tmdb_id)
    return {"actor_tmdb_id": tmdb_id, "costars": costars}


# ------------------------------------------------------------------ #
#  Use Case 2: Common movies between two actors
# ------------------------------------------------------------------ #

@app.get("/api/actors/{actor1_id}/common-movies/{actor2_id}")
async def get_common_movies(actor1_id: int, actor2_id: int):
    """
    Find all movies that two actors have both appeared in.
    """
    movies = await ingestion.get_common_movies(actor1_id, actor2_id)
    return {
        "actor1_tmdb_id": actor1_id,
        "actor2_tmdb_id": actor2_id,
        "common_movies": movies,
        "count": len(movies),
    }


# ------------------------------------------------------------------ #
#  Use Case 3: Six degrees / shortest path
# ------------------------------------------------------------------ #

@app.get("/api/actors/{actor1_id}/path/{actor2_id}")
async def get_shortest_path(
    actor1_id: int,
    actor2_id: int,
    max_depth: int = Query(default=8, ge=2, le=12),
):
    """
    Find the shortest path between two actors through shared movies.
    Returns the chain: Actor → Movie → Actor → Movie → Actor ...
    """
    result = await ingestion.get_shortest_path(actor1_id, actor2_id, max_depth)
    if not result:
        return {
            "actor1_tmdb_id": actor1_id,
            "actor2_tmdb_id": actor2_id,
            "found": False,
            "message": "No connection found within the current graph. "
                       "Try searching for more actors to expand the network.",
        }
    return {
        "actor1_tmdb_id": actor1_id,
        "actor2_tmdb_id": actor2_id,
        "found": True,
        "degrees": result["degrees"],
        "path": result["path"],
    }


# ------------------------------------------------------------------ #
#  Graph visualization data
# ------------------------------------------------------------------ #

@app.get("/api/graph/neighborhood/{tmdb_id}")
async def get_actor_neighborhood(
    tmdb_id: int,
    limit: int = Query(default=30, ge=5, le=100),
    year_from: int | None = Query(default=None, description="Filter: earliest movie year"),
    year_to: int | None = Query(default=None, description="Filter: latest movie year"),
):
    """
    Get a subgraph around an actor for force-directed graph rendering.
    The primary entry point: user selects an actor → sees their co-star network.

    Optional year_from/year_to narrow the visible connections without
    removing any data from the database.
    """
    graph = await ingestion.get_actor_neighborhood(
        tmdb_id, limit=limit, year_from=year_from, year_to=year_to
    )
    return graph


@app.get("/api/graph/expand/{tmdb_id}")
async def expand_graph_node(tmdb_id: int):
    """
    Expand a node in the graph: when a user clicks a co-star node,
    ingest that actor's full filmography so their connections appear.
    Returns the newly available neighborhood for that actor.
    """
    await ingestion.ensure_actor(tmdb_id)
    graph = await ingestion.get_actor_neighborhood(tmdb_id, limit=20)
    return graph


# ------------------------------------------------------------------ #
#  Actor filmography
# ------------------------------------------------------------------ #

@app.get("/api/actors/{tmdb_id}/filmography")
async def get_actor_filmography(tmdb_id: int):
    """
    Get an actor's complete filmography with character names.
    Used by the detail panel sidebar.
    """
    movies = await ingestion.get_actor_filmography(tmdb_id)
    return {"actor_tmdb_id": tmdb_id, "movies": movies}
