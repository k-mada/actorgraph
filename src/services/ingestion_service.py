"""
Data Ingestion Orchestrator
The "cache-through" layer: checks Neo4j first, fetches from TMDB only if needed,
then persists to Neo4j for future queries.

This is the ONLY module that coordinates between TMDB and Neo4j.
All API routes should call this module rather than TMDB or Neo4j directly.
"""

import asyncio
import logging
from typing import Optional
from .tmdb_service import TMDBService
from .neo4j_service import Neo4jService

logger = logging.getLogger(__name__)

# Only ingest the top N cast members per movie to keep the graph manageable.
# Most movies list 20-80+ cast members; the top 15 covers all principal actors.
MAX_CAST_PER_MOVIE = 15


class DataIngestionService:
    """
    Orchestrates the cache-through pattern:
    1. Check if the data exists in Neo4j
    2. If yes → return it from Neo4j
    3. If no → fetch from TMDB, store in Neo4j, then return it
    """

    def __init__(self, tmdb: TMDBService, neo4j: Neo4jService):
        self.tmdb = tmdb
        self.neo4j = neo4j

    # ------------------------------------------------------------------ #
    #  Actor ingestion
    # ------------------------------------------------------------------ #

    async def ensure_actor(self, tmdb_id: int) -> dict:
        """
        Guarantee an actor and their filmography exist in Neo4j.
        If the actor is missing, not fully fetched, or has no credits stored,
        fetch from TMDB. Returns the actor's profile from the database.
        """
        actor = await self.neo4j.get_actor(tmdb_id)

        if not actor or not actor.get("fully_fetched"):
            logger.info(f"Actor {tmdb_id} not fully ingested — fetching from TMDB")
            await self._ingest_actor_from_tmdb(tmdb_id)
            actor = await self.neo4j.get_actor(tmdb_id)

        return actor

    async def _ingest_actor_from_tmdb(self, tmdb_id: int):
        """
        Fetch an actor's profile + filmography from TMDB and store everything.
        This also creates Movie nodes and ACTED_IN relationships.
        """
        # Step 1: Fetch actor details + their movie credits in one API call
        person_data = await self.tmdb.get_person(tmdb_id)

        # Step 2: Upsert the Actor node
        await self.neo4j.upsert_actor(person_data)

        # Step 3: Process their movie credits (the "cast" portion)
        # Batch all movies + relationships into a single Neo4j transaction
        # to avoid 200+ sequential remote round-trips.
        movie_credits = person_data.get("movie_credits", {}).get("cast", [])

        batch = []
        for credit in movie_credits:
            movie_tmdb_id = credit.get("id")
            if not movie_tmdb_id:
                continue

            release_date = credit.get("release_date") or ""
            release_year = None
            if release_date and len(release_date) >= 4:
                try:
                    release_year = int(release_date[:4])
                except ValueError:
                    pass

            poster_path = credit.get("poster_path")
            poster_url = (
                f"https://image.tmdb.org/t/p/w342{poster_path}" if poster_path else None
            )

            batch.append({
                "tmdb_id": movie_tmdb_id,
                "title": credit.get("title"),
                "release_date": release_date,
                "release_year": release_year,
                "poster_path": poster_path,
                "poster_url": poster_url,
                "overview": credit.get("overview"),
                "popularity": credit.get("popularity"),
                "vote_average": credit.get("vote_average"),
                "genres": [],
                "character": credit.get("character"),
                "billing_order": credit.get("order"),
            })

        if batch:
            await self.neo4j.batch_upsert_movies_and_credits(tmdb_id, batch)

        logger.info(
            f"Ingested actor '{person_data.get('name')}' with "
            f"{len(batch)} movie credits"
        )

    # ------------------------------------------------------------------ #
    #  Movie ingestion (with full cast)
    # ------------------------------------------------------------------ #

    async def ensure_movie_with_cast(self, tmdb_id: int) -> dict:
        """
        Guarantee a movie AND its principal cast exist in Neo4j.
        This is the deeper ingestion path — it fetches the movie's credits
        endpoint to get the full cast list, then creates Actor nodes
        and ACTED_IN relationships for the top cast members.
        """
        # Single query: if the movie exists and has cast, return it directly
        movie_data = await self.neo4j.get_movie_with_cast(tmdb_id)
        if movie_data and movie_data.get("cast"):
            return movie_data

        logger.info(f"Movie {tmdb_id} needs full cast ingestion — fetching from TMDB")
        await self._ingest_movie_with_cast(tmdb_id)
        return await self.neo4j.get_movie_with_cast(tmdb_id)

    async def _ingest_movie_with_cast(self, tmdb_id: int):
        """
        Fetch a movie's full details + cast from TMDB and store everything.
        Uses append_to_response=credits to get cast in one API call.
        Batches all actor upserts + relationships in a single Neo4j transaction.
        """
        movie_data = await self.tmdb.get_movie(tmdb_id)

        # Upsert the Movie node with full details
        await self.neo4j.upsert_movie(movie_data)

        # Build batch of cast members (limited to top billing)
        raw_cast = movie_data.get("credits", {}).get("cast", [])
        raw_cast = sorted(raw_cast, key=lambda c: c.get("order", 999))[:MAX_CAST_PER_MOVIE]

        batch = []
        for member in raw_cast:
            actor_tmdb_id = member.get("id")
            if not actor_tmdb_id:
                continue
            batch.append({
                "tmdb_id": actor_tmdb_id,
                "name": member.get("name"),
                "profile_path": member.get("profile_path"),
                "popularity": member.get("popularity"),
                "character": member.get("character"),
                "billing_order": member.get("order"),
            })

        if batch:
            await self.neo4j.batch_upsert_actors_and_movie_credits(tmdb_id, batch)

        logger.info(
            f"Ingested movie '{movie_data.get('title')}' with "
            f"{len(batch)} cast members"
        )

    # ------------------------------------------------------------------ #
    #  Search — hybrid: DB first, then TMDB fallback
    # ------------------------------------------------------------------ #

    async def search_actors(self, query: str) -> list[dict]:
        """
        Search for actors: check the database first, then supplement
        with TMDB API results for actors not yet in the DB.
        """
        # Search locally first
        db_results = await self.neo4j.search_actors_in_db(query, limit=10)
        db_ids = {r["tmdb_id"] for r in db_results}

        # Also search TMDB to find actors not yet in our database
        try:
            tmdb_results = await self.tmdb.search_person(query)
            for person in tmdb_results.get("results", [])[:10]:
                if person["id"] not in db_ids:
                    db_results.append({
                        "tmdb_id": person["id"],
                        "name": person["name"],
                        "profile_path": person.get("profile_path"),
                        "popularity": person.get("popularity", 0),
                        "in_database": False,  # flag for the frontend
                    })
        except Exception as e:
            logger.warning(f"TMDB search fallback failed: {e}")

        # Mark database results
        for r in db_results:
            if "in_database" not in r:
                r["in_database"] = True

        # Sort by popularity so the most relevant actors appear first
        db_results.sort(key=lambda x: x.get("popularity", 0), reverse=True)
        return db_results[:15]

    async def search(self, query: str) -> dict:
        """
        Combined search for actors and movies. Runs all four data sources
        (Neo4j actors, Neo4j movies, TMDB actors, TMDB movies) concurrently,
        then merges and deduplicates.
        """
        # Run all four searches in parallel — response time = slowest single call
        db_actors, db_movies, tmdb_actor_resp, tmdb_movie_resp = await asyncio.gather(
            self.neo4j.search_actors_in_db(query, limit=10),
            self.neo4j.search_movies_in_db(query, limit=10),
            self._safe_tmdb_call(self.tmdb.search_person, query),
            self._safe_tmdb_call(self.tmdb.search_movie, query),
        )

        # Merge actors: DB results first, then TMDB results not already in DB
        actor_ids = {r["tmdb_id"] for r in db_actors}
        actors = [{**r, "type": "actor", "in_database": True} for r in db_actors]
        for person in tmdb_actor_resp.get("results", [])[:10]:
            if person["id"] not in actor_ids:
                actors.append({
                    "tmdb_id": person["id"],
                    "name": person["name"],
                    "profile_path": person.get("profile_path"),
                    "popularity": person.get("popularity", 0),
                    "type": "actor",
                    "in_database": False,
                })
        actors.sort(key=lambda x: x.get("popularity", 0), reverse=True)

        # Merge movies: DB results first, then TMDB results not already in DB
        movie_ids = {r["tmdb_id"] for r in db_movies}
        movies = [{**r, "type": "movie", "in_database": True} for r in db_movies]
        for movie in tmdb_movie_resp.get("results", [])[:10]:
            if movie["id"] not in movie_ids:
                poster_path = movie.get("poster_path")
                movies.append({
                    "tmdb_id": movie["id"],
                    "title": movie.get("title"),
                    "poster_path": poster_path,
                    "poster_url": f"https://image.tmdb.org/t/p/w342{poster_path}" if poster_path else None,
                    "release_year": int(movie["release_date"][:4]) if movie.get("release_date") and len(movie["release_date"]) >= 4 else None,
                    "popularity": movie.get("popularity", 0),
                    "type": "movie",
                    "in_database": False,
                })
        movies.sort(key=lambda x: x.get("popularity", 0), reverse=True)

        return {
            "actors": actors[:10],
            "movies": movies[:10],
        }

    async def _safe_tmdb_call(self, fn, query: str) -> dict:
        """Call a TMDB search method, returning empty dict on failure."""
        try:
            return await fn(query)
        except Exception as e:
            logger.warning(f"TMDB search failed ({fn.__name__}): {e}")
            return {}

    # ------------------------------------------------------------------ #
    #  High-level use case methods (call these from API routes)
    # ------------------------------------------------------------------ #

    async def get_costars(self, actor_tmdb_id: int) -> list[dict]:
        """
        USE CASE 1: Get all co-stars for an actor.
        Ensures the actor is fully ingested first.
        """
        await self.ensure_actor(actor_tmdb_id)
        return await self.neo4j.get_costars(actor_tmdb_id)

    async def get_common_movies(self, actor1_id: int, actor2_id: int) -> list[dict]:
        """
        USE CASE 2: Find movies two actors share.
        Ensures both actors are fully ingested first.
        """
        await asyncio.gather(
            self.ensure_actor(actor1_id),
            self.ensure_actor(actor2_id),
        )
        return await self.neo4j.get_common_movies(actor1_id, actor2_id)

    async def get_shortest_path(self, actor1_id: int, actor2_id: int,
                                 max_depth: int = 8) -> Optional[dict]:
        """
        USE CASE 3: Six degrees of separation.
        Ensures both actors are ingested. If no path is found,
        attempts to deepen the graph by ingesting co-stars' credits.
        """
        # First, ensure both actors and their filmographies are in the DB
        await asyncio.gather(
            self.ensure_actor(actor1_id),
            self.ensure_actor(actor2_id),
        )

        # Try finding a path with existing data
        path = await self.neo4j.get_shortest_path(actor1_id, actor2_id, max_depth)
        if path:
            return path

        # If no path found, the graph might not be connected enough.
        # Deepen by ingesting the cast of movies from both actors.
        # This is the "lazy expansion" strategy.
        logger.info("No path found — expanding graph by ingesting movie casts")
        await asyncio.gather(
            self._expand_graph_for_path(actor1_id),
            self._expand_graph_for_path(actor2_id),
        )

        # Retry
        return await self.neo4j.get_shortest_path(actor1_id, actor2_id, max_depth)

    async def _expand_graph_for_path(self, actor_tmdb_id: int):
        """
        For shortest-path queries: ingest the full cast of the actor's
        most popular movies to build more connections in the graph.
        """
        async with self.neo4j.driver.session() as session:
            # Get movies this actor appeared in, ordered by popularity
            result = await session.run(
                """
                MATCH (a:Actor {tmdb_id: $tmdb_id})-[:ACTED_IN]->(m:Movie)
                RETURN m.tmdb_id AS tmdb_id
                ORDER BY m.popularity DESC
                LIMIT 5
                """,
                tmdb_id=actor_tmdb_id,
            )
            movie_ids = [record["tmdb_id"] async for record in result]

        # Ingest full cast for each movie concurrently (creates many new Actor
        # nodes and connections, greatly increasing the chance of finding a path)
        async def _safe_ingest(movie_id):
            try:
                await self.ensure_movie_with_cast(movie_id)
            except Exception as e:
                logger.warning(f"Failed to expand movie {movie_id}: {e}")

        await asyncio.gather(*[_safe_ingest(mid) for mid in movie_ids])

    async def get_actor_neighborhood(self, actor_tmdb_id: int, limit: int = 50,
                                      year_from: int | None = None,
                                      year_to: int | None = None) -> dict:
        """
        Get graph data for visualization.
        Ensures the actor is ingested first.
        Optional year filters narrow the visible connections without losing data.
        """
        await self.ensure_actor(actor_tmdb_id)
        return await self.neo4j.get_actor_neighborhood(
            actor_tmdb_id, limit=limit, year_from=year_from, year_to=year_to
        )

    async def get_actor_filmography(self, actor_tmdb_id: int) -> list[dict]:
        """
        Get an actor's full filmography for the detail panel.
        Ensures the actor is ingested first.
        """
        await self.ensure_actor(actor_tmdb_id)
        return await self.neo4j.get_actor_filmography(actor_tmdb_id)
