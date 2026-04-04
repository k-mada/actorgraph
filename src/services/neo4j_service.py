"""
Neo4j Database Service
Handles all graph database operations: reads, writes, and schema setup.
"""

import logging
from typing import Optional
from neo4j import AsyncGraphDatabase, AsyncDriver

logger = logging.getLogger(__name__)


class Neo4jService:
    """Async Neo4j driver wrapper with actor-graph-specific queries."""

    def __init__(self, uri: str, user: str, password: str):
        self.driver: AsyncDriver = AsyncGraphDatabase.driver(
            uri,
            auth=(user, password),
            liveness_check_timeout=30,
        )

    async def close(self):
        await self.driver.close()

    # ------------------------------------------------------------------ #
    #  Schema setup — run once on app startup
    # ------------------------------------------------------------------ #

    async def setup_schema(self):
        """Create constraints and indexes for optimal performance."""
        async with self.driver.session() as session:
            # Uniqueness constraints (these also create indexes)
            await session.run(
                "CREATE CONSTRAINT actor_tmdb_id IF NOT EXISTS "
                "FOR (a:Actor) REQUIRE a.tmdb_id IS UNIQUE"
            )
            await session.run(
                "CREATE CONSTRAINT movie_tmdb_id IF NOT EXISTS "
                "FOR (m:Movie) REQUIRE m.tmdb_id IS UNIQUE"
            )
            # Full-text index for autocomplete search
            await session.run(
                "CREATE TEXT INDEX actor_name_search IF NOT EXISTS "
                "FOR (a:Actor) ON (a.name)"
            )
            await session.run(
                "CREATE TEXT INDEX movie_title_search IF NOT EXISTS "
                "FOR (m:Movie) ON (m.title)"
            )
            # Range index for year-based filtering
            await session.run(
                "CREATE INDEX movie_release_year IF NOT EXISTS "
                "FOR (m:Movie) ON (m.release_year)"
            )
        logger.info("Neo4j schema setup complete")

    # ------------------------------------------------------------------ #
    #  Check existence — the "cache-through" gate
    # ------------------------------------------------------------------ #

    async def actor_exists(self, tmdb_id: int) -> bool:
        """Check if an actor is already stored in the database."""
        async with self.driver.session() as session:
            result = await session.run(
                "MATCH (a:Actor {tmdb_id: $tmdb_id}) RETURN a.tmdb_id LIMIT 1",
                tmdb_id=tmdb_id,
            )
            record = await result.single()
            return record is not None

    async def movie_exists(self, tmdb_id: int) -> bool:
        """Check if a movie is already stored in the database."""
        async with self.driver.session() as session:
            result = await session.run(
                "MATCH (m:Movie {tmdb_id: $tmdb_id}) RETURN m.tmdb_id LIMIT 1",
                tmdb_id=tmdb_id,
            )
            record = await result.single()
            return record is not None

    async def actor_has_credits(self, tmdb_id: int) -> bool:
        """Check if an actor already has ACTED_IN relationships stored."""
        async with self.driver.session() as session:
            result = await session.run(
                "MATCH (a:Actor {tmdb_id: $tmdb_id})-[:ACTED_IN]->() "
                "RETURN count(*) AS count",
                tmdb_id=tmdb_id,
            )
            record = await result.single()
            return record and record["count"] > 0

    async def actor_fully_fetched(self, tmdb_id: int) -> bool:
        """Check if an actor has been fully fetched from TMDB (not just a lightweight node)."""
        async with self.driver.session() as session:
            result = await session.run(
                "MATCH (a:Actor {tmdb_id: $tmdb_id}) RETURN a.fully_fetched AS fully_fetched",
                tmdb_id=tmdb_id,
            )
            record = await result.single()
            return record is not None and record["fully_fetched"] is True

    # ------------------------------------------------------------------ #
    #  Write operations — store data from TMDB
    # ------------------------------------------------------------------ #

    async def upsert_actor(self, actor_data: dict):
        """
        Create or update an Actor node.
        actor_data should contain: tmdb_id, name, profile_path, popularity, etc.
        """
        async with self.driver.session() as session:
            await session.run(
                """
                MERGE (a:Actor {tmdb_id: $tmdb_id})
                ON CREATE SET
                    a.name = $name,
                    a.profile_path = $profile_path,
                    a.popularity = $popularity,
                    a.birthday = $birthday,
                    a.biography = $biography,
                    a.place_of_birth = $place_of_birth,
                    a.fully_fetched = true,
                    a.fetched_at = datetime()
                ON MATCH SET
                    a.name = $name,
                    a.profile_path = $profile_path,
                    a.popularity = $popularity,
                    a.birthday = $birthday,
                    a.biography = $biography,
                    a.place_of_birth = $place_of_birth,
                    a.fully_fetched = true,
                    a.fetched_at = datetime()
                """,
                tmdb_id=actor_data.get("id"),
                name=actor_data.get("name"),
                profile_path=actor_data.get("profile_path"),
                popularity=actor_data.get("popularity"),
                birthday=actor_data.get("birthday"),
                biography=actor_data.get("biography"),
                place_of_birth=actor_data.get("place_of_birth"),
            )

    # TMDB image base URL — used to resolve full poster/profile URLs at write time
    TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p"

    async def upsert_movie(self, movie_data: dict):
        """
        Create or update a Movie node.
        movie_data should contain: id, title, release_date, poster_path, etc.
        Extracts release_year as a separate integer property for efficient filtering.
        Resolves poster_path into a full poster_url for direct use by the frontend.
        """
        # Extract year from release_date string (e.g. "1994-07-06" → 1994)
        release_date = movie_data.get("release_date") or ""
        release_year = None
        if release_date and len(release_date) >= 4:
            try:
                release_year = int(release_date[:4])
            except ValueError:
                pass

        # Resolve full poster URL from the relative path
        poster_path = movie_data.get("poster_path")
        poster_url = f"{self.TMDB_IMAGE_BASE}/w342{poster_path}" if poster_path else None

        # Handle genres: may come as list of dicts or list of strings
        raw_genres = movie_data.get("genres", [])
        if raw_genres and isinstance(raw_genres[0], dict):
            genres = [g["name"] for g in raw_genres]
        else:
            genres = raw_genres

        async with self.driver.session() as session:
            await session.run(
                """
                MERGE (m:Movie {tmdb_id: $tmdb_id})
                ON CREATE SET
                    m.title = $title,
                    m.release_date = $release_date,
                    m.release_year = $release_year,
                    m.poster_path = $poster_path,
                    m.poster_url = $poster_url,
                    m.overview = $overview,
                    m.popularity = $popularity,
                    m.vote_average = $vote_average,
                    m.genres = $genres,
                    m.fetched_at = datetime()
                ON MATCH SET
                    m.title = $title,
                    m.release_date = $release_date,
                    m.release_year = $release_year,
                    m.poster_path = $poster_path,
                    m.poster_url = $poster_url,
                    m.overview = $overview,
                    m.popularity = $popularity,
                    m.vote_average = $vote_average,
                    m.genres = $genres,
                    m.fetched_at = datetime()
                """,
                tmdb_id=movie_data.get("id"),
                title=movie_data.get("title"),
                release_date=release_date,
                release_year=release_year,
                poster_path=poster_path,
                poster_url=poster_url,
                overview=movie_data.get("overview"),
                popularity=movie_data.get("popularity"),
                vote_average=movie_data.get("vote_average"),
                genres=genres,
            )

    async def create_acted_in(self, actor_tmdb_id: int, movie_tmdb_id: int,
                               character: Optional[str] = None, order: Optional[int] = None):
        """Create an ACTED_IN relationship between an actor and a movie."""
        async with self.driver.session() as session:
            await session.run(
                """
                MATCH (a:Actor {tmdb_id: $actor_id})
                MATCH (m:Movie {tmdb_id: $movie_id})
                MERGE (a)-[r:ACTED_IN]->(m)
                ON CREATE SET r.character = $character, r.billing_order = $order
                ON MATCH SET r.character = $character, r.billing_order = $order
                """,
                actor_id=actor_tmdb_id,
                movie_id=movie_tmdb_id,
                character=character,
                order=order,
            )

    async def batch_upsert_movies_and_credits(
        self, actor_tmdb_id: int, credits: list[dict]
    ):
        """
        Upsert multiple Movie nodes and their ACTED_IN relationships to an actor
        in a single transaction. Much faster than individual calls over a remote connection.
        """
        async with self.driver.session() as session:
            await session.run(
                """
                UNWIND $credits AS credit
                MERGE (m:Movie {tmdb_id: credit.tmdb_id})
                ON CREATE SET
                    m.title = credit.title,
                    m.release_date = credit.release_date,
                    m.release_year = credit.release_year,
                    m.poster_path = credit.poster_path,
                    m.poster_url = credit.poster_url,
                    m.overview = credit.overview,
                    m.popularity = credit.popularity,
                    m.vote_average = credit.vote_average,
                    m.genres = credit.genres,
                    m.fetched_at = datetime()
                ON MATCH SET
                    m.title = credit.title,
                    m.release_date = credit.release_date,
                    m.release_year = credit.release_year,
                    m.poster_path = credit.poster_path,
                    m.poster_url = credit.poster_url,
                    m.overview = credit.overview,
                    m.popularity = credit.popularity,
                    m.vote_average = credit.vote_average,
                    m.genres = credit.genres,
                    m.fetched_at = datetime()
                WITH m, credit
                MATCH (a:Actor {tmdb_id: $actor_id})
                MERGE (a)-[r:ACTED_IN]->(m)
                ON CREATE SET r.character = credit.character, r.billing_order = credit.billing_order
                ON MATCH SET r.character = credit.character, r.billing_order = credit.billing_order
                """,
                actor_id=actor_tmdb_id,
                credits=credits,
            )

    async def batch_upsert_actors_and_movie_credits(
        self, movie_tmdb_id: int, cast: list[dict]
    ):
        """
        Upsert multiple Actor nodes and their ACTED_IN relationships to a movie
        in a single transaction. Lightweight actors get fully_fetched=false on
        creation; existing fully-fetched actors are not downgraded.
        """
        async with self.driver.session() as session:
            await session.run(
                """
                UNWIND $cast AS member
                MERGE (a:Actor {tmdb_id: member.tmdb_id})
                ON CREATE SET
                    a.name = member.name,
                    a.profile_path = member.profile_path,
                    a.popularity = member.popularity,
                    a.birthday = null,
                    a.biography = null,
                    a.place_of_birth = null,
                    a.fully_fetched = false,
                    a.fetched_at = datetime()
                ON MATCH SET
                    a.name = member.name,
                    a.profile_path = member.profile_path,
                    a.popularity = member.popularity
                WITH a, member
                MATCH (m:Movie {tmdb_id: $movie_id})
                MERGE (a)-[r:ACTED_IN]->(m)
                ON CREATE SET r.character = member.character, r.billing_order = member.billing_order
                ON MATCH SET r.character = member.character, r.billing_order = member.billing_order
                """,
                movie_id=movie_tmdb_id,
                cast=cast,
            )

    # ------------------------------------------------------------------ #
    #  Read operations — power your three use cases
    # ------------------------------------------------------------------ #

    async def get_actor(self, tmdb_id: int) -> Optional[dict]:
        """Retrieve an actor's stored profile from the database."""
        async with self.driver.session() as session:
            result = await session.run(
                "MATCH (a:Actor {tmdb_id: $tmdb_id}) RETURN a",
                tmdb_id=tmdb_id,
            )
            record = await result.single()
            if record:
                return dict(record["a"])
            return None

    async def get_movie(self, tmdb_id: int) -> Optional[dict]:
        """Retrieve a movie's stored data from the database."""
        async with self.driver.session() as session:
            result = await session.run(
                "MATCH (m:Movie {tmdb_id: $tmdb_id}) RETURN m",
                tmdb_id=tmdb_id,
            )
            record = await result.single()
            if record:
                return dict(record["m"])
            return None

    async def get_movie_with_cast(self, tmdb_id: int) -> Optional[dict]:
        """Retrieve a movie and its cast from the database."""
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (m:Movie {tmdb_id: $tmdb_id})
                OPTIONAL MATCH (a:Actor)-[r:ACTED_IN]->(m)
                WITH m, a, r
                ORDER BY r.billing_order ASC
                RETURN m,
                       collect(CASE WHEN a IS NOT NULL THEN {
                           tmdb_id: a.tmdb_id,
                           name: a.name,
                           profile_path: a.profile_path,
                           character: r.character,
                           billing_order: r.billing_order
                       } END) AS cast
                """,
                tmdb_id=tmdb_id,
            )
            record = await result.single()
            if not record:
                return None
            movie = dict(record["m"])
            movie["cast"] = [c for c in record["cast"] if c is not None]
            return movie

    async def get_costars(self, actor_tmdb_id: int) -> list[dict]:
        """
        USE CASE 1: Find all actors who have shared a movie with the given actor.
        Returns co-stars sorted by number of shared movies.
        """
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (a:Actor {tmdb_id: $tmdb_id})-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(costar:Actor)
                RETURN costar.tmdb_id AS tmdb_id,
                       costar.name AS name,
                       costar.profile_path AS profile_path,
                       collect(m.title) AS shared_movies,
                       count(m) AS movie_count
                ORDER BY movie_count DESC
                """,
                tmdb_id=actor_tmdb_id,
            )
            return [dict(record) async for record in result]

    async def get_common_movies(self, actor1_tmdb_id: int, actor2_tmdb_id: int) -> list[dict]:
        """
        USE CASE 2: Find all movies that two actors have both appeared in.
        """
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (a1:Actor {tmdb_id: $id1})-[r1:ACTED_IN]->(m:Movie)<-[r2:ACTED_IN]-(a2:Actor {tmdb_id: $id2})
                RETURN m.tmdb_id AS tmdb_id,
                       m.title AS title,
                       m.release_date AS release_date,
                       m.poster_path AS poster_path,
                       m.poster_url AS poster_url,
                       m.vote_average AS vote_average,
                       r1.character AS actor1_character,
                       r2.character AS actor2_character
                ORDER BY m.release_date DESC
                """,
                id1=actor1_tmdb_id,
                id2=actor2_tmdb_id,
            )
            return [dict(record) async for record in result]

    async def get_shortest_path(self, actor1_tmdb_id: int, actor2_tmdb_id: int,
                                 max_depth: int = 8) -> Optional[dict]:
        """
        USE CASE 3: Six degrees of separation.
        Finds the shortest path between two actors through shared movies.
        Returns the full chain: Actor -> Movie -> Actor -> Movie -> Actor ...
        """
        async with self.driver.session() as session:
            # max_depth is interpolated into the query string because Neo4j
            # does not support parameterized relationship length bounds inside
            # shortestPath(). The value is a validated int from the API layer.
            result = await session.run(
                f"""
                MATCH p = shortestPath(
                    (a1:Actor {{tmdb_id: $id1}})-[:ACTED_IN*1..{int(max_depth)}]-(a2:Actor {{tmdb_id: $id2}})
                )
                UNWIND nodes(p) AS node
                RETURN
                    CASE
                        WHEN node:Actor THEN {{
                            type: 'actor',
                            tmdb_id: node.tmdb_id,
                            name: node.name,
                            profile_path: node.profile_path
                        }}
                        WHEN node:Movie THEN {{
                            type: 'movie',
                            tmdb_id: node.tmdb_id,
                            title: node.title,
                            poster_path: node.poster_path,
                            poster_url: node.poster_url,
                            release_date: node.release_date
                        }}
                    END AS step
                """,
                id1=actor1_tmdb_id,
                id2=actor2_tmdb_id,
            )
            steps = [record["step"] async for record in result]
            if not steps:
                return None
            degrees = len([s for s in steps if s["type"] == "actor"]) - 1
            return {"degrees": degrees, "path": steps}

    async def get_actor_neighborhood(self, actor_tmdb_id: int, limit: int = 50,
                                      year_from: int | None = None,
                                      year_to: int | None = None) -> dict:
        """
        Get a subgraph around an actor for force-directed graph visualization.
        Returns nodes and edges suitable for react-force-graph.

        Optional year_from/year_to filters restrict to movies released in that
        range without removing any data — just narrowing the view.
        """
        # Build an optional WHERE clause for year filtering
        year_filter = ""
        params: dict = {"tmdb_id": actor_tmdb_id, "limit": limit}
        if year_from is not None:
            year_filter += " AND m.release_year >= $year_from"
            params["year_from"] = year_from
        if year_to is not None:
            year_filter += " AND m.release_year <= $year_to"
            params["year_to"] = year_to

        async with self.driver.session() as session:
            result = await session.run(
                f"""
                MATCH (center:Actor {{tmdb_id: $tmdb_id}})-[r1:ACTED_IN]->(m:Movie)<-[r2:ACTED_IN]-(costar:Actor)
                WHERE true {year_filter}
                WITH center, costar,
                     collect(DISTINCT {{
                         tmdb_id: m.tmdb_id,
                         title: m.title,
                         release_date: m.release_date,
                         release_year: m.release_year,
                         poster_path: m.poster_path,
                         poster_url: m.poster_url,
                         vote_average: m.vote_average
                     }}) AS shared_movies,
                     count(DISTINCT m) AS weight
                ORDER BY weight DESC
                LIMIT $limit
                RETURN center.tmdb_id AS center_id,
                       center.name AS center_name,
                       center.profile_path AS center_profile,
                       center.popularity AS center_popularity,
                       costar.tmdb_id AS costar_id,
                       costar.name AS costar_name,
                       costar.profile_path AS costar_profile,
                       costar.popularity AS costar_popularity,
                       shared_movies,
                       weight
                """,
                **params,
            )
            records = [dict(r) async for r in result]

            if not records:
                return {"nodes": [], "links": []}

            # Build nodes and links for the graph
            nodes = {}
            links = []

            # Add center node
            center = records[0]
            nodes[center["center_id"]] = {
                "id": center["center_id"],
                "name": center["center_name"],
                "profile_path": center["center_profile"],
                "popularity": center["center_popularity"],
                "type": "actor",
                "is_center": True,
            }

            for r in records:
                costar_id = r["costar_id"]
                if costar_id not in nodes:
                    nodes[costar_id] = {
                        "id": costar_id,
                        "name": r["costar_name"],
                        "profile_path": r["costar_profile"],
                        "popularity": r["costar_popularity"],
                        "type": "actor",
                        "is_center": False,
                    }
                links.append({
                    "source": center["center_id"],
                    "target": costar_id,
                    "shared_movies": r["shared_movies"],
                    "weight": r["weight"],
                })

            return {
                "nodes": list(nodes.values()),
                "links": links,
                "center_tmdb_id": actor_tmdb_id,
            }

    async def get_actor_filmography(self, actor_tmdb_id: int) -> list[dict]:
        """Get all movies an actor has appeared in, for the detail panel."""
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (a:Actor {tmdb_id: $tmdb_id})-[r:ACTED_IN]->(m:Movie)
                RETURN m.tmdb_id AS tmdb_id,
                       m.title AS title,
                       m.release_date AS release_date,
                       m.release_year AS release_year,
                       m.poster_path AS poster_path,
                       m.poster_url AS poster_url,
                       m.vote_average AS vote_average,
                       r.character AS character
                ORDER BY m.release_date DESC
                """,
                tmdb_id=actor_tmdb_id,
            )
            return [dict(record) async for record in result]

    async def search_actors_in_db(self, query: str, limit: int = 10) -> list[dict]:
        """Search for actors already in the database using text index."""
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (a:Actor)
                WHERE a.name CONTAINS $search_term
                RETURN a.tmdb_id AS tmdb_id,
                       a.name AS name,
                       a.profile_path AS profile_path,
                       a.popularity AS popularity
                ORDER BY a.popularity DESC
                LIMIT $limit
                """,
                search_term=query,
                limit=limit,
            )
            return [dict(record) async for record in result]

    async def search_movies_in_db(self, query: str, limit: int = 10) -> list[dict]:
        """Search for movies already in the database by title."""
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (m:Movie)
                WHERE m.title CONTAINS $search_term
                RETURN m.tmdb_id AS tmdb_id,
                       m.title AS title,
                       m.poster_path AS poster_path,
                       m.poster_url AS poster_url,
                       m.release_year AS release_year,
                       m.popularity AS popularity
                ORDER BY m.popularity DESC
                LIMIT $limit
                """,
                search_term=query,
                limit=limit,
            )
            return [dict(record) async for record in result]
