"""
TMDB API Service
Handles all interactions with The Movie Database API.
"""

import os
import httpx
import logging
from typing import Optional

logger = logging.getLogger(__name__)

TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p"


class TMDBService:
    """Async client for the TMDB API."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("TMDB_READ_API_TOKEN")
        if not self.api_key:
            raise ValueError("TMDB_READ_API_TOKEN must be set as an environment variable or passed directly")
        self.client = httpx.AsyncClient(
            base_url=TMDB_BASE_URL,
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=30.0,
        )

    async def close(self):
        await self.client.aclose()

    # ------------------------------------------------------------------ #
    #  Search
    # ------------------------------------------------------------------ #

    async def search_person(self, query: str, page: int = 1) -> dict:
        """Search for actors/people by name."""
        resp = await self.client.get(
            "/search/person",
            params={"query": query, "page": page, "include_adult": False},
        )
        resp.raise_for_status()
        return resp.json()

    async def search_movie(self, query: str, page: int = 1) -> dict:
        """Search for movies by title."""
        resp = await self.client.get(
            "/search/movie",
            params={"query": query, "page": page, "include_adult": False},
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    #  Person / Actor details
    # ------------------------------------------------------------------ #

    async def get_person(self, tmdb_id: int) -> dict:
        """Get full details for a person, including their movie credits."""
        resp = await self.client.get(
            f"/person/{tmdb_id}",
            params={"append_to_response": "movie_credits"},
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    #  Movie details
    # ------------------------------------------------------------------ #

    async def get_movie(self, tmdb_id: int) -> dict:
        """Get full details for a movie, including its cast."""
        resp = await self.client.get(
            f"/movie/{tmdb_id}",
            params={"append_to_response": "credits"},
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def profile_url(path: Optional[str], size: str = "w185") -> Optional[str]:
        """Build a full image URL from a TMDB profile_path."""
        if not path:
            return None
        return f"{TMDB_IMAGE_BASE}/{size}{path}"

    @staticmethod
    def poster_url(path: Optional[str], size: str = "w342") -> Optional[str]:
        """Build a full poster URL from a TMDB poster_path."""
        if not path:
            return None
        return f"{TMDB_IMAGE_BASE}/{size}{path}"
