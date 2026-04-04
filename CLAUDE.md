# CLAUDE.md

## Project Overview

Actor Graph API — a FastAPI app that ingests actor/movie data from TMDB and stores it in a Neo4j graph database. Uses a cache-through pattern: checks Neo4j first, fetches from TMDB only if missing, then persists for future queries.

## Stack

- **Runtime:** Python 3.14, FastAPI, uvicorn
- **Database:** Neo4j (remote, hosted on Aura — `neo4j+s://`)
- **External API:** TMDB v3 (Bearer token auth)
- **HTTP client:** httpx (async)
- **Env:** python-dotenv for `.env` loading

## Running

```
uvicorn src.app:app --reload
```

## Coding Guidelines

### Database operations must be batched

Never loop over individual database writes when a batch operation is possible. Each `async with self.driver.session()` opens a new remote connection. Over a remote Neo4j instance, sequential session creation is extremely slow and can cause request timeouts.

- **Wrong:** Looping over N items and calling `upsert_movie()` + `create_acted_in()` individually (2N remote round-trips)
- **Right:** Build a list of parameters and use a single `UNWIND $params AS param` Cypher query to process everything in one transaction

This applies to all database drivers, not just Neo4j. If you're writing a loop that awaits a database call on each iteration, refactor it into a batch operation.

### Avoid neo4j `session.run()` parameter name conflicts

The neo4j driver's `session.run(query, **kwargs)` has `query` as its first positional parameter. Never pass a Cypher parameter named `query=` as a keyword argument — it will cause `TypeError: got multiple values for argument 'query'`. Use a different name like `search_term`.

### Keep services stateless and composable

- `TMDBService` handles all TMDB API calls
- `Neo4jService` handles all database operations
- `DataIngestionService` orchestrates between the two
- API routes should only call `DataIngestionService`, never TMDB or Neo4j directly

### Error handling

- Validate at system boundaries (API input, external service responses), not internally
- Don't catch broad `Exception` unless you have a specific recovery strategy — let errors propagate so they're visible
- Log enough context to debug failures (include IDs, counts, operation names)
