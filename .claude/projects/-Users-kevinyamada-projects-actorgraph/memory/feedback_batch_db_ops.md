---
name: Always batch database operations
description: Never loop over individual DB writes — use batch/UNWIND queries. Missed this pattern in a code review and it caused request timeouts.
type: feedback
---

Always batch database operations instead of looping over individual writes. I failed to catch a loop that made 200+ sequential remote Neo4j sessions (2 per movie credit) when reviewing for inefficiencies — the user had to discover the timeout issue themselves.

**Why:** Each `async with driver.session()` opens a new remote connection. Over a hosted Neo4j instance, this is extremely slow and caused HTTP request timeouts during actor ingestion. The fix was a single `UNWIND`-based Cypher query that completed in <1s.

**How to apply:** When reviewing or writing code that interacts with any database, always flag loops that await individual DB calls. Proactively refactor into batch operations (e.g., `UNWIND` in Cypher, `executemany` in SQL, bulk writes in MongoDB). This is a critical performance pattern, not an optional optimization.
