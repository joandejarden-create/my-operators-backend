# Research payload integrity

- Oversized payloads fail closed (`RESEARCH_PAYLOAD_TOO_LARGE`) — no silent truncate
- Telemetry-only keys dropped before serialize (`orsRaw`, `firecrawlRaw`, etc.)
- Material compare canonicalizes JSON so key-order / null-vs-missing does not false-update
- Read-back verifies compositeKey + hotelId + opportunityId after CREATE/UPDATE
