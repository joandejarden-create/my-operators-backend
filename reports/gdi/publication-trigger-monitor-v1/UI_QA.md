# UI QA

Customer Watch cards may show:
- **Publication monitoring:** Monitoring for: Exhibitor list; …
- **Last checked**
- **Next expected trigger window**

Never shown: content hashes, crawler debug, SERP queries, campaign IDs as customer objects.

Watch cards synced: AC 2, RAD 3.

API: `GET /api/group-demand-intelligence/hotels/:hotelId/publication-monitors?customerSafe=1`
