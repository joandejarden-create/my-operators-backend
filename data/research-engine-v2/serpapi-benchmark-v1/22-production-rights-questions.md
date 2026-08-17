# Production Rights Questions — Ask SerpApi (before any production integration)

Joan should get written answers (not marketing copy) on whether a commercial SaaS may:

1. **Persist** factual property data returned by the API (name, address, phone, website, coords, amenities, hotel class)?
2. **Retain** that data after the request/session completes?
3. **Combine** it with independently researched hotel data in a proprietary database?
4. Use **derived** factual fields inside a proprietary Hotel Census product?
5. **Display** those factual fields to paying SaaS users (customer-facing)?
6. Maintain **historical snapshots** / change history of those fields?
7. Persist and reuse **property_token** / Google property identifiers across time?
8. Store **image URLs** as references (without downloading)?
9. **Download or reuse images** at all (almost certainly restricted — confirm)?
10. What obligations apply regarding **Google** as the underlying Hotels data source (attribution, prohibited uses, geographic restrictions)?
11. Are there differences between **benchmark/R&D** use and **production enrichment** under the subscribed plan?
12. What happens on **plan cancellation** — must derived Census fields be deleted?

Do not assume Terms of Service marketing language answers these. Block production writes until answered.
