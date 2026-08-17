# Source Fallback Ladder

1. Official property page
2. Official brand directory API/feed/sitemap
3. Official parent sitemap
4. Official cached/index metadata (if reliable)
5. Secondary official announcement
6. High-quality trade source (corroboration only)

If all fail → **Source Blocked / Needs External Research**  
Do **not** infer closed / removed / reflagged / discontinued / missing.

Trade press = corroboration only (never sole High material update).

Example terminal summary shape:
```json
{
  "ladder": [
    {
      "step": 1,
      "id": "official_property_page",
      "label": "Official property page"
    },
    {
      "step": 2,
      "id": "official_brand_directory",
      "label": "Official brand directory API/feed/sitemap"
    },
    {
      "step": 3,
      "id": "official_parent_sitemap",
      "label": "Official parent sitemap"
    },
    {
      "step": 4,
      "id": "official_cached_index",
      "label": "Official cached/index metadata (if reliable)"
    },
    {
      "step": 5,
      "id": "official_announcement",
      "label": "Secondary official announcement"
    },
    {
      "step": 6,
      "id": "trade_corroboration_only",
      "label": "High-quality trade source (corroboration only)"
    }
  ],
  "attempts": [
    {
      "ladderId": "official_property_page",
      "sourceState": "Blocked"
    }
  ],
  "availableStep": null,
  "fallbackUsed": false,
  "terminal": {
    "classification": "Source Blocked / Needs External Research",
    "escalation": "Webhound candidate or manual source retrieval (explicit auth required)",
    "doNotInfer": [
      "closed",
      "removed",
      "reflagged",
      "discontinued",
      "missing"
    ]
  }
}
```
