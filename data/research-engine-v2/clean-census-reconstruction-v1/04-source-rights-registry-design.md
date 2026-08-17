# Source Rights Registry Design

| Field | Purpose |
|-------|---------|
| source_name | Display name |
| source_type | Official Parent / Brand / Operator / Registry / Trade / … |
| url_domain | Primary domain |
| permitted_research_use | yes / no / unknown |
| permitted_factual_extraction | yes / no / unknown |
| permitted_production_display | yes / no / unknown |
| permitted_image_use | yes / no / unknown |
| restrictions | Free text |
| robots_access | notes |
| terms_reviewed | date or null |
| review_date | |
| legal_review_required | boolean |
| notes | |

Where unknown: **Unknown — Legal Review Required** (not a legal conclusion).

Pilot registry stubs:

| Source | Type | Research | Factual extract | Production display | Images | Legal review |
|--------|------|----------|-----------------|--------------------|--------|--------------|
| ihg.com directory / hoteldetail | Official Parent/Brand | yes (factual) | yes (factual) | unknown pending product policy | Unknown — Legal Review Required | Yes for display/images |
| Legacy Hotel Census CSV | Quarantined reference | **no** (independent phase) | **no** as evidence | n/a | n/a | n/a |
