# GDI Global Discovered Intelligence (local SoT)

Runtime store for independently sourced public demand facts retained across hotels.

- `demands/` — global demand records (`gdi_gbl_*.json`)
- `hotel-matches/` — hotel-specific match rows (no sales CRM)
- `index.json` — lookup by id / org / market

These directories are gitignored (local runtime). Schema and code live under
`lib/group-demand-intelligence/global-intelligence/`.

Airtable sync is dry-run only until founder activation.
