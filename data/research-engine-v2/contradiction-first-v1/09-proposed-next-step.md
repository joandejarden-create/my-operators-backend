# Proposed next step

1. Add **high-confidence match gates** (name + city/country) before emitting Proposed Status Change / Proposed Reflag.
2. Re-run blind benchmark with:
   - full Marriott soft-brand CALA directory (Tribute + Autograph + Design Hotels)
   - Choice sitemap gap scan (census vs directory)
   - census rows beyond amenities-blank (include Barbados Autograph inventory)
3. Only after FP rate drops: wire proposed corrections into an existing **dry-run apply-gate** review pack (still no auto-write).

Do **not** enable Airtable writes or Webhound for this path yet.
