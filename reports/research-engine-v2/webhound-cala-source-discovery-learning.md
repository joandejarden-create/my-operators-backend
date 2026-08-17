# Webhound CALA Source Discovery Learning

**Status:** `webhound_cala_source_discovery_learning_complete_ready_for_adapter_build`  
**Recommendation:** **B — Build Marriott discovery adapter next**  
**Production writes:** false · **Airtable writes:** false · **Old Census:** not used  
**Webhound session:** [485549eb…](https://webhound.ai/session/485549eb-4541-44fd-86dd-5c7919b839b0) ($5, completed)  
**Artifacts:** `reports/research-engine-v2/webhound-cala-source-discovery-learning.json` · code probe `webhound-cala-source-discovery-code-probe.json`

## Verdict

Official discovery patterns for Marriott, IHG, Hilton, and Choice across Mexico, Dominican Republic, Costa Rica, Colombia, and Panama are clear and repeatable. Webhound was useful as a learning sidecar; it must not become the Census population path.

Dealality recommendation diverges from Webhound’s “build IHG next” preference: **wire Marriott country hotel-sitemaps into Autopilot `source_discovery` first.** Listing extractors already exist and the code probe confirmed live counts (MX 301, DR 25, CR 25, CO 30, PA 17). HQV GraphQL is an enrichment concern, not a discovery blocker.

## What Autopilot already handles

| Pattern | Status |
| --- | --- |
| Hilton Mexico `locations/{brand}/` + ctyhocn | Wired |
| Choice Mexico regional JSON-LD + MXnnn | Wired |
| Marriott country sitemap → MARSHA5 | Extractor exists, **not** wired |
| Marriott HQV coords | Enrichment only |
| IHG destination + hoteldetail | Extractor exists, **not** wired |
| Choice multi-country regional URL builder | Exists; Autopilot Mexico-only |
| VIC claims | Evidence / dedupe only |

## Gaps Webhound + code probe confirmed

1. Marriott CALA sitemaps work for all five countries — Autopilot never calls them.
2. IHG destination pages + `/bin/sitemapindex.xml` hoteldetail XMLs — not wired.
3. Hilton `locations/{country}/` returns 200 + ctyhocn for DR/CR/CO/PA — Mexico-only in Autopilot.
4. Choice regional pages return JSON-LD hotels for all five — Mexico-only in Autopilot.
5. Deprecated Marriott `/en/hotels/{country}.sitemap-hotels.xml` is **404** — do not implement.

## Field availability (summary)

| Family | Identity | Address | Coords | Rooms/keys |
| --- | --- | --- | --- | --- |
| Marriott | MARSHA5 + name + brand heading | Overview HTML | HQV / geocode (not HTML) | Room types ≠ keys |
| IHG | Holidex code + destination cards | Destination cards | Geocode (not HTML) | Room names ≠ keys |
| Hilton | ctyhocn / 7-char code | Cards / property | Maps link or geocode | Optional property |
| Choice | `[CC]###` (MX/DO/CR/CB/PN) | Property page | Static Maps URL on property | Rare |

## Do-not-learn noise

- Webhound approximate counts (Mexico Marriott ~75+ vs code probe **301**)
- OTA prices / TripAdvisor ratings as identity
- Sustainability / pet policy as discovery requirements
- Owner / operator / developer / opening dates
- Company Validated / Brand Verified
- VIC as production SoT

## Blocked / risky patterns

- Marriott HQV without signature harvest + edge bypass
- Property overview 403/Akamai on some Node IPs (Marriott/Hilton/Choice)
- Hilton large property timeouts
- Choice Colombia prefix **CB** (not ISO CO) — use `CHOICE_CENSUS_COUNTRY_PROPERTY_PREFIX`

## Recommended next code changes

1. Wire Marriott country sitemap into `census-autopilot-source-discovery` for CALA slugs.
2. Mark Marriott ready in `CALA_DISCOVERY_ADAPTER_COVERAGE` (and add non-Mexico countries).
3. Parameterize Hilton/Choice Autopilot beyond Mexico.
4. Then wire IHG destination directory.
5. Fixtures + tests for non-Mexico listing samples.
6. Blocked-source rule for deprecated Marriott `sitemap-hotels.xml`.

## Final recommendation options

| Letter | Meaning | Chosen |
| --- | --- | --- |
| A | Code good enough without Webhound | — |
| **B** | **Build Marriott discovery adapter next** | **Yes** |
| C | Build IHG next (Webhound preference) | Defer |
| D | Non-Mexico Hilton/Choice adapters | Parallel / soon after B |
| E | Second narrower Webhound sample | Not needed for listing patterns |
| F | Stop — patterns unreliable | No |

## Change impact

- **Impact:** Low (learning docs + ledger only; no production writes).
- **Rollback:** Delete/ignore these learning reports; no Airtable changes to revert.
