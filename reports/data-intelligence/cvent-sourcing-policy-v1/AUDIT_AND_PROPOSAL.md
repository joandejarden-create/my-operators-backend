# Cvent Sourcing Rule — Audit & Proposal V1

**Date:** 2026-10-03  
**Mode:** Audit only — **no production behavior changed**  
**Scope:** GDI, ADP, Hotel Intelligence, Hotel Property Census / Airtable writes, research pipelines  
**Status:** `AUDIT_COMPLETE — AWAITING IMPLEMENTATION APPROVAL`

---

## Phase 1 — Audit table (highest-signal locations)

| Location | Current behavior | Data involved | Stored? | Displayed? | Risk / issue | Recommended change |
|----------|------------------|---------------|---------|------------|--------------|-------------------|
| `lib/research-engine-v2/census-cvent-venue-client.js` | Fetches/parses `cvent.com/venues/*`; disk cache under `reports/cvent-venue-cache/` (~290 files + 97 country harvests) | Guest rooms, meeting rooms, listingText/description, address, venue IDs | Yes (disk cache); can feed apply scripts | Indirect via census/HI | **Bulk Cvent venue corpus retained locally** | Treat cache as DISCOVERY_ONLY; block canonical writes; no customer display of listingText |
| `scripts/census-choice-cvent-rooms-fill.mjs` | Explicitly applies Cvent Guest Rooms (+ optional Address) to HPC with Medium confidence | Rooms / Keys, Address, source URL | **Yes — Airtable HPC when `--apply`** | Yes (census UI / downstream) | **Direct Cvent → Rooms / Keys persistence** | Gate with `canPersistAsCanonical=false`; require independent verify; remediating existing rows = flag NEEDS_SOURCE_REVIEW |
| `scripts/census-choice-cvent-bulk-fill.mjs` + `scripts/census-cvent-latam-harvest.mjs` | Harvest / bulk fill Choice + LATAM Cvent venues | Identity URLs, rooms candidates, shell provenance | Harvest JSON + optional Airtable | Shell discovery labels | Harvest OK as discovery; fill path risky if rooms written | Keep harvest; strip/block field SoT writes |
| `lib/research-engine-v2/external-hotel-source-policy.js` | **Approves `cvent` for** Rooms/Keys, Address, Phone, Official URL, Description Source Text, Current Brand, Canonical Name | Census enrichment fields | Policy allows writes when env gates on | Downstream census | **Policy contradicts intended DISCOVERY_ONLY rule** | Remove Cvent from verified-write approvals; add DISCOVERY_ONLY role + hard block for canonical persist |
| `lib/research-engine-v2/external-hotel-source-registry.js` | Registers Cvent as external source candidate | Capability matrix | No (registry only) | Docs/eval | Registry OK if tier = discovery | Reclassify tier / recommended_use to discovery-only |
| `lib/research-engine-v2/census-autopilot-v2-3/cvent-firewall.js` | Fail-closed: independent discovery must not read Cvent content; freeze keeps minimum ID fields only (no rooms/desc) | Challenge IDs / URL hashes | Freeze JSON | No customer | **Good pattern — incomplete coverage outside autopilot v2.3** | Reuse as global gate; extend beyond v2.3 |
| `lib/research-engine-v2/cvent-provenance-audit-v1.js` + `docs/.../cvent-provenance-audit-v1.md` | Read-only HPC audit DR/CR/PA: 256 Cvent-only shells, 144 Cvent+HBX; labeled “Cvent Candidate / Not Field Source” | Discovery Source, Source Candidate Type | Airtable provenance fields | Steward notes | Provenance labeling exists for shells; **does not cover HI meeting attrs or Choice rooms fill** | Extend audit to HI commercial + ADP attrs + rooms fills |
| `lib/hotel-intelligence/research/research-hotel-intelligence.js` | Fetches known Cvent URL (Bethesda hardcoded); extracts yearBuilt/renovated/suites/parking/airport/acres into `proposedCommercial`; tiers as T2_INDUSTRY MEDIUM | Commercial profile fields | Yes if research apply path used | Evidence + HI if applied | **Cvent-only commercial fields can enter HI** | Mark DISCOVERY_ONLY; block persist until independent verify |
| `lib/hotel-intelligence/research/event-space-depth-v2.js` | Source ladder prefers Cvent after first-party incomplete; `parseCventVenueHtml` → sets meeting totals / roomsKeys / event spaces with **`confidence: "HIGH"`** and can persist on apply | Meeting space, room count, capacities, roomsKeys | **Yes on apply** | HI Event Spaces / Commercial → ADP attrs / GDI fit | **Critical: Cvent treated as HIGH structured authority** | Cap Cvent at DISCOVERY_ONLY / UNVERIFIED; never HIGH; never GDI verified scoring input |
| `lib/hotel-intelligence/research/venue-page-parsers-v2.js` | Parses Cvent HTML into commercial + eventSpaces | Same as above | Via callers | Via callers | Parser OK for discovery; confidence labeling wrong upstream | Keep parser; callers must set sourceRole=DISCOVERY_ONLY |
| `lib/hotel-intelligence/research/evidence-depth-v2.js` | Classifies `cvent.com/venues/` as `TIER_B_STRUCTURED_VENUE`; family CVENT in ladder | Authority ranking | N/A | Research routing | Tier B implies stronger than discovery-only | Downgrade Cvent to DISCOVERY_ONLY authority class |
| HI universe expansion / discovery-factory / coverage-dashboard | Uses `cvent_candidates` as **coverage stock / reopen queue**, not field SoT | Candidate counts, cvent_id | Scores/queues | Internal dashboards | Acceptable as discovery benchmark | Keep; document as coverage signal only |
| `lib/group-demand-intelligence/*` | Cvent mostly as **event platform / registration / housing** signal (web.cvent.com events), not venue attribute SoT; also hygiene demotion of cvent.com directories | Event URLs, contact ontology | Opportunity evidence URLs | Sometimes as source links on events | Different product surface (events ≠ hotel facts); still must not scrape venue profiles into hotel capability | Separate event-platform Cvent from venue-profile Cvent in policy |
| `lib/ai-demand-positioning/**` | No direct Cvent client code | Indirect via HI/ADP attributes / notes mentioning Cvent corroboration (e.g. Hotel Phillips notes) | Profiles/evidence JSON | Customer ADP if attrs derived from HI | **Indirect risk** if HI stores Cvent as HIGH | Provenance-aware ADP attr sync; block discovery-only fields |
| `api/` | No Cvent string matches | — | — | — | Low direct API risk | Ensure customer APIs never surface discovery-only observations as facts |
| ADP fixtures (Waterstone, Phillips, etc.) | Occasional notes citing Cvent among corroborators | Narrative notes | Fixture/evidence | Possible in evidence packs | Low if not sole source | Flag sole-Cvent claims in remediation |
| Autopilot v1–v4 / full-CALA shell insert docs | Widespread Cvent as discovery mix for shells | Identity candidates | HPC shells | Steward | Mostly labeled Not Field Source | Preserve; block field-level SoT drift |

---

## A. CURRENT STATE

Dealality already has **partial** Cvent discipline in Census:

1. **Shell provenance language** — “Cvent Candidate / Not Field Source” and prior provenance audit (DR/CR/PA shells).
2. **Autopilot v2.3 Cvent firewall** — fail-closed against using Cvent content during independent discovery; freeze stores opaque challenge IDs only.
3. **Coverage/universe tooling** — Cvent candidate stock used as a **benchmark / reopen queue**, which matches “coverage signal” intent.

But the same repo also has **active Cvent→field paths** that violate the new rule:

1. **HI event-space depth** can parse Cvent venue pages and persist meeting inventory / roomsKeys as **HIGH** confidence.
2. **HI general research** extracts Cvent commercial facts into proposed profiles (MEDIUM, T2).
3. **Census Choice rooms-fill scripts** write Rooms/Keys from Cvent.
4. **External hotel source policy still lists `cvent` as approved** for Rooms/Keys, Address, Phone, Description Source Text, Brand, etc.
5. **Local bulk cache** `reports/cvent-venue-cache/` (~290+ venue files) is a retained Cvent-derived corpus (even if not all applied to Airtable).
6. **GDI** primarily uses Cvent as an **event/registration platform**, not hotel venue SoT — lower risk for property attributes, but hotel capability inputs come from HI/HPC and inherit any Cvent-persisted facts.
7. **ADP** has no direct Cvent scraper; risk is **inherited** through HI → ADP Attributes.

There is **no global** `SourceRole` / `canPersistAsCanonical` / `canUseForScoring` / `canDisplayToCustomer` API used by HI + GDI + ADP together. Policy is fragmented (census external policy ≠ HI evidence tiers ≠ GDI scoring).

---

## B. RISKS / GAPS

| Severity | Gap |
|----------|-----|
| **P0** | HI `event-space-depth-v2` can canonicalize Cvent meeting facts at HIGH and feed GDI/ADP |
| **P0** | `external-hotel-source-policy` approves Cvent for Rooms/Keys and description Source Text |
| **P0** | `census-choice-cvent-rooms-fill` can write Rooms/Keys from Cvent alone |
| **P1** | HI research Bethesda path persists Cvent-only commercial fields without independent verification gate |
| **P1** | Evidence authority treats Cvent venues as Tier B structured (too strong) |
| **P1** | No unified customer-display gate for discovery-only observations |
| **P2** | Disk venue cache = bulk Cvent retention (discovery OK if not promoted; policy must say so) |
| **P2** | Remediations incomplete outside DR/CR/PA shell audit; Choice rooms fills / HI attrs not inventoried |
| **P2** | GDI event URLs on `web.cvent.com` are fine for events; must not be confused with venue-attribute ingestion |
| **P3** | ADP notes occasionally cite Cvent among sources — usually corroborative, not sole SoT |

**Interpretation risk:** “We labeled Discovery Source = Cvent” does **not** prevent later pipelines from using Cvent-parsed numbers as HIGH facts.

---

## C. PROPOSED ARCHITECTURE (minimal, global, reusable)

Add one shared module (suggested path):

`lib/data-intelligence/source-policy/v1/`

- `source-roles.js` — `PRIMARY | AUTHORITATIVE | INDEPENDENT_SECONDARY | DISCOVERY_ONLY | UNVERIFIED | PROHIBITED_FOR_PERSISTENCE`
- `domain-classifiers.js` — host → role (includes `cvent.com`, `web.cvent.com` venue vs event split)
- `gates.js` — `canPersistAsCanonical`, `canUseForScoring`, `canDisplayToCustomer`, `requiresIndependentVerification`
- `observation-model.js` — `PropertyObservation` / `PropertyCanonicalField` shapes
- `conflict-resolve.js` — prefer Tier-1/2 independent over discovery
- `overview-synthesis.js` — factual synthesis from canonical structured fields only (no Cvent prose)

**Wire points (additive, fail-closed for Cvent venue facts):**

1. `external-hotel-source-policy` — remove Cvent from verified write approvals; call shared gates
2. HI `event-space-depth-v2` + `research-hotel-intelligence` — observations only until independent verify
3. ADP attribute sync — skip / mark provisional if sourceRole DISCOVERY_ONLY
4. GDI hotel capability scoring — exclude DISCOVERY_ONLY from verified fit inputs; confidence downrank
5. Census apply scripts — refuse Cvent-only Rooms/Keys/description writes

**Do not** build a new scraper. Keep parsers for discovery/QA comparison only.

---

## D. AIRTABLE IMPACT

| Base / table | Fields at risk | Change type |
|--------------|----------------|-------------|
| HPC `appCCUsuGsE1ifoLk` · Hotel Property Census | Rooms/Keys, Address, Phone, Hotel Description*, Discovery Source, Source Candidate Type, Notes, Official URL, Brand | **Additive** provenance flags; **no mass overwrite** |
| Platform `appa2cE7FTRmIbB32` · Hotel Commercial Profiles | rooms, suites, meeting totals, capacities, airport, renovation, notes | Additive verificationStatus / primarySource / discoverySource |
| Hotel Event Spaces | sqFt, capacities, sourceUrl | Flag Cvent sourceUrl rows NEEDS_SOURCE_REVIEW |
| Hotel Intelligence Evidence | source name/url/tier | Allow DISCOVERY_ONLY evidence rows; block promotion |
| Hotel ADP Attributes | derived values | Re-sync after HI provenance fix; do not invent |
| GDI Opportunities | hotel-fit inputs (indirect) | No schema destroy; scoring reads verification |

**Suggested additive fields (only if missing; do not invent names without schema confirm):**  
Verification Status, Primary Source, Primary Source URL, Supporting Sources, Discovery Source, Confidence, Verified Date, Research Notes, Needs Source Review.

Preserve historical values; flag rather than delete.

---

## E. GDI IMPACT

- **Discovery of events** via Cvent registration/housing pages: **keep** (event platform), with URL classifier separating venue vs event hosts.
- **Hotel capability / fit scoring:** must read provenance-aware HI/HPC fields; if only DISCOVERY_ONLY → treat as unknown/provisional, lower confidence, do not treat as verified rooms/meeting capacity.
- **Reports/UI:** show verified capabilities separately from unknown; never show Cvent venue prose as hotel overview.
- **No change** to event qualification gates beyond excluding unverified hotel facts from fit math.

---

## F. ADP IMPACT

- No direct Cvent client today.
- **Block path:** HI Cvent HIGH → ADP Attributes → customer positioning / comps.
- Property profile generation / competitor similarity must use canonical verified attributes only.
- Overview copy: synthesize from structured canonical fields; ban Cvent listingText as source text.

---

## G. EXISTING DATA REMEDIATION

| Class | Evidence so far | Action |
|-------|-----------------|--------|
| **A** Independently supported | Many brand/official HI rows; shells with HBX+independent | Retain; attach provenance if missing |
| **B** Likely correct, weak provenance | Choice Cvent rooms fills; HI MEDIUM Cvent commercial fields; any Event Space with `cvent.com/venues` sourceUrl | Flag `NEEDS_SOURCE_REVIEW`; queue independent verify |
| **C** Conflicts with authoritative | Research already prefers Marriott over Cvent when both present (Bethesda pattern) | Prefer authoritative; keep Cvent as observation |
| **D** Copied Cvent prose | `listingText` / Description Source Text approved for Cvent in policy; cache contains descriptions | Do not display; replace with Dealality synthesis when regenerating |

**Known quantified slice (prior audit, not full estate):** DR/CR/PA shells — 256 Cvent-only + 144 Cvent+HBX (400 with Cvent provenance).  
**Not yet counted:** live HI Commercial/Event Space rows with `cvent.com` sourceUrl; Choice rooms-fill apply history; full CALA beyond that audit.  
**Next non-destructive step:** read-only inventory scripts (no writes) for HI + HPC rooms/description fields whose only URL host is cvent.com.

---

## H. IMPLEMENTATION PLAN (small reversible phases)

| Phase | Action | Destructive? | Can start now? |
|-------|--------|--------------|----------------|
| **0** | Land shared `source-policy` module + domain classifier + unit tests (Cvent venue = DISCOVERY_ONLY) | No | **YES** |
| **1** | Policy patch: remove Cvent from verified `FIELD_SOURCE_APPROVALS`; hard-block canonical persist/scoring/display | No (blocks future bad writes) | **YES** |
| **2** | HI event-space + research: tag Cvent parses DISCOVERY_ONLY; never set HIGH; never persist as canonical without independent source | No if dry-run first | **YES** (code) |
| **3** | Disable/guard `census-choice-cvent-*-fill` apply paths behind independent-verify requirement | No | **YES** |
| **4** | ADP attr sync: skip discovery-only for verified attributes; mark provisional | No | **YES** |
| **5** | GDI capability scoring: exclude DISCOVERY_ONLY verified-scoring inputs | No | **YES** |
| **6** | Read-only remediation inventory → queue Class B/C/D | No | **YES** |
| **7** | Overview synthesis helper (no Cvent prose) | No | After 0–2 |
| **8** | Observability dashboard (% verified after Cvent discovery, blocked counts) | No | After 0 |
| **9** | Targeted independent verification batches (costly) | Writes only with dry-run + confirms | Later |
| **10** | Customer-facing copy refresh where Class D proven | Controlled | Later |

**Do not** mass-delete HPC/HI values. **Do not** alter Bethesda certified ADP baselines.

---

## I. TEST PLAN

**Automated (new):**

1. Cvent observation alone → `canPersistAsCanonical === false`
2. Cvent + official hotel confirm → canonical allowed with official as primary
3. Conflict Cvent vs brand → brand/authoritative wins; Cvent retained as observation
4. GDI scoring rejects DISCOVERY_ONLY rooms/meeting inputs
5. ADP attr builder rejects discovery-only as verified facts
6. Airtable payload builder attaches discoverySource vs primarySource correctly
7. Overview synthesizer refuses Cvent listingText
8. Unverified existing fixture flagged NEEDS_SOURCE_REVIEW (not deleted)
9. Generic `https://www.cvent.com/venues/...` URL classified without adapter self-ID
10. Customer API/report serializers strip discovery-only from canonical fact maps

**Manual:**

- Dry-run HI event-space on a hotel with Cvent hit → no Airtable write of meeting totals
- Confirm GDI fit unchanged for hotels with fully independent HI
- Confirm ADP Bethesda baseline untouched

---

## J. COST / PERFORMANCE IMPACT

| Item | Impact |
|------|--------|
| Policy gates / classifiers | Negligible CPU |
| Independent verification when Cvent flags a gap | Extra SerpAPI/http fetches per field (existing ladder already budgets queries) |
| Stopping Cvent canonical fills | **Saves** apply risk; may leave rooms/meeting null longer (correct: unknown > fabricated) |
| Remediation inventory | One-time Airtable read cost |
| Not building bulk Cvent DB | Avoids ongoing cache growth as SoT |

---

## Immediate safe sequence (recommended next)

1. Implement shared source-policy module + tests (**no Airtable writes**).
2. Patch `external-hotel-source-policy` + HI event-space confidence/persist gates.
3. Guard Choice Cvent rooms-fill apply.
4. Run read-only remediation inventory; produce Class A–D queue.
5. Only then run independent verification for high-value GDI/ADP hotels.

**STOP before mass schema changes or live remediations until founder approves Phase 0–3.**
