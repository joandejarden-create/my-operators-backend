# Renaissance New York Times Square — GDI delivery report

**Hotel:** Renaissance New York Times Square Hotel  
**Canonical ID:** `recG66DQJKP2c0UNh`  
**Run ID:** `gdi_workflow_recG66DQ_2026-10-16T10-26-54-849Z_80516d`  
**Authorization:** `auth_gdi_renaissance_ts_12usd_v1` — **$12** (same prior GDI auth; not additional)  
**Execution owner:** `cursor-agent-renaissance-gdi-exec`  
**Worktree:** `C:\Dev\dealality\gdi-renaissance-exec` @ `exec/gdi-renaissance-ts-12usd`  
**Started:** 2026-10-16T10:26:54Z · **Published:** 2026-10-16T11:30:51Z  

---

## Verdict

Research-to-page execution **ran and published** 7 reviewed PRIORITY prospects to Airtable with named contacts and complete drawers. Local `:8080` and production both show **15** customer-visible opportunities after Railway allowlist update (`recG66DQJKP2c0UNh` added; redeploy `f622fc6e` SUCCESS).

---

## Budget

| Item | USD |
|------|-----|
| Cap | 12.00 |
| Parallel discovery (`trun_567f3335d0c140adbc64e005916c92ac`) | 0.21 |
| Surfe | 0.00 (not required after unpaid named-contact upgrade for PLUS) |
| **Estimated spent** | **0.21** |
| Available remaining | ~11.79 |

Ledger: `data/group-demand-intelligence/renaissance-gdi-budget/authorization-ledger-v1.json`  
No ADP / Mayflower funds used. Cap not exhausted.

---

## Inventory (verified, not hardcoded)

| Measure | At start | After publish (Airtable) |
|---------|----------|---------------------------|
| Total unique opportunities | 29 | 36 (29 preserved + 7 created) |
| HOTEL_FACING eligibility (baseline method) | 2 | 9 |
| Legacy/ready surface-visible | 8 | 8 (flag-OFF / allowlist-OFF path) |
| Reviewed package customer-visible (flag ON + hotel allowlisted) | 0 | **7** |
| Unmatched preserved | — | 29 |

Do not remove legacy-visible records to force measure alignment.

---

## Research executed by segment

| Segment | Action |
|---------|--------|
| Associations / conferences | Reuse + fit + contact depth; 7 PRIORITY published (SSSP, NAMT, BLIA, Forum EA, Broadway League, PLUS EPL, ARIAS) |
| Corporate / training | Partial via existing corpus; no separate paid corporate package apply this run |
| Scaling companies | Plan only / reuse path — no net-new publish |
| LMC | Gap remains — not padded |
| Planners / intermediaries | Covered via named meeting contacts on published cards |
| PE venue lodging/referral | Draft PE candidates retained; not auto-published (checkpoint / PE packages not applied this stage) |
| Entertainment / production | Broadway League published; other entertainment gaps remain |
| Exhibitor / sponsor | Noise UI strings excluded; genuine exhibitor orgs held in enrichment queue |
| Fresh Parallel live discovery | Submitted once; **5 candidates quarantined** (competing-venue / mixed geo — Hilton Midtown, Javits, Plaza, etc.) — not auto-published |

Primary horizon: 18 months from run date.

---

## Publish / hold decisions

| Disposition | Count | Notes |
|-------------|------:|-------|
| **PUBLISH** (PRIORITY / HOTEL_FACING) | 7 | Named/functional contacts; drawer pass on dry-run + apply |
| **INTERNAL_HOLD** (enrichment queue) | 16 | Incomplete drawer / UNSUPPORTED_ORG / not facing |
| **EXCLUDE** (noise) | 5 | Webpage UI strings misread as orgs |

Checkpoint: `reports/gdi-six-hotel-e2e/renaissance/publication-package/human-review-checkpoint.json`

### Published accounts (named contact)

1. Society for the Study of Social Problems — Michele Smith Koontz — mkoontz3@utk.edu  
2. National Alliance for Musical Theatre — Adam Grosswirth — adam@namt.org  
3. Boutique & Luxury Lodging Association — Ariela Kiradjian — info@bllaevents.com (named person; org email labeled)  
4. Forum on Education Abroad — Genesis Jardinico — jardinig@forumea.org  
5. The Broadway League — Josh Cacchione — jcacchione@broadway.org  
6. PLUS — Diane Dukes, VP Meetings & Events — ddukes@plusweb.org (upgraded from org inbox via public staff page)  
7. ARIAS U.S. — Larry Schiffer — lschiffer@arias-us.org  

Named-contact vs fallback: **7/7** published have a named person; BLIA uses person-named + org email (channel gap noted; Surfe not spent).

---

## Profile facts (coordinated with ADP)

Synced from ADP-verified profile (do not trust Broadway Ballroom / Shake Shack / NYCRN):

- Rooms: **317**  
- Marriott code: **NYCRT**  
- Largest listed meeting space: **Bowtie Bar ~1,591 sq ft** (theater 100)  
- Config: `config/group-demand-intelligence/hotels/recG66DQJKP2c0UNh.json`  
- Fixture: `fixtures/ai-demand-positioning/renaissance-times-square-property-profile.json`

---

## Publication & verification

| Check | Result |
|-------|--------|
| Airtable apply | **7 create**, 0 quarantine, 7 drawer pass, 29 unmatched preserved |
| Airtable re-read | 36 total; 7 `REVIEWED_PACKAGE_V1`; 7 customer-visible when flag+allowlist ON |
| Local `:8080` list | **15** opportunities; reviewed cards + contacts present; detail OK for PLUS |
| Production list | **15** after allowlist env update + redeploy SUCCESS |
| Production detail/contact | Verified for sample reviewed cards (named person + email) |
| Outreach | **Not sent** |
| Recurring schedule | **Not activated** |

Production host: `https://my-operators-backend-production.up.railway.app`  
Page: `/group-demand-intelligence.html?hotelId=recG66DQJKP2c0UNh`

### Production env (applied)

```
GDI_REVIEWED_PROSPECT_PUBLICATION_V1=1
GDI_REVIEWED_PROSPECT_HOTEL_IDS=recLuxvwwxID7U2B8,rec0eTEnCqlsGJJt9,recnPB3Q02NO8Ma1o,recG66DQJKP2c0UNh
```

Code default allowlist also updated on branch `exec/gdi-renaissance-ts-12usd` (PR open link below).

---

## Code changes (scoped)

1. Add Renaissance to `REVIEWED_PROSPECT_HOTEL_IDS`  
2. Remove hard block on Renaissance `apply_publish`  
3. Enable `GDI_REVIEWED_PROSPECT_PUBLICATION_V1=1` on production apply + pass content overlay  
4. Sync verified rooms/meeting/code into GDI hotel config + ADP fixture  

---

## Remaining coverage gaps

- LMC, scaling companies, PE partnerships — not published this pass  
- Quarantined overflow/competing-venue live discoveries — need explicit supported-fit path if pursued  
- Segment breadth beyond associations incomplete by design (precision over padding)  
- Weekly refresh backlog seeded only — **schedule not activated**  
- Legacy + reviewed title pairs coexist (e.g. “76th Annual Meeting” and “Society for the Study of Social Problems 2027”) — legacy not removed

---

## Weekly refresh backlog (inactive)

See `reports/gdi-six-hotel-e2e/WEEKLY_REFRESH_BACKLOG_SEED.json` — do not enable paid cron.

---

## Separate tasks (untouched)

- Mayflower/San Diego $11 accounting / disputed $2.65  
- ADP Renaissance $15 delivery (separate)  
- Tampa / other queued hotels — **not launched**
