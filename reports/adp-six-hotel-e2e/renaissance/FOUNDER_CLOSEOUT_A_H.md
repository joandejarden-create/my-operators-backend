# Renaissance ADP customer delivery — founder closeout (A–H)

**Hotel:** Renaissance New York Times Square (`adp_renaissance_times_square` / `recG66DQJKP2c0UNh`)  
**Research period:** `adp_period_adp_renaissance_times_square_20261016083441_2ff16a`  
**ADP authorization:** `$15` (`auth_adp_renaissance_ts_15usd_v1`) — estimated incurred **$10.53**, remaining **$4.47**, actual billed **UNKNOWN** (ledger verified; no additional paid ops this closeout)  
**Branch:** `fix/adp-renaissance-delivery-closeout`

---

## A. Can the customer open the corrected production report, actions, and both PDFs?

**Report + actions (production share path):** YES after this PR deploys (pre-deploy: share resolve/report already OK for period `…2ff16a` at 45% / 36/80 / 2 actions).  
**PDFs:** Current PDF regenerated from corrected published edition (6 pages; text scan: no Broadway Ballroom / Shake Shack / nycrn / storage stubs; Marquis + 45% presence retained). Monthly v2 retained from prior closeout visual pass.  
**Post-deploy required:** re-click production share PDF downloads (mark NOT RUN until deploy completes).

Share token id (not bearer): `sht_06ba4cd8b5a5cdb865d5109a`. Usable URL only in gitignored local file.

---

## B. Did any corrected fact require targeted remeasurement?

**No paid rematrix.** See `FACT_PROMPT_SCORING_IMPACT.md`.

| Fact | Paid rematrix? |
|------|----------------|
| nycrt / 317 rooms / Shake Shack / Bowtie inventory | No — not in paid prompt bodies; profile/display |
| Broadway Ballroom | No — only in unmentioned raw responses |
| `rooftop_bar` expected attribute | **Derived rebuild** from existing 320 responses (no new calls) |

Capture remains **45% (36/80)**. Family **0/6** sample finding retained. No 40%→45% improvement claim.

---

## C. Can compacted Airtable records be read safely without losing report content?

**YES — contract implemented + tested.**  
Stubs resolve from PRIMARY_SOT or load returns null → filesystem fallback. See `COMPACTION_READ_PATH_PROOF.md`.  
Live Airtable reads remain disabled until explicitly enabled.

---

## D. Do new runtime publications and customer inputs survive redeployment?

| Class | Status |
|-------|--------|
| Git-bundled published report/evidence/PDFs/share registry | Survive deploy with this PR |
| Filesystem PRIMARY_SOT adapter write→read (isolated probe) | PASS — `DURABILITY_PROBE.json` |
| Ephemeral Railway runtime-only writes without volume | **NOT PROVEN** — precise blocker: no durable volume mounted for ADP runtime; do not claim otherwise |
| Customer action notes/status | Preserved on derived rebuild; not modified for testing |

---

## E. What remains unverified?

1. Post-deploy production PDF click-path on share page  
2. AI Admin authenticated browser — **NOT RUN** (no Memberstack session)  
3. Actual provider billed $ (still UNKNOWN)  
4. Matched-scope 65↔80 trend comparison (explicitly non-comparable)  
5. Railway ephemeral runtime durability without volume  

---

## F. What was merged/deployed?

*Filled at merge/deploy time.*

| Item | Value |
|------|-------|
| Branch | `fix/adp-renaissance-delivery-closeout` |
| Base | `origin/main` @ `2c58fef4` |
| Closeout commits | `3512e0da` + follow-up compaction/rebuild commits |
| PR | *(create/merge this closeout)* |
| Deploy | `npm run deploy:production` from clean main |

---

## G. What did this closeout cost and what remains in the $15 authorization?

| Item | USD |
|------|-----|
| Prior research (smoke + 320 monitoring) | 10.53 estimated |
| This closeout paid calls | **0.00** |
| Remaining authorization | **~$4.47** |
| Actual billed | UNKNOWN |

---

## H. What is the actual status of the separate Renaissance GDI execution?

**Separate $12 GDI execution (`auth_gdi_renaissance_ts_12usd_v1`) — PUBLISHED_VERIFIED** (distinct from this ADP closeout; not launched from here).

- Run: `gdi_workflow_recG66DQ_2026-10-16T10-26-54-849Z_80516d`  
- Spent ~$0.21 Parallel; 7 reviewed prospects published; production list 15 after allowlist  
- Report: `reports/gdi-six-hotel-e2e/renaissance/FOUNDER_DELIVERY_REPORT.md`  
- September historical GDI runs are **not** this $12 E2E  

Do not mix ADP/GDI ledgers. Tampa/other hotels not started from this closeout. Waterstone HOLD unchanged.

---

## Status labels (honest)

| Gate | Status |
|------|--------|
| Research completion (320) | COMPLETE |
| Certification | CERTIFIED (prior; re-attached on rebuild) |
| Canonical publication (filesystem) | COMPLETE (corrected edition) |
| Production customer delivery | PENDING_DEPLOY then VERIFY |
| Runtime durability (ephemeral) | NOT PROVEN |
| Full E2E COMPLETE | **Not until post-deploy verify** |
