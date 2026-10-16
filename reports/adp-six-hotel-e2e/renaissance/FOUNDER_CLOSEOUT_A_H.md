# Renaissance ADP customer delivery — founder closeout (A–H)

**Hotel:** Renaissance New York Times Square (`adp_renaissance_times_square` / `recG66DQJKP2c0UNh`)  
**ADP profile:** `adp_renaissance_times_square`  
**Research period:** `adp_period_adp_renaissance_times_square_20261016083441_2ff16a`  
**ADP authorization:** `$15` (`auth_adp_renaissance_ts_15usd_v1`) — estimated incurred `$10.53`, remaining `$4.47`, actual billed **UNKNOWN**  
**Branch work:** `fix/adp-renaissance-delivery-closeout` from `origin/main` @ `2c58fef4`  
**No full 320-call rerun. No other hotel started. No GDI/ADP fund mixing.**

---

## A. Can the customer open the correct production report and PDFs?

**Report + action plan (production share path): YES** (verified live).

- Production resolve + `/property/.../report` return Renaissance, period `…2ff16a`, **45%** scenario presence (**36/80**), **2** actions, hotel isolation OK.
- Customer page: `/owner-ai-demand-share.html?share=…` (usable URL only in protected `PRODUCTION_SHARE_URL.local.txt`).
- Token `sht_06ba4cd8b5a5cdb865d5109a` minted with **Railway production** signing secret.
- DEV-signed prior token rejected on production (`invalid_share_signature` → 401).

**PDFs:** Local current + monthly PDFs regenerated and visually inspected (every page rasterized).  
**Production customer PDF download path:** admin routes exist; share surfaces load report/actions via API. After this PR deploys, production filesystem SoT will include corrected property facts + PDF edition `v4` archive.

**Pending until deploy:** production still serves pre-patch rooms `310` / `nycrn` URL until published snapshot + code land on Railway.

---

## B. Where is every required artifact durably stored?

| Artifact | Authoritative store | Notes |
|----------|---------------------|-------|
| Published report JSON | **Filesystem PRIMARY_SOT** `data/ai-demand-positioning/published/adp_renaissance_times_square/` | Git-deployed; period `…2ff16a` |
| Evidence index | Filesystem `evidence-…2ff16a.json` | Full evidence not mirrored into Airtable |
| Runtime raw period | Filesystem `data/ai-demand-positioning/runtime/` | Includes certified live + excluded dry-run stub |
| Current-report PDF | `reports/ai-demand-positioning/current-report-pdf/...` + dealality archive `…_v4` | Prior PDF editions retained under archive / `pdf-history/` |
| Monthly review + PDF | `reports/.../monthly-review/archive/.../2026-10/..._v2/` | Cover date corrected to **OCT 16, 2026** |
| Airtable Live overlay | Record `rech8MWMcTRiCDlJm` | Compacted; evidence stubbed with `payloadStoreRef` |
| Customer actions (report) | Embedded in published payload (`actions`, 2) | Monthly review has expanded 5-action agenda (non-contradictory) |
| Share registry | `config/client-share/adp-share-registry/` | ACTIVE token id + revoked DEV ids |

**Durability caveat (explicit):** Railway filesystem persistence for *newly generated* runtime data is only as durable as the deploy/git SoT and configured volume. Successful reload of **git-bundled** published files is not proof that ephemeral runtime writes survive redeploy. Canonical customer edition for this delivery is the **published snapshot + archived PDFs** committed/deployed with this closeout. Airtable is an optional **Live overlay/index**, not PRIMARY_SOT.

---

## C. Is Airtable persistence resolved, or what remains limited?

**Resolved for this hotel’s Live overlay upsert.**

- Failure was payload size (~97.5k > 95k Airtable field limit).
- Shared fix: `compactPayloadForAirtableOverlay` stubs large nested blobs (`evidence`, …) with stable `payloadStoreRef` — no mid-string truncation.
- Applied upsert: **OK**, record `rech8MWMcTRiCDlJm`, compacted payload **94076** chars, stubbed `evidence`.
- Readers that need full evidence must use filesystem PRIMARY_SOT via `payloadStoreRef` / evidence file — Airtable evidence field is a storage stub when oversized.

**Remaining limitation:** Airtable History remains policy-disabled; Live overlay is not a full evidence mirror.

---

## D. Which hotel facts changed, and did conclusions change?

| Claim | Classification | Correction |
|-------|----------------|------------|
| Official URL `nycrn` | **Contradicted** (404) | → `nycrt` Marriott overview/events |
| Street address Two Times Square / 714 7th Ave | **Supported and current** | Kept |
| 310 guestrooms | **Contradicted** (stale) | → **317** (Cvent; Marriott overview omits count) |
| ~5,000 sq ft / 4 meeting rooms | **Supported and current** | Marriott events FAQ |
| “Broadway Ballroom” 2,400 / theater 200 | **Contradicted** | Westin inventory; Ren largest listed room **Bowtie Bar** 1,591 sq ft / theater 100 (Marriott). Cvent also lists combined Bowtie Bar & Main Floor 2,500 / 200 cocktail — not Broadway Ballroom |
| R Lounge | **Supported but freshness uncertain** | Overview emphasizes Bowtie Bar rebrand; older NYE copy still mentions R Lounge |
| Shake Shack on-site | **Contradicted** | Not an on-site Ren outlet |
| Traditional “rooftop bar” attribute | **Contradicted** | Terrace rooms + Bowtie views; removed `rooftop_bar` from profile attributes |

**Impact on certified research conclusions:** **No scoring remeasure.** Capture remains **45% (36/80)** from existing 320 responses. Profile/display + recommendation wording for dining/meeting inventory updated. Actions (walking distance to Broadway; Marquis displacement) unchanged and still supported by evidence. Family **0%** retained as sample finding, not universal invisibility.

---

## E. Were both PDFs visually inspected?

**YES** — every page of current (6) and monthly v2 (9) rendered to PNG under `pdf-visual/` and reviewed.

Fixes applied after inspection:
- Removed false **+45.0 pts vs last run** (dry-run stub prior).
- Monthly cover monitoring date **SEP 2 → OCT 16, 2026**.
- KPI baseline date → **October 16, 2026**.
- Trends deltas across 65→80 scope labeled **Not comparable scope** (no fake improvement).

Prior PDF bytes preserved under `pdf-history/` and archive editions `v1`…`v4`.

---

## F. Were any committed access credentials found and remediated?

**YES — usable share bearers found; signing secret not found in tracked files.**

| Item | Finding | Action |
|------|---------|--------|
| PR #110 / ad-hoc issued-share URL files | Usable `adpshare.v1.…` bearers | Redacted in tracked copies; local URL gitignored |
| `production-share-contract-tokens.json` | Contained usable bearer | Redacted to `[REDACTED]` |
| DEV tokens `sht_691d…`, `sht_57ee…` | DEV-signed / invalid on prod | Recorded in `revoked-token-ids.json`; prod rejects DEV signature |
| Replacement | `sht_06ba4cd8b5a5cdb865d5109a` | Production-signed; ACTIVE |
| Signing secret | Not printed; not copied to local | Railway env only |

**History note:** Removing a bearer from the tip commit does not erase Git history; capability validity was addressed by production-secret issuance + DEV rejection. No force-push / history rewrite.

---

## G. What remains incomplete?

1. **Deploy this PR** so production SoT picks up rooms/URL corrections, PDF v4, longitudinal dry-run guard, Airtable compaction, share registry, and redactions.
2. **Post-deploy production PDF click-path** re-check on the customer share page (mark NOT RUN until deploy).
3. **AI Admin** authenticated surfaces — NOT RUN in this session (no admin session); share + public report APIs verified.
4. **Actual provider billed $** still UNKNOWN (ledger estimated only).
5. **Matched-scenario 65↔80 comparison** not certified — scope caveat retained (no improvement claim).
6. **GDI $12** separate track — prior COMPLETED GDI runs exist; do not call “not activated”; this closeout did not start a duplicate GDI job.
7. Controlled Railway redeploy durability probe for *new* runtime writes — documented architecture; not simulated against live customer data.

---

## H. Is this now a repeatable ADP delivery path for the next hotel?

**Mostly yes, with explicit gates:**

1. Authorize hotel-specific ADP ledger (do not borrow).  
2. Run research → certify → publish filesystem PRIMARY_SOT.  
3. Issue share via **production** signing path (never DEV secret in prod).  
4. Upsert Airtable Live overlay with compaction (evidence stub + `payloadStoreRef`).  
5. Generate current + monthly PDFs; **visually** inspect every page.  
6. Verify production share resolve/report/actions; revoke any committed DEV bearers.  
7. Reconcile profile facts from official sources before customer language.  
8. Keep other hotels / Waterstone HOLD / unapproved `$57` untouched.

**Repeatability blockers removed this closeout:** prod share signature mismatch, Airtable payload size, dry-run false deltas, stale profile URL/rooms/ballroom, monthly cover date bug.

---

## Final IDs

| Item | Value |
|------|-------|
| Research period | `adp_period_adp_renaissance_times_square_20261016083441_2ff16a` |
| Capture | 45% · 36/80 scenarios · 86/320 observations |
| Share token | `sht_06ba4cd8b5a5cdb865d5109a` |
| Airtable Live record | `rech8MWMcTRiCDlJm` |
| Current PDF archive | `adp_current_2026-10-16_adp_renaissance_times_square_v4` |
| Monthly review | `adp_mr_renaissance_times_square_2026-10_v2` |
| Base commit | `2c58fef4` (PR #110 merge) |
