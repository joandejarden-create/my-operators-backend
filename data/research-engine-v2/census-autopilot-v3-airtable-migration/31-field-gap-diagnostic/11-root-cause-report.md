# Root Cause Report — V3 blank fields

**Run:** `cav3_2026-08-08T15-04-05-566Z`  
**No Airtable writes performed by this diagnostic.**

## Exact root causes

### 1. State / Region — EXPECTED (cohort) + latent BUG (writer)
1. V2.3 `toDiscoveryRecord()` does not store `state` / `state_region` on `physical`.
2. `resolveDealalityGeography()` returns market/submarket/continent but **not** State / Region.
3. `classifyFieldWrites()` in `dry-run.js` **never calls** `add("State / Region", …)` even though `field-policy.js` lists it `AUTO_WRITE_SAFE` and mapping exists.
4. Result for this cohort: **0/150** staging → **0** Phase 1 mutations → **0** Phase 2 writes.

### 2. Address — EXPECTED (cohort) + latent BUG (writer)
1. Discovery freeze does not persist address (physical has name/city/country/url/id/lat/lng only).
2. `field_evidence` empty for all 150 cohort research IDs.
3. Classifier never `add("Address")` despite `CORROBORATED_WRITE` / official_page policy.
4. Address was **not** counted in the 1,050 SerpApi-blocked rows.

### 3. Submarket — EXPECTED blanks; NOT a write omission for matched rows
1. Geography ran for all 150.
2. **46** had `geography.submarket` → included in dry-run → **46** transaction writes.
3. **104** `submarket_confidence: No Match` / `no_corridor_match` (city often admin region or postal code).
4. Golden V1.2 100% Submarket does **not** transfer: V3 re-resolved from discovery city labels, did not inherit prior Golden geography artifacts.

### 4. Latitude / Longitude — BUG for official values
1. Freeze has official coords for **60** properties: Hilton **40**, Choice **20**, SerpApi **0**.
2. Policy text: `serpapi_blocked_unless_official`; source-rights allow `coords_if_official_structured`.
3. Implementation: `for (const f of policy.blocked_rights) blocked.push({value:null…})` — **no check** of freeze `physical.lat/lng`.
4. Official higher-authority claims were blocked by a blanket SerpApi-class list (contamination / fail-closed without exception).

### 5. Phone — EXPECTED
1. **0/150** phone values in freeze.
2. Listed in `blocked_rights`; not in `INSERT_ALLOWED_FIELDS`.

## Code fixes needed (do not apply in this diagnostic)

1. **dry-run.js `classifyFieldWrites`:**
   - `add("State / Region", …)` when independently derived/researched.
   - `add("Address", …)` when official (non-SerpApi) evidence exists.
   - For Lat/Lng/Phone: only BLOCKED_RIGHTS when claim is SerpApi-only; if official freeze/page evidence exists → CORROBORATED_WRITE / AUTO as policy allows.
2. **pilot-selection / discovery:** persist official address/phone/state when adapters provide them; derive State / Region in geography where possible.
3. **geography:** improve city normalization (postal ≠ city) to raise Submarket match rate.
4. **INSERT allowlist:** add Phone only when official writes are enabled.

## Records affected

| Issue | Count | Keys source |
|-------|------:|-------------|
| Official coords blocked | 60 | `05-latitude-longitude-audit.json` → `official_eligible_property_identity_keys` |
| Submarket No Match | 104 | pilot selection geography |
| State/Address/Phone no staging | 150 | freeze physical |

## Safe immediate backfill (approved independent evidence)

- Latitude + Longitude blank-fill for **60** Airtable records (see `10-corrective-backfill-dry-run.json`).
- **Do not** backfill Address / Phone / State from SerpApi.
- **Do not** invent Submarket for No Match rows.

## Must remain blank pending clarification / research

- SerpApi-only Address/Coords/Phone/Amenities/Descriptions
- Phone (no official staging yet)
- Address (no official staging yet)
- State / Region (not derived yet)
- Submarket No Match set (needs geography work)

## Final verdicts

| Field | EXPECTED or BUG |
|-------|-----------------|
| State / Region | **EXPECTED** |
| Address | **EXPECTED** |
| Submarket | **EXPECTED** (blanks); writes correct when present |
| Latitude | **BUG** (60 official suppressed) |
| Longitude | **BUG** (60 official suppressed) |
| Phone | **EXPECTED** |
