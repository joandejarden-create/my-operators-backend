# Hotelbeds LIVE Room Count Validation

`HOTELBEDS_LIVE_PREFLIGHT_COMPLETE`  
`HOTELBEDS_LIVE_ROOM_COUNT_VALIDATION_COMPLETE`

## Verdict

**`HOTELBEDS_LIVE_ACCESS_BLOCKED`**

Controlled 10 and full HBX-linked frozen-sample LIVE enrich were **not executed**.

Reason: LIVE credentials missing; active config is TEST; refused to send TEST keys to LIVE host.

---

## 1. Safety

```text
Airtable writes: 0
Census writes: 0
Brand Explorer writes: 0
Automatic merges: 0
Schema changes: 0
Migrations: 0
Secrets exposed: no
ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES: 0
ENABLE_HBX_CENSUS_WRITES: 0
```

## 2. LIVE Preflight

```text
TEST credentials: PRESENT (HBX_API_KEY / HBX_API_SECRET)
LIVE credentials: MISSING (HBX_API_KEY_LIVE / HBX_API_SECRET_LIVE)
Active environment: test
Active host class: TEST_HOST (api.test.hotelbeds.com)
Content API flag: ENABLE_HBX_CONTENT_API=1
signature check: PASS
clock check: PASS
Content API reachable LIVE: false
HTTP status: n/a (no LIVE call)
quota status: UNKNOWN (LIVE not called)
rate-limit status: UNKNOWN
sanitized error: HBX_API_KEY_LIVE and HBX_API_SECRET_LIVE missing; HBX_ENV=test; refusing to send TEST credentials to LIVE host
```

## 3. Room Count Field Semantics

```text
HOTELBEDS_ROOM_COUNT_FIELD_CONFIRMED: UNCERTAIN
field: roomsNumber (adapter also accepts roomCount / numberOfRooms / totalRooms)
raw type: not observed on LIVE this run
example sanitized value: n/a (no LIVE hotel-content response)
adapter mapping: extractRawHotel → rooms_total_value → candidate.room_count
```

Supporting prior evidence (not LIVE this run):

- Dealality HBX rooms field hunt (2026-08-09): **true hotel-level Rooms/Keys in HBX: NO (not proven)**; `rooms[]` = room-type catalog; live hotels payloads scanned for roomsNumber: **0** under quota.
- Public Hotelbeds Content API docs emphasize room **types / occupancies / facilities**, not a documented hotel-level total-keys inventory field.
- Smoke test note: do not derive Rooms/Keys from `rooms[]` length.

Therefore: even after LIVE access is fixed, semantics must be confirmed on a real LIVE hotel payload before scaling.

## 4. Frozen Sample Eligibility

Seed: `hotel-intelligence-cala-validation-v1`

```text
400 total
HBX-linked: 86
HBX-linked missing room count: 72
HBX-linked with existing room count: 14
```

Country mix (HBX-linked): Colombia 22 · Mexico 21 · Costa Rica 17 · Panama 14 · Dominican Republic 12

## 5. 10-Hotel Controlled Test

**NOT RUN** — blocked by LIVE access gate.

## 6–13. Full Sample / Recovery / Efficiency

**NOT RUN**

```text
ROOM_COUNT_RESPONSE_RATE: n/a
ROOM_COUNT_RECOVERY_RATE: n/a
AUTO_ACCEPT_ELIGIBLE_ROOM_COUNTS: 0 (no LIVE candidates)
```

## 14. 3,016-Hotel Projection

**Not extrapolated from LIVE yield** (no LIVE observations).

Eligible live census HBX codes: **3,016** — potential only if LIVE access + roomsNumber semantics confirmed.

## 15. Post-HBX Census Gap

```text
POST_HBX_ROOM_COUNT_GAP_ESTIMATE: UNKNOWN (LIVE not measured)
live missing rooms: 5,765
HBX-linked live: 3,016
non-HBX missing rooms (structural floor if HBX covered all linked): >= 5,765 - min(3,016, recoverable)
```

If LIVE later shows roomsNumber sparsely populated (as prior TEST/wave1 evidence suggests), the post-HBX gap may remain close to **5,765**.

## 16. Hotelbeds Verdict

**`HOTELBEDS_LIVE_ACCESS_BLOCKED`**

## 17. 1,000 Verified Rooms

```text
DEALALITY_1000_VERIFIED_ROOMS_FEASIBLE: UNCERTAIN
```

Cannot assess until LIVE credentials exist and `roomsNumber` (or equivalent) is confirmed as total keys on real payloads.

## 18. Highest-Value Next Step

**`FIX_HOTELBEDS_ACCESS`**

Provide `HBX_API_KEY_LIVE` + `HBX_API_SECRET_LIVE`, set `HBX_ENV=live` only for validation runs (or resolve LIVE config without sending TEST keys to LIVE host), then re-run this exact validation protocol.

Do not execute controlled/full enrich until that is done.
