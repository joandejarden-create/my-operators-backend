# Census Autopilot V1.1 — Final Report (Live Deep Mexico)

**Run:** `cav11_2026-08-08T10-31-18_c65fc2`  
**Version:** census-autopilot-v1.1-live-deep-research  
**Constraints:** no Webhound · no credits · no Airtable · no BE activation · no Cvent/legacy production evidence  
**Runtime:** ~3.1 minutes wall (365 hotels, concurrency 3) · **$0**

---

## MOST IMPORTANTLY

**YES, WITH SPECIFIC BOUNDARIES.**

If Joan says *"Build and maintain the Dealality Hotel Census for Mexico"*, Autopilot can **execute the research program** for supported families (IHG / Hilton / Choice), resolve routine fields via Lane A→B, continue through isolated failures, escalate only true exceptions, and produce a high-completeness verified *staging* Census — **without Joan managing each research step**.

### Boundaries
1. **No automatic Airtable writes** until write-lanes + steward history prove Green fields.
2. **No Webhound auto-spend** — candidates queued only; owner/operator Unknown ≠ automatic Webhound.
3. **Rooms / Keys remain hard** (~4% native) — first-party + structured endpoints before Webhound.
4. **Unsupported families** (deep Marriott, independents, other parents) need adapter coverage before equal autonomy.
5. **Brand Explorer activation** and **image rehost** remain human/governance gated.
6. **Cvent/legacy** stay quarantine challenges only.

---

## Headline results vs V1 baseline

| Metric | V1 (orchestration) | V1.1 (live deep) |
|--------|-------------------:|-----------------:|
| Hotels researched | 365 (index-only) | **365 live** |
| Material completeness avg | 60% | **66%** |
| Production candidates | 99 | **153** (+54) |
| Material remediation | 248 | **194** (−54 promoted) |
| Partial | 18 | 18 |
| Failed hotels | 0 | **0** |
| Amenities native | ~0 (index) | **95%** |
| Phone native | ~0 | **81%** |
| Rooms native | ~0 | **4%** |
| Operator native | ~0 | **1%** |
| External cost | $0 | **$0** |

**≥80% material target: NOT reached** — evidence standards held. Gap is rooms/opening/owner/operator + Choice/Hilton property HTML bot limits (Hilton still reached **81%** material via directory + GraphQL without HTML).

---

## Answers (1–30)

1. **Deeply researched:** **365 / 365** (failed: 0)
2. **Material completeness BEFORE vs AFTER:** **60% → 66%**
3. **Reached ≥80%?** **NO**
4. **If not, why:** Rooms rarely on official pages (4%); owner/operator almost never explicit; Choice property URLs 403; Hilton property HTML blocked (directory/GraphQL still rich); no paid secondaries; **did not invent values**
5. **Native resolution % (material fields):**
   - Property Name / Identity / URL / Country / City / Brand / Family: **100%**
   - Amenities - Source Text: **95%**
   - Affiliation Status: **94%**
   - Phone: **81%**
   - Latitude / Longitude: **42%**
   - Address: **36%**
   - Opening Date: **28%**
   - Rooms / Keys: **4%**
   - Operator / Management: **1%**
   - Owner Name: **0%**
6. **Routinely solvable:** Identity, brand/family, URL, amenities text, affiliation, many phones, Hilton coords/address/opening via directory+GraphQL, IHG amenity pages when fetchable
7. **Remain difficult:** Rooms, Owner, Operator, many openings, Choice page-level fields, Hilton HTML-only fields
8. **Material Remediation → Production Candidate:** **54 / 244** (~22% of remediation cohort promoted without Joan)
9. **Production Candidates now:** **153** (was 99)
10. **Genuine human review:** Deep/hold ~0; steward **batch** on remaining 194 remediation + exception packs — not per-field research ops
11. **Human exception rate:** ~**53%** still touch remediation/deep queues; **routine research autonomous** (amenities/phone/identity/directory). Joan should not run the fetch loop.
12. **First-party validation preferable:** Primarily **rooms** (branded chains with live page but no room count) + **owner** disclosures — hundreds of field gaps, not hundreds of mandatory sends
13. **Webhound-beneficial (realistic):** Rooms/opening hard cases where directories + pages exhausted — **not** every operator/owner Unknown
14. **% hotels needing Webhound (realistic est.):** **~15–25%** (rooms/opening hard cases); prior naive classifier over-counted operator→Webhound at 99% — corrected policy: owner/operator ≠ auto-Webhound
15. **% material fields needing Webhound (realistic est.):** **~5–10%** of material field-slots (dominated by rooms/opening)
16. **Operator relationships independently resolved:** **2** explicit (1% class) — most correctly remain Unknown / Deep Research
17. **Owner relationships independently resolved:** **1**
18. **Cvent challenges:** 100-sample; **0** independently confirmed new hotels from Cvent-alone (correct — needs Lane A directory sweep; **no Cvent values used**)
19. **Legacy challenges:** Coverage signal only; **no legacy values copied**
20. **Unsupported values entered?** **NO**
21. **Recover from source failures?** **YES** (0 aborts; 170 source-blocked pages continued)
22. **Autonomous prioritization?** **YES** (priority engine; Choice incompletes first)
23. **Stop researching a field?** **YES** (ladder + confidence stops)
24. **Future auto-safe candidates?** **YES** — identity, brand, URL, amenities text, many phones, Hilton directory coords/address (see `18-future-write-lanes.md`)
25. **Controlled migration pilot?** **PILOT MIGRATION READY** for Production Candidate subset (staging) — **not** full production write
26. **Rest of Mexico without per-family Joan approval?** **YES — VERIFIED STAGING** for adapter-supported families
27. **CALA ready?** Orchestration yes; coverage **not** — do not launch
28. **Brand Completion → BE Autopilot?** Conditionally — packs ready, `activate=false`
29. **Top 5 engineering improvements:**
    1. Rooms from family structured endpoints / fact sheets (not Webhound-first)
    2. Choice anti-bot / licensed snapshot path (0/68 page_ok)
    3. Hilton property HTML alternative (0/102 page_ok but directory already strong)
    4. Operator portfolio cross-walk from explicit language only
    5. Persist full per-hotel field JSON for resume analytics
30. **NEXT:** Steward review of **153** Production Candidates → Green-field shadow write dry-run → rooms structured-endpoint sprint → Brand Completion sandbox

---

## Class movement

```json
{
  "before": {
    "PARTIAL — NONCRITICAL GAPS": 22,
    "VERIFIED — MATERIAL REMEDIATION REQUIRED": 244,
    "VERIFIED — PRODUCTION CANDIDATE": 99
  },
  "after": {
    "PARTIAL — NONCRITICAL GAPS": 18,
    "VERIFIED — MATERIAL REMEDIATION REQUIRED": 194,
    "VERIFIED — PRODUCTION CANDIDATE": 153
  }
}
```

## Family live yield

| Family | Hotels | page_ok | material avg after |
|--------|-------:|--------:|-------------------:|
| Hilton | 102 | 0 (HTML blocked) | **81%** (directory + GraphQL) |
| Choice | 68 | 0 (403) | **63%** (regional directory) |
| IHG | 195 | 193 | **60%** (hoteldetail amenities/phone) |

## Lane performance

- Lane A directory warmed: Hilton 316 / Choice 156 CALA rows
- Official pages OK: **193/365**
- Lane B standalone sites: rare (few brand pages expose external hotel sites)

External cost this run: **$0** · Webhound calls: **0** · Airtable writes: **0**
