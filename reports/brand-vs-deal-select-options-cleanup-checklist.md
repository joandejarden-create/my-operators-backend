# Brand Setup vs Deal Setup — canonical select options (cleanup)

Generated for Airtable UI cleanup. Goal: same option strings on both sides so Match Score compares apples to apples.

**How to use:** For each field, keep only the **KEEP** list. Remap any records that use a **DELETE / RENAME** value first, then delete the old option.

---

## 1. Hotel Service Model

| Side | Table → Field |
|------|----------------|
| Brand | `Brand Setup - Brand Basics` → **Hotel Service Model** |
| Deal | `Location & Property` → **Hotel Service Model** |

### KEEP (same on both)
1. Full-Service  
2. Select-Service  
3. Lifestyle / Boutique  
4. Extended Stay  
5. All-Inclusive  

### DELETE (Deal only — unused)
- Lifestyle / Full-Service  
- Lifestyle / Select-Service  
- Lifestyle / Wellness Resort  

*(Brand list already matches KEEP.)*

---

## 2. Hotel Chain Scale

| Side | Table → Field |
|------|----------------|
| Brand | `Brand Setup - Brand Basics` → **Hotel Chain Scale** |
| Deal | `Location & Property` → **Hotel Chain Scale** |

### KEEP (same on both)
1. Luxury  
2. Upper Upscale  
3. Upscale  
4. Upper Midscale  
5. Midscale  
6. Economy  
7. Independent  

### DELETE (Deal only)
- Verify Hotel Chain Scale  

---

## 3. Soft / Hard preference

These are **different field designs** — do not force Yes/No onto the deal.

| Side | Table → Field | Type |
|------|----------------|------|
| Brand | `Brand Setup - Project Fit` → **Soft/Collection Brand** | Yes / No |
| Deal | `Strategic Intent - Operational - Key Challenges` → **Soft vs Hard Brand Preference** | preference |

### KEEP — Brand (`Soft/Collection Brand`)
1. Yes  
2. No  

### KEEP — Deal (`Soft vs Hard Brand Preference`) — only these 3
1. Soft Brand  
2. Hard Brand  
3. Unsure / Open to Both  

*(Live records already use only these three.)*

### DELETE — Deal (sample-deal prose / verify — unused)
- Verify Soft vs Hard Brand Preference  
- Hard brand with fee discipline and manageable standards  
- Open to soft/collection first; hard brand if economics justify  
- Operator-led all-inclusive model required  
- Prefer soft/collection; limited standardization flexibility  
- Open — operator capability priority  
- Brand must support mixed-use financing narrative  
- Soft luxury preferred; preservation-first  
- Lifestyle path preferred; soft vs hard still open  
- Preserve identity; soft brand strongly preferred  
- Hard brand with manageable PIP  
- Lifestyle brand supporting mixed-use visibility  
- Hard brand acceptable; prioritize efficient prototype and fee discipline  

---

## 4. Agreements / Deal Structure

Use **Brand wording** as source of truth (matches scoring aliases).

| Side | Table → Field |
|------|----------------|
| Brand | `Brand Setup - Project Fit` → **Acceptable Agreements Type** |
| Deal | `Market - Performance - Deal & Capital Structure` → **Preferred Deal Structure** |

### KEEP (same labels on both)
1. Franchise Only  
2. Third-Party Management Only  
3. Brand + Third-Party Mgmt. (Combined)  
4. Brand + Third-Party Mgmt. (Separate)  
5. Brand-Managed Only  
6. Lease  
7. Joint Venture  
8. Flexible/Open  

### Brand — already close; keep as above
*(Current brand names already match KEEP.)*

### Deal — RENAME records first, then fix options
| Current deal option | Action |
|---------------------|--------|
| Brand + Third-Party Management (Combined) | **Rename option →** `Brand + Third-Party Mgmt. (Combined)` (9 records use it) |
| Brand-Managed | **Rename option →** `Brand-Managed Only` (2 records) |
| Franchise Only | Keep |
| Lease | Keep |
| Joint Venture | Keep |
| Flexible/Open | Keep |
| Third-Party Management Only | Keep / add if missing |
| Brand + Third-Party Mgmt. (Separate) | **Add** if missing |
| Franchise | **DELETE** (unused; duplicate of Franchise Only) |
| Franchise + 3rd Party Mgmt. | **DELETE** (unused) |
| Verify Preferred Deal Structure | **DELETE** |

---

## 5. Project Type

| Side | Table → Field |
|------|----------------|
| Brand | `Brand Setup - Project Fit` → **Acceptable Project Type** |
| Deal | `Deals` → **Project Type** |

### KEEP (same on both)
1. New Build  
2. Conversion / Reflag  
3. Renovation / Repositioning  
4. Expansion / Add-on  
5. Mixed-Use Hospitality Project  
6. Existing Operating Hotel  
7. Adaptive Reuse  

### Brand — DELETE (redundant)
- Conversion *(use Conversion / Reflag)*  
- Repositioning / Rebrand *(use Renovation / Repositioning)*  

### Brand — ADD
- Mixed-Use Hospitality Project  
- Existing Operating Hotel  
- Adaptive Reuse  

### Deal — already has most KEEP items; ADD if missing
- Expansion / Add-on  

### Deal — DELETE
- Other / To Be Confirmed *(or remap to Adaptive Reuse / Existing Operating Hotel first if any record uses it — currently unused)*  

---

## 6. Building Type

| Side | Table → Field |
|------|----------------|
| Brand | `Brand Setup - Project Fit` → **Acceptable Building Types** |
| Deal | `Location & Property` → **Building Type** |

### KEEP (same on both)
1. High-Rise  
2. Mid-Rise  
3. Low-Rise  
4. Mixed-Use  
5. Historic / Renovated  
6. Historic / Adaptive Reuse  
7. Podium / Tower  
8. Resort-Style Compound  

### Brand — DELETE
- Resort *(use Resort-Style Compound)*  
- Urban *(too vague for scoring; remap records if any)*  

### Deal — DELETE
- Verify Building Type  
- High-Rise / Mixed-Use  
- Historic  
- Historic / Mixed-Use  
- Low-Rise Resort  

---

## 7. Project Stage — already aligned

| Side | Table → Field |
|------|----------------|
| Brand | `Brand Setup - Project Fit` → **Acceptable Project Stages** |
| Deal | `Deals` → **Stage of Development** |

### KEEP (same on both — no change needed)
1. Land Under Control Only  
2. Entitlements in Process  
3. Fully Entitled  
4. Under Construction  
5. Stabilized Operating Asset  

---

## 8. Incentive Types — already aligned

| Side | Table → Field |
|------|----------------|
| Brand | `Brand Setup - Operational Support` → **Incentive Types** |
| Deal | `Strategic Intent…` → **Incentive Types Interested In** |

### KEEP — leave as-is (11 matching options)
No cleanup required for parity.

---

## 9. Additional Amenities

Use **Brand Standards** list as source of truth.

| Side | Table → Field |
|------|----------------|
| Brand | `Brand Setup - Brand Standards` → **Additional Amenities** |
| Deal | `Deals` → **Additional Amenities** |

### KEEP (same on both)
1. Bar or Beverage Concept  
2. Business Center  
3. Co-working or Lounge Space  
4. Fitness Center  
5. Grab & Go or Marketplace  
6. Hot Tub/Jacuzzi  
7. Kids' Area or Family Zone  
8. Lobby  
9. Meeting/Event Space  
10. Not Applicable / None  
11. Outdoor Area / Courtyard  
12. Pet Amenities  
13. Pool  
14. Room Service  
15. Spa or Wellness Center  
16. Other Amenities  

### Brand — optional rename for clarity
- `Other Amenities` → keep (or align deal’s `Other Amenities (specify)` **to** `Other Amenities`)

### Deal — DELETE all junk / duplicates (unused on live deals)
- Other Amenities (specify) *(after renaming to Other Amenities if you want parity)*  
- fitness / fitness center  
- business center / business center (public class)  
- Spa / spa / spa services / wellness/spa programming  
- Pools / Outdoor pool / rooftop pool  
- Rooftop bar / Rooftop Bar / Terrace  
- Beach club / beach frontage  
- design-led public spaces (public listing class)  
- design-led villas (public class)  
- entertainment venues (public class)  
- event spaces (public class)  
- heritage architecture (public class)  
- crew-lounge capable meeting cluster (sample)  
- oxygen assistance common in market (inferred)  

*(Live deals already use the clean Brand-list names above.)*

---

## 10. Target Guest Segment(s) — shared vocabulary

> Full contract: [`docs/target-guest-segment-vocabulary.md`](../docs/target-guest-segment-vocabulary.md) · code: `scripts/lib/target-guest-segment-vocabulary.mjs`

| Side | Table → Field |
|------|----------------|
| Brand | `Brand Setup - Brand Basics` → **Target Guest Segments** (multi) |
| Deal | `Strategic Intent - Operational - Key Challenges` → **Target Guest Segment** (multi) |

### KEEP (same 14 strings on both; both multi-select)

1. Corporate / Business  
2. Leisure  
3. Bleisure  
4. Family  
5. Solo Traveler  
6. Wellness Seeker  
7. Group / MICE  
8. Contract / Extended Stay  
9. Government / Military  
10. International Inbound  
11. Staycation / Local  
12. Digital Nomad  
13. Luxury / Discerning  
14. Experience-Oriented  

*(Deal Meta may still have unused `Other` — delete so Meta matches the form.)*

### Remap then DELETE

| Old | New |
|-----|-----|
| Business | Corporate / Business |
| Group / Events | Group / MICE |
| Bleisure (Business + Leisure) | Bleisure |
| Family Leisure | Family |
| Convention / Meetings | Group / MICE |
| Tour Groups | Group / MICE |
| Free-text Deal Meta blobs | Other + Other Text |

### Scripts

- `npm run ensure:target-guest-segment-vocab` (`--dry-run` / `--apply` / `--apply --prune`)  
- `npm run remap:target-guest-segments` (`--dry-run` / `--apply`)  
- `npm run apply-brand-target-guest-segments` (`--dry-run --all` / `--all --correct`)  

**Note:** Meta field PATCH is blocked on this base — KEEP options were seeded via typecast; obsolete choices need manual UI delete per `reports/target-guest-segment-manual-meta-prune.md`.

---

## Suggested cleanup order

1. Soft vs Hard Brand Preference (deal) — delete unused prose options  
2. Hotel Service Model (deal) — delete 3 lifestyle hybrids  
3. Hotel Chain Scale (deal) — delete Verify…  
4. Preferred Deal Structure — **rename** Combined + Brand-Managed, then delete Franchise / Verify / Franchise+3rd  
5. Project Type — align both lists to KEEP  
6. Building Type — align both lists to KEEP  
7. Additional Amenities (deal) — delete junk options  
8. **Target Guest Segment(s)** — ensure KEEP, remap, delete obsolete  

After cleanup, re-run option compare (or ask me to “check again”).
