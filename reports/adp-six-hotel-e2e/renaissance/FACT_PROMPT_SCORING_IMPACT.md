# Fact → prompt → scoring → narrative impact

**Period:** `adp_period_adp_renaissance_times_square_20261016083441_2ff16a` (320/320 retained)  
**Evidence:** runtime observations + published editions; scan artifact `FACT_PROMPT_SCORING_IMPACT.json`  
**Paid remeasurement:** **NOT REQUIRED** (remaining auth $4.47 unused)

| Corrected fact | Entered paid prompts? | Influenced entity matching / scoring? | Influenced recommendations / customer copy? | Fix without new calls? | Targeted remeasure? |
|----------------|----------------------|----------------------------------------|---------------------------------------------|------------------------|---------------------|
| Official URL `nycrn` → `nycrt` | **No** — `nycrn` absent from 320 observations; providers sometimes cited correct `nycrt` URLs | **No** on capture (45% / 36/80 unchanged) | Profile + published property URL patched | Yes | No |
| Guestrooms 310 → **317** | **No** — neither 310 nor 317 in observation corpus | **No** on scenario presence | Profile + published `property.rooms` | Yes | No |
| Broadway Ballroom claim removed | **No as hotel prompt seed** — appears only in **unmentioned** competitor-confusion `rawResponse` (7–11 obs, `mentioned:false`) | **No** on Ren scoring | No ballroom claim in customer actions | Yes (do not promote those responses) | No |
| Bowtie Bar inventory | Sparse in responses (2 hits); not required for capture KPI | Neutral | Profile meeting-space language | Yes | No |
| Shake Shack on-site removed | **No** — zero hits in runtime observations | **No** | Profile dining corrected | Yes | No |
| Traditional `rooftop_bar` attribute removed | Attribute was **expected** in prior reality-gap (profile attrs), not injected as free-text prompt copy. Parser also maps “R Lounge”/“rooftop” → `rooftop_bar` in `attributesRecognized` (66 mentioned obs) | **Yes on reality-gap composition** — prior edition treated rooftop_bar as a property attribute with 76.7% recognition | Prior reality-gap listed rooftop_bar as recognized strength; misleading vs corrected inventory | **Yes** — derived rebuild from existing 320 responses with corrected profile (`PROFILE_FACT_REBUILD_*.json`) | **No** |
| R Lounge freshness uncertain | Mentions appear in provider raw text (39 paths) | Contributes to rooftop_bar recognition via parser synonyms | Do not assert as current primary outlet | Retain uncertainty language; no rematrix | No |

## Rebuild outcome (no paid calls)

- Capture / scenario presence: **45% · 36/80** unchanged  
- Actions: same two (Broadway walking distance; Marquis displacement)  
- Reality-gap: `rooftop_bar` **removed** from expected attributes; gapScore recomputed on corrected attribute set (62.5% on 16 attrs) — not claimed as improvement vs prior edition  
- Raw responses preserved; prior published files backed up under `rebuild-backup/`

## Conclusion

Incorrect identity/inventory facts were **primarily customer-profile / display** issues. The only scoring-surface impact was **reality-gap expected attributes** for `rooftop_bar`, corrected by a **derived republish** without new provider calls. Full 320 rematrix not justified.
