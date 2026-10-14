# Drawer field-gap comparison (Mayflower ACTS + Bethesda Premier Cup)

**As of:** 2026-10-15  
**Method:** research packet → Airtable payload → detail fields → renderer sections

## Mayflower ACTS (`may-acts-ts27` / `gdi_opp_prospect_mv8fnmjw_b8f2b42f`)

| Sales question | Research packet | Airtable (before repair) | Renderer issue | Gap class |
|----------------|-----------------|--------------------------|----------------|-----------|
| Account / occasion | Present (TS27, ACTS) | Title cleaned; org present | OK | — |
| Who travels / why hotel business | Thin (“TS27 attendees”) | Same thin cohort | Shown once; weak | **never researched deeply** (cohort exists but not synthesized) |
| Why this hotel | One-line downtown generic | Same line copied into thesis + fit + summaryWhyHotel | **Duplicate paragraphs** across Event / Why demand / Why matters | **lost during import synthesis** (overlay → single thin string triplicated) |
| Limitations / competition | “hotel not named” only | venueHistory/competitive = boilerplate “Not researched…” | Shows as empty “—” competitive STR/Group | **genuinely unknown** + **boilerplate treated as content** |
| Why investigate now | Dates verified; housing TBA | whyNow = timing string; bookingWindow WATCH | Future-cycle code may leak; timing ≠ sales urgency narrative | **editorial unfinished** |
| Who / route / why | Elizabeth Lancaster package | Contact present; relevance thin | Contact OK | mostly OK |
| Specific sales action | Ask housing + shortlist | Generic default often won over package action | Suggested Action weak | **dropped by preferRicher / defaultAction** |
| Sources | ACTS Translational-Science + Contact | Present | OK | — |
| Location / venue | Washington, DC; hotel not named | city Washington; no destinationStatus nuance | Location row shows raw status codes | **renderer** + thin destination |
| Room demand | Unknown | UNKNOWN | “Unknown” / empty score columns | **acceptable unknown**; **renderer** shows null fit/evidence poorly |
| Numeric fit / confidence | N/A (no canonical score) | hotelFitScore null; note = implementation string | Empty Overall Hotel Fit + “reviewed package did not run…” | **hidden/shown badly by renderer** + **import note not customer-safe** |

**ACTS location fact (open web, 2026-10-15):** Official ACTS pages confirm **April 20–23, 2027, Washington, DC**; registration later; **no named venue or host hotel**. Past meetings alternate (2026 Milwaukee; 2025 DC). Downtown lodging is a **hypothesis**, not a verified block.

## Bethesda Premier Cup (reference depth — do not copy conclusions)

| Strength to reuse | Stale / unsupported risk |
|-------------------|--------------------------|
| Multi-field destination detail; HBC housing path; distinct thesis vs fit; named sources with supportsFact | CONTACT_NOW / “lodging demand path open” may overstate openability; evidenceConfidence 0.85 vs contactConfidence 64 contradiction; Brad vs HBC as primary route tension |

## Waterstone / Renaissance

Spot-check deferred to regression pass after shared renderer fix — same code path; no Mayflower-only template.

## Gap taxonomy counts (ACTS before repair)

| Class | Examples |
|-------|----------|
| never researched | Prior host hotels; peak rooms; exact TS27 venue |
| lost during import | Distinct whatWeKnow vs whyFits collapsed; nextAction often genericized |
| dropped by API | None material (fields present, just thin) |
| hidden / mangled by renderer | Duplicate sections; null scores; internal score note; raw enums |
| genuinely unknown | Room volume; host hotel; overflow need |
| not applicable | Reactivation; on-property meeting host claim |
