# Census Autopilot V1.2 — Golden Census 95% Final Report

**Run:** `cav12_2026-08-08T11-17-46_bc5e27`  
**Hotels:** 365 Mexico IHG+Hilton+Choice  
**Cost:** $0 · **Webhound:** 0 · **Airtable writes:** 0

## MOST IMPORTANTLY

**YES, WITH SPECIFIC BOUNDARIES**

Can Dealality Census Autopilot build a hotel record that is ≥95% complete across Priority hotel information (Identity/Geography, Physical Profile, Amenities, F&B, Meetings, Dealality Classification, Content) while keeping Lifecycle, Ownership/Operation, Images, and Governance as separate tracks?

**Answer: YES, WITH SPECIFIC BOUNDARIES**

Architecture and geography/classification/content tracks work. Primary blocker(s): Rooms / Keys, Address, Latitude. Rooms/Keys native completion remains the critical gap (4.4%). Lifecycle/ownership/images correctly excluded from Priority 95%.

---

## Exact answers (1–40)

1. **Final Golden Census Priority Schema:** Priority groups G1–G7 as in `01-golden-census-schema.md` / `02-golden-census-field-registry.json` (version `census-autopilot-v1.2-golden-schema`).
2. **Fields counting toward Priority Completeness:** 59 Priority-track fields in registry; denominator uses REQUIRED + applicable CONDITIONAL only (OPTIONAL excluded from score).
3. **Required:** Property Name; Current Brand; Brand Family; Official Property URL; Address; City; State / Region; Country; Continent; Sub-Continent; Market; Submarket; Latitude; Longitude; Phone; Rooms / Keys; Property Type; Resort / Urban; Amenities - Source Text; Amenities - Structured Tags; F&B Flag; Meeting / Event Space; Dealality Segment / Positioning; Hotel Description - Source Text
4. **Conditional:** Official Property ID; Suites; Asset Context; All-Suite Flag; Beach / Beachfront; Ski; Restaurant Count; Total Meeting Space; Largest Meeting Room / Ballroom; Number of Meeting Rooms; Hotel Description - AI Summary
5. **Optional:** Postal Code; Floors; Boutique Flag; Mixed-Use Flag; Branded Residences Flag; Pool; Spa; Fitness; Golf; Beach Club; Casino; Kids Club; Club Lounge; All-Inclusive; Parking; Airport Shuttle; Residences Amenity; Restaurant Names; Bars / Lounges; Rooftop F&B; Signature / Third-Party Restaurant; Room Service; Ballroom; Convention Hotel
6. **Excluded — Lifecycle:** Affiliation Status; Opening Date; Expected Opening Date; Renovation Date; Conversion / Reflag Date
7. **Excluded — Ownership/Operation:** Owner Name; Developer Name; Operator / Management Company; Operation Type
8. **Governance/Provenance:** Source URL; Source Type; Data Confidence Tier; Production Use Status; Last Verified
9. **Baseline Priority Completeness (NEW schema):** 65.5% avg raw; 0% hotels ≥95%.
10. **Final Priority Completeness:** 86.8% avg raw; material-weighted 85.7%.
11. **Average ≥95%?** NO
12. **% hotels individually ≥95%:** 8.5%
13. **Reached 100%:** 0
14. **90–94.9%:** 70
15. **Remain <90%:** 264 (80–89.9: 233; <80: 31)
16. **Top 5 incompleteness fields:** Rooms / Keys (impact=1396, 4.4%); Address (impact=928, 36.4%); Latitude (impact=636, 41.9%); Longitude (impact=636, 41.9%); Phone (impact=204, 81.4%)
17. **Rooms / Keys completion:** 4.4%
18. **Hotel Identity & Geography:** 87%
19. **Amenities:** 89.6%
20. **F&B:** 91.8%
21. **Meetings & Groups:** 79.7%
22. **Physical Profile:** 74.4%
23. **Content:** 100%
24. **Market completion:** 100%
25. **Submarket completion:** 100%
26. **Continent/Sub-Continent ~100%?** Continent 100%; Sub-Continent 100%
27. **Unknown applicable field-cells remaining:** 1341
28. **Fields needing first-party validation (primary):** Rooms / Keys (+ meetings metrics / F&B counts where still Unknown) — see escalation map
29. **Hotels that would benefit from Webhound:** 318
30. **Estimated % needing Webhound to reach 95%:** 87.1%
31. **Cvent in production evidence?** NO
32. **Legacy in production evidence?** NO
33. **Unsupported values staged?** NO
34. **Autopilot continue gap-attack without Joan?** YES — multi-pass loop ran autonomously
35. **Know when passes diminish?** YES — Pass 3 gated on delta≥0.3pp + remaining researchable gaps
36. **Brand validation packs?** YES — design in `19-first-party-validation-pack-design.md` (not sent)
37. **95% sustainable for new hotels?** CONDITIONAL — sustainable for geo/amenities/classification; Rooms/Keys needs structured sources or first-party to hold 95%
38. **What must change to maintain ≥95%:** stronger rooms structured sources per family; Dealality geo steward for Other Mexico; first-party rooms packs; optional Webhound only for hard rooms gaps
39. **Ready for all Mexico Golden Census?** NOT YET for 95% claim — expand geography taxonomy yes; hold 95% marketing until Rooms/Keys ladder improves
40. **Next:** Engineer IHG/Hilton/Choice rooms structured extraction + first-party rooms validation packs; keep autonomous gap loop; do not weaken Unknown policy

---

## Separate track scores (not in Priority 95%)

- Lifecycle: 24.4%
- Ownership/Operation: 0.2%
- Image: 0%
- Governance: 100%

## Firewall confirmation

- Cvent production evidence: **NONE**
- Legacy production evidence: **NONE**
- External cost: **$0**
