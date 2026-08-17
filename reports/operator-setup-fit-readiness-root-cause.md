# Fit Readiness Root Cause (from Setup audit)

Two different “Fit Ready” concepts exist:

1. **OE diagnostic** `classifyFitDataReadinessDiagnostic`: asg≥6 && mpRows≥3 && brRows≥2 → currently **4** operators
2. **Operator Fit Ranking Ready** (enrichment catalog): requires Setup fields like Active Countries, Management Structures, chainScales, project experience

## Gap class counts (Fit enrichment catalog)

- **OK:** 1
- **DATA EXISTS — SETUP NOT BACKFILLED:** 1
- **DATA EXISTS — NORMALIZED OE NOT MAPPED TO FIT:** 4
- **PARTIAL:** 1
- **FIT EXPECTATION USES SETUP FORM FIELDS — often sparse:** 7

## Primary explanation

Fit Data Ready (OE diagnostic) stays at 4 because thresholds exceed Explorer Strong and many Strong profiles have asg=5 or thin BR/MP **row** counts.

Separately, Fit Ranking Ready stays blocked because Fit still reads **sparse Setup form fields** even when normalized OE already has geography/brand/assignment evidence — i.e. **DATA EXISTS — SETUP NOT BACKFILLED / NOT MAPPED**.
