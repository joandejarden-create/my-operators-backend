# Implementation notes

- Experiment only; proposed corrections never applied.
- Status detection for IHG uses hoteldetail permanence + Book Now / Check Rates / New Hotel banner.
- Marriott often 403s overview HTML; adapter probes `/photos/` pages.
- Avani uses generic adapter (no dedicated Minor directory in V1).
- Operator claims intentionally Unverified — never inferred from brand alone.
- Probe scripts under `scripts/_probe-ihg-*.mjs` are disposable diagnostics.

## Proposed next step

Broaden Marriott + Choice directory coverage, add opening-announcement fetcher for pipeline-only hotels,
then re-run blind benchmark on a larger CALA slice before any apply-gate wiring.
