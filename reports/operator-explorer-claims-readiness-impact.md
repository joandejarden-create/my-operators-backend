# Claims / PI Readiness Impact

## Finding

Neither the dry-run `buildProfile` classifier nor the Phase 1 Airtable classifier uses Claims, Claim Category free text, or PI Source links as readiness gates.

## Persistence

- Calibration Claim IDs were already present in Airtable (25 matched / skipped on seed).
- `PI Source Library` field was added on Claims; many rows still lack linked PI when calibration only stored `sourceUrls` text.
- Profile “Recent Momentum” filters Claims by category/subject regex — free-text categories may under-fill that **section**, not readiness class.

## Verdict

**Claims/PI did not cause the 19 → 5 publishable drop.** Section-level Recent Momentum / Evidence may be partial vs dry-run, but Explorer Publishable/Strong thresholds ignore Claims today.
