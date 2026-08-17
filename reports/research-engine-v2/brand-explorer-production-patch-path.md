# Brand Explorer Production Patch Path

**Execute now:** false
**Production patch remains blocked:** true
**Default recommendation:** Option A

## Sandbox source (approved medium pilot)

- Slot namespace: `vic.pilot.medium.*`
- Rows: 28
- Brands: 7
- Founder review: `medium_sandbox_founder_review_approved_pause_vic_lane`

## Options

### Option A (recommended)

- Namespace: `vic.production.pilot.medium.*`
- Create non-rendering production Presentation rows under vic.production.pilot.medium.* so approved evidence is stored without changing owner-facing UI
- Risk: low

### Option B (higher risk)

- Write into live rendered slots — higher risk; requires founder approval + visual review
- Risk: high

## Must not touch

- Brand Status
- release fields
- Company Validated
- Brand Verified
- Recent Momentum
- owner / operator / rooms / open-date / affiliation-start-date fields
- frozen_62 baseline artifacts

## Next step

After Census schema + VIC census write path is approved, run a separate BE Option A dry-run (no execute) for founder approval
