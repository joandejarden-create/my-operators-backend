# Target Guest Segment — manual Meta prune checklist

Meta API field PATCH returns `INVALID_REQUEST_UNKNOWN` on this base (same as Preferred Brands cleanup). KEEP options were **seeded via typecast**; remaps are applied. Delete obsolete choices in the Airtable UI.

**Snapshot before prune:** `reports/target-guest-segment-meta-ensure-apply.json` → `snapshotBefore` / `verify.*Obsolete`

## Brand Setup - Brand Basics → Target Guest Segments

Delete unused leftover options (records remapped):

- [ ] `Business` *(remapped → Corporate / Business)*
- [ ] `Group / Events` *(remapped → Group / MICE)*

KEEP (14) must remain: Corporate / Business, Leisure, Bleisure, Family, Solo Traveler, Wellness Seeker, Group / MICE, Contract / Extended Stay, Government / Military, International Inbound, Staycation / Local, Digital Nomad, Luxury / Discerning, Experience-Oriented.

## Strategic Intent → Target Guest Segment

Delete renamed leftovers + free-text typecast blobs + unused Other:

- [ ] `Other` *(form no longer offers Other; Brand/Deal KEEP are identical)*
- [ ] `Bleisure (Business + Leisure)`
- [ ] `Family Leisure`
- [ ] `Convention / Meetings`
- [ ] `Tour Groups`
- [ ] Any free-text prose options (e.g. “Regional corporate, SME groups…”, “Airport corporate, meetings…”) — full list in ensure report `verify.dealObsolete`

KEEP (14) must remain — same as Brand.
## After prune

Re-run:

```bash
node scripts/ensure-target-guest-segment-vocab.mjs --dry-run
```

Expect `brandMissing=0`, `dealMissing=0`, `obsolete=0`.
