# Renaissance Times Square — profile fact verification (ADP refresh)

**Verified at activation:** 2026-10-16  
**Property:** `adp_renaissance_times_square` / HPC `recG66DQJKP2c0UNh`  
**Sources reused:** ADP fixture, GDI canonical profile (read-only), prior CERTIFIED ADP period. Official Marriott overview fetch returned HTTP 404 from this environment — did **not** invent replacements; retained existing supported facts.

## Material facts (supported)

| Fact | Value | Source |
|------|-------|--------|
| Exact property | Renaissance New York Times Square Hotel | ADP profile + GDI profile |
| Official URL | `https://www.marriott.com/en-us/hotels/nycrn-renaissance-new-york-times-square-hotel/overview/` | ADP `officialPropertyPageUrl` / GDI website |
| Brand code hint | NYCRN | brandPropertyPathHints |
| Guestrooms | **310** | ADP + GDI (FACT / MEDIUM) — **not** meeting-room count |
| Meeting space | **5,000 sq ft**, **4** rooms | ADP + GDI |
| Largest room | Broadway Ballroom · 2,400 sq ft · theater 200 | ADP + GDI |
| Dining | R Lounge (rooftop); Shake Shack (ground) | ADP profile |
| Positioning | Design-forward lifestyle · Times Square / Midtown | ADP positioning |
| Access | Midtown / Times Square walkability; near Broadway, Rockefeller, Penn Station (attributes) | ADP attributes — no invented shuttle/accessibility claims |
| Distinct from | Renaissance Midtown / Chelsea / Harlem / Downtown | `identityConfusableExclusions` |

## Address

GDI `identity.address` is currently **null**. City/state/geo (40.7580, -73.9855) present. No fabricated street address written this run.

## Concurrent GDI

- GDI profile path preserved; ADP refresh does not rewrite whole GDI documents.
- GDI Renaissance `$12` authorization remains separate; this ADP run uses `$15` ADP ledger only.
- Profile facts re-verified 2026-10-16 against Marriott `nycrt` + Cvent — see `PROFILE_FACT_RECONCILIATION.md` (rooms 317, Bowtie Bar, no Broadway Ballroom, no on-site Shake Shack).
- GDI six-hotel activation remains unstarted (prep only).
- No GDI opportunities created/reclassified from ADP.

## Corrections persisted this run

None required beyond reuse of existing supported facts. Fixture rooms/meeting inventory left unchanged.
