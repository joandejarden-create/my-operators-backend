# CALA Tribute Property Visual Candidate Discovery v1/v2

**Status:** Metadata discovery + registry staging (Tribute Portfolio pilot) — **v2 cover-image resolver**
**Module:** `lib/partner-intelligence/cala-tribute-property-visual-discovery.js`
**Script:** `npm run cala-tribute-property-visual-discovery -- --dry-run`

## Purpose

Tribute Portfolio is text/governance Platform Ready and the Brand Asset Registry has slot-governance fields populated, but weak global hero/gallery candidates remain **Not Enough Context** because they are not named CALA Tribute hotel/property images.

This module discovers **real, named Tribute Portfolio hotel/property visual candidates** in CALA markets from **Marriott-controlled sources** (country hotel sitemaps + official property pages). It stages metadata-only registry record proposals — it does **not** download images, attach files, overwrite Brand Setup, or approve Explorer use.

## Commands

```bash
# Default — crawl CALA sitemaps, probe property pages, propose registry records
npm run cala-tribute-property-visual-discovery -- --dry-run

# Skip live page probes (sitemap + seeds only)
npm run cala-tribute-property-visual-discovery -- --dry-run --no-probe

# Use Puppeteer when Marriott overview pages return access_denied (slower)
npm run cala-tribute-property-visual-discovery -- --dry-run --use-puppeteer

# Gated apply — create metadata-only registry records
npm run cala-tribute-property-visual-discovery -- --apply --approve-cala-tribute-visual-candidates
```

## v2 cover image resolution

For each named Tribute property, the module now:

1. Fetches the official Marriott **`/overview/`** page first.
2. Resolves the **property cover image** in priority order:
   - `og:image` meta tag
   - `twitter:image` meta tag
   - Primary/hero image in `__NEXT_DATA__` JSON
   - First image on `/photos/` page (fallback)
3. Fetches **`/photos/`** separately for **gallery** candidates (excluding the cover URL).
4. Labels each candidate `imageRole: cover` or `gallery`.
5. Uses the **cover image** for Hero + value-driver proposals; gallery slots use non-cover photos.

Optional `--use-puppeteer` (or `CALA_TRIBUTE_DISCOVERY_USE_PUPPETEER=1`) retries blocked pages with headless browser — still metadata-only, no binary download.

## Discovery sources (v1)

1. Marriott CALA country hotel sitemaps (`marriott-brand-directory-extract.js`)
2. Seed list from hotel census / overview import (named Tribute CALA properties)
3. Official Marriott property overview + photos pages (HTML image URL refs only — no binary download)

**Not used:** OTA images, Google Images, generic brand lifestyle crops, unnamed `trbcl.*` consumer-site images.

## Slot matching

| Slot | Requirements |
|------|----------------|
| Hero Image | Named CALA Tribute property + brand-confirmed + Marriott-controlled image URL |
| Image Gallery | Different named properties; mix of contexts; CALA preferred |
| Recent Openings | Specific opening/PR + date (v1: typically Missing) |
| Where This Brand Creates the Most Value | Image matches inferred property setting (Urban, Resort, etc.) |

## Registry behavior

- Dry-run proposes metadata-only records with **Candidate** / **Candidate Only** / **Pending Review**
- Dedupes on Brand Record ID + Source URL + Recommended Explorer Slot + Related Property Name
- Never sets **Approved For Explorer Use**
- Weak existing generic candidates remain unchanged

## Related

- [brand-explorer-visual-slot-requirements-v1.md](./brand-explorer-visual-slot-requirements-v1.md)
- [brand-asset-registry-workflow-v1.md](./brand-asset-registry-workflow-v1.md)
- [tribute-visual-asset-slot-review-v3.md](./tribute-visual-asset-slot-review-v3.md) — groups competing candidates and recommends primary/alternate per slot

## Future v2

- Rendered Source Capture for access-denied Marriott property pages
- Recent Openings PR/date linkage
- Value-driver image gap fill for drivers still Missing
