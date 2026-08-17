/**
 * TEMPLATE — Section Pattern Parity content pack (copy for each new brand).
 *
 * Future Brand Explorer builds MUST ship Recent Momentum this way:
 * 1. Copy this file → brand-explorer-section-pattern-parity-content-<slug>.js
 * 2. Fill momentumCards with buildRecentMomentumCard (title, dateLine, summary, https url)
 * 3. Register in CONTENT_BY_SLUG in brand-explorer-section-pattern-parity-content.js
 * 4. Dry-run: npm run brand-explorer-section-pattern-parity-remediation -- --brands <slug> --dry-run
 * 5. If Bodies were scrubbed: npm run brand-explorer-momentum-announcement-link-restore -- --brands <slug> --dry-run
 *
 * Contract: lib/partner-intelligence/brand-explorer-recent-momentum-contract.js
 * Do NOT write untitled footprint.momentum blobs from tab-factory remediation.
 */
import {
  buildRecentMomentumCard,
  RECENT_MOMENTUM_DEFAULT_LABEL,
} from "./brand-explorer-recent-momentum-contract.js";

/** @example Replace EXAMPLE_* with stewarded brand-specific announcement URLs. */
const EXAMPLE_OPENING_URL =
  "https://example.com/news/replace-with-stewarded-brand-announcement";

export const TEMPLATE_SECTION_PATTERN_PARITY_CONTENT = Object.freeze({
  brandSlug: "replace-with-brand-slug",
  brandName: "Replace With Brand Name",
  replaceMomentum: true,
  momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
  momentumCards: [
    buildRecentMomentumCard({
      title: "Named Opening Or Conversion Headline With Geography",
      dateLine: "2025",
      summary:
        "Owner-useful interpretation of the opening/conversion/membership signal—property or market context plus what to diligence next. Not development-page filler.",
      url: EXAMPLE_OPENING_URL,
      sort: 1,
    }),
    buildRecentMomentumCard({
      title: "Second Named Activity Card Newest First After Sort",
      dateLine: "2024",
      summary:
        "Second dated openings/press card with brand-specific geography and owner relevance. Trailing URL required for Proper Case hyperlink in Brand Explorer.",
      url: EXAMPLE_OPENING_URL,
      sort: 2,
    }),
  ],
  geoIntro:
    "Brand-specific geographic footprint intro (owner-useful, not parent-umbrella only).",
  regions: [
    {
      slotKey: "footprint.region.am",
      title: "Americas",
      body: "Region card body with brand-specific pattern language.",
      sort: 11,
    },
  ],
});
