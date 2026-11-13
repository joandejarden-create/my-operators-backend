/**
 * GDI opportunity card quality — contacts, titles, revenue-stream icons.
 * Run: node scripts/test-gdi-opportunity-card-quality-v1.mjs
 */
import assert from "node:assert/strict";
import {
  mergeFsMirrorFieldsIntoOpportunity,
  isNamedVerifiedContact,
} from "../lib/group-demand-intelligence/fs-mirror-opportunity-enrichment-v1.js";
import {
  normalizeGdiCustomerDisplayTitle,
  stripResearchCommentaryFromDisplayTitle,
  toProperCaseDisplayTitle,
} from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import { attachOpportunityRevenueStreamIcons } from "../lib/group-demand-intelligence/opportunity-revenue-stream-icons-v1.js";
import { TILE_NO_NAMED_CONTACT_LABEL } from "../lib/group-demand-intelligence/universal-tile-contact-preview-v1.js";
import { attachUniversalTileContactPreview } from "../lib/group-demand-intelligence/universal-tile-contact-preview-v1.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

check("title_case_lifts_lowercase_words_in_mixed_titles", () => {
  assert.equal(
    toProperCaseDisplayTitle("CTN / clinical trials network lodging 2027"),
    "CTN / Clinical Trials Network Lodging 2027",
  );
});

check("strip_research_commentary_from_titles", () => {
  assert.equal(
    stripResearchCommentaryFromDisplayTitle(
      "NIH SBPO Vendor Outreach Sessions — recurring Bethesda on-site pattern (next date TBD after April 2026 session)",
    ),
    "NIH SBPO Vendor Outreach Sessions",
  );
  assert.equal(
    normalizeGdiCustomerDisplayTitle(
      "Georgetown Homecoming Weekend 2027 — recommended-hotel outreach",
    ),
    "Georgetown Homecoming Weekend 2027",
  );
});

check("fs_mirror_recovers_named_contact_over_unknown", () => {
  const { opportunity, patches } = mergeFsMirrorFieldsIntoOpportunity(
    {
      id: "gdi_opp_nih_sbpo_vos_pattern",
      primaryContact: { name: "UNKNOWN", email: "NIHSmallBusiness@od.nih.gov" },
      revenueStreamClass: "REVENUE_STREAM_UNCONFIRMED",
      title:
        "NIH SBPO Vendor Outreach Sessions — recurring Bethesda on-site pattern (next date TBD after April 2026 session)",
    },
    {
      id: "gdi_opp_nih_sbpo_vos_pattern",
      primaryContact: {
        name: "Annette Owens-Scarboro",
        email: "scarbora@od.nih.gov",
        role: "Program Lead",
      },
      revenueStreamClass: "GUESTROOMS_ONLY",
      guestroomThesis: "NIH SBPO runs Vendor Outreach Sessions in Bethesda.",
      title: "NIH SBPO Vendor Outreach Sessions",
    },
  );
  assert.ok(patches.includes("primaryContact"));
  assert.ok(patches.includes("revenueStreamClass"));
  assert.ok(patches.includes("guestroomThesis"));
  assert.ok(patches.includes("title"));
  assert.equal(opportunity.primaryContact.name, "Annette Owens-Scarboro");
  assert.equal(opportunity.revenueStreamClass, "GUESTROOMS_ONLY");
  assert.ok(isNamedVerifiedContact(opportunity.primaryContact));
});

check("revenue_stream_icons_guestroom_only", () => {
  const row = attachOpportunityRevenueStreamIcons({
    id: "x",
    revenueStreamClass: "GUESTROOMS_ONLY",
    guestroomThesis: "Documented guestroom demand.",
  });
  assert.equal(row.revenueStreamIcons.guestrooms, "active");
  assert.equal(row.revenueStreamIcons.eventFnb, "muted");
});

check("contact_research_required_label", () => {
  assert.equal(TILE_NO_NAMED_CONTACT_LABEL, "Contact Research Required");
  const previewed = attachUniversalTileContactPreview({
    id: "no-contact",
    primaryContact: { name: "UNKNOWN" },
  });
  assert.equal(previewed.tileContactPreview.kind, "none");
  assert.equal(
    previewed.tileContactPreview.displayName,
    "Contact Research Required",
  );
});

check("revenue_stream_attach_is_idempotent", () => {
  const first = attachOpportunityRevenueStreamIcons({
    id: "idem",
    revenueStreamClass: "GUESTROOMS_ONLY",
    guestroomThesis: "Documented room nights for traveling staff.",
  });
  assert.equal(first.revenueStreamIcons.guestrooms, "active");
  const second = attachOpportunityRevenueStreamIcons({
    ...first,
    guestroomThesis: null,
  });
  assert.equal(second.revenueStreamClass, "GUESTROOMS_ONLY");
  assert.equal(second.revenueStreamIcons.guestrooms, "active");
});

if (failed) {
  console.error(`\n${failed} failing assertion(s)`);
  process.exit(1);
}
console.log("\nAll card-quality checks passed.");
