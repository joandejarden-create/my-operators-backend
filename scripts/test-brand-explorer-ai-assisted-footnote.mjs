#!/usr/bin/env node
/**
 * Unit checks for Brand Explorer AI-Assisted footnote resolver / gate.
 */
import {
  AI_ASSISTED_PROFILE_LABEL,
  applyBrandExplorerAiAssistedFootnote,
  evaluateAiAssistedProfileFootnoteGate,
  formatBrandExplorerFootnoteDate,
  resolveBrandExplorerAiAssistedFootnote,
} from "../lib/partner-intelligence/brand-explorer-ai-assisted-footnote.js";

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

const fmt = formatBrandExplorerFootnoteDate("2026-07-07");
assert(fmt === "Jul 7, 2026", `date format expected Jul 7, 2026 got ${fmt}`);

const bare = resolveBrandExplorerAiAssistedFootnote({
  governance: { companyValidated: false },
  brand: { slug: "so-hotels-and-resorts", brandExplorer: { blocks: [] } },
  slug: "so-hotels-and-resorts",
});
assert(bare.displayLabel === AI_ASSISTED_PROFILE_LABEL, "default label");
assert(bare.regionBasis === "International Reference", "SO/ region");
assert(/Last Reviewed:/.test(bare.displaySubtitle), "subtitle has last reviewed");
assert(/Source Basis:/.test(bare.displaySubtitle), "subtitle has source basis");
assert(/Region:/.test(bare.displaySubtitle), "subtitle has region");
assert(!/Company Validated|Brand Verified/i.test(bare.displayLabel), "no CV wording");

const brand = {
  slug: "mama-shelter",
  governance: {
    validationStatus: null,
    externalDisplayStatus: null,
    companyValidated: false,
  },
  brandExplorer: { blocks: [] },
};
applyBrandExplorerAiAssistedFootnote(brand, {});
const gate = evaluateAiAssistedProfileFootnoteGate(brand, "");
assert(gate.pass === true, `mama gate should pass: ${gate.failures.join(",")}`);
assert(brand.governance.displayLabel === AI_ASSISTED_PROFILE_LABEL, "mama label");
assert(brand.governance.brandExplorerFootnote.regionBasis === "CALA-informed", "mama region");

const cv = resolveBrandExplorerAiAssistedFootnote({
  governance: { companyValidated: true, lastReviewedDate: "2026-06-01" },
  brand: { slug: "x" },
});
assert(/Company-Validated/i.test(cv.displayLabel), "CV label when validated");
assert(cv.sourceBasis === "Company Validated", "CV source basis");

console.log("test:brand-explorer-ai-assisted-footnote OK");
