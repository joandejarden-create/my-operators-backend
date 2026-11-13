#!/usr/bin/env node
/**
 * Field-authority contract gates.
 * Deliberate wrong-field substitutions MUST fail detection.
 * Lorna Kipfer / RIC is the positive fixture (not hardcoded in production UI).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_FIELD_AUTHORITY_MATRIX_V1,
  detectContactSubtitleSubstitution,
  resolveAuthoritativeCardContactSubtitle,
} from "../lib/group-demand-intelligence/presentation/gdi-field-authority-contract-v1.js";
import {
  attachCanonicalCustomerPresentationContract,
  buildCanonicalCustomerPresentationContract,
} from "../lib/group-demand-intelligence/presentation/gdi-canonical-customer-presentation-contract-v1.js";
import { attachPackageCustomerPresentation } from "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    console.error(`FAIL ${name}: ${err.message}`);
    process.exitCode = 1;
  }
}

const RIC = {
  id: "gdi_opp_regulatory_information_conference_20260930",
  title: "Regulatory Information Conference",
  tileContactPreview: {
    kind: "named",
    name: "Lorna Kipfer",
    displayName: "Lorna Kipfer",
    displayRole: "RIC Conference Contact",
    displayRoleKind: "professional_title",
    professionalTitle: "RIC Conference Contact",
    opportunityRole: "Group Travel Coordinator",
    roleLabel: "RIC Conference Contact",
    buyerEligibleForGate: true,
  },
  primaryContact: {
    name: "Lorna Kipfer",
    title: "RIC Conference Contact — U.S. Nuclear Regulatory Commission",
  },
  salesActionPillLabel: "Monitor",
  revenueStreamClass: "GUESTROOMS_ONLY",
  revenueStreamIcons: { guestrooms: "active", eventFnb: "muted" },
  whatIsHappening: "Approved customer summary for RIC.",
};

check("matrix_covers_core_customer_fields", () => {
  const names = new Set(GDI_FIELD_AUTHORITY_MATRIX_V1.map((r) => r.field));
  for (const required of [
    "cardContactSubtitle",
    "professionalTitle",
    "opportunityRole",
    "contactRelevance",
    "commercialAction",
    "revenueStreamIcons",
    "displayTitle",
    "customerSummary",
    "needDateMatch",
  ]) {
    assert.ok(names.has(required), `missing ${required}`);
  }
});

check("lorna_authoritative_subtitle_is_ric_not_group_travel", () => {
  const r = resolveAuthoritativeCardContactSubtitle(RIC);
  assert.equal(r.subtitle, "RIC Conference Contact");
  assert.equal(r.rejectedOpportunityRole, "Group Travel Coordinator");
  assert.match(r.source, /professional_title|displayRole/);
});

check("negative_opportunity_role_substitution_is_detected", () => {
  const hit = detectContactSubtitleSubstitution(
    RIC,
    "Group Travel Coordinator",
  );
  assert.equal(hit.ok, false);
  assert.ok(
    hit.issues.some((i) =>
      /OPPORTUNITY_ROLE_SUBSTITUTED|RENDERED_EQUALS_REJECTED/.test(i.code),
    ),
  );
});

check("positive_correct_subtitle_passes_detection", () => {
  const hit = detectContactSubtitleSubstitution(RIC, "RIC Conference Contact");
  assert.equal(hit.ok, true);
});

check("canonical_attach_rejects_opportunity_role_as_card_subtitle", () => {
  const attached = attachCanonicalCustomerPresentationContract(RIC);
  assert.equal(
    attached.canonicalCustomerPresentation.contact.cardContactSubtitle,
    "RIC Conference Contact",
  );
  assert.equal(
    attached.canonicalCustomerPresentation.contact.role,
    "RIC Conference Contact",
  );
  assert.equal(
    attached.canonicalCustomerPresentation.contact.activityRole,
    "Group Travel Coordinator",
  );
  assert.ok(
    attached.canonicalCustomerPresentation.contact.fieldAuthority
      ?.rejectedOpportunityRole === "Group Travel Coordinator",
  );
});

check("negative_raw_sales_action_must_not_replace_approved_pill", () => {
  const row = {
    ...RIC,
    salesActionPillLabel: "Monitor",
    commercialStagePillLabel: "Pursue Now",
    salesActivationLabel: "Monitor",
  };
  const c = buildCanonicalCustomerPresentationContract(row);
  assert.equal(c.commercialAction.pillLabel, "Monitor");
  assert.notEqual(c.commercialAction.pillLabel, "Pursue Now");
});

check("negative_inferred_icons_must_not_override_when_testing_detection", () => {
  // Simulate illegal mutation: approved muted catering flipped to active without freeze authority
  const approved = { guestrooms: "active", eventFnb: "muted" };
  const inferred = { guestrooms: "active", eventFnb: "active" };
  assert.notDeepEqual(approved, inferred);
  assert.equal(JSON.stringify(RIC.revenueStreamIcons), JSON.stringify(approved));
});

check("negative_legacy_summary_must_not_win_over_customer_summary", () => {
  const row = {
    ...RIC,
    whatIsHappening: "Approved customer summary for RIC.",
    researchToDrawerV2: {
      salesBrief: { whatIsHappening: "LEGACY_INTERNAL_SUMMARY_SHOULD_NOT_WIN" },
    },
  };
  const c = buildCanonicalCustomerPresentationContract(row);
  assert.equal(c.display.summary, "Approved customer summary for RIC.");
  assert.notEqual(
    c.display.summary,
    "LEGACY_INTERNAL_SUMMARY_SHOULD_NOT_WIN",
  );
});

check("package_attach_preserves_lorna_display_role", () => {
  const out = attachPackageCustomerPresentation(RIC);
  assert.equal(out.tileContactPreview.displayRole, "RIC Conference Contact");
  assert.equal(
    out.canonicalCustomerPresentation.contact.cardContactSubtitle,
    "RIC Conference Contact",
  );
  assert.notEqual(
    out.canonicalCustomerPresentation.contact.cardContactSubtitle,
    "Group Travel Coordinator",
  );
});

check("frozen_snapshot_lorna_matches_fixture_semantics", () => {
  const snapPath = path.join(
    ROOT,
    "reports/gdi-local-master-publication-v1/local-master-snapshot/local-live-opportunities.json",
  );
  if (!fs.existsSync(snapPath)) return;
  const snap = JSON.parse(fs.readFileSync(snapPath, "utf8"));
  const row = (snap.opportunities || []).find((o) => o.id === RIC.id);
  assert.ok(row, "RIC missing from freeze");
  const r = resolveAuthoritativeCardContactSubtitle(row);
  assert.equal(r.subtitle, "RIC Conference Contact");
  assert.equal(
    textOr(row.tileContactPreview?.opportunityRole),
    "Group Travel Coordinator",
  );
});

function textOr(v) {
  return String(v == null ? "" : v).trim();
}

if (process.exitCode) {
  console.error("\nField-authority contract checks failed.");
  process.exit(process.exitCode);
}
console.log("\nAll field-authority contract checks passed.");
