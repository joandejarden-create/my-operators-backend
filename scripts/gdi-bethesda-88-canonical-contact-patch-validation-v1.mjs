#!/usr/bin/env node
/**
 * Validate the 15 canonical Bethesda contact dry-run patches.
 * No opportunities.json writes. Emits founder approval-candidate manifest.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadHotelOpportunityDoc } from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import {
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID,
  extractContactSnapshot,
  mergeContactAntiClobber,
  COVERAGE_BUCKET,
  buildCanonicalContactDryRunPatches,
} from "../lib/group-demand-intelligence/research/bethesda-88-contact-quality-completion-v1.js";
import { evaluateSharedCommercialEligibility } from "../lib/group-demand-intelligence/commercial-eligibility-contract-v1.js";
import { evaluateAccountDevelopmentEligibility } from "../lib/group-demand-intelligence/account-development-eligibility-v1.js";
import { CONTACT_ELIGIBILITY_CLASS } from "../lib/group-demand-intelligence/contact-eligibility-standard-v1.js";
import { writeJsonAtomic } from "../lib/group-demand-intelligence/research/gdi-research-stage-persistence-v1.js";
import {
  applyBethesdaCommercialEventFamilyPresentation,
  buildBethesdaCommercialEventFamilyCrosswalk,
} from "../lib/group-demand-intelligence/research/bethesda-88-commercial-event-family-v1.js";
import { isGenericOrganizationEmail } from "../lib/group-demand-intelligence/contact-display-surface-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT_DIR = path.join(
  ROOT,
  "reports/gdi-bethesda-88-commercial-certification-v1"
);
const MANIFEST_PATH = path.join(
  REPORT_DIR,
  "03-canonical-59-contact-dry-run-manifest.json"
);
const SCORECARD_PATH = path.join(REPORT_DIR, "01-record-level-scorecard.json");

/** Original 15 proposed canonical contact patches (mission baseline). */
const ORIGINAL_15_PROPOSED_IDS = Object.freeze([
  "gdi_opp_series_request_a_conference_registration_website_2026",
  "gdi_opp_2027_nci_rna_biology_symposium_7",
  "gdi_opp_series_nci_rna_biology_symposium_ncif_conference_2027",
  "gdi_opp_amwa_2028_annual",
  "gdi_opp_arlington_spring_tournament_2027",
  "gdi_opp_georgetown_homecoming_2027",
  "gdi_opp_series_new_in_the_library_august_2026",
  "gdi_opp_accp_annual_meeting_20261001",
  "gdi_opp_umd_alumni_weekend_watch",
  "gdi_opp_amwa_2027_annual",
  "gdi_opp_nih_sbpo_vos_pattern",
  "gdi_opp_amwa_2027_interim",
  "gdi_opp_series_ctn_annual_conference_2027",
  "gdi_opp_ecs_251_2027_overflow",
  "gdi_opp_series_march_15_17_bethesda_md_ctn_dissemination_2027",
]);

const CONTACT_ONLY_KEYS = new Set([
  "primaryContact",
  "secondaryContact",
  "contactRecovery",
  "primaryContactName",
  "primaryContactRole",
  "primaryContactEmail",
  "primaryContactPhone",
  "functionalContactEmail",
  "organizationContactEmail",
  "contactSourceUrl",
  "contactVerifiedAt",
  "whyThisContact",
  "salesReady",
  "customerFacing",
  "customerVisible",
  "publicationApproved",
  "publication",
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function validatePatch(p, byId) {
  const live = byId.get(p.id);
  const ev = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[p.id];
  const reasons = [];
  let verdict = "PASS";

  if (!live) {
    return {
      id: p.id,
      verdict: "FAIL",
      reasons: ["OPPORTUNITY_ID_NOT_FOUND"],
    };
  }
  if (ev?.rejected === true || ev?.founderValidation === "FAIL") {
    return {
      id: p.id,
      title: p.title || live.title || null,
      organizationName: p.organizationName || live.organizationName || null,
      verdict: "FAIL",
      proposedName: text(ev?.primaryContact?.name) || text(p.after?.name) || null,
      proposedTitle: text(ev?.primaryContact?.title) || null,
      contactClass: text(ev?.primaryContact?.contactClass) || null,
      emailClassification: "REJECTED",
      directEmail: null,
      orgRoute: text(ev?.primaryContact?.organizationContactRoute) || null,
      sourceUrl: text(ev?.primaryContact?.sourceUrl) || text(p.after?.sourceUrl) || null,
      whyRelevant: text(ev?.rejectionReason || ev?.primaryContact?.whyRelevant) || null,
      before: extractContactSnapshot(live),
      afterProjected: extractContactSnapshot(live),
      fieldDiff: p.fieldDiff || null,
      family: [],
      reasons: [
        ev?.rejectionReason || "FOUNDER_VALIDATION_FAIL",
        "EXCLUDED_FROM_APPROVED_CANDIDATE_MANIFEST",
      ],
      requiresFounderApproval: true,
      opportunitiesJsonWriteAuthorized: false,
    };
  }
  if (!ev || ev.inventory !== "canonical") {
    reasons.push("EVIDENCE_MISSING_OR_NOT_CANONICAL");
    verdict = "FAIL";
  }

  const before = extractContactSnapshot(live);
  if (before.named && before.name && before.name !== p.after?.name) {
    reasons.push(`WOULD_CLOBBER_EXISTING_NAMED:${before.name}`);
    verdict = "FAIL";
  }

  const pc = ev?.primaryContact || {};
  const name = text(pc.name);
  if (!name) {
    reasons.push("NO_NAMED_PERSON");
    verdict = "FAIL";
  }

  const cls = text(pc.contactClass);
  if (
    cls !== CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT &&
    cls !== CONTACT_ELIGIBILITY_CLASS.DIRECT_BUYER
  ) {
    reasons.push(`CONTACT_CLASS:${cls || "MISSING"}`);
    if (verdict !== "FAIL") verdict = "UNCERTAIN";
  }

  const ownsOrg =
    text(pc.emailOwnership).toLowerCase() === "organization_or_department" ||
    text(pc.emailOwnership).toLowerCase() === "role_inbox" ||
    pc.directPersonEmail === false;
  const hasDirect = Boolean(text(pc.email) && text(pc.email).includes("@") && !ownsOrg);
  const hasOrg = Boolean(
    text(pc.organizationContactRoute) ||
      (ownsOrg && text(pc.email)) ||
      text(pc.organizationContactRoute)
  );
  if (!hasDirect && !hasOrg) {
    reasons.push("NO_OUTREACH_ROUTE");
    verdict = "FAIL";
  }
  if (ownsOrg && p.after?.hasDirectEmail === true) {
    reasons.push("MANIFEST_AFTER_MISLABELS_ORG_ROUTE_AS_DIRECT");
    if (verdict === "PASS") verdict = "UNCERTAIN";
  }
  if (!text(pc.sourceUrl)) {
    reasons.push("MISSING_SOURCE_URL");
    verdict = "FAIL";
  }
  if (!text(pc.whyRelevant) || text(pc.whyRelevant).length < 40) {
    reasons.push("WEAK_COMMERCIAL_RELEVANCE_RATIONALE");
    if (verdict === "PASS") verdict = "UNCERTAIN";
  }

  // Per-opportunity relevance gates
  if (/carrie wagerman/i.test(name)) {
    reasons.push(
      "CARRIE_WAGERMAN_FREDERICK_NODE_3423_404_AND_SYMPOSIUM_LODGING_ROLE_NOT_INDEPENDENTLY_CONFIRMED"
    );
    verdict = "FAIL";
  }
  if (/john lewis/i.test(name)) {
    reasons.push(
      "ECS_OFFICIAL_HOUSING_IS_MARQUIS_WEWCC_OVERFLOW_ONLY_CONTACT_OK_FOR_WATCH"
    );
  }
  if (/lisa c\.?\s*miller/i.test(name)) {
    reasons.push(
      "ACCP_2026_CONFIRMED_SALT_LAKE_CITY_BETHESDA_FIT_REMAINS_RESEARCH_GATED_CONTACT_OK"
    );
  }
  if (/kelly mcmahon/i.test(name)) {
    if (!ownsOrg || !text(pc.organizationContactRoute)) {
      reasons.push("AMWA_ASSOCIATEDIRECTOR_MUST_BE_ORG_ROUTE");
      verdict = "FAIL";
    }
  }
  if (/vanessa otarola kehoe/i.test(name)) {
    if (!ownsOrg) {
      reasons.push("GEORGETOWN_ADVANCEMENT_EVENTS_ROUTE_MUST_BE_ORG_LABELED");
      if (verdict === "PASS") verdict = "UNCERTAIN";
    }
  }

  const merged = mergeContactAntiClobber(live, ev);
  const afterSnap = extractContactSnapshot(merged);

  // Field consistency: declared DIRECT must persist as person email, not org inbox.
  if (hasDirect && text(pc.email)) {
    if (text(afterSnap.email).toLowerCase() !== text(pc.email).toLowerCase()) {
      reasons.push(
        `DIRECT_EMAIL_NOT_PERSISTED:expected=${pc.email};got=${afterSnap.email || "null"}`
      );
      verdict = "FAIL";
    }
    if (isGenericOrganizationEmail(afterSnap.email)) {
      reasons.push(`ORG_INBOX_MISLABELED_AS_DIRECT:${afterSnap.email}`);
      verdict = "FAIL";
    }
  }
  if (hasOrg && text(pc.organizationContactRoute)) {
    const orgPersisted = text(afterSnap.orgEmail || afterSnap.orgRoute).toLowerCase();
    if (
      orgPersisted &&
      orgPersisted !== text(pc.organizationContactRoute).toLowerCase() &&
      !orgPersisted.includes("@")
    ) {
      // URL org routes OK
    }
  }
  if (
    hasDirect &&
    text(afterSnap.email) &&
    text(afterSnap.orgEmail || afterSnap.orgRoute) &&
    text(afterSnap.email).toLowerCase() ===
      text(afterSnap.orgEmail || afterSnap.orgRoute).toLowerCase()
  ) {
    reasons.push("PERSON_EMAIL_EQUALS_ORG_ROUTE");
    verdict = "FAIL";
  }
  const changedKeys = Object.keys(merged).filter((k) => {
    try {
      return JSON.stringify(merged[k]) !== JSON.stringify(live[k]);
    } catch {
      return true;
    }
  });
  const unrelated = changedKeys.filter((k) => !CONTACT_ONLY_KEYS.has(k));
  if (unrelated.length) {
    reasons.push(`UNRELATED_FIELD_CHANGES:${unrelated.join(",")}`);
    verdict = "FAIL";
  }

  // Shared-contact families must still be demand-relevant
  const familyNote = [];
  if (/nci rna|rna biology/i.test(`${p.title} ${live.organizationName}`)) {
    familyNote.push("NCI_RNA_FAMILY");
  }
  if (/amwa/i.test(`${p.title} ${live.organizationName}`)) {
    familyNote.push("AMWA_FAMILY");
  }
  if (/ctn|clinical trials network/i.test(`${p.title} ${live.organizationName} ${live.whatIsHappening || ""}`)) {
    familyNote.push("CTN_FAMILY");
  }

  return {
    id: p.id,
    title: p.title,
    organizationName: p.organizationName || live.organizationName || null,
    verdict,
    proposedName: name || null,
    proposedTitle: text(pc.title) || null,
    contactClass: cls || null,
    emailClassification: hasDirect
      ? "DIRECT_PERSON_EMAIL"
      : hasOrg
        ? "ORGANIZATION_ROUTE"
        : "NONE",
    directEmail: hasDirect ? text(pc.email) : null,
    orgRoute: text(pc.organizationContactRoute) || (ownsOrg ? text(pc.email) : null),
    sourceUrl: text(pc.sourceUrl) || null,
    whyRelevant: text(pc.whyRelevant) || null,
    before: {
      named: before.named,
      name: before.name,
      coverage: before.coverage,
      email: before.email || before.orgEmail || null,
    },
    afterProjected: {
      named: afterSnap.named,
      name: afterSnap.name,
      coverage: afterSnap.coverage,
      email: afterSnap.email || null,
      orgRoute: afterSnap.orgEmail || afterSnap.orgRoute || null,
      hasDirectEmail: afterSnap.hasDirectEmail,
      hasOrgRoute: afterSnap.hasOrgRoute,
    },
    fieldDiff: {
      primaryContactName: {
        from: before.name,
        to: afterSnap.name,
      },
      primaryContactEmail: {
        from: before.email || null,
        to: afterSnap.hasDirectEmail ? afterSnap.email : null,
      },
      organizationContactRoute: {
        from: before.orgEmail || before.orgRoute || null,
        to: afterSnap.orgEmail || afterSnap.orgRoute || null,
      },
      primaryContactRole: { from: before.title, to: afterSnap.title },
      coverage: { from: before.coverage, to: afterSnap.coverage },
      emailClassification: {
        to: afterSnap.hasDirectEmail
          ? "DIRECT_PERSON_EMAIL"
          : afterSnap.hasOrgRoute
            ? "ORGANIZATION_ROUTE"
            : "NONE",
      },
    },
    hotelFitHonesty: ev?.hotelFitHonesty || null,
    hotelFitNote: ev?.hotelFitNote || null,
    family: familyNote,
    reasons,
    requiresFounderApproval: true,
    opportunitiesJsonWriteAuthorized: false,
  };
}

function reviewCertified(row, byId) {
  const live = byId.get(row.id);
  if (!live) {
    return { id: row.id, error: "NOT_FOUND" };
  }
  const ev = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[row.id];
  const projected = ev ? mergeContactAntiClobber(live, ev) : live;
  const sr = evaluateSharedCommercialEligibility(projected);
  const ad = evaluateAccountDevelopmentEligibility(projected);
  const preventPromotion = [];
  if (!sr.salesReady) {
    preventPromotion.push(
      ...(sr.gaps || []).map((g) => `SR:${g}`),
      sr.demandThesisStandard?.mustRemainResearchContinuation
        ? "SR:SPECULATIVE_OR_THESIS_BLOCKS"
        : null
    );
  }
  if (!ad.accountDevelopment) {
    preventPromotion.push(...(ad.gaps || []).map((g) => `AD:${g}`));
  }

  return {
    id: row.id,
    inventory: row.inventory,
    title: row.title,
    organizationName: row.organizationName,
    productMaturity: row.productMaturity,
    certificationOutcome: row.certificationOutcome,
    whyCommerciallyCertified: {
      namedRelevantContact: row.contactAfter?.named === true,
      usableOutreachRoute: row.outreachRoute !== "NONE",
      demandEvidenceQuality: row.demandEvidenceQuality,
      hotelFitQuality: row.hotelFitQuality,
      note:
        "Certification report label uses named+route+non-insufficient demand+non-missing fit. It is weaker than evaluateSharedCommercialEligibility Sales Ready and may also be weaker than Account Development.",
    },
    contact: {
      name: row.contactAfter?.name || null,
      coverage: row.contactAfter?.coverage || null,
      outreachRoute: row.outreachRoute || null,
    },
    salesReadyEligible: sr.salesReady === true,
    salesReadyStatus: sr.status,
    salesReadyGaps: sr.gaps || [],
    demandThesis: {
      standard: sr.demandThesisStandard?.thesisStandard || null,
      mustRemainResearchContinuation:
        sr.demandThesisStandard?.mustRemainResearchContinuation === true,
      mayQualifySalesReadyDemandGate:
        sr.demandThesisStandard?.mayQualifySalesReadyDemandGate === true,
    },
    accountDevelopmentEligible: ad.accountDevelopment === true,
    accountDevelopmentStatus: ad.status,
    accountDevelopmentGaps: ad.gaps || [],
    preventsPromotion: preventPromotion.filter(Boolean),
    enginesAgreeWithCertificationLabel:
      sr.salesReady === true || ad.accountDevelopment === true
        ? "CERTIFICATION_AT_OR_BELOW_ENGINE"
        : "CERTIFICATION_WEAKER_THAN_ENGINE_GATES",
  };
}

const manifest = fs.existsSync(MANIFEST_PATH)
  ? JSON.parse(fs.readFileSync(MANIFEST_PATH, "utf8"))
  : { patches: [] };
const scorecard = fs.existsSync(SCORECARD_PATH)
  ? JSON.parse(fs.readFileSync(SCORECARD_PATH, "utf8"))
  : { rows: [] };
const store = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
const union = mergeBethesdaInternalResearchOpportunities(
  "recLuxvwwxID7U2B8",
  store.opportunities
);
const byId = new Map(union.opportunities.map((o) => [o.id, o]));

// Always validate the original 15 proposed IDs (not only the current dry-run file).
const patchById = new Map((manifest.patches || []).map((p) => [p.id, p]));
const validations = ORIGINAL_15_PROPOSED_IDS.map((id) => {
  const fromManifest = patchById.get(id);
  const live = byId.get(id);
  const stub = fromManifest || {
    id,
    title: live?.title || null,
    organizationName: live?.organizationName || null,
    after: {},
    fieldDiff: null,
  };
  return validatePatch(stub, byId);
});
const passed = validations.filter((v) => v.verdict === "PASS");
const uncertain = validations.filter((v) => v.verdict === "UNCERTAIN");
const failed = validations.filter((v) => v.verdict === "FAIL");

// Coverage simulation: apply only PASS patches to canonical snapshot counts
const currentNamed = scorecard.rows.filter((r) => r.contactBefore?.named).length;
// Use live union snapshots for accurate persisted named count
const persistedNamed = union.opportunities.filter(
  (o) => extractContactSnapshot(o).named
).length;

let projectedNamed = persistedNamed;
const passIds = new Set(passed.map((v) => v.id));
for (const v of passed) {
  const live = byId.get(v.id);
  const beforeNamed = extractContactSnapshot(live).named;
  if (!beforeNamed && v.afterProjected?.named) projectedNamed += 1;
}

const certified = (scorecard.rows || []).filter(
  (r) => r.certificationOutcome === "COMMERCIALLY_CERTIFIED"
);
const certifiedReview = certified.map((r) => reviewCertified(r, byId));

const dryRun = buildCanonicalContactDryRunPatches({ root: ROOT });
const familyPresentation = applyBethesdaCommercialEventFamilyPresentation(
  union.opportunities,
  { hideLinkedFromList: true }
);
const familyCrosswalk = buildBethesdaCommercialEventFamilyCrosswalk(
  union.opportunities
);

const approvedCandidate = {
  version: "gdi-bethesda-88-canonical-contact-approved-candidate-v1",
  generatedAt: new Date().toISOString(),
  hotelId: "recLuxvwwxID7U2B8",
  authorized: false,
  opportunitiesJsonWriteAuthorized: false,
  note:
    "FOUNDER APPROVAL GATE — exact PASS patches only. Do not write opportunities.json until explicit approval of this manifest (or a subset). Direct person emails and organization routes are stored in separate fields.",
  store: {
    sha256: store.sha256,
    count: store.count,
    unchangedRequired: true,
  },
  validationSummary: {
    totalProposed: validations.length,
    pass: passed.length,
    uncertain: uncertain.length,
    fail: failed.length,
  },
  expectedCoverageIfPassApplied: {
    namedRelevantPrimaryPersistedNow: persistedNamed,
    namedRelevantPrimaryAfterPassPatches: projectedNamed,
    deltaNamed: projectedNamed - persistedNamed,
    stillMissingNamedApprox: 88 - projectedNamed,
    rawResearchIds: 88,
    uniqueCommercialOpportunitiesListVisible:
      familyPresentation.counts.uniqueCommercialOpportunities,
    linkedResearchHidden: familyPresentation.counts.linkedResearchHidden,
    quarantinedHidden: familyPresentation.counts.quarantinedHidden,
  },
  commercialEventFamilies: familyCrosswalk,
  dryRunPatchCount: dryRun.patches.length,
  passedPatches: passed,
  uncertainPatches: uncertain,
  failedPatches: failed,
  rejectedOrDeferredIds: [...uncertain, ...failed].map((v) => v.id),
  approvedCandidateIds: passed.map((v) => v.id),
};

const certificationMaturityAnalysis = {
  version: "gdi-bethesda-88-certification-vs-maturity-analysis-v1",
  generatedAt: new Date().toISOString(),
  reportCounts: {
    COMMERCIALLY_CERTIFIED: 12,
    CONDITIONALLY_READY: 11,
    RESEARCH_INCOMPLETE: 54,
    DISQUALIFIED_WITH_EVIDENCE: 11,
  },
  productMaturityCounts: {
    SALES_READY: 0,
    ACCOUNT_DEVELOPMENT: 0,
    RESEARCH_CONTINUATION: 63,
    PARTNERSHIP_WATCH: 8,
    OVERFLOW_WATCH: 6,
    DISQUALIFIED: 11,
  },
  finding:
    "The COMMERCIALLY_CERTIFIED report label is a weaker audit bar than evaluateSharedCommercialEligibility (Sales Ready) and often weaker than evaluateAccountDevelopmentEligibility. Zero Sales Ready / zero Account Development is expected until product maturity fields are updated by a separate governed promotion step — certification alone must not auto-promote.",
  eligibilityDefect:
    certifiedReview.some(
      (r) =>
        r.salesReadyEligible !== true && r.accountDevelopmentEligible !== true
    )
      ? "CERTIFICATION_LABEL_WEAKER_THAN_SR_AD_ENGINES"
      : "CORRECTED_CERTIFICATION_REQUIRES_SR_OR_AD_ENGINE_PASS",
  proposedCorrection:
    "Applied: COMMERCIALLY_CERTIFIED now requires evaluateSharedCommercialEligibility Sales Ready OR evaluateAccountDevelopmentEligibility pass. CONDITIONALLY_READY remains for named+route short of engine gates. Product maturity fields are NOT auto-promoted — zero Sales Ready / zero Account Development product counts remain until a separate governed promotion write.",
  certifiedRows: certifiedReview,
  salesReadyEligibleCount: certifiedReview.filter((r) => r.salesReadyEligible).length,
  accountDevelopmentEligibleCount: certifiedReview.filter(
    (r) => r.accountDevelopmentEligible
  ).length,
};

fs.mkdirSync(REPORT_DIR, { recursive: true });
writeJsonAtomic(
  path.join(REPORT_DIR, "06-canonical-contact-patch-validation.json"),
  {
    version: "gdi-bethesda-88-canonical-contact-patch-validation-v1",
    generatedAt: new Date().toISOString(),
    validations,
    summary: approvedCandidate.validationSummary,
  }
);
writeJsonAtomic(
  path.join(REPORT_DIR, "07-founder-approved-candidate-manifest.json"),
  approvedCandidate
);
writeJsonAtomic(
  path.join(REPORT_DIR, "08-certification-vs-maturity-analysis.json"),
  certificationMaturityAnalysis
);
writeJsonAtomic(
  path.join(REPORT_DIR, "09-commercial-event-family-crosswalk.json"),
  {
    version: "gdi-bethesda-88-commercial-event-family-crosswalk-v1",
    generatedAt: new Date().toISOString(),
    counts: familyPresentation.counts,
    families: familyCrosswalk,
    note:
      "Raw 88 research IDs preserved. Linked/quarantined rows hidden from actionable list only.",
  }
);
writeJsonAtomic(
  path.join(REPORT_DIR, "03-canonical-59-contact-dry-run-manifest.json"),
  {
    authorized: false,
    patchCount: dryRun.patches.length,
    patches: dryRun.patches,
    note: "Corrected field-level dry-run: primaryContactEmail = direct person only; organizationContactRoute separate.",
  }
);

const md = [
  `# Bethesda 88 — Canonical Contact Patch Validation + Founder Gate`,
  ``,
  `**Authorized store write:** false`,
  `**Store SHA:** \`${store.sha256}\``,
  ``,
  `## 15-patch validation`,
  `| Verdict | Count |`,
  `|---|---:|`,
  `| PASS | ${passed.length} |`,
  `| UNCERTAIN | ${uncertain.length} |`,
  `| FAIL | ${failed.length} |`,
  ``,
  `### PASS (approved candidates — awaiting founder authorization)`,
  ...passed.map(
    (v) =>
      `- \`${v.id}\` → **${v.proposedName}** (${v.emailClassification}) — ${v.sourceUrl}`
  ),
  ``,
  `### FAIL / UNCERTAIN (excluded from write candidates)`,
  ...[...failed, ...uncertain].map(
    (v) =>
      `- \`${v.id}\` **${v.verdict}** — ${v.proposedName || "n/a"} — ${v.reasons.join("; ")}`
  ),
  ``,
  `## Expected coverage if PASS patches applied`,
  `- Named now (persisted): **${persistedNamed}**`,
  `- Named after PASS only: **${projectedNamed}** (Δ +${projectedNamed - persistedNamed})`,
  `- Approx still missing named: **${88 - projectedNamed}**`,
  ``,
  `## Certification vs maturity`,
  `- Sales Ready among 12 certified (engine): **${certificationMaturityAnalysis.salesReadyEligibleCount}**`,
  `- Account Development among 12 certified (engine): **${certificationMaturityAnalysis.accountDevelopmentEligibleCount}**`,
  `- Defect: **${certificationMaturityAnalysis.eligibilityDefect}**`,
  ``,
  certificationMaturityAnalysis.finding,
  ``,
  `See \`08-certification-vs-maturity-analysis.json\` for per-ID gaps.`,
  ``,
].join("\n");
fs.writeFileSync(
  path.join(REPORT_DIR, "FOUNDER_PATCH_VALIDATION_AND_MATURITY_GATE.md"),
  md,
  "utf8"
);

console.log(
  JSON.stringify(
    {
      ok: true,
      pass: passed.length,
      uncertain: uncertain.length,
      fail: failed.length,
      approvedCandidateIds: passed.map((v) => v.id),
      rejectedOrDeferredIds: approvedCandidate.rejectedOrDeferredIds,
      expectedNamedAfterPass: projectedNamed,
      namedNow: persistedNamed,
      certifiedSalesReadyEligible:
        certificationMaturityAnalysis.salesReadyEligibleCount,
      certifiedAccountDevelopmentEligible:
        certificationMaturityAnalysis.accountDevelopmentEligibleCount,
      eligibilityDefect: certificationMaturityAnalysis.eligibilityDefect,
      storeSha: store.sha256,
      writeAuthorized: false,
    },
    null,
    2
  )
);
