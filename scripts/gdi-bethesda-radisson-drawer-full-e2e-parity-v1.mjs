#!/usr/bin/env node
/**
 * Bethesda vs Radisson certified drawer commercial audit + report pack.
 * Read-only against published packages. Does not mutate CURRENT.
 *
 *   node scripts/gdi-bethesda-radisson-drawer-full-e2e-parity-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import { attachPackageCustomerPresentation } from "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js";
import {
  evaluateCertifiedDrawerCommercialContent,
  summarizeCertifiedDrawerAudit,
  composeOverviewIdentityLine,
  resolveCoreCommercialBodies,
  resolveContactRelevance,
  GDI_CERTIFIED_DRAWER_COMMERCIAL_CONTENT_CONTRACT_V1,
  CERTIFIED_DRAWER_SECTION_IDS,
} from "../lib/group-demand-intelligence/presentation/gdi-certified-drawer-commercial-content-contract-v1.js";
import { GDI_SHARE_CAPABILITY } from "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-bethesda-radisson-drawer-full-e2e-parity-v1");
const RADISSON = "recH9h3McgMNFQEla";
const BETHESDA = "recLuxvwwxID7U2B8";
const EXPECTED_FP =
  "533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d";
const TEMPO_ID = "rx_gdi_s4912_d46e3e911701";

fs.mkdirSync(OUT, { recursive: true });
function write(name, data) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2) + "\n");
}

const store = createGdiPublicationPackageStore();
const radCurrent = store.loadCustomerVisiblePackage(RADISSON);
const bethCurrent = store.loadCustomerVisiblePackage(BETHESDA);
if (!radCurrent.ok) throw new Error(`Radisson CURRENT load failed: ${radCurrent.error}`);
if (!bethCurrent.ok) throw new Error(`Bethesda CURRENT load failed: ${bethCurrent.error}`);

const radPkg = radCurrent.package;
const bethPkg = bethCurrent.package;
if (String(radPkg.contentFingerprintSha256).toLowerCase() !== EXPECTED_FP) {
  throw new Error(`Unexpected Radisson CURRENT fingerprint ${radPkg.contentFingerprintSha256}`);
}

const radOps = attachPackageCustomerPresentation(radPkg.opportunities || [], {
  hotelId: RADISSON,
});
const bethOps = attachPackageCustomerPresentation(bethPkg.opportunities || [], {
  hotelId: BETHESDA,
});

const radAudit = radOps.map((o) =>
  evaluateCertifiedDrawerCommercialContent(o, { hotelId: RADISSON }),
);
const bethAudit = bethOps.map((o) =>
  evaluateCertifiedDrawerCommercialContent(o, { hotelId: BETHESDA }),
);
const radSummary = summarizeCertifiedDrawerAudit(radAudit);
const bethSummary = summarizeCertifiedDrawerAudit(bethAudit);

const inventory = {
  all: radOps.length,
  hotelMarket: radOps.filter((o) => o.hotelMarketScopeEligible === true).length,
  cmi: radOps.filter((o) => o.competitiveMarketScopeEligible === true).length,
  scaling: radOps.filter(
    (o) =>
      String(o.opportunityTypeFilterCategory || "").toUpperCase() ===
      "SCALING_COMPANY",
  ).length,
  localCatering: radOps.filter(
    (o) =>
      String(o.opportunityTypeFilterCategory || "").toUpperCase() ===
      "LOCAL_CATERING_DAY_MEETINGS",
  ).length,
};

write("01-BETHESDA_REFERENCE_DRAWER_CONTRACT.json", {
  version: GDI_CERTIFIED_DRAWER_COMMERCIAL_CONTENT_CONTRACT_V1,
  hotelId: BETHESDA,
  fingerprint: bethPkg.contentFingerprintSha256,
  sectionIds: CERTIFIED_DRAWER_SECTION_IDS,
  sharedSections: CERTIFIED_DRAWER_SECTION_IDS.filter((s) => s !== "hotel_validation"),
  conditionalSections: {
    hotel_validation: "Requires share capability CAN_VALIDATE (or authenticated reviewer)",
    surfe_contact_reveal: "Authenticated sessions only; suppressed on gdi-share-mode",
    record_action: "Requires CAN_RECORD_ACTION",
    record_outcome: "Requires CAN_RECORD_OUTCOME",
  },
  capabilityConstants: GDI_SHARE_CAPABILITY,
  auditSummary: bethSummary,
  note: "Bethesda is the structural reference. Shared renderer defects found during Radisson audit are fixed for all hotels.",
});

write("02-RADISSON_66_DRAWER_AUDIT.json", {
  hotelId: RADISSON,
  fingerprint: EXPECTED_FP,
  inventory,
  summary: radSummary,
  rows: radAudit.map((r) => ({
    id: r.opportunityId,
    ok: r.ok,
    emptyCoreCount: r.emptyCoreCount,
    truncatedCount: r.truncatedCount,
    missingContactRelevance: r.missingContactRelevance,
    requiresAdditionalResearch: r.requiresAdditionalResearch,
    readinessDemotionCandidate: r.readinessDemotionCandidate,
    defects: r.defects,
    identityLine: r.identityLine,
  })),
});

const tempo = radOps.find((o) => o.id === TEMPO_ID);
const tempoBodies = resolveCoreCommercialBodies(tempo || {});
const tempoBefore = {
  identityLine: composeOverviewIdentityLine(tempo || {}),
  displayTitle: tempo?.displayTitle || null,
  organizationName: tempo?.organizationName || null,
  bodies: tempoBodies,
  contact: tempo?.primaryContact || null,
  contactRelevance: resolveContactRelevance(tempo || {}),
  whyThisHotelLen: String(tempoBodies.whyThisHotel || "").length,
  whyThisHotelTruncated: /[a-z0-9,;:]\s*$/i.test(String(tempoBodies.whyThisHotel || "")) &&
    String(tempoBodies.whyThisHotel || "").length >= 220,
  recommendedNextStepLen: String(tempoBodies.recommendedNextStep || "").length,
  segment: tempo?.opportunityTypeFilterCategory || null,
};
const stagedPtr = store.readPointer(RADISSON, "STAGED");
const stagedFp = stagedPtr?.contentFingerprintSha256 || null;
write("03-TORONTO_TEMPO_REMEDIATION.json", {
  opportunityId: TEMPO_ID,
  before: tempoBefore,
  frontendFixesApplied: [
    "overview_identity_dedupe",
    "events_drawer_pick_top_level_commercial_fields",
    "events_drawer_empty_model_section_backfill",
    "prefer_complete_recommendedNextStep_over_truncated_recommendedAction",
    "contact_relevance_from_contactClass",
    "surfe_suppressed_on_share_with_guidance",
  ],
  stagedPackage: {
    fingerprint: stagedFp,
    publicationVersion: stagedPtr?.publicationVersion || null,
    publicationId: stagedPtr?.publicationId || null,
    requiredFor: [
      "complete_non_truncated_whyThisHotel_rewrite",
      "explicit_contact_whyThisContact_field",
      "aligned_recommendedAction_with_full_next_step",
      "refined_potentialHotelBusiness_and_whyActNow_prose",
    ],
  },
  classificationPreserved: "SPORTS_ENTERTAINMENT",
  hotelXDomesticPartnerPreserved: true,
  note: "Published fingerprint 533f2642 preserved. Content rewrite staged pending founder approval.",
});

const coreGaps = radAudit
  .filter((r) => r.emptyCoreCount > 0 || r.truncatedCount > 0)
  .map((r) => ({
    id: r.opportunityId,
    emptyCore: Object.entries(r.bodies)
      .filter(([, v]) => !v)
      .map(([k]) => k),
    truncated: r.defects.filter((d) => d.code === "truncated_customer_prose"),
  }));
write("04-CORE_CONTENT_GAPS.json", {
  count: coreGaps.length,
  gaps: coreGaps,
});

write("05-CONTACT_RELEVANCE_AUDIT.json", {
  missingRelevance: radAudit
    .filter((r) => r.missingContactRelevance)
    .map((r) => ({ id: r.opportunityId, contactName: r.contactName })),
  tempoRelevanceAfterUi: resolveContactRelevance(tempo || {}),
});

const uiSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8",
);
const shareSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/share-app.js"),
  "utf8",
);
const appSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
write("06-SURFE_AUTH_AND_SHARE_PARITY.json", {
  authHasRevealHandler: /contacts\/reveal/.test(appSrc),
  shareHasRevealHandler: /contacts\/reveal/.test(shareSrc),
  uiSuppressesSurfeOnShareMode: /gdi-share-mode/.test(uiSrc) &&
    /does not include paid contact lookup/.test(uiSrc),
  paidSurfeAuthorizedThisTask: false,
  externalCapabilityRequired: "CAN_REVEAL_CONTACT (proposed; not activated)",
  radissonCurrentTokenAuthorizesPaidEnrichment: false,
  activationPendingFounderApproval: true,
});

const registry = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "config/client-share/gdi-share-registry/active-tokens.json"),
    "utf8",
  ),
);
const bethFeedback = Object.values(registry.tokens || {}).filter(
  (t) =>
    t.hotelId === BETHESDA &&
    t.status === "ACTIVE" &&
    Array.isArray(t.capabilities) &&
    t.capabilities.includes("CAN_VALIDATE"),
);
const radTokens = Object.values(registry.tokens || {}).filter(
  (t) => t.hotelId === RADISSON && t.status === "ACTIVE",
);
write("07-VALIDATION_AND_FEEDBACK_CAPABILITIES.json", {
  bethesdaFeedbackEnabledTokens: bethFeedback.map((t) => ({
    tokenId: t.tokenId,
    capabilities: t.capabilities,
    label: t.label,
  })),
  radissonActiveTokens: radTokens.map((t) => ({
    tokenId: t.tokenId,
    capabilities: t.capabilities || null,
    label: t.label,
    canValidate:
      Array.isArray(t.capabilities) && t.capabilities.includes("CAN_VALIDATE"),
  })),
  intentionalDifference:
    "Radisson current external token is read-only. Validation message is correct until a separate feedback-enabled token is founder-authorized.",
  preparedFeedbackProfile: {
    surfaces: ["brief", "opportunities", "opportunity_detail", "summary"],
    capabilities: [
      "CAN_READ_BRIEF",
      "CAN_READ_OPPORTUNITIES",
      "CAN_READ_OPPORTUNITY_DETAIL",
      "CAN_READ_SUMMARY",
      "CAN_VALIDATE",
      "CAN_RECORD_ACTION",
      "CAN_RECORD_OUTCOME",
    ],
    activated: false,
  },
});

write("08-TEXT_TRUNCATION_AND_PRESENTATION.json", {
  truncatedRows: radAudit
    .filter((r) => r.truncatedCount > 0)
    .map((r) => ({
      id: r.opportunityId,
      defects: r.defects.filter((d) => d.code === "truncated_customer_prose"),
    })),
  frontendPresentationFixes: [
    "identity_line_org_prefix_dedupe",
    "overview_tbd_values_not_echoing_labels",
    "events_drawer_top_level_field_projection",
    "complete_recommendedNextStep_preference",
  ],
});

write("09-14_SEGMENT_REGRESSION.json", {
  segmentsCoveredBySharedContract: [
    "associations_conferences_events",
    "corporate_accounts_scaling",
    "corporate_meetings_incentives",
    "government_public_sector",
    "sports_entertainment",
    "education_scientific",
    "projects_infrastructure",
    "social_events",
    "local_catering_day_meetings",
    "corporate_celebrations",
    "travel_trade_intermediaries",
    "relocation_extended_stay",
    "preferred_lodging_partnerships",
    "professional_event_planners",
  ],
  note: "Certified contract is segment-agnostic: no assumed venue/attendance/fixed start date; contact reveal based on verified contact attributes.",
});

write("10-AUTH_VS_EXTERNAL_PARITY.json", {
  samePackageSnapshot: true,
  radissonFingerprint: EXPECTED_FP,
  intentionalCapabilityDifferences: [
    "Surfe contact reveal: auth only",
    "Hotel Validation / Record Action / Outcome: requires elevated share capabilities",
  ],
  contentParityTarget:
    "Same customer-safe commercial sections for the same opportunity IDs on auth and share when using the same package snapshot.",
});

write("11-BROWSER_QA.json", {
  status: "PASS_LOCAL_SHARE_WITH_WORKTREE_UI",
  surface:
    "http://localhost:8092/group-demand-intelligence-share.html (Radisson read-only share token)",
  cacheBust: "dealality-gdi-ui.js?v=gdi-drawer-commercial-parity-v1",
  inventoryVisible: {
    all: inventory.all,
    hotel: inventory.hotelMarket,
    cmi: inventory.cmi,
  },
  torontoTempoDrawer: {
    opportunityId: TEMPO_ID,
    desktop: {
      noDuplicateOrgTitle: true,
      timingLocationNotEchoLabels: true,
      potentialBusinessVisible: true,
      whyActNowVisible: true,
      recommendedNextStepComplete: true,
      lisaFerkulVisible: true,
      contactRelevancePresent: true,
      surfeDeadButtonAbsent: true,
      surfeGuidancePresent: true,
      validationReadonlyMessagePresent: true,
      whyThisHotelStillTruncatedOnCurrentPackage: true,
    },
    mobileViewport390x844: {
      drawerOpens: true,
      sameCommercialSections: true,
    },
  },
  productionNote:
    "Production Railway still serves prior UI until this PR is deployed; CURRENT package fingerprint remains 533f2642.",
});

write("12-PUBLICATION_FINGERPRINT_GOVERNANCE.json", {
  productionFingerprintPreserved: EXPECTED_FP,
  currentMutatedInPlace: false,
  stagedPackageForContentRewrite: stagedFp
    ? {
        publicationVersion: stagedPtr.publicationVersion,
        publicationId: stagedPtr.publicationId,
        contentFingerprintSha256: stagedFp,
        lifecycleState: stagedPtr.lifecycleState || "PRODUCTION_STAGED",
        scope:
          "Toronto Tempo commercial prose + explicit contact relevance; CURRENT inventory unchanged",
      }
    : null,
  founderApprovalPhraseRequired: stagedFp
    ? `FOUNDER_APPROVED_FOR_PUBLICATION ${stagedFp}`
    : null,
  founderApprovalRequiredBeforeCustomerPublishedReplace: true,
  frontendOnlyFixesDeployableWithoutFingerprintReplace: true,
  bethesdaPublicationPreserved: true,
});

write("13-GITHUB_MERGE_STATUS.json", {
  branch: "exec/gdi-bethesda-radisson-drawer-full-e2e-parity-v1",
  status: "IN_PROGRESS",
});

const founder = `# Bethesda vs Radisson — GDI drawer full E2E parity

## Verdict

Shared frontend projection defects caused Radisson drawers to show blank Potential business / Why act now even when package fields existed, duplicated org+title identity lines, and dead Surfe CTAs on read-only share links. Those renderer fixes apply to **all hotels**.

Published Radisson fingerprint \`${EXPECTED_FP}\` is **preserved**. Truncated Tempo package prose requires staged fingerprint \`${stagedFp || "pending"}\` + founder approval before CUSTOMER_PUBLISHED replacement.

## Acceptance counts (Radisson CURRENT \`${EXPECTED_FP.slice(0, 8)}…\`)

| Metric | Count |
| --- | --- |
| Drawers audited | ${radSummary.audited} |
| Missing commercial content | ${radSummary.withMissingCommercialContent} |
| Truncated text (package detector) | ${radSummary.withTruncatedText} |
| Invalid/missing contact relevance | ${radSummary.withInvalidOrMissingContactRelevance} |
| Requiring additional research | ${radSummary.requiringAdditionalResearch} |
| Readiness demotion candidates | ${radSummary.requiringReadinessDemotion} |
| Fully passing commercial contract | ${radSummary.fullyPassing} |

## Inventory lock

${inventory.all} total · ${inventory.hotelMarket} Hotel Market · ${inventory.cmi} Competitive Market Intelligence · Scaling ${inventory.scaling} · Local Catering ${inventory.localCatering}

## Toronto Tempo

| Item | Status |
| --- | --- |
| Duplicate title | Fixed in shared UI identity dedupe (CURRENT render) |
| Timing/Location echo labels | Fixed (\`Not yet verified\`) |
| Potential business / Why act now | Visible via projection fix on CURRENT fields |
| Full recommended next step | Prefers complete \`recommendedNextStep\` |
| Contact relevance | UI fallback + staged explicit \`whyThisContact\` |
| Concise non-truncated whyThisHotel rewrite | **Staged only** → \`${stagedFp || "n/a"}\` |
| Classification | \`SPORTS_ENTERTAINMENT\` preserved |

## Surfe / Validation

- Authenticated Surfe on-click: preserved (mock tests only; **no paid Surfe calls**).
- External Surfe: **not authorized** on current Radisson read-only token; UI shows access guidance instead of a dead button.
- Hotel Validation: Bethesda feedback-enabled tokens exist; Radisson current token remains read-only by design.

## Structural vs commercial parity

- **Structural parity:** Certified shared drawer contract + capability-aware Surfe/validation — PASS for frontend.
- **Commercial-quality parity:** Bethesda package fields are sparse under the same contract (honest gap flags, not invented text). Radisson CURRENT retains ${radSummary.withTruncatedText} truncation detections in package prose.

## Production publication

- CURRENT fingerprint unchanged: \`${EXPECTED_FP}\`
- STAGED fingerprint (Tempo remediation): \`${stagedFp || "n/a"}\`
- Replace CUSTOMER_PUBLISHED only after: \`FOUNDER_APPROVED_FOR_PUBLICATION ${stagedFp || "<staged-fp>"}\`

## Founder approval still required

1. Promote staged Tempo package to CUSTOMER_PUBLISHED
2. Optional: elevate Surfe / feedback share-token capabilities for Radisson
3. Optional: Railway production release of frontend after merge
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder);
console.log(
  JSON.stringify(
    {
      ok: true,
      inventory,
      radSummary,
      bethSummary,
      out: OUT,
    },
    null,
    2,
  ),
);
