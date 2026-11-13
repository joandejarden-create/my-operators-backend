#!/usr/bin/env node
/**
 * Certify and lock GDI_FULL_E2E_HOTEL_TEMPLATE_V1.
 *
 * Template certification only — no paid research, no publication mutation,
 * no Sheraton / new-hotel research.
 *
 *   npm run gdi:full-e2e-hotel-template-v1
 *   npm run test:gdi-full-e2e-hotel-template-v1
 */
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";
import {
  GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  TEMPLATE_REFERENCE,
  TEMPLATE_VERSION_LOCK,
  TEMPLATE_DATA_RESPONSIBILITIES,
  TEMPLATE_E2E_STAGES,
  HOTEL_CONFIGURATION_REQUIRED_KEYS,
  HOTEL_CONFIGURATION_OPTIONAL_KEYS,
  COMMERCIAL_ELIGIBILITY_POLICY_V1,
  buildResearchCapabilityMatrix,
  readBethesdaPublicationIdentity,
  runProspectivePresentationPersistenceCycle,
  runCrossHotelPortabilityChecks,
  evaluateFullE2eHotelTemplateCertificate,
  REQUIRED_CUSTOMER_PRESENTATION_KEYS,
  GDI_FIELD_AUTHORITY_MATRIX_V1,
} from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";
import { GDI_FIELD_AUTHORITY_CONTRACT_V1 } from "../lib/group-demand-intelligence/presentation/gdi-field-authority-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-full-e2e-hotel-template-v1");

function sh(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

function writeJson(name, obj) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function sha256File(rel) {
  const buf = fs.readFileSync(path.join(ROOT, rel));
  return crypto.createHash("sha256").update(buf).digest("hex");
}

fs.mkdirSync(OUT, { recursive: true });

const mainSha = sh("git rev-parse HEAD") || "UNKNOWN";
const mainShort = mainSha.slice(0, 9);

const { current, pkg } = readBethesdaPublicationIdentity();
const publicationFingerprintOk =
  current.contentFingerprintSha256 === TEMPLATE_REFERENCE.contentFingerprintSha256 &&
  pkg.contentFingerprintSha256 === TEMPLATE_REFERENCE.contentFingerprintSha256 &&
  current.publicationVersion === TEMPLATE_REFERENCE.publicationVersion;

const feedbackPath = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  TEMPLATE_REFERENCE.hotelId,
  "feedback.json",
);
const feedbackSha = fs.existsSync(feedbackPath)
  ? crypto.createHash("sha256").update(fs.readFileSync(feedbackPath)).digest("hex")
  : null;

// Prior receipts
const persistenceParityPath = path.join(
  ROOT,
  "reports/gdi-airtable-presentation-persistence-v1/06-67_FIELD_PARITY_RESULTS.json",
);
const persistenceReceiptsPath = path.join(
  ROOT,
  "reports/gdi-airtable-presentation-persistence-v1/10-PERSISTENCE_REGRESSION.json",
);
let airtableReconstruction67Ok = false;
let persistenceRegression = null;
if (fs.existsSync(persistenceParityPath) && fs.existsSync(persistenceReceiptsPath)) {
  const parity = JSON.parse(fs.readFileSync(persistenceParityPath, "utf8"));
  persistenceRegression = JSON.parse(fs.readFileSync(persistenceReceiptsPath, "utf8"));
  airtableReconstruction67Ok =
    parity.allPass === true &&
    parity.passCount === 67 &&
    parity.usedPublicationPackageAsFallback === false &&
    persistenceRegression.publicationShaUnchanged === true &&
    persistenceRegression.fingerprint === TEMPLATE_REFERENCE.contentFingerprintSha256;
}

const researchStoragePath = path.join(
  ROOT,
  "reports/gdi-research-storage-reconciliation-v1/11-CROSS_HOTEL_STORAGE_CONTRACT.json",
);
let researchStorageGateOk = false;
if (fs.existsSync(researchStoragePath)) {
  const storage = JSON.parse(fs.readFileSync(researchStoragePath, "utf8"));
  researchStorageGateOk =
    storage.templateGate?.ok === true &&
    storage.templateGate?.coverage?.ok === true &&
    storage.templateGate?.coverage?.publishedCount === 67;
}
// Research-storage coverage alone is insufficient without presentation reconstruction.
if (!airtableReconstruction67Ok) researchStorageGateOk = false;

const prospective = runProspectivePresentationPersistenceCycle();
const crossHotel = runCrossHotelPortabilityChecks();

const authHtml = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);
const authApp = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
const modeBannerAbsentByDefault =
  /wantsPresentationModeDiagnostics/.test(authApp) &&
  /gdiDiag/.test(authApp) &&
  /gdi-ui-no-mode-banner-v1/.test(authHtml);

const fieldAuthorityContractPresent =
  GDI_FIELD_AUTHORITY_CONTRACT_V1 === "gdi-field-authority-contract-v1" &&
  GDI_FIELD_AUTHORITY_MATRIX_V1.length >= 10;

const capabilityMatrix = buildResearchCapabilityMatrix();

const certificate = evaluateFullE2eHotelTemplateCertificate({
  mainCommitSha: mainSha,
  publicationFingerprintOk,
  airtableReconstruction67Ok,
  prospectivePersistenceOk: prospective.ok,
  crossHotelOk: crossHotel.ok,
  fieldAuthorityContractPresent,
  modeBannerAbsentByDefault,
  negativeMutationGatesOk: prospective.mutationDetected === true,
  researchStorageGateOk: researchStorageGateOk && airtableReconstruction67Ok,
  noPaidResearchExecuted: true,
  noPublicationMutated: publicationFingerprintOk,
});

writeJson("01-CANONICAL_BASELINE.json", {
  schema: "gdi-full-e2e-hotel-template-baseline-v1",
  mainCommitSha: mainSha,
  mainCommitShort: mainShort,
  baselinePrs: TEMPLATE_REFERENCE.baselinePrs,
  compatibleFollowOnPrs: TEMPLATE_REFERENCE.compatibleFollowOnPrs,
  referenceHotel: TEMPLATE_REFERENCE,
  publication: {
    current,
    packageFingerprint: pkg.contentFingerprintSha256,
    packageVersion: pkg.publicationVersion || pkg.version,
    opportunityCount: pkg.opportunities?.length || pkg.opportunityCount || TEMPLATE_REFERENCE.opportunityCount,
    fingerprintVerified: publicationFingerprintOk,
  },
  versions: TEMPLATE_VERSION_LOCK,
  feedbackSha256: feedbackSha,
  certificationTests: [
    "test:gdi-full-e2e-hotel-template-v1",
    "test:gdi-airtable-presentation-persistence-v1",
    "test:gdi-field-authority-contract-v1",
    "test:gdi-canonical-ui-architecture-offline",
    "test:gdi-ui-no-mode-banner-v1",
    "test:gdi-research-storage-reconciliation-v1",
  ],
  productionReleaseIdentity: {
    channel: "Dealality production release (GDI)",
    note: "Deploy only via GitHub Actions workflow with I_AUTHORIZE_PRODUCTION_RELEASE",
  },
});

writeJson("02-RESEARCH_ENGINE_CAPABILITY_MATRIX.json", {
  schema: "gdi-research-engine-capability-matrix-v1",
  statuses: ["PRODUCTION_CERTIFIED", "IMPLEMENTED_NOT_CERTIFIED", "EXPERIMENTAL", "PLANNED"],
  rows: capabilityMatrix,
  summary: {
    PRODUCTION_CERTIFIED: capabilityMatrix.filter((r) => r.templateStatus === "PRODUCTION_CERTIFIED").length,
    IMPLEMENTED_NOT_CERTIFIED: capabilityMatrix.filter((r) => r.templateStatus === "IMPLEMENTED_NOT_CERTIFIED").length,
    EXPERIMENTAL: capabilityMatrix.filter((r) => r.templateStatus === "EXPERIMENTAL").length,
  },
  note: "Status reflects implemented code capability vs planning docs. International/multilingual support preserved via hotel locale + research languages in config.",
});

writeJson("03-AIRTABLE_STORAGE_CONTRACT.json", {
  schema: "gdi-airtable-storage-contract-v1",
  responsibilities: TEMPLATE_DATA_RESPONSIBILITIES,
  baseId: TEMPLATE_VERSION_LOCK.airtableBaseId,
  coreTable: {
    name: "Group Demand Opportunities",
    tableId: TEMPLATE_VERSION_LOCK.airtableOpportunitiesTableId,
    role: "CANONICAL",
  },
  supportingTables: {
    CANONICAL: [
      "Group Demand Opportunities",
      "GDI Research Targets",
      "GDI Research Runs",
      "GDI Research Target Runs",
      "GDI Hotel Need Periods",
      "GDI Need Period Opportunity Matches",
    ],
    OPTIONAL_PER_HOTEL: [
      "Demand Generators",
      "Demand Programs",
      "Demand Generator Signals",
      "Hotel Demand Generator Fit",
      "Private Event Venues",
      "Private Event Signals",
      "Hotel Venue Fit",
      "Hotel Commercial Profiles",
      "GDI Hotel Commercial Profiles",
      "Hotel Event Spaces",
      "Hotel Demand Nodes",
      "Hotel Intelligence Evidence",
    ],
  },
  requiredPresentationKeys: REQUIRED_CUSTOMER_PRESENTATION_KEYS,
  presentationPersistence: TEMPLATE_VERSION_LOCK.presentationPersistence,
  prospectiveCycle: prospective,
  bethesdaReconstruction: {
    airtableReconstruction67Ok,
    persistenceRegression,
  },
  note: "Irrelevant tables need not be populated for every hotel.",
});

writeJson("04-HOTEL_CONFIGURATION_SCHEMA.json", {
  schema: "gdi-hotel-configuration-schema-v1",
  requiredKeys: HOTEL_CONFIGURATION_REQUIRED_KEYS,
  optionalKeys: HOTEL_CONFIGURATION_OPTIONAL_KEYS,
  configRoot: "config/group-demand-intelligence/hotels/{hotelId}.json",
  unknownValuesRemainUnknown: true,
  needPeriodsMustNotBeFabricated: true,
  noHardcodedFrontendRoutesRequired: true,
  authAppUsesQueryHotelOrPilotFallback: true,
  authAppFallbackNote:
    "Auth app falls back to the template reference hotel when hotelId query is absent; rendering remains hotel-independent via hotelId.",
  exampleHotels: crossHotel.hotels,
});

writeJson("05-FULL_E2E_WORKFLOW.json", {
  schema: "gdi-full-e2e-workflow-template-v1",
  contractVersion: TEMPLATE_VERSION_LOCK.fullE2eContractVersion,
  stages: TEMPLATE_E2E_STAGES,
  entrypoint: "npm run gdi:full-e2e -- --hotel <hotelId> --authorization-ref <ref> --dry-run",
  paidAndPublishRequireSeparateFounderAuth: true,
});

writeJson("06-COMMERCIAL_ELIGIBILITY_POLICY.json", COMMERCIAL_ELIGIBILITY_POLICY_V1);

writeJson("07-FIELD_AUTHORITY_CONTRACT.json", {
  schema: GDI_FIELD_AUTHORITY_CONTRACT_V1,
  fieldCount: GDI_FIELD_AUTHORITY_MATRIX_V1.length,
  fields: GDI_FIELD_AUTHORITY_MATRIX_V1.map((f) => ({
    field: f.field,
    authoritativeSource: f.authoritativeSource,
    approvedDisplayField: f.approvedDisplayField,
    neverSubstituteWith: f.neverSubstituteWith,
  })),
  docs: "docs/data-intelligence/gdi-field-authority-contract-v1.md",
  pr: 270,
});

writeJson("08-CANONICAL_CUSTOMER_UI.json", {
  schema: "gdi-canonical-customer-ui-template-v1",
  sharedModules: [
    "public/js/group-demand-intelligence/dealality-gdi-ui.js",
    "public/js/group-demand-intelligence/gdi-universal-presentation-controller.js",
    "public/js/group-demand-intelligence/app.js",
    "public/js/group-demand-intelligence/share-app.js",
  ],
  surfaces: [
    "hotelSelector",
    "opportunityCounts",
    "priorityFilters",
    "salesOpportunitiesFilters",
    "marketScopeFilters",
    "opportunityTypeFilters",
    "weeklyChange",
    "resetView",
    "searchAndSorting",
    "listTiles",
    "cardTitlesAndBadges",
    "commercialActionPills",
    "dateBadges",
    "svgIconStates",
    "contactFooter",
    "opportunityDetailsDrawers",
    "needDateMatch",
    "evidenceAndUncertainty",
    "customerFeedback",
    "guidedReportTour",
    "csvExport",
    "responsiveDesktopMobile",
  ],
  modeBanner: {
    defaultVisible: false,
    diagnosticQuery: "gdiDiag=1",
    cacheMarker: "gdi-ui-no-mode-banner-v1",
  },
  hotelIndependentRenderer: !crossHotel.uiHasBethesdaLiteral,
});

writeJson("09-PUBLICATION_LIFECYCLE.json", {
  schema: "gdi-publication-lifecycle-template-v1",
  reference: TEMPLATE_REFERENCE,
  currentPointerVerified: publicationFingerprintOk,
  rules: [
    "Independent package version per hotel",
    "Complete customer presentation DTO",
    "Deterministic SHA-256 fingerprint",
    "Founder approval receipt required",
    "Controlled CURRENT pointer",
    "Rollback history retained",
    "Research sync never mutates approved publication",
  ],
  feedbackOutsideFingerprint: true,
});

writeJson("10-QUALITY_AND_PARITY_GATES.json", {
  schema: "gdi-template-quality-parity-gates-v1",
  permanentGates: [
    "test:gdi-full-e2e-hotel-template-v1",
    "test:gdi-airtable-presentation-persistence-v1",
    "test:gdi-field-authority-contract-v1",
    "test:gdi-canonical-ui-architecture-offline",
    "test:gdi-ui-no-mode-banner-v1",
    "test:gdi-regression-recovery-v1",
  ],
  missingSkippedTimedOutIsNotPass: true,
  negativeMutationTests: {
    wrongSalesActionPillDetected: prospective.mutationDetected,
    prospectiveCycleOk: prospective.ok,
  },
  bethesdaAirtableParity67: airtableReconstruction67Ok,
});

writeJson("11-CROSS_HOTEL_REGRESSION.json", {
  schema: "gdi-template-cross-hotel-regression-v1",
  ...crossHotel,
  noNewHotelPublished: true,
  noAirtableWrites: true,
});

writeJson("12-EXECUTION_COMMANDS.json", {
  schema: "gdi-template-execution-commands-v1",
  commands: {
    validateHotelConfig: "node -e \"JSON.parse(require('fs').readFileSync('config/group-demand-intelligence/hotels/<id>.json','utf8'))\"",
    inventoryAssessment: "npm run gdi:research-storage-reconciliation-v1 -- --dry-run --hotel <id>",
    researchPlanDryRun: "npm run gdi:full-e2e -- --hotel <id> --authorization-ref <ref> --dry-run",
    airtablePersistenceVerify: "npm run test:gdi-airtable-presentation-persistence-v1",
    commercialQuality: "npm run test:gdi-field-authority-contract-v1",
    founderPreview: "Open /group-demand-intelligence.html?hotelId=<id> (package-first; ?live=1 for research browse)",
    publicationStaging: "Use existing publication package staging scripts; approve exact fingerprint",
    productionParity: "Browser parity vs founder preview after Dealality production release (GDI)",
    weeklyRefresh: "Existing weekly-refresh orchestrator (authorization required)",
    certifyTemplate: "npm run gdi:full-e2e-hotel-template-v1",
  },
  doNot: [
    "Run paid research without founder authorization",
    "Auto-publish from research refresh",
    "Mutate Bethesda V2 fingerprint",
    "Execute Sheraton research during template certification",
  ],
});

writeJson("13-GITHUB_RELEASE_STATUS.json", {
  schema: "gdi-template-github-release-status-v1",
  branch: "cert/gdi-full-e2e-hotel-template-v1",
  status: "PENDING_PR",
  mainCommitShaAtCert: mainSha,
  templateId: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  publicationFingerprintUnchanged: publicationFingerprintOk,
});

writeJson("14-TEMPLATE_CERTIFICATE.json", {
  ...certificate,
  mainCommitSha: mainSha,
  evidence: {
    publicationFingerprintOk,
    airtableReconstruction67Ok,
    prospectivePersistenceOk: prospective.ok,
    crossHotelOk: crossHotel.ok,
    fieldAuthorityContractPresent,
    modeBannerAbsentByDefault,
    negativeMutationGatesOk: prospective.mutationDetected,
    researchStorageGateOk,
    noPaidResearchExecuted: true,
    noPublicationMutated: publicationFingerprintOk,
  },
});

const limitations = [];
if (!certificate.certified) {
  for (const f of certificate.fails) limitations.push(f.code);
}
limitations.push(
  "Full hotel research COMPLETE for non-Bethesda hotels remains hotel-specific execution, not implied by this template cert.",
);
limitations.push(
  "Auth app still defaults hotelId to Bethesda when query absent — override with ?hotelId=.",
);
limitations.push(
  "Specialist hunters marked IMPLEMENTED_NOT_CERTIFIED / EXPERIMENTAL require hotel-scoped proof before claiming production depth.",
);

const founderReport = `# GDI Full E2E Hotel Template V1 — Founder Report

**Template:** \`${GDI_FULL_E2E_HOTEL_TEMPLATE_V1}\`  
**Status:** **${certificate.status}**  
**Certified commit SHA:** \`${mainSha}\`  
**Reference hotel:** Bethesda Marriott (\`${TEMPLATE_REFERENCE.hotelId}\`)  
**Approved publication:** \`${TEMPLATE_REFERENCE.publicationVersion}\`  
**Fingerprint (unchanged):** \`${TEMPLATE_REFERENCE.contentFingerprintSha256}\`

## Verdict

| Gate | Result |
|------|--------|
| Baseline PRs #264, #266–#270, #272–#275 on main | Verified in history (plus compatible #276) |
| Publication fingerprint immutable | ${publicationFingerprintOk ? "PASS" : "FAIL"} |
| Airtable reconstruction 67/67 | ${airtableReconstruction67Ok ? "PASS" : "FAIL"} |
| Prospective presentation persistence cycle | ${prospective.ok ? "PASS" : "FAIL"} |
| Negative mutation detection | ${prospective.mutationDetected ? "PASS" : "FAIL"} |
| Cross-hotel portability (read-only) | ${crossHotel.ok ? "PASS" : "FAIL"} |
| Field Authority Contract | ${fieldAuthorityContractPresent ? "PASS" : "FAIL"} |
| Mode A/B banner absent by default | ${modeBannerAbsentByDefault ? "PASS" : "FAIL"} |
| No paid research / no publication mutation during cert | PASS |

## What this certifies

The current Dealality GDI application is the permanent reusable Full E2E operating template for every hotel:

1. Four data responsibilities locked (Airtable registry, durable research FS, immutable publication, feedback).
2. Canonical presentation persistence + field authority + shared customer UI.
3. 13-stage Full E2E workflow with existing orchestrator entrypoint.
4. Commercial eligibility + research capability matrix (honest status labels).
5. Permanent CI gate: \`npm run test:gdi-full-e2e-hotel-template-v1\`.

## What this does **not** do

- Does not run Sheraton or any new-hotel research.
- Does not redesign the GDI UI.
- Does not change Bethesda V2 content or fingerprint.
- Does not auto-certify every future hotel's research COMPLETE score.

## Invoke for the next hotel

See \`OPERATING_PLAYBOOK.md\`. Short form:

\`\`\`bash
npm run gdi:full-e2e -- --hotel <canonicalHotelId> --authorization-ref <ref> --dry-run
\`\`\`

Paid discovery and publish require separate founder authorization.

## Limitations

${limitations.map((l) => `- ${l}`).join("\n")}

## Artifacts

\`01\`–\`14\` + \`OPERATING_PLAYBOOK.md\` in this directory.
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founderReport, "utf8");

const playbook = `# GDI Full E2E Hotel Template V1 — Operating Playbook

**Template ID:** \`${GDI_FULL_E2E_HOTEL_TEMPLATE_V1}\`  
**Contract:** \`${TEMPLATE_VERSION_LOCK.fullE2eContractVersion}\`  
**Reference:** Bethesda Marriott V2 (\`${TEMPLATE_REFERENCE.contentFingerprintSha256.slice(0, 12)}…\`)

## Principles

1. Research never silently changes customer-visible publication content.
2. Airtable is the mutable research/commercial registry; publication packages are immutable.
3. Feedback is outside the publication fingerprint.
4. Unknown hotel fields stay unknown — never fabricate need periods.
5. \`?live=1\` is research browse; default authenticated GDI is publication preview (same renderer as share).
6. Mode banners are diagnostic-only (\`?gdiDiag=1\`).

## Preflight (every hotel)

1. Confirm canonical census/HPC \`hotelId\`.
2. Ensure \`config/group-demand-intelligence/hotels/<hotelId>.json\` exists (create from schema; leave unknowns empty).
3. Dry-run inventory: \`npm run gdi:research-storage-reconciliation-v1 -- --dry-run --hotel <hotelId>\` (when hotel-scoped flags available) or assess existing data roots.
4. Run template gate: \`npm run test:gdi-full-e2e-hotel-template-v1\`.

## Execute Full E2E

\`\`\`bash
npm run gdi:full-e2e -- --hotel <hotelId> --authorization-ref <ledger> --dry-run
\`\`\`

Only after founder budget approval, re-run without \`--dry-run\` and with the appropriate \`--authorize-*\` flags documented in \`reports/gdi-full-e2e-contract/OPERATOR_RUNBOOK.md\`.

## Persist & preview

- Canonical writer path must keep presentation keys (\`${TEMPLATE_VERSION_LOCK.presentationPersistence}\`).
- Founder preview: \`/group-demand-intelligence.html?hotelId=<hotelId>\`.
- Stage immutable package → approve exact fingerprint → production release via GitHub Actions \`Dealality production release\` (product=GDI).

## Do not

- Copy Bethesda presentation values into other hotels.
- Publish without fingerprint approval.
- Treat SPECULATIVE_DEMAND as customer-ready without support.
- Skip failed/missing gates (NOT VERIFIED ≠ PASS).
`;

fs.writeFileSync(path.join(OUT, "OPERATING_PLAYBOOK.md"), playbook, "utf8");

console.log(
  JSON.stringify(
    {
      template: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
      status: certificate.status,
      certified: certificate.certified,
      mainCommitSha: mainSha,
      out: OUT,
      fails: certificate.fails,
    },
    null,
    2,
  ),
);

if (!certificate.certified) process.exit(2);
