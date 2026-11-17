#!/usr/bin/env node
/**
 * YOTEL Geneva Lake — external client publication parity preparation.
 * No CUSTOMER_PUBLISHED write, no token issue, no paid research.
 *
 *   node scripts/gdi-yotel-geneva-external-client-publication-parity-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assessAuthSharePackageParity,
  assertAuthShareSimulatedParity,
  COMMERCIAL_DRAWER_PARITY_FIELDS,
} from "../lib/group-demand-intelligence/share/gdi-auth-share-package-parity-v1.js";
import { evaluateLocalExternalCustomerLinkActivationGate } from "../lib/group-demand-intelligence/share/gdi-local-external-customer-link-activation-gate-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import { verifyPublicationPackageIntegrity } from "../lib/group-demand-intelligence/publication-package/gdi-customer-publication-package-v1.js";
import {
  evaluateCertifiedDrawerCommercialContent,
  summarizeCertifiedDrawerAudit,
} from "../lib/group-demand-intelligence/presentation/gdi-certified-drawer-commercial-content-contract-v1.js";
import { assertPackageEssentialProseCompleteness } from "../lib/group-demand-intelligence/presentation/gdi-customer-prose-completeness-v1.js";
import { readGdiShareRegistry } from "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-yotel-geneva-external-client-publication-parity-v1",
);
const HOTEL = "recrPQcZg7SFARRb2";
const EXPECTED_FP =
  "353430092ceea5626c85e04bffbeeb84d0738bf7a5382c5177d5fdf37413eba3";
const PRIOR_FP =
  "41a1b1ca0c34eb5da4b2569db499743ef1721f52376d141c379677e4cd6c054f";
const VERSION =
  "gdi_pub_recrPQcZg7SFARRb2_20261117_yotel_geneva_lake_exa_30usd_scale_v1";
const PRIOR_VERSION =
  "gdi_pub_recrPQcZg7SFARRb2_20261116_yotel_geneva_lake_commercial_quality_40plus_readiness_v1";
const PRODUCTION_HOST =
  "https://my-operators-backend-production.up.railway.app";
const SHARE_ROUTE = "/group-demand-intelligence-share.html";
const REQUIRED_PHRASE = `FOUNDER_APPROVED_FOR_PUBLICATION ${EXPECTED_FP}`;

function writeJson(name, value) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(value, null, 2) + "\n");
}

function writeMd(name, body) {
  fs.writeFileSync(path.join(OUT, name), body.endsWith("\n") ? body : `${body}\n`);
}

fs.mkdirSync(OUT, { recursive: true });

const store = createGdiPublicationPackageStore();
const staged = store.readPointer(HOTEL, "STAGED");
const current = store.readPointer(HOTEL, "CURRENT");
const loaded = store.loadPackage(HOTEL, VERSION);
const prior = store.loadPackage(HOTEL, PRIOR_VERSION);
const pkg = loaded.package;
const integrity = verifyPublicationPackageIntegrity(pkg);
const prose = assertPackageEssentialProseCompleteness(pkg);
const certified = summarizeCertifiedDrawerAudit(
  (pkg?.opportunities || []).map((o) =>
    evaluateCertifiedDrawerCommercialContent(o, { hotelId: HOTEL }),
  ),
);

const assessment = assessAuthSharePackageParity(HOTEL);
assertAuthShareSimulatedParity(assessment);
const gate = evaluateLocalExternalCustomerLinkActivationGate(HOTEL, {
  expectedFingerprint: EXPECTED_FP,
  expectedCounts: { all: 42, hotelMarket: 34, cmi: 8 },
});

const oppIdReport = (assessment.auth.opportunityIds || []).map((id) => {
  const mismatch = (assessment.simulatedShareFromStaged.fieldMismatches || []).find(
    (r) => r.id === id,
  );
  return {
    opportunityId: id,
    inAuthStaged: true,
    inSimulatedShare: !(assessment.simulatedShareFromStaged.missingInSim || []).includes(id),
    commercialFieldParity: mismatch ? "FAIL" : "PASS",
    mismatchFields: mismatch ? mismatch.mismatches.map((m) => m.field) : [],
  };
});

// Registry shape varies; collect token metadata only (never raw share secrets).
let existingYotelShareTokenIds = [];
try {
  const raw = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "config/client-share/gdi-share-registry/active-tokens.json"),
      "utf8",
    ),
  );
  const entries = Array.isArray(raw)
    ? raw
    : Array.isArray(raw?.tokens)
      ? raw.tokens
      : Object.values(raw || {}).filter((t) => t && typeof t === "object");
  existingYotelShareTokenIds = entries
    .filter((t) => t && t.hotelId === HOTEL)
    .map((t) => ({
      tokenId: t.tokenId,
      label: t.label || null,
      hasValidate: Array.isArray(t.capabilities)
        ? t.capabilities.includes("CAN_VALIDATE")
        : false,
      status: t.status || "ACTIVE",
    }));
} catch {
  existingYotelShareTokenIds = [];
}
try {
  readGdiShareRegistry();
} catch {
  /* optional — file-based inventory above is authoritative for reports */
}

const readiness = {
  ok:
    gate.ok === true &&
    String(pkg?.contentFingerprintSha256).toLowerCase() === EXPECTED_FP &&
    integrity.ok === true &&
    prose.ok === true &&
    certified.fullyPassing === 42 &&
    certified.withTruncatedText === 0 &&
    prior.ok === true &&
    String(prior.package.contentFingerprintSha256).toLowerCase() === PRIOR_FP &&
    !current,
  hotelId: HOTEL,
  fingerprint: EXPECTED_FP,
  staged,
  current,
  inventory: assessment.auth.inventory,
  integrityOk: integrity.ok,
  proseOk: prose.ok,
  certified,
  priorPackagePreserved: {
    ok: prior.ok,
    fingerprint: prior.package?.contentFingerprintSha256 || null,
    expected: PRIOR_FP,
  },
  gate,
  founderApprovalGate: {
    status: "BLOCKED_WAITING_EXPLICIT_PHRASE",
    requiredPhrase: REQUIRED_PHRASE,
    note: "Do not publish or issue production share until founder supplies exact phrase.",
  },
  productionHost: PRODUCTION_HOST,
  shareRoute: SHARE_ROUTE,
  existingYotelShareTokenIds,
};

writeJson("01-PUBLICATION_READINESS.json", readiness);
writeJson("02-PACKAGE_FINGERPRINT_AUDIT.json", {
  expected: EXPECTED_FP,
  stagedPointer: staged,
  packageFingerprint: pkg?.contentFingerprintSha256 || null,
  integrity,
  priorFingerprint: PRIOR_FP,
  priorPresent: prior.ok,
  currentPresent: Boolean(current),
});
writeJson("03-OPPORTUNITY_ID_COMPARISON.json", {
  count: oppIdReport.length,
  allPass: oppIdReport.every((r) => r.commercialFieldParity === "PASS"),
  opportunities: oppIdReport,
});
writeJson("04-COMMERCIAL_FIELD_COMPARISON.json", {
  fields: COMMERCIAL_DRAWER_PARITY_FIELDS,
  fieldMismatchCount: assessment.simulatedShareFromStaged.fieldMismatchCount,
  fieldMismatches: assessment.simulatedShareFromStaged.fieldMismatches,
  simulatedParityOk: assessment.simulatedShareFromStaged.parityOk,
});
writeJson("05-AUTH_SHARE_PARITY_ASSESSMENT.json", assessment);
writeJson("06-CONTACT_FEEDBACK_CAPABILITY_AUDIT.json", {
  surfe: {
    policy: "ON_CLICK_FOUNDER_GATED",
    externalDefault: "DO_NOT_SILENTLY_ENABLE_PAID_SURFE",
    guidance:
      "Until separately authorized, external UI must show guidance instead of a dead paid enrichment button",
  },
  feedback: {
    schema: "existing hotel validation / action / outcome",
    defaultIssueCapabilities: "READ_ONLY",
    bethesdaStyleRequires: ["CAN_VALIDATE", "CAN_RECORD_ACTION", "CAN_RECORD_OUTCOME"],
    authorizationRequired: true,
    plan: "Issue read-only share first after publication; prepare feedback-enabled grant for separate founder approval",
  },
  existingYotelShareTokenIds,
});
writeJson("07-APPROVED_CAPABILITY_DIFFERENCES.json", {
  differences: gate.approvedCapabilityDifferences,
});

const receiptTemplate = {
  authorizationType: "FOUNDER_APPROVED_FOR_PUBLICATION",
  explicitPhrase: REQUIRED_PHRASE,
  approved: false,
  hotelId: HOTEL,
  publicationId: pkg?.publicationId || null,
  publicationVersion: VERSION,
  contentFingerprintSha256: EXPECTED_FP,
  approvedAt: null,
  approvedBy: null,
  authority: "FOUNDER_EXPLICIT_APPROVAL_PHRASE",
  source: "awaiting_cursor_chat_explicit_phrase",
  note: "Set approved=true only after founder pastes the exact FOUNDER_APPROVED_FOR_PUBLICATION phrase.",
};
writeJson("FOUNDER_APPROVED_FOR_PUBLICATION.template.json", receiptTemplate);
if (!fs.existsSync(path.join(OUT, "FOUNDER_APPROVED_FOR_PUBLICATION.json"))) {
  writeJson("FOUNDER_APPROVED_FOR_PUBLICATION.json", receiptTemplate);
}

writeMd(
  "FOUNDER_REPORT.md",
  `# YOTEL Geneva Lake — External client publication + local parity V1

**Status:** PREPARATION COMPLETE — BLOCKED at founder approval gate  
**Hotel:** YOTEL Geneva Lake (\`${HOTEL}\`)  
**Fingerprint:** \`${EXPECTED_FP}\`  
**Inventory:** ${assessment.auth.inventory?.all} total · ${assessment.auth.inventory?.hotelMarket} Hotel Market · ${assessment.auth.inventory?.cmi} CMI  

## Publication readiness

| Gate | Result |
|---|---|
| Staged fingerprint | ${String(pkg?.contentFingerprintSha256).toLowerCase() === EXPECTED_FP ? "PASS" : "FAIL"} |
| Package integrity | ${integrity.ok ? "PASS" : "FAIL"} |
| 42 unique IDs | ${assessment.auth.opportunityIds?.length === 42 ? "PASS" : "FAIL"} |
| 34 / 8 inventory | ${assessment.auth.inventory?.hotelMarket === 34 && assessment.auth.inventory?.cmi === 8 ? "PASS" : "FAIL"} |
| Certified drawers | ${certified.fullyPassing}/${certified.total ?? 42} · truncated=${certified.withTruncatedText} |
| Essential prose | ${prose.ok ? "PASS" : "FAIL"} |
| Auth↔simulated share commercial parity | ${assessment.simulatedShareFromStaged.parityOk ? "PASS" : "FAIL"} |
| Prior package 41a1b1ca preserved | ${prior.ok ? "PASS" : "FAIL"} |
| CURRENT present | ${current ? "YES (unexpected for pre-publish)" : "NO (expected)"} |

## Founder approval gate (HARD STOP)

Required exact phrase:

\`\`\`
${REQUIRED_PHRASE}
\`\`\`

This chat has **not** supplied that phrase. Activation remains dry-run only.

After authorization:
1. Set \`FOUNDER_APPROVED_FOR_PUBLICATION.json\` approved=true
2. \`node scripts/gdi-activate-yotel-geneva-35343009-approved.mjs --apply --confirm-fingerprint=${EXPECTED_FP}\`
3. Commit CURRENT + receipt → PR → merge
4. Railway production release for exact merge SHA
5. \`railway run node scripts/gdi-issue-share.mjs --hotelId=${HOTEL} --label="YOTEL Geneva Lake GDI"\`
6. Optional feedback grant via \`gdi-grant-share-lifecycle-capabilities.mjs\` after separate approval
7. Production browser QA of all 42 drawers

## Capability differences (approved, not parity failures)

- Local uses founder auth + \`preview=staged\`; external uses share token + CURRENT only
- Surfe stays on-click / founder-gated — not silent paid enrichment for clients
- Feedback (Bethesda-style) requires explicit \`CAN_VALIDATE\` / action / outcome capabilities

## External URL

**Not issued.** Production CUSTOMER_PUBLISHED pointer and share token await founder phrase + deploy.

Planned path shape (token redacted):  
\`${PRODUCTION_HOST}${SHARE_ROUTE}?share=gdishare.v1.<redacted>\`

## GitHub

Filled when activation PR merges (post-approval).
`,
);

console.log(
  JSON.stringify(
    {
      ok: readiness.ok,
      gate: gate.ok,
      fingerprint: EXPECTED_FP,
      inventory: assessment.auth.inventory,
      founderApproval: "BLOCKED",
      out: OUT,
    },
    null,
    2,
  ),
);
process.exit(readiness.ok ? 0 : 1);
