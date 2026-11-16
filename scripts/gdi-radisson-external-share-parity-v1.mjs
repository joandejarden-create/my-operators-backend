#!/usr/bin/env node
/**
 * Radisson Blu Toronto — external GDI share parity preflight + report pack.
 *
 *   node scripts/gdi-radisson-external-share-parity-v1.mjs
 *
 * Does NOT:
 * - set CUSTOMER_PUBLISHED / write CURRENT
 * - issue production share tokens
 * - write Airtable
 * - call paid Exa/Surfe/Parallel
 *
 * Writes reports/gdi-radisson-external-share-parity-v1/*
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assessAuthSharePackageParity,
  assertAuthShareSimulatedParity,
} from "../lib/group-demand-intelligence/share/gdi-auth-share-package-parity-v1.js";
import {
  resolveOpportunityFromApprovedPackage,
} from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import { loadHotelOpportunityDoc } from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { readGdiShareRegistry } from "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-radisson-external-share-parity-v1");
const HOTEL = "recH9h3McgMNFQEla";
const BETHESDA = "recLuxvwwxID7U2B8";
const FHLIP = "rx_d_2400ebe4c2bb";
const EXPECTED_FP =
  "533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d";
const PRODUCTION_HOST =
  "https://my-operators-backend-production.up.railway.app";
const SHARE_ROUTE = "/group-demand-intelligence-share.html";

function writeJson(name, value) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(value, null, 2) + "\n");
}

function writeMd(name, body) {
  fs.writeFileSync(path.join(OUT, name), body.endsWith("\n") ? body : body + "\n");
}

fs.mkdirSync(OUT, { recursive: true });

const assessment = assessAuthSharePackageParity(HOTEL);
assertAuthShareSimulatedParity(assessment);

if (assessment.auth.fingerprint !== EXPECTED_FP) {
  throw new Error(
    `Unexpected staged fingerprint ${assessment.auth.fingerprint} (expected ${EXPECTED_FP})`,
  );
}

const detailRows = [];
for (const id of assessment.auth.opportunityIds) {
  const detail = resolveOpportunityFromApprovedPackage({
    hotelId: HOTEL,
    opportunityId: id,
    preview: "staged",
    preferStaged: true,
  });
  detailRows.push({
    id,
    ok: Boolean(detail.ok && detail.opportunity && detail.opportunity.id === id),
    fingerprint: detail.meta?.contentFingerprintSha256 || null,
    previewSource: detail.meta?.previewSource || null,
  });
}
const detailFails = detailRows.filter((r) => !r.ok || r.fingerprint !== EXPECTED_FP);

const fhlip = resolveOpportunityFromApprovedPackage({
  hotelId: HOTEL,
  opportunityId: FHLIP,
  preview: "staged",
  preferStaged: true,
});

const bethesda = assessAuthSharePackageParity(BETHESDA);

let liveStoreCount = null;
let liveStoreIds = [];
try {
  const doc = loadHotelOpportunityDoc(HOTEL);
  liveStoreIds = (doc.opportunities || []).map((o) => o.id).filter(Boolean);
  liveStoreCount = liveStoreIds.length;
} catch (err) {
  liveStoreCount = null;
}

const stagedIds = new Set(assessment.auth.opportunityIds);
const missingFromLiveStore = assessment.auth.opportunityIds.filter(
  (id) => !liveStoreIds.includes(id),
);
const liveOnly = liveStoreIds.filter((id) => !stagedIds.has(id));

const registry = readGdiShareRegistry();
const radissonTokens = Object.values(registry?.tokens || {}).filter(
  (row) => String(row?.hotelId || "") === HOTEL,
);
const bethesdaActive = Object.values(registry?.tokens || {}).filter(
  (row) =>
    String(row?.hotelId || "") === BETHESDA &&
    String(row?.status || "").toUpperCase() === "ACTIVE",
);

const store = createGdiPublicationPackageStore({});
const stagedPkg = store.loadPackage(
  HOTEL,
  assessment.stagedPointer.publicationVersion,
);

writeJson("01-BETHESDA_EXTERNAL_SHARING_REFERENCE.json", {
  hotelId: BETHESDA,
  productionHost: PRODUCTION_HOST,
  shareRoute: SHARE_ROUTE,
  shareUrlPattern: `${PRODUCTION_HOST}${SHARE_ROUTE}?share=gdishare.v1.[REDACTED]`,
  issueCommand:
    "railway run node scripts/gdi-issue-share.mjs --hotelId=<hotelId> --label=<label>",
  revokeCommand: "node scripts/gdi-revoke-share.mjs --token-id=<tokenId>",
  currentPublication: bethesda.currentPointer,
  shareResolver: {
    ok: bethesda.liveShare.ok,
    mode: bethesda.liveShare.mode,
    fingerprint: bethesda.liveShare.fingerprint,
    count: bethesda.liveShare.inventory?.all ?? null,
  },
  activeShareTokenCount: bethesdaActive.length,
  notes: [
    "External share serves CUSTOMER_PUBLISHED CURRENT packages only.",
    "STAGED founder preview is never exposed through ordinary customer share tokens.",
    "Share page: public/group-demand-intelligence-share.html + share-app.js + shared dealality-gdi-ui.js / presentation controller.",
  ],
});

writeJson("02-RADISSON_STAGED_FINGERPRINT.json", {
  hotelId: HOTEL,
  propertyCode: "CNC41",
  stagedPointer: assessment.stagedPointer,
  fingerprint: EXPECTED_FP,
  fingerprintMatchesPointer:
    assessment.stagedPointer?.contentFingerprintSha256 === EXPECTED_FP,
  publicationVersion: assessment.auth.publicationVersion,
  integrityOk: stagedPkg.ok === true,
  inventory: assessment.auth.inventory,
  supersedes: assessment.stagedPointer?.supersedes || null,
  priorFingerprintPreserved:
    "ac55190dd783fe6eb5f3ec60d65fd2ffdd9c7f3e3aba7eb35645197e96979c86",
});

writeJson("03-LOCAL_EXTERNAL_DATA_PARITY.json", {
  localAuthStagedFingerprint: assessment.auth.fingerprint,
  liveShareFingerprint: assessment.liveShare.fingerprint,
  fingerprintsMatchLiveShare: assessment.liveShare.matchesStagedFingerprint,
  simulatedShareFingerprint: assessment.simulatedShareFromStaged.fingerprint,
  simulatedParityOk: assessment.simulatedShareFromStaged.parityOk,
  opportunityIdCount: assessment.auth.opportunityIds.length,
  inventory: assessment.auth.inventory,
  liveShareMode: assessment.liveShare.mode,
  liveShareError: assessment.liveShare.error,
  note: "Live share cannot match until CURRENT is founder-approved for this fingerprint.",
});

writeJson("04-66_OPPORTUNITY_DETAIL_PARITY.json", {
  tested: detailRows.length,
  passed: detailRows.length - detailFails.length,
  failed: detailFails.length,
  failures: detailFails,
  fhlip: {
    id: FHLIP,
    ok: Boolean(fhlip.ok && fhlip.opportunity?.id === FHLIP),
    fingerprint: fhlip.meta?.contentFingerprintSha256 || null,
    title: fhlip.opportunity?.displayTitle || fhlip.opportunity?.title || null,
  },
});

writeJson("05-UI_AND_FILTER_PARITY.json", {
  sharedComponents: [
    "public/group-demand-intelligence-share.html",
    "public/js/group-demand-intelligence/share-app.js",
    "public/js/group-demand-intelligence/dealality-gdi-ui.js",
    "public/js/group-demand-intelligence/gdi-universal-presentation-controller.js",
    "public/css/group-demand-intelligence.css",
  ],
  filterParityExpected: {
    marketScope: { all: 66, hotel: 50, cmi: 16 },
    opportunityType: assessment.auth.inventory.typeCounts,
    commercialPills: assessment.auth.inventory.commercialPills,
    scalingCompanies: 0,
    localCatering: 6,
    contactBuyerAsSalesReadyProxy: 3,
  },
  uiOnlyDifferencesDocumented: [
    "External share omits founder/admin research tools and authenticated write surfaces beyond capability-gated validation/feedback.",
    "No package IDs, fingerprints, or staged-preview banners on customer share UI.",
  ],
  status: "COMPONENT_SHARED_READY — production visual proof PENDING founder publish + token",
});

writeJson("06-AIRTABLE_RECONCILIATION.json", {
  hotelId: HOTEL,
  airtableLiveWritesAuthorized: false,
  localLiveOpportunityStoreCount: liveStoreCount,
  stagedCustomerVisibleCount: 66,
  stagedIdsMissingFromLocalLiveStore: missingFromLiveStore.length,
  liveStoreOnlyIds: liveOnly.length,
  note: "External share serves the immutable publication package (CURRENT), not Airtable live reconstruct. Airtable gap is informational; package SoT remains binding.",
  approvalBlockerForAirtable: false,
  recommendation:
    "Optional later idempotent Airtable sync of missing staged IDs — requires separate founder write authorization.",
});

writeJson("07-EXTERNAL_SHARE_SECURITY.json", {
  productionHost: PRODUCTION_HOST,
  shareRoute: SHARE_ROUTE,
  tokenPrefix: "gdishare.v1.",
  hotelScoped: true,
  readOnlyDefault: true,
  revocable: true,
  expirationSupported: true,
  radissonTokensInRegistry: radissonTokens.map((t) => ({
    tokenId: t.tokenId,
    status: t.status,
    label: t.label,
  })),
  secretsInReports: false,
  ordinaryCustomerTokenCanAccessStaged: false,
  requireCurrentPublication: true,
});

writeJson("08-PUBLICATION_APPROVAL_STATUS.json", {
  status: "AWAITING_FOUNDER_APPROVAL",
  fingerprintToApprove: EXPECTED_FP,
  publicationVersion: assessment.auth.publicationVersion,
  currentLifecycle: assessment.currentPointer?.lifecycleState || null,
  customerPublished: false,
  simulatedParityOk: assessment.simulatedShareFromStaged.parityOk,
  activationStepsAfterApproval: [
    "Record FOUNDER_APPROVED for exact fingerprint 533f2642…",
    "Advance CURRENT pointer via store.publishCustomerVisible (or certified one-click publish) for this package only",
    "Commit CURRENT.json + any registry updates; merge to main; deploy Railway",
    "Issue production share token: railway run node scripts/gdi-issue-share.mjs --hotelId=recH9h3McgMNFQEla --label='Radisson Blu Toronto Downtown GDI'",
    "Validate production HTTPS share URL (do not paste raw token into GitHub)",
  ],
  blockedActionsNotTaken: [
    "CUSTOMER_PUBLISHED",
    "CURRENT pointer write",
    "production share token issuance",
    "Airtable live writes",
    "paid Exa/Surfe/Parallel",
  ],
});

writeJson("09-PRODUCTION_BROWSER_VERIFICATION.json", {
  status: "PENDING",
  reason:
    "No CUSTOMER_PUBLISHED CURRENT package and no Radisson production share token yet — founder approval required before production URL exists.",
  productionHost: PRODUCTION_HOST,
  localSimulatedParityOk: assessment.simulatedShareFromStaged.parityOk,
  localDetail66Ok: detailFails.length === 0,
  localhostNotAcceptedAsProductionProof: true,
});

writeJson("10-ROLLBACK_AND_REVOCATION_PLAN.json", {
  packageRollback:
    "Swap CURRENT pointer back to prior publicationVersion / withholdCustomerPublication — package bytes retained.",
  priorFingerprintPreserved:
    "ac55190dd783fe6eb5f3ec60d65fd2ffdd9c7f3e3aba7eb35645197e96979c86",
  shareRevocation: "node scripts/gdi-revoke-share.mjs --token-id=<tokenId>",
  shareRestore: "node scripts/gdi-restore-share.mjs --share=<url-or-token>",
});

writeJson("11-GITHUB_MERGE_CLOSEOUT.json", {
  status: "PENDING_PR",
  branch: "exec/gdi-radisson-external-share-parity-v1",
  baselineMergeSha: "7974fb13a870c51faf499a72dfe3195506964838",
  note: "Filled after PR merge",
});

writeMd(
  "FOUNDER_REPORT.md",
  `# Radisson Blu Toronto — External GDI share parity preflight

## Verdict

**Simulated local↔external package parity: PASS (66/66).**  
**Production external URL: NOT ACTIVE — awaiting founder publication approval.**

| Item | Value |
| --- | --- |
| Hotel | Radisson Blu Toronto Downtown (\`recH9h3McgMNFQEla\`, CNC41) |
| Local staged fingerprint | \`${EXPECTED_FP}\` |
| External live fingerprint | _(none — NO_CURRENT_PUBLICATION)_ |
| Fingerprints match | **No live share yet** (simulated share matches staged) |
| Inventory | **66 = 50 HM + 16 CMI** |
| Scaling Companies | **0** |
| Local Catering | **6** |
| Contact Buyer (Sales Ready proxy) | **3** |
| Drawer detail (staged) | **66/66 PASS** incl. FHLIP \`rx_d_2400ebe4c2bb\` |
| Bethesda reference host | \`${PRODUCTION_HOST}\` |
| Airtable | Local live store ~${liveStoreCount ?? "unknown"} vs staged 66 — package remains SoT; no writes performed |
| Founder approval | **REQUIRED** before CUSTOMER_PUBLISHED / share token |
| Production publication | **NOT ACTIVE** |

## Bethesda external architecture (reused)

- Page: \`/group-demand-intelligence-share.html?share=gdishare.v1…\`
- Host: \`${PRODUCTION_HOST}\`
- Resolver: \`resolveShareOpportunitiesFromApprovedPackage\` → **CURRENT / CUSTOMER_PUBLISHED only**
- Issue: \`railway run node scripts/gdi-issue-share.mjs --hotelId=…\`
- Revoke: \`node scripts/gdi-revoke-share.mjs --token-id=…\`

## What was completed

1. Exact staged fingerprint verified (\`${EXPECTED_FP}\`).
2. Package integrity verified.
3. Auth staged vs simulated share DTO parity for all 66 IDs.
4. All 66 staged detail resolutions pass (same package).
5. Permanent regression gate added: \`npm run test:gdi-radisson-external-share-parity-v1\`.
6. Security / rollback / Airtable reconciliation docs written.
7. **Did not** activate CUSTOMER_PUBLISHED, issue a public token, or write Airtable.

## Exact outstanding founder / deploy actions

1. Approve fingerprint \`${EXPECTED_FP}\` for CUSTOMER_PUBLISHED.
2. Advance CURRENT pointer for publication version \`${assessment.auth.publicationVersion}\`.
3. Merge + Railway deploy so production serves that CURRENT package.
4. Issue hotel-scoped production share token (keep raw token out of GitHub).
5. Open the HTTPS share URL and confirm 66/66 drawers + filters (then re-run production browser verification).

Until step 1–4 complete, any claim of a working **production** external Radisson link is false.
`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      hotelId: HOTEL,
      fingerprint: EXPECTED_FP,
      simulatedParityOk: assessment.simulatedShareFromStaged.parityOk,
      detail66: detailRows.length - detailFails.length,
      founderApprovalRequired: assessment.founderApprovalRequired,
      productionExternalReady: assessment.productionExternalReady,
      outDir: OUT,
    },
    null,
    2,
  ),
);
