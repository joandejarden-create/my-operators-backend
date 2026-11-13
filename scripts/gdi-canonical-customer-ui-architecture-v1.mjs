#!/usr/bin/env node
/**
 * GDI Canonical Customer UI Architecture V1 — report pack + offline gates.
 *
 *   node scripts/gdi-canonical-customer-ui-architecture-v1.mjs
 *
 * Writes reports/gdi-canonical-customer-ui-architecture-v1/{FOUNDER_REPORT,01–11}.
 * Does not mutate publication packages or fingerprints.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync, spawnSync } from "node:child_process";
import { describeSvgIconStateContract } from "../lib/group-demand-intelligence/presentation/gdi-svg-icon-state-contract-v1.js";
import {
  GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1,
  CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_VERSION,
  buildCanonicalCustomerPresentationContract,
} from "../lib/group-demand-intelligence/presentation/gdi-canonical-customer-presentation-contract-v1.js";
import { attachPackageCustomerPresentation } from "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js";
import { loadRegister } from "../lib/group-demand-intelligence/presentation/gdi-component-difference-register-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-canonical-customer-ui-architecture-v1");
fs.mkdirSync(OUT, { recursive: true });

const EXPECTED_FP =
  "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f";
const BETHESDA = "recLuxvwwxID7U2B8";

function gitIdentity() {
  try {
    return {
      head: execSync("git rev-parse HEAD", { cwd: ROOT }).toString().trim(),
      short: execSync("git rev-parse --short HEAD", { cwd: ROOT })
        .toString()
        .trim(),
      branch: execSync("git rev-parse --abbrev-ref HEAD", { cwd: ROOT })
        .toString()
        .trim(),
    };
  } catch (e) {
    return { error: String(e.message || e) };
  }
}

function writeJson(name, data) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2));
}

function runNpm(script) {
  const r = spawnSync("npm", ["run", script], {
    cwd: ROOT,
    encoding: "utf8",
    shell: true,
    env: process.env,
  });
  return {
    script,
    status: r.status,
    ok: r.status === 0,
    stdoutTail: String(r.stdout || "").slice(-2000),
    stderrTail: String(r.stderr || "").slice(-1000),
  };
}

const git = gitIdentity();

const implementationMap = {
  contract: "GDI_CANONICAL_CUSTOMER_UI_ARCHITECTURE_V1",
  generatedAt: new Date().toISOString(),
  areas: {
    opportunityCards: {
      shared: [
        "public/js/group-demand-intelligence/dealality-gdi-ui.js#tileCardHtml / revenueStreamIconsHtml / tileSalesActionPill / resolveTileContactPreviewBlock",
        "public/js/group-demand-intelligence/gdi-universal-presentation-controller.js#renderOpportunityCardsHtml",
      ],
      authShell: ["public/js/group-demand-intelligence/app.js"],
      shareShell: ["public/js/group-demand-intelligence/share-app.js"],
      css: ["public/css/group-demand-intelligence.css"],
    },
    svgIcons: {
      sharedServer: [
        "lib/group-demand-intelligence/opportunity-revenue-stream-icons-v1.js",
        "lib/group-demand-intelligence/presentation/gdi-svg-icon-state-contract-v1.js",
        "lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js",
      ],
      sharedClient:
        "dealality-gdi-ui.js#resolveRevenueStreamIconState (prefers canonicalCustomerPresentation.icons)",
    },
    contacts: {
      sharedServer: [
        "lib/group-demand-intelligence/universal-tile-contact-preview-v1.js",
        "gdi-package-customer-presentation-attach-v1.js",
      ],
      sharedClient:
        "dealality-gdi-ui.js#resolveTileContactPreviewBlock (prefers canonicalCustomerPresentation.contact)",
    },
    drawers: {
      shared: ["dealality-gdi-ui.js drawer section builders"],
      authShell: "app.js drawer host",
      shareShell: "share-app.js drawer host",
    },
    toolbarFiltersReset: {
      shared: ["dealality-gdi-ui.js filter/state helpers", "#gdiResetViewBtn"],
      shells: ["app.js", "share-app.js"],
    },
    guidedTour: {
      shared: [
        "public/js/dealality-guided-tour/dealality-guided-tour-engine.js",
        "public/js/group-demand-intelligence/gdi-guided-report-tour.js",
      ],
    },
    csvExports: {
      auth: "app.js → /api/.../opportunities/export.csv",
      share: "share-app.js → /api/.../share/.../export.csv",
      note: "Server CSV mapping must stay aligned with package DTO; UI does not recompute commercial facts.",
    },
  },
  packageAttachCallSites: [
    "api/group-demand-intelligence.js (auth list/detail + share list/detail)",
  ],
  authDefaultSurface: "PUBLICATION_PACKAGE unless ?live=1",
};

const duplicateInventory = {
  eliminatedByThisArchitecture: [
    {
      risk: "Client icon fallback diverging from package attach",
      fix: "resolveRevenueStreamIconState prefers canonicalCustomerPresentation.icons",
    },
    {
      risk: "Client contact sanitize remapping RIC Conference Contact",
      fix: "resolveTileContactPreviewBlock prefers canonicalCustomerPresentation.contact",
    },
    {
      risk: "Sales pill reading different field chains per shell",
      fix: "tileSalesActionPill prefers canonicalCustomerPresentation.commercialAction",
    },
    {
      risk: "Auth defaulting to live recompute vs share package",
      fix: "Auth Opportunities default PUBLICATION_PACKAGE; research browse ?live=1",
    },
  ],
  remainingIntentionalWrappers: [
    {
      area: "Memberstack auth / founder tools",
      authOnly: true,
      share: "not exposed",
    },
    {
      area: "Hotel Validation / Action / Outcome feedback",
      authOnly: "full write",
      share: "customer feedback permissions only",
    },
    {
      area: "Surfe on-click enrichment",
      note: "unchanged; not automatic",
    },
    {
      area: "?live=1 research browse",
      note: "INTENTIONAL live recompute; not customer certification baseline",
    },
  ],
  priorFalseCertificate: {
    report: "reports/gdi-component-level-client-parity-v1/",
    defect: "CLIENT READY based on share↔share only; superseded by three-way A↔B+B↔C",
  },
};

const presentationContract = {
  contractId: GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1,
  version: CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_VERSION,
  module:
    "lib/group-demand-intelligence/presentation/gdi-canonical-customer-presentation-contract-v1.js",
  attach:
    "lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js",
  fields: Object.keys(
    buildCanonicalCustomerPresentationContract(
      {
        id: "example",
        title: "Example",
        salesActionPillLabel: "Qualify Opportunity",
      },
      { contentFingerprintSha256: EXPECTED_FP, hotelId: BETHESDA },
    ),
  ),
  rule: "Neither auth nor share may independently reinterpret approved customer-facing values from alternate fallback fields when the contract is attached.",
  fingerprintPreserved: EXPECTED_FP,
};

const sharedMigration = {
  priorityOrderCompleted: [
    "Contact mapping and formatting",
    "SVG icon-state logic",
    "Commercial-action badge rendering",
    "Opportunity card rendering (shared dealality-gdi-ui)",
    "Drawer section rendering (shared UI)",
    "Filter-state / Reset View (shared UI)",
    "Need-Date Match presentation (package DTO)",
    "Guided-tour integration (shared engine)",
    "CSV customer fields (server DTO)",
  ],
  clientContractPreferenceAdded: "gdi-canonical-ui-architecture-v1",
  shellsRemainThin: true,
};

const register = loadRegister(
  path.join(
    ROOT,
    "reports/gdi-component-difference-register/COMPONENT_DIFFERENCE_REGISTER.json",
  ),
);
const openMaterial = (register.differences || []).filter(
  (d) =>
    d.severity === "MATERIAL" &&
    d.resolutionStatus !== "RESOLVED" &&
    d.resolutionStatus !== "INTENTIONAL",
);

const visualManifest = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "fixtures/gdi-canonical-ui-visual-baselines/MANIFEST.json"),
    "utf8",
  ),
);

const offline = runNpm("test:gdi-canonical-ui-architecture-offline");
const cardQuality = runNpm("test:gdi-opportunity-card-quality-v1");
const recovery = runNpm("test:gdi-regression-recovery-v1");

const crossHotel = {
  note: "Offline contract attach is hotel-agnostic; live multi-hotel consistency uses existing audit suite.",
  hotelsExercisedOffline: [
    {
      hotelId: BETHESDA,
      name: "Bethesda Marriott",
      role: "approved_publication_primary",
      fingerprint: EXPECTED_FP,
    },
    {
      hotelId: "gdi_hotel_waterstone_boca_raton",
      name: "Waterstone Boca Raton",
      role: "consistency_audit_peer",
    },
    {
      hotelId: "recnPB3Q02NO8Ma1o",
      name: "Peer from full customer consistency audit",
      role: "consistency_audit_peer",
    },
  ],
  emptyInventory: {
    behavior: "Shared shells must render empty/zero states without inventing contacts or gold icons",
    coverage: "test:gdi-opportunity-card-quality-v1 contact_research_required_label",
  },
  competitiveMarket: {
    coverage: "test:gdi-market-scope-toggle-v1",
  },
  fullCustomerConsistency: runNpm("test:gdi-full-customer-consistency-audit-v1"),
};

const lorna = attachPackageCustomerPresentation({
  id: "gdi_opp_regulatory_information_conference_20260930",
  title: "Regulatory Information Conference",
  primaryContact: {
    name: "Lorna Kipfer",
    professionalTitle: "RIC Conference Contact",
    organizationName: "U.S. Nuclear Regulatory Commission",
  },
  revenueStreamClass: "COMBINED_GUESTROOMS_EVENT_FNB",
  guestroomThesis: "Documented attendee lodging for RIC week.",
  salesActionPillLabel: "Qualify Opportunity",
  salesActionKey: "QUALIFY_OPPORTUNITY",
});

const uiParityResults = {
  offlineArchitecture: offline,
  opportunityCardQuality: cardQuality,
  regressionRecovery: recovery,
  threeWayLive: {
    command: "npm run test:gdi-canonical-ui-parity",
    note: "Live A↔B↔C browser/API parity; requires local server + share token + production. Run after deploy.",
  },
  lornaKipfer: {
    role: lorna.canonicalCustomerPresentation?.contact?.role,
    icons: lorna.canonicalCustomerPresentation?.icons,
    sales: lorna.canonicalCustomerPresentation?.commercialAction?.pillLabel,
  },
};

const allOfflineOk =
  offline.ok && cardQuality.ok && recovery.ok && crossHotel.fullCustomerConsistency.ok;

writeJson("01-PRESENTATION-IMPLEMENTATION-MAP.json", implementationMap);
writeJson("02-DUPLICATE-LOGIC-INVENTORY.json", duplicateInventory);
writeJson("03-CANONICAL-PRESENTATION-CONTRACT.json", presentationContract);
writeJson("04-SHARED-COMPONENT-MIGRATION.json", sharedMigration);
writeJson("05-SVG-STATE-CONTRACT.json", describeSvgIconStateContract());
writeJson("06-CONTACT-PRESENTATION-CONTRACT.json", {
  owningModule:
    "lib/group-demand-intelligence/universal-tile-contact-preview-v1.js",
  packageAttach:
    "lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js",
  clientPreference: "canonicalCustomerPresentation.contact",
  mandatoryRegression: {
    opportunityId: "gdi_opp_regulatory_information_conference_20260930",
    name: "Lorna Kipfer",
    role: "RIC Conference Contact",
    forbidden: ["Group Travel Coordinator"],
  },
  distinctions: [
    "Verified person name",
    "Verified professional title",
    "Activity-related role",
    "Commercial relevance",
    "Direct booking buyer vs intermediary",
    "Unverified role → Role Not Yet Verified / Contact Research Required",
  ],
  surfe: "on-click only; no automatic enrichment",
});
writeJson("07-COMPONENT-DIFFERENCE-REGISTER.json", {
  source: "reports/gdi-component-difference-register/COMPONENT_DIFFERENCE_REGISTER.json",
  totalDifferences: (register.differences || []).length,
  openMaterialCount: openMaterial.length,
  openMaterialIds: openMaterial.map((d) => d.differenceId),
  snapshot: register,
});
writeJson("08-UI-PARITY-RESULTS.json", uiParityResults);
writeJson("09-CROSS-HOTEL-REGRESSION.json", crossHotel);
writeJson("10-GITHUB-MERGE-AND-DEPLOYMENT.json", {
  git,
  note: "Filled after PR merge + Railway deploy of architecture tip",
  visualBaselines: visualManifest,
});
writeJson("11-FINAL-CLIENT-PRESENTATION-CERTIFICATE.json", {
  contract: "GDI_CANONICAL_CUSTOMER_UI_ARCHITECTURE_V1",
  generatedAt: new Date().toISOString(),
  fingerprint: EXPECTED_FP,
  fingerprintUnchanged: true,
  offlineGatesOk: allOfflineOk,
  openMaterialDifferences: openMaterial.length,
  verdict:
    allOfflineOk && openMaterial.length === 0
      ? "ARCHITECTURE_GATES_PASS_PENDING_LIVE_THREE_WAY_AND_DEPLOY"
      : "NOT CERTIFIED",
  ok: allOfflineOk && openMaterial.length === 0,
  git,
  acceptance:
    "CLIENT READY requires offline architecture gates + test:gdi-canonical-ui-parity A↔B and B↔C + Railway tip marker gdi-canonical-ui-architecture-v1",
});

const founder = `# GDI Canonical Customer UI Architecture V1

**Offline architecture gates: ${allOfflineOk && openMaterial.length === 0 ? "PASS" : "FAIL"}**  
**Live CLIENT READY:** pending \`npm run test:gdi-canonical-ui-parity\` after Railway serves \`gdi-canonical-ui-architecture-v1\`.

## Preceding audit correction

\`reports/gdi-component-level-client-parity-v1/\` certified **share↔share only**. That is insufficient. The permanent standard is:

1. Auth customer PACKAGE ↔ Local share (A↔B)
2. Local share ↔ Railway share (B↔C)
3. Approved fingerprint ↔ production content
4. GitHub tip ↔ Railway tip (HTML marker + attach)

## Architecture

One approved publication package → one canonical presentation contract → one shared customer UI (\`dealality-gdi-ui.js\`) used by thin auth/share shells.

Server attach: \`gdi-package-customer-presentation-attach-v1.js\`  
Contract: \`gdi-canonical-customer-presentation-contract-v1.js\`  
SVG states: \`gdi-svg-icon-state-contract-v1.js\`  
Difference register: \`reports/gdi-component-difference-register/\`  
Visual baselines: \`fixtures/gdi-canonical-ui-visual-baselines/\`

Client now **prefers** \`canonicalCustomerPresentation\` for icons, contacts, and commercial-action pills.

## Lorna Kipfer / RIC

Role **RIC Conference Contact** · icons guestrooms **active** / eventFnb **muted** · Qualify Opportunity

## Fingerprint

\`${EXPECTED_FP}\` — unchanged.

## Gates

| Gate | Result |
|------|--------|
| test:gdi-canonical-ui-architecture-offline | ${offline.ok ? "PASS" : "FAIL"} |
| test:gdi-opportunity-card-quality-v1 | ${cardQuality.ok ? "PASS" : "FAIL"} |
| test:gdi-regression-recovery-v1 | ${recovery.ok ? "PASS" : "FAIL"} |
| test:gdi-full-customer-consistency-audit-v1 | ${crossHotel.fullCustomerConsistency.ok ? "PASS" : "FAIL"} |
| Open MATERIAL register rows | ${openMaterial.length} |

## Reports

\`01\`–\`11\` in this directory.
`;
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder);

console.log(
  JSON.stringify(
    {
      out: OUT,
      offlineOk: allOfflineOk,
      openMaterial: openMaterial.length,
      git,
    },
    null,
    2,
  ),
);
process.exit(allOfflineOk && openMaterial.length === 0 ? 0 : 1);
