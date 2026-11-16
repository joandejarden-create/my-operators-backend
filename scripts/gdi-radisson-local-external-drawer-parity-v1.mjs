#!/usr/bin/env node
/**
 * Radisson local (STAGED) ↔ external (CURRENT / simulated share) drawer parity.
 *
 * Does not publish. Does not spend. Does not write Airtable.
 *
 *   node scripts/gdi-radisson-local-external-drawer-parity-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assessAuthSharePackageParity,
  assertAuthShareSimulatedParity,
  COMMERCIAL_DRAWER_PARITY_FIELDS,
} from "../lib/group-demand-intelligence/share/gdi-auth-share-package-parity-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import {
  assertPackageEssentialProseCompleteness,
  appearsTruncatedCustomerProse,
} from "../lib/group-demand-intelligence/presentation/gdi-customer-prose-completeness-v1.js";
import {
  evaluateCertifiedDrawerCommercialContent,
  summarizeCertifiedDrawerAudit,
} from "../lib/group-demand-intelligence/presentation/gdi-certified-drawer-commercial-content-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recH9h3McgMNFQEla";
const PRIOR_CURRENT_FP =
  "533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d";
const CURRENT_FP =
  "f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d";
const STAGED_FP =
  "f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d";
const OUT = path.join(
  ROOT,
  "reports/gdi-radisson-production-drawer-content-remediation-v1",
);
const PROD =
  process.env.GDI_PROD_HOST ||
  "https://my-operators-backend-production.up.railway.app";

function text(v) {
  return String(v == null ? "" : v).trim();
}

function pick(o, ...keys) {
  const canon = o?.canonicalCustomerPresentation || {};
  for (const key of keys) {
    const v = text(o?.[key] || canon?.[key] || "");
    if (v) return v;
  }
  return "";
}

function truncationHits(opportunities = []) {
  const fields = [
    ["whyThisHotel", (o) => pick(o, "whyThisHotel", "hotelProductFit")],
    [
      "potentialHotelBusiness",
      (o) =>
        pick(o, "potentialHotelBusiness", "potentialBusiness", "hotelPotentialBusiness"),
    ],
    [
      "recommendedNextStep",
      (o) =>
        pick(
          o,
          "recommendedNextStep",
          "nextSalesAction",
          "customerFacingRecommendedNextStep",
        ),
    ],
    [
      "whatIsHappening",
      (o) => pick(o, "whatIsHappening", "demandThesis", "summary", "description"),
    ],
    ["whyActNow", (o) => pick(o, "whyActNow", "timingRationale")],
  ];
  const hits = [];
  for (const o of opportunities) {
    for (const [field, getter] of fields) {
      const value = getter(o);
      if (appearsTruncatedCustomerProse(value)) {
        hits.push({
          id: o.id,
          title: text(o.displayTitle || o.title),
          field,
          length: value.length,
          tail: value.slice(-48),
        });
      }
    }
  }
  return hits;
}

async function fetchProdShareMeta() {
  try {
    const htmlRes = await fetch(`${PROD}/group-demand-intelligence-share.html`, {
      redirect: "follow",
    });
    const html = await htmlRes.text();
    const cacheBust =
      (html.match(/dealality-gdi-ui\.js\?v=([^"'\s]+)/) || [])[1] || null;
    const healthRes = await fetch(`${PROD}/health`);
    const health = await healthRes.json().catch(() => null);
    return {
      ok: htmlRes.ok,
      cacheBust,
      health,
      htmlLastModified: htmlRes.headers.get("last-modified"),
    };
  } catch (err) {
    return { ok: false, error: String(err?.message || err) };
  }
}

const store = createGdiPublicationPackageStore();
const current = store.loadCustomerVisiblePackage(HOTEL);
const stagedPtr = store.readPointer(HOTEL, "STAGED");
const staged = store.loadPackage(HOTEL, stagedPtr.publicationVersion);
const parity = assessAuthSharePackageParity(HOTEL);
assertAuthShareSimulatedParity(parity);

const stagedOpps = staged.package?.opportunities || [];
const currentOpps = current.package?.opportunities || [];
const stagedProse = assertPackageEssentialProseCompleteness(staged.package);
const currentProse = assertPackageEssentialProseCompleteness(current.package);
const stagedAudit = summarizeCertifiedDrawerAudit(
  stagedOpps.map((o) =>
    evaluateCertifiedDrawerCommercialContent(o, { hotelId: HOTEL }),
  ),
);
const currentAudit = summarizeCertifiedDrawerAudit(
  currentOpps.map((o) =>
    evaluateCertifiedDrawerCommercialContent(o, { hotelId: HOTEL }),
  ),
);
const stagedTrunc = truncationHits(stagedOpps);
const currentTrunc = truncationHits(currentOpps);

const fieldByField = [];
const stagedById = new Map(stagedOpps.map((o) => [o.id, o]));
const currentById = new Map(currentOpps.map((o) => [o.id, o]));
for (const id of [...stagedById.keys()].sort()) {
  const s = stagedById.get(id);
  const c = currentById.get(id);
  const row = {
    id,
    title: text(s?.displayTitle || s?.title),
    presentInCurrent: Boolean(c),
    diffs: [],
  };
  if (!c) {
    row.diffs.push({ field: "presence", staged: true, current: false });
  } else {
    const pairs = [
      ["title", text(s.displayTitle || s.title), text(c.displayTitle || c.title)],
      [
        "whyThisHotel",
        pick(s, "whyThisHotel", "hotelProductFit"),
        pick(c, "whyThisHotel", "hotelProductFit"),
      ],
      [
        "potentialHotelBusiness",
        pick(s, "potentialHotelBusiness", "potentialBusiness"),
        pick(c, "potentialHotelBusiness", "potentialBusiness"),
      ],
      [
        "recommendedNextStep",
        pick(s, "recommendedNextStep", "nextSalesAction"),
        pick(c, "recommendedNextStep", "nextSalesAction"),
      ],
      [
        "whatIsHappening",
        pick(s, "whatIsHappening", "demandThesis", "summary"),
        pick(c, "whatIsHappening", "demandThesis", "summary"),
      ],
      ["whyActNow", pick(s, "whyActNow"), pick(c, "whyActNow")],
    ];
    for (const [field, a, b] of pairs) {
      if (a !== b) row.diffs.push({ field, stagedLen: a.length, currentLen: b.length });
    }
  }
  if (row.diffs.length) fieldByField.push(row);
}

const prodMeta = await fetchProdShareMeta();

const report = {
  schema: "gdi-radisson-local-external-drawer-parity-v1",
  hotelId: HOTEL,
  generatedAt: new Date().toISOString(),
  fingerprints: {
    priorCurrentPreservedImmutable: PRIOR_CURRENT_FP,
    currentPublished: CURRENT_FP,
    stagedCorrected: STAGED_FP,
    currentActual: current.package?.contentFingerprintSha256 || null,
    stagedActual: staged.package?.contentFingerprintSha256 || null,
  },
  inventory: {
    staged: stagedOpps.length,
    current: currentOpps.length,
  },
  truncations: {
    staged: stagedTrunc.length,
    current: currentTrunc.length,
    stagedHits: stagedTrunc.slice(0, 5),
    currentHits: currentTrunc.slice(0, 10),
  },
  certified: {
    staged: stagedAudit,
    current: currentAudit,
  },
  proseGate: {
    stagedOk: stagedProse.ok,
    currentOk: currentProse.ok,
    currentFailingCount: currentProse.failing?.length || 0,
  },
  authShareSimulatedParity: {
    ok: parity.simulatedShareFromStaged?.parityOk === true,
    commercialDrawerFieldsCompared: COMMERCIAL_DRAWER_PARITY_FIELDS,
    fieldMismatchCount: parity.simulatedShareFromStaged?.fieldMismatchCount || 0,
    founderApprovalRequired: parity.founderApprovalRequired,
    productionExternalReady: parity.productionExternalReady,
    liveShareMatchesStaged: parity.liveShare?.matchesStagedFingerprint === true,
  },
  stagedVsCurrentFieldDiffs: {
    opportunityCountWithDiffs: fieldByField.length,
    sample: fieldByField.slice(0, 15),
  },
  productionExternal: prodMeta,
  founderApprovalApplied:
    "FOUNDER_APPROVED_FOR_PUBLICATION f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d",
  verdict: {
    localStagedClean: stagedTrunc.length === 0 && stagedAudit.fullyPassing === 66,
    simulatedShareParityClean: parity.simulatedShareFromStaged?.parityOk === true,
    currentPublishedClean: currentTrunc.length === 0 && currentAudit.fullyPassing === 66,
    currentMatchesStaged: CURRENT_FP === STAGED_FP,
    liveShareMatchesStaged: parity.liveShare?.matchesStagedFingerprint === true,
    productionDeployRequiredForExternalHost: true,
  },
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "09-LOCAL_EXTERNAL_FIELD_PARITY.json"),
  JSON.stringify(report, null, 2),
);
console.log(JSON.stringify(report.verdict, null, 2));
console.log(`Wrote ${path.join(OUT, "09-LOCAL_EXTERNAL_FIELD_PARITY.json")}`);

if (
  !(
    report.verdict.localStagedClean &&
    report.verdict.simulatedShareParityClean &&
    report.verdict.currentPublishedClean &&
    report.verdict.liveShareMatchesStaged
  )
) {
  process.exit(1);
}
if (report.fingerprints.stagedActual !== STAGED_FP) {
  console.error("STAGED fingerprint drift");
  process.exit(1);
}
if (report.fingerprints.currentActual !== CURRENT_FP) {
  console.error("CURRENT fingerprint must be approved f2bfebc9");
  process.exit(1);
}
