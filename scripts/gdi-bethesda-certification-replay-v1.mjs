#!/usr/bin/env node
/**
 * Bethesda certification replay ($0) — isolated candidate only.
 * Does not mutate STAGED/CURRENT / 8080 / Airtable.
 */
import fs from "fs";
import path from "path";
import { createHash } from "crypto";
import { fileURLToPath } from "url";
import { runHotelWideFinalizationStage } from "../lib/group-demand-intelligence/workflow/gdi-hotel-wide-finalization-stage-v1.js";
import { evaluateFinalizedQualifiedOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recLuxvwwxID7U2B8";
const GOLD = "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";
const OUT = path.join(ROOT, "reports/gdi-bethesda-certification-v1");
const PKG_ROOT = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  HOTEL,
);

function shaFile(p) {
  return createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}
function shaLf(p) {
  const raw = fs.readFileSync(p);
  const lf = Buffer.from(raw.toString("utf8").replace(/\r\n/g, "\n"));
  return createHash("sha256").update(lf).digest("hex");
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

fs.mkdirSync(OUT, { recursive: true });

const stagedBefore = shaFile(path.join(PKG_ROOT, "STAGED.json"));
const currentBefore = shaFile(path.join(PKG_ROOT, "CURRENT.json"));

const goldPkg = JSON.parse(
  fs.readFileSync(path.join(PKG_ROOT, GOLD, "package.json"), "utf8"),
);

/** AAD 2028 — Priority-3 Destination DC convention-services pathway (buyer still open). */
const AAD_CONTACT_PATCH = {
  primaryContact: {
    name: "Dianna Waldroup",
    role: "Vice President, Convention Services",
    title: "Vice President, Convention Services",
    organization: "Destination DC",
    email: "dianna.waldroup@destinationdc.com",
    emailOwnership: "person_specific",
    phone: "202-789-7036",
    phoneTypeLabel: "Direct / person phone",
    relevanceEvidence:
      "Public Destination DC Convention Services leadership. Destination DC markets AAD Annual Meeting 2028 (Mar 17–21, Walter E. Washington Convention Center). Credible viable alternative to request citywide housing / overflow introduction; AAD official housing buyer (Maritz/AAD Meetings) remains unnamed in public sources reviewed.",
    suggestedContactLabel:
      "Suggested contact: Dianna Waldroup (VP, Convention Services — Destination DC)",
    organizationContactRouteLabel:
      "Destination DC Convention Services — AAD 2028 citywide housing pathway",
    askToBeDirectedTo:
      "Ask to be directed to Convention Services regarding AAD Annual Meeting 2028 citywide / overflow housing",
    contactClassification: "VIABLE_ALTERNATIVE",
    buyerAuthority: "NOT_CLAIMED_AS_AAD_HOUSING_BUYER",
  },
  secondaryContact: {
    name: "Morgan Simonet",
    role: "Senior Convention Services Manager",
    title: "Senior Convention Services Manager",
    organization: "Destination DC",
    email: null,
    phone: "202-789-7044",
    phoneTypeLabel: "Direct / person phone",
    relevanceEvidence:
      "Public Destination DC Convention Services manager; secondary pathway alongside VP Convention Services for AAD 2028 citywide lodging coordination.",
    contactClassification: "VIABLE_ALTERNATIVE",
  },
  sourceUrls: [
    "https://washington.org/meetings/aad-2028",
    "https://washington.org/DC-information/meet-destination-dc-team",
    "https://www.aad.org/member/meetings-education/am27/housing/official-provider",
    "https://www.businesstravelnews.com/Suppliers/Convention-Visitor-Bureaus/Destination-DC-p3111031",
  ],
  hotelOpportunityThesis:
    "AAD Annual Meeting 2028 (Mar 17–21) is confirmed for Washington, DC at the Walter E. Washington Convention Center. Bethesda Marriott can pursue overflow / edge-of-block lodging as citywide demand fills the official Maritz-managed housing pattern. Approach Destination DC Convention Services for introduction into the citywide housing conversation while continuing to seek the named AAD/Maritz housing buyer.",
  recommendedNextStep:
    "Call Dianna Waldroup (Destination DC Convention Services) to request AAD 2028 citywide/overflow housing introduction for Bethesda Marriott; ask to be connected to the AAD/Maritz housing desk for official-block adjacency.",
  whyThisHotel:
    "Bethesda Marriott sits on the Metro-accessible Montgomery County corridor for DC citywide medical congress overflow when downtown/official blocks tighten.",
  futureDemandPathway: true,
  futureCycleEvidenceState: "FUTURE_CYCLE_EVIDENCED",
  competitiveMarketScopeEligible: true,
  marketScope: "COMPETITIVE",
  competitiveContext: {
    incumbentHotel: "Official AAD Maritz hotel block (Washington, DC citywide)",
    pathway: "OVERFLOW_OR_FUTURE_CYCLE",
    label: "EVIDENCED_OVERFLOW",
  },
  aadContactResearchV1: {
    outcome: "CONTACT_VERIFIED_PRIORITY_3_VIABLE_ALTERNATIVE",
    buyerNamedPersonResolved: false,
    maritzRemainsOrganizationOnly: true,
    sourcesReviewed: [
      "https://washington.org/meetings/aad-2028",
      "https://washington.org/DC-information/meet-destination-dc-team",
      "https://www.aad.org/member/meetings-education/am27/housing/official-provider",
      "https://www.aad.org/contact",
      "existing gold + research store (Maritz org-only)",
    ],
  },
};

const seedOpportunities = goldPkg.opportunities.map((o) => {
  if (o.id !== "gdi_opp_aad_2028_overflow") return o;
  return {
    ...o,
    ...AAD_CONTACT_PATCH,
    primaryContact: AAD_CONTACT_PATCH.primaryContact,
    secondaryContact: AAD_CONTACT_PATCH.secondaryContact,
  };
});

const aadFqo = evaluateFinalizedQualifiedOpportunity(
  seedOpportunities.find((o) => o.id === "gdi_opp_aad_2028_overflow"),
  { historicalApprovedInventory: true },
);

const hw = await runHotelWideFinalizationStage({
  hotelId: HOTEL,
  hotelName: "Bethesda Marriott",
  runId: "bethesda_certification_20261125",
  outDir: path.join(OUT, "hotel-wide-run"),
  writeCandidatePackage: true,
  agentAuthorized: false,
  paidSpendUsd: 0,
  seedOpportunities,
});

const candPath = hw.manifest?.stagedCandidate?.path;
const cand =
  candPath && fs.existsSync(candPath)
    ? JSON.parse(fs.readFileSync(candPath, "utf8"))
    : null;
const candById = new Map((cand?.opportunities || []).map((o) => [o.id, o]));
const goldIds = new Set(goldPkg.opportunities.map((o) => o.id));

const goldDisp = goldPkg.opportunities.map((o) => {
  const row = (hw.manifest?.rows || []).find((r) => r.opportunityId === o.id);
  const c = candById.get(o.id);
  return {
    canonicalId: o.id,
    title: o.title,
    disposition: row?.disposition || null,
    processingDisposition: row?.processingDisposition || null,
    reasonCodes: row?.reasonCodes || [],
    inCandidate: Boolean(c),
    goldPrimary: o.primaryContact?.name || null,
    candPrimary: c?.primaryContact?.name || null,
    goldType: o.opportunityType || o.opportunityTypeLabel || null,
    candType: c?.opportunityType || c?.opportunityTypeLabel || null,
    goldPill: o.salesActionPillLabel || o.tileBadgePlan?.salesActionPillLabel || null,
    candPill: c?.salesActionPillLabel || null,
    primaryLost:
      Boolean(text(o.primaryContact?.name)) &&
      Boolean(c) &&
      !text(c?.primaryContact?.name),
  };
});

const extras = (cand?.opportunities || []).filter((o) => !goldIds.has(o.id));
const extrasEval = extras.map((o) => {
  const ev = evaluateFinalizedQualifiedOpportunity(o);
  return {
    id: o.id,
    title: o.title,
    primary: o.primaryContact?.name || null,
    type: o.opportunityType || null,
    pill: o.salesActionPillLabel || null,
    fqoPass: ev.finalizedQualified === true,
    reasons: ev.reasons || [],
    recommendation:
      ev.finalizedQualified === true
        ? "RETAIN_IF_FQO_PASS"
        : "EXCLUDE_OR_HOLD_NOT_COUNT_INFLATION",
  };
});

const warnings = (cand?.hotelWidePassToPackageReconciliation?.exclusions || []).filter(
  (e) => e.severity === "WARNING_NOT_EXCLUSION",
);
const warningClass = warnings.map((w) => {
  const code = w.code;
  let classification = "HARMLESS_PRESENTATION_WARNING";
  if (code === "CONFLICTING_COMMERCIAL_ACTIONS") {
    classification = "REMAINS_OPEN";
  }
  if (code === "TRUNCATED_ESSENTIAL_COMMERCIAL_PROSE") {
    classification = "HARMLESS_PRESENTATION_WARNING";
  }
  return { ...w, classification };
});

const summary = {
  at: new Date().toISOString(),
  pr597: {
    state: "OPEN_AT_CERT_RUN",
    note: "GDI chat does not merge; Integration Controller owns merge",
  },
  aadResearch: {
    opportunityId: "gdi_opp_aad_2028_overflow",
    outcome: AAD_CONTACT_PATCH.aadContactResearchV1.outcome,
    buyerNamedPersonResolved: false,
    primary: AAD_CONTACT_PATCH.primaryContact.name,
    secondary: AAD_CONTACT_PATCH.secondaryContact.name,
    fqoAfterPatch: {
      finalizedQualified: aadFqo.finalizedQualified,
      reasons: aadFqo.reasons,
      processingDisposition: aadFqo.processingDisposition,
    },
  },
  reconciliation: {
    goldCount: 67,
    goldInCandidate: goldDisp.filter((r) => r.inCandidate).length,
    goldByDisp: goldDisp.reduce((a, r) => {
      a[r.disposition || "NULL"] = (a[r.disposition || "NULL"] || 0) + 1;
      return a;
    }, {}),
    goldByProc: goldDisp.reduce((a, r) => {
      a[r.processingDisposition || "NULL"] =
        (a[r.processingDisposition || "NULL"] || 0) + 1;
      return a;
    }, {}),
    primaryLost: goldDisp.filter((r) => r.primaryLost).length,
    typeChanged: goldDisp.filter(
      (r) =>
        r.inCandidate &&
        r.goldType &&
        r.candType &&
        r.goldType !== r.candType,
    ).length,
    pillChanged: goldDisp.filter(
      (r) =>
        r.inCandidate &&
        r.goldPill &&
        r.candPill &&
        r.goldPill !== r.candPill,
    ).length,
  },
  package: {
    written: hw.manifest?.stagedCandidate?.written,
    path: candPath,
    passCount: hw.manifest?.stagedCandidate?.passCount,
    packageCount: cand?.opportunityCount ?? 0,
    passToPackage: cand?.hotelWidePassToPackageReconciliation || null,
  },
  extrasEval,
  warnings: {
    count: warningClass.length,
    byClass: warningClass.reduce((a, w) => {
      a[w.classification] = (a[w.classification] || 0) + 1;
      return a;
    }, {}),
    rows: warningClass,
  },
  pointers: {
    stagedUnchanged: stagedBefore === shaFile(path.join(PKG_ROOT, "STAGED.json")),
    currentUnchanged: currentBefore === shaFile(path.join(PKG_ROOT, "CURRENT.json")),
    stagedShaRaw: stagedBefore,
    currentShaRaw: currentBefore,
    stagedShaLf: shaLf(path.join(PKG_ROOT, "STAGED.json")),
    currentShaLf: shaLf(path.join(PKG_ROOT, "CURRENT.json")),
  },
  dispositions: hw.manifest?.dispositions,
  blockers: hw.manifest?.blockers || [],
};

fs.writeFileSync(
  path.join(OUT, "CERTIFICATION_REPLAY_SUMMARY.json"),
  JSON.stringify(summary, null, 2),
);
fs.writeFileSync(
  path.join(OUT, "GOLD_67_DISPOSITIONS.json"),
  JSON.stringify({ count: goldDisp.length, rows: goldDisp }, null, 2),
);
fs.writeFileSync(
  path.join(OUT, "AAD_2028_CONTACT_RESEARCH.json"),
  JSON.stringify(
    {
      ...AAD_CONTACT_PATCH.aadContactResearchV1,
      primary: AAD_CONTACT_PATCH.primaryContact,
      secondary: AAD_CONTACT_PATCH.secondaryContact,
      fqoAfterPatch: summary.aadResearch.fqoAfterPatch,
    },
    null,
    2,
  ),
);

console.log(JSON.stringify(summary, null, 2));
