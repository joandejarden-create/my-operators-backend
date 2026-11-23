#!/usr/bin/env node
/**
 * Build repository-safe V1.1 frozen ORS certification fixtures.
 *
 * Sources (durable, already on main):
 * - reports/gdi-portfolio-cert-v1-1-paid-pilot/.../deep-research-selection-register.json
 * - reports/gdi-portfolio-cert-v1-2-commercial-conversion-recovery/offline-conversion-recovery.json
 * - git-history ORS for disc_9334f4f1e3dd (AHA) as structural reference
 *
 * Does NOT invent investigated URLs, Sales Ready, publication, or paid enrichment.
 * Provider-call boolean is reconstructed from frozen recovery deepResearchImprovedEvidence.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  V1_1_EIGHT_OPPORTUNITY_IDS,
  reclassifyFrozenOpportunity,
} from "../lib/group-demand-intelligence/workflow/gdi-portfolio-cert-v1-2-commercial-conversion-recovery-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const FIXTURE_VERSION = "gdi-portfolio-cert-v1-1-frozen-ors-fixtures-v1";
const outDir = path.join(root, "fixtures", FIXTURE_VERSION);

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function findObj(o, target, acc = []) {
  if (!o || typeof o !== "object") return acc;
  if (Array.isArray(o)) {
    for (const x of o) findObj(x, target, acc);
    return acc;
  }
  if (o.opportunityId === target || o.id === target) acc.push(o);
  for (const v of Object.values(o)) findObj(v, target, acc);
  return acc;
}

function buildEligibilityContract(row) {
  const gaps = row.excludeRootCause?.eligibilityGaps || [];
  const hotelFitDefensible = !gaps.includes(
    "HOTEL_FIT_NOT_DEFENSIBLE_FOR_SALES_READY",
  );
  const namedBuyer = (row.namedStakeholderCount || 0) > 0;
  const hasContactRoute = Boolean(
    row.contactAudit?.organizationalContactRoute,
  );
  return {
    version: "gdi-commercial-eligibility-contract-v1",
    status: "RESEARCH_CONTINUATION",
    commercialEligible: false,
    salesReady: false,
    researchContinuation: true,
    preserveOpportunity: true,
    doNotSilentWithdraw: true,
    doNotTreatAsExhausted: true,
    gates: {
      credibleOrganizationActivity: true,
      realisticHotelDemand: false,
      hotelFitDefensibleForSalesReady: hotelFitDefensible,
      namedRelevantBuyer: namedBuyer,
      buyerResponsibilitySupported: !gaps.includes(
        "BUYER_RESPONSIBILITY_UNKNOWN",
      ) && namedBuyer,
      usableContactRoute: hasContactRoute,
      sourcesTraceable: true,
    },
    gaps,
    unresolvedResearchGaps: gaps,
    reasons: ["CREDIBLE_ORG_ACTIVITY", ...gaps],
  };
}

function buildNamedStakeholders(row) {
  const buyers = row.contactAudit?.namedBuyers || [];
  // Prefer non-duplicated entries from recovery contact audit (already committed).
  const seen = new Set();
  const out = [];
  for (const b of buyers) {
    const key = `${b.name}|${b.role}|${b.sourceUrl || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      name: b.name,
      role: b.role || null,
      title: b.role || null,
      sourceUrl: b.sourceUrl || null,
      decisionMakerClass: b.decisionMakerClass || null,
      buyerVersusIntermediary: b.buyerVersusIntermediary || null,
    });
  }
  return out;
}

function buildPrimaryContact(row) {
  const first = (row.contactAudit?.namedBuyers || [])[0];
  if (!first?.name) return null;
  return {
    name: first.name,
    role: first.role || null,
    title: first.role || null,
    sourceUrl: first.sourceUrl || null,
    email: null,
    phone: null,
  };
}

function main() {
  const recovery = readJson(
    path.join(
      root,
      "reports/gdi-portfolio-cert-v1-2-commercial-conversion-recovery/offline-conversion-recovery.json",
    ),
  );
  const betReg = readJson(
    path.join(
      root,
      "reports/gdi-portfolio-cert-v1-1-paid-pilot/hotels/recLuxvwwxID7U2B8/gdi_portfolio_cert_v1_1_paid_pilot__recLuxvwwxID7U2B8/deep-research-selection-register.json",
    ),
  );
  const yoReg = readJson(
    path.join(
      root,
      "reports/gdi-portfolio-cert-v1-1-paid-pilot/hotels/recrPQcZg7SFARRb2/gdi_portfolio_cert_v1_1_paid_pilot__recrPQcZg7SFARRb2/deep-research-selection-register.json",
    ),
  );

  fs.mkdirSync(outDir, { recursive: true });
  const inventory = [];

  for (const id of V1_1_EIGHT_OPPORTUNITY_IDS) {
    const row = recovery.rows.find((r) => r.opportunityId === id);
    if (!row) throw new Error(`missing recovery row for ${id}`);
    const deep = findObj(betReg, id)[0] || findObj(yoReg, id)[0];
    if (!deep) throw new Error(`missing deep-register object for ${id}`);

    const eligibility = buildEligibilityContract(row);
    const namedStakeholders = buildNamedStakeholders(row);
    const primaryContact = buildPrimaryContact(row);
    const deepMeta = row.deepResearchImprovedEvidence || {};

    const ors = {
      fixtureVersion: FIXTURE_VERSION,
      fixtureKind: "HISTORICAL_V1_1_ORS_CERTIFICATION_FIXTURE",
      fixtureProvenance: {
        recoveryReport:
          "reports/gdi-portfolio-cert-v1-2-commercial-conversion-recovery/offline-conversion-recovery.json",
        deepRegisterHotelId: row.hotelId,
        notes: [
          "Curated from durable V1.1/V1.2 reports because live ORS under data/.../opportunity-research-state/ is gitignored.",
          "exaRunId/researchJobId are fixture sentinels certifying frozen providerRunRecorded=true; not live Exa API identifiers.",
          "No personal emails/phones; LinkedIn/public org routes only when already present on committed recovery report.",
        ],
      },
      opportunityId: id,
      id,
      hotelId: row.hotelId,
      hotelName: row.hotelName,
      organizationName: row.organizationName,
      title: deep.title || null,
      programmeName: deep.programmeName || null,
      hotelOpportunityThesis: deep.hotelOpportunityThesis || null,
      summaryWhyHotel: deep.hotelOpportunityThesis || null,
      seedUrls: deep.seedUrls || [],
      investigatedUrls: deep.investigatedUrls || [],
      evidence: (deep.evidence || []).map((e) => ({
        evidenceId: e.evidenceId || null,
        url: e.url || null,
        statement: e.statement || null,
        accepted: e.accepted !== false,
        provenance: e.provenance || "discovery_evidence",
      })),
      acceptedClaims: deep.acceptedClaims || [],
      namedStakeholders,
      namedStakeholdersFound: namedStakeholders.length,
      primaryContact,
      contactUrl: row.contactAudit?.organizationalContactRoute || null,
      organizationRouteUrl: row.contactAudit?.organizationalContactRoute || null,
      contactEmail: null,
      contactPhone: null,
      organizationContactEmail: null,
      customerPublicationReady: false,
      publicationOccurred: false,
      airtableWrites: false,
      commercialEligibilityStatus: "RESEARCH_CONTINUATION",
      commercialEligibilityContractV1: eligibility,
      researchContinuationRequired: true,
      historicalInternalDisposition: "EXCLUDE",
      internalDisposition: "EXCLUDE",
      dispositionReconcile: {
        version: "gdi-commercial-disposition-reconcile-v1",
        finalDisposition: "EXCLUDE",
        explanation:
          "Frozen V1.1 pre-recovery disposition retained EXCLUDE despite RESEARCH_CONTINUATION eligibility.",
        conflict: true,
        eligibilityStatus: "RESEARCH_CONTINUATION",
        v41Disposition: "EXCLUDE",
        promotionBlocked: true,
        at: recovery.at,
      },
      commercialQualificationV41: {
        version: "gdi-commercial-qualification-v41-frozen-fixture",
        internalDisposition: "EXCLUDE",
        commerciallyUsefulInternal: false,
        v3: {
          internalDisposition: "EXCLUDE",
          salesReadiness: { salesReady: false },
        },
      },
      // Sentinel only — durable recovery certifies providerRunRecorded without storing live Exa run IDs.
      researchJobId: deepMeta.providerRunRecorded
        ? `HISTORICAL_V1_1_PROVIDER_CALL__${id}`
        : null,
      exaRunId: deepMeta.providerRunRecorded
        ? `HISTORICAL_V1_1_PROVIDER_CALL__${id}`
        : null,
      exaAgentDeepResearch: deepMeta.dossierStubOnly
        ? {
            investigatedUrlCount: 0,
            stub: true,
            note: "Frozen recovery marked dossierStubOnly=true",
          }
        : {
            investigatedUrlCount: 0,
            stub: false,
          },
      unresolvedQuestions: [],
    };

    // Self-check against frozen recovery expectations before write.
    const replay = reclassifyFrozenOpportunity(ors, { replayV3: true });
    if (replay.afterDisposition !== "WATCHLIST_RESEARCH") {
      throw new Error(
        `${id}: expected afterDisposition WATCHLIST_RESEARCH got ${replay.afterDisposition}`,
      );
    }
    if (replay.beforeDisposition !== "EXCLUDE") {
      throw new Error(
        `${id}: expected beforeDisposition EXCLUDE got ${replay.beforeDisposition}`,
      );
    }
    if (replay.salesReadyAfter !== false) {
      throw new Error(`${id}: salesReadyAfter must be false`);
    }
    if (replay.drawerReady !== false) {
      throw new Error(`${id}: drawerReady must be false`);
    }
    if (replay.deepResearchImprovedEvidence.verdict !== deepMeta.verdict) {
      throw new Error(
        `${id}: deep verdict mismatch fixture=${replay.deepResearchImprovedEvidence.verdict} frozen=${deepMeta.verdict}`,
      );
    }
    for (const cls of deepMeta.forensicClasses || row.forensicClasses || []) {
      // forensic classes compared after write via suite; soft-check subset presence
      void cls;
    }
    const expectedForensic = row.forensicClasses || [];
    for (const cls of expectedForensic) {
      if (!replay.forensicClasses.includes(cls)) {
        throw new Error(
          `${id}: missing forensic class ${cls}; got ${replay.forensicClasses.join(",")}`,
        );
      }
    }

    const fileName = `${id}.json`;
    const abs = path.join(outDir, fileName);
    fs.writeFileSync(abs, `${JSON.stringify(ors, null, 2)}\n`, "utf8");
    inventory.push({
      opportunityId: id,
      hotelId: row.hotelId,
      hotelName: row.hotelName,
      organizationName: row.organizationName,
      relativePath: path
        .join("fixtures", FIXTURE_VERSION, fileName)
        .replace(/\\/g, "/"),
      beforeDisposition: "EXCLUDE",
      expectedAfterDisposition: "WATCHLIST_RESEARCH",
      investigatedUrlCount: (ors.investigatedUrls || []).length,
      seedUrlCount: (ors.seedUrls || []).length,
      evidenceRows: (ors.evidence || []).length,
      personalEmailPresent: false,
      personalPhonePresent: false,
      customerPublicationReady: false,
    });
  }

  const manifest = {
    version: FIXTURE_VERSION,
    generatedAt: new Date().toISOString(),
    purpose:
      "Repository-safe historical V1.1 ORS fixtures for V1.2/V1.3 offline certification reproducibility",
    rootCause:
      "data/group-demand-intelligence/hotels/*/opportunity-research-state/ is gitignored (d719eca7a); V1.1 paid-pilot ORS never entered source control",
    opportunityCount: inventory.length,
    expected: {
      falseExcludeBefore: 8,
      watchlistResearchAfter: 8,
      salesReady: 0,
      customerPublication: 0,
      deepResearchUrlImprovements: 0,
    },
    sanitization: {
      personalEmails: "stripped/null",
      personalPhones: "stripped/null",
      liveExaRunIds: "not included; researchJobId fixture sentinel only",
      airtableSecrets: "none",
    },
    inventory,
  };
  fs.writeFileSync(
    path.join(outDir, "MANIFEST.json"),
    `${JSON.stringify(manifest, null, 2)}\n`,
    "utf8",
  );
  console.log(
    JSON.stringify(
      {
        ok: true,
        outDir: path.relative(root, outDir).replace(/\\/g, "/"),
        count: inventory.length,
      },
      null,
      2,
    ),
  );
}

main();
