#!/usr/bin/env node
/**
 * Bethesda gold-standard diagnostic replay V1 ($0).
 *
 * - Reconstructs BETHESDA_GOLD_STANDARD_67.json from approved package
 * - Evaluates current FQO against gold rows (historical compatibility probe)
 * - Runs hotel-wide finalization stage with writeCandidate into isolated reports/
 * - Optionally invokes canonical Full E2E in dry-run (no paid providers)
 * - Emits parity matrix + founder metrics
 *
 * NEVER writes STAGED/CURRENT pointers. NEVER calls paid providers.
 */
import fs from "fs";
import path from "path";
import { createHash } from "crypto";
import { fileURLToPath } from "url";
import { evaluateFinalizedQualifiedOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import { runHotelWideFinalizationStage } from "../lib/group-demand-intelligence/workflow/gdi-hotel-wide-finalization-stage-v1.js";
import {
  runCanonicalFullE2eResearch,
  CANONICAL_EXECUTION_MODE,
} from "../lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js";
import { CHAIN_STAGES } from "../lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js";

function readPointerFile(hotelId, kind) {
  const p = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotelId,
    kind === "CURRENT" ? "CURRENT.json" : "STAGED.json",
  );
  if (!fs.existsSync(p)) return null;
  return {
    path: p,
    sha256: sha256(fs.readFileSync(p)),
    json: JSON.parse(fs.readFileSync(p, "utf8")),
  };
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "recLuxvwwxID7U2B8";
const GOLD_VERSION = "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";
const OUT = path.join(ROOT, "reports", "gdi-bethesda-gold-standard-replay-v1");
const GOLD_PKG = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  HOTEL_ID,
  GOLD_VERSION,
  "package.json",
);

function sha256(buf) {
  return createHash("sha256").update(buf).digest("hex");
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function writeJson(rel, obj) {
  const p = path.join(OUT, rel);
  ensureDir(path.dirname(p));
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + "\n", "utf8");
  return p;
}

function pickContact(c) {
  if (!c || typeof c !== "object") return null;
  return {
    name: text(c.name) || null,
    role: text(c.role || c.title) || null,
    organization: text(c.organization) || null,
    email: text(c.email) || null,
    emailOwnership: text(c.emailOwnership) || null,
    phone: text(c.phone) || null,
    phoneTypeLabel: text(c.phoneTypeLabel || c.phoneType) || null,
    linkedinUrl: text(c.linkedinUrl || c.linkedin || c.profileUrl) || null,
    relevanceEvidence: text(c.relevanceEvidence) || null,
    suggestedContactLabel: text(c.suggestedContactLabel) || null,
    organizationContactRouteLabel:
      text(c.organizationContactRouteLabel) || null,
    askToBeDirectedTo: text(c.askToBeDirectedTo) || null,
  };
}

function extractUrls(opp) {
  const urls = new Set();
  for (const k of ["sourceUrls", "evidenceUrls", "sources", "citations"]) {
    const arr = opp[k];
    if (Array.isArray(arr)) {
      for (const u of arr) {
        const s = typeof u === "string" ? u : u?.url;
        if (/^https?:\/\//i.test(String(s || ""))) urls.add(String(s));
      }
    }
  }
  const blob = JSON.stringify(opp);
  for (const m of blob.match(/https?:\/\/[^\\"'\s]+/g) || []) {
    urls.add(m.replace(/[),.;]+$/, ""));
  }
  return [...urls];
}

function extractGoldRow(opp) {
  const ccp = opp.canonicalCustomerPresentation || {};
  const display = ccp.display || {};
  const badges = opp.tileBadgePlan || {};
  return {
    canonicalId: text(opp.id || opp.opportunityId),
    opportunityTitle: text(opp.title || display.title || opp.displayTitle),
    demandGenerator: text(opp.organizationName || display.organizationName),
    demandType: text(opp.demandFamily || opp.demandFamilyLabel),
    demandEvidenceUrls: extractUrls(opp),
    demandConfidence: text(
      opp.demandConfidence || opp.confidence || opp.evidenceConfidence,
    ),
    eventOrBookingDates: {
      display: text(opp.eventDateDisplay || badges.datePillDisplayLabel),
      start: text(opp.tileEventStartDate || badges.tileEventStartDate),
      end: text(opp.tileEventEndDate || badges.tileEventEndDate),
      timingType: text(badges.timingType || badges.canonicalTiming?.timingType),
      hasVerifiedUpcomingDate: Boolean(
        badges.hasVerifiedEventDate ||
          badges.canonicalTiming?.hasVerifiedUpcomingDate,
      ),
    },
    futureCycleOrSourcingWindow: text(
      opp.eventsOpportunityOverview?.demandCycle ||
        opp.hotelSourcingStatusLabel ||
        badges.canonicalTiming?.timingNotes,
    ),
    hotelRelevance: text(
      opp.summaryWhat ||
        opp.whatIsHappening ||
        display.summary ||
        opp.cardWhyNowLine,
    ),
    marketScope: {
      hotel: opp.hotelMarketScopeEligible !== false,
      competitive: opp.competitiveMarketScopeEligible === true,
      competitiveStatus: text(opp.competitiveStatus),
      listScope: text(ccp.listScope || opp.inventoryPopulationPrimary),
    },
    contacts: {
      primary: pickContact(opp.primaryContact),
      secondary: pickContact(opp.secondaryContact),
      tilePreview: opp.tileContactPreview
        ? {
            name: text(opp.tileContactPreview.name || opp.tileContactPreview.displayName),
            role: text(
              opp.tileContactPreview.displayRole ||
                opp.tileContactPreview.roleLabel,
            ),
            kind: text(opp.tileContactPreview.kind),
          }
        : null,
    },
    commercial: {
      priority: text(opp.priority || badges.priorityRaw),
      priorityPill: text(opp.priorityPillLabel || badges.priorityPillLabel),
      opportunityType: text(opp.opportunityType || opp.opportunityTypeLabel),
      opportunityTypeFilter: text(opp.opportunityTypeFilterCategory),
      salesActionKey: text(opp.salesActionKey || badges.salesActionKey),
      salesActionPill: text(
        opp.salesActionPillLabel || badges.salesActionPillLabel,
      ),
      commercialStageKey: text(
        opp.commercialStageKey || badges.commercialStageKey,
      ),
      commercialStagePill: text(
        opp.commercialStagePillLabel || badges.commercialStagePillLabel,
      ),
      recommendedAction: text(
        opp.recommendedNextAction ||
          opp.recommendedNextStep ||
          opp.recommendedAction ||
          ccp.commercialAction?.recommendedNextStep,
      ),
      thesis: text(opp.summaryWhat || display.summary),
      whyNow: text(opp.cardWhyNowLine),
    },
    presentation: {
      cardTitle: text(display.title || opp.displayTitle || opp.title),
      classification: text(
        opp.tileClassificationLine || display.opportunityTypeLabel,
      ),
      filterCategory: text(opp.opportunityTypeFilterCategory),
      drawerHeaderTitle: text(
        opp.drawerHeaderTitle || display.drawerHeaderTitle,
      ),
      timingLabel: text(display.timingLabel || opp.eventDateDisplay),
    },
    rawFieldPresence: {
      hasPrimaryContact: Boolean(opp.primaryContact?.name),
      hasSecondaryContact: Boolean(opp.secondaryContact?.name),
      hasPrimaryEmail: Boolean(opp.primaryContact?.email),
      hasPrimaryPhone: Boolean(opp.primaryContact?.phone),
      hasSourceUrlsArray: Array.isArray(opp.sourceUrls) && opp.sourceUrls.length > 0,
      hasCanonicalCustomerPresentation: Boolean(ccp.contractId),
      hasTileBadgePlan: Boolean(badges.version),
    },
  };
}

function classifyParity(gold, fqo, candidateRow) {
  const reasons = [];
  if (!candidateRow) {
    // Not in new PASS set — diagnose why
    if (!fqo.finalizedQualified) {
      const tech = fqo.reasons || [];
      const evidenceGap = tech.includes("MISSING_EVIDENCE_URLS");
      const contactGap =
        tech.includes("NAMED_COMMERCIAL_PERSON_REQUIRED") ||
        tech.includes("PERSON_SPECIFIC_CHANNEL_REQUIRED");
      if (evidenceGap && !gold.rawFieldPresence.hasSourceUrlsArray) {
        return {
          disposition: "EVIDENCE_MISSING",
          reasons: tech,
          note: "Gold presentation row lacks top-level sourceUrls; FQO gate fails on serialization shape",
        };
      }
      if (contactGap && gold.contacts.primary?.name) {
        return {
          disposition: "TECHNICAL_REGRESSION",
          reasons: tech,
          note: "Gold has named primary but current FQO channel/person gates fail",
        };
      }
      if (tech.includes("NO_VALID_FUTURE_SOURCING_PATHWAY")) {
        return {
          disposition: "GENUINE_COMMERCIAL_CHANGE",
          reasons: tech,
          note: "Future pathway gate — may be expiration or recurring-pathway miss",
        };
      }
      return {
        disposition: "TECHNICAL_REGRESSION",
        reasons: tech,
        note: "FQO failed against historically approved opportunity",
      };
    }
    return {
      disposition: "TECHNICAL_REGRESSION",
      reasons: ["MISSING_FROM_CANDIDATE"],
      note: "FQO pass but absent from candidate",
    };
  }

  // In candidate — compare retention
  const gName = gold.contacts.primary?.name || "";
  const cName =
    candidateRow.primaryContact?.name ||
    candidateRow.contactName ||
    "";
  if (gName && cName && gName.toLowerCase() !== String(cName).toLowerCase()) {
    reasons.push("PRIMARY_NAME_CHANGED");
  }
  if (gName && !cName) reasons.push("PRIMARY_LOST");

  const gType = gold.commercial.opportunityType;
  const cType = text(
    candidateRow.opportunityType || candidateRow.opportunityTypeLabel,
  );
  if (gType && cType && gType !== cType) reasons.push("OPPORTUNITY_TYPE_CHANGED");
  if (gType && (!cType || /other|unknown|generic/i.test(cType))) {
    reasons.push("GENERIC_OR_MISSING_TYPE");
  }

  const gPill = gold.commercial.salesActionPill;
  const cPill = text(
    candidateRow.salesActionPillLabel || candidateRow.salesActionKey,
  );
  if (gPill && cPill && gPill !== cPill) reasons.push("SALES_PILL_CHANGED");

  if (reasons.some((r) => /LOST|GENERIC/.test(r))) {
    return {
      disposition: "TECHNICAL_REGRESSION",
      reasons,
      note: "Material field regression vs gold",
    };
  }
  if (reasons.length) {
    return {
      disposition: "GENUINE_COMMERCIAL_CHANGE",
      reasons,
      note: "Fields differ; review commercially",
    };
  }
  return {
    disposition: "PARITY_PASS",
    reasons: [],
    note: "Present in candidate with core identity retained",
  };
}

async function main() {
  ensureDir(OUT);

  const stagedBefore = readPointerFile(HOTEL_ID, "STAGED");
  const currentBefore = readPointerFile(HOTEL_ID, "CURRENT");
  const goldRaw = fs.readFileSync(GOLD_PKG);
  const goldPkg = JSON.parse(goldRaw.toString("utf8"));
  const goldRows = goldPkg.opportunities.map(extractGoldRow);

  const goldManifest = {
    contract: "gdi-bethesda-gold-standard-67-v1",
    hotelId: HOTEL_ID,
    hotelName: goldPkg.hotelName || "Bethesda Marriott",
    publicationVersion: GOLD_VERSION,
    packageSha256: sha256(goldRaw),
    opportunityCount: goldRows.length,
    extractedAt: new Date().toISOString(),
    opportunities: goldRows,
  };
  writeJson("BETHESDA_GOLD_STANDARD_67.json", goldManifest);

  // FQO against gold presentation rows (as-is)
  const fqoOnGold = goldRows.map((g, i) => {
    const raw = goldPkg.opportunities[i];
    const evaluation = evaluateFinalizedQualifiedOpportunity(raw);
    return {
      canonicalId: g.canonicalId,
      finalizedQualified: evaluation.finalizedQualified,
      status: evaluation.status,
      reasons: evaluation.reasons,
      gates: evaluation.gates,
      namedCoverage: evaluation.namedCoverage,
      secondaryPresent: evaluation.secondaryPresent,
      evidenceUrlCount: evaluation.evidenceUrls?.length || 0,
    };
  });
  writeJson("FQO_ON_GOLD_PACKAGE.json", {
    hotelId: HOTEL_ID,
    pass: fqoOnGold.filter((r) => r.finalizedQualified).length,
    fail: fqoOnGold.filter((r) => !r.finalizedQualified).length,
    reasonHistogram: fqoOnGold.reduce((acc, r) => {
      for (const x of r.reasons || []) acc[x] = (acc[x] || 0) + 1;
      return acc;
    }, {}),
    rows: fqoOnGold,
  });

  // Hotel-wide finalization → isolated candidate (no pointer)
  const hwOut = path.join(OUT, "hotel-wide-run");
  ensureDir(hwOut);
  const hotelWide = await runHotelWideFinalizationStage({
    hotelId: HOTEL_ID,
    hotelName: goldPkg.hotelName || "Bethesda Marriott",
    runId: `bethesda_gold_replay_${new Date().toISOString().slice(0, 10).replace(/-/g, "")}`,
    outDir: hwOut,
    writeCandidatePackage: true,
    agentAuthorized: false,
    paidSpendUsd: 0,
    seedOpportunities: goldPkg.opportunities,
  });
  writeJson("HOTEL_WIDE_REPLAY_SUMMARY.json", {
    ok: hotelWide.ok,
    dispositions: hotelWide.manifest?.dispositions,
    stagedCandidate: hotelWide.manifest?.stagedCandidate,
    pointersUnchanged: hotelWide.pointersUnchanged,
    reconciliation: hotelWide.manifest?.reconciliation,
    blockers: hotelWide.manifest?.blockers || [],
    inventory: hotelWide.manifest?.inventory,
  });

  const candidatePath = hotelWide.manifest?.stagedCandidate?.packagePath;
  let candidatePkg = null;
  if (candidatePath && fs.existsSync(candidatePath)) {
    candidatePkg = JSON.parse(fs.readFileSync(candidatePath, "utf8"));
    // Copy reference into reports (do not rely solely on bundled path)
    writeJson(
      "REPLAY_STAGED_CANDIDATE_PACKAGE.json",
      {
        publicationVersion: candidatePkg.publicationVersion,
        opportunityCount: candidatePkg.opportunityCount,
        lifecycleState: candidatePkg.lifecycleState,
        opportunityIds: (candidatePkg.opportunities || []).map(
          (o) => o.id || o.opportunityId,
        ),
        note: "Full package also at bundled path if written; IDs listed here for audit",
      },
    );
  }

  const candidateById = new Map();
  for (const o of candidatePkg?.opportunities || []) {
    candidateById.set(text(o.id || o.opportunityId), o);
  }

  const parityRows = goldRows.map((g, i) => {
    const fqo = fqoOnGold[i];
    const cand = candidateById.get(g.canonicalId) || null;
    const parity = classifyParity(g, fqo, cand);
    return {
      canonicalId: g.canonicalId,
      title: g.opportunityTitle,
      goldPrimary: g.contacts.primary?.name || null,
      goldSecondary: g.contacts.secondary?.name || null,
      goldType: g.commercial.opportunityType,
      goldSalesPill: g.commercial.salesActionPill,
      goldPriority: g.commercial.priority,
      inCandidate: Boolean(cand),
      fqoPass: fqo.finalizedQualified,
      fqoReasons: fqo.reasons,
      parityDisposition: parity.disposition,
      parityReasons: parity.reasons,
      parityNote: parity.note,
    };
  });

  const dispositionCounts = parityRows.reduce((acc, r) => {
    acc[r.parityDisposition] = (acc[r.parityDisposition] || 0) + 1;
    return acc;
  }, {});

  const metrics = {
    originalIdsReconciled: parityRows.length,
    expected67: parityRows.length === 67,
    newCandidateCount: candidatePkg?.opportunityCount ?? 0,
    namedPrimaryRetention: {
      goldWithPrimary: goldRows.filter((g) => g.contacts.primary?.name).length,
      candidateWithPrimary: (candidatePkg?.opportunities || []).filter(
        (o) => o.primaryContact?.name,
      ).length,
      unexplainedPrimaryLoss: parityRows.filter(
        (r) =>
          r.goldPrimary &&
          r.inCandidate &&
          (r.parityReasons || []).includes("PRIMARY_LOST"),
      ).length,
    },
    secondaryRetention: {
      goldWithSecondary: goldRows.filter((g) => g.contacts.secondary?.name)
        .length,
      candidateWithSecondary: (candidatePkg?.opportunities || []).filter(
        (o) => o.secondaryContact?.name,
      ).length,
    },
    publicChannelRetention: {
      goldWithEmail: goldRows.filter((g) => g.contacts.primary?.email).length,
      goldWithPhone: goldRows.filter((g) => g.contacts.primary?.phone).length,
      fqoChannelFail: fqoOnGold.filter((r) =>
        (r.reasons || []).includes("PERSON_SPECIFIC_CHANNEL_REQUIRED"),
      ).length,
      fqoEvidenceFail: fqoOnGold.filter((r) =>
        (r.reasons || []).includes("MISSING_EVIDENCE_URLS"),
      ).length,
    },
    opportunityTypeCoverage: {
      goldDistinct: [...new Set(goldRows.map((g) => g.commercial.opportunityType).filter(Boolean))].length,
      genericOrMissingInParity: parityRows.filter((r) =>
        (r.parityReasons || []).includes("GENERIC_OR_MISSING_TYPE"),
      ).length,
    },
    salesPillCoverage: {
      goldWithPill: goldRows.filter((g) => g.commercial.salesActionPill).length,
      changed: parityRows.filter((r) =>
        (r.parityReasons || []).includes("SALES_PILL_CHANGED"),
      ).length,
    },
    commercialRecommendationCompleteness: {
      goldWithAction: goldRows.filter((g) => g.commercial.recommendedAction)
        .length,
      fqoActionFail: fqoOnGold.filter((r) =>
        (r.reasons || []).includes("MISSING_HOTEL_SPECIFIC_COMMERCIAL_ACTION"),
      ).length,
    },
    dispositionCounts,
    technicalRegressionCount: dispositionCounts.TECHNICAL_REGRESSION || 0,
    genuineExpirationOrChangeCount:
      (dispositionCounts.GENUINE_COMMERCIAL_CHANGE || 0) +
      (dispositionCounts.EVIDENCE_MISSING || 0),
    researchGapsRequiringPaid: parityRows.filter(
      (r) => r.parityDisposition === "RESEARCH_COMPLETION_REQUIRED",
    ).length,
  };

  writeJson("BETHESDA_67_REPLAY_PARITY_MATRIX.json", {
    hotelId: HOTEL_ID,
    goldVersion: GOLD_VERSION,
    candidateVersion: candidatePkg?.publicationVersion || null,
    metrics,
    rows: parityRows,
  });

  // Canonical Full E2E dry-run (reuse evidence; no paid)
  let canonicalTrace = null;
  try {
    const e2e = await runCanonicalFullE2eResearch({
      hotelId: HOTEL_ID,
      mode: CANONICAL_EXECUTION_MODE.DRY_RUN,
      dryRun: true,
      authorizePaidDiscovery: false,
      authorizePaidResearch: false,
      agentAuthorized: false,
      budgetCeilingUsd: 0,
      writeHotelWideCandidatePackage: true,
      hotelWideOutDir: path.join(OUT, "canonical-dry-run-hotel-wide"),
    });
    canonicalTrace = {
      ok: e2e.ok !== false,
      mode: e2e.mode || CANONICAL_EXECUTION_MODE.DRY_RUN,
      contract: e2e.contract,
      stagesPresent: Object.keys(e2e).filter((k) =>
        /stage|chain|hotelWide|persistence|discovery|research/i.test(k),
      ),
      hotelWide: e2e.hotelWideFinalization
        ? {
            ok: e2e.hotelWideFinalization.ok,
            dispositions: e2e.hotelWideFinalization.manifest?.dispositions,
            pointerActivated:
              e2e.hotelWideFinalization.manifest?.stagedCandidate
                ?.pointerActivated,
          }
        : e2e.hotelWide || null,
      blocked: e2e.blocked || e2e.blockers || null,
      summaryKeys: Object.keys(e2e),
      note: "Dry-run / $0 — not live production equivalence",
    };
  } catch (err) {
    canonicalTrace = {
      ok: false,
      error: String(err?.message || err),
      note: "Canonical dry-run threw; see stack for disconnected stages",
      stack: String(err?.stack || "").split("\n").slice(0, 12),
    };
  }
  writeJson("CANONICAL_DRY_RUN_TRACE.json", canonicalTrace);

  const stagedAfter = readPointerFile(HOTEL_ID, "STAGED");
  const currentAfter = readPointerFile(HOTEL_ID, "CURRENT");
  writeJson("POINTER_PRESERVATION.json", {
    stagedBefore,
    stagedAfter,
    currentBefore,
    currentAfter,
    stagedUnchanged: stagedBefore?.sha256 === stagedAfter?.sha256,
    currentUnchanged: currentBefore?.sha256 === currentAfter?.sha256,
  });

  writeJson("CALL_GRAPH_STATIC.json", {
    entry: "gdi-canonical-full-e2e-entry-v1 / runCanonicalFullE2eResearch",
    originalChainStages: CHAIN_STAGES,
    founderRequestedStages: [
      "DISCOVERY",
      "DEMAND_VALIDATION",
      "HOTEL_FIT",
      "WHO_BUYER_RESEARCH",
      "ALT_CONTACT_RESEARCH",
      "PERSON_VERIFICATION",
      "HOW_PUBLIC_CONTACT_RESEARCH",
      "COMMERCIAL_QUALIFICATION",
      "OPPORTUNITY_CLASSIFICATION",
      "COMMERCIAL_RECOMMENDATION",
      "HOTEL_WIDE_RECONCILIATION",
      "FQO_CERTIFICATION",
      "PRESENTATION_PROJECTION",
      "VERSIONED_STAGED_CANDIDATE",
    ],
    mappingNotes: [
      "CHAIN_STAGES collapses WHO/ALT/HOW under CONTACT_RESEARCH (+ nested stage events)",
      "HOTEL_WIDE_RECONCILIATION runs after chain via runHotelWideFinalizationStage",
      "FQO_CERTIFICATION is applied inside hotel-wide dispositions + optional quality gate",
      "PRESENTATION_PROJECTION occurs in chain SALES_INTELLIGENCE_OUTPUT / ten-section builders",
      "Dry-run does not exercise live Exa WHO/HOW provider calls",
    ],
  });

  console.log(
    JSON.stringify(
      {
        ok: true,
        out: OUT,
        goldCount: goldRows.length,
        fqoPassOnGold: fqoOnGold.filter((r) => r.finalizedQualified).length,
        candidateCount: candidatePkg?.opportunityCount ?? 0,
        metrics,
        pointersPreserved:
          stagedBefore?.sha256 === stagedAfter?.sha256 &&
          currentBefore?.sha256 === currentAfter?.sha256,
        canonicalDryRunOk: canonicalTrace?.ok !== false,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
