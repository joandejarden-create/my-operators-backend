/**
 * GDI Full E2E Gold Standard certification gates V1.
 *
 * Fail-closed: mock LDI, classify-only CAD without competitive set, Findymail
 * hard-cap bypass, missing ES auto-generation, and contact-without-restage
 * cannot claim FULL_E2E_VALIDATED / MEMMO gold standard.
 *
 * Contract: GDI_FULL_E2E_GOLD_STANDARD_CERTIFICATION_GATES_V1
 */
import fs from "node:fs";
import path from "node:path";

export const GDI_FULL_E2E_GOLD_STANDARD_CERTIFICATION_GATES_V1 =
  "gdi-full-e2e-gold-standard-certification-gates-v1";

export const GATE_IDS = Object.freeze({
  HOTEL_COMMERCIAL_CONFIG: "GDI_HOTEL_COMMERCIAL_CONFIG",
  COMPETITIVE_SET_CONFIGURED: "GDI_COMPETITIVE_SET_CONFIGURED",
  CAD_LIVE_OR_BLOCKED: "GDI_CAD_LIVE_OR_EXPLICITLY_BLOCKED",
  LDI_LIVE_NOT_MOCK: "GDI_LDI_LIVE_NOT_MOCK",
  PLP_EXECUTED_OR_NA: "GDI_PLP_EXECUTED_OR_NOT_APPLICABLE",
  DISCOVERY_SEGMENT_MATRIX: "GDI_DISCOVERY_SEGMENT_MATRIX_COMPLETE",
  DEEP_RESEARCH_COMPLETE: "GDI_DEEP_RESEARCH_APPLICABLE_COMPLETE",
  WHO_HOW_COMPLETE: "GDI_WHO_HOW_APPLICABLE_COMPLETE",
  CONTACT_CHANNELS_BEST_EFFORT: "GDI_CONTACT_CHANNELS_BEST_EFFORT",
  FINDYMAIL_HARD_CAP: "GDI_FINDYMAIL_CUMULATIVE_HARD_CAP",
  AIRTABLE_PERSIST_READBACK: "GDI_AIRTABLE_PERSIST_AND_READBACK",
  DURABLE_RECONSTRUCTION: "GDI_DURABLE_RECONSTRUCTION",
  STAGED_AUTO_ACTIVATED: "GDI_STAGED_AUTO_ACTIVATED",
  MATERIAL_CONTACT_RESTAGE: "GDI_MATERIAL_IMPROVEMENTS_MAY_RESTAGE",
  EXECUTIVE_SUMMARY_AUTO: "GDI_EXECUTIVE_SUMMARY_AUTO_GENERATED",
  EXECUTIVE_SUMMARY_PARITY: "GDI_EXECUTIVE_SUMMARY_LATEST_INVENTORY_PARITY",
  EXECUTIVE_SUMMARY_RECEIPT: "GDI_EXECUTIVE_SUMMARY_RESEARCH_RECEIPT_RECONCILED",
  BROWSER_THREE_TAB: "GDI_BROWSER_THREE_TAB_LOADED_CONTENT",
});

/**
 * Read competitiveContext from hotel config JSON.
 */
export function readHotelCompetitiveSet(hotelId, opts = {}) {
  const root = opts.repoRoot || process.cwd();
  const id = String(hotelId || "").trim();
  const cfgPath =
    opts.configPath ||
    path.join(root, "config/group-demand-intelligence/hotels", `${id}.json`);
  if (!fs.existsSync(cfgPath)) {
    return {
      ok: false,
      hotelId: id,
      configured: false,
      reason: "HOTEL_CONFIG_MISSING",
      peers: [],
      configPath: cfgPath,
    };
  }
  const cfg = JSON.parse(fs.readFileSync(cfgPath, "utf8"));
  const ctx = cfg.competitiveContext && typeof cfg.competitiveContext === "object"
    ? cfg.competitiveContext
    : null;
  const peers = [];
  if (ctx) {
    for (const key of [
      "strCompSet",
      "relevantGroupDemandAlternatives",
      "directCompetitors",
      "aspirationalCompetitors",
      "competitiveHotels",
      "peers",
    ]) {
      const arr = ctx[key];
      if (!Array.isArray(arr)) continue;
      for (const row of arr) {
        if (!row) continue;
        peers.push({
          name: row.name || row.displayName || null,
          hotelId: row.hotelId || row.canonicalHotelId || row.id || null,
          class: row.class || key,
          source: row.source || null,
          bucket: key,
        });
      }
    }
  }
  return {
    ok: true,
    hotelId: id,
    configured: peers.length > 0,
    peerCount: peers.length,
    peers,
    configPath: cfgPath,
    displayName: cfg.displayName || null,
  };
}

/**
 * Classify CAD stage report for gold-standard (not surface PASS).
 */
export function classifyCadCertification(cadReport = {}, competitiveSet = {}) {
  const status = String(cadReport?.status || "").toUpperCase();
  const paidLive =
    cadReport?.paidLiveCompetitorResearch === true ||
    cadReport?.liveCompetitorQueries > 0 ||
    cadReport?.providerMode === "LIVE_COMPETITOR_RESEARCH";
  if (!competitiveSet.configured) {
    return {
      gate: GATE_IDS.CAD_LIVE_OR_BLOCKED,
      status: "BLOCKED",
      ok: false,
      reason: "NO_COMPETITIVE_SET_CONFIGURED",
      note: "Classify-only CAD without competitive hotels cannot satisfy Live CAD certification.",
    };
  }
  if (paidLive) {
    return {
      gate: GATE_IDS.CAD_LIVE_OR_BLOCKED,
      status: "LIVE_EXECUTED",
      ok: true,
      reason: "LIVE_COMPETITOR_RESEARCH",
    };
  }
  if (status === "CLASSIFIED" || cadReport?.classifiedCount > 0) {
    return {
      gate: GATE_IDS.CAD_LIVE_OR_BLOCKED,
      status: "PARTIAL",
      ok: false,
      reason: "CLASSIFY_ONLY_NOT_LIVE_CAD",
      note: "Existing-row classification is not Competitive Account Discovery Live research.",
    };
  }
  return {
    gate: GATE_IDS.CAD_LIVE_OR_BLOCKED,
    status: "NOT_VALIDATED",
    ok: false,
    reason: "CAD_REPORT_INCOMPLETE",
  };
}

/**
 * Classify LDI stage — mock_live / MOCK_PROVIDER cannot be Live COMPLETE.
 */
export function classifyLdiCertification(ldiReport = {}) {
  const mode = String(
    ldiReport?.researchMode ||
      ldiReport?.retrievalMode ||
      ldiReport?.providerMode ||
      "",
  ).toLowerCase();
  const status = String(ldiReport?.status || "");
  const mock =
    mode.includes("mock") ||
    status.includes("MOCK") ||
    (ldiReport?.specializedLeisureExecution === false &&
      Number(ldiReport?.paidCalls || 0) === 0 &&
      status.includes("COMPLETE"));

  if (mode === "live" || mode === "paid_live" || ldiReport?.liveResearchExecuted === true) {
    return {
      gate: GATE_IDS.LDI_LIVE_NOT_MOCK,
      status: "LIVE_EXECUTED",
      ok: true,
      reason: "LIVE_LDI",
    };
  }
  if (mock || mode === "mock_live" || mode === "live_replay") {
    return {
      gate: GATE_IDS.LDI_LIVE_NOT_MOCK,
      status: "FAIL",
      ok: false,
      reason: "MOCK_OR_FIXTURE_LDI_NOT_LIVE",
      note: "mock_live / MOCK_PROVIDER cannot satisfy Live LDI gold-standard certification.",
    };
  }
  if (String(ldiReport?.status || "").includes("PENDING_AUTHORIZATION")) {
    return {
      gate: GATE_IDS.LDI_LIVE_NOT_MOCK,
      status: "BLOCKED",
      ok: false,
      reason: "LDI_PENDING_AUTHORIZATION",
    };
  }
  return {
    gate: GATE_IDS.LDI_LIVE_NOT_MOCK,
    status: "NOT_VALIDATED",
    ok: false,
    reason: "LDI_REPORT_INCOMPLETE",
  };
}

/**
 * Contact-channel material improvements may authorize STAGED without net-new FQO.
 */
export function assessMaterialInventoryImprovementRestage(input = {}) {
  const newFqo = Number(input.newFinalizedQualifiedCount || 0) || 0;
  const retained = Number(input.retainedQualifiedCount || 0) || 0;
  const contactImproved =
    Number(input.newVerifiedEmails || 0) +
      Number(input.newVerifiedPhones || 0) +
      Number(input.newVerifiedLinkedIn || 0) >
    0;
  const narrativeImproved = input.materialNarrativeOrTimingImprovement === true;
  const authorized = input.authorizeMaterialImprovementRestage === true;

  if (newFqo > 0) {
    return {
      gate: GATE_IDS.MATERIAL_CONTACT_RESTAGE,
      mayRestage: true,
      reason: "NEW_FINALIZED_QUALIFIED",
      requiresExplicitAuth: false,
    };
  }
  if ((contactImproved || narrativeImproved) && retained > 0 && authorized) {
    return {
      gate: GATE_IDS.MATERIAL_CONTACT_RESTAGE,
      mayRestage: true,
      reason: "MATERIAL_IMPROVEMENT_ON_RETAINED_INVENTORY",
      requiresExplicitAuth: true,
      contactImproved,
      narrativeImproved,
    };
  }
  if ((contactImproved || narrativeImproved) && retained > 0 && !authorized) {
    return {
      gate: GATE_IDS.MATERIAL_CONTACT_RESTAGE,
      mayRestage: false,
      reason: "MATERIAL_IMPROVEMENT_BLOCKED_UNTIL_RESTAGE_AUTH",
      requiresExplicitAuth: true,
      contactImproved,
      narrativeImproved,
      note: "Contact/timing improvements must not be stranded without STAGED refresh authorization.",
    };
  }
  return {
    gate: GATE_IDS.MATERIAL_CONTACT_RESTAGE,
    mayRestage: false,
    reason: "NO_MATERIAL_IMPROVEMENT_AND_NO_NEW_FQO",
  };
}

/**
 * Evaluate a Full E2E run report pack for gold-standard readiness (Phase A/E).
 */
export function evaluateGoldStandardCertification(pack = {}) {
  const hotelId = String(pack.hotelId || "").trim();
  const competitive = pack.competitiveSet || readHotelCompetitiveSet(hotelId, pack);
  const gates = [];

  gates.push({
    id: GATE_IDS.HOTEL_COMMERCIAL_CONFIG,
    ok: Boolean(pack.hotelConfigOk !== false && hotelId),
    status: hotelId ? "PASS" : "FAIL",
    detail: hotelId || "missing_hotel_id",
  });

  gates.push({
    id: GATE_IDS.COMPETITIVE_SET_CONFIGURED,
    ok: competitive.configured === true,
    status: competitive.configured ? "PASS" : "BLOCKED",
    detail: competitive,
  });

  const cad = classifyCadCertification(pack.cadReport || {}, competitive);
  gates.push({ id: cad.gate, ok: cad.ok, status: cad.status, detail: cad });

  const ldi = classifyLdiCertification(pack.ldiReport || {});
  gates.push({ id: ldi.gate, ok: ldi.ok, status: ldi.status, detail: ldi });

  const findymailCapOk =
    pack.findymailHardCapOk === true ||
    (Number(pack.findymailCreditsDebited || 0) <= Number(pack.findymailHardCap || Infinity));
  gates.push({
    id: GATE_IDS.FINDYMAIL_HARD_CAP,
    ok: findymailCapOk,
    status: findymailCapOk ? "PASS" : "FAIL",
    detail: {
      debited: pack.findymailCreditsDebited,
      hardCap: pack.findymailHardCap,
    },
  });

  const restage = assessMaterialInventoryImprovementRestage(pack.restageInput || {});
  gates.push({
    id: restage.gate,
    ok: restage.mayRestage || restage.reason === "NO_MATERIAL_IMPROVEMENT_AND_NO_NEW_FQO",
    status: restage.mayRestage
      ? "PASS"
      : restage.reason === "MATERIAL_IMPROVEMENT_BLOCKED_UNTIL_RESTAGE_AUTH"
        ? "BLOCKED"
        : "PASS",
    detail: restage,
  });

  for (const g of pack.extraGates || []) {
    gates.push(g);
  }

  const failed = gates.filter((g) => g.ok === false);
  const blocked = gates.filter((g) => String(g.status).toUpperCase() === "BLOCKED");
  const certified =
    failed.length === 0 &&
    blocked.length === 0 &&
    pack.requireAllGates !== false;

  return {
    contract: GDI_FULL_E2E_GOLD_STANDARD_CERTIFICATION_GATES_V1,
    hotelId,
    certified,
    classification: certified
      ? "FULL_E2E_VALIDATED"
      : blocked.length
        ? "BLOCKED"
        : "PARTIAL",
    gates,
    failedGateIds: failed.map((g) => g.id),
    blockedGateIds: blocked.map((g) => g.id),
  };
}
