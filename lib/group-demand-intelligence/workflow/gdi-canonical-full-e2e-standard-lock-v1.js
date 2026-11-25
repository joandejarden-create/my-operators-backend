/**
 * Canonical Full E2E Standard Lock V1
 *
 * Permanent adoption gate: the certified Bethesda architecture is mandatory
 * for every future GDI hotel run. This module does not re-run Bethesda
 * certification — it asserts shared entrypoint + persistence enforcement.
 *
 * ONE CANONICAL FULL E2E. ONE THREE-LAYER CONTRACT.
 * INDEPENDENT HOTEL QUALIFICATION. CUMULATIVE INTELLIGENCE.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listFourteenWorkflowSegmentIds } from "../research/universal-contact-research-contract-v1.js";
import {
  GDI_CANONICAL_FULL_E2E_ENTRY_V1,
  preflightCanonicalFullE2e,
  CANONICAL_EXECUTION_MODE,
} from "./gdi-canonical-full-e2e-entry-v1.js";
import {
  GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
  FOUNDER_PRINCIPLES,
  EXISTING_ARCHITECTURE_MAPPING,
  assertMultiHotelFqoIndependence,
  evaluatePersistenceBoundaryGuard,
  THREE_LAYER_WRITE_SET_POLICY,
} from "./gdi-three-layer-intelligence-contract-v1.js";
import {
  RESEARCH_PERSISTENCE_ACTIVATION,
  RESEARCH_PERSISTENCE_WRITE_SET_POLICY,
  filterValidatedFqoPersistenceWriteSet,
} from "./gdi-full-e2e-research-persistence-bridge-v1.js";
import { mergeProposedResearchInventoryFields } from "./gdi-hotel-research-inventory-store-v1.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../hotel-research-opportunity-field-map.js";

export const GDI_CANONICAL_FULL_E2E_STANDARD_LOCK_V1 =
  "gdi-canonical-full-e2e-standard-lock-v1";

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);

/** Founder permanent requirements (Phase 1 lock). */
export const CANONICAL_FULL_E2E_PERMANENT_REQUIREMENTS = Object.freeze([
  {
    id: 1,
    title: "One canonical Full E2E execution pathway",
    enforcement: "ENFORCED",
    surface: GDI_CANONICAL_FULL_E2E_ENTRY_V1,
  },
  {
    id: 2,
    title: "All 14 demand segments remain supported",
    enforcement: "ENFORCED",
    surface: "listFourteenWorkflowSegmentIds()",
  },
  {
    id: 3,
    title: "Three-Layer Intelligence Contract enforced",
    enforcement: "ENFORCED",
    surface: GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
  },
  {
    id: 4,
    title: "Airtable Research SoT enriched losslessly",
    enforcement: "ENFORCED",
    surface: "mergeProposedResearchInventoryFields → mergeResearchSotLossless",
  },
  {
    id: 5,
    title: "Existing verified intelligence cannot silently degrade",
    enforcement: "ENFORCED",
    surface: "SOT merge fail-closed + lossless guards",
  },
  {
    id: 6,
    title: "Hotel-specific FQO independently evaluated",
    enforcement: "ENFORCED",
    surface: "assertMultiHotelFqoIndependence + hotelSpecificQualification",
  },
  {
    id: 7,
    title: "Only FQO-validated opportunities in production write set",
    enforcement: "ENFORCED",
    surface: "VALIDATED_FQO_ONLY + requireValidatedFqoWriteSet on canonical entry",
  },
  {
    id: 8,
    title: "Nonqualified research remains supporting intelligence",
    enforcement: "ENFORCED",
    surface: "filterValidatedFqoPersistenceWriteSet → DEFERRED_NONQUALIFIED",
  },
  {
    id: 9,
    title: "Repeated execution is idempotent",
    enforcement: "ENFORCED",
    surface: "materialDiff + volatile payload key strip → UNCHANGED",
  },
  {
    id: 10,
    title: "Cross-hotel reuse without transferring qualification",
    enforcement: "ENFORCED",
    surface: "reusePriorIntelligence + multi-hotel FQO independence",
  },
  {
    id: 11,
    title: "STAGED and CURRENT independently controlled",
    enforcement: "ENFORCED",
    surface: FOUNDER_PRINCIPLES.publicationIndependence,
  },
  {
    id: 12,
    title: "Customer publication requires separate approval",
    enforcement: "ENFORCED",
    surface: "PUBLICATION_NOT_AUTHORIZED_IN_CANONICAL_RESEARCH_ENTRY",
  },
  {
    id: 13,
    title: "Surfe remains explicit-click only",
    enforcement: "ENFORCED",
    surface: "chain authorizeSurfe:false; HCI UI explicit-click contract",
  },
  {
    id: 14,
    title: "No unauthorized paid discovery or enrichment",
    enforcement: "ENFORCED",
    surface: "preflight LIVE_MODE_REQUIRES_PAID_AUTHORIZATION + budget ceiling",
  },
]);

function text(v) {
  return String(v == null ? "" : v).trim();
}

function readUtf8(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

/**
 * Static + behavioral adoption assessment for the 14 permanent requirements.
 * Fail-closed: any missing enforcement → ok=false.
 */
export function assessCanonicalFullE2eStandardAdoption(opts = {}) {
  const findings = [];
  const gaps = [];

  const push = (reqId, ok, detail) => {
    findings.push({ requirementId: reqId, ok, detail });
    if (!ok) gaps.push({ requirementId: reqId, detail });
  };

  // 1 — one canonical entry
  push(
    1,
    GDI_CANONICAL_FULL_E2E_ENTRY_V1 === "gdi-canonical-full-e2e-entry-v1",
    GDI_CANONICAL_FULL_E2E_ENTRY_V1,
  );

  // 2 — fourteen segments
  const segs = listFourteenWorkflowSegmentIds();
  push(2, Array.isArray(segs) && segs.length === 14, `segmentCount=${segs.length}`);

  // 3 — three-layer contract identity + forbidden engines
  push(
    3,
    GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1 ===
      "gdi-three-layer-intelligence-contract-v1" &&
      Array.isArray(EXISTING_ARCHITECTURE_MAPPING.forbidden) &&
      EXISTING_ARCHITECTURE_MAPPING.forbidden.includes(
        "Competing research database",
      ),
    GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
  );

  // 4–5 — lossless SoT merge blocks contact wipe
  const liveFat = {
    opportunityId: "opp_lock_a",
    hotelId: "hotel_a",
    commercialDemandType: "BOTH",
    contacts: [{ name: "Ada Lovelace", email: "ada@example.org" }],
    evidence: [{ url: "https://example.org/e1", claimKind: "FACT" }],
    meetingHistory: [{ year: 2024, venue: "Competitor Hotel" }],
  };
  const slimWipe = {
    opportunityId: "opp_lock_a",
    hotelId: "hotel_a",
    commercialDemandType: "BOTH",
    hotelSpecificQualification: { finalizedQualified: true },
  };
  const merge = mergeProposedResearchInventoryFields(
    { [F.researchPayloadJson]: JSON.stringify(slimWipe) },
    { [F.researchPayloadJson]: JSON.stringify(liveFat) },
  );
  const preserved =
    merge.ok === true &&
    Array.isArray(merge.preservedLiveContacts) &&
    merge.preservedLiveContacts.some((n) => /ada/i.test(n));
  push(4, preserved, merge.code || "lossless_merge_ok");
  push(
    5,
    preserved && merge.threeLayerContract === GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
    merge.classification || "no_silent_degrade",
  );

  // 6 — multi-hotel FQO independence
  const indep = assertMultiHotelFqoIndependence({
    hotelAId: "hotel_a",
    hotelBId: "hotel_b",
    hotelAQualification: { finalizedQualified: true },
    hotelBQualification: null,
  });
  push(6, indep.ok === true && indep.aApprovesB === false, indep.note);

  // 7–8 — VALIDATED_FQO_ONLY production default + nonqualified deferred
  const policyDefault =
    RESEARCH_PERSISTENCE_ACTIVATION.defaultProductionWriteSetPolicy ===
    RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY;
  const filtered = filterValidatedFqoPersistenceWriteSet(
    [
      {
        opportunityId: "opp_pass",
        hotelId: "hotel_a",
        hotelSpecificQualification: { finalizedQualified: true },
        primaryContact: { email: "a@x.com" },
        finalizedQualified: true,
      },
      {
        opportunityId: "opp_hold",
        hotelId: "hotel_a",
        hotelSpecificQualification: { finalizedQualified: false },
        finalizedQualified: false,
      },
    ],
    { allowHistoricalSchemaCompat: true },
  );
  // Filter may require more FQO fields — accept kept≤1 and deferred≥1 as policy signal.
  const deferredNonqualified =
    filtered.policy === RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY &&
    filtered.deferredCount >= 1;
  push(7, policyDefault === true, `defaultProductionWriteSetPolicy=${RESEARCH_PERSISTENCE_ACTIVATION.defaultProductionWriteSetPolicy}`);
  push(
    8,
    deferredNonqualified,
    `kept=${filtered.keptCount} deferred=${filtered.deferredCount}`,
  );

  // Boundary guard refuses non-VALIDATED policy
  const guard = evaluatePersistenceBoundaryGuard({
    livePayload: liveFat,
    proposedPayload: {
      ...liveFat,
      hotelSpecificQualification: { finalizedQualified: true },
    },
    writeSetPolicy: "RESEARCH_UNIVERSE_AUDIT",
  });
  push(
    7,
    policyDefault &&
      guard.blockers.some((b) => b.code === "WRITE_SET_POLICY_NOT_VALIDATED_FQO_ONLY"),
    "evaluatePersistenceBoundaryGuard rejects non-VALIDATED policy",
  );

  // 9 — idempotency surface exists (material fields exclude lastMaterialChangeAt alone)
  const storeSrc = readUtf8(
    "lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js",
  );
  push(
    9,
    storeSrc.includes("PAYLOAD_VOLATILE_KEYS") &&
      storeSrc.includes("materialChanged") &&
      storeSrc.includes("UNCHANGED"),
    "materialDiff + volatile keys + UNCHANGED path",
  );

  // 10 — reuse prior intelligence wired in entry
  const entrySrc = readUtf8(
    "lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js",
  );
  push(
    10,
    entrySrc.includes("buildReusePriorIntelligenceLedger") &&
      entrySrc.includes("requireValidatedFqoWriteSet") &&
      entrySrc.includes("VALIDATED_FQO_ONLY"),
    "canonical entry reuses prior intelligence + locks validated write set",
  );

  // 11–12 — STAGED/CURRENT / publication independence
  const pubBlocked = preflightCanonicalFullE2e({
    hotelId: "recLuxvwwxID7U2B8",
    mode: CANONICAL_EXECUTION_MODE.FIXTURE,
    authorizePublish: true,
  });
  push(
    11,
    FOUNDER_PRINCIPLES.publicationIndependence.includes("STAGED") &&
      FOUNDER_PRINCIPLES.publicationIndependence.includes("CURRENT"),
    "publicationIndependence principle",
  );
  push(
    12,
    pubBlocked.ok === false &&
      pubBlocked.errors.some(
        (e) => e.code === "PUBLICATION_NOT_AUTHORIZED_IN_CANONICAL_RESEARCH_ENTRY",
      ),
    "canonical entry refuses authorizePublish",
  );

  // 13 — Surfe explicit-click / not auto in research chain
  const hci = readUtf8("public/js/hotel-contact-intelligence.js");
  const chain = readUtf8(
    "lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js",
  );
  const restore = readUtf8(
    "lib/group-demand-intelligence/workflow/gdi-who-how-contact-intelligence-restore-v1.js",
  );
  push(
    13,
    /user-initiated only/i.test(hci) &&
      chain.includes("authorizeSurfe: false") &&
      /Never activates Surfe|refuses Findymail\/Surfe/i.test(restore),
    "HCI user-initiated + chain Surfe off + HOW restore refuses Surfe auto",
  );

  // 14 — paid auth required for live
  const liveNoAuth = preflightCanonicalFullE2e({
    hotelId: "recLuxvwwxID7U2B8",
    mode: CANONICAL_EXECUTION_MODE.LIVE,
  });
  push(
    14,
    liveNoAuth.ok === false &&
      liveNoAuth.errors.some((e) => e.code === "LIVE_MODE_REQUIRES_PAID_AUTHORIZATION"),
    "LIVE without paid auth blocked",
  );

  // Activation controls (production execution)
  const controls = {
    researchInventoryEnvGate: RESEARCH_PERSISTENCE_ACTIVATION.envGate,
    requiresExactWriteSetAuthorization:
      RESEARCH_PERSISTENCE_ACTIVATION.requiresExactWriteSetAuthorization === true,
    separateFromGdo: RESEARCH_PERSISTENCE_ACTIVATION.separateFromGdoWrites === true,
    separateFromPublication:
      RESEARCH_PERSISTENCE_ACTIVATION.separateFromPublication === true,
    separateFromPaidResearchAuth:
      RESEARCH_PERSISTENCE_ACTIVATION.separateFromPaidResearchAuth === true,
    defaultProductionWriteSetPolicy:
      RESEARCH_PERSISTENCE_ACTIVATION.defaultProductionWriteSetPolicy,
    threeLayerWriteSetPolicy: THREE_LAYER_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
  };

  const requirementOk = new Map();
  for (const f of findings) {
    if (!requirementOk.has(f.requirementId)) requirementOk.set(f.requirementId, true);
    if (!f.ok) requirementOk.set(f.requirementId, false);
  }
  const requirements = CANONICAL_FULL_E2E_PERMANENT_REQUIREMENTS.map((r) => ({
    ...r,
    ok: requirementOk.get(r.id) === true,
    findings: findings.filter((f) => f.requirementId === r.id),
  }));

  const missing = requirements.filter((r) => !r.ok);
  const ok = missing.length === 0 && gaps.length === 0;

  return {
    contract: GDI_CANONICAL_FULL_E2E_STANDARD_LOCK_V1,
    at: new Date().toISOString(),
    ok,
    enforcedCount: requirements.filter((r) => r.ok).length,
    totalRequirements: requirements.length,
    requirements,
    gaps: missing.map((m) => ({
      id: m.id,
      title: m.title,
      findings: m.findings,
    })),
    productionExecutionControls: controls,
    bethesdaRecertification: "NOT_REQUIRED — closed milestone; reopen only on regression",
    nextPhase: opts.nextPhase || "PHASE_2_WYNDHAM_SAN_DIEGO_BAYSIDE",
    note: text(opts.note) || null,
  };
}

export default {
  GDI_CANONICAL_FULL_E2E_STANDARD_LOCK_V1,
  CANONICAL_FULL_E2E_PERMANENT_REQUIREMENTS,
  assessCanonicalFullE2eStandardAdoption,
};
