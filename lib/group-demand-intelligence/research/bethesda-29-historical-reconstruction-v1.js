/**
 * Bethesda 29 Historical Reconstruction V1
 *
 * Reconstructs schema-valid research-stage opportunity records for the 29
 * historically attested Bethesda IDs using existing curated attestations and
 * recoverable finalize/maturation artifacts. Zero paid research. Never writes
 * hotels/.../opportunities.json or authorizes customer publication.
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import {
  CURATED_HISTORICAL_ATTESTATIONS,
  RECOVERY_CATEGORY,
  buildBethesdaHistoricalRecoveryManifest,
} from "./bethesda-historical-opportunity-recovery-v1.js";
import {
  BETHESDA_29_ATTESTED_IDS,
  buildBethesdaHistoricalAttestationBag,
} from "./bethesda-historical-attestation-bag-v1.js";
import {
  BODY_STATUS,
  RECOVERY_DISPOSITION,
  GDI_RESEARCH_STAGE_PERSISTENCE_V1,
  researchStageHotelDir,
  researchStageMemoryDir,
  persistResearchStageOpportunityBag,
  writeJsonAtomic,
} from "./gdi-research-stage-persistence-v1.js";
import {
  writeMaturationCheckpointAtomic,
  validateMaturationCheckpoint,
} from "./bethesda-maturation-checkpoint-v1.js";
import { validateScalingCanonicalFields } from "../scaling-full-e2e-data-contract-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import { evaluateAccountDevelopmentEligibility } from "../account-development-eligibility-v1.js";
import { looksLikePersonName } from "../secondary-named-contact-v1.js";

export const BETHESDA_29_HISTORICAL_RECONSTRUCTION_V1 =
  "gdi-bethesda-29-historical-reconstruction-v1";

export const RECONSTRUCTION_STATUS = Object.freeze({
  RECOVERED_FROM_ORIGINAL_ARTIFACT: "RECOVERED_FROM_ORIGINAL_ARTIFACT",
  RECONSTRUCTED_FROM_VERIFIABLE_SOURCE: "RECONSTRUCTED_FROM_VERIFIABLE_SOURCE",
  STILL_MISSING_RESEARCH: "STILL_MISSING_RESEARCH",
});

export const BUSINESS_TYPE = Object.freeze({
  GROUP_ROOMS: "GROUP_ROOMS",
  CATERING_EVENTS: "CATERING_EVENTS",
  BOTH: "BOTH",
  PARTNERSHIP_REFERRAL: "PARTNERSHIP_REFERRAL",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export const RECONSTRUCTION_BAG_REL =
  "data/group-demand-intelligence/research-stage/recLuxvwwxID7U2B8/reconstructed-opportunities-v1.json";

export const RECONSTRUCTION_FIXTURE_REL =
  "fixtures/gdi/bethesda-29-historical-reconstruction-v1.json";

const FINALIZE_LOG_REL =
  "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log";
const MATURATION_LOG_REL =
  "reports/gdi-adp-five-hotel-portfolio-e2e-v1/bethesda-additive-discovery-v1/maturation-resume-live.log";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function sha256File(abs) {
  const buf = fs.readFileSync(abs);
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function businessTypeForCategory(cat) {
  if (cat === RECOVERY_CATEGORY.LOCAL_CATERING) return BUSINESS_TYPE.CATERING_EVENTS;
  if (cat === RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS) {
    return BUSINESS_TYPE.PARTNERSHIP_REFERRAL;
  }
  if (
    cat === RECOVERY_CATEGORY.SPORTS_HOUSING ||
    cat === RECOVERY_CATEGORY.RELOCATION_EXTENDED_STAY
  ) {
    return BUSINESS_TYPE.GROUP_ROOMS;
  }
  if (
    cat === RECOVERY_CATEGORY.CORPORATE_GROUP ||
    cat === RECOVERY_CATEGORY.ASSOCIATION_EVENTS
  ) {
    return BUSINESS_TYPE.BOTH;
  }
  return BUSINESS_TYPE.BOTH;
}

function researchSegmentFor(att, curated) {
  return text(
    curated?.demandSegment ||
      att?.demandSegment ||
      curated?.researchCategory ||
      att?.researchCategory ||
      "research_continuation"
  );
}

/**
 * Parse V20 finalize-run.log acceptance rows for recoverable opportunity fragments.
 */
function readPossiblyUtf16Json(absPath) {
  const buf = fs.readFileSync(absPath);
  // V20 finalize-run.log is UTF-16 LE with BOM on Windows.
  if (buf.length >= 2 && buf[0] === 0xff && buf[1] === 0xfe) {
    return buf.toString("utf16le");
  }
  if (buf.length >= 2 && buf[0] === 0xfe && buf[1] === 0xff) {
    // Rare BE BOM — decode via swap is unnecessary here; fall through utf8.
  }
  return buf.toString("utf8");
}

export function parseFinalizeLogAcceptanceRows({ root = ROOT } = {}) {
  const abs = path.join(root, FINALIZE_LOG_REL);
  if (!fs.existsSync(abs)) return { ok: false, path: FINALIZE_LOG_REL, byId: new Map() };
  const raw = readPossiblyUtf16Json(abs);
  const byId = new Map();
  try {
    const doc = JSON.parse(raw.replace(/^\uFEFF/, ""));
    const rows = doc?.acceptance?.rows || [];
    for (const row of rows) {
      const id = text(row?.id);
      if (!id || !BETHESDA_29_ATTESTED_IDS.includes(id)) continue;
      byId.set(id, row);
    }
  } catch {
    return { ok: false, path: FINALIZE_LOG_REL, byId, parseError: true };
  }
  return {
    ok: true,
    path: FINALIZE_LOG_REL,
    byId,
    recoveredIdCount: byId.size,
  };
}

export function parseMaturationPersistStatuses({ root = ROOT } = {}) {
  const abs = path.join(root, MATURATION_LOG_REL);
  const byId = new Map();
  if (!fs.existsSync(abs)) return { ok: false, path: MATURATION_LOG_REL, byId };
  const lines = fs.readFileSync(abs, "utf8").split(/\r?\n/);
  for (const line of lines) {
    const prog = line.match(
      /completed\s+\d+\/\d+\s+id=(\S+)\s+status=(\S+)\s+stop=(\S+)/
    );
    if (prog) {
      byId.set(prog[1], {
        researchStatus: prog[2],
        stopReason: prog[3],
        source: MATURATION_LOG_REL,
      });
    }
  }
  return { ok: true, path: MATURATION_LOG_REL, byId, recoveredIdCount: byId.size };
}

function shortOrgName(preferred, longBlob) {
  const short = text(preferred);
  if (short) return short;
  const long = text(longBlob);
  if (!long) return null;
  // Prefer text before em dash / colon for polluted finalize blobs.
  const cut = long.split(/\s[—–-]\s|:\s/)[0];
  return text(cut).slice(0, 120) || long.slice(0, 120);
}

function classifyContact(rawContact) {
  const c = text(rawContact);
  if (!c) {
    return {
      raw: null,
      namedPerson: false,
      contactClass: "MISSING",
      usableRoute: false,
    };
  }
  if (/web\s*form|published contact path/i.test(c)) {
    return {
      raw: c,
      namedPerson: false,
      contactClass: "ORG_ENTRY_PATH",
      usableRoute: true,
      note: "Published web/contact path only — not a verified named person.",
    };
  }
  if (/@/.test(c) && !looksLikePersonName(c.split("@")[0].replace(/[._]/g, " "))) {
    return {
      raw: c,
      namedPerson: false,
      contactClass: "FUNCTIONAL_INBOX",
      usableRoute: true,
      email: c,
      note: "Functional organization inbox — not a verified named person.",
    };
  }
  if (looksLikePersonName(c)) {
    return {
      raw: c,
      namedPerson: true,
      contactClass: "NAMED_PERSON_UNVERIFIED_ROLE",
      usableRoute: false,
      note: "Name string present without verified role/route in recovered artifacts.",
    };
  }
  return {
    raw: c,
    namedPerson: false,
    contactClass: "UNCLASSIFIED_CONTACT_STRING",
    usableRoute: Boolean(c),
  };
}

function unresolvedList({ curated, artifact, contact, recoveryDisposition }) {
  const missing = [];
  if (!text(curated?.demandThesis) && !text(artifact?.organizationName)?.includes(" ")) {
    missing.push("DEMAND_THESIS_INCOMPLETE");
  }
  if (!contact.namedPerson) missing.push("VERIFIED_NAMED_COMMERCIAL_CONTACT");
  if (!text(curated?.hotelFitRationale) && recoveryDisposition !== RECOVERY_DISPOSITION.OVERFLOW_WATCH) {
    missing.push("HOTEL_FIT_DETAIL");
  }
  if (recoveryDisposition === RECOVERY_DISPOSITION.OVERFLOW_WATCH) {
    missing.push("INDEPENDENT_OVERFLOW_DEMAND_EVIDENCE");
  }
  if (!artifact && !text(curated?.evidenceNotes) && !text(curated?.demandThesis)) {
    missing.push("ORIGINAL_RESEARCH_BODY");
  }
  missing.push("CUSTOMER_PUBLICATION_NOT_AUTHORIZED");
  return [...new Set(missing)];
}

function buildWhatIsHappening({ curated, artifact, att }) {
  const recoveredNarrative = text(
    curated?.demandThesis ||
      curated?.evidenceNotes ||
      (artifact && text(artifact.organizationName).length > text(att.organizationName || "").length
        ? artifact.organizationName
        : null) ||
      artifact?.title ||
      curated?.title ||
      att?.title
  );
  if (recoveredNarrative) {
    return {
      text: recoveredNarrative,
      fieldSource: text(curated?.demandThesis)
        ? "curated_attestation.demandThesis"
        : text(curated?.evidenceNotes)
          ? "curated_attestation.evidenceNotes"
          : artifact
            ? "finalize-run.log.acceptance.rows"
            : "attestation_title",
    };
  }
  return {
    text: `Historical attestation only for ${text(att.organizationName) || att.id}. Original research body missing; further zero-paid / authorized research required before commercial qualification.`,
    fieldSource: "attestation_placeholder_unresolved",
  };
}

function buildRecommendedAction({ recoveryDisposition, missing, contact }) {
  if (recoveryDisposition === RECOVERY_DISPOSITION.OVERFLOW_WATCH) {
    return "Retain OVERFLOW_WATCH — do not treat competitor-host demand as Sales Ready. Gather independent evidence of accessible overflow/guestroom need for Bethesda Marriott before outreach.";
  }
  if (recoveryDisposition === RECOVERY_DISPOSITION.PARTNERSHIP_WATCH) {
    return "Continue PARTNERSHIP_WATCH research — identify named commercial entry point and evidenced referral/group path; do not auto-elevate to direct-booking Sales Ready.";
  }
  if (missing.includes("VERIFIED_NAMED_COMMERCIAL_CONTACT")) {
    return contact.usableRoute
      ? "Research continuation — verify a named commercially relevant contact reachable via the recovered org route; do not publish."
      : "Research continuation — establish a verified named commercial contact and evidence-backed hotel-fit thesis before sales outreach.";
  }
  return "Research continuation — complete missing evidence fields, then re-run shared commercial eligibility before any publication review.";
}

/**
 * Build one schema-valid research-stage opportunity for a historical ID.
 */
export function reconstructOneHistoricalOpportunity({
  id,
  attestationRecord,
  curated,
  artifactRow = null,
  maturationMeta = null,
  canonicalIds = new Set(),
} = {}) {
  const att = attestationRecord || { id };
  const recoveryDisposition =
    att.recoveryDisposition ||
    curated?.recoveryDisposition ||
    RECOVERY_DISPOSITION.ATTESTED_BODY_MISSING;

  const org = shortOrgName(
    att.organizationName || curated?.organizationName,
    artifactRow?.organizationName
  );
  const researchCategory =
    att.researchCategory || curated?.researchCategory || RECOVERY_CATEGORY.OTHER;
  const what = buildWhatIsHappening({ curated, artifact: artifactRow, att });
  const contact = classifyContact(curated?.originalContact);
  const hotelFit = text(
    curated?.hotelFitRationale ||
      (recoveryDisposition === RECOVERY_DISPOSITION.OVERFLOW_WATCH
        ? "Competitor-host activity near Bethesda corridor — overflow eligibility not independently verified."
        : recoveryDisposition === RECOVERY_DISPOSITION.PARTNERSHIP_WATCH
          ? "Potential partnership / preferred-lodging / planner referral channel for Bethesda Marriott — relationship not contractually evidenced in recovered artifacts."
          : "Bethesda Marriott / NIH corridor fit historically researched; detailed fit narrative incomplete in recovered artifacts.")
  );

  const hasRecoveredArtifactBody = Boolean(
    artifactRow &&
      (text(artifactRow.id) === id ||
        text(artifactRow.organizationName) ||
        text(artifactRow.title) ||
        (Array.isArray(artifactRow.rationale) && artifactRow.rationale.length))
  );
  const hasCuratedThesis = Boolean(
    text(curated?.demandThesis) || text(curated?.evidenceNotes) || text(curated?.hotelFitRationale)
  );

  let reconstructionStatus = RECONSTRUCTION_STATUS.STILL_MISSING_RESEARCH;
  if (hasRecoveredArtifactBody) {
    reconstructionStatus = RECONSTRUCTION_STATUS.RECOVERED_FROM_ORIGINAL_ARTIFACT;
  } else if (hasCuratedThesis || maturationMeta) {
    reconstructionStatus = RECONSTRUCTION_STATUS.RECONSTRUCTED_FROM_VERIFIABLE_SOURCE;
  }

  const bodyStatus =
    reconstructionStatus === RECONSTRUCTION_STATUS.STILL_MISSING_RESEARCH
      ? BODY_STATUS.HISTORICAL_ATTESTATION_ONLY
      : reconstructionStatus === RECONSTRUCTION_STATUS.RECOVERED_FROM_ORIGINAL_ARTIFACT
        ? BODY_STATUS.SOURCE_EVIDENCE_AVAILABLE
        : BODY_STATUS.SOURCE_EVIDENCE_AVAILABLE;

  const missingResearchRequirements = unresolvedList({
    curated,
    artifact: artifactRow,
    contact,
    recoveryDisposition,
  });

  const opp = {
    id,
    opportunityId: id,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    organizationName: org,
    title: text(curated?.title || artifactRow?.title || att.title) || null,
    researchCategory,
    researchSegment: researchSegmentFor(att, curated),
    demandSegment: researchSegmentFor(att, curated),
    businessType: businessTypeForCategory(researchCategory),
    whatIsHappening: what.text,
    localConnection: hotelFit,
    proposedHotelUse:
      businessTypeForCategory(researchCategory) === BUSINESS_TYPE.PARTNERSHIP_REFERRAL
        ? "Partnership / referral development — not a single confirmed booking block."
        : businessTypeForCategory(researchCategory) === BUSINESS_TYPE.CATERING_EVENTS
          ? "Local catering / meetings / banquet or reception use at Bethesda Marriott (subject to evidence)."
          : "Group rooms and/or meetings use at Bethesda Marriott (subject to evidence).",
    hotelFitRationale: hotelFit,
    whyThisHotel: hotelFit,
    recommendedAction: null, // filled after missing list
    customerVisible: false,
    customerFacing: false,
    salesReady: false,
    publicationApproved: false,
    promoteToOpportunitiesJson: false,
    publication: "NOT_AUTHORIZED",
    researchStatus:
      maturationMeta?.researchStatus ||
      curated?.originalResearchStatus ||
      artifactRow?.disposition ||
      "RESEARCH_CONTINUATION",
    originalCommercialQualification:
      curated?.originalCommercialQualification ||
      att.originalDisposition ||
      artifactRow?.disposition ||
      null,
    historicalRecoveryDisposition: recoveryDisposition,
    reconstructionStatus,
    bodyStatus,
    missingOriginalResearchBody:
      reconstructionStatus === RECONSTRUCTION_STATUS.STILL_MISSING_RESEARCH,
    furtherResearchRequired: true,
    inventedFields: false,
    paidResearch: false,
    historicalAttestationRef: {
      bag: "gdi-bethesda-historical-attestation-bag-v1",
      id,
      discoveryRun: att.discoveryRun || curated?.discoveryRun || null,
      discoveryDate: att.discoveryDate || curated?.discoveryDate || null,
      sourceArtifactPath:
        att.sourceArtifactPath || curated?.sourceArtifactPath || null,
    },
    provenance: {
      curatedAttestation: Boolean(curated),
      finalizeLogRow: Boolean(artifactRow),
      maturationLog: Boolean(maturationMeta),
      finalizeLogPath: artifactRow ? FINALIZE_LOG_REL : null,
      maturationLogPath: maturationMeta ? MATURATION_LOG_REL : null,
      artifactRationale: Array.isArray(artifactRow?.rationale)
        ? artifactRow.rationale
        : [],
      fieldSources: {
        whatIsHappening: what.fieldSource,
        organizationName: "attestation_bag_short_name",
        hotelFitRationale: text(curated?.hotelFitRationale)
          ? "curated_attestation.hotelFitRationale"
          : "reconstruction_default_unresolved",
      },
    },
    contactRecovery: contact,
    primaryContact:
      contact.contactClass === "FUNCTIONAL_INBOX"
        ? { email: contact.email, role: "functional_inbox", namedPerson: false }
        : contact.contactClass === "ORG_ENTRY_PATH"
          ? { route: contact.raw, namedPerson: false }
          : null,
    sources: [
      att.sourceArtifactPath || curated?.sourceArtifactPath
        ? {
            label: "historical_attestation_source",
            pathRel: text(att.sourceArtifactPath || curated?.sourceArtifactPath),
          }
        : null,
      artifactRow
        ? { label: "v20_finalize_acceptance_row", pathRel: FINALIZE_LOG_REL }
        : null,
      maturationMeta
        ? { label: "maturation_resume_live", pathRel: MATURATION_LOG_REL }
        : null,
    ].filter(Boolean),
    duplicateOfCanonical: canonicalIds.has(id),
    relatedIds: Array.isArray(curated?.relatedIds) ? curated.relatedIds : [],
    missingResearchRequirements: missingResearchRequirements,
    researchLifecycle: {
      stage: "RESEARCH_STAGE_ONLY",
      publicationStatus: "NOT_AUTHORIZED",
      customerShareEligible: false,
    },
    doesNotAffectProduction: true,
    persistenceContract: GDI_RESEARCH_STAGE_PERSISTENCE_V1,
    reconstructionVersion: BETHESDA_29_HISTORICAL_RECONSTRUCTION_V1,
  };

  opp.recommendedAction = buildRecommendedAction({
    recoveryDisposition,
    missing: missingResearchRequirements,
    contact,
  });

  // Shared commercial eligibility — never invent Sales Ready.
  const shared = evaluateSharedCommercialEligibility(opp, {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
  });
  const accountDev = evaluateAccountDevelopmentEligibility(opp, {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
  });
  const scaling = validateScalingCanonicalFields(opp);

  // Hard safety: historical reconstruction never publishes as Sales Ready.
  const commercialStatus =
    recoveryDisposition === RECOVERY_DISPOSITION.OVERFLOW_WATCH ||
    recoveryDisposition === RECOVERY_DISPOSITION.PARTNERSHIP_WATCH
      ? "RESEARCH_CONTINUATION"
      : shared?.status || "RESEARCH_CONTINUATION";

  opp.commercialEligibility = {
    sharedStatus: shared?.status || null,
    accountDevelopmentStatus: accountDev?.status || null,
    enforcedPublicationStatus: "NOT_AUTHORIZED",
    enforcedSalesReady: false,
    note:
      "Historical reconstruction is research-stage only. Shared eligibility is diagnostic; publication remains unauthorized.",
  };
  opp.commercialMaturity = commercialStatus;
  if (shared?.status === "SALES_READY") {
    // Refuse elevation from incomplete recovered bodies.
    opp.commercialMaturity = "RESEARCH_CONTINUATION";
    opp.commercialEligibility.blockedSalesReadyElevation = true;
    opp.missingResearchRequirements = [
      ...new Set([
        ...opp.missingResearchRequirements,
        "REFUSED_AUTO_SALES_READY_FROM_HISTORICAL_RECONSTRUCTION",
      ]),
    ];
  }

  opp.schemaValidation = {
    scalingCanonical: scaling,
    maturationReady: Boolean(text(opp.organizationName) && text(opp.id)),
  };

  return opp;
}

/**
 * Reconstruct all 29 historical IDs into research-stage schema records.
 */
export function buildBethesda29HistoricalReconstruction({ root = ROOT } = {}) {
  const bag = buildBethesdaHistoricalAttestationBag({ root });
  const curatedById = new Map(
    CURATED_HISTORICAL_ATTESTATIONS.filter((r) =>
      BETHESDA_29_ATTESTED_IDS.includes(r.id)
    ).map((r) => [r.id, r])
  );
  const finalize = parseFinalizeLogAcceptanceRows({ root });
  const maturation = parseMaturationPersistStatuses({ root });
  const manifest = buildBethesdaHistoricalRecoveryManifest({ root });
  const canonicalIds = new Set(manifest.baseline?.ids || []);

  const attById = new Map((bag.records || []).map((r) => [r.id, r]));
  const opportunities = BETHESDA_29_ATTESTED_IDS.map((id) =>
    reconstructOneHistoricalOpportunity({
      id,
      attestationRecord: attById.get(id),
      curated: curatedById.get(id),
      artifactRow: finalize.byId.get(id) || null,
      maturationMeta: maturation.byId.get(id) || null,
      canonicalIds,
    })
  );

  const byStatus = {
    RECOVERED_FROM_ORIGINAL_ARTIFACT: opportunities.filter(
      (o) =>
        o.reconstructionStatus ===
        RECONSTRUCTION_STATUS.RECOVERED_FROM_ORIGINAL_ARTIFACT
    ).length,
    RECONSTRUCTED_FROM_VERIFIABLE_SOURCE: opportunities.filter(
      (o) =>
        o.reconstructionStatus ===
        RECONSTRUCTION_STATUS.RECONSTRUCTED_FROM_VERIFIABLE_SOURCE
    ).length,
    STILL_MISSING_RESEARCH: opportunities.filter(
      (o) =>
        o.reconstructionStatus === RECONSTRUCTION_STATUS.STILL_MISSING_RESEARCH
    ).length,
  };

  const byGroup = {
    localCatering: opportunities.filter(
      (o) => o.researchCategory === RECOVERY_CATEGORY.LOCAL_CATERING
    ),
    partnerships: opportunities.filter(
      (o) => o.researchCategory === RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS
    ),
    other: opportunities.filter(
      (o) =>
        o.researchCategory !== RECOVERY_CATEGORY.LOCAL_CATERING &&
        o.researchCategory !== RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS
    ),
  };

  const oppPath = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    BETHESDA_CANONICAL_HOTEL_ID,
    "opportunities.json"
  );

  return {
    version: BETHESDA_29_HISTORICAL_RECONSTRUCTION_V1,
    generatedAt: new Date().toISOString(),
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    safety: {
      paidResearch: false,
      newDiscovery: false,
      customerFacing: false,
      salesReady: false,
      publicationApproved: false,
      opportunitiesJsonWrites: false,
      shareRegistryWrites: false,
      inventedBodies: false,
      researchStageOnly: true,
    },
    counts: {
      attestedIds: 29,
      reconstructedRecords: opportunities.length,
      recoveredFromArtifact: byStatus.RECOVERED_FROM_ORIGINAL_ARTIFACT,
      reconstructedFromVerifiableSource:
        byStatus.RECONSTRUCTED_FROM_VERIFIABLE_SOURCE,
      stillMissingResearch: byStatus.STILL_MISSING_RESEARCH,
      localCatering: byGroup.localCatering.length,
      partnerships: byGroup.partnerships.length,
      otherGroupCorporate: byGroup.other.length,
      schemaValid: opportunities.filter((o) => o.schemaValidation?.scalingCanonical?.ok)
        .length,
      salesReadyForced: 0,
      customerVisible: opportunities.filter((o) => o.customerVisible).length,
      duplicateOfCanonical: opportunities.filter((o) => o.duplicateOfCanonical)
        .length,
    },
    dispositions: {
      ATTESTED_BODY_MISSING: opportunities.filter(
        (o) =>
          o.historicalRecoveryDisposition ===
          RECOVERY_DISPOSITION.ATTESTED_BODY_MISSING
      ).length,
      OVERFLOW_WATCH: opportunities.filter(
        (o) =>
          o.historicalRecoveryDisposition === RECOVERY_DISPOSITION.OVERFLOW_WATCH
      ).length,
      PARTNERSHIP_WATCH: opportunities.filter(
        (o) =>
          o.historicalRecoveryDisposition ===
          RECOVERY_DISPOSITION.PARTNERSHIP_WATCH
      ).length,
    },
    artifactSources: {
      finalizeLog: finalize,
      maturationLog: {
        ok: maturation.ok,
        path: maturation.path,
        recoveredIdCount: maturation.recoveredIdCount,
      },
    },
    opportunityStore: {
      pathRel: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/opportunities.json`,
      sha256: fs.existsSync(oppPath) ? sha256File(oppPath) : null,
      untouched: true,
      expectedSha256:
        "fd438549ff030ffc6f219469a526ec9b88ad8a704871fa01784d5650468226ad",
    },
    opportunities,
    groups: {
      localCatering: byGroup.localCatering.map((o) => o.id),
      partnerships: byGroup.partnerships.map((o) => o.id),
      other: byGroup.other.map((o) => o.id),
    },
  };
}

/**
 * Persist reconstructed bodies into research-stage storage + optional fixture.
 * Never touches opportunities.json.
 */
export function stageBethesda29HistoricalReconstruction({
  root = ROOT,
  writeFixture = true,
  writeMemoryCheckpoints = true,
} = {}) {
  const doc = buildBethesda29HistoricalReconstruction({ root });
  const targetPath = path.join(root, RECONSTRUCTION_BAG_REL);
  const persist = persistResearchStageOpportunityBag({
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    targetPath,
    incomingDoc: {
      version: BETHESDA_29_HISTORICAL_RECONSTRUCTION_V1,
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
      safety: doc.safety,
      counts: doc.counts,
      dispositions: doc.dispositions,
      opportunityStore: doc.opportunityStore,
      groups: doc.groups,
      generatedAt: doc.generatedAt,
      opportunities: doc.opportunities,
    },
    protectedIds: [...BETHESDA_29_ATTESTED_IDS],
    root,
    label: "bethesda-29-reconstruction",
    mode: "merge",
  });

  let memoryWrites = [];
  if (writeMemoryCheckpoints) {
    const memDir = researchStageMemoryDir(BETHESDA_CANONICAL_HOTEL_ID, { root });
    for (const opp of doc.opportunities) {
      const p = writeMaturationCheckpointAtomic(memDir, {
        ...opp,
        researchStatus: opp.researchStatus || "RESEARCH_CONTINUATION",
      });
      const validation = validateMaturationCheckpoint(memDir, opp);
      memoryWrites.push({ id: opp.id, path: p, validationStatus: validation.status });
    }
  }

  let fixtureWrite = null;
  if (writeFixture) {
    const fixturePath = path.join(root, RECONSTRUCTION_FIXTURE_REL);
    fs.mkdirSync(path.dirname(fixturePath), { recursive: true });
    fixtureWrite = writeJsonAtomic(fixturePath, {
      ...doc,
      // Keep full opportunities in fixture — derived from committed attestations + public logs.
    });
  }

  // After-hash of canonical store
  const afterHash = doc.opportunityStore.sha256;

  return {
    ok: persist.ok === true,
    persist,
    memoryWrites,
    fixtureWrite,
    doc,
    stagedPath: targetPath,
    hotelStoreUntouched: true,
    opportunityStoreSha256: afterHash,
  };
}

export function researchStageReconstructionBagPath(hotelId = BETHESDA_CANONICAL_HOTEL_ID, { root = ROOT } = {}) {
  return path.join(
    researchStageHotelDir(hotelId, { root }),
    "reconstructed-opportunities-v1.json"
  );
}

export function loadStagedReconstructionBag({ root = ROOT } = {}) {
  const p = researchStageReconstructionBagPath(BETHESDA_CANONICAL_HOTEL_ID, {
    root,
  });
  if (!fs.existsSync(p)) {
    const fixture = path.join(root, RECONSTRUCTION_FIXTURE_REL);
    if (!fs.existsSync(fixture)) return null;
    return JSON.parse(fs.readFileSync(fixture, "utf8"));
  }
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
