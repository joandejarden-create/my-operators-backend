/**
 * Bethesda Historical Attestation Bag V1 — durable internal recovery bag.
 *
 * Represents the 29 historically attested opportunities as ATTESTED_BODY_MISSING.
 * Never customer-facing. Never Sales Ready. Never auto-promoted into opportunities.json.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import {
  CURATED_HISTORICAL_ATTESTATIONS,
  buildBethesdaHistoricalRecoveryManifest,
  RECOVERY_CATEGORY,
} from "./bethesda-historical-opportunity-recovery-v1.js";
import {
  BODY_STATUS,
  RECOVERY_DISPOSITION,
  GDI_RESEARCH_STAGE_PERSISTENCE_V1,
  researchStageAttestationBagPath,
  writeJsonAtomic,
  checkpointExistingFile,
} from "./gdi-research-stage-persistence-v1.js";

export const BETHESDA_HISTORICAL_ATTESTATION_BAG_V1 =
  "gdi-bethesda-historical-attestation-bag-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
export const ATTESTATION_FIXTURE_REL =
  "fixtures/gdi/bethesda-historical-attestation-bag-v1.json";

const COMPETITOR_HOST_IDS = new Set([
  "gdi_s4912_732496c2f8aa",
  "gdi_s4912_512deebd0963",
  "gdi_s4912_6c8a62552bfa",
  "gdi_s4912_b0ea89e0716d",
  "gdi_beth_v16_evt_3c8228774517",
]);

const PARTNERSHIP_WATCH_IDS = new Set([
  "gdi_plp_live_fa2063dc1195",
  "gdi_plp_live_4958569d826e",
  "gdi_pep_live_af0711e711ba",
  "gdi_pep_live_c165c897d233",
  "disp_recLuxvwwxID7U2B8_strathmore",
  "disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_",
  "disp_recLuxvwwxID7U2B8_linder_global_events",
  "gdi_e2d_bc0a94d7c6007f36",
]);

/** The exact 29 missing historical opportunity IDs from recovery coverage. */
export const BETHESDA_29_ATTESTED_IDS = Object.freeze([
  "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce",
  "gdi_e2d_770e3b44ad97c1ae",
  "disp_recLuxvwwxID7U2B8_montgomery_county_shrm",
  "disp_recLuxvwwxID7U2B8_pmi_montgomery_county_maryland_chapter",
  "disp_recLuxvwwxID7U2B8_city_of_rockville",
  "disp_recLuxvwwxID7U2B8_nature_forward",
  "gdi_s4912_59382d1e71a2",
  "gdi_s4912_d55ee8a8e9ca",
  "gdi_s4912_6c8a62552bfa",
  "gdi_s4912_6ffc303bf55a",
  "disp_recLuxvwwxID7U2B8_strathmore",
  "disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_",
  "disp_recLuxvwwxID7U2B8_linder_global_events",
  "gdi_e2d_bc0a94d7c6007f36",
  "gdi_plp_live_fa2063dc1195",
  "gdi_plp_live_4958569d826e",
  "gdi_pep_live_af0711e711ba",
  "gdi_pep_live_c165c897d233",
  "disp_recLuxvwwxID7U2B8_aledade",
  "disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup",
  "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
  "gdi_s4912_66a1f89e5afb",
  "gdi_s4912_364713d41c67",
  "gdi_s4912_732496c2f8aa",
  "gdi_s4912_512deebd0963",
  "gdi_beth_v16_evt_3c8228774517",
  "gdi_beth_v16_evt_90a6ddcb10ca",
  "gdi_beth_v16_evt_0ab91e208405",
  "gdi_s4912_b0ea89e0716d",
]);

function resolveDisposition(rec) {
  if (COMPETITOR_HOST_IDS.has(rec.id)) return RECOVERY_DISPOSITION.OVERFLOW_WATCH;
  if (PARTNERSHIP_WATCH_IDS.has(rec.id)) return RECOVERY_DISPOSITION.PARTNERSHIP_WATCH;
  return RECOVERY_DISPOSITION.ATTESTED_BODY_MISSING;
}

function bodyFlags(rec) {
  const bodyAvailable = rec.bodyAvailable === true;
  return {
    originalRecordBodyAvailable: bodyAvailable,
    historicalAttestationOnly: !bodyAvailable,
    sourceEvidenceAvailable: Boolean(
      rec.sourceArtifactPath || rec.evidenceNotes || (rec.rationale || []).length
    ),
    missingOriginalResearchBody: !bodyAvailable,
    furtherResearchRequired: true,
    bodyStatus: bodyAvailable
      ? BODY_STATUS.ORIGINAL_BODY_AVAILABLE
      : BODY_STATUS.HISTORICAL_ATTESTATION_ONLY,
  };
}

export function buildBethesdaHistoricalAttestationBag({ root = ROOT } = {}) {
  const byId = new Map();
  for (const rec of CURATED_HISTORICAL_ATTESTATIONS) {
    if (!BETHESDA_29_ATTESTED_IDS.includes(rec.id)) continue;
    byId.set(rec.id, rec);
  }
  // Fill any gaps from recovery manifest v20 rows
  const manifest = buildBethesdaHistoricalRecoveryManifest({ root });
  for (const row of manifest.groups.B_and_F_missingHistorical.rows || []) {
    if (!BETHESDA_29_ATTESTED_IDS.includes(row.id)) continue;
    if (!byId.has(row.id)) byId.set(row.id, row);
  }

  const records = BETHESDA_29_ATTESTED_IDS.map((id) => {
    const rec = byId.get(id) || { id, organizationName: null };
    const flags = bodyFlags(rec);
    const disposition = resolveDisposition(rec);
    return {
      id,
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      organizationName: rec.organizationName || null,
      title: rec.title || null,
      researchCategory: rec.researchCategory || null,
      demandSegment: rec.demandSegment || null,
      originalDisposition:
        rec.originalCommercialQualification ||
        rec.originalResearchStatus ||
        rec.disposition ||
        null,
      recoveryDisposition: disposition,
      discoveryRun: rec.discoveryRun || null,
      discoveryDate: rec.discoveryDate || null,
      sourceArtifactPath: rec.sourceArtifactPath || null,
      evidenceThatRecordExisted: [
        rec.sourceArtifactPath ? `source:${rec.sourceArtifactPath}` : null,
        rec.discoveryRun ? `run:${rec.discoveryRun}` : null,
        "attested_in_bethesda_historical_opportunity_recovery_v1",
      ].filter(Boolean),
      ...flags,
      customerFacing: false,
      salesReady: false,
      publicationApproved: false,
      promoteToOpportunitiesJson: false,
      inventedFields: false,
      note:
        "ATTESTED_BODY_MISSING — do not invent descriptions, contacts, theses, room nights, or quotations.",
    };
  });

  if (records.length !== 29) {
    throw new Error(`Expected 29 attestation records, got ${records.length}`);
  }

  return {
    version: BETHESDA_HISTORICAL_ATTESTATION_BAG_V1,
    persistenceContract: GDI_RESEARCH_STAGE_PERSISTENCE_V1,
    generatedAt: new Date().toISOString(),
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    count: 29,
    ids: [...BETHESDA_29_ATTESTED_IDS],
    safety: {
      customerFacing: false,
      salesReady: false,
      publicationApproved: false,
      opportunitiesJsonWrites: false,
      paidResearch: false,
      inventedBodies: false,
    },
    dispositions: {
      ATTESTED_BODY_MISSING: records.filter(
        (r) => r.recoveryDisposition === RECOVERY_DISPOSITION.ATTESTED_BODY_MISSING
      ).length,
      OVERFLOW_WATCH: records.filter(
        (r) => r.recoveryDisposition === RECOVERY_DISPOSITION.OVERFLOW_WATCH
      ).length,
      PARTNERSHIP_WATCH: records.filter(
        (r) => r.recoveryDisposition === RECOVERY_DISPOSITION.PARTNERSHIP_WATCH
      ).length,
    },
    records,
  };
}

/**
 * Write durable bag to research-stage path (internal) + optional fixture refresh.
 * Does not touch opportunities.json.
 */
export function persistBethesdaHistoricalAttestationBag({
  root = ROOT,
  writeFixture = true,
} = {}) {
  const bag = buildBethesdaHistoricalAttestationBag({ root });
  const stagePath = researchStageAttestationBagPath(BETHESDA_CANONICAL_HOTEL_ID, {
    root,
  });
  checkpointExistingFile(stagePath, BETHESDA_CANONICAL_HOTEL_ID, {
    root,
    label: "attestation-bag",
  });
  const stageWrite = writeJsonAtomic(stagePath, bag);

  let fixtureWrite = null;
  if (writeFixture) {
    const fixturePath = path.join(root, ATTESTATION_FIXTURE_REL);
    fs.mkdirSync(path.dirname(fixturePath), { recursive: true });
    fixtureWrite = writeJsonAtomic(fixturePath, bag);
  }

  return {
    bag,
    stagePath,
    stageWrite,
    fixtureWrite,
    opportunitiesJsonUntouched: true,
  };
}

export { RECOVERY_CATEGORY, RECOVERY_DISPOSITION, BODY_STATUS };
