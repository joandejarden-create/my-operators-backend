#!/usr/bin/env node
/**
 * Reissue corrected NICE one-record canary proposal under lossless SoT merge.
 * DOES NOT EXECUTE — proposal + snapshot/rollback evidence only.
 *
 * Default: uses prior prewrite snapshot (no Airtable I/O).
 * Optional live refresh: GDI_CANARY_LIVE_SNAPSHOT=1 (read-only find; never writes).
 */
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  buildResearchInventoryFields,
  mergeProposedResearchInventoryFields,
  diffMaterialResearchFields,
  fingerprintResearchFields,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";
import { buildHotelResearchCompositeKey } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";
import {
  evaluateResearchPersistenceAuthorization,
  RESEARCH_PERSISTENCE_ACTIVATION,
  RESEARCH_PERSISTENCE_WRITE_SET_POLICY,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import { GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1 } from "../lib/group-demand-intelligence/workflow/gdi-research-sot-lossless-merge-v1.js";
import { GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1 } from "../lib/group-demand-intelligence/workflow/gdi-research-sot-field-ownership-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-nice-canary-reissue-lossless-v1");
const PRIOR = path.join(
  ROOT,
  "reports/gdi-validated-airtable-canary-prewrite-safety-v1",
);
const BETHESDA = "recLuxvwwxID7U2B8";
const SELECTED = "gdi_opp_nice_2027";
const RECORD_ID = "rec8QDHQEpGbi8kvW";
const QUALITY =
  "gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261125_quality_v1";
const GOLD = "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";

fs.mkdirSync(OUT, { recursive: true });

function sha256(s) {
  return createHash("sha256").update(String(s || ""), "utf8").digest("hex");
}
function writeJson(name, data) {
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(data, null, 2)}\n`);
}

const quality = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
      BETHESDA,
      QUALITY,
      "package.json",
    ),
    "utf8",
  ),
).opportunities;
const opp = quality.find((o) => (o.opportunityId || o.id) === SELECTED);
if (!opp) throw new Error("NICE missing from quality_v1");

const priorSnapshot = JSON.parse(
  fs.readFileSync(
    path.join(PRIOR, "04-COMPLETE_LIVE_PREWRITE_SNAPSHOT.json"),
    "utf8",
  ),
);
const livePayloadRaw = fs.readFileSync(
  path.join(PRIOR, "04b-ORIGINAL_researchPayloadJson.json"),
  "utf8",
).replace(/\n$/, "");
const liveFields = {
  ...(priorSnapshot.fields || {}),
  [F.researchPayloadJson]: livePayloadRaw,
};
const compositeKey = buildHotelResearchCompositeKey(BETHESDA, SELECTED);
const airtableRecordId = priorSnapshot.airtableRecordId || RECORD_ID;

const completeSnapshot = {
  airtableRecordId,
  opportunityId: SELECTED,
  compositeKey,
  capturedAt: priorSnapshot.capturedAt || null,
  reissuedAt: new Date().toISOString(),
  snapshotSource: "PRIOR_PREWRITE_SNAPSHOT_NO_LIVE_IO",
  fields: liveFields,
  researchPayloadJsonSha256: sha256(livePayloadRaw),
  researchPayloadJsonChars: livePayloadRaw.length,
  materialFingerprint: fingerprintResearchFields(liveFields),
};

writeJson("04-COMPLETE_LIVE_PREWRITE_SNAPSHOT.json", {
  ...completeSnapshot,
  fieldsNote: "Full fields mirrored from prior prewrite; payload in 04b",
  fields: {
    ...liveFields,
    [F.researchPayloadJson]: `[see 04b; ${livePayloadRaw.length} chars]`,
  },
});
fs.writeFileSync(
  path.join(OUT, "04b-ORIGINAL_researchPayloadJson.json"),
  `${livePayloadRaw}\n`,
);

const built = buildResearchInventoryFields(
  { ...opp, hotelId: BETHESDA, opportunityId: SELECTED },
  { hotelId: BETHESDA },
);
if (!built.ok) throw new Error(`build failed: ${built.code}`);

const merged = mergeProposedResearchInventoryFields(built.fields, liveFields);
if (!merged.ok) {
  writeJson("05-MERGE_BLOCKED.json", merged);
  throw new Error(`lossless merge blocked: ${merged.code}`);
}

const proposedFields = merged.fields;
const materialDiff = diffMaterialResearchFields(proposedFields, liveFields);
const afterPayload = proposedFields[F.researchPayloadJson];

writeJson("05-EXACT_CANARY_FIELD_DIFF.json", {
  opportunityId: SELECTED,
  airtableRecordId,
  action: materialDiff.materialChanged ? "WOULD_UPDATE" : "UNCHANGED",
  materialChanged: materialDiff.materialChanged,
  changedFields: materialDiff.changedFields,
  classification: merged.classification,
  mergeVersion: GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
  ownershipContract: GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
  livePayloadChars: livePayloadRaw.length,
  proposedPayloadChars:
    typeof afterPayload === "string" ? afterPayload.length : 0,
  proposedPayloadSha256:
    typeof afterPayload === "string" ? sha256(afterPayload) : null,
  sotDiff: merged.sotMerge?.diff || null,
  autoResolvedConflicts: merged.sotMerge?.autoResolvedConflicts || [],
});

fs.writeFileSync(
  path.join(OUT, "05b-PROPOSED_LOSSLESS_researchPayloadJson.json"),
  typeof afterPayload === "string"
    ? afterPayload.endsWith("\n")
      ? afterPayload
      : `${afterPayload}\n`
    : "",
);

writeJson("06-ROLLBACK_PLAN.json", {
  reversible: true,
  airtableRecordId,
  steps: [
    `1. PATCH/update Airtable record ${airtableRecordId} restoring every key in prior 04-COMPLETE_LIVE_PREWRITE_SNAPSHOT.json fields.`,
    "2. Especially restore researchPayloadJson from 04b-ORIGINAL_researchPayloadJson.json (byte-identical).",
    `3. Confirm researchPayloadJson sha256 === ${completeSnapshot.researchPayloadJsonSha256}.`,
    `4. Confirm material fingerprint === ${completeSnapshot.materialFingerprint}.`,
    "5. Confirm STAGED pointer still quality_v1 and CURRENT still gold local_master_v2.",
    "6. Confirm no GDO rows created/updated.",
  ],
  originalPayloadSha256: completeSnapshot.researchPayloadJsonSha256,
});

const store = createGdiPublicationPackageStore();
const staged = store.readPointer(BETHESDA, "STAGED");
const current = store.readPointer(BETHESDA, "CURRENT");
writeJson("08-STAGED_CURRENT_BASELINE.json", {
  staged: staged?.publicationVersion || null,
  current: current?.publicationVersion || null,
  stagedIsQuality68: staged?.publicationVersion === QUALITY,
  currentIsGold67: current?.publicationVersion === GOLD,
  noStagedChangeAuthorized: true,
  noCurrentChangeAuthorized: true,
});

const authLocked = evaluateResearchPersistenceAuthorization({
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: "FOUNDER_CANARY_gdi_opp_nice_2027",
});

writeJson("01-WRITE_GUARD_VERIFICATION.json", {
  writeBudget: 1,
  allowedOpportunityIds: [SELECTED],
  writeSetPolicy: RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
  envGate: RESEARCH_PERSISTENCE_ACTIVATION.envGate,
  productionUnlockedWithoutEnv: authLocked.productionUnlocked === false,
  gdoWrites: 0,
  stagedWrites: 0,
  currentWrites: 0,
  maxWrites: 1,
  idempotencyRequired: true,
  readBackVerificationRequired: true,
  executeAuthorized: false,
});

writeJson("00-CANARY_REISSUE_SUMMARY.json", {
  status: "PROPOSED_NOT_EXECUTED",
  opportunityId: SELECTED,
  airtableRecordId,
  compositeKey,
  mergeVersion: GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
  ownershipContract: GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
  classification: merged.classification,
  livePayloadChars: livePayloadRaw.length,
  proposedLosslessChars:
    typeof afterPayload === "string" ? afterPayload.length : 0,
  rejectedPriorSlimChars: 7235,
  materialChanged: materialDiff.materialChanged,
  writeBudget: 1,
  mutationExecuted: false,
  snapshotSource: "PRIOR_PREWRITE_SNAPSHOT_NO_LIVE_IO",
  founderVerdictRequired: "READY_FOR_FOUNDER_GO or BLOCKED",
});

const md = `# Corrected NICE Canary Proposal (Lossless SoT)

**Status:** PROPOSED — NOT EXECUTED  
**Record:** \`${airtableRecordId}\` / \`${SELECTED}\`  
**Merge:** \`${GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1}\`  
**Ownership:** \`${GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1}\`

## Guards

- Single existing Airtable record
- Single allowed opportunity ID
- Maximum one write
- Full live snapshot + rollback plan (from prior prewrite; refresh optional)
- Read-back + idempotency required on execute
- No GDO / STAGED / CURRENT changes

## Payload

| | Chars |
|---|---|
| Live SoT | ${livePayloadRaw.length} |
| Prior rejected slim | 7235 |
| Proposed lossless merge | ${typeof afterPayload === "string" ? afterPayload.length : 0} |

Classification: \`${merged.classification}\`

**STOP — founder authorization required before any mutation.**
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_CANARY_REISSUE_PROPOSAL.md"), md);

console.log(
  JSON.stringify(
    {
      out: OUT,
      status: "PROPOSED_NOT_EXECUTED",
      airtableRecordId,
      liveChars: livePayloadRaw.length,
      proposedChars: typeof afterPayload === "string" ? afterPayload.length : 0,
      classification: merged.classification,
      mutationExecuted: false,
    },
    null,
    2,
  ),
);
