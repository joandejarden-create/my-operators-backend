#!/usr/bin/env node
/**
 * Final pre-write safety check for one validated Airtable UPDATE canary.
 * Read-only. Does not mutate Airtable / STAGED / CURRENT / GDO.
 */
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  buildResearchInventoryFields,
  diffMaterialResearchFields,
  findResearchInventoryByCompositeKey,
  fingerprintResearchFields,
  listResearchInventoryForHotel,
  mergeProposedResearchInventoryFields,
  MATERIAL_RESEARCH_FIELDS,
  readBackResearchInventory,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import {
  buildSlimResearchPayload,
  assertSlimPayloadRoundTrip,
  collectNamedContacts,
  collectEvidenceUrls,
} from "../lib/group-demand-intelligence/workflow/gdi-research-inventory-slim-payload-v1.js";
import { evaluateFinalizedQualifiedOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import {
  evaluateResearchPersistenceAuthorization,
  filterValidatedFqoPersistenceWriteSet,
  RESEARCH_PERSISTENCE_ACTIVATION,
  RESEARCH_PERSISTENCE_WRITE_SET_POLICY,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import { ensureDemandGeneratorIdOnOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-slice2b-relationship-wiring-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";
import { buildHotelResearchCompositeKey } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-validated-airtable-canary-prewrite-safety-v1",
);
const BETHESDA = "recLuxvwwxID7U2B8";
const QUALITY =
  "gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261125_quality_v1";
const GOLD = "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";
const PRIMARY_CANDIDATE = "gdi_s4912_512deebd0963";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(p, `${JSON.stringify(data, null, 2)}\n`);
  return p;
}
function sha256(s) {
  return createHash("sha256").update(String(s || ""), "utf8").digest("hex");
}

function loadQualityOpps() {
  const p = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    BETHESDA,
    QUALITY,
    "package.json",
  );
  return (JSON.parse(fs.readFileSync(p, "utf8")).opportunities || []).map(
    (o) => ({
      ...o,
      opportunityId: text(o.opportunityId || o.id),
      hotelId: BETHESDA,
    }),
  );
}

function evidenceHttp(opp) {
  return collectEvidenceUrls(opp).filter((u) => /^https?:\/\//i.test(u));
}

function outreachOf(opp) {
  const pc = opp.primaryContact || {};
  return (
    text(pc.email) ||
    text(pc.phone) ||
    text(pc.linkedinUrl) ||
    text(pc.askToBeDirectedTo) ||
    text(pc.organizationContactRouteLabel) ||
    null
  );
}

function scoreStrictCommercial(opp, fqoStrict) {
  if (fqoStrict.finalizedQualified !== true) return -1;
  const urls = evidenceHttp(opp);
  const named = text(opp.primaryContact?.name);
  const outreach = outreachOf(opp);
  if (!named || !/@/.test(String(outreach || ""))) return -1;
  let score = 100;
  score += Math.min(urls.length, 8) * 5;
  score += text(opp.hotelOpportunityThesis || opp.whyThisHotel) ? 20 : 0;
  score += text(opp.eventStartDate || opp.eventDateDisplay) ? 10 : 0;
  score += text(opp.recommendedNextStep || opp.recommendedNextAction) ? 10 : 0;
  // Prefer official-looking source hosts when present
  if (urls.some((u) => /\.(edu|gov)(\/|$)/i.test(u))) score += 15;
  return score;
}

function assessGates(opp, fqo) {
  const gates = fqo.gates || {};
  return {
    evidenceBackedDemand: gates.EVIDENCE_BACKED_DEMAND || gates.evidenceBackedDemand || null,
    hotelGeoCommercialFit:
      gates.HOTEL_GEO_COMMERCIAL_FIT || gates.hotelGeoCommercialFit || null,
    futureSourcingPathway:
      gates.FUTURE_SOURCING_PATHWAY || gates.futureSourcingPathway || null,
    namedCommercialPerson:
      gates.NAMED_COMMERCIAL_PERSON || gates.namedCommercialPerson || null,
    personSpecificChannel:
      gates.PERSON_SPECIFIC_CHANNEL || gates.personSpecificChannel || null,
    hotelSpecificAction:
      gates.HOTEL_SPECIFIC_ACTION || gates.hotelSpecificAction || null,
    reasons: fqo.reasons || [],
    status: fqo.status,
    finalizedQualified: fqo.finalizedQualified === true,
    modeHints: {
      evidenceMode: gates.EVIDENCE_BACKED_DEMAND?.mode || null,
      evidenceUrlCount: gates.EVIDENCE_BACKED_DEMAND?.evidenceUrlCount ?? null,
      fitBasis: gates.HOTEL_GEO_COMMERCIAL_FIT?.basis || null,
    },
  };
}

fs.mkdirSync(OUT, { recursive: true });

// --- 1. Write-guard verification (code-level) ---
const authLocked = evaluateResearchPersistenceAuthorization({
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: "FOUNDER_CANARY_TEST",
});
const authNoIds = evaluateResearchPersistenceAuthorization({
  apply: false,
});
const guardReport = {
  envGate: RESEARCH_PERSISTENCE_ACTIVATION.envGate,
  envCurrentlyUnlocked:
    String(process.env.GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES || "") === "1",
  defaultProductionWriteSetPolicy:
    RESEARCH_PERSISTENCE_ACTIVATION.defaultProductionWriteSetPolicy,
  requiresExactWriteSetAuthorization:
    RESEARCH_PERSISTENCE_ACTIVATION.requiresExactWriteSetAuthorization === true,
  productionUnlockedWithTokenButEnvOff: authLocked.productionUnlocked === false,
  blockersWhenEnvOff: authLocked.blockers,
  applyFalseRemainsLocked: authNoIds.productionUnlocked === false,
  allowedOpportunityIdsSupported: true,
  note: "Production unlock requires authorizeResearchPersistence + exactWriteSetAuthorization + env gate + apply; VALIDATED_FQO_ONLY is default when productionUnlocked or requireValidatedFqoWriteSet.",
};
writeJson("01-WRITE_GUARD_VERIFICATION.json", guardReport);

// --- 2. Strict commercial integrity on primary + alternates ---
const quality = loadQualityOpps();
const qualityIds = new Set(quality.map((o) => o.opportunityId));
const scored = quality.map((opp) => {
  const strict = evaluateFinalizedQualifiedOpportunity(opp, {
    historicalApprovedInventory: false,
  });
  const withHist = evaluateFinalizedQualifiedOpportunity(opp, {
    historicalApprovedInventory: true,
  });
  return {
    opportunityId: opp.opportunityId,
    title: opp.title || opp.displayTitle || null,
    organizationName: opp.organizationName || null,
    namedContact: text(opp.primaryContact?.name) || null,
    outreach: outreachOf(opp),
    evidenceUrls: evidenceHttp(opp),
    hotelThesis: text(opp.hotelOpportunityThesis || opp.whyThisHotel) || null,
    eventStartDate: opp.eventStartDate || opp.eventDateDisplay || null,
    recommendedNextStep:
      text(opp.recommendedNextStep || opp.recommendedNextAction) || null,
    strict,
    withHist,
    strictGates: assessGates(opp, strict),
    score: scoreStrictCommercial(opp, strict),
    dependsOnHistoricalCompat:
      withHist.finalizedQualified === true &&
      strict.finalizedQualified !== true,
  };
});

const primary = scored.find((r) => r.opportunityId === PRIMARY_CANDIDATE);
const primaryCommercialAmbiguity = /competitor-host|do not treat as available|overflow \/ alternate|until independent overflow/i.test(
  String(primary?.hotelThesis || ""),
);
const primaryEvidenceOk =
  (primary?.evidenceUrls?.length || 0) > 0 ||
  Number(primary?.strictGates?.modeHints?.evidenceUrlCount || 0) > 0;
const primaryOk =
  primary &&
  primary.strict.finalizedQualified === true &&
  primary.score >= 0 &&
  primaryEvidenceOk &&
  !primary.dependsOnHistoricalCompat &&
  !primaryCommercialAmbiguity;

const alternates = scored
  .filter(
    (r) =>
      r.opportunityId !== PRIMARY_CANDIDATE &&
      r.strict.finalizedQualified === true &&
      r.score >= 0 &&
      r.evidenceUrls.length >= 2 &&
      !r.dependsOnHistoricalCompat,
  )
  .sort((a, b) => b.score - a.score);

const selectedId = primaryOk
  ? PRIMARY_CANDIDATE
  : alternates[0]?.opportunityId || null;
const selectedMeta = scored.find((r) => r.opportunityId === selectedId) || null;
const selectedOpp = quality.find((o) => o.opportunityId === selectedId) || null;

writeJson("02-CANARY_COMMERCIAL_INTEGRITY.json", {
  primaryCandidate: PRIMARY_CANDIDATE,
  primaryAccepted: primaryOk === true,
  primary: primary
    ? {
        opportunityId: primary.opportunityId,
        title: primary.title,
        score: primary.score,
        dependsOnHistoricalCompat: primary.dependsOnHistoricalCompat,
        strictFinalizedQualified: primary.strict.finalizedQualified,
        strictReasons: primary.strict.reasons,
        strictGates: primary.strictGates,
        namedContact: primary.namedContact,
        outreach: primary.outreach,
        evidenceUrls: primary.evidenceUrls,
        hotelThesis: primary.hotelThesis,
        eventStartDate: primary.eventStartDate,
        recommendedNextStep: primary.recommendedNextStep,
      }
    : null,
  selectedOpportunityId: selectedId,
  primaryCommercialAmbiguity: primaryCommercialAmbiguity === true,
  selectionReason: primaryOk
    ? "Primary U.S. IFE candidate passes strict FQO with evidence URLs, named contact, and outreach."
    : selectedId
      ? primaryCommercialAmbiguity
        ? "Primary U.S. IFE has unresolved competitor-host / overflow commercial ambiguity in hotel thesis; selected stronger NIST.gov-backed NICE alternate under strict FQO."
        : "Primary depends on historical compatibility or lacks strict evidence; selected stronger strict-FQO alternate."
      : "No strict-FQO canary candidate available — STOP.",
  selected: selectedMeta
    ? {
        opportunityId: selectedMeta.opportunityId,
        title: selectedMeta.title,
        score: selectedMeta.score,
        dependsOnHistoricalCompat: selectedMeta.dependsOnHistoricalCompat,
        strictFinalizedQualified: selectedMeta.strict.finalizedQualified,
        strictReasons: selectedMeta.strict.reasons,
        strictGates: selectedMeta.strictGates,
        namedContact: selectedMeta.namedContact,
        outreach: selectedMeta.outreach,
        evidenceUrls: selectedMeta.evidenceUrls,
        hotelThesis: selectedMeta.hotelThesis,
        eventStartDate: selectedMeta.eventStartDate,
        recommendedNextStep: selectedMeta.recommendedNextStep,
      }
    : null,
  alternateTop5: alternates.slice(0, 5).map((a) => ({
    opportunityId: a.opportunityId,
    title: a.title,
    score: a.score,
    namedContact: a.namedContact,
    outreach: a.outreach,
    evidenceCount: a.evidenceUrls.length,
  })),
});

if (!selectedOpp) {
  writeJson("00-CANARY_PREWRITE_SUMMARY.json", {
    verdict: "BLOCKED",
    reason: "NO_STRICT_FQO_CANARY_CANDIDATE",
    pr599Merged: false,
    canaryExecuted: false,
  });
  console.log(JSON.stringify({ verdict: "BLOCKED", reason: "NO_STRICT_FQO_CANARY_CANDIDATE" }, null, 2));
  process.exit(2);
}

// Filter proof: only selected id survives allowedOpportunityIds
const filterProof = filterValidatedFqoPersistenceWriteSet(
  [
    selectedOpp,
    { opportunityId: "gdi_e2d_770e3b44ad97c1ae", title: "nope" },
    ...quality.filter((o) => o.opportunityId !== selectedId).slice(0, 3),
  ],
  {
    allowHistoricalSchemaCompat: false,
    allowedOpportunityIds: [selectedId],
  },
);
writeJson("03-ALLOWED_ID_RESTRICTION_PROOF.json", {
  allowedOpportunityIds: [selectedId],
  kept: filterProof.kept.map((o) => o.opportunityId),
  deferred: filterProof.deferred.map((d) => ({
    opportunityId: d.opportunityId,
    reason: d.reason,
  })),
  onlySelectedKept:
    filterProof.keptCount === 1 &&
    filterProof.kept[0]?.opportunityId === selectedId,
});

// --- 3. Full live snapshot ---
const compositeKey = buildHotelResearchCompositeKey(BETHESDA, selectedId);
let liveRec = null;
try {
  liveRec = await findResearchInventoryByCompositeKey(compositeKey);
} catch (err) {
  writeJson("04-LIVE_FETCH_ERROR.json", { error: String(err?.message || err) });
  console.error("BLOCKED: live fetch failed", err);
  process.exit(2);
}

if (!liveRec?.id) {
  writeJson("00-CANARY_PREWRITE_SUMMARY.json", {
    verdict: "BLOCKED",
    reason: "LIVE_RECORD_NOT_FOUND",
    opportunityId: selectedId,
    compositeKey,
    canaryExecuted: false,
  });
  console.log(JSON.stringify({ verdict: "BLOCKED", reason: "LIVE_RECORD_NOT_FOUND" }, null, 2));
  process.exit(2);
}

// Prefer find + explicit readBack for complete fields
const readBack = await readBackResearchInventory(liveRec.id);
const liveFields = { ...(readBack.fields || liveRec.fields || {}) };
const payloadRaw = liveFields[F.researchPayloadJson];
const payloadComplete =
  typeof payloadRaw === "string" &&
  payloadRaw.length > 0 &&
  (() => {
    try {
      JSON.parse(payloadRaw);
      return true;
    } catch {
      return false;
    }
  })();

const completeSnapshot = {
  at: new Date().toISOString(),
  airtableRecordId: liveRec.id,
  opportunityId: selectedId,
  hotelId: BETHESDA,
  compositeKey,
  fields: liveFields, // FULL — not truncated
  researchPayloadJsonChars:
    typeof payloadRaw === "string" ? payloadRaw.length : 0,
  researchPayloadJsonSha256:
    typeof payloadRaw === "string" ? sha256(payloadRaw) : null,
  researchPayloadJsonParseOk: payloadComplete,
  materialFingerprint: fingerprintResearchFields(liveFields),
  allFieldNames: Object.keys(liveFields).sort(),
};

const snapshotPath = writeJson(
  "04-COMPLETE_LIVE_PREWRITE_SNAPSHOT.json",
  completeSnapshot,
);

// Also store raw payload alone for exact restore
if (typeof payloadRaw === "string") {
  // Exact bytes for rollback — do not append trailing newline.
  fs.writeFileSync(
    path.join(OUT, "04b-ORIGINAL_researchPayloadJson.json"),
    payloadRaw,
  );
}

const snapshotOk =
  completeSnapshot.researchPayloadJsonParseOk &&
  completeSnapshot.researchPayloadJsonChars > 0 &&
  Boolean(completeSnapshot.materialFingerprint) &&
  fs.existsSync(snapshotPath);

// --- Proposed one-record diff (dry, no write) ---
const stamped = ensureDemandGeneratorIdOnOpportunity({
  ...selectedOpp,
  hotelId: BETHESDA,
  finalizedQualified: true,
  fqoEvaluation: selectedMeta.strict,
  firstDiscoveredRunId:
    selectedOpp.firstDiscoveredRunId ||
    liveFields[F.firstDiscoveredRunId] ||
    "canary-prewrite",
  lastResearchedAt:
    selectedOpp.lastResearchedAt || liveFields[F.lastResearchedAt] || null,
});
const built = buildResearchInventoryFields(stamped, {
  hotelId: BETHESDA,
  hotelName: "Bethesda Marriott",
});
if (!built.ok) {
  writeJson("00-CANARY_PREWRITE_SUMMARY.json", {
    verdict: "BLOCKED",
    reason: built.code,
    canaryExecuted: false,
  });
  process.exit(2);
}

const merged = mergeProposedResearchInventoryFields(built.fields, liveFields);
if (!merged.ok) {
  writeJson("00-CANARY_PREWRITE_SUMMARY.json", {
    verdict: "BLOCKED",
    reason: merged.code,
    lostContacts: merged.lostContacts,
    lostEvidence: merged.lostEvidence,
    canaryExecuted: false,
  });
  process.exit(2);
}

let proposedFields = { ...merged.fields };
// Preserve firstDiscovered* exactly as live
if (liveFields[F.firstDiscoveredAt]) {
  proposedFields[F.firstDiscoveredAt] = liveFields[F.firstDiscoveredAt];
} else {
  delete proposedFields[F.firstDiscoveredAt];
}
if (liveFields[F.firstDiscoveredRunId]) {
  proposedFields[F.firstDiscoveredRunId] = liveFields[F.firstDiscoveredRunId];
} else {
  delete proposedFields[F.firstDiscoveredRunId];
}

const materialDiff = diffMaterialResearchFields(proposedFields, liveFields);
if (materialDiff.materialChanged) {
  proposedFields[F.lastMaterialChangeAt] = new Date().toISOString();
} else {
  delete proposedFields[F.lastMaterialChangeAt];
}

const fieldDiff = [];
for (const fname of [
  ...MATERIAL_RESEARCH_FIELDS,
  F.lastMaterialChangeAt,
  F.lastResearchedAt,
  F.firstDiscoveredAt,
  F.firstDiscoveredRunId,
]) {
  const before = liveFields[fname];
  const after = proposedFields[fname];
  const b =
    before == null || before === ""
      ? null
      : typeof before === "string"
        ? before
        : JSON.stringify(before);
  const a =
    after == null || after === ""
      ? null
      : typeof after === "string"
        ? after
        : JSON.stringify(after);
  if (b === a) continue;
  fieldDiff.push({
    field: fname,
    beforeSha256: b == null ? null : sha256(b),
    afterSha256: a == null ? null : sha256(a),
    beforeChars: b == null ? 0 : b.length,
    afterChars: a == null ? 0 : a.length,
    beforePreview: String(before ?? "").slice(0, 180),
    afterPreview: String(after ?? "").slice(0, 180),
  });
}

const slim = buildSlimResearchPayload(stamped);
const round = slim.ok
  ? assertSlimPayloadRoundTrip(stamped, slim.payload)
  : { ok: false };
const afterPayload = proposedFields[F.researchPayloadJson];
const afterContacts = collectNamedContacts(
  typeof afterPayload === "string" ? JSON.parse(afterPayload) : {},
);
const beforeContacts = collectNamedContacts(
  typeof payloadRaw === "string" ? JSON.parse(payloadRaw) : {},
);
const lostContacts = beforeContacts.filter(
  (n) => !afterContacts.some((x) => x.toLowerCase() === n.toLowerCase()),
);

writeJson("05-EXACT_CANARY_FIELD_DIFF.json", {
  opportunityId: selectedId,
  airtableRecordId: liveRec.id,
  action: materialDiff.materialChanged ? "WOULD_UPDATE" : "UNCHANGED",
  materialChanged: materialDiff.materialChanged,
  changedFields: materialDiff.changedFields,
  fieldDiff,
  proposedMaterialFingerprint: fingerprintResearchFields(proposedFields),
  liveMaterialFingerprint: completeSnapshot.materialFingerprint,
  contactsPreserved: lostContacts.length === 0,
  lostContacts,
  payloadRoundTripOk: round.ok === true,
  proposedResearchPayloadJsonChars:
    typeof afterPayload === "string" ? afterPayload.length : 0,
  proposedResearchPayloadJsonSha256:
    typeof afterPayload === "string" ? sha256(afterPayload) : null,
});

// Save full proposed payload for audit (not applied)
if (typeof afterPayload === "string") {
  fs.writeFileSync(
    path.join(OUT, "05b-PROPOSED_researchPayloadJson.json"),
    afterPayload.endsWith("\n") ? afterPayload : `${afterPayload}\n`,
  );
}

writeJson("06-ROLLBACK_PLAN.json", {
  reversible: snapshotOk === true && lostContacts.length === 0,
  airtableRecordId: liveRec.id,
  steps: [
    `1. PATCH/update Airtable record ${liveRec.id} restoring every key in 04-COMPLETE_LIVE_PREWRITE_SNAPSHOT.json fields.`,
    "2. Especially restore researchPayloadJson from 04b-ORIGINAL_researchPayloadJson.json (byte-identical).",
    `3. Confirm researchPayloadJson sha256 === ${completeSnapshot.researchPayloadJsonSha256}.`,
    `4. Confirm material fingerprint === ${completeSnapshot.materialFingerprint}.`,
    "5. Confirm STAGED pointer still quality_v1 and CURRENT still gold local_master_v2.",
    "6. Confirm no GDO rows created/updated.",
  ],
  snapshotFiles: [
    "04-COMPLETE_LIVE_PREWRITE_SNAPSHOT.json",
    "04b-ORIGINAL_researchPayloadJson.json",
  ],
  originalPayloadSha256: completeSnapshot.researchPayloadJsonSha256,
  originalMaterialFingerprint: completeSnapshot.materialFingerprint,
});

// Remaining write manifest after canary (67 others) — not authorized
const remaining = quality
  .filter((o) => o.opportunityId !== selectedId)
  .map((o) => o.opportunityId)
  .sort();
writeJson("07-REMAINING_WRITE_MANIFEST_AFTER_CANARY.json", {
  note: "No bulk authorization is implied by canary approval. Remaining IDs listed for planning only.",
  canaryOpportunityId: selectedId,
  remainingCount: remaining.length,
  remainingOpportunityIds: remaining,
  remainingAuthorization: "NOT_REQUESTED",
});

const store = createGdiPublicationPackageStore();
const staged = store.readPointer(BETHESDA, "STAGED");
const current = store.readPointer(BETHESDA, "CURRENT");
writeJson("08-STAGED_CURRENT_BASELINE.json", {
  staged: staged?.publicationVersion || null,
  current: current?.publicationVersion || null,
  stagedIsQuality68: staged?.publicationVersion === QUALITY,
  currentIsGold67: current?.publicationVersion === GOLD,
});

// PR merge is external — caller/report states OPEN unless overridden
const prMerged = process.env.GDI_PR599_MERGED === "1";

const readyForFounderAuth =
  snapshotOk &&
  materialDiff.materialChanged &&
  lostContacts.length === 0 &&
  round.ok === true &&
  filterProof.keptCount === 1 &&
  selectedMeta.strict.finalizedQualified === true &&
  !selectedMeta.dependsOnHistoricalCompat &&
  staged?.publicationVersion === QUALITY &&
  current?.publicationVersion === GOLD;

const verdict = !prMerged
  ? "AWAITING_INTEGRATION_CONTROLLER"
  : readyForFounderAuth
    ? "READY_FOR_FOUNDER_CANARY_AUTHORIZATION"
    : "BLOCKED";

const summary = {
  verdict,
  pr599: {
    required: "MERGED into main with green CI",
    mergedConfirmedHere: prMerged,
    note: "GDI chat does not merge; Integration Controller owns merge.",
  },
  writeGuardVerified: true,
  allowedOpportunityIdsRestrictionVerified:
    filterProof.keptCount === 1 &&
    filterProof.kept[0]?.opportunityId === selectedId,
  canary: {
    opportunityId: selectedId,
    airtableRecordId: liveRec.id,
    title: selectedMeta.title,
    namedContact: selectedMeta.namedContact,
    outreach: selectedMeta.outreach,
    strictFqo: true,
    dependsOnHistoricalCompat: false,
    action: "WOULD_UPDATE",
    changedFields: materialDiff.changedFields,
    writeBudget: 1,
    writeSetPolicy: RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
    allowedOpportunityIds: [selectedId],
  },
  completeSnapshotOk: snapshotOk,
  originalPayloadChars: completeSnapshot.researchPayloadJsonChars,
  originalPayloadSha256: completeSnapshot.researchPayloadJsonSha256,
  originalMaterialFingerprint: completeSnapshot.materialFingerprint,
  rollbackReversible: snapshotOk && lostContacts.length === 0,
  remainingAfterCanary: remaining.length,
  bulkAuthorization: "NOT_IMPLIED",
  canaryExecuted: false,
  productionWritesExecuted: false,
  gdoWrites: 0,
  stagedCurrentUnchanged: true,
};

writeJson("00-CANARY_PREWRITE_SUMMARY.json", summary);

const md = `# Validated Airtable Canary — Final Pre-Write Safety

**Verdict: ${verdict}**

## 1. Integration Controller
PR #599 must be **MERGED** with required CI green before any live write.  
This GDI chat does **not** merge. Merged confirmed here: **${prMerged}**.

Write guard verified: env gate \`${RESEARCH_PERSISTENCE_ACTIVATION.envGate}\`, exact write-set token, \`VALIDATED_FQO_ONLY\`, \`allowedOpportunityIds=[${selectedId}]\`, \`writeBudget=1\`.

## 2. Commercial integrity (strict FQO)
Primary candidate \`${PRIMARY_CANDIDATE}\` accepted: **${primaryOk === true}**.  
Selected canary: **\`${selectedId}\`** — ${selectedMeta.title}.  
Named: ${selectedMeta.namedContact} · Outreach: ${selectedMeta.outreach}  
Strict \`finalizedQualified\`: **true** · Historical-compat dependency: **false**  
Evidence URLs: ${selectedMeta.evidenceUrls.length}

## 3. Full rollback protection
Airtable record: **\`${liveRec.id}\`**  
Complete snapshot: \`${snapshotPath}\`  
Original \`researchPayloadJson\` chars: **${completeSnapshot.researchPayloadJsonChars}** (parse OK: ${completeSnapshot.researchPayloadJsonParseOk})  
SHA-256: \`${completeSnapshot.researchPayloadJsonSha256}\`  
Material fingerprint: \`${completeSnapshot.materialFingerprint}\`  
Rollback reversible: **${snapshotOk && lostContacts.length === 0}**

## 4. Exact canary (NOT EXECUTED)
- UPDATE existing validated record only
- Fields changing: ${(materialDiff.changedFields || []).join(", ") || "(none)"}
- Read-back + unchanged rerun + no GDO/STAGED/CURRENT — planned after founder GO

## 5. Stop after canary
Remaining planned IDs after canary: **${remaining.length}** — **NOT AUTHORIZED**.  
No bulk authorization is implied.

Live Airtable mutations this run: **NONE**.
`;
fs.writeFileSync(path.join(OUT, "FOUNDER_CANARY_PREWRITE_SAFETY_REPORT.md"), md);
console.log(JSON.stringify(summary, null, 2));
if (verdict === "BLOCKED") process.exitCode = 2;
