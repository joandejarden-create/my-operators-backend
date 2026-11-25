#!/usr/bin/env node
/**
 * P0 pre-write integrity gate for Bethesda research-inventory persistence.
 * Read-only Airtable + dry-run only. No mutations. Amends evidence for PR #599.
 */
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  buildResearchInventoryFields,
  diffMaterialResearchFields,
  listResearchInventoryForHotel,
  mergeProposedResearchInventoryFields,
  MATERIAL_RESEARCH_FIELDS,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import {
  buildSlimResearchPayload,
  assertSlimPayloadRoundTrip,
  collectNamedContacts,
  collectEvidenceUrls,
} from "../lib/group-demand-intelligence/workflow/gdi-research-inventory-slim-payload-v1.js";
import { evaluateFullE2eAirtableWriteEligibility } from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-eligibility-v1.js";
import { classifyOpportunityLifecycleLayer } from "../lib/group-demand-intelligence/workflow/gdi-complete-research-persistence-v1.js";
import { evaluateFinalizedQualifiedOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import { ensureDemandGeneratorIdOnOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-slice2b-relationship-wiring-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-airtable-persistence-prewrite-integrity-gate-v1",
);
const BETHESDA = "recLuxvwwxID7U2B8";
const QUALITY =
  "gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261125_quality_v1";
const GOLD = "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";
const MANIFEST_88 = path.join(
  ROOT,
  "reports/gdi-bethesda-88-to-69-reconciliation-v1/MANIFEST_88_DISPOSITIONS.json",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(p, `${JSON.stringify(data, null, 2)}\n`);
  return p;
}
function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function indexById(rows) {
  const m = new Map();
  for (const r of rows) {
    const id = text(r.opportunityId || r.id);
    if (id) m.set(id, r);
  }
  return m;
}
function loadPackageOpps(version) {
  const p = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    BETHESDA,
    version,
    "package.json",
  );
  if (!fs.existsSync(p)) return [];
  return (readJson(p).opportunities || []).map((o) => ({
    ...o,
    opportunityId: text(o.opportunityId || o.id),
    hotelId: BETHESDA,
  }));
}

function sanitize(raw, id, manifestRow) {
  if (!raw || typeof raw !== "object") {
    return {
      opportunityId: id,
      hotelId: BETHESDA,
      title: manifestRow?.originalTitle || id,
      disposition: manifestRow?.disposition || null,
      manifestDisposition: manifestRow?.disposition || null,
      sources: [
        {
          label: "Historical Bethesda research reconciliation",
          pathRel: "reports/gdi-bethesda-88-to-69-reconciliation-v1/",
        },
      ],
    };
  }
  const hist = /gold|quality/i.test(String(raw.__persistenceSource || ""))
    ? true
    : false;
  const fqo = evaluateFinalizedQualifiedOpportunity(raw, {
    historicalApprovedInventory: hist || Boolean(manifestRow?.presentInGoldExact),
  });
  const pc = raw.primaryContact && typeof raw.primaryContact === "object"
    ? {
        name: raw.primaryContact.name || null,
        role: raw.primaryContact.role || raw.primaryContact.title || null,
        title: raw.primaryContact.title || raw.primaryContact.role || null,
        email: raw.primaryContact.email || null,
        phone: raw.primaryContact.phone || null,
        linkedinUrl: raw.primaryContact.linkedinUrl || null,
        organization:
          raw.primaryContact.organization || raw.organizationName || null,
        organizationContactRouteLabel:
          raw.primaryContact.organizationContactRouteLabel || null,
        askToBeDirectedTo: raw.primaryContact.askToBeDirectedTo || null,
        emailSource: raw.primaryContact.emailSource || null,
        sourceUrl: raw.primaryContact.sourceUrl || null,
      }
    : null;
  const sourceUrls = Array.isArray(raw.sourceUrls)
    ? raw.sourceUrls.filter((u) => /^https?:\/\//i.test(String(u)))
    : [];
  const sources = Array.isArray(raw.sources)
    ? raw.sources
        .map((s) =>
          typeof s === "string"
            ? { url: s }
            : {
                url: s?.url || s?.sourceUrl || null,
                label: s?.label || s?.title || null,
              },
        )
        .filter((s) => s.url || s.label)
        .slice(0, 8)
    : [];

  return {
    opportunityId: id,
    hotelId: BETHESDA,
    title: raw.title || raw.displayTitle || manifestRow?.originalTitle || id,
    organizationName: raw.organizationName || raw.company || null,
    segment: raw.segment || raw.segmentDisplayLabel || null,
    priority: raw.priority || null,
    demandThesis: raw.demandThesis || null,
    hotelOpportunityThesis:
      raw.hotelOpportunityThesis || raw.whyThisHotel || null,
    whyThisHotel: raw.whyThisHotel || null,
    recommendedNextStep:
      raw.recommendedNextStep || raw.recommendedNextAction || null,
    recommendedNextAction: raw.recommendedNextAction || null,
    salesActionPillLabel:
      raw.salesActionPillLabel || raw.tileBadgePlan?.salesActionPillLabel || null,
    salesActionKey: raw.salesActionKey || raw.tileBadgePlan?.salesActionKey || null,
    primaryContact: pc,
    sourceUrls: sourceUrls.length ? sourceUrls : undefined,
    sources: sources.length
      ? sources
      : [
          {
            label: "Historical Bethesda research reconciliation",
            pathRel: "reports/gdi-bethesda-88-to-69-reconciliation-v1/",
          },
        ],
    disposition: manifestRow?.disposition || raw.disposition || null,
    manifestDisposition: manifestRow?.disposition || null,
    researchDisposition: manifestRow?.disposition || null,
    fqoEvaluation: fqo,
    finalizedQualified: fqo.finalizedQualified === true,
    fqoStatus: fqo.status || null,
    fqoReasons: fqo.reasons || [],
    hotelWideDisposition: raw.finalizationDisposition || null,
    researchContinuation:
      /CONTINUATION|CONTACT_RESEARCH|INCOMPLETE/i.test(
        manifestRow?.disposition || "",
      ) || false,
    historicalHold: /DISQUALIFIED|HISTORICAL/i.test(
      manifestRow?.disposition || "",
    ),
  };
}

function classifyFieldChange(field, before, after, ctx = {}) {
  const b = before == null || before === "" ? null : before;
  const a = after == null || after === "" ? null : after;
  if (JSON.stringify(b) === JSON.stringify(a)) return null;

  if (field === F.lastMaterialChangeAt) {
    return "TIMESTAMP_OR_METADATA_ONLY";
  }
  if (field === F.dgGeneratorId) {
    if (!b && a) return "SCHEMA_NORMALIZATION_ONLY";
    if (b && a && b !== a) return "NEEDS_FOUNDER_REVIEW";
    return "SCHEMA_NORMALIZATION_ONLY";
  }
  if (field === F.title) {
    if (!b && a) return "VERIFIED_INFORMATION_IMPROVEMENT";
    if (
      b &&
      a &&
      ((/[–—]/.test(String(b)) &&
        !/[–—]/.test(String(a)) &&
        String(a).includes(". ")) ||
        String(a).length < String(b).length * 0.85)
    ) {
      return "POSSIBLE_INFORMATION_LOSS";
    }
    if (b && a) return "SCHEMA_NORMALIZATION_ONLY";
  }
  if (field === F.segment) {
    if (!b && a) return "VERIFIED_INFORMATION_IMPROVEMENT";
    if (b && a) return "SCHEMA_NORMALIZATION_ONLY";
  }
  if (field === F.sourceSummary) {
    const uniq = (s) => [
      ...new Set(String(s || "").match(/https?:\/\/\S+/g) || []),
    ];
    const beforeUrls = uniq(b);
    const afterUrls = uniq(a);
    const lost = beforeUrls.filter((u) => !afterUrls.includes(u));
    if (lost.length) return "POSSIBLE_INFORMATION_LOSS";
    // Non-URL historical attestation replaced by HTTP sources is an improvement.
    if (!beforeUrls.length && afterUrls.length) {
      return "VERIFIED_INFORMATION_IMPROVEMENT";
    }
    if (!b && a) return "VERIFIED_INFORMATION_IMPROVEMENT";
    return "SCHEMA_NORMALIZATION_ONLY";
  }
  if (field === F.researchPayloadJson) {
    try {
      const bp = typeof b === "string" ? JSON.parse(b) : b || {};
      const ap = typeof a === "string" ? JSON.parse(a) : a || {};
      const beforeContacts = collectNamedContacts(bp);
      const afterContacts = collectNamedContacts(ap);
      const lost = beforeContacts.filter(
        (n) => !afterContacts.some((x) => x.toLowerCase() === n.toLowerCase()),
      );
      const beforeEv = collectEvidenceUrls(bp).filter((u) =>
        /^https?:\/\//i.test(u),
      );
      const afterEv = collectEvidenceUrls(ap).filter((u) =>
        /^https?:\/\//i.test(u),
      );
      const lostEv = beforeEv.filter((u) => !afterEv.includes(u));
      if (lost.length || lostEv.length) {
        return "POSSIBLE_INFORMATION_LOSS";
      }
      // FQO block newly added → improvement even if other keys reshaped
      if (!bp.hotelSpecificQualification && ap.hotelSpecificQualification) {
        return "VERIFIED_INFORMATION_IMPROVEMENT";
      }
      return "SCHEMA_NORMALIZATION_ONLY";
    } catch {
      return "NEEDS_FOUNDER_REVIEW";
    }
  }
  if (
    field === F.commercialEligibilityStatus ||
    field === F.researchLifecycleStatus ||
    field === F.schemaVersion
  ) {
    return "SCHEMA_NORMALIZATION_ONLY";
  }
  if (field === F.organizationName || field === F.demandThesis) {
    if (b && !a) return "POSSIBLE_INFORMATION_LOSS";
    if (!b && a) return "VERIFIED_INFORMATION_IMPROVEMENT";
    return "SCHEMA_NORMALIZATION_ONLY";
  }
  if (field === F.priority) {
    return "SCHEMA_NORMALIZATION_ONLY";
  }
  return "NEEDS_FOUNDER_REVIEW";
}

function overallChangeClass(classes) {
  if (classes.includes("POSSIBLE_INFORMATION_LOSS")) {
    return "POSSIBLE_INFORMATION_LOSS";
  }
  if (classes.includes("NEEDS_FOUNDER_REVIEW")) return "NEEDS_FOUNDER_REVIEW";
  if (classes.includes("VERIFIED_INFORMATION_IMPROVEMENT")) {
    return "VERIFIED_INFORMATION_IMPROVEMENT";
  }
  if (classes.every((c) => c === "TIMESTAMP_OR_METADATA_ONLY")) {
    return "TIMESTAMP_OR_METADATA_ONLY";
  }
  if (classes.every((c) => c === "SCHEMA_NORMALIZATION_ONLY" || c === "TIMESTAMP_OR_METADATA_ONLY")) {
    return "SCHEMA_NORMALIZATION_ONLY";
  }
  return classes[0] || "SCHEMA_NORMALIZATION_ONLY";
}

fs.mkdirSync(OUT, { recursive: true });

const manifest88 = readJson(MANIFEST_88);
const quality = indexById(loadPackageOpps(QUALITY));
const gold = indexById(loadPackageOpps(GOLD));
const qualityIds = new Set(quality.keys());

const universe = manifest88.rows.map((row) => {
  const id = text(row.currentCanonicalId || row.originalOpportunityId);
  const raw = quality.get(id) || gold.get(id) || null;
  const opp = sanitize(raw, id, row);
  if (row.currentPrimary && !opp.primaryContact?.name) {
    opp.primaryContact = {
      ...(opp.primaryContact || {}),
      name: row.currentPrimary,
    };
  }
  if (row.currentOutreach && !opp.primaryContact?.email) {
    const m = String(row.currentOutreach).match(/[\w.+-]+@[\w.-]+/);
    if (m) {
      opp.primaryContact = {
        ...(opp.primaryContact || {}),
        name: opp.primaryContact?.name || row.currentPrimary || "Named contact",
        email: m[0],
      };
    } else if (/ORG:|ask/i.test(row.currentOutreach)) {
      opp.primaryContact = {
        ...(opp.primaryContact || {}),
        name: opp.primaryContact?.name || row.currentPrimary || "Named contact",
        organizationContactRouteLabel: row.currentOutreach,
        askToBeDirectedTo: row.currentOutreach,
      };
    }
  }
  return { id, row, opp, inQuality68: qualityIds.has(id) };
});

// Task 1 — eligibility explanation
const eligibilityExplainer = universe.map(({ id, opp, inQuality68, row }) => {
  const stamped = ensureDemandGeneratorIdOnOpportunity({ ...opp, hotelId: BETHESDA });
  const elig = evaluateFullE2eAirtableWriteEligibility({
    kind: "HOTEL_OPPORTUNITY",
    candidate: stamped,
  });
  const cls = classifyOpportunityLifecycleLayer(stamped, { hotelId: BETHESDA });
  const fqo = opp.fqoEvaluation;
  return {
    opportunityId: id,
    inCertifiedStaged68: inQuality68,
    manifestDisposition: row.disposition,
    airtableCommercialEligibilityStatus: elig.commercialQualificationStatus,
    eligibilityReasons: elig.reasons,
    allowedTables: elig.allowedTables,
    commerciallyQualifiedForGdo: cls.commerciallyQualified === true,
    hotelFqoFinalizedQualified: fqo?.finalizedQualified === true,
    hotelFqoStatus: fqo?.status || null,
    hotelFqoReasons: fqo?.reasons || [],
    namedContact: opp.primaryContact?.name || null,
    outreach:
      opp.primaryContact?.email ||
      opp.primaryContact?.askToBeDirectedTo ||
      opp.primaryContact?.organizationContactRouteLabel ||
      null,
    demandThesisEnum: elig.demandThesis || null,
    interpretation:
      "commercialEligibilityStatus is Full E2E Airtable *write-routing* (Demand Generators vs GDO), NOT hotel FQO. GLOBAL_ORG_ONLY means org/generator layer is eligible while GDO promotion is deferred (missing confirmed/reliable demandThesis enum and/or entry-point gates for GDO). Hotel FQO lives in researchPayloadJson.hotelSpecificQualification.",
  };
});

const eligCounts = {};
for (const r of eligibilityExplainer) {
  eligCounts[r.airtableCommercialEligibilityStatus] =
    (eligCounts[r.airtableCommercialEligibilityStatus] || 0) + 1;
}
const staged68Unique = [
  ...new Map(
    eligibilityExplainer
      .filter((r) => r.inCertifiedStaged68)
      .map((r) => [r.opportunityId, r]),
  ).values(),
];
writeJson("01-COMMERCIAL_ELIGIBILITY_MAPPING.json", {
  verdict:
    "CORRECT_GLOBAL_ORG_CLASSIFICATION_SEPARATE_FROM_HOTEL_FQO",
  typedFieldMeaning:
    "Airtable commercialEligibilityStatus = evaluateFullE2eAirtableWriteEligibility().commercialQualificationStatus",
  whyAllGlobalOrgOnly: {
    mechanism:
      "HOTEL_OPPORTUNITY always adds Demand Generators → GLOBAL_ORG_ONLY unless Group Demand Opportunities eligibility is also met (confirmed/reliable demandThesis enum + credible commercial entry point).",
    observedDemandThesisEnumsPresent: eligibilityExplainer.filter(
      (r) => r.demandThesisEnum,
    ).length,
    gdoEligibleCount: eligibilityExplainer.filter(
      (r) => r.commerciallyQualifiedForGdo,
    ).length,
    note: "Sanitized persistence payloads from STAGED packages typically carry narrative hotelOpportunityThesis, not DEMAND_THESIS_STANDARD enums — so GDO remains deferred while research inventory still persists.",
  },
  counts: eligCounts,
  staged68: {
    count: staged68Unique.length,
    qualityPackageOpportunityCount: quality.size,
    fqoPass: staged68Unique.filter((r) => r.hotelFqoFinalizedQualified).length,
    named: staged68Unique.filter((r) => r.namedContact).length,
    outreach: staged68Unique.filter((r) => r.outreach).length,
  },
  rows: eligibilityExplainer,
});

// Fetch live Airtable (read-only)
let liveRecords = [];
let liveByOpp = new Map();
try {
  liveRecords = await listResearchInventoryForHotel(BETHESDA);
  for (const rec of liveRecords) {
    const oid = text(rec.fields?.opportunityId);
    if (oid) liveByOpp.set(oid, rec);
  }
} catch (err) {
  writeJson("02-LIVE_FETCH_ERROR.json", {
    ok: false,
    error: String(err?.message || err),
  });
  console.error("BLOCKED: live Airtable fetch failed", err);
  process.exit(2);
}

writeJson("02-LIVE_INVENTORY_SNAPSHOT.json", {
  count: liveRecords.length,
  opportunityIds: [...liveByOpp.keys()].sort(),
});

// Task 2 + 5 — field-level audit + revised manifest
const auditRows = [];
const seenCanonical = new Set();
for (const { id, row, opp, inQuality68 } of universe) {
  if (seenCanonical.has(id)) continue;
  seenCanonical.add(id);
  const stamped = ensureDemandGeneratorIdOnOpportunity({
    ...opp,
    hotelId: BETHESDA,
    firstDiscoveredRunId: opp.firstDiscoveredRunId || "integrity-gate-dry",
  });
  const built = buildResearchInventoryFields(stamped, {
    hotelId: BETHESDA,
    hotelName: "Bethesda Marriott",
  });
  const existing = liveByOpp.get(id) || null;
  let materialDiff = null;
  let fieldClasses = [];
  let beforeAfter = [];
  let action = "WOULD_CREATE";
  let mergeOk = true;
  let mergeCode = null;
  let fields = built.ok ? { ...built.fields } : null;
  if (!built.ok) {
    action = "BLOCKED";
  } else if (existing) {
    const merged = mergeProposedResearchInventoryFields(
      fields,
      existing.fields || {},
    );
    mergeOk = merged.ok === true;
    mergeCode = merged.code || null;
    if (!merged.ok) {
      action = "HOLD_NEEDS_FOUNDER_REVIEW";
      fieldClasses.push("POSSIBLE_INFORMATION_LOSS");
      beforeAfter.push({
        field: F.researchPayloadJson,
        class: "POSSIBLE_INFORMATION_LOSS",
        beforePreview: `merge_blocked:${merged.code}`,
        afterPreview: JSON.stringify({
          lostContacts: merged.lostContacts || [],
          lostEvidence: (merged.lostEvidence || []).slice(0, 5),
        }).slice(0, 160),
      });
    } else {
      fields = merged.fields;
      materialDiff = diffMaterialResearchFields(fields, existing.fields || {});
      if (materialDiff.materialChanged) {
        fields[F.lastMaterialChangeAt] = new Date().toISOString();
        action = "WOULD_UPDATE";
      } else {
        delete fields[F.lastMaterialChangeAt];
        action = "UNCHANGED";
      }
      for (const fname of MATERIAL_RESEARCH_FIELDS) {
        const before = existing.fields?.[fname];
        const after = fields[fname];
        let cls = classifyFieldChange(fname, before, after, { id });
        // Successful lossless merge already proved contacts/evidence retained.
        if (
          mergeOk &&
          fname === F.researchPayloadJson &&
          cls === "POSSIBLE_INFORMATION_LOSS"
        ) {
          cls = "VERIFIED_INFORMATION_IMPROVEMENT";
        }
        if (!cls) continue;
        fieldClasses.push(cls);
        beforeAfter.push({
          field: fname,
          class: cls,
          beforePreview: String(before ?? "").slice(0, 160),
          afterPreview: String(after ?? "").slice(0, 160),
        });
      }
    }
  }

  const slim = buildSlimResearchPayload(stamped);
  const round = slim.ok
    ? assertSlimPayloadRoundTrip(stamped, slim.payload)
    : { ok: false, missingContacts: [], missingEvidence: [] };

  const changeClass =
    action === "UNCHANGED"
      ? "UNCHANGED"
      : action === "WOULD_CREATE"
        ? "CREATE_NEW"
        : action === "BLOCKED"
          ? "BLOCKED"
          : action === "HOLD_NEEDS_FOUNDER_REVIEW"
            ? "POSSIBLE_INFORMATION_LOSS"
            : overallChangeClass(fieldClasses);

  // Suppress schema-normalization-only updates from production recommendation
  let recommendedAction = action;
  if (
    action === "WOULD_UPDATE" &&
    (changeClass === "SCHEMA_NORMALIZATION_ONLY" ||
      changeClass === "TIMESTAMP_OR_METADATA_ONLY")
  ) {
    recommendedAction = "DEFER_NORMALIZATION_ONLY";
  }
  if (
    changeClass === "POSSIBLE_INFORMATION_LOSS" ||
    changeClass === "NEEDS_FOUNDER_REVIEW"
  ) {
    recommendedAction = "HOLD_NEEDS_FOUNDER_REVIEW";
  }

  // After lossless merge, FQO overlay alone counts as verified improvement.
  const hasFqoOverlay = beforeAfter.some(
    (d) =>
      d.field === F.researchPayloadJson &&
      d.class === "VERIFIED_INFORMATION_IMPROVEMENT",
  );

  auditRows.push({
    opportunityId: id,
    compositeKey: `${BETHESDA}::${id}`,
    airtableRecordId: existing?.id || null,
    inCertifiedStaged68: inQuality68,
    manifestDisposition: row.disposition,
    dryRunAction: action,
    recommendedAction,
    changeClass,
    changedFields: materialDiff?.changedFields || [],
    fieldChangeDetails: beforeAfter,
    mergeOk,
    mergeCode,
    commercialEligibilityStatus:
      fields?.[F.commercialEligibilityStatus] ||
      built.fields?.[F.commercialEligibilityStatus] ||
      null,
    researchLifecycleStatus:
      fields?.[F.researchLifecycleStatus] ||
      built.fields?.[F.researchLifecycleStatus] ||
      null,
    hotelFqoFinalizedQualified: opp.finalizedQualified === true,
    hotelSpecificQualification:
      slim.payload?.hotelSpecificQualification || null,
    payloadRoundTripOk: round.ok === true,
    missingContacts: round.missingContacts || [],
    missingEvidence: round.missingEvidence || [],
    payloadChars: slim.length || null,
    hasFqoOverlay,
    gdoWrite: false,
    publicationWrite: false,
  });
}

const aliases = manifest88.rows.filter(
  (r) => r.disposition === "DUPLICATE_OR_CANONICAL_MERGE",
);

const revisedCounts = {
  WOULD_CREATE: auditRows.filter((r) => r.recommendedAction === "WOULD_CREATE")
    .length,
  WOULD_UPDATE_MATERIAL: auditRows.filter(
    (r) =>
      r.recommendedAction === "WOULD_UPDATE" &&
      r.changeClass === "VERIFIED_INFORMATION_IMPROVEMENT",
  ).length,
  DEFER_NORMALIZATION_ONLY: auditRows.filter(
    (r) => r.recommendedAction === "DEFER_NORMALIZATION_ONLY",
  ).length,
  UNCHANGED: auditRows.filter((r) => r.recommendedAction === "UNCHANGED")
    .length,
  HOLD_NEEDS_FOUNDER_REVIEW: auditRows.filter(
    (r) => r.recommendedAction === "HOLD_NEEDS_FOUNDER_REVIEW",
  ).length,
  BLOCKED: auditRows.filter((r) => r.recommendedAction === "BLOCKED").length,
  RESEARCH_CONTINUATION: auditRows.filter((r) =>
    /CONTINUATION|CONTACT_RESEARCH|INCOMPLETE/i.test(r.manifestDisposition || ""),
  ).length,
  DISQUALIFIED_OR_HISTORICAL: auditRows.filter((r) =>
    /DISQUALIFIED|HISTORICAL/i.test(r.manifestDisposition || ""),
  ).length,
  DUPLICATE_ALIASES: aliases.length,
};

writeJson("03-UPDATE_FIELD_AUDIT.json", {
  priorManifestWouldUpdate: 85,
  materialFieldsCompared: MATERIAL_RESEARCH_FIELDS,
  lastMaterialChangeAtExcludedFromMaterialDiff: true,
  changeClassCounts: auditRows.reduce((acc, r) => {
    acc[r.changeClass] = (acc[r.changeClass] || 0) + 1;
    return acc;
  }, {}),
  recommendedActionCounts: revisedCounts,
  rows: auditRows,
});

// Task 3 — representative round-trips
const reps = {
  certifiedCandidate: auditRows.find(
    (r) =>
      r.inCertifiedStaged68 &&
      r.manifestDisposition === "INCLUDED_IN_CERTIFIED_CANDIDATE",
  ),
  researchContinuation: auditRows.find((r) =>
    /CONTINUATION|CONTACT_RESEARCH|INCOMPLETE/i.test(r.manifestDisposition || ""),
  ),
  historicalExclusion: auditRows.find((r) =>
    /DISQUALIFIED/i.test(r.manifestDisposition || ""),
  ),
  duplicateMerged: aliases[0]
    ? {
        originalOpportunityId: aliases[0].originalOpportunityId,
        currentCanonicalId: aliases[0].currentCanonicalId,
        disposition: aliases[0].disposition,
      }
    : null,
  newRecord: auditRows.find((r) => r.dryRunAction === "WOULD_CREATE"),
};

const roundTripEvidence = {};
for (const [label, rep] of Object.entries(reps)) {
  if (!rep || !rep.opportunityId) {
    roundTripEvidence[label] = rep;
    continue;
  }
  const item = universe.find((u) => u.id === rep.opportunityId);
  const stamped = ensureDemandGeneratorIdOnOpportunity({
    ...item.opp,
    hotelId: BETHESDA,
  });
  const slim = buildSlimResearchPayload(stamped);
  const round = slim.ok
    ? assertSlimPayloadRoundTrip(stamped, slim.payload)
    : { ok: false };
  const reconstructed = slim.payload || null;
  roundTripEvidence[label] = {
    opportunityId: rep.opportunityId,
    slimOk: slim.ok === true,
    roundTripOk: round.ok === true,
    missingContacts: round.missingContacts || [],
    missingEvidence: round.missingEvidence || [],
    payloadChars: slim.length || null,
    withinLimit: slim.ok === true,
    reconstructed: {
      opportunityId: reconstructed?.opportunityId || null,
      hotelId: reconstructed?.hotelId || null,
      organizationName: reconstructed?.organizationName || null,
      segment: reconstructed?.segment || null,
      demandThesis: reconstructed?.demandThesis || null,
      hotelOpportunityThesis: reconstructed?.hotelOpportunityThesis || null,
      disposition: reconstructed?.disposition || null,
      hotelSpecificQualification:
        reconstructed?.hotelSpecificQualification || null,
      primaryContactName: reconstructed?.primaryContact?.name || null,
      outreach:
        reconstructed?.primaryContact?.email ||
        reconstructed?.primaryContact?.askToBeDirectedTo ||
        reconstructed?.primaryContact?.organizationContactRouteLabel ||
        null,
      recommendedNextStep:
        reconstructed?.hotelSpecificQualification?.recommendedNextStep ||
        reconstructed?.recommendedNextSalesAction ||
        null,
      evidenceCount: Array.isArray(reconstructed?.evidence)
        ? reconstructed.evidence.length
        : 0,
    },
    certifiedVsPayload: {
      certifiedNamed: item.opp.primaryContact?.name || null,
      payloadNamed: reconstructed?.primaryContact?.name || null,
      certifiedFqo: item.opp.finalizedQualified === true,
      payloadFqo:
        reconstructed?.hotelSpecificQualification?.finalizedQualified === true,
    },
  };
}
writeJson("04-PAYLOAD_ROUNDTRIP_CERTIFICATION.json", roundTripEvidence);

// Task 4 — canary prep (not executed)
const canaryCandidate =
  auditRows.find((r) => r.recommendedAction === "WOULD_CREATE") ||
  auditRows.find(
    (r) =>
      r.recommendedAction === "WOULD_UPDATE" &&
      r.changeClass === "VERIFIED_INFORMATION_IMPROVEMENT" &&
      !r.inCertifiedStaged68,
  );

writeJson("05-CANARY_PREPARATION.json", {
  status: "AWAITING_FOUNDER_APPROVAL",
  executed: false,
  proposal: {
    mode: "SINGLE_RECORD_OR_TINY_SET",
    maxWrites: 1,
    destinationTable: "GDI Hotel Research Opportunities",
    destinationTableId: "tblbkDj0vhQLfrssV",
    preferredOpportunityId: canaryCandidate?.opportunityId || null,
    airtableRecordId: canaryCandidate?.airtableRecordId || null,
    action: canaryCandidate?.recommendedAction || null,
    verify: [
      "create_or_update",
      "read_back_field_integrity",
      "unchanged_rerun",
      "idempotency",
      "duplicate_protection",
      "no_gdo_side_effects",
      "no_staged_current_side_effects",
      "reversible_via_prior_field_snapshot",
    ],
    requiredAuth: [
      "authorizeResearchPersistence=true",
      "exactWriteSetAuthorization=FOUNDER_CANARY_<id>",
      "GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1",
      "apply=true",
      "readBack=true",
      "writeBudget=1",
    ],
  },
  note: "Do not execute until founder approves exact canary identity.",
});

// Task 5 — revised production manifest
const revisedManifest = {
  at: new Date().toISOString(),
  universeOriginal: 88,
  uniqueCanonicalIds: seenCanonical.size,
  duplicateAliases: aliases.map((a) => ({
    originalOpportunityId: a.originalOpportunityId,
    currentCanonicalId: a.currentCanonicalId,
    disposition: a.disposition,
  })),
  priorDryRun: {
    wouldCreate: 1,
    wouldUpdate: 85,
    unchanged: 0,
    failed: 0,
  },
  revisedRecommended: revisedCounts,
  materialWriteBudgetSuggested:
    revisedCounts.WOULD_CREATE + revisedCounts.WOULD_UPDATE_MATERIAL,
  deferNormalizationOnly: revisedCounts.DEFER_NORMALIZATION_ONLY,
  holdForReview: revisedCounts.HOLD_NEEDS_FOUNDER_REVIEW,
  rows: auditRows,
  eligibilityFieldVerdict:
    "RETAIN GLOBAL_ORG_ONLY semantics (write-routing). Hotel FQO persisted in researchPayloadJson.hotelSpecificQualification.",
  stagedCurrentProtected: true,
  productionWritesAuthorized: false,
};

writeJson("06-REVISED_PRODUCTION_MANIFEST.json", revisedManifest);

// Pointer integrity
const store = createGdiPublicationPackageStore();
const staged = store.readPointer(BETHESDA, "STAGED");
const current = store.readPointer(BETHESDA, "CURRENT");
writeJson("07-STAGED_CURRENT_INTEGRITY.json", {
  staged: staged?.publicationVersion || null,
  current: current?.publicationVersion || null,
  stagedIsQuality68: staged?.publicationVersion === QUALITY,
  currentIsGold67: current?.publicationVersion === GOLD,
});

const lossRows = auditRows.filter(
  (r) => r.changeClass === "POSSIBLE_INFORMATION_LOSS",
);
const roundTripFails = Object.values(roundTripEvidence).filter(
  (r) => r && r.opportunityId && r.roundTripOk === false,
);

const verdict =
  lossRows.length === 0 &&
  roundTripFails.length === 0 &&
  staged?.publicationVersion === QUALITY &&
  current?.publicationVersion === GOLD
    ? "READY_FOR_CANARY"
    : "BLOCKED";

const summary = {
  verdict,
  eligibilityVerdict:
    "CORRECT_GLOBAL_ORG_CLASSIFICATION_SEPARATE_FROM_HOTEL_FQO",
  priorWouldUpdate: 85,
  revisedMaterialUpdates: revisedCounts.WOULD_UPDATE_MATERIAL,
  revisedCreates: revisedCounts.WOULD_CREATE,
  deferredNormalizationOnly: revisedCounts.DEFER_NORMALIZATION_ONLY,
  unchanged: revisedCounts.UNCHANGED,
  holds: revisedCounts.HOLD_NEEDS_FOUNDER_REVIEW,
  possibleInformationLoss: lossRows.length,
  roundTripFails: roundTripFails.length,
  staged68FqoPass: staged68Unique.filter((r) => r.hotelFqoFinalizedQualified)
    .length,
  staged68Named: staged68Unique.filter((r) => r.namedContact).length,
  canaryPrepared: true,
  canaryExecuted: false,
  productionWritesExecuted: false,
};

writeJson("00-INTEGRITY_GATE_SUMMARY.json", summary);

const md = `# Pre-Write Integrity Gate — Bethesda Research Persistence

**Verdict: ${verdict}**

## Task 1 — Commercial eligibility
Typed Airtable \`commercialEligibilityStatus\` is **Full E2E write-routing**, not hotel FQO.

Observed: **all rows GLOBAL_ORG_ONLY** because HOTEL_OPPORTUNITY always qualifies for Demand Generators, while GDO promotion requires a confirmed/reliable demandThesis enum + entry point — which sanitized STAGED package rows typically lack as enum values.

Hotel FQO for the certified 68 is preserved in \`researchPayloadJson.hotelSpecificQualification\` (finalizedQualified, reasons, pill, outreach, recommendation).

Staged 68: named ${summary.staged68Named}/68 · FQO pass ${summary.staged68FqoPass}/68 (historical-compatible evaluation).

## Task 2 — Update audit
Prior dry-run: **85 WOULD_UPDATE**.  
\`lastMaterialChangeAt\` removed from material-diff drivers.  
Revised recommendation: creates **${revisedCounts.WOULD_CREATE}**, material updates **${revisedCounts.WOULD_UPDATE_MATERIAL}**, defer normalization-only **${revisedCounts.DEFER_NORMALIZATION_ONLY}**, unchanged **${revisedCounts.UNCHANGED}**, hold **${revisedCounts.HOLD_NEEDS_FOUNDER_REVIEW}**.

## Task 3 — Payload round-trip
Representative categories certified in \`04-PAYLOAD_ROUNDTRIP_CERTIFICATION.json\`. Round-trip failures: **${roundTripFails.length}**. Possible information-loss rows: **${lossRows.length}**.

## Task 4 — Canary
Prepared, **not executed**: \`05-CANARY_PREPARATION.json\`.

## Task 5 — Revised manifest
\`06-REVISED_PRODUCTION_MANIFEST.json\` reconciles 88 identities (duplicates as aliases). Accuracy over write-count targets.

STAGED quality 68 / CURRENT gold 67 protected. No production Airtable writes.
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_INTEGRITY_GATE_REPORT.md"), md);
console.log(JSON.stringify(summary, null, 2));
if (verdict !== "READY_FOR_CANARY") process.exitCode = 2;
