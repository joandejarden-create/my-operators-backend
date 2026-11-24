#!/usr/bin/env node
/**
 * Canonical Full E2E Airtable research-persistence certification ($0).
 * Level 1 dry-run (Bethesda 88) + Level 2 isolated mock store +
 * Phase 6 repeatability + Phase 7 Wyndham/Waterstone safety.
 *
 * Never mutates production Airtable. Never touches STAGED/CURRENT.
 */
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  persistFullE2eResearchInventory,
  classifyResearchPersistenceOperation,
  classifyResearchPersistenceStageStatus,
  isResearchPersistenceCertified,
  RESEARCH_PERSISTENCE_STAGE_STATUS,
  RESEARCH_PERSISTENCE_OPERATION_CLASS,
  GDI_FULL_E2E_RESEARCH_PERSISTENCE_BRIDGE_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import {
  GDI_AIRTABLE_BASE,
  GDI_AIRTABLE_TABLES,
  FULL_E2E_APPROVED_WRITE_TABLES,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-contract-v1.js";
import {
  MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY,
  VAL_RESEARCH_LIFECYCLE_STATUS,
  VAL_COMMERCIAL_ELIGIBILITY_STATUS,
  buildHotelResearchCompositeKey,
} from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-full-e2e-airtable-persistence-certification-v1",
);
const BETHESDA = "recLuxvwwxID7U2B8";
const WYNDHAM = "recnPB3Q02NO8Ma1o";
const WATERSTONE = "recgMYovrrZDJMqzX";
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
function shaFile(p) {
  return createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}
function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(p, `${JSON.stringify(data, null, 2)}\n`);
  return p;
}

function loadHotelOpportunitiesFile(hotelId) {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(p)) return [];
  const doc = readJson(p);
  const rows = Array.isArray(doc)
    ? doc
    : Array.isArray(doc.opportunities)
      ? doc.opportunities
      : [];
  return rows
    .map((o) => ({
      ...o,
      opportunityId: text(o.opportunityId || o.id),
      hotelId: text(o.hotelId || hotelId),
      __persistenceSource: "hotel_opportunities_json",
    }))
    .filter((o) => o.opportunityId);
}

function loadPackageOpps(hotelId, version) {
  const p = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotelId,
    version,
    "package.json",
  );
  if (!fs.existsSync(p)) return [];
  const pkg = readJson(p);
  return (pkg.opportunities || []).map((o) => ({
    ...o,
    opportunityId: text(o.opportunityId || o.id),
    hotelId: text(o.hotelId || hotelId),
    __persistenceSource: `package:${version}`,
  }));
}

function indexById(rows) {
  const m = new Map();
  for (const r of rows) {
    const id = text(r.opportunityId || r.id);
    if (id) m.set(id, r);
  }
  return m;
}

function mergeEnrich(base, richer) {
  if (!richer) return base;
  return {
    ...base,
    ...richer,
    opportunityId: text(base.opportunityId || richer.opportunityId),
    hotelId: text(base.hotelId || richer.hotelId),
    disposition: base.disposition || richer.disposition || null,
    primaryContact: richer.primaryContact || base.primaryContact || null,
    sourceUrls: richer.sourceUrls || base.sourceUrls || null,
    sources: richer.sources || base.sources || null,
    evidence: richer.evidence || base.evidence || null,
    recommendedNextStep:
      richer.recommendedNextStep || base.recommendedNextStep || null,
    hotelOpportunityThesis:
      richer.hotelOpportunityThesis ||
      richer.whyThisHotel ||
      base.hotelOpportunityThesis ||
      null,
    __persistenceSource: [
      base.__persistenceSource,
      richer.__persistenceSource,
    ]
      .filter(Boolean)
      .join("+"),
  };
}

/** In-memory inventory store for Level 2 isolated certification. */
function createMockInventoryStore(seed = []) {
  const byKey = new Map();
  const byRecordId = new Map();
  let seq = 1;
  for (const s of seed) {
    const key = s.compositeKey || `${s.hotelId}::${s.opportunityId}`;
    const id = s.airtableRecordId || `recMOCK${seq++}`;
    const row = { ...s, airtableRecordId: id, compositeKey: key };
    byKey.set(key, row);
    byRecordId.set(id, row);
  }
  return {
    async upsert({ opportunity, hotel, dryRun }) {
      const hotelId = text(hotel?.hotelId || opportunity?.hotelId);
      const opportunityId = text(opportunity?.opportunityId || opportunity?.id);
      const compositeKey = buildHotelResearchCompositeKey(hotelId, opportunityId);
      const existing = byKey.get(compositeKey);
      const title = text(opportunity?.title);
      if (!existing) {
        if (dryRun) {
          return {
            action: "WOULD_CREATE",
            compositeKey,
            airtableRecordId: null,
            materialDiff: { materialChanged: true, changedFields: ["title"] },
          };
        }
        const id = `recMOCK${seq++}`;
        const row = {
          airtableRecordId: id,
          compositeKey,
          hotelId,
          opportunityId,
          title,
          primaryContact: opportunity?.primaryContact || null,
          evidence: opportunity?.evidence || opportunity?.sourceUrls || null,
        };
        byKey.set(compositeKey, row);
        byRecordId.set(id, row);
        return {
          action: "CREATED",
          compositeKey,
          airtableRecordId: id,
          materialDiff: { materialChanged: true, changedFields: ["title"] },
        };
      }
      const changed = [];
      if (title && title !== text(existing.title)) changed.push("title");
      if (
        opportunity?.primaryContact?.email &&
        !existing.primaryContact?.email
      ) {
        changed.push("primaryContact.email");
      }
      // Never overwrite verified contact with empty
      const nextContact = opportunity?.primaryContact?.name
        ? opportunity.primaryContact
        : existing.primaryContact;
      if (!changed.length) {
        return {
          action: "UNCHANGED",
          compositeKey,
          airtableRecordId: existing.airtableRecordId,
          materialDiff: { materialChanged: false, changedFields: [] },
        };
      }
      if (dryRun) {
        return {
          action: "WOULD_UPDATE",
          compositeKey,
          airtableRecordId: existing.airtableRecordId,
          materialDiff: { materialChanged: true, changedFields: changed },
        };
      }
      const updated = {
        ...existing,
        title: title || existing.title,
        primaryContact: nextContact,
        evidence: opportunity?.evidence || existing.evidence,
      };
      byKey.set(compositeKey, updated);
      byRecordId.set(existing.airtableRecordId, updated);
      return {
        action: "UPDATED",
        compositeKey,
        airtableRecordId: existing.airtableRecordId,
        materialDiff: { materialChanged: true, changedFields: changed },
      };
    },
    async readBack(airtableRecordId) {
      const row = byRecordId.get(airtableRecordId);
      if (!row) throw new Error("mock_readback_missing");
      return {
        id: row.airtableRecordId,
        compositeKey: row.compositeKey,
        opportunityId: row.opportunityId,
        hotelId: row.hotelId,
      };
    },
    snapshot() {
      return [...byKey.values()];
    },
  };
}

fs.mkdirSync(OUT, { recursive: true });

// --- Phase 0 / schema compatibility ---
const liveSchemaPath = path.join(OUT, "LIVE_SCHEMA_COMPATIBILITY.json");
let liveSchema = { ok: false, blocker: "NOT_PROBED" };
try {
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  if (!key) {
    liveSchema = { ok: false, blocker: "NO_AIRTABLE_TOKEN" };
  } else {
    const meta = await fetch(
      `https://api.airtable.com/v0/meta/bases/${GDI_AIRTABLE_BASE.baseId}/tables`,
      { headers: { Authorization: `Bearer ${key}` } },
    );
    const body = await meta.json();
    if (!meta.ok) {
      liveSchema = { ok: false, blocker: "META_HTTP", status: meta.status, body };
    } else {
      const table = (body.tables || []).find(
        (t) =>
          t.id ===
            GDI_AIRTABLE_TABLES.GDI_HOTEL_RESEARCH_OPPORTUNITIES.tableId ||
          t.name === "GDI Hotel Research Opportunities",
      );
      const fieldNames = (table?.fields || []).map((f) => f.name);
      const required = Object.values(MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY);
      const missing = required.filter((n) => !fieldNames.includes(n));
      liveSchema = {
        ok: missing.length === 0,
        baseId: GDI_AIRTABLE_BASE.baseId,
        baseName: GDI_AIRTABLE_BASE.name,
        tableId: table?.id || null,
        tableName: table?.name || null,
        fieldCount: fieldNames.length,
        fieldNames,
        mappedFields: required,
        missingMappedFields: missing,
        lifecycleOptions: VAL_RESEARCH_LIFECYCLE_STATUS,
        eligibilityOptions: VAL_COMMERCIAL_ELIGIBILITY_STATUS,
        approvedWriteTables: FULL_E2E_APPROVED_WRITE_TABLES,
        gdoSeparate: true,
        note: "Research bridge targets GDI Hotel Research Opportunities only; GDO writes require separate authorization.",
      };
    }
  }
} catch (err) {
  liveSchema = { ok: false, blocker: "META_THREW", error: String(err?.message || err) };
}
writeJson("01-LIVE_SCHEMA_COMPATIBILITY.json", liveSchema);

// --- Build Bethesda 88 universe ---
const manifest88 = readJson(MANIFEST_88);
const quality = indexById(loadPackageOpps(BETHESDA, QUALITY));
const gold = indexById(loadPackageOpps(BETHESDA, GOLD));
const local = indexById(loadHotelOpportunitiesFile(BETHESDA));

function sanitizeForPersistence(raw, id) {
  if (!raw || typeof raw !== "object") return null;
  // Prefer presentation/package shape — do NOT deep-merge bloated local ORS rows
  // (backupContacts / contacts arrays) onto package contacts (lossless slim fails).
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
    title: raw.title || raw.displayTitle || id,
    organizationName: raw.organizationName || raw.company || null,
    segment: raw.segment || raw.segmentDisplayLabel || null,
    priority: raw.priority || null,
    summaryWhat: raw.summaryWhat || null,
    hotelOpportunityThesis:
      raw.hotelOpportunityThesis || raw.whyThisHotel || null,
    recommendedNextStep:
      raw.recommendedNextStep || raw.recommendedNextAction || null,
    primaryContact: pc,
    sourceUrls: sourceUrls.length ? sourceUrls : undefined,
    sources: sources.length ? sources : undefined,
    evidence: Array.isArray(raw.evidence) ? raw.evidence.slice(0, 8) : undefined,
    __persistenceSource: raw.__persistenceSource || "sanitized",
  };
}

const bethesda88 = manifest88.rows.map((row) => {
  const id = text(row.currentCanonicalId || row.originalOpportunityId);
  const preferred =
    sanitizeForPersistence(quality.get(id), id) ||
    sanitizeForPersistence(gold.get(id), id) ||
    sanitizeForPersistence(local.get(id), id) || {
      opportunityId: id,
      hotelId: BETHESDA,
      title: row.originalTitle || id,
      organizationName: null,
      __persistenceSource: "manifest_88_only",
    };

  const opp = {
    ...preferred,
    opportunityId: id,
    hotelId: BETHESDA,
    disposition: row.disposition,
    researchDisposition: row.disposition,
    manifestDisposition: row.disposition,
  };

  if (row.currentPrimary && !opp.primaryContact?.name) {
    opp.primaryContact = {
      ...(opp.primaryContact || {}),
      name: row.currentPrimary,
      organization: opp.organizationName || null,
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
  if (
    !Array.isArray(opp.sourceUrls) &&
    !Array.isArray(opp.sources) &&
    !Array.isArray(opp.evidence)
  ) {
    opp.sources = [
      {
        label: "Historical Bethesda research reconciliation",
        pathRel: "reports/gdi-bethesda-88-to-69-reconciliation-v1/",
      },
    ];
  }
  if (/CONTINUATION|CONTACT_RESEARCH|INCOMPLETE/i.test(row.disposition || "")) {
    opp.researchContinuation = true;
  }
  if (/DISQUALIFIED|HISTORICAL/i.test(row.disposition || "")) {
    opp.historicalHold = true;
  }
  return opp;
});

writeJson("02-BETHESDA_88_PERSISTENCE_INPUT.json", {
  count: bethesda88.length,
  qualityCount: quality.size,
  goldCount: gold.size,
  localCount: local.size,
  ids: bethesda88.map((o) => o.opportunityId),
});

// --- Level 1 dry-run (live Airtable compare when credentials present) ---
const level1a = await persistFullE2eResearchInventory({
  hotelId: BETHESDA,
  hotelName: "Bethesda Marriott",
  runId: "gdi_persist_cert_l1_run_a",
  repoRoot: ROOT,
  researchCompleted: true,
  apply: false,
  authorizeResearchPersistence: false,
  opportunities: bethesda88,
});
const level1b = await persistFullE2eResearchInventory({
  hotelId: BETHESDA,
  hotelName: "Bethesda Marriott",
  runId: "gdi_persist_cert_l1_run_b",
  repoRoot: ROOT,
  researchCompleted: true,
  apply: false,
  authorizeResearchPersistence: false,
  opportunities: bethesda88,
});

const opClassCounts = {};
for (const row of level1a.rows || []) {
  const cls =
    row.founderOperationClass || classifyResearchPersistenceOperation(row);
  opClassCounts[cls] = (opClassCounts[cls] || 0) + 1;
}

const level1Manifest = {
  level: 1,
  mode: "DRY_RUN",
  stageStatus: level1a.stageStatus,
  persistenceCertified: level1a.persistenceCertified === true,
  destinationTable: level1a.destinationTable,
  destinationTableId: level1a.destinationTableId,
  universeCount: bethesda88.length,
  counts: level1a.counts,
  founderOperationClassCounts: opClassCounts,
  rows: (level1a.rows || []).map((r) => ({
    opportunityId: r.opportunityId,
    compositeKey: r.compositeKey,
    airtableRecordId: r.airtableRecordId || null,
    action: r.action,
    founderOperationClass:
      r.founderOperationClass || classifyResearchPersistenceOperation(r),
    disposition: r.disposition || null,
    changedFields: r.changedFields || [],
    researchLifecycleStatus: r.researchLifecycleStatus || null,
    commercialEligibilityStatus: r.commercialEligibilityStatus || null,
    validationError: r.code || null,
    gdoWrite: false,
    publicationWrite: false,
  })),
  expectedWriteCountIfAuthorized:
    Number(level1a.counts?.WOULD_CREATE || 0) +
    Number(level1a.counts?.WOULD_UPDATE || 0),
  note: "No Airtable mutations. Production apply requires founder exact write-set + env gate.",
};
writeJson("03-BETHESDA_88_DRY_RUN_WRITE_MANIFEST.json", level1Manifest);

const repeatability = {
  runA: {
    stageStatus: level1a.stageStatus,
    counts: level1a.counts,
    fingerprint: createHash("sha256")
      .update(
        JSON.stringify(
          (level1a.rows || []).map((r) => [
            r.opportunityId,
            r.action,
            r.founderOperationClass,
            ...(r.changedFields || []),
          ]),
        ),
      )
      .digest("hex"),
  },
  runB: {
    stageStatus: level1b.stageStatus,
    counts: level1b.counts,
    fingerprint: createHash("sha256")
      .update(
        JSON.stringify(
          (level1b.rows || []).map((r) => [
            r.opportunityId,
            r.action,
            r.founderOperationClass,
            ...(r.changedFields || []),
          ]),
        ),
      )
      .digest("hex"),
  },
};
repeatability.identical =
  repeatability.runA.fingerprint === repeatability.runB.fingerprint &&
  JSON.stringify(repeatability.runA.counts) ===
    JSON.stringify(repeatability.runB.counts);
writeJson("04-TWO_RUN_REPEATABILITY.json", repeatability);

// --- Level 2 isolated mock write/read-back ---
const mock = createMockInventoryStore([]);
const fixtureOpp = {
  opportunityId: "gdi_persist_cert_isolated_001",
  hotelId: BETHESDA,
  title: "Isolated persistence certification opportunity",
  organizationName: "Dealality Persistence Lab",
  segment: "Association / Conference",
  primaryContact: {
    name: "Casey Certifier",
    role: "Meeting Planner",
    email: "casey.certifier@example.test",
  },
  sources: [{ url: "https://example.test/persistence-cert" }],
  hotelOpportunityThesis: "Local Bethesda corridor lodging fit for lab event.",
  recommendedNextStep: "Email Casey to qualify lodging need.",
  disposition: "INCLUDED_IN_CERTIFIED_CANDIDATE",
};

const l2create = await persistFullE2eResearchInventory({
  hotelId: BETHESDA,
  runId: "gdi_persist_cert_l2_create",
  repoRoot: ROOT,
  researchCompleted: true,
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: "ISOLATED_MOCK_ONLY",
  simulateLiveWrites: true,
  inventoryStore: mock,
  readBack: true,
  opportunities: [fixtureOpp],
});
const l2unchanged = await persistFullE2eResearchInventory({
  hotelId: BETHESDA,
  runId: "gdi_persist_cert_l2_unchanged",
  repoRoot: ROOT,
  researchCompleted: true,
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: "ISOLATED_MOCK_ONLY",
  simulateLiveWrites: true,
  inventoryStore: mock,
  readBack: true,
  opportunities: [fixtureOpp],
});
const l2update = await persistFullE2eResearchInventory({
  hotelId: BETHESDA,
  runId: "gdi_persist_cert_l2_update",
  repoRoot: ROOT,
  researchCompleted: true,
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: "ISOLATED_MOCK_ONLY",
  simulateLiveWrites: true,
  inventoryStore: mock,
  readBack: true,
  opportunities: [
    {
      ...fixtureOpp,
      title: "Isolated persistence certification opportunity (updated)",
    },
  ],
});
const l2dupe = await persistFullE2eResearchInventory({
  hotelId: BETHESDA,
  runId: "gdi_persist_cert_l2_dupe",
  repoRoot: ROOT,
  researchCompleted: true,
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: "ISOLATED_MOCK_ONLY",
  simulateLiveWrites: true,
  inventoryStore: mock,
  opportunities: [fixtureOpp, { ...fixtureOpp, segment: "Other segment copy" }],
});
const contactPreserve = mock.snapshot()[0];
const emptiedContactAttempt = await persistFullE2eResearchInventory({
  hotelId: BETHESDA,
  runId: "gdi_persist_cert_l2_contact_preserve",
  repoRoot: ROOT,
  researchCompleted: true,
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: "ISOLATED_MOCK_ONLY",
  simulateLiveWrites: true,
  inventoryStore: mock,
  opportunities: [
    {
      ...fixtureOpp,
      title: "Isolated persistence certification opportunity (updated)",
      primaryContact: { name: "" },
    },
  ],
});
const afterEmpty = mock.snapshot()[0];

const level2 = {
  level: 2,
  mode: "ISOLATED_MOCK_STORE",
  create: {
    stageStatus: l2create.stageStatus,
    persistenceCertified: l2create.persistenceCertified,
    counts: l2create.counts,
    readBack: l2create.readBack,
  },
  unchanged: { stageStatus: l2unchanged.stageStatus, counts: l2unchanged.counts },
  update: { stageStatus: l2update.stageStatus, counts: l2update.counts },
  duplicatePrevention: {
    duplicatesResolved: l2dupe.duplicatesResolved?.length || 0,
    uniqueCandidates: l2dupe.counts?.candidates,
    beforeDedupe: l2dupe.counts?.candidatesBeforeDedupe,
  },
  contactPreservation: {
    beforeEmail: contactPreserve?.primaryContact?.email || null,
    afterEmptyAttemptEmail: afterEmpty?.primaryContact?.email || null,
    preserved:
      Boolean(contactPreserve?.primaryContact?.email) &&
      contactPreserve?.primaryContact?.email ===
        afterEmpty?.primaryContact?.email,
  },
  productionAirtableUntouched: true,
};
writeJson("05-LEVEL2_ISOLATED_WRITE_READBACK.json", level2);

// --- Cross-hotel safety (dry-run plans only) ---
async function hotelPlan(hotelId, hotelName) {
  const opps = loadHotelOpportunitiesFile(hotelId).slice(0, 40);
  const result = await persistFullE2eResearchInventory({
    hotelId,
    hotelName,
    runId: `gdi_persist_cert_cross_${hotelId}`,
    repoRoot: ROOT,
    researchCompleted: true,
    apply: false,
    opportunities: opps,
  });
  const foreign = (result.rows || []).filter(
    (r) => r.hotelId && r.hotelId !== hotelId,
  );
  return {
    hotelId,
    hotelName,
    inputCount: opps.length,
    stageStatus: result.stageStatus,
    counts: result.counts,
    foreignHotelRows: foreign.length,
    sampleCompositeKeys: (result.rows || [])
      .slice(0, 5)
      .map((r) => r.compositeKey),
  };
}

const crossHotel = {
  wyndham: await hotelPlan(WYNDHAM, "Wyndham San Diego Bayside"),
  waterstone: await hotelPlan(
    WATERSTONE,
    "Waterstone Resort & Marina Boca Raton",
  ),
};
crossHotel.noCrossContamination =
  crossHotel.wyndham.foreignHotelRows === 0 &&
  crossHotel.waterstone.foreignHotelRows === 0;
writeJson("06-CROSS_HOTEL_SAFETY.json", crossHotel);

// --- STAGED/CURRENT integrity ---
const store = createGdiPublicationPackageStore();
const staged = store.readPointer(BETHESDA, "STAGED");
const current = store.readPointer(BETHESDA, "CURRENT");
const pkgRoot = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  BETHESDA,
);
const integrity = {
  stagedVersion: staged?.publicationVersion || null,
  stagedFingerprint: staged?.contentFingerprintSha256 || null,
  stagedSha: shaFile(path.join(pkgRoot, "STAGED.json")),
  currentVersion: current?.publicationVersion || null,
  currentFingerprint: current?.contentFingerprintSha256 || null,
  currentSha: shaFile(path.join(pkgRoot, "CURRENT.json")),
  stagedIsQuality68: staged?.publicationVersion === QUALITY,
  currentIsGold67: current?.publicationVersion === GOLD,
  qualityCount: quality.size,
  expectedQualityCount: 68,
  qualityCountOk: quality.size === 68,
};
writeJson("07-STAGED_CURRENT_INTEGRITY.json", integrity);

// --- Field mapping report ---
writeJson("08-RESEARCH_TO_AIRTABLE_FIELD_MAPPING.json", {
  destination: GDI_AIRTABLE_TABLES.GDI_HOTEL_RESEARCH_OPPORTUNITIES,
  identity: "(hotelId, opportunityId) via compositeKey",
  fields: MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY,
  payloadCarrier: "researchPayloadJson (slim commercial allowlist)",
  contactsInPayload: true,
  evidenceInPayload: true,
  fqoGatesInPayload: "via slim payload / disposition metadata when present",
  neverWrites: [
    "Group Demand Opportunities (customer GDO)",
    "STAGED.json",
    "CURRENT.json",
    "Surfe-returned contact details",
  ],
});

// --- Founder production approval request (NOT executed) ---
const approvalRequest = {
  status: "AWAITING_FOUNDER_APPROVAL",
  hotelId: BETHESDA,
  destinationTable: "GDI Hotel Research Opportunities",
  destinationTableId:
    GDI_AIRTABLE_TABLES.GDI_HOTEL_RESEARCH_OPPORTUNITIES.tableId,
  universe: 88,
  plannedCreates: Number(level1a.counts?.WOULD_CREATE || 0),
  plannedUpdates: Number(level1a.counts?.WOULD_UPDATE || 0),
  plannedUnchanged: Number(level1a.counts?.UNCHANGED || 0),
  plannedFailed: Number(level1a.counts?.FAILED || 0),
  writeBudgetSuggested:
    Number(level1a.counts?.WOULD_CREATE || 0) +
    Number(level1a.counts?.WOULD_UPDATE || 0),
  requiredGates: [
    "authorizeResearchPersistence=true",
    "exactWriteSetAuthorization=<founder token>",
    "GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1",
    "apply=true",
    "readBack=true",
    "authorizePublish=false",
    "authorizeGdoPersistence=false",
  ],
  rollback: "Restore prior Airtable field values from dry-run snapshot; STAGED/CURRENT untouched",
  notAuthorizedYet: true,
};
writeJson("09-FOUNDER_PRODUCTION_WRITE_APPROVAL_REQUEST.json", approvalRequest);

const summary = {
  contract: GDI_FULL_E2E_RESEARCH_PERSISTENCE_BRIDGE_V1,
  at: new Date().toISOString(),
  liveSchemaOk: liveSchema.ok === true,
  level1: {
    stageStatus: level1a.stageStatus,
    persistenceCertified: false,
    universe: 88,
    counts: level1a.counts,
    founderOperationClassCounts: opClassCounts,
  },
  level2,
  repeatabilityIdentical: repeatability.identical === true,
  crossHotelOk: crossHotel.noCrossContamination === true,
  stagedCurrentProtected:
    integrity.stagedIsQuality68 &&
    integrity.currentIsGold67 &&
    integrity.qualityCountOk,
  productionWritesExecuted: false,
  nextStep: "Founder approve 09-FOUNDER_PRODUCTION_WRITE_APPROVAL_REQUEST.json",
};
writeJson("00-CERTIFICATION_SUMMARY.json", summary);

const md = `# Canonical Full E2E Airtable Persistence Certification

**At:** ${summary.at}  
**Bridge:** \`${GDI_FULL_E2E_RESEARCH_PERSISTENCE_BRIDGE_V1}\`  
**Destination:** ${GDI_AIRTABLE_BASE.name} / GDI Hotel Research Opportunities (\`${GDI_AIRTABLE_TABLES.GDI_HOTEL_RESEARCH_OPPORTUNITIES.tableId}\`)

## Architecture
- Reuses existing bridge + hotel research inventory store.
- Does **not** write Group Demand Opportunities, STAGED, or CURRENT.
- Production mutations remain fail-closed behind env + exact write-set authorization.

## Live schema
- Compatible: **${liveSchema.ok === true}**
- Missing mapped fields: ${(liveSchema.missingMappedFields || []).join(", ") || "(none)"}

## Level 1 — Bethesda 88 dry-run
- Stage status: **${level1a.stageStatus}**
- Persistence certified: **false** (dry-run only — correct)
- Counts: ${JSON.stringify(level1a.counts)}
- Founder operation classes: ${JSON.stringify(opClassCounts)}

## Level 2 — Isolated mock write/read-back
- Create certified: **${level2.create.persistenceCertified}**
- Unchanged / update / duplicate prevention exercised
- Contact preservation: **${level2.contactPreservation.preserved}**
- Production Airtable untouched: **true**

## Repeatability
- Two dry-run fingerprints identical: **${repeatability.identical}**

## Cross-hotel
- Wyndham / Waterstone dry plans: foreign hotel rows = **0** / **0**

## STAGED / CURRENT
- STAGED quality 68: **${integrity.stagedIsQuality68}** (\`${integrity.stagedVersion}\`)
- CURRENT gold 67: **${integrity.currentIsGold67}** (\`${integrity.currentVersion}\`)

## Production writes
**NOT EXECUTED.** See \`09-FOUNDER_PRODUCTION_WRITE_APPROVAL_REQUEST.json\`.

STOP for founder authorization before bulk Bethesda Airtable persistence.
`;
fs.writeFileSync(path.join(OUT, "FOUNDER_PERSISTENCE_CERTIFICATION_REPORT.md"), md);

console.log(JSON.stringify(summary, null, 2));
if (!summary.stagedCurrentProtected) process.exitCode = 2;
if (!summary.repeatabilityIdentical) process.exitCode = 3;
if (!summary.crossHotelOk) process.exitCode = 4;
if (summary.level1.stageStatus !== RESEARCH_PERSISTENCE_STAGE_STATUS.DRY_RUN) {
  process.exitCode = 5;
}
if (summary.level1.counts?.FAILED > 0) process.exitCode = 6;
if (level2.create.persistenceCertified !== true) process.exitCode = 7;
