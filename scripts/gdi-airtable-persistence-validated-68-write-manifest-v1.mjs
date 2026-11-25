#!/usr/bin/env node
/**
 * Founder policy: persist only FQO-validated hotel opportunities.
 * Source of truth for Bethesda = certified STAGED quality package (68).
 * Read-only Airtable compare. No mutations.
 */
import fs from "node:fs";
import path from "node:path";
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
} from "../lib/group-demand-intelligence/workflow/gdi-research-inventory-slim-payload-v1.js";
import { evaluateFinalizedQualifiedOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import {
  filterValidatedFqoPersistenceWriteSet,
  RESEARCH_PERSISTENCE_WRITE_SET_POLICY,
  persistFullE2eResearchInventory,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import { ensureDemandGeneratorIdOnOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-slice2b-relationship-wiring-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-airtable-persistence-validated-68-write-manifest-v1",
);
const BETHESDA = "recLuxvwwxID7U2B8";
const QUALITY =
  "gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261125_quality_v1";
const GOLD = "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";
const REJECTED_CANARY = "gdi_e2d_770e3b44ad97c1ae";
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

function loadPackageOpps(version) {
  const p = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    BETHESDA,
    version,
    "package.json",
  );
  return (readJson(p).opportunities || []).map((o) => ({
    ...o,
    opportunityId: text(o.opportunityId || o.id),
    hotelId: BETHESDA,
  }));
}

function hasUsableOutreach(opp) {
  const pc = opp.primaryContact || {};
  return Boolean(
    text(pc.email) ||
      text(pc.phone) ||
      text(pc.linkedinUrl) ||
      text(pc.askToBeDirectedTo) ||
      text(pc.organizationContactRouteLabel),
  );
}

function outreachPreview(opp) {
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

fs.mkdirSync(OUT, { recursive: true });

const qualityOpps = loadPackageOpps(QUALITY);
const qualityIds = new Set(qualityOpps.map((o) => o.opportunityId));
if (qualityOpps.length !== 68) {
  console.error(`BLOCKED: quality package count ${qualityOpps.length} !== 68`);
  process.exit(2);
}

const manifest88 = readJson(MANIFEST_88);
const outsideCertified = manifest88.rows
  .map((r) => ({
    originalOpportunityId: r.originalOpportunityId,
    currentCanonicalId: r.currentCanonicalId,
    disposition: r.disposition,
    inCertified68: qualityIds.has(
      text(r.currentCanonicalId || r.originalOpportunityId),
    ),
  }))
  .filter((r) => !r.inCertified68);

// Task 1 — FQO gate on exact 68
const fqoRows = qualityOpps.map((opp) => {
  const withHist = evaluateFinalizedQualifiedOpportunity(opp, {
    historicalApprovedInventory: true,
  });
  const strict = evaluateFinalizedQualifiedOpportunity(opp, {
    historicalApprovedInventory: false,
  });
  return {
    opportunityId: opp.opportunityId,
    title: opp.title || opp.displayTitle || null,
    namedContact: text(opp.primaryContact?.name) || null,
    outreach: outreachPreview(opp),
    fqoWithHistoricalCompat: {
      finalizedQualified: withHist.finalizedQualified === true,
      status: withHist.status,
      reasons: withHist.reasons || [],
      processingDisposition: withHist.processingDisposition || null,
    },
    fqoStrictNoHistoricalBypass: {
      finalizedQualified: strict.finalizedQualified === true,
      status: strict.status,
      reasons: strict.reasons || [],
    },
    // Write eligibility: must pass FQO; historical compat may assist schema
    // evidence checks but cannot invent a pass if finalizedQualified is false.
    writeEligible: withHist.finalizedQualified === true,
    historicalAloneWouldNotPass:
      withHist.finalizedQualified === true &&
      strict.finalizedQualified !== true,
  };
});

const writeFilter = filterValidatedFqoPersistenceWriteSet(qualityOpps, {
  allowHistoricalSchemaCompat: true,
  allowedOpportunityIds: qualityIds,
});

writeJson("01-FQO_GATE_VERIFICATION_68.json", {
  policy: "Discover broadly. Research thoroughly. Qualify rigorously. Persist validated opportunities. Preserve rejected research as supporting intelligence.",
  certifiedPackage: QUALITY,
  packageCount: qualityOpps.length,
  writeEligibleCount: fqoRows.filter((r) => r.writeEligible).length,
  strictPassCount: fqoRows.filter(
    (r) => r.fqoStrictNoHistoricalBypass.finalizedQualified,
  ).length,
  historicalCompatAssistOnlyCount: fqoRows.filter(
    (r) => r.historicalAloneWouldNotPass,
  ).length,
  rejectedPriorCanary: {
    opportunityId: REJECTED_CANARY,
    inCertified68: qualityIds.has(REJECTED_CANARY),
    reason: "Research-continuation identity — not a validated hotel opportunity; not approved for CREATE.",
  },
  rows: fqoRows,
  filterResult: {
    keptCount: writeFilter.keptCount,
    deferredCount: writeFilter.deferredCount,
    deferred: writeFilter.deferred,
  },
});

// Live Airtable (read-only)
let liveRecords = [];
let liveByOpp = new Map();
try {
  liveRecords = await listResearchInventoryForHotel(BETHESDA);
  for (const rec of liveRecords) {
    const oid = text(rec.fields?.opportunityId);
    if (oid) liveByOpp.set(oid, rec);
  }
} catch (err) {
  writeJson("02-LIVE_FETCH_ERROR.json", { error: String(err?.message || err) });
  console.error("BLOCKED: live fetch failed", err);
  process.exit(2);
}

writeJson("02-NONQUALIFIED_PRESERVATION.json", {
  policy:
    "Do not delete existing Airtable research outside the certified 68. Do not silently mutate them in this write set.",
  certified68Count: 68,
  liveAirtableBethesdaCount: liveRecords.length,
  liveMatchedToCertified68: [...liveByOpp.keys()].filter((id) =>
    qualityIds.has(id),
  ).length,
  liveOutsideCertified68: [...liveByOpp.keys()].filter(
    (id) => !qualityIds.has(id),
  ).length,
  liveOutsideIds: [...liveByOpp.keys()]
    .filter((id) => !qualityIds.has(id))
    .sort(),
  universe88OutsideCertified: outsideCertified,
  proposedMutationsToNonqualified: 0,
});

// Task 3 — exact write manifest for 68 only
const manifestRows = [];
for (const opp of writeFilter.kept) {
  const id = opp.opportunityId;
  const stamped = ensureDemandGeneratorIdOnOpportunity({
    ...opp,
    hotelId: BETHESDA,
    firstDiscoveredRunId: opp.firstDiscoveredRunId || "validated-68-dry",
  });
  const built = buildResearchInventoryFields(stamped, {
    hotelId: BETHESDA,
    hotelName: "Bethesda Marriott",
  });
  const existing = liveByOpp.get(id) || null;
  let action = "WOULD_CREATE";
  let materialDiff = null;
  let fieldChanges = [];
  let mergeOk = true;
  let mergeCode = null;
  let fields = built.ok ? { ...built.fields } : null;
  let blocker = null;

  if (!built.ok) {
    action = "BLOCKED";
    blocker = built.code;
  } else if (!existing) {
    action = "WOULD_CREATE";
  } else {
    const merged = mergeProposedResearchInventoryFields(
      fields,
      existing.fields || {},
    );
    mergeOk = merged.ok === true;
    mergeCode = merged.code || null;
    if (!merged.ok) {
      action = "BLOCKED";
      blocker = merged.code;
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
        const beforeN =
          before == null || before === ""
            ? null
            : typeof before === "string"
              ? before
              : JSON.stringify(before);
        const afterN =
          after == null || after === ""
            ? null
            : typeof after === "string"
              ? after
              : JSON.stringify(after);
        if (beforeN === afterN) continue;
        fieldChanges.push({
          field: fname,
          beforePreview: String(before ?? "").slice(0, 200),
          afterPreview: String(after ?? "").slice(0, 200),
          beforeLen: String(before ?? "").length,
          afterLen: String(after ?? "").length,
        });
      }
    }
  }

  const slim = buildSlimResearchPayload(stamped);
  const round = slim.ok
    ? assertSlimPayloadRoundTrip(stamped, slim.payload)
    : { ok: false };
  const fqoMeta = fqoRows.find((r) => r.opportunityId === id);

  manifestRows.push({
    opportunityId: id,
    compositeKey: `${BETHESDA}::${id}`,
    airtableRecordId: existing?.id || null,
    action,
    blocker,
    mergeOk,
    mergeCode,
    changedFields: materialDiff?.changedFields || fieldChanges.map((c) => c.field),
    fieldChanges,
    commercialEligibilityStatus:
      fields?.[F.commercialEligibilityStatus] || null,
    hotelFqoFinalizedQualified: fqoMeta?.writeEligible === true,
    namedContact: fqoMeta?.namedContact || null,
    outreach: fqoMeta?.outreach || null,
    payloadRoundTripOk: round.ok === true,
    missingContacts: round.missingContacts || [],
    gdoWrite: false,
    publicationWrite: false,
    inWriteSet: true,
  });
}

const counts = {
  certifiedUniverse: 68,
  writeEligible: writeFilter.keptCount,
  WOULD_CREATE: manifestRows.filter((r) => r.action === "WOULD_CREATE").length,
  WOULD_UPDATE: manifestRows.filter((r) => r.action === "WOULD_UPDATE").length,
  UNCHANGED: manifestRows.filter((r) => r.action === "UNCHANGED").length,
  BLOCKED: manifestRows.filter((r) => r.action === "BLOCKED").length,
  deferredNonqualifiedFromPackage: writeFilter.deferredCount,
  duplicateAliasesOutsideWrite: outsideCertified.filter((r) =>
    /DUPLICATE/i.test(r.disposition || ""),
  ).length,
  nonqualifiedPreservedNoMutation: outsideCertified.length,
};

writeJson("03-VALIDATED_68_EXACT_WRITE_MANIFEST.json", {
  at: new Date().toISOString(),
  writeSetPolicy: RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
  certifiedPackage: QUALITY,
  priorRejectedUniverseWrite: {
    proposed86: "NOT_APPROVED",
    priorCanary: REJECTED_CANARY,
    priorCanaryStatus: "NOT_APPROVED_RESEARCH_CONTINUATION",
  },
  counts,
  productionWriteBudgetSuggested:
    counts.WOULD_CREATE + counts.WOULD_UPDATE,
  rows: manifestRows,
});

// Task 4 — replace canary
const canaryCandidates = manifestRows
  .filter(
    (r) =>
      r.action === "WOULD_UPDATE" &&
      r.hotelFqoFinalizedQualified === true &&
      Boolean(r.namedContact) &&
      /@/.test(String(r.outreach || "")) &&
      r.mergeOk === true &&
      r.payloadRoundTripOk === true &&
      Boolean(r.airtableRecordId),
  )
  .map((r) => {
    const changes = r.fieldChanges || [];
    const onlyPayloadAndIds = changes.every((c) =>
      [
        F.researchPayloadJson,
        F.dgGeneratorId,
        F.sourceSummary,
        F.commercialEligibilityStatus,
        F.schemaVersion,
      ].includes(c.field),
    );
    const score =
      (onlyPayloadAndIds ? 100 : 0) +
      (changes.length <= 3 ? 20 : 0) -
      changes.length +
      (/@/.test(String(r.outreach || "")) ? 10 : 0);
    return { ...r, onlyPayloadAndIds, score, changeCount: changes.length };
  })
  .sort((a, b) => b.score - a.score || a.changeCount - b.changeCount);

const canary = canaryCandidates[0] || null;
let canarySnapshot = null;
if (canary) {
  const live = liveByOpp.get(canary.opportunityId);
  canarySnapshot = {
    airtableRecordId: live.id,
    opportunityId: canary.opportunityId,
    fieldsBefore: Object.fromEntries(
      MATERIAL_RESEARCH_FIELDS.map((f) => [
        f,
        live.fields?.[f] == null
          ? null
          : String(live.fields[f]).slice(0, 2000),
      ]),
    ),
    lastMaterialChangeAtBefore: live.fields?.[F.lastMaterialChangeAt] || null,
  };
}

writeJson("04-CERTIFIED_OPPORTUNITY_CANARY.json", {
  status: "AWAITING_FOUNDER_APPROVAL",
  executed: false,
  rejectedPriorCanary: REJECTED_CANARY,
  proposal: canary
    ? {
        mode: "SINGLE_EXISTING_VALIDATED_UPDATE",
        maxWrites: 1,
        opportunityId: canary.opportunityId,
        airtableRecordId: canary.airtableRecordId,
        namedContact: canary.namedContact,
        outreach: canary.outreach,
        fqoFinalizedQualified: true,
        action: "WOULD_UPDATE",
        changedFields: canary.changedFields,
        fieldChanges: canary.fieldChanges,
        onlyPayloadAndIds: canary.onlyPayloadAndIds,
        destinationTable: "GDI Hotel Research Opportunities",
        destinationTableId: "tblbkDj0vhQLfrssV",
        verify: [
          "update",
          "read_back_field_integrity",
          "unchanged_rerun",
          "idempotency",
          "duplicate_protection",
          "no_gdo_side_effects",
          "no_staged_current_side_effects",
          "contacts_and_evidence_preserved",
        ],
        requiredAuth: [
          "authorizeResearchPersistence=true",
          `exactWriteSetAuthorization=FOUNDER_CANARY_${canary.opportunityId}`,
          "writeSetPolicy=VALIDATED_FQO_ONLY",
          "allowedOpportunityIds=[canary id only]",
          "GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1",
          "apply=true",
          "readBack=true",
          "writeBudget=1",
        ],
      }
    : {
        mode: "NO_SAFE_MINIMAL_UPDATE",
        note: "No safe minimal update found among certified 68; request disposable validated test record separately.",
      },
  preWriteSnapshot: canarySnapshot,
  rollbackSteps: canary
    ? [
        "1. Confirm canary airtableRecordId from pre-write snapshot.",
        "2. Restore each changed typed field from fieldsBefore in 04-CERTIFIED_OPPORTUNITY_CANARY.json preWriteSnapshot.",
        "3. Restore researchPayloadJson exactly from snapshot (full value in live backup if truncated in report).",
        "4. Re-read record; confirm fingerprint matches pre-write snapshot.",
        "5. Confirm STAGED pointer and CURRENT pointer unchanged.",
        "6. Confirm no GDO rows created/updated.",
      ]
    : [],
});

// Task 5 — idempotency on validated set (mock store)
function createMemoryStore() {
  const byKey = new Map();
  return {
    async upsert({ opportunity, hotel, dryRun }) {
      const built = buildResearchInventoryFields(opportunity, hotel);
      if (!built.ok) {
        return { ok: false, action: "FAILED", code: built.code };
      }
      const key = built.compositeKey;
      const existing = byKey.get(key) || null;
      let fields = { ...built.fields };
      if (existing) {
        const merged = mergeProposedResearchInventoryFields(
          fields,
          existing.fields,
        );
        if (!merged.ok) {
          return { ok: false, action: "FAILED", code: merged.code };
        }
        fields = merged.fields;
        const diff = diffMaterialResearchFields(fields, existing.fields);
        if (!diff.materialChanged) {
          return {
            ok: true,
            action: dryRun ? "UNCHANGED" : "UNCHANGED",
            compositeKey: key,
            airtableRecordId: existing.id,
            materialDiff: diff,
          };
        }
        if (!dryRun) {
          existing.fields = fields;
        }
        return {
          ok: true,
          action: dryRun ? "WOULD_UPDATE" : "UPDATED",
          compositeKey: key,
          airtableRecordId: existing.id,
          materialDiff: diff,
        };
      }
      const id = `rec_mem_${byKey.size + 1}`;
      if (!dryRun) {
        byKey.set(key, { id, fields });
      }
      return {
        ok: true,
        action: dryRun ? "WOULD_CREATE" : "CREATED",
        compositeKey: key,
        airtableRecordId: dryRun ? null : id,
      };
    },
  };
}

const mem = createMemoryStore();
const idemOpps = writeFilter.kept.map((o) => ({
  ...o,
  // Freeze provenance so rerun fingerprints are stable.
  firstDiscoveredRunId: "validated-68-idem",
  lastResearchedAt: "2026-11-25T00:00:00.000Z",
  lastMaterialChangeAt: "2026-11-25T00:00:00.000Z",
}));
async function idemRun(store) {
  return persistFullE2eResearchInventory({
    hotelId: BETHESDA,
    hotelName: "Bethesda Marriott",
    runId: "validated-68-idem",
    opportunities: idemOpps,
    writeSetPolicy: RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
    requireValidatedFqoWriteSet: true,
    allowedOpportunityIds: [...qualityIds],
    inventoryStore: store,
    simulateLiveWrites: true,
    authorizeResearchPersistence: false,
  });
}
const run1 = await idemRun(mem);
const run2 = await idemRun(mem); // may normalize once (merge shape)
const run3 = await idemRun(mem); // must be fully unchanged

// Inject a nonqualified + rejected canary into input — must be deferred
const pollution = await persistFullE2eResearchInventory({
  hotelId: BETHESDA,
  runId: "validated-68-pollution",
  opportunities: [
    ...writeFilter.kept.slice(0, 2),
    {
      opportunityId: REJECTED_CANARY,
      hotelId: BETHESDA,
      title: "Should not persist",
      researchContinuation: true,
    },
  ],
  writeSetPolicy: RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
  requireValidatedFqoWriteSet: true,
  allowedOpportunityIds: [...qualityIds],
  inventoryStore: createMemoryStore(),
  simulateLiveWrites: true,
});

writeJson("05-IDEMPOTENCY_AND_SAFETY.json", {
  run1: {
    candidates: run1.counts.candidates,
    created: run1.counts.CREATED,
    updated: run1.counts.UPDATED,
    unchanged: run1.counts.UNCHANGED,
    failed: run1.counts.FAILED,
    deferred: run1.counts.DEFERRED_NONQUALIFIED,
  },
  run2: {
    candidates: run2.counts.candidates,
    created: run2.counts.CREATED,
    updated: run2.counts.UPDATED,
    unchanged: run2.counts.UNCHANGED,
    failed: run2.counts.FAILED,
  },
  run3: {
    candidates: run3.counts.candidates,
    created: run3.counts.CREATED,
    updated: run3.counts.UPDATED,
    unchanged: run3.counts.UNCHANGED,
    failed: run3.counts.FAILED,
  },
  note: "Run1 CREATE → run2 may apply one-time merge normalization → run3 must be UNCHANGED (no duplicates).",
  idempotentRerun:
    run3.counts.CREATED === 0 &&
    run3.counts.UPDATED === 0 &&
    run3.counts.UNCHANGED === 68 &&
    run1.counts.CREATED === 68,
  noDuplicateCreatesOnRerun: run2.counts.CREATED === 0 && run3.counts.CREATED === 0,
  pollutionGuard: {
    rejectedCanaryDeferred: (pollution.deferredNonqualified || []).some(
      (d) => d.opportunityId === REJECTED_CANARY,
    ),
    candidatesPersisted: pollution.counts.candidates,
    note: "Nonqualified / outside-certified identities deferred; not written as active opportunities.",
  },
});

const store = createGdiPublicationPackageStore();
const staged = store.readPointer(BETHESDA, "STAGED");
const current = store.readPointer(BETHESDA, "CURRENT");
writeJson("06-STAGED_CURRENT_SAFETY.json", {
  staged: staged?.publicationVersion || null,
  current: current?.publicationVersion || null,
  stagedIsQuality68: staged?.publicationVersion === QUALITY,
  currentIsGold67: current?.publicationVersion === GOLD,
  gdoWrites: 0,
  publicationWrites: 0,
  customerPublication: false,
});

const summary = {
  verdict:
    counts.BLOCKED === 0 &&
    writeFilter.keptCount === 68 &&
    canary &&
    staged?.publicationVersion === QUALITY &&
    current?.publicationVersion === GOLD
      ? "READY_FOR_VALIDATED_CANARY"
      : "BLOCKED",
  certifiedPackage: QUALITY,
  writeEligible: writeFilter.keptCount,
  wouldCreate: counts.WOULD_CREATE,
  wouldUpdate: counts.WOULD_UPDATE,
  unchanged: counts.UNCHANGED,
  blocked: counts.BLOCKED,
  productionWriteBudgetSuggested: counts.WOULD_CREATE + counts.WOULD_UPDATE,
  prior86NotApproved: true,
  priorCanaryRejected: REJECTED_CANARY,
  proposedCanary: canary?.opportunityId || null,
  canaryExecuted: false,
  productionWritesExecuted: false,
  nonqualifiedMutationsProposed: 0,
};

writeJson("00-VALIDATED_68_SUMMARY.json", summary);

const md = `# Validated-68 Persistence Write Scope — Bethesda

**Verdict: ${summary.verdict}**

## Founder policy (permanent)
Discover broadly. Research thoroughly. Qualify rigorously. **Persist validated opportunities.** Preserve rejected research as supporting intelligence, not as active hotel opportunities.

## Source
Certified STAGED: \`${QUALITY}\` (**68**).  
Broader 88 research universe: **not** the production write source.  
Prior 86-write set: **NOT APPROVED**.  
Prior canary \`${REJECTED_CANARY}\`: **NOT APPROVED** (research continuation).

## Counts
| Action | Count |
|---|---:|
| Write-eligible (FQO) | ${writeFilter.keptCount} |
| WOULD_CREATE | ${counts.WOULD_CREATE} |
| WOULD_UPDATE | ${counts.WOULD_UPDATE} |
| UNCHANGED | ${counts.UNCHANGED} |
| BLOCKED | ${counts.BLOCKED} |
| Suggested budget | ${counts.WOULD_CREATE + counts.WOULD_UPDATE} |
| Nonqualified mutations proposed | 0 |

## Canary (awaiting founder GO)
${
  canary
    ? `- **${canary.opportunityId}** (existing Airtable \`${canary.airtableRecordId}\`)
- Named: ${canary.namedContact}
- Outreach: ${canary.outreach}
- Action: minimal UPDATE
- Rollback: restore pre-write snapshot in \`04-CERTIFIED_OPPORTUNITY_CANARY.json\``
    : "- No safe minimal update; disposable validated test record required."
}

## Safety
- STAGED quality 68 unchanged: **${staged?.publicationVersion === QUALITY}**
- CURRENT gold 67 unchanged: **${current?.publicationVersion === GOLD}**
- GDO / customer publication: **none**
- Live Airtable mutations this run: **none**

Evidence folder: \`reports/gdi-airtable-persistence-validated-68-write-manifest-v1/\`
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_VALIDATED_68_WRITE_SCOPE_REPORT.md"), md);
console.log(JSON.stringify(summary, null, 2));
if (summary.verdict !== "READY_FOR_VALIDATED_CANARY") process.exitCode = 2;
