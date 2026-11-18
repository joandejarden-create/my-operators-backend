#!/usr/bin/env node
/**
 * Bethesda two-write production research-inventory pilot.
 *
 * Founder auth: gdi-bethesda-live-14-segment-persistence-pilot-write-v1
 *
 *   node scripts/gdi-bethesda-two-write-production-pilot-v1.mjs
 *   node scripts/gdi-bethesda-two-write-production-pilot-v1.mjs --apply \
 *     --authorize-production-writes \
 *     --exact-write-set-authorization=gdi-bethesda-live-14-segment-persistence-pilot-write-v1
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  persistFullE2eResearchInventory,
  RESEARCH_PERSISTENCE_ACTIVATION,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import {
  listResearchInventoryForHotel,
  findResearchInventoryByCompositeKey,
  buildResearchInventoryFields,
  diffMaterialResearchFields,
  readBackResearchInventory,
  PAYLOAD_VOLATILE_KEYS,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import {
  MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F,
  buildHotelResearchCompositeKey,
} from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-bethesda-two-write-production-pilot-v1");
const APPROVED_MANIFEST = path.join(
  ROOT,
  "reports/gdi-bethesda-live-14-segment-persistence-pilot-v1/EXACT_PRODUCTION_PILOT_WRITE_MANIFEST.json",
);
const AUTH = "gdi-bethesda-live-14-segment-persistence-pilot-write-v1";
const HOTEL_ID = "recLuxvwwxID7U2B8";
const HOTEL_NAME = "Bethesda Marriott";
const CREATE_ID = "gdi_live_pinnacle_lacrosse_championships_2027";
const UPDATE_ID = "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_";
const CREATE_KEY = `${HOTEL_ID}::${CREATE_ID}`;
const UPDATE_KEY = `${HOTEL_ID}::${UPDATE_ID}`;
const ALLOWED = new Set([CREATE_KEY, UPDATE_KEY]);

const APPLY = process.argv.includes("--apply");
const AUTHORIZE = process.argv.includes("--authorize-production-writes");
const authArg = process.argv.find((a) =>
  a.startsWith("--exact-write-set-authorization="),
);
const EXACT_AUTH = authArg
  ? authArg.slice("--exact-write-set-authorization=".length)
  : "";

function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof body === "string" ? body : `${JSON.stringify(body, null, 2)}\n`,
  );
  return p;
}

function parsePayload(raw) {
  if (!raw) return {};
  if (typeof raw === "object") return { ...raw };
  try {
    return JSON.parse(String(raw));
  } catch {
    return {};
  }
}

function sourceUrl(s) {
  if (!s) return "";
  if (typeof s === "string") return s.trim();
  return String(s.url || s.sourceUrl || s.href || "").trim();
}

function mergeSources(existing = [], additions = []) {
  const out = [];
  const seen = new Set();
  for (const s of [...existing, ...additions]) {
    const url = sourceUrl(s);
    const key = url || JSON.stringify(s);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(typeof s === "string" ? { url: s, label: s } : s);
  }
  return out;
}

function buildCreateOpportunity(manifestEntry) {
  const now = new Date().toISOString();
  return {
    id: CREATE_ID,
    opportunityId: CREATE_ID,
    hotelId: HOTEL_ID,
    organizationName: manifestEntry.proposedValues.organizationName,
    title: manifestEntry.proposedValues.title,
    segment: "sports_entertainment",
    priority: "WATCHLIST",
    disposition: "WATCHLIST_RESEARCH",
    inventoryPopulationPrimary: "WATCHLIST",
    researchLifecycleHint: "WATCHLIST",
    eventStartDate: manifestEntry.proposedValues.eventStartDate,
    eventEndDate: manifestEntry.proposedValues.eventEndDate,
    whatIsHappening:
      "Summer Pinnacle Lacrosse Championships 2027 at Maryland SoccerPlex (Boyds). Stay-to-play housing partner Athletes Hospitality; hotel link pending. Bethesda Marriott is overflow / corridor lodging research only.",
    localConnection:
      "Montgomery County SoccerPlex stay-to-play overflow corridor for Bethesda Marriott when housing-partner inventory is constrained.",
    sources: manifestEntry.sourceEvidence,
    demandThesis: null,
    firstDiscoveredAt: now,
    firstDiscoveredRunId: "bethesda_two_write_production_pilot_v1",
    lastMaterialChangeAt: now,
    lastResearchedAt: now,
  };
}

function buildUpdateOpportunity(liveRecord, manifestEntry) {
  const prev = liveRecord.fields || {};
  const base = parsePayload(prev[F.researchPayloadJson]);
  const additions = manifestEntry.sourceEvidence || [];
  const mergedSources = mergeSources(
    Array.isArray(base.sources) ? base.sources : [],
    additions,
  );
  const whatBase = String(
    base.whatIsHappening || base.summaryWhat || prev[F.title] || "",
  );
  const additionText =
    " Public PDA TRI calendar confirms Dec 2026 Bethesda courses including PDA 529 (Dec 1), PDA 531 (Dec 2–4), PDA 374 (Dec 1), and PDA 100 (Dec 7–11).";
  const alreadyHas =
    /PDA 100|pda-tri-quality-risk-management|aseptic-processing-training-certificate-program-pda-100/i.test(
      whatBase + JSON.stringify(mergedSources),
    );
  const whatIsHappening = alreadyHas
    ? whatBase
    : `${whatBase}${additionText}`.trim();
  const now = new Date().toISOString();
  return {
    ...base,
    id: UPDATE_ID,
    opportunityId: UPDATE_ID,
    hotelId: HOTEL_ID,
    organizationName:
      prev[F.organizationName] || base.organizationName || "Parenteral Drug Association (PDA)",
    title:
      prev[F.title] ||
      base.title ||
      "PDA TRI Bethesda training — December 2026 lodging research",
    segment: prev[F.segment] || base.segment || "education_scientific",
    priority: prev[F.priority] || base.priority || null,
    sources: mergedSources,
    whatIsHappening,
    summaryWhat: whatIsHappening,
    firstDiscoveredAt: prev[F.firstDiscoveredAt] || base.firstDiscoveredAt || null,
    firstDiscoveredRunId:
      prev[F.firstDiscoveredRunId] || base.firstDiscoveredRunId || null,
    lastMaterialChangeAt: now,
    lastResearchedAt: now,
    researchLifecycleHint: prev[F.researchLifecycleStatus] || "ACTIVE_RESEARCH",
  };
}

async function analyzeSixtyThreeDrift() {
  const bag = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "data/group-demand-intelligence/hotels",
        HOTEL_ID,
        "opportunities.json",
      ),
      "utf8",
    ),
  );
  const rows = Array.isArray(bag.opportunities) ? bag.opportunities : [];
  const probe = await persistFullE2eResearchInventory({
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    runId: "bethesda_63_drift_probe_v1",
    repoRoot: ROOT,
    researchCompleted: true,
    apply: false,
    authorizeResearchPersistence: false,
    opportunities: rows.map((r) => ({
      ...r,
      opportunityId: r.id || r.opportunityId,
      hotelId: HOTEL_ID,
    })),
  });

  const would = (probe.rows || []).filter((r) => r.action === "WOULD_UPDATE");
  const fieldHits = {};
  const classifications = {
    OPERATIONAL_PROVENANCE_STAMP_FALSE_POSITIVE: 0,
    SOURCE_SUMMARY_SERIALIZATION: 0,
    GENUINELY_MATERIAL_RESEARCH: 0,
    OTHER: 0,
  };
  const samples = [];
  for (const r of would) {
    for (const f of r.changedFields || []) {
      fieldHits[f] = (fieldHits[f] || 0) + 1;
    }
    const fields = r.changedFields || [];
    let cls = "OTHER";
    if (
      fields.length === 1 &&
      fields[0] === "researchPayloadJson" &&
      // After volatile-key fix these should largely disappear; remaining payload
      // diffs need manual inspection.
      true
    ) {
      cls =
        fields.includes("sourceSummary") && fields.length === 1
          ? "SOURCE_SUMMARY_SERIALIZATION"
          : fields.length === 1 && fields[0] === "researchPayloadJson"
            ? "GENUINELY_MATERIAL_RESEARCH_OR_RESIDUAL_SERIALIZATION"
            : "OTHER";
    }
    if (fields.length === 1 && fields[0] === "sourceSummary") {
      cls = "SOURCE_SUMMARY_SERIALIZATION";
    }
    if (!classifications[cls]) classifications[cls] = 0;
    classifications[cls] += 1;
    if (samples.length < 12) {
      samples.push({
        opportunityId: r.opportunityId,
        changedFields: fields,
        classification: cls,
      });
    }
  }

  return {
    bridgeCounts: probe.counts,
    wouldUpdateCount: would.length,
    fieldHits,
    classifications,
    samples,
    volatileKeysExcludedFromCompare: [...PAYLOAD_VOLATILE_KEYS],
    recommendation:
      would.length <= 5
        ? "Most prior 63 false positives eliminated by excluding firstDiscoveredRunId/firstDiscoveredAt from payload compare. Remaining diffs (if any) should be reviewed individually; do not mass-apply FS bag over Airtable."
        : "Residual WOULD_UPDATE remain — inspect samples before any broader sync. Do not apply mass FS→Airtable updates.",
  };
}

async function main() {
  const approved = JSON.parse(fs.readFileSync(APPROVED_MANIFEST, "utf8"));
  write("EXACT_WRITE_AUTHORIZATION.json", {
    authorizationId: AUTH,
    approvedManifestPath:
      "reports/gdi-bethesda-live-14-segment-persistence-pilot-v1/EXACT_PRODUCTION_PILOT_WRITE_MANIFEST.json",
    approvedAuthorizationId: approved.authorizationId,
    authorizedKeys: [CREATE_KEY, UPDATE_KEY],
    maxWrites: 2,
    destination: {
      baseId: "appa2cE7FTRmIbB32",
      tableId: "tblbkDj0vhQLfrssV",
      hotelId: HOTEL_ID,
    },
    envGate: RESEARCH_PERSISTENCE_ACTIVATION.envGate,
    recurringPersistenceEnabled: false,
  });

  console.log("[preflight] fetching live Airtable…");
  const liveRows = await listResearchInventoryForHotel(HOTEL_ID);
  const byOpp = new Map(
    liveRows.map((r) => [r.fields?.[F.opportunityId], r]),
  );
  const createExisting = await findResearchInventoryByCompositeKey(CREATE_KEY);
  const updateExisting = await findResearchInventoryByCompositeKey(UPDATE_KEY);
  const createEntry = (approved.entries || []).find((e) => e.action === "CREATE");
  const updateEntry = (approved.entries || []).find((e) => e.action === "UPDATE");

  const createOpp = buildCreateOpportunity(createEntry);
  const updateOpp = updateExisting
    ? buildUpdateOpportunity(updateExisting, updateEntry)
    : null;

  const createBuilt = buildResearchInventoryFields(createOpp, {
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
  });
  const updateBuilt = updateOpp
    ? buildResearchInventoryFields(updateOpp, {
        hotelId: HOTEL_ID,
        hotelName: HOTEL_NAME,
      })
    : { ok: false, code: "UPDATE_BASE_MISSING" };

  let updateDiff = null;
  if (updateBuilt.ok && updateExisting) {
    const fields = { ...updateBuilt.fields };
    const prev = updateExisting.fields || {};
    if (prev[F.firstDiscoveredAt]) fields[F.firstDiscoveredAt] = prev[F.firstDiscoveredAt];
    if (prev[F.firstDiscoveredRunId]) {
      fields[F.firstDiscoveredRunId] = prev[F.firstDiscoveredRunId];
    }
    updateDiff = diffMaterialResearchFields(fields, prev);
  }

  const stopReasons = [];
  if (approved.authorizationId !== AUTH) {
    stopReasons.push("APPROVED_MANIFEST_AUTH_MISMATCH");
  }
  if (!createEntry || createEntry.compositeKey !== CREATE_KEY) {
    stopReasons.push("CREATE_MANIFEST_IDENTITY_MISMATCH");
  }
  if (!updateEntry || updateEntry.compositeKey !== UPDATE_KEY) {
    stopReasons.push("UPDATE_MANIFEST_IDENTITY_MISMATCH");
  }
  if (createExisting) stopReasons.push("CREATE_ALREADY_PRESENT");
  if (!updateExisting) stopReasons.push("UPDATE_TARGET_MISSING");
  if (
    updateExisting &&
    updateEntry.airtableRecordId &&
    updateExisting.id !== updateEntry.airtableRecordId
  ) {
    stopReasons.push("UPDATE_RECORD_ID_MISMATCH");
  }
  if (!createBuilt.ok) stopReasons.push(`CREATE_FIELDS_${createBuilt.code}`);
  if (!updateBuilt.ok) stopReasons.push(`UPDATE_FIELDS_${updateBuilt.code}`);
  if (updateDiff && updateDiff.materialChanged !== true) {
    stopReasons.push("UPDATE_NOT_MATERIAL_VS_LIVE");
  }
  if (String(createBuilt.fields?.[F.sourceSummary] || "").includes("[object Object]")) {
    stopReasons.push("CREATE_SOURCE_SUMMARY_PLACEHOLDER");
  }
  if (String(updateBuilt.fields?.[F.sourceSummary] || "").includes("[object Object]")) {
    stopReasons.push("UPDATE_SOURCE_SUMMARY_PLACEHOLDER");
  }

  const preflight = {
    at: new Date().toISOString(),
    liveBethesdaCount: liveRows.length,
    createAbsent: !createExisting,
    updatePresent: Boolean(updateExisting),
    updateAirtableRecordId: updateExisting?.id || null,
    updateProvenance: updateExisting
      ? {
          firstDiscoveredAt: updateExisting.fields?.[F.firstDiscoveredAt] || null,
          firstDiscoveredRunId:
            updateExisting.fields?.[F.firstDiscoveredRunId] || null,
          researchLifecycleStatus:
            updateExisting.fields?.[F.researchLifecycleStatus] || null,
          commercialEligibilityStatus:
            updateExisting.fields?.[F.commercialEligibilityStatus] || null,
        }
      : null,
    createFieldsOk: createBuilt.ok === true,
    updateFieldsOk: updateBuilt.ok === true,
    updateMaterialChanged: updateDiff?.materialChanged === true,
    updateChangedFields: updateDiff?.changedFields || [],
    sourceEvidence: {
      create: createEntry?.sourceEvidence || [],
      update: updateEntry?.sourceEvidence || [],
    },
    stopReasons,
    ok: stopReasons.length === 0,
  };
  write("PRODUCTION_PREFLIGHT.json", preflight);
  write(
    "PRODUCTION_PREFLIGHT.md",
    `# Production preflight

| Check | Result |
|-------|--------|
| Live Bethesda count | ${preflight.liveBethesdaCount} |
| CREATE absent | ${preflight.createAbsent} |
| UPDATE present | ${preflight.updatePresent} |
| UPDATE record | \`${preflight.updateAirtableRecordId}\` |
| CREATE fields ok | ${preflight.createFieldsOk} |
| UPDATE fields ok | ${preflight.updateFieldsOk} |
| UPDATE material | ${preflight.updateMaterialChanged} |
| Stop reasons | ${stopReasons.length ? stopReasons.join(", ") : "none"} |
| Ready | **${preflight.ok}** |

Auth: \`${AUTH}\`
`,
  );

  if (!preflight.ok) {
    write("EXECUTION_BLOCKED.json", { preflight, applyRequested: APPLY });
    console.error("PREFLIGHT_BLOCKED — no writes applied");
    process.exitCode = 2;
    return;
  }

  const opportunities = [createOpp, updateOpp].filter(Boolean);
  for (const opp of opportunities) {
    const key = buildHotelResearchCompositeKey(HOTEL_ID, opp.opportunityId);
    if (!ALLOWED.has(key)) {
      throw new Error(`unauthorized_identity:${key}`);
    }
  }

  const shouldApply =
    APPLY &&
    AUTHORIZE &&
    EXACT_AUTH === AUTH &&
    preflight.ok;

  const prevEnv = process.env.GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES;
  if (shouldApply) {
    process.env.GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES = "1";
  }

  console.log(
    `[execute] apply=${shouldApply} auth=${EXACT_AUTH || "(none)"} budget=2`,
  );
  let execution;
  try {
    execution = await persistFullE2eResearchInventory({
      hotelId: HOTEL_ID,
      hotelName: HOTEL_NAME,
      runId: "bethesda_two_write_production_pilot_v1",
      repoRoot: ROOT,
      researchCompleted: true,
      apply: shouldApply,
      authorizeResearchPersistence: shouldApply,
      exactWriteSetAuthorization: shouldApply ? AUTH : "",
      writeBudget: 2,
      readBack: shouldApply,
      opportunities,
    });
  } finally {
    if (prevEnv === undefined) {
      delete process.env.GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES;
    } else {
      process.env.GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES = prevEnv;
    }
  }

  write("PRODUCTION_WRITE_RESULTS.json", execution);

  const readBack = [];
  for (const row of execution.rows || []) {
    if (!row.airtableRecordId) continue;
    try {
      const rb = await readBackResearchInventory(row.airtableRecordId);
      readBack.push({
        opportunityId: row.opportunityId,
        action: row.action,
        airtableRecordId: rb.id,
        compositeKey: rb.compositeKey,
        matched:
          rb.compositeKey === `${HOTEL_ID}::${row.opportunityId}` &&
          rb.opportunityId === row.opportunityId &&
          rb.hotelId === HOTEL_ID,
        sourceSummaryHasObjectObject: String(
          rb.fields?.[F.sourceSummary] || "",
        ).includes("[object Object]"),
        firstDiscoveredAt: rb.fields?.[F.firstDiscoveredAt] || null,
        firstDiscoveredRunId: rb.fields?.[F.firstDiscoveredRunId] || null,
        researchLifecycleStatus: rb.fields?.[F.researchLifecycleStatus] || null,
        commercialEligibilityStatus:
          rb.fields?.[F.commercialEligibilityStatus] || null,
        payloadPresent: Boolean(rb.fields?.[F.researchPayloadJson]),
      });
    } catch (err) {
      readBack.push({
        opportunityId: row.opportunityId,
        error: String(err?.message || err),
        matched: false,
      });
    }
  }
  write("AIRTABLE_READ_BACK.json", { count: readBack.length, rows: readBack });

  console.log("[idempotency] dry-run same two identities…");
  const idem = await persistFullE2eResearchInventory({
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    runId: "bethesda_two_write_idempotency_v1",
    repoRoot: ROOT,
    researchCompleted: true,
    apply: false,
    authorizeResearchPersistence: false,
    writeBudget: 2,
    opportunities,
  });
  write("IDEMPOTENCY_VERIFICATION.json", {
    counts: idem.counts,
    rows: idem.rows,
    expectedAfterSuccessfulApply: {
      UNCHANGED: 2,
      WOULD_CREATE: 0,
      WOULD_UPDATE: 0,
      CREATED: 0,
      UPDATED: 0,
    },
  });

  console.log("[drift] analyzing prior 63 WOULD_UPDATE with corrected compare…");
  const drift = await analyzeSixtyThreeDrift();
  write("SIXTY_THREE_DRIFT_ROOT_CAUSE.json", drift);
  write(
    "SIXTY_THREE_DRIFT_ROOT_CAUSE.md",
    `# 63 WOULD_UPDATE root cause

## Prior pilot observation

Full FS bag dry-run reported **63 WOULD_UPDATE** / **23 UNCHANGED**.

## Root cause

Bridge dry-runs injected \`firstDiscoveredRunId: <currentRunId>\` into \`researchPayloadJson\` when the opportunity lacked that key. Typed Airtable provenance columns are the SoT for first discovery; most live payloads did **not** contain \`firstDiscoveredRunId\`. Material compare then treated the injected stamp as a content change.

| Signal | Count (prior) |
|--------|--------------:|
| researchPayloadJson | ~61 |
| sourceSummary | ~2 |

Classification: **operational provenance stamp false positive** — not 61 genuinely material research improvements, and not authorization to overwrite Airtable from FS.

## Correction applied

\`PAYLOAD_VOLATILE_KEYS\` now excludes \`firstDiscoveredRunId\`, \`firstDiscoveredAt\`, and \`firstSeenRunId\` from payload material compare (in addition to \`lastResearchedAt\` / \`runId\` / etc.).

## Post-correction bag dry-run

\`\`\`json
${JSON.stringify(drift.bridgeCounts, null, 2)}
\`\`\`

Would-update remaining: **${drift.wouldUpdateCount}**

Field hits: ${JSON.stringify(drift.fieldHits)}

**These residual updates remain UNAPPLIED.** Do not replace newer Airtable information with older filesystem snapshots.
`,
  );

  write("GDO_PUBLICATION_ISOLATION.json", {
    gdoWrites: execution.gdoWrites || 0,
    publicationWrites: execution.publicationWrites || 0,
    rockCreekWrites: 0,
    adpWrites: 0,
    researchTableOnly: true,
    recurringPersistenceEnabled: false,
    envGateRestoredOff:
      String(process.env.GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES || "") !== "1",
  });

  write(
    "CONTROLLED_EXA_DISCOVERY_PROPOSAL.md",
    `# Controlled Exa discovery proposal (Bethesda) — NOT AUTHORIZED TO SPEND

## Objective

Smallest paid Exa exercise that meaningfully evaluates recently expanded demand segments after the zero-cost pilot (14/14 executors invoked, 0 fresh Exa retrievals).

## Scope

Hotel: Bethesda Marriott (\`${HOTEL_ID}\`)  
Taxonomy: canonical 14 segments (unchanged)  
Mode: discovery-only; research inventory persistence remain founder-gated; no GDO/publication.

## Priority segments (paid)

| Segment | Why | Suggested queries | Max provider calls | Budget USD |
|---------|-----|-------------------|-------------------:|-----------:|
| corporate_meetings_incentives | Forward-looking meetings/training near NIH/Marriott HQ | 4 | 4 | 8 |
| corporate_accounts_scaling | Scaling / HQ moves (Tetracore/Hansoh class) | 4 | 4 | 8 |
| projects_infrastructure | Workforce lodging from corridor projects | 3 | 3 | 6 |
| education_scientific | Medical/pharma/scientific training beyond known PDA | 4 | 4 | 8 |
| relocation_extended_stay | Extended-stay from corp relocations | 3 | 3 | 6 |
| preferred_lodging_partnerships | NIH/Strathmore/hospital preferred lists | 3 | 3 | 6 |
| professional_event_planners | Local planner / 3rd-party housing owners | 3 | 3 | 6 |
| government_public_sector | Federal/local public-sector demand | 3 | 3 | 6 |

Suggested ceiling: **≤ $54** for the 8 priority segments (or **≤ $80** if founder expands to all 14 at light depth).

Non-priority segments (assoc/sports/social/local catering/celebrations/travel trade) may stay historical/public-only in this exercise unless budget expands.

## Hotel-specific research objectives

1. Net-new dated lodging-relevant demand within ~12–24 months.
2. Named commercial entry points (housing owner, meetings owner, planner).
3. Evidence URLs that survive drawer attribution.
4. Dedup against existing Bethesda research identities / shared \`dgGeneratorId\`.
5. Qualification honesty: WATCH / RESEARCH_CONTINUATION vs sales-ready.

## Commercial-quality acceptance criteria

- ≥1 research-continuation candidate with CONFIRMED or RELIABLE_INFERRED demand thesis **or** clear BLOCKED_EMPTY with retrieval proof.
- 0 fabricated opportunities.
- 0 automatic production persistence.
- Duplicate composite keys in-run: 0.
- Sales-ready count reported separately (may be 0).

## Authorization required before spend

Separate founder approval for paid Exa budget + \`--authorize-paid-discovery\`. This document does **not** authorize spend.
`,
  );

  const created = (execution.rows || []).filter((r) => r.action === "CREATED").length;
  const updated = (execution.rows || []).filter((r) => r.action === "UPDATED").length;
  const wouldCreate = (execution.rows || []).filter((r) => r.action === "WOULD_CREATE").length;
  const wouldUpdate = (execution.rows || []).filter((r) => r.action === "WOULD_UPDATE").length;

  write(
    "TEST_RESULTS.md",
    `# Test results

| Check | Result |
|-------|--------|
| Preflight ok | ${preflight.ok} |
| Applied | ${shouldApply} |
| CREATED | ${created} |
| UPDATED | ${updated} |
| WOULD_CREATE (if dry) | ${wouldCreate} |
| WOULD_UPDATE (if dry) | ${wouldUpdate} |
| Read-back matched | ${readBack.filter((r) => r.matched).length}/${readBack.length} |
| Idempotency UNCHANGED | ${idem.counts?.UNCHANGED ?? "n/a"} |
| GDO writes | ${execution.gdoWrites || 0} |
| Publication writes | ${execution.publicationWrites || 0} |
| 63 mass updates applied | **false** |
| Post-fix bag WOULD_UPDATE | ${drift.wouldUpdateCount} |
`,
  );

  write(
    "FOUNDER_NEXT_APPROVAL.md",
    `# Founder next approval

## Completed (this pilot)

- Exact two-record research inventory write test (${shouldApply ? "APPLIED" : "DRY-RUN ONLY"}).
- 63-record drift explained; mass updates **not** applied.
- Comparison defect corrected (\`firstDiscoveredRunId\` volatile).
- Recurring persistence remains **OFF**.

## Still requires separate approval

1. Any additional research inventory CREATE/UPDATE beyond the two authorized identities.
2. Mass reconciliation of residual FS↔Airtable diffs.
3. Paid Exa controlled discovery budget (see \`CONTROLLED_EXA_DISCOVERY_PROPOSAL.md\`).
4. Enabling \`GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES\` for recurring Full E2E persistence.

**STOP for founder review.**
`,
  );

  write(
    "FOUNDER_EXECUTIVE_SUMMARY.md",
    `# Founder executive summary — Bethesda two-write production pilot

| Question | Answer |
|----------|--------|
| CREATE applied? | **${created === 1 ? "YES" : shouldApply ? "NO/UNEXPECTED" : "DRY-RUN ONLY"}** \`${CREATE_KEY}\` |
| UPDATE applied? | **${updated === 1 ? "YES" : shouldApply ? "NO/UNEXPECTED" : "DRY-RUN ONLY"}** \`${UPDATE_KEY}\` |
| Read-back ok? | ${readBack.every((r) => r.matched) && readBack.length >= (shouldApply ? 2 : 0)} |
| Idempotent? | UNCHANGED=${idem.counts?.UNCHANGED}, WOULD_CREATE=${idem.counts?.WOULD_CREATE}, WOULD_UPDATE=${idem.counts?.WOULD_UPDATE} |
| 63 updates applied? | **No** |
| 63 root cause? | Bridge injected \`firstDiscoveredRunId\` into payloads → false material diffs (~61); +2 sourceSummary. Fix shipped. |
| Post-fix residual WOULD_UPDATE | **${drift.wouldUpdateCount}** (unapplied) |
| GDO / publication / Rock Creek | **0 / 0 / 0** |
| Recurring persistence | **OFF** |
| Next paid Exa | Proposal ready — **not spent** |

Auth: \`${AUTH}\`
`,
  );

  console.log(
    JSON.stringify(
      {
        ok: shouldApply
          ? created === 1 &&
            updated === 1 &&
            (execution.gdoWrites || 0) === 0 &&
            readBack.filter((r) => r.matched).length === 2
          : preflight.ok && !shouldApply,
        shouldApply,
        created,
        updated,
        idemUnchanged: idem.counts?.UNCHANGED,
        driftWouldUpdate: drift.wouldUpdateCount,
        outDir: OUT,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
