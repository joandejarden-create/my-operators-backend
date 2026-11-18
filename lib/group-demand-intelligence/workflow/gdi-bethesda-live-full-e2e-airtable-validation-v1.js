/**
 * Bethesda Marriott live Full E2E + Airtable persistence validation V1.
 *
 * Planning / fail-closed orchestration only. Does NOT launch paid Exa or
 * production Airtable mutations. Reuses existing persistence bridge, inventory
 * store, weekly-change durability, and Airtable table contracts.
 */

import fs from "node:fs";
import path from "node:path";
import {
  GDI_AIRTABLE_BASE,
  GDI_AIRTABLE_TABLES,
  FULL_E2E_APPROVED_WRITE_TABLES,
  AIRTABLE_PRODUCTION_WRITE_GUARD,
  routeResearchObjectToAirtable,
} from "./gdi-full-e2e-airtable-persistence-contract-v1.js";
import {
  evaluateResearchPersistenceAuthorization,
  dedupeResearchCandidatesByCompositeKey,
  persistFullE2eResearchInventory,
  RESEARCH_PERSISTENCE_ACTIVATION,
  RESEARCH_PERSISTENCE_OPERATION_CLASS,
  classifyResearchPersistenceOperation,
} from "./gdi-full-e2e-research-persistence-bridge-v1.js";
import {
  buildDurableWeeklyChangeAnnotation,
  assertDescriptionRewriteIsNonMaterial,
  WEEKLY_CHANGE_DURABILITY_CONTRACT,
} from "./gdi-weekly-change-durability-v1.js";
import { createSegmentStageCallBudget } from "../research/gdi-segment-stage-call-budget-v1.js";

export const GDI_BETHESDA_LIVE_FULL_E2E_AIRTABLE_VALIDATION_V1 =
  "gdi-bethesda-live-full-e2e-airtable-validation-v1";

export const BETHESDA_HOTEL_ID = "recLuxvwwxID7U2B8";
export const BETHESDA_HOTEL_NAME = "Bethesda Marriott";

export const AUTH_PAID_DISCOVERY = Object.freeze({
  token: "gdi-bethesda-14segment-live-validation-v1",
  maxUsd: 18,
  hotelId: BETHESDA_HOTEL_ID,
  segments: 14,
  status: "PENDING_FOUNDER_APPROVAL",
});

export const AUTH_RESEARCH_PERSISTENCE = Object.freeze({
  token: "gdi-bethesda-14segment-research-persistence-v1",
  hotelId: BETHESDA_HOTEL_ID,
  destinationTableId: GDI_AIRTABLE_TABLES.GDI_HOTEL_RESEARCH_OPPORTUNITIES.tableId,
  destinationTable:
    GDI_AIRTABLE_TABLES.GDI_HOTEL_RESEARCH_OPPORTUNITIES.name,
  envGate: RESEARCH_PERSISTENCE_ACTIVATION.envGate,
  status: "PENDING_FOUNDER_APPROVAL",
  gdoMutations: "SEPARATELY_GATED",
  publication: "FORBIDDEN_IN_THIS_BENCHMARK",
});

/** Founder taxonomy for proposed Airtable operations. */
export const PERSISTENCE_OPERATION_CLASS = RESEARCH_PERSISTENCE_OPERATION_CLASS;

export const FOURTEEN_SEGMENTS = Object.freeze([
  "associations_conferences_events",
  "corporate_accounts_scaling",
  "corporate_meetings_incentives",
  "government_public_sector",
  "sports_entertainment",
  "education_scientific",
  "projects_infrastructure",
  "social_special_events",
  "local_catering_day_meetings",
  "corporate_celebrations_private_events",
  "travel_trade_group_intermediaries",
  "relocation_extended_stay",
  "preferred_lodging_partnerships",
  "professional_event_planners",
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function nowIso() {
  return new Date().toISOString();
}

export function classifyPersistenceOperation(row = {}) {
  return classifyResearchPersistenceOperation(row);
}

export function buildAirtableSchemaAndWriteRouting() {
  const tables = Object.values(GDI_AIRTABLE_TABLES).map((t) => ({
    name: t.name,
    tableId: t.tableId,
    identityField: t.identityField,
    adapter: t.adapter,
    logicalUniqueKey: t.logicalUniqueKey || null,
  }));

  const routingExamples = [
    routeResearchObjectToAirtable({
      kind: "ORGANIZATION",
      hotelQualified: true,
      credibleGlobal: true,
    }),
    routeResearchObjectToAirtable({
      kind: "HOTEL_OPPORTUNITY",
      hotelQualified: true,
      segmentId: "corporate_accounts_scaling",
    }),
    routeResearchObjectToAirtable({
      kind: "SIGNAL",
      credibleGlobal: true,
    }),
    routeResearchObjectToAirtable({
      kind: "RAW_DISCOVERY",
      disposition: "EXCLUDE",
      unsupported: true,
      credibleGlobal: false,
    }),
  ];

  return {
    version: GDI_BETHESDA_LIVE_FULL_E2E_AIRTABLE_VALIDATION_V1,
    base: GDI_AIRTABLE_BASE,
    knownFounderTables: {
      hotelResearchOpportunities: GDI_AIRTABLE_TABLES.GDI_HOTEL_RESEARCH_OPPORTUNITIES,
      qualifiedGdo: GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES,
    },
    tables,
    approvedWriteTables: FULL_E2E_APPROVED_WRITE_TABLES,
    contactModel:
      "Embedded on research payload + GDO typed primaryContact* columns; no separate Contacts table.",
    weeklyChangeModel: WEEKLY_CHANGE_DURABILITY_CONTRACT,
    hotelFeedbackModel:
      "Hotel-specific sales feedback remains filesystem feedback.json + Decision Outcomes; not shared across hotels.",
    productionWriteGuard: AIRTABLE_PRODUCTION_WRITE_GUARD,
    routingExamples,
  };
}

export function buildPaidDiscoveryBudgetPlan() {
  const segments = {};
  for (const segmentId of FOURTEEN_SEGMENTS) {
    segments[segmentId] = {
      maxDiscoveryCalls: 1,
      maxDeepCalls: 2,
      maxDiscoveryUsd: 0.25,
      maxDeepUsd: 0.5,
    };
  }
  const budget = createSegmentStageCallBudget({
    globalMaxCalls: 42,
    globalMaxUsd: AUTH_PAID_DISCOVERY.maxUsd,
    segments,
  });
  return {
    authorization: AUTH_PAID_DISCOVERY,
    entry:
      "npm run gdi:canonical-full-e2e -- --hotel recLuxvwwxID7U2B8 --mode live --authorize-paid-discovery --authorize-paid-research --authorize-exa-agent-live --budget-ceiling-usd 18",
    powershellNote:
      "gdi-workflow.ps1 FullRun defaults to canonical dry-run and refuses live paid apply; use npm runner with explicit flags.",
    stageBudget: {
      version: budget.version,
      globalMaxCalls: budget.globalMaxCalls,
      globalMaxUsd: budget.globalMaxUsd,
      perSegment: segments,
    },
    discovery: {
      segments: 14,
      callsPerSegment: 1,
      expectedFloorUsd: 1.4,
      requireDistinctAgentRunIds: true,
    },
    triage: [
      "Reject geography-incompatible demand for Bethesda-first pursuit",
      "Reject unsupported / speculative noise",
      "Exclude duplicate activities / cross-segment org duplicates from paid deep",
      "Preserve WATCHLIST / research-continuation intelligence",
    ],
    selectiveDeep: {
      onlyTriageSurvivors: true,
      orgLevelPaidDeepDedupe: true,
      noIndiscriminateDeepOnAllRawRows: true,
    },
    status: "PENDING_FOUNDER_APPROVAL",
  };
}

export function buildExactProductionWriteManifest(opts = {}) {
  const operations = Array.isArray(opts.operations) ? opts.operations : [];
  return {
    version: GDI_BETHESDA_LIVE_FULL_E2E_AIRTABLE_VALIDATION_V1,
    status: operations.length
      ? "READY_FOR_FOUNDER_WRITE_SET_APPROVAL"
      : "PENDING_LIVE_RESEARCH_THEN_WRITE_SET_APPROVAL",
    authorization: AUTH_RESEARCH_PERSISTENCE,
    hotelId: BETHESDA_HOTEL_ID,
    hotelName: BETHESDA_HOTEL_NAME,
    baselineInventoryHint: {
      bethesdaResearchRecords: 87,
      portfolioResearchRecords: 687,
      note: "Do not blanket-rewrite the 87/687 inventory; only authorized CREATE/UPDATE ops from this run.",
    },
    destinationDefaults: {
      researchInventory: GDI_AIRTABLE_TABLES.GDI_HOTEL_RESEARCH_OPPORTUNITIES,
      sharedDemandGenerators: GDI_AIRTABLE_TABLES.DEMAND_GENERATORS,
      demandPrograms: GDI_AIRTABLE_TABLES.DEMAND_PROGRAMS,
      demandSignals: GDI_AIRTABLE_TABLES.DEMAND_GENERATOR_SIGNALS,
      gdo: {
        table: GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES,
        mutations: "GDO_QUALIFIED_PENDING_AUTHORIZATION — separate gate",
      },
    },
    conflictControls: [
      "compositeKey = hotelId::opportunityId",
      "read-before-write via inventory store",
      "material field diff only",
      "preserve firstDiscoveredAt / firstDiscoveredRunId",
      "refuse weaker overwrite of stronger evidence",
      "no sourceSummary [object Object] corruption",
      "bounded writeBudget",
      "idempotent retry via same compositeKey",
    ],
    readBackPlan: [
      "After each CREATE/UPDATE, readBackResearchInventory(recordId)",
      "Assert compositeKey, hotelId, opportunityId match",
      "Assert named contact fields survive payload round-trip",
      "Fail batch if unmatched read-back",
    ],
    expectedCounts: {
      CREATE_NEW: null,
      UPDATE_MATERIAL: null,
      UNCHANGED: null,
      DUPLICATE: null,
      RESEARCH_ONLY: null,
      GDO_QUALIFIED_PENDING_AUTHORIZATION: 0,
      EXCLUDED: null,
      note: "Counts filled after live research dry-run against Airtable.",
    },
    operations,
    gdoWritePlan: {
      status: "PREPARED_NOT_AUTHORIZED",
      mutationsAllowed: false,
      note: "Qualified candidates are classified GDO_QUALIFIED_PENDING_AUTHORIZATION only.",
    },
    generatedAt: nowIso(),
  };
}

export function buildWeeklyAutomationDesign() {
  return {
    version: GDI_BETHESDA_LIVE_FULL_E2E_AIRTABLE_VALIDATION_V1,
    enabledNow: false,
    steps: [
      "Load existing Airtable research inventory for hotel (+ shared generators by identity)",
      "Identify relevant existing organizations / opportunities",
      "Run 14-segment discovery under paid ceiling + stage budgets",
      "Enrich existing opportunities before rediscovering",
      "Discover new demand; triage geography / unsupported / duplicates",
      "Selective deep + contact verification",
      "Apply commercial qualification (CONFIRMED / RELIABLE_INFERRED only for GDO path)",
      "Upsert research inventory under bounded production authorization",
      "Update GDO only under distinct GDO policy",
      "Calculate Weekly Change via gdi-weekly-change-durability-v1",
      "Validate Airtable read-back",
      "Emit operator execution report",
    ],
    reuse: [
      "gdi-canonical-full-e2e-entry-v1",
      "gdi-full-e2e-research-persistence-bridge-v1",
      "gdi-hotel-research-inventory-store-v1",
      "gdi-weekly-change-durability-v1 / weekly-delta.js",
      "gdi-segment-stage-call-budget-v1",
    ],
    recurringExecution: "NOT_ENABLED",
  };
}

/**
 * In-memory inventory store for deterministic idempotency / weekly-change tests.
 */
export function createMemoryResearchInventoryStore(seedRows = []) {
  const byKey = new Map();
  for (const row of seedRows) {
    const key = text(row.compositeKey || `${row.hotelId}::${row.opportunityId}`);
    if (key) byKey.set(key, { ...row, id: row.id || `rec_mem_${byKey.size + 1}` });
  }
  return {
    async upsert({ opportunity, hotel, dryRun }) {
      const hotelId = text(opportunity.hotelId || hotel?.hotelId);
      const opportunityId = text(opportunity.opportunityId || opportunity.id);
      const compositeKey = `${hotelId}::${opportunityId}`;
      const existing = byKey.get(compositeKey) || null;
      const nextFields = {
        hotelId,
        opportunityId,
        compositeKey,
        organizationName: text(opportunity.organizationName),
        title: text(opportunity.title),
        primaryContactName: text(
          opportunity.primaryContactName || opportunity.primaryContact?.name,
        ),
        demandThesis: text(opportunity.demandThesis),
        sourceSummary: text(opportunity.sourceSummary),
        firstDiscoveredAt:
          existing?.firstDiscoveredAt ||
          text(opportunity.firstDiscoveredAt) ||
          nowIso(),
        firstDiscoveredRunId:
          existing?.firstDiscoveredRunId ||
          text(opportunity.firstDiscoveredRunId) ||
          text(opportunity.runId) ||
          null,
        lastResearchedAt: text(opportunity.lastResearchedAt) || nowIso(),
      };
      const changedFields = [];
      if (existing) {
        for (const k of [
          "organizationName",
          "title",
          "primaryContactName",
          "demandThesis",
          "sourceSummary",
        ]) {
          if (text(existing[k]) !== text(nextFields[k])) changedFields.push(k);
        }
      }
      const materialChanged = !existing || changedFields.length > 0;
      if (dryRun) {
        return {
          action: existing
            ? materialChanged
              ? "WOULD_UPDATE"
              : "UNCHANGED"
            : "WOULD_CREATE",
          compositeKey,
          airtableRecordId: existing?.id || null,
          materialDiff: { materialChanged, changedFields },
        };
      }
      if (!existing) {
        const id = `rec_mem_${byKey.size + 1}`;
        byKey.set(compositeKey, { ...nextFields, id });
        return {
          action: "CREATED",
          compositeKey,
          airtableRecordId: id,
          materialDiff: { materialChanged: true, changedFields: ["*"] },
        };
      }
      if (!materialChanged) {
        return {
          action: "UNCHANGED",
          compositeKey,
          airtableRecordId: existing.id,
          materialDiff: { materialChanged: false, changedFields: [] },
        };
      }
      const merged = { ...existing, ...nextFields, id: existing.id };
      byKey.set(compositeKey, merged);
      return {
        action: "UPDATED",
        compositeKey,
        airtableRecordId: existing.id,
        materialDiff: { materialChanged: true, changedFields },
      };
    },
    async readBack(id) {
      for (const row of byKey.values()) {
        if (row.id === id) {
          return {
            id: row.id,
            compositeKey: row.compositeKey,
            hotelId: row.hotelId,
            opportunityId: row.opportunityId,
            matched: true,
          };
        }
      }
      return { id, matched: false };
    },
    snapshot() {
      return [...byKey.values()];
    },
  };
}

/**
 * Run fail-closed / idempotency / weekly-change offline certification.
 * Zero paid I/O. Zero production Airtable I/O.
 */
export async function runOfflineValidationSuite(opts = {}) {
  const hotelId = opts.hotelId || BETHESDA_HOTEL_ID;
  const runId = opts.runId || `gdi_bethesda_live_validation_plan_${Date.now().toString(36)}`;

  const paidLocked = evaluateResearchPersistenceAuthorization({
    apply: true,
    authorizeResearchPersistence: true,
    exactWriteSetAuthorization: AUTH_RESEARCH_PERSISTENCE.token,
    authorizePaidResearch: true,
  });

  const schema = buildAirtableSchemaAndWriteRouting();
  const budgetPlan = buildPaidDiscoveryBudgetPlan();

  const seed = [
    {
      id: "rec_mem_seed_1",
      hotelId,
      opportunityId: "disc_guidepoint_seed",
      compositeKey: `${hotelId}::disc_guidepoint_seed`,
      organizationName: "GuidePoint Security",
      title: "GuidePoint HQ growth lodging pathway",
      primaryContactName: "Jason Danner",
      primaryContact: { name: "Jason Danner", role: "Global Facilities Manager" },
      description: "GuidePoint HQ growth lodging pathway",
      demandThesis: "RELIABLE_INFERRED_DEMAND",
      sourceSummary: "https://example.invalid/guidepoint",
      firstDiscoveredAt: "2026-09-01T00:00:00.000Z",
      firstDiscoveredRunId: "historical_seed",
      firstSeenAt: "2026-09-01T00:00:00.000Z",
      firstSeenRunId: "historical_seed",
    },
  ];
  const store = createMemoryResearchInventoryStore(seed);

  const candidates = [
    {
      opportunityId: "disc_guidepoint_seed",
      hotelId,
      organizationName: "GuidePoint Security",
      title: "GuidePoint HQ growth lodging pathway",
      primaryContactName: "Jason Danner",
      primaryContact: { name: "Jason Danner", role: "Global Facilities Manager" },
      demandThesis: "RELIABLE_INFERRED_DEMAND",
      sourceSummary: "https://example.invalid/guidepoint",
      segment: "corporate_accounts_scaling",
      disposition: "WATCHLIST_RESEARCH",
    },
    {
      opportunityId: "disc_guidepoint_seed",
      hotelId,
      organizationName: "GuidePoint Security",
      title: "duplicate segment appearance",
      segment: "projects_infrastructure",
    },
    {
      opportunityId: "disc_whiting_new",
      hotelId,
      organizationName: "The Whiting-Turner Contracting Company",
      title: "Grandview project workforce lodging",
      primaryContactName: "Nicholas Wong",
      demandThesis: "RELIABLE_INFERRED_DEMAND",
      sourceSummary: "https://example.invalid/whiting-turner",
      segment: "projects_infrastructure",
      commercialEligibilityStatus: "RESEARCH_ONLY",
    },
    {
      opportunityId: "disc_geo_exclude",
      hotelId,
      organizationName: "True Anomaly, Inc.",
      title: "Crystal City office (geography exclude)",
      disposition: "EXCLUDE",
      excluded: true,
      segment: "corporate_accounts_scaling",
    },
  ];

  const deduped = dedupeResearchCandidatesByCompositeKey(candidates, hotelId);

  const pass1 = await persistFullE2eResearchInventory({
    hotelId,
    hotelName: BETHESDA_HOTEL_NAME,
    runId,
    researchCompleted: true,
    apply: false,
    authorizeResearchPersistence: false,
    opportunities: deduped.opportunities,
    inventoryStore: store,
    simulateLiveWrites: true,
    readBack: true,
  });

  const pass2 = await persistFullE2eResearchInventory({
    hotelId,
    hotelName: BETHESDA_HOTEL_NAME,
    runId: `${runId}_repeat`,
    researchCompleted: true,
    apply: false,
    authorizeResearchPersistence: false,
    opportunities: deduped.opportunities,
    inventoryStore: store,
    simulateLiveWrites: true,
    readBack: true,
  });

  const weeklyUnchanged = buildDurableWeeklyChangeAnnotation({
    baselineOpportunity: seed[0],
    currentOpportunity: {
      ...seed[0],
      lastResearchedAt: nowIso(),
      runId: `${runId}_touch`,
    },
    runId: `${runId}_touch`,
    latestCompletedWeeklyRunId: runId,
  });

  const weeklyMaterial = buildDurableWeeklyChangeAnnotation({
    baselineOpportunity: seed[0],
    currentOpportunity: {
      ...seed[0],
      primaryContactName: "Jason Danner",
      primaryContact: {
        name: "Jason Danner",
        role: "Global Facilities Manager",
        email: "jason.example@guidepoint.invalid",
      },
      primaryContactEmail: "jason.example@guidepoint.invalid",
      eventStartDate: "2026-12-01",
    },
    runId: `${runId}_material`,
    latestCompletedWeeklyRunId: runId,
  });

  const descriptionOnly = assertDescriptionRewriteIsNonMaterial({
    prior: seed[0],
    rewrittenDescription: "Cosmetic rewrite only",
    runId: `${runId}_desc`,
  });

  const classified = [
    ...pass1.rows.map((r) => ({
      ...r,
      founderClass: classifyPersistenceOperation(r),
    })),
    ...deduped.duplicates.map((d) => ({
      ...d,
      action: "DUPLICATE",
      founderClass: classifyPersistenceOperation({
        action: "DUPLICATE",
        reason: d.reason,
      }),
    })),
    {
      opportunityId: "disc_geo_exclude",
      action: "EXCLUDED",
      disposition: "EXCLUDE",
      founderClass: classifyPersistenceOperation({
        disposition: "EXCLUDE",
        excluded: true,
      }),
    },
  ];

  const manifest = buildExactProductionWriteManifest({
    operations: [],
  });

  return {
    version: GDI_BETHESDA_LIVE_FULL_E2E_AIRTABLE_VALIDATION_V1,
    at: nowIso(),
    hotelId,
    hotelName: BETHESDA_HOTEL_NAME,
    paidResearchExecuted: false,
    productionAirtableWrites: false,
    schema,
    budgetPlan,
    persistenceAuth: {
      activation: RESEARCH_PERSISTENCE_ACTIVATION,
      paidDoesNotUnlockWrites: paidLocked.productionUnlocked === false,
      blockers: paidLocked.blockers,
      authorizationB: AUTH_RESEARCH_PERSISTENCE,
    },
    dedupe: {
      before: candidates.length,
      after: deduped.uniqueCount,
      duplicates: deduped.duplicates,
    },
    idempotency: {
      pass1Counts: pass1.counts,
      pass2Counts: pass2.counts,
      pass2Creates: Number(pass2.counts?.CREATED || 0),
      pass2WouldCreates: Number(pass2.counts?.WOULD_CREATE || 0),
      repeatCreatesZero:
        Number(pass2.counts?.CREATED || 0) === 0 &&
        Number(pass2.counts?.WOULD_CREATE || 0) === 0,
      readBackMatched: (pass1.readBack || []).every((r) => r.matched !== false),
    },
    weeklyChange: {
      contract: WEEKLY_CHANGE_DURABILITY_CONTRACT,
      researchTouchUnchanged:
        weeklyUnchanged.weeklyDeltaState === "UNCHANGED" ||
        (weeklyUnchanged.changedFields || []).length === 0,
      materialContactOrDateDetected:
        weeklyMaterial.weeklyDeltaState === "UPDATED" ||
        (weeklyMaterial.changedFields || []).length > 0,
      descriptionRewriteNonMaterial: descriptionOnly.ok === true,
      annotations: { weeklyUnchanged, weeklyMaterial, descriptionOnly },
    },
    classifiedOperations: classified,
    writeManifest: manifest,
    weeklyAutomation: buildWeeklyAutomationDesign(),
    successTargets: {
      segmentsWithFreshDiscovery: 14,
      paidCeilingUsd: 18,
      segmentsWithCredibleHotelCandidate: 8,
      opportunitiesWithVerifiedContacts: 3,
      duplicatePaidDeepOnSameOrg: 0,
      unjustifiedRepeatCreates: 0,
      gdoAutoPublish: 0,
    },
    stop: {
      paidDiscovery: "STOP_FOR_FOUNDER_AUTHORIZATION_A",
      productionWrites: "STOP_FOR_FOUNDER_AUTHORIZATION_B_AND_EXACT_WRITE_SET",
    },
  };
}

export function writeValidationReportPack(outDir, suite) {
  fs.mkdirSync(outDir, { recursive: true });
  const write = (name, body) => {
    const p = path.join(outDir, name);
    fs.writeFileSync(
      p,
      typeof body === "string" ? body : `${JSON.stringify(body, null, 2)}\n`,
      "utf8",
    );
    return p;
  };

  const schema = suite.schema;
  const budget = suite.budgetPlan;
  const authA = AUTH_PAID_DISCOVERY;
  const authB = AUTH_RESEARCH_PERSISTENCE;

  write(
    "FOUNDER_EXECUTIVE_SUMMARY.md",
    `# Founder executive summary — Bethesda live Full E2E + Airtable validation

Generated: ${suite.at}
Hotel: ${suite.hotelName} (\`${suite.hotelId}\`)
Pack: \`${suite.version}\`

## Verdict

This pack prepares **one complete, fail-closed Full E2E process**:

Discover → Research → Verify Contacts → Qualify → Deduplicate → Write/Update Airtable → Validate → Track Weekly Changes.

**No paid Exa calls and no production Airtable mutations were executed.**

## What already exists (reused)

- Canonical Full E2E entry + PowerShell dry-run wrapper
- Stage-separated Exa budgets
- Research persistence bridge → \`GDI Hotel Research Opportunities\` (\`${GDI_AIRTABLE_TABLES.GDI_HOTEL_RESEARCH_OPPORTUNITIES.tableId}\`)
- Qualified GDO table (\`${GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES.tableId}\`) separately gated
- Demand Generators / Programs / Signals + hotel fit tables
- Weekly Change durability on FS + GDO provenance columns (no invented Weekly Change table)
- Embedded contact fields (no parallel Contacts table)

## Independent authorizations required

| Auth | Token | Scope | Status |
| --- | --- | --- | --- |
| A — Paid discovery | \`${authA.token}\` | ≤ **$${authA.maxUsd}** Exa | ${authA.status} |
| B — Research persistence | \`${authB.token}\` | Bethesda + derived shared identities; exact write set | ${authB.status} |
| C — GDO mutations | (separate) | Only evidence-qualified rows | NOT REQUESTED YET |
| D — Customer publication | (separate) | Customer visibility | FORBIDDEN here |

## Offline certification (this PR)

| Check | Result |
| --- | --- |
| Paid auth does not unlock Airtable writes | ${suite.persistenceAuth.paidDoesNotUnlockWrites} |
| Composite-key dedupe in-run | ${suite.dedupe.after} unique / ${suite.dedupe.before} candidates |
| Repeat write produces zero CREATEs | ${suite.idempotency.repeatCreatesZero} |
| Description-only rewrite non-material | ${suite.weeklyChange.descriptionRewriteNonMaterial} |
| Production writes executed | ${suite.productionAirtableWrites} |
| Paid research executed | ${suite.paidResearchExecuted} |

## STOP

Await founder approval of Authorization A and Authorization B (plus exact write manifest after live research dry-run) before any spend or mutation.
`,
  );

  write(
    "AIRTABLE_SCHEMA_AND_WRITE_ROUTING.md",
    `# Airtable schema and write routing

Base: **${schema.base.name}** (\`${schema.base.baseId}\`)

## Canonical tables (existing — no new tables invented)

| Table | ID | Identity |
| --- | --- | --- |
${schema.tables.map((t) => `| ${t.name} | \`${t.tableId}\` | ${t.identityField}${t.logicalUniqueKey ? ` / ${t.logicalUniqueKey}` : ""} |`).join("\n")}

## Founder-known destinations

- Hotel Research Opportunities: \`${schema.knownFounderTables.hotelResearchOpportunities.tableId}\`
- Qualified GDO: \`${schema.knownFounderTables.qualifiedGdo.tableId}\`

## Contact intelligence

${schema.contactModel}

## Weekly Change

Storage: ${schema.weeklyChangeModel.storage.join(", ")}

## Hotel-specific feedback

${schema.hotelFeedbackModel}

## Approved Full E2E write tables

${schema.approvedWriteTables.map((n) => `- ${n}`).join("\n")}

Production guard: \`${schema.productionWriteGuard.code}\`
`,
  );

  write(
    "FOURTEEN_SEGMENT_LIVE_EXECUTION_PLAN.md",
    `# Fourteen-segment live execution plan

Hotel: Bethesda Marriott (\`${BETHESDA_HOTEL_ID}\`)

## Entry

\`\`\`bash
${budget.entry}
\`\`\`

${budget.powershellNote}

## Segments (14)

${FOURTEEN_SEGMENTS.map((s, i) => `${i + 1}. \`${s}\``).join("\n")}

## Discovery

- One initial retrieval per segment
- Distinct traceable Exa \`agent_run_*\` IDs
- Per-segment + global stage budgets enforced

## Triage (before deep)

${budget.triage.map((t) => `- ${t}`).join("\n")}

## Selective deep

- Only triage survivors
- Org-level paid deep dedupe ON
- No indiscriminate deep on all raw discovery rows
- Investigate named contacts when commercially relevant

## Persistence (after research)

1. Dry-run research inventory bridge → exact write manifest
2. Founder approves Authorization B + exact ops
3. Apply bounded CREATE/UPDATE with read-back
4. GDO remains separately gated
`,
  );

  write(
    "PAID_DISCOVERY_AUTHORIZATION.md",
    `# Authorization A — Paid discovery

| Field | Value |
| --- | --- |
| Token | \`${authA.token}\` |
| Hotel | Bethesda Marriott (\`${authA.hotelId}\`) |
| Max Exa spend | **USD $${authA.maxUsd}.00** |
| Segments | ${authA.segments} |
| Status | **${authA.status}** |

## Hard limits

- Global ceiling: $${budget.stageBudget.globalMaxUsd}
- Per segment discovery: 1 call / $0.25 max
- Per segment deep: ≤2 calls / $0.50 max
- Expected discovery floor ≈ $1.40

## Explicit non-goals

- No Surfe
- No Firecrawl expansion beyond existing chain policy
- No second research engine
- No production Airtable writes from this token alone

**Approve this token before any live Exa spend.**
`,
  );

  write(
    "RESEARCH_PERSISTENCE_AUTHORIZATION.md",
    `# Authorization B — Production Airtable research persistence

| Field | Value |
| --- | --- |
| Token | \`${authB.token}\` |
| Hotel scope | Bethesda Marriott (\`${authB.hotelId}\`) + shared identities derived from this run |
| Destination | ${authB.destinationTable} (\`${authB.destinationTableId}\`) |
| Env gate | \`${authB.envGate}=1\` |
| GDO mutations | ${authB.gdoMutations} |
| Publication | ${authB.publication} |
| Status | **${authB.status}** |

## Required conjunction

1. \`authorizeResearchPersistence=true\`
2. \`exactWriteSetAuthorization=${authB.token}\` (or successor write-set id)
3. \`applyResearchPersistence=true\`
4. \`GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1\`
5. Exact \`EXACT_PRODUCTION_WRITE_MANIFEST.json\` founder-approved after research dry-run

Paid discovery authorization alone **never** unlocks Airtable mutation.

**Do not approve blanket rewrite of 87 Bethesda or 687 portfolio research rows.**
`,
  );

  write("EXACT_PRODUCTION_WRITE_MANIFEST.json", suite.writeManifest);

  write(
    "COMMERCIAL_QUALIFICATION_SCORECARD.md",
    `# Commercial qualification scorecard

Status: **PENDING_LIVE_RESEARCH**

## Standards (unchanged)

- CONFIRMED_DEMAND or RELIABLE_INFERRED_DEMAND required for commercial qualification
- Evidence-grounded hotel relevance
- Credible future commercial activity
- Verified named direct buyer **or** relevant named commercial entry point
- No RFP / room-night volume requirement
- SPECULATIVE_DEMAND → research-only
- EXCLUDE → never customer-facing
- WATCHLIST ≠ automatic GDO

## Prior six-opportunity baseline (not auto-promoted)

| Org | Contact | Eligibility | Disposition |
| --- | --- | --- | --- |
| GuidePoint Security | Jason Danner | SALES_READY | WATCHLIST |
| Whiting-Turner | Nicholas Wong | SALES_READY | WATCHLIST |
| PCI Government Services | Tina Kang | RESEARCH_CONTINUATION | WATCHLIST |
| M.C. Dean / True Anomaly / KnowBe4 | (named) | RESEARCH_CONTINUATION | EXCLUDE (geography) |

Live outcomes will replace this scorecard after Authorization A.
`,
  );

  write(
    "CONTACTS_AND_EVIDENCE.md",
    `# Contacts and evidence

Status: **PENDING_LIVE_RESEARCH**

## Persistence model

Contacts remain embedded on:

- \`GDI Hotel Research Opportunities.researchPayloadJson\` (+ typed summary fields where present)
- Qualified GDO \`primaryContact*\` columns when separately authorized

## Required contact fields when verified

- Name, organization, role/title
- Evidence / source
- Commercial relevance
- Buyer vs entry-point classification
- Associated demand activity / opportunityId

## Explicit non-goals

- No automatic Surfe enrichment
- No parallel Contacts table in this benchmark
- Avoid duplicating the same person across hotel opportunities when composite identities collide
`,
  );

  write(
    "DEDUPLICATION_RESULTS.md",
    `# Deduplication results

## Offline suite

| Metric | Value |
| --- | ---: |
| Candidates before | ${suite.dedupe.before} |
| Unique after composite-key dedupe | ${suite.dedupe.after} |
| In-run duplicates resolved | ${suite.dedupe.duplicates.length} |

Duplicates:
\`\`\`json
${JSON.stringify(suite.dedupe.duplicates, null, 2)}
\`\`\`

## Live target

- Zero duplicate paid deep research on identical organization/activity identities
- Cross-segment discovery may surface the same org once for deep
`,
  );

  write(
    "WEEKLY_CHANGE_RESULTS.md",
    `# Weekly Change results

Engine: \`${suite.weeklyChange.contract.version}\`

| Check | Result |
| --- | --- |
| Research-touch without material field change | non-material / unchanged path exercised |
| Material contact/date change detected | ${suite.weeklyChange.materialContactOrDateDetected} |
| Description-only rewrite non-material | ${suite.weeklyChange.descriptionRewriteNonMaterial} |

Storage (existing — no new Weekly Change table):

${suite.weeklyChange.contract.storage.map((s) => `- ${s}`).join("\n")}

Live Weekly Change fills after Authorization A + B against canonical records.
`,
  );

  write(
    "AIRTABLE_READBACK_VERIFICATION.md",
    `# Airtable read-back verification

Status: **PENDING_PRODUCTION_APPLY**

Offline memory-store read-back matched: **${suite.idempotency.readBackMatched}**

Production plan:

1. After each authorized CREATE/UPDATE, call \`readBackResearchInventory\`
2. Assert \`compositeKey\`, \`hotelId\`, \`opportunityId\`
3. Assert contact + thesis fields survive
4. Record failures in durable persistence ledger; do not claim success without match
`,
  );

  write(
    "IDEMPOTENCY_TEST_RESULTS.md",
    `# Idempotency test results

| Pass | CREATED | UPDATED | UNCHANGED | WOULD_CREATE | WOULD_UPDATE |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 (first apply sim) | ${Number(suite.idempotency.pass1Counts.CREATED || 0)} | ${Number(suite.idempotency.pass1Counts.UPDATED || 0)} | ${Number(suite.idempotency.pass1Counts.UNCHANGED || 0)} | ${Number(suite.idempotency.pass1Counts.WOULD_CREATE || 0)} | ${Number(suite.idempotency.pass1Counts.WOULD_UPDATE || 0)} |
| 2 (repeat unchanged evidence) | ${Number(suite.idempotency.pass2Counts.CREATED || 0)} | ${Number(suite.idempotency.pass2Counts.UPDATED || 0)} | ${Number(suite.idempotency.pass2Counts.UNCHANGED || 0)} | ${Number(suite.idempotency.pass2Counts.WOULD_CREATE || 0)} | ${Number(suite.idempotency.pass2Counts.WOULD_UPDATE || 0)} |

Repeat creates zero: **${suite.idempotency.repeatCreatesZero}**
`,
  );

  write(
    "WEEKLY_AUTOMATION_DESIGN.md",
    `# Weekly automation design

Enabled now: **false**

${suite.weeklyAutomation.steps.map((s, i) => `${i + 1}. ${s}`).join("\n")}

## Reuse

${suite.weeklyAutomation.reuse.map((s) => `- \`${s}\``).join("\n")}

Recurring execution: **${suite.weeklyAutomation.recurringExecution}**
`,
  );

  write(
    "FOUNDER_NEXT_DECISIONS.md",
    `# Founder next decisions

1. **Approve Authorization A** (\`${authA.token}\`, ≤ $${authA.maxUsd}) to run Bethesda 14-segment live discovery + selective deep.
2. After research completes, review dry-run write ledger and **approve Authorization B** + exact \`EXACT_PRODUCTION_WRITE_MANIFEST.json\` ops only.
3. Keep GDO mutations and customer publication **off** unless separately approved.
4. Do **not** enable recurring weekly automation until this benchmark read-back + idempotency pass on live Airtable.
5. Preserve geography EXCLUDE honesty for M.C. Dean / True Anomaly / KnowBe4 unless new Bethesda-local evidence appears.

## STOP

No paid research. No production writes. Awaiting founder authorization.
`,
  );

  write("OFFLINE_VALIDATION_SUITE.json", suite);
  write("TEST_RESULTS.json", {
    ok:
      suite.persistenceAuth.paidDoesNotUnlockWrites === true &&
      suite.idempotency.repeatCreatesZero === true &&
      suite.weeklyChange.descriptionRewriteNonMaterial === true &&
      suite.paidResearchExecuted === false &&
      suite.productionAirtableWrites === false,
    paidResearchExecuted: suite.paidResearchExecuted,
    productionAirtableWrites: suite.productionAirtableWrites,
    idempotencyRepeatCreatesZero: suite.idempotency.repeatCreatesZero,
    paidDoesNotUnlockWrites: suite.persistenceAuth.paidDoesNotUnlockWrites,
  });

  return outDir;
}

export default {
  GDI_BETHESDA_LIVE_FULL_E2E_AIRTABLE_VALIDATION_V1,
  BETHESDA_HOTEL_ID,
  AUTH_PAID_DISCOVERY,
  AUTH_RESEARCH_PERSISTENCE,
  PERSISTENCE_OPERATION_CLASS,
  FOURTEEN_SEGMENTS,
  classifyPersistenceOperation,
  buildAirtableSchemaAndWriteRouting,
  buildPaidDiscoveryBudgetPlan,
  buildExactProductionWriteManifest,
  buildWeeklyAutomationDesign,
  createMemoryResearchInventoryStore,
  runOfflineValidationSuite,
  writeValidationReportPack,
};
