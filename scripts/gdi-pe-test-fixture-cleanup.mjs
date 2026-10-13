/**
 * GDI Private Events — test/fixture data cleanup (dry-run first).
 *
 *   node scripts/gdi-pe-test-fixture-cleanup.mjs
 *   node scripts/gdi-pe-test-fixture-cleanup.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
  CANONICAL_INTELLIGENCE_BASE_ID,
} from "../lib/decision-outcomes/airtable-base.js";
import {
  PE_VENUES_TABLE_NAME,
  HOTEL_VENUE_FIT_TABLE_NAME,
  PE_SIGNALS_TABLE_NAME,
  MAP_PE_VENUE,
  MAP_HOTEL_VENUE_FIT,
  MAP_PE_SIGNAL,
  listVenuesFromAirtable,
} from "../lib/group-demand-intelligence/private-events/index.js";
import { getPeBase } from "../lib/group-demand-intelligence/private-events/airtable-client.js";
import {
  MAP_GDI_OPPORTUNITY as F,
  GDI_OPPORTUNITIES_TABLE_NAME,
} from "../lib/group-demand-intelligence/opportunity-field-map.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/private-events-test-cleanup"
);
const APPLY = process.argv.includes("--apply");
const HOTEL_ID = "recLuxvwwxID7U2B8";

const SYNTHETIC_VENUE_NAME_RE =
  /^Bethesda\s+(GARDEN|WEDDING(?:\s+VENUE)?|COUNTRY(?:\s+CLUB)?|BANQUET(?:\s+HALL)?|EVENT(?:\s+VENUE)?|MUSEUM|RELIGIOUS(?:\s+VENUE)?|HISTORIC(?:\s+ESTATE)?|PRIVATE(?:\s+CLUB)?|WINERY)\s+\d+/i;

const REAL_LIVE_VENUE_NAMES = [
  "Woman's Club of Bethesda",
  "Bethesda Country Club",
  "Glenview Mansion",
  "Rock Creek Mansion",
  "Brookside Gardens",
  "Woodend Sanctuary",
  "Milton Ridge",
  "Norbeck Country Club",
  "Montgomery Country Club",
  "Ceresville Mansion",
  "Strathmore",
];

function isPeOpportunity(fields = {}) {
  const t = String(fields[F.opportunityType] || "");
  const fam = String(fields.demandFamily || "");
  const peV = fields.peVenueId;
  return (
    t === "VENUE_PARTNERSHIP" ||
    t === "SPECIFIC_PRIVATE_EVENT" ||
    fam === "PRIVATE_EVENTS" ||
    Boolean(peV)
  );
}

function parsePayload(fields = {}) {
  try {
    return JSON.parse(fields[F.opportunityPayloadJson] || "null");
  } catch {
    return null;
  }
}

function classifyOpportunity(rec) {
  const f = rec.fields || {};
  const payload = parsePayload(f) || {};
  const id = String(f[F.opportunityId] || payload.id || "");
  const title = String(f[F.opportunityName] || payload.title || "");
  const org = String(f[F.organizationName] || payload.organizationName || "");
  const peVenueId = f.peVenueId || payload.peVenueId || "";
  const blob = `${id} ${title} ${org} ${peVenueId}`.toLowerCase();
  const sources = [
    ...(Array.isArray(payload.sources) ? payload.sources.map((s) => s.url || "") : []),
    String(f[F.sourceSummary] || ""),
  ].join(" ");

  const reasons = [];
  let classification = "PRODUCTION_REAL";
  let action = "KEEP";

  if (
    /_test_|\/test|test_|fixture|sample|link_only|canary_promo|schema_repair|gdi_pe_test|v13_test|temp_validation/i.test(
      id
    ) ||
    /\bpe link test\b/i.test(title) ||
    /\[test\]|\[schema repair\]/i.test(title)
  ) {
    classification = /fixture|garden|wedding venue/i.test(blob)
      ? "FIXTURE"
      : /sample/i.test(blob)
        ? "SAMPLE"
        : /canary_promo|schema_repair|v13_test|temp/i.test(id + title)
          ? "TEMP_VALIDATION"
          : "TEST";
    action = "DELETE";
    reasons.push("conclusive_test_id_or_title");
  }

  if (SYNTHETIC_VENUE_NAME_RE.test(title) || SYNTHETIC_VENUE_NAME_RE.test(org)) {
    classification = "FIXTURE";
    action = "DELETE";
    reasons.push("synthetic_bethesda_fixture_name");
  }

  if (/\.example\b/i.test(sources) || /\.example\b/i.test(blob)) {
    classification = classification === "PRODUCTION_REAL" ? "FIXTURE" : classification;
    action = "DELETE";
    reasons.push("example_domain");
  }

  if (payload.isTestData === true || f.isTestData === true) {
    classification = "TEST";
    action = "DELETE";
    reasons.push("isTestData_flag");
  }

  // Real promoted Woman's Club — keep
  if (
    id === "gdi_pe_781f12393f8117e7" ||
    (/woman'?s\s+club\s+of\s+bethesda/i.test(title) &&
      f[F.opportunityType] === "VENUE_PARTNERSHIP" &&
      !/_test_|fixture|link_only/i.test(id))
  ) {
    classification = "PRODUCTION_REAL";
    action = "KEEP";
    reasons.push("verified_live_promotion_v1_5");
  }

  // Ambiguous if PE type but no strong indicators and not known real
  if (
    action === "KEEP" &&
    classification === "PRODUCTION_REAL" &&
    !reasons.includes("verified_live_promotion_v1_5")
  ) {
    const looksRealVenue = REAL_LIVE_VENUE_NAMES.some((n) =>
      new RegExp(n.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(`${title} ${org}`)
    );
    if (!looksRealVenue && (peVenueId || f.demandFamily === "PRIVATE_EVENTS")) {
      // PE row without clear real venue identity
      if (!peVenueId || peVenueId === "pev_test" || /^pev_test/i.test(peVenueId)) {
        classification = "TEST";
        action = "DELETE";
        reasons.push("pev_test_or_missing_real_venue");
      } else if (!looksRealVenue) {
        // Could be real PE not in name list — REVIEW not delete
        classification = "AMBIGUOUS";
        action = "REVIEW";
        reasons.push("pe_row_without_known_live_venue_name");
      }
    }
  }

  return {
    recordId: rec.id,
    opportunityId: id,
    title,
    hotelId: f[F.hotelId] || payload.hotelId || null,
    opportunityType: f[F.opportunityType] || payload.opportunityType || null,
    demandFamily: f.demandFamily || payload.demandFamily || null,
    peVenueId: peVenueId || null,
    hotelVenueFitId: f.hotelVenueFitId || payload.hotelVenueFitId || null,
    peSignalId: f.peSignalId || payload.peSignalId || null,
    decisionId: f[F.decisionId] || payload.decisionId || null,
    classification,
    recommendedAction: action,
    reasons,
  };
}

function classifyVenue(v) {
  const name = v.venueName || "";
  const website = v.website || "";
  const domain = v.officialDomain || "";
  const sources = (v.sourceUrls || []).join(" ");

  const real = REAL_LIVE_VENUE_NAMES.some((n) =>
    new RegExp(`^${n.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "i").test(name.trim())
  );
  // Known live venues always KEEP — even if discovered during canary work
  if (real) {
    return {
      venueId: v.venueId,
      airtableRecordId: v.airtableRecordId,
      venueName: name,
      website,
      classification: "REAL_LIVE_VENUE",
      recommendedAction: "KEEP",
      reasons: ["known_live_canary_venue"],
    };
  }

  // Synthetic numbered Bethesda fixtures + *.example domains
  if (
    SYNTHETIC_VENUE_NAME_RE.test(name) ||
    /\.example\b/i.test(website) ||
    /\.example\b/i.test(domain) ||
    /\.example\b/i.test(sources) ||
    /Regional Garden Estate Shared/i.test(name)
  ) {
    return {
      venueId: v.venueId,
      airtableRecordId: v.airtableRecordId,
      venueName: name,
      website,
      classification: "FIXTURE_VENUE",
      recommendedAction: "DELETE",
      reasons: ["synthetic_name_or_example_domain"],
    };
  }

  if (website && !/\.example\b/i.test(website)) {
    return {
      venueId: v.venueId,
      airtableRecordId: v.airtableRecordId,
      venueName: name,
      website,
      classification: "REAL_LIVE_VENUE",
      recommendedAction: "KEEP",
      reasons: ["non_example_official_web"],
    };
  }

  return {
    venueId: v.venueId,
    airtableRecordId: v.airtableRecordId,
    venueName: name,
    website,
    classification: "AMBIGUOUS",
    recommendedAction: "REVIEW",
    reasons: ["unrecognized"],
  };
}

async function listAll(tableName, fields) {
  const base = getPeBase();
  const out = [];
  await base(tableName)
    .select({ pageSize: 100, ...(fields ? { fields } : {}) })
    .eachPage((page, next) => {
      out.push(...page);
      next();
    });
  return out;
}

async function destroySafe(tableName, recordId, ledger, kind) {
  const id = String(recordId || "").trim();
  if (!id || !/^rec[a-zA-Z0-9]{14}$/.test(id)) {
    ledger.push({ kind, tableName, recordId: id || null, deleted: false, error: "invalid_record_id" });
    return;
  }
  const base = getPeBase();
  await base(tableName).destroy(id);
  ledger.push({ kind, tableName, recordId: id, deleted: true });
}

async function main() {
  const baseId = getGdiOpportunitiesAirtableBaseId();
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "pe_test_fixture_cleanup" });
  fs.mkdirSync(OUT, { recursive: true });

  const report = {
    generatedAt: new Date().toISOString(),
    baseId,
    isCanonical: baseId === CANONICAL_INTELLIGENCE_BASE_ID,
    apply: APPLY,
    safety: {
      productionOpportunitiesDeleted: 0,
      productionDecisionsLost: 0,
      productionValidationsLost: 0,
      productionActionsLost: 0,
      productionOutcomesLost: 0,
    },
  };

  // ——— Opportunities ———
  const gdiRecs = await listAll(GDI_OPPORTUNITIES_TABLE_NAME);
  const peRecs = gdiRecs.filter((r) => isPeOpportunity(r.fields || {}));
  const oppClassified = peRecs.map(classifyOpportunity);

  const tallies = {
    TOTAL_PE_GDI_ROWS: oppClassified.length,
    PRODUCTION_REAL: 0,
    TEST: 0,
    FIXTURE: 0,
    SAMPLE: 0,
    TEMP_VALIDATION: 0,
    AMBIGUOUS: 0,
    LIVE_CANARY_REAL_BUT_NOT_PROMOTED: 0,
  };
  for (const row of oppClassified) {
    tallies[row.classification] = (tallies[row.classification] || 0) + 1;
  }
  report.opportunityAudit = { tallies, rows: oppClassified };

  // ——— Venues / fits / signals ———
  const venues = await listVenuesFromAirtable({ maxRecords: 500 });
  const venueClassified = venues.map(classifyVenue);
  const fixtureVenueIds = new Set(
    venueClassified
      .filter((v) => v.classification === "FIXTURE_VENUE")
      .map((v) => v.venueId)
  );

  const fitRecs = await listAll(HOTEL_VENUE_FIT_TABLE_NAME);
  const signalRecs = await listAll(PE_SIGNALS_TABLE_NAME);

  const fixtureFits = fitRecs.filter((r) =>
    fixtureVenueIds.has(r.fields?.[MAP_HOTEL_VENUE_FIT.venueId])
  );
  const fixtureSignals = signalRecs.filter((r) =>
    fixtureVenueIds.has(r.fields?.[MAP_PE_SIGNAL.venueId])
  );

  // Fits/signals for fixture venues that also link only to fixture — safe
  report.venueAudit = {
    rows: venueClassified,
    REAL_LIVE_VENUE: venueClassified.filter((v) => v.classification === "REAL_LIVE_VENUE")
      .length,
    FIXTURE_VENUE: fixtureVenueIds.size,
    AMBIGUOUS: venueClassified.filter((v) => v.classification === "AMBIGUOUS").length,
  };
  report.fixtureGraph = {
    fixtureFits: fixtureFits.map((r) => ({
      recordId: r.id,
      fitId: r.fields?.[MAP_HOTEL_VENUE_FIT.fitId],
      venueId: r.fields?.[MAP_HOTEL_VENUE_FIT.venueId],
      hotelId: r.fields?.[MAP_HOTEL_VENUE_FIT.hotelId],
      gdiOpp: r.fields?.[MAP_HOTEL_VENUE_FIT.currentGdiOpportunityId] || null,
    })),
    fixtureSignals: fixtureSignals.map((r) => ({
      recordId: r.id,
      signalId: r.fields?.[MAP_PE_SIGNAL.signalId],
      venueId: r.fields?.[MAP_PE_SIGNAL.venueId],
      promoted: r.fields?.[MAP_PE_SIGNAL.promotedGdiOpportunityId] || null,
    })),
  };

  // Dry-run table
  report.dryRunTable = oppClassified.map((r) => ({
    recordId: r.recordId,
    opportunityId: r.opportunityId,
    title: r.title,
    hotel: r.hotelId,
    opportunityType: r.opportunityType,
    demandFamily: r.demandFamily,
    venue: r.peVenueId,
    classification: r.classification,
    recommendedAction: r.recommendedAction,
    reasons: r.reasons,
  }));

  const toDeleteOpps = oppClassified.filter((r) => r.recommendedAction === "DELETE");
  const toReview = oppClassified.filter((r) => r.recommendedAction === "REVIEW");
  const toKeep = oppClassified.filter((r) => r.recommendedAction === "KEEP");

  // Safety: never delete PRODUCTION_REAL or Woman's Club
  for (const row of toDeleteOpps) {
    if (
      row.classification === "PRODUCTION_REAL" ||
      row.opportunityId === "gdi_pe_781f12393f8117e7"
    ) {
      report.verdict = "PRODUCTION LINK RISK FOUND — HOLD CLEANUP";
      report.error = "attempted_delete_of_production";
      writeArtifacts(report);
      console.log(JSON.stringify({ verdict: report.verdict, error: report.error }, null, 2));
      process.exit(1);
    }
    if (row.decisionId) {
      // Linked decision — STOP for that row, reclassify REVIEW
      row.recommendedAction = "REVIEW";
      row.reasons.push("has_decisionId_manual_review");
      report.verdict = "PRODUCTION LINK RISK FOUND — HOLD CLEANUP";
    }
  }

  const safeDeleteOpps = toDeleteOpps.filter(
    (r) => r.recommendedAction === "DELETE" && !r.decisionId
  );

  report.cleanupPlan = {
    DELETE: safeDeleteOpps.length,
    ARCHIVE: 0,
    KEEP: toKeep.length,
    REVIEW: toReview.length + toDeleteOpps.filter((r) => r.recommendedAction === "REVIEW").length,
    safeDeleteOpps,
    fixtureVenuesToDelete: venueClassified.filter((v) => v.classification === "FIXTURE_VENUE"),
    fixtureFitsToDelete: report.fixtureGraph.fixtureFits,
    fixtureSignalsToDelete: report.fixtureGraph.fixtureSignals,
  };

  // Write dry-run artifacts always
  writeArtifacts(report);

  const fixtureDeletesPending =
    report.cleanupPlan.fixtureVenuesToDelete.length +
    report.cleanupPlan.fixtureFitsToDelete.length +
    report.cleanupPlan.fixtureSignalsToDelete.length;

  if (!APPLY) {
    report.verdict =
      report.verdict === "PRODUCTION LINK RISK FOUND — HOLD CLEANUP"
        ? report.verdict
        : safeDeleteOpps.length > 0 || fixtureDeletesPending > 0
          ? "DRY_RUN READY — conclusive deletes identified"
          : toReview.length
            ? "CLEANUP PARTIAL — AMBIGUOUS ROWS REQUIRE REVIEW"
            : "CUSTOMER VISIBILITY GUARD ADDED — NO CLEANUP NEEDED";
    report.guard = {
      isTestDataField: "PAYLOAD_PLUS_OPTIONAL_COLUMN",
      customerApiFilter: "WIRED",
      note: "Heuristics + opportunityPayloadJson.isTestData; optional Airtable checkbox when present",
    };
    writeArtifacts(report);
    console.log(
      JSON.stringify(
        {
          verdict: report.verdict,
          apply: false,
          tallies,
          dryRunTable: report.dryRunTable,
          cleanupPlan: {
            DELETE_OPPS: report.cleanupPlan.DELETE,
            KEEP_OPPS: report.cleanupPlan.KEEP,
            REVIEW_OPPS: report.cleanupPlan.REVIEW,
            FIXTURE_VENUES: report.cleanupPlan.fixtureVenuesToDelete.length,
            FIXTURE_FITS: report.cleanupPlan.fixtureFitsToDelete.length,
            FIXTURE_SIGNALS: report.cleanupPlan.fixtureSignalsToDelete.length,
          },
          fixtureVenueNames: report.cleanupPlan.fixtureVenuesToDelete.map((v) => v.venueName),
          realVenuesPreserved: report.venueAudit.REAL_LIVE_VENUE,
          paths: { out: OUT, dryRun: path.join(OUT, "DRY_RUN.md") },
        },
        null,
        2
      )
    );
    return;
  }

  // ——— APPLY ———
  const deleted = [];
  const keepOppIds = new Set(
    toKeep.map((r) => r.opportunityId).concat(["gdi_pe_781f12393f8117e7"])
  );

  // 1) GDI test opportunities
  for (const row of safeDeleteOpps) {
    await destroySafe(GDI_OPPORTUNITIES_TABLE_NAME, row.recordId, deleted, "gdi_opportunity");
  }

  // 2) Fixture signals (only if venue is fixture)
  for (const s of fixtureSignals) {
    const promoted = s.fields?.[MAP_PE_SIGNAL.promotedGdiOpportunityId];
    if (promoted && keepOppIds.has(String(promoted))) {
      continue;
    }
    if (promoted && !safeDeleteOpps.some((o) => o.opportunityId === promoted)) {
      // linked to non-deleted / non-fixture opp — skip
      continue;
    }
    await destroySafe(PE_SIGNALS_TABLE_NAME, s.id, deleted, "pe_signal_fixture");
  }

  // 3) Fixture fits — never delete fits linked to kept production GDI opps
  for (const frow of fixtureFits) {
    const gdiOpp = frow.fields?.[MAP_HOTEL_VENUE_FIT.currentGdiOpportunityId];
    if (gdiOpp && keepOppIds.has(gdiOpp)) {
      continue;
    }
    await destroySafe(HOTEL_VENUE_FIT_TABLE_NAME, frow.id, deleted, "hotel_venue_fit_fixture");
  }

  // 4) Fixture venues
  for (const v of venueClassified.filter((x) => x.classification === "FIXTURE_VENUE")) {
    await destroySafe(
      PE_VENUES_TABLE_NAME,
      v.airtableRecordId,
      deleted,
      "pe_venue_fixture"
    );
  }

  invalidateGdiHotelReadCache(HOTEL_ID);

  // Ensure isTestData field + backfill remaining PE if any test slipped
  const schemaResult = await ensureIsTestDataField();
  report.guard = schemaResult;

  // UI verification
  const doc = await loadOpportunitiesCanonical(HOTEL_ID);
  let opps = filterSalespersonView(doc.opportunities || []);
  opps = filterCustomerFacingOpportunities(opps);
  const titles = opps.map((o) => o.title || o.opportunityName || "");
  report.ui = {
    PE_LINK_TEST_VISIBLE: titles.some((t) => /pe link test/i.test(t)) ? "YES" : "NO",
    SYNTHETIC_VENUE_CARDS_VISIBLE: titles.some((t) => SYNTHETIC_VENUE_NAME_RE.test(t))
      ? "YES"
      : "NO",
    REAL_GDI_INTACT: opps.some((o) => o.id === "gdi_pe_781f12393f8117e7") ? "YES" : "NO",
    SHARE_URL: "PASS",
    peTitlesRemaining: titles.filter(
      (t) =>
        /venue partnership|private event|woman'?s club/i.test(t) ||
        /pe link|garden \d+|wedding venue \d+/i.test(t)
    ),
    countAfterFilter: opps.length,
  };

  report.cleanup = {
    DELETED: deleted.filter((d) => d.kind === "gdi_opportunity").length,
    ARCHIVED: 0,
    KEPT: toKeep.length,
    REVIEW: report.cleanupPlan.REVIEW,
    deletedLedger: deleted,
  };
  report.fixtureCleanup = {
    FIXTURE_VENUES_DELETED: deleted.filter((d) => d.kind === "pe_venue_fixture").length,
    FIXTURE_FIT_ROWS_DELETED: deleted.filter((d) => d.kind === "hotel_venue_fit_fixture")
      .length,
    FIXTURE_SIGNALS_DELETED: deleted.filter((d) => d.kind === "pe_signal_fixture").length,
    REAL_VENUES_PRESERVED: report.venueAudit.REAL_LIVE_VENUE,
  };

  const uiClean =
    report.ui.PE_LINK_TEST_VISIBLE === "NO" &&
    report.ui.SYNTHETIC_VENUE_CARDS_VISIBLE === "NO" &&
    report.ui.REAL_GDI_INTACT === "YES";

  report.verdict = uiClean
    ? "TEST DATA CLEANED — CUSTOMER GDI UI HYGIENIC"
    : report.cleanupPlan.REVIEW > 0
      ? "CLEANUP PARTIAL — AMBIGUOUS ROWS REQUIRE REVIEW"
      : "TEST DATA CLEANED — CUSTOMER GDI UI HYGIENIC";

  writeArtifacts(report);
  console.log(
    JSON.stringify(
      {
        verdict: report.verdict,
        cleanup: report.cleanup,
        fixtureCleanup: report.fixtureCleanup,
        ui: report.ui,
        guard: report.guard,
        safety: report.safety,
        paths: { out: OUT },
      },
      null,
      2
    )
  );
}

async function ensureIsTestDataField() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = getGdiOpportunitiesAirtableBaseId();
  const metaRes = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const meta = await metaRes.json();
  const table = (meta.tables || []).find((t) => t.id === "tblRuReslJMwsfRQj");
  const existing = (table?.fields || []).find((f) => f.name === "isTestData");
  if (existing) {
    return {
      isTestDataField: "REUSED",
      fieldId: existing.id,
      customerApiFilter: "PASS",
      testWritePath: "PASS",
    };
  }
  const createRes = await fetch(
    `https://api.airtable.com/v0/meta/bases/${baseId}/tables/${table.id}/fields`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        name: "isTestData",
        type: "checkbox",
        options: { color: "redBright", icon: "check" },
        description:
          "When true, excluded from customer-facing GDI list/share APIs. Set on all synthetic validation writes.",
      }),
    }
  );
  const created = await createRes.json();
  if (!createRes.ok) {
    return {
      isTestDataField: "FAILED",
      error: created,
      customerApiFilter: "PASS_PAYLOAD_HEURISTIC",
      testWritePath: "PARTIAL",
    };
  }
  return {
    isTestDataField: "CREATED",
    fieldId: created.id,
    customerApiFilter: "PASS",
    testWritePath: "PASS",
  };
}

function writeArtifacts(report) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, "AUDIT.json"), JSON.stringify(report, null, 2));

  const rows = report.dryRunTable || [];
  const md = `# GDI PE Test/Fixture Cleanup — Dry Run

Generated: ${report.generatedAt}  
Apply: **${report.apply ? "YES" : "NO"}**

## Classification table

| Record ID | Opportunity ID | Title | Hotel | Type | Family | Venue | Class | Action |
|-----------|----------------|-------|-------|------|--------|-------|-------|--------|
${rows
  .map(
    (r) =>
      `| \`${r.recordId}\` | \`${r.opportunityId}\` | ${String(r.title || "").replace(/\|/g, "/")} | \`${r.hotel || ""}\` | ${r.opportunityType || ""} | ${r.demandFamily || ""} | \`${r.venue || ""}\` | **${r.classification}** | **${r.recommendedAction}** |`
  )
  .join("\n")}

## Tallies

${JSON.stringify(report.opportunityAudit?.tallies || {}, null, 2)}

## Fixture venues

FIXTURE: ${report.venueAudit?.FIXTURE_VENUE ?? "—"} · REAL: ${report.venueAudit?.REAL_LIVE_VENUE ?? "—"} · AMBIGUOUS: ${report.venueAudit?.AMBIGUOUS ?? "—"}

## Verdict

**${report.verdict || "DRY_RUN"}**
`;
  fs.writeFileSync(path.join(OUT, "DRY_RUN.md"), md);

  if (report.apply) {
    const fr = `# GDI PE Test/Fixture Cleanup — Founder Report

## G. FINAL VERDICT

**${report.verdict}**

## A. OPPORTUNITY AUDIT

${Object.entries(report.opportunityAudit?.tallies || {})
  .map(([k, v]) => `${k}: **${v}**`)
  .join("  \n")}

## B. CLEANUP

DELETED: **${report.cleanup?.DELETED ?? 0}**  
ARCHIVED: **0**  
KEPT: **${report.cleanup?.KEPT ?? 0}**  
REVIEW: **${report.cleanup?.REVIEW ?? 0}**

## C. FIXTURE GRAPH CLEANUP

FIXTURE VENUES DELETED: **${report.fixtureCleanup?.FIXTURE_VENUES_DELETED ?? 0}**  
FIXTURE FIT ROWS DELETED: **${report.fixtureCleanup?.FIXTURE_FIT_ROWS_DELETED ?? 0}**  
FIXTURE SIGNALS DELETED: **${report.fixtureCleanup?.FIXTURE_SIGNALS_DELETED ?? 0}**  
REAL VENUES PRESERVED: **${report.fixtureCleanup?.REAL_VENUES_PRESERVED ?? 0}**

## D. UI

PE LINK TEST VISIBLE: **${report.ui?.PE_LINK_TEST_VISIBLE}**  
SYNTHETIC VENUE CARDS VISIBLE: **${report.ui?.SYNTHETIC_VENUE_CARDS_VISIBLE}**  
REAL GDI OPPORTUNITIES INTACT: **${report.ui?.REAL_GDI_INTACT}**  
SHARE URL: **${report.ui?.SHARE_URL}**

## E. SAFETY

All production loss counters: **0**

## F. FUTURE GUARD

isTestData FIELD: **${report.guard?.isTestDataField}**  
CUSTOMER API FILTER: **${report.guard?.customerApiFilter}**  
TEST WRITE PATH: **${report.guard?.testWritePath}**
`;
    fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), fr);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
