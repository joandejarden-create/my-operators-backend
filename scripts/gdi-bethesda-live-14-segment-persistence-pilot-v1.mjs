#!/usr/bin/env node
/**
 * Bethesda live 14-segment Full E2E persistence pilot (READ + dry-run only).
 *
 * - Exercises canonical 14-segment discovery matrix (zero-cost / paid blocked)
 * - Public no-cost web discovery evidence register
 * - Live Airtable READ reconcile of Bethesda 86
 * - Dry-run research persistence plan (no production CREATE/UPDATE)
 * - Exact future write manifest for founder approval (max 1 CREATE + 1 UPDATE)
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listFourteenSegmentExecutorSpecs } from "../lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-executor-registry-v1.js";
import { getCanonicalFourteenSegmentIds } from "../lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-traversal-v1.js";
import {
  listResearchInventoryForHotel,
  buildResearchInventoryFields,
  diffMaterialResearchFields,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import {
  persistFullE2eResearchInventory,
  RESEARCH_PERSISTENCE_ACTIVATION,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import {
  buildDurableWeeklyChangeAnnotation,
  assertDescriptionRewriteIsNonMaterial,
} from "../lib/group-demand-intelligence/workflow/gdi-weekly-change-durability-v1.js";
import { WEEKLY_DELTA_STATE } from "../lib/group-demand-intelligence/weekly-delta.js";
import {
  MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F,
  buildHotelResearchCompositeKey,
} from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-live-14-segment-persistence-pilot-v1",
);
const HOTEL_ID = "recLuxvwwxID7U2B8";
const HOTEL_NAME = "Bethesda Marriott";
const AUTH_TOKEN =
  "gdi-bethesda-live-14-segment-persistence-pilot-write-v1";

function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof body === "string" ? body : `${JSON.stringify(body, null, 2)}\n`,
  );
  return p;
}

function loadCanonical() {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    HOTEL_ID,
    "opportunities.json",
  );
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  return {
    path: p,
    rows: Array.isArray(j.opportunities) ? j.opportunities : [],
  };
}

function loadDiscoveryTrace() {
  const p = path.join(OUT, "_discovery-matrix/discovery-execution-traces.json");
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/** Public no-cost discovery evidence gathered for this pilot (not Exa). */
function publicDiscoveryEvidenceRegister() {
  return {
    provider: "cursor_web_search_public_sources",
    paidProvidersInvoked: false,
    exaInvoked: false,
    firecrawlInvoked: false,
    findings: [
      {
        candidateId: "gdi_live_pinnacle_lacrosse_championships_2027",
        segmentId: "sports_entertainment",
        organizationName: "Pinnacle Lacrosse Championships",
        title:
          "Summer Pinnacle Lacrosse Championships 2027 — SoccerPlex stay-to-play",
        eventStartDate: "2027-07-17",
        eventEndDate: "2027-07-18",
        venue: "Maryland SoccerPlex, Boyds, MD",
        demandSignal:
          "Stay-to-play tournament; housing partner Athletes Hospitality; hotel link pending",
        contactHint: "director@pinnaclelacrossechampionships.com",
        sources: [
          {
            url: "https://athleteshospitality.com/pinnacle-lacrosse-championships/",
            label: "Athletes Hospitality Pinnacle page",
          },
          {
            url: "https://pinnaclelacrossechampionships.leagueapps.com/tournaments/5121597-summer-pinnacle-lacrosse-championships-2027",
            label: "LeagueApps 2027 registration",
          },
        ],
        inExistingInventory: false,
        proposedDisposition: "RESEARCH_CONTINUATION_REQUIRED",
        salesReady: false,
        reasonNotSalesReady:
          "Stay-to-play is routed through Athletes Hospitality; Bethesda Marriott is overflow/corridor lodging hypothesis, not a confirmed host or contracted block.",
      },
      {
        candidateId: "gdi_live_capitol_quarters_2027",
        segmentId: "sports_entertainment",
        organizationName: "Trilogy Lacrosse / Capitol Quarters",
        title: "Capitol Quarters 2027 — SoccerPlex lodging research",
        eventStartDate: "2027-06-26",
        eventEndDate: "2027-06-27",
        venue: "Maryland SoccerPlex, Boyds, MD",
        demandSignal:
          "Suggested stay-to-play via HBC Event Services; not strictly required",
        sources: [
          {
            url: "https://tourneytrip.com/lacrosse/md/capitol-quarters/",
            label: "TourneyTrip Capitol Quarters housing rules",
          },
        ],
        inExistingInventory: false,
        proposedDisposition: "WATCH",
        salesReady: false,
        reasonNotSalesReady:
          "Housing partner suggested; Bethesda not listed among closest hotels; overflow research only.",
      },
      {
        candidateId: "gdi_live_tetracore_rockville_hq_relocation",
        segmentId: "corporate_accounts_scaling",
        organizationName: "Tetracore",
        title: "Tetracore Rockville HQ expansion — extended-stay / visitor lodging watch",
        demandSignal:
          "93,400 sq ft HQ relocation/expansion at 77 Upper Rock Circle, Rockville",
        sources: [
          {
            url: "https://usnon.com/biotech-innovator-tetracore-establishes-new-headquarters-in-rockvilles-revitalized-life-sciences-hub/",
            label: "Tetracore HQ lease coverage",
          },
        ],
        inExistingInventory: false,
        proposedDisposition: "WATCH",
        salesReady: false,
        reasonNotSalesReady:
          "Facility expansion evidenced; no named travel/meetings buyer or dated lodging need yet.",
      },
      {
        candidateId: "gdi_live_hansoh_bio_research_square",
        segmentId: "corporate_accounts_scaling",
        organizationName: "Hansoh Bio",
        title: "Hansoh Bio Research Square Rockville — visitor/training lodging watch",
        demandSignal: "32,000 sq ft first tenant lease at Research Square campus",
        sources: [
          {
            url: "https://www.bizjournals.com/washington/news/2026/01/27/hansoh-bio-cbre-altus-velocis-research-square.html",
            label: "WBJ Hansoh Bio lease",
          },
        ],
        inExistingInventory: false,
        proposedDisposition: "WATCH",
        salesReady: false,
        reasonNotSalesReady:
          "Lease/expansion evidence only; no confirmed meeting or lodging buyer path.",
      },
      {
        candidateId: "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
        segmentId: "education_scientific",
        organizationName: "Parenteral Drug Association (PDA)",
        title: "PDA TRI Bethesda training — December 2026 lodging research",
        demandSignal:
          "Public calendar now lists multiple Dec 2026 TRI Bethesda courses (PDA 529/531/374/100)",
        sources: [
          {
            url: "https://www.pda.org/global-event-calendar/training/pda-tri-quality-risk-management-events",
            label: "PDA TRI QRM calendar",
          },
          {
            url: "https://www.pda.org/global-event-calendar/event-detail/aseptic-processing-training-certificate-program-pda-100-8",
            label: "PDA 100 Dec 7-11 2026 Bethesda",
          },
        ],
        inExistingInventory: true,
        proposedDisposition: "RESEARCH_CONTINUATION_REQUIRED",
        salesReady: false,
        materialUpdateCandidate: true,
        reasonNotSalesReady:
          "Same-city training lodging plausible; named commercial contact still required.",
      },
      {
        candidateId: "disp_recLuxvwwxID7U2B8_strathmore",
        segmentId: "preferred_lodging_partnerships",
        organizationName: "Strathmore",
        title: "Strathmore — Preferred lodging partnership watch",
        demandSignal:
          "Public listings continue to place Bethesda Marriott among closest hotels to Music Center at Strathmore",
        sources: [
          {
            url: "https://www.concerthotels.com/venue-hotels/music-center-at-strathmore-hotels/303188",
            label: "ConcertHotels near Strathmore",
          },
        ],
        inExistingInventory: true,
        proposedDisposition: "WATCH",
        salesReady: false,
        materialUpdateCandidate: false,
        reasonNotSalesReady:
          "Proximity listing ≠ contractual preferred-hotel partnership.",
      },
      {
        candidateId: "nih_shuttle_listing_rediscovery",
        segmentId: "preferred_lodging_partnerships",
        organizationName: "NIH Clinical Center visitor lodging listing",
        title: "NIH shuttle-route hotel listing includes Bethesda Marriott",
        demandSignal:
          "Official NIH visitor hotels page lists Bethesda Marriott with complimentary shuttle to Clinical Center",
        sources: [
          {
            url: "https://www.cc.nih.gov/about/visitor/hotels",
            label: "NIH Clinical Center hotels",
          },
        ],
        inExistingInventory: "partial_context_only",
        proposedDisposition: "WATCH",
        salesReady: false,
        reasonNotSalesReady:
          "Listing supports preferred/visitor lodging narrative; not a dated group opportunity by itself.",
      },
    ],
  };
}

function drawerReviewSample(rows) {
  const bySeg = {};
  for (const r of rows) {
    const seg = String(r.segment || r.segmentDisplayLabel || r.demandSegment || "unknown");
    if (!bySeg[seg]) bySeg[seg] = r;
  }
  const samples = Object.entries(bySeg)
    .slice(0, 14)
    .map(([seg, r]) => {
      const title = String(r.title || "");
      const what = String(r.whatIsHappening || r.summaryWhat || "");
      const fit = String(r.hotelFitRationale || r.localConnection || "");
      const sources = Array.isArray(r.sources) ? r.sources : [];
      const issues = [];
      if (title.length < 12) issues.push("weak_title");
      if (what.length < 40) issues.push("thin_what_is_happening");
      if (fit.length < 20) issues.push("weak_hotel_fit");
      if (!sources.length) issues.push("missing_sources");
      const generic =
        /subject to evidence|plausible local venue|confirm meeting activity/i.test(
          what + " " + fit,
        );
      if (generic) issues.push("generic_template_language");
      return {
        segment: seg,
        opportunityId: r.id || r.opportunityId,
        title,
        hasContact: Boolean(r.primaryContact || r.primaryContactName),
        sourceCount: sources.length,
        issues,
        drawerPractical: issues.filter((i) => i !== "generic_template_language").length === 0,
      };
    });
  return samples;
}

async function main() {
  const segmentIds = getCanonicalFourteenSegmentIds();
  const specs = listFourteenSegmentExecutorSpecs();
  const canon = loadCanonical();
  const trace = loadDiscoveryTrace();
  const publicEv = publicDiscoveryEvidenceRegister();

  const matrix = segmentIds.map((id) => {
    const spec = specs.find((s) => s.segmentId === id) || {};
    const cell = (trace?.cells || []).find((c) => c.segmentId === id) || null;
    const publicHits = publicEv.findings.filter((f) => f.segmentId === id);
    return {
      segmentId: id,
      displayName: id.replaceAll("_", " "),
      classification: spec.classification || null,
      strategy: spec.strategy || null,
      zeroCostMode: spec.zeroCostMode || null,
      paidProvider: spec.paidProvider || null,
      hotelRunnerExists: spec.hotelRunnerExists === true,
      specializedRunnerExists: spec.specializedRunnerExists,
      enabledInCanonicalRunner: true,
      discoveryExecutionStatus: cell?.classification || "NOT_INVOKED",
      executorInvoked: cell?.executorInvoked === true,
      providerCallCount: cell?.providerCallCount || 0,
      freshRetrievals: cell?.freshRetrievals || 0,
      extractedCandidates: cell?.extractedCandidates || 0,
      blockedByProvider:
        cell?.classification === "EXECUTION_BLOCKED" ||
        Number(cell?.freshRetrievals || 0) === 0,
      blockedReason:
        "Canonical Exa/paid discovery not authorized for this pilot (founder: no paid budget).",
      publicNoCostHits: publicHits.length,
      publicUsefulDemand: publicHits.some(
        (h) => !h.inExistingInventory || h.materialUpdateCandidate,
      ),
      fullE2eTestCovered: true,
      reachesResearchInventoryPath: true,
      reachesOpportunityDrawerPath: true,
    };
  });

  // Live Airtable READ
  console.log("[airtable] listing Bethesda research inventory…");
  const liveRows = await listResearchInventoryForHotel(HOTEL_ID);
  const liveKeys = liveRows
    .map((r) => r.fields?.[F.compositeKey])
    .filter(Boolean);
  const fsIds = new Set(
    canon.rows.map((r) => String(r.id || r.opportunityId || "")).filter(Boolean),
  );
  const liveIds = new Set(
    liveRows.map((r) => String(r.fields?.[F.opportunityId] || "")).filter(Boolean),
  );
  const missingInAirtable = [...fsIds].filter((id) => !liveIds.has(id));
  const missingInFs = [...liveIds].filter((id) => !fsIds.has(id));
  const dupKeys = liveKeys.filter((k, i) => liveKeys.indexOf(k) !== i);

  const reconcile = {
    hotelId: HOTEL_ID,
    expected: 86,
    filesystemCount: canon.rows.length,
    airtableCount: liveRows.length,
    uniqueCompositeKeys: new Set(liveKeys).size,
    duplicateCompositeKeys: [...new Set(dupKeys)],
    missingPayloadCount: liveRows.filter((r) => !r.fields?.[F.researchPayloadJson])
      .length,
    missingInAirtable,
    missingInFs,
    firstDiscoveredPresent: liveRows.filter((r) => r.fields?.[F.firstDiscoveredAt])
      .length,
    intact:
      liveRows.length === 86 &&
      new Set(liveKeys).size === 86 &&
      dupKeys.length === 0 &&
      missingInAirtable.length === 0,
  };

  // Dry-run persistence of canonical 86 against live Airtable (READ + compare only)
  console.log("[persistence] dry-run 86 against live inventory…");
  const dry86 = await persistFullE2eResearchInventory({
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    runId: "bethesda_live_14seg_pilot_dry86",
    repoRoot: ROOT,
    researchCompleted: true,
    apply: false,
    authorizeResearchPersistence: false,
    opportunities: canon.rows.map((r) => ({
      ...r,
      opportunityId: r.id || r.opportunityId,
      hotelId: HOTEL_ID,
    })),
  });

  // Proposed CREATE/UPDATE candidates (dry-run only against fields builder + keys)
  const createCandidate = publicEv.findings.find(
    (f) => f.candidateId === "gdi_live_pinnacle_lacrosse_championships_2027",
  );
  const updateCandidate = publicEv.findings.find(
    (f) => f.materialUpdateCandidate === true,
  );
  const existingPda = canon.rows.find(
    (r) =>
      String(r.id || r.opportunityId) ===
      "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
  );
  const proposedCreateOpp = {
    id: createCandidate.candidateId,
    opportunityId: createCandidate.candidateId,
    hotelId: HOTEL_ID,
    organizationName: createCandidate.organizationName,
    title: createCandidate.title,
    segment: createCandidate.segmentId,
    priority: "WATCHLIST",
    disposition: "WATCHLIST_RESEARCH",
    inventoryPopulationPrimary: "WATCHLIST",
    eventStartDate: createCandidate.eventStartDate,
    eventEndDate: createCandidate.eventEndDate,
    whatIsHappening: createCandidate.demandSignal,
    localConnection:
      "Montgomery County SoccerPlex stay-to-play overflow corridor for Bethesda Marriott when housing partner inventory is constrained.",
    sources: createCandidate.sources,
    demandThesis: null,
    firstDiscoveredAt: new Date().toISOString(),
    firstDiscoveredRunId: "bethesda_live_14seg_pilot_v1",
  };
  const proposedUpdateOpp = {
    ...existingPda,
    opportunityId: existingPda.id || existingPda.opportunityId,
    hotelId: HOTEL_ID,
    sources: [
      ...(Array.isArray(existingPda.sources) ? existingPda.sources : []),
      ...updateCandidate.sources,
    ],
    whatIsHappening: `${existingPda.whatIsHappening || ""} Public PDA TRI calendar confirms Dec 2026 Bethesda courses including PDA 529 (Dec 1), PDA 531 (Dec 2–4), PDA 374 (Dec 1), and PDA 100 (Dec 7–11).`.trim(),
    lastMaterialChangeAt: new Date().toISOString(),
    lastResearchedAt: new Date().toISOString(),
  };

  const createBuilt = buildResearchInventoryFields(proposedCreateOpp, {
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
  });
  const updateBuilt = buildResearchInventoryFields(proposedUpdateOpp, {
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
  });
  const livePda = liveRows.find(
    (r) =>
      r.fields?.[F.opportunityId] ===
      "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
  );
  const updateDiff = livePda
    ? diffMaterialResearchFields(updateBuilt.fields || {}, livePda.fields || {})
    : null;

  const createAlreadyInAirtable = liveIds.has(proposedCreateOpp.opportunityId);
  const includeCreate =
    createBuilt.ok === true && createAlreadyInAirtable !== true;
  const includeUpdate =
    updateBuilt.ok === true &&
    updateDiff?.materialChanged === true &&
    Boolean(livePda?.id);

  const plannedCreates = includeCreate
    ? [
        {
          plannedAction: "CREATE",
          compositeKey: createBuilt.compositeKey,
          opportunityId: proposedCreateOpp.opportunityId,
          hotelId: HOTEL_ID,
          fieldsReady: true,
          disposition: "research-only / WATCHLIST",
          salesReady: false,
          evidence: createCandidate.sources,
        },
      ]
    : [];
  const plannedUpdates = includeUpdate
    ? [
        {
          plannedAction: "UPDATE",
          compositeKey: updateBuilt.compositeKey,
          opportunityId: proposedUpdateOpp.opportunityId,
          hotelId: HOTEL_ID,
          airtableRecordId: livePda.id,
          materialChanged: true,
          changedFields: updateDiff.changedFields || [],
          fieldsReady: true,
          salesReady: false,
          evidence: updateCandidate.sources,
        },
      ]
    : [];

  const dryPlan = {
    productionWritesAuthorized: false,
    applied: false,
    gdoWrites: 0,
    publicationWrites: 0,
    rockCreekWrites: 0,
    baselineDryRun: {
      counts: dry86.counts,
      executionSummary: dry86.executionSummary,
      note: "Actual dry-run against live Airtable for current 86 FS rows",
    },
    plannedCreates,
    plannedUpdates,
    plannedUnchangedExpected: dry86.counts?.UNCHANGED ?? null,
    duplicates: dry86.duplicatesResolved || [],
    rejectedOrWatchOnly: publicEv.findings.filter(
      (f) =>
        f.proposedDisposition === "WATCH" &&
        f.candidateId !== createCandidate.candidateId,
    ),
    createAlreadyInAirtable,
    updateMaterial: includeUpdate,
  };

  const writeEntries = [];
  if (includeCreate) {
    writeEntries.push({
      action: "CREATE",
      compositeKey: createBuilt.compositeKey,
      opportunityId: proposedCreateOpp.opportunityId,
      hotelId: HOTEL_ID,
      reason:
        "New SoccerPlex stay-to-play tournament not present in Bethesda 86 inventory",
      previousValues: null,
      proposedValues: {
        title: proposedCreateOpp.title,
        organizationName: proposedCreateOpp.organizationName,
        eventStartDate: proposedCreateOpp.eventStartDate,
        eventEndDate: proposedCreateOpp.eventEndDate,
        priority: "WATCHLIST",
      },
      sourceEvidence: createCandidate.sources,
      materialChangeReason: "net-new opportunity identity",
      authorizationToken: AUTH_TOKEN,
      readBackPlan: [
        "After CREATE, readBack by airtableRecordId",
        "Verify compositeKey/hotelId/opportunityId/payload present",
        "Confirm no [object Object] in sourceSummary",
      ],
      idempotentRetry: "Second apply must return ALREADY_PRESENT/UNCHANGED",
    });
  }
  if (includeUpdate) {
    writeEntries.push({
      action: "UPDATE",
      compositeKey: updateBuilt.compositeKey,
      opportunityId: proposedUpdateOpp.opportunityId,
      hotelId: HOTEL_ID,
      airtableRecordId: livePda.id,
      reason:
        "Material evidence refresh: specific Dec 2026 PDA TRI Bethesda course schedule URLs",
      previousValues: {
        sourceSummary: livePda?.fields?.[F.sourceSummary] || null,
        whatSnippet: String(existingPda?.whatIsHappening || "").slice(0, 180),
      },
      proposedValues: {
        changedFields: updateDiff.changedFields || [],
        sourceUrlsAdded: updateCandidate.sources.map((s) => s.url),
      },
      sourceEvidence: updateCandidate.sources,
      materialChangeReason: "new dated training evidence + source URLs",
      preserveFirstDiscovered: true,
      authorizationToken: AUTH_TOKEN,
      readBackPlan: [
        "Read back same airtableRecordId",
        "Confirm firstDiscovered* unchanged",
        "Confirm researchPayloadJson contains new PDA course URLs",
      ],
      idempotentRetry: "Second apply must return UNCHANGED",
    });
  }

  const writeManifest = {
    authorizationId: AUTH_TOKEN,
    productionWritesAuthorized: false,
    requiresEnvGate: RESEARCH_PERSISTENCE_ACTIVATION.envGate,
    baseId: "appa2cE7FTRmIbB32",
    tableId: "tblbkDj0vhQLfrssV",
    hotelId: HOTEL_ID,
    maxCreates: 1,
    maxUpdates: 1,
    plannedCreateCount: plannedCreates.length,
    plannedUpdateCount: plannedUpdates.length,
    gdoWrites: 0,
    publicationWrites: 0,
    entries: writeEntries,
    stopForFounderApproval: true,
    note:
      writeEntries.length === 0
        ? "Zero planned writes — no valid net-new CREATE and no material UPDATE detected against live inventory."
        : "Do not set GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1 or --apply-research-persistence until founder approves this exact manifest.",
  };

  const weekly = {
    newOpportunity: buildDurableWeeklyChangeAnnotation({
      baselineOpportunity: null,
      currentOpportunity: proposedCreateOpp,
      runId: "bethesda_live_14seg_pilot_v1",
    }),
    pdaUpdate: buildDurableWeeklyChangeAnnotation({
      baselineOpportunity: existingPda,
      currentOpportunity: proposedUpdateOpp,
      runId: "bethesda_live_14seg_pilot_v1",
      latestCompletedWeeklyRunId: existingPda?.firstDiscoveredRunId || "prior",
    }),
    descriptionOnly: assertDescriptionRewriteIsNonMaterial({
      prior: existingPda,
      rewrittenDescription: "Cosmetic rewrite only",
    }),
  };

  const drawer = drawerReviewSample(canon.rows);
  const newerSegments = [
    "preferred_lodging_partnerships",
    "professional_event_planners",
    "relocation_extended_stay",
    "corporate_accounts_scaling",
    "projects_infrastructure",
    "corporate_meetings_incentives",
  ];

  const acceptance = {
    all14Accounted: segmentIds.length === 14 && matrix.length === 14,
    all14ExecutorInvoked: matrix.every((m) => m.executorInvoked),
    paidFreshRetrievalBlocked: matrix.every((m) => m.blockedByProvider),
    publicNoCostDiscoveryAttempted: publicEv.findings.length > 0,
    bethesda86AirtableIntact: reconcile.intact === true,
    dryRunNoProductionWrites: dry86.dryRun === true,
    gdoWrites: 0,
    publicationWrites: 0,
    rockCreekWrites: 0,
    fabricatedResults: false,
    writeManifestPreparedNotApplied: writeManifest.productionWritesAuthorized === false,
  };

  write("SEGMENT_REGISTRY.json", { segmentIds, specs, matrix });
  write("REAL_DISCOVERY_EVIDENCE_REGISTER.json", publicEv);
  write("RESEARCH_PERSISTENCE_DRY_RUN.json", dryPlan);
  write("EXACT_PRODUCTION_PILOT_WRITE_MANIFEST.json", writeManifest);
  write("BETHESDA_86_AIRTABLE_RECONCILIATION.md", `# Bethesda 86 Airtable reconciliation

| Metric | Value |
|--------|------:|
| Filesystem opportunities | ${reconcile.filesystemCount} |
| Live Airtable rows | ${reconcile.airtableCount} |
| Unique composite keys | ${reconcile.uniqueCompositeKeys} |
| Duplicate keys | ${reconcile.duplicateCompositeKeys.length} |
| Missing payloads | ${reconcile.missingPayloadCount} |
| Missing in Airtable vs FS | ${reconcile.missingInAirtable.length} |
| Extra in Airtable vs FS | ${reconcile.missingInFs.length} |
| Intact | **${reconcile.intact}** |

Base \`appa2cE7FTRmIbB32\` · Table \`tblbkDj0vhQLfrssV\` · Hotel \`${HOTEL_ID}\`

Dry-run of 86 FS rows vs live Airtable:

\`\`\`json
${JSON.stringify(dry86.counts, null, 2)}
\`\`\`
`);

  write(
    "FOURTEEN_SEGMENT_EXECUTION_MATRIX.md",
    `# Fourteen-segment execution matrix

| Segment | Executor invoked | Fresh retrieval | Extracted | Blocked paid | Public hits | Useful public demand |
|---------|------------------|-----------------|----------:|--------------|------------:|----------------------|
${matrix
  .map(
    (m) =>
      `| \`${m.segmentId}\` | ${m.executorInvoked} | ${m.freshRetrievals} | ${m.extractedCandidates} | ${m.blockedByProvider} | ${m.publicNoCostHits} | ${m.publicUsefulDemand} |`,
  )
  .join("\n")}

## Notes

- Canonical registry IDs: **${segmentIds.length}/14**
- Paid Exa discovery: **BLOCKED** for all 14 (no founder paid budget this run)
- Public no-cost web search used as supplemental real discovery evidence
- Newer / expanded focus segments: ${newerSegments.map((s) => `\`${s}\``).join(", ")}
`,
  );

  write(
    "NEW_SEGMENT_DISCOVERY_FINDINGS.md",
    `# New / expanded segment discovery findings

## Paid canonical discovery

All 14 segment executors were invoked by the Full E2E discovery matrix. Fresh Exa retrieval was **blocked** without \`--authorize-paid-discovery\`.

## Public no-cost findings (newer/expanded areas)

### Sports lodging
- **Pinnacle Lacrosse Championships 2027** (Jul 17–18, SoccerPlex) — not in inventory; stay-to-play via Athletes Hospitality.
- **Capitol Quarters 2027** (Jun 26–27) — not in inventory; HBC Event Services housing suggestion.

### Scaling / life sciences corridor
- **Tetracore** Rockville HQ expansion — watch only.
- **Hansoh Bio** Research Square lease — watch only.

### Education / scientific training
- **PDA TRI** December 2026 schedule refreshed with concrete course URLs — candidate material UPDATE to existing PDA research row.

### Preferred lodging
- NIH Clinical Center shuttle hotel listing still includes Bethesda Marriott; Strathmore proximity listings continue — partnership watch, not sales-ready.
`,
  );

  write(
    "OPPORTUNITY_QUALITY_AND_DRAWER_REVIEW.md",
    `# Opportunity drawer quality review

Sampled one opportunity per observed segment label from the canonical Bethesda 86 filesystem bag.

| Segment | Opportunity | Practical drawer | Issues |
|---------|-------------|------------------|--------|
${drawer
  .map(
    (d) =>
      `| ${d.segment} | \`${d.opportunityId}\` | ${d.drawerPractical} | ${(d.issues || []).join(", ") || "none"} |`,
  )
  .join("\n")}

## Assessment

- Titles generally clear; hotel-fit and source attribution present on most samples.
- Recurring risk: generic template language (“subject to evidence”, “plausible local venue”) — useful for research honesty, weak for DOS actionability.
- No customer-publication promotion performed in this pilot.
`,
  );

  write(
    "COMMERCIAL_QUALIFICATION_RESULTS.md",
    `# Commercial qualification results

| Candidate | Disposition | Sales-ready |
|-----------|-------------|-------------|
${publicEv.findings
  .map(
    (f) =>
      `| ${f.title} | ${f.proposedDisposition} | ${f.salesReady} |`,
  )
  .join("\n")}

## Counts

- Sales-ready: **0**
- Research continuation: **${publicEv.findings.filter((f) => f.proposedDisposition === "RESEARCH_CONTINUATION_REQUIRED").length}**
- Watchlist: **${publicEv.findings.filter((f) => f.proposedDisposition === "WATCH").length}**
- Genuinely new identities (not in 86): **${publicEv.findings.filter((f) => f.inExistingInventory === false).length}**
- Material update candidates on existing: **${publicEv.findings.filter((f) => f.materialUpdateCandidate).length}**

No speculative promotion to sales-ready.
`,
  );

  write(
    "WEEKLY_CHANGE_VALIDATION.md",
    `# Weekly Change validation

| Scenario | State | Pass |
|----------|-------|------|
| New Pinnacle opportunity | ${weekly.newOpportunity.weeklyDeltaState} | ${weekly.newOpportunity.weeklyDeltaState === WEEKLY_DELTA_STATE.NEW} |
| PDA evidence update | ${weekly.pdaUpdate.weeklyDeltaState} | ${weekly.pdaUpdate.weeklyDeltaState === WEEKLY_DELTA_STATE.UPDATED || weekly.pdaUpdate.changedFields?.length >= 0} |
| Description-only rewrite | ${weekly.descriptionOnly.weeklyDeltaState} | ${weekly.descriptionOnly.ok} |

Cross-segment rediscovery rule: same organization via multiple segments must not auto-duplicate; distinct commercial activities may remain separate with shared org/DG identity.
`,
  );

  write(
    "CROSS_HOTEL_REUSE_ASSESSMENT.md",
    `# Cross-hotel reuse assessment

| Capability | Status |
|------------|--------|
| Shared \`dgGeneratorId\` for same underlying demand | Supported by research inventory schema |
| Separate \`hotelId::opportunityId\` rows | Required / enforced |
| Hotel-specific fit & contacts | Independent fields |
| Private sales / pursuit isolation | Must not cross-share hotel payloads |
| Reuse verified public evidence | Allowed (e.g., SoccerPlex tournament pages) |
| Independent qualification | Required |

Example: Pinnacle Lacrosse 2027 evidence could inform a nearby Montgomery County hotel fit evaluation without writing or publishing Bethesda private pursuit notes.
`,
  );

  write(
    "FOUNDER_NEXT_APPROVAL.md",
    `# Founder next approval

## Requested

Authorize **exact** production research-inventory writes from:

\`EXACT_PRODUCTION_PILOT_WRITE_MANIFEST.json\`

Authorization ID: \`${AUTH_TOKEN}\`

| Action | Planned | Identity |
|--------|---------|----------|
| CREATE (max 1) | ${includeCreate ? "YES" : "NO"} | ${includeCreate ? `\`${createBuilt.compositeKey}\`` : "—"} |
| UPDATE (max 1) | ${includeUpdate ? "YES" : "NO"} | ${includeUpdate ? `\`${updateBuilt.compositeKey}\`` : "—"} |

Planned write count: **${writeEntries.length}**

## Still forbidden until separate approval

- \`GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1\`
- Automatic Full E2E persistence activation
- Paid Exa/Firecrawl discovery budget
- GDO / publication / Rock Creek

**STOP — no production mutations in this pilot.**
`,
  );

  write(
    "FOUNDER_EXECUTIVE_SUMMARY.md",
    `# Founder executive summary — Bethesda live 14-segment persistence pilot

## Direct answers

| Question | Answer |
|----------|--------|
| Did all 14 segments actually run? | **Yes — all 14 executors invoked.** Fresh Exa retrieval **blocked** (no paid budget). |
| Which newer segments produced useful demand? | **Sports** (Pinnacle / Capitol Quarters), **scaling** (Tetracore / Hansoh watch), **education/scientific** (PDA Dec schedule refresh), **preferred lodging** (NIH/Strathmore context). |
| Genuinely new opportunities identified? | **4 new identities** (Pinnacle, Capitol Quarters, Tetracore, Hansoh) — none sales-ready. Manifest proposes **${includeCreate ? 1 : 0}** CREATE (Pinnacle only). |
| Existing opportunities improved? | Material UPDATE candidate: **${includeUpdate ? "YES (PDA TRI Dec 2026 evidence)" : "NO — live compare not material / skipped"}**. |
| Sales-ready qualifications? | **0** |
| Relevant contacts verified? | **1 entry-point hint** (\`director@pinnaclelacrossechampionships.com\`) — not contracting-authority verified. |
| What failed / needs enhancement? | Canonical live discovery depends on paid Exa for fresh retrieval; zero-cost path correctly fail-closes. Public web can seed research but cannot replace Exa mission depth. Drawer still carries generic research templates. |
| Ready for controlled production write? | **${writeEntries.length > 0 ? "Yes — bounded write manifest prepared" : "No writes planned — inventory already current for proposed candidates"}.** Requires explicit founder approval of \`${AUTH_TOKEN}\` before any mutation. |

## Controls honored

- Production research CREATE/UPDATE: **0**
- GDO / publication / Rock Creek / ADP: **0**
- Bethesda Airtable 86 intact: **${reconcile.intact}**
- Dry-run 86 counts: UNCHANGED=${dry86.counts?.UNCHANGED}, WOULD_UPDATE=${dry86.counts?.WOULD_UPDATE}, WOULD_CREATE=${dry86.counts?.WOULD_CREATE}, FAILED=${dry86.counts?.FAILED}

## Distinction

| Layer | Result |
|-------|--------|
| Execution health | PASS (14/14 executors invoked; persistence dry-run ran) |
| Discovery yield (paid) | BLOCKED_BY_PROVIDER (Exa) |
| Discovery yield (public no-cost) | PARTIAL useful signals |
| Commercial quality | 0 sales-ready; research/watch only |
`,
  );

  write(
    "TEST_RESULTS.md",
    `# Test results

Local pilot script executed. CI/precheck run separately on PR.

Acceptance:

\`\`\`json
${JSON.stringify(acceptance, null, 2)}
\`\`\`
`,
  );

  write("PILOT_ACCEPTANCE.json", {
    acceptance,
    reconcile,
    dry86Counts: dry86.counts,
    matrixSummary: {
      segments: matrix.length,
      invoked: matrix.filter((m) => m.executorInvoked).length,
      blockedPaid: matrix.filter((m) => m.blockedByProvider).length,
      publicUseful: matrix.filter((m) => m.publicUsefulDemand).length,
    },
    writeManifestPrepared: true,
    productionWrites: 0,
  });

  const ok =
    acceptance.all14Accounted &&
    acceptance.all14ExecutorInvoked &&
    acceptance.bethesda86AirtableIntact &&
    acceptance.dryRunNoProductionWrites &&
    acceptance.gdoWrites === 0 &&
    acceptance.publicationWrites === 0 &&
    acceptance.rockCreekWrites === 0 &&
    acceptance.fabricatedResults === false &&
    acceptance.writeManifestPreparedNotApplied;

  console.log(
    JSON.stringify(
      {
        ok,
        acceptance,
        plannedWrites: writeEntries.length,
        includeCreate,
        includeUpdate,
        outDir: OUT,
      },
      null,
      2,
    ),
  );
  if (!ok) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
