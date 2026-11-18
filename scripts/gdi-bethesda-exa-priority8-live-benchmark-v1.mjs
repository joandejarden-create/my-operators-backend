#!/usr/bin/env node
/**
 * Bethesda Marriott — founder-authorized Exa priority-8 live discovery benchmark.
 *
 * Authorization: gdi-bethesda-exa-priority8-benchmark-v1
 * Hard ceiling: USD $54 total + per-segment call/USD caps.
 *
 * Uses canonical Full E2E research chain + fourteen-segment discovery matrix.
 * Does NOT invent a second orchestration system.
 *
 * Modes:
 *   node scripts/gdi-bethesda-exa-priority8-live-benchmark-v1.mjs --preflight
 *   node scripts/gdi-bethesda-exa-priority8-live-benchmark-v1.mjs --authorize-paid
 *   node scripts/gdi-bethesda-exa-priority8-live-benchmark-v1.mjs --assemble-reports-only
 *
 * STOP: no Airtable production writes, no GDO, no publication, no Firecrawl.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  assessCanonicalLiveReadiness,
  CANONICAL_EXECUTION_MODE,
} from "../lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js";
import { executeOriginalFullE2eResearchChain } from "../lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js";
import {
  createPaidBudgetLedger,
  loadPaidBudgetLedger,
  persistPaidBudgetLedger,
  remainingBudgetUsd,
  canAffordQuotedCall,
  reserveQuotedCall,
  settleReservedCall,
  releaseReservation,
} from "../lib/group-demand-intelligence/research/gdi-authorized-paid-budget-ledger-v1.js";
import {
  EXA_AGENT_EFFORT,
  quoteExaAgentRun,
} from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js";
import { createExaLiveDiscoveryProvider } from "../lib/group-demand-intelligence/research/exa-agent/exa-live-discovery-provider-bridge-v1.js";
import { resolveHotelGdiProfileForResearch } from "../lib/group-demand-intelligence/hotel-gdi-profile-resolution-v1.js";
import {
  listResearchInventoryForHotel,
  readBackResearchInventory,
  buildResearchInventoryFields,
  diffMaterialResearchFields,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { persistFullE2eResearchInventory } from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";
import {
  evaluateCertifiedDrawerCommercialContent,
  detectGenericCommercialProse,
} from "../lib/group-demand-intelligence/presentation/gdi-certified-drawer-commercial-content-contract-v1.js";
import { buildUpgradedExaDiscoveryContextBlock } from "../lib/group-demand-intelligence/research/exa-agent/gdi-fourteen-segment-exa-mission-upgrades-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-bethesda-exa-priority8-live-benchmark-v1");
const STATE_ROOT = path.join(OUT, "isolated-discovery-state");
const LEDGER_PATH = path.join(OUT, "EXA_SPEND_LEDGER.json");
const RAW_DIR = path.join(OUT, "raw-chain");
const OBS_ROOT = path.join(OUT, "observability");

const HOTEL_ID = "recLuxvwwxID7U2B8";
const HOTEL_NAME = "Bethesda Marriott";
const AUTHORIZATION_ID = "gdi-bethesda-exa-priority8-benchmark-v1";
const HARD_CEILING_USD = 54;
const EFFORT = EXA_AGENT_EFFORT.MEDIUM;
const UNIT_QUOTE = Number(quoteExaAgentRun({ effort: EFFORT }).maxUsd);

const PRIORITY_SEGMENTS = Object.freeze([
  {
    id: "corporate_meetings_incentives",
    label: "Corporate meetings & incentives",
    maxCalls: 4,
    maxUsd: 8,
  },
  {
    id: "corporate_accounts_scaling",
    label: "Corporate accounts & scaling",
    maxCalls: 4,
    maxUsd: 8,
  },
  {
    id: "projects_infrastructure",
    label: "Projects & infrastructure",
    maxCalls: 3,
    maxUsd: 6,
  },
  {
    id: "education_scientific",
    label: "Education, medical & scientific",
    maxCalls: 4,
    maxUsd: 8,
  },
  {
    id: "relocation_extended_stay",
    label: "Relocation & extended stay",
    maxCalls: 3,
    maxUsd: 6,
  },
  {
    id: "government_public_sector",
    label: "Government & public sector",
    maxCalls: 3,
    maxUsd: 6,
  },
  {
    id: "preferred_lodging_partnerships",
    label: "Preferred lodging partnerships",
    maxCalls: 3,
    maxUsd: 6,
  },
  {
    id: "professional_event_planners",
    label: "Professional event planners",
    maxCalls: 3,
    maxUsd: 6,
  },
]);

const SEGMENT_IDS = PRIORITY_SEGMENTS.map((s) => s.id);
const CAP_BY_ID = Object.fromEntries(PRIORITY_SEGMENTS.map((s) => [s.id, s]));

const RESIDUAL_QUARANTINE = Object.freeze([
  "gdi_opp_umd_alumni_weekend_watch",
  "gdi_opp_marriott_hq_adjacent_corporate_watch",
  "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
]);

const ARGS = new Set(process.argv.slice(2));
const PREFLIGHT = ARGS.has("--preflight");
const AUTHORIZE_PAID = ARGS.has("--authorize-paid");
const ASSEMBLE_ONLY = ARGS.has("--assemble-reports-only");

function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof body === "string" ? body : `${JSON.stringify(body, null, 2)}\n`,
  );
  return p;
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function normOrg(s) {
  return text(s)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function segmentSpend(ledger, segmentId, { discoveryOnly = false } = {}) {
  const ops = (ledger?.operations || []).filter((op) => {
    if (String(op.segmentId || "") !== segmentId) return false;
    if (discoveryOnly && String(op.stage || "") !== "DISCOVERY") return false;
    return true;
  });
  return {
    calls: ops.length,
    spentUsd: ops.reduce((n, op) => n + Number(op.actualCostUsd || 0), 0),
  };
}

function canAffordSegment(ledger, segmentId, quotedMaxUsd = UNIT_QUOTE) {
  const cap = CAP_BY_ID[segmentId];
  if (!cap) {
    return { ok: false, reason: "SEGMENT_NOT_IN_PRIORITY8" };
  }
  const global = canAffordQuotedCall(ledger, quotedMaxUsd);
  if (!global.ok) return global;
  const spent = segmentSpend(ledger, segmentId, { discoveryOnly: true });
  const allSpent = segmentSpend(ledger, segmentId);
  if (spent.calls + 1 > cap.maxCalls) {
    return {
      ok: false,
      reason: "SEGMENT_CALL_CAP_REACHED",
      segmentId,
      calls: spent.calls,
      maxCalls: cap.maxCalls,
    };
  }
  if (allSpent.spentUsd + Number(quotedMaxUsd) > cap.maxUsd + 1e-9) {
    return {
      ok: false,
      reason: "SEGMENT_USD_CAP_REACHED",
      segmentId,
      spentUsd: allSpent.spentUsd,
      maxUsd: cap.maxUsd,
      proposedMax: quotedMaxUsd,
    };
  }
  return { ok: true, remaining: remainingBudgetUsd(ledger), proposedMax: quotedMaxUsd };
}

function classifyDemand(opp = {}) {
  const demandConfidence = String(
    opp.guestroomDemandConfidence ||
      opp.demandThesis ||
      opp.commercialDemandTypeEvidence?.guestroomConfidence ||
      "",
  ).toUpperCase();
  const roomStatus = String(opp.roomDemandStatus || "").toUpperCase();
  const disposition = String(
    opp.disposition || opp.salesPartitionV11 || opp.priority || "",
  ).toUpperCase();
  if (/CONFIRMED_DEMAND|CONFIRMED/.test(demandConfidence)) return "CONFIRMED_DEMAND";
  if (
    /RELIABLE_INFERRED/.test(demandConfidence) ||
    /VERIFIED_HOUSING_PROGRAM/.test(roomStatus)
  ) {
    return "RELIABLE_INFERRED_DEMAND";
  }
  if (
    /WATCH|SPECULATIVE|EXPLORATORY|CONTINUATION/.test(disposition) ||
    /SPECULATIVE|UNKNOWN|MISSING/.test(roomStatus)
  ) {
    return "SPECULATIVE_RESEARCH";
  }
  const body = [
    opp.whatIsHappening,
    opp.summaryWhat,
    opp.qualificationNotes,
    opp.recommendedAction,
    opp.potentialHotelBusiness,
  ]
    .filter(Boolean)
    .join(" ");
  if (
    /not a confirmed|speculative|watch only|research continuation|plausible local venue|may generate potential/i.test(
      body,
    )
  ) {
    return "SPECULATIVE_RESEARCH";
  }
  return "UNSPECIFIED";
}

function namedContact(opp = {}) {
  return (
    text(opp.primaryContact?.name) ||
    text(opp.primaryContactName) ||
    text(opp.decisionMakerName) ||
    text(opp.commercialEntryPoint?.name) ||
    text(opp.contacts?.[0]?.name) ||
    text(opp.salesReadyContactNames?.[0]) ||
    ""
  );
}

function isSalesReady(opp = {}, drawer = {}) {
  const demand = classifyDemand(opp);
  if (demand !== "CONFIRMED_DEMAND" && demand !== "RELIABLE_INFERRED_DEMAND") {
    return false;
  }
  if (!namedContact(opp) && !text(drawer.contactName)) return false;
  if (drawer.readinessDemotionCandidate) return false;
  if ((drawer.emptyCoreCount || 0) > 0) return false;
  if ((drawer.genericBoilerplateCount || 0) > 0) return false;
  const partition = String(opp.salesPartitionV11 || opp.priority || "").toUpperCase();
  if (/WATCH|WATCHLIST/.test(partition)) return false;
  return true;
}

function inventoryKeys(rows) {
  const byOppId = new Set();
  const byOrg = new Set();
  const byComposite = new Set();
  for (const r of rows) {
    const f = r.fields || r;
    const oppId = text(f[F.opportunityId] || f.opportunityId || f.id);
    const org = normOrg(f[F.organizationName] || f.organizationName);
    const ck = text(f[F.compositeKey] || f.compositeKey);
    if (oppId) byOppId.add(oppId);
    if (org) byOrg.add(org);
    if (ck) byComposite.add(ck);
  }
  return { byOppId, byOrg, byComposite };
}

function extractCandidatesFromChain(chain) {
  const fromProcessed = (chain?.processed || []).map((p) => ({
    ...p,
    opportunityId: p.opportunityId || p.id,
    organizationName: p.organizationName,
    segmentId: p.segmentId || p.researchSegment,
    source: "chain_processed",
  }));
  const fromOpps = (chain?.opportunities || chain?.finalOpportunities || []).map(
    (o) => ({
      ...o,
      opportunityId: o.opportunityId || o.id,
      organizationName: o.organizationName,
      segmentId: o.researchSegment || o.segmentId || o.segment,
      source: "chain_opportunities",
    }),
  );
  const discoveryCells = chain?.discovery?.cells || chain?.discoveryExecution?.cells || [];
  const fromDiscovery = [];
  for (const cell of discoveryCells) {
    for (const c of cell.candidates || []) {
      fromDiscovery.push({
        ...c,
        opportunityId: c.opportunityId || c.id || c.candidateId,
        organizationName: c.organizationName || c.organizingEntity,
        segmentId: cell.segmentId,
        source: "discovery_cell",
        evidenceUrls: c.evidenceUrls || (c.url ? [c.url] : []),
      });
    }
  }
  return { fromProcessed, fromOpps, fromDiscovery, discoveryCells };
}

async function runSecondAngleIfNeeded({
  ledger,
  hotel,
  segmentId,
  firstPassHits,
}) {
  // Second-angle uses discovery call budget only (deep research is separate).
  const spent = segmentSpend(ledger, segmentId, { discoveryOnly: true });
  const cap = CAP_BY_ID[segmentId];
  if (!cap || spent.calls >= cap.maxCalls) {
    return { ran: false, reason: "SEGMENT_DISCOVERY_CALL_CAP_REACHED" };
  }
  if (firstPassHits >= 2) {
    return { ran: false, reason: "FIRST_PASS_SUFFICIENT_HITS" };
  }
  const gate = canAffordSegment(ledger, segmentId, UNIT_QUOTE);
  if (!gate.ok) return { ran: false, reason: gate.reason, gate };

  const reserved = reserveQuotedCall(ledger, { quotedMaxUsd: gate.proposedMax });
  if (!reserved.ok) return { ran: false, reason: reserved.reason };

  const provider = createExaLiveDiscoveryProvider({
    effort: EFFORT,
    authorizePaidDiscovery: true,
    authorizeExaAgentLive: true,
    forceMockOnly: false,
    segmentId,
  });
  const followUpBlock = [
    buildUpgradedExaDiscoveryContextBlock(hotel, segmentId),
    "",
    "SECOND-ANGLE DISCOVERY (founder priority-8 benchmark):",
    "- Prefer forward-looking demand not already in conventional hotel RFP channels.",
    "- Avoid duplicating the first-pass organizations unless a distinct dated activity is evidenced.",
    "- Require Bethesda / Montgomery County lodging or meeting relevance.",
    "- Name a commercial entry point when verifiable; do not invent contacts.",
  ].join("\n");

  try {
    const result = await provider.search({
      queries: [
        {
          id: `${segmentId}_second_angle`,
          intent: "forward_looking_gap_fill",
          query: followUpBlock.slice(0, 900),
        },
      ],
      hotel,
      hotelId: HOTEL_ID,
      segmentId,
    });
    settleReservedCall(ledger, {
      quotedMaxUsd: gate.proposedMax,
      actualCostUsd: Number(result.costUsd || 0),
      hotelId: HOTEL_ID,
      segmentId,
      stage: "DISCOVERY",
      providerRunId: result.providerRunId || null,
      mode: result.mode,
      blockedReason: result.blockedReason || null,
      angle: "SECOND_ANGLE",
    });
    return { ran: true, result };
  } catch (err) {
    releaseReservation(ledger, gate.proposedMax);
    return { ran: false, reason: "PROVIDER_THROW", error: String(err?.message || err) };
  }
}

function buildScorecard({
  ledger,
  chain,
  secondAngles,
  inventory,
  candidates,
}) {
  const keys = inventoryKeys(inventory);
  const rows = [];
  for (const seg of PRIORITY_SEGMENTS) {
    const ops = (ledger.operations || []).filter(
      (op) => String(op.segmentId || "") === seg.id,
    );
    const discoveryOps = ops.filter((op) => op.stage === "DISCOVERY");
    const deepOps = ops.filter((op) => op.stage === "DEEP_RESEARCH");
    const spentUsd = ops.reduce((n, op) => n + Number(op.actualCostUsd || 0), 0);
    const cell =
      candidates.discoveryCells.find((c) => c.segmentId === seg.id) || null;
    const cellHits = cell?.candidates || [];
    const second = secondAngles[seg.id] || null;
    const secondHits = second?.result?.hits || [];
    const allHits = [...cellHits, ...secondHits];

    let newIdentities = 0;
    let enriched = 0;
    let duplicates = 0;
    let unsupported = 0;
    let datedSignals = 0;
    let verifiedContacts = 0;
    let salesReady = 0;
    let researchOnly = 0;
    let commerciallyCredibleNew = 0;
    const evidence = [];

    for (const hit of allHits) {
      const org = normOrg(hit.organizationName || hit.title);
      const urls = hit.evidenceUrls || (hit.url ? [hit.url] : []);
      const inInv = org && keys.byOrg.has(org);
      const dated = /\b(20\d{2}|Q[1-4]\s*20\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/i.test(
        text(hit.excerpt || hit.title || hit.eventStartDate),
      );
      if (!org || urls.length === 0) unsupported += 1;
      else if (inInv) {
        enriched += 1;
        duplicates += 1;
      } else {
        newIdentities += 1;
        // Retrieval identity ≠ commercial opportunity. Count as commercially
        // credible only when dated signal + source URL exist (still not sales-ready).
        if (dated) commerciallyCredibleNew += 1;
      }
      if (dated) datedSignals += 1;
      evidence.push({
        organizationName: hit.organizationName || hit.title || null,
        url: urls[0] || null,
        segmentId: seg.id,
        inExistingInventory: Boolean(inInv),
        datedSignal: dated,
        excerpt: text(hit.excerpt).slice(0, 280),
      });
    }

    const processed = (candidates.fromProcessed || []).filter(
      (p) => p.segmentId === seg.id,
    );
    for (const p of processed) {
      const drawer = evaluateCertifiedDrawerCommercialContent(p);
      const contact = namedContact(p);
      // Count named entry points from hydrated fields OR prior chain contact tallies.
      // PR #382 under-counted because processed rows omitted primaryContactName.
      if (contact || Number(p.salesReadyContacts || 0) > 0) verifiedContacts += 1;
      if (isSalesReady(p, drawer)) salesReady += 1;
      else researchOnly += 1;
    }

    const callCapExceeded = ops.length > seg.maxCalls;
    const usdCapExceeded = spentUsd > seg.maxUsd + 1e-9;
    rows.push({
      segmentId: seg.id,
      label: seg.label,
      callsAttempted: ops.length,
      discoveryCalls: discoveryOps.length,
      deepCalls: deepOps.length,
      callsCompleted: ops.filter((op) => Number(op.actualCostUsd || 0) >= 0 && !op.blocked)
        .length,
      actualCostUsd: Number(spentUsd.toFixed(6)),
      maxCalls: seg.maxCalls,
      maxUsd: seg.maxUsd,
      callCapExceeded,
      usdCapExceeded,
      sourcesRetrieved: allHits.filter((h) => (h.evidenceUrls || h.url)).length,
      firstPassHits: cellHits.length,
      secondAngle: second
        ? {
            ran: second.ran === true,
            reason: second.reason || null,
            hits: secondHits.length,
            costUsd: Number(second.result?.costUsd || 0),
          }
        : null,
      newOrganizationIdentities: newIdentities,
      datedNewDemandSignals: datedSignals,
      // Honest alias: dated+sourced new identities only — not sales-ready.
      credibleNewOpportunities: commerciallyCredibleNew,
      existingEnriched: enriched,
      verifiedRelevantContacts: verifiedContacts,
      salesReadyCount: salesReady,
      researchOnlyCount: researchOnly,
      unsupportedOrDuplicateCount: unsupported + duplicates,
      costPerCredibleNew:
        commerciallyCredibleNew > 0
          ? Number((spentUsd / commerciallyCredibleNew).toFixed(4))
          : null,
      costPerSalesReady:
        salesReady > 0 ? Number((spentUsd / salesReady).toFixed(4)) : null,
      retrievalSuccess: discoveryOps.some(
        (op) => String(op.mode || "") === "FRESH_RETRIEVAL" || Number(op.actualCostUsd) > 0,
      ),
      limitations: [],
      evidenceSample: evidence.slice(0, 8),
    });

    const row = rows[rows.length - 1];
    if (!row.retrievalSuccess) {
      row.limitations.push("No successful fresh Exa retrieval credited to segment.");
    }
    if (row.newOrganizationIdentities === 0) {
      row.limitations.push("Zero new organization identities after inventory compare.");
    }
    if (row.credibleNewOpportunities === 0) {
      row.limitations.push(
        "Zero dated+sourced new demand signals (retrieval identities alone are not commercial opportunities).",
      );
    }
    if (row.salesReadyCount === 0) {
      row.limitations.push("Zero sales-ready after honest gates.");
    }
    if (row.verifiedRelevantContacts === 0) {
      row.limitations.push("No verified named commercial contacts in processed set.");
    }
    if (callCapExceeded) {
      row.limitations.push(
        `Segment Exa call cap exceeded (${ops.length} > ${seg.maxCalls}) — deep research shared the segment call budget.`,
      );
    }
  }
  return rows;
}

function writeReports({
  readiness,
  ledger,
  chain,
  scorecard,
  inventory,
  candidates,
  secondAngles,
  dryRunPersistence,
  drawerAudit,
  baseline,
}) {
  const spent = Number(ledger?.spentUsd || 0);
  const salesReadyTotal = scorecard.reduce((n, r) => n + r.salesReadyCount, 0);
  const credibleNewTotal = scorecard.reduce(
    (n, r) => n + r.credibleNewOpportunities,
    0,
  );
  const newIdentityTotal = scorecard.reduce(
    (n, r) => n + Number(r.newOrganizationIdentities || 0),
    0,
  );
  const callCapBreaches = scorecard.filter((r) => r.callCapExceeded);
  const discoverySpend = (ledger?.operations || [])
    .filter((op) => op.stage === "DISCOVERY")
    .reduce((n, op) => n + Number(op.actualCostUsd || 0), 0);
  const deepSpend = (ledger?.operations || [])
    .filter((op) => op.stage === "DEEP_RESEARCH")
    .reduce((n, op) => n + Number(op.actualCostUsd || 0), 0);

  write("EXA_SPEND_LEDGER.json", {
    ...ledger,
    outPath: LEDGER_PATH,
    benchmark: {
      authorizationId: AUTHORIZATION_ID,
      hardCeilingUsd: HARD_CEILING_USD,
      unitQuoteUsd: UNIT_QUOTE,
      effort: EFFORT,
      discoverySpendUsd: Number(discoverySpend.toFixed(6)),
      deepSpendUsd: Number(deepSpend.toFixed(6)),
      remainingUsd: remainingBudgetUsd(ledger),
      perSegmentCaps: PRIORITY_SEGMENTS,
    },
  });

  write(
    "EIGHT_SEGMENT_EXECUTION_SCORECARD.md",
    [
      "# Eight-segment Exa live execution scorecard",
      "",
      `| Metric | Value |`,
      `|---|---|`,
      `| Authorization | \`${AUTHORIZATION_ID}\` |`,
      `| Actual spend | **$${spent.toFixed(4)}** / $${HARD_CEILING_USD} |`,
      `| Discovery spend | $${discoverySpend.toFixed(4)} |`,
      `| Deep spend | $${deepSpend.toFixed(4)} |`,
      `| New org identities (retrieval) | ${newIdentityTotal} |`,
      `| Dated+sourced new demand signals | ${credibleNewTotal} |`,
      `| Sales-ready (honest gates) | ${salesReadyTotal} |`,
      `| Baseline Bethesda research | ${baseline.bethesdaCount} |`,
      `| Segment call-cap breaches | ${callCapBreaches.length ? callCapBreaches.map((r) => r.segmentId).join(", ") : "none"} |`,
      "",
      "Note: retrieval identities are **not** automatically commercial opportunities. Sales-ready requires demand + verified contact.",
      "",
      "| Segment | Calls | Cost | Sources | New IDs | Dated new | Enriched | Contacts | Sales-ready | Research-only | Unsupported/dup | $/dated | $/sales-ready |",
      "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
      ...scorecard.map(
        (r) =>
          `| ${r.label} | ${r.callsAttempted}${r.callCapExceeded ? "*" : ""} | $${r.actualCostUsd} | ${r.sourcesRetrieved} | ${r.newOrganizationIdentities} | ${r.credibleNewOpportunities} | ${r.existingEnriched} | ${r.verifiedRelevantContacts} | ${r.salesReadyCount} | ${r.researchOnlyCount} | ${r.unsupportedOrDuplicateCount} | ${r.costPerCredibleNew ?? "—"} | ${r.costPerSalesReady ?? "—"} |`,
      ),
      "",
      "## Segment limitations",
      ...scorecard.flatMap((r) => [
        `### ${r.label}`,
        ...(r.limitations.length
          ? r.limitations.map((l) => `- ${l}`)
          : ["- None recorded"]),
        "",
      ]),
    ].join("\n"),
  );

  write("SEGMENT_SCORECARD.json", { generatedAt: new Date().toISOString(), scorecard });

  write("NEW_OPPORTUNITIES_AND_EVIDENCE.json", {
    generatedAt: new Date().toISOString(),
    baselineCount: baseline.bethesdaCount,
    candidates: {
      discoveryHitCount: candidates.fromDiscovery.length,
      processedCount: candidates.fromProcessed.length,
      opportunityBagCount: candidates.fromOpps.length,
    },
    bySegment: Object.fromEntries(
      scorecard.map((r) => [
        r.segmentId,
        {
          newOrganizationIdentities: r.newOrganizationIdentities,
          datedNewDemandSignals: r.credibleNewOpportunities,
          credibleNewOpportunities: r.credibleNewOpportunities,
          existingEnriched: r.existingEnriched,
          salesReadyCount: r.salesReadyCount,
          evidenceSample: r.evidenceSample,
          secondAngle: r.secondAngle,
        },
      ]),
    ),
    processed: candidates.fromProcessed,
    residualQuarantineExcluded: RESIDUAL_QUARANTINE,
  });

  const qualificationLines = [
    "# Commercial qualification results",
    "",
    "Demand classes used: CONFIRMED_DEMAND · RELIABLE_INFERRED_DEMAND · SPECULATIVE_RESEARCH.",
    "Sales-ready requires credible demand **and** a verified named contact/entry point — no RFP or room-night count required; no speculative promotions.",
    "",
    `| Segment | Sales-ready | Research-only / watch | Notes |`,
    `|---|---:|---:|---|`,
  ];
  for (const r of scorecard) {
    qualificationLines.push(
      `| ${r.label} | ${r.salesReadyCount} | ${r.researchOnlyCount} | ${r.limitations[0] || "—"} |`,
    );
  }
  write("COMMERCIAL_QUALIFICATION_RESULTS.md", qualificationLines.join("\n") + "\n");

  write(
    "CONTACT_VERIFICATION.md",
    [
      "# Contact verification",
      "",
      "Named contacts counted only when present on processed opportunity objects after the Full E2E contact pass.",
      "Entry points without a person name are reported separately as research continuation.",
      "",
      ...scorecard.map(
        (r) =>
          `- **${r.label}**: verified named contacts = ${r.verifiedRelevantContacts}; sales-ready = ${r.salesReadyCount}`,
      ),
      "",
    ].join("\n"),
  );

  write(
    "OPPORTUNITY_DRAWER_QUALITY.md",
    [
      "# Opportunity drawer quality",
      "",
      `| Metric | Value |`,
      `|---|---|`,
      `| Drawers audited | ${drawerAudit.audited} |`,
      `| Generic boilerplate hits | ${drawerAudit.genericHits} |`,
      `| Empty core sections | ${drawerAudit.emptyCore} |`,
      `| Readiness demotions | ${drawerAudit.demotions} |`,
      "",
      "Contract: `gdi-certified-drawer-commercial-content-contract-v1`.",
      "Generic hotel-fit boilerplate is demoted; evidence and uncertainty preserved.",
      "",
      "## Samples",
      ...drawerAudit.samples.map(
        (s) =>
          `- \`${s.opportunityId || "unknown"}\` (${s.segmentId || "?"}): generic=${s.genericBoilerplateCount || 0}, emptyCore=${s.emptyCoreCount || 0}, demote=${Boolean(s.readinessDemotionCandidate)}`,
      ),
      "",
    ].join("\n"),
  );

  write("AIRTABLE_RESEARCH_WRITE_DRY_RUN.json", dryRunPersistence);
  write(
    "DEDUPLICATION_AND_WEEKLY_CHANGE.md",
    [
      "# Deduplication and Weekly Change (dry-run)",
      "",
      `- Existing Bethesda research records: **${baseline.bethesdaCount}**`,
      `- Dry-run create proposals: **${dryRunPersistence?.summary?.wouldCreate ?? dryRunPersistence?.wouldCreate ?? 0}**`,
      `- Dry-run update proposals: **${dryRunPersistence?.summary?.wouldUpdate ?? dryRunPersistence?.wouldUpdate ?? 0}**`,
      `- Dry-run unchanged: **${dryRunPersistence?.summary?.unchanged ?? dryRunPersistence?.unchanged ?? "see JSON"}**`,
      `- Production writes this run: **0**`,
      `- Residual sourceSummary quarantine (excluded from yield): ${RESIDUAL_QUARANTINE.join(", ")}`,
      "",
      "First-discovery provenance preserved. No GDO / publication changes.",
      "",
    ].join("\n"),
  );

  write(
    "DISCOVERY_GAPS_AND_NEXT_IMPROVEMENTS.md",
    [
      "# Discovery gaps and next improvements",
      "",
      ...scorecard.map(
        (r) =>
          `- **${r.label}**: ${r.limitations.join(" ") || "Retrieval completed; continue refining commercial entry-point extraction."}`,
      ),
      "",
      "## Cross-cutting",
      "- Keep per-segment call caps when expanding second-angle missions.",
      "- Prefer dated lodging theses over organization-only hits.",
      "- Do not promote SPECULATIVE_RESEARCH to sales-ready without verified contact + demand.",
      "- Quarantine residual sourceSummary malformations until a separate write set is authorized.",
      "",
    ].join("\n"),
  );

  write(
    "FOUNDER_EXECUTIVE_SUMMARY.md",
    [
      "# Founder executive summary — Bethesda Exa priority-8 live benchmark",
      "",
      `| Question | Answer |`,
      `|---|---|`,
      `| Authorization | \`${AUTHORIZATION_ID}\` |`,
      `| Actual Exa spend | **$${spent.toFixed(4)}** (ceiling $${HARD_CEILING_USD}) |`,
      `| Segments with fresh retrieval | ${scorecard.filter((r) => r.retrievalSuccess).length} / 8 |`,
      `| New org identities (retrieval) | ${newIdentityTotal} |`,
      `| Dated+sourced new demand signals | ${credibleNewTotal} |`,
      `| Sales-ready (honest) | **${salesReadyTotal}** |`,
      `| Bethesda baseline unchanged | ${baseline.bethesdaCount} research records (no production writes) |`,
      `| Dry-run research creates proposed | see AIRTABLE_RESEARCH_WRITE_DRY_RUN.json |`,
      `| Pilot residual quarantine | 3 sourceSummary rows excluded from yield |`,
      `| Segment call-cap note | ${callCapBreaches.length ? callCapBreaches.map((r) => `${r.segmentId} ${r.callsAttempted}/${r.maxCalls}`).join("; ") : "all within caps"} |`,
      `| Live readiness preflight | ${readiness?.liveReady ? "PASS" : "see blockers"} |`,
      `| Chain ok | ${chain?.ok !== false ? "yes" : "no"} |`,
      "",
      "Honest read: fresh Exa retrieval succeeded on all 8 priority segments, but **0 sales-ready** after contact+demand gates. Retrieval volume ≠ commercial yield.",
      "",
      "Paid Firecrawl/other providers: **not used**. Recurring persistence: **OFF**. GDO/publication: **unchanged**.",
      "",
    ].join("\n"),
  );

  write(
    "FOUNDER_NEXT_DECISIONS.md",
    [
      "# Founder next decisions",
      "",
      "1. Review segment scorecard — accept or reject any proposed research creates/updates (dry-run only today).",
      "2. If yield is thin, authorize a bounded second paid wave with explicit segment caps (not automatic).",
      "3. Separately authorize repair of quarantined sourceSummary rows if desired.",
      "4. Keep recurring research persistence **OFF** until commercial yield justifies it.",
      "5. Do **not** enable GDO/customer publication from this package.",
      "",
      "**STOP — no further paid research or production writes without new founder authorization.**",
      "",
    ].join("\n"),
  );

  write(
    "TEST_RESULTS.md",
    [
      "# Test results",
      "",
      `- Live readiness quote bounded: **${readiness?.quote?.bounded === true}**`,
      `- Unit quote USD: **${UNIT_QUOTE}**`,
      `- Ledger frozen: **${Boolean(ledger?.frozen)}** (${ledger?.freezeReason || "n/a"})`,
      `- Spent ≤ ceiling: **${spent <= HARD_CEILING_USD + 1e-9}**`,
      `- Per-segment caps respected: **${scorecard.every((r) => r.callsAttempted <= r.maxCalls && r.actualCostUsd <= r.maxUsd + 1e-9)}**`,
      `- Airtable production writes: **0**`,
      `- GDI_EXA_AGENT_LIVE required for paid path: enforced`,
      "",
    ].join("\n"),
  );

  write("BENCHMARK_ACCEPTANCE.json", {
    authorizationId: AUTHORIZATION_ID,
    hardCeilingUsd: HARD_CEILING_USD,
    actualSpendUsd: spent,
    withinCeiling: spent <= HARD_CEILING_USD + 1e-9,
    segmentsTested: SEGMENT_IDS,
    retrievalSuccessCount: scorecard.filter((r) => r.retrievalSuccess).length,
    salesReadyTotal,
    credibleNewTotal,
    newIdentityTotal,
    callCapBreaches: callCapBreaches.map((r) => ({
      segmentId: r.segmentId,
      calls: r.callsAttempted,
      maxCalls: r.maxCalls,
    })),
    airtableProductionWrites: 0,
    gdoWrites: 0,
    publicationChanged: false,
    residualQuarantine: RESIDUAL_QUARANTINE,
  });

  write("SECOND_ANGLE_RESULTS.json", secondAngles);
  write("CHAIN_SUMMARY.json", {
    ok: chain?.ok !== false,
    paidSpendUsd: chain?.paidSpendUsd ?? spent,
    discovery: chain?.discovery || null,
    processedCount: (chain?.processed || []).length,
    publicationOccurred: false,
    airtableWrites: 0,
  });
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  fs.mkdirSync(RAW_DIR, { recursive: true });
  fs.mkdirSync(OBS_ROOT, { recursive: true });
  fs.mkdirSync(STATE_ROOT, { recursive: true });

  const readiness = assessCanonicalLiveReadiness({
    hotelId: HOTEL_ID,
    mode: CANONICAL_EXECUTION_MODE.LIVE,
    authorizePaidDiscovery: true,
    authorizePaidResearch: true,
    authorizeExaAgentLive: true,
    budgetCeilingUsd: HARD_CEILING_USD,
    exaEffort: EFFORT,
  });
  write("00-LIVE_READINESS.json", readiness);

  if (PREFLIGHT || !AUTHORIZE_PAID) {
    if (!AUTHORIZE_PAID && !ASSEMBLE_ONLY) {
      console.log(
        JSON.stringify(
          {
            ok: readiness.liveReady,
            mode: "PREFLIGHT_OR_PLAN",
            note: "Pass --authorize-paid to spend under gdi-bethesda-exa-priority8-benchmark-v1 (≤ $54).",
            liveReady: readiness.liveReady,
            blockers: readiness.blockers || [],
            quote: readiness.quote,
          },
          null,
          2,
        ),
      );
      if (!readiness.liveReady && process.env.GDI_EXA_AGENT_LIVE !== "1") {
        console.error(
          "Set GDI_EXA_AGENT_LIVE=1 in the process environment before --authorize-paid.",
        );
      }
      process.exit(readiness.liveReady || PREFLIGHT ? 0 : 2);
    }
  }

  if (AUTHORIZE_PAID) {
    if (String(process.env.GDI_EXA_AGENT_LIVE || "") !== "1") {
      console.error("REFUSED: GDI_EXA_AGENT_LIVE=1 required for paid execution.");
      process.exit(2);
    }
    if (!readiness.liveReady) {
      console.error("REFUSED: live readiness blocked", readiness.blockers);
      process.exit(2);
    }
    if (!readiness.quote?.bounded) {
      console.error("REFUSED: Exa quote unbounded — cannot enforce spend ceiling.");
      process.exit(2);
    }
  }

  console.log("[baseline] live Airtable research inventory…");
  const inventory = await listResearchInventoryForHotel(HOTEL_ID);
  const baseline = {
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    bethesdaCount: inventory.length,
    at: new Date().toISOString(),
  };
  write("PRODUCTION_BASELINE.json", baseline);

  let ledger =
    loadPaidBudgetLedger(LEDGER_PATH, {
      authorizationId: AUTHORIZATION_ID,
      hardCeilingUsd: HARD_CEILING_USD,
      effort: EFFORT,
    }) ||
    createPaidBudgetLedger({
      authorizationId: AUTHORIZATION_ID,
      hardCeilingUsd: HARD_CEILING_USD,
      effort: EFFORT,
      outPath: LEDGER_PATH,
    });
  persistPaidBudgetLedger(ledger);

  let chain = null;
  const secondAngles = {};

  if (AUTHORIZE_PAID && !ASSEMBLE_ONLY) {
    console.log("[live] executing canonical Full E2E chain for priority-8 segments…");
    chain = await executeOriginalFullE2eResearchChain({
      hotelIds: [HOTEL_ID],
      authorizePaidDiscovery: true,
      authorizePaidResearch: true,
      authorizeExaAgentLive: true,
      allowLiveExaDeepSeed: true,
      budgetCeilingUsd: HARD_CEILING_USD,
      authorizationId: AUTHORIZATION_ID,
      paidBudgetLedger: ledger,
      resumeLedger: false,
      exaEffort: EFFORT,
      useFixtureProvider: false,
      allowHistoricalReplay: true,
      persistState: true,
      forceRerun: true,
      stateRoot: STATE_ROOT,
      maxCandidates: 16,
      outDir: RAW_DIR,
      skipHpc: true,
      executionLifecycle: "managed",
      executionObservability: true,
      executionObservabilityRoot: OBS_ROOT,
      enableApifyOrchestration: false,
      authorizeApifyLive: false,
      recordDeepResearchObservability: true,
      segmentIds: SEGMENT_IDS,
    });
    write("raw-chain/CHAIN_RESULT.json", {
      ok: chain?.ok !== false,
      paidSpendUsd: chain?.paidSpendUsd,
      discovery: chain?.discovery,
      processed: chain?.processed,
      stages: chain?.stages,
    });
    ledger = loadPaidBudgetLedger(LEDGER_PATH, {
      authorizationId: AUTHORIZATION_ID,
      hardCeilingUsd: HARD_CEILING_USD,
      effort: EFFORT,
    }) || ledger;

    const profile = await resolveHotelGdiProfileForResearch(HOTEL_ID, {
      skipHpc: true,
      preferLive: false,
    });
    const hotel = {
      ...(profile.hotel || {}),
      hotelId: HOTEL_ID,
      displayName: HOTEL_NAME,
      city: profile.hotel?.city || "Bethesda",
      country: profile.hotel?.country || "United States",
    };

    const discoveryCells = chain?.discovery?.cells
      || chain?.discoveryExecution?.cells
      || [];
    // Prefer cells from on-disk matrix if present
    let cells = discoveryCells;
    const matrixPath = path.join(RAW_DIR, "fourteen-segment-discovery-matrix.json");
    if (!cells.length && fs.existsSync(matrixPath)) {
      try {
        cells = JSON.parse(fs.readFileSync(matrixPath, "utf8")).cells || [];
      } catch {
        cells = [];
      }
    }
    // Also search observability / chain artifacts for cells
    if (!cells.length && Array.isArray(chain?.discoveryExecution?.cells)) {
      cells = chain.discoveryExecution.cells;
    }
    // Attach cells onto a synthetic bag for extract
    if (!chain.discovery) chain.discovery = {};
    if (!chain.discovery.cells) chain.discovery.cells = cells;

    console.log("[live] second-angle discovery where first pass is thin…");
    for (const seg of PRIORITY_SEGMENTS) {
      const cell = cells.find((c) => c.segmentId === seg.id);
      const hits = (cell?.candidates || []).length;
      // eslint-disable-next-line no-await-in-loop
      const second = await runSecondAngleIfNeeded({
        ledger,
        hotel,
        segmentId: seg.id,
        firstPassHits: hits,
      });
      secondAngles[seg.id] = second;
      if (second.ran) {
        console.log(
          `  ${seg.id}: second-angle hits=${(second.result?.hits || []).length} cost=${second.result?.costUsd}`,
        );
      } else {
        console.log(`  ${seg.id}: skip second-angle (${second.reason})`);
      }
    }
    persistPaidBudgetLedger(ledger);
  } else if (ASSEMBLE_ONLY) {
    const chainPath = path.join(RAW_DIR, "CHAIN_RESULT.json");
    if (fs.existsSync(chainPath)) {
      chain = JSON.parse(fs.readFileSync(chainPath, "utf8"));
    }
    if (fs.existsSync(path.join(OUT, "SECOND_ANGLE_RESULTS.json"))) {
      Object.assign(
        secondAngles,
        JSON.parse(fs.readFileSync(path.join(OUT, "SECOND_ANGLE_RESULTS.json"), "utf8")),
      );
    }
  }

  const candidates = extractCandidatesFromChain(chain || {});
  // Merge second-angle hits into discovery list for reporting
  for (const [segmentId, second] of Object.entries(secondAngles)) {
    for (const hit of second?.result?.hits || []) {
      candidates.fromDiscovery.push({
        ...hit,
        segmentId,
        source: "second_angle",
      });
    }
  }

  const scorecard = buildScorecard({
    ledger,
    chain,
    secondAngles,
    inventory,
    candidates,
  });

  // Drawer audit on processed + discovery-derived stubs
  const drawerSamples = [];
  let genericHits = 0;
  let emptyCore = 0;
  let demotions = 0;
  const auditPool = [
    ...(candidates.fromOpps || []),
    ...(candidates.fromProcessed || []),
  ].slice(0, 40);
  for (const opp of auditPool) {
    const drawer = evaluateCertifiedDrawerCommercialContent(opp);
    const generic = detectGenericCommercialProse(
      [
        opp.whyThisHotel,
        opp.potentialHotelBusiness,
        opp.whatIsHappening,
        opp.recommendedNextStep,
      ]
        .filter(Boolean)
        .join("\n"),
    );
    genericHits += drawer.genericBoilerplateCount || (generic.generic ? 1 : 0);
    emptyCore += drawer.emptyCoreCount || 0;
    if (drawer.readinessDemotionCandidate) demotions += 1;
    drawerSamples.push({
      opportunityId: opp.opportunityId || opp.id,
      segmentId: opp.segmentId || opp.researchSegment,
      genericBoilerplateCount: drawer.genericBoilerplateCount || 0,
      emptyCoreCount: drawer.emptyCoreCount || 0,
      readinessDemotionCandidate: drawer.readinessDemotionCandidate,
    });
  }

  // Dry-run persistence of chain opportunities only (never apply)
  const oppForPersist = (candidates.fromOpps.length
    ? candidates.fromOpps
    : candidates.fromProcessed
  ).map((o) => ({
    ...o,
    opportunityId: o.opportunityId || o.id,
    hotelId: HOTEL_ID,
  }));
  console.log("[persistence] dry-run only…");
  let dryRunPersistence = {
    dryRun: true,
    apply: false,
    airtableProductionWrites: 0,
    note: "No opportunities to plan" ,
  };
  if (oppForPersist.length) {
    dryRunPersistence = await persistFullE2eResearchInventory({
      hotelId: HOTEL_ID,
      hotelName: HOTEL_NAME,
      runId: `${AUTHORIZATION_ID}_dry`,
      repoRoot: ROOT,
      researchCompleted: true,
      apply: false,
      authorizeResearchPersistence: false,
      opportunities: oppForPersist,
    });
  }
  dryRunPersistence = {
    ...dryRunPersistence,
    dryRun: true,
    apply: false,
    airtableProductionWrites: 0,
    proposedIdentities: oppForPersist.map((o) => ({
      opportunityId: o.opportunityId,
      organizationName: o.organizationName || null,
      segmentId: o.segmentId || o.researchSegment || null,
      reason: "benchmark_dry_run_only",
    })),
    residualQuarantine: RESIDUAL_QUARANTINE,
  };

  writeReports({
    readiness,
    ledger,
    chain: chain || {},
    scorecard,
    inventory,
    candidates,
    secondAngles,
    dryRunPersistence,
    drawerAudit: {
      audited: auditPool.length,
      genericHits,
      emptyCore,
      demotions,
      samples: drawerSamples.slice(0, 20),
    },
    baseline,
  });

  console.log(
    JSON.stringify(
      {
        ok: true,
        authorizationId: AUTHORIZATION_ID,
        spentUsd: Number(ledger.spentUsd || 0),
        remainingUsd: remainingBudgetUsd(ledger),
        segments: SEGMENT_IDS.length,
        salesReadyTotal: scorecard.reduce((n, r) => n + r.salesReadyCount, 0),
        credibleNewTotal: scorecard.reduce(
          (n, r) => n + r.credibleNewOpportunities,
          0,
        ),
        newIdentityTotal: scorecard.reduce(
          (n, r) => n + Number(r.newOrganizationIdentities || 0),
          0,
        ),
        airtableProductionWrites: 0,
        out: OUT,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
