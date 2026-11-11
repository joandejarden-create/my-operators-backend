/**
 * Code-derived audit: discovery vs maturation (V4).
 * Reports actual entrypoints and gaps — not a hypothetical architecture.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  COMMERCIAL_DEMAND_DISCOVERY_V4,
  generateHotelDemandDiscoveryStrategies,
  loadHotelGdiConfig,
  threeHotelDiscoveryBenchmarkScope,
  planIndependentSourceExpansion,
} from "./commercial-demand-discovery-v4.js";
import { SUPERVISOR_EXPECTED_SEGMENTS } from "./research-constants-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const OUT_DIR = path.join(ROOT, "reports/gdi-commercial-demand-discovery-v4");

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

/**
 * Trace actual research entrypoints from repository modules.
 */
export function auditDiscoveryVersusMaturationV4() {
  const findings = [
    {
      id: "entry_ors_mature",
      question: "Where do new organizations/meetings/programs originate vs mature?",
      finding: "Maturation sole orchestrator is matureOpportunity (opportunity-research-supervisor-v1). Discovery historically enters via orchestrator-segment-discovery-v1 → event-to-demand (seed packs + optional live calendars) or Parallel discovery adapter, then queues into Hotel Research Orchestrator / ORS. V3 live validation and three-hotel benchmarks largely mature frozen corpora rather than generating new hotel-specific searches.",
      evidenceModules: [
        "lib/group-demand-intelligence/research/opportunity-research-supervisor-v1.js",
        "lib/group-demand-intelligence/research/orchestrator-segment-discovery-v1.js",
        "lib/group-demand-intelligence/research/event-to-demand/event-to-demand-discovery-v1.js",
        "lib/group-demand-intelligence/research/v3-targeted-live-validation.js",
        "lib/group-demand-intelligence/research/three-hotel-benchmark-execute-v2.js",
      ],
      gap: "Primary commercial spend path repeatedly re-validates known orgs; hotel-profile-generated demand search was not the default objective.",
    },
    {
      id: "frozen_corpus_restriction",
      question: "Is the discovery pool restricted to frozen candidate corpora?",
      finding: "Commercial yield/completion/V3 reset operate on the frozen 22-candidate three-hotel cohort. Segment pilots (e.g. Nobu DMC) prefer frozen six-org lists before paid discovery. Event-to-demand uses market seed packs under fixtures/group-demand-intelligence/event-to-demand/ with optional live calendar merge — not a frozen-only hard lock, but live benchmarks underpaid Parallel stay corpus-bound.",
      evidenceModules: [
        "lib/group-demand-intelligence/research/commercial-yield-recovery-v2.js",
        "lib/group-demand-intelligence/research/commercial-qualification-reset-v3.js",
        "lib/group-demand-intelligence/research/segment-pilot-professional-planners-dmc-nobu-v1.js",
        "lib/group-demand-intelligence/research/event-to-demand/event-to-demand-discovery-v1.js",
      ],
      gap: "YES for the commercial validation path that consumed recent live budget — effectively frozen-candidate maturation.",
    },
    {
      id: "hotel_characteristic_search",
      question: "Are discovery searches dynamically generated from hotel characteristics?",
      finding: "Hotel configs (capabilityProfile, demandTerritory, commercialPriorities) exist and feed planners/coverage. Dynamic research planner proposes questions for an existing opportunity. Orchestrator segment discovery selects shallow segments then E2D/Parallel — it does not systematically emit activity-seeking queries from rooms/meeting inventory/positioning across all 11 segments as the primary discovery driver.",
      evidenceModules: [
        "config/group-demand-intelligence/hotels/*.json",
        "lib/group-demand-intelligence/research/dynamic-research-planner-v1.js",
        "lib/group-demand-intelligence/research/demand-segment-research-supervisor-v1.js",
      ],
      gap: "Hotel characteristics inform fit/coverage but were not the primary generator of new demand-search strategies in the commercial path. V4 generateHotelDemandDiscoveryStrategies closes this.",
    },
    {
      id: "canonical_queue_entry",
      question: "Do newly discovered demand signals enter the canonical research queue?",
      finding: "E2D candidates and Parallel normalized candidates can enter the orchestrator queue. ORS V2 hooks include a discovery rehearsal that marks entered_candidate_queue. There was no durable commercial-demand-discovery track separating NEW_DISCOVERY from EXISTING_REFINEMENT in the V3 live path.",
      evidenceModules: [
        "lib/group-demand-intelligence/research/ors-supervisor-v2-hooks.js",
        "lib/group-demand-intelligence/research/hotel-research-orchestrator-v1.js",
      ],
      gap: "Queue entry exists for E2D/Parallel; V4 enqueueDiscoveryIntoOrsQueue makes hotel-specific discovery an explicit NEW_DISCOVERY track into ORS-shaped opportunities.",
    },
    {
      id: "lead_follow_unknown_orgs",
      question: "Can lead-following expand into previously unknown organizations?",
      finding: "autonomous-lead-following-v2 extracts related entities and defers live retrieval to ORS related_entity_followup. Expansion is bounded (maxFollowUps) and typically stays near the seed organization's graph — not a hotel-wide demand search that invents new accounts from inventory.",
      evidenceModules: [
        "lib/group-demand-intelligence/research/autonomous-lead-following-v2.js",
        "lib/group-demand-intelligence/research/opportunity-research-supervisor-v1.js",
      ],
      gap: "Related-entity follow-up can surface unknown orgs when links exist; it does not replace hotel-specific demand discovery across segments.",
    },
    {
      id: "public_path_exhausted",
      question: "Does PUBLIC_PATH_EXHAUSTED incorrectly end all commercially relevant investigation?",
      finding: "Constants already document PUBLIC_PATH_EXHAUSTED as public/Jev path done under paidBlocked — resumable. classifyExhaustionStop distinguishes PAID_FOLLOWUP_AVAILABLE / PROVIDER_BLOCKED. In practice V3 live stops often labeled PUBLIC_PATH_EXHAUSTED after org homepage/seed URLs without generating independent-publisher or program-specific searches. V4 stopWith now attaches planIndependentSourceExpansion and remaps to PROVIDER_RESTRICTED_NOT_EXHAUSTION when paid paths are blocked.",
      evidenceModules: [
        "lib/group-demand-intelligence/research/research-constants-v1.js",
        "lib/group-demand-intelligence/research/opportunity-research-supervisor-v1.js",
        "lib/group-demand-intelligence/research/commercial-demand-discovery-v4.js",
      ],
      gap: "Behavioral conflation in live runs — fixed at ORS stop boundary for V4.",
    },
  ];

  const strategyDemo = threeHotelDiscoveryBenchmarkScope().map((h) => {
    const pack = generateHotelDemandDiscoveryStrategies(h.config || h);
    return {
      hotelId: h.hotelId,
      hotelName: h.displayName,
      strategyCount: pack.strategyCount,
      allElevenSegmentsCovered: pack.allElevenSegmentsCovered,
      segmentsCovered: pack.segmentsCovered,
    };
  });

  const expansionDemo = planIndependentSourceExpansion({
    opportunity: { organizationName: "Example Org", eventName: "Annual Meeting" },
    hotel: { city: "London", displayName: "Nobu Hotel London Portman Square" },
    paidBlocked: true,
    remainingBudgetUsd: 1.0,
  });

  return {
    version: COMMERCIAL_DEMAND_DISCOVERY_V4,
    auditedAt: new Date().toISOString(),
    expectedSegments: SUPERVISOR_EXPECTED_SEGMENTS,
    findings,
    hotelStrategyGenerationProof: strategyDemo,
    publicPathExpansionClarification: {
      means: expansionDemo.publicPathExhaustedMeans,
      notFullEnd: expansionDemo.notEquivalentToFullInvestigationEnd,
      blockedExample: expansionDemo.blockedByAuthorizationOrProvider.map((b) => b.limitation),
      stopIfCannotContinue: expansionDemo.stopReasonIfCannotContinue,
    },
    integrationChanges: [
      "Added commercial-demand-discovery-v4.js (strategies, dedupe, prioritize, ORS enqueue, expansion planning).",
      "ORS stopWith attaches independent-source expansion after PUBLIC_PATH_EXHAUSTED.",
      "demand-evidence-dimensions-v2 strips parent-org National Conference nav; blocks guestroom inference from that chrome.",
      "commercial-qualification-v3 distinguishes announced cycle vs open hotel sourcing; does not auto-infer overflow.",
      "V3 qualification remains SoT inside ORS hooks; publication gate untouched.",
    ],
  };
}

export function writeDiscoveryAuditV4Report(audit = auditDiscoveryVersusMaturationV4()) {
  ensureDir(OUT_DIR);
  const jsonPath = path.join(OUT_DIR, "DISCOVERY_VS_MATURATION_AUDIT.json");
  const mdPath = path.join(OUT_DIR, "DISCOVERY_VS_MATURATION_AUDIT.md");
  fs.writeFileSync(jsonPath, JSON.stringify(audit, null, 2));
  const md = [
    "# Discovery vs Maturation Audit (V4)",
    "",
    `Version: \`${audit.version}\``,
    "",
    "## Findings",
    ...audit.findings.flatMap((f) => [
      "",
      `### ${f.id}`,
      `**Q:** ${f.question}`,
      "",
      f.finding,
      "",
      `**Gap:** ${f.gap}`,
      "",
      `Modules: ${f.evidenceModules.map((m) => `\`${m}\``).join(", ")}`,
    ]),
    "",
    "## Hotel-specific strategy generation (proof)",
    "",
    "| Hotel | Strategies | All 11 segments |",
    "|---|---|---|",
    ...audit.hotelStrategyGenerationProof.map((r) =>
      `| ${r.hotelName} | ${r.strategyCount} | ${r.allElevenSegmentsCovered ? "Y" : "N"} |`),
    "",
    "## Integration changes",
    ...audit.integrationChanges.map((c) => `- ${c}`),
    "",
  ];
  fs.writeFileSync(mdPath, md.join("\n"));
  return { jsonPath, mdPath, audit };
}

export { OUT_DIR as DISCOVERY_V4_REPORT_DIR };
