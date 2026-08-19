/**
 * Brand AI Visibility — admin reference (prompts/themes + benchmark cohort peers).
 * Served via GET /api/support/ai-visibility-benchmark-admin (admin auth required).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  ACTIVE_SHOWCASE_INTENTS,
  SHOWCASE_INTENT_DEFINITIONS,
} from "../ai-visibility/showcase-intents.js";
import {
  PEER_SET_ID_V2,
  PEER_SET_ID_V5,
  loadPeerSetConfig,
  peerSetBrandNamesById,
} from "../ai-visibility/peer-sets.js";
import { loadApprovedInternalAdditionsConfig } from "../ai-visibility/competitive-moat/approved-internal-additions.js";
import { runBrandPresenceIndexPilot } from "../ai-visibility/competitive-moat/brand-presence-index-pilot.js";
import { buildPromptMetadataById } from "../ai-visibility/associations/prompt-metadata-lookup.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..");
const PROMPT_SEED_PATH = path.join(
  ROOT,
  "fixtures",
  "ai-visibility",
  "phase3a9-showcase-prompt-seed.json"
);
const OBSERVED_PROMPTS_PATH = path.join(
  ROOT,
  "fixtures",
  "ai-visibility",
  "observed-demand-prompts-v1.json"
);
const PILOT_REPORT_PATH = path.join(
  ROOT,
  "reports",
  "ai-visibility",
  "brand-presence-index-pilot-v1.json"
);

function loadPilotReport() {
  if (fs.existsSync(PILOT_REPORT_PATH)) {
    return JSON.parse(fs.readFileSync(PILOT_REPORT_PATH, "utf8"));
  }
  return runBrandPresenceIndexPilot({ writeReport: true });
}

function loadShowcasePromptSeed() {
  if (!fs.existsSync(PROMPT_SEED_PATH)) return { prompts: [], activeShowcaseIntents: [] };
  return JSON.parse(fs.readFileSync(PROMPT_SEED_PATH, "utf8"));
}

function loadObservedPromptSeed() {
  if (!fs.existsSync(OBSERVED_PROMPTS_PATH)) return { prompts: [] };
  return JSON.parse(fs.readFileSync(OBSERVED_PROMPTS_PATH, "utf8"));
}

function geoLabel(prompt) {
  if (!prompt) return "—";
  if (prompt.geographyScope === "Global") return "Global";
  if (prompt.country) return prompt.country;
  if (prompt.commercialRegion) return prompt.commercialRegion;
  return prompt.geographyScope || "—";
}

function escapeCell(value) {
  return String(value ?? "—")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function buildIntentTerritorySection() {
  const rows = ACTIVE_SHOWCASE_INTENTS.map((name) => {
    const def = SHOWCASE_INTENT_DEFINITIONS[name];
    if (!def) return [escapeCell(name), "—", "—", "—"];
    return [
      `<strong>${escapeCell(def.displayName)}</strong>`,
      escapeCell(def.definition),
      escapeCell(def.ownerDecision),
      escapeCell(def.geographicRelevance),
    ];
  });

  return {
    id: "intent-territories",
    title: "2. Active intent territories (Wave-1 showcase)",
    defaultOpen: true,
    contentBlocks: [
      {
        type: "paragraph",
        html:
          "These six owner-decision territories govern the monitored Brand AI prompt set. Eligibility is used downstream for analysis — not injected into open-ended prompt text.",
      },
      {
        type: "table",
        headers: ["Intent territory", "Definition", "Owner decision", "Geography"],
        rows,
      },
    ],
  };
}

function buildPromptInventorySections(seed) {
  const prompts = (seed.prompts || []).filter((p) => p.active !== false && p.monitoringEligible !== false);
  const byIntent = new Map();
  for (const intent of ACTIVE_SHOWCASE_INTENTS) byIntent.set(intent, []);
  for (const p of prompts) {
    const key = p.intentTerritory;
    if (!byIntent.has(key)) byIntent.set(key, []);
    byIntent.get(key).push(p);
  }

  const sections = [];
  let index = 0;
  for (const [intent, list] of byIntent) {
    if (!list.length) continue;
    list.sort((a, b) => {
      const geo = geoLabel(a).localeCompare(geoLabel(b));
      if (geo !== 0) return geo;
      return String(a.language || "").localeCompare(String(b.language || ""));
    });
    sections.push({
      id: `prompts-${intent.replace(/\s+/g, "-").toLowerCase()}`,
      title: `3.${index + 1} Prompts — ${intent} (${list.length})`,
      defaultOpen: index === 0,
      contentBlocks: [
        {
          type: "table",
          headers: ["Prompt ID", "Geography", "Lang", "Prompt text"],
          rows: list.map((p) => [
            `<code>${escapeCell(p.promptId)}</code>`,
            escapeCell(geoLabel(p)),
            escapeCell(p.language || "en"),
            escapeCell(p.promptText),
          ]),
        },
      ],
    });
    index += 1;
  }

  return {
    summaryBlock: {
      type: "paragraph",
      html: `<strong>${prompts.length}</strong> active, monitoring-eligible prompts in <code>phase3a9-showcase-prompt-seed.json</code> (${escapeCell(seed.governanceVersion || "phase3a9_v1")}). Peer set at prompt definition: <code>${escapeCell(seed.peerSetId || PEER_SET_ID_V2)}</code>.`,
    },
    sections,
  };
}

function buildObservedPromptsSection(observedSeed) {
  const prompts = (observedSeed.prompts || []).filter((p) => p.active !== false);
  if (!prompts.length) {
    return {
      id: "observed-prompts",
      title: "4. Observed-demand prompts (not yet monitoring)",
      contentBlocks: [
        {
          type: "paragraph",
          html: "No observed-demand prompt seed found.",
        },
      ],
    };
  }

  const monitoring = prompts.filter((p) => p.monitoringEligible === true);
  const paused = prompts.filter((p) => p.monitoringEligible !== true);

  return {
    id: "observed-prompts",
    title: "4. Observed-demand prompts (V1 seed — monitoring off)",
    contentBlocks: [
      {
        type: "paragraph",
        html:
          "Literal observed queries from licensed SEO demand signals. <strong>monitoringEligible=false</strong> until stability gates approve activation. These do not replace scenario prompts.",
      },
      {
        type: "table",
        headers: ["Prompt ID", "Theme", "Geography", "Lang", "Monitoring", "Prompt text"],
        rows: prompts.map((p) => [
          `<code>${escapeCell(p.promptId)}</code>`,
          escapeCell(p.observedTheme || p.intentTerritory),
          escapeCell(geoLabel(p)),
          escapeCell(p.language || "en"),
          p.monitoringEligible ? "Eligible" : "Paused",
          escapeCell(p.promptText || p.observedQuery),
        ]),
      },
      {
        type: "paragraph",
        html: `Summary: ${monitoring.length} monitoring-eligible, ${paused.length} paused.`,
      },
    ],
  };
}

function buildBenchmarkOverviewSection(pilotReport, peerNames) {
  const additions = loadApprovedInternalAdditionsConfig();
  return {
    id: "benchmark-overview",
    title: "5. AI Presence Index — benchmark construction",
    defaultOpen: true,
    contentBlocks: [
      {
        type: "paragraph",
        html:
          "The AI Presence Index compares each showcase brand's presence rate against a contextual peer cohort drawn from the internal benchmark peer set. Customer-facing Brand AI still uses peer v2 for comparative rank; this page reflects the expanded internal peer set used for index piloting.",
      },
      {
        type: "table",
        headers: ["Setting", "Value"],
        rows: [
          ["Customer-visible portfolio brands", String(pilotReport.customerVisibleBrands || 19)],
          ["Live comparative peer set (customer)", `<code>${PEER_SET_ID_V2}</code>`],
          ["Internal benchmark peer set (index pilot)", `<code>${PEER_SET_ID_V5}</code>`],
          ["Internal-only benchmark additions", String(additions.additions?.length || 7)],
          ["Measurement period", "DEMO_VALIDATION (re-extracted stored responses)"],
          ["Provider calls", "0"],
          ["Index readiness", escapeCell(pilotReport.readiness?.aiPresenceIndex || "READY_FOR_INTERNAL_REVIEW")],
          ["Benchmark aggregation", "Median of peer presence rates"],
        ],
      },
      {
        type: "alert",
        html:
          "<strong>Cohort integrity note.</strong> Index values depend on which peers share prompt×provider×geo grains with the subject (UNION denominator). Run <code>npm run benchmark-cohort-integrity:audit</code> before calibrating mean vs median. Vignette is a known governance gap in peer v5.",
      },
      {
        type: "heading",
        level: 4,
        text: "Internal-only benchmark brands (not customer-visible)",
      },
      {
        type: "table",
        headers: ["Brand", "Parent", "Cohort tags"],
        rows: (additions.additions || []).map((a) => [
          `<strong>${escapeCell(a.brandName)}</strong>`,
          escapeCell(a.canonicalParent),
          escapeCell((a.cohortTags || []).join(", ")),
        ]),
      },
    ],
  };
}

function buildPerBrandCohortSection(pilotReport, peerNames) {
  const subjects = [...(pilotReport.pilotResults?.subjects || [])].sort((a, b) =>
    String(a.subject || "").localeCompare(String(b.subject || ""))
  );

  const summaryRows = subjects.map((s) => {
    const members = s.internalPayload?.benchmarkMembers || [];
    const peerNamesList = members
      .map((m) => m.entityName || peerNames[m.entityId] || m.entityId)
      .filter(Boolean);
    return [
      `<strong>${escapeCell(s.subject)}</strong>`,
      `<code>${escapeCell(s.cohortType)}</code>`,
      s.aiPresenceIndex != null ? String(Math.round(s.aiPresenceIndex)) : "—",
      escapeCell(s.benchmarkStatus),
      String(members.length),
      escapeCell(peerNamesList.join(", ")),
    ];
  });

  const detailSections = subjects.map((s, i) => {
    const members = s.internalPayload?.benchmarkMembers || [];
    const cohort = s.internalPayload?.cohortSelectionExplanation || {};
    return {
      id: `brand-cohort-${s.subjectEntityId}`,
      title: `6.${i + 1} ${s.subject} — peers (${members.length})`,
      contentBlocks: [
        {
          type: "table",
          headers: ["Field", "Value"],
          rows: [
            ["Brand ID", `<code>${escapeCell(s.subjectEntityId)}</code>`],
            ["Primary cohort type", `<code>${escapeCell(s.cohortType)}</code>`],
            ["Used broader peer-set fallback", cohort.usedBroaderFallback ? "Yes" : "No"],
            ["Subject presence rate", s.subjectPresence != null ? `${Math.round(s.subjectPresence * 1000) / 10}%` : "—"],
            ["Benchmark presence (median peers)", s.benchmarkPresence != null ? `${Math.round(s.benchmarkPresence * 1000) / 10}%` : "—"],
            ["AI Presence Index", s.aiPresenceIndex != null ? String(Math.round(s.aiPresenceIndex)) : "—"],
            ["Benchmark status", escapeCell(s.benchmarkStatus)],
            ["Benchmark sample (peer count)", String(s.benchmarkSample ?? members.length)],
          ],
        },
        {
          type: "heading",
          level: 4,
          text: "Brands used to calculate this index",
        },
        {
          type: "table",
          headers: ["Peer brand", "Presence rate in shared grains"],
          rows: members.length
            ? members.map((m) => [
                escapeCell(m.entityName || peerNames[m.entityId]),
                m.presenceRate != null ? `${Math.round(m.presenceRate * 1000) / 10}%` : "—",
              ])
            : [["—", "No peers resolved"]],
        },
        {
          type: "paragraph",
          html:
            'Per-brand JSON diagnostics: <code>GET /api/ai-visibility/brand/' +
            escapeCell(s.subjectEntityId) +
            "/benchmark/diagnostics</code>",
        },
      ],
    };
  });

  return {
    summarySection: {
      id: "brand-cohort-summary",
      title: "6. Benchmark peers by showcase brand (summary)",
      defaultOpen: true,
      contentBlocks: [
        {
          type: "paragraph",
          html:
            "For each customer-visible showcase brand, the table lists the contextual peer cohort used as the benchmark denominator for the AI Presence Index pilot. Expand a brand section below for presence rates per peer.",
        },
        {
          type: "table",
          headers: ["Brand", "Cohort", "Index", "Status", "Peer count", "Benchmark peers"],
          rows: summaryRows,
        },
      ],
    },
    detailSections,
  };
}

function buildGovernanceSection() {
  const metadataCount = buildPromptMetadataById().size;
  return {
    id: "governance",
    title: "7. Governance & related docs",
    contentBlocks: [
      {
        type: "unorderedList",
        items: [
          "Prompt seed: <code>fixtures/ai-visibility/phase3a9-showcase-prompt-seed.json</code>",
          "Observed demand: <code>fixtures/ai-visibility/observed-demand-prompts-v1.json</code>",
          "Intent definitions: <code>lib/ai-visibility/showcase-intents.js</code>",
          "Cohort resolver: <code>lib/ai-visibility/competitive-moat/contextual-cohort-v1.js</code>",
          "Index pilot: <code>docs/ai-visibility/brand-presence-index-pilot-v1.md</code>",
          "Cohort integrity audit: <code>docs/ai-visibility/benchmark-cohort-integrity-audit-v1.md</code>",
          `Prompt metadata lookup cache: ${metadataCount} governed prompt IDs`,
        ],
      },
      {
        type: "paragraph",
        html:
          "<strong>Do not</strong> expose full peer matrices or internal benchmark brands on customer-facing Brand AI. Customer dropdown remains the 19-brand showcase portfolio.",
      },
    ],
  };
}

/** @returns {object} */
export function getAiVisibilityBenchmarkAdminRunbook() {
  const seed = loadShowcasePromptSeed();
  const observedSeed = loadObservedPromptSeed();
  const pilotReport = loadPilotReport();
  const peerCfg = loadPeerSetConfig();
  const peerNames = peerSetBrandNamesById(PEER_SET_ID_V5, peerCfg);

  const promptInventory = buildPromptInventorySections(seed);
  const perBrand = buildPerBrandCohortSection(pilotReport, peerNames);

  return {
    title: "Brand AI Visibility — Prompts & Benchmark Cohorts",
    subtitle:
      "Admin reference for monitored prompt themes and the peer brands used to calculate each showcase brand's AI Presence Index (internal benchmark peer set v5).",
    badges: [
      { label: "Admin Only", variant: "internal" },
      { label: "AI Visibility" },
      { label: "DEMO_VALIDATION" },
    ],
    warning:
      "<strong>Internal admin only.</strong> Shows full benchmark peer lists and internal-only brands not visible to customers. Do not share externally.",
    sections: [
      {
        id: "overview",
        title: "1. Overview",
        defaultOpen: true,
        contentBlocks: [
          {
            type: "paragraph",
            html:
              "This page answers two questions for Brand AI visibility work: <strong>(1)</strong> which owner-decision prompts and intent themes we monitor, and <strong>(2)</strong> which brands form the benchmark cohort behind each showcase brand's AI Presence Index.",
          },
          promptInventory.summaryBlock,
          {
            type: "paragraph",
            html:
              "Prompt entity mode: <strong>OPEN_ENDED</strong> — brand names are not injected into prompt text; peer resolution happens downstream from stored provider responses.",
          },
        ],
      },
      buildIntentTerritorySection(),
      ...promptInventory.sections,
      buildObservedPromptsSection(observedSeed),
      buildBenchmarkOverviewSection(pilotReport, peerNames),
      perBrand.summarySection,
      ...perBrand.detailSections,
      buildGovernanceSection(),
    ],
    meta: {
      generatedAt: new Date().toISOString(),
      promptSeedId: seed.seedId || null,
      pilotVersion: pilotReport.pilotVersion || null,
      peerSetId: PEER_SET_ID_V5,
      providerCalls: 0,
    },
  };
}
