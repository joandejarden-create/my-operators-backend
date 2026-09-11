/**
 * Cambridge Full HI adapter — research corpus → client-safe dossier.
 * Packet 2.7-R2 preserved research; Packet 2.7-R5 (dossier quality) enforces
 * INTERNAL RESEARCH vs CUSTOMER dossier compile boundary.
 * No paid Webhound. No KGPV content. No invented facts.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createEmptyDossier,
  refreshDossierCounts,
  DOSSIER_SECTION_TITLES,
  DOSSIER_TITLE,
} from "../schema.js";
import { markdownToBlocks } from "./from-webhound-kgpv-modules.js";
import {
  prepareCustomerProse,
  prepareCustomerProseDeep,
  customerRelationshipLabels,
  parseMarkdownSourceIndex,
  normalizeResearchSources,
  mapClaimSourceIdsToDisplayNumbers,
  validateClientSafeReport,
} from "../client-safe/index.js";
import {
  applyRoomsResolutionToPortfolioHotel,
  buildResearchRoomsObservationsFromExistingCorpus,
} from "../../property-fundamentals/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../..");

export const CAMBRIDGE_AIRTABLE_ID = "recIwaP1etgx2g9nA";
export const CAMBRIDGE_DHL = "dhl_06G6TRD5N8Q1YVXKSFD1A5E2NT";
export const CAMBRIDGE_DOSSIER_ID_V3 = "dossier_cambridge_beaches_full_hi_v3";
export const CAMBRIDGE_DOSSIER_ID_V2 = "dossier_cambridge_beaches_full_hi_v2";
export const CAMBRIDGE_DOSSIER_ID_V1 = "dossier_cambridge_beaches_full_hi_v1";
export const CAMBRIDGE_SESSION = "9d6b0a8d-e038-44ce-ab61-2ee92a2a4807";

const DEFAULT_MD = path.join(
  ROOT,
  "data/hotel-intelligence/research/hotels/recIwaP1etgx2g9nA/raw/webhound-9d6b0a8d-output.md"
);
const DEFAULT_SLIM = path.join(
  ROOT,
  "data/hotel-intelligence/research/hotels/recIwaP1etgx2g9nA/raw/webhound-9d6b0a8d-evidence-slim.json"
);
const DEFAULT_DEEP = path.join(ROOT, "fixtures/golden-demo/cambridge-beaches-deep-research-v1.json");

function wordCountFromBlocks(blocks) {
  let n = 0;
  for (const b of blocks || []) {
    if (b.type === "paragraphs") n += b.paragraphs.join(" ").split(/\s+/).filter(Boolean).length;
    if (b.type === "list") n += b.items.join(" ").split(/\s+/).filter(Boolean).length;
    if (b.type === "heading") n += String(b.text || "").split(/\s+/).filter(Boolean).length;
    if (b.type === "table") {
      n += (b.headers || []).join(" ").split(/\s+/).filter(Boolean).length;
      for (const row of b.rows || []) n += row.join(" ").split(/\s+/).filter(Boolean).length;
    }
    if (b.type === "callout" || b.type === "finding_callout") {
      n += String(b.title || b.text || b.body || "").split(/\s+/).filter(Boolean).length;
    }
  }
  return n;
}

/**
 * Split markdown into heading→body maps. When duplicate headings exist, keep the longest body.
 */
export function splitMarkdownByHeadings(md) {
  const text = String(md || "").replace(/\r\n/g, "\n");
  const parts = text.split(/\n(?=#{1,3}\s+)/);
  const map = new Map();
  for (const part of parts) {
    const m = part.match(/^(#{1,3})\s+([^\n]+)\n?([\s\S]*)$/);
    if (!m) continue;
    const title = m[2].replace(/\*\*/g, "").trim();
    const body = (m[3] || "").trim();
    const prev = map.get(title);
    if (!prev || body.length > prev.length) map.set(title, body);
  }
  return map;
}

function findBodies(map, predicates) {
  const out = [];
  for (const [title, body] of map.entries()) {
    if (predicates.some((re) => re.test(title))) out.push({ title, body });
  }
  // Prefer longer bodies first when concatenating
  out.sort((a, b) => b.body.length - a.body.length);
  return out;
}

function joinBodies(entries, maxChars = null) {
  const seen = new Set();
  const chunks = [];
  let total = 0;
  for (const e of entries) {
    const key = e.body.slice(0, 200);
    if (seen.has(key)) continue;
    seen.add(key);
    if (maxChars != null && total + e.body.length > maxChars && chunks.length) break;
    chunks.push(`## ${e.title}\n\n${e.body}`);
    total += e.body.length;
  }
  return chunks.join("\n\n");
}

function blocksFromMdChunks(entries) {
  const md = prepareCustomerProse(joinBodies(entries));
  const blocks = markdownToBlocks(md);
  return prepareCustomerProseDeep(blocks);
}

function normalizeSources(slim, deep, mdText) {
  const raw = (slim?.evidence?.sources || deep?.sources || []).slice();
  const mdIndex = parseMarkdownSourceIndex(mdText || "");
  return normalizeResearchSources(raw, {
    mdSourceIndex: mdIndex,
    observedDate: slim?.completed_at || deep?.observed_date || "2026-09-04",
    accessDate: slim?.completed_at || null,
  });
}

function claimsToCandidates(claims) {
  return (claims || []).map((c, idx) => ({
    claim_id: c.claim_id || c.id || `claim_${idx + 1}`,
    text: c.claim || c.text || "",
    explanation: c.explanation || c.evidence_note || c.evidence || null,
    confidence: c.importance || c.confidence || null,
    source_ids: c.source_ids || [],
    auto_promote: false,
    status: "RESEARCH_FINDING",
  }));
}

function buildKeyFindingsFromClaims(claims, byInternalId) {
  const ranked = (claims || []).slice().sort((a, b) => {
    const ai = Number(a.importance) || 0;
    const bi = Number(b.importance) || 0;
    return bi - ai;
  });
  const picks = ranked.slice(0, 8);
  return picks.map((c, i) => ({
    id: `kf_cb_v3_${i + 1}`,
    headline: prepareCustomerProse(String(c.claim || "").slice(0, 160)),
    explanation: prepareCustomerProse(
      String(c.explanation || c.evidence || c.method || "").slice(0, 600)
    ),
    body: prepareCustomerProse(
      String(c.explanation || c.evidence || c.method || "").slice(0, 600)
    ),
    status: "RESEARCH_FINDING",
    citations: mapClaimSourceIdsToDisplayNumbers(c.source_ids || [], byInternalId),
  }));
}

function buildExecutiveSummary(deep) {
  const profile = deep?.property_profile || {};
  const acre = profile.acreage || {};
  return {
    paragraphs: [
      "Cambridge Beaches Resort & Spa is an independent luxury resort on a private peninsula in Sandys Parish, Bermuda, with approximately 86 suites across a 23-acre setting. Historical tourism listings that cite 20 acres are disclosed as a conflicting reference and are not silently dropped.",
      "Public Bermuda government materials identify Cambridge Beaches Holdings Limited as the property owner and designated hotel developer. Dovetail + Co acquired the resort in 2021 and is the economic sponsor behind the current ownership era. Deed-level natural-person beneficial ownership and CBHL director/shareholder records remain unresolved in reviewed public sources.",
      "Current first-party evidence strongly supports Dovetail-led stewardship and operating control, although historical Benchmark/Pyramid involvement and continuing residual portfolio references leave the precise formal management arrangement unresolved.",
      "The property underwent a substantial multi-year repositioning following the 2021 acquisition, including accommodations, public spaces, F&B, spa and resort amenities. Public editorial evidence indicates that most of the redesign was substantially complete by 2023. Formal Tourism Investment completion certification remains a separate open item.",
      "Butterfield Bank is publicly identified as a lender to the acquisition/redevelopment programme. Public evidence does not establish loan quantum, terms, maturity or current registered charges.",
      "Phil Hospod is the strongest publicly evidenced sponsor/control contact through Dovetail + Co. Clarence Hofheins provides the strongest property-level operating path as long-serving General Manager. Karla Bruning appears in journalism as a co-owner narrative and is not registry-verified as a CBHL shareholder.",
      "Priority unresolved items include CBHL shareholder/director records, formal operator contract status, deed/title chain around historical Frascati ownership, Butterfield security/charge records, and formal Tourism Investment completion certification.",
    ],
  };
}

function buildPriorityOpenQuestions() {
  return [
    {
      id: "oq_cb_v3_1",
      question: "Who are the registered directors, shareholders and beneficial owners of Cambridge Beaches Holdings Limited?",
      why_it_matters: "Resolves legal control of the PropCo and clarifies who can authorize ownership-level decisions.",
      best_next_evidence: "Bermuda Registrar / corporate registry search and any disclosed UBO filings.",
      priority: "HIGH",
      status: "OPEN",
    },
    {
      id: "oq_cb_v3_2",
      question: "What is the current contractual status of Benchmark / Pyramid relative to Cambridge Beaches?",
      why_it_matters: "Resolves management rights, fee economics and who controls day-to-day operating decisions.",
      best_next_evidence: "Management agreement, termination notice, or direct confirmation from owner and operator.",
      priority: "HIGH",
      status: "OPEN",
    },
    {
      id: "oq_cb_v3_3",
      question: "What charges or security interests are registered against CBHL or the property?",
      why_it_matters: "Clarifies capital structure, lender priority and refinance / sale constraints.",
      best_next_evidence: "Corporate/title registry and Butterfield financing documents.",
      priority: "HIGH",
      status: "OPEN",
    },
    {
      id: "oq_cb_v3_4",
      question: "What is the exact legal seller identity behind the 2021 acquisition (Frascati naming discrepancy)?",
      why_it_matters: "Press names Frascati Hotel Company while historical corporate records show a 2011 dissolution — title chain needs qualification.",
      best_next_evidence: "Deed/title instrument, successor entity records, or closing documents.",
      priority: "HIGH",
      status: "OPEN",
    },
    {
      id: "oq_cb_v3_5",
      question: "Has Tourism Investment Order completion / certified opening been formally documented?",
      why_it_matters: "Separates physical redevelopment progress from statutory completion conditions.",
      best_next_evidence: "Government certification or Order-condition compliance evidence.",
      priority: "MEDIUM",
      status: "OPEN",
    },
    {
      id: "oq_cb_v3_6",
      question: "What is the commercial meaning of residual Pyramid / Benchmark portfolio and media listings?",
      why_it_matters: "Distinguishes stale marketing assets from an active management or commercial association.",
      best_next_evidence: "Operator confirmation and current portfolio disclosure review.",
      priority: "MEDIUM",
      status: "OPEN",
    },
    {
      id: "oq_cb_v3_7",
      question: "Does Karla Bruning hold any registry-verified interest in CBHL or the hotel?",
      why_it_matters: "Journalism co-owner language must not be confused with deed-level ownership.",
      best_next_evidence: "Registry shareholder/director confirmation.",
      priority: "MEDIUM",
      status: "OPEN",
    },
    {
      id: "oq_cb_v3_8",
      question: "What loan quantum, maturity and covenants apply to the Butterfield financing?",
      why_it_matters: "Lender identity is confirmed; economics and security package are not public.",
      best_next_evidence: "Facility documents or registered charge particulars.",
      priority: "MEDIUM",
      status: "OPEN",
    },
  ];
}

function ownershipDiagram() {
  return {
    type: "ownership_diagram",
    nodes: [
      { role: "hotel", name: "Cambridge Beaches Resort & Spa" },
      { role: "propco", name: "Cambridge Beaches Holdings Limited — property owner / hotel developer" },
      { role: "parent", name: "Dovetail + Co — economic sponsor / acquirer" },
      {
        role: "control",
        name: "Phil Hospod — Dovetail Founder & CEO (sponsor control path; not deed-level CBHL UBO)",
      },
    ],
    note:
      "Distinguish property owner, developer, economic sponsor, and control figure. Operator and brand affiliation are separate relationship layers. Journalism co-owner language is not treated as deed-level natural-person title.",
  };
}

/**
 * @param {object} [options]
 */
export function adaptCambridgeWebhoundToDossier(options = {}) {
  const mdPath = options.mdPath || DEFAULT_MD;
  const slimPath = options.slimPath || DEFAULT_SLIM;
  const deepPath = options.deepPath || DEFAULT_DEEP;

  const md = fs.readFileSync(mdPath, "utf8");
  const slim = JSON.parse(fs.readFileSync(slimPath, "utf8"));
  const deep = fs.existsSync(deepPath) ? JSON.parse(fs.readFileSync(deepPath, "utf8")) : {};
  const headingMap = splitMarkdownByHeadings(md);
  const claims = slim?.evidence?.claims || [];
  const sourceMeta = normalizeSources(slim, deep, md);
  const sources = sourceMeta.normalized;
  const byInternalId = sourceMeta.byInternalId || new Map();

  const profile = deep.property_profile || {};
  const acre = profile.acreage || {};

  const identityBlocks = [
    {
      type: "paragraphs",
      paragraphs: [
        `Cambridge Beaches Resort & Spa — ${profile.address || "30 Kings Point Road, Somerset / Sandys Parish, Bermuda"}.`,
        `Product: approximately ${profile.rooms_suites || 86} suites. Acreage: best-supported ${acre.best_supported_current || 23} acres; conflicting tourism-listing values ${Array.isArray(acre.conflict?.values) ? acre.conflict.values.join(" and ") : "20 and 23"} acres remain disclosed.`,
        `Established context: ${profile.established_year || 1923} as Bermuda's first cottage-style accommodations. Positioning: ${profile.positioning || deep.hotel?.positioning_statement || "Independent luxury cottage resort"}.`,
        "Identity checks: not Beaches Resorts / Sandals; not Cambridge, Massachusetts. Coordinates are available in Dealality property records when useful for mapping.",
      ].map(prepareCustomerProse),
    },
    {
      type: "list",
      items: (profile.amenities || []).slice(0, 16).map(prepareCustomerProse),
    },
  ];

  const ownershipMd = findBodies(headingMap, [
    /^A\.\s*Current Ownership/i,
    /^B\.\s*CBHL/i,
    /^C\.\s*Beneficial Ownership/i,
    /^Bermuda Entity Structure/i,
    /^Cambridge Beaches Holdings Limited/i,
    /^CBHL Role/i,
    /^The Frascati Hotel Company/i,
    /^Trott Family Ownership/i,
    /^Karla Bruning Role/i,
    /^Ownership Chain \(Verified\)/i,
    /^Other Bermuda Entities/i,
    /^Tourism Investment Order 2022/i,
  ]);
  const operatorMd = findBodies(headingMap, [
    /^E\.\s*Current Operator/i,
    /^Operator Resolution/i,
    /^Evidence For Dovetail/i,
    /^Evidence of Benchmark/i,
    /^Timeline of Operator/i,
    /^NEW FINDINGS/i,
    /^Key Finding: Evidence strongly supports Dovetail/i,
  ]);
  const developmentMd = findBodies(headingMap, [
    /^F\.\s*Redevelopment/i,
    /^Timeline of Key Events/i,
    /^Redevelopment Status/i,
    /^Acreage Conflict/i,
    /^Key Dates/i,
  ]);
  const peopleMd = findBodies(headingMap, [
    /^H\.\s*Current Property Leadership/i,
    /^Property Leadership/i,
    /^Current Property Leadership/i,
    /^Dovetail \+ Co Leadership/i,
  ]);
  const txMd = findBodies(headingMap, [
    /^D\.\s*Acquisition Structure/i,
    /^G\.\s*Financing/i,
    /^Financing & Public Charges/i,
    /^Butterfield Bank Financing/i,
    /^Confirmed: Butterfield/i,
    /^Financing Structure Known/i,
    /^Butterfield Bank Context/i,
    /^Argus Group Involvement/i,
    /^Acquisition Details/i,
    /^Historic Ownership Timeline/i,
    /^Notable: Frascati/i,
  ]);

  const brandBlocks = [
    {
      type: "paragraphs",
      paragraphs: [
        `Current brand / affiliation: ${(deep.brand_resolution && (deep.brand_resolution.current_trading_brand || deep.brand_resolution.current_brand)) || "Independent — Cambridge Beaches"} (${(deep.brand_resolution && deep.brand_resolution.current_status) || "CURRENT"}).`,
        "Owner/operator stewardship branding (“A Dovetail + Co Production” / steward language) is distinct from guest-facing chain affiliation. Residual Pyramid Gemstone Collection taxonomy is not treated as a guest-facing chain brand unless first-party hotel surfaces confirm it.",
        deep.brand_resolution?.resolution ||
          "Research treats Cambridge Beaches as an independent trading brand under Dovetail stewardship, not Beaches Resorts.",
      ]
        .filter(Boolean)
        .map(prepareCustomerProse),
    },
  ];

  const reconciliationBlocks = [
    {
      type: "paragraphs",
      paragraphs: [
        "Acreage: 23 acres is best-supported across Dovetail first-party and multiple trade/press sources; GoToBermuda and related tourism copy also use 20 acres in places. Both values remain disclosed.",
        "Operator: Benchmark's May 2021 announcement is historical. Current public-facing evidence points to Dovetail-led stewardship and operation. Continuing Pyramid/Benchmark portfolio references indicate that some commercial association may remain. No reviewed public source definitively resolves the current contractual management arrangement. Absence of third-party operator branding on hotel careers/contact surfaces weakens an active-management hypothesis but does not alone prove termination.",
        "CBHL: Tourism Investment Order defines Cambridge Beaches Holdings Limited as hotel developer; ministerial materials describe CBHL as property owner. These roles are related but not identical.",
        "Dovetail: organizational capability statements (owner / developer / operator) do not automatically create property-level OWNED / DEVELOPED / OPERATED relationships without property-linked evidence.",
        "Karla Bruning: 2025 journalism co-owner narrative is retained as a research finding only — not deed-verified beneficial ownership.",
        "Frascati: press names Frascati Hotel Company as 2021 seller while historical corporate data indicate The Frascati Hotel Company Limited was dissolved in 2011 — treated as a seller-identity discrepancy requiring title-chain qualification.",
      ].map(prepareCustomerProse),
    },
  ];

  const peopleStructured = (deep.people || []).map((p) => {
    const url =
      p.linkedin_url ||
      p.professional_profile_url ||
      (p.professional_profile && p.professional_profile.url) ||
      null;
    const verified =
      p.professional_profile_verified === true ||
      p.professional_profile_status === "VERIFIED" ||
      (p.professional_profile && p.professional_profile.verified === true);
    const safeUrl =
      verified && url && /linkedin\.com\/in\//i.test(String(url)) ? url : null;
    return {
      person_id: p.person_id || null,
      name: p.name,
      full_name: p.name,
      title: p.title || p.role || null,
      current_title: p.title || p.role || null,
      organization: p.organization || null,
      relationship_to_hotel: p.relationship_to_hotel || p.relevance || null,
      relevance: p.relevance || p.note || null,
      authority_evidence: p.authority_evidence || p.evidence || null,
      authority_status: p.decision_authority || p.authority_status || "Authority Not Verified",
      professional_profile_url: safeUrl,
      professional_profiles: safeUrl
        ? [{ type: "LINKEDIN", url: safeUrl, verified: true }]
        : [],
      professional_profile_status: safeUrl ? "VERIFIED" : p.professional_profile_status || "UNKNOWN",
      professional_profile_verified: Boolean(safeUrl),
      confidence: p.confidence || null,
      temporal: p.temporal || p.status || "CURRENT",
      current_historical_status: p.temporal || p.status || "CURRENT",
    };
  });

  const verifiedProfileRows = peopleStructured
    .filter((p) => p.professional_profile_verified && p.professional_profile_url)
    .map((p) => [
      `${p.name} — Professional Profile`,
      p.professional_profile_url,
      "Verified LinkedIn (person record)",
    ]);
  const unresolvedProfileRows = peopleStructured
    .filter((p) => !p.professional_profile_verified)
    .map((p) => [
      `${p.name} — Professional Profile`,
      "—",
      p.professional_profile_status === "NOT_FOUND"
        ? "No verified person-level LinkedIn in reviewed public sources"
        : "Profile not verified for product display",
    ]);

  const contactBlocks = [
    {
      type: "table",
      headers: ["Channel", "Contact", "Source basis"],
      rows: [
        ["Hotel website", "https://www.cambridgebeaches.com", "First-party hotel"],
        ["Hotel phone", "+441234 0331", "First-party / tourism listings"],
        [
          "Hotel address",
          profile.address || "30 Kings Point Road, Somerset, Sandys Parish, Bermuda",
          "Property profile",
        ],
        ["Sponsor website", "https://www.dovetailandco.com", "Dovetail first-party"],
        ...verifiedProfileRows,
        ...unresolvedProfileRows,
      ],
    },
    {
      type: "paragraphs",
      paragraphs: [
        "Professional Profile rule: only verified person-level public profiles show View LinkedIn. Unverified profiles display as —.",
      ],
    },
  ];

  const priorityOpenQuestions = buildPriorityOpenQuestions();
  const internalGaps = (deep.research_gaps || []).map((g, i) => ({
    id: `gap_internal_${i + 1}`,
    question: typeof g === "string" ? g : g.what || g.question || String(g),
    priority: "INTERNAL",
    status: "OPEN",
  }));

  const roomsObs = buildResearchRoomsObservationsFromExistingCorpus(
    (deep.portfolio_notes?.assets || []).map((a) => ({ name: a.name }))
  );

  const portfolioRows = (deep.portfolio_notes?.assets || []).map((a) => {
    const resolved = applyRoomsResolutionToPortfolioHotel(
      {
        name: a.name,
        rooms: a.rooms,
        rooms_confidence: a.rooms_confidence || a.confidence,
        rooms_source_type: a.rooms_source_type,
        accommodation_units: a.accommodation_units,
      },
      { researchObservations: roomsObs }
    );
    const roomsCell =
      resolved.rooms != null
        ? String(resolved.rooms)
        : a.accommodation_units != null
          ? `${a.accommodation_units} accommodations`
          : "—";
    return [
      a.name,
      a.market || "—",
      roomsCell,
      a.brand || "—",
      customerRelationshipLabels(a.relationships || a.relationship_type || "—"),
      a.status || "Open",
      a.confidence || "PROBABLE",
    ];
  });

  const operatorIntro = {
    type: "paragraphs",
    paragraphs: [
      "Current public-facing evidence points to Dovetail-led stewardship and operation. Benchmark/Pyramid's historical management role is verified, and continuing portfolio references indicate that some commercial association may remain. No reviewed public source definitively resolves the current contractual management arrangement.",
    ].map(prepareCustomerProse),
  };

  const commercialBlocks = [
    {
      type: "heading",
      level: 3,
      text: "Who matters",
    },
    {
      type: "paragraphs",
      paragraphs: [
        "Phil Hospod (Dovetail Founder & CEO) is the strongest sponsor/control path. Clarence Hofheins (General Manager) is the strongest property operating path. James Kot appears as Dovetail Chief Development Officer. Approach CBHL for Bermuda legal/developer matters.",
      ].map(prepareCustomerProse),
    },
    {
      type: "heading",
      level: 3,
      text: "Why now",
    },
    {
      type: "paragraphs",
      paragraphs: [
        "Dovetail ownership and repositioning era, substantially complete design-led redevelopment by 2023, independent brand positioning, and unresolved formal operator status create a live diligence window.",
      ].map(prepareCustomerProse),
    },
    {
      type: "heading",
      level: 3,
      text: "What to verify before outreach",
    },
    {
      type: "list",
      items: [
        "Authority to discuss ownership, brand or management changes",
        "Current operator / management agreement status",
        "Ownership documents and CBHL control",
        "Debt / security position with Butterfield",
        "Future brand strategy for the independent product",
      ],
    },
  ];

  const evidenceQualityBlocks = [
    {
      type: "paragraphs",
      paragraphs: [
        "Evidence quality for this investigation draws on Government / Legal materials (Tourism Investment Order; ministerial statements), first-party hotel and owner sites, local transaction press, operator/portfolio materials, professional profiles, and historical corporate data. Search snippets and residual portfolio listings are treated as discovery or ambiguous evidence — not decisive current proof.",
      ].map(prepareCustomerProse),
    },
  ];

  const sections = [
    {
      id: "property_identity",
      title: DOSSIER_SECTION_TITLES.property_identity,
      blocks: identityBlocks,
    },
    {
      id: "ownership_chain_propco",
      title: DOSSIER_SECTION_TITLES.ownership_chain_propco,
      blocks: [ownershipDiagram(), ...blocksFromMdChunks(ownershipMd)],
    },
    {
      id: "operator_management",
      title: DOSSIER_SECTION_TITLES.operator_management,
      blocks: [operatorIntro, ...blocksFromMdChunks(operatorMd)],
    },
    {
      id: "brand_reflag",
      title: DOSSIER_SECTION_TITLES.brand_reflag,
      blocks: brandBlocks,
    },
    {
      id: "property_history",
      title: DOSSIER_SECTION_TITLES.property_history,
      blocks: blocksFromMdChunks(developmentMd),
    },
    {
      id: "organization_portfolio",
      title: DOSSIER_SECTION_TITLES.organization_portfolio,
      blocks: [
        {
          type: "paragraphs",
          paragraphs: [
            "Known Dovetail-linked hotel relationships vary by asset. Organizational capability statements do not automatically create ownership, development, or operating relationships for every listed property. Room counts use Census when available; otherwise validated research observations (for example Wayfinder Newport).",
          ].map(prepareCustomerProse),
        },
        {
          type: "table",
          table_kind: "portfolio",
          headers: ["Property", "Market", "Rooms / Keys", "Brand", "Relationship", "Status", "Confidence"],
          rows: portfolioRows,
        },
      ],
    },
    {
      id: "people_decision_authority",
      title: DOSSIER_SECTION_TITLES.people_decision_authority,
      blocks: [
        {
          type: "paragraphs",
          paragraphs: [
            "People below use one resolved person model across this report. Public titles are not treated as legal deed-signing authority without independent instrument evidence.",
          ].map(prepareCustomerProse),
        },
        ...blocksFromMdChunks(peopleMd),
        {
          type: "table",
          table_kind: "people",
          headers: ["Name", "Title", "Organization", "Profile", "Confidence"],
          rows: peopleStructured.map((p) => [
            p.name,
            p.title || "—",
            p.organization || "—",
            p.professional_profile_verified ? p.professional_profile_url : "—",
            p.confidence || "—",
          ]),
        },
      ],
    },
    {
      id: "transactions_capital",
      title: DOSSIER_SECTION_TITLES.transactions_capital,
      blocks: blocksFromMdChunks(txMd),
    },
    {
      id: "commercial_pursuit",
      title: DOSSIER_SECTION_TITLES.commercial_pursuit,
      blocks: commercialBlocks,
    },
    {
      id: "open_questions",
      title: DOSSIER_SECTION_TITLES.open_questions,
      blocks: [
        {
          type: "list",
          items: priorityOpenQuestions.map(
            (q) =>
              `${q.question} — Why it matters: ${q.why_it_matters} Best next evidence: ${q.best_next_evidence}`
          ),
        },
      ],
    },
    {
      id: "sources_evidence",
      title: DOSSIER_SECTION_TITLES.sources_evidence,
      blocks: [
        ...evidenceQualityBlocks,
        {
          type: "table",
          table_kind: "sources",
          headers: ["#", "Title", "Publisher", "Type", "URL"],
          rows: sources.map((s) => [
            String(s.number),
            s.title,
            s.publisher || "—",
            s.source_type || "—",
            s.url || "—",
          ]),
        },
      ],
    },
    {
      id: "research_reconciliation",
      title: DOSSIER_SECTION_TITLES.research_reconciliation,
      blocks: reconciliationBlocks,
    },
    {
      id: "contacts_appendix",
      title: DOSSIER_SECTION_TITLES.contacts_appendix,
      blocks: contactBlocks,
    },
  ];

  const bodyWords = sections.reduce((acc, s) => acc + wordCountFromBlocks(s.blocks), 0);
  const archiveWords = md.split(/\s+/).filter(Boolean).length;

  const dossier = createEmptyDossier({
    dossier_id: CAMBRIDGE_DOSSIER_ID_V3,
    hotel_id: CAMBRIDGE_DHL,
    hotel_airtable_record_id: CAMBRIDGE_AIRTABLE_ID,
    hotel_name: "Cambridge Beaches Resort & Spa",
    title: DOSSIER_TITLE,
    status: "COMPLETED_WITH_OPEN_QUESTIONS",
    research_provider: "WEBHOUND",
    research_run_id: CAMBRIDGE_SESSION,
    version: 3,
    started_at: "2026-09-04T17:17:00.000Z",
    completed_at: "2026-09-04T17:39:05.000Z",
    research_cost_usd: Number(slim.actual_cost_usd || 5),
    executive_summary: buildExecutiveSummary(deep),
    key_findings: buildKeyFindingsFromClaims(claims, byInternalId),
    sections,
    open_questions: priorityOpenQuestions,
    sources,
    people: peopleStructured,
    relationships: deep.relationships || [],
    entities: deep.entities || [],
    methodology_summary:
      "Full Hotel Intelligence Investigation compiled from government, first-party, press, operator, professional-profile and historical corporate sources. Findings remain research-grade until independently reviewed.",
    raw_artifact_reference: {
      kind: "webhound_cambridge_full_hi",
      path: "data/hotel-intelligence/research/hotels/recIwaP1etgx2g9nA/raw/webhound-9d6b0a8d-output.md",
      session_id: CAMBRIDGE_SESSION,
      slim_path: "data/hotel-intelligence/research/hotels/recIwaP1etgx2g9nA/raw/webhound-9d6b0a8d-evidence-slim.json",
      immutable: true,
    },
    mapping_notes: [
      "v1 thin summary SUPERSEDED; v2 pre-client-safe SUPERSEDED by dossier_cambridge_beaches_full_hi_v3.",
      `Archive MD words≈${archiveWords}; dossier body words≈${bodyWords}.`,
      `Sources: raw=${sourceMeta.raw_count}, normalized=${sources.length}, deduped/rejected=${sourceMeta.deduped}.`,
      `Claims accounted in claim_handoff.candidates=${claims.length}.`,
      "Customer surfaces sanitized: no provider name/cost/run IDs, no internal record IDs, no ontology codes, no research-instruction leaks.",
      "No KGPV / GSF / Breathless content included.",
    ],
    claim_handoff: {
      auto_promote: false,
      candidates: claimsToCandidates(claims),
    },
  });

  dossier.investigation_status = "COMPLETED_WITH_OPEN_QUESTIONS";
  dossier.hotel_location = {
    city: "Sandys Parish",
    country: "Bermuda",
    label: "Sandys Parish, Bermuda",
  };
  dossier.golden_demo = "GOLDEN_DEMO_2";
  dossier.substantive_word_count = bodyWords;
  dossier.archive_word_count = archiveWords;
  dossier.source_accounting = {
    raw_sources: sourceMeta.raw_count,
    normalized_sources: sources.length,
    report_sources: sources.length,
    deduped: sourceMeta.deduped,
    rejected: sourceMeta.rejected,
  };
  dossier.claim_accounting = {
    raw_claims: claims.length,
    candidates: claims.length,
    key_findings: (dossier.key_findings || []).length,
  };
  dossier.internal_research_gaps = internalGaps;
  dossier.cover_metrics = {
    sources: sources.length,
    key_findings: (dossier.key_findings || []).length,
    priority_open_questions: priorityOpenQuestions.length,
  };
  dossier.supersedes = CAMBRIDGE_DOSSIER_ID_V2;
  dossier.schema_version = "hotel-intelligence-dossier-v1.1";
  dossier.customer_visible = true;

  const safe = validateClientSafeReport(dossier);
  dossier._client_safe_validation = {
    ok: safe.ok,
    error_count: safe.errors.length,
    warning_count: safe.warnings.length,
    errors: safe.errors.slice(0, 40),
  };
  if (!safe.ok && options.strictClientSafe !== false) {
    const err = new Error(
      `Cambridge dossier failed client-safe validation: ${safe.errors.slice(0, 12).join(" | ")}`
    );
    err.clientSafe = safe;
    throw err;
  }

  return refreshDossierCounts(dossier);
}

export function loadAndAdaptCambridgeFullDossier(options = {}) {
  return adaptCambridgeWebhoundToDossier(options);
}
