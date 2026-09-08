/**
 * Packet 2.6B-R — Full preservation adapter from archived Webhound KGPV modules.
 * No Webhound network calls. No invented facts.
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
import { buildClaimCandidatesFromDeep } from "./from-kgpv-deep-research.js";
import { prepareCustomerProseDeep } from "../client-safe/text-normalize.js";
import { enrichReportForPublishing } from "../report-hotel-identity.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ARCHIVE_DIR = path.resolve(
  __dirname,
  "../../../../fixtures/hotel-intelligence/dossier/source-archive"
);
const MODULES_DIR = path.join(ARCHIVE_DIR, "modules");
const DEEP_PATH = path.resolve(
  __dirname,
  "../../../../fixtures/golden-demo/krystal-grand-pv-deep-research-v1.json"
);

const MODULE_MAP = [
  { file: "01-ownership-chain-propco.md", sectionId: "ownership_chain_propco" },
  { file: "02-operator-management.md", sectionId: "operator_management" },
  { file: "03-brand-reflag.md", sectionId: "brand_reflag" },
  { file: "04-property-history.md", sectionId: "property_history" },
  { file: "05-organization-portfolio.md", sectionId: "organization_portfolio" },
  { file: "06-people-decision-authority.md", sectionId: "people_decision_authority" },
  { file: "07-commercial-pursuit.md", sectionId: "commercial_pursuit" },
];

function stripMdNoise(s) {
  return String(s || "")
    .replace(/\*\*/g, "")
    .replace(/`/g, "")
    .trim();
}

function parseMarkdownTable(lines) {
  const rows = lines
    .map((l) => l.trim())
    .filter((l) => l.startsWith("|") && !/^\|\s*-+/.test(l))
    .map((l) =>
      l
        .replace(/^\|/, "")
        .replace(/\|$/, "")
        .split("|")
        .map((c) => stripMdNoise(c))
    );
  if (rows.length < 2) return null;
  return { type: "table", headers: rows[0], rows: rows.slice(1) };
}

/**
 * Convert markdown body into dossier blocks, preserving substantive text.
 */
export function markdownToBlocks(md) {
  const lines = String(md || "").replace(/\r\n/g, "\n").split("\n");
  const blocks = [];
  let i = 0;
  let paraBuf = [];
  let listBuf = [];
  let tableBuf = [];

  function flushPara() {
    if (!paraBuf.length) return;
    const text = paraBuf.join(" ").replace(/\s+/g, " ").trim();
    paraBuf = [];
    if (!text) return;
    if (/^##\s/.test(text)) return;
    blocks.push({ type: "paragraphs", paragraphs: [text] });
  }
  function flushList() {
    if (!listBuf.length) return;
    blocks.push({ type: "list", items: listBuf.slice() });
    listBuf = [];
  }
  function flushTable() {
    if (!tableBuf.length) return;
    const t = parseMarkdownTable(tableBuf);
    tableBuf = [];
    if (t) blocks.push(t);
  }

  while (i < lines.length) {
    const raw = lines[i];
    const line = raw.trim();
    i += 1;
    if (!line) {
      flushPara();
      flushList();
      flushTable();
      continue;
    }
    if (line.startsWith("|")) {
      flushPara();
      flushList();
      tableBuf.push(line);
      continue;
    }
    if (tableBuf.length) flushTable();

    if (/^#{1,4}\s+/.test(line)) {
      flushPara();
      flushList();
      const level = (line.match(/^#+/) || ["#"])[0].length;
      const title = stripMdNoise(line.replace(/^#{1,4}\s+/, ""));
      if (/^Ownership Chain & PropCo|^Operator & Management|^Brand & Reflag|^Property History|^GSF Organization|^People & Decision|^Commercial & Pursuit|^Sources$/i.test(title)) {
        continue;
      }
      blocks.push({ type: "heading", level, text: title });
      continue;
    }
    if (/^[-*]\s+/.test(line)) {
      flushPara();
      listBuf.push(stripMdNoise(line.replace(/^[-*]\s+/, "")));
      continue;
    }
    if (listBuf.length) flushList();
    paraBuf.push(stripMdNoise(line));
  }
  flushPara();
  flushList();
  flushTable();
  return blocks.filter((b) => {
    if (b.type === "paragraphs") return b.paragraphs?.some(Boolean);
    if (b.type === "list") return b.items?.length;
    if (b.type === "table") return b.rows?.length;
    if (b.type === "heading") return Boolean(b.text);
    return true;
  });
}

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
      n += `${b.title || ""} ${b.headline || ""} ${b.body || ""}`.split(/\s+/).filter(Boolean).length;
    }
  }
  return n;
}

function parseSourcesFile(md) {
  const sources = [];
  const lines = String(md || "").split(/\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed.startsWith("[")) continue;
    const m =
      trimmed.match(
        /^\[(\d+)\]\s+(.+?)\s+[—\-–]\s+(https?:\/\/\S+)\s+[—\-–]\s+(\S+)\s+[—\-–]\s+(.+)$/u
      ) ||
      trimmed.match(/^\[(\d+)\]\s+(.+?)\s+(https?:\/\/\S+)\s+(\S+)\s+(.+)$/u) ||
      trimmed.match(/^\[(\d+)\]\s+\[?(https?:\/\/[^\s\]]+)\]?(?:\((https?:\/\/[^)]+)\))?/);
    if (!m) continue;
    if (m[3] && String(m[3]).startsWith("http")) {
      sources.push({
        number: Number(m[1]),
        title: m[2].trim(),
        url: m[3].trim(),
        publisher: (m[4] || "").trim() || null,
        source_type: (m[5] || "").trim() || null,
        date: null,
      });
    } else if (m[2] && String(m[2]).startsWith("http")) {
      sources.push({
        number: Number(m[1]),
        title: m[2].trim(),
        url: m[3] || m[2],
        publisher: null,
        source_type: null,
        date: null,
      });
    }
  }
  // Fallback master bibliography if module file parse fails.
  if (!sources.length) {
    const fallback = [
      ["GSF 4Q24 Earnings Report", "https://gsf-hotels.com/corporativo/en/investors/pdf/HOTEL_4Q24_ENG.pdf", "gsf-hotels.com", "securities disclosure"],
      ["GSF 1Q25 Earnings Report", "https://gsf-hotels.com/corporativo/en/investors/pdf/HOTEL_1Q25_ENG.pdf", "gsf-hotels.com", "securities disclosure"],
      ["GSF 4Q23 Earnings Report", "https://gsf-hotels.com/corporativo/en/investors/pdf/HOTEL_4Q23_ENG.pdf", "gsf-hotels.com", "securities disclosure"],
      ["CoStar: SFG Seeks to Reinvigorate Krystal Brand", "https://www.costar.com/article/1650410982/sfg-seeks-to-reinvigorate-krystal-brand", "costar.com", "trade press"],
      ["GSF Reporte Anual 2024", "https://gsf-hotels.com/corporativo/en/investors/pdf/Hotel_Reporte_Anual_2024.pdf", "gsf-hotels.com", "securities disclosure"],
      ["GSF Reporte Anual 2023 XBRL", "https://gsf-hotels.com/corporativo/en/investors/pdf/HOTEL_Reporte_anual_2023_XBRL.pdf", "gsf-hotels.com", "securities disclosure"],
      ["Hyatt Newsroom — Breathless Puerto Vallarta announcement", "https://newsroom.hyatt.com/news-releases?item=124458", "newsroom.hyatt.com", "brand/corporate news"],
      ["Hyatt Inclusive Collection — Krystal Grand Puerto Vallarta", "https://www.hyattinclusivecollection.com/en/resorts-hotels/independent-properties/krystal-grand-puerto-vallarta/", "hyattinclusivecollection.com", "brand distribution"],
      ["GSF — Krystal Grand Vallarta corporate page", "https://gsf-hotels.com/corporativo/en/krystal_grand_vallarta.php", "gsf-hotels.com", "first party"],
      ["Travel Weekly — Krystal Grand Puerto Vallarta", "https://www.travelweekly.com/Hotels/Puerto-Vallarta/Krystal-Grand-Puerto-Vallarta-p61237595", "travelweekly.com", "trade press"],
      ["Official hotel website", "https://en.krystalgrand-puertovallarta.com/", "krystalgrand-puertovallarta.com", "first party"],
      ["GSF Press Releases", "https://gsf-hotels.com/corporativo/en/comunicados-de-prensa.php", "gsf-hotels.com", "first party"],
      ["GSF Operator page", "https://gsf-hotels.com/corporativo/en/operadora.php", "gsf-hotels.com", "first party"],
      ["GSF BMV page", "https://gsf-hotels.com/corporativo/en/bolsa-mexicana-de-valores.php", "gsf-hotels.com", "first party / IR"],
      ["GSF IR Contact", "https://gsf-hotels.com/corporativo/en/contacto-inversionistas.php", "gsf-hotels.com", "first party / IR"],
      ["StockAnalysis HOTEL", "https://stockanalysis.com/quote/bmv/HOTEL/company/", "stockanalysis.com", "market data"],
      ["GSF Investors hub", "https://gsf-hotels.com/corporativo/en/inversionistas.php", "gsf-hotels.com", "first party / IR"],
      ["GSF Corporate Homepage", "https://gsf-hotels.com/corporativo/en/", "gsf-hotels.com", "first party"],
      ["GSF Corporate Governance", "https://gsf-hotels.com/corporativo/en/gobierno-corporativo.php", "gsf-hotels.com", "first party"],
      ["KGPV Contact Page", "https://en.krystalgrand-puertovallarta.com/location-contact/", "krystalgrand-puertovallarta.com", "first party"],
    ];
    fallback.forEach((row, i) => {
      sources.push({
        number: i + 1,
        title: row[0],
        url: row[1],
        publisher: row[2],
        source_type: row[3],
        date: null,
      });
    });
  }
  return sources;
}

function buildExecutiveSummary() {
  return {
    paragraphs: [
      "This Full Hotel Intelligence Investigation consolidates the strongest available evidence on ownership, operation, brand status, corporate relationships, people and asset history for Krystal Grand Puerto Vallarta. The investigation concludes that the hotel is a company-owned, self-operated GSF asset — not a third-party managed property.",
      "Economic ownership and operating control sit with Grupo Hotelero Santa Fe, S.A.B. de C.V. (BMV: HOTEL). Primary filings support 100% ownership after the 2014 acquisition of Chartwell's remaining stake, with consolidation in GSF's company-owned segment across 4Q24, 1Q25, and 2Q25 disclosures.",
      "The property vehicle identified in related-party filings is Inmobiliaria en Hotelería Vallarta Santa Fe, S. de R.L. de C.V. (IHVSF). IHVSF appears as counterparty to (a) a 2013 federal-zone services contract under concession DZF-334/89 and (b) a 2011 lease-with-purchase-option covering event-hall parcels tied to Hotel Hilton Vallarta — both linking IHVSF to this physical asset.",
      "Public-company beneficial shareholders exceeding 10% of GSF capital are disclosed in the 2025 Annual Report: Carlos Gerardo Ancira Elizondo (29.3585%), Jorge M. Pérez (29.3233%), Pablo Villanueva Martínez (10.5001%), and Hector Fabian Gomez Sainz Garcia (10.0547%). Ancira is designated as the sole shareholder exercising control or command power. These are public-company control findings, not deed-level natural-person title ownership of the PropCo.",
      "Operator posture is owner-operator under GSF's proprietary Krystal Grand brand. The hotel is also distributed via Hyatt Inclusive Collection as an independent property — a distribution/marketing arrangement rather than a current franchise/management agreement. Any intercompany management agreement between PropCo and operating subsidiary is not publicly disclosed.",
      "Current public branding remains Krystal Grand Puerto Vallarta. Brand history runs Hilton (2012) → Krystal Altitude → Krystal Grand. Later brand research resolves that the February 2024 Breathless Puerto Vallarta announcement is a same-asset conversion/rebranding of this hotel — not a separate adjacent or greenfield project.",
      "At the same time, conversion is not shown as completed. 2Q25 still lists Krystal Grand; September 2026 operating checks (TripAdvisor/Google) still show Krystal Grand; Hyatt's active Breathless Mexico destinations do not list Puerto Vallarta. The conversion appears materially delayed and may be postponed or cancelled.",
      "Asset history includes 2011 development with Chartwell, 2012 Hilton opening (~259 rooms), 2014 full GSF ownership, 2016–2018 Hacienda expansion to 451 rooms (259 Resort + 192 Hacienda), all-inclusive Grand Tourism positioning, and extensive amenity inventory.",
      "Organization intelligence positions GSF as a dual-model Mexican public hotel company (~25 hotels / 6,215 rooms as of 2Q25; 15 company-owned; 10 third-party managed), with IR and CFO contact channels published and a multi-brand partner set spanning Hyatt, Hilton, Accor, and Inclusive Collection brands.",
      "Decision relevance concentrates with Francisco Zinser (strategically relevant for brand/partnership strategy and Breathless announcement spokesperson), Francisco Medina (CEO), Carlos Ancira (board president / control shareholder), Enrique Martínez (CFO), and Hyatt counterparts Javier Coll and Camilo Bolaños. Property GM is only partially identified (\"Luis A, Director General\"). Titles establish strategic relevance; decision/signing authority is not independently established.",
      "Principal open issues now concern land tenure (fee simple vs ground lease), deed/notarial signatories, natural-person PropCo UBO beyond consolidated ownership, full GM identity, and an updated Breathless conversion timeline. Earlier gaps on PropCo identity and >10% beneficial shareholders are resolved by later filings and should not be restated as current unknowns.",
    ],
  };
}

function buildKeyFindings() {
  return [
    {
      id: "kf_gsf_owns",
      headline: "GSF owns and self-operates Krystal Grand Puerto Vallarta",
      explanation:
        "Primary earnings disclosures place the hotel in GSF's company-owned segment at 100% ownership; GSF operates under proprietary Krystal Grand branding.",
      status: "VERIFIED_INTELLIGENCE",
      citations: [1, 2, 3],
    },
    {
      id: "kf_ihvsf",
      headline: "IHVSF is the identified PropCo / property vehicle",
      explanation:
        "GSF 2025 Annual Report related-party section names IHVSF in contracts expressly tied to Hilton Vallarta / event-hall parcels at the asset.",
      status: "VERIFIED_INTELLIGENCE",
      citations: [5, 6],
    },
    {
      id: "kf_shareholders",
      headline: "Public-company beneficial shareholders >10% are disclosed",
      explanation:
        "Ancira 29.3585%, Jorge M. Pérez 29.3233%, Villanueva 10.5001%, Gomez Sainz Garcia 10.0547%; Ancira alone designated with control/command power.",
      status: "VERIFIED_INTELLIGENCE",
      citations: [5],
    },
    {
      id: "kf_breathless_same_asset",
      headline: "Breathless is the announced same-asset conversion of this hotel",
      explanation:
        "Hyatt/GSF announcement and later brand research identify Breathless Puerto Vallarta as conversion/rebranding of Krystal Grand at Av. de las Garzas 136 — not a separate development.",
      status: "RESEARCH_FINDING",
      citations: [7, 3, 10],
    },
    {
      id: "kf_breathless_delayed",
      headline: "Breathless conversion is not completed and appears delayed",
      explanation:
        "2Q25 still lists Krystal Grand; Sep 2026 operating checks remain Krystal Grand; Hyatt active Breathless Mexico destinations omit Puerto Vallarta.",
      status: "RESEARCH_FINDING",
      citations: [2, 8, 10],
    },
    {
      id: "kf_resort_distinct",
      headline: "Krystal Resort PV is a distinct Chartwell-owned asset",
      explanation:
        "Adjacent Krystal Resort Puerto Vallarta is Chartwell-owned and GSF-managed under contracts through 2030 — do not conflate ownership with Grand.",
      status: "VERIFIED_INTELLIGENCE",
      citations: [5, 1],
    },
    {
      id: "kf_hacienda",
      headline: "2016–2018 Hacienda expansion created today's 451-room configuration",
      explanation:
        "GSF acquired adjacent units/land in 2016 and opened Hacienda (192 suites) in 2018, bringing the property to 451 rooms.",
      status: "VERIFIED_INTELLIGENCE",
      citations: [5, 12],
    },
    {
      id: "kf_contacts",
      headline: "Corporate IR/CFO and property contact channels are publicly reported",
      explanation:
        "Published contacts include IR emails/phones, CFO email/phone, corporate switchboard, and property reservations email/phone.",
      status: "RESEARCH_FINDING",
      citations: [15, 14, 20],
    },
  ];
}

function buildPropertyIdentityBlocks() {
  return [
    {
      type: "heading",
      level: 3,
      text: "Focus Asset",
    },
    {
      type: "paragraphs",
      paragraphs: [
        "Focus hotel: Krystal Grand Puerto Vallarta (former Hilton Puerto Vallarta / Krystal Altitude), Av. de las Garzas 136, Zona Hotelera, Puerto Vallarta, Jalisco, Mexico 48333 — 451 rooms.",
        "Adjacent distinct hotel: Krystal Resort Puerto Vallarta (Chartwell-owned; GSF-managed) on Av. Francisco Medina Ascencio — not the same ownership chain as Grand.",
      ],
    },
    {
      type: "heading",
      level: 3,
      text: "Research Resolution Highlights",
    },
    {
      type: "list",
      items: [
        "Initial third-party-managed premise was reversed: Grand is GSF-owned and self-operated.",
        "PropCo unknown → IHVSF confirmed via related-party contracts.",
        "Breathless site ambiguity → same-asset announced conversion; conversion itself incomplete/delayed.",
        "Census Breathless affiliation is treated as announced-conversion evidence, not completed reflag.",
      ],
    },
    {
      type: "callout",
      kind: "note",
      title: "Research Status",
      body: "Certain findings remain subject to primary legal, contractual or property-level verification where indicated.",
    },
  ];
}

function buildTransactionsBlocks(ownershipBlocks) {
  const history = ownershipBlocks.filter(
    (b) =>
      (b.type === "heading" && /Ownership History|Critical Additions|Subsidiaries|Distinction/i.test(b.text || "")) ||
      (b.type === "list" && (b.items || []).some((it) => /2014|2016|Hacienda|Development|Brand change/i.test(it))) ||
      (b.type === "paragraphs" && (b.paragraphs || []).some((p) => /Chartwell|Hacienda|IHVSF|Krystal Resort/i.test(p)))
  );
  return [
    {
      type: "heading",
      level: 3,
      text: "Material Capital & Corporate Events",
    },
    {
      type: "paragraphs",
      paragraphs: [
        "This chapter consolidates transaction and capital events from ownership and property-vehicle evidence, including development, Chartwell buyout, IPO-era ownership consolidation, Hacienda expansion investment, related-party leases/contracts, and brand-change announcements.",
      ],
    },
    ...history,
  ];
}

function buildReconciliationBlocks() {
  return [
    {
      type: "heading",
      level: 3,
      text: "Evidence Reconciliation",
    },
    {
      type: "paragraphs",
      paragraphs: [
        "Later primary evidence resolved several earlier uncertainties. The conclusions below reflect the strongest currently available evidence; superseded findings are retained only where useful to explain how the conclusion was resolved.",
      ],
    },
    {
      type: "list",
      items: [
        "Stronger or later primary evidence takes precedence when it clearly resolves an earlier question.",
        "Resolved conclusions are stated once; earlier views are retained only as resolution context.",
        "Remaining uncertainty is labeled Unresolved rather than silently closed.",
      ],
    },
    {
      type: "callout",
      kind: "superseded",
      title: "Resolved research question — Property vehicle identity",
      body: "Previous uncertainty: PropCo entity unknown. Resolved conclusion: GSF 2025 Annual Report related-party disclosures confirm IHVSF as the PropCo / property vehicle tied to the Vallarta event parcels. Historic legal labels may preserve former brand names and are not treated alone as current brand signals.",
    },
    {
      type: "callout",
      kind: "superseded",
      title: "Resolved research question — Beneficial shareholders / control",
      body: "Previous uncertainty: beneficial shareholders and control unknown. Resolved conclusion: Accionistas Beneficiarios >10% and Ancira control/command designation are disclosed in the GSF 2025 Annual Report.",
    },
    {
      type: "callout",
      kind: "superseded",
      title: "Resolved research question — Breathless asset identity",
      body: "Previous uncertainty: Breathless may be adjacent land or a separate development. Resolved conclusion: Brand and reflag evidence identifies Breathless Puerto Vallarta as the announced same-asset conversion of Krystal Grand Puerto Vallarta. Conversion completion remains unproven and appears delayed.",
    },
    {
      type: "callout",
      kind: "superseded",
      title: "Resolved research question — Earlier commercial gaps on PropCo / shareholders",
      body: "Those items are resolved. Remaining commercial gaps are land tenure, updated conversion timeline, full GM identity, deed signatories, and PropCo natural-person UBO beyond consolidated ownership.",
    },
    {
      type: "paragraphs",
      paragraphs: [
        "No contradictory-evidence pairs remain after reconciliation for PropCo identity, public-company beneficial shareholders, or Breathless asset identity. Open questions are incompleteness issues, not mutually exclusive competing conclusions.",
      ],
    },
  ];
}

function buildContactsBlocks() {
  return [
    {
      type: "heading",
      level: 3,
      text: "Published Contact Channels",
    },
    {
      type: "table",
      headers: ["Channel", "Contact", "Source basis"],
      rows: [
        ["Corporate switchboard", "+52 (55) 5261 0800", "GSF corporate contact"],
        ["General email", "contacto@gsf-hotels.com", "GSF corporate contact"],
        ["IR email", "inversionistas@gsf-hotels.com", "GSF IR page"],
        ["IR — Rodrigo Ancira", "+52 (55) 5261 4508 / inversionistas@gsf-hotels.com", "GSF IR page"],
        ["IR — Maximilian Zimmermann", "+52 (55) 5261-4508 / mzimmermann@gsf-hotels.com", "GSF BMV page"],
        ["CFO — Enrique Martínez", "+52 (55) 5261 0807 / emartinez@gsf-hotels.com", "GSF IR page"],
        ["Property reservations", "reservations.kgapv@krystal.hotels.com", "Hotel contact page"],
        ["Property phone", "+52 (322) 176 11 76", "Hotel contact page"],
      ],
    },
  ];
}

function buildOpenQuestions() {
  return [
    {
      id: "oq_land_tenure",
      question: "Is the land fee-simple or subject to ground lease / federal-zone concessions beyond the disclosed event-hall parcels?",
      status: "UNRESOLVED",
    },
    {
      id: "oq_deed_signatories",
      question: "Who are the legal/notarial deed signatories for title?",
      status: "UNRESOLVED",
    },
    {
      id: "oq_propco_nubo",
      question: "Natural-person UBO of IHVSF beyond consolidated public-company ownership remains unverified from deed sources.",
      status: "UNRESOLVED",
    },
    {
      id: "oq_breathless_timeline",
      question: "Updated Breathless conversion timeline — still proceeding, postponed, or cancelled?",
      status: "UNRESOLVED",
    },
    {
      id: "oq_gm",
      question: "Full property GM identity beyond partial 'Luis A, Director General' evidence.",
      status: "UNRESOLVED",
    },
    {
      id: "oq_hacienda_breathless",
      question: "If conversion proceeds, will Hacienda (192 suites) be incorporated into Breathless or operated separately?",
      status: "UNRESOLVED",
    },
  ];
}

export function adaptWebhoundModulesToDossier() {
  const moduleBodies = {};
  for (const m of MODULE_MAP) {
    const p = path.join(MODULES_DIR, m.file);
    if (!fs.existsSync(p)) throw new Error(`Missing module archive: ${m.file}`);
    moduleBodies[m.sectionId] = fs.readFileSync(p, "utf8");
  }
  const sourcesMd = fs.readFileSync(path.join(MODULES_DIR, "99-sources.md"), "utf8");
  const sources = parseSourcesFile(sourcesMd);

  const ownershipBlocks = [
    {
      type: "ownership_diagram",
      nodes: [
        { role: "hotel", name: "Krystal Grand Puerto Vallarta" },
        {
          role: "propco",
          name: "Inmobiliaria en Hotelería Vallarta Santa Fe (IHVSF)",
        },
        { role: "parent", name: "Grupo Hotelero Santa Fe, S.A.B. de C.V." },
        {
          role: "control",
          name: "Public-company control / shareholders (Ancira control/command)",
        },
      ],
      note:
        "Operator, brand affiliation, and Hyatt distribution are separate relationship layers — not ownership. Public-company shareholders are not deed-level natural-person PropCo title.",
    },
    ...markdownToBlocks(moduleBodies.ownership_chain_propco),
  ];
  const operatorBlocks = markdownToBlocks(moduleBodies.operator_management);
  const brandBlocks = markdownToBlocks(moduleBodies.brand_reflag);
  const historyBlocks = markdownToBlocks(moduleBodies.property_history);
  const orgBlocks = markdownToBlocks(moduleBodies.organization_portfolio);
  const peopleBlocks = markdownToBlocks(moduleBodies.people_decision_authority).map((b) => {
    if (
      b.type === "table" &&
      (b.headers || []).some((h) => /^name$/i.test(String(h))) &&
      (b.headers || []).length >= 5
    ) {
      return { ...b, table_kind: "people" };
    }
    if (b.type === "table" && (b.headers || []).length <= 4) {
      return { ...b, table_kind: "executive" };
    }
    if (b.type === "table") return { ...b, table_kind: "evidence" };
    return b;
  });
  const commercialBlocks = markdownToBlocks(moduleBodies.commercial_pursuit);

  const sections = [
    { id: "property_identity", title: DOSSIER_SECTION_TITLES.property_identity, blocks: buildPropertyIdentityBlocks() },
    { id: "ownership_chain_propco", title: DOSSIER_SECTION_TITLES.ownership_chain_propco, blocks: ownershipBlocks },
    { id: "operator_management", title: DOSSIER_SECTION_TITLES.operator_management, blocks: operatorBlocks },
    { id: "brand_reflag", title: DOSSIER_SECTION_TITLES.brand_reflag, blocks: brandBlocks },
    { id: "property_history", title: DOSSIER_SECTION_TITLES.property_history, blocks: historyBlocks },
    { id: "organization_portfolio", title: DOSSIER_SECTION_TITLES.organization_portfolio, blocks: orgBlocks },
    { id: "people_decision_authority", title: DOSSIER_SECTION_TITLES.people_decision_authority, blocks: peopleBlocks },
    {
      id: "transactions_capital",
      title: DOSSIER_SECTION_TITLES.transactions_capital,
      blocks: buildTransactionsBlocks(ownershipBlocks),
    },
    { id: "commercial_pursuit", title: DOSSIER_SECTION_TITLES.commercial_pursuit, blocks: commercialBlocks },
    {
      id: "open_questions",
      title: DOSSIER_SECTION_TITLES.open_questions,
      blocks: [
        {
          type: "list",
          items: buildOpenQuestions().map((q) => q.question),
        },
      ],
    },
    {
      id: "sources_evidence",
      title: DOSSIER_SECTION_TITLES.sources_evidence,
      blocks: [
        {
          type: "table",
          headers: ["#", "Title", "Publisher", "Type", "URL"],
          rows: sources.map((s) => [
            String(s.number),
            s.title,
            s.publisher,
            s.source_type,
            s.url,
          ]),
        },
      ],
    },
    {
      id: "research_reconciliation",
      title: DOSSIER_SECTION_TITLES.research_reconciliation,
      blocks: buildReconciliationBlocks(),
    },
    {
      id: "contacts_appendix",
      title: DOSSIER_SECTION_TITLES.contacts_appendix,
      blocks: buildContactsBlocks(),
    },
  ];

  let deep = null;
  if (fs.existsSync(DEEP_PATH)) {
    deep = JSON.parse(fs.readFileSync(DEEP_PATH, "utf8"));
  }

  const bodyWords = sections.reduce((acc, s) => acc + wordCountFromBlocks(s.blocks), 0);
  const archiveWords = MODULE_MAP.reduce((acc, m) => {
    const t = moduleBodies[m.sectionId] || "";
    return acc + t.split(/\s+/).filter(Boolean).length;
  }, 0);

  const dossier = createEmptyDossier({
    dossier_id: "dossier_kgpv_full_hi_v1",
    hotel_id: "kgpv",
    hotel_airtable_record_id: "recUNycnMwOVFX0hc",
    hotel_name: "Krystal Grand Puerto Vallarta",
    title: DOSSIER_TITLE,
    status: "COMPLETED_WITH_OPEN_QUESTIONS",
    research_provider: "EXTERNAL_RESEARCH",
    research_run_id: "23969ba8-60d2-402d-887d-c94f555d3e3b",
    version: 2,
    started_at: "2026-09-03T05:42:03.000Z",
    completed_at: "2026-09-04T16:00:00.000Z",
    research_cost_usd: 5.4,
    executive_summary: buildExecutiveSummary(),
    key_findings: buildKeyFindings(),
    sections,
    open_questions: buildOpenQuestions(),
    sources,
    methodology_summary:
      "Packet 2.6B-R recovery: preserve substantive archived KGPV research; reconcile superseded findings; normalize citations; no new research.",
    raw_artifact_reference: {
      kind: "archived_kgpv_modules",
      path: "fixtures/hotel-intelligence/dossier/source-archive/modules",
      session_id: "23969ba8-60d2-402d-887d-c94f555d3e3b",
      immutable: true,
    },
    mapping_notes: [
      "Packet 2.6B-R content recovery from seven archived research working modules.",
      "Breathless identity reconciled to same-asset announced conversion; completion status remains delayed/possibly postponed.",
      "Commercial missing-evidence PropCo/shareholder items superseded by Ownership evidence.",
      "Public-company beneficial shareholders distinguished from PropCo deed UBO.",
      `Approx archive module words=${archiveWords}; dossier body words=${bodyWords}.`,
      "Certain findings remain subject to primary legal, contractual or property-level verification where indicated.",
    ],
    claim_handoff: {
      auto_promote: false,
      candidates: deep ? buildClaimCandidatesFromDeep(deep) : [],
    },
  });

  dossier.investigation_status = "COMPLETED_WITH_OPEN_QUESTIONS";
  dossier.substantive_word_count = bodyWords;
  dossier.archive_word_count = archiveWords;
  dossier.schema_version = "hotel-intelligence-dossier-v1.1";
  dossier.hotel_location = {
    label: "Puerto Vallarta, Mexico",
    city: "Puerto Vallarta",
    country: "Mexico",
    state_region: "Jalisco",
  };
  dossier.hotel_airtable_record_id = dossier.hotel_airtable_record_id || "recUNycnMwOVFX0hc";

  // Client-safe customer surfaces (keep claim_handoff / raw refs internal).
  dossier.executive_summary = prepareCustomerProseDeep(dossier.executive_summary);
  dossier.key_findings = prepareCustomerProseDeep(dossier.key_findings);
  dossier.sections = prepareCustomerProseDeep(dossier.sections);
  dossier.open_questions = prepareCustomerProseDeep(dossier.open_questions);
  dossier.methodology_summary = prepareCustomerProseDeep(dossier.methodology_summary);

  return enrichReportForPublishing(refreshDossierCounts(dossier));
}

export function loadAndAdaptKgpvModulesDossier() {
  return adaptWebhoundModulesToDossier();
}
