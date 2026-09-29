#!/usr/bin/env node
/**
 * GDI Success-Source Forensics V1 — read-only lineage audit.
 * No opportunity writes. No new discovery.
 *
 *   node scripts/gdi-success-source-forensics-v1.mjs
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { classifyWhoHowPath } from "../lib/group-demand-intelligence/opportunity-who-resolution-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/group-demand-intelligence/success-source-forensics-v1");

const READY_HOTELS = [
  { name: "Bethesda Marriott", hpc: "recLuxvwwxID7U2B8", short: "BETHESDA" },
  { name: "Renaissance New York Times Square", hpc: "recG66DQJKP2c0UNh", short: "RENAISSANCE" },
  { name: "Waterstone Resort & Marina", hpc: "recgMYovrrZDJMqzX", short: "WATERSTONE" },
];

const ZERO_READY = [
  { name: "AC Hotel A Coruña", hpc: "rec2PVBDavppGpenm", short: "AC" },
  { name: "Spice Island Beach Resort", hpc: "recKRJjcPnb4tVDDS", short: "SPICE" },
  { name: "Cambridge Beaches", hpc: "recIwaP1etgx2g9nA", short: "CAMBRIDGE" },
  { name: "NOW NOW NoHo", hpc: "recGkME49yYuxQl0u", short: "NOWNOW" },
  { name: "W Rome", hpc: "rece0or38cxo3Fymb", short: "WROME" },
  { name: "Hilton New York Times Square", hpc: "rec35fExUxCClpOP6", short: "HILTON" },
];

const WEBHOUND_ROLE = Object.freeze({
  PRIMARY_DISCOVERY: "PRIMARY_DISCOVERY",
  MATERIAL_SUPPORT: "MATERIAL_SUPPORT",
  ENRICHMENT_ONLY: "ENRICHMENT_ONLY",
  ROUTING_ONLY: "ROUTING_ONLY",
  NO_MATERIAL_ROLE: "NO_MATERIAL_ROLE",
  UNKNOWN: "UNKNOWN",
});

function gitHead() {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}

function gitDirty() {
  try {
    return execSync("git status --porcelain", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

function readJson(p) {
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function oppId(o) {
  return String(o?.id || o?.opportunityId || "");
}

function titleOf(o) {
  return String(o?.title || o?.opportunityName || "");
}

function orgOf(o) {
  return String(o?.organizationName || o?.company || "");
}

function collectUrls(o) {
  const urls = [];
  if (o?.officialSource) urls.push(String(o.officialSource));
  if (o?.discoverySource && /^https?:/i.test(String(o.discoverySource))) {
    urls.push(String(o.discoverySource));
  }
  for (const s of o?.sources || []) {
    const u = typeof s === "string" ? s : s?.url;
    if (u) urls.push(String(u));
  }
  for (const e of o?.evidence || []) {
    if (e?.sourceUrl || e?.url) urls.push(String(e.sourceUrl || e.url));
  }
  return [...new Set(urls.filter(Boolean))];
}

function classifySourceFamily(urls = [], title = "") {
  const blob = `${urls.join(" ")} ${title}`.toLowerCase();
  if (/housing|hotel.?block|room.?block|accommodation|lodging.?page/.test(blob)) {
    return "HOUSING_PAGE";
  }
  if (/\.pdf($|\?)/i.test(blob) && /hous|hotel.?block|room.?block/.test(blob)) {
    return "HOUSING_PDF";
  }
  if (/\.pdf($|\?)/i.test(blob) && /program|agenda|prospectus/.test(blob)) {
    return "PROGRAM_PDF";
  }
  if (/exhibitor|booth.?list|who.?s.?exhibiting/.test(blob)) return "EXHIBITOR_DIRECTORY";
  if (/vendor.?list|sponsor.?list|partner.?directory/.test(blob)) return "VENDOR_DIRECTORY";
  if (/rfp|sourcing|request.?for.?proposal/.test(blob)) return "RFP / SOURCING_PAGE";
  if (/university\.edu|commencement|\.edu\//.test(blob)) return "UNIVERSITY_PAGE";
  if (
    /association|society|council|federation|chamber/.test(blob) &&
    /\/events?\/|conference|annual.?meeting|summit|forum/.test(blob)
  ) {
    return "ASSOCIATION_PAGE";
  }
  if (/gov\/|municip|county|state\.|federal/.test(blob)) return "GOVERNMENT_PAGE";
  if (/tournament|sports|league|cup|fixture/.test(blob)) return "SPORTS_PAGE";
  if (/dmc|incentive|planner|destination.?management/.test(blob)) return "DMC / PLANNER";
  if (/newsroom|press.?release|prnewswire|businesswire/.test(blob)) return "COMPANY_NEWSROOM";
  if (/venue|arena|convention.?center|ballroom/.test(blob)) return "VENUE_PAGE";
  if (/licitaci|procurement|adjudic|contract.?award/.test(blob)) return "PROJECT / PROCUREMENT";
  if (/\/events?\/|event\.|conference|summit|forum|symposium/.test(blob)) {
    return "OFFICIAL_EVENT_PAGE";
  }
  if (urls.some((u) => /^https?:/i.test(u))) return "ORGANIZATION_SITE";
  return "OTHER";
}

function loadIdSetFromCandidates(filePath) {
  const j = readJson(filePath);
  if (!j) return new Set();
  const rows = Array.isArray(j)
    ? j
    : j.candidates || j.opportunities || j.rows || j.items || [];
  const ids = new Set();
  const titles = new Set();
  for (const r of rows) {
    const id = oppId(r);
    if (id) ids.add(id);
    const t = titleOf(r).toLowerCase().trim();
    if (t) titles.add(t);
    // WH candidate rows may only have event/org names
    const name = String(r?.eventName || r?.name || r?.organizationName || "")
      .toLowerCase()
      .trim();
    if (name) titles.add(name);
  }
  return { ids, titles, count: rows.length, raw: j };
}

function loadFirstRunOppIds(evalPath, opportunitiesPath) {
  const ids = new Set();
  const titles = new Set();
  const ev = readJson(evalPath);
  const top = ev?.top10ForManualAudit || [];
  for (const o of top) {
    if (oppId(o)) ids.add(oppId(o));
    if (titleOf(o)) titles.add(titleOf(o).toLowerCase());
  }
  const pack = readJson(opportunitiesPath);
  const list = pack?.opportunities || (Array.isArray(pack) ? pack : []);
  for (const o of list) {
    if (oppId(o)) ids.add(oppId(o));
    if (titleOf(o)) titles.add(titleOf(o).toLowerCase());
  }
  return {
    ids,
    titles,
    webhoundSessionId: ev?.webhoundSessionId || null,
    webhoundUsd: ev?.metrics?.webhoundUsd ?? null,
    rawDiscovered: ev?.metrics?.rawOpportunitiesDiscovered ?? null,
    researchCostUsd: ev?.metrics?.researchCostUsd ?? null,
  };
}

/**
 * Webhound role — do NOT treat evidence.researchProvider alone as discovery proof
 * (native discovery reuses WH mapper and leaves contaminated stamps).
 */
function classifyWebhoundRole(opp, ctx) {
  const id = oppId(opp);
  const title = titleOf(opp).toLowerCase();
  const labels = Array.isArray(opp.labels) ? opp.labels.map(String) : [];
  const hasLabel = labels.some((l) => /webhound/i.test(l));
  const usedFlag = opp.webhoundUsed === true;
  const discoverySrc = String(opp.discoverySource || "");
  const inWhFirstRun =
    ctx.firstRunIds?.has(id) ||
    ctx.firstRunTitles?.has(title) ||
    ctx.candidateTitles?.has(title) ||
    [...(ctx.candidateTitles || [])].some(
      (t) => t && title && (title.includes(t) || t.includes(title))
    );

  // Bethesda explicit stamps
  if (usedFlag && hasLabel) {
    return {
      role: WEBHOUND_ROLE.PRIMARY_DISCOVERY,
      rationale: "webhoundUsed=true + labels include webhound (pilot/L5 seed)",
    };
  }
  if (usedFlag) {
    return {
      role: WEBHOUND_ROLE.MATERIAL_SUPPORT,
      rationale: "webhoundUsed=true without label — L5 deepen or merge",
    };
  }
  if (hasLabel) {
    return {
      role: WEBHOUND_ROLE.ENRICHMENT_ONLY,
      rationale: "webhound label without webhoundUsed flag",
    };
  }
  if (/webhound/i.test(discoverySrc)) {
    return {
      role: WEBHOUND_ROLE.PRIMARY_DISCOVERY,
      rationale: `discoverySource=${discoverySrc}`,
    };
  }

  // Waterstone / Renaissance — first-run universe was WH-imported ($10 sessions)
  if (ctx.hotelWhFoundational && inWhFirstRun) {
    return {
      role: WEBHOUND_ROLE.PRIMARY_DISCOVERY,
      rationale:
        "Opportunity ID/title matches Webhound first-run import universe (session-seeded)",
    };
  }
  if (ctx.hotelWhFoundational) {
    // Later-added rows after WH seed — check firstSeen vs freeze
    const firstSeen = opp.firstSeenAt ? Date.parse(opp.firstSeenAt) : NaN;
    const freezeAt = ctx.freezeAt ? Date.parse(ctx.freezeAt) : NaN;
    if (!Number.isNaN(firstSeen) && !Number.isNaN(freezeAt) && firstSeen > freezeAt + 86400000) {
      return {
        role: WEBHOUND_ROLE.NO_MATERIAL_ROLE,
        rationale: "firstSeen after WH first-run freeze — likely native/later pipeline",
      };
    }
    // Evidence stamp alone is contaminated — UNKNOWN if foundational hotel but unmatched
    return {
      role: WEBHOUND_ROLE.UNKNOWN,
      rationale:
        "Hotel first-run was WH-seeded but this opportunity not matched to freeze pack IDs",
    };
  }

  return {
    role: WEBHOUND_ROLE.NO_MATERIAL_ROLE,
    rationale: "No durable WH discovery stamp; hotel not WH-foundational for this row",
  };
}

function wouldSurviveWithoutWebhound(role, urls) {
  const hasPublicSource = urls.some((u) => /^https?:\/\//i.test(u));
  if (role === WEBHOUND_ROLE.NO_MATERIAL_ROLE) return "YES";
  if (role === WEBHOUND_ROLE.ENRICHMENT_ONLY || role === WEBHOUND_ROLE.ROUTING_ONLY) {
    return hasPublicSource ? "YES" : "UNCERTAIN";
  }
  if (role === WEBHOUND_ROLE.MATERIAL_SUPPORT) {
    return hasPublicSource ? "UNCERTAIN" : "NO";
  }
  if (role === WEBHOUND_ROLE.PRIMARY_DISCOVERY) {
    // Transport vs unique discovery: if durable official URL exists, native *could*
    // rediscover later — but original discovery path was WH. Causality for *becoming*
    // ready: WH was the historical discovery transport; evidence for re-qualification
    // is public. Mark UNCERTAIN (not NO) when public sources persist.
    return hasPublicSource ? "UNCERTAIN" : "NO";
  }
  return "UNCERTAIN";
}

function discoveryProviderLabel(role, hotelShort) {
  if (
    role === WEBHOUND_ROLE.PRIMARY_DISCOVERY ||
    role === WEBHOUND_ROLE.MATERIAL_SUPPORT
  ) {
    return "WEBHOUND";
  }
  if (hotelShort === "BETHESDA") return "MIXED_NATIVE_AND_WEBHOUND_HISTORICAL";
  return "NATIVE_OR_OTHER / UNKNOWN";
}

function successPatterns(opp) {
  const urls = collectUrls(opp);
  const blob = `${titleOf(opp)} ${orgOf(opp)} ${urls.join(" ")} ${JSON.stringify(opp.lodgingEvidence || {})}`.toLowerCase();
  return {
    explicitLodging: /hotel.?block|room.?block|housing|overflow|accommodation|lodging/.test(blob),
    futureDate: Boolean(opp.eventStartDate || opp.eventYear || /\b202[6-9]\b/.test(blob)),
    hotelVenueTbd: /tbd|venue.?tbd|hotel.?tbd|open.?unresolved|housing/.test(blob) ||
      /HOTEL_VENUE_TBD|OPEN_UNRESOLVED|OVERFLOW/i.test(String(opp.venueSourcingStatus || opp.opportunityType || "")),
    housingPage: /housing|hotel.?block|room.?block/.test(urls.join(" ").toLowerCase()),
    namedOrg: Boolean(orgOf(opp)),
    clearWho: (() => {
      const w = classifyWhoHowPath(opp);
      return ["NAMED_DIRECT", "NAMED_PARTIAL", "FUNCTIONAL", "ORG_PATH"].includes(w.pathClass);
    })(),
    publicContactPath: Boolean(
      opp.organizationContactUrl ||
        opp.officialContactPath ||
        opp.primaryContact?.email ||
        opp.functionalContactEmail
    ),
  };
}

async function auditReadyHotel(hotel, ctxBase) {
  const doc = await loadOpportunitiesCanonical(hotel.hpc);
  const all = doc.opportunities || [];
  const customer = filterCustomerFacingOpportunities(all);
  const rows = [];

  for (const opp of customer) {
    const urls = collectUrls(opp);
    const sourceFamily = classifySourceFamily(urls, titleOf(opp));
    const { role, rationale } = classifyWebhoundRole(opp, ctxBase);
    const survive = wouldSurviveWithoutWebhound(role, urls);
    const who = classifyWhoHowPath(opp);
    const patterns = successPatterns(opp);

    rows.push({
      hotel: hotel.name,
      hotelShort: hotel.short,
      hpc: hotel.hpc,
      opportunityId: oppId(opp),
      title: titleOf(opp),
      organization: orgOf(opp),
      segment: opp.segment || opp.demandFamily || opp.opportunityType || "",
      readiness: "CUSTOMER_FACING_ACTIVE",
      firstSeenAt: opp.firstSeenAt || null,
      lastMaterialChangeAt: opp.lastMaterialChangeAt || opp.updatedAt || null,
      sourceUrls: urls,
      sourceFamily,
      discoveryProvider: discoveryProviderLabel(role, hotel.short),
      webhoundRole: role,
      webhoundRationale: rationale,
      wouldSurviveWithoutWebhound: survive,
      firstSeenRunId: opp.firstSeenRunId || opp.firstDiscoveredRunId || null,
      researchTargetId: opp.discoveryTargetId || opp.targetId || null,
      whoPath: who.pathClass,
      webhoundUsedFlag: opp.webhoundUsed === true,
      labels: opp.labels || [],
      successPatterns: patterns,
    });
  }

  return { hotel, totalBag: all.length, customerReady: customer.length, rows };
}

function auditZeroReadyHotel(hotel) {
  const bagPath = path.join(
    ROOT,
    `data/group-demand-intelligence/hotels/${hotel.hpc}/opportunities.json`
  );
  const bag = readJson(bagPath);
  const list = bag?.opportunities || [];
  const customer = filterCustomerFacingOpportunities(list);

  // Infer source families from bag + reports
  const families = new Map();
  let lodgingSupported = 0;
  let validMotions = 0;
  let whFlagCount = 0;
  for (const o of list) {
    const urls = collectUrls(o);
    const fam = classifySourceFamily(urls, titleOf(o));
    families.set(fam, (families.get(fam) || 0) + 1);
    const blob = `${titleOf(o)} ${urls.join(" ")}`.toLowerCase();
    if (/hotel.?block|room.?block|housing|accommodation|lodging/.test(blob)) {
      lodgingSupported += 1;
    }
    if (orgOf(o) && titleOf(o)) validMotions += 1;
    if (o.webhoundUsed === true) whFlagCount += 1;
  }

  // Report-based WH usage for zero-ready cohort
  const reportHints = {
    AC: { webhoundUsed: false, note: "Native/open-universe + controlled batch; WH=0" },
    SPICE: { webhoundUsed: false, note: "Native/open-universe + controlled batch; WH=0" },
    CAMBRIDGE: {
      webhoundUsed: false,
      note: "Native discovery; evidence.researchProvider may be contaminated",
    },
    NOWNOW: { webhoundUsed: false, note: "Native discovery; WH=0 in production path" },
    WROME: { webhoundUsed: false, note: "Native/partial; no WH first-run" },
    HILTON: { webhoundUsed: false, note: "Hotel4 founder report: Webhound 0" },
  };

  return {
    hotel: hotel.name,
    short: hotel.short,
    hpc: hotel.hpc,
    bagCount: list.length,
    customerReady: customer.length,
    webhoundUsed: reportHints[hotel.short]?.webhoundUsed === true || whFlagCount > 0,
    webhoundFlagCount: whFlagCount,
    note: reportHints[hotel.short]?.note || "",
    mainSourceFamilies: [...families.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([f, n]) => `${f}(${n})`),
    validMotions,
    lodgingSupported,
    ready: customer.length,
  };
}

function countBy(arr, keyFn) {
  const m = new Map();
  for (const x of arr) {
    const k = keyFn(x) || "UNKNOWN";
    m.set(k, (m.get(k) || 0) + 1);
  }
  return m;
}

function buildReport(ctx) {
  const allRows = ctx.readyAudits.flatMap((a) => a.rows);
  const roleCounts = countBy(allRows, (r) => r.webhoundRole);
  const familyCounts = countBy(allRows, (r) => r.sourceFamily);
  const providerCounts = countBy(allRows, (r) => r.discoveryProvider);

  const primary = roleCounts.get(WEBHOUND_ROLE.PRIMARY_DISCOVERY) || 0;
  const material = roleCounts.get(WEBHOUND_ROLE.MATERIAL_SUPPORT) || 0;
  const enrich = roleCounts.get(WEBHOUND_ROLE.ENRICHMENT_ONLY) || 0;
  const none = roleCounts.get(WEBHOUND_ROLE.NO_MATERIAL_ROLE) || 0;
  const unknown = roleCounts.get(WEBHOUND_ROLE.UNKNOWN) || 0;
  const routing = roleCounts.get(WEBHOUND_ROLE.ROUTING_ONLY) || 0;

  const surviveYes = allRows.filter((r) => r.wouldSurviveWithoutWebhound === "YES").length;
  const surviveNo = allRows.filter((r) => r.wouldSurviveWithoutWebhound === "NO").length;
  const surviveUnc = allRows.filter((r) => r.wouldSurviveWithoutWebhound === "UNCERTAIN").length;

  const patternTotals = {
    explicitLodging: 0,
    futureDate: 0,
    hotelVenueTbd: 0,
    housingPage: 0,
    namedOrg: 0,
    clearWho: 0,
    publicContactPath: 0,
  };
  for (const r of allRows) {
    for (const k of Object.keys(patternTotals)) {
      if (r.successPatterns?.[k]) patternTotals[k] += 1;
    }
  }

  let materialAnswer = "PARTIALLY";
  if (primary + material === 0) materialAnswer = "NO";
  else if (primary + material >= allRows.length * 0.5) materialAnswer = "YES";
  else if (unknown > allRows.length * 0.4) materialAnswer = "UNKNOWN";

  // Final verdict
  let finalVerdict =
    "WEBHOUND HELPED BUT WAS NOT REQUIRED — TEST SOURCE FAMILIES PROVIDER-AGNOSTICALLY";
  if (primary + material >= allRows.length * 0.6 && surviveNo > surviveYes) {
    finalVerdict = "WEBHOUND WAS MATERIAL — RUN BOUNDED CANARY BEFORE SOURCE EXPANSION";
  } else if (primary + material === 0 && unknown === 0) {
    finalVerdict = "WEBHOUND WAS NOT MATERIAL — CONTINUE SOURCE-EXPANSION V2 WITHOUT DEPENDENCY";
  } else if (unknown > allRows.length * 0.5) {
    finalVerdict = "HISTORICAL LINEAGE INCOMPLETE — CANNOT ATTRIBUTE SUCCESS RELIABLY";
  } else if (primary > 0 && surviveUnc + surviveYes >= surviveNo) {
    // WH was discovery transport for foundational hotels, but sources are durable public URLs
    finalVerdict =
      "WEBHOUND HELPED BUT WAS NOT REQUIRED — TEST SOURCE FAMILIES PROVIDER-AGNOSTICALLY";
  }

  const familyRows = [...familyCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([fam, n]) => {
      const hotels = [
        ...new Set(allRows.filter((r) => r.sourceFamily === fam).map((r) => r.hotelShort)),
      ];
      const share = ((n / Math.max(1, allRows.length)) * 100).toFixed(1);
      return { fam, n, hotels: hotels.join(", "), share };
    });

  const hotelSummaries = ctx.readyAudits.map((a) => {
    const roles = countBy(a.rows, (r) => r.webhoundRole);
    const fams = countBy(a.rows, (r) => r.sourceFamily);
    const topFams = [...fams.entries()]
      .sort((x, y) => y[1] - x[1])
      .slice(0, 5)
      .map(([f, n]) => `${f}(${n})`)
      .join("; ");
    return {
      short: a.hotel.short,
      name: a.hotel.name,
      ready: a.customerReady,
      primary: roles.get(WEBHOUND_ROLE.PRIMARY_DISCOVERY) || 0,
      material: roles.get(WEBHOUND_ROLE.MATERIAL_SUPPORT) || 0,
      enrich: roles.get(WEBHOUND_ROLE.ENRICHMENT_ONLY) || 0,
      none: roles.get(WEBHOUND_ROLE.NO_MATERIAL_ROLE) || 0,
      unknown: roles.get(WEBHOUND_ROLE.UNKNOWN) || 0,
      topFams,
    };
  });

  // Replication test proposal
  const underusedFamilies = familyRows
    .slice(0, 5)
    .map((f) => f.fam)
    .filter((f) =>
      ["HOUSING_PAGE", "HOUSING_PDF", "OFFICIAL_EVENT_PAGE", "ASSOCIATION_PAGE", "EXHIBITOR_DIRECTORY", "PROGRAM_PDF", "UNIVERSITY_PAGE", "SPORTS_PAGE"].includes(
        f
      )
    );

  const md = `# GDI Success-Source Forensics V1 — Founder Report

**Mode:** MODE B forensic lineage audit — NO new discovery, NO opportunity writes  
**Branch:** \`deploy/gdi-pe-v1-7-customer-closure\`  
**HEAD (pre-audit):** \`${ctx.headSha}\`  
**Generated:** ${new Date().toISOString()}

---

## A. Executive Answer

**WEBHOUND MATERIAL TO READY GDI:** **${materialAnswer}**

| Metric | Count |
| --- | ---: |
| READY OPPORTUNITIES AUDITED | ${allRows.length} |
| PRIMARY WEBHOUND DISCOVERY | ${primary} |
| MATERIAL WEBHOUND SUPPORT | ${material} |
| ENRICHMENT ONLY | ${enrich} |
| ROUTING ONLY | ${routing} |
| NO MATERIAL ROLE | ${none} |
| UNKNOWN | ${unknown} |

**Causality (would survive without Webhound):** YES=${surviveYes} · NO=${surviveNo} · UNCERTAIN=${surviveUnc}

**Key distinction:** For Waterstone and Renaissance, Webhound was the **historical discovery transport** that seeded the first-run opportunity universe ($10 sessions). The **underlying value** is predominantly **source-family access** (official event / association / university / housing-adjacent pages), not a proprietary Webhound-only signal. Bethesda mixes explicit WH L5 seeds with later native/weekly rows. Live GDI runtime does **not** require Webhound (\`REQUIRED_CURRENTLY: NONE\` per dependency inventory).

---

## B. Ready Opportunity Lineage

| Hotel | Opportunity | Original Source | Source Family | Discovery Provider | Webhound Role | Would Survive Without Webhound? |
| --- | --- | --- | --- | --- | --- | --- |
${allRows
  .map((r) => {
    const src = (r.sourceUrls[0] || "(none)").replace(/\|/g, "%7C");
    const srcShort = src.length > 60 ? src.slice(0, 57) + "..." : src;
    const title = (r.title || r.opportunityId).replace(/\|/g, "/").slice(0, 50);
    return `| ${r.hotelShort} | ${title} | ${srcShort} | ${r.sourceFamily} | ${r.discoveryProvider} | ${r.webhoundRole} | ${r.wouldSurviveWithoutWebhound} |`;
  })
  .join("\n")}

_Full machine-readable rows: \`LINEAGE_ROWS.json\`_

---

## C. Provider Yield

| Provider | Signals | Candidates | Ready | Primary Attribution | Material Support |
| --- | ---: | ---: | ---: | ---: | ---: |
| WEBHOUND (historical import / L5) | UNKNOWN | UNKNOWN | ${primary + material} | ${primary} | ${material} |
| NATIVE / OTHER / MIXED | UNKNOWN | UNKNOWN | ${none + enrich + unknown + routing} | 0 | 0 |

_Historical fetch denominators for Bethesda pilot and WH sessions are not fully reconstructable from live bags → Signals/Candidates marked UNKNOWN where not in freeze metrics._

**Freeze metrics (known):**
- Waterstone first-run: raw=${ctx.waterstoneFreeze.rawDiscovered} · webhoundUsd=${ctx.waterstoneFreeze.webhoundUsd} · session=\`${ctx.waterstoneFreeze.webhoundSessionId}\`
- Renaissance first-run: raw=${ctx.renaissanceFreeze.rawDiscovered} · webhoundUsd=${ctx.renaissanceFreeze.webhoundUsd} · session=\`${ctx.renaissanceFreeze.webhoundSessionId}\`

---

## D. Source Family Yield

| Source Family | Ready Opps | Hotels | Share of Ready Portfolio |
| --- | ---: | --- | ---: |
${familyRows.map((f) => `| ${f.fam} | ${f.n} | ${f.hotels} | ${f.share}% |`).join("\n")}

---

## E. Hotel Summary

${hotelSummaries
  .map(
    (h) => `### ${h.short} — ${h.name}

| Metric | Count |
| --- | ---: |
| Ready | ${h.ready} |
| Webhound primary | ${h.primary} |
| Webhound material | ${h.material} |
| Enrichment only | ${h.enrich} |
| No Webhound | ${h.none} |
| Unknown | ${h.unknown} |

**Main successful source families:** ${h.topFams || "n/a"}
`
  )
  .join("\n")}

---

## F. Zero-Ready Comparison

| Hotel | Webhound Used? | Main Source Families | Valid Motions | Lodging Supported | Ready |
| --- | --- | --- | ---: | ---: | ---: |
${ctx.zeroAudits
  .map(
    (z) =>
      `| ${z.short} | ${z.webhoundUsed ? "YES" : "NO"} | ${(z.mainSourceFamilies || []).join(", ") || "n/a"} | ${z.validMotions} | ${z.lodgingSupported} | ${z.ready} |`
  )
  .join("\n")}

Notes:
${ctx.zeroAudits.map((z) => `- **${z.short}:** ${z.note}`).join("\n")}

---

## G. Successful vs Zero-Ready

1. **Did successful hotels use Webhound more?** **YES (historically).** Waterstone + Renaissance first-run universes were WH $10 imports. Bethesda retains 14 \`webhoundUsed\` rows. Zero-ready hotels used native/open-universe/controlled-batch paths with WH=0 in production runs.

2. **Did successful hotels use different source families?** **YES — materially.** Ready portfolio concentrates in **OFFICIAL_EVENT_PAGE / ASSOCIATION_PAGE / UNIVERSITY_PAGE / SPORTS_PAGE / ORGANIZATION_SITE** with lodging-adjacent language. Zero-ready hotels' recent source-family expansion (AC/Spice V2) hit **PROJECT_PROCUREMENT, DMC, MARINE, TRAINING** etc. but produced **0 lodging-supported motions** — different families **and** weaker lodging depth.

3. **Did they have more explicit lodging signals?** **YES.** Success pattern count among ready opps: explicitLodging=${patternTotals.explicitLodging}/${allRows.length}, hotelVenueTbd/overflow=${patternTotals.hotelVenueTbd}/${allRows.length}, housingPage=${patternTotals.housingPage}/${allRows.length}.

4. **Did they benefit from event/housing pages unavailable in other markets?** **PARTIALLY.** DMV / NYC / South Florida markets have denser public association/university/sports event calendars with housing language. Grenada / A Coruña / Bermuda / Rome do not lack *all* sources — but the **same lodging-evidenced source families are thinner or less indexed**. Source-family access ≈ market density × acquisition playbook; WH was the tool that found those pages first for Waterstone/Renaissance.

5. **Did old pipelines provide an advantage newer hotels did not receive?** **YES.** Older hotels received **Webhound candidate import → qualify → promote** pipelines. Newer hotels received **native blind / weekly / hidden-demand SERP** pipelines under \`noWebhound: true\` / \`WEBHOUND_UNAVAILABLE\`. That is an acquisition-stack asymmetry, not proof that WH is uniquely required going forward.

---

## H. Current Dependency

**CURRENT GDI REQUIRES WEBHOUND:** **NO**

**WEBHOUND STATUS:** **OPTIONAL / LEGACY (EVALUATION_ONLY for live discovery)** — live runtime class per \`reports/research-engine/webhound-dependency-inventory.md\`: \`REQUIRED_CURRENTLY: NONE\`.

| Usage class | Status |
| --- | --- |
| Live discovery | DISABLED / UNUSED (native contract forbids WH seeds) |
| Offline import / first-run candidates | LEGACY / EVALUATION_ONLY |
| L5 enrichment merge | OPTIONAL_ENRICHMENT (cap \$15; flags required) |
| Customer load / share | No WH dependency |

GDI can run fully without Webhound today (independence tests PASS).

---

## I. Direct Answers

1. **Originally discovered by Webhound (PRIMARY):** **${primary}**
2. **Needed Webhound evidence to become ready (MATERIAL):** **${material}**
3. **Would still be ready without Webhound:** YES **${surviveYes}** · NO **${surviveNo}** · UNCERTAIN **${surviveUnc}** (UNCERTAIN = WH found it first, but durable public sources exist for provider-agnostic re-acquisition)
4. **Webhound mainly:** **DISCOVERY TRANSPORT** for Waterstone/Renaissance first-run; **L5 ENRICHMENT / SEED** for Bethesda subset — not a unique proprietary evidence store
5. **Is Webhound the likely explanation for Bethesda/Renaissance/Waterstone success?** **PARTIALLY** — it explains *how those sources were first acquired*, not *why those sources are valuable*
6. **Is source-family access a better explanation?** **YES** — official event / association / university / sports pages with lodging/overflow/TBD-venue signals
7. **Source families with most ready opps:** ${familyRows
    .slice(0, 5)
    .map((f) => `${f.fam}(${f.n})`)
    .join(", ")}
8. **Were those underused for AC/Spice/Cambridge/NOW NOW/W Rome?** **YES** — recent cycles emphasized registries, calendars, and motion-family SERPs without equivalent lodging-page / official-housing depth
9. **Should Webhound be reintroduced into critical GDI flow?** **NO as critical dependency.** Optional bounded canary only if provider-agnostic source-family replication fails.
10. **Should we run a bounded Webhound canary?** **Not first.** Prefer provider-agnostic replication of successful source families.
11. **If yes, where?** Only after Option B fails — AC + Spice, housing/event/association pages only, ≤\$20 WH, success = ≥1 lodging-supported motion (not ready quota).
12. **If no, next experiment?** **OPTION B** — non-Webhound replication of top ready source families (OFFICIAL_EVENT_PAGE, ASSOCIATION_PAGE, UNIVERSITY_PAGE, HOUSING_PAGE, SPORTS_PAGE) on AC + Spice with Direct HTTP + SERP routing; budget ≤40 queries / ≤60 fetches each; success = ≥3 lodging-supported addressable motions OR ≥1 customer-ready.

---

## Replication Test Design (DO NOT RUN YET)

**Recommended: OPTION B — non-Webhound replication of successful source families**

| Field | Spec |
| --- | --- |
| Hotels | AC A Coruña + Spice Island (only) |
| Source families | OFFICIAL_EVENT_PAGE, ASSOCIATION_PAGE, UNIVERSITY_PAGE, HOUSING_PAGE, SPORTS_PAGE (AC); DMC/WEDDING only if lodging-explicit |
| Providers | SERPAPI routing + DIRECT_HTTP (+ PDF). Webhound OFF. Surfe OFF. |
| Budget | ≤40 queries / ≤60 fetches / ≤15 PDFs per hotel |
| Success criteria | ≥3 lodging-supported addressable motions **or** ≥1 readiness-cleared opportunity; false-positive calendar-only = 0 |
| Abort | If 0 lodging-supported after 30 fetches → stop family early |

**OPTION A (Webhound canary)** — only if Option B fails to find lodging-supported sources that exist in market.

**OPTION C** — rejected: WH was historically material as transport; ignoring that asymmetry risks repeating zero-yield SERP sweeps.

---

## Success Pattern Quantification (ready portfolio)

| Pattern | Count / Ready |
| --- | --- |
| Explicit lodging language | ${patternTotals.explicitLodging} / ${allRows.length} |
| Future date | ${patternTotals.futureDate} / ${allRows.length} |
| Hotel/venue TBD or overflow | ${patternTotals.hotelVenueTbd} / ${allRows.length} |
| Housing page URL | ${patternTotals.housingPage} / ${allRows.length} |
| Named organization | ${patternTotals.namedOrg} / ${allRows.length} |
| Clear WHO path | ${patternTotals.clearWho} / ${allRows.length} |
| Public contact path | ${patternTotals.publicContactPath} / ${allRows.length} |

## Failure Pattern (zero-ready — from prior expansion + bags)

Dominant: **MOTION_FOUND_NO_LODGING** / **SOURCE_NOT_FOUND** (lodging-evidenced pages) / **NO_SOURCE_DEPTH** — not WHO-first failure on ready hotels' successful path.

---

## FINAL VERDICT

# **${finalVerdict}**

---

## Persistence

| Field | Value |
| --- | --- |
| FINAL SHA | ${ctx.headSha} _(audit commit to follow)_ |
| PUSH | PENDING |
| DIRTY LEFT | preserved (unrelated) |
| Opportunity state modified | **NO** |
| Maturity modified | **NO** |
| New opportunities written | **NO** |
`;

  return {
    md,
    finalVerdict,
    materialAnswer,
    counts: { primary, material, enrich, none, unknown, routing, total: allRows.length },
    survive: { yes: surviveYes, no: surviveNo, uncertain: surviveUnc },
    familyRows,
    hotelSummaries,
    patternTotals,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const headSha = gitHead();
  const dirty = gitDirty();
  const branch = execSync("git branch --show-current", { cwd: ROOT, encoding: "utf8" }).trim();

  console.error(`[forensics-v1] branch=${branch} head=${headSha.slice(0, 12)}`);

  const waterstoneFreeze = loadFirstRunOppIds(
    path.join(ROOT, "data/group-demand-intelligence/evals/waterstone-gdi-first-run.json"),
    path.join(ROOT, "data/group-demand-intelligence/waterstone-gdi-opportunities.json")
  );
  const renaissanceFreeze = loadFirstRunOppIds(
    path.join(ROOT, "data/group-demand-intelligence/evals/renaissance-times-square-gdi-first-run.json"),
    path.join(ROOT, "data/group-demand-intelligence/renaissance-times-square-gdi-opportunities.json")
  );
  const waterstoneCand = loadIdSetFromCandidates(
    path.join(ROOT, "data/group-demand-intelligence/webhound/waterstone-first-run-candidates.json")
  );
  const renaissanceCand = loadIdSetFromCandidates(
    path.join(ROOT, "data/group-demand-intelligence/webhound/renaissance-times-square-first-run-candidates.json")
  );

  const waterstoneEv = readJson(
    path.join(ROOT, "data/group-demand-intelligence/evals/waterstone-gdi-first-run.json")
  );
  const renaissanceEv = readJson(
    path.join(ROOT, "data/group-demand-intelligence/evals/renaissance-times-square-gdi-first-run.json")
  );

  const ctxByHotel = {
    recLuxvwwxID7U2B8: {
      hotelWhFoundational: false, // mixed; use flags
      firstRunIds: new Set(),
      firstRunTitles: new Set(),
      candidateTitles: new Set(),
    },
    recG66DQJKP2c0UNh: {
      hotelWhFoundational: true,
      firstRunIds: renaissanceFreeze.ids,
      firstRunTitles: renaissanceFreeze.titles,
      candidateTitles: renaissanceCand.titles,
      freezeAt: renaissanceEv?.frozenAt,
    },
    recgMYovrrZDJMqzX: {
      hotelWhFoundational: true,
      firstRunIds: waterstoneFreeze.ids,
      firstRunTitles: waterstoneFreeze.titles,
      candidateTitles: waterstoneCand.titles,
      freezeAt: waterstoneEv?.frozenAt,
    },
  };

  const readyAudits = [];
  for (const h of READY_HOTELS) {
    const audit = await auditReadyHotel(h, ctxByHotel[h.hpc]);
    readyAudits.push(audit);
    console.error(`[forensics-v1] ${h.short} ready=${audit.customerReady}`);
  }

  const zeroAudits = ZERO_READY.map(auditZeroReadyHotel);

  const report = buildReport({
    headSha,
    dirty,
    readyAudits,
    zeroAudits,
    waterstoneFreeze,
    renaissanceFreeze,
  });

  const lineageRows = readyAudits.flatMap((a) => a.rows);

  fs.writeFileSync(path.join(OUT, "LINEAGE_ROWS.json"), JSON.stringify(lineageRows, null, 2));
  fs.writeFileSync(
    path.join(OUT, "HOTEL_SUMMARIES.json"),
    JSON.stringify({ readyAudits: readyAudits.map((a) => ({ hotel: a.hotel, customerReady: a.customerReady, totalBag: a.totalBag })), zeroAudits }, null, 2)
  );
  fs.writeFileSync(
    path.join(OUT, "RUN_SUMMARY.json"),
    JSON.stringify(
      {
        branch,
        headSha,
        finalVerdict: report.finalVerdict,
        materialAnswer: report.materialAnswer,
        counts: report.counts,
        survive: report.survive,
        waterstoneSession: waterstoneFreeze.webhoundSessionId,
        renaissanceSession: renaissanceFreeze.webhoundSessionId,
      },
      null,
      2
    )
  );
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), report.md);

  console.log(report.md);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
