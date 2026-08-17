/**
 * Wave 14 — Founder visual semantic failure extraction.
 *
 * Audits Recent Momentum, Portfolio Mix, and Openings/Property cards
 * for the eight active Wave 14 brands.
 *
 * Read-only. Does not patch Airtable.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE14_PARTIAL_PROMOTION_SLUGS,
  WAVE14_HELD_PROMOTION_SLUG,
} from "./brand-explorer-wave14-factory-plan.js";

export const WAVE14_FOUNDER_VISUAL_SEMANTIC_FAILURES_VERSION =
  "wave14-founder-visual-semantic-failures-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

const INTERNAL_LANGUAGE_RES = [
  /\bvisual diligence\b/i,
  /\bsteward-matched\b/i,
  /\bunderwriting lane\b/i,
  /\bbrand-lane evidence\b/i,
  /\bsource-supported\b/i,
  /\bsource pack\b/i,
  /\bfactory\b/i,
  /\bStage\s*\d/i,
  /\bgovernance\b/i,
  /\bUse this labeled example\b/i,
  /\bconfirm the asset under review\b/i,
  /\bconfirm that the asset under review\b/i,
  /\bEnsure the asset under review\b/i,
  /\bDirectory card\b/i,
  /\bQA\b/,
  /\bprocess\b/i,
  /https?:\/\/[^\s]+/,
];

const MOMENTUM_FAKE_TITLES_RES = [
  /Development Page Frames/i,
  /Development Positioning Remains/i,
  /Brand Site Confirms/i,
  /Brand Presence Confirms/i,
  /Guest Brand Site Confirms/i,
  /Brand Page Confirms/i,
  /on Marriott Longer Stays family positioning/i,
];

const GENERIC_PROPERTY_TITLE_RE =
  /^(Marriott Hotels|Sheraton|Westin|Residence Inn|SpringHill Suites|TownePlace Suites|Aloft Hotels|StudioRes|Residence Inn by Marriott|SpringHill Suites by Marriott|TownePlace Suites by Marriott)\s*[-—]\s*(International Reference|CALA|Property Example)$/i;

const ARCHETYPE_PROPERTY_RE =
  /^(US |North America |Suburban |Airport |Upper-midscale |US longer|employment|kitchenette|competitive sets|secondary)/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function isHidden(row) {
  return (
    row?.active === false ||
    /do not display|internal only/i.test(nz(row?.externalDisplayStatus))
  );
}

function classifyMomentumSource(title, body, url) {
  const text = `${title}\n${body}\n${url}`;
  if (/hotel-development\.marriott\.com/i.test(url)) return "development_page";
  if (/marriott\.com\/brands\//i.test(url) || /brand site|brand page/i.test(title))
    return "brand_page";
  if (/marriott\.com\/hotels\//i.test(url) || /\.marriott\.com\/en-us\/hotels\//i.test(url))
    return "directory_page";
  if (/marriott\.com\/longer-stays/i.test(url)) return "brand_page";
  if (/press release|announcement|opening|signed|conversion|renovation/i.test(text))
    return "press_announcement";
  if (/skift\.com|hospitality net|hotel management|htrends/i.test(url))
    return "trade_press";
  return "other";
}

function classifyMomentumFailures(title, body, sourceType) {
  const failures = [];
  if (MOMENTUM_FAKE_TITLES_RES.some((re) => re.test(title))) {
    failures.push("title_is_source_note_not_momentum");
  }
  if (sourceType === "development_page") failures.push("development_page_as_momentum");
  if (sourceType === "brand_page") failures.push("brand_page_as_momentum");
  if (sourceType === "directory_page" && !/property proof/i.test(body)) {
    failures.push("directory_as_momentum_without_proof_label");
  }
  for (const re of INTERNAL_LANGUAGE_RES) {
    if (re.test(body)) {
      failures.push(`internal_language:${re.source.slice(0, 30)}`);
      break;
    }
  }
  if (!/\b(open|sign|develop|convert|renovat|pipeline|announc|project|prototype|anchor|proof)/i.test(title)) {
    failures.push("title_lacks_momentum_semantics");
  }
  return [...new Set(failures)];
}

function classifyPortfolioMixFailures(body) {
  const failures = [];
  if (!body) {
    failures.push("missing");
    return failures;
  }
  const hasPercent = /%/.test(body);
  const isBullets = /^-\s/m.test(body);
  const isLongProse = body.length > 200 && !hasPercent && !isBullets;
  if (isLongProse) failures.push("long_prose_paragraph");
  if (!hasPercent) failures.push("no_percentages");
  if (/peer comparison set/i.test(body)) failures.push("internal_peer_comparison_note");
  if (/steward-confirmed/i.test(body)) failures.push("internal_steward_language");
  if (/evaluate on operating model/i.test(body)) failures.push("internal_process_language");
  if (/Property examples are labeled/i.test(body)) failures.push("internal_labeling_note");
  if (/CALA-supported operating examples/i.test(body)) failures.push("internal_cala_note");
  if (/International Reference examples used/i.test(body)) failures.push("internal_ir_note");
  return failures;
}

function classifyOpeningsFailures(title, body) {
  const failures = [];
  if (GENERIC_PROPERTY_TITLE_RE.test(title)) failures.push("generic_brand_reference_title");
  if (ARCHETYPE_PROPERTY_RE.test(title)) failures.push("archetype_not_actual_hotel");
  if (/Use this labeled example/i.test(body)) failures.push("internal_diligence_language");
  if (/visual diligence/i.test(body)) failures.push("visual_diligence_language");
  if (/steward-matched/i.test(body)) failures.push("steward_language");
  if (/confirm the asset under review|Ensure the asset under review/i.test(body))
    failures.push("confirm_asset_language");
  if (/Market archetype pending/i.test(body)) failures.push("pending_archetype_note");
  if (/bind property/i.test(body)) failures.push("internal_bind_note");
  for (const re of INTERNAL_LANGUAGE_RES) {
    if (re.test(body)) {
      failures.push(`internal_language:${re.source.slice(0, 30)}`);
      break;
    }
  }
  return [...new Set(failures)];
}

export async function extractWave14FounderVisualSemanticFailures({
  brands = WAVE14_PARTIAL_PROMOTION_SLUGS,
} = {}) {
  const brandRows = [];

  for (const slug of brands) {
    if (slug === WAVE14_HELD_PROMOTION_SLUG) continue;
    const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    if (!identity?.recordId) {
      brandRows.push({ brandSlug: slug, blocked: true, blockers: ["unknown_identity"] });
      continue;
    }

    const fetch = await listPresentationRowsLight(identity.recordId, identity.name);
    const rows = (fetch.rows || []).filter((r) => !isHidden(r));

    // Momentum
    const momRows = rows.filter((r) => r.slotKey === "footprint.momentum");
    const momentumAudit = momRows.map((m) => {
      const sourceType = classifyMomentumSource(m.title || "", m.body || "", "");
      const failures = classifyMomentumFailures(m.title || "", m.body || "", sourceType);
      return {
        title: nz(m.title),
        body: nz(m.body).slice(0, 200),
        recordId: m.recordId,
        sourceType,
        failures,
      };
    });

    // Portfolio Mix
    const mixRows = rows.filter((r) => r.slotKey === "footprint.portfolio_mix");
    const portfolioMixAudit = mixRows.map((m) => {
      const failures = classifyPortfolioMixFailures(m.body || "");
      return {
        title: nz(m.title),
        body: nz(m.body).slice(0, 300),
        recordId: m.recordId,
        failures,
      };
    });

    // Openings
    const openRows = rows.filter((r) => r.slotKey === "footprint.openings");
    const openingsAudit = openRows.map((o) => {
      const failures = classifyOpeningsFailures(o.title || "", o.body || "");
      return {
        title: nz(o.title),
        body: nz(o.body).slice(0, 200),
        recordId: o.recordId,
        hasImage: Boolean(o.imageUrl),
        failures,
      };
    });

    brandRows.push({
      brandSlug: slug,
      brandName: identity.name,
      recordId: identity.recordId,
      momentum: {
        count: momRows.length,
        cards: momentumAudit,
        totalFailures: momentumAudit.reduce((n, c) => n + c.failures.length, 0),
      },
      portfolioMix: {
        count: mixRows.length,
        cards: portfolioMixAudit,
        totalFailures: portfolioMixAudit.reduce((n, c) => n + c.failures.length, 0),
      },
      openings: {
        count: openRows.length,
        cards: openingsAudit,
        totalFailures: openingsAudit.reduce((n, c) => n + c.failures.length, 0),
      },
    });
  }

  const totalFailures = brandRows.reduce(
    (n, b) =>
      n +
      (b.momentum?.totalFailures || 0) +
      (b.portfolioMix?.totalFailures || 0) +
      (b.openings?.totalFailures || 0),
    0
  );

  const report = {
    version: WAVE14_FOUNDER_VISUAL_SEMANTIC_FAILURES_VERSION,
    generatedAt: new Date().toISOString(),
    summary: {
      brands: brandRows.length,
      totalFailures,
      momentumFailures: brandRows.reduce((n, b) => n + (b.momentum?.totalFailures || 0), 0),
      portfolioMixFailures: brandRows.reduce((n, b) => n + (b.portfolioMix?.totalFailures || 0), 0),
      openingsFailures: brandRows.reduce((n, b) => n + (b.openings?.totalFailures || 0), 0),
    },
    brands: brandRows,
    readyStatement: "wave14_founder_visual_semantic_failures_extracted_awaiting_remediation",
  };

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave14-founder-visual-semantic-failures.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave14-founder-visual-semantic-failures.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    "# Wave 14 — Founder Visual Semantic Failures",
    "",
    `Version: \`${report.version}\``,
    `Generated: ${report.generatedAt}`,
    "",
    `Total failures: ${totalFailures}`,
    "",
    "## Summary by Brand",
    "",
  ];
  for (const b of brandRows) {
    if (b.blocked) {
      lines.push(`### ${b.brandSlug} — BLOCKED`);
      continue;
    }
    lines.push(`### ${b.brandName} (\`${b.brandSlug}\`)`);
    lines.push("");
    lines.push("**Momentum:**");
    for (const c of b.momentum?.cards || []) {
      lines.push(`- ${c.title || "(blank)"} · ${c.sourceType} · ${c.failures.join(", ") || "ok"}`);
    }
    lines.push("");
    lines.push("**Portfolio Mix:**");
    for (const c of b.portfolioMix?.cards || []) {
      lines.push(`- ${c.failures.join(", ") || "ok"}: ${c.body.slice(0, 120)}`);
    }
    lines.push("");
    lines.push("**Openings:**");
    for (const c of b.openings?.cards || []) {
      lines.push(`- ${c.title || "(blank)"} · ${c.failures.join(", ") || "ok"}`);
    }
    lines.push("");
  }
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");

  return { ...report, paths: { jsonPath, mdPath } };
}

const isMain =
  process.argv[1] &&
  path.resolve(process.argv[1]).includes("brand-explorer-wave14-founder-visual-semantic-failures");

if (isMain) {
  extractWave14FounderVisualSemanticFailures()
    .then((r) => {
      console.log(JSON.stringify({ ready: r.readyStatement, summary: r.summary }, null, 2));
    })
    .catch((err) => {
      console.error(err);
      process.exitCode = 1;
    });
}
