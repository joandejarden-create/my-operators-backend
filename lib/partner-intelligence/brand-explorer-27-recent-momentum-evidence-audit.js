/**
 * Brand Explorer — 27-wave Recent Momentum + Openings evidence audit & targeted fixes.
 *
 * Targets: dazzler-by-wyndham, trademark-collection-by-wyndham, tapestry-collection-by-hilton
 * Allowed: Presentation Title/Body, Case Summary*, momentum/openings labels only.
 * Forbidden: CV, Source Library, Registry, Brand Status, release fields, images, protected 24.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { resolveLane2BrandIdentity } from "./brand-explorer-lane2-common.js";
import { LANE2_PROPERTY_CATALOG_BY_SLUG } from "./brand-explorer-lane2-property-catalog.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import {
  evaluateRecentMomentumEvidenceQuality,
  MOMENTUM_EVIDENCE_QUALITY_VERSION,
} from "./brand-explorer-recent-momentum-evidence-quality.js";
import {
  CALA_AVAILABLE_BY_SLUG,
  EVIDENCE_FIX_TARGET_SLUGS,
  getMomentumFixPack,
} from "./brand-explorer-27-recent-momentum-evidence-fix-content.js";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";

export const AUDIT_VERSION = "27-recent-momentum-evidence-audit-v1";

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-recent-momentum-evidence-fixes",
  "--confirm-target-brands-only",
  "--confirm-recent-momentum-and-openings-only",
  "--confirm-cala-first-priority",
  "--confirm-international-reference-labels-where-needed",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-protected-24-brand-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-raw-urls",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const FORBIDDEN_WRITE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Brand Status",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Image",
  "Source Library Status",
  "Registry Approval",
  "Registry Status",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isHidden(row) {
  return (
    row?.active === false ||
    /do not display|internal only/i.test(nz(row?.externalDisplayStatus))
  );
}

function visibleRows(rows, slotKey) {
  return (rows || []).filter((r) => nz(r.slotKey) === slotKey && !isHidden(r));
}

export function resolveTargetBrands(requested = []) {
  const list = (requested || []).map((s) => String(s).trim().toLowerCase()).filter(Boolean);
  if (!list.length) return [...EVIDENCE_FIX_TARGET_SLUGS];
  for (const s of list) {
    if (!EVIDENCE_FIX_TARGET_SLUGS.includes(s)) {
      throw new Error(`Targets only: ${EVIDENCE_FIX_TARGET_SLUGS.join(", ")} (refused ${s})`);
    }
  }
  return list;
}

export function parseEvidenceApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

async function listPresentationRowsDetailed(brandName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
  const formula = `{Brand Name}='${nz(brandName).replace(/'/g, "\\'")}'`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Presentation list failed: ${res.status}`);
    for (const rec of json.records || []) {
      const f = rec.fields || {};
      rows.push({
        recordId: rec.id,
        slotKey: nz(f["Slot Key"]),
        title: nz(f.Title),
        body: nz(f.Body),
        caseSummaryOverview: nz(f["Case Summary Overview"]),
        caseSummaryBrandRelevance: nz(f["Case Summary Brand Relevance"]),
        caseSummaryOwnerObjective: nz(f["Case Summary Owner Objective"]),
        caseSummaryInterpretation: nz(f["Case Summary Interpretation"]),
        caseSummaryTags: nz(f["Case Summary Tags"]),
        externalDisplayStatus: nz(f["External Display Status"]),
        active: f.Active !== false,
        sortOrder: f["Sort Order"] || 0,
      });
    }
    offset = json.offset || "";
  } while (offset);
  return rows;
}

async function fetchBrandApi(slug) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`Brand API failed for ${slug}: HTTP ${res.statusCode}`);
  }
  return res.payload.brand;
}

function validateMomentumBody(body) {
  const forbidden = scanForbiddenLanguage(body).filter((h) => h.id !== "raw_url");
  if (forbidden.length) {
    return { ok: false, reason: forbidden.map((f) => f.id).join(",") };
  }
  return { ok: true };
}

function openingsCaseFields(slug, propertyName, regionLabel) {
  if (slug === "dazzler-by-wyndham") {
    return {
      "Case Summary Overview": `${propertyName} is a CALA Dazzler by Wyndham property reference for owners comparing design-led upscale lifestyle affiliation in Latin America urban markets.`,
      "Case Summary Brand Relevance":
        "Shows Dazzler's defined design personality under Wyndham Rewards on a Latin America urban lifestyle asset.",
      "Case Summary Owner Objective":
        "Use when underwriting design capital and whether the asset can support Dazzler's lifestyle guest promise in a CALA market.",
      "Case Summary Interpretation":
        "Confirm current design-review criteria and market authorization with Wyndham development before underwriting fit.",
      "Case Summary Tags": "CALA, Dazzler, Lifestyle, Urban, Property example",
    };
  }
  if (slug === "trademark-collection-by-wyndham") {
    return {
      "Case Summary Overview": `${propertyName} is an International Reference Trademark Collection by Wyndham property example — not labeled as CALA presence.`,
      "Case Summary Brand Relevance":
        "Shows Trademark Collection's property-specific identity retention with Wyndham Rewards on an International Reference asset.",
      "Case Summary Owner Objective":
        "Use when underwriting conversion scope and owner control over positioning on an International Reference asset.",
      "Case Summary Interpretation":
        "Confirm acceptance criteria and systems participation with Wyndham development; do not infer CALA coverage from this example.",
      "Case Summary Tags": "International Reference, Trademark Collection, Soft brand, Property example",
    };
  }
  return {
    "Case Summary Overview": `${propertyName} is an International Reference Tapestry Collection by Hilton property example — not labeled as CALA presence.`,
    "Case Summary Brand Relevance":
      "Shows Tapestry's independent-character upscale soft-brand path with Hilton Honors on an International Reference asset.",
    "Case Summary Owner Objective":
      "Use when underwriting design-review intensity and Honors participation on an International Reference asset.",
    "Case Summary Interpretation":
      "Confirm live affiliation criteria with Hilton development; do not infer CALA coverage from this example.",
    "Case Summary Tags": "International Reference, Tapestry Collection, Soft brand, Property example",
  };
}

function rebuildOpeningsBody({ chips, locationLine, metaLine, teaser, sourceUrl }) {
  const parts = [chips, locationLine, metaLine, teaser].map(nz).filter(Boolean);
  if (sourceUrl && /^https?:\/\//i.test(sourceUrl)) parts.push(sourceUrl);
  return parts.join("\n\n");
}

/** Normalize property titles for catalog matching (never match openings by array index). */
export function normalizePropertyMatchKey(value) {
  return nz(value)
    .toLowerCase()
    .replace(/[,·—–|]/g, " ")
    .replace(
      /\b(trademark collection by wyndham|tapestry collection by hilton|dazzler by wyndham|collection by hilton|collection by wyndham|the)\b/g,
      " "
    )
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Match an openings row title to the Lane2 property catalog by name tokens.
 * Index-order pairing is forbidden — it mis-links sibling properties (Trademark bug).
 */
export function matchCatalogProperty(title, catalog = []) {
  const titleKey = normalizePropertyMatchKey(String(title || "").split(/\s*[—–]\s*/)[0]);
  if (!titleKey || !catalog.length) return null;

  let best = null;
  let bestScore = 0;
  for (const entry of catalog) {
    const nameKey = normalizePropertyMatchKey(entry?.propertyName || "");
    if (!nameKey) continue;

    let score = 0;
    if (titleKey === nameKey) score = 100;
    else if (titleKey.includes(nameKey) || nameKey.includes(titleKey)) {
      score = 80 + Math.min(titleKey.length, nameKey.length) / 100;
    } else {
      const titleTokens = titleKey.split(" ").filter((w) => w.length > 3);
      const nameTokens = new Set(nameKey.split(" ").filter((w) => w.length > 3));
      const overlap = titleTokens.filter((w) => nameTokens.has(w)).length;
      if (overlap >= 2) score = 40 + overlap * 10;
    }

    if (score > bestScore) {
      best = entry;
      bestScore = score;
    }
  }
  return bestScore >= 40 ? best : null;
}

function momentumAlreadyMatchesPack(rows, pack) {
  const live = visibleRows(rows, "footprint.momentum");
  const expected = pack?.momentum || [];
  if (live.length !== expected.length || !expected.length) return false;
  const liveTitles = new Set(live.map((r) => nz(r.title).toLowerCase()));
  return expected.every((c) => liveTitles.has(nz(c.title).toLowerCase()));
}

export async function planEvidenceAuditForBrand(brandSlug) {
  const slug = String(brandSlug).toLowerCase();
  if (!EVIDENCE_FIX_TARGET_SLUGS.includes(slug)) {
    throw new Error(`Targets only: ${EVIDENCE_FIX_TARGET_SLUGS.join(", ")}`);
  }
  const identity = resolveLane2BrandIdentity(slug);
  const pack = getMomentumFixPack(slug);
  if (!pack) throw new Error(`No evidence fix pack for ${slug}`);

  const rows = await listPresentationRowsDetailed(identity.name);
  const brand = await fetchBrandApi(slug);
  const html = renderBrandExplorerHtmlForTest(brand, { allPanels: true, internalPreview: false });
  const catalog = LANE2_PROPERTY_CATALOG_BY_SLUG[slug] || [];

  const before = evaluateRecentMomentumEvidenceQuality({
    brandSlug: slug,
    brandName: identity.name,
    presentationRows: rows,
    html,
    propertyCatalog: catalog,
    calaAvailableOverride: CALA_AVAILABLE_BY_SLUG[slug],
  });

  const patches = [];
  const blockers = [];
  const keepMomentum = momentumAlreadyMatchesPack(rows, pack);

  // Hide existing momentum cards only when replacing with curated pack
  if (!keepMomentum) {
    for (const m of visibleRows(rows, "footprint.momentum")) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: m.recordId,
        slotKey: "footprint.momentum",
        reason: "hide_prior_momentum_for_evidence_replace",
        fields: { Active: false, "External Display Status": "Do Not Display" },
        beforeTitle: m.title,
      });
    }
  }

  // Label
  const labelRows = visibleRows(rows, "footprint.momentum_label");
  const anyLabel = (rows || []).find((r) => r.slotKey === "footprint.momentum_label");
  if (labelRows[0]) {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: labelRows[0].recordId,
      slotKey: "footprint.momentum_label",
      reason: "momentum_label_region_contract",
      fields: { Body: pack.label, Title: labelRows[0].title || "" },
    });
  } else if (anyLabel?.recordId) {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: anyLabel.recordId,
      slotKey: "footprint.momentum_label",
      reason: "reactivate_momentum_label",
      fields: { Active: true, Body: pack.label },
    });
  } else {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "POST",
      recordId: null,
      slotKey: "footprint.momentum_label",
      reason: "create_momentum_label",
      fields: {
        "Slot Key": "footprint.momentum_label",
        "Brand Name": identity.name,
        Brand: [identity.recordId],
        Active: true,
        "Sort Order": 1,
        Title: "",
        Body: pack.label,
      },
    });
  }

  // Create structured momentum cards only when replacing
  if (!keepMomentum) {
    for (const card of pack.momentum) {
      const v = validateMomentumBody(card.body);
      if (!v.ok) {
        blockers.push(`momentum_forbidden:${card.title}:${v.reason}`);
        continue;
      }
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey: "footprint.momentum",
        reason: "create_evidence_quality_momentum_card",
        fields: {
          "Slot Key": "footprint.momentum",
          "Brand Name": identity.name,
          Brand: [identity.recordId],
          Active: true,
          "Sort Order": card.sort || 1,
          Title: card.title,
          Body: card.body,
        },
        meta: {
          date: card.dateLine,
          link: card.url,
          region: card.region,
          evidenceType: card.evidenceType,
        },
      });
    }
  }

  // Openings: region labels + case summaries + structured body (no Image writes)
  // Match catalog by property name — never by array index.
  const openings = visibleRows(rows, "footprint.openings");
  const regionLabel = pack.openingsLabelUpdates.proposedLabel;
  openings.forEach((row) => {
    const propertyName = nz(row.title).split("—")[0].trim() || nz(row.title) || "Property example";
    const catalogHit = matchCatalogProperty(row.title, catalog) || {};
    if (!catalogHit.sourcePageUrl) {
      blockers.push(`openings_no_catalog_match:${propertyName}`);
    }
    const market =
      catalogHit.marketCity ||
      nz(row.title).split("—")[1]?.trim() ||
      "";
    const geo = catalogHit.geographyLabel || regionLabel;
    const chips =
      regionLabel === "CALA"
        ? `CALA, ${market || "Market"}, Urban, Collection`
        : `International Reference, ${market || "Market"}, Urban, Collection`;
    const locationLine = market ? `${market} (${geo})` : geo;
    const metaLine = geo;
    const teaser =
      regionLabel === "CALA"
        ? `${propertyName}${market ? ` in ${market}` : ""} is a CALA ${identity.name} property reference for owners underwriting design narrative, capital intensity, and platform fit.`
        : `${propertyName}${market ? ` in ${market}` : ""} is an International Reference ${identity.name} property example — not a CALA presence claim.`;
    const sourceUrl = catalogHit.sourcePageUrl || "";
    const body = rebuildOpeningsBody({ chips, locationLine, metaLine, teaser, sourceUrl });
    const caseFields = openingsCaseFields(slug, propertyName, regionLabel);

    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: row.recordId,
      slotKey: "footprint.openings",
      reason: "openings_region_label_and_case_summary",
      fields: {
        Body: body,
        ...caseFields,
      },
      meta: {
        propertyName,
        region: regionLabel,
        sourceLink: sourceUrl,
        proposedLabel: regionLabel,
        catalogKey: catalogHit.propertyKey || null,
      },
    });
  });

  for (const p of patches) {
    for (const key of Object.keys(p.fields || {})) {
      if (FORBIDDEN_WRITE_FIELDS.has(key)) {
        delete p.fields[key];
        blockers.push(`stripped_forbidden:${key}`);
      }
    }
  }

  return {
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    calaAvailable: CALA_AVAILABLE_BY_SLUG[slug],
    before,
    patches,
    blockers,
    blocked: blockers.some((b) => b.startsWith("momentum_forbidden")),
    auditTableRows: before.momentumCards.map((c) => ({
      Brand: slug,
      Section: "Recent Momentum",
      Card: c.title,
      "Record ID": c.recordId,
      Title: c.title,
      Date: c.date,
      Link: c.link,
      Region: c.region,
      "Evidence Type": c.evidenceType,
      Issue: c.issues.join("; ") || "—",
      "Proposed Fix": c.issues.length ? "replace_with_source_backed_card" : "keep",
    })),
    openingsAuditRows: before.openingsCards.map((c) => ({
      Brand: slug,
      "Property / Example": c.title,
      "Region Classification": c.region,
      "Brand Correct?": c.brandCorrect,
      "CALA Available?": c.calaAvailable,
      "Current Label": c.currentLabel,
      "Proposed Label": regionLabel,
      "Source Link": c.sourceLink || "",
      Issue: c.issues.join("; ") || "—",
    })),
  };
}

export async function planEvidenceAudit({ brands = EVIDENCE_FIX_TARGET_SLUGS } = {}) {
  const resolved = resolveTargetBrands(brands);
  const brandResults = [];
  for (const slug of resolved) {
    brandResults.push(await planEvidenceAuditForBrand(slug));
  }
  return {
    version: AUDIT_VERSION,
    gateVersion: MOMENTUM_EVIDENCE_QUALITY_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands: resolved,
    brandResults,
    summary: {
      totalPatches: brandResults.reduce((n, b) => n + b.patches.length, 0),
      brandsFailingBefore: brandResults.filter((b) => !b.before.pass).length,
      writes: false,
    },
  };
}

async function airtableWrite({ baseId, apiKey, table, recordId, fields, method }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `${method} ${table} failed: ${res.status}`);
  return json;
}

export async function applyEvidenceFixes({ plan, apply = false, argv = [] } = {}) {
  const flagCheck = parseEvidenceApplyFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flagCheck };
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing, flagCheck };
  }
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const resultsByBrand = {};
  for (const brand of plan.brandResults || []) {
    if (!EVIDENCE_FIX_TARGET_SLUGS.includes(brand.brandSlug)) {
      throw new Error(`Refuse non-target write: ${brand.brandSlug}`);
    }
    if (brand.blocked) {
      resultsByBrand[brand.brandSlug] = { applied: false, reason: "blocked", blockers: brand.blockers };
      continue;
    }
    const created = [];
    const updated = [];
    const errors = [];
    for (const patch of brand.patches) {
      for (const key of Object.keys(patch.fields || {})) {
        if (FORBIDDEN_WRITE_FIELDS.has(key)) throw new Error(`Forbidden field write: ${key}`);
      }
      try {
        if (patch.action === "POST") {
          const json = await airtableWrite({
            baseId,
            apiKey,
            table: patch.table,
            fields: patch.fields,
            method: "POST",
          });
          created.push({ recordId: json.id, slotKey: patch.slotKey, reason: patch.reason });
        } else {
          await airtableWrite({
            baseId,
            apiKey,
            table: patch.table,
            recordId: patch.recordId,
            fields: patch.fields,
            method: "PATCH",
          });
          updated.push({ recordId: patch.recordId, slotKey: patch.slotKey, reason: patch.reason });
        }
        await sleep(220);
      } catch (err) {
        errors.push({ slotKey: patch.slotKey, reason: patch.reason, message: err.message });
      }
    }
    resultsByBrand[brand.brandSlug] = {
      applied: errors.length === 0,
      created,
      updated,
      errors,
    };
  }
  return { applied: true, resultsByBrand, flagCheck };
}

function mdTable(rows, columns) {
  if (!rows.length) return "_None._\n";
  const header = `| ${columns.join(" | ")} |`;
  const sep = `| ${columns.map(() => "---").join(" | ")} |`;
  const body = rows.map((r) => `| ${columns.map((c) => String(r[c] ?? "").replace(/\|/g, "/")).join(" | ")} |`);
  return [header, sep, ...body].join("\n") + "\n";
}

export function writeEvidenceAuditReports(plan, applyResult = null) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const report = {
    ...plan,
    dryRun: !(applyResult?.applied === true),
    applyResult: applyResult || { applied: false, reason: "dry_run_only" },
    summary: {
      ...plan.summary,
      writes: applyResult?.applied === true,
    },
  };

  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-27-recent-momentum-evidence-audit.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-27-recent-momentum-evidence-audit.md");
  const openingsMd = path.join(REPORTS_DIR, "brand-explorer-27-openings-examples-regional-priority-audit.md");
  const fixesMd = path.join(REPORTS_DIR, "brand-explorer-27-recent-momentum-fixes.md");
  const docsPath = path.join(DOCS_DIR, "brand-explorer-27-recent-momentum-evidence-quality.md");

  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const allMomentum = report.brandResults.flatMap((b) => b.auditTableRows);
  const allOpenings = report.brandResults.flatMap((b) => b.openingsAuditRows);

  const auditMd = [
    `# Recent Momentum Evidence Audit (27-wave)`,
    "",
    `Generated: ${report.generatedAt}`,
    `Version: ${AUDIT_VERSION}`,
    `dryRun=${report.dryRun}`,
    "",
    `Brands failing before: **${report.summary.brandsFailingBefore}**`,
    `Total patches: **${report.summary.totalPatches}**`,
    `Writes: **${report.summary.writes}**`,
    "",
    "## Recent Momentum cards",
    "",
    mdTable(allMomentum, [
      "Brand",
      "Section",
      "Card",
      "Record ID",
      "Title",
      "Date",
      "Link",
      "Region",
      "Evidence Type",
      "Issue",
      "Proposed Fix",
    ]),
  ];
  fs.writeFileSync(mdPath, auditMd.join("\n"));

  const openingsReport = [
    `# Openings / Examples / Properties — Regional Priority Audit`,
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "Rule: prioritize CALA when available; otherwise label **International Reference**.",
    "",
    mdTable(allOpenings, [
      "Brand",
      "Property / Example",
      "Region Classification",
      "Brand Correct?",
      "CALA Available?",
      "Current Label",
      "Proposed Label",
      "Source Link",
      "Issue",
    ]),
  ];
  fs.writeFileSync(openingsMd, openingsReport.join("\n"));

  const fixesLines = [
    `# Recent Momentum Evidence Fixes`,
    "",
    `Generated: ${report.generatedAt}`,
    `Writes: **${report.summary.writes}**`,
    "",
  ];
  for (const b of report.brandResults) {
    fixesLines.push(`## ${b.brandSlug}`);
    fixesLines.push(`- CALA available: **${b.calaAvailable}**`);
    fixesLines.push(`- Before pass: **${b.before.pass}** (fails=${b.before.failures.length})`);
    fixesLines.push(`- Patches: **${b.patches.length}**`);
    fixesLines.push("");
    for (const p of b.patches) {
      fixesLines.push(`- \`${p.action}\` ${p.slotKey} · ${p.reason}`);
    }
    const applyRow = applyResult?.resultsByBrand?.[b.brandSlug];
    if (applyRow) {
      fixesLines.push("");
      fixesLines.push(
        `Apply: created=${applyRow.created?.length || 0} updated=${applyRow.updated?.length || 0} errors=${applyRow.errors?.length || 0}`
      );
    }
    fixesLines.push("");
  }
  fs.writeFileSync(fixesMd, fixesLines.join("\n"));

  const docs = [
    `# Recent Momentum / Openings Evidence Quality (27-wave)`,
    "",
    `Gate version: \`${MOMENTUM_EVIDENCE_QUALITY_VERSION}\``,
    `Audit version: \`${AUDIT_VERSION}\``,
    "",
    "## Purpose",
    "",
    "Permanent factory gate ensuring Recent Momentum and Openings/Examples/Properties use source-backed dates/links, correct region labels (CALA first when available; otherwise International Reference), brand-correct evidence, and no raw URLs in public rendered HTML.",
    "",
    "## Targets",
    "",
    "- `dazzler-by-wyndham` (CALA available)",
    "- `trademark-collection-by-wyndham` (no CALA in pack → International Reference)",
    "- `tapestry-collection-by-hilton` (no CALA in pack → International Reference)",
    "",
    "## npm",
    "",
    "```bash",
    "npm run brand-explorer-27-recent-momentum-evidence-audit -- --brands dazzler-by-wyndham,trademark-collection-by-wyndham,tapestry-collection-by-hilton --dry-run",
    "npm run test:brand-explorer-recent-momentum-evidence-quality -- --brands dazzler-by-wyndham,trademark-collection-by-wyndham,tapestry-collection-by-hilton",
    "```",
    "",
    "## Gate failure conditions",
    "",
    "- Missing/invalid date",
    "- Missing source URL (Body third paragraph for frontend link render)",
    "- Raw URL visible in public HTML text (outside href)",
    "- Wrong-brand / sibling-brand evidence",
    "- Invented year on property-listing-only cards without announcement framing",
    "- CALA available but unused / not prioritized",
    "- Non-CALA examples missing International Reference label",
    "- Thin momentum body (<35 words)",
    "- Generic / diligence-filler momentum copy",
    "",
    "## Data contract",
    "",
    "- Table: Brand Setup - Brand Explorer Presentation",
    "- Momentum Body shape: `dateLine \\n\\n summary \\n\\n https://url` (URL rendered as labeled hyperlink, not raw text)",
    "- Openings: Case Summary Tags + structured Body chips; region in tags/overview",
    "- No CV / Source Library / Registry / Brand Status / release / Image writes",
    "",
    `Last report: ${report.generatedAt}`,
    "",
  ];
  fs.writeFileSync(docsPath, docs.join("\n"));

  return { jsonPath, mdPath, openingsMd, fixesMd, docsPath };
}
