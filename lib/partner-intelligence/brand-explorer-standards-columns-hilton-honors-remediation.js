/**
 * Brand Explorer — standards column reshape + Hilton Honors Diamond Reserve fill.
 *
 * Fixes:
 * 1) standards.requirement rows missing Owner Planning / Notes to confirm labels
 * 2) Hilton Honors brands missing loyalty.elite (incl. Diamond Reserve)
 *
 * Presentation-table writes only. No Brand Status / CV / Source / Registry writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { PRESENTATION_TABLE } from "./brand-explorer-residual-owner-copy-remediation.js";
import {
  parseRequirementColumns,
  requirementRowHasRequiredColumns,
} from "./brand-explorer-tribute-standard-detail-review-approval-writer.js";
import { applyHiltonLoyaltyPresentationSlots } from "./build-hilton-loyalty-presentation-slots.js";
import { HILTON_HONORS_ELITE_TIERS } from "./hilton-honors-loyalty-source.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { resolveActiveUniverseRecordId } from "./brand-explorer-active-universe.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";

export const WRITER_VERSION = "standards-columns-hilton-honors-v1";
export const REPORT_JSON = "brand-explorer-standards-columns-hilton-honors-remediation.json";
export const REPORT_MD = "brand-explorer-standards-columns-hilton-honors-remediation.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

/** Brands with unstructured standards.requirement (from live audit). */
export const STANDARDS_COLUMN_TARGET_SLUGS = Object.freeze([
  "tapestry-collection-by-hilton",
  "autograph-collection",
  "bw-premier-collection",
  "bw-signature-collection",
  "handwritten-collection",
  "preferred-hotels-and-resorts",
  "vignette-collection",
  "radisson-blu",
]);

/** Hilton Honors brands that need elite ladder / Diamond Reserve when missing. */
export const HILTON_HONORS_LOYALTY_TARGET_SLUGS = Object.freeze([
  "tapestry-collection-by-hilton",
]);

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-standards-columns-hilton-honors-remediation",
  "--confirm-presentation-only",
  "--confirm-no-brand-status-changes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
]);

const FORBIDDEN_FIELD_NAMES = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Brand Status",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

const RECORD_ID_BY_SLUG = Object.freeze({
  "tapestry-collection-by-hilton": "reccXxMHEh7NNRhIE",
  "autograph-collection": "recEJCTDj1zrsjPM6",
  "bw-premier-collection": "recwXZ5gVZ8ZH8ekA",
  "bw-signature-collection": "recdeh1NsP4gjrv80",
  "handwritten-collection": null, // resolve via universe / API
  "preferred-hotels-and-resorts": null,
  "vignette-collection": "recDwzv86TWnz2gGB",
  "radisson-blu": null,
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function mockRes() {
  return {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
      return this;
    },
  };
}

async function fetchBrand(brandId) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) {
    throw new Error(`Brand fetch failed ${brandId}: ${res.statusCode}`);
  }
  return res.payload.brand;
}

async function resolveRecordId(slug) {
  const known =
    RECORD_ID_BY_SLUG[slug] ||
    FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug]?.recordId ||
    null;
  if (known) return known;
  return resolveActiveUniverseRecordId(slug);
}

/**
 * Convert unstructured (or partially labeled) requirement Body into the
 * five-column owner-planning shape the Brand Explorer table parser expects.
 */
export function normalizeUnstructuredRequirementBody(body, { brandName = "brand" } = {}) {
  const raw = normalizeBody(body);
  const cols = parseRequirementColumns(raw);
  const labeledAlready =
    hasVal(cols.typical) || hasVal(cols.owner) || hasVal(cols.status) || hasVal(cols.notes);

  let typical = cols.typical;
  let owner = cols.owner;
  let status = cols.status;
  let notes = cols.notes;

  if (!labeledAlready) {
    typical = raw;
  } else if (!typical) {
    // Labeled but missing typical — keep remaining unlabeled lines as typical if any
    typical = raw
      .split("\n")
      .map((l) => l.trim())
      .filter(
        (t) =>
          t &&
          !/^Typical consideration:/i.test(t) &&
          !/^Owner planning consideration:/i.test(t) &&
          !/^Owner planning:/i.test(t) &&
          !/^Typical status:/i.test(t) &&
          !/^Notes to confirm:/i.test(t) &&
          !/^Applies to:/i.test(t) &&
          !/^Flexibility/i.test(t) &&
          !/^Source confidence:/i.test(t)
      )
      .join(" ")
      .trim();
  }

  // Pull trailing "Confirm …" sentence into Notes when Notes empty.
  if (!notes && typical) {
    const m = typical.match(/([^.!?]*\bconfirm\b[^.!?]*[.!?])\s*$/i);
    if (m) {
      notes = m[1].trim();
      const stripped = typical.replace(m[0], "").trim();
      if (stripped) typical = stripped;
    }
  }

  if (!owner) {
    owner = `Plan capital, timeline, staffing, and operator responsibilities for this requirement before underwriting; validate against current ${brandName} disclosure for the specific asset.`;
  }
  if (!status) {
    status = /\b(may|optional|case-by-case|varies)\b/i.test(typical)
      ? "May Apply"
      : /\b(typically|required|must|expected)\b/i.test(typical)
        ? "Typically Expected"
        : "Confirm with brand";
  }
  if (!notes) {
    notes = `Confirm current ${brandName} standards, agreement vintage, and development guidance for this asset.`;
  }
  if (!typical) {
    typical = `Confirm ${brandName} requirement detail with brand disclosure.`;
  }

  const normalizedBody = [
    `Typical consideration: ${typical}`,
    `Owner planning consideration: ${owner}`,
    `Typical status: ${status}`,
    `Notes to confirm: ${notes}`,
  ].join("\n");

  const beforeComplete = requirementRowHasRequiredColumns({ title: "x", body: raw });
  const afterComplete = requirementRowHasRequiredColumns({ title: "x", body: normalizedBody });
  return {
    normalizedBody,
    changed: normalizeBody(raw) !== normalizeBody(normalizedBody),
    beforeComplete,
    afterComplete,
    beforeCols: cols,
    afterCols: parseRequirementColumns(normalizedBody),
  };
}

function hasVal(v) {
  return nz(v) !== "";
}

async function airtableWrite({ baseId, apiKey, method, recordId = null, fields }) {
  for (const k of Object.keys(fields || {})) {
    if (FORBIDDEN_FIELD_NAMES.includes(k)) {
      throw new Error(`Forbidden field write blocked: ${k}`);
    }
  }
  const url = recordId
    ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`
    : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}`;

  let lastErr = null;
  for (let attempt = 0; attempt < 5; attempt++) {
    if (attempt > 0 || method) {
      await new Promise((r) => setTimeout(r, 220 + attempt * 400));
    }
    const res = await fetch(url, {
      method,
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields, typecast: true }),
    });
    const json = await res.json();
    if (res.ok) return json;
    lastErr = new Error(`${method} failed: ${res.status} ${JSON.stringify(json)}`);
    if (res.status !== 429) throw lastErr;
  }
  throw lastErr;
}

function blocksFor(brand, slotKey) {
  return (brand?.brandExplorer?.blocks || []).filter((b) => nz(b.slotKey) === slotKey);
}

function hasDiamondReserve(brand) {
  return blocksFor(brand, "loyalty.elite").some(
    (e) => /diamond\s*reserve/i.test(e.title || "") || /diamond\s*reserve/i.test(e.body || "")
  );
}

function isHiltonHonors(brand) {
  const prog = nz(brand?.loyaltyCommercial?.formValues?.typicalLoyaltyProgramName);
  const parent = nz(brand?.parentCompany);
  const name = nz(brand?.name);
  return /hilton honors/i.test(prog) || (/hilton/i.test(parent) && /hilton/i.test(name));
}

/**
 * @param {{ apply?: boolean, slugs?: string[], approveFlags?: boolean }} options
 */
export async function runStandardsColumnsHiltonHonorsRemediation(options = {}) {
  const apply = Boolean(options.apply);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const slugs = options.slugs?.length
    ? options.slugs
    : [
        ...new Set([...STANDARDS_COLUMN_TARGET_SLUGS, ...HILTON_HONORS_LOYALTY_TARGET_SLUGS]),
      ];

  const brandResults = [];
  const patches = [];

  for (const slug of slugs) {
    const recordId = await resolveRecordId(slug);
    if (!recordId) {
      brandResults.push({ slug, error: "record_id_unresolved" });
      continue;
    }
    const brand = await fetchBrand(recordId);
    const brandName = brand.name || slug;
    const reqRows = blocksFor(brand, "standards.requirement");
    const eliteBefore = blocksFor(brand, "loyalty.elite");
    const standardsPatches = [];

    for (const row of reqRows) {
      const norm = normalizeUnstructuredRequirementBody(row.body, { brandName });
      if (!norm.changed) continue;
      const patch = {
        kind: "update",
        slug,
        brandName,
        slotKey: "standards.requirement",
        recordId: row.recordId || row.id,
        title: row.title,
        beforeBody: row.body,
        afterBody: norm.normalizedBody,
        beforeComplete: norm.beforeComplete,
        afterComplete: norm.afterComplete,
        fields: { Body: norm.normalizedBody },
      };
      standardsPatches.push(patch);
      patches.push(patch);
    }

    const loyaltyPatches = [];
    const needsHiltonLoyalty =
      HILTON_HONORS_LOYALTY_TARGET_SLUGS.includes(slug) ||
      (isHiltonHonors(brand) && (!eliteBefore.length || !hasDiamondReserve(brand)));

    if (needsHiltonLoyalty && isHiltonHonors(brand)) {
      const proposed = applyHiltonLoyaltyPresentationSlots([], [], { brandName });
      const existingLoyalty = (brand.brandExplorer?.blocks || []).filter((b) =>
        nz(b.slotKey).startsWith("loyalty.")
      );
      const existingBySlot = new Map();
      for (const b of existingLoyalty) {
        const k = nz(b.slotKey);
        if (!existingBySlot.has(k)) existingBySlot.set(k, []);
        existingBySlot.get(k).push(b);
      }

      for (const row of proposed) {
        const slot = nz(row.slotKey);
        if (slot === "loyalty.elite") {
          const titles = new Set(
            eliteBefore.map((e) => nz(e.title).toLowerCase().replace(/^hilton honors\s+/i, ""))
          );
          const proposedTitle = nz(row.title).toLowerCase().replace(/^hilton honors\s+/i, "");
          const already =
            titles.has(proposedTitle) ||
            [...titles].some(
              (t) => t === proposedTitle || t.includes(proposedTitle) || proposedTitle.includes(t)
            );
          if (already) continue;
          // If ladder is complete with Diamond Reserve and all 5 tiers, skip.
          if (hasDiamondReserve(brand) && eliteBefore.length >= 5) continue;
        } else if (existingBySlot.has(slot) && existingBySlot.get(slot).length > 0) {
          continue; // keep existing non-elite loyalty rows
        }

        const patch = {
          kind: "create",
          slug,
          brandName,
          slotKey: slot,
          recordId: null,
          title: row.title || "",
          beforeBody: null,
          afterBody: row.body,
          fields: {
            Active: true,
            Brand: [brand.id || recordId],
            "Brand Name": brandName,
            "Slot Key": slot,
            Title: row.title || "",
            Body: row.body,
            "Sort Order": row.sort ?? 0,
          },
        };
        loyaltyPatches.push(patch);
        patches.push(patch);
      }

      // If elite exists without Diamond Reserve only — add DR row alone
      if (eliteBefore.length > 0 && !hasDiamondReserve(brand)) {
        const dr = HILTON_HONORS_ELITE_TIERS.find((t) => t.name === "Diamond Reserve");
        if (dr && !loyaltyPatches.some((p) => /diamond reserve/i.test(p.title))) {
          const patch = {
            kind: "create",
            slug,
            brandName,
            slotKey: "loyalty.elite",
            recordId: null,
            title: dr.headline,
            beforeBody: null,
            afterBody: `${dr.qualification} — ${dr.body}`,
            fields: {
              Active: true,
              Brand: [brand.id || recordId],
              "Brand Name": brandName,
              "Slot Key": "loyalty.elite",
              Title: dr.headline,
              Body: `${dr.qualification} — ${dr.body}`,
              "Sort Order": 4,
            },
          };
          loyaltyPatches.push(patch);
          patches.push(patch);
        }
      }
    }

    brandResults.push({
      slug,
      brandName,
      recordId: brand.id || recordId,
      isHiltonHonors: isHiltonHonors(brand),
      requirementRowCount: reqRows.length,
      standardsPatches: standardsPatches.length,
      eliteBefore: eliteBefore.map((e) => e.title),
      hasDiamondReserveBefore: hasDiamondReserve(brand),
      loyaltyCreates: loyaltyPatches.length,
      loyaltySlotsProposed: loyaltyPatches.map((p) => ({
        slotKey: p.slotKey,
        title: p.title,
      })),
    });
  }

  const applied = [];
  const errors = [];
  if (apply) {
    if (!options.approveFlags) {
      throw new Error(`Apply requires flags: ${REQUIRED_APPLY_FLAGS.join(" ")}`);
    }
    for (const p of patches) {
      try {
        if (p.kind === "update") {
          if (!p.recordId) throw new Error(`Missing recordId for ${p.slug} ${p.title}`);
          await airtableWrite({
            baseId,
            apiKey,
            method: "PATCH",
            recordId: p.recordId,
            fields: p.fields,
          });
        } else if (p.kind === "create") {
          const created = await airtableWrite({
            baseId,
            apiKey,
            method: "POST",
            fields: p.fields,
          });
          p.createdRecordId = created.id;
        }
        applied.push({
          kind: p.kind,
          slug: p.slug,
          slotKey: p.slotKey,
          title: p.title,
          recordId: p.recordId || p.createdRecordId,
        });
      } catch (err) {
        errors.push({
          slug: p.slug,
          slotKey: p.slotKey,
          title: p.title,
          error: err.message || String(err),
        });
      }
    }
  }

  const report = {
    version: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    airtableWrites: apply && applied.length > 0,
    brandStatusWrites: false,
    companyValidatedWrites: false,
    sourceLibraryWrites: false,
    registryWrites: false,
    presentationTable: PRESENTATION_TABLE,
    brandResults,
    patchCount: patches.length,
    patches: patches.map((p) => ({
      kind: p.kind,
      slug: p.slug,
      slotKey: p.slotKey,
      title: p.title,
      recordId: p.recordId,
      beforeBodyPreview: p.beforeBody ? String(p.beforeBody).slice(0, 180) : null,
      afterBodyPreview: p.afterBody ? String(p.afterBody).slice(0, 280) : null,
    })),
    applied,
    errors,
    hiltonEliteTierNames: HILTON_HONORS_ELITE_TIERS.map((t) => t.name),
  };

  return report;
}

export function formatRemediationMarkdown(report) {
  const lines = [
    `# Brand Explorer — Standards Columns + Hilton Honors Remediation`,
    ``,
    `> ${report.dryRun ? "Dry-run" : "Apply"} · \`${report.generatedAt}\` · Presentation only`,
    ``,
    `## Summary`,
    ``,
    `| Metric | Value |`,
    `|---|---|`,
    `| Patches | ${report.patchCount} |`,
    `| Applied | ${report.applied?.length || 0} |`,
    `| Errors | ${report.errors?.length || 0} |`,
    `| Airtable writes | ${report.airtableWrites ? "yes" : "no"} |`,
    `| Hilton elite tiers | ${report.hiltonEliteTierNames?.join(", ")} |`,
    ``,
    `## Brands`,
    ``,
  ];
  for (const b of report.brandResults || []) {
    if (b.error) {
      lines.push(`- \`${b.slug}\`: ERROR ${b.error}`);
      continue;
    }
    lines.push(
      `- **${b.brandName}** (\`${b.slug}\`): standards patches=${b.standardsPatches}, loyalty creates=${b.loyaltyCreates}, elite before=${(b.eliteBefore || []).join("/") || "—"}, DR before=${b.hasDiamondReserveBefore}`
    );
  }
  lines.push(``);
  lines.push(`## Apply`);
  lines.push(``);
  lines.push("```bash");
  lines.push(
    `npm run brand-explorer-standards-columns-hilton-honors-remediation -- --apply ${REQUIRED_APPLY_FLAGS.join(" ")}`
  );
  lines.push("```");
  lines.push(``);
  return `${lines.join("\n")}\n`;
}

export function writeRemediationReports(report) {
  const dir = path.join(ROOT, "reports");
  fs.mkdirSync(dir, { recursive: true });
  const jsonPath = path.join(dir, REPORT_JSON);
  const mdPath = path.join(dir, REPORT_MD);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, formatRemediationMarkdown(report), "utf8");
  return { jsonPath, mdPath };
}
