/**
 * Brand Setup child-table validation — Active-62 read-only.
 *
 * Validates structure/linkage/language for the 10 Brand Setup child tables that
 * sit outside Brand Explorer Active-62 content gates. Report-only; no writes.
 *
 * Excludes: Presentation (BE content), Basics (parent), Census, held brands as
 * validation targets (Flex / House / Morgans / Radisson Collection probed only
 * for stale linkage).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BASELINE_62_HELD_EXCLUDED,
  EXPECTED_ACTIVE_COUNT_62,
  FREEZE_DECISION_62,
  REPORT_JSON_62,
  ROOT,
} from "./brand-explorer-62-active-public-full-baseline.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { EXTRA_FORBIDDEN_RE } from "./brand-explorer-62-background-validation.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export const VALIDATION_VERSION = "brand-setup-child-table-validation-62-readonly-v1";
export const VALIDATION_STATUS =
  "brand_setup_child_table_validation_62_readonly_complete_ready_for_remediation_queue";

export const BASICS_TABLE = "Brand Setup - Brand Basics";
export const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

/** 1:1 Brand Setup child tables (Brand Library form support). */
export const CHILD_TABLES = Object.freeze([
  "Brand Setup - Brand Footprint",
  "Brand Setup - Project Fit",
  "Brand Setup - Portfolio & Performance",
  "Brand Setup - Brand Standards",
  "Brand Setup - Fee Structure",
  "Brand Setup - Deal Terms",
  "Brand Setup - Operational Support",
  "Brand Setup - Legal Terms",
  "Brand Setup - Loyalty & Commercial",
  "Brand Setup - Sustainability & ESG",
]);

/** Basics → child reverse-link field names (Brand Library contract). */
export const BASICS_CHILD_LINK_FIELDS = Object.freeze({
  "Brand Setup - Brand Footprint": ["Brand Setup - Brand Footprint"],
  "Brand Setup - Loyalty & Commercial": ["Brand Setup - Loyalty & Commercial"],
  "Brand Setup - Fee Structure": ["Brand Setup - Fee Structure"],
  "Brand Setup - Brand Standards": ["Brand Setup - Brand Standards"],
  "Brand Setup - Deal Terms": ["Brand Setup - Deal Terms"],
  "Brand Setup - Portfolio & Performance": ["Brand Setup - Portfolio & Performance"],
  "Brand Setup - Project Fit": ["Brand Setup - Project Fit"],
  "Brand Setup - Operational Support": [
    "Brand Setup - Operational Support",
    "Operational Support",
  ],
  "Brand Setup - Legal Terms": ["Brand Setup - Legal Terms", "Legal Terms"],
  "Brand Setup - Sustainability & ESG": ["Brand Setup - Sustainability & ESG"],
});

export const CHILD_TO_BASICS_LINK_FIELDS = Object.freeze([
  "Brand",
  "Brand_Basic_ID",
  "Brand Setup - Brand Basics",
  "Brand Basics",
]);

const REPORTS_DIR = path.join(ROOT, "reports", "brand-explorer");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");
export const REPORT_JSON = "brand-setup-child-table-validation-62-readonly.json";
export const REPORT_MD = "brand-setup-child-table-validation-62-readonly.md";
export const DOCS_MD = "brand-setup-child-table-validation-62-readonly.md";

const HELD_PROBE_FALLBACKS = Object.freeze([
  {
    slug: "four-points-flex-by-sheraton",
    recordId: "recgaMzDn2GKkpUsi",
    name: "Four Points Flex by Sheraton",
  },
  {
    slug: "the-house-of-originals",
    recordId: null,
    name: "The House of Originals",
  },
  {
    slug: "morgans-originals",
    recordId: null,
    name: "Morgans Originals",
  },
  {
    slug: "radisson-collection",
    recordId: "rec2DDyPu38C6zDBC",
    name: "Radisson Collection",
  },
]);

function nz(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return v.length ? String(v[0] ?? "").trim() : "";
  return String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function firstRecIds(val) {
  if (!val) return [];
  if (Array.isArray(val)) {
    return val
      .map((x) => (typeof x === "string" ? x : x?.id))
      .filter((id) => typeof id === "string" && id.startsWith("rec"));
  }
  if (typeof val === "string" && val.startsWith("rec")) return [val];
  return [];
}

function extractBasicsIds(fields = {}) {
  const ids = new Set();
  for (const name of CHILD_TO_BASICS_LINK_FIELDS) {
    for (const id of firstRecIds(fields[name])) ids.add(id);
  }
  return [...ids];
}

function extractBasicsReverseChildIds(basicsFields = {}, tableName) {
  const names = BASICS_CHILD_LINK_FIELDS[tableName] || [];
  const ids = new Set();
  for (const name of names) {
    for (const id of firstRecIds(basicsFields[name])) ids.add(id);
  }
  return [...ids];
}

function isTextFieldType(type) {
  return (
    type === "singleLineText" ||
    type === "multilineText" ||
    type === "richText" ||
    type === "email" ||
    type === "url" ||
    type === "phoneNumber"
  );
}

function collectTextBlob(fields = {}, textFieldNames = []) {
  const parts = [];
  for (const name of textFieldNames) {
    const v = fields[name];
    if (v == null) continue;
    if (typeof v === "string" && v.trim()) parts.push(`${name}: ${v.trim()}`);
    else if (Array.isArray(v) && v.every((x) => typeof x === "string")) {
      const joined = v.map((x) => x.trim()).filter(Boolean).join(" | ");
      if (joined) parts.push(`${name}: ${joined}`);
    }
  }
  return parts.join("\n");
}

function fingerprintText(text) {
  const norm = String(text || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
  if (norm.length < 80) return null;
  // Simple stable fingerprint without crypto dependency weight
  let h = 0;
  for (let i = 0; i < norm.length; i += 1) h = (h * 31 + norm.charCodeAt(i)) >>> 0;
  return `${norm.length}:${h.toString(16)}`;
}

/** Process/internal leakage that is always high risk on child tables. */
const CHILD_HIGH_FORBIDDEN_RE = Object.freeze(
  EXTRA_FORBIDDEN_RE.filter((r) =>
    [
      "census",
      "census_url",
      "source_pack",
      "source_data",
      "source_capture",
      "pipeline_extraction",
      "factory",
      "stage_process",
      "qa_process",
      "governance",
      "staging",
      "sandbox",
      "overlay",
      "vic",
      "chd",
      "listed_on_choice",
      "consumer_site",
      "metadata",
      "active_property_page",
      "source_supported",
    ].includes(r.id)
  )
);

/**
 * Franchise disclosure / fee shorthand is expected inside Brand Setup form
 * tables (Fee, Deal, Legal, Project Fit). Do not queue as public-language risk.
 * Those terms remain high-risk only on Brand Explorer Presentation.
 */
const CHILD_EXPECTED_SETUP_TERMS = Object.freeze([
  "fdd",
  "item_19",
  "loi",
  "confirm_fees_fdd",
  "adr",
  "revpar",
  "disclosure_document",
  "franchise_disclosure",
]);

function scanChildForbidden(text) {
  const raw = String(text || "");
  const high = [];
  if (!raw.trim()) return { high, medium: [] };
  for (const rule of CHILD_HIGH_FORBIDDEN_RE) {
    if (rule.re.test(raw)) high.push(rule.id);
  }
  return { high, medium: [] };
}

async function metaListTables(baseId, token) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`meta tables ${res.status}: ${JSON.stringify(json.error || json)}`);
  return json.tables || [];
}

async function listAllRecords(baseId, token, tableIdOrName, { fields = null } = {}) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    if (fields?.length) for (const f of fields) params.append("fields[]", f);
    const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableIdOrName)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(`list ${tableIdOrName} ${res.status}: ${JSON.stringify(json.error || json)}`);
    }
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(150);
  } while (offset);
  return out;
}

function readFrozenBaseline() {
  const p = path.join(ROOT, "reports", REPORT_JSON_62);
  if (!fs.existsSync(p)) {
    throw new Error(`Missing freeze artifact reports/${REPORT_JSON_62}`);
  }
  const frozen = JSON.parse(fs.readFileSync(p, "utf8"));
  if (frozen.freezeDecision !== FREEZE_DECISION_62) {
    throw new Error(
      `Freeze decision mismatch: expected ${FREEZE_DECISION_62}, got ${frozen.freezeDecision}`
    );
  }
  if (!frozen.frozen) throw new Error("Freeze artifact frozen=false");
  return frozen;
}

function heldProbesFromFreeze(frozen) {
  const fromFreeze = (frozen.heldExcluded || frozen.excludedNonActive || []).map((e) => ({
    slug: e.slug,
    recordId: e.recordId || null,
    name: e.brandName || e.name || e.slug,
  }));
  const bySlug = new Map(fromFreeze.map((h) => [h.slug, h]));
  for (const h of BASELINE_62_HELD_EXCLUDED) {
    if (!bySlug.has(h.slug)) {
      bySlug.set(h.slug, { slug: h.slug, recordId: h.recordId || null, name: h.name });
    }
  }
  for (const h of HELD_PROBE_FALLBACKS) {
    if (!bySlug.has(h.slug)) bySlug.set(h.slug, h);
  }
  return [...bySlug.values()];
}

/**
 * @returns {Promise<object>} validation report (no writes)
 */
export async function runBrandSetupChildTableValidation62({ token, baseId } = {}) {
  const apiKey = token || process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const bid = baseId || process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !bid) throw new Error("Set AIRTABLE_API_KEY (or AIRTABLE_PAT) and AIRTABLE_BASE_ID");

  const frozen = readFrozenBaseline();
  const liveUniverse = await loadActiveUniverse({ includeBrandApi: false });
  if (liveUniverse.totalCount !== EXPECTED_ACTIVE_COUNT_62) {
    throw new Error(
      `Live Active universe ${liveUniverse.totalCount} ≠ expected ${EXPECTED_ACTIVE_COUNT_62}`
    );
  }
  if ((frozen.brands || []).length !== EXPECTED_ACTIVE_COUNT_62) {
    throw new Error(`Frozen brand count ${(frozen.brands || []).length} ≠ ${EXPECTED_ACTIVE_COUNT_62}`);
  }

  const activeById = new Map();
  const activeBySlug = new Map();
  for (const b of frozen.brands) {
    const row = {
      slug: b.slug,
      recordId: b.recordId,
      name: b.brandName || b.name,
      brandStatus: b.brandStatus,
      parentCompany: null,
      basicsFields: null,
    };
    activeById.set(b.recordId, row);
    activeBySlug.set(b.slug, row);
  }

  const heldProbes = heldProbesFromFreeze(frozen);
  const heldIds = new Set(heldProbes.map((h) => h.recordId).filter(Boolean));

  const tables = await metaListTables(bid, apiKey);
  const tableByName = new Map(tables.map((t) => [t.name, t]));
  for (const name of CHILD_TABLES) {
    if (!tableByName.has(name)) throw new Error(`Missing child table in base meta: ${name}`);
  }
  if (!tableByName.has(BASICS_TABLE)) throw new Error(`Missing ${BASICS_TABLE}`);

  // Load Active Basics (+ held probes with recordIds)
  const basicsMeta = tableByName.get(BASICS_TABLE);
  const basicsLinkFieldNames = [
    ...new Set(Object.values(BASICS_CHILD_LINK_FIELDS).flat()),
    "Brand Name",
    "Brand Status",
    "Parent Company",
  ];
  const basicsRecords = await listAllRecords(bid, apiKey, basicsMeta.id, {
    fields: basicsLinkFieldNames.filter((f) =>
      (basicsMeta.fields || []).some((mf) => mf.name === f)
    ),
  });
  const basicsById = new Map(basicsRecords.map((r) => [r.id, r]));

  for (const row of activeById.values()) {
    const rec = basicsById.get(row.recordId);
    if (!rec) {
      row.basicsMissing = true;
      continue;
    }
    row.basicsFields = rec.fields || {};
    row.parentCompany = nz(rec.fields?.["Parent Company"]);
    row.name = nz(rec.fields?.["Brand Name"]) || row.name;
    row.brandStatus = nz(rec.fields?.["Brand Status"]) || row.brandStatus;
  }

  const coverageMatrix = [];
  const missingRecords = [];
  const duplicateRecords = [];
  const orphanRecords = [];
  const staleRecords = [];
  const publicLanguageRisks = [];
  const mismatchFindings = [];
  const remediationQueue = [];
  const tableSummaries = [];

  const pushRemediation = (item) => {
    remediationQueue.push({
      id: `rem-${remediationQueue.length + 1}`,
      priority: item.priority || "medium",
      category: item.category,
      table: item.table || null,
      brandSlug: item.brandSlug || null,
      brandRecordId: item.brandRecordId || null,
      childRecordId: item.childRecordId || null,
      summary: item.summary,
      detail: item.detail || null,
    });
  };

  for (const tableName of CHILD_TABLES) {
    const meta = tableByName.get(tableName);
    const textFields = (meta.fields || [])
      .filter((f) => isTextFieldType(f.type) && f.name !== "Brand Name")
      .map((f) => f.name);
    const brandNameFieldExists = (meta.fields || []).some((f) => f.name === "Brand Name");
    const fetchFields = [
      ...CHILD_TO_BASICS_LINK_FIELDS.filter((f) =>
        (meta.fields || []).some((mf) => mf.name === f)
      ),
      ...(brandNameFieldExists ? ["Brand Name"] : []),
      ...textFields.slice(0, 40),
    ];

    const records = await listAllRecords(bid, apiKey, meta.id, {
      fields: [...new Set(fetchFields)],
    });

    /** @type {Map<string, object[]>} */
    const byBrandId = new Map();
    const fingerprints = new Map(); // fp -> [{brandId, recordId}]
    let orphanCount = 0;
    let staleCount = 0;
    let activeLinkedCount = 0;
    let languageHitCount = 0;

    for (const rec of records) {
      const fields = rec.fields || {};
      const linkedIds = extractBasicsIds(fields);
      const brandNameText = nz(fields["Brand Name"]);

      if (!linkedIds.length) {
        orphanCount += 1;
        orphanRecords.push({
          table: tableName,
          recordId: rec.id,
          reason: "no_brand_basics_link",
          brandNameText: brandNameText || null,
        });
        pushRemediation({
          priority: "high",
          category: "orphan",
          table: tableName,
          childRecordId: rec.id,
          summary: `${tableName}: orphan child record (no Brand Basics link)`,
          detail: brandNameText || null,
        });
        continue;
      }

      // Multi-link is unusual for 1:1 — flag
      if (linkedIds.length > 1) {
        mismatchFindings.push({
          type: "multi_brand_link",
          table: tableName,
          recordId: rec.id,
          linkedBasicsIds: linkedIds,
        });
        pushRemediation({
          priority: "high",
          category: "mismatch",
          table: tableName,
          childRecordId: rec.id,
          summary: `${tableName}: child record links to multiple Brand Basics ids`,
          detail: linkedIds.join(","),
        });
      }

      for (const brandId of linkedIds) {
        if (!byBrandId.has(brandId)) byBrandId.set(brandId, []);
        byBrandId.get(brandId).push(rec);

        const active = activeById.get(brandId);
        const held = heldIds.has(brandId);
        const basics = basicsById.get(brandId);
        const basicsStatus = nz(basics?.fields?.["Brand Status"]);

        if (active) {
          activeLinkedCount += 1;
          const expectedName = nz(active.name);
          if (brandNameText && expectedName && brandNameText !== expectedName) {
            // Soft mismatch — may be alias; still report for review
            mismatchFindings.push({
              type: "brand_name_text_mismatch",
              table: tableName,
              recordId: rec.id,
              brandSlug: active.slug,
              brandRecordId: brandId,
              childBrandName: brandNameText,
              basicsBrandName: expectedName,
            });
            pushRemediation({
              priority: "medium",
              category: "mismatch",
              table: tableName,
              brandSlug: active.slug,
              brandRecordId: brandId,
              childRecordId: rec.id,
              summary: `${active.slug}: Brand Name text on child ≠ Basics`,
              detail: `"${brandNameText}" vs "${expectedName}"`,
            });
          }

          const blob = collectTextBlob(fields, textFields);
          if (blob) {
            // Child Brand Setup tables are form/setup data — do not apply full
            // BE Presentation PVQL owner-facing scan (FDD/LOI/Item 19 are expected).
            const childHits = scanChildForbidden(blob);
            const highHits = [...childHits.high];
            const mediumHits = [...childHits.medium];
            if (highHits.length || mediumHits.length) {
              languageHitCount += 1;
              publicLanguageRisks.push({
                table: tableName,
                recordId: rec.id,
                brandSlug: active.slug,
                brandRecordId: brandId,
                severity: highHits.length ? "high" : "medium",
                highHits,
                mediumHits,
                snippet: blob.slice(0, 240),
              });
              pushRemediation({
                priority: highHits.length ? "high" : "medium",
                category: "public_language_risk",
                table: tableName,
                brandSlug: active.slug,
                brandRecordId: brandId,
                childRecordId: rec.id,
                summary: `${active.slug}: process/internal language in ${tableName}`,
                detail: [...highHits, ...mediumHits].join("|"),
              });
            }

            const fp = fingerprintText(blob);
            if (fp) {
              if (!fingerprints.has(fp)) fingerprints.set(fp, []);
              fingerprints.get(fp).push({ brandId, recordId: rec.id, slug: active.slug });
            }
          }
        } else if (held || (basics && !isBrandStatusActive(basicsStatus))) {
          staleCount += 1;
          const staleRow = {
            table: tableName,
            recordId: rec.id,
            brandRecordId: brandId,
            brandStatus: basicsStatus || null,
            heldProbe: held,
            brandNameText: brandNameText || nz(basics?.fields?.["Brand Name"]) || null,
            reason: held
              ? "linked_to_held_or_excluded_brand"
              : "linked_to_non_active_live_brand",
          };
          staleRecords.push(staleRow);
          // Only queue held/excluded probe links — bulk Under Review inventory is summarized.
          if (held) {
            pushRemediation({
              priority: "medium",
              category: "stale_held_probe",
              table: tableName,
              brandRecordId: brandId,
              childRecordId: rec.id,
              summary: `${tableName}: child linked to held/excluded brand probe`,
              detail: basicsStatus || "held_probe",
            });
          }
        } else if (!basics) {
          orphanCount += 1;
          orphanRecords.push({
            table: tableName,
            recordId: rec.id,
            reason: "broken_brand_basics_link",
            linkedBasicsId: brandId,
            brandNameText: brandNameText || null,
          });
          pushRemediation({
            priority: "high",
            category: "orphan",
            table: tableName,
            childRecordId: rec.id,
            summary: `${tableName}: broken Brand Basics link (${brandId})`,
          });
        }
      }
    }

    // Cross-brand identical content
    for (const [fp, rows] of fingerprints.entries()) {
      const brands = [...new Set(rows.map((r) => r.brandId))];
      if (brands.length < 2) continue;
      mismatchFindings.push({
        type: "copied_or_shared_content",
        table: tableName,
        fingerprint: fp,
        brandSlugs: [...new Set(rows.map((r) => r.slug))],
        recordIds: rows.map((r) => r.recordId),
        note: "May be shared franchise template; review before rewrite",
      });
      pushRemediation({
        priority: "medium",
        category: "copied_content",
        table: tableName,
        summary: `${tableName}: identical text blob shared across brands (possible template)`,
        detail: [...new Set(rows.map((r) => r.slug))].join(", "),
      });
    }

    // Per-brand coverage + reverse-link consistency
    for (const brand of activeById.values()) {
      const fromChild = byBrandId.get(brand.recordId) || [];
      const reverseIds = extractBasicsReverseChildIds(brand.basicsFields || {}, tableName);
      const reverseRecs = reverseIds
        .map((id) => records.find((r) => r.id === id))
        .filter(Boolean);

      // Union of discovery paths
      const seen = new Set();
      const linked = [];
      for (const r of [...fromChild, ...reverseRecs]) {
        if (!seen.has(r.id)) {
          seen.add(r.id);
          linked.push(r);
        }
      }

      const bidirectionalOk =
        reverseIds.length === 0 ||
        reverseIds.every((id) => fromChild.some((r) => r.id === id)) ||
        fromChild.length === 0;

      if (reverseIds.length && fromChild.length) {
        const childIds = new Set(fromChild.map((r) => r.id));
        const onlyReverse = reverseIds.filter((id) => !childIds.has(id));
        const onlyChild = fromChild.filter((r) => !reverseIds.includes(r.id)).map((r) => r.id);
        if (onlyReverse.length || onlyChild.length) {
          mismatchFindings.push({
            type: "parent_child_link_inconsistency",
            table: tableName,
            brandSlug: brand.slug,
            brandRecordId: brand.recordId,
            basicsReverseLinkIds: reverseIds,
            childToBasicsRecordIds: fromChild.map((r) => r.id),
            onlyOnBasicsReverse: onlyReverse,
            onlyOnChildForward: onlyChild,
          });
          pushRemediation({
            priority: "high",
            category: "link_inconsistency",
            table: tableName,
            brandSlug: brand.slug,
            brandRecordId: brand.recordId,
            summary: `${brand.slug}: Basics↔${tableName} link mismatch`,
            detail: `reverse=${reverseIds.join(",") || "—"} child=${fromChild.map((r) => r.id).join(",") || "—"}`,
          });
        }
      }

      const status =
        linked.length === 0
          ? "missing"
          : linked.length === 1
            ? "ok"
            : "duplicate";

      coverageMatrix.push({
        brandSlug: brand.slug,
        brandRecordId: brand.recordId,
        brandName: brand.name,
        table: tableName,
        required: true,
        linkedCount: linked.length,
        status,
        childRecordIds: linked.map((r) => r.id),
        basicsReverseLinkIds: reverseIds,
        bidirectionalOk,
      });

      if (linked.length === 0) {
        missingRecords.push({
          brandSlug: brand.slug,
          brandRecordId: brand.recordId,
          brandName: brand.name,
          table: tableName,
          reason: "required_child_record_missing",
        });
        pushRemediation({
          priority: "high",
          category: "missing",
          table: tableName,
          brandSlug: brand.slug,
          brandRecordId: brand.recordId,
          summary: `${brand.slug}: missing required ${tableName} record`,
        });
      } else if (linked.length > 1) {
        duplicateRecords.push({
          brandSlug: brand.slug,
          brandRecordId: brand.recordId,
          brandName: brand.name,
          table: tableName,
          recordIds: linked.map((r) => r.id),
          count: linked.length,
        });
        pushRemediation({
          priority: "high",
          category: "duplicate",
          table: tableName,
          brandSlug: brand.slug,
          brandRecordId: brand.recordId,
          summary: `${brand.slug}: duplicate ${tableName} records (${linked.length})`,
          detail: linked.map((r) => r.id).join(","),
        });
      }
    }

    tableSummaries.push({
      table: tableName,
      totalRecords: records.length,
      activeLinkedRecordTouches: activeLinkedCount,
      brandsWithRecord: [...activeById.values()].filter(
        (b) => (byBrandId.get(b.recordId) || []).length > 0
      ).length,
      brandsMissing: missingRecords.filter((m) => m.table === tableName).length,
      brandsDuplicate: duplicateRecords.filter((d) => d.table === tableName).length,
      orphanCount,
      staleCount,
      languageHitCount,
    });
  }

  // Parent-company family consistency across child Brand Name / Basics
  const parentGroups = new Map();
  for (const b of activeById.values()) {
    const pc = nz(b.parentCompany) || "(unspecified)";
    if (!parentGroups.has(pc)) parentGroups.set(pc, []);
    parentGroups.get(pc).push(b.slug);
  }

  const summary = {
    activeUniverse: liveUniverse.totalCount,
    frozenBrandCount: frozen.brands.length,
    freezeDecision: frozen.freezeDecision,
    freezeUnchanged: frozen.freezeDecision === FREEZE_DECISION_62 && frozen.frozen === true,
    childTablesAudited: CHILD_TABLES.length,
    coverageCells: coverageMatrix.length,
    missingCount: missingRecords.length,
    duplicateCount: duplicateRecords.length,
    orphanCount: orphanRecords.length,
    staleCount: staleRecords.length,
    staleHeldProbeCount: staleRecords.filter((s) => s.heldProbe).length,
    staleNonActiveInventoryCount: staleRecords.filter((s) => !s.heldProbe).length,
    publicLanguageRiskCount: publicLanguageRisks.length,
    publicLanguageHighCount: publicLanguageRisks.filter((r) => r.severity === "high").length,
    publicLanguageMediumCount: publicLanguageRisks.filter((r) => r.severity === "medium").length,
    mismatchCount: mismatchFindings.length,
    remediationCount: remediationQueue.length,
    remediationHighCount: remediationQueue.filter((r) => r.priority === "high").length,
    brandsFullyCovered: [...activeBySlug.keys()].filter((slug) =>
      CHILD_TABLES.every((table) => {
        const cell = coverageMatrix.find((c) => c.brandSlug === slug && c.table === table);
        return cell && cell.status === "ok";
      })
    ).length,
  };

  const priorityRank = { high: 0, medium: 1, low: 2 };
  remediationQueue.sort(
    (a, b) => (priorityRank[a.priority] ?? 9) - (priorityRank[b.priority] ?? 9)
  );

  return {
    version: VALIDATION_VERSION,
    generatedAt: new Date().toISOString(),
    status: VALIDATION_STATUS,
    mode: "readonly",
    airtableWrites: false,
    brandExplorerWrites: false,
    brandSetupWrites: false,
    censusWrites: false,
    brandStatusWrites: false,
    releaseFieldWrites: false,
    companyValidatedWrites: false,
    brandVerifiedWrites: false,
    recentMomentumWrites: false,
    freezeDecision: frozen.freezeDecision,
    freezeUnchanged: true,
    activeUniverse: liveUniverse.totalCount,
    excludedFromScope: {
      brands: heldProbes.map((h) => h.slug),
      tables: [PRESENTATION_TABLE, BASICS_TABLE, "Hotel Property Census"],
      note: "Held/excluded brands probed only for stale linkage; not remediation targets.",
    },
    childTables: [...CHILD_TABLES],
    summary,
    tableSummaries,
    coverageMatrix,
    missingRecordReport: missingRecords,
    duplicateOrphanStaleReport: {
      duplicates: duplicateRecords,
      orphans: orphanRecords,
      stale: staleRecords,
      staleSummary: {
        total: staleRecords.length,
        heldProbeLinks: staleRecords.filter((s) => s.heldProbe).length,
        otherNonActiveLiveInventory: staleRecords.filter((s) => !s.heldProbe).length,
        note: "Non-Active/Live inventory is expected at Brand Setup scale; remediation queue only elevates held/excluded probe links.",
      },
    },
    publicLanguageRiskReport: publicLanguageRisks,
    publicLanguagePolicy: {
      note: "Franchise disclosure terms (FDD / LOI / Item 19 / ADR / RevPAR) are expected inside Brand Setup form tables and are not queued. Process leakage (census / factory / staging / QA / source pack / etc.) remains queued.",
      expectedSetupTermsNotQueued: [...CHILD_EXPECTED_SETUP_TERMS],
      highProcessTerms: CHILD_HIGH_FORBIDDEN_RE.map((r) => r.id),
    },
    mismatchReport: mismatchFindings,
    remediationQueue,
    parentCompanyGroups: [...parentGroups.entries()].map(([parentCompany, slugs]) => ({
      parentCompany,
      brandCount: slugs.length,
      slugs,
    })),
    confirmations: {
      activeUniverseRemains62: liveUniverse.totalCount === 62,
      frozenBaselineUnchanged: frozen.freezeDecision === FREEZE_DECISION_62,
      noBrandExplorerWrites: true,
      noBrandSetupWrites: true,
      noHotelPropertyCensusWrites: true,
      noBrandStatusChanges: true,
      noReleaseFieldChanges: true,
      noCompanyValidatedOrBrandVerifiedWrites: true,
    },
  };
}

export function writeBrandSetupChildTableValidationReports(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, REPORT_JSON);
  const mdPath = path.join(REPORTS_DIR, REPORT_MD);
  const docsPath = path.join(DOCS_DIR, DOCS_MD);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  const md = renderMarkdown(report);
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");
  return { jsonPath, mdPath, docsPath };
}

function renderMarkdown(report) {
  const s = report.summary || {};
  const lines = [];
  lines.push("# Brand Setup Child-Table Validation — Active-62 (Read-Only)");
  lines.push("");
  lines.push(`**Status:** \`${report.status}\``);
  lines.push(`**Generated:** ${report.generatedAt}`);
  lines.push(`**Freeze:** \`${report.freezeDecision}\` · unchanged=${report.freezeUnchanged}`);
  lines.push(`**Mode:** read-only · Airtable writes=${report.airtableWrites}`);
  lines.push("");
  lines.push("## Verdict");
  lines.push("");
  lines.push(
    `Audited **${s.childTablesAudited}** Brand Setup child tables × **${s.frozenBrandCount}** Active-62 brands. Fully covered brands: **${s.brandsFullyCovered}/${s.frozenBrandCount}**. Remediation queue: **${s.remediationCount}** (high=${s.remediationHighCount ?? "—"}; missing=${s.missingCount}, duplicate=${s.duplicateCount}, orphan=${s.orphanCount}, held-stale=${s.staleHeldProbeCount ?? 0}, language high/med=${s.publicLanguageHighCount ?? "—"}/${s.publicLanguageMediumCount ?? "—"}, mismatch=${s.mismatchCount}). Non-Active inventory rows observed=${s.staleNonActiveInventoryCount ?? s.staleCount} (reported, not auto-queued).`
  );
  lines.push("");
  lines.push("## Scope");
  lines.push("");
  lines.push("- Active-62 only (frozen quality-clean baseline)");
  lines.push("- Child tables only (not Presentation, not Basics parent, not Census)");
  lines.push(`- Excluded brand probes: ${(report.excludedFromScope?.brands || []).join(", ")}`);
  lines.push("");
  lines.push("## Table summary");
  lines.push("");
  lines.push(
    "| Table | Records | Brands w/ row | Missing | Duplicate | Orphan | Stale | Language |"
  );
  lines.push("|------|--------:|-------------:|--------:|----------:|-------:|------:|---------:|");
  for (const t of report.tableSummaries || []) {
    lines.push(
      `| ${t.table} | ${t.totalRecords} | ${t.brandsWithRecord} | ${t.brandsMissing} | ${t.brandsDuplicate} | ${t.orphanCount} | ${t.staleCount} | ${t.languageHitCount} |`
    );
  }
  lines.push("");
  lines.push("## Confirmations");
  lines.push("");
  for (const [k, v] of Object.entries(report.confirmations || {})) {
    lines.push(`- \`${k}\`: **${v}**`);
  }
  lines.push("");
  lines.push("## Remediation queue (top 40)");
  lines.push("");
  for (const item of (report.remediationQueue || []).slice(0, 40)) {
    lines.push(
      `- **[${item.priority}]** ${item.category} · ${item.summary}${item.detail ? ` — ${item.detail}` : ""}`
    );
  }
  if ((report.remediationQueue || []).length > 40) {
    lines.push(`- … +${report.remediationQueue.length - 40} more (see JSON)`);
  }
  lines.push("");
  lines.push("## Outputs");
  lines.push("");
  lines.push(`- Coverage matrix: ${s.coverageCells} cells in JSON \`coverageMatrix\``);
  lines.push("- Missing / duplicate / orphan / stale / language / mismatch reports embedded in JSON");
  lines.push("");
  lines.push(`**Final status:** \`${report.status}\``);
  lines.push("");
  return `${lines.join("\n")}\n`;
}
