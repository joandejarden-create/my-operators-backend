#!/usr/bin/env node
/**
 * Brand Explorer — production Airtable inventory (READ-ONLY).
 *
 * Fetches Brand Basics + Brand Explorer Presentation + Factory Queue.
 * Cross-checks protected 62 Active/Live public-full baseline.
 * Never writes to Airtable.
 *
 * Usage:
 *   node scripts/brand-explorer-production-inventory.mjs
 *   node scripts/brand-explorer-production-inventory.mjs --out-dir reports/data-intelligence
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import { isBrandStatusActive } from "../lib/brand-status-active.js";
import {
  buildKnownSlugByRecordId,
  resolveSlugForActiveBrand,
} from "../lib/partner-intelligence/brand-explorer-active-universe.js";
import { slugifyBrandName } from "../lib/partner-intelligence/brand-explorer-expansion-backlog-planner.js";
import {
  EXPECTED_ACTIVE_COUNT_62,
  BASELINE_62_HELD_EXCLUDED,
  REPORT_JSON_62,
} from "../lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
dotenv.config({ path: path.join(ROOT, ".env") });

const TABLE_BASICS = "Brand Setup - Brand Basics";
const TABLE_PRESENTATION = "Brand Setup - Brand Explorer Presentation";
const TABLE_FACTORY_QUEUE = "Brand Explorer Factory Queue";

const DISPOSITIONS = Object.freeze([
  "COMPLETE / PROTECTED",
  "NEEDS QA",
  "NEEDS REMEDIATION",
  "READY TO BUILD",
  "HOLD / EXCLUDED",
  "NEEDS RESEARCH",
]);

const MAJOR_PARENTS = Object.freeze([
  "Marriott",
  "Hilton",
  "IHG",
  "Choice",
  "Accor",
  "Wyndham",
  "Hyatt",
  "Best Western",
  "Radisson",
  "Other",
]);

function nz(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (Array.isArray(v)) {
    if (!v.length) return "";
    if (typeof v[0] === "string") return v[0].trim();
    if (v[0]?.name) return String(v[0].name).trim();
    return String(v[0]).trim();
  }
  if (typeof v === "object" && v.name) return String(v.name).trim();
  return String(v).trim();
}

function parseArgs(argv) {
  const out = { outDir: path.join(ROOT, "reports", "data-intelligence") };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--out-dir" && argv[i + 1]) {
      out.outDir = path.resolve(ROOT, argv[++i]);
    }
  }
  return out;
}

async function airtableListAll(table, { fields = null, filterByFormula = null } = {}) {
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID || "appvtnDurnMSjINP6";
  if (!key) throw new Error("AIRTABLE_API_KEY missing");

  const records = [];
  let offset = null;
  let pages = 0;
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    if (offset) params.set("offset", offset);
    if (filterByFormula) params.set("filterByFormula", filterByFormula);
    if (fields?.length) for (const f of fields) params.append("fields[]", f);

    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${params}`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${key}`, Accept: "application/json" },
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(
        `Airtable ${table} ${res.status}: ${body?.error?.message || body?.error?.type || res.statusText}`
      );
    }
    records.push(...(body.records || []));
    offset = body.offset || null;
    pages += 1;
    if (pages > 500) throw new Error(`Pagination safety stop on ${table}`);
  } while (offset);

  return records;
}

function normalizeParent(raw) {
  const p = nz(raw);
  if (!p) return "Unknown";
  const lower = p.toLowerCase();
  if (lower.includes("marriott") || lower.includes("starwood")) return "Marriott";
  if (lower.includes("hilton")) return "Hilton";
  if (lower.includes("ihg") || lower.includes("intercontinental hotels")) return "IHG";
  if (lower.includes("choice")) return "Choice";
  if (lower.includes("accor")) return "Accor";
  if (lower.includes("wyndham")) return "Wyndham";
  if (lower.includes("hyatt")) return "Hyatt";
  if (lower.includes("best western")) return "Best Western";
  if (lower.includes("radisson")) return "Radisson / Choice family";
  return p;
}

function parentBucket(raw) {
  const n = normalizeParent(raw);
  if (MAJOR_PARENTS.includes(n)) return n;
  if (n === "Radisson / Choice family") return "Choice";
  if (MAJOR_PARENTS.some((m) => n.toLowerCase().includes(m.toLowerCase()))) {
    return MAJOR_PARENTS.find((m) => n.toLowerCase().includes(m.toLowerCase()));
  }
  return "Other";
}

function loadProtected62() {
  const p = path.join(ROOT, "reports", REPORT_JSON_62);
  if (!fs.existsSync(p)) {
    return { bySlug: new Map(), byRecordId: new Map(), brands: [], path: p, present: false };
  }
  const json = JSON.parse(fs.readFileSync(p, "utf8"));
  const brands = json.brands || [];
  const bySlug = new Map(brands.map((b) => [b.slug, b]));
  const byRecordId = new Map(brands.map((b) => [b.recordId, b]));
  return { bySlug, byRecordId, brands, path: p, present: true, meta: json };
}

function classifyProfileDepth({ presentationRowCount, hasMomentum, hasGallery, hasValueScenarios }) {
  if (presentationRowCount <= 0) return "empty";
  if (presentationRowCount < 25) return "sparse";
  const coreOk = hasMomentum && hasGallery && hasValueScenarios;
  if (presentationRowCount >= 80 && coreOk) return "full";
  if (presentationRowCount >= 80) return "partial";
  return "partial";
}

function isHoldOrExcluded({ brandStatus, slug, heldExcluded, notes }) {
  const status = nz(brandStatus).toLowerCase();
  if (heldExcluded.has(slug)) return true;
  if (status.includes("hold") || status === "excluded" || status === "archived") return true;
  const blob = `${nz(notes)}`.toLowerCase();
  if (/\bhold\b|\bexcluded\b|\bdo not (build|promote)\b/.test(blob)) return true;
  return false;
}

function recommendDisposition(row) {
  // Never re-queue protected-62 brands as net-new factory build.
  if (row.isHoldExcluded) return "HOLD / EXCLUDED";

  if (row.inProtected62Baseline) {
    if (row.isActiveLive && row.profileDepth === "full" && row.qaReleaseGatesAppearSatisfied) {
      return "COMPLETE / PROTECTED";
    }
    if (row.isActiveLive && row.profileDepth === "full") {
      // Full depth but gate signal soft-fail → QA, not rebuild
      return "NEEDS QA";
    }
    if (row.isActiveLive) {
      return "NEEDS REMEDIATION";
    }
    // Status demoted / drifted off Active/Live while still in freeze set
    return "NEEDS QA";
  }

  if (row.isActiveLive && row.profileDepth === "full") {
    return "NEEDS QA";
  }
  if (row.isActiveLive && (row.profileDepth === "partial" || row.profileDepth === "sparse")) {
    return "NEEDS REMEDIATION";
  }
  if (row.isActiveLive && row.profileDepth === "empty") {
    return "READY TO BUILD";
  }

  const unfinished = ["draft", "under review", "queued", "in progress", "factory"].some((s) =>
    nz(row.brandStatus).toLowerCase().includes(s)
  );

  if (row.presentationRowCount >= 80 && unfinished) return "NEEDS QA";
  if (row.presentationRowCount > 0 && unfinished) return "NEEDS REMEDIATION";
  if (row.presentationRowCount === 0 && unfinished) return "READY TO BUILD";
  if (row.presentationRowCount === 0) return "NEEDS RESEARCH";
  if (row.presentationRowCount > 0 && !row.isActiveLive) return "NEEDS QA";
  return "NEEDS RESEARCH";
}

function scanPresentation(rows) {
  const slotKeys = [];
  let activeCount = 0;
  let momentum = false;
  let gallery = 0;
  let valueScenarios = 0;
  let overviewScenarios = 0;
  let provenanceish = false;
  let emptyBody = 0;
  let malformed = 0;

  for (const r of rows) {
    const f = r.fields || {};
    const slot = nz(f["Slot Key"]);
    const body = nz(f.Body);
    const title = nz(f.Title);
    const active = f.Active !== false;
    if (active) activeCount += 1;
    if (slot) slotKeys.push(slot);
    if (!slot) malformed += 1;
    if (!body && !title) emptyBody += 1;

    const sk = slot.toLowerCase();
    if (
      sk.includes("momentum") ||
      sk.includes("openings") ||
      sk === "footprint.recent_momentum" ||
      sk.startsWith("footprint.momentum")
    ) {
      momentum = true;
    }
    if (sk.startsWith("materials.gallery.")) gallery += 1;
    if (sk.startsWith("valueowners.scenario.")) {
      if (body.length >= 40) valueScenarios += 1;
      else valueScenarios += 1;
    }
    if (sk.startsWith("overview.scenario.")) overviewScenarios += 1;
    if (
      sk.includes("source") ||
      sk.includes("provenance") ||
      sk.includes("footnote") ||
      sk.includes("ai_assisted") ||
      sk.includes("last_reviewed")
    ) {
      provenanceish = true;
    }
  }

  return {
    presentationRowCount: rows.length,
    presentationActiveRowCount: activeCount,
    slotKeys,
    hasRecentMomentum: momentum,
    galleryImageSlotCount: gallery,
    hasGalleryImages: gallery > 0,
    valueCreationScenarioCount: valueScenarios,
    hasValueCreationScenarios: valueScenarios >= 4,
    overviewScenarioCount: overviewScenarios,
    hasSourcePackOrProvenanceSlots: provenanceish,
    emptyBodyOrTitleCount: emptyBody,
    malformedSlotCount: malformed,
  };
}

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toCsv(rows, columns) {
  const header = columns.map((c) => csvEscape(c.header)).join(",");
  const lines = rows.map((row) =>
    columns.map((c) => csvEscape(typeof c.value === "function" ? c.value(row) : row[c.value])).join(",")
  );
  return [header, ...lines].join("\n") + "\n";
}

function countBy(rows, keyFn) {
  const m = new Map();
  for (const r of rows) {
    const k = keyFn(r) || "Unknown";
    m.set(k, (m.get(k) || 0) + 1);
  }
  return Object.fromEntries([...m.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])));
}

function factoryQueueOrder(rows) {
  const rank = {
    "READY TO BUILD": 1,
    "NEEDS RESEARCH": 2,
    "NEEDS REMEDIATION": 3,
    "NEEDS QA": 4,
    "HOLD / EXCLUDED": 90,
    "COMPLETE / PROTECTED": 99,
  };
  const parentPriority = {
    Marriott: 1,
    Hilton: 2,
    IHG: 3,
    Choice: 4,
    Accor: 5,
    Wyndham: 6,
    Hyatt: 7,
    "Best Western": 8,
    Other: 50,
    Unknown: 60,
  };

  return [...rows]
    .filter((r) => {
      // Do not put protected-baseline or hold brands back into the build queue.
      if (r.inProtected62Baseline) return false;
      if (["COMPLETE / PROTECTED", "HOLD / EXCLUDED"].includes(r.factoryDisposition)) return false;
      return true;
    })
    .sort((a, b) => {
      const dr = (rank[a.factoryDisposition] || 50) - (rank[b.factoryDisposition] || 50);
      if (dr) return dr;
      const pr =
        (parentPriority[a.parentCompanyBucket] || 40) - (parentPriority[b.parentCompanyBucket] || 40);
      if (pr) return pr;
      // Prefer more presentation depth already started for remediation/QA
      if (a.factoryDisposition !== "READY TO BUILD") {
        const pc = (b.presentationRowCount || 0) - (a.presentationRowCount || 0);
        if (pc) return pc;
      }
      return (a.brandName || "").localeCompare(b.brandName || "");
    })
    .map((r, i) => ({
      queueOrder: i + 1,
      brandName: r.brandName,
      brandSlug: r.brandSlug,
      recordId: r.recordId,
      parentCompany: r.parentCompany,
      parentCompanyBucket: r.parentCompanyBucket,
      brandStatus: r.brandStatus,
      presentationRowCount: r.presentationRowCount,
      profileDepth: r.profileDepth,
      factoryDisposition: r.factoryDisposition,
      inProtected62Baseline: r.inProtected62Baseline,
      isActiveLive: r.isActiveLive,
      reason: r.dispositionReason,
    }));
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer — Production Inventory (read-only)");
  lines.push("");
  lines.push(`> Generated: ${report.generatedAt}`);
  lines.push(`> Airtable base: \`${report.airtable.baseId}\``);
  lines.push(`> Mode: **READ-ONLY** · writesPerformed: \`${report.airtable.writesPerformed}\``);
  lines.push(`> Protected baseline cross-check: ${report.protectedBaseline.version} (${report.protectedBaseline.expectedCount} brands)`);
  lines.push(`> Status: **READY FOR CHATGPT QA**`);
  lines.push("");
  lines.push("## Headline counts");
  lines.push("");
  lines.push(`| Metric | Count |`);
  lines.push(`| --- | ---: |`);
  lines.push(`| Brand Basics records scanned | ${report.counts.brandBasicsTotal} |`);
  lines.push(`| Presentation rows scanned | ${report.counts.presentationRowsTotal} |`);
  lines.push(`| Factory Queue rows (Airtable) | ${report.counts.airtableFactoryQueueRows} |`);
  lines.push(`| Active / Live | ${report.counts.activeLive} |`);
  lines.push(`| In protected 62 baseline | ${report.counts.inProtected62} |`);
  lines.push(`| Protected 62 missing from live Active/Live | ${report.counts.protected62MissingFromActiveLive} |`);
  lines.push(`| Active/Live not in protected 62 | ${report.counts.activeLiveNotInProtected62} |`);
  lines.push(`| Presentation present | ${report.counts.withPresentation} |`);
  lines.push(`| Presentation empty | ${report.counts.withoutPresentation} |`);
  lines.push(`| Profile depth full | ${report.counts.profileDepth.full || 0} |`);
  lines.push(`| Profile depth partial | ${report.counts.profileDepth.partial || 0} |`);
  lines.push(`| Profile depth sparse | ${report.counts.profileDepth.sparse || 0} |`);
  lines.push(`| Profile depth empty | ${report.counts.profileDepth.empty || 0} |`);
  lines.push("");
  lines.push("### By factory disposition");
  lines.push("");
  lines.push(`| Disposition | Count |`);
  lines.push(`| --- | ---: |`);
  for (const d of DISPOSITIONS) {
    lines.push(`| ${d} | ${report.counts.byDisposition[d] || 0} |`);
  }
  lines.push("");
  lines.push("### By Brand Status");
  lines.push("");
  lines.push(`| Brand Status | Count |`);
  lines.push(`| --- | ---: |`);
  for (const [k, v] of Object.entries(report.counts.byStatus)) {
    lines.push(`| ${k} | ${v} |`);
  }
  lines.push("");
  lines.push("### By parent company (bucket)");
  lines.push("");
  lines.push(`| Parent | Total | Active/Live | Protected 62 | COMPLETE/PROTECTED | READY TO BUILD |`);
  lines.push(`| --- | ---: | ---: | ---: | ---: | ---: |`);
  for (const [parent, stats] of Object.entries(report.counts.byParentBucketDetail)) {
    lines.push(
      `| ${parent} | ${stats.total} | ${stats.activeLive} | ${stats.inProtected62} | ${stats.completeProtected} | ${stats.readyToBuild} |`
    );
  }
  lines.push("");
  lines.push("## Protected 62 cross-check");
  lines.push("");
  lines.push(
    `Expected protected Active/Live public-full count: **${report.protectedBaseline.expectedCount}**. Live Active/Live count: **${report.counts.activeLive}**.`
  );
  lines.push("");
  if (report.protectedBaseline.missingFromActiveLive?.length) {
    lines.push("### Protected baseline brands NOT currently Active/Live");
    lines.push("");
    for (const b of report.protectedBaseline.missingFromActiveLive) {
      lines.push(`- \`${b.slug}\` (${b.brandName || "?"}) — live status: \`${b.liveBrandStatus || "NOT_FOUND"}\``);
    }
    lines.push("");
  } else {
    lines.push("All protected 62 baseline record IDs are present in the live Brand Basics pull with Active/Live status (or matched by slug).");
    lines.push("");
  }
  if (report.protectedBaseline.activeLiveNotInBaseline?.length) {
    lines.push("### Active/Live brands NOT in protected 62 (do not treat as protected; may need QA / baseline revision)");
    lines.push("");
    for (const b of report.protectedBaseline.activeLiveNotInBaseline) {
      lines.push(
        `- \`${b.brandSlug}\` — ${b.brandName} — status \`${b.brandStatus}\` — presentation ${b.presentationRowCount} — disposition **${b.factoryDisposition}**`
      );
    }
    lines.push("");
  }
  if (report.protectedBaseline.protectedBaselineDrift?.length) {
    lines.push("### Protected 62 drift (must NOT re-enter net-new factory build queue)");
    lines.push("");
    for (const b of report.protectedBaseline.protectedBaselineDrift) {
      lines.push(
        `- \`${b.brandSlug}\` — ${b.brandName} — live \`${b.brandStatus}\` — depth ${b.profileDepth} (${b.presentationRowCount} rows) — **${b.factoryDisposition}** — ${b.incompleteNotes || ""}`
      );
    }
    lines.push("");
  }
  lines.push("### Held / excluded (governance)");
  lines.push("");
  for (const h of report.heldExcludedGovernance) {
    lines.push(`- \`${h.slug}\` — ${h.name || h.brandName || "?"} — ${h.reason || h.category || "held/excluded"}`);
  }
  lines.push("");
  lines.push("## Incomplete / malformed signals");
  lines.push("");
  const malformed = report.brands.filter(
    (b) =>
      b.malformedSlotCount > 0 ||
      b.obviousIncomplete ||
      (b.isActiveLive && b.profileDepth !== "full") ||
      (b.inProtected62Baseline && !b.qaReleaseGatesAppearSatisfied)
  );
  lines.push(`Flagged brands: **${malformed.length}**`);
  lines.push("");
  lines.push(`| Brand | Slug | Status | Depth | Pres. | Momentum | Gallery | VCS | Disposition | Notes |`);
  lines.push(`| --- | --- | --- | --- | ---: | --- | ---: | --- | --- | --- |`);
  for (const b of malformed.slice(0, 80)) {
    lines.push(
      `| ${b.brandName} | \`${b.brandSlug}\` | ${b.brandStatus} | ${b.profileDepth} | ${b.presentationRowCount} | ${b.hasRecentMomentum} | ${b.galleryImageSlotCount} | ${b.hasValueCreationScenarios} | ${b.factoryDisposition} | ${csvEscape(b.incompleteNotes || "").replace(/\|/g, "/")} |`
    );
  }
  if (malformed.length > 80) lines.push(`| … | ${malformed.length - 80} more | | | | | | | | |`);
  lines.push("");
  lines.push("## Proposed factory queue (ordered)");
  lines.push("");
  lines.push(
    "Protected COMPLETE brands are **excluded** from the build queue. HOLD/EXCLUDED are listed separately and must not re-enter build without governance."
  );
  lines.push("");
  lines.push(`| # | Brand | Parent | Status | Pres. | Disposition |`);
  lines.push(`| ---: | --- | --- | --- | ---: | --- |`);
  for (const q of report.proposedFactoryQueue.slice(0, 120)) {
    lines.push(
      `| ${q.queueOrder} | ${q.brandName} (\`${q.brandSlug}\`) | ${q.parentCompanyBucket} | ${q.brandStatus} | ${q.presentationRowCount} | ${q.factoryDisposition} |`
    );
  }
  if (report.proposedFactoryQueue.length > 120) {
    lines.push(`| … | ${report.proposedFactoryQueue.length - 120} more in JSON/CSV | | | | |`);
  }
  lines.push("");
  lines.push("## Major parent snapshots");
  lines.push("");
  for (const parent of ["Marriott", "Hilton", "IHG", "Choice", "Accor", "Wyndham", "Hyatt", "Best Western"]) {
    const subset = report.brands
      .filter((b) => b.parentCompanyBucket === parent)
      .sort((a, b) => a.brandName.localeCompare(b.brandName));
    lines.push(`### ${parent} (${subset.length})`);
    lines.push("");
    if (!subset.length) {
      lines.push("_No Brand Basics rows mapped to this parent bucket._");
      lines.push("");
      continue;
    }
    lines.push(`| Brand | Status | Active/Live | Protected62 | Pres. | Depth | Disposition |`);
    lines.push(`| --- | --- | --- | --- | ---: | --- | --- |`);
    for (const b of subset) {
      lines.push(
        `| ${b.brandName} | ${b.brandStatus} | ${b.isActiveLive} | ${b.inProtected62Baseline} | ${b.presentationRowCount} | ${b.profileDepth} | ${b.factoryDisposition} |`
      );
    }
    lines.push("");
  }
  lines.push("## Files");
  lines.push("");
  lines.push(`- JSON: \`${report.outputs.json}\``);
  lines.push(`- CSV: \`${report.outputs.csv}\``);
  lines.push(`- Markdown: \`${report.outputs.md}\``);
  lines.push(`- Proposed queue CSV: \`${report.outputs.queueCsv}\``);
  lines.push("");
  lines.push("---");
  lines.push("");
  lines.push("**READY FOR CHATGPT QA**");
  lines.push("");
  return lines.join("\n");
}

async function main() {
  const { outDir } = parseArgs(process.argv.slice(2));
  fs.mkdirSync(outDir, { recursive: true });

  const baseId = process.env.AIRTABLE_BASE_ID || "appvtnDurnMSjINP6";
  const knownById = buildKnownSlugByRecordId();
  const protected62 = loadProtected62();
  const heldExcluded = new Map(
    (BASELINE_62_HELD_EXCLUDED || []).map((h) => [h.slug, h])
  );
  // Explicit known holds from governance docs (only when NOT already in protected 62 freeze)
  if (!heldExcluded.has("four-points-flex-by-sheraton")) {
    heldExcluded.set("four-points-flex-by-sheraton", {
      slug: "four-points-flex-by-sheraton",
      name: "Four Points Flex by Sheraton",
      reason: "Held out of Wave 14 partial promotion; not Active/Live public-full",
      category: "held",
    });
  }
  // Tapestry / Radisson Collection: exclude only when not in the protected freeze.
  // If a brand is in the 62 freeze as Active/Live public-full, do not override to HOLD.

  console.log("[inventory] fetching Brand Basics (read-only)…");
  const basicsRecords = await airtableListAll(TABLE_BASICS, {
    fields: [
      "Brand Name",
      "Brand Status",
      "Parent Company",
      "Original Parent Company",
      "External Display Status",
      "Validation Status",
      "Completion Rate",
      "Source Type",
      "Source Region",
      "Last Reviewed Date",
      "Founder Visual Review Pass",
      "Ready for Active Profile",
      "Active Profile Approved",
      "Brand Website",
      "Internal Notes",
      "Evidence Notes",
      "Explorer Hero Data Source",
      "Explorer Hero Verification",
    ],
  });
  console.log(`[inventory] Brand Basics: ${basicsRecords.length}`);

  console.log("[inventory] fetching Presentation rows (read-only)…");
  const presentationRecords = await airtableListAll(TABLE_PRESENTATION, {
    fields: ["Brand", "Brand Name", "Slot Key", "Title", "Body", "Active", "Sort Order"],
  });
  console.log(`[inventory] Presentation: ${presentationRecords.length}`);

  console.log("[inventory] fetching Factory Queue (read-only)…");
  let factoryQueueRecords = [];
  try {
    factoryQueueRecords = await airtableListAll(TABLE_FACTORY_QUEUE);
    console.log(`[inventory] Factory Queue: ${factoryQueueRecords.length}`);
  } catch (err) {
    console.warn(`[inventory] Factory Queue skipped: ${err.message}`);
  }

  const presentationByBrandId = new Map();
  for (const rec of presentationRecords) {
    const brandIds = rec.fields?.Brand || [];
    const ids = Array.isArray(brandIds) ? brandIds : [brandIds];
    for (const id of ids) {
      if (!id) continue;
      if (!presentationByBrandId.has(id)) presentationByBrandId.set(id, []);
      presentationByBrandId.get(id).push(rec);
    }
  }

  const factoryQueueByBasicsId = new Map();
  const factoryQueueBySlug = new Map();
  for (const fq of factoryQueueRecords) {
    const f = fq.fields || {};
    const basicsId = nz(f["Brand Basics Record ID"]);
    const slug = nz(f["Brand Slug"]);
    const entry = {
      recordId: fq.id,
      factoryStatus: nz(f["Factory Status"]),
      factoryEligible: f["Factory Eligible"],
      priority: f.Priority,
      blockerType: nz(f["Blocker Type"]),
      rowKind: nz(f["Row Kind"]),
      brandSlug: slug,
      brandBasicsRecordId: basicsId,
      notes: nz(f.Notes),
      risk: nz(f.Risk),
    };
    if (basicsId) factoryQueueByBasicsId.set(basicsId, entry);
    if (slug) factoryQueueBySlug.set(slug, entry);
  }

  const brands = [];
  for (const rec of basicsRecords) {
    const f = rec.fields || {};
    const brandName = nz(f["Brand Name"]) || "(unnamed)";
    const brandStatus = nz(f["Brand Status"]) || "(blank)";
    const parentCompany = nz(f["Parent Company"]) || nz(f["Original Parent Company"]) || "";
    const { slug, slugSource } = resolveSlugForActiveBrand({
      recordId: rec.id,
      name: brandName,
      knownById,
    });
    const presRows = presentationByBrandId.get(rec.id) || [];
    const scan = scanPresentation(presRows);
    const isActiveLive = isBrandStatusActive(brandStatus);
    const protectedById = protected62.byRecordId.get(rec.id) || null;
    const protectedBySlug = protected62.bySlug.get(slug) || null;
    const protectedHit = protectedById || null;
    const inProtected62Baseline = Boolean(protectedHit);
    const slugCollidesWithProtected62 =
      Boolean(protectedBySlug) && !protectedById && protectedBySlug.recordId !== rec.id;
    const holdMeta = heldExcluded.get(slug);
    // Never HOLD a brand that is in the protected 62 freeze by record id.
    const isHoldExcluded =
      !inProtected62Baseline &&
      isHoldOrExcluded({
        brandStatus,
        slug,
        heldExcluded,
        notes: `${nz(f["Internal Notes"])} ${nz(f["Evidence Notes"])}`,
      });

    const hasSourcePackOrProvenance =
      scan.hasSourcePackOrProvenanceSlots ||
      Boolean(nz(f["Source Type"])) ||
      Boolean(nz(f["Source Region"])) ||
      Boolean(nz(f["Explorer Hero Data Source"]));

    const profileDepth = classifyProfileDepth({
      presentationRowCount: scan.presentationRowCount,
      hasMomentum: scan.hasRecentMomentum,
      hasGallery: scan.hasGalleryImages,
      hasValueScenarios: scan.hasValueCreationScenarios,
    });

    const qaReleaseGatesAppearSatisfied =
      inProtected62Baseline &&
      isActiveLive &&
      profileDepth === "full" &&
      scan.hasRecentMomentum &&
      scan.hasGalleryImages &&
      scan.hasValueCreationScenarios &&
      hasSourcePackOrProvenance &&
      scan.malformedSlotCount === 0;

    const obviousIncomplete =
      (isActiveLive && scan.presentationRowCount === 0) ||
      (isActiveLive && profileDepth !== "full") ||
      (inProtected62Baseline && !qaReleaseGatesAppearSatisfied) ||
      scan.malformedSlotCount > 0 ||
      (scan.presentationRowCount > 0 && scan.presentationRowCount < 25);

    const incompleteNotes = [];
    if (isActiveLive && scan.presentationRowCount === 0) incompleteNotes.push("active_live_zero_presentation");
    if (isActiveLive && !scan.hasRecentMomentum) incompleteNotes.push("missing_recent_momentum");
    if (isActiveLive && !scan.hasGalleryImages) incompleteNotes.push("missing_gallery");
    if (isActiveLive && !scan.hasValueCreationScenarios) incompleteNotes.push("missing_value_creation_scenarios");
    if (isActiveLive && !hasSourcePackOrProvenance) incompleteNotes.push("missing_source_provenance");
    if (scan.malformedSlotCount > 0) incompleteNotes.push(`malformed_slots:${scan.malformedSlotCount}`);
    if (inProtected62Baseline && profileDepth !== "full") incompleteNotes.push("protected_baseline_not_full_live");
    if (holdMeta && !inProtected62Baseline) incompleteNotes.push(`governance_hold:${holdMeta.reason || holdMeta.category}`);
    if (slugCollidesWithProtected62) {
      incompleteNotes.push(
        `slug_collision_with_protected62_record:${protectedBySlug.recordId}`
      );
    }

    const row = {
      brandName,
      parentCompany,
      parentCompanyNormalized: normalizeParent(parentCompany),
      parentCompanyBucket: parentBucket(parentCompany),
      brandSlug: slug,
      slugSource,
      recordId: rec.id,
      brandStatus,
      isActiveLive,
      isUnderReviewOrDraftOrUnfinished: ["draft", "under review", "queued", "in progress"].some((s) =>
        brandStatus.toLowerCase().includes(s)
      ),
      isHoldExcluded,
      holdReason: holdMeta?.reason || (isHoldExcluded ? "status_or_notes_hold" : null),
      externalDisplayStatus: nz(f["External Display Status"]),
      validationStatus: nz(f["Validation Status"]),
      completionRate: f["Completion Rate"] ?? null,
      lastReviewedDate: nz(f["Last Reviewed Date"]),
      founderVisualReviewPass: Boolean(f["Founder Visual Review Pass"]),
      readyForActiveProfile: Boolean(f["Ready for Active Profile"]),
      activeProfileApproved: Boolean(f["Active Profile Approved"]),
      brandWebsite: nz(f["Brand Website"]),
      sourceType: nz(f["Source Type"]),
      sourceRegion: nz(f["Source Region"]),
      hasPresentationRows: scan.presentationRowCount > 0,
      presentationRowCount: scan.presentationRowCount,
      presentationActiveRowCount: scan.presentationActiveRowCount,
      profileDepth,
      hasSourcePackOrProvenance,
      hasRecentMomentum: scan.hasRecentMomentum,
      hasGalleryImages: scan.hasGalleryImages,
      galleryImageSlotCount: scan.galleryImageSlotCount,
      hasValueCreationScenarios: scan.hasValueCreationScenarios,
      valueCreationScenarioCount: scan.valueCreationScenarioCount,
      overviewScenarioCount: scan.overviewScenarioCount,
      qaReleaseGatesAppearSatisfied,
      inProtected62Baseline,
      slugCollidesWithProtected62,
      protected62CollisionRecordId: slugCollidesWithProtected62 ? protectedBySlug.recordId : null,
      protected62Snapshot: protectedHit
        ? {
            presentationRowCountAtFreeze: protectedHit.presentationRowCount,
            publicFullProfile: protectedHit.publicFullProfile,
            pvqlStatus: protectedHit.pvqlStatus,
            qualityRecommendation: protectedHit.qualityRecommendation,
          }
        : null,
      obviousIncomplete,
      incompleteNotes: incompleteNotes.join("; "),
      malformedSlotCount: scan.malformedSlotCount,
      emptyBodyOrTitleCount: scan.emptyBodyOrTitleCount,
      airtableFactoryQueue: factoryQueueByBasicsId.get(rec.id) || factoryQueueBySlug.get(slug) || null,
    };

    row.factoryDisposition = recommendDisposition(row);
    row.dispositionReason = (() => {
      if (row.factoryDisposition === "COMPLETE / PROTECTED") {
        return "In protected 62 Active/Live public-full baseline with live full profile signals; do not re-queue for factory build.";
      }
      if (row.factoryDisposition === "HOLD / EXCLUDED") {
        return row.holdReason || "Governance hold/exclusion";
      }
      if (row.inProtected62Baseline && !row.isActiveLive) {
        return `Protected-62 baseline drift: live Brand Status is ${row.brandStatus} (not Active/Live). Do not treat as net-new factory build; restore/QA under baseline governance.`;
      }
      if (row.factoryDisposition === "NEEDS REMEDIATION") {
        return incompleteNotes.join("; ") || "Partial/sparse profile needs remediation";
      }
      if (row.factoryDisposition === "NEEDS QA") {
        return row.inProtected62Baseline
          ? "Protected-62 member needing QA (not a net-new factory candidate)."
          : "Has substantial presentation depth but not protected-complete; needs QA before scale/promotion";
      }
      if (row.factoryDisposition === "READY TO BUILD") {
        return "Unfinished status with empty or near-empty presentation; eligible for Tab Factory build";
      }
      return "Insufficient Explorer presence or research needed before factory build";
    })();

    brands.push(row);
  }

  brands.sort((a, b) => {
    const pb = a.parentCompanyBucket.localeCompare(b.parentCompanyBucket);
    if (pb) return pb;
    return a.brandName.localeCompare(b.brandName);
  });

  const activeLive = brands.filter((b) => b.isActiveLive);
  const activeLiveIds = new Set(activeLive.map((b) => b.recordId));
  const activeLiveSlugs = new Set(activeLive.map((b) => b.brandSlug));

  const missingFromActiveLive = [];
  for (const pb of protected62.brands) {
    const live =
      brands.find((b) => b.recordId === pb.recordId) ||
      brands.find((b) => b.brandSlug === pb.slug && b.isActiveLive) ||
      brands.find((b) => b.brandSlug === pb.slug);
    if (!live || !live.isActiveLive) {
      missingFromActiveLive.push({
        slug: pb.slug,
        brandName: pb.brandName,
        recordId: pb.recordId,
        liveBrandStatus: live?.brandStatus || null,
        liveRecordIdMatched: live?.recordId || null,
      });
    }
  }
  const activeLiveNotInBaseline = activeLive.filter((b) => !b.inProtected62Baseline);
  const protectedBaselineDrift = brands.filter(
    (b) =>
      b.inProtected62Baseline &&
      (b.factoryDisposition !== "COMPLETE / PROTECTED" || !b.isActiveLive || b.profileDepth !== "full")
  );

  const byParentBucketDetail = {};
  for (const b of brands) {
    const k = b.parentCompanyBucket;
    if (!byParentBucketDetail[k]) {
      byParentBucketDetail[k] = {
        total: 0,
        activeLive: 0,
        inProtected62: 0,
        completeProtected: 0,
        readyToBuild: 0,
        needsQa: 0,
        needsRemediation: 0,
        holdExcluded: 0,
        needsResearch: 0,
      };
    }
    const s = byParentBucketDetail[k];
    s.total += 1;
    if (b.isActiveLive) s.activeLive += 1;
    if (b.inProtected62Baseline) s.inProtected62 += 1;
    if (b.factoryDisposition === "COMPLETE / PROTECTED") s.completeProtected += 1;
    if (b.factoryDisposition === "READY TO BUILD") s.readyToBuild += 1;
    if (b.factoryDisposition === "NEEDS QA") s.needsQa += 1;
    if (b.factoryDisposition === "NEEDS REMEDIATION") s.needsRemediation += 1;
    if (b.factoryDisposition === "HOLD / EXCLUDED") s.holdExcluded += 1;
    if (b.factoryDisposition === "NEEDS RESEARCH") s.needsResearch += 1;
  }

  const proposedFactoryQueue = factoryQueueOrder(brands);

  const stamp = new Date().toISOString();
  const jsonName = "brand-explorer-production-inventory.json";
  const csvName = "brand-explorer-production-inventory.csv";
  const mdName = "brand-explorer-production-inventory.md";
  const queueCsvName = "brand-explorer-factory-queue-proposed.csv";
  const queueMdName = "brand-explorer-factory-queue-proposed.md";

  const report = {
    version: "brand-explorer-production-inventory-v1",
    generatedAt: stamp,
    status: "READY FOR CHATGPT QA",
    airtable: {
      baseId,
      writesPerformed: false,
      tablesRead: [TABLE_BASICS, TABLE_PRESENTATION, TABLE_FACTORY_QUEUE],
      mode: "read-only",
    },
    protectedBaseline: {
      present: protected62.present,
      version: protected62.meta?.version || "62-active-public-full-baseline-v1",
      expectedCount: EXPECTED_ACTIVE_COUNT_62,
      freezePath: path.relative(ROOT, protected62.path).replace(/\\/g, "/"),
      freezeBrandCount: protected62.brands.length,
      missingFromActiveLive,
      activeLiveNotInBaseline: activeLiveNotInBaseline.map((b) => ({
        brandName: b.brandName,
        brandSlug: b.brandSlug,
        recordId: b.recordId,
        brandStatus: b.brandStatus,
        presentationRowCount: b.presentationRowCount,
        factoryDisposition: b.factoryDisposition,
      })),
      protectedBaselineDrift: protectedBaselineDrift.map((b) => ({
        brandName: b.brandName,
        brandSlug: b.brandSlug,
        recordId: b.recordId,
        brandStatus: b.brandStatus,
        isActiveLive: b.isActiveLive,
        presentationRowCount: b.presentationRowCount,
        profileDepth: b.profileDepth,
        factoryDisposition: b.factoryDisposition,
        incompleteNotes: b.incompleteNotes,
        note: "Was in protected 62 freeze; do not re-queue as net-new factory build.",
      })),
    },
    heldExcludedGovernance: [...heldExcluded.values()],
    counts: {
      brandBasicsTotal: brands.length,
      presentationRowsTotal: presentationRecords.length,
      airtableFactoryQueueRows: factoryQueueRecords.length,
      activeLive: activeLive.length,
      inProtected62: brands.filter((b) => b.inProtected62Baseline).length,
      protected62MissingFromActiveLive: missingFromActiveLive.length,
      activeLiveNotInProtected62: activeLiveNotInBaseline.length,
      withPresentation: brands.filter((b) => b.hasPresentationRows).length,
      withoutPresentation: brands.filter((b) => !b.hasPresentationRows).length,
      profileDepth: countBy(brands, (b) => b.profileDepth),
      byStatus: countBy(brands, (b) => b.brandStatus),
      byDisposition: countBy(brands, (b) => b.factoryDisposition),
      byParentBucket: countBy(brands, (b) => b.parentCompanyBucket),
      byParentBucketDetail,
      byParentCompanyRaw: countBy(brands, (b) => b.parentCompany || "Unknown"),
    },
    proposedFactoryQueue,
    brands,
    outputs: {
      json: path.relative(ROOT, path.join(outDir, jsonName)).replace(/\\/g, "/"),
      csv: path.relative(ROOT, path.join(outDir, csvName)).replace(/\\/g, "/"),
      md: path.relative(ROOT, path.join(outDir, mdName)).replace(/\\/g, "/"),
      queueCsv: path.relative(ROOT, path.join(outDir, queueCsvName)).replace(/\\/g, "/"),
      queueMd: path.relative(ROOT, path.join(outDir, queueMdName)).replace(/\\/g, "/"),
    },
  };

  const csvColumns = [
    { header: "brandName", value: "brandName" },
    { header: "parentCompany", value: "parentCompany" },
    { header: "parentCompanyBucket", value: "parentCompanyBucket" },
    { header: "brandSlug", value: "brandSlug" },
    { header: "recordId", value: "recordId" },
    { header: "brandStatus", value: "brandStatus" },
    { header: "isActiveLive", value: (r) => r.isActiveLive },
    { header: "isUnderReviewOrDraftOrUnfinished", value: (r) => r.isUnderReviewOrDraftOrUnfinished },
    { header: "isHoldExcluded", value: (r) => r.isHoldExcluded },
    { header: "inProtected62Baseline", value: (r) => r.inProtected62Baseline },
    { header: "hasPresentationRows", value: (r) => r.hasPresentationRows },
    { header: "presentationRowCount", value: "presentationRowCount" },
    { header: "profileDepth", value: "profileDepth" },
    { header: "hasSourcePackOrProvenance", value: (r) => r.hasSourcePackOrProvenance },
    { header: "hasRecentMomentum", value: (r) => r.hasRecentMomentum },
    { header: "hasGalleryImages", value: (r) => r.hasGalleryImages },
    { header: "galleryImageSlotCount", value: "galleryImageSlotCount" },
    { header: "hasValueCreationScenarios", value: (r) => r.hasValueCreationScenarios },
    { header: "valueCreationScenarioCount", value: "valueCreationScenarioCount" },
    { header: "qaReleaseGatesAppearSatisfied", value: (r) => r.qaReleaseGatesAppearSatisfied },
    { header: "obviousIncomplete", value: (r) => r.obviousIncomplete },
    { header: "incompleteNotes", value: "incompleteNotes" },
    { header: "factoryDisposition", value: "factoryDisposition" },
    { header: "dispositionReason", value: "dispositionReason" },
    { header: "externalDisplayStatus", value: "externalDisplayStatus" },
    { header: "validationStatus", value: "validationStatus" },
    { header: "brandWebsite", value: "brandWebsite" },
  ];

  const queueCsvColumns = [
    { header: "queueOrder", value: "queueOrder" },
    { header: "brandName", value: "brandName" },
    { header: "brandSlug", value: "brandSlug" },
    { header: "recordId", value: "recordId" },
    { header: "parentCompany", value: "parentCompany" },
    { header: "parentCompanyBucket", value: "parentCompanyBucket" },
    { header: "brandStatus", value: "brandStatus" },
    { header: "presentationRowCount", value: "presentationRowCount" },
    { header: "profileDepth", value: "profileDepth" },
    { header: "factoryDisposition", value: "factoryDisposition" },
    { header: "isActiveLive", value: (r) => r.isActiveLive },
    { header: "inProtected62Baseline", value: (r) => r.inProtected62Baseline },
    { header: "reason", value: "reason" },
  ];

  fs.writeFileSync(path.join(outDir, jsonName), JSON.stringify(report, null, 2) + "\n", "utf8");
  fs.writeFileSync(path.join(outDir, csvName), toCsv(brands, csvColumns), "utf8");
  fs.writeFileSync(path.join(outDir, mdName), buildMarkdown(report), "utf8");
  fs.writeFileSync(path.join(outDir, queueCsvName), toCsv(proposedFactoryQueue, queueCsvColumns), "utf8");

  const queueMd = [
    "# Brand Explorer — Proposed Factory Queue",
    "",
    `> Generated: ${stamp}`,
    `> Source inventory: \`${report.outputs.json}\``,
    `> Protected COMPLETE / PROTECTED brands excluded from build queue.`,
    `> HOLD / EXCLUDED excluded from build queue.`,
    "",
    `Proposed queue size: **${proposedFactoryQueue.length}**`,
    "",
    "| # | Brand | Parent | Status | Pres. | Disposition | Reason |",
    "| ---: | --- | --- | --- | ---: | --- | --- |",
    ...proposedFactoryQueue.map(
      (q) =>
        `| ${q.queueOrder} | ${q.brandName} (\`${q.brandSlug}\`) | ${q.parentCompanyBucket} | ${q.brandStatus} | ${q.presentationRowCount} | ${q.factoryDisposition} | ${String(q.reason || "").replace(/\|/g, "/")} |`
    ),
    "",
    "**READY FOR CHATGPT QA**",
    "",
  ].join("\n");
  fs.writeFileSync(path.join(outDir, queueMdName), queueMd, "utf8");

  console.log("[inventory] wrote:");
  console.log(" ", report.outputs.json);
  console.log(" ", report.outputs.csv);
  console.log(" ", report.outputs.md);
  console.log(" ", report.outputs.queueCsv);
  console.log(" ", report.outputs.queueMd);
  console.log("[inventory] headline:");
  console.log(JSON.stringify(report.counts.byDisposition, null, 2));
  console.log(
    JSON.stringify(
      {
        brandBasicsTotal: report.counts.brandBasicsTotal,
        activeLive: report.counts.activeLive,
        inProtected62: report.counts.inProtected62,
        proposedQueue: proposedFactoryQueue.length,
      },
      null,
      2
    )
  );
  console.log("READY FOR CHATGPT QA");
}

main().catch((err) => {
  console.error("[inventory] FAILED:", err?.stack || err);
  process.exit(1);
});
