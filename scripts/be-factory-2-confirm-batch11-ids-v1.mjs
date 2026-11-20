#!/usr/bin/env node
/**
 * Batch 11 — live Brand Basics reconcile.
 * Part A: 5 EXISTING founder IDs (Choice / extended-stay).
 * Part B: 15 NEW-brand candidates — search live inventory; reuse if found;
 * never invent rec… IDs. Substitute if already completed/active on queue.
 */
import { config as loadEnv } from "dotenv";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";
import { fetchPresentationBlocksForBrand } from "../lib/brand-explorer/be-presentation-writeback-apply-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
loadEnv({ path: join(ROOT, ".env") });

const PART_A = [
  {
    key: "radisson-blu",
    brandName: "Radisson Blu by Choice",
    parentCompany: "Choice Hotels",
    expectedId: "recWPEvxBQxVVzSq3",
    aliases: ["Radisson Blu by Choice", "Radisson Blu", "Radisson BLU"],
    part: "A",
  },
  {
    key: "comfort-inn-suites",
    brandName: "Comfort Inn & Suites",
    parentCompany: "Choice Hotels",
    expectedId: "recOzH5iAE1xEjyD0",
    aliases: ["Comfort Inn & Suites", "Comfort Inn and Suites", "Comfort Inn"],
    part: "A",
  },
  {
    key: "radisson-individuals",
    brandName: "Radisson Individuals by Choice",
    parentCompany: "Choice Hotels",
    expectedId: "recRyvM8OmLlDj9G7",
    aliases: ["Radisson Individuals by Choice", "Radisson Individuals", "Radisson Individual"],
    part: "A",
  },
  {
    key: "country-inn-suites",
    brandName: "Country Inn & Suites by Choice",
    parentCompany: "Choice Hotels",
    expectedId: "recaayt9u7YYg8h7Y",
    aliases: ["Country Inn & Suites by Choice", "Country Inn & Suites", "Country Inn and Suites"],
    part: "A",
  },
  {
    key: "woodspring-suites",
    brandName: "WoodSpring Suites",
    parentCompany: "Choice Hotels",
    expectedId: "recsOd51NzRPYsMko",
    aliases: ["WoodSpring Suites", "WoodSpring", "Wood Spring Suites"],
    part: "A",
  },
];

const PART_B = [
  {
    key: "cambria-hotels",
    brandName: "Cambria Hotels",
    parentCompany: "Choice Hotels",
    aliases: ["Cambria Hotels", "Cambria", "Cambria Suites"],
    part: "B",
  },
  {
    key: "clarion-pointe",
    brandName: "Clarion Pointe",
    parentCompany: "Choice Hotels",
    aliases: ["Clarion Pointe", "Clarion Point"],
    part: "B",
  },
  {
    key: "clarion",
    brandName: "Clarion",
    parentCompany: "Choice Hotels",
    aliases: ["Clarion", "Clarion Hotels", "Clarion Inn"],
    part: "B",
  },
  {
    key: "wyndham-grand",
    brandName: "Wyndham Grand",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["Wyndham Grand", "Wyndham Grand Hotels"],
    part: "B",
  },
  {
    key: "dolce",
    brandName: "Dolce Hotels & Resorts by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["Dolce Hotels & Resorts by Wyndham", "Dolce by Wyndham", "Dolce Hotels", "Dolce"],
    part: "B",
  },
  {
    key: "tryp",
    brandName: "TRYP by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["TRYP by Wyndham", "TRYP", "Tryp by Wyndham"],
    part: "B",
  },
  {
    key: "esplendor",
    brandName: "Esplendor by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["Esplendor by Wyndham", "Esplendor", "Esplendor Boutique Hotels"],
    part: "B",
  },
  {
    key: "registry-collection",
    brandName: "Registry Collection Hotels",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["Registry Collection Hotels", "Registry Collection", "Registry Collection by Wyndham"],
    part: "B",
  },
  {
    key: "caption-by-hyatt",
    brandName: "Caption by Hyatt",
    parentCompany: "Hyatt",
    aliases: ["Caption by Hyatt", "Caption"],
    part: "B",
  },
  {
    key: "hyatt-studios",
    brandName: "Hyatt Studios",
    parentCompany: "Hyatt",
    aliases: ["Hyatt Studios", "Hyatt Studio"],
    part: "B",
  },
  {
    key: "jdv-by-hyatt",
    brandName: "JdV by Hyatt",
    parentCompany: "Hyatt",
    aliases: ["JdV by Hyatt", "JdV", "Joie de Vivre", "Joie de Vivre by Hyatt"],
    part: "B",
  },
  {
    key: "unbound-collection",
    brandName: "The Unbound Collection by Hyatt",
    parentCompany: "Hyatt",
    aliases: ["The Unbound Collection by Hyatt", "Unbound Collection", "Unbound Collection by Hyatt"],
    part: "B",
  },
  {
    key: "hyatt-select",
    brandName: "Hyatt Select",
    parentCompany: "Hyatt",
    aliases: ["Hyatt Select"],
    part: "B",
  },
  {
    key: "sonesta-simply-suites",
    brandName: "Sonesta Simply Suites",
    parentCompany: "Sonesta",
    aliases: ["Sonesta Simply Suites", "Simply Suites"],
    part: "B",
  },
  {
    key: "sonesta-essential",
    brandName: "Sonesta Essential Hotels",
    parentCompany: "Sonesta",
    aliases: ["Sonesta Essential Hotels", "Sonesta Essential", "Essential Hotels"],
    part: "B",
  },
];

const DONE_RE = /ready|approved|published|certified|complete/i;
const PRIOR_BATCH_IDS = new Set();

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function norm(s) {
  return nz(s)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function loadPriorBatchIds() {
  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  for (const f of [
    "90-batch9-identity-confirm.json",
    "100-batch10-identity-confirm.json",
  ]) {
    const p = join(dir, f);
    if (!existsSync(p)) continue;
    try {
      const j = JSON.parse(readFileSync(p, "utf8"));
      for (const r of j.rows || []) {
        if (r.brandId) PRIOR_BATCH_IDS.add(r.brandId);
        if (r.expectedId) PRIOR_BATCH_IDS.add(r.expectedId);
      }
    } catch {
      /* ignore */
    }
  }
}

async function loadAirtableBasics() {
  const Airtable = (await import("airtable")).default;
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("AIRTABLE_API_KEY / AIRTABLE_BASE_ID required");
  const base = new Airtable({ apiKey: key }).base(baseId);
  const rows = [];
  await base("tbl1x6S7I7JwTcRdV")
    .select({ pageSize: 100, fields: ["Brand Name", "Parent Company", "Brand Status"] })
    .eachPage((records, next) => {
      for (const r of records) {
        rows.push({
          id: r.id,
          name: nz(r.get("Brand Name")),
          parentCompany: nz(r.get("Parent Company")),
          status: nz(r.get("Brand Status")),
        });
      }
      next();
    });
  return rows;
}

function loadQueue() {
  const p = join(ROOT, "data/brand-explorer/factory-2-0/queue.json");
  if (!existsSync(p)) return { brands: [] };
  try {
    return JSON.parse(readFileSync(p, "utf8"));
  } catch {
    return { brands: [] };
  }
}

function queueStatus(queue, brandId) {
  const brands = Array.isArray(queue.brands) ? queue.brands : Array.isArray(queue) ? queue : [];
  const hit = brands.find((b) => b.brandId === brandId || b.id === brandId);
  return hit ? nz(hit.status || hit.factoryStatus || hit.stage) : "";
}

function findByAliases(byName, aliases) {
  for (const a of aliases) {
    const hit = byName.get(norm(a));
    if (hit) return hit;
  }
  // fuzzy: all alias tokens present
  for (const [n, row] of byName) {
    for (const a of aliases) {
      const toks = norm(a).split(" ").filter((t) => t.length > 2);
      if (toks.length && toks.every((t) => n.includes(t))) return row;
    }
  }
  return null;
}

async function main() {
  loadPriorBatchIds();
  const basics = await loadAirtableBasics();
  const byId = new Map(basics.map((b) => [b.id, b]));
  const byName = new Map();
  for (const b of basics) byName.set(norm(b.name), b);
  const queue = loadQueue();
  const rows = [];
  const substitutions = [];
  const usedIds = new Set();
  const creationPlan = [];

  const targets = [...PART_A, ...PART_B];

  for (const t of targets) {
    let live = t.expectedId ? byId.get(t.expectedId) || null : null;
    let brandId = t.expectedId || null;
    let match = t.expectedId ? "expected_id" : "search";
    let classification = "EXISTING_ACTIVE";

    if (!live) {
      live = findByAliases(byName, t.aliases);
      if (live) {
        brandId = live.id;
        match = t.expectedId ? "alias_fallback" : "alias_found";
      }
    }

    if (!live) {
      // Truly new — stage creation plan; do not invent ID
      classification = "NEW_BRAND_CANDIDATE";
      match = "not_in_airtable";
      creationPlan.push({
        key: t.key,
        brandName: t.brandName,
        parentCompany: t.parentCompany,
        proposedAction: "CREATE_BRAND_BASICS_AFTER_FOUNDER_AUTH",
        note: "No live Brand Basics match. Stage research against provisional slug; do not invent rec ID.",
      });
      rows.push({
        key: t.key,
        part: t.part,
        expectedName: t.brandName,
        expectedId: t.expectedId || null,
        brandId: null,
        liveName: null,
        parentCompany: t.parentCompany,
        brandStatus: null,
        queueStatus: null,
        status: "NEW_BRAND_CANDIDATE",
        classification,
        presentationBlockCount: 0,
        match,
      });
      continue;
    }

    if (PRIOR_BATCH_IDS.has(brandId) || usedIds.has(brandId)) {
      // Already in prior factory batch — retain state, attempt substitute for cohort size
      const qStatus = queueStatus(queue, brandId);
      if (/published|approved/i.test(qStatus) || PRIOR_BATCH_IDS.has(brandId)) {
        const sub = basics.find(
          (b) =>
            !usedIds.has(b.id) &&
            !PRIOR_BATCH_IDS.has(b.id) &&
            !targets.some((x) => x.expectedId === b.id) &&
            /active|live/i.test(b.status) &&
            !DONE_RE.test(queueStatus(queue, b.id))
        );
        if (sub && t.part === "B") {
          substitutions.push({
            from: t.brandName,
            fromId: brandId,
            to: sub.name,
            toId: sub.id,
            reason: PRIOR_BATCH_IDS.has(brandId) ? "already_in_prior_factory_batch" : `queue=${qStatus}`,
          });
          live = sub;
          brandId = sub.id;
          match = "substitute";
        }
      }
    }

    usedIds.add(brandId);
    let presentationBlockCount = 0;
    try {
      const livePres = await fetchPresentationBlocksForBrand(brandId, live.name);
      presentationBlockCount = Array.isArray(livePres?.blocks) ? livePres.blocks.length : 0;
    } catch {
      presentationBlockCount = -1;
    }

    const isActive = /active|live/i.test(live.status);
    rows.push({
      key: t.key,
      part: t.part,
      expectedName: t.brandName,
      expectedId: t.expectedId || null,
      brandId,
      liveName: live.name,
      parentCompany: live.parentCompany || t.parentCompany,
      brandStatus: live.status,
      queueStatus: queueStatus(queue, brandId) || null,
      status: isActive ? "EXISTING_ACTIVE" : `EXISTING_${live.status || "UNKNOWN"}`,
      classification: isActive ? "EXISTING_ACTIVE" : "EXISTING_NON_ACTIVE",
      presentationBlockCount,
      match,
    });
  }

  const out = {
    contract: "BE_FACTORY_2_0_BATCH_11_IDENTITY_CONFIRM_V1",
    generatedAt: new Date().toISOString(),
    ok: rows.filter((r) => r.part === "A").every((r) => r.brandId),
    newBrandBasicsCreatesProposed: creationPlan.length,
    totalTargets: targets.length,
    partA: rows.filter((r) => r.part === "A").length,
    partBExisting: rows.filter((r) => r.part === "B" && r.brandId).length,
    partBNewCandidates: creationPlan.length,
    existingActive: rows.filter((r) => r.status === "EXISTING_ACTIVE").length,
    missing: rows.filter((r) => r.status === "NEW_BRAND_CANDIDATE").length,
    substitutions,
    creationPlan,
    rows,
  };

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "110-batch11-identity-confirm.json"), JSON.stringify(out, null, 2));
  writeFileSync(
    join(dir, "110-batch11-identity-confirm.md"),
    [
      `# Batch 11 — Identity Confirm`,
      ``,
      `Generated: ${out.generatedAt}`,
      `Part A existing: ${out.partA}`,
      `Part B found in Airtable: ${out.partBExisting}`,
      `Part B NEW candidates (no rec ID): ${out.partBNewCandidates}`,
      `Substitutions: ${substitutions.length}`,
      ``,
      `| Part | Brand | Expected ID | Live ID | Classification | Presentation | Match |`,
      `|---|---|---|---|---|---:|---|`,
      ...rows.map(
        (r) =>
          `| ${r.part} | ${r.expectedName} | \`${r.expectedId || "—"}\` | \`${r.brandId || "—"}\` | ${r.classification} | ${r.presentationBlockCount} | ${r.match} |`
      ),
      ``,
      `## Creation plan (NEW — no invented IDs)`,
      ...(creationPlan.length
        ? creationPlan.map(
            (c) =>
              `- **${c.brandName}** (${c.parentCompany}) — ${c.proposedAction}. ${c.note}`
          )
        : [`None — all Part B brands already have Brand Basics records.`]),
      ``,
      ...(substitutions.length
        ? [`## Substitutions`, ...substitutions.map((s) => `- ${s.from} → ${s.to} (\`${s.toId}\`) — ${s.reason}`)]
        : [`No substitutions.`]),
    ].join("\n") + "\n"
  );

  console.log(
    JSON.stringify(
      {
        ok: out.ok,
        partA: out.partA,
        partBExisting: out.partBExisting,
        partBNewCandidates: out.partBNewCandidates,
        substitutions: substitutions.length,
        report: "reports/brand-explorer/factory-2-0/110-batch11-identity-confirm.json",
      },
      null,
      2
    )
  );
  if (!out.ok) process.exitCode = 2;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
