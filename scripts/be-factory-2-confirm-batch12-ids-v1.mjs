#!/usr/bin/env node
/**
 * Batch 12 — live Brand Basics reconcile (Hilton / Accor-Ennismore / Choice).
 * Search live inventory; reuse canonical IDs; never invent rec… IDs.
 * Substitute if already completed/active on prior factory batches.
 */
import { config as loadEnv } from "dotenv";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";
import { fetchPresentationBlocksForBrand } from "../lib/brand-explorer/be-presentation-writeback-apply-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
loadEnv({ path: join(ROOT, ".env") });

const TARGETS = [
  // Hilton
  {
    key: "graduate-by-hilton",
    brandName: "Graduate by Hilton",
    parentCompany: "Hilton",
    aliases: ["Graduate by Hilton", "Graduate Hotels", "Graduate"],
    family: "Hilton",
  },
  {
    key: "outset-collection",
    brandName: "Outset Collection by Hilton",
    parentCompany: "Hilton",
    aliases: ["Outset Collection by Hilton", "Outset Collection", "Outset by Hilton", "Outset"],
    family: "Hilton",
  },
  {
    key: "livsmart-studios",
    brandName: "LivSmart Studios by Hilton",
    parentCompany: "Hilton",
    aliases: ["LivSmart Studios by Hilton", "LivSmart Studios", "LivSmart", "Liv Smart Studios"],
    family: "Hilton",
  },
  {
    key: "embassy-suites",
    brandName: "Embassy Suites by Hilton",
    parentCompany: "Hilton",
    aliases: ["Embassy Suites by Hilton", "Embassy Suites", "Embassy Suites Hotels"],
    family: "Hilton",
  },
  {
    key: "nomad-hotels",
    brandName: "NoMad Hotels",
    parentCompany: "Hilton",
    aliases: ["NoMad Hotels", "NoMad", "Nomad Hotels", "NoMad by Hilton"],
    family: "Hilton",
  },
  // Accor / Ennismore
  {
    key: "ibis-styles",
    brandName: "ibis Styles",
    parentCompany: "Accor",
    aliases: ["ibis Styles", "Ibis Styles", "ibis styles"],
    family: "Accor",
  },
  {
    key: "ibis-budget",
    brandName: "ibis budget",
    parentCompany: "Accor",
    aliases: ["ibis budget", "Ibis Budget", "ibis budget hotels"],
    family: "Accor",
  },
  {
    key: "greet",
    brandName: "greet",
    parentCompany: "Accor",
    aliases: ["greet", "Greet", "greet hotels", "Greet by Accor"],
    family: "Accor",
  },
  {
    key: "hotelf1",
    brandName: "hotelF1",
    parentCompany: "Accor",
    aliases: ["hotelF1", "Hotel F1", "hotel F1", "Formule 1", "hotelF1 Accor"],
    family: "Accor",
  },
  {
    key: "tribe",
    brandName: "TRIBE",
    parentCompany: "Accor",
    aliases: ["TRIBE", "Tribe", "TRIBE Hotels", "Tribe by Accor"],
    family: "Accor",
  },
  {
    key: "adagio",
    brandName: "Adagio",
    parentCompany: "Accor",
    aliases: ["Adagio", "Adagio Aparthotel", "adagio"],
    family: "Accor",
  },
  {
    key: "adagio-access",
    brandName: "Adagio Access",
    parentCompany: "Accor",
    aliases: ["Adagio Access", "adagio access", "Adagio access"],
    family: "Accor",
  },
  {
    key: "25hours",
    brandName: "25hours Hotels",
    parentCompany: "Accor / Ennismore",
    aliases: ["25hours Hotels", "25hours", "25 Hours", "25hours Hotel"],
    family: "Accor/Ennismore",
  },
  {
    key: "the-hoxton",
    brandName: "The Hoxton",
    parentCompany: "Accor / Ennismore",
    aliases: ["The Hoxton", "Hoxton", "The Hoxton Hotels"],
    family: "Accor/Ennismore",
  },
  {
    key: "mondrian",
    brandName: "Mondrian",
    parentCompany: "Accor / Ennismore",
    aliases: ["Mondrian", "Mondrian Hotels", "Mondrian Hotel"],
    family: "Accor/Ennismore",
  },
  {
    key: "hyde-hotels",
    brandName: "Hyde Hotels",
    parentCompany: "Accor / Ennismore",
    aliases: ["Hyde Hotels", "Hyde", "Hyde Hotel"],
    family: "Accor/Ennismore",
  },
  {
    key: "sls-hotels",
    brandName: "SLS Hotels",
    parentCompany: "Accor / Ennismore",
    aliases: ["SLS Hotels", "SLS", "SLS Hotel"],
    family: "Accor/Ennismore",
  },
  {
    key: "swissotel",
    brandName: "Swissôtel",
    parentCompany: "Accor",
    aliases: ["Swissôtel", "Swissotel", "Swissôtel Hotels & Resorts", "Swissotel Hotels"],
    family: "Accor",
  },
  // Choice
  {
    key: "park-plaza",
    brandName: "Park Plaza",
    parentCompany: "Choice Hotels",
    aliases: ["Park Plaza", "Park Plaza Hotels", "Park Plaza by Radisson", "Park Plaza Hotels & Resorts"],
    family: "Choice",
  },
  {
    key: "park-inn",
    brandName: "Park Inn by Radisson",
    parentCompany: "Choice Hotels",
    aliases: ["Park Inn by Radisson", "Park Inn", "Park Inn by Choice", "Park Inn Radisson"],
    family: "Choice",
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
    .replace(/ô/g, "o")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function loadPriorBatchIds() {
  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  for (const f of [
    "90-batch9-identity-confirm.json",
    "100-batch10-identity-confirm.json",
    "110-batch11-identity-confirm.json",
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
  // Also harvest Batch 6–10 pack IDs if present
  for (const n of [6, 7, 8, 9, 10, 11]) {
    const packPath = join(
      ROOT,
      `lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-batch${n}-v1.js`
    );
    if (!existsSync(packPath)) continue;
    try {
      const src = readFileSync(packPath, "utf8");
      for (const m of src.matchAll(/"(rec[a-zA-Z0-9]+)"/g)) PRIOR_BATCH_IDS.add(m[1]);
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
  const byName = new Map();
  for (const b of basics) byName.set(norm(b.name), b);
  const queue = loadQueue();
  const rows = [];
  const substitutions = [];
  const usedIds = new Set();
  const creationPlan = [];

  for (const t of TARGETS) {
    let live = findByAliases(byName, t.aliases);
    let brandId = live ? live.id : null;
    let match = live ? "alias_found" : "not_in_airtable";
    let classification = "EXISTING_ACTIVE";

    if (!live) {
      classification = "NEW_BRAND_CANDIDATE";
      creationPlan.push({
        key: t.key,
        brandName: t.brandName,
        parentCompany: t.parentCompany,
        family: t.family,
        proposedAction: "CREATE_BRAND_BASICS_AFTER_FOUNDER_AUTH",
        note: "No live Brand Basics match. Stage research against provisional slug; do not invent rec ID.",
      });
      rows.push({
        key: t.key,
        family: t.family,
        expectedName: t.brandName,
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

    // Avoid matching wrong siblings (e.g. classic ibis for ibis Styles)
    if (t.key === "ibis-styles" && /^ibis$/i.test(live.name) && !/styles/i.test(live.name)) {
      live = basics.find((b) => /ibis\s*styles/i.test(b.name)) || null;
      brandId = live ? live.id : null;
      match = live ? "sibling_disambiguated" : "not_in_airtable";
    }
    if (t.key === "ibis-budget" && /^ibis$/i.test(live.name) && !/budget/i.test(live.name)) {
      live = basics.find((b) => /ibis\s*budget/i.test(b.name)) || null;
      brandId = live ? live.id : null;
      match = live ? "sibling_disambiguated" : "not_in_airtable";
    }
    if (t.key === "adagio" && /access/i.test(live.name)) {
      live = basics.find((b) => /^adagio$/i.test(norm(b.name)) || (/adagio/i.test(b.name) && !/access/i.test(b.name))) || null;
      brandId = live ? live.id : null;
      match = live ? "sibling_disambiguated" : "not_in_airtable";
    }
    if (t.key === "adagio-access" && /adagio/i.test(live.name) && !/access/i.test(live.name)) {
      live = basics.find((b) => /adagio\s*access/i.test(b.name)) || null;
      brandId = live ? live.id : null;
      match = live ? "sibling_disambiguated" : "not_in_airtable";
    }

    if (!live) {
      classification = "NEW_BRAND_CANDIDATE";
      creationPlan.push({
        key: t.key,
        brandName: t.brandName,
        parentCompany: t.parentCompany,
        family: t.family,
        proposedAction: "CREATE_BRAND_BASICS_AFTER_FOUNDER_AUTH",
        note: "No live Brand Basics match after sibling disambiguation. Do not invent rec ID.",
      });
      rows.push({
        key: t.key,
        family: t.family,
        expectedName: t.brandName,
        brandId: null,
        liveName: null,
        parentCompany: t.parentCompany,
        brandStatus: null,
        queueStatus: null,
        status: "NEW_BRAND_CANDIDATE",
        classification,
        presentationBlockCount: 0,
        match: "not_in_airtable",
      });
      continue;
    }

    if (PRIOR_BATCH_IDS.has(brandId) || usedIds.has(brandId)) {
      const qStatus = queueStatus(queue, brandId);
      const sub = basics.find(
        (b) =>
          !usedIds.has(b.id) &&
          !PRIOR_BATCH_IDS.has(b.id) &&
          !TARGETS.some((x) => findByAliases(byName, x.aliases)?.id === b.id) &&
          /active|live|under review|draft/i.test(b.status) &&
          !DONE_RE.test(queueStatus(queue, b.id))
      );
      if (sub) {
        substitutions.push({
          from: t.brandName,
          fromId: brandId,
          to: sub.name,
          toId: sub.id,
          reason: PRIOR_BATCH_IDS.has(brandId) ? "already_in_prior_factory_batch" : `queue=${qStatus || "duplicate"}`,
        });
        live = sub;
        brandId = sub.id;
        match = "substitute";
      } else {
        // Keep prior brand but flag already_processed — still research/enrich in batch if not published
        match = PRIOR_BATCH_IDS.has(brandId) ? "prior_batch_reuse" : match;
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
      family: t.family,
      expectedName: t.brandName,
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
    contract: "BE_FACTORY_2_0_BATCH_12_IDENTITY_CONFIRM_V1",
    generatedAt: new Date().toISOString(),
    ok: true,
    newBrandBasicsCreatesProposed: creationPlan.length,
    totalTargets: TARGETS.length,
    existingFound: rows.filter((r) => r.brandId).length,
    newCandidates: creationPlan.length,
    existingActive: rows.filter((r) => r.status === "EXISTING_ACTIVE").length,
    missing: rows.filter((r) => r.status === "NEW_BRAND_CANDIDATE").length,
    substitutions,
    creationPlan,
    rows,
  };

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "120-batch12-identity-confirm.json"), JSON.stringify(out, null, 2) + "\n");
  writeFileSync(
    join(dir, "120-batch12-identity-confirm.md"),
    [
      `# Batch 12 — Identity Confirm (Hilton / Accor-Ennismore / Choice)`,
      ``,
      `Generated: ${out.generatedAt}`,
      `Found in Airtable: ${out.existingFound}`,
      `NEW candidates (no rec ID): ${out.newCandidates}`,
      `Substitutions: ${substitutions.length}`,
      ``,
      `| Family | Brand | Live ID | Live Name | Classification | Presentation | Match |`,
      `|---|---|---|---|---|---:|---|`,
      ...rows.map(
        (r) =>
          `| ${r.family} | ${r.expectedName} | \`${r.brandId || "—"}\` | ${r.liveName || "—"} | ${r.classification} | ${r.presentationBlockCount} | ${r.match} |`
      ),
      ``,
      `## Creation plan (NEW — no invented IDs)`,
      ...(creationPlan.length
        ? creationPlan.map(
            (c) =>
              `- **${c.brandName}** (${c.parentCompany}) — ${c.proposedAction}. ${c.note}`
          )
        : [`None — all brands already have Brand Basics records.`]),
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
        existingFound: out.existingFound,
        newCandidates: out.newCandidates,
        substitutions: substitutions.length,
        report: "reports/brand-explorer/factory-2-0/120-batch12-identity-confirm.json",
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
