#!/usr/bin/env node
/**
 * Batch 15 — live Brand Basics reconcile (Minor + Meliá + Barceló).
 * Never invent rec… IDs. Substitute if already on prior factory batches when needed.
 */
import { config as loadEnv } from "dotenv";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";
import { fetchPresentationBlocksForBrand } from "../lib/brand-explorer/be-presentation-writeback-apply-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
loadEnv({ path: join(ROOT, ".env") });

const TARGETS = [
  // Minor Hotels
  {
    key: "anantara",
    brandName: "Anantara Hotels & Resorts",
    parentCompany: "Minor Hotels",
    aliases: ["Anantara Hotels & Resorts", "Anantara", "Anantara Hotels", "Anantara Hotels and Resorts"],
    family: "Minor",
  },
  {
    key: "tivoli",
    brandName: "Tivoli Hotels & Resorts",
    parentCompany: "Minor Hotels",
    aliases: ["Tivoli Hotels & Resorts", "Tivoli", "Tivoli Hotels", "Tivoli Hotels and Resorts"],
    family: "Minor",
  },
  {
    key: "nh-collection",
    brandName: "NH Collection Hotels & Resorts",
    parentCompany: "Minor Hotels",
    aliases: [
      "NH Collection Hotels & Resorts",
      "NH Collection",
      "NH Collection Hotels",
      "NH Collection Hotels and Resorts",
    ],
    family: "Minor",
  },
  {
    key: "nh-hotels",
    brandName: "NH Hotels & Resorts",
    parentCompany: "Minor Hotels",
    aliases: ["NH Hotels & Resorts", "NH Hotels", "NH Hotel Group", "NH Hotels and Resorts", "NH"],
    family: "Minor",
  },
  {
    key: "nhow",
    brandName: "nhow Hotels & Resorts",
    parentCompany: "Minor Hotels",
    aliases: ["nhow Hotels & Resorts", "nhow", "Nhow", "nhow Hotels", "NHOW"],
    family: "Minor",
  },
  {
    key: "avani",
    brandName: "Avani Hotels & Resorts",
    parentCompany: "Minor Hotels",
    aliases: ["Avani Hotels & Resorts", "Avani", "Avani Hotels", "Avani Hotels and Resorts"],
    family: "Minor",
  },
  {
    key: "oaks",
    brandName: "Oaks Hotels, Resorts & Suites",
    parentCompany: "Minor Hotels",
    aliases: [
      "Oaks Hotels, Resorts & Suites",
      "Oaks",
      "Oaks Hotels",
      "Oaks Hotels Resorts & Suites",
      "Oaks Hotels and Resorts",
    ],
    family: "Minor",
  },
  {
    key: "elewana",
    brandName: "Elewana Collection",
    parentCompany: "Minor Hotels",
    aliases: ["Elewana Collection", "Elewana", "Elewana Safari Collection", "Elewana Collection Africa"],
    family: "Minor",
  },
  // Meliá
  {
    key: "gran-melia",
    brandName: "Gran Meliá Hotels & Resorts",
    parentCompany: "Meliá Hotels International",
    aliases: [
      "Gran Meliá Hotels & Resorts",
      "Gran Melia",
      "Gran Meliá",
      "Gran Melia Hotels & Resorts",
      "Gran Meliá Hotels and Resorts",
    ],
    family: "Melia",
  },
  {
    key: "me-by-melia",
    brandName: "ME by Meliá",
    parentCompany: "Meliá Hotels International",
    aliases: ["ME by Meliá", "ME by Melia", "ME Hotels", "ME by Meliá Hotels", "Meliá ME"],
    family: "Melia",
  },
  {
    key: "melia-collection",
    brandName: "The Meliá Collection",
    parentCompany: "Meliá Hotels International",
    aliases: [
      "The Meliá Collection",
      "The Melia Collection",
      "Meliá Collection",
      "Melia Collection",
    ],
    family: "Melia",
  },
  {
    key: "paradisus",
    brandName: "Paradisus by Meliá",
    parentCompany: "Meliá Hotels International",
    aliases: ["Paradisus by Meliá", "Paradisus by Melia", "Paradisus", "Paradisus Resorts"],
    family: "Melia",
  },
  {
    key: "melia-hotels",
    brandName: "Meliá Hotels & Resorts",
    parentCompany: "Meliá Hotels International",
    aliases: [
      "Meliá Hotels & Resorts",
      "Melia Hotels & Resorts",
      "Meliá",
      "Melia",
      "Meliá Hotels",
      "Melia Hotels and Resorts",
    ],
    family: "Melia",
  },
  {
    key: "innside",
    brandName: "INNSiDE by Meliá",
    parentCompany: "Meliá Hotels International",
    aliases: ["INNSiDE by Meliá", "INNSiDE by Melia", "INNSiDE", "Innside", "INNSiDE Hotels"],
    family: "Melia",
  },
  {
    key: "zel",
    brandName: "ZEL",
    parentCompany: "Meliá Hotels International",
    aliases: ["ZEL", "ZEL Hotels", "ZEL by Meliá", "ZEL by Melia"],
    family: "Melia",
  },
  {
    key: "sol-by-melia",
    brandName: "Sol by Meliá",
    parentCompany: "Meliá Hotels International",
    aliases: ["Sol by Meliá", "Sol by Melia", "Sol Hotels", "Sol Melia", "Sol Meliá"],
    family: "Melia",
  },
  // Barceló
  {
    key: "royal-hideaway",
    brandName: "Royal Hideaway Hotels & Resorts",
    parentCompany: "Barceló Hotel Group",
    aliases: [
      "Royal Hideaway Hotels & Resorts",
      "Royal Hideaway",
      "Royal Hideaway Luxury Hotels & Resorts",
      "Royal Hideaway Hotels and Resorts",
    ],
    family: "Barcelo",
  },
  {
    key: "barcelo",
    brandName: "Barceló Hotels & Resorts",
    parentCompany: "Barceló Hotel Group",
    aliases: [
      "Barceló Hotels & Resorts",
      "Barcelo Hotels & Resorts",
      "Barceló",
      "Barcelo",
      "Barceló Hotels",
      "Barcelo Hotels and Resorts",
    ],
    family: "Barcelo",
  },
  {
    key: "occidental",
    brandName: "Occidental Hotels & Resorts",
    parentCompany: "Barceló Hotel Group",
    aliases: [
      "Occidental Hotels & Resorts",
      "Occidental",
      "Occidental Hotels",
      "Occidental Hotels and Resorts",
    ],
    family: "Barcelo",
  },
  {
    key: "allegro",
    brandName: "Allegro Hotels",
    parentCompany: "Barceló Hotel Group",
    aliases: ["Allegro Hotels", "Allegro", "Allegro by Barceló", "Allegro Hotels & Resorts"],
    family: "Barcelo",
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
    .replace(/[áàäâ]/g, "a")
    .replace(/[éèêë]/g, "e")
    .replace(/[íìîï]/g, "i")
    .replace(/[óòôö]/g, "o")
    .replace(/[úùûü]/g, "u")
    .replace(/ñ/g, "n")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function loadPriorBatchIds() {
  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  for (const n of [9, 10, 11, 12, 13, 14]) {
    const p = join(dir, `${n === 9 ? "90" : n === 10 ? "100" : `${n}0`}-batch${n}-identity-confirm.json`);
    // also try 110/120/130/140 pattern
  }
  for (const f of [
    "90-batch9-identity-confirm.json",
    "100-batch10-identity-confirm.json",
    "110-batch11-identity-confirm.json",
    "120-batch12-identity-confirm.json",
    "130-batch13-identity-confirm.json",
    "140-batch14-identity-confirm.json",
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
  for (const n of [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]) {
    const packPath = join(
      ROOT,
      `lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-batch${n}-v1.js`
    );
    if (!existsSync(packPath)) continue;
    try {
      const src = readFileSync(packPath, "utf8");
      for (const m of src.matchAll(/"(rec[a-zA-Z0-9]+)"/g)) PRIOR_BATCH_IDS.add(m[1]);
      for (const m of src.matchAll(/"(NEW_[a-z0-9-]+)"/g)) PRIOR_BATCH_IDS.add(m[1]);
    } catch {
      /* ignore */
    }
  }
  const packsV1 = join(ROOT, "lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js");
  if (existsSync(packsV1)) {
    try {
      const src = readFileSync(packsV1, "utf8");
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

function disambiguate(t, live, basics) {
  if (!live) return { live, match: "not_in_airtable" };
  // NH Collection must contain NH — never match Radisson/other *Collection peers
  if (t.key === "nh-collection") {
    if (!/\bnh\b/i.test(live.name) || !/collection/i.test(live.name)) {
      const hit = basics.find((b) => /\bnh\b/i.test(b.name) && /collection/i.test(b.name)) || null;
      return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
    }
  }
  // NH Hotels vs NH Collection / nhow
  if (t.key === "nh-hotels" && /collection|nhow/i.test(live.name)) {
    const hit =
      basics.find(
        (b) =>
          (/^nh\b/i.test(b.name) || /^nh\s+hotels/i.test(b.name)) &&
          !/collection|nhow/i.test(b.name)
      ) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  if (t.key === "nhow" && !/nhow/i.test(live.name)) {
    const hit = basics.find((b) => /nhow/i.test(b.name)) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  // Gran Meliá vs Meliá Hotels
  if (t.key === "melia-hotels" && /gran\s*meli|collection|paradisus|innside|\bme\b|\bzel\b|\bsol\b/i.test(live.name)) {
    const hit =
      basics.find(
        (b) =>
          (/^meli[aá]\b/i.test(b.name) || /^meli[aá]\s+hotels/i.test(b.name)) &&
          !/gran|collection|paradisus|innside|\bme\b|\bzel\b|\bsol\b/i.test(b.name)
      ) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  if (t.key === "gran-melia" && !/gran/i.test(live.name)) {
    const hit = basics.find((b) => /gran\s*meli/i.test(b.name)) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  if (t.key === "me-by-melia" && !/\bme\b/i.test(live.name)) {
    const hit = basics.find((b) => /\bme\b.*meli|meli.*\bme\b/i.test(b.name)) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  if (t.key === "sol-by-melia" && !/\bsol\b/i.test(live.name)) {
    const hit = basics.find((b) => /\bsol\b.*meli|meli.*\bsol\b/i.test(b.name)) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  // Barceló vs Royal Hideaway / Occidental / Allegro
  if (t.key === "barcelo" && /royal\s*hideaway|occidental|allegro/i.test(live.name)) {
    const hit =
      basics.find(
        (b) => /barcel/i.test(b.name) && !/royal\s*hideaway|occidental|allegro/i.test(b.name)
      ) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  return { live, match: "alias_found" };
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
    let dis = disambiguate(t, live, basics);
    live = dis.live;
    let brandId = live ? live.id : null;
    let match = dis.match;

    if (!live) {
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
        classification: "NEW_BRAND_CANDIDATE",
        presentationBlockCount: 0,
        match: "not_in_airtable",
      });
      continue;
    }

    if (PRIOR_BATCH_IDS.has(brandId) || usedIds.has(brandId)) {
      const qStatus = queueStatus(queue, brandId);
      if (PRIOR_BATCH_IDS.has(brandId) && DONE_RE.test(qStatus)) {
        const sub = basics.find(
          (b) =>
            !usedIds.has(b.id) &&
            !PRIOR_BATCH_IDS.has(b.id) &&
            /active|live|under review|draft/i.test(b.status) &&
            !DONE_RE.test(queueStatus(queue, b.id))
        );
        if (sub) {
          substitutions.push({
            from: t.brandName,
            fromId: brandId,
            to: sub.name,
            toId: sub.id,
            reason: `already_in_prior_factory_batch_queue=${qStatus}`,
          });
          live = sub;
          brandId = sub.id;
          match = "substitute";
        } else {
          match = "prior_batch_reuse";
        }
      } else if (PRIOR_BATCH_IDS.has(brandId)) {
        match = "prior_batch_reuse";
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
    contract: "BE_FACTORY_2_0_BATCH_15_IDENTITY_CONFIRM_V1",
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
  writeFileSync(join(dir, "150-batch15-identity-confirm.json"), JSON.stringify(out, null, 2) + "\n");
  writeFileSync(
    join(dir, "150-batch15-identity-confirm.md"),
    [
      `# Batch 15 — Identity Confirm (Minor / Meliá / Barceló)`,
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
        report: "reports/brand-explorer/factory-2-0/150-batch15-identity-confirm.json",
        rows: rows.map((r) => ({
          key: r.key,
          brandId: r.brandId,
          liveName: r.liveName,
          classification: r.classification,
        })),
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
