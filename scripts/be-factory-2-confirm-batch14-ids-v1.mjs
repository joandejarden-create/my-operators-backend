#!/usr/bin/env node
/**
 * Batch 14 — live Brand Basics reconcile (Accor/partner + Marriott + Wyndham).
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
  // Accor / partner
  {
    key: "sofitel",
    brandName: "Sofitel",
    parentCompany: "Accor",
    aliases: ["Sofitel", "Sofitel Hotels & Resorts", "Sofitel Hotels and Resorts"],
    family: "Accor",
  },
  {
    key: "sofitel-legend",
    brandName: "Sofitel Legend",
    parentCompany: "Accor",
    aliases: ["Sofitel Legend", "Sofitel Legend Hotels", "Sofitel Légende"],
    family: "Accor",
  },
  {
    key: "raffles",
    brandName: "Raffles Hotels & Resorts",
    parentCompany: "Accor",
    aliases: ["Raffles Hotels & Resorts", "Raffles", "Raffles Hotels", "Raffles Hotels and Resorts"],
    family: "Accor",
  },
  {
    key: "movenpick",
    brandName: "Mövenpick Hotels & Resorts",
    parentCompany: "Accor",
    aliases: [
      "Mövenpick Hotels & Resorts",
      "Movenpick Hotels & Resorts",
      "Mövenpick",
      "Movenpick",
      "Mövenpick Hotels and Resorts",
    ],
    family: "Accor",
  },
  {
    key: "grand-mercure",
    brandName: "Grand Mercure",
    parentCompany: "Accor",
    aliases: ["Grand Mercure", "Grand Mercure Hotels", "grandmercure"],
    family: "Accor",
  },
  {
    key: "peppers",
    brandName: "Peppers",
    parentCompany: "Accor",
    aliases: ["Peppers", "Peppers Hotels", "Peppers Retreats & Resorts", "Peppers Hotels Resorts & Villas"],
    family: "Accor",
  },
  {
    key: "mantra",
    brandName: "Mantra",
    parentCompany: "Accor",
    aliases: ["Mantra", "Mantra Hotels", "Mantra Hotels Resorts & Apartments"],
    family: "Accor",
  },
  {
    key: "breakfree",
    brandName: "BreakFree",
    parentCompany: "Accor",
    aliases: ["BreakFree", "BreakFree Hotels", "Breakfree", "Break Free"],
    family: "Accor",
  },
  {
    key: "the-sebel",
    brandName: "The Sebel",
    parentCompany: "Accor",
    aliases: ["The Sebel", "Sebel", "Sebel Hotels", "The Sebel Hotels"],
    family: "Accor",
  },
  {
    key: "banyan-tree",
    brandName: "Banyan Tree",
    parentCompany: "Accor Partner / Banyan Tree Holdings",
    aliases: ["Banyan Tree", "Banyan Tree Hotels & Resorts", "Banyan Tree Hotels and Resorts"],
    family: "AccorPartner",
  },
  {
    key: "angsana",
    brandName: "Angsana",
    parentCompany: "Accor Partner / Banyan Tree Holdings",
    aliases: ["Angsana", "Angsana Hotels & Resorts", "Angsana Hotels and Resorts"],
    family: "AccorPartner",
  },
  {
    key: "art-series",
    brandName: "Art Series Hotels",
    parentCompany: "Accor",
    aliases: ["Art Series Hotels", "Art Series", "Artseries", "Art Series by Accor"],
    family: "Accor",
  },
  // Marriott
  {
    key: "citizenm",
    brandName: "citizenM",
    parentCompany: "Marriott International",
    aliases: ["citizenM", "CitizenM", "citizenM Hotels", "Citizen M"],
    family: "Marriott",
  },
  {
    key: "element",
    brandName: "Element Hotels",
    parentCompany: "Marriott International",
    aliases: ["Element Hotels", "Element", "Element by Westin", "Element Hotels by Westin"],
    family: "Marriott",
  },
  {
    key: "protea",
    brandName: "Protea Hotels by Marriott",
    parentCompany: "Marriott International",
    aliases: ["Protea Hotels by Marriott", "Protea Hotels", "Protea"],
    family: "Marriott",
  },
  {
    key: "four-points-flex",
    brandName: "Four Points Flex by Sheraton",
    parentCompany: "Marriott International",
    aliases: [
      "Four Points Flex by Sheraton",
      "Four Points Flex",
      "FourPoints Flex",
      "Four Points by Sheraton Flex",
    ],
    family: "Marriott",
  },
  {
    key: "marriott-executive-apartments",
    brandName: "Marriott Executive Apartments",
    parentCompany: "Marriott International",
    aliases: [
      "Marriott Executive Apartments",
      "Executive Apartments",
      "Marriott Executive Apartment",
      "MEA",
    ],
    family: "Marriott",
  },
  // Wyndham
  {
    key: "echo-suites",
    brandName: "ECHO Suites Extended Stay by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: [
      "ECHO Suites Extended Stay by Wyndham",
      "ECHO Suites",
      "Echo Suites",
      "ECHO Suites Extended Stay",
      "Echo Suites by Wyndham",
    ],
    family: "Wyndham",
  },
  {
    key: "waterwalk",
    brandName: "WaterWalk Extended Stay by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: [
      "WaterWalk Extended Stay by Wyndham",
      "WaterWalk",
      "Water Walk",
      "WaterWalk Extended Stay",
      "WaterWalk by Wyndham",
    ],
    family: "Wyndham",
  },
  {
    key: "wyndham-alltra",
    brandName: "Wyndham Alltra",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["Wyndham Alltra", "Alltra", "Wyndham Alltra All Inclusive", "Alltra by Wyndham"],
    family: "Wyndham",
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
    .replace(/[öô]/g, "o")
    .replace(/[éèê]/g, "e")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function loadPriorBatchIds() {
  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  for (const f of [
    "90-batch9-identity-confirm.json",
    "100-batch10-identity-confirm.json",
    "110-batch11-identity-confirm.json",
    "120-batch12-identity-confirm.json",
    "130-batch13-identity-confirm.json",
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
  for (const n of [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]) {
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
  // Sofitel must not swallow Sofitel Legend
  if (t.key === "sofitel" && /legend|legende/i.test(live.name)) {
    const hit =
      basics.find((b) => /^sofitel$/i.test(norm(b.name)) || (/^sofitel\b/i.test(b.name) && !/legend/i.test(b.name))) ||
      null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  if (t.key === "sofitel-legend" && !/legend|legende/i.test(live.name)) {
    const hit = basics.find((b) => /sofitel\s*legend/i.test(b.name)) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  // Four Points Flex must not match classic Four Points
  if (t.key === "four-points-flex" && !/flex/i.test(live.name)) {
    const hit = basics.find((b) => /four\s*points.*flex|flex.*four\s*points/i.test(b.name)) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  // Element must not match Element Collection / other Element brands if present
  if (t.key === "element" && /collection|extended/i.test(live.name) && !/\belement\b/i.test(live.name)) {
    const hit = basics.find((b) => /^element\b/i.test(b.name) && !/collection/i.test(b.name)) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  // Banyan Tree vs Angsana
  if (t.key === "banyan-tree" && /angsana/i.test(live.name)) {
    const hit = basics.find((b) => /banyan/i.test(b.name)) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  if (t.key === "angsana" && /banyan/i.test(live.name)) {
    const hit = basics.find((b) => /angsana/i.test(b.name)) || null;
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
    contract: "BE_FACTORY_2_0_BATCH_14_IDENTITY_CONFIRM_V1",
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
  writeFileSync(join(dir, "140-batch14-identity-confirm.json"), JSON.stringify(out, null, 2) + "\n");
  writeFileSync(
    join(dir, "140-batch14-identity-confirm.md"),
    [
      `# Batch 14 — Identity Confirm (Accor / Marriott / Wyndham)`,
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
        report: "reports/brand-explorer/factory-2-0/140-batch14-identity-confirm.json",
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
