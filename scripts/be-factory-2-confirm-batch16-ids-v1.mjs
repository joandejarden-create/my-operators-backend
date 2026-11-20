#!/usr/bin/env node
/**
 * Batch 16 — live Brand Basics reconcile (Pan Pacific + Shangri-La + Dusit + Rotana + Ascott).
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
  // Pan Pacific
  {
    key: "pan-pacific",
    brandName: "Pan Pacific Hotels & Resorts",
    parentCompany: "Pan Pacific Hotels Group",
    aliases: [
      "Pan Pacific Hotels & Resorts",
      "Pan Pacific",
      "Pan Pacific Hotels",
      "Pan Pacific Hotels and Resorts",
    ],
    family: "PanPacific",
  },
  {
    key: "parkroyal-collection",
    brandName: "PARKROYAL COLLECTION",
    parentCompany: "Pan Pacific Hotels Group",
    aliases: [
      "PARKROYAL COLLECTION",
      "Parkroyal Collection",
      "PARKROYAL Collection",
      "Park Royal Collection",
    ],
    family: "PanPacific",
  },
  {
    key: "parkroyal",
    brandName: "PARKROYAL Hotels & Resorts",
    parentCompany: "Pan Pacific Hotels Group",
    aliases: [
      "PARKROYAL Hotels & Resorts",
      "Parkroyal Hotels & Resorts",
      "PARKROYAL",
      "Parkroyal",
      "Park Royal Hotels",
      "PARKROYAL Hotels and Resorts",
    ],
    family: "PanPacific",
  },
  // Shangri-La
  {
    key: "shangri-la",
    brandName: "Shangri-La Hotels & Resorts",
    parentCompany: "Shangri-La Group",
    aliases: [
      "Shangri-La Hotels & Resorts",
      "Shangri-La",
      "Shangri La",
      "Shangri-La Hotels",
      "Shangri-La Hotels and Resorts",
    ],
    family: "ShangriLa",
  },
  {
    key: "kerry-hotels",
    brandName: "Kerry Hotels",
    parentCompany: "Shangri-La Group",
    aliases: ["Kerry Hotels", "Kerry Hotel", "Kerry Hotels by Shangri-La"],
    family: "ShangriLa",
  },
  {
    key: "jen",
    brandName: "JEN by Shangri-La",
    parentCompany: "Shangri-La Group",
    aliases: ["JEN by Shangri-La", "JEN", "Jen by Shangri-La", "JEN Hotels"],
    family: "ShangriLa",
  },
  {
    key: "traders",
    brandName: "Traders Hotels",
    parentCompany: "Shangri-La Group",
    aliases: ["Traders Hotels", "Traders", "Traders Hotel", "Traders Hotels by Shangri-La"],
    family: "ShangriLa",
  },
  // Dusit
  {
    key: "dusit-thani",
    brandName: "Dusit Thani",
    parentCompany: "Dusit International",
    aliases: ["Dusit Thani", "Dusit Thani Hotels", "Dusit Thani Hotels & Resorts"],
    family: "Dusit",
  },
  {
    key: "dusitd2",
    brandName: "dusitD2",
    parentCompany: "Dusit International",
    aliases: ["dusitD2", "Dusit D2", "dusit d2", "DusitD2"],
    family: "Dusit",
  },
  {
    key: "dusit-princess",
    brandName: "Dusit Princess",
    parentCompany: "Dusit International",
    aliases: ["Dusit Princess", "Dusit Princess Hotels", "Dusit Princess Hotels & Resorts"],
    family: "Dusit",
  },
  {
    key: "asai",
    brandName: "ASAI Hotels",
    parentCompany: "Dusit International",
    aliases: ["ASAI Hotels", "ASAI", "Asai Hotels", "ASAI by Dusit"],
    family: "Dusit",
  },
  {
    key: "dusit-collection",
    brandName: "Dusit Collection",
    parentCompany: "Dusit International",
    aliases: ["Dusit Collection", "The Dusit Collection"],
    family: "Dusit",
  },
  // Rotana
  {
    key: "rotana",
    brandName: "Rotana Hotels & Resorts",
    parentCompany: "Rotana",
    aliases: [
      "Rotana Hotels & Resorts",
      "Rotana",
      "Rotana Hotels",
      "Rotana Hotels and Resorts",
    ],
    family: "Rotana",
  },
  {
    key: "rayhaan",
    brandName: "Rayhaan Hotels & Resorts",
    parentCompany: "Rotana",
    aliases: [
      "Rayhaan Hotels & Resorts",
      "Rayhaan",
      "Rayhaan Hotels",
      "Rayhaan by Rotana",
      "Rayhaan Hotels and Resorts",
    ],
    family: "Rotana",
  },
  {
    key: "arjaan",
    brandName: "Arjaan Hotel Apartments",
    parentCompany: "Rotana",
    aliases: [
      "Arjaan Hotel Apartments",
      "Arjaan",
      "Arjaan by Rotana",
      "Arjaan Rotana",
      "Arjaan Hotel Apartment",
    ],
    family: "Rotana",
  },
  {
    key: "centro",
    brandName: "Centro Hotels",
    parentCompany: "Rotana",
    aliases: ["Centro Hotels", "Centro", "Centro by Rotana", "Centro Hotels by Rotana"],
    family: "Rotana",
  },
  // Ascott
  {
    key: "citadines",
    brandName: "Citadines",
    parentCompany: "The Ascott Limited",
    aliases: ["Citadines", "Citadines Apart'hotel", "Citadines Aparthotel", "Citadines by Ascott"],
    family: "Ascott",
  },
  {
    key: "somerset",
    brandName: "Somerset",
    parentCompany: "The Ascott Limited",
    aliases: ["Somerset", "Somerset Serviced Residence", "Somerset by Ascott"],
    family: "Ascott",
  },
  {
    key: "lyf",
    brandName: "lyf",
    parentCompany: "The Ascott Limited",
    aliases: ["lyf", "Lyf", "lyf by Ascott", "LYF"],
    family: "Ascott",
  },
  {
    key: "oakwood",
    brandName: "Oakwood",
    parentCompany: "The Ascott Limited",
    aliases: ["Oakwood", "Oakwood Apartments", "Oakwood Worldwide", "Oakwood by Ascott"],
    family: "Ascott",
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
    "110-batch11-identity-confirm.json",
    "120-batch12-identity-confirm.json",
    "130-batch13-identity-confirm.json",
    "140-batch14-identity-confirm.json",
    "150-batch15-identity-confirm.json",
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
  for (const n of [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]) {
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

  // PARKROYAL COLLECTION must not match PARKROYAL (and vice versa); never match unrelated *Collection
  if (t.key === "parkroyal-collection") {
    if (!/park\s*royal/i.test(live.name) || !/collection/i.test(live.name)) {
      const hit =
        basics.find((b) => /park\s*royal/i.test(b.name) && /collection/i.test(b.name)) || null;
      return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
    }
  }
  if (t.key === "parkroyal" && /collection/i.test(live.name)) {
    const hit =
      basics.find((b) => /park\s*royal/i.test(b.name) && !/collection/i.test(b.name)) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }

  // Shangri-La must not swallow Kerry / JEN / Traders
  if (t.key === "shangri-la" && /kerry|\bjen\b|traders/i.test(live.name)) {
    const hit =
      basics.find(
        (b) => /shangri/i.test(b.name) && !/kerry|\bjen\b|traders/i.test(b.name)
      ) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }

  // Dusit siblings
  if (t.key === "dusit-thani" && /d2|princess|asai|collection/i.test(live.name)) {
    const hit =
      basics.find((b) => /dusit\s*thani/i.test(b.name) && !/d2|princess|asai|collection/i.test(b.name)) ||
      null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  if (t.key === "dusitd2" && !/d2/i.test(live.name)) {
    const hit = basics.find((b) => /dusit\s*d2|dusitd2/i.test(b.name)) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  if (t.key === "dusit-princess" && !/princess/i.test(live.name)) {
    const hit = basics.find((b) => /dusit\s*princess/i.test(b.name)) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }
  if (t.key === "dusit-collection") {
    if (!/dusit/i.test(live.name) || !/collection/i.test(live.name)) {
      const hit = basics.find((b) => /dusit/i.test(b.name) && /collection/i.test(b.name)) || null;
      return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
    }
  }

  // Rotana siblings
  if (t.key === "rotana" && /rayhaan|arjaan|centro/i.test(live.name)) {
    const hit =
      basics.find(
        (b) => /^rotana\b/i.test(b.name) && !/rayhaan|arjaan|centro/i.test(b.name)
      ) || null;
    return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
  }

  // Oakwood must never match Minor Oaks
  if (t.key === "oakwood") {
    if (/^oaks\b/i.test(live.name) && !/oakwood/i.test(live.name)) {
      const hit = basics.find((b) => /oakwood/i.test(b.name)) || null;
      return { live: hit, match: hit ? "sibling_disambiguated" : "not_in_airtable" };
    }
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
    contract: "BE_FACTORY_2_0_BATCH_16_IDENTITY_CONFIRM_V1",
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
  writeFileSync(join(dir, "160-batch16-identity-confirm.json"), JSON.stringify(out, null, 2) + "\n");
  writeFileSync(
    join(dir, "160-batch16-identity-confirm.md"),
    [
      `# Batch 16 — Identity Confirm (Pan Pacific / Shangri-La / Dusit / Rotana / Ascott)`,
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
        report: "reports/brand-explorer/factory-2-0/160-batch16-identity-confirm.json",
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
