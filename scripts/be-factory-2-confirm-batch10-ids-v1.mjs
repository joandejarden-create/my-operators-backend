#!/usr/bin/env node
/**
 * Batch 10 — live Brand Basics reconcile for EXISTING Airtable brands
 * (Accor, Choice, Wyndham, BWH & independent collections).
 * Founder-supplied canonical IDs are authoritative. Never invents rec… IDs.
 * Skip already READY/APPROVED/PUBLISHED/CERTIFIED when substitute needed.
 */
import { config as loadEnv } from "dotenv";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";
import { fetchPresentationBlocksForBrand } from "../lib/brand-explorer/be-presentation-writeback-apply-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
loadEnv({ path: join(ROOT, ".env") });

const TARGETS = [
  {
    key: "handwritten-collection",
    brandName: "Handwritten Collection",
    parentCompany: "Accor",
    expectedId: "rec7hTXwMRC81EPqz",
    aliases: ["Handwritten Collection", "Handwritten"],
  },
  {
    key: "mgallery-collection",
    brandName: "MGallery Collection",
    parentCompany: "Accor",
    expectedId: "recrWCD1LMqu864oU",
    aliases: ["MGallery Collection", "MGallery", "M Gallery"],
  },
  {
    key: "pullman",
    brandName: "Pullman",
    parentCompany: "Accor",
    expectedId: "recFW9kfqKfOjv7Z1",
    aliases: ["Pullman", "Pullman Hotels", "Pullman Hotels & Resorts"],
  },
  {
    key: "fairmont",
    brandName: "Fairmont",
    parentCompany: "Accor",
    expectedId: "recJhPaDVU3YUDQUt",
    aliases: ["Fairmont", "Fairmont Hotels", "Fairmont Hotels & Resorts"],
  },
  {
    key: "mercure",
    brandName: "Mercure",
    parentCompany: "Accor",
    expectedId: "recevrLJ3m6rIug3S",
    aliases: ["Mercure", "Mercure Hotels"],
  },
  {
    key: "ibis",
    brandName: "ibis",
    parentCompany: "Accor",
    expectedId: "reclFXbpZ5XzLWbGP",
    aliases: ["ibis", "Ibis", "ibis Hotels"],
  },
  {
    key: "mama-shelter",
    brandName: "Mama Shelter",
    parentCompany: "Accor",
    expectedId: "recXCZCK05XXYX7Q8",
    aliases: ["Mama Shelter", "MamaShelter"],
  },
  {
    key: "novotel",
    brandName: "Novotel",
    parentCompany: "Accor",
    expectedId: "recQE2lSSSSyuUrMQ",
    aliases: ["Novotel", "Novotel Hotels"],
  },
  {
    key: "so",
    brandName: "SO/",
    parentCompany: "Accor",
    expectedId: "recTJdPlr4mDs9app",
    aliases: ["SO/", "SO", "Sofitel SO", "SO Hotels", "SO/ Paris"],
  },
  {
    key: "bw-premier-collection",
    brandName: "BW Premier Collection",
    parentCompany: "Best Western Hotels & Resorts",
    expectedId: "recwXZ5gVZ8ZH8ekA",
    aliases: ["BW Premier Collection", "Best Western Premier Collection", "Premier Collection"],
  },
  {
    key: "bw-signature-collection",
    brandName: "BW Signature Collection",
    parentCompany: "Best Western Hotels & Resorts",
    expectedId: "recdeh1NsP4gjrv80",
    aliases: ["BW Signature Collection", "Best Western Signature Collection", "Signature Collection by Best Western"],
  },
  {
    key: "trademark-collection",
    brandName: "Trademark Collection by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    expectedId: "recob7tgHRryRSbeO",
    aliases: ["Trademark Collection by Wyndham", "Trademark Collection", "Trademark"],
  },
  {
    key: "dazzler",
    brandName: "Dazzler by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    expectedId: "rec5CNMM4ZUD7ZHlM",
    aliases: ["Dazzler by Wyndham", "Dazzler", "Dazzler Hotels"],
  },
  {
    key: "suburban-studios",
    brandName: "Suburban Studios",
    parentCompany: "Choice Hotels",
    expectedId: "reclcjg5Foa9Vs5TC",
    aliases: ["Suburban Studios", "Suburban", "Suburban Extended Stay"],
  },
  {
    key: "everhome-suites",
    brandName: "Everhome Suites",
    parentCompany: "Choice Hotels",
    expectedId: "recqkkrsevi4r9ibj",
    aliases: ["Everhome Suites", "Everhome", "Ever Home Suites"],
  },
  {
    key: "quality-inn",
    brandName: "Quality Inn",
    parentCompany: "Choice Hotels",
    expectedId: "recd8o4k1JddhkRWW",
    aliases: ["Quality Inn", "Quality Inn & Suites", "Quality"],
  },
  {
    key: "radisson-red",
    brandName: "Radisson RED by Choice",
    parentCompany: "Choice Hotels",
    expectedId: "recmKqo7M7mLZgRqQ",
    aliases: ["Radisson RED by Choice", "Radisson RED", "Radisson Red"],
  },
  {
    key: "radisson",
    brandName: "Radisson by Choice",
    parentCompany: "Choice Hotels",
    expectedId: "recywbx1YQSTCPqW1",
    aliases: ["Radisson by Choice", "Radisson", "Radisson Hotels"],
  },
  {
    key: "preferred-hotels",
    brandName: "Preferred Hotels & Resorts",
    parentCompany: "Preferred Hotels & Resorts",
    expectedId: "recwl5JOYxlChuCAr",
    aliases: ["Preferred Hotels & Resorts", "Preferred Hotels", "Preferred"],
  },
  {
    key: "slh",
    brandName: "Small Luxury Hotels of the World",
    parentCompany: "Small Luxury Hotels of the World",
    expectedId: "recjjSnY2opb8P4DG",
    aliases: ["Small Luxury Hotels of the World", "SLH", "Small Luxury Hotels"],
  },
];

const DONE_RE = /ready|approved|published|certified|complete/i;

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function norm(s) {
  return nz(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
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

async function main() {
  const basics = await loadAirtableBasics();
  const byId = new Map(basics.map((b) => [b.id, b]));
  const byName = new Map();
  for (const b of basics) {
    byName.set(norm(b.name), b);
  }
  const queue = loadQueue();
  const rows = [];
  const substitutions = [];
  const usedIds = new Set();

  for (const t of TARGETS) {
    let live = byId.get(t.expectedId) || null;
    let brandId = t.expectedId;
    let match = "expected_id";
    let subNote = null;

    if (!live) {
      for (const a of t.aliases) {
        const hit = byName.get(norm(a));
        if (hit) {
          live = hit;
          brandId = hit.id;
          match = "alias_name";
          break;
        }
      }
    }

    if (!live) {
      rows.push({
        key: t.key,
        expectedName: t.brandName,
        expectedId: t.expectedId,
        brandId: null,
        status: "MISSING",
        presentationBlockCount: 0,
        match: "not_found",
      });
      continue;
    }

    const qStatus = queueStatus(queue, brandId);
    if (DONE_RE.test(qStatus) && brandId !== t.expectedId) {
      // keep
    } else if (DONE_RE.test(qStatus)) {
      // Founder said: if already completed/active, retain and substitute next eligible.
      // For Batch 10 we retain existing state and still process enrichment on the same ID
      // unless the queue marks PUBLISHED/APPROVED with no further work — then substitute.
      if (/published|approved/i.test(qStatus)) {
        const sub = basics.find(
          (b) =>
            !usedIds.has(b.id) &&
            !TARGETS.some((x) => x.expectedId === b.id) &&
            /active|live/i.test(b.status) &&
            !DONE_RE.test(queueStatus(queue, b.id))
        );
        if (sub) {
          substitutions.push({
            from: t.brandName,
            fromId: t.expectedId,
            to: sub.name,
            toId: sub.id,
            reason: `queue_status=${qStatus}`,
          });
          live = sub;
          brandId = sub.id;
          match = "substitute";
          subNote = qStatus;
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

    rows.push({
      key: t.key,
      expectedName: t.brandName,
      expectedId: t.expectedId,
      brandId,
      liveName: live.name,
      parentCompany: live.parentCompany,
      brandStatus: live.status,
      queueStatus: qStatus || null,
      status: /active|live/i.test(live.status) ? "EXISTING_ACTIVE" : `EXISTING_${live.status || "UNKNOWN"}`,
      presentationBlockCount,
      match,
      substitutionReason: subNote,
    });
  }

  const out = {
    contract: "BE_FACTORY_2_0_BATCH_10_IDENTITY_CONFIRM_V1",
    generatedAt: new Date().toISOString(),
    ok: rows.every((r) => r.brandId) && rows.filter((r) => r.status === "MISSING").length === 0,
    newBrandBasicsCreatesProposed: 0,
    totalTargets: TARGETS.length,
    existingActive: rows.filter((r) => r.status === "EXISTING_ACTIVE").length,
    missing: rows.filter((r) => r.status === "MISSING").length,
    substitutions,
    rows,
  };

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "100-batch10-identity-confirm.json"), JSON.stringify(out, null, 2));
  writeFileSync(
    join(dir, "100-batch10-identity-confirm.md"),
    [
      `# Batch 10 — Identity Confirm`,
      ``,
      `Generated: ${out.generatedAt}`,
      `EXISTING_ACTIVE: ${out.existingActive} / ${out.totalTargets}`,
      `Missing: ${out.missing}`,
      `Substitutions: ${substitutions.length}`,
      ``,
      `| Brand | Expected ID | Live ID | Status | Presentation blocks | Match |`,
      `|---|---|---|---|---:|---|`,
      ...rows.map(
        (r) =>
          `| ${r.expectedName} | \`${r.expectedId}\` | \`${r.brandId || "—"}\` | ${r.status} | ${r.presentationBlockCount} | ${r.match} |`
      ),
      ``,
      ...(substitutions.length
        ? [
            `## Substitutions`,
            ...substitutions.map(
              (s) => `- ${s.from} (\`${s.fromId}\`) → ${s.to} (\`${s.toId}\`) — ${s.reason}`
            ),
          ]
        : [`No substitutions.`]),
    ].join("\n") + "\n"
  );

  console.log(
    JSON.stringify(
      {
        existingActive: out.existingActive,
        missing: out.missing,
        substitutions: substitutions.length,
        report: "reports/brand-explorer/factory-2-0/100-batch10-identity-confirm.json",
      },
      null,
      2
    )
  );
  if (out.missing > 0) process.exitCode = 2;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
