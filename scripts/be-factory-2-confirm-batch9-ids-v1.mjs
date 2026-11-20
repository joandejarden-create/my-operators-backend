#!/usr/bin/env node
/**
 * Batch 9 — live Brand Basics reconcile for EXISTING Airtable brands
 * (Marriott / Hilton / Hyatt select-service, extended-stay, full-service).
 * Reuses founder-supplied canonical IDs; resolves Delta Hotels by Marriott.
 * Never invents rec… IDs. Skip already READY/APPROVED/PUBLISHED/CERTIFIED.
 */
import { config as loadEnv } from "dotenv";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";
import { fetchPresentationBlocksForBrand } from "../lib/brand-explorer/be-presentation-writeback-apply-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
loadEnv({ path: join(ROOT, ".env") });

/** Founder inventory — IDs are authoritative when present; Delta resolves live. */
const TARGETS = [
  {
    key: "city-express",
    brandName: "City Express by Marriott",
    parentCompany: "Marriott International, Inc.",
    expectedId: "recucEzAS6724tOYA",
    aliases: ["City Express by Marriott", "City Express", "Cityexpress"],
  },
  {
    key: "springhill-suites",
    brandName: "SpringHill Suites by Marriott",
    parentCompany: "Marriott International, Inc.",
    expectedId: "recBzdGfkMUN9fYsv",
    aliases: ["SpringHill Suites by Marriott", "SpringHill Suites", "Spring Hill Suites"],
  },
  {
    key: "studiores",
    brandName: "StudioRes",
    parentCompany: "Marriott International, Inc.",
    expectedId: "recDM0LAD8jVRA2x3",
    aliases: ["StudioRes", "Studio Res", "StudioRes by Marriott"],
  },
  {
    key: "towneplace-suites",
    brandName: "TownePlace Suites by Marriott",
    parentCompany: "Marriott International, Inc.",
    expectedId: "recUPiPDivkhNUogr",
    aliases: ["TownePlace Suites by Marriott", "TownePlace Suites", "Towne Place Suites"],
  },
  {
    key: "fairfield",
    brandName: "Fairfield by Marriott",
    parentCompany: "Marriott International, Inc.",
    expectedId: "recpUTDtwt1wPMDPj",
    aliases: ["Fairfield by Marriott", "Fairfield Inn", "Fairfield Inn & Suites", "Fairfield"],
  },
  {
    key: "four-points",
    brandName: "Four Points by Sheraton",
    parentCompany: "Marriott International, Inc.",
    expectedId: "recH5ZF9V6ivz9p5h",
    aliases: ["Four Points by Sheraton", "Four Points", "FourPoints"],
  },
  {
    key: "residence-inn",
    brandName: "Residence Inn by Marriott",
    parentCompany: "Marriott International, Inc.",
    expectedId: "rec9Ufbpa0GxJGzt8",
    aliases: ["Residence Inn by Marriott", "Residence Inn"],
  },
  {
    key: "marriott-hotels",
    brandName: "Marriott Hotels",
    parentCompany: "Marriott International, Inc.",
    expectedId: "recn59UtkyyoYwzSz",
    aliases: ["Marriott Hotels", "Marriott Hotel", "Marriott"],
  },
  {
    key: "sheraton",
    brandName: "Sheraton",
    parentCompany: "Marriott International, Inc.",
    expectedId: "recg8HjT5Bky7NXeV",
    aliases: ["Sheraton", "Sheraton Hotels", "Sheraton Hotels & Resorts"],
  },
  {
    key: "hilton-hotels",
    brandName: "Hilton Hotels & Resorts",
    parentCompany: "Hilton Worldwide",
    expectedId: "recWubG3rhiS1BaWi",
    aliases: ["Hilton Hotels & Resorts", "Hilton Hotels", "Hilton"],
  },
  {
    key: "doubletree",
    brandName: "DoubleTree by Hilton",
    parentCompany: "Hilton Worldwide",
    expectedId: "rechVYWQ5ikRnr99B",
    aliases: ["DoubleTree by Hilton", "DoubleTree", "Double Tree"],
  },
  {
    key: "hampton",
    brandName: "Hampton by Hilton",
    parentCompany: "Hilton Worldwide",
    expectedId: "rectRvOWQPaL6FkzZ",
    aliases: ["Hampton by Hilton", "Hampton Inn", "Hampton Inn & Suites", "Hampton"],
  },
  {
    key: "hilton-garden-inn",
    brandName: "Hilton Garden Inn",
    parentCompany: "Hilton Worldwide",
    expectedId: "recrvdAjRlXxPvPPF",
    aliases: ["Hilton Garden Inn", "HGI", "Garden Inn"],
  },
  {
    key: "home2-suites",
    brandName: "Home2 Suites by Hilton",
    parentCompany: "Hilton Worldwide",
    expectedId: "reccZ4zV6wMav7a2i",
    aliases: ["Home2 Suites by Hilton", "Home2 Suites", "Home 2 Suites"],
  },
  {
    key: "homewood-suites",
    brandName: "Homewood Suites by Hilton",
    parentCompany: "Hilton Worldwide",
    expectedId: "recZjYI4nYflGHFNR",
    aliases: ["Homewood Suites by Hilton", "Homewood Suites", "Homewood"],
  },
  {
    key: "spark",
    brandName: "Spark by Hilton",
    parentCompany: "Hilton Worldwide",
    expectedId: "recfv66er4Ch2vJDO",
    aliases: ["Spark by Hilton", "Spark"],
  },
  {
    key: "tru",
    brandName: "Tru by Hilton",
    parentCompany: "Hilton Worldwide",
    expectedId: "recJLiMTv4W8VgO9L",
    aliases: ["Tru by Hilton", "Tru", "TRU by Hilton"],
  },
  {
    key: "hyatt-regency",
    brandName: "Hyatt Regency",
    parentCompany: "Hyatt Hotels Corporation",
    expectedId: "recP9SqDootMrzaU1",
    aliases: ["Hyatt Regency", "Regency"],
  },
  {
    key: "thompson",
    brandName: "Thompson Hotels",
    parentCompany: "Hyatt Hotels Corporation",
    expectedId: "rec4Mga6ejz3L1M3P",
    aliases: ["Thompson Hotels", "Thompson", "Thompson Hotel"],
  },
  {
    key: "delta-hotels",
    brandName: "Delta Hotels by Marriott",
    parentCompany: "Marriott International, Inc.",
    expectedId: null,
    aliases: [
      "Delta Hotels by Marriott",
      "Delta Hotels",
      "Delta Hotel",
      "Delta Hotels Marriott",
    ],
  },
];

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function loadStage(brandId) {
  const p = join(ROOT, "data/brand-explorer/factory-2-0/brands", brandId, "stage.json");
  if (!existsSync(p)) return null;
  return JSON.parse(readFileSync(p, "utf8"));
}

function scoreMatch(liveName, aliases) {
  const live = norm(liveName);
  let best = 0;
  for (const a of aliases) {
    const n = norm(a);
    if (live === n) return 100;
    if (live.startsWith(n) || n.startsWith(live)) best = Math.max(best, 95);
    if (live.includes(n) || n.includes(live)) best = Math.max(best, 85);
  }
  return best;
}

function disambiguate(targetKey, scored) {
  if (!scored.length) return scored;
  if (targetKey === "marriott-hotels") {
    return scored.filter((r) => {
      const n = norm(r.name);
      return (
        n === "marriott hotels" ||
        n === "marriott hotel" ||
        (n === "marriott" && !n.includes("bonvoy"))
      );
    });
  }
  if (targetKey === "hilton-hotels") {
    return scored.filter((r) => {
      const n = norm(r.name);
      return (
        n.includes("hilton hotels") ||
        n === "hilton" ||
        (n.startsWith("hilton") &&
          !n.includes("garden") &&
          !n.includes("hampton") &&
          !n.includes("homewood") &&
          !n.includes("home2") &&
          !n.includes("double") &&
          !n.includes("spark") &&
          !n.includes("tru") &&
          !n.includes("canopy") &&
          !n.includes("curio") &&
          !n.includes("tapestry") &&
          !n.includes("tempo") &&
          !n.includes("motto") &&
          !n.includes("signia") &&
          !n.includes("waldorf") &&
          !n.includes("conrad") &&
          !n.includes("lxr"))
      );
    });
  }
  if (targetKey === "sheraton") {
    return scored.filter((r) => {
      const n = norm(r.name);
      return n === "sheraton" || n.startsWith("sheraton hotels") || n === "sheraton hotels resorts";
    });
  }
  if (targetKey === "hyatt-regency") {
    return scored.filter((r) => {
      const n = norm(r.name);
      return n.includes("hyatt regency") || n === "regency";
    });
  }
  if (targetKey === "delta-hotels") {
    return scored.filter((r) => {
      const n = norm(r.name);
      return n.includes("delta") && !n.includes("deltahotels wait");
    });
  }
  return scored;
}

async function fetchAllBasics(token, baseId) {
  const all = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    params.append("fields[]", "Brand Name");
    params.append("fields[]", "Brand Status");
    params.append("fields[]", "Parent Company");
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent("Brand Setup - Brand Basics")}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json).slice(0, 400));
    all.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return all;
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing Airtable credentials");

  const records = await fetchAllBasics(token, baseId);
  const byId = new Map(records.map((r) => [r.id, r]));
  const rows = [];
  const substitutions = [];

  for (const t of TARGETS) {
    let best = null;
    let scored = [];
    let idMatch = null;

    if (t.expectedId && byId.has(t.expectedId)) {
      const rec = byId.get(t.expectedId);
      idMatch = {
        id: rec.id,
        name: rec.fields?.["Brand Name"] || null,
        status: rec.fields?.["Brand Status"] || null,
        parent: rec.fields?.["Parent Company"] || null,
        score: scoreMatch(rec.fields?.["Brand Name"], t.aliases),
      };
      // Prefer founder ID when name is compatible
      if (idMatch.score >= 70 || norm(idMatch.name).includes(norm(t.brandName).split(" ")[0])) {
        best = idMatch;
      }
    }

    scored = records
      .map((r) => ({
        id: r.id,
        name: r.fields?.["Brand Name"] || null,
        status: r.fields?.["Brand Status"] || null,
        parent: r.fields?.["Parent Company"] || null,
        score: scoreMatch(r.fields?.["Brand Name"], t.aliases),
      }))
      .filter((r) => r.score >= 80)
      .sort((a, b) => b.score - a.score);

    scored = disambiguate(t.key, scored).sort((a, b) => b.score - a.score);

    if (!best) {
      best = scored[0] || null;
    } else if (idMatch && scored[0] && scored[0].id !== idMatch.id && scored[0].score > idMatch.score + 10) {
      // Name search strongly prefers another record — flag but keep expected if still valid
      substitutions.push({
        key: t.key,
        keptExpectedId: idMatch.id,
        strongerNameMatch: scored[0],
        note: "Founder ID retained; stronger name match noted for review",
      });
    }

    const confirmed = Boolean(best && (best.score >= 70 || (t.expectedId && best.id === t.expectedId)));
    let presentationBlockCount = 0;
    if (confirmed) {
      try {
        const live = await fetchPresentationBlocksForBrand(best.id, best.name);
        presentationBlockCount = (live.blocks || []).length;
      } catch {
        /* keep 0 */
      }
    }
    const stage = confirmed ? loadStage(best.id) : null;
    const alreadyComplete = [
      "READY_FOR_REVIEW",
      "APPROVED",
      "PUBLISHED",
      "CERTIFIED_READY_FOR_REVIEW",
    ].includes(stage?.factoryStatus);

    const idOk = !t.expectedId || (confirmed && best.id === t.expectedId);
    rows.push({
      key: t.key,
      expectedName: t.brandName,
      expectedParent: t.parentCompany,
      expectedId: t.expectedId,
      brandId: confirmed ? best.id : null,
      liveName: confirmed ? best.name : null,
      status: confirmed ? best.status : null,
      parent: confirmed ? best.parent : null,
      matchScore: best?.score || 0,
      candidates: scored.slice(0, 5),
      classification: confirmed
        ? "EXISTING_CANONICAL"
        : scored.length
          ? "AMBIGUOUS_OR_WEAK_MATCH"
          : "MISSING_FROM_INVENTORY",
      newBrandBasicsCreateRequired: !confirmed,
      confirmed,
      expectedIdMatched: idOk,
      presentationBlockCount,
      existingFactoryStatus: stage?.factoryStatus || null,
      alreadyComplete,
      process: confirmed && !alreadyComplete,
      note: !confirmed
        ? "MISSING or weak match — do not invent ID; substitute next eligible existing brand"
        : alreadyComplete
          ? "Skip — already READY/APPROVED/PUBLISHED; substitute next eligible if batch must stay at 20"
          : !idOk
            ? `Resolved alternate ID ${best.id} (expected ${t.expectedId})`
            : presentationBlockCount === 0
              ? "EXISTING Brand Basics; Presentation empty — depth stage"
              : "Reuse existing Brand Basics; enrich Presentation via depth stage",
    });
  }

  // If any missing/already-complete, propose substitutions from inventory not in authorized batches / this batch
  const processIds = new Set(rows.filter((r) => r.process).map((r) => r.brandId));
  const missingSlots = rows.filter((r) => !r.process).length;

  const out = {
    ok: rows.every((r) => r.confirmed) && rows.every((r) => !r.expectedId || r.expectedIdMatched),
    batch: "BATCH_9",
    generatedAt: new Date().toISOString(),
    inventoryTotal: records.length,
    inventoryNote:
      "Batch 9 EXISTING Airtable brands (Marriott/Hilton/Hyatt). Reuse EXISTING_CANONICAL only. No invented rec… IDs. Delta Hotels resolved live.",
    newBrandBasicsCreatesProposed: rows.filter((r) => r.newBrandBasicsCreateRequired).length,
    deltaHotelsResolvedId: rows.find((r) => r.key === "delta-hotels")?.brandId || null,
    substitutions,
    missingSlots,
    processCount: processIds.size,
    rows,
    toProcess: rows.filter((r) => r.process).map((r) => r.brandId),
    skipOrSubstitute: rows.filter((r) => !r.process).map((r) => ({
      expectedName: r.expectedName,
      brandId: r.brandId,
      reason: r.alreadyComplete ? "already_complete" : r.confirmed ? "other" : "unconfirmed",
      note: r.note,
    })),
    missingOrAmbiguous: rows.filter((r) => !r.confirmed).map((r) => r.expectedName),
  };

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "90-batch9-identity-confirm.json"), JSON.stringify(out, null, 2) + "\n");
  console.log(JSON.stringify(out, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
