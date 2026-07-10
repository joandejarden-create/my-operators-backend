/**
 * Populate Project Fit & Deal fields for Brand Setup brands.
 *
 *   node scripts/populate-active-brand-project-fit-deal.mjs --dry-run
 *   node scripts/populate-active-brand-project-fit-deal.mjs --apply
 *   node scripts/populate-active-brand-project-fit-deal.mjs --apply --active-only
 *   node scripts/populate-active-brand-project-fit-deal.mjs --apply --brand "Four Seasons"
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  buildBrandProjectFitDealProfile,
  projectFitDealProfileToAirtableFields,
} from "../lib/brand-explorer/active-brand-project-fit-deal-profiles.js";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const PF_TABLE = "Brand Setup - Project Fit";

const BASICS_FIELDS = [
  "Brand Name",
  "Brand Status",
  "Parent Company",
  "Hotel Chain Scale",
  "Brand Model",
  "Brand Architecture",
  "Hotel Service Model",
  "Brand Development Stage",
  "Branded Residences Status",
  "Brand Setup - Project Fit",
];

function parseArgs(argv) {
  const args = argv.slice(2);
  const bi = args.indexOf("--brand");
  return {
    dryRun: !args.includes("--apply"),
    activeOnly: args.includes("--active-only"),
    brandFilter: bi >= 0 ? String(args[bi + 1] || "").trim() : "",
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function loadProjectFitIndex(base) {
  const rows = await base(PF_TABLE).select({ fields: ["Brand Name", "Brand"] }).all();
  const byBrandId = new Map();
  const byBrandName = new Map();
  for (const row of rows) {
    const nm = String(row.get("Brand Name") || "").trim();
    if (nm) byBrandName.set(nm, row.id);
    for (const id of row.get("Brand") || []) {
      if (id && !byBrandId.has(id)) byBrandId.set(id, row.id);
    }
  }
  return { byBrandId, byBrandName, rowCount: rows.length };
}

function resolveProjectFitId(basics, index) {
  const linked = basics.get("Brand Setup - Project Fit");
  if (Array.isArray(linked) && linked[0]) return linked[0];
  const byLink = index.byBrandId.get(basics.id);
  if (byLink) return byLink;
  const name = String(basics.get("Brand Name") || "").trim();
  return name ? index.byBrandName.get(name) || null : null;
}

async function createProjectFit(base, brandName, basicsId, fields) {
  const tries = [
    { "Brand Name": brandName, Brand: [basicsId], ...fields },
    { "Brand Name": brandName, Brand_Basic_ID: [basicsId], ...fields },
    { "Brand Name": brandName, "Brand Setup - Brand Basics": [basicsId], ...fields },
    { "Brand Name": brandName, ...fields },
  ];
  let lastErr;
  for (const payload of tries) {
    try {
      const [created] = await base(PF_TABLE).create([{ fields: payload }], { typecast: true });
      return created;
    } catch (err) {
      lastErr = err;
      if (err.error === "UNKNOWN_FIELD_NAME") {
        const msg = String(err.message || "");
        const m = msg.match(/Unknown field name: "([^"]+)"/);
        if (m) delete payload[m[1]];
        continue;
      }
      throw err;
    }
  }
  throw lastErr || new Error(`Could not create Project Fit for ${brandName}`);
}

async function updateWithPruning(base, table, recordId, fields) {
  let payload = { ...fields };
  for (let attempt = 0; attempt < 80; attempt++) {
    if (!Object.keys(payload).length) return;
    try {
      await base(table).update(recordId, payload, { typecast: true });
      return;
    } catch (err) {
      if (err.error === "UNKNOWN_FIELD_NAME") {
        const msg = String(err.message || "");
        const m = msg.match(/Unknown field name: "([^"]+)"/);
        if (m && Object.hasOwn(payload, m[1])) {
          delete payload[m[1]];
          continue;
        }
      }
      if (err.error === "INVALID_MULTIPLE_CHOICE_OPTIONS") {
        const msg = String(err.message || "");
        const m = msg.match(/option "([^"]+)"/);
        if (m) {
          for (const [k, v] of Object.entries(payload)) {
            if (Array.isArray(v)) {
              const next = v.filter((x) => x !== m[1]);
              if (next.length !== v.length) payload[k] = next;
            }
          }
          continue;
        }
      }
      throw err;
    }
  }
}

async function main() {
  const { dryRun, activeOnly, brandFilter } = parseArgs(process.argv);
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );

  let basicsRows = await base(BASICS_TABLE).select({ fields: BASICS_FIELDS }).all();

  if (activeOnly) {
    basicsRows = basicsRows.filter((r) => String(r.get("Brand Status") || "").trim() === "Active");
  }

  if (brandFilter) {
    basicsRows = basicsRows.filter((r) => String(r.get("Brand Name") || "").trim() === brandFilter);
    if (!basicsRows.length) throw new Error(`Brand not found: ${brandFilter}`);
  }

  console.log(
    `Processing ${basicsRows.length} brand(s)${activeOnly ? " (Active only)" : " (all statuses)"} — ${dryRun ? "dry-run" : "apply"}…`
  );

  const pfIndex = await loadProjectFitIndex(base);
  console.log(`Loaded ${pfIndex.rowCount} Project Fit row(s).`);

  let basicsUpdated = 0;
  let pfUpdated = 0;
  let pfCreated = 0;
  let errors = 0;

  for (let i = 0; i < basicsRows.length; i++) {
    const basics = basicsRows[i];
    const name = String(basics.get("Brand Name") || "").trim();
    if (!name) continue;

    try {
      const profile = buildBrandProjectFitDealProfile({
        name,
        parentCompany: basics.get("Parent Company"),
        chainScale: basics.get("Hotel Chain Scale"),
        brandModel: basics.get("Brand Model"),
        architecture: basics.get("Brand Architecture"),
        serviceModel: basics.get("Hotel Service Model"),
        brandDevelopmentStage: basics.get("Brand Development Stage"),
        brandedResidencesStatus: basics.get("Branded Residences Status"),
      });
      const { basics: basicsPatch, projectFit: pfPatch } = projectFitDealProfileToAirtableFields(profile);

      if (Object.keys(basicsPatch).length) {
        if (dryRun) {
          if (i < 3 || i >= basicsRows.length - 2) {
            console.log(`[dry-run] ${name} basics (${Object.keys(basicsPatch).length} fields)`);
          }
        } else {
          await updateWithPruning(base, BASICS_TABLE, basics.id, basicsPatch);
        }
        basicsUpdated++;
      }

      const pfId = resolveProjectFitId(basics, pfIndex);
      if (!pfId) {
        if (dryRun) {
          pfCreated++;
        } else {
          await createProjectFit(base, name, basics.id, pfPatch);
          pfCreated++;
        }
      } else if (dryRun) {
        pfUpdated++;
      } else {
        await updateWithPruning(base, PF_TABLE, pfId, pfPatch);
        pfUpdated++;
      }

      if (!dryRun && (i + 1) % 25 === 0) {
        console.log(`  … ${i + 1}/${basicsRows.length}`);
        await sleep(300);
      }
    } catch (err) {
      errors++;
      console.error(`Error on ${name}: ${err.message || err}`);
    }
  }

  console.log(
    `\nDone (${dryRun ? "dry-run" : "apply"}). Basics: ${basicsUpdated}, Project Fit updated: ${pfUpdated}, created: ${pfCreated}, errors: ${errors}.`
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
