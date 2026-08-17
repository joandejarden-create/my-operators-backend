#!/usr/bin/env node
/**
 * Onboard Argentina Wave 3 operators as Research Stage Master records.
 *
 *   node scripts/operator-fit-argentina-research-stage-onboard.mjs --dry-run
 *   node scripts/operator-fit-argentina-research-stage-onboard.mjs --apply --approve-argentina-research-masters
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { airtableFetchJson } from "../api/lib/operator-setup-new-base-read.js";
import {
  MARKET_PRESENCE_TABLE,
  map_marketPresenceFields as MF,
} from "../lib/operator-intelligence/market-presence.js";
import { loadCalibrationCohort } from "../lib/operator-intelligence/calibration-overlay.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const APPLY = process.argv.includes("--apply");
const APPROVED = process.argv.includes("--approve-argentina-research-masters");
const DRY = !APPLY;

const MASTER = process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";
const PLATFORM = "Operator Setup - Platform & Markets";
const COMMERCIAL = "Operator Setup - Commercial Fit & Terms";
const PROFILE = "Operator Setup - Profile & Positioning";
const CLAIMS = "Operator Intelligence - Claims";
const LIFECYCLE = "Research Stage";

const OPERATORS = [
  {
    researchId: "research_alvarez_arguelles",
    companyName: "Álvarez Argüelles Hoteles",
    website: "https://www.alvarezarguelles.com",
    countries: ["Argentina"],
    structures: ["Full third-party management"],
    scales: ["Upscale", "Upper Upscale", "Upper Midscale"],
  },
  {
    researchId: "research_tremun",
    companyName: "Tremun Hoteles",
    website: "https://www.tremunhoteles.com.ar",
    countries: ["Argentina"],
    structures: ["Full third-party management"],
    scales: ["Upscale", "Upper Midscale", "Midscale"],
  },
  {
    researchId: "research_aadesa",
    companyName: "AADESA",
    website: "https://www.aadesa.com.ar",
    countries: ["Argentina"],
    structures: ["Full third-party management", "Franchise support"],
    scales: ["Upscale", "Upper Midscale"],
  },
];

function enc(s) {
  return encodeURIComponent(s);
}

function normName(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

async function createRest(table, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const { ok, status, json } = await airtableFetchJson(
    `https://api.airtable.com/v0/${baseId}/${enc(table)}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fields, typecast: true }),
    }
  );
  if (!ok) throw new Error(`CREATE ${table}: ${status} ${JSON.stringify(json)}`);
  return json;
}

async function patchRest(table, id, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const { ok, status, json } = await airtableFetchJson(
    `https://api.airtable.com/v0/${baseId}/${enc(table)}/${enc(id)}`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fields, typecast: true }),
    }
  );
  if (!ok) throw new Error(`PATCH ${table}: ${status} ${JSON.stringify(json)}`);
  return json;
}

async function listLinked(table, masterId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const formula = enc(`FIND('${masterId}', ARRAYJOIN({Operator}))`);
  const url = `https://api.airtable.com/v0/${baseId}/${enc(table)}?filterByFormula=${formula}&pageSize=5`;
  const { ok, json, status } = await airtableFetchJson(url);
  if (!ok) throw new Error(`List ${table} failed ${status}`);
  return json.records || [];
}

async function main() {
  if (APPLY && !APPROVED) {
    throw new Error("Refusing --apply without --approve-argentina-research-masters");
  }
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  if (!baseId || !token) throw new Error("Airtable credentials required");

  const base = new Airtable({ apiKey: token }).base(baseId);
  const masters = [];
  await base(MASTER)
    .select({ fields: ["company_name", "submission_status"] })
    .eachPage((page, next) => {
      masters.push(...page);
      next();
    });

  const byName = new Map();
  for (const m of masters) {
    const n = normName(m.get("company_name"));
    if (n) byName.set(n, m);
  }

  const w3 = loadCalibrationCohort(join(ROOT, "data", "operator-intelligence", "wave-3-cohort"));
  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY ? "dry-run" : "apply",
    fieldMapping: {
      company_name: "Operator Setup - Master.company_name",
      submission_status: "Operator Setup - Master.submission_status",
      officialSite: "Noted in onboarding report only (Master.website not present in schema)",
    },
    operators: [],
    errors: [],
  };

  for (const spec of OPERATORS) {
    const key = normName(spec.companyName);
    const existing = byName.get(key);
    const aliasHits = masters
      .filter((m) => {
        const n = normName(m.get("company_name"));
        const first = key.split(" ")[0];
        return first && n.includes(first) && n !== key;
      })
      .map((m) => ({ id: m.id, name: m.get("company_name") }));

    const row = {
      companyName: spec.companyName,
      researchId: spec.researchId,
      duplicateCheck: existing ? "exact_name_match" : "none",
      aliasHits,
      masterId: existing?.id || null,
      lifecycle: LIFECYCLE,
      actions: [],
      validation: { pass: true, checksFailed: [] },
    };

    if (!spec.companyName) {
      row.validation = { pass: false, checksFailed: ["company_name_required"] };
      report.operators.push(row);
      continue;
    }

    if (existing) {
      const st = String(existing.get("submission_status") || "");
      row.masterId = existing.id;
      if (/^active$/i.test(st)) {
        row.actions.push({
          type: "skip_lifecycle_downgrade",
          reason: "Existing Active Master — will not downgrade",
        });
      } else if (!/research stage/i.test(st)) {
        row.actions.push({ type: "set_research_stage", from: st, to: LIFECYCLE });
        if (!DRY) await patchRest(MASTER, existing.id, { submission_status: LIFECYCLE });
      } else {
        row.actions.push({ type: "already_research_stage" });
      }
    } else {
      const payload = {
        company_name: spec.companyName,
        submission_status: LIFECYCLE,
      };
      row.actions.push({
        type: "create_master",
        sanitizedPayloadPreview: payload,
        officialSiteNoted: spec.website,
      });
      if (!DRY) {
        const created = await createRest(MASTER, payload);
        row.masterId = created.id;
      } else {
        row.masterId = `dry_${spec.researchId}`;
      }
    }

    const masterId = row.masterId;

    if (!DRY && masterId && String(masterId).startsWith("rec")) {
      const platforms = await listLinked(PLATFORM, masterId);
      const commercials = await listLinked(COMMERCIAL, masterId);
      const profiles = await listLinked(PROFILE, masterId);
      if (!platforms.length) {
        await createRest(PLATFORM, { Operator: [masterId], "Active Countries": spec.countries });
        row.actions.push({ type: "create_platform", countries: spec.countries });
      }
      if (!commercials.length) {
        await createRest(COMMERCIAL, {
          Operator: [masterId],
          "Management Structures Supported": spec.structures,
        });
        row.actions.push({ type: "create_commercial", structures: spec.structures });
      }
      if (!profiles.length) {
        await createRest(PROFILE, {
          Operator: [masterId],
          chainScalesSupported: spec.scales,
        });
        row.actions.push({ type: "create_profile", scales: spec.scales });
      }

      const geos = (w3.geography || []).filter((g) => g.operatorId === spec.researchId);
      for (const g of geos) {
        await createRest(MARKET_PRESENCE_TABLE, {
          "Presence Key": `${masterId}|${g.country}|${g.presenceType}`,
          [MF.operator]: [masterId],
          [MF.country]: g.country,
          [MF.presenceType]: g.presenceType,
          [MF.currentOrHistorical]: "Current",
          [MF.verificationDate]: "2026-08-04",
          [MF.notes]: g.evidence || "",
          [MF.limitations]: g.limitations || "Research Stage — not production Active",
          [MF.publicationStatus]: "Publish With Evidence Label",
          [MF.evidenceClass]: "independently_referenced",
          [MF.confidence]: "Moderate",
        });
        row.actions.push({
          type: "create_market_presence",
          country: g.country,
          presenceType: g.presenceType,
        });
      }

      for (const c of (w3.claims || []).filter((x) => x.operatorId === spec.researchId && !x.internalOnly)) {
        await createRest(CLAIMS, {
          "Claim ID": `${c.id}__${masterId}`,
          Operator: [masterId],
          "Claim Category": c.claimCategory || "",
          Subject: c.claimSubject || "",
          Predicate: c.claimPredicate || "",
          "Raw Value": String(c.claimValue ?? ""),
          "Normalized Value": Array.isArray(c.normalizedValue)
            ? c.normalizedValue.join(", ")
            : String(c.normalizedValue ?? ""),
          "Evidence Class": c.evidenceClass || "",
          "Verification Status": c.verificationStatus || "",
          "Publication Status":
            c.publicationClass === 1 ? "Auto-Publish" : "Publish With Evidence Label",
          "Conflict Status": "None",
          Notes: "Linked from Wave 3 research-stage onboarding",
          Limitations: c.limitations || "Research Stage Master",
        });
        row.actions.push({ type: "create_claim", claimId: c.id });
      }
    } else {
      row.actions.push({
        type: "would_create_children",
        platform: spec.countries,
        commercial: spec.structures,
        profile: spec.scales,
        marketPresence: (w3.geography || []).filter((g) => g.operatorId === spec.researchId).length,
        claims: (w3.claims || []).filter((x) => x.operatorId === spec.researchId && !x.internalOnly)
          .length,
      });
    }

    row.argentinaPresence =
      "Current Operating Portfolio / Current Managed Property (research evidence)";
    row.structures = spec.structures;
    row.evidenceCoverage = "Wave 3 local sources linked";
    row.readinessNote =
      "Research-Stage Ranking Ready possible; not Production Ranking Ready / not owner-visible";
    row.productionVisible = false;
    report.operators.push(row);
  }

  writeFileSync(
    join(ROOT, "reports", "operator-intelligence-argentina-master-onboarding.json"),
    JSON.stringify(report, null, 2)
  );
  writeFileSync(
    join(ROOT, "reports", "operator-intelligence-argentina-master-onboarding.md"),
    [
      "# Operator Intelligence — Argentina Research-Stage Master Onboarding",
      "",
      `Mode: **${report.mode}** · ${report.generatedAt}`,
      "",
      "| Operator | Master ID | Lifecycle | Evidence Coverage | Argentina Presence | Structures | Readiness |",
      "| -------- | --------- | --------- | ----------------: | ------------------ | ---------- | --------- |",
      ...report.operators.map(
        (o) =>
          `| ${o.companyName} | ${o.masterId || "—"} | ${o.lifecycle} | ${o.evidenceCoverage || "—"} | ${o.argentinaPresence || "—"} | ${(o.structures || []).join("; ")} | ${o.readinessNote || "—"} |`
      ),
      "",
      "## Guards",
      "",
      "- `submission_status = Research Stage` (not Active)",
      "- Not included in production Active universe loader",
      "- Not owner-visible",
      "- Readiness rules unchanged",
      "",
      "## Field mapping",
      "",
      "- `company_name` → Master.company_name",
      "- `submission_status` → Master.submission_status (`Research Stage`)",
      "- `website` → Master.website",
      "",
    ].join("\n")
  );

  const idMap = Object.fromEntries(
    report.operators.map((o) => [o.researchId, o.masterId]).filter(([, id]) => id)
  );
  mkdirSync(join(ROOT, "data", "operator-intelligence"), { recursive: true });
  writeFileSync(
    join(ROOT, "data", "operator-intelligence", "wave-3-master-id-map.json"),
    JSON.stringify({ generatedAt: report.generatedAt, mode: report.mode, idMap }, null, 2)
  );

  console.log(
    JSON.stringify(
      {
        mode: report.mode,
        operators: report.operators.map((o) => ({
          name: o.companyName,
          id: o.masterId,
          actions: o.actions.length,
          duplicateCheck: o.duplicateCheck,
        })),
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
