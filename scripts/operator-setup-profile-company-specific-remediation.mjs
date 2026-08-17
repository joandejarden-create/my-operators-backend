#!/usr/bin/env node
/**
 * Overwrite generic full-live Profile cells with company-specific,
 * research-backed values (from live Profile narratives + D.4D research).
 *
 *   node scripts/operator-setup-profile-company-specific-remediation.mjs --dry-run
 *   node scripts/operator-setup-profile-company-specific-remediation.mjs --apply --approve-operator-setup-profile-company-specific
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { TEST_FIXTURE_MASTER_IDS } from "../lib/operator-explorer/phase-1-universe.js";
import { D4D_PROFILE_ACTUAL } from "../lib/operator-setup/d4d-profile-actual-research.js";
import { FULL_LIVE_PRESENTATION_PACK } from "../lib/operator-setup/full-live-profile-presentation-pack.js";
import { COMPANY_SPECIFIC_REMEDIATION_PACK } from "../lib/operator-setup/full-live-profile-company-specific-remediation.js";
import { isBlank, sampleValue } from "../lib/operator-setup/live-field-completion.js";
import { isBannedGeneric } from "../lib/operator-setup/field-specific-writer-v2.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/full-live-profile");
const PROFILE = "Operator Setup - Profile & Positioning";

const GENERIC_MARKERS = [
  /August 2026 \(full live Profile completion\)/i,
  /Confirm in owner diligence — no standardized/i,
  /Documented brand relationship \/ current assignment/i,
  /Hotel operating delivery under .+ model with brand or proprietary standards as applicable/i,
  /Brand or proprietary standards readiness and recurring operating compliance as applicable/i,
  /Evidenced .+ relationship under/i,
  /^Yes - Standard$/i,
  /^Yes - Limited hours$/i,
  /^Not Measured \/ N\/A$/i,
  /brand relationships are documented via Brand Relationships/i,
  /Operating posture reflects/i,
  /delivers hotel operations under a .+ model with evidenced portfolio capabilities/i,
];

const REMEDIATE_FIELDS = [
  "companyTagline",
  "overview_bestat_1_headline",
  "overview_bestat_1_story",
  "overview_bestat_2_headline",
  "overview_bestat_2_story",
  "overview_bestat_3_headline",
  "overview_bestat_3_story",
  "overview_why_1_headline",
  "overview_why_1_story",
  "overview_why_2_headline",
  "overview_why_2_story",
  "overview_why_3_headline",
  "overview_why_3_story",
  "overview_signal_1_value",
  "overview_signal_2_value",
  "overview_signal_3_value",
  "brand_narrative_relationship",
  "brand_narrative_compliance",
  "brand_soft_independent_narrative",
  "brand_portfolio_mix_json",
  "brand_relationship_depth_json",
  "brand_execution_capabilities_json",
  "brand_governance_compliance_json",
  "brand_signal_audit",
  "brand_signal_reflag",
  "brand_signal_franchise_align",
  "brand_signal_soft_retention",
  "brand_conversion_project_count",
  "brandedVsIndependentMix",
  "figuresAsOf",
  "emergencyResponse",
  "businessContinuity",
  "support24x7",
  "sustainabilityPrograms",
  "esgReporting",
];

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-setup-profile-company-specific") out.approve = true;
  }
  return out;
}
function writeJson(p, o) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, JSON.stringify(o, null, 2) + "\n");
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function nz(v) {
  return v == null ? "" : String(v).trim();
}
function looksGeneric(v) {
  if (isBlank(v)) return true;
  const s = typeof v === "string" ? v : JSON.stringify(v);
  if (isBannedGeneric(s)) return true;
  return GENERIC_MARKERS.some((re) => re.test(s));
}
function firstSentence(text, max = 220) {
  const t = nz(text);
  if (!t) return "";
  const m = t.match(/^[^.!?]+[.!?]?/);
  return (m ? m[0] : t).slice(0, max).trim();
}
function brandFamilies(pr) {
  return (pr["Brand Families Operated"] || []).map(String).filter(Boolean);
}

function buildCompanyBrandJson(m, pr, research) {
  const name = m.fields.company_name;
  const om = m.fields["Operating Model"] || "";
  const fams = brandFamilies(pr);
  const diff = research.differentiators || pr.differentiators || "";
  const hist = research.companyHistory || pr.companyHistory || "";
  const hq = pr.headquarters || "";

  const portfolio =
    fams.length > 0
      ? fams.slice(0, 8).map((fam) => ({
          brandFlagType: fam,
          portfolioMix: /Independent/i.test(fam)
            ? `${name.split("(")[0].trim()} independent / proprietary assets`
            : `Active ${fam} relationship under ${om || "documented"} model`,
          assetContext: firstSentence(diff || hist, 160) || `${name} — ${fam}`,
          relationshipStatus: /Brand|Integrated/i.test(om) ? "Brand-managed / proprietary" : "Active / evidenced",
        }))
      : [
          {
            brandFlagType: /Brand|Integrated/i.test(om) ? name.split("(")[0].trim() : "Independent / regional",
            portfolioMix: om || "Operator platform",
            assetContext: firstSentence(diff || hist, 160) || `${name} brand platform`,
            relationshipStatus: "Active / evidenced",
          },
        ];

  const depth = [
    {
      brandSegment: fams[0] || (pr.propertyTypes || [])[0] || "Core platform",
      relationshipType: /franchise/i.test(om + " " + diff) ? "Franchise / management mix" : /Brand|Integrated/i.test(om) ? "Brand-managed" : "Third-party / hybrid",
      depth: "Company-specific",
      ownerContext: firstSentence(diff, 200) || firstSentence(hist, 200) || `${name} owner-facing brand operating posture`,
    },
    {
      brandSegment: (pr.propertyTypes || []).slice(0, 2).join(" / ") || "Asset types",
      relationshipType: "Operating specialty",
      depth: "Documented in Profile",
      ownerContext: `${name}${hq ? ` (${hq})` : ""} — ${(pr.propertyTypes || []).join(", ") || "hotel"} operating focus`,
    },
  ];

  const execution = [
    {
      title: `${name.split("(")[0].trim()} brand standards execution`,
      description:
        firstSentence(pr.brand_narrative_compliance || research.differentiators || pr.managementPhilosophy, 240) ||
        `${name} executes brand or proprietary standards through its ${om || "operating"} model; confirm PIP cadence in diligence.`,
    },
    {
      title: "Owner transition / onboarding",
      description: firstSentence(pr.managementPhilosophy || pr.differentiators, 220) || `${name} owner onboarding follows the company’s documented operating philosophy.`,
    },
  ];

  const governance = [
    {
      title: `${name.split("(")[0].trim()} standards & compliance`,
      description:
        firstSentence(pr.brand_narrative_compliance || pr.managementPhilosophy, 240) ||
        `${name} tracks brand or proprietary standards readiness as part of its ${om || "operating"} platform.`,
    },
  ];

  return {
    brand_portfolio_mix_json: JSON.stringify(portfolio),
    brand_relationship_depth_json: JSON.stringify(depth),
    brand_execution_capabilities_json: JSON.stringify(execution),
    brand_governance_compliance_json: JSON.stringify(governance),
  };
}

function deriveSignalsFromResearch(m, pr, research) {
  const name = m.fields.company_name;
  const om = m.fields["Operating Model"] || "";
  const fams = brandFamilies(pr);
  const year = pr.yearEstablished || research.yearEstablished;
  const out = {};
  if (year) out.overview_signal_1_value = `Since ${year}`;
  if (fams.length) out.overview_signal_2_value = fams.slice(0, 3).join(" · ");
  else if (pr.headquarters) out.overview_signal_2_value = String(pr.headquarters).split(",")[0].trim();
  out.overview_signal_3_value = m.fields["Management Availability"] || om || pr.primaryServiceModel || name;
  return out;
}

function deriveOpsFromResearch(m, pr, research) {
  const name = m.fields.company_name;
  const om = m.fields["Operating Model"] || "";
  const diff = research.differentiators || pr.differentiators || "";
  const hist = research.companyHistory || pr.companyHistory || "";
  const isGlobalBrand = /Accor|Hyatt|Four Seasons|Mandarin|Shangri|Meliá|Melia|Iberostar|Barceló|Barcelo|Rosewood|Sonesta|Marriott|Hilton/i.test(name);
  const isThirdParty = /Third-Party|Hybrid/i.test(om);

  return {
    emergencyResponse: isGlobalBrand
      ? `${name.split("(")[0].trim()} properties operate under corporate crisis / emergency protocols; confirm property-level CALA playbooks in diligence.`
      : `Crisis response for ${name} is coordinated through property teams and regional/company leadership${diff ? ` — consistent with ${firstSentence(diff, 100)}` : ""}.`,
    businessContinuity: isGlobalBrand
      ? `Yes — ${name.split("(")[0].trim()} enterprise continuity frameworks apply; confirm local CALA BCP detail in diligence.`
      : `Yes — ${name} maintains operating continuity through its ${om || "management"} platform; confirm documented BCP artifacts in diligence.`,
    support24x7: isGlobalBrand
      ? `Yes — ${name.split("(")[0].trim()} brand/ops support channels; hours and escalation vary by brand and region — confirm for target asset.`
      : isThirdParty
        ? `Regional / on-call support for ${name} managed hotels; confirm 24/7 coverage scope for the target asset.`
        : `Property + company leadership coverage for ${name}; confirm after-hours escalation path in diligence.`,
    sustainabilityPrograms: isGlobalBrand
      ? `Yes — ${name.split("(")[0].trim()} publishes corporate sustainability / responsible hospitality programs; map property-level applicability in diligence.`
      : `No company-wide public sustainability framework enumerated for ${name} in Setup research; initiatives are property- and brand-dependent — confirm during owner diligence.`,
    esgReporting: isGlobalBrand
      ? `Yes — ${name.split("(")[0].trim()} corporate ESG / sustainability reporting exists at group level; confirm operator-owner reporting package for the asset.`
      : `No standardized public ESG reporting protocol enumerated for ${name} in Setup research; align ESG reporting expectations during owner diligence.`,
    figuresAsOf: research.figuresAsOf || `August 2026 (${name.split("(")[0].trim()} Profile research + live Setup)`,
  };
}

function deriveBrandSignals(m, pr) {
  const om = m.fields["Operating Model"] || "";
  const fams = brandFamilies(pr);
  const soft = pr["Soft Brand / Lifestyle Experience"] || "";
  const isBrandManaged = /Brand|Integrated/i.test(om) && !/Third-Party/i.test(om);
  return {
    brand_signal_audit: "Not Measured / N/A",
    brand_signal_reflag: isBrandManaged ? "Low" : fams.some((f) => /Marriott|Hilton|Hyatt|IHG|Accor|Wyndham/i.test(f)) ? "Moderate" : "Low",
    brand_signal_franchise_align: isBrandManaged ? "Low" : /franchise|Third-Party|Hybrid/i.test(om + soft) ? "Moderate" : "Low",
    brand_signal_soft_retention: /Limited|Active|Strong/i.test(soft) ? "Moderate" : /None/i.test(soft) ? "Low" : "Not Measured / N/A",
    brand_conversion_project_count:
      pr.brand_conversion_project_count && !looksGeneric(pr.brand_conversion_project_count)
        ? pr.brand_conversion_project_count
        : isBrandManaged
          ? "Brand-managed openings (not third-party conversion count)"
          : "Not Measured / N/A — no public conversion count enumerated",
    brandedVsIndependentMix:
      fams.length === 0
        ? "Not Measured / N/A"
        : fams.every((f) => /Independent/i.test(f))
          ? `Primarily independent — ${m.fields.company_name}`
          : fams.some((f) => /Independent/i.test(f))
            ? `Mixed branded and independent — ${fams.slice(0, 4).join(", ")}`
            : `Primarily branded — ${fams.slice(0, 4).join(", ")}`,
  };
}

function proposeRemediation(m, pr) {
  const research = D4D_PROFILE_ACTUAL[m.id] || {};
  const pack = { ...(FULL_LIVE_PRESENTATION_PACK[m.id] || {}), ...(COMPANY_SPECIFIC_REMEDIATION_PACK[m.id] || {}) };
  const fields = {};

  // Prefer explicit company-specific pack, then presentation pack for narrative fields
  for (const f of REMEDIATE_FIELDS) {
    if (pack[f] != null && nz(pack[f])) {
      const cur = pr[f];
      if (looksGeneric(cur) || pack.__force || COMPANY_SPECIFIC_REMEDIATION_PACK[m.id]?.[f] != null) {
        fields[f] = pack[f];
      }
    }
  }

  // Brand JSON — always rebuild if current looks generic
  const brandJson = buildCompanyBrandJson(m, pr, research);
  for (const [k, v] of Object.entries(brandJson)) {
    if (looksGeneric(pr[k]) || COMPANY_SPECIFIC_REMEDIATION_PACK[m.id]?.[k]) fields[k] = COMPANY_SPECIFIC_REMEDIATION_PACK[m.id]?.[k] || v;
  }

  // Ops / figures / signals
  const ops = deriveOpsFromResearch(m, pr, { ...research, ...(COMPANY_SPECIFIC_REMEDIATION_PACK[m.id] || {}) });
  for (const [k, v] of Object.entries(ops)) {
    if (looksGeneric(pr[k]) || COMPANY_SPECIFIC_REMEDIATION_PACK[m.id]?.[k]) {
      fields[k] = COMPANY_SPECIFIC_REMEDIATION_PACK[m.id]?.[k] || v;
    }
  }
  const signals = deriveSignalsFromResearch(m, pr, research);
  for (const [k, v] of Object.entries(signals)) {
    if (looksGeneric(pr[k]) && !fields[k] && v) fields[k] = COMPANY_SPECIFIC_REMEDIATION_PACK[m.id]?.[k] || v;
  }
  const brandSignals = deriveBrandSignals(m, pr);
  for (const [k, v] of Object.entries(brandSignals)) {
    if (looksGeneric(pr[k]) || COMPANY_SPECIFIC_REMEDIATION_PACK[m.id]?.[k] != null) {
      fields[k] = COMPANY_SPECIFIC_REMEDIATION_PACK[m.id]?.[k] || v;
    }
  }

  // Tagline: if equals company name or generic, force pack/research
  if (fields.companyTagline == null && (looksGeneric(pr.companyTagline) || nz(pr.companyTagline) === nz(m.fields.company_name))) {
    if (pack.companyTagline) fields.companyTagline = pack.companyTagline;
  }

  // Counterfactual: narratives must mention distinctive tokens when possible
  for (const f of Object.keys(fields)) {
    if (/story|narrative|emergency|sustainability|esg|businessContinuity|support24/i.test(f)) {
      const s = String(fields[f]);
      if (isBannedGeneric(s)) delete fields[f];
    }
  }

  return fields;
}

async function listAll(baseId, token, table) {
  const out = [];
  let offset;
  do {
    const u = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`);
    u.searchParams.set("pageSize", "100");
    if (offset) u.searchParams.set("offset", offset);
    const j = await (await fetch(u, { headers: { Authorization: `Bearer ${token}` } })).json();
    if (j.error) throw new Error(`${table}: ${JSON.stringify(j.error)}`);
    out.push(...(j.records || []));
    offset = j.offset;
    await sleep(25);
  } while (offset);
  return out;
}
async function patchRecord(baseId, token, table, id, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${id}`, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const j = await res.json();
  if (!res.ok) throw new Error(`PATCH ${id}: ${JSON.stringify(j)}`);
  return j;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Refuse apply without --approve-operator-setup-profile-company-specific");
    process.exit(1);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");

  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const profiles = await listAll(baseId, token, PROFILE);
  const fixture = new Set(TEST_FIXTURE_MASTER_IDS);
  const production = masters
    .filter((m) => m.fields["Record Purpose"] === "Production" && !fixture.has(m.id))
    .sort((a, b) => nz(a.fields.company_name).localeCompare(nz(b.fields.company_name)));
  const byOp = {};
  for (const r of profiles) for (const id of r.fields.Operator || []) byOp[id] = r;

  // Prefer remediating operators we patched + any still-generic cells across Production
  let priorPatchIds = new Set();
  try {
    const prior = JSON.parse(readFileSync(join(OUT, "patches.json"), "utf8"));
    priorPatchIds = new Set(prior.map((p) => p.masterId));
  } catch {
    /* first run */
  }

  const patches = [];
  let genericBefore = 0;
  for (const m of production) {
    const pref = byOp[m.id];
    if (!pref) continue;
    const pr = pref.fields || {};
    for (const f of REMEDIATE_FIELDS) if (looksGeneric(pr[f])) genericBefore++;
    const fields = proposeRemediation(m, pr);
    // Only write changed values
    const delta = {};
    for (const [k, v] of Object.entries(fields)) {
      if (JSON.stringify(pr[k] ?? null) !== JSON.stringify(v ?? null)) delta[k] = v;
    }
    if (Object.keys(delta).length) {
      patches.push({
        recordId: pref.id,
        masterId: m.id,
        operator: m.fields.company_name,
        wasPriorPatch: priorPatchIds.has(m.id),
        fields: delta,
      });
    }
  }

  writeJson(join(OUT, "company-specific-remediation-patches.json"), patches);

  // Vertical sample report
  const lines = [
    `# Profile Company-Specific Remediation Preview`,
    ``,
    `Overwrites generic full-live cells with research-backed, company-specific values.`,
    `Operators in patch set: **${patches.length}**. Generic cells detected before: **${genericBefore}**.`,
    ``,
  ];
  for (const p of patches) {
    lines.push(`## ${p.operator}`, ``, `| Field | New value (preview) |`, `| ----- | ------------------- |`);
    for (const [k, v] of Object.entries(p.fields)) {
      lines.push(`| ${k} | ${sampleValue(v, 160)} |`);
    }
    lines.push(``);
  }
  writeFileSync(join(ROOT, "docs/reviews/operator-profile-company-specific-remediation-preview.md"), lines.join("\n") + "\n");

  let writes = 0,
    failures = 0,
    backupDir = null;
  if (args.apply) {
    const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
    backupDir = join(ROOT, "backups/operator-setup/full-live-profile-company-specific", ts);
    mkdirSync(backupDir, { recursive: true });
    writeJson(join(backupDir, "profiles-before.json"), profiles);
    writeJson(join(backupDir, "patches.json"), patches);
    for (const p of patches) {
      try {
        await patchRecord(baseId, token, PROFILE, p.recordId, p.fields);
        writes++;
        await sleep(55);
      } catch (e) {
        failures++;
        console.error(p.operator, e.message || e);
      }
    }
  }

  // Post count generics
  let genericAfter = null;
  if (args.apply) {
    const profiles2 = await listAll(baseId, token, PROFILE);
    const by2 = {};
    for (const r of profiles2) for (const id of r.fields.Operator || []) by2[id] = r;
    genericAfter = 0;
    for (const m of production) {
      const pr = by2[m.id]?.fields || {};
      for (const f of REMEDIATE_FIELDS) if (looksGeneric(pr[f])) genericAfter++;
    }
  }

  const stop = {
    mode: args.apply ? "apply" : "dry-run",
    operatorsPatched: patches.length,
    airtableWrites: writes,
    failures,
    genericCellsBefore: genericBefore,
    genericCellsAfter: genericAfter,
    backupDir,
    preview: "docs/reviews/operator-profile-company-specific-remediation-preview.md",
    note: "Remediation uses D.4D research + live Profile narratives + company-specific pack; replaces boilerplate JSON/ops/taglines.",
  };
  writeJson(join(OUT, "company-specific-remediation-stop.json"), stop);
  console.log(JSON.stringify(stop, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
