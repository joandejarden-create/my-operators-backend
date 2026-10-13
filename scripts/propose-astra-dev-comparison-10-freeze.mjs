#!/usr/bin/env node
/** Offline cohort proposal helper — no paid calls. */
import fs from "node:fs";
import crypto from "node:crypto";
import { execSync } from "node:child_process";

const freeze20 = JSON.parse(fs.readFileSync("reports/ci-20hotel-e2e-freeze.json", "utf8"));
const man = JSON.parse(fs.readFileSync("reports/contact-intelligence-benchmark-real-v1.2.json", "utf8"));
const src = fs.readFileSync("scripts/ci-20hotel-e2e-development-experiment.mjs", "utf8");
const excl = new Set([...src.matchAll(/"(rec[A-Za-z0-9]{14})"/g)].map((m) => m[1]));
for (const h of freeze20.hotels || []) excl.add(h.hotel_id);

for (const p of [
  "reports/surfe-development-owner-discovery-freeze.json",
  "reports/surfe-pro-smoke-dovetail-fibrahotel.json",
]) {
  if (!fs.existsSync(p)) continue;
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  for (const h of j.hotels || j.cohort || []) {
    if (h.hotel_id) excl.add(h.hotel_id);
  }
}

const hotels = man.hotels || [];
const heldOut = new Set(
  hotels.filter((h) => h.held_out_reserved || h.split === "held_out").map((h) => h.hotel_id)
);
heldOut.forEach((id) => excl.add(id));

const fixedTen = new Set((man.fixed_comparison_ten || man.fixed_ten || []).map((h) => h.hotel_id || h));

const eligible = hotels.filter(
  (h) =>
    h.split === "development" &&
    !h.held_out_reserved &&
    !excl.has(h.hotel_id) &&
    !fixedTen.has(h.hotel_id) &&
    h.hotel_id &&
    h.hotel_name &&
    !h.synthetic
);

/** Research language from geography — manifesto language can mislabel EN Caribbean as es. */
function researchLang(h) {
  const c = String(h.country || "").toLowerCase();
  if (/brazil|portugal/.test(c)) return "pt";
  if (
    /mexico|spain|argentina|chile|colombia|peru|dominican|cuba|puerto rico|costa rica|panama|guatemala|honduras|nicaragua|el salvador|venezuela|ecuador|bolivia|paraguay|uruguay/.test(
      c
    )
  ) {
    return "es";
  }
  // Anguilla, Antigua, Jamaica, Barbados, Bahamas, Trinidad, Belize, etc.
  return "en";
}

function stratumGuess(h) {
  const name = String(h.hotel_name || "");
  const web = String(h.census?.website || h.website || "");
  const blob = `${name} ${web}`;
  if (
    /marriott|hilton|ihg|hyatt|accor|ibis|novotel|wyndham|radisson|meli[aá]|barcel[oó]|riu|iberostar|four seasons|slh|autograph|curio|tribute|sheraton|westin|courtyard|holiday inn|intercontinental|crowne|doubletree|hampton|embassy|aloft|le meridien|st\.?\s*regis|ritz|fairmont|sofitel|pullman|mercure|voco|kimpton|indigo|belmond|auberge|oetker|rosewood|mandarin oriental|peninsula|shangri|conrad|waldorf|edition|six senses|anantara|langham|hard rock|beach park/i.test(
      blob
    )
  ) {
    return "portfolio_business_signal";
  }
  return "independent_private_signal";
}

const buckets = { en: [], es: [], pt: [] };
for (const h of eligible) {
  const language = researchLang(h);
  if (!buckets[language]) buckets[language] = [];
  buckets[language].push({
    hotel_id: h.hotel_id,
    hotel_name: h.hotel_name,
    country: h.country || null,
    city: h.city || null,
    language,
    manifesto_language: h.language || null,
    language_basis:
      h.language && h.language !== language
        ? `country_inferred_${language}_overrides_manifesto_${h.language}`
        : h.language
          ? "manifesto_language"
          : `country_inferred_${language}`,
    official_website: h.census?.website || null,
    owner_group: h.owner_group || null,
    owner_group_status: h.owner_group_status || "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
    stratum_sampling_label: stratumGuess(h),
    split: h.split,
  });
}

function pickDiverse(arr, n) {
  const port = arr.filter((x) => x.stratum_sampling_label === "portfolio_business_signal");
  const indep = arr.filter((x) => x.stratum_sampling_label !== "portfolio_business_signal");
  const out = [];
  // Prefer at least one portfolio when available, then fill with independents
  for (const x of port) {
    if (out.length >= Math.min(2, n)) break;
    out.push(x);
  }
  for (const x of indep) {
    if (out.length >= n) break;
    out.push(x);
  }
  for (const x of port) {
    if (out.length >= n) break;
    if (out.some((o) => o.hotel_id === x.hotel_id)) continue;
    out.push(x);
  }
  return out;
}

const targets = { pt: 4, en: 4, es: 2 };
const selected = [];
const gaps = [];

for (const [lang, n] of Object.entries(targets)) {
  const pool = buckets[lang] || [];
  if (pool.length === 0) {
    gaps.push({
      language: lang,
      requested: n,
      available: 0,
      note: "No eligible DEVELOPMENT hotels remain after held-out/EXCLUDE/prior-20/fixed-ten filters. Not padded.",
    });
    continue;
  }
  const take = Math.min(n, pool.length);
  selected.push(...pickDiverse(pool, take));
  if (pool.length < n) {
    gaps.push({
      language: lang,
      requested: n,
      available: pool.length,
      note: `Shortfall ${n - pool.length}; not padded with synthetic identities.`,
    });
  }
}

// Fill only within available languages if under 10 and pool remains
if (selected.length < 10) {
  const used = new Set(selected.map((s) => s.hotel_id));
  const remainder = eligible
    .map((h) => {
      const language = researchLang(h);
      return {
        hotel_id: h.hotel_id,
        hotel_name: h.hotel_name,
        country: h.country || null,
        city: h.city || null,
        language,
        manifesto_language: h.language || null,
        language_basis:
          h.language && h.language !== language
            ? `country_inferred_${language}_overrides_manifesto_${h.language}`
            : h.language
              ? "manifesto_language"
              : `country_inferred_${language}`,
        official_website: h.census?.website || null,
        owner_group: h.owner_group || null,
        owner_group_status: h.owner_group_status || "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
        stratum_sampling_label: stratumGuess(h),
        split: h.split,
      };
    })
    .filter((h) => !used.has(h.hotel_id));
  // Prefer portfolio then underrepresented language
  remainder.sort((a, b) => {
    const ap = a.stratum_sampling_label === "portfolio_business_signal" ? 0 : 1;
    const bp = b.stratum_sampling_label === "portfolio_business_signal" ? 0 : 1;
    if (ap !== bp) return ap - bp;
    return a.language.localeCompare(b.language);
  });
  for (const h of remainder) {
    if (selected.length >= 10) break;
    selected.push(h);
  }
}

const commit = execSync("git rev-parse HEAD", { encoding: "utf8" }).trim();
const codeFiles = [
  "lib/context-dev/client.js",
  "lib/hotel-intelligence/contact-intelligence/fullenrich-gated-submit.js",
  "lib/hotel-intelligence/contact-intelligence/owner-person-enrichment-gate.js",
  "lib/hotel-intelligence/contact-intelligence/ownership-contact-research-handoff.js",
  "lib/hotel-intelligence/contact-intelligence/ownership-research-planning.js",
  "lib/hotel-intelligence/contact-intelligence/research-evidence.js",
];
const hashes = {};
for (const f of codeFiles) {
  if (!fs.existsSync(f)) continue;
  hashes[f] = crypto.createHash("sha256").update(fs.readFileSync(f)).digest("hex");
}

const languages_selected = selected.reduce((a, h) => {
  a[h.language] = (a[h.language] || 0) + 1;
  return a;
}, {});
const strata_selected = selected.reduce((a, h) => {
  a[h.stratum_sampling_label] = (a[h.stratum_sampling_label] || 0) + 1;
  return a;
}, {});

const out = {
  version: "astra-dev-comparison-10-freeze-v1",
  status: "PROPOSED_NOT_EXECUTED",
  created_at: new Date().toISOString(),
  baseline_commit_at_proposal: commit,
  note:
    "Discovery-only comparison plan. No expected owners or helpful owner URLs in research inputs. Sampling stratum is a label only — not an ownership conclusion.",
  exclusions: {
    held_out_excluded: true,
    prior_20hotel_e2e_excluded: true,
    experiment_EXCLUDE_IDS_applied: true,
    fixed_comparison_ten_excluded: true,
    synthetic_excluded: true,
    owner_group_separation:
      "UNCERTAIN — provisional_unassigned for selected rows; do not claim disjoint owner groups",
  },
  languages_target: targets,
  languages_selected,
  language_gaps: gaps,
  strata_selected,
  cohort_limitations: [
    gaps.length
      ? `Language shortfall after exclusions: ${gaps.map((g) => g.language).join(", ")}`
      : null,
    strata_selected.portfolio_business_signal
      ? null
      : "No brand/soft-brand name signals remained in eligible pool for portfolio_business_signal (or none selected).",
    "Manifesto language labels Anguilla/Antigua as es; freeze uses country-inferred en for research language.",
  ].filter(Boolean),
  hotels: selected.map((h, i) => ({
    order: i + 1,
    hotel_id: h.hotel_id,
    hotel_name: h.hotel_name,
    country: h.country,
    city: h.city,
    language: h.language,
    manifesto_language: h.manifesto_language,
    language_basis: h.language_basis,
    official_website: h.official_website,
    owner_group: h.owner_group,
    owner_group_status: h.owner_group_status,
    stratum_sampling_label: h.stratum_sampling_label,
    split: h.split,
    selection_reason: `DEVELOPMENT split; research_lang=${h.language} (${h.language_basis}); stratum_label=${h.stratum_sampling_label}; not in held-out/EXCLUDE/prior-20/fixed-ten; not synthetic`,
    research_inputs_allowed: [
      "hotel_id",
      "hotel_name",
      "aliases_if_census",
      "address",
      "country",
      "city",
      "coordinates",
      "official_property_website_if_known",
    ],
    research_inputs_forbidden: [
      "expected_owner_name",
      "owner_domain_hints",
      "curated_ownership_urls",
      "prior_surfe_people",
    ],
  })),
  eligible_pool_size: eligible.length,
  bucket_sizes: {
    en: buckets.en.length,
    es: buckets.es.length,
    pt: buckets.pt.length,
  },
  code_file_sha256_at_proposal: hashes,
  evaluation_rules: {
    arms: ["pre_astra_discovery", "patched_astra_discovery"],
    discovery_only: true,
    enrichment: false,
    canonical_writes: false,
    customer_publication: false,
    unsafe_enrichment_gates: "MUST_NOT_RESTORE",
    identical_budgets: true,
    pre_astra_ref: "Use parent of Astra integration or tagged pre-Astra tip for arm A; never restore old enrichment gates",
  },
  proposed_budgets: {
    context_dev_credits_total: 50,
    context_dev_credits_per_hotel_cap: 5,
    context_dev_credits_per_arm: 50,
    serpapi: 0,
    surfe_email: 0,
    surfe_mobile: 0,
    surfe_search: 0,
    fullenrich: 0,
    webhound: 0,
    apify: 0,
    note: "Both arms share identical per-hotel (5) and total (50) Context caps. No enrichment credits. Two arms ⇒ up to 100 Context credits if both execute.",
  },
  prerequisites: [
    "CONTEXT_DEV_API_KEY configured",
    "AIRTABLE_BASE_ID_ALT + token for Census identity refresh (optional if freeze census fields are sufficient)",
    "Patched commit checked out for arm B (baseline_commit_at_proposal)",
    "Pre-Astra commit/tag available for arm A without restoring unsafe enrichment gates",
    "Offline gate tests PASS on arm B: test-astra-ownership-review, test-owner-person-enrichment-gate, test-astra-handoff-offline",
    "Runner scripts astra-dev-comparison-10-run.mjs / score.mjs not yet created — create only at execution",
    "Independent reviewer worksheet prepared",
    gaps.some((g) => g.language === "es" && g.available === 0)
      ? "Spanish-country DEVELOPMENT hotels exhausted by prior cohorts — expand eligible DEVELOPMENT pool before claiming trilingual coverage"
      : null,
  ].filter(Boolean),
  reviewer_rubric: {
    property_identity:
      "Confirm hotel_id / name / geo match the intended Census property; reject wrong-property hits",
    owner_sponsor:
      "PROPERTY_OWNER or ECONOMIC_OWNER_OR_SPONSOR with passage + URL + claim date; brand/operator alone fail",
    currentness:
      "State claim date and whether later sale/change was checked; absence of later evidence ≠ continued ownership",
    org_domain:
      "Domain confirmed by identity evidence for the same org — not sibling/related-domain resemblance",
    person_affiliation:
      "Relevant role + independent affiliation evidence to the owner/sponsor org; Surfe-only or name-match-only fails",
  },
  persistence_required: [
    "queries",
    "ranked_results",
    "fetch_failures",
    "document_passages",
    "source_urls",
    "content_hashes",
    "cost_records",
    "labels_for_independent_discovery_vs_manual_followup_vs_portfolio_reuse",
  ],
  comparison_commands_planned: {
    note: "Do not execute until approved. Runners are planned filenames.",
    arm_a:
      "node scripts/astra-dev-comparison-10-run.mjs --arm=pre_astra --freeze=reports/astra-dev-comparison-10-freeze-proposed-v1.json --context-total=50 --per-hotel=5 --discovery-only",
    arm_b:
      "node scripts/astra-dev-comparison-10-run.mjs --arm=patched_astra --freeze=reports/astra-dev-comparison-10-freeze-proposed-v1.json --context-total=50 --per-hotel=5 --discovery-only",
    score:
      "node scripts/astra-dev-comparison-10-score.mjs --a=reports/astra-dev-comparison-10-pre.json --b=reports/astra-dev-comparison-10-patched.json",
  },
};

fs.mkdirSync("reports", { recursive: true });
fs.writeFileSync(
  "reports/astra-dev-comparison-10-freeze-proposed-v1.json",
  JSON.stringify(out, null, 2)
);

const md = [];
md.push("# Astra DEVELOPMENT comparison — 10-hotel freeze (PROPOSED, not executed)");
md.push("");
md.push(`**Status:** PROPOSED_NOT_EXECUTED · ${out.created_at}`);
md.push(`**Baseline commit at proposal:** \`${commit}\``);
md.push("");
md.push("## Cohort");
md.push("");
md.push("| # | Hotel ID | Name | Country | Lang | Lang basis | Stratum label | Owner-group status |");
md.push("|---|---|---|---|---|---|---|---|");
for (const h of out.hotels) {
  md.push(
    `| ${h.order} | \`${h.hotel_id}\` | ${h.hotel_name} | ${h.country || "—"} | ${h.language} | ${h.language_basis} | ${h.stratum_sampling_label} | ${h.owner_group_status} |`
  );
}
md.push("");
md.push(`Languages selected: ${JSON.stringify(out.languages_selected)}`);
md.push(`Strata labels: ${JSON.stringify(out.strata_selected)}`);
md.push(`Eligible pool after exclusions: **${out.eligible_pool_size}**`);
md.push(`Bucket sizes: ${JSON.stringify(out.bucket_sizes)}`);
md.push("");
if (out.language_gaps.length) {
  md.push("## Language gaps (not padded)");
  for (const g of out.language_gaps) {
    md.push(`- **${g.language}:** requested ${g.requested}, available ${g.available} — ${g.note}`);
  }
  md.push("");
}
if (out.cohort_limitations.length) {
  md.push("## Cohort limitations");
  for (const c of out.cohort_limitations) md.push(`- ${c}`);
  md.push("");
}
md.push("## Exclusions");
md.push("- Held-out reserved hotels");
md.push("- Prior 20-hotel e2e freeze");
md.push("- Experiment `EXCLUDE_IDS` (held-out + prior cohorts)");
md.push("- Fixed comparison ten");
md.push("- Synthetic identities");
md.push("- **Owner-group separation: UNCERTAIN** (provisional_unassigned)");
md.push("");
md.push("## Research inputs");
md.push("Allowed: hotel identity + geo + official property website if known.");
md.push("Forbidden: expected owner names, owner domain hints, curated ownership URLs, prior Surfe people.");
md.push("");
md.push("## Budgets (proposed — not spent)");
md.push("```json");
md.push(JSON.stringify(out.proposed_budgets, null, 2));
md.push("```");
md.push("");
md.push("## Comparison commands (do not run until approved)");
md.push("");
md.push("```bash");
md.push(`# Arm A — pre-Astra discovery`);
md.push(`# ${out.comparison_commands_planned.arm_a}`);
md.push("");
md.push(`# Arm B — patched discovery (commit ${commit.slice(0, 7)})`);
md.push(`# ${out.comparison_commands_planned.arm_b}`);
md.push("");
md.push(`# Score offline after both arms complete (no enrichment)`);
md.push(`# ${out.comparison_commands_planned.score}`);
md.push("```");
md.push("");
md.push("Both arms: discovery-only; Surfe/FullEnrich/SerpAPI/Webhound/Apify = 0; never restore unsafe enrichment gates.");
md.push("");
md.push("## Reviewer assessment");
for (const [k, v] of Object.entries(out.reviewer_rubric)) md.push(`- **${k}:** ${v}`);
md.push("");
md.push("## Persistence");
md.push(out.persistence_required.map((x) => `- ${x}`).join("\n"));
md.push("");
md.push("## Prerequisites");
for (const p of out.prerequisites) md.push(`- ${p}`);
md.push("");
md.push("Do **not** claim coverage improvement until both arms run and are independently reviewed.");

fs.writeFileSync("reports/astra-dev-comparison-10-freeze-proposed-v1.md", md.join("\n"));
console.log(
  JSON.stringify(
    {
      selected: selected.length,
      languages: languages_selected,
      strata: strata_selected,
      eligible: eligible.length,
      buckets: { en: buckets.en.length, es: buckets.es.length, pt: buckets.pt.length },
      gaps,
      ids: selected.map((h) => h.hotel_id),
      commit,
    },
    null,
    2
  )
);
