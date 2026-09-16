#!/usr/bin/env node
/** Offline cohort proposal helper — no paid calls. */
import fs from "node:fs";
import crypto from "node:crypto";
import { execSync } from "node:child_process";

const freeze20 = JSON.parse(fs.readFileSync("reports/ci-20hotel-e2e-freeze.json", "utf8"));
const man = JSON.parse(fs.readFileSync("reports/contact-intelligence-benchmark-real-v1.2.json", "utf8"));
const src = fs.readFileSync("scripts/ci-20hotel-e2e-development-experiment.mjs", "utf8");
const excl = new Set(
  [...src.matchAll(/"(rec[A-Za-z0-9]{14})"/g)].map((m) => m[1])
);
for (const h of freeze20.hotels || []) excl.add(h.hotel_id);

// Prior researched org domains / hotels from surfe freezes
const priorOwnerHints = [];
for (const p of [
  "reports/surfe-development-owner-discovery-freeze.json",
  "reports/surfe-pro-smoke-dovetail-fibrahotel.json",
]) {
  if (!fs.existsSync(p)) continue;
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  for (const h of j.hotels || j.cohort || []) {
    if (h.hotel_id) excl.add(h.hotel_id);
    if (h.owner_domain || h.domain) priorOwnerHints.push(String(h.owner_domain || h.domain).toLowerCase());
  }
}

const hotels = man.hotels || [];
const heldOut = new Set(
  hotels.filter((h) => h.held_out_reserved || h.split === "held_out").map((h) => h.hotel_id)
);
heldOut.forEach((id) => excl.add(id));

const fixedTen = new Set(
  (man.fixed_comparison_ten || man.fixed_ten || []).map((h) => h.hotel_id || h)
);

const eligible = hotels.filter(
  (h) =>
    h.split === "development" &&
    !h.held_out_reserved &&
    !excl.has(h.hotel_id) &&
    !fixedTen.has(h.hotel_id) &&
    h.hotel_id &&
    h.hotel_name
);

function langOf(h) {
  if (h.language) return h.language;
  const c = String(h.country || "");
  if (/brazil|portugal/i.test(c)) return "pt";
  if (/mexico|spain|argentina|chile|colombia|peru|dominican|cuba|puerto/i.test(c)) return "es";
  return "en";
}

function stratumGuess(h) {
  const name = String(h.hotel_name || "");
  const web = String(h.census?.website || h.website || "");
  if (
    /marriott|hilton|ihg|hyatt|accor|ibis|novotel|wyndham|radisson|meli[aá]|barcel[oó]|riu|iberostar|four seasons|slh|autograph|curio|tribute/i.test(
      `${name} ${web}`
    )
  ) {
    return "portfolio_business_signal";
  }
  return "independent_private_signal";
}

const buckets = { en: [], es: [], pt: [] };
for (const h of eligible) {
  const lang = langOf(h);
  if (!buckets[lang]) buckets[lang] = [];
  buckets[lang].push({
    hotel_id: h.hotel_id,
    hotel_name: h.hotel_name,
    country: h.country || null,
    city: h.city || null,
    language: lang,
    official_website: h.census?.website || null,
    owner_group: h.owner_group || null,
    owner_group_status: h.owner_group_status || "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
    stratum_sampling_label: stratumGuess(h),
    split: h.split,
  });
}

// Prefer mix: aim ~4 pt, ~3 es, ~3 en; mix strata
function pick(arr, n, preferStratum) {
  const pref = arr.filter((x) => x.stratum_sampling_label === preferStratum);
  const rest = arr.filter((x) => x.stratum_sampling_label !== preferStratum);
  const out = [];
  for (const x of [...pref, ...rest]) {
    if (out.length >= n) break;
    out.push(x);
  }
  return out;
}

const selected = [
  ...pick(buckets.pt, 4, "portfolio_business_signal"),
  ...pick(buckets.es, 3, "independent_private_signal"),
  ...pick(buckets.en, 3, "portfolio_business_signal"),
].slice(0, 10);

// If short, fill from remaining eligible
if (selected.length < 10) {
  const used = new Set(selected.map((s) => s.hotel_id));
  for (const h of eligible) {
    if (selected.length >= 10) break;
    if (used.has(h.hotel_id)) continue;
    selected.push({
      hotel_id: h.hotel_id,
      hotel_name: h.hotel_name,
      country: h.country || null,
      city: h.city || null,
      language: langOf(h),
      official_website: h.census?.website || null,
      owner_group: h.owner_group || null,
      owner_group_status: h.owner_group_status || "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
      stratum_sampling_label: stratumGuess(h),
      split: h.split,
    });
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

const out = {
  version: "astra-dev-comparison-10-freeze-v1",
  status: "PROPOSED_NOT_EXECUTED",
  created_at: new Date().toISOString(),
  baseline_commit_at_proposal: commit,
  note:
    "Discovery-only comparison plan. No expected owners or helpful owner URLs in research inputs. Sampling stratum is a label only.",
  exclusions: {
    held_out_excluded: true,
    prior_20hotel_e2e_excluded: true,
    experiment_EXCLUDE_IDS_applied: true,
    fixed_comparison_ten_excluded: true,
    owner_group_separation: "UNCERTAIN — provisional_unassigned for most rows; do not claim disjoint owner groups",
  },
  languages_target: { en: 3, es: 3, pt: 4 },
  languages_selected: selected.reduce((a, h) => {
    a[h.language] = (a[h.language] || 0) + 1;
    return a;
  }, {}),
  strata_selected: selected.reduce((a, h) => {
    a[h.stratum_sampling_label] = (a[h.stratum_sampling_label] || 0) + 1;
    return a;
  }, {}),
  hotels: selected.map((h, i) => ({
    order: i + 1,
    ...h,
    selection_reason: `DEVELOPMENT split; lang=${h.language}; stratum_label=${h.stratum_sampling_label}; not in held-out/EXCLUDE/prior-20/fixed-ten`,
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
  },
  proposed_budgets: {
    context_dev_credits_total: 50,
    context_dev_credits_per_hotel_cap: 5,
    serpapi: 0,
    surfe_email: 0,
    surfe_mobile: 0,
    surfe_search: 0,
    fullenrich: 0,
    webhound: 0,
    apify: 0,
    note: "Both arms share the same per-hotel and total Context caps. No enrichment credits.",
  },
  prerequisites: [
    "CONTEXT_DEV_API_KEY configured",
    "AIRTABLE_BASE_ID_ALT + token for Census identity refresh (optional if freeze is sufficient)",
    "Patched commit checked out for arm B; pre-Astra commit/tag or stash for arm A",
    "Offline gate tests still PASS on arm B",
    "Reviewer worksheet prepared (identity / ownership / date / domain / person)",
  ],
  reviewer_rubric: {
    property_identity: "Confirm hotel_id/name/geo match the intended Census property",
    owner_sponsor: "PROPERTY_OWNER or ECONOMIC_OWNER_OR_SPONSOR with passage+URL+date; brand/operator alone fail",
    currentness: "State claim date and whether later sale/change was checked; absence ≠ continued ownership",
    org_domain: "Domain confirmed by identity evidence for the same org — not sibling resemblance",
    person_affiliation: "Relevant role + independent affiliation evidence; Surfe-only fails",
  },
  persistence_required: [
    "queries",
    "ranked_results",
    "fetch_failures",
    "document_passages",
    "source_urls",
    "content_hashes",
    "cost_records",
    "labels_for_independent_vs_manual_vs_portfolio_reuse",
  ],
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
md.push("| # | Hotel ID | Name | Country | Lang | Stratum label | Owner-group status |");
md.push("|---|---|---|---|---|---|---|");
for (const h of out.hotels) {
  md.push(
    `| ${h.order} | \`${h.hotel_id}\` | ${h.hotel_name} | ${h.country || "—"} | ${h.language} | ${h.stratum_sampling_label} | ${h.owner_group_status} |`
  );
}
md.push("");
md.push(`Languages selected: ${JSON.stringify(out.languages_selected)}`);
md.push(`Strata labels: ${JSON.stringify(out.strata_selected)}`);
md.push(`Eligible pool after exclusions: **${out.eligible_pool_size}**`);
md.push("");
md.push("## Exclusions");
md.push("- Held-out reserved hotels");
md.push("- Prior 20-hotel e2e freeze");
md.push("- Experiment `EXCLUDE_IDS` (held-out + prior cohorts)");
md.push("- Fixed comparison ten");
md.push("- **Owner-group separation: UNCERTAIN** (provisional_unassigned)");
md.push("");
md.push("## Budgets (proposed)");
md.push("```json");
md.push(JSON.stringify(out.proposed_budgets, null, 2));
md.push("```");
md.push("");
md.push("## Comparison commands (do not run until approved)");
md.push("");
md.push("```bash");
md.push("# Arm A — pre-Astra discovery (checkout/tag the pre-integration commit)");
md.push("# node scripts/astra-dev-comparison-10-run.mjs --arm=pre_astra --freeze=reports/astra-dev-comparison-10-freeze-proposed-v1.json --context-total=50 --per-hotel=5");
md.push("");
md.push("# Arm B — patched discovery (this commit)");
md.push("# node scripts/astra-dev-comparison-10-run.mjs --arm=patched_astra --freeze=reports/astra-dev-comparison-10-freeze-proposed-v1.json --context-total=50 --per-hotel=5");
md.push("");
md.push("# Score offline after both arms complete (no enrichment)");
md.push("# node scripts/astra-dev-comparison-10-score.mjs --a=reports/astra-dev-comparison-10-pre.json --b=reports/astra-dev-comparison-10-patched.json");
md.push("```");
md.push("");
md.push("Runner scripts above are **planned** — create only when executing. Discovery-only; Surfe/FullEnrich/SerpAPI/Webhound/Apify = 0.");
md.push("");
md.push("## Reviewer assessment");
for (const [k, v] of Object.entries(out.reviewer_rubric)) md.push(`- **${k}:** ${v}`);
md.push("");
md.push("Do **not** claim coverage improvement until both arms run and are independently reviewed.");

fs.writeFileSync("reports/astra-dev-comparison-10-freeze-proposed-v1.md", md.join("\n"));
console.log(
  JSON.stringify(
    {
      selected: selected.length,
      languages: out.languages_selected,
      strata: out.strata_selected,
      eligible: eligible.length,
      ids: selected.map((h) => h.hotel_id),
    },
    null,
    2
  )
);
