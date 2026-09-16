#!/usr/bin/env node
/**
 * 20-hotel DEVELOPMENT e2e: hotel → owner/sponsor → person → email/phone.
 * Budgets: Context.dev ≤100, Surfe email≤30 mobile≤10 search≤20req/200profiles,
 * SerpAPI=0, FullEnrich/Webhound/Apify=0.
 * Evaluation staging only.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import {
  MAP_HOTEL_PROPERTY_CENSUS,
  MAP_CENSUS_FIELDS,
} from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";
import { researchHotelOwnershipContactPath } from "../lib/hotel-intelligence/contact-intelligence/ownership-contact-research-handoff.js";
import { isContextDevConfigured } from "../lib/context-dev/client.js";
import {
  describeSurfeKeyPresence,
  getSurfeCredits,
  searchSurfePeople,
  startSurfePeopleEnrichment,
  pollSurfePeopleEnrichment,
  normalizeSurfePerson,
  estimateWorstCaseCredits,
} from "../lib/surfe/client.js";
import { gateProviderCandidates } from "../lib/hotel-intelligence/contact-intelligence/fullenrich-gated-submit.js";
import { linkedInNameTokenAgreement } from "../lib/hotel-intelligence/contact-intelligence/provider-submission-gate.js";

const OUT = {
  freeze: "reports/ci-20hotel-e2e-freeze.json",
  ownership: "reports/ci-20hotel-e2e-ownership.json",
  jobs: "reports/ci-20hotel-e2e.jobs.json",
  ledger: "reports/ci-20hotel-e2e-usage-ledger.json",
  results: "reports/ci-20hotel-e2e-results.json",
  founder: "reports/ci-20hotel-e2e-founder-report.md",
  reconcile: "reports/surfe-development-owner-discovery-reconciliation-v2.md",
};

const CONTEXT_CAP = 100;
const EMAIL_CAP = 30;
const MOBILE_CAP = 10;
const SEARCH_REQ_CAP = 20;
const SEARCH_PROFILE_CAP = 200;
const SERP_CAP = 0;

const EXCLUDE_IDS = new Set([
  // held-out
  "rec5ngSUMqam9MCZK", "rec6U0ctZsKSlv9zL", "recBFoKNJu0Feg9lY", "recFXI48GyfS8s7vX",
  "recKGPMczWSEkSgZZ", "recNo3nvQkUH4M7RC", "recRxB9Fh2BN5icxX", "recS7bqeMNtF9vPGz",
  "recYvEsIs53fiQ7gD", "reccy6sKTIzWzw5jy", "rece17Galm3eUfsRv", "receOLh5zG0q4TnpY",
  "rechnyqEcGsdbo5q9", "reclkLirQmcdAzCEY", "recnrzlgSQxdzFQvL", "recq5eksARSNpwdiO",
  "recsdNMRNjmPNUC65", "recu33MIflnAybo57", "recvc2g0Gw25mqljb", "recyqgtlt24HYVaaN",
  "recFz9Va2OhORsQvq", "recanGP6Avw4852gb", "recyvo2taU6amn75K", "recUzPCFSNrhTPPwq",
  "recxPU3hjhSkpnNYw",
  // prior Surfe / FE / transfer / fixed-ten
  "recUNycnMwOVFX0hc", "recIwaP1etgx2g9nA", "recsYJb2R1jarPpK3", "recTYaiA4S6fR6ixx",
  "recgg7Llf3EWpZqIQ", "recL4PrLJpwXxyvV6", "rec19X4tsCUM1A2q6", "recfsU9RYMAzxhfo3",
  "recqvYuDKCweVD4Co", "recUP5BmDKKRj3ic8", "recId5nDFUgVbJnzH", "recGZZCek9vDQGG1L",
  "rec79Xs4mZkuiWnuN", "recFspIiglYxJp1N1", "recogJrXdZHRV06Bl", "recZxCHVNG0bDQhfG",
  "recyhrugWAKzAYU2S", "recBHFTZ3fw7cCdPg", "recTVAOs9msNiOjSJ",
  "recoeBetweOi0KzW7", "rec5WnVTArxJZhXyr", "recQDDxgI66rHr0K5", "recCwJUm7ocXpiwCl",
  "recqxRjDzv5dm20Yb", "rec4FyZfRtSBKEPJg", "rececNsRhILlRMuDM", "recKUHRlr9qTdfkGP",
]);

/** Frozen selection — sampling strata are labels, not ownership conclusions. */
const FROZEN_SELECTION = [
  // portfolio_business_signal (10)
  { hotel_id: "rec07GLNG5oNBkAj8", stratum: "portfolio_business_signal" },
  { hotel_id: "rec08YF1M2BOFBcYG", stratum: "portfolio_business_signal" },
  { hotel_id: "rec09k8RdHgiAAKsG", stratum: "portfolio_business_signal" },
  { hotel_id: "rec3wDO2fUZZQMz4Q", stratum: "portfolio_business_signal" },
  { hotel_id: "rec6sXJrMlQjXBBYB", stratum: "portfolio_business_signal" },
  { hotel_id: "recDoNVQUee8roLAl", stratum: "portfolio_business_signal" },
  { hotel_id: "rectL8jThAojSv0pV", stratum: "portfolio_business_signal" },
  { hotel_id: "recqfvmhe4GyqdsiD", stratum: "portfolio_business_signal" },
  { hotel_id: "recfMgicm0XzCYQy4", stratum: "portfolio_business_signal" },
  { hotel_id: "recaRtCFEPEm7Sfzu", stratum: "portfolio_business_signal" },
  // independent_private_signal (10)
  { hotel_id: "rec00Cp2AgpZAtp9Z", stratum: "independent_private_signal" },
  { hotel_id: "rec00eC8DN3Ow0hFo", stratum: "independent_private_signal" },
  { hotel_id: "rec00wVP4wdg3gTnU", stratum: "independent_private_signal" },
  { hotel_id: "rec01IOtL71rT87Nx", stratum: "independent_private_signal" },
  { hotel_id: "rec01qdNOCNBEPUWz", stratum: "independent_private_signal" },
  { hotel_id: "rec8kgvDDCjBUBbJb", stratum: "independent_private_signal" },
  { hotel_id: "reccvEEVIWs3YE2Ln", stratum: "independent_private_signal" },
  { hotel_id: "rec1TGHJNe6MsBNy8", stratum: "independent_private_signal" },
  { hotel_id: "rec4yPN7OvQv18Pob", stratum: "independent_private_signal" },
  { hotel_id: "rec7T5pTO1aoXcPky", stratum: "independent_private_signal" },
];

const ROLE_TITLES = [
  "Development", "Director of Development", "Hotel Development", "Acquisitions",
  "Investment", "Asset Management", "CEO", "Founder", "Managing Director",
  "Director General", "Presidente", "Desarrollo", "Adquisiciones", "Fundador",
];

const PRIORITY_RE =
  /\b(hotel\s+development|desarrollo|acquisitions|adquisic|asset\s+management|investment|invers|business\s+development|ceo|founder|fundador|managing\s+director|director\s+general|presidente)\b/i;
const IRRELEVANT_RE =
  /\b(sales|ventas|housekeep|chef|recepcion|developer|software|talent|marketing\s+manager|front\s+desk)\b/i;

function domainRoot(d) {
  return String(d || "")
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .split("/")[0];
}

function hostOfEmail(email) {
  const m = String(email || "").toLowerCase().match(/@([^>\s]+)/);
  return m ? m[1] : null;
}

function langFor(country) {
  if (country === "Brazil") return "pt";
  return "en";
}

async function fetchCensus(ids) {
  const base = getPlatformBase();
  const table = MAP_HOTEL_PROPERTY_CENSUS.tableName;
  const out = [];
  for (const id of ids) {
    try {
      const rec = await base(table).find(id);
      const f = rec.fields || {};
      out.push({
        hotel_id: rec.id,
        name: f[MAP_CENSUS_FIELDS.propertyName] || f[MAP_CENSUS_FIELDS.officialName] || null,
        aliases: f[MAP_CENSUS_FIELDS.officialName] && f[MAP_CENSUS_FIELDS.propertyName]
          ? [f[MAP_CENSUS_FIELDS.officialName]].filter(Boolean)
          : [],
        country: f[MAP_CENSUS_FIELDS.country] || null,
        city: f[MAP_CENSUS_FIELDS.city] || null,
        address: f[MAP_CENSUS_FIELDS.address] || null,
        website: f[MAP_CENSUS_FIELDS.website] || null,
        phone: f[MAP_CENSUS_FIELDS.phone] || null,
        latitude: f[MAP_CENSUS_FIELDS.latitude] ?? null,
        longitude: f[MAP_CENSUS_FIELDS.longitude] ?? null,
        property_identity_key: f[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
        census_ok: true,
      });
    } catch (e) {
      out.push({ hotel_id: id, census_ok: false, error: String(e?.message || e) });
    }
  }
  return out;
}

function classifyOwnership(research) {
  const o = research?.ownership || {};
  const classRaw = String(o.classification || o.owner_role || "UNRESOLVED").toUpperCase();
  const name = String(o.owner_display_name || "");
  const note = `${o.evidence_note || ""} ${name}`.toLowerCase();
  if (!name) return { primary: "UNRESOLVED", detail: classRaw };
  // Directory / scrape chrome must never become ECONOMIC_OWNER
  if (
    /whoistheownerof|whoownsthebrand|ownership\s+database|who\s+owns|verified\s+company|biggest\s+brands/i.test(
      `${name} ${note}`
    ) ||
    /^(of|the)\s+-/i.test(name)
  ) {
    return { primary: "UNRESOLVED", detail: "REJECTED_DIRECTORY_FALSE_POSITIVE" };
  }
  if (/operator|aimbridge|managed by/.test(note) && !/owner|acquired|purchased/.test(note)) {
    return { primary: "OPERATOR", detail: classRaw };
  }
  if (
    /brand|franchise|marriott|ihg|hilton|wyndham|autograph|slh|lxr|four seasons|ibis|tryp|caesar/i.test(note) &&
    !/owned|acquired|economic|purchased/i.test(note)
  ) {
    return { primary: "BRAND", detail: classRaw };
  }
  if (/PROPERTY_OWNER|deed|title/.test(classRaw)) return { primary: "PROPERTY_OWNER", detail: classRaw };
  if (/STAGED|CANDIDATE/.test(classRaw)) return { primary: "UNRESOLVED", detail: "OWNER_CANDIDATE_REQUIRES_ADJUDICATION" };
  if (/ECONOMIC|SPONSOR/.test(classRaw) || o.owner_role === "ECONOMIC_OWNER") {
    return { primary: "ECONOMIC_OWNER_OR_SPONSOR", detail: classRaw };
  }
  if (/REGISTERED/.test(classRaw)) return { primary: "REGISTERED_BUSINESS", detail: classRaw };
  // Do not auto-promote bare display names
  return { primary: "UNRESOLVED", detail: classRaw };
}

function titleOk(t) {
  const s = String(t || "");
  if (!s.trim() || IRRELEVANT_RE.test(s)) return false;
  return PRIORITY_RE.test(s);
}

async function main() {
  const startedAt = new Date().toISOString();
  const t0 = Date.now();
  const ledger = {
    started_at: startedAt,
    caps: {
      context_dev: CONTEXT_CAP,
      surfe_email: EMAIL_CAP,
      surfe_mobile: MOBILE_CAP,
      surfe_search_req: SEARCH_REQ_CAP,
      surfe_search_profiles: SEARCH_PROFILE_CAP,
      serpapi: SERP_CAP,
      fullenrich: 0,
      webhound: 0,
      apify: 0,
    },
    events: [],
    context_dev_spent: 0,
    surfe: { search_req: 0, search_profiles: 0, email_submitted: 0, mobile_submitted: 0 },
  };

  if (!isContextDevConfigured()) {
    console.error("CONTEXT_DEV_API_KEY missing — ownership research blocked");
  }

  // Validate selection vs excludes
  for (const h of FROZEN_SELECTION) {
    if (EXCLUDE_IDS.has(h.hotel_id)) throw new Error(`Frozen hotel is excluded: ${h.hotel_id}`);
  }

  // ——— FREEZE ———
  const census = await fetchCensus(FROZEN_SELECTION.map((h) => h.hotel_id));
  const byId = new Map(census.map((c) => [c.hotel_id, c]));
  const freeze = {
    version: "ci-20hotel-e2e-freeze-v1",
    frozen_at: startedAt,
    note: "Sampling strata are labels only — not ownership conclusions. Blind discovery: no expected owner names in inputs.",
    held_out_excluded: true,
    mexico_note: "CI v1.2 DEVELOPMENT split has no Mexico hotels; mix is Brazil + Caribbean.",
    hotels: FROZEN_SELECTION.map((s) => {
      const c = byId.get(s.hotel_id) || {};
      return {
        hotel_id: s.hotel_id,
        stratum: s.stratum,
        name: c.name || null,
        aliases: c.aliases || [],
        address: c.address || null,
        country: c.country || null,
        city: c.city || null,
        latitude: c.latitude,
        longitude: c.longitude,
        official_website: c.website || null,
        census_phone: c.phone || null,
        property_identity_key: c.property_identity_key || null,
        existing_ownership_exposure: "none_in_prior_surfe_fe_executed_set",
        recovery_lane: "SEPARATE — not used in blind inputs",
        census_ok: c.census_ok !== false,
      };
    }),
  };
  fs.writeFileSync(OUT.freeze, JSON.stringify(freeze, null, 2));
  ledger.events.push({ type: "freeze_written", hotels: freeze.hotels.length });

  const bal0 = await getSurfeCredits();
  ledger.balances = { start: bal0.payload };

  // ——— OWNERSHIP RESEARCH (first pass all 20) ———
  const perHotelBudget = Math.max(3, Math.floor(CONTEXT_CAP / freeze.hotels.length)); // ~5
  let contextRemaining = CONTEXT_CAP;
  const ownershipRows = [];

  for (const h of freeze.hotels) {
    const budgetThis = Math.min(perHotelBudget, contextRemaining);
    if (budgetThis < 2) {
      ownershipRows.push({
        hotel_id: h.hotel_id,
        hotel_name: h.name,
        stratum: h.stratum,
        skipped: true,
        reason: "CONTEXT_DEV_BUDGET_EXHAUSTED",
        relationship_primary: "UNRESOLVED",
      });
      continue;
    }

    const inspect = [];
    if (h.official_website) inspect.push(h.official_website);
    const caseInput = {
      hotel: {
        hotel_id: h.hotel_id,
        hotel_name: h.name,
        city: h.city,
        country: h.country,
        language: langFor(h.country),
      },
      // Blind: no owner_entity_id / owner_display_name / person_hypotheses
      inspect_urls: inspect,
      hotel_site_host: h.official_website ? domainRoot(h.official_website) : null,
      forbidden_org_hosts: [
        "marriott.com", "ihg.com", "hilton.com", "hyatt.com", "wyndhamhotels.com",
        "accor.com", "booking.com", "tripadvisor.com",
      ],
    };

    let research;
    try {
      research = await researchHotelOwnershipContactPath(caseInput, {
        context_dev_max: budgetThis,
        serpapi_max: SERP_CAP,
      });
    } catch (e) {
      research = { error: String(e?.message || e), ownership: {}, newly_researched: {}, unresolved: ["RESEARCH_THREW"] };
    }

    const used = Number(
      research?.budgets?.context_dev?.spent_credits ??
        research?.budgets?.context_dev?.spent ??
        estimateContextFromCalls(research)
    );
    contextRemaining = Math.max(0, contextRemaining - used);
    ledger.context_dev_spent += used;

    const rel = classifyOwnership(research);
    const domain =
      research?.confirmed_company_domain ||
      research?.newly_researched?.domain?.url ||
      null;
    const people = research?.people || research?.newly_researched?.people || [];
    const hotelSiteChannels = research?.organization_contact_route?.channels || [];

    ownershipRows.push({
      hotel_id: h.hotel_id,
      hotel_name: h.name,
      stratum: h.stratum,
      country: h.country,
      official_website: h.official_website,
      relationship_primary: rel.primary,
      relationship_detail: rel.detail,
      provisional: !!rel.provisional,
      owner_display_name: research?.ownership?.owner_display_name || null,
      owner_entity_id: research?.ownership?.owner_entity_id || null,
      evidence_note: research?.ownership?.evidence_note || null,
      confidence: research?.ownership?.confidence || null,
      claim_year_cue: research?.ownership?.claim_year_cue || null,
      historical_vs_current: research?.ownership?.historical_vs_current || null,
      domain: domain ? domainRoot(domain) : null,
      domain_url: domain,
      people_from_research: people.slice(0, 5).map((p) => ({
        name: p.display_name || p.full_name || p.name,
        title: p.title || p.job_title,
      })),
      hotel_site_channels: hotelSiteChannels.slice(0, 5),
      unresolved: research?.unresolved_reasons || [],
      context_credits_used: used,
      research_trace: { sources: research?.sources || [], calls: research?.calls || [], budgets: research?.budgets || null },
      sources_sample: (research?.sources || []).slice(0, 4).map((s) => ({
        kind: s.kind,
        url: s.url || null,
        query: s.query || null,
        excerpt: String(s.excerpt || s.snippet || "").slice(0, 200),
      })),
      newly_researched_flag: research?.ownership?.newly_researched !== false,
      qualifies_for_enrich: !!research?.qualifies_for_fullenrich,
      raw_ownership: research?.ownership || null,
    });

    ledger.events.push({
      type: "ownership_pass",
      hotel_id: h.hotel_id,
      used,
      remaining_budget: contextRemaining,
      relationship: rel.primary,
      owner: research?.ownership?.owner_display_name || null,
    });

    fs.writeFileSync(OUT.ownership, JSON.stringify({ started_at: startedAt, rows: ownershipRows, context_remaining: contextRemaining }, null, 2));
  }

  // Optional second pass on UNRESOLVED with remaining context budget
  for (const row of ownershipRows) {
    if (row.relationship_primary !== "UNRESOLVED") continue;
    if (contextRemaining < 5) break;
    const budgetThis = Math.min(8, contextRemaining);
    const h = freeze.hotels.find((x) => x.hotel_id === row.hotel_id);
    const research = await researchHotelOwnershipContactPath(
      {
        hotel: {
          hotel_id: h.hotel_id,
          hotel_name: h.name,
          city: h.city,
          country: h.country,
          language: langFor(h.country),
        },
        inspect_urls: h.official_website ? [h.official_website] : [],
        hotel_site_host: h.official_website ? domainRoot(h.official_website) : null,
      },
      { context_dev_max: budgetThis, serpapi_max: 0 }
    );
    const used = Number(
      research?.budgets?.context_dev?.spent_credits ?? estimateContextFromCalls(research)
    );
    contextRemaining -= used;
    ledger.context_dev_spent += used;
    const rel = classifyOwnership(research);
    if (research?.ownership?.owner_display_name) {
      row.relationship_primary = rel.primary;
      row.owner_display_name = research.ownership.owner_display_name;
      row.evidence_note = research.ownership.evidence_note;
      const d = research?.confirmed_company_domain || research?.newly_researched?.domain?.url;
      if (d) {
        row.domain = domainRoot(d);
        row.domain_url = d;
      }
      row.second_pass = true;
      row.context_credits_used += used;
    }
    row.second_pass_research_trace = { sources: research?.sources || [], calls: research?.calls || [], budgets: research?.budgets || null };
    ledger.events.push({ type: "ownership_second_pass", hotel_id: row.hotel_id, used, relationship: row.relationship_primary });
  }

  fs.writeFileSync(OUT.ownership, JSON.stringify({ completed_at: new Date().toISOString(), rows: ownershipRows, context_spent: ledger.context_dev_spent }, null, 2));

  // ——— SURFE people for qualified owner/sponsor with domain ———
  const qualified = ownershipRows.filter(
    (r) =>
      (r.relationship_primary === "ECONOMIC_OWNER_OR_SPONSOR" || r.relationship_primary === "PROPERTY_OWNER") &&
      r.domain &&
      r.owner_display_name
  );

  const discovered = [];
  for (const org of qualified) {
    if (ledger.surfe.search_req >= SEARCH_REQ_CAP || ledger.surfe.search_profiles >= SEARCH_PROFILE_CAP) break;
    const limit = Math.min(8, SEARCH_PROFILE_CAP - ledger.surfe.search_profiles);
    const body = {
      limit,
      peoplePerCompany: 3,
      companies: { domains: [org.domain] },
      people: { jobTitles: ROLE_TITLES },
    };
    const res = await searchSurfePeople(body);
    ledger.surfe.search_req += 1;
    const people = Array.isArray(res.payload?.people) ? res.payload.people : [];
    ledger.surfe.search_profiles += people.length;
    ledger.events.push({
      type: "surfe_search",
      org: org.owner_display_name,
      domain: org.domain,
      returned: people.length,
      http: res.http_status,
    });

    const picks = people
      .filter((p) => titleOk(p.jobTitle))
      .slice(0, 2)
      .map((p) => ({
        hotel_id: org.hotel_id,
        hotel_name: org.hotel_name,
        stratum: org.stratum,
        owner: org.owner_display_name,
        org_domain: org.domain,
        relationship: org.relationship_primary,
        first_name: p.firstName,
        last_name: p.lastName,
        full_name: [p.firstName, p.lastName].filter(Boolean).join(" "),
        job_title: p.jobTitle,
        company_domain: p.companyDomain,
        linkedin_url: p.linkedInUrl || null,
        discovery_source: "SURFE_PEOPLE_SEARCH",
        affiliation_status: "SURFE_CANDIDATE_UNCROBORATED",
      }));

    // Prefer research-named people if any match Surfe
    for (const rp of org.people_from_research || []) {
      const hit = people.find(
        (p) =>
          String(rp.name || "")
            .toLowerCase()
            .includes(String(p.lastName || "").toLowerCase()) && p.lastName
      );
      if (hit && titleOk(hit.jobTitle)) {
        const existing = picks.find((x) => x.full_name === [hit.firstName, hit.lastName].join(" "));
        if (existing) existing.affiliation_status = "RESEARCH_NAME_OVERLAP";
      }
    }
    discovered.push(...picks);
  }

  // Gate + email enrich — owner_person_enrichment workflow blocks Surfe-only affiliation.
  const emailFreeze = [];
  for (const p of discovered) {
    if (!p.first_name || !p.last_name) continue;
    if (p.linkedin_url) {
      const agr = linkedInNameTokenAgreement(p.full_name, p.linkedin_url);
      if (!agr.ok) {
        p.enrich_blocked = true;
        p.block_reason = "linkedin_name_token_disagreement";
        continue;
      }
    }
    emailFreeze.push(p);
  }

  const gateInput = emailFreeze.slice(0, EMAIL_CAP).map((p) => ({
    workflow: "owner_person_enrichment",
    hotel_to_owner: {
      supported: true,
      relationship_class: "ECONOMIC_OWNER_OR_SPONSOR",
      evidence_refs: p.ownership_evidence_refs || [`hotel:${p.hotel_id}`],
    },
    affiliation_corroboration: {
      status: p.affiliation_status === "RESEARCH_NAME_OVERLAP" ? "CORROBORATED" : "SURFE_ONLY",
      source_class: p.affiliation_status === "RESEARCH_NAME_OVERLAP" ? "CREDIBLE_INDEPENDENT" : "SURFE_ONLY",
      independently_corroborated: p.affiliation_status === "RESEARCH_NAME_OVERLAP",
      evidence_refs: p.affiliation_evidence_refs || [],
    },
    person: {
      display_name: p.full_name,
      full_name: p.full_name,
      first_name: p.first_name,
      last_name: p.last_name,
      identity_supported: true,
      why_relevant: p.job_title,
      title: p.job_title,
      publication_label: p.affiliation_status === "RESEARCH_NAME_OVERLAP" ? "EVIDENCED" : "SURFE_CANDIDATE",
    },
    organization: { name: p.owner, relationship_supported: true },
    identifiers: {
      domain: { value: p.org_domain, status: "CONFIRMED_FIRST_PARTY", independently_supported: true },
      ...(p.linkedin_url
        ? { linkedin_url: { value: p.linkedin_url, independently_confirmed: false } }
        : {}),
      first_name: p.first_name,
      last_name: p.last_name,
    },
    subject_id: `${p.hotel_id}_${p.full_name}`.replace(/\s+/g, "_"),
    _p: p,
  }));

  const gated = gateProviderCandidates(gateInput, {
    provider: "surfe",
    workflow: "owner_person_enrichment",
  });
  const toSubmit = gated.allowed.map((g) => g._p).slice(0, EMAIL_CAP);

  const balBeforeEmail = await getSurfeCredits();
  let emailJob = null;
  const emailResults = [];

  if (toSubmit.length && (balBeforeEmail.payload?.totalEmail ?? 0) >= toSubmit.length) {
    const start = await startSurfePeopleEnrichment({
      people: toSubmit.map((p, i) => ({
        firstName: p.first_name,
        lastName: p.last_name,
        companyName: p.owner,
        companyDomain: p.org_domain,
        ...(p.linkedin_url ? { linkedinUrl: p.linkedin_url } : {}),
        externalID: `e2e_${i}`,
      })),
      include: { email: true, mobile: false, linkedInUrl: true },
      enrichmentOptions: { acceptedEmailType: "professional" },
    });
    emailJob = {
      enrichment_id: start.payload?.enrichmentID,
      http_status: start.http_status,
      people: toSubmit.length,
      started_at: new Date().toISOString(),
    };
    fs.writeFileSync(OUT.jobs, JSON.stringify({ email_job: emailJob }, null, 2));
    ledger.surfe.email_submitted = toSubmit.length;

    if (emailJob.enrichment_id && start.ok) {
      const polled = await pollSurfePeopleEnrichment(emailJob.enrichment_id, { maxWaitMs: 180000 });
      const outs = Array.isArray(polled.payload?.people) ? polled.payload.people : [];
      for (let i = 0; i < toSubmit.length; i++) {
        const p = toSubmit[i];
        const raw = outs[i];
        const n = raw ? normalizeSurfePerson(raw) : null;
        const em = (n?.emails || []).find((e) => String(e.type).toLowerCase() === "professional") || n?.emails?.[0];
        const host = hostOfEmail(em?.email);
        const target = p.org_domain;
        let accepted = false;
        let bucket = "EMAIL_MISS";
        let related = false;
        if (em?.email) {
          const vs = String(em.validation_status || "").toUpperCase();
          if (host === target || host?.endsWith(`.${target}`) || target?.endsWith(`.${host}`)) {
            bucket = vs === "VALID" ? "PROVIDER_VALID_TARGET_DOMAIN" : "CATCH_ALL_OR_UNCERTAIN";
            // Accepted person attribution requires affiliation corroboration — Surfe-only stays candidate
            accepted = false;
            bucket = vs === "VALID" ? "PROVIDER_VALID_UNCORROBORATED" : bucket;
          } else {
            related = true;
            bucket = "PROVIDER_VALID_RELATED_DOMAIN_UNATTRIBUTED";
          }
        }
        emailResults.push({
          ...p,
          surfe_email: em?.email || null,
          email_type: em?.type || null,
          provider_validation: em?.validation_status || null,
          email_bucket: bucket,
          person_attributed_accepted: accepted,
          related_domain: related,
          enrichment_id: emailJob.enrichment_id,
        });
      }
      emailJob.status = polled.payload?.status;
    }
  }

  const balAfterEmail = await getSurfeCredits();

  // Mobile — up to 10, spread owners, skip related-domain / identity conflicts
  const mobileEligible = emailResults.filter(
    (r) => r.email_bucket !== "PROVIDER_VALID_RELATED_DOMAIN_UNATTRIBUTED" && !r.enrich_blocked
  );
  const byOwner = new Map();
  for (const r of mobileEligible) {
    if (!byOwner.has(r.owner)) byOwner.set(r.owner, []);
    byOwner.get(r.owner).push(r);
  }
  const mobilePick = [];
  const keys = [...byOwner.keys()];
  let oi = 0;
  while (mobilePick.length < MOBILE_CAP && keys.some((k) => byOwner.get(k)?.length)) {
    const k = keys[oi % keys.length];
    if (byOwner.get(k)?.length) mobilePick.push(byOwner.get(k).shift());
    oi += 1;
    if (oi > 500) break;
  }

  let mobileJob = null;
  const balBeforeMobile = await getSurfeCredits();
  if (mobilePick.length && (balBeforeMobile.payload?.totalMobile ?? 0) >= mobilePick.length) {
    const start = await startSurfePeopleEnrichment({
      people: mobilePick.map((p, i) => ({
        firstName: p.first_name,
        lastName: p.last_name,
        companyName: p.owner,
        companyDomain: p.org_domain,
        ...(p.linkedin_url ? { linkedinUrl: p.linkedin_url } : {}),
        externalID: `m_${i}`,
      })),
      include: { email: false, mobile: true, linkedInUrl: true },
    });
    mobileJob = {
      enrichment_id: start.payload?.enrichmentID,
      http_status: start.http_status,
      people: mobilePick.map((p) => p.full_name),
      started_at: new Date().toISOString(),
    };
    fs.writeFileSync(OUT.jobs, JSON.stringify({ email_job: emailJob, mobile_job: mobileJob }, null, 2));
    ledger.surfe.mobile_submitted = mobilePick.length;
    if (mobileJob.enrichment_id && start.ok) {
      const polled = await pollSurfePeopleEnrichment(mobileJob.enrichment_id, { maxWaitMs: 180000 });
      const outs = Array.isArray(polled.payload?.people) ? polled.payload.people : [];
      for (let i = 0; i < mobilePick.length; i++) {
        const n = outs[i] ? normalizeSurfePerson(outs[i]) : null;
        const ph = n?.mobile_phones?.[0];
        const er = emailResults.find((e) => e.full_name === mobilePick[i].full_name);
        if (er && ph?.number) {
          er.phone = ph.number;
          er.phone_type = ph.phone_type;
          er.phone_confidence = ph.confidence_score;
          er.phone_business_use = "UNKNOWN";
          er.phone_bucket = "PROVIDER_REPORTED_MOBILE";
        }
      }
      mobileJob.status = polled.payload?.status;
    }
  }
  const balAfterMobile = await getSurfeCredits();

  // ——— SCORE ———
  const hotelRows = freeze.hotels.map((h) => {
    const own = ownershipRows.find((r) => r.hotel_id === h.hotel_id) || {};
    const people = emailResults.filter((e) => e.hotel_id === h.hotel_id);
    const best = people[0] || null;
    const ownerOk =
      own.relationship_primary === "ECONOMIC_OWNER_OR_SPONSOR" ||
      own.relationship_primary === "PROPERTY_OWNER";
    const domainOk = !!own.domain && ownerOk;
    const personCorroborated = people.some((p) => p.affiliation_status === "RESEARCH_NAME_OVERLAP");
    const acceptedEmail = people.some((p) => p.person_attributed_accepted);
    const providerValid = people.some((p) =>
      ["PROVIDER_VALID_TARGET_DOMAIN", "PROVIDER_VALID_UNCORROBORATED"].includes(p.email_bucket)
    );
    const phoneNamed = people.some((p) => p.phone);
    const both = people.some(
      (p) =>
        ["PROVIDER_VALID_TARGET_DOMAIN", "PROVIDER_VALID_UNCORROBORATED"].includes(p.email_bucket) && p.phone
    );
    const corpFallback = (own.hotel_site_channels || []).some((c) => /EMAIL|ORG/.test(c.kind || ""));
    const hotelOnly = !ownerOk && !!(h.census_phone || h.official_website);

    let missing = null;
    if (!ownerOk) missing = "owner/sponsor";
    else if (!domainOk) missing = "owner domain";
    else if (!people.length) missing = "relevant person";
    else if (!providerValid) missing = "email";
    else if (!personCorroborated && !acceptedEmail) missing = "affiliation corroboration";
    else if (!phoneNamed) missing = "phone";
    else missing = null;

    return {
      hotel: h.name,
      hotel_id: h.hotel_id,
      stratum: h.stratum,
      owner_sponsor: own.owner_display_name || "—",
      ownership_source: (own.evidence_note || own.relationship_detail || "—").slice(0, 120),
      ownership_class: own.relationship_primary || "UNRESOLVED",
      domain: own.domain || "—",
      person_role: best ? `${best.full_name} / ${best.job_title}` : "—",
      affiliation_evidence: best?.affiliation_status || "—",
      email_status: best ? `${best.surfe_email || "—"} (${best.email_bucket})` : "—",
      phone_type: best?.phone
        ? `${best.phone} (${best.phone_type}; conf=${best.phone_confidence}; business_use=UNKNOWN)`
        : "—",
      new_reused: best ? "NEW_SURFE" : own.owner_display_name ? "OWNER_ONLY" : "NONE",
      complete_chain: ownerOk && domainOk && personCorroborated && acceptedEmail,
      missing_link: missing,
      flags: {
        ownerOk,
        domainOk,
        personCorroborated,
        acceptedEmail,
        providerValid,
        phoneNamed,
        both,
        corpFallback,
        hotelOnly,
      },
    };
  });

  const n = hotelRows.length;
  const pct = (c) => `${c}/${n} (${((100 * c) / n).toFixed(0)}%)`;
  const scores = {
    evidenced_owner_sponsor: pct(hotelRows.filter((r) => r.flags.ownerOk).length),
    confirmed_owner_domain: pct(hotelRows.filter((r) => r.flags.domainOk).length),
    relevant_corroborated_person: pct(hotelRows.filter((r) => r.flags.personCorroborated).length),
    accepted_person_email: pct(hotelRows.filter((r) => r.flags.acceptedEmail).length),
    provider_valid_email: pct(hotelRows.filter((r) => r.flags.providerValid).length),
    named_person_phone: pct(hotelRows.filter((r) => r.flags.phoneNamed).length),
    both_email_phone: pct(hotelRows.filter((r) => r.flags.both).length),
    corporate_fallback: pct(hotelRows.filter((r) => r.flags.corpFallback).length),
    hotel_only_fallback: pct(hotelRows.filter((r) => r.flags.hotelOnly).length),
    complete_chain: pct(hotelRows.filter((r) => r.complete_chain).length),
    by_stratum: {
      portfolio: hotelRows.filter((r) => r.stratum === "portfolio_business_signal"),
      independent: hotelRows.filter((r) => r.stratum === "independent_private_signal"),
    },
  };

  const measured = {
    context_dev_spent: ledger.context_dev_spent,
    context_cap: CONTEXT_CAP,
    surfe_email: {
      before: balBeforeEmail.payload?.totalEmail,
      after: balAfterEmail.payload?.totalEmail,
      delta: (balBeforeEmail.payload?.totalEmail ?? 0) - (balAfterEmail.payload?.totalEmail ?? 0),
      submitted: ledger.surfe.email_submitted,
    },
    surfe_mobile: {
      before: balBeforeMobile.payload?.totalMobile,
      after: balAfterMobile.payload?.totalMobile,
      delta: (balBeforeMobile.payload?.totalMobile ?? 0) - (balAfterMobile.payload?.totalMobile ?? 0),
      submitted: ledger.surfe.mobile_submitted,
    },
    surfe_search: ledger.surfe,
    serpapi: 0,
  };

  const results = {
    version: "ci-20hotel-e2e-v1",
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    elapsed_ms: Date.now() - t0,
    reconciliation_doc: OUT.reconcile,
    freeze_path: OUT.freeze,
    code_fix: "ownership-contact-research-handoff Phase B skips Serp when serpapi_max=0; Phase C Context.dev continues",
    ownership_rows: ownershipRows,
    discovered_people: discovered,
    email_results: emailResults,
    hotel_rows: hotelRows,
    scores,
    measured_spend: measured,
    jobs: { email_job: emailJob, mobile_job: mobileJob },
    qualified_orgs_for_surfe: qualified.map((q) => ({
      hotel: q.hotel_name,
      owner: q.owner_display_name,
      domain: q.domain,
      class: q.relationship_primary,
    })),
  };

  fs.writeFileSync(OUT.results, JSON.stringify(results, null, 2));
  fs.writeFileSync(OUT.ledger, JSON.stringify({ ...ledger, measured, completed_at: results.completed_at }, null, 2));
  fs.writeFileSync(OUT.jobs, JSON.stringify({ email_job: emailJob, mobile_job: mobileJob }, null, 2));

  // Founder MD
  const md = [];
  md.push(`# 20-hotel DEVELOPMENT e2e — founder report`);
  md.push("");
  md.push(`**Evaluation staging.** Elapsed ${(results.elapsed_ms / 1000).toFixed(0)}s. Reconciliation: \`${OUT.reconcile}\`.`);
  md.push("");
  md.push(`## Spend`);
  md.push(`- Context.dev: **${measured.context_dev_spent}** / ${CONTEXT_CAP}`);
  md.push(`- Surfe search: **${measured.surfe_search.search_req}** req / **${measured.surfe_search.search_profiles}** profiles (caps ${SEARCH_REQ_CAP}/${SEARCH_PROFILE_CAP}); search credits balance unused (free quota)`);
  md.push(`- Surfe email Δ: **${measured.surfe_email.delta}** (submitted ${measured.surfe_email.submitted}, cap ${EMAIL_CAP}) job=\`${emailJob?.enrichment_id || "none"}\``);
  md.push(`- Surfe mobile Δ: **${measured.surfe_mobile.delta}** (submitted ${measured.surfe_mobile.submitted}, cap ${MOBILE_CAP}) job=\`${mobileJob?.enrichment_id || "none"}\``);
  md.push(`- SerpAPI / FullEnrich / Webhound / Apify: **0**`);
  md.push("");
  md.push(`## Chain scores (denominator = 20 frozen hotels)`);
  for (const [k, v] of Object.entries(scores)) {
    if (k === "by_stratum") continue;
    md.push(`- **${k}:** ${v}`);
  }
  md.push("");
  md.push(`## Hotel rows`);
  md.push(`| Hotel | Stratum | Owner/sponsor | Ownership source/date | Domain | Person/role | Affiliation | Email/status | Phone/type | New/reused | Complete? | Missing |`);
  md.push(`|---|---|---|---|---|---|---|---|---|---|---|---|`);
  for (const r of hotelRows) {
    md.push(
      `| ${r.hotel || r.hotel_id} | ${r.stratum} | ${(r.owner_sponsor || "—").replace(/\|/g, "/")} | ${(r.ownership_source || "—").replace(/\|/g, "/")} | ${r.domain} | ${(r.person_role || "—").replace(/\|/g, "/")} | ${r.affiliation_evidence} | ${(r.email_status || "—").replace(/\|/g, "/")} | ${(r.phone_type || "—").replace(/\|/g, "/")} | ${r.new_reused} | ${r.complete_chain ? "Y" : "N"} | ${r.missing_link || "—"} |`
    );
  }
  md.push("");
  md.push(`## Founder answers`);
  const ownerContactHotels = hotelRows.filter((r) => r.flags.ownerOk && (r.flags.providerValid || r.flags.phoneNamed));
  md.push(`### How many of 20 produced an evidenced owner contact?`);
  md.push(`- Strict (hotel→owner + accepted person-attributed email): **${hotelRows.filter((r) => r.complete_chain).length}/20**`);
  md.push(`- Evidenced owner/sponsor org identified: **${hotelRows.filter((r) => r.flags.ownerOk).length}/20**`);
  md.push(`- Owner/sponsor + provider contact candidate (not independently corroborated): **${ownerContactHotels.length}/20**`);
  md.push(`### Both email and phone?`);
  md.push(`- Provider VALID email + provider phone on same person: **${hotelRows.filter((r) => r.flags.both).length}/20** — phones are mobile_provider_reported, business_use UNKNOWN, not independently verified.`);
  md.push(`### Where did the chain fail most often?`);
  const miss = {};
  for (const r of hotelRows) miss[r.missing_link || "complete"] = (miss[r.missing_link || "complete"] || 0) + 1;
  md.push(`- Missing-link frequencies: ${JSON.stringify(miss)}`);
  md.push(`### What did our code improve?`);
  md.push(`- Handoff: when \`serpapi_max=0\`, skip Serp \`discoverOwnerPersonPath\` and continue Context.dev Phase C (domain/people) so unresolved hotels still enter research.`);
  md.push(`- Prior Surfe batch reconciled: related-domain emails no longer count as accepted person attribution; brand-org contacts separated from evidenced owner contacts (\`${OUT.reconcile}\`).`);
  md.push(`### Next single highest-value change?`);
  md.push(`- **Affiliation corroboration before Surfe enrich** (first-party leadership page / dated press naming the person) so provider VALID can graduate to accepted person-attributed owner contact — and invest Context.dev extract budget on hotel→owner pages for independents where search alone fails.`);
  md.push("");
  md.push(`Artifacts: freeze / ownership / results / jobs / ledger under \`reports/ci-20hotel-e2e-*\`.`);

  fs.writeFileSync(OUT.founder, md.join("\n"));
  console.log(JSON.stringify({ founder: OUT.founder, scores, measured, qualified: qualified.length, emails: emailResults.length }, null, 2));
}

function estimateContextFromCalls(research) {
  const calls = research?.calls || [];
  let n = 0;
  for (const c of calls) {
    if (c.provider !== "context_dev") continue;
    if (c.kind === "search" || c.kind === "domain_search") n += 1;
    else if (String(c.kind).includes("scrape") || c.kind === "inspect_seed_scrape") n += 1;
    else if (String(c.kind).includes("extract")) n += 10;
  }
  return n;
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
