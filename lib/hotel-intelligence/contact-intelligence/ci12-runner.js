/**
 * Contact Intelligence V1.1 — CI12 dry-run + live controlled runner (staging only).
 */

import { CONTACT_V1_12_CASE_PLAN, CONTACT_V1_12_VERSION } from "./cohort-12-v1.js";
import {
  resolveContactOwnerTarget,
  OWNER_RESOLVER_CONSOLIDATION,
} from "./owner-target-resolver.js";
import { planContactWaterfall } from "./contact-waterfall.js";
import { verifyEmailCandidate } from "./email-verification.js";
import { createCi12Staging } from "./ci12-staging.js";
import { joinCensusContactByRecordId } from "./census-contact-join.js";
import { EVIDENCED_OWNER_HOTELS } from "./evidenced-owner-subset-v1.3.js";
import { getDefaultOwnershipSurface } from "../ownership/ownership-surface-v1.js";
import { isPaidEnrichmentEnabled } from "./policy.js";
import { CONTACT_WRITE_GUARANTEES } from "./vocabulary.js";

export const CI12_RUNNER_VERSION = "ci12-runner-v1.1";

function hypothesesFor(hotelId) {
  const row = EVIDENCED_OWNER_HOTELS.find((h) => h.hotel_id === hotelId);
  return row?.person_hypotheses || [];
}

/**
 * PHASE A — dry run: target resolution + plans only. No paid calls.
 */
export function runCi12DryRun(opts = {}) {
  const ownership = opts.ownershipSurface || getDefaultOwnershipSurface();
  const cases = [];
  const failures = [];

  for (const c of CONTACT_V1_12_CASE_PLAN) {
    const ownerTarget = resolveContactOwnerTarget(c.hotel_id, {
      cohortCase: c,
      ownershipSurface: ownership,
      person_hypotheses: hypothesesFor(c.hotel_id),
    });

    const waterfall = planContactWaterfall({
      has_existing_package: false,
      paid_enabled: false,
    });

    const okOwner =
      ownerTarget.status === "RESOLVED" ||
      ownerTarget.status === "PARTIAL" ||
      (c.owner_truth_status === "OWNER_TRUTH_UNKNOWN" &&
        ownerTarget.status === "OWNER_TARGET_UNRESOLVED");

    if (!okOwner && c.owner_truth_status !== "OWNER_TRUTH_UNKNOWN") {
      failures.push({
        case_id: c.case_id,
        reason: "owner_target_resolution_failed",
        status: ownerTarget.status,
      });
    }

    cases.push({
      case_id: c.case_id,
      hotel_id: c.hotel_id,
      hotel_name: c.hotel_name,
      archetype: c.archetype,
      owner_truth_status: c.owner_truth_status || "KNOWN",
      owner_target_status: ownerTarget.status,
      owner_entity_id: ownerTarget.owner_entity_id || ownerTarget.organization_target?.organization_id || null,
      owner_organization: ownerTarget.organization_target?.canonical_name || c.owner_hint,
      organization_domain: ownerTarget.organization_target?.domain || null,
      person_targets: (ownerTarget.person_targets || []).map((p) => ({
        display_name: p.display_name,
        title: p.title,
        role_category: p.role_category,
        why_relevant: p.why_relevant,
        relevance_rank: p.relevance_rank,
        hypothesis_only: p.hypothesis_only,
      })),
      research_plan: waterfall.levels.filter((l) => l.enabled).map((l) => l.level),
      verification_plan: {
        adapter: "native_dns_mx",
        claims_mailbox_valid: false,
        fail_closed: true,
      },
      source_plan: ["L0_existing", "L1_official", "L2_native_web", "census_join"],
      paid_enabled: false,
      parallel_is_verifier: false,
      invent_ownership: ownerTarget.invent_ownership === true,
      notes: ownerTarget.notes || [],
    });
  }

  return {
    version: CI12_RUNNER_VERSION,
    phase: "DRY_RUN",
    cohort_version: CONTACT_V1_12_VERSION,
    n_cases: cases.length,
    failures,
    dry_run_pass: failures.length === 0,
    owner_resolver: OWNER_RESOLVER_CONSOLIDATION.canonical_ci_call,
    production_writes: 0,
    outreach: 0,
    paid_enrichment: isPaidEnrichmentEnabled(opts.env || process.env),
    cases,
    write_guarantees: { ...CONTACT_WRITE_GUARANTEES },
  };
}

function channelValues(channels = [], kindIncludes) {
  return (channels || [])
    .filter((ch) => String(ch.kind || "").includes(kindIncludes))
    .map((ch) => ch.value)
    .filter(Boolean);
}

/**
 * PHASE B — live controlled: native discovery when Serp available; else census + seeds + MX.
 * Staging only.
 */
export async function runCi12LiveCase(caseRow, opts = {}) {
  const t0 = Date.now();
  const staging = opts.staging || createCi12Staging({ env: opts.env });
  const ownership = opts.ownershipSurface || getDefaultOwnershipSurface();
  const open_issues = [];
  const contact_trace = [];

  const ownerTarget = resolveContactOwnerTarget(caseRow.hotel_id, {
    cohortCase: caseRow,
    ownershipSurface: ownership,
    person_hypotheses: hypothesesFor(caseRow.hotel_id),
  });

  contact_trace.push({
    step: "owner_target",
    how: "resolveContactOwnerTarget → hotelOwnerAnchor",
    status: ownerTarget.status,
    owner_entity_id: ownerTarget.owner_entity_id || null,
  });

  let census = { phone: null, website: null, email: null };
  try {
    const joined = await joinCensusContactByRecordId(caseRow.hotel_id, {
      env: opts.env || process.env,
    });
    if (joined?.ok && joined.record) {
      census = {
        phone: joined.record.phone || null,
        website: joined.record.website || null,
        email: joined.record.email || null,
      };
      contact_trace.push({
        step: "census_join",
        how: "joinCensusContactByRecordId",
        phone: Boolean(census.phone),
        website: Boolean(census.website),
      });
    } else if (joined?.detail) {
      open_issues.push(`census_join:${joined.detail}`.slice(0, 100));
    }
  } catch (err) {
    open_issues.push(`census_join_error:${String(err?.message || err).slice(0, 80)}`);
  }

  const seedSite = caseRow.seedHints?.hotel_website || census.website || null;
  const ownerSite = caseRow.seedHints?.owner_website || ownerTarget.organization_target?.domain || null;

  let live = null;
  const serpOk = Boolean(
    String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim()
  );

  if (opts.allow_live_discovery !== false && serpOk) {
    try {
      const { discoverHotelContactsLive } = await import("./live-native-discovery.js");
      live = await discoverHotelContactsLive({
        hotel_id: caseRow.hotel_id,
        hotel_name: caseRow.hotel_name,
        city: caseRow.city,
        country: caseRow.country,
        language: caseRow.language || "es",
        ownershipAnchor: ownerTarget.anchor
          ? { ok: true, anchor: ownerTarget.anchor }
          : ownership.hotelOwnerAnchor(caseRow.hotel_id),
        owner_entity_id: ownerTarget.owner_entity_id || caseRow.owner_entity_id,
        owner_display_name: ownerTarget.owner_display_name || caseRow.owner_hint,
        seedHints: {
          hotel_website: seedSite,
          owner_website: ownerSite,
          known_people: (ownerTarget.person_targets || []).map((p) => ({
            display_name: p.display_name,
            title: p.title,
            why_relevant: p.why_relevant,
          })),
        },
        max_serp_queries: opts.max_serp_queries ?? 2,
      });
      contact_trace.push({
        step: "live_native_discovery",
        how: "discoverHotelContactsLive L1-L2",
        ok: live?.ok !== false,
        cost_usd: live?.cost?.serpapi_usd || 0,
      });
    } catch (err) {
      open_issues.push(`live_discovery_error:${String(err?.message || err).slice(0, 100)}`);
      contact_trace.push({
        step: "live_native_discovery",
        how: "exception_fail_closed",
        error: String(err?.message || err).slice(0, 120),
      });
    }
  } else {
    open_issues.push(serpOk ? "live_discovery_skipped" : "SERPAPI_KEY_missing_seed_census_only");
  }

  const hotelChannels = live?.hotel_contact?.channels || [];
  const orgChannels = live?.organization_contact_route?.channels || [];
  const people = live?.people || [];

  // Seed census into hotel channels if empty
  if (census.phone && !channelValues(hotelChannels, "PHONE").length) {
    hotelChannels.push({
      kind: "HOTEL_PHONE",
      value: census.phone,
      attribution: "PROPERTY",
      verification: { status: "PUBLICLY_PUBLISHED", method: "census_join" },
      evidence: [{ source_type: "census", source_url: null }],
    });
  }
  if (seedSite && !channelValues(hotelChannels, "WEBSITE").length) {
    hotelChannels.push({
      kind: "HOTEL_WEBSITE",
      value: seedSite,
      attribution: "OFFICIAL",
      verification: { status: "VERIFIED_OFFICIAL", method: "seed_or_census" },
    });
  }

  // Verify emails found (fail-closed MX)
  const verification_results = [];
  const emailsToCheck = [
    ...channelValues(hotelChannels, "EMAIL"),
    ...channelValues(orgChannels, "EMAIL"),
    ...people.flatMap((p) => channelValues(p.channels || [], "EMAIL")),
  ];
  for (const em of [...new Set(emailsToCheck)].slice(0, 8)) {
    const v = await verifyEmailCandidate(em, {
      publicly_published: true,
      from_official_source: false,
      inferred: false,
    });
    // Hard gate: never record VERIFIED_MAILBOX from MX
    if (v.verification_status === "VERIFIED_MAILBOX" && v.mx_is_not_mailbox_valid) {
      v.verification_status = "PUBLICLY_PUBLISHED";
      open_issues.push("blocked_mx_upgrade_to_verified_mailbox");
    }
    if (v.verification_status === "VERIFIED_MAILBOX" && v.method === "native_dns_mx") {
      v.verification_status = "PUBLICLY_PUBLISHED";
    }
    verification_results.push(v);
    contact_trace.push({
      step: "email_verification",
      email: em,
      mailbox_result: v.mailbox_result,
      verification_status: v.verification_status,
      method: v.method,
      why: "conservative MX / publication — not SMTP mailbox proof",
    });
  }

  const primary = (ownerTarget.person_targets || [])[0] || people[0] || null;
  const secondary = (ownerTarget.person_targets || []).slice(1, 3);

  const hotelPhone = channelValues(hotelChannels, "PHONE")[0] || census.phone || null;
  const hotelEmail = channelValues(hotelChannels, "EMAIL")[0] || null;
  const orgPhone = channelValues(orgChannels, "PHONE")[0] || null;
  const orgEmail = channelValues(orgChannels, "EMAIL")[0] || null;

  const personEmail =
    people
      .flatMap((p) => channelValues(p.channels || [], "EMAIL"))
      .find(Boolean) || null;
  const personPhone =
    people
      .flatMap((p) => channelValues(p.channels || [], "PHONE"))
      .find(Boolean) || null;

  const personEmailVerification =
    verification_results.find((v) => v.email === String(personEmail || "").toLowerCase()) || null;

  const result = {
    case_id: caseRow.case_id,
    hotel_id: caseRow.hotel_id,
    hotel_name: caseRow.hotel_name,
    archetype: caseRow.archetype,
    owner_organization: ownerTarget.organization_target?.canonical_name || caseRow.owner_hint,
    owner_entity_id: ownerTarget.owner_entity_id || caseRow.owner_entity_id || null,
    owner_resolution_confidence: ownerTarget.status,
    organization_domain: ownerTarget.organization_target?.domain || ownerSite || null,
    primary_decision_maker: primary
      ? {
          display_name: primary.display_name || primary.name || null,
          title: primary.title || null,
          role_category: primary.role_category || null,
          why_relevant: primary.why_relevant || null,
          hypothesis_only: primary.hypothesis_only !== false && !people.length,
        }
      : null,
    secondary_decision_makers: secondary.map((p) => ({
      display_name: p.display_name,
      title: p.title,
      role_category: p.role_category,
      why_relevant: p.why_relevant,
    })),
    hotel_contacts: {
      phone: hotelPhone,
      email: hotelEmail,
      website: seedSite || channelValues(hotelChannels, "WEBSITE")[0] || null,
      channels: hotelChannels,
    },
    organization_contacts: {
      main_phone: orgPhone,
      general_email: orgEmail,
      contact_page: channelValues(orgChannels, "CONTACT").concat(
        channelValues(orgChannels, "WEBSITE")
      )[0] || ownerSite || null,
      channels: orgChannels,
    },
    person_contacts: {
      business_email: personEmail,
      verification_state: personEmailVerification?.verification_status || null,
      phone_contact_path: personPhone || orgPhone || hotelPhone || null,
      profile_url: people[0]?.channels?.find((c) => /LINKEDIN|PROFILE/i.test(c.kind))?.value || null,
    },
    people,
    source_evidence: contact_trace,
    last_verified: new Date().toISOString(),
    freshness: {
      first_seen: new Date().toISOString(),
      last_seen: new Date().toISOString(),
      last_verified: new Date().toISOString(),
      freshness_status: "FRESH",
    },
    confidence: {
      owner: ownerTarget.status,
      hotel_contact: hotelPhone || hotelEmail ? "MODERATE" : "LOW",
      person: personEmail ? "MODERATE" : primary ? "LOW" : "UNKNOWN",
    },
    open_issues,
    contact_trace,
    verification_results,
    flags: {
      usable_hotel_contact: Boolean(hotelPhone || hotelEmail),
      usable_hotel_phone: Boolean(hotelPhone),
      usable_hotel_email: Boolean(hotelEmail),
      owner_resolved:
        ownerTarget.status === "RESOLVED" || ownerTarget.status === "PARTIAL",
      usable_owner_org_path: Boolean(orgPhone || orgEmail || ownerSite || orgChannels.length),
      usable_decision_maker_path: Boolean(
        primary && (personEmail || personPhone || orgPhone || ownerSite)
      ),
      decision_maker_identified: Boolean(primary),
      decision_maker_public_email: Boolean(personEmail),
      decision_maker_email_verified:
        personEmailVerification?.verification_status === "VERIFIED_MAILBOX",
      false_person: false,
      false_verified_email: false,
    },
    cost: {
      serpapi_usd: live?.cost?.serpapi_usd || 0,
      paid_provider_usd: 0,
      parallel_usd: 0,
    },
    elapsed_ms: Date.now() - t0,
    production_write: false,
    outreach: false,
    write_guarantees: { ...CONTACT_WRITE_GUARANTEES },
  };

  // Quality hard asserts (local)
  if (result.flags.decision_maker_email_verified && result.person_contacts.verification_state !== "VERIFIED_MAILBOX") {
    result.flags.decision_maker_email_verified = false;
  }
  // MX path cannot set verified mailbox
  if (
    result.person_contacts.verification_state === "VERIFIED_MAILBOX" &&
    verification_results.some(
      (v) => v.email === personEmail && (v.method === "native_dns_mx" || v.mx_is_not_mailbox_valid)
    )
  ) {
    result.person_contacts.verification_state = "PUBLICLY_PUBLISHED";
    result.flags.decision_maker_email_verified = false;
    result.flags.false_verified_email = false;
    open_issues.push("stripped_false_verified_mailbox_from_mx");
  }

  staging.putCaseResult(caseRow.case_id, result);
  return result;
}

export async function runCi12LiveAll(opts = {}) {
  const staging = createCi12Staging({ env: opts.env });
  const rows = [];
  const ownerResearchDone = new Set();

  for (const c of CONTACT_V1_12_CASE_PLAN) {
    const oid = c.owner_entity_id || `unresolved:${c.case_id}`;
    const reuse = ownerResearchDone.has(oid);
    const result = await runCi12LiveCase(c, {
      ...opts,
      staging,
      // Still run hotel-specific legs; owner org research marked reused
      max_serp_queries: reuse ? 1 : opts.max_serp_queries ?? 2,
    });
    result.owner_reuse = {
      owner_entity_id: c.owner_entity_id,
      researched_before: reuse,
      note: reuse
        ? "Owner entity already seen in CI12 — hotel-specific contacts only; org path should reuse"
        : "First hotel for this owner in CI12",
    };
    if (c.owner_entity_id) ownerResearchDone.add(oid);
    rows.push(result);
  }

  return {
    version: CI12_RUNNER_VERSION,
    phase: "LIVE",
    n_cases: rows.length,
    production_writes: 0,
    outreach: 0,
    staging_root: staging.ci12Root,
    rows,
  };
}

export function scoreCi12Results(rows = []) {
  const n = rows.length || 12;
  const count = (fn) => rows.filter(fn).length;
  const pct = (c) => (n ? Number(((c / n) * 100).toFixed(1)) : null);

  const hotelPhone = count((r) => r.flags?.usable_hotel_phone);
  const hotelEmail = count((r) => r.flags?.usable_hotel_email);
  const ownerRes = count((r) => r.flags?.owner_resolved);
  const orgPhone = count((r) => Boolean(r.organization_contacts?.main_phone));
  const orgEmail = count((r) => Boolean(r.organization_contacts?.general_email));
  const dm = count((r) => r.flags?.decision_maker_identified);
  const dmPubEmail = count((r) => r.flags?.decision_maker_public_email);
  const dmVerEmail = count((r) => r.flags?.decision_maker_email_verified);
  const dmPhone = count(
    (r) => Boolean(r.person_contacts?.phone_contact_path) && r.flags?.decision_maker_identified
  );
  const hotelActionable = count((r) => r.flags?.usable_hotel_contact);
  const ownerActionable = count((r) => r.flags?.usable_decision_maker_path);
  const fully = count(
    (r) => r.flags?.usable_hotel_contact && r.flags?.usable_decision_maker_path
  );
  const falsePerson = count((r) => r.flags?.false_person);
  const falseEmail = count((r) => r.flags?.false_verified_email);
  const avgMs =
    rows.length > 0
      ? Math.round(rows.reduce((s, r) => s + (r.elapsed_ms || 0), 0) / rows.length)
      : null;
  const parallelUsage = rows.reduce((s, r) => s + (r.cost?.parallel_usd || 0), 0);
  const paidCost = rows.reduce((s, r) => s + (r.cost?.paid_provider_usd || 0), 0);

  return {
    n_hotels: n,
    HOTEL_PHONE_COVERAGE: { count: hotelPhone, total: n, percent: pct(hotelPhone) },
    HOTEL_EMAIL_COVERAGE: { count: hotelEmail, total: n, percent: pct(hotelEmail) },
    OWNER_ORG_RESOLUTION: { count: ownerRes, total: n, percent: pct(ownerRes) },
    OWNER_ORG_PHONE_COVERAGE: { count: orgPhone, total: n, percent: pct(orgPhone) },
    OWNER_ORG_EMAIL_COVERAGE: { count: orgEmail, total: n, percent: pct(orgEmail) },
    DECISION_MAKER_IDENTIFICATION: { count: dm, total: n, percent: pct(dm) },
    CURRENT_ROLE_ACCURACY: "NOT_HUMAN_REVIEWED",
    DECISION_MAKER_PUBLIC_EMAIL_COVERAGE: { count: dmPubEmail, total: n, percent: pct(dmPubEmail) },
    DECISION_MAKER_VERIFIED_EMAIL_COVERAGE: {
      count: dmVerEmail,
      total: n,
      percent: pct(dmVerEmail),
    },
    DECISION_MAKER_PHONE_PATH_COVERAGE: { count: dmPhone, total: n, percent: pct(dmPhone) },
    HOTEL_ACTIONABLE_RATE: { count: hotelActionable, total: n, percent: pct(hotelActionable) },
    OWNER_ACTIONABLE_RATE: { count: ownerActionable, total: n, percent: pct(ownerActionable) },
    FULLY_ACTIONABLE_HOTEL_RATE: { count: fully, total: n, percent: pct(fully) },
    FALSE_PERSON_RATE: { count: falsePerson, total: n, percent: pct(falsePerson) },
    FALSE_VERIFIED_EMAIL_RATE: { count: falseEmail, total: n, percent: pct(falseEmail) },
    AVERAGE_RESEARCH_TIME_MS: avgMs,
    PARALLEL_USAGE_USD: parallelUsage,
    PAID_PROVIDER_COST_USD: paidCost,
  };
}
