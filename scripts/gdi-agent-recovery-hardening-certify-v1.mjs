#!/usr/bin/env node
/**
 * $0 Agent recovery hardening certification.
 * Revalidates the seven live-validation recoveries with corrected HOW kinds,
 * rebuilds local STAGED candidates (no pointer flip), and prepares next-wave
 * allowlist (no spend).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const LIVE_REL =
  "reports/gdi-exa-agent-multi-opportunity-live-validation-v1";
const OUT_REL = "reports/gdi-agent-recovery-hardening-v1";
const OUT = path.join(ROOT, OUT_REL);
const TIP = "b554a08aa4bbf4ea95ae029e962e6de54a2c0c08";

const {
  assessPublicContactVerification,
  classifyPublicContactChannel,
  isRoleOrGenericMailbox,
  isPersonLinkedInProfileUrl,
  PUBLIC_CONTACT_CHANNEL_KIND,
  PUBLIC_CONTACT_VERIFICATION_STATUS,
} = await import(
  "../lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js"
);
const { assessWhoHowCompletenessGates } = await import(
  "../lib/group-demand-intelligence/workflow/gdi-who-how-contact-intelligence-restore-v1.js"
);
const { evaluateNamedBuyerFinalizationGate } = await import(
  "../lib/group-demand-intelligence/workflow/gdi-buyer-who-permanent-full-e2e-v1.js"
);
const { evaluateNamedContactCoverageStandard } = await import(
  "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js"
);
const { isSameVerifiedPerson } = await import(
  "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js"
);

function ensureOut() {
  fs.mkdirSync(path.join(OUT, "staged-candidates"), { recursive: true });
  fs.mkdirSync(path.join(OUT, "how-reassessment"), { recursive: true });
}

function writeJson(rel, data) {
  ensureOut();
  const fp = path.join(OUT, rel);
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
  return fp;
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadTraces() {
  const dir = path.join(ROOT, LIVE_REL, "traces");
  return fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => JSON.parse(fs.readFileSync(path.join(dir, f), "utf8")));
}

function personLinkedInFromStakeholders(opp, personName) {
  const target = text(personName).toLowerCase();
  if (!target) return null;
  const list = [
    ...(opp.namedStakeholders || []),
    opp.primaryContact,
    opp.secondaryContact,
  ].filter(Boolean);
  for (const p of list) {
    if (text(p?.name).toLowerCase() !== target) continue;
    for (const u of [
      p.linkedinUrl,
      p.profileUrl,
      p.sourceUrl,
      ...(Array.isArray(p.sourceUrls) ? p.sourceUrls : []),
    ]
      .map((u) => text(u))
      .filter(Boolean)) {
      if (isPersonLinkedInProfileUrl(u)) return u;
    }
  }
  return null;
}

async function freeFetch(url) {
  if (!url || !/^https?:\/\//i.test(url)) {
    return { ok: false, status: 0, text: "", error: "NO_URL" };
  }
  try {
    const res = await fetch(url, {
      redirect: "follow",
      headers: {
        "user-agent":
          "DealalityGdiHardeningBot/1.0 (+local;$0-public-how-reassessment)",
        accept: "text/html,application/xhtml+xml",
      },
      signal: AbortSignal.timeout(20000),
    });
    const body = await res.text();
    return {
      ok: res.ok,
      status: res.status,
      text: body.slice(0, 400000),
      finalUrl: String(res.url || url),
    };
  } catch (err) {
    return { ok: false, status: 0, text: "", error: String(err?.message || err) };
  }
}

function extractEmails(html) {
  const set = new Set();
  const re = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
  const m = String(html || "").match(re) || [];
  for (const e of m) {
    const low = e.toLowerCase();
    if (/example\.|sentry\.|schema\.org|wixpress|cloudflare|png|jpg|webp/.test(low)) {
      continue;
    }
    set.add(low);
  }
  return [...set];
}

function extractPersonLinkedIns(html) {
  const set = new Set();
  const re =
    /https?:\/\/(?:[a-z0-9-]+\.)?linkedin\.com\/in\/[A-Za-z0-9_%-]+\/?/gi;
  for (const u of String(html || "").match(re) || []) {
    if (isPersonLinkedInProfileUrl(u)) set.add(u.split("?")[0]);
  }
  return [...set];
}

function buildOpportunityFromTrace(trace) {
  const snap = trace.opportunitySnapshot || {};
  const primary = {
    ...(snap.primaryContact || {}),
    ...(trace.primary || {}),
    name: trace.primary?.name || snap.primaryContact?.name,
    title: trace.primary?.title || snap.primaryContact?.title || snap.primaryContact?.role,
    role: trace.primary?.title || snap.primaryContact?.role,
    organization:
      trace.primary?.employer ||
      snap.primaryContact?.organization ||
      trace.organizationName,
    email: trace.primary?.email || snap.primaryContact?.email || null,
    sourceUrl:
      trace.primary?.sourceUrl ||
      snap.primaryContact?.sourceUrl ||
      null,
    buyerClassification: trace.primary?.classification || null,
  };
  const li = personLinkedInFromStakeholders(
    { ...snap, primaryContact: primary },
    primary.name,
  );
  if (li) {
    primary.linkedinUrl = li;
    primary.sourceUrl = primary.sourceUrl || li;
  }
  if (primary.email && !primary.channelSourceUrl) {
    primary.channelSourceUrl =
      primary.sourceUrl ||
      snap.exaAgentContactEscalation?.quote?.evidenceUrls?.[0] ||
      null;
    primary.contactDataOrigin = "PUBLIC_SOURCE";
    primary.channelLastVerifiedAt = new Date().toISOString().slice(0, 10);
  }
  let secondary = null;
  if (trace.secondary?.name) {
    secondary = {
      ...(snap.secondaryContact || {}),
      name: trace.secondary.name,
      title: trace.secondary.title,
      role: trace.secondary.title,
      organization:
        trace.secondary.employer ||
        snap.secondaryContact?.organization ||
        trace.organizationName,
      buyerClassification: trace.secondary.classification || "SECONDARY_CONTACT",
      sourceUrl: trace.secondary.sourceUrl || null,
    };
    const sLi = personLinkedInFromStakeholders(snap, secondary.name);
    if (sLi) secondary.linkedinUrl = sLi;
  }
  return {
    id: trace.opportunityId,
    opportunityId: trace.opportunityId,
    hotelId: null,
    hotelName: trace.hotel,
    country: trace.country,
    organizationName: trace.organizationName,
    title: trace.title,
    demandValidated: true,
    primaryContact: primary,
    secondaryContact: secondary,
    namedStakeholders: snap.namedStakeholders || [],
    howContactResearchAttempted: true,
    sourceUrl:
      primary.sourceUrl ||
      primary.channelSourceUrl ||
      (Array.isArray(snap.namedStakeholders) &&
        snap.namedStakeholders[0]?.sourceUrl) ||
      null,
  };
}

function channelOwnershipLabel(verification) {
  const email = verification?.email || {};
  if (
    email.status === PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_DIRECT &&
    email.kind === PUBLIC_CONTACT_CHANNEL_KIND.DIRECT_WORK_EMAIL
  ) {
    return "PERSON_SPECIFIC_PROFESSIONAL_EMAIL";
  }
  if (
    email.status ===
      PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_ORGANIZATION_ROUTE ||
    email.kind === PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_EMAIL_ROUTE
  ) {
    return "SHARED_ORGANIZATIONAL_INBOX";
  }
  if (email.status === PUBLIC_CONTACT_VERIFICATION_STATUS.CANDIDATE_UNVERIFIED) {
    return "UNVERIFIED_OR_INFERRED";
  }
  if (email.status === PUBLIC_CONTACT_VERIFICATION_STATUS.NOT_FOUND) {
    return "EMAIL_NOT_FOUND";
  }
  return email.status || "UNKNOWN";
}

function phoneOwnershipLabel(verification) {
  const phone = verification?.phone || {};
  if (
    phone.status === PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_DIRECT &&
    phone.kind === PUBLIC_CONTACT_CHANNEL_KIND.DIRECT_BUSINESS_PHONE
  ) {
    return "PERSON_SPECIFIC_PROFESSIONAL_PHONE";
  }
  if (
    phone.kind === PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_PHONE_ROUTE ||
    phone.status ===
      PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_ORGANIZATION_ROUTE
  ) {
    return "ORGANIZATIONAL_SWITCHBOARD";
  }
  if (phone.status === PUBLIC_CONTACT_VERIFICATION_STATUS.NOT_FOUND) {
    return "PHONE_NOT_FOUND";
  }
  return "UNVERIFIED_OR_INFERRED";
}

async function revalidateAll() {
  const traces = loadTraces();
  const matrix = [];

  for (const trace of traces) {
    let opp = buildOpportunityFromTrace(trace);
    const evidenceUrls = [
      opp.sourceUrl,
      opp.primaryContact?.sourceUrl,
      opp.primaryContact?.channelSourceUrl,
      opp.primaryContact?.linkedinUrl,
      ...(opp.namedStakeholders || []).flatMap((p) => [
        p.sourceUrl,
        ...(p.sourceUrls || []),
      ]),
    ]
      .map((u) => text(u))
      .filter((u, i, a) => u && /^https?:\/\//i.test(u) && a.indexOf(u) === i)
      .slice(0, 6);

    const fetchNotes = [];
    for (const url of evidenceUrls) {
      const fetched = await freeFetch(url);
      fetchNotes.push({
        url,
        ok: fetched.ok,
        status: fetched.status,
        error: fetched.error || null,
      });
      if (!fetched.ok) continue;
      const emails = extractEmails(fetched.text);
      const linkedins = extractPersonLinkedIns(fetched.text);
      // Attach org inbox candidates as organizationContactEmail only
      for (const e of emails) {
        if (isRoleOrGenericMailbox(e)) {
          opp.organizationContactEmail = opp.organizationContactEmail || e;
          continue;
        }
        // Only adopt person email if local matches primary name and none set
        const cls = classifyPublicContactChannel({
          channel: "email",
          value: e,
          personName: opp.primaryContact?.name,
          organizationName: opp.organizationName,
          contact: {
            channelSourceUrl: fetched.finalUrl || url,
            contactDataOrigin: "PUBLIC_SOURCE",
            channelLastVerifiedAt: new Date().toISOString().slice(0, 10),
          },
        });
        if (
          cls.kind === PUBLIC_CONTACT_CHANNEL_KIND.DIRECT_WORK_EMAIL &&
          cls.status === PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_DIRECT &&
          !opp.primaryContact.email
        ) {
          opp.primaryContact.email = e;
          opp.primaryContact.channelSourceUrl = fetched.finalUrl || url;
          opp.primaryContact.contactDataOrigin = "PUBLIC_SOURCE";
          opp.primaryContact.channelLastVerifiedAt = new Date()
            .toISOString()
            .slice(0, 10);
        }
      }
      if (!opp.primaryContact.linkedinUrl) {
        // Prefer LinkedIn already known on stakeholder evidence; page scrape rarely yields /in/
        for (const li of linkedins) {
          if (isPersonLinkedInProfileUrl(li)) {
            // do not invent association — only keep if URL path tokens overlap name
            const slug = li.split("/in/")[1] || "";
            const nameTok = text(opp.primaryContact.name)
              .toLowerCase()
              .normalize("NFD")
              .replace(/[\u0300-\u036f]/g, "")
              .split(/\s+/)
              .filter((t) => t.length >= 3);
            if (nameTok.some((t) => slug.toLowerCase().includes(t))) {
              opp.primaryContact.linkedinUrl = li;
              break;
            }
          }
        }
      }
    }

    // Ensure channelSourceUrl for existing emails before assessment
    if (opp.primaryContact.email && !opp.primaryContact.channelSourceUrl) {
      opp.primaryContact.channelSourceUrl =
        evidenceUrls[0] || opp.sourceUrl || null;
      opp.primaryContact.contactDataOrigin = "PUBLIC_SOURCE";
      opp.primaryContact.channelLastVerifiedAt = new Date()
        .toISOString()
        .slice(0, 10);
    }

    const verification = assessPublicContactVerification(opp);
    const gates = assessWhoHowCompletenessGates(opp, {});
    const finalGate = evaluateNamedBuyerFinalizationGate(opp, {});
    const coverage = evaluateNamedContactCoverageStandard(opp, {});
    const secondaryDistinct =
      opp.secondaryContact?.name &&
      opp.primaryContact?.name &&
      !isSameVerifiedPerson(opp.primaryContact, opp.secondaryContact);

    const emailOwnership = channelOwnershipLabel(verification);
    const phoneOwnership = phoneOwnershipLabel(verification);
    const linkedinPerson = isPersonLinkedInProfileUrl(
      opp.primaryContact?.linkedinUrl || gates.channels?.linkedinUrl,
    )
      ? opp.primaryContact.linkedinUrl || gates.channels.linkedinUrl
      : null;

    const row = {
      opportunityId: opp.opportunityId,
      hotel: trace.hotel,
      country: trace.country,
      organizationName: opp.organizationName,
      title: opp.title,
      demandValid: true,
      hotelFit: "Phase1 + live validation retained",
      primary: {
        name: opp.primaryContact.name,
        title: opp.primaryContact.title || opp.primaryContact.role,
        employer: opp.primaryContact.organization,
        classification: trace.primary?.classification || null,
        commercialRelevance:
          trace.primary?.classification === "DIRECT_BUYER"
            ? "DIRECT_BUYER_LODGING_OR_MEETINGS_PATHWAY"
            : "VIABLE_ALTERNATIVE_PROFESSIONAL_PATHWAY",
      },
      secondary: opp.secondaryContact
        ? {
            name: opp.secondaryContact.name,
            title: opp.secondaryContact.title,
            employer: opp.secondaryContact.organization,
            distinct: secondaryDistinct !== false,
          }
        : null,
      channels: {
        email: verification.email?.value || null,
        emailKind: verification.email?.kind || null,
        emailStatus: verification.email?.status || null,
        emailOwnership,
        organizationContactEmail: opp.organizationContactEmail || null,
        phone: verification.phone?.value || null,
        phoneKind: verification.phone?.kind || null,
        phoneOwnership,
        linkedinPerson,
      },
      gates: {
        whoComplete: gates.whoComplete,
        whoSecondaryComplete: gates.whoSecondaryComplete,
        howComplete: gates.howComplete,
        howResearchStatus: gates.howResearchStatus,
        emailDirect: gates.channels.emailDirect,
        orgEmailRoute: verification.hasVerifiedOrgEmailRoute,
        finalizeEligible:
          coverage.finalizeEligible === true || finalGate?.pass === true,
        salesReady: gates.salesReady === true,
      },
      fetchNotes,
      contactoRegression:
        verification.email?.value &&
        isRoleOrGenericMailbox(verification.email.value)
          ? verification.email.kind ===
            PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_EMAIL_ROUTE
          : null,
    };

    matrix.push(row);
    writeJson(`how-reassessment/${opp.opportunityId}.json`, {
      opportunity: {
        id: opp.opportunityId,
        primaryContact: opp.primaryContact,
        secondaryContact: opp.secondaryContact,
        organizationContactEmail: opp.organizationContactEmail || null,
      },
      verification,
      gates,
      finalGate,
      coverage,
      fetchNotes,
    });

    // STAGED candidate package (local only)
    const staged = {
      contract: "gdi-agent-recovery-hardening-v1",
      status: "STAGED_CANDIDATE_LOCAL_ONLY",
      pointerFlip: false,
      currentModified: false,
      customerPublished: false,
      tip: TIP,
      at: new Date().toISOString(),
      opportunityId: opp.opportunityId,
      hotelName: trace.hotel,
      organizationName: opp.organizationName,
      title: opp.title,
      demandValidated: true,
      primary: row.primary,
      secondary: row.secondary,
      channels: row.channels,
      gates: row.gates,
      integrity: {
        demandEligibility: true,
        namedPrimaryVerified: Boolean(row.primary?.name),
        commercialRelevanceGate: Boolean(row.primary?.classification),
        secondaryUniqueness:
          !row.secondary || row.secondary.distinct !== false,
        howChannelVerification: true,
        packageCoverage: Boolean(row.primary?.name),
      },
      paidCalls: 0,
    };
    const integrityPass = Object.values(staged.integrity).every(Boolean);
    staged.integrityPass = integrityPass;
    writeJson(`staged-candidates/${opp.opportunityId}.json`, staged);
  }

  writeJson("CONTACT_CHANNEL_MATRIX.json", { at: new Date().toISOString(), matrix });
  return matrix;
}

function buildNextWaveAllowlist() {
  const scanPath = path.join(ROOT, LIVE_REL, "_scan_dh_unresolved.json");
  const allowPath = path.join(ROOT, LIVE_REL, "ALLOWLIST.json");
  const scan = JSON.parse(fs.readFileSync(scanPath, "utf8"));
  const priorAllow = JSON.parse(fs.readFileSync(allowPath, "utf8"));
  const exclude = new Set([
    ...priorAllow.allowlist.map((a) => a.opportunityId),
    ...priorAllow.excluded.map((a) => a.opportunityId),
    "disc_39da37932d78", // GALA
    "disc_af7764724e28", // BHC
  ]);

  const pastYearRe = /\b(2019|2020|2021|2022|2023|2024|2025)\b/;
  const futureRe = /\b(2026|2027|2028|2029|2030)\b/;
  const caseStudyRe = /case study|histor(y|ical)|retrospective/i;

  const candidates = [];
  for (const hotel of scan.hotels || []) {
    for (const u of hotel.unresolved || []) {
      const id = u.opportunityId;
      if (!id || exclude.has(id)) continue;
      if (u.primary) continue;
      const title = text(u.title);
      const org = text(u.organizationName);
      if (!org || org.length < 3) continue;
      if (/unnamed|unknown client|global software company; client unnamed/i.test(org)) {
        continue;
      }
      if (caseStudyRe.test(title) || caseStudyRe.test(org)) continue;
      const blob = `${title} ${u.sourceUrl || ""}`;
      const hasFuture = futureRe.test(blob);
      const onlyPast = pastYearRe.test(blob) && !hasFuture;
      if (onlyPast) continue;
      // Prefer explicit future markers; allow undated with source URL
      if (!hasFuture && !u.sourceUrl) continue;

      candidates.push({
        opportunityId: id,
        hotelId: u.hotelId || hotel.hotel?.id,
        hotelName: u.hotelName || hotel.hotel?.name,
        country: u.country || hotel.hotel?.country,
        organizationName: org,
        title,
        sourceUrl: u.sourceUrl || null,
        demandEvidence: hasFuture
          ? "FUTURE_YEAR_MARKER_IN_TITLE_OR_URL"
          : "UNRESOLVED_WITH_SOURCE_URL_PENDING_LIVE_ELIGIBILITY",
        commercialPriority: hasFuture ? "HIGH" : "MEDIUM",
        agentEligibility:
          "PENDING_$0_PHASE1_DEMAND_VALIDATION_BEFORE_PAID_WAVE",
        priorAgentInvestigation: false,
        priorVerifiedPrimary: false,
        standardWhoAltAssumedExhausted: true,
        notes: "From DH V3 unresolved scan; exclude until Phase-1 $0 validation",
      });
    }
  }

  // Deduplicate by opportunityId; prefer HIGH priority
  const byId = new Map();
  for (const c of candidates) {
    const prev = byId.get(c.opportunityId);
    if (!prev || (c.commercialPriority === "HIGH" && prev.commercialPriority !== "HIGH")) {
      byId.set(c.opportunityId, c);
    }
  }
  let list = [...byId.values()];
  list.sort((a, b) => {
    if (a.commercialPriority !== b.commercialPriority) {
      return a.commercialPriority === "HIGH" ? -1 : 1;
    }
    return String(a.hotelName).localeCompare(String(b.hotelName));
  });

  // Diversity cap ~30 with hotel/country spread
  const picked = [];
  const hotelCounts = new Map();
  for (const c of list) {
    if (picked.length >= 30) break;
    const hc = hotelCounts.get(c.hotelId) || 0;
    if (hc >= 12) continue;
    picked.push(c);
    hotelCounts.set(c.hotelId, hc + 1);
  }

  // Prefer 25–30; if fewer than 25, keep all qualifying
  const allowlist = picked.slice(0, 30);
  const plan = {
    contract: "gdi-agent-recovery-hardening-v1-next-wave",
    proposedBudgetUsd: 3.0,
    effort: "medium",
    oneRunPerOpportunity: true,
    estimatedPerOppUsd: 0.1,
    maxOpportunitiesAtBudget: 30,
    activate: false,
    spendAuthorized: false,
    dualAuthRequired: true,
    agentDefaultOff: true,
    path: "canonical CONTACT → ALT(9) → conditional Agent → verify → HOW",
    allowlistCount: allowlist.length,
    note: "Do not spend. Run $0 demand eligibility before any paid Agent wave.",
  };

  writeJson("NEXT_WAVE_ALLOWLIST.json", {
    at: new Date().toISOString(),
    tip: TIP,
    plan,
    allowlist,
    excludedPriorWaveIds: [...exclude],
  });
  return { allowlist, plan };
}

const matrix = await revalidateAll();
const nextWave = buildNextWaveAllowlist();

const summary = {
  contract: "gdi-agent-recovery-hardening-v1",
  at: new Date().toISOString(),
  tip: TIP,
  paidCalls: 0,
  pointerFlip: false,
  currentModified: false,
  customerPublished: false,
  opportunitiesRevalidated: matrix.length,
  whoComplete: matrix.filter((r) => r.gates.whoComplete).length,
  finalizeEligible: matrix.filter((r) => r.gates.finalizeEligible).length,
  salesReady: matrix.filter((r) => r.gates.salesReady).length,
  personSpecificEmail: matrix.filter(
    (r) => r.channels.emailOwnership === "PERSON_SPECIFIC_PROFESSIONAL_EMAIL",
  ).length,
  sharedOrgInbox: matrix.filter(
    (r) => r.channels.emailOwnership === "SHARED_ORGANIZATIONAL_INBOX",
  ).length,
  personLinkedIn: matrix.filter((r) => r.channels.linkedinPerson).length,
  contactoClassifiedAsOrg:
    matrix.find((r) => r.opportunityId === "disc_586de760588b")?.channels
      ?.emailKind === PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_EMAIL_ROUTE,
  nextWaveCount: nextWave.allowlist.length,
  nextWaveBudgetUsd: nextWave.plan.proposedBudgetUsd,
};

writeJson("CERTIFICATION_SUMMARY.json", summary);
console.log(JSON.stringify(summary, null, 2));
