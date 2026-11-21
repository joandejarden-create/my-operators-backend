/**
 * Founder-authorized PDL + Prospeo controlled enrichment pilot (10 contacts).
 *
 * Ceilings:
 *   PDL: ≤10 successful (HTTP 200) enrichment credits
 *   Prospeo: ≤10 email-finder credits
 * Mobile / FullEnrich / Surfe enrich / Apollo: NOT AUTHORIZED
 *
 * Provider-derived channels stay in internal evaluation report only —
 * never written to canonical public-source fields.
 *
 * Usage:
 *   node scripts/gdi-pdl-prospeo-enrichment-pilot-10-v1.mjs
 *   node scripts/gdi-pdl-prospeo-enrichment-pilot-10-v1.mjs --dry-run
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  enrichPdlPerson,
  describePdlKeyPresence,
  PDL_CREDIT_COSTS,
  sanitizePdlParamsForLog,
} from "../lib/pdl/client.js";
import { scorePdlEnrichmentResult } from "../lib/hotel-intelligence/contact-intelligence/pdl-contact-enrichment-eval.js";
import {
  enrichGdiContactViaProspeo,
  getProspeoAccountInformation,
  describeProspeoKeyPresence,
} from "../lib/group-demand-intelligence/presentation/gdi-prospeo-contact-enrichment-v1.js";
import {
  runContactIdentityPreflight,
  CONTACT_IDENTITY_READINESS,
  LINKEDIN_PROFILE_MATCH,
  buildSurfeEnrichmentIdentityInput,
} from "../lib/group-demand-intelligence/presentation/gdi-contact-identity-preflight-v1.js";
import {
  classifyLinkedInProfileMatch,
} from "../lib/group-demand-intelligence/presentation/gdi-google-grounded-contact-discovery-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports",
  "gdi-pdl-prospeo-enrichment-pilot-v1",
);
const DRY_RUN = process.argv.includes("--dry-run");

const PDL_CEILING = 10;
const PROSPEO_CEILING = 10;

const KNOWN_DOMAINS = Object.freeze({
  "the tie": "thetie.io",
  "boutique & luxury lodging association": "bllaevents.com",
  "international association of airport and duty free stores (iaadfs)":
    "iaadfs.org",
  iaadfs: "iaadfs.org",
  "easyfairs group": "easyfairs.com",
  "insurance bureau of canada": "ibc.ca",
  "alexandria soccer association": "alexandriasoccer.com",
  "university of toronto": "utoronto.ca",
  "international chamber of commerce": "iccwbo.org",
  "world chambers federation": "iccwbo.org",
  oenoviti: "oenoviti.u-bordeaux.fr",
  red: "redbull.com",
  "red bull": "redbull.com",
});

const COHORT = [
  {
    personName: "Abbas Zavar",
    opportunityId: "rx_d_2400ebe4c2bb",
    hotelId: "recH9h3McgMNFQEla",
  },
  {
    personName: "Daryl Simon",
    opportunityId: "wy_pd_fc13e2e08dd5",
    hotelId: "recXh4NgF8sDbi9cQ",
  },
  {
    personName: "Ariela Kiradjian",
    opportunityId: "gdi_opp_15th_annual_boutique_hotel_investment_conference_11",
    hotelId: "gdi_hotel_renaissance_times_square",
  },
  {
    personName: "Francesco Francavilla",
    opportunityId: "gdi_opp_wrome_pilot_v0_red_bull_sailgp_rome_2027",
    hotelId: "rece0or38cxo3Fymb",
  },
  {
    personName: "Olivia Jallits",
    opportunityId: "gdi_opp_iaadfs_summit_of_the_americas_2027_6",
    hotelId: "recgMYovrrZDJMqzX",
  },
  {
    personName: "Aimee Welsh",
    opportunityId: "rx_d_5976330f194d",
    hotelId: "recH9h3McgMNFQEla",
  },
  {
    personName: "Agathe Lairy",
    opportunityId: "rx_d_52859ee0961e",
    hotelId: "recrPQcZg7SFARRb2",
  },
  {
    personName: "Alexandre Catton",
    opportunityId: "rx_d_d29851414427",
    hotelId: "recrPQcZg7SFARRb2",
  },
  {
    personName: "Andrew Bartucci",
    opportunityId: "rx_d_096138499154",
    hotelId: "recH9h3McgMNFQEla",
  },
  {
    personName: "Ben Hawkins",
    opportunityId: "gdi_opp_alexandria_soccer_kickoff_2027",
    hotelId: "recLuxvwwxID7U2B8",
  },
];

function loadJson(rel) {
  return JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function stripAccents(s) {
  return text(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function lookupDomain(employerName) {
  const key = stripAccents(employerName);
  if (KNOWN_DOMAINS[key]) return KNOWN_DOMAINS[key];
  for (const [k, d] of Object.entries(KNOWN_DOMAINS)) {
    if (key.includes(k) || k.includes(key.slice(0, 16))) return d;
  }
  return null;
}

function splitName(fullName = "") {
  const parts = text(fullName).split(/\s+/).filter(Boolean);
  if (!parts.length) return { first_name: null, last_name: null };
  if (parts.length === 1) return { first_name: parts[0], last_name: null };
  return { first_name: parts[0], last_name: parts.slice(1).join(" ") };
}

function assembleBaseline() {
  const sp = loadJson(
    "reports/gdi-google-first-second-pass-v1/SECOND_PASS_RESULTS.json",
  ).results;
  let p2 = null;
  try {
    p2 = loadJson(
      "reports/gdi-contact-identity-preflight-v1/phase2-linkedin-recovery/PHASE2_RESULTS.json",
    );
  } catch {
    p2 = null;
  }
  let p1 = null;
  try {
    p1 = loadJson(
      "reports/gdi-contact-identity-preflight-v1/PILOT_20_RESULTS.json",
    );
  } catch {
    p1 = null;
  }

  return COHORT.map((c) => {
    const s = sp.find(
      (r) =>
        r.opportunityId === c.opportunityId || r.personName === c.personName,
    );
    const idRow =
      (p2?.results || []).find((r) => r.opportunityId === c.opportunityId) ||
      (p1?.results || []).find((r) => r.opportunityId === c.opportunityId) ||
      null;

    const employerName =
      idRow?.identity?.employerName ||
      s?.organization ||
      null;
    let domain =
      idRow?.identity?.employerDomain ||
      lookupDomain(employerName) ||
      null;
    const title = idRow?.identity?.title || null;

    let linkedinUrl =
      idRow?.identity?.linkedinTrustedForSurfe
        ? idRow.identity.linkedinUrl
        : null;
    let linkedinTrusted = idRow?.identity?.linkedinTrustedForSurfe === true;
    // Second-pass VERIFIED only when identity preflight did not reject
    if (
      !linkedinTrusted &&
      s?.linkedInStatus === "VERIFIED_LINKEDIN_PROFILE" &&
      s?.linkedInUrl
    ) {
      const re = classifyLinkedInProfileMatch({
        url: s.linkedInUrl,
        personName: c.personName,
        organization: employerName,
        evidenceText: `${employerName || ""} ${s.organization || ""} ${c.personName}`,
      });
      if (re.status === LINKEDIN_PROFILE_MATCH.VERIFIED_LINKEDIN_PROFILE) {
        linkedinUrl = re.url;
        linkedinTrusted = true;
      }
    }

    const preflight = runContactIdentityPreflight({
      opportunity: {
        id: c.opportunityId,
        hotelId: c.hotelId,
        organizationName: employerName,
      },
      contact: {
        name: c.personName,
        title,
        employerName,
        employerDomain: domain,
        email: s?.email || null,
        linkedinUrl,
        linkedinEvidenceText: linkedinTrusted
          ? `${c.personName} ${employerName} verified`
          : null,
      },
      employerDomain: domain,
      assumeDemandOrgIsEmployer: false,
    });

    if (linkedinTrusted && linkedinUrl) {
      preflight.identity.linkedinUrl = linkedinUrl;
      preflight.identity.linkedinTrustedForSurfe = true;
      preflight.identity.linkedinStatus =
        LINKEDIN_PROFILE_MATCH.VERIFIED_LINKEDIN_PROFILE;
      if (
        preflight.identity.firstName &&
        preflight.identity.lastName &&
        preflight.identity.employerName
      ) {
        preflight.identity.readiness = CONTACT_IDENTITY_READINESS.READY;
      }
    }

    const existingEmail =
      s?.email && !/^(info|contact|hall|training)@/i.test(s.email)
        ? s.email
        : null;

    return {
      ...c,
      employerName,
      employerDomain: domain || preflight.identity.employerDomain,
      title,
      linkedinUrl: preflight.identity.linkedinUrl,
      linkedinTrusted: preflight.identity.linkedinTrustedForSurfe === true,
      existingEmail,
      existingPhone: s?.phone || null,
      readinessBefore: preflight.identity.readiness,
      identity: preflight.identity,
      surfeInputBefore: buildSurfeEnrichmentIdentityInput(preflight.identity),
      secondPassStatus: s?.status || null,
    };
  });
}

function pdlCreditForResponse(res) {
  // Founder ceiling: successful enrichment credits = HTTP 200 matches
  if (res?.http_status === 200 && res?.normalized?.matched) return 1;
  const hdr = res?.credit_headers?.call_credits_spent;
  if (Number.isFinite(hdr) && hdr > 0) return Number(hdr);
  return 0;
}

async function runPdl(contact, creditsUsed) {
  if (creditsUsed >= PDL_CEILING) {
    return { skipped: true, reason: "PDL_CREDIT_CEILING", creditsConsumed: 0 };
  }
  const { first_name, last_name } = splitName(contact.personName);
  const person = {
    first_name,
    last_name,
    company_name: contact.employerName,
    company_domain: contact.employerDomain,
    linkedin_url: contact.linkedinTrusted ? contact.linkedinUrl : null,
  };
  // Never email/phone as input — buildPdlPersonEnrichParams enforces

  try {
    const res = await enrichPdlPerson(person, {
      min_likelihood: 6,
      retries: 0,
    });
    const credits = pdlCreditForResponse(res);
    const score = scorePdlEnrichmentResult({
      subject: {
        full_name: contact.personName,
        first_name,
        last_name,
        domain: contact.employerDomain,
        company_domain: contact.employerDomain,
        company_name: contact.employerName,
        known_email_for_scoring: contact.existingEmail,
      },
      normalized: res.normalized,
      http_status: res.http_status,
      latency_ms: res.latency_ms,
      cost_credits: credits,
    });

    // GDI stricter employer gate: reject clear cross-employer matches.
    const pdlCompany = String(
      res.normalized?.identity?.job_company_name || "",
    ).toLowerCase();
    const expectedEmp = String(contact.employerName || "").toLowerCase();
    const empTokens = expectedEmp
      .split(/[^a-z0-9]+/)
      .filter((t) => t.length > 3 && !/^(the|and|association|international|university|institute)$/.test(t))
      .slice(0, 4);
    const employerCorroborated =
      !pdlCompany ||
      !expectedEmp ||
      empTokens.some((t) => pdlCompany.includes(t)) ||
      pdlCompany.split(/[^a-z0-9]+/).some((t) => t.length > 3 && expectedEmp.includes(t));
    if (res.normalized?.matched && !employerCorroborated) {
      score.identity_match_accepted = false;
      score.affiliation_accepted = false;
      score.bucket = "IDENTITY_CONFLICT";
      score.notes = [...(score.notes || []), "GDI_EMPLOYER_MISMATCH_QUARANTINE"];
    }

    const li = res.normalized?.identity?.linkedin_url || null;
    let linkedInVerified = false;
    if (li) {
      const cls = classifyLinkedInProfileMatch({
        url: li,
        personName: contact.personName,
        organization:
          res.normalized?.identity?.job_company_name || contact.employerName,
        evidenceText: `${res.normalized?.identity?.job_title || ""} ${contact.employerName || ""}`,
      });
      linkedInVerified =
        cls.status === LINKEDIN_PROFILE_MATCH.VERIFIED_LINKEDIN_PROFILE;
      if (
        cls.status === LINKEDIN_PROFILE_MATCH.PROFILE_MATCH_UNCERTAIN &&
        score.identity_match_accepted
      ) {
        // keep as candidate only
      }
    }

    // Masked boolean emails are not usable
    const workEmail =
      typeof res.normalized?.work_email === "string"
        ? res.normalized.work_email
        : null;
    const usableEmails = (res.normalized?.emails || []).filter(
      (e) => typeof e === "string" || typeof e?.address === "string",
    );

    return {
      skipped: false,
      creditsConsumed: credits,
      http_status: res.http_status,
      matched: res.normalized?.matched === true,
      likelihood: res.normalized?.likelihood ?? null,
      score,
      identity: {
        full_name: res.normalized?.identity?.full_name || null,
        job_title: res.normalized?.identity?.job_title || null,
        job_company_name: res.normalized?.identity?.job_company_name || null,
        job_company_website:
          res.normalized?.identity?.job_company_website || null,
        linkedin_url: li,
        linkedInVerified,
      },
      work_email: workEmail,
      emails_usable: usableEmails,
      field_presence: res.normalized?.field_presence?.contact_fields || null,
      credit_headers: res.credit_headers,
      request: sanitizePdlParamsForLog({
        first_name,
        last_name,
        company: contact.employerDomain || contact.employerName,
        profile: person.linkedin_url || undefined,
      }),
      identityAccepted: score.identity_match_accepted === true,
      affiliationAccepted: score.affiliation_accepted === true,
      quarantined:
        res.normalized?.matched === true &&
        score.identity_match_accepted !== true,
      dataOrigin: "PDL_PROVIDER_DERIVED",
      independentlyVerifiedPublic: false,
    };
  } catch (err) {
    return {
      skipped: false,
      error: String(err?.message || err).slice(0, 200),
      code: err?.code || null,
      creditsConsumed: 0,
    };
  }
}

async function runProspeo(contact, creditsUsed, pdlResult) {
  if (creditsUsed >= PROSPEO_CEILING) {
    return { skipped: true, reason: "PROSPEO_CREDIT_CEILING", creditsConsumed: 0 };
  }
  // Prefer PDL-improved LinkedIn / domain when verified
  const li =
    (pdlResult?.linkedInVerified && pdlResult?.identity?.linkedin_url) ||
    (contact.linkedinTrusted && contact.linkedinUrl) ||
    null;
  const domain =
    pdlResult?.identity?.job_company_website ||
    contact.employerDomain ||
    null;
  const employer =
    (pdlResult?.identityAccepted && pdlResult?.identity?.job_company_name) ||
    contact.employerName;

  const result = await enrichGdiContactViaProspeo({
    authorizeLiveProspeo: true,
    creditCeiling: PROSPEO_CEILING,
    creditsConsumedSoFar: creditsUsed,
    contact: {
      name: contact.personName,
      employerName: employer,
      employerDomain: domain,
      linkedinUrl: li,
      linkedinTrustedForSurfe: Boolean(li),
      email: contact.existingEmail,
    },
    existingEmail: contact.existingEmail,
  });
  return result;
}

function improveSurfeReadiness(contact, pdl, prospeo) {
  const patches = {
    name: contact.personName,
    title:
      (pdl?.identityAccepted && pdl?.identity?.job_title) || contact.title,
    employerName:
      (pdl?.identityAccepted && pdl?.identity?.job_company_name) ||
      contact.employerName,
    employerDomain:
      (pdl?.identityAccepted && pdl?.identity?.job_company_website) ||
      contact.employerDomain,
    linkedinUrl: null,
  };
  if (pdl?.linkedInVerified && pdl?.identity?.linkedin_url) {
    patches.linkedinUrl = pdl.identity.linkedin_url;
  } else if (contact.linkedinTrusted && contact.linkedinUrl) {
    patches.linkedinUrl = contact.linkedinUrl;
  }

  const after = runContactIdentityPreflight({
    opportunity: {
      id: contact.opportunityId,
      hotelId: contact.hotelId,
    },
    contact: {
      ...patches,
      linkedinEvidenceText: patches.linkedinUrl
        ? `${patches.name} ${patches.employerName} pdl_or_baseline`
        : null,
    },
    employerDomain: patches.employerDomain,
    assumeDemandOrgIsEmployer: false,
  });

  if (patches.linkedinUrl && after.identity.linkedinTrustedForSurfe) {
    // ok
  } else if (patches.linkedinUrl && pdl?.linkedInVerified) {
    after.identity.linkedinUrl = patches.linkedinUrl;
    after.identity.linkedinTrustedForSurfe = true;
    after.identity.linkedinStatus =
      LINKEDIN_PROFILE_MATCH.VERIFIED_LINKEDIN_PROFILE;
    after.identity.readiness = CONTACT_IDENTITY_READINESS.READY;
  }

  const surfeAfter = buildSurfeEnrichmentIdentityInput(after.identity);
  return {
    readinessAfter: after.identity.readiness,
    identityAfter: after.identity,
    surfeInputAfter: surfeAfter,
    movedPartialToReady:
      contact.readinessBefore === CONTACT_IDENTITY_READINESS.PARTIAL &&
      after.identity.readiness === CONTACT_IDENTITY_READINESS.READY,
    prospeoEmailAccepted:
      prospeo?.ok === true && Boolean(prospeo?.contactDetails?.email),
  };
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const pdlKey = describePdlKeyPresence();
  const prospeoKey = describeProspeoKeyPresence();

  let prospeoBefore = null;
  try {
    const acc = await getProspeoAccountInformation();
    prospeoBefore = {
      ok: acc.ok,
      plan: acc.payload?.response?.current_plan ?? null,
      remaining: acc.payload?.response?.remaining_credits ?? null,
      used: acc.payload?.response?.used_credits ?? null,
    };
  } catch (err) {
    prospeoBefore = { ok: false, error: String(err.message || err).slice(0, 120) };
  }

  if (!pdlKey.present) {
    console.error("PDL key missing — stop PDL");
  }
  if (!prospeoKey.present || prospeoBefore?.remaining == null) {
    console.error("Prospeo balance uncertain — stop Prospeo if cannot establish");
  }
  if (prospeoBefore?.remaining != null && prospeoBefore.remaining < 1) {
    throw new Error("Prospeo remaining credits < 1 — abort Prospeo");
  }

  const baseline = assembleBaseline();
  const results = [];
  let pdlCredits = 0;
  let prospeoCredits = 0;
  const pdlHeaderTrail = [];

  for (const contact of baseline) {
    const row = {
      personName: contact.personName,
      opportunityId: contact.opportunityId,
      hotelId: contact.hotelId,
      baseline: {
        readiness: contact.readinessBefore,
        employerName: contact.employerName,
        employerDomain: contact.employerDomain,
        linkedinUrl: contact.linkedinUrl,
        linkedinTrusted: contact.linkedinTrusted,
        existingEmail: contact.existingEmail,
        existingPhone: contact.existingPhone,
        surfeFieldNames: contact.surfeInputBefore.surfeFieldNames,
      },
      pdl: null,
      prospeo: null,
      after: null,
      skippedEntirely: false,
    };

    // Skip only if named email already present AND identity READY with LI
    // (founder: skip when identities AND required channels sufficiently resolved)
    if (
      contact.existingEmail &&
      contact.readinessBefore === CONTACT_IDENTITY_READINESS.READY &&
      contact.linkedinTrusted
    ) {
      row.skippedEntirely = true;
      row.skipReason = "IDENTITY_AND_EMAIL_ALREADY_RESOLVED";
      results.push(row);
      continue;
    }

    if (DRY_RUN) {
      row.pdl = { dryRun: true, wouldCall: true };
      row.prospeo = { dryRun: true, wouldCall: !contact.existingEmail };
      results.push(row);
      continue;
    }

    // --- PDL ---
    if (pdlKey.present && pdlCredits < PDL_CEILING) {
      const pdl = await runPdl(contact, pdlCredits);
      row.pdl = pdl;
      pdlCredits += Number(pdl.creditsConsumed || 0);
      if (pdl.credit_headers) pdlHeaderTrail.push(pdl.credit_headers);
      if (pdlCredits >= PDL_CEILING) {
        row.pdl.ceilingReachedAfter = true;
      }
    } else {
      row.pdl = {
        skipped: true,
        reason: !pdlKey.present ? "PDL_KEY_MISSING" : "PDL_CREDIT_CEILING",
        creditsConsumed: 0,
      };
    }

    // --- Prospeo email ---
    if (
      prospeoKey.present &&
      prospeoBefore?.remaining != null &&
      prospeoCredits < PROSPEO_CEILING &&
      !contact.existingEmail
    ) {
      const prospeo = await runProspeo(contact, prospeoCredits, row.pdl);
      row.prospeo = {
        skipped: prospeo.skipped === true,
        reason: prospeo.reason || null,
        ok: prospeo.ok === true,
        miss: prospeo.miss === true,
        quarantined: prospeo.quarantined === true,
        creditsConsumed: Number(prospeo.creditsConsumed || 0),
        verification: prospeo.verification || null,
        email: prospeo.contactDetails?.email || null,
        free_enrichment: prospeo.free_enrichment === true,
        request_sanitized: prospeo.request_sanitized || null,
        http_status: prospeo.http_status,
        dataOrigin: "PROSPEO_PROVIDER_DERIVED",
        independentlyVerifiedPublic: false,
      };
      prospeoCredits += Number(prospeo.creditsConsumed || 0);
    } else {
      row.prospeo = {
        skipped: true,
        reason: contact.existingEmail
          ? "NAMED_EMAIL_ALREADY_PRESENT"
          : !prospeoKey.present
            ? "PROSPEO_KEY_MISSING"
            : prospeoCredits >= PROSPEO_CEILING
              ? "PROSPEO_CREDIT_CEILING"
              : "PROSPEO_BALANCE_UNCERTAIN",
        creditsConsumed: 0,
      };
    }

    row.after = improveSurfeReadiness(contact, row.pdl, row.prospeo);
    results.push(row);

    // Soft pause between paid calls
    await new Promise((r) => setTimeout(r, 400));
  }

  let prospeoAfter = null;
  try {
    const acc = await getProspeoAccountInformation();
    prospeoAfter = {
      ok: acc.ok,
      plan: acc.payload?.response?.current_plan ?? null,
      remaining: acc.payload?.response?.remaining_credits ?? null,
      used: acc.payload?.response?.used_credits ?? null,
    };
  } catch (err) {
    prospeoAfter = { ok: false, error: String(err.message || err).slice(0, 120) };
  }

  const pdlMatched = results.filter((r) => r.pdl?.matched).length;
  const pdlAccepted = results.filter((r) => r.pdl?.identityAccepted).length;
  const pdlLi = results.filter((r) => r.pdl?.linkedInVerified).length;
  const prospeoEmails = results.filter((r) => r.prospeo?.ok && r.prospeo?.email);
  const quarantined = results.filter(
    (r) => r.pdl?.quarantined || r.prospeo?.quarantined,
  );
  const movedReady = results.filter((r) => r.after?.movedPartialToReady).length;
  const overlap = results.filter(
    (r) =>
      r.pdl?.work_email &&
      r.prospeo?.email &&
      text(r.pdl.work_email).toLowerCase() === text(r.prospeo.email).toLowerCase(),
  );

  const report = {
    generatedAt: new Date().toISOString(),
    version: "gdi-pdl-prospeo-enrichment-pilot-v1",
    dryRun: DRY_RUN,
    authorization: {
      pdlCeiling: PDL_CEILING,
      prospeoCeiling: PROSPEO_CEILING,
      mobile: false,
      fullenrich: false,
      surfeEnrich: false,
      apollo: false,
      newPurchases: false,
    },
    pr484: {
      state: "OPEN",
      url: "https://github.com/joandejarden-create/my-operators-backend/pull/484",
      note: "Pilot lands on same GDI contact-enrichment branch; IC owns merge.",
    },
    balances: {
      pdl: {
        keyPresent: pdlKey.present,
        note: "No free balance endpoint; credits inferred from HTTP 200 matches + response headers",
        creditsConsumedSuccessfulMatches: pdlCredits,
        headerTrail: pdlHeaderTrail.slice(0, 10),
      },
      prospeo: {
        before: prospeoBefore,
        after: prospeoAfter,
        creditsConsumedEstimate: prospeoCredits,
        deltaRemaining:
          prospeoBefore?.remaining != null && prospeoAfter?.remaining != null
            ? prospeoBefore.remaining - prospeoAfter.remaining
            : null,
      },
    },
    metrics: {
      cohortSize: results.length,
      skippedEntirely: results.filter((r) => r.skippedEntirely).length,
      pdlMatched,
      pdlIdentityAccepted: pdlAccepted,
      pdlLinkedInVerified: pdlLi,
      prospeoNamedEmailsAccepted: prospeoEmails.length,
      quarantined: quarantined.length,
      movedPartialToReady: movedReady,
      pdlProspeoEmailOverlap: overlap.length,
      surfeInputsImproved: results.filter((r) => {
        if (!r.after?.surfeInputAfter) return false;
        return (
          JSON.stringify(r.baseline.surfeFieldNames) !==
          JSON.stringify(r.after.surfeInputAfter.surfeFieldNames)
        );
      }).length,
    },
    costPerValidIncrementalRecovery: {
      pdlCredits,
      prospeoCredits,
      acceptedProspeoEmails: prospeoEmails.length,
      acceptedPdlIdentity: pdlAccepted,
      note: "Provider-derived — not customer-publishable without license confirmation",
    },
    licensing: {
      persistenceMode: "INTERNAL_EVALUATION_REPORT_ONLY",
      canonicalPublicFieldsWritten: false,
      stagedToCurrent: false,
      pdlEmbeddedProductPermission:
        "Reported in .env.example / client (2026-09) — still verify exact customer-display use before publication",
      prospeo:
        "Standard ToS = internal B2B use; provider-derived emails must NOT auto-enter customer-visible research records",
    },
    recommendations: null, // filled below
    results,
    stop: "STOP after ten-contact evaluation. No full-database backfill. No mobile. No Surfe/FullEnrich/Apollo.",
  };

  report.recommendations = {
    pdlAsOptionalIdentityProvider:
      pdlAccepted > 0
        ? "YES_OPTIONAL — use when LinkedIn/employer unresolved after public research; gate on identity_match_accepted"
        : "INCONCLUSIVE_OR_WEAK — review match quality before adopting",
    prospeoAsSelectiveEmailFallback:
      prospeoEmails.length > 0
        ? "YES_SELECTIVE — email-only after public+PDL miss; keep provider-derived / non-customer until Order permits"
        : "WEAK_THIS_COHORT — keep adapter; do not expand spend yet",
    fullenrichStillNeeded:
      "HOLD — not tested; revisit only if PDL+Prospeo miss rate stays high on larger PARTIAL set",
    surfeSearchPeople:
      "HOLD — zero search credits; Search is org/role discovery not exact-person; Enrich remains on-click",
    customerFacingLawfulUse:
      "Public-verified only by default. PDL embedded permission needs counsel check for hotel disclosure. Prospeo standard ToS does not authorize customer disclosure without Schedule 5/Order.",
  };

  fs.writeFileSync(
    path.join(OUT_DIR, "PILOT_RESULTS.json"),
    JSON.stringify(report, null, 2) + "\n",
  );

  const md = [
    "# GDI PDL + Prospeo Controlled Enrichment Pilot — Founder Report",
    "",
    `Generated: ${report.generatedAt}`,
    DRY_RUN ? "**DRY RUN — no paid calls**" : null,
    "",
    "## Authorization respected",
    "",
    `- PDL ≤ ${PDL_CEILING} successful credits (consumed: **${pdlCredits}**)`,
    `- Prospeo ≤ ${PROSPEO_CEILING} email credits (consumed est: **${prospeoCredits}**; account Δ remaining: **${report.balances.prospeo.deltaRemaining}**)`,
    `- Mobile / FullEnrich / Surfe enrich / Apollo: **not used**`,
    "",
    "## Balances",
    "",
    `- Prospeo before: plan=${prospeoBefore?.plan} remaining=${prospeoBefore?.remaining} used=${prospeoBefore?.used}`,
    `- Prospeo after: remaining=${prospeoAfter?.remaining} used=${prospeoAfter?.used}`,
    `- PDL: no free balance API; successful-match credits counted = ${pdlCredits}`,
    "",
    "## Metrics",
    "",
    `| Measure | Value |`,
    `|---|---:|`,
    `| PARTIAL → READY | ${movedReady} |`,
    `| PDL matches | ${pdlMatched} |`,
    `| PDL identity accepted | ${pdlAccepted} |`,
    `| PDL LinkedIn verified | ${pdlLi} |`,
    `| Prospeo named emails accepted | ${prospeoEmails.length} |`,
    `| Quarantined | ${quarantined.length} |`,
    `| PDL∩Prospeo same email | ${overlap.length} |`,
    `| Surfe input fields improved | ${report.metrics.surfeInputsImproved} |`,
    "",
    "## Recommendations",
    "",
    `- **PDL:** ${report.recommendations.pdlAsOptionalIdentityProvider}`,
    `- **Prospeo:** ${report.recommendations.prospeoAsSelectiveEmailFallback}`,
    `- **FullEnrich:** ${report.recommendations.fullenrichStillNeeded}`,
    `- **Surfe Search:** ${report.recommendations.surfeSearchPeople}`,
    `- **Customer-facing:** ${report.recommendations.customerFacingLawfulUse}`,
    "",
    "## Persistence",
    "",
    "INTERNAL_EVALUATION_REPORT_ONLY — no canonical public-source writes; no STAGED→CURRENT.",
    "",
    "## STOP",
    "",
    report.stop,
    "",
  ]
    .filter((x) => x != null)
    .join("\n");

  fs.writeFileSync(path.join(OUT_DIR, "FOUNDER_PILOT_REPORT.md"), md, "utf8");

  console.log(
    JSON.stringify(
      {
        ok: true,
        dryRun: DRY_RUN,
        outDir: OUT_DIR,
        pdlCredits,
        prospeoCredits,
        prospeoRemainingAfter: prospeoAfter?.remaining ?? null,
        metrics: report.metrics,
        stop: true,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
