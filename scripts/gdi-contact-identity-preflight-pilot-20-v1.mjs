/**
 * GDI Contact Identity Preflight — 20-opportunity pilot (Phase 9)
 *
 * Offline identity readiness using existing verified intelligence.
 * No paid Surfe / Apollo / unbounded research.
 *
 * Usage:
 *   node scripts/gdi-contact-identity-preflight-pilot-20-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  CONTACT_IDENTITY_READINESS,
  LINKEDIN_PROFILE_MATCH,
  runContactIdentityPreflight,
  evaluatePersonalLinkedInUrl,
} from "../lib/group-demand-intelligence/presentation/gdi-contact-identity-preflight-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports", "gdi-contact-identity-preflight-v1");

function loadJson(rel) {
  return JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
}

function indexSecondPass() {
  const sp = loadJson(
    "reports/gdi-google-first-second-pass-v1/SECOND_PASS_RESULTS.json",
  );
  const map = new Map();
  for (const r of sp.results || []) {
    map.set(`${r.opportunityId}::${r.personName}`, r);
  }
  return { map, spendUsd: Number(sp.actualSpend || 0) };
}

function indexCci() {
  const cci = loadJson(
    "reports/gdi-commercial-contact-intelligence-v1/pilot-100/PILOT_RESULTS.json",
  );
  const map = new Map();
  for (const r of cci.results || []) {
    const name = r.enhanced?.name || r.baseline?.name;
    if (r.opportunityId && name) map.set(`${r.opportunityId}::${name}`, r);
  }
  return map;
}

function indexApify() {
  const ap = loadJson(
    "reports/gdi-apify-google-contact-pilot-v1/PILOT_RESULTS.json",
  );
  const map = new Map();
  for (const r of ap.contacts || []) {
    map.set(`${r.opportunityId}::${r.personName}`, r);
  }
  return {
    map,
    spendUsd: Number(ap.usageDeltaUsd || ap.spendTrackedFromRunsUsd || 0),
  };
}

/**
 * Curated 20-opportunity pilot covering founder-required categories.
 * Each row references existing GDI research evidence — no new paid discovery.
 */
const PILOT_CASES = [
  {
    opportunityId: "rx_d_2400ebe4c2bb",
    hotelId: "recH9h3McgMNFQEla",
    personName: "Abbas Zavar",
    category: "LINKEDIN_VERIFIED",
    title: "Researcher / FHLIP",
    employerName:
      "University of Toronto — Institute of Health Policy, Management and Evaluation",
    demandOrganizationName: "FHLIP / University of Toronto",
    assumeDemandOrgIsEmployer: true,
  },
  {
    opportunityId: "hist_de85f642e969",
    hotelId: "recCAGqkERSXgiku4",
    personName: "Abir Chermiti",
    category: "LINKEDIN_VERIFIED",
    title: null,
    employerName: "IEEE",
    demandOrganizationName: "IEEE World Technology Summits",
    assumeDemandOrgIsEmployer: false,
  },
  {
    opportunityId: "wy_pd_fc13e2e08dd5",
    hotelId: "recXh4NgF8sDbi9cQ",
    personName: "Daryl Simon",
    category: "LINKEDIN_VERIFIED",
    employerName: "The Tie",
    demandOrganizationName: "The Tie",
    assumeDemandOrgIsEmployer: true,
  },
  {
    opportunityId: "gdi_opp_divcult_spring_webinar_7",
    hotelId: "rec2PVBDavppGpenm",
    personName: "Michel Lafleur",
    category: "LINKEDIN_VERIFIED_SLUG_VARIANT",
    employerName: "Faculty of Sociology, University of A Coruña",
    demandOrganizationName: "DivCult",
    assumeDemandOrgIsEmployer: false,
  },
  {
    opportunityId: "paid3_0a6b19883953",
    hotelId: "recCAGqkERSXgiku4",
    personName: "Alejandra Vázquez",
    category: "LINKEDIN_MISSING_STRONG_EMAIL",
    title: "Sales Manager",
    employerName: "IGECO (Italian German Exhibition Company)",
    demandOrganizationName: "IGECO",
    assumeDemandOrgIsEmployer: true,
  },
  {
    opportunityId: "gdi_opp_insurance_risk_capital_bermuda_2026_0",
    hotelId: "recIwaP1etgx2g9nA",
    personName: "Adam Jordan",
    category: "LINKEDIN_MISSING_STRONG_EMAIL",
    employerName: "InsuranceERM",
    demandOrganizationName: "InsuranceERM",
    assumeDemandOrgIsEmployer: true,
  },
  {
    opportunityId: "gdi_opp_ndss_advocacy_2027",
    hotelId: "recLuxvwwxID7U2B8",
    personName: "Amy Drow",
    category: "LINKEDIN_MISSING_ASSOCIATION",
    employerName: "National Down Syndrome Society (NDSS)",
    demandOrganizationName: "National Down Syndrome Society (NDSS)",
    assumeDemandOrgIsEmployer: true,
  },
  {
    opportunityId: "gdi_opp_autoamericas_2027",
    hotelId: "recUOyzOXn2Zdp98I",
    personName: "Andrés Caballero",
    category: "LINKEDIN_MISSING",
    employerName: "AutoAméricas / Latinpress",
    demandOrganizationName: "AutoAméricas",
    assumeDemandOrgIsEmployer: true,
  },
  {
    opportunityId: "hist_9d12871f512d",
    hotelId: "recCAGqkERSXgiku4",
    personName: "Abraham Yasir Maciel Montoya",
    category: "LINKEDIN_MISSING_GOV",
    employerName: "Gobierno de Guadalajara",
    demandOrganizationName: "Gobierno de Guadalajara",
    assumeDemandOrgIsEmployer: true,
  },
  {
    opportunityId: "wy_pd_5cc46249ac1f",
    hotelId: "recXh4NgF8sDbi9cQ",
    personName: "Bacilio Sánchez",
    category: "EMPLOYER_ORGANIZER_MISMATCH",
    title: "Advisor",
    employerName: "SPA Consultores",
    demandOrganizationName: "Asociación Interamericana de Contabilidad",
    assumeDemandOrgIsEmployer: false,
  },
  {
    opportunityId: "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
    hotelId: "recLuxvwwxID7U2B8",
    personName: "Laura Keeley",
    category: "WRONG_CHANNEL_GENERIC_MAILBOX",
    title: null,
    employerName: "Parenteral Drug Association (PDA)",
    demandOrganizationName: "Parenteral Drug Association (PDA)",
    assumeDemandOrgIsEmployer: true,
    // hall@pda.org was quarantined as generic mailbox — do not treat as named email
    forceOmitEmail: true,
  },
  {
    opportunityId: "gdi_opp_15th_annual_boutique_hotel_investment_conference_11",
    hotelId: "gdi_hotel_renaissance_times_square",
    personName: "Ariela Kiradjian",
    category: "LINKEDIN_MISSING_ENTRY_POINT",
    title: "Partner & COO",
    employerName: "Boutique & Luxury Lodging Association",
    demandOrganizationName: "Boutique & Luxury Lodging Association",
    assumeDemandOrgIsEmployer: true,
  },
  {
    opportunityId: "rx_d_2d58b95c5905",
    hotelId: "recrPQcZg7SFARRb2",
    personName: "Abramo A. Chabib",
    category: "UNRESOLVED_QUARANTINED",
    employerName: null,
    demandOrganizationName: null,
    assumeDemandOrgIsEmployer: false,
  },
  {
    opportunityId: "gdi_opp_iafl_usa_canadian_chapter_meeting_10",
    hotelId: "recLuxvwwxID7U2B8",
    personName: "IAFL",
    category: "UNRESOLVED_ORG_NOT_PERSON",
    forceUnresolvedName: true,
    employerName: "IAFL",
    demandOrganizationName: "IAFL",
    assumeDemandOrgIsEmployer: true,
  },
  {
    opportunityId: "gdi_opp_wrome_pilot_v0_red_bull_sailgp_rome_2027",
    hotelId: "rece0or38cxo3Fymb",
    personName: "Francesco Francavilla",
    category: "LINKEDIN_MISSING_ENTRY_POINT",
    employerName: null,
    demandOrganizationName: "Red Bull SailGP Rome",
    assumeDemandOrgIsEmployer: false,
  },
  {
    opportunityId: "gdi_opp_iaadfs_summit_of_the_americas_2027_6",
    hotelId: "recgMYovrrZDJMqzX",
    personName: "Olivia Jallits",
    category: "LINKEDIN_MISSING",
    employerName: null,
    demandOrganizationName: "IAADFS",
    assumeDemandOrgIsEmployer: true,
  },
  {
    opportunityId: "disc_1490e4966ee8",
    hotelId: "recCAGqkERSXgiku4",
    personName: "David Mendoza Martínez",
    category: "MULTIPLE_EMPLOYER_LABELS",
    employerName: "Secretaría de Desarrollo Económico de Jalisco",
    demandOrganizationName:
      "Secretaría de Desarrollo Económico de Jalisco; Secretaría de Turismo de Jalisco",
    assumeDemandOrgIsEmployer: false,
  },
  {
    opportunityId: "gdi_opp_cda_annual_meeting_2026_8",
    hotelId: "recLuxvwwxID7U2B8",
    personName: "CDA Contact",
    category: "UNVERIFIED_COMPANY_DOMAIN",
    forceUnresolvedName: false,
    employerName: "Canadian Dental Association",
    demandOrganizationName: "Canadian Dental Association",
    assumeDemandOrgIsEmployer: true,
    // No email domain; force domain unresolved unless second-pass has one
    preferNoEmailDomain: true,
  },
  {
    opportunityId: "wy_pd_c6455b6578ee",
    hotelId: "recXh4NgF8sDbi9cQ",
    personName: "Dra. Carmen Encarnación",
    category: "NAME_VARIANT_PARTIAL",
    employerName: null,
    demandOrganizationName: null,
    assumeDemandOrgIsEmployer: true,
  },
  {
    opportunityId: "gdi_pe_879047c3211d2e82",
    hotelId: "recnPB3Q02NO8Ma1o",
    personName: "CityCruises",
    category: "ORG_LABEL_NOT_PERSON",
    forceUnresolvedName: true,
    employerName: "City Cruises San Diego",
    demandOrganizationName: "City Cruises San Diego",
    assumeDemandOrgIsEmployer: true,
  },
];

// Synthetic wrong-LinkedIn rejection probe (does not invent READY)
const WRONG_LINKEDIN_PROBE = {
  opportunityId: "probe_wrong_linkedin_rejection",
  hotelId: "probe",
  personName: "Laura Keeley",
  category: "WRONG_LINKEDIN_PREVIOUSLY_REJECTED",
  employerName: "Parenteral Drug Association (PDA)",
  demandOrganizationName: "Parenteral Drug Association (PDA)",
  linkedinUrl: "https://www.linkedin.com/company/parenteral-drug-association",
  linkedinEvidenceText: "PDA company page",
  assumeDemandOrgIsEmployer: true,
};

function enrichFromStores(caseRow, spMap, cciMap, apifyMap) {
  const key = `${caseRow.opportunityId}::${caseRow.personName}`;
  const sp = spMap.get(key) || null;
  const cci = cciMap.get(key) || null;
  const ap = apifyMap.get(key) || null;

  let email =
    caseRow.forceOmitEmail === true
      ? null
      : sp?.email || cci?.enhanced?.email || null;
  if (caseRow.preferNoEmailDomain) email = null;

  const linkedinUrl =
    caseRow.linkedinUrl ||
    sp?.linkedInUrl ||
    ap?.verified?.linkedinUrl ||
    ap?.mergedCandidates?.linkedinUrl ||
    null;

  const linkedInStatus = sp?.linkedInStatus || null;
  const evidence =
    caseRow.linkedinEvidenceText ||
    [sp?.organization, sp?.sourceUrl, caseRow.employerName, caseRow.personName]
      .filter(Boolean)
      .join(" ");

  // Prefer email domain; else hostname from public source URL when employer known.
  let employerDomain = caseRow.employerDomain || null;
  if (!employerDomain && email && email.includes("@")) {
    const host = email.split("@")[1];
    if (host && !/gmail|yahoo|hotmail|outlook|icloud/i.test(host)) {
      employerDomain = host;
    }
  }
  if (!employerDomain && sp?.sourceUrl) {
    try {
      const host = new URL(sp.sourceUrl).hostname.replace(/^www\./, "");
      const org = String(
        caseRow.employerName || sp?.organization || "",
      ).toLowerCase();
      const orgTokens = org
        .split(/[^a-z0-9]+/i)
        .filter((t) => t.length > 3 && !/^(the|and|association|university)$/i.test(t))
        .slice(0, 4);
      const hostMatchesOrg =
        host &&
        orgTokens.some((t) => host.includes(t.slice(0, 6).toLowerCase()));
      // Only accept source-URL host when it corroborates the employer name —
      // personal sites (e.g. personname.com) must not become employer domains.
      if (
        host &&
        hostMatchesOrg &&
        !/linkedin\.com|facebook\.com|twitter\.com/i.test(host)
      ) {
        employerDomain = host;
      }
    } catch {
      /* ignore */
    }
  }

  const employerName =
    caseRow.employerName ||
    sp?.organization ||
    cci?.enhanced?.organization ||
    ap?.organization ||
    null;

  const title =
    caseRow.title ||
    sp?.title ||
    cci?.enhanced?.title ||
    ap?.title ||
    null;

  let personName = caseRow.personName;
  if (caseRow.forceUnresolvedName) {
    personName = caseRow.personName; // org-like labels stay; readiness will flag
  }

  return {
    ...caseRow,
    email,
    linkedinUrl,
    linkedInStatusPrior: linkedInStatus,
    linkedinEvidenceText: evidence,
    employerName,
    employerDomain,
    title,
    personName,
    sourceUrl: sp?.sourceUrl || null,
    secondPassStatus: sp?.status || null,
    apifyVerifiedLinkedIn: ap?.verified?.linkedinUrl || null,
  };
}

function runCase(row) {
  const contactName = row.forceUnresolvedName
    ? row.personName // single-token or org label → UNRESOLVED name
    : row.personName;

  const result = runContactIdentityPreflight({
    opportunity: {
      id: row.opportunityId,
      hotelId: row.hotelId,
      organizationName: row.demandOrganizationName,
      organizationDomain: null,
    },
    contact: {
      name: contactName,
      title: row.title,
      employerName: row.employerName,
      employerDomain: row.employerDomain,
      email: row.email,
      linkedinUrl: row.linkedinUrl,
      linkedinEvidenceText: row.linkedinEvidenceText,
    },
    employerDomain: row.employerDomain,
    // Only seed domain heuristics from employer-corroborated URLs.
    seedUrls: row.employerDomain && row.sourceUrl ? [row.sourceUrl] : [],
    assumeDemandOrgIsEmployer: row.assumeDemandOrgIsEmployer === true,
    commercialClassification: row.category,
    sourceEvidence: row.sourceUrl,
    verifiedAt: new Date().toISOString(),
  });

  // Extra reject probe for company LinkedIn
  let incorrectRejected = null;
  if (row.linkedinUrl) {
    const evalLi = evaluatePersonalLinkedInUrl({
      url: row.linkedinUrl,
      personName: row.personName,
      employerName: row.employerName,
      title: row.title,
      evidenceText: row.linkedinEvidenceText,
    });
    if (evalLi.rejected || evalLi.rejectReason) {
      incorrectRejected = {
        url: row.linkedinUrl,
        reason: evalLi.rejectReason || evalLi.status,
      };
    }
  }

  return {
    opportunityId: row.opportunityId,
    hotelId: row.hotelId,
    category: row.category,
    personName: row.personName,
    identity: result.identity,
    surfeInput: {
      sufficient: result.surfeInput.sufficient,
      surfeCallExecuted: result.surfeInput.surfeCallExecuted,
      paidCallAuthorized: result.surfeInput.paidCallAuthorized,
      surfeFieldNames: result.surfeInput.surfeFieldNames,
      fieldsProvided: result.surfeInput.fieldsProvided,
      omittedUnverifiedLinkedIn: result.surfeInput.omittedUnverifiedLinkedIn,
      message: result.surfeInput.message,
    },
    researchContinuation: result.researchContinuation,
    incorrectRejected,
    priorLinkedInStatus: row.linkedInStatusPrior || null,
  };
}

function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const { map: spMap, spendUsd: secondPassSpend } = indexSecondPass();
  const cciMap = indexCci();
  const { map: apifyMap, spendUsd: apifySpend } = indexApify();

  const enriched = PILOT_CASES.map((c) =>
    enrichFromStores(c, spMap, cciMap, apifyMap),
  );
  enriched.push(
    enrichFromStores(WRONG_LINKEDIN_PROBE, spMap, cciMap, apifyMap),
  );

  const results = enriched.map(runCase);

  // Pilot metrics exclude the synthetic probe from opportunity coverage totals
  // but include it in rejection counts.
  const oppResults = results.filter(
    (r) => r.opportunityId !== "probe_wrong_linkedin_rejection",
  );
  const probe = results.find(
    (r) => r.opportunityId === "probe_wrong_linkedin_rejection",
  );

  const readinessCounts = {
    READY: 0,
    PARTIAL: 0,
    UNRESOLVED: 0,
  };
  for (const r of oppResults) {
    readinessCounts[r.identity.readiness] =
      (readinessCounts[r.identity.readiness] || 0) + 1;
  }

  const employerVerified = oppResults.filter((r) =>
    Boolean(r.identity.employerName),
  ).length;
  const domainVerified = oppResults.filter(
    (r) => r.identity.employerDomainVerified === true,
  ).length;
  const domainPresent = oppResults.filter((r) =>
    Boolean(r.identity.employerDomain),
  ).length;
  const linkedInVerified = oppResults.filter(
    (r) => r.identity.linkedinTrustedForSurfe === true,
  ).length;
  const linkedInCandidate = oppResults.filter(
    (r) =>
      r.identity.linkedinStatus ===
      LINKEDIN_PROFILE_MATCH.CANDIDATE_LINKEDIN_PROFILE,
  ).length;
  const incorrectRejected = [
    ...oppResults.filter((r) => r.incorrectRejected),
    ...(probe?.incorrectRejected ? [probe] : []),
  ].map((r) => ({
    opportunityId: r.opportunityId,
    personName: r.personName,
    ...r.incorrectRejected,
  }));

  const surfePrepared = oppResults.filter((r) => r.surfeInput.sufficient).length;
  const organizerMismatch = oppResults.filter(
    (r) => r.identity.demandOrganizationDistinct === true,
  );

  const commercialRelevanceImprovements = organizerMismatch.map((r) => ({
    opportunityId: r.opportunityId,
    personName: r.personName,
    employerName: r.identity.employerName,
    demandOrganizationName: r.identity.demandOrganizationName,
    note: "Employer kept distinct from opportunity demand organization",
  }));

  // Airtable: dry-run certification only — no writes in this pilot
  const airtable = {
    mode: "DRY_RUN_NO_WRITES",
    reason:
      "Identity preflight pilot uses existing local research evidence; Airtable apply deferred pending founder approval of READY/PARTIAL identities.",
    plannedFields: [
      "firstName",
      "lastName",
      "fullName",
      "employerName",
      "employerDomain",
      "title",
      "personalLinkedInUrl",
      "linkedinStatus",
      "identityReadiness",
      "verificationTimestamp",
      "opportunityRelationship",
      "sourceEvidence",
    ],
    surfeDerivedPersistProhibited: true,
    readBack: {
      attempted: false,
      success: null,
      note: "No Airtable mutations in this phase; read-back certification required on first apply batch.",
    },
    localCanonicalStore: {
      pathPattern:
        "data/group-demand-intelligence/hotels/<hotelId>/canonical-contacts.json",
      pilotWrites: false,
    },
  };

  const surfeExamples = oppResults
    .filter((r) =>
      [
        CONTACT_IDENTITY_READINESS.READY,
        CONTACT_IDENTITY_READINESS.PARTIAL,
      ].includes(r.identity.readiness),
    )
    .slice(0, 6)
    .map((r) => ({
      opportunityId: r.opportunityId,
      personName: r.personName,
      readiness: r.identity.readiness,
      surfeFieldNames: r.surfeInput.surfeFieldNames,
      fieldsProvided: r.surfeInput.fieldsProvided,
      surfeCallExecuted: false,
    }));

  const report = {
    generatedAt: new Date().toISOString(),
    version: "gdi-contact-identity-preflight-v1",
    phase: "PILOT_20",
    pr484: {
      number: 484,
      state: "OPEN",
      url: "https://github.com/joandejarden-create/my-operators-backend/pull/484",
      note: "Contact identity preflight extends GDI contact enrichment on the same open branch; Integration Controller owns merge.",
    },
    authorization: {
      paidSurfe: false,
      paidApollo: false,
      unboundedResearch: false,
      airtableApply: false,
      researchSpendThisPilotUsd: 0,
    },
    researchSpend: {
      thisPilotUsd: 0,
      reusedPriorSecondPassUsd: secondPassSpend,
      reusedPriorApifyUsd: Number(apifySpend.toFixed?.(4) || apifySpend),
      note: "Pilot reused existing Google-first / Apify evidence; no new paid provider calls.",
    },
    pilotOpportunityIds: oppResults.map((r) => r.opportunityId),
    categoriesCovered: [...new Set(oppResults.map((r) => r.category))],
    metrics: {
      opportunityCount: oppResults.length,
      readiness: readinessCounts,
      employerNameCoverage: employerVerified,
      employerDomainPresent: domainPresent,
      employerDomainVerified: domainVerified,
      linkedInVerified,
      linkedInCandidate,
      incorrectIdentitiesRejected: incorrectRejected.length,
      surfeInputPrepared: surfePrepared,
      commercialEmployerOrganizerSplits: organizerMismatch.length,
    },
    incorrectIdentitiesRejected: incorrectRejected,
    commercialRelevanceImprovements,
    airtable,
    surfeRequestPreparationExamples: surfeExamples,
    results: results,
    phasedRolloutProposal: {
      phaseA:
        "Expand identity preflight offline across all eligible GDI opportunities with named contacts (no paid search).",
      phaseB:
        "Bounded Gemini LinkedIn discovery only for PARTIAL/UNRESOLVED with verified employer — batch ≤20, budget-capped.",
      phaseC:
        "Airtable idempotent persist + read-back for READY and high-confidence PARTIAL identities.",
      phaseD:
        "Optional controlled Surfe A/B (name+company vs +domain vs +verified LinkedIn) under explicit founder auth.",
      doNot: [
        "Unrestricted paid research",
        "Invent LinkedIn to force READY",
        "Persist Surfe-derived fields into canonical public-contact",
        "Block publication solely for missing LinkedIn",
      ],
    },
    stop: "STOP for founder review before additional paid research, full database backfill, or paid Surfe enrichment.",
  };

  fs.writeFileSync(
    path.join(OUT_DIR, "PILOT_20_RESULTS.json"),
    JSON.stringify(report, null, 2) + "\n",
    "utf8",
  );

  const md = [
    "# GDI Contact Identity Preflight — Founder Acceptance Report",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "## Verdict",
    "",
    `Pilot of **${oppResults.length}** opportunities complete offline. **No paid Surfe / Apollo / new research** in this phase.`,
    "",
    `| READY | PARTIAL | UNRESOLVED |`,
    `| ---: | ---: | ---: |`,
    `| ${readinessCounts.READY} | ${readinessCounts.PARTIAL} | ${readinessCounts.UNRESOLVED} |`,
    "",
    `- Employer name coverage: **${employerVerified}/${oppResults.length}**`,
    `- Employer domain present: **${domainPresent}/${oppResults.length}** (verified: **${domainVerified}**)`,
    `- Verified personal LinkedIn: **${linkedInVerified}/${oppResults.length}**`,
    `- Incorrect identities rejected: **${incorrectRejected.length}**`,
    `- Surfe inputs prepared (no calls): **${surfePrepared}/${oppResults.length}**`,
    `- Research spend this pilot: **$0** (reused prior evidence)`,
    "",
    "## 1. Architecture audit (summary)",
    "",
    "- Canonical GDI contacts: hotel-scoped `canonical-contacts.json` + research inventory slim payload",
    "- CCI / Google-first / Exa / LinkedIn classification: reused (`classifyLinkedInProfileMatch`)",
    "- Org domain: email domain + `resolveOfficialDomains` heuristics",
    "- Surfe: `lib/surfe/client.js` fields `firstName/lastName/companyName/companyDomain/linkedInUrl`",
    "- New permanent component: `gdi-contact-identity-preflight-v1.js` + Surfe input adapter",
    "- Does **not** create a competing discovery engine",
    "",
    "## 2. Pilot opportunity IDs",
    "",
    ...oppResults.map(
      (r) =>
        `- \`${r.opportunityId}\` — ${r.personName} — **${r.identity.readiness}** (${r.category})`,
    ),
    "",
    "## 3–7. Coverage & relevance",
    "",
    "See `PILOT_20_RESULTS.json` metrics, incorrect rejections, and employer/organizer splits.",
    "",
    "## 8. Airtable",
    "",
    "DRY_RUN_NO_WRITES. Persist + read-back certification deferred until founder approves apply for READY/PARTIAL identities. Surfe-derived fields remain prohibited in canonical public-contact fields.",
    "",
    "## 9. Surfe request preparation examples (no paid calls)",
    "",
    "```json",
    JSON.stringify(surfeExamples, null, 2),
    "```",
    "",
    "## 10. Research spend",
    "",
    `- This pilot: **$0**`,
    `- Reused second-pass spend (prior): $${secondPassSpend}`,
    `- Reused Apify pilot delta (prior): $${Number(apifySpend).toFixed(4)}`,
    "",
    "## 11. Integration / PR status",
    "",
    `- PR #484: **OPEN** — ${report.pr484.url}`,
    `- Work landed on \`feat/gdi-google-style-contact-search-before-surfe-v1\` (same contact-enrichment stream)`,
    `- GDI must **not** merge — Integration Controller owns merge`,
    "",
    "## 12. Recommended phased rollout",
    "",
    ...Object.entries(report.phasedRolloutProposal)
      .filter(([k]) => k.startsWith("phase"))
      .map(([k, v]) => `- **${k}:** ${v}`),
    "",
    "## STOP",
    "",
    report.stop,
    "",
  ].join("\n");

  fs.writeFileSync(path.join(OUT_DIR, "FOUNDER_ACCEPTANCE_REPORT.md"), md, "utf8");

  console.log(
    JSON.stringify(
      {
        ok: true,
        outDir: OUT_DIR,
        readiness: readinessCounts,
        linkedInVerified,
        domainVerified,
        incorrectRejected: incorrectRejected.length,
        spendUsd: 0,
        pr484: "OPEN",
        stop: true,
      },
      null,
      2,
    ),
  );
}

main();
