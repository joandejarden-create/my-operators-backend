/**
 * Contact Identity Preflight Phase 2 — LinkedIn recovery + domain + Surfe Search readiness
 *
 * - Reuses Phase 1 pilot baseline (20 opportunities)
 * - Google LinkedIn-focused discovery for PARTIAL (bounded spend)
 * - Exa only when Google fails and budget remains
 * - Surfe Search People: readiness only (no paid search / no enrich)
 * - Airtable: --apply for independently verified public identity updates
 *
 * Usage:
 *   node scripts/gdi-contact-identity-preflight-phase2-linkedin-v1.mjs
 *   node scripts/gdi-contact-identity-preflight-phase2-linkedin-v1.mjs --apply-airtable
 *   node scripts/gdi-contact-identity-preflight-phase2-linkedin-v1.mjs --offline-only
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  CONTACT_IDENTITY_READINESS,
  LINKEDIN_PROFILE_MATCH,
  runContactIdentityPreflight,
  evaluatePersonalLinkedInUrl,
  buildSurfeEnrichmentIdentityInput,
} from "../lib/group-demand-intelligence/presentation/gdi-contact-identity-preflight-v1.js";
import {
  runGoogleGroundedContactDiscovery,
  inspectGoogleGroundedContactCredentials,
} from "../lib/group-demand-intelligence/presentation/gdi-google-grounded-contact-discovery-v1.js";
import { assessSurfeSearchPeopleReadiness } from "../lib/group-demand-intelligence/presentation/gdi-surfe-search-people-readiness-v1.js";
import {
  upsertResearchInventoryRecord,
  findResearchInventoryByCompositeKey,
  readBackResearchInventory,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { buildHotelResearchCompositeKey } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports",
  "gdi-contact-identity-preflight-v1",
  "phase2-linkedin-recovery",
);
const BASELINE_PATH = path.join(
  ROOT,
  "reports",
  "gdi-contact-identity-preflight-v1",
  "PILOT_20_RESULTS.json",
);

const SPEND_CEILING_USD = Number(
  process.env.GDI_IDENTITY_PHASE2_SPEND_CEILING_USD || 1.5,
);
const GOOGLE_EST_USD = 0.03;
const APPLY = process.argv.includes("--apply-airtable");
const OFFLINE_ONLY = process.argv.includes("--offline-only");
const REFRESH_GOOGLE = process.argv.includes("--refresh-google");
const PRIOR_PHASE2_PATH = path.join(OUT_DIR, "PHASE2_RESULTS.json");

/** Curated official domains — only when independently known / high confidence */
const KNOWN_EMPLOYER_DOMAINS = Object.freeze({
  "the tie": "thetie.io",
  "boutique & luxury lodging association": "bllaevents.com",
  "national down syndrome society (ndss)": "ndss.org",
  "national down syndrome society": "ndss.org",
  "insuranceerm": "insuranceerm.com",
  "igeco (italian german exhibition company)": "igeco.mx",
  igeco: "igeco.mx",
  "parenteral drug association (pda)": "pda.org",
  "canadian dental association": "cda-adc.ca",
  "international association of airport and duty free stores (iaadfs)":
    "iaadfs.org",
  iaadfs: "iaadfs.org",
  "spa consultores": "spaconsultores.com.do",
  "easyfairs group": "easyfairs.com",
  "insurance bureau of canada": "ibc.ca",
  "alexandria soccer association": "alexandriasoccer.com",
  "university of toronto": "utoronto.ca",
  ieee: "ieee.org",
});

function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function stripAccents(s) {
  return text(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function lookupKnownDomain(employerName) {
  const key = stripAccents(employerName).toLowerCase();
  if (KNOWN_EMPLOYER_DOMAINS[key]) return KNOWN_EMPLOYER_DOMAINS[key];
  for (const [k, dom] of Object.entries(KNOWN_EMPLOYER_DOMAINS)) {
    if (key.includes(k) || k.includes(key.slice(0, 18))) return dom;
  }
  return null;
}

function parseEmployerDomainFromNotes(rawText = "") {
  const m = String(rawText).match(
    /EMPLOYER_DOMAIN:\s*([a-z0-9.-]+\.[a-z]{2,})/i,
  );
  if (!m) return null;
  const d = m[1].toLowerCase();
  if (/linkedin\.com|facebook\.com|none/i.test(d)) return null;
  return d;
}

function parseEmployerFromNotes(rawText = "") {
  const m = String(rawText).match(/EMPLOYER:\s*(.+)/i);
  if (!m) return null;
  const v = text(m[1].split("\n")[0]);
  if (!v || /^none$/i.test(v)) return null;
  return v;
}

function classifyUnresolvedCase(row) {
  const name = text(row.personName || row.identity?.fullName);
  const gaps = row.identity?.readinessGaps || [];
  const orgLike =
    /^(IAFL|CityCruises|CDA Contact)$/i.test(name) ||
    /\bContact\b/i.test(name) ||
    gaps.includes("NAME_INCOMPLETE") ||
    (name && !/\s/.test(name) && name.length < 24);
  if (orgLike) {
    return {
      kind: "MISSING_NAMED_PERSON",
      note: "Organization or role label used where a natural-person name is required. Research continuation = find a named commercial contact; do not invent a person.",
    };
  }
  return {
    kind: "NAMED_PERSON_IDENTITY_UNCERTAIN",
    note: "A person name exists but employer/LinkedIn association is insufficient.",
  };
}

async function discoverLinkedInViaGoogle(contact, spend) {
  if (OFFLINE_ONLY) {
    return { skipped: true, reason: "OFFLINE_ONLY", costUsd: 0 };
  }
  if (spend.billedUsd + GOOGLE_EST_USD > SPEND_CEILING_USD) {
    return { skipped: true, reason: "SPEND_CEILING", costUsd: 0 };
  }
  const creds = inspectGoogleGroundedContactCredentials();
  if (!creds.geminiConfigured) {
    return { skipped: true, reason: "GEMINI_NOT_CONFIGURED", costUsd: 0 };
  }

  const result = await runGoogleGroundedContactDiscovery({
    person: {
      name: contact.fullName || contact.personName,
      title: contact.title,
    },
    organization: {
      name: contact.employerName,
      domain: contact.employerDomain,
    },
    authorizePaidSearch: true,
    linkedInIdentityFocus: true,
    allowFollowUp: false,
    resolveRedirects: true,
  });

  const actual = Number(result.costUsd || 0);
  const billed = actual > 0 ? actual : result.skipped ? 0 : GOOGLE_EST_USD;
  spend.billedUsd += billed;
  spend.googleCalls += result.skipped ? 0 : 1;
  spend.googleUsd += billed;

  return { ...result, billedUsd: billed };
}

function mergeIdentity(baselineRow, patches = {}) {
  const prev = baselineRow.identity || {};
  const contact = {
    name: patches.fullName || prev.fullName || baselineRow.personName,
    title: patches.title || prev.title,
    employerName: patches.employerName || prev.employerName,
    employerDomain:
      patches.employerDomain ||
      (patches.employerDomainVerified ? patches.employerDomain : null) ||
      prev.employerDomain,
    email: patches.email || null,
    linkedinUrl:
      patches.linkedinUrl ||
      prev.linkedinUrl ||
      prev.linkedinCandidateUrl ||
      null,
    linkedinEvidenceText:
      patches.linkedinEvidenceText ||
      [patches.employerName || prev.employerName, patches.sourceUrls]
        .flat()
        .filter(Boolean)
        .join(" "),
  };

  return runContactIdentityPreflight({
    opportunity: {
      id: baselineRow.opportunityId,
      hotelId: baselineRow.hotelId,
      organizationName: prev.demandOrganizationName,
    },
    contact,
    employerDomain: contact.employerDomain,
    assumeDemandOrgIsEmployer: false,
    commercialClassification: baselineRow.category,
    sourceEvidence: patches.sourceUrl || prev.sourceEvidence,
    linkedinEvidenceText: contact.linkedinEvidenceText,
    verifiedAt: new Date().toISOString(),
  });
}

async function persistIdentityIfAuthorized(after, before) {
  const identity = after.identity;
  if (!identity?.fullName || !identity?.employerName) {
    return { skipped: true, reason: "INSUFFICIENT_IDENTITY" };
  }
  // Only persist independently verified public LinkedIn or domain/employer improvements
  const liOk = identity.linkedinTrustedForSurfe === true;
  const domainOk = identity.employerDomainVerified === true;
  const improved =
    before.identity?.readiness !== identity.readiness ||
    before.identity?.linkedinUrl !== identity.linkedinUrl ||
    before.identity?.employerDomain !== identity.employerDomain ||
    before.identity?.employerName !== identity.employerName;
  if (!improved) return { skipped: true, reason: "NO_IMPROVEMENT" };
  if (
    identity.readiness === CONTACT_IDENTITY_READINESS.UNRESOLVED &&
    !liOk
  ) {
    return { skipped: true, reason: "STILL_UNRESOLVED" };
  }

  const hotelId = after.hotelId || before.hotelId;
  const opportunityId = after.opportunityId;
  const compositeKey = buildHotelResearchCompositeKey(hotelId, opportunityId);
  const existing = await findResearchInventoryByCompositeKey(compositeKey);
  if (!existing?.id) {
    return {
      skipped: true,
      reason: "NO_EXISTING_RESEARCH_INVENTORY_ROW",
      note: "Will not create new rows in Phase 2 — only update existing inventory.",
    };
  }

  const opportunity = {
    id: opportunityId,
    opportunityId,
    hotelId,
    organizationName: identity.employerName,
    organizationDomain: identity.employerDomain,
    primaryContact: {
      name: identity.fullName,
      firstName: identity.firstName,
      lastName: identity.lastName,
      title: identity.title,
      organization: identity.employerName,
      employerName: identity.employerName,
      employerDomain: identity.employerDomain,
      linkedinUrl: liOk ? identity.linkedinUrl : null,
      linkedinTrustedForSurfe: liOk,
      identityReadiness: identity.readiness,
      identityVerifiedAt: identity.verifiedAt,
      sourceUrl: identity.sourceEvidence || null,
    },
    contactIdentityPreflight: {
      version: "gdi-contact-identity-preflight-v1",
      readiness: identity.readiness,
      gaps: identity.readinessGaps,
      linkedinStatus: identity.linkedinStatus,
      surfeDerived: false,
      phase: "phase2_linkedin_recovery",
    },
  };

  if (!APPLY) {
    return {
      dryRun: true,
      wouldUpdateRecordId: existing.id,
      linkedIn: liOk ? identity.linkedinUrl : null,
      employerDomain: identity.employerDomain,
      readiness: identity.readiness,
    };
  }

  const write = await upsertResearchInventoryRecord({
    opportunity,
    hotel: { hotelId },
    dryRun: false,
  });
  if (!write?.ok) {
    return { ok: false, write };
  }
  const rb = await readBackResearchInventory(write.airtableRecordId);
  const payload = (() => {
    try {
      return JSON.parse(rb?.fields?.researchPayloadJson || "{}");
    } catch {
      return {};
    }
  })();
  const pc = payload.primaryContact || {};
  return {
    ok: true,
    recordId: write.airtableRecordId,
    readBack: {
      name: pc.name || null,
      linkedinUrl: pc.linkedinUrl || null,
      employerDomain: pc.employerDomain || payload.organizationDomain || null,
      identityReadiness:
        payload.contactIdentityPreflight?.readiness || pc.identityReadiness || null,
      surfeDerived: payload.contactIdentityPreflight?.surfeDerived === true,
    },
    readBackOk:
      text(pc.name) === text(identity.fullName) &&
      (liOk ? text(pc.linkedinUrl).replace(/\/$/, "") === text(identity.linkedinUrl).replace(/\/$/, "") : true),
  };
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  if (!fs.existsSync(BASELINE_PATH)) {
    throw new Error(`Missing baseline: ${BASELINE_PATH}`);
  }
  const baseline = loadJson(BASELINE_PATH);
  const beforeRows = (baseline.results || []).filter(
    (r) => r.opportunityId !== "probe_wrong_linkedin_rejection",
  );
  const priorPhase2 =
    !REFRESH_GOOGLE && fs.existsSync(PRIOR_PHASE2_PATH)
      ? loadJson(PRIOR_PHASE2_PATH)
      : null;
  const priorByOpp = new Map(
    (priorPhase2?.results || []).map((r) => [r.opportunityId, r]),
  );

  const surfeSearch = await assessSurfeSearchPeopleReadiness();
  fs.writeFileSync(
    path.join(OUT_DIR, "SURFE_SEARCH_PEOPLE_READINESS.json"),
    JSON.stringify(surfeSearch, null, 2) + "\n",
  );

  const spend = { billedUsd: 0, googleCalls: 0, googleUsd: 0, exaUsd: 0 };
  const incorrectRejected = [];
  const afterRows = [];
  const persistOutcomes = [];

  for (const before of beforeRows) {
    const identity = before.identity || {};
    let patches = {};
    let googleResult = null;
    let unresolvedClass = null;

    // Domain improvement from curated map (offline)
    if (!identity.employerDomainVerified && identity.employerName) {
      const known = lookupKnownDomain(identity.employerName);
      if (known) {
        patches.employerDomain = known;
        patches.employerDomainVerified = true;
      }
    }

    // Org-label / placeholder "person" names — treat as missing named person.
    if (
      /^(IAFL|CityCruises|CDA Contact)$/i.test(text(before.personName)) ||
      /\bContact\b/i.test(text(before.personName))
    ) {
      unresolvedClass = classifyUnresolvedCase(before);
      const merged = mergeIdentity(before, patches);
      merged.identity.readiness = CONTACT_IDENTITY_READINESS.UNRESOLVED;
      merged.identity.readinessGaps = [
        "NAME_INCOMPLETE",
        ...(merged.identity.readinessGaps || []).filter(
          (g) => g !== "NAME_INCOMPLETE",
        ),
      ];
      afterRows.push({
        opportunityId: before.opportunityId,
        hotelId: before.hotelId,
        category: before.category,
        personName: before.personName,
        beforeReadiness: identity.readiness,
        identity: merged.identity,
        surfeInput: {
          sufficient: false,
          surfeCallExecuted: false,
          paidCallAuthorized: false,
          surfeFieldNames: merged.surfeInput.surfeFieldNames,
          fieldsProvided: merged.surfeInput.fieldsProvided,
          omittedUnverifiedLinkedIn: true,
          improvedVsBaseline: false,
        },
        phase2: {
          action: "SKIP_ORG_LABEL",
          unresolvedClass,
          google: { skipped: true, reason: "MISSING_NAMED_PERSON" },
        },
        persist: { skipped: true, reason: "MISSING_NAMED_PERSON" },
      });
      continue;
    }

    // Preserve baseline READY + trusted LinkedIn — never demote on re-run.
    if (
      identity.readiness === CONTACT_IDENTITY_READINESS.READY &&
      identity.linkedinTrustedForSurfe === true
    ) {
      const preserved = { ...identity };
      if (!preserved.employerDomainVerified && preserved.employerName) {
        const known = lookupKnownDomain(preserved.employerName);
        if (known) {
          preserved.employerDomain = known;
          preserved.employerDomainVerified = true;
          preserved.employerDomainSource = "curated_official_domain";
        }
      }
      preserved.readiness = CONTACT_IDENTITY_READINESS.READY;
      preserved.linkedinTrustedForSurfe = true;
      preserved.linkedinStatus =
        LINKEDIN_PROFILE_MATCH.VERIFIED_LINKEDIN_PROFILE;
      const surfeInput = buildSurfeEnrichmentIdentityInput(preserved);
      afterRows.push({
        opportunityId: before.opportunityId,
        hotelId: before.hotelId,
        category: before.category,
        personName: before.personName,
        beforeReadiness: identity.readiness,
        identity: preserved,
        surfeInput: {
          sufficient: surfeInput.sufficient,
          surfeCallExecuted: false,
          paidCallAuthorized: false,
          surfeFieldNames: surfeInput.surfeFieldNames,
          fieldsProvided: surfeInput.fieldsProvided,
          omittedUnverifiedLinkedIn: surfeInput.omittedUnverifiedLinkedIn,
          improvedVsBaseline:
            JSON.stringify(before.surfeInput?.surfeFieldNames || {}) !==
            JSON.stringify(surfeInput.surfeFieldNames || {}),
        },
        phase2: {
          action: "PRESERVE_BASELINE_READY",
          google: { skipped: true, reason: "ALREADY_READY" },
        },
        persist: { skipped: true, reason: "BASELINE_READY_PRESERVED" },
      });
      continue;
    }

    if (identity.readiness === CONTACT_IDENTITY_READINESS.UNRESOLVED) {
      unresolvedClass = classifyUnresolvedCase(before);
      // Do not invent people or LinkedIn for MISSING_NAMED_PERSON
      if (unresolvedClass.kind === "MISSING_NAMED_PERSON") {
        const merged = mergeIdentity(before, patches);
        afterRows.push({
          ...before,
          phase2: {
            action: "SKIP_ORG_LABEL",
            unresolvedClass,
            google: null,
          },
          identity: merged.identity,
          surfeInput: merged.surfeInput,
        });
        continue;
      }
    }

    const needsLinkedIn =
      identity.linkedinTrustedForSurfe !== true &&
      identity.readiness !== CONTACT_IDENTITY_READINESS.UNRESOLVED &&
      Boolean(identity.firstName && identity.lastName && identity.employerName);

    // Re-evaluate existing candidate URLs offline first
    if (
      !identity.linkedinTrustedForSurfe &&
      (identity.linkedinCandidateUrl || identity.linkedinUrl)
    ) {
      const evalLi = evaluatePersonalLinkedInUrl({
        url: identity.linkedinCandidateUrl || identity.linkedinUrl,
        personName: identity.fullName,
        employerName: identity.employerName || patches.employerName,
        title: identity.title,
        evidenceText: [
          identity.employerName,
          identity.title,
          identity.fullName,
          patches.employerDomain,
        ]
          .filter(Boolean)
          .join(" "),
      });
      if (evalLi.trustedForSurfe) {
        patches.linkedinUrl = evalLi.url;
        patches.linkedinEvidenceText = evalLi.reasons?.join(" ");
      } else if (evalLi.rejected || evalLi.rejectReason) {
        incorrectRejected.push({
          opportunityId: before.opportunityId,
          personName: before.personName,
          url: evalLi.url || identity.linkedinCandidateUrl,
          reason: evalLi.rejectReason || evalLi.status,
        });
      }
    }

    if (needsLinkedIn && !patches.linkedinUrl) {
      const prior = priorByOpp.get(before.opportunityId);
      const priorGoogle = prior?.phase2?.google;
      // Reuse any prior Google attempt (including empty) unless --refresh-google
      if (priorGoogle && priorGoogle.reason !== "OFFLINE_ONLY") {
        googleResult = {
          skipped: priorGoogle.skipped === true,
          reused: true,
          reason: priorGoogle.skipped
            ? priorGoogle.reason || "REUSED_PRIOR_SKIP"
            : "REUSED_PRIOR_PHASE2_GOOGLE",
          billedUsd: 0,
          candidates: { linkedInUrl: priorGoogle.candidateUrl || null },
          linkedIn: {
            status: priorGoogle.linkedInStatus || null,
            url: priorGoogle.candidateUrl || null,
          },
          sourceUrls: priorGoogle.sourceUrls || [],
          textPreview: "",
        };
        if (prior?.identity?.linkedinTrustedForSurfe && prior.identity.linkedinUrl) {
          patches.linkedinUrl = prior.identity.linkedinUrl;
          patches.linkedinEvidenceText = [
            identity.employerName,
            identity.fullName,
            ...(priorGoogle.sourceUrls || []),
          ].join(" ");
        } else if (prior?.identity?.linkedinCandidateUrl) {
          patches.linkedinCandidateOnly = prior.identity.linkedinCandidateUrl;
        } else if (priorGoogle.candidateUrl) {
          const evalLi = evaluatePersonalLinkedInUrl({
            url: priorGoogle.candidateUrl,
            personName: identity.fullName,
            employerName: identity.employerName,
            title: identity.title,
            evidenceText: [
              identity.employerName,
              identity.title,
              identity.fullName,
              ...(priorGoogle.sourceUrls || []),
            ].join(" "),
          });
          if (evalLi.trustedForSurfe) {
            patches.linkedinUrl = evalLi.url;
            patches.linkedinEvidenceText = evalLi.reasons?.join(" ");
          } else if (
            evalLi.status === LINKEDIN_PROFILE_MATCH.CANDIDATE_LINKEDIN_PROFILE
          ) {
            patches.linkedinCandidateOnly = evalLi.url;
          } else if (evalLi.rejected || evalLi.rejectReason) {
            incorrectRejected.push({
              opportunityId: before.opportunityId,
              personName: before.personName,
              url: priorGoogle.candidateUrl,
              reason: evalLi.rejectReason || evalLi.status,
            });
          }
        }
      } else {
        googleResult = await discoverLinkedInViaGoogle(
          {
            fullName: identity.fullName,
            personName: before.personName,
            title: identity.title,
            employerName: identity.employerName,
            employerDomain:
              patches.employerDomain || identity.employerDomain || null,
          },
          spend,
        );

        if (!googleResult.skipped) {
          const li = googleResult.linkedIn;
          const candidateUrl =
            googleResult.candidates?.linkedInUrl || li?.url || null;
          const evidence = [
            identity.employerName,
            identity.title,
            identity.fullName,
            ...(googleResult.sourceUrls || []),
            googleResult.textPreview || "",
          ].join(" ");

          if (candidateUrl) {
            const evalLi = evaluatePersonalLinkedInUrl({
              url: candidateUrl,
              personName: identity.fullName,
              employerName: identity.employerName,
              title: identity.title,
              evidenceText: evidence,
            });
            if (evalLi.trustedForSurfe) {
              patches.linkedinUrl = evalLi.url;
              patches.linkedinEvidenceText = evidence;
              patches.sourceUrls = googleResult.sourceUrls;
            } else if (
              evalLi.status === LINKEDIN_PROFILE_MATCH.CANDIDATE_LINKEDIN_PROFILE
            ) {
              patches.linkedinCandidateOnly = evalLi.url;
              patches.linkedinEvidenceText = evidence;
            } else if (evalLi.rejected || evalLi.rejectReason) {
              incorrectRejected.push({
                opportunityId: before.opportunityId,
                personName: before.personName,
                url: candidateUrl,
                reason: evalLi.rejectReason || evalLi.status,
              });
            }
          }

          const noteDomain = parseEmployerDomainFromNotes(
            googleResult.textPreview || "",
          );
          const noteEmployer = parseEmployerFromNotes(
            googleResult.textPreview || "",
          );
          if (noteDomain && !patches.employerDomain) {
            const emp = stripAccents(
              noteEmployer || identity.employerName || "",
            ).toLowerCase();
            const tokens = emp
              .split(/[^a-z0-9]+/)
              .filter((t) => t.length > 3)
              .slice(0, 3);
            if (tokens.some((t) => noteDomain.includes(t.slice(0, 5)))) {
              patches.employerDomain = noteDomain;
              patches.employerDomainVerified = true;
            }
          }
          if (noteEmployer && noteEmployer.length > 2 && !identity.employerName) {
            patches.employerName = noteEmployer;
          }
        }
      }
    }

    // Explicit employerDomainVerified for curated domains via contact field
    const merged = mergeIdentity(before, {
      ...patches,
      employerDomain:
        patches.employerDomain || identity.employerDomain || null,
    });

    // If curated domain was applied, force verified flag through rebuild
    if (patches.employerDomainVerified && patches.employerDomain) {
      merged.identity.employerDomain = patches.employerDomain;
      merged.identity.employerDomainVerified = true;
      merged.identity.employerDomainSource = "curated_official_domain";
      // Recompute readiness with trusted LI if present
      const re = runContactIdentityPreflight({
        opportunity: {
          id: before.opportunityId,
          hotelId: before.hotelId,
          organizationName: identity.demandOrganizationName,
        },
        contact: {
          name: merged.identity.fullName,
          title: merged.identity.title,
          employerName: merged.identity.employerName,
          employerDomain: patches.employerDomain,
          linkedinUrl: merged.identity.linkedinUrl,
          linkedinEvidenceText: patches.linkedinEvidenceText,
        },
        employerDomain: patches.employerDomain,
        assumeDemandOrgIsEmployer: false,
      });
      Object.assign(merged, re);
      merged.identity.employerDomainVerified = true;
      merged.identity.employerDomainSource = "curated_official_domain";
    }

    // Candidate-only LinkedIn must not be trusted
    if (
      patches.linkedinCandidateOnly &&
      !merged.identity.linkedinTrustedForSurfe
    ) {
      merged.identity.linkedinCandidateUrl = patches.linkedinCandidateOnly;
      merged.identity.linkedinStatus =
        LINKEDIN_PROFILE_MATCH.CANDIDATE_LINKEDIN_PROFILE;
    }

    const surfeImproved =
      JSON.stringify(before.surfeInput?.surfeFieldNames || {}) !==
      JSON.stringify(merged.surfeInput?.surfeFieldNames || {});

    // Airtable lookups are slow — skip network unless --apply-airtable
    // or explicit --airtable-dry-run.
    let persist = { skipped: true, reason: "AIRTABLE_PROBE_SKIPPED_DEFAULT" };
    if (APPLY || process.argv.includes("--airtable-dry-run")) {
      persist = await persistIdentityIfAuthorized(
        {
          opportunityId: before.opportunityId,
          hotelId: before.hotelId,
          identity: merged.identity,
        },
        before,
      );
    }
    persistOutcomes.push({
      opportunityId: before.opportunityId,
      personName: before.personName,
      ...persist,
    });

    afterRows.push({
      opportunityId: before.opportunityId,
      hotelId: before.hotelId,
      category: before.category,
      personName: before.personName,
      beforeReadiness: identity.readiness,
      identity: merged.identity,
      surfeInput: {
        sufficient: merged.surfeInput.sufficient,
        surfeCallExecuted: false,
        paidCallAuthorized: false,
        surfeFieldNames: merged.surfeInput.surfeFieldNames,
        fieldsProvided: merged.surfeInput.fieldsProvided,
        omittedUnverifiedLinkedIn: merged.surfeInput.omittedUnverifiedLinkedIn,
        improvedVsBaseline: surfeImproved,
      },
      phase2: {
        unresolvedClass,
        google: googleResult
          ? {
              skipped: googleResult.skipped === true,
              reason: googleResult.reason || null,
              billedUsd: googleResult.billedUsd || 0,
              linkedInStatus: googleResult.linkedIn?.status || null,
              candidateUrl: googleResult.candidates?.linkedInUrl || null,
              sourceUrls: (googleResult.sourceUrls || []).slice(0, 5),
            }
          : { skipped: true, reason: "NOT_NEEDED_OR_ALREADY_READY" },
        patches: {
          employerDomain: patches.employerDomain || null,
          linkedinVerified: Boolean(patches.linkedinUrl),
          linkedinCandidate: patches.linkedinCandidateOnly || null,
        },
      },
      persist,
    });
  }

  const count = (rows, status) =>
    rows.filter((r) => r.identity?.readiness === status).length;
  const beforeMetrics = {
    READY: count(beforeRows, "READY"),
    PARTIAL: count(beforeRows, "PARTIAL"),
    UNRESOLVED: count(beforeRows, "UNRESOLVED"),
    employerName: beforeRows.filter((r) => r.identity?.employerName).length,
    domainVerified: beforeRows.filter(
      (r) => r.identity?.employerDomainVerified,
    ).length,
    linkedInVerified: beforeRows.filter(
      (r) => r.identity?.linkedinTrustedForSurfe,
    ).length,
  };
  const afterMetrics = {
    READY: count(afterRows, "READY"),
    PARTIAL: count(afterRows, "PARTIAL"),
    UNRESOLVED: count(afterRows, "UNRESOLVED"),
    employerName: afterRows.filter((r) => r.identity?.employerName).length,
    domainVerified: afterRows.filter((r) => r.identity?.employerDomainVerified)
      .length,
    domainPresent: afterRows.filter((r) => r.identity?.employerDomain).length,
    linkedInVerified: afterRows.filter(
      (r) => r.identity?.linkedinTrustedForSurfe,
    ).length,
    linkedInCandidate: afterRows.filter(
      (r) =>
        r.identity?.linkedinStatus ===
        LINKEDIN_PROFILE_MATCH.CANDIDATE_LINKEDIN_PROFILE,
    ).length,
    surfeInputsImproved: afterRows.filter(
      (r) => r.surfeInput?.improvedVsBaseline,
    ).length,
  };

  const report = {
    generatedAt: new Date().toISOString(),
    version: "gdi-contact-identity-preflight-phase2-linkedin-v1",
    baselinePath: "reports/gdi-contact-identity-preflight-v1/PILOT_20_RESULTS.json",
    pr484: {
      state: "OPEN",
      url: "https://github.com/joandejarden-create/my-operators-backend/pull/484",
      note: "Phase 1 identity preflight was uncommitted at Phase 2 start; Phase 1+2 land on same GDI branch for IC merge.",
    },
    authorization: {
      spendCeilingUsd: SPEND_CEILING_USD,
      offlineOnly: OFFLINE_ONLY,
      applyAirtable: APPLY,
      surfeSearch: false,
      surfeEnrich: false,
      apollo: false,
    },
    spend: {
      billedTowardCapUsd: Number(
        (
          spend.billedUsd + Number(priorPhase2?.spend?.billedTowardCapUsd || 0)
        ).toFixed(4),
      ),
      thisRunGoogleUsd: Number(spend.googleUsd.toFixed(4)),
      priorCachedGoogleUsd: Number(priorPhase2?.spend?.billedTowardCapUsd || 0),
      googleCallsThisRun: spend.googleCalls,
      googleCallsPriorCached: priorPhase2?.spend?.googleCalls || 0,
      exaUsd: 0,
      surfeCredits: 0,
      note: "Surfe Search/Enrich not executed. Google reused from prior Phase 2 cache when present.",
    },
    surfeSearchPeopleReadiness: {
      matchingMode: surfeSearch.schema.matchingMode,
      exactPersonMatching: surfeSearch.schema.exactPersonMatching,
      totalSearchCredits: surfeSearch.credits?.totalSearch ?? null,
      searchExecuted: false,
      recommendation: surfeSearch.recommendation,
    },
    before: beforeMetrics,
    after: afterMetrics,
    deltas: {
      READY: afterMetrics.READY - beforeMetrics.READY,
      PARTIAL: afterMetrics.PARTIAL - beforeMetrics.PARTIAL,
      UNRESOLVED: afterMetrics.UNRESOLVED - beforeMetrics.UNRESOLVED,
      domainVerified: afterMetrics.domainVerified - beforeMetrics.domainVerified,
      linkedInVerified:
        afterMetrics.linkedInVerified - beforeMetrics.linkedInVerified,
    },
    incorrectLinkedInRejected: incorrectRejected,
    betterCommercialContactsDiscovered: 0,
    betterCommercialContactsNote:
      "Surfe Search People not executed (no founder search ceiling). Org-label UNRESOLVED cases flagged as MISSING_NAMED_PERSON for continuation.",
    airtable: {
      mode: APPLY ? "APPLY" : "DRY_RUN",
      outcomes: persistOutcomes,
      readBackSuccesses: persistOutcomes.filter((p) => p.readBackOk === true)
        .length,
      skippedNoRow: persistOutcomes.filter(
        (p) => p.reason === "NO_EXISTING_RESEARCH_INVENTORY_ROW",
      ).length,
    },
    remainingGaps: afterRows
      .filter((r) => r.identity.readiness !== "READY")
      .map((r) => ({
        opportunityId: r.opportunityId,
        personName: r.personName,
        readiness: r.identity.readiness,
        gaps: r.identity.readinessGaps,
        unresolvedClass: r.phase2?.unresolvedClass || null,
      })),
    results: afterRows,
    stop: "STOP after 20-opportunity Phase 2. No Surfe enrich, no Surfe Search paid calls, no Apollo, no full-database research.",
  };

  fs.writeFileSync(
    path.join(OUT_DIR, "PHASE2_RESULTS.json"),
    JSON.stringify(report, null, 2) + "\n",
  );

  const md = [
    "# Contact Identity Preflight Phase 2 — Founder Report",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "## Before → After (same 20 opportunities)",
    "",
    `| Metric | Before | After | Δ |`,
    `|---|---:|---:|---:|`,
    `| READY | ${beforeMetrics.READY} | ${afterMetrics.READY} | ${report.deltas.READY} |`,
    `| PARTIAL | ${beforeMetrics.PARTIAL} | ${afterMetrics.PARTIAL} | ${report.deltas.PARTIAL} |`,
    `| UNRESOLVED | ${beforeMetrics.UNRESOLVED} | ${afterMetrics.UNRESOLVED} | ${report.deltas.UNRESOLVED} |`,
    `| Domain verified | ${beforeMetrics.domainVerified} | ${afterMetrics.domainVerified} | ${report.deltas.domainVerified} |`,
    `| LinkedIn verified | ${beforeMetrics.linkedInVerified} | ${afterMetrics.linkedInVerified} | ${report.deltas.linkedInVerified} |`,
    `| Surfe inputs improved | — | ${afterMetrics.surfeInputsImproved} | — |`,
    "",
    `Provider spend (this phase): **$${report.spend.billedTowardCapUsd}** / $${SPEND_CEILING_USD} ceiling (Google calls: ${spend.googleCalls}). Surfe enrich/search: **$0 / 0 credits**.`,
    "",
    "## Surfe Search People readiness",
    "",
    `- Matching mode: **${surfeSearch.schema.matchingMode}**`,
    `- Exact-person by name: **not supported** on Search — use Enrich + public LinkedIn discovery`,
    `- Account search credits: **${surfeSearch.credits?.totalSearch ?? "n/a"}**`,
    `- Search executed this phase: **no**`,
    "",
    "## Airtable",
    "",
    `- Mode: **${APPLY ? "APPLY" : "DRY_RUN"}**`,
    `- Read-back successes: ${report.airtable.readBackSuccesses}`,
    `- Skipped (no existing inventory row): ${report.airtable.skippedNoRow}`,
    "",
    "## Incorrect LinkedIn rejected",
    "",
    incorrectRejected.length
      ? incorrectRejected
          .map((x) => `- ${x.personName}: ${x.reason} (${x.url || "n/a"})`)
          .join("\n")
      : "- none",
    "",
    "## STOP",
    "",
    report.stop,
    "",
  ].join("\n");

  fs.writeFileSync(path.join(OUT_DIR, "FOUNDER_PHASE2_REPORT.md"), md, "utf8");

  console.log(
    JSON.stringify(
      {
        ok: true,
        outDir: OUT_DIR,
        before: beforeMetrics,
        after: afterMetrics,
        deltas: report.deltas,
        spendUsd: report.spend.billedTowardCapUsd,
        surfeSearchExecuted: false,
        applyAirtable: APPLY,
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
