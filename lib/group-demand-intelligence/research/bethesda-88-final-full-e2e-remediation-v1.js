/**
 * Bethesda 88 — Final Full E2E Remediation V1
 *
 * One-shot founder-authorized factual remediation across the 88-ID inventory.
 * Applies independently verified contact / destination / hotel-fit / description
 * corrections. Does NOT promote Sales Ready, publish, or spend paid research.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID,
  mergeContactAntiClobber,
  extractContactSnapshot,
  applyHistoricalContactEnrichment,
} from "./bethesda-88-contact-quality-completion-v1.js";
import { WAVE2_ACVNU_DEFERRED_ID } from "./bethesda-88-contact-completion-wave2-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "./bethesda-internal-research-union-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "./bethesda-88-commercial-event-family-v1.js";
import {
  loadHotelOpportunityDoc,
  backupHotelOpportunityStore,
  BETHESDA_HOTEL_ID,
  BETHESDA_PROTECTED_SCALING_IDS,
  WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256,
  ACCEPTABLE_59_ROW_SHA256S,
  assertNoProtectedIdLoss,
  assertCountNotDropped,
  sha256File,
} from "./gdi-hotel-store-integrity-guard-v1.js";
import { writeJsonAtomic } from "./gdi-research-stage-persistence-v1.js";
import { runGdiOpportunityFullE2eQualityOrchestration } from "../gdi-opportunity-full-e2e-quality-orchestrator-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import { evaluateAccountDevelopmentEligibility } from "../account-development-eligibility-v1.js";

export const BETHESDA_88_FINAL_FULL_E2E_REMEDIATION_V1 =
  "gdi-bethesda-88-final-full-e2e-remediation-v1";

export const FINAL_REMEDIATION_APPROVAL_TOKEN =
  "FOUNDER_APPROVE_BETHESDA_FINAL_FULL_E2E_REMEDIATION_20261112";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_REL = "reports/gdi-bethesda-88-final-full-e2e-remediation-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function coverageCounts(opportunities = []) {
  let named = 0;
  let direct = 0;
  let orgRoute = 0;
  for (const o of opportunities) {
    const s = extractContactSnapshot(o);
    if (s.named) named += 1;
    if (s.hasDirectEmail) direct += 1;
    if (s.hasOrgRoute) orgRoute += 1;
  }
  return { named, direct, orgRoute, total: opportunities.length };
}

function uniqueMissingNamed(opportunities = []) {
  const fam = applyBethesdaCommercialEventFamilyPresentation(opportunities, {
    hideLinkedFromList: true,
  });
  const missing = [];
  for (const row of fam.allAnnotated || []) {
    const snap = extractContactSnapshot(row);
    if (snap.named) continue;
    if (
      row.commercialEventFamilyRole === "LINKED_RESEARCH" ||
      row.commercialEventFamilyRole === "QUARANTINED"
    ) {
      continue;
    }
    const priority = text(row.priority).toUpperCase();
    missing.push({
      id: row.id,
      title: text(row.title).slice(0, 100),
      coverage: snap.coverage,
      role: row.commercialEventFamilyRole || "STANDALONE",
      priority: priority || null,
      requiresNamedContact: priority !== "DISQUALIFIED",
      listVisible: row.listVisible !== false,
    });
  }
  return {
    uniqueCommercial: fam.counts.uniqueCommercialOpportunities,
    missing,
    missingCount: missing.length,
    credibleMissing: missing.filter((m) => m.requiresNamedContact),
  };
}

/**
 * Factual destination / honesty / description patches authorized under final remediation.
 * Contact-only ACVNU is applied via Wave2 subset apply.
 */
export const FINAL_FACTUAL_FIELD_PATCHES = Object.freeze([
  {
    id: "gdi_opp_aao_2027_annual_session_9",
    fields: {
      hotelFitHonesty: "DESTINATION_OUTSIDE_LAS_VEGAS",
      hotelFitNote:
        "Official AAO 2027 is Las Vegas (Venetian Expo). Outside Bethesda demand territory — remain DISQUALIFIED. Named meetings owner not required.",
      eventLocation: "Las Vegas, NV",
      destinationStatus: "Las Vegas, NV",
      demandTerritoryFit: "OUTSIDE",
      organizationContactRoute: "meetings@aao.org",
      functionalContactEmail: "meetings@aao.org",
      contactOfficialUrl: "https://www.aao.org/annual-meeting/meeting-information",
      contactResearchStatus: "EXHAUSTED_DESTINATION_OUTSIDE",
      contactResearchNote:
        "Zero-paid 2026-11-12: AAO Meetings dept meetings@aao.org; housing via Expovision. Destination Las Vegas — no Bethesda lodging buyer.",
      recommendedSalesAction:
        "Do not pursue for Bethesda Marriott. Archive/monitor only if AAO relocates a future cycle into DMV.",
      hotelOpportunityThesis:
        "AAO 2027 Annual Meeting is contracted for Las Vegas (Venetian Expo), not the DMV. Outside Bethesda Marriott demand territory — keep DISQUALIFIED. Guestrooms/meetings demand is not actionable for this hotel.",
    },
    sources: [
      "https://www.aao.org/annual-meeting/named-lectures/meeting-information--past-future-meetings",
      "https://www.aao.org/annual-meeting/meeting-information",
    ],
  },
  {
    id: "gdi_opp_aao_2028_annual_meeting_10",
    fields: {
      hotelFitHonesty: "DESTINATION_OUTSIDE_CHICAGO",
      hotelFitNote:
        "Official AAO 2028 is Chicago (McCormick Place). Outside Bethesda demand territory — remain DISQUALIFIED.",
      eventLocation: "Chicago, IL",
      destinationStatus: "Chicago, IL",
      demandTerritoryFit: "OUTSIDE",
      organizationContactRoute: "meetings@aao.org",
      functionalContactEmail: "meetings@aao.org",
      contactOfficialUrl: "https://www.aao.org/annual-meeting/meeting-information",
      contactResearchStatus: "EXHAUSTED_DESTINATION_OUTSIDE",
      contactResearchNote:
        "Zero-paid 2026-11-12: AAO Meetings meetings@aao.org / Meeting Services leadership published on exhibitor contacts. Destination Chicago — no Bethesda lodging buyer.",
      recommendedSalesAction:
        "Do not pursue for Bethesda Marriott. Monitor only if a future AAO cycle enters DMV.",
      hotelOpportunityThesis:
        "AAO 2028 Annual Meeting is set for Chicago (McCormick Place), not the DMV. Outside Bethesda Marriott demand territory — keep DISQUALIFIED.",
    },
    sources: [
      "https://www.aao.org/annual-meeting/named-lectures/meeting-information--past-future-meetings",
      "https://www.aao.org/annual-meeting/meeting-information",
    ],
  },
  {
    id: "gdi_opp_48th_iscb_annual_conference_2027_12",
    fields: {
      hotelFitHonesty: "DESTINATION_OUTSIDE_ROTTERDAM",
      hotelFitNote:
        "ISCB 48 (2027) is in Rotterdam, Netherlands (De Doelen). Outside Bethesda demand territory — remain DISQUALIFIED.",
      eventLocation: "Rotterdam, The Netherlands",
      destinationStatus: "Rotterdam, The Netherlands",
      venueName: "De Doelen Conference Center, Rotterdam",
      demandTerritoryFit: "OUTSIDE",
      primaryContactName: "Dimitris Rizopoulos",
      primaryContactRole:
        "LOC Chair — 48th ISCB Annual Conference (Erasmus MC Biostatistics)",
      primaryContact: {
        name: "Dimitris Rizopoulos",
        title: "LOC Chair — 48th ISCB Annual Conference (Erasmus MC)",
        email: null,
        directPersonEmail: false,
        organizationContactRoute: "iscb48@erasmusmc.nl",
        contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
        whyRelevant:
          "Published Local Organizing Committee Chair for ISCB 48 Rotterdam 2027. Destination is Rotterdam — not a Bethesda lodging buyer.",
        sourceUrl: "https://iscb2027.info/committee-speakers/",
        verifiedAt: "2026-11-12",
      },
      organizationContactRoute: "iscb48@erasmusmc.nl",
      functionalContactEmail: "iscb48@erasmusmc.nl",
      organizationContactEmail: "iscb48@erasmusmc.nl",
      contactOfficialUrl: "https://iscb2027.info/",
      contactSourceUrl: "https://iscb2027.info/committee-speakers/",
      contactVerifiedAt: "2026-11-12",
      contactResearchStatus: "RESOLVED_NAMED_BUT_DESTINATION_OUTSIDE",
      contactResearchNote:
        "Zero-paid 2026-11-12: LOC Chair Dimitris Rizopoulos + secretariat iscb48@erasmusmc.nl. Conference is Rotterdam 22–26 Aug 2027 — not Bethesda-eligible demand.",
      recommendedSalesAction:
        "Do not pursue for Bethesda Marriott. Record is destination-outside Europe; keep DISQUALIFIED.",
      hotelOpportunityThesis:
        "48th ISCB Annual Conference (22–26 Aug 2027) is hosted in Rotterdam at De Doelen — not the DMV. Outside Bethesda Marriott demand territory. Named LOC Chair verified for research completeness only; do not treat as a local sales opportunity.",
      whyThisContact:
        "Published ISCB 48 LOC Chair (Rotterdam). Useful for research integrity; not a Bethesda lodging buyer.",
    },
    sources: [
      "https://iscb2027.info/",
      "https://iscb2027.info/committee-speakers/",
      "https://iscb2027.info/registration-contact/",
    ],
  },
  {
    id: "gdi_opp_2027_nci_rna_biology_symposium_7",
    fields: {
      hotelFitHonesty: "NIH_CAMPUS_LODGING_ADJACENT",
      hotelFitNote:
        "NCI RNA Biology Symposium Apr 15–16 2027 (CCR Conferences). Bethesda campus-adjacent lodging possible; named lodging owner not published — retain CCR functional route only (prior named-owner founder FAIL).",
      eventLocation: "Bethesda / NCI campus area",
      organizationContactRoute: "ccrconferences@mail.nih.gov",
      functionalContactEmail: "ccrconferences@mail.nih.gov",
      organizationContactEmail: "ccrconferences@mail.nih.gov",
      contactOfficialUrl:
        "https://ncifrederick.cancer.gov/events/conferences/2027NCI_RNA_Symposium/",
      contactResearchStatus: "EXHAUSTED_FUNCTIONAL_ORG_ONLY",
      contactResearchNote:
        "Zero-paid 2026-11-12: Official page publishes CCR Conferences ccrconferences@mail.nih.gov only. Prior Carrie Wagerman named-owner proposal founder-rejected. No public named lodging coordinator found.",
      recommendedSalesAction:
        "Email CCR Conferences (ccrconferences@mail.nih.gov) to ask whether a Bethesda Marriott overflow/housing block is needed for Apr 15–16 2027 attendees; do not invent a named buyer.",
      hotelOpportunityThesis:
        "NCI 2027 RNA Biology Symposium (Apr 15–16 2027) is organized by the NCI Initiative in RNA Biology via CCR Conferences. Campus-adjacent Bethesda lodging is plausible, but guestrooms demand and a named lodging owner are not yet published. Best verified route remains ccrconferences@mail.nih.gov.",
      eventDateStart: "2027-04-15",
      eventDateEnd: "2027-04-16",
      eventDateStatus: "VERIFIED",
    },
    sources: [
      "https://ncifrederick.cancer.gov/events/conferences/2027NCI_RNA_Symposium/",
    ],
  },
  {
    id: "gdi_opp_marriott_hq_adjacent_corporate_watch",
    fields: {
      hotelFitHonesty: "CATEGORY_WATCH_NO_SINGLE_BUYER",
      hotelFitNote:
        "Category watch near Marriott HQ corridor — no single buyer entity. Named contact research not applicable until a specific account surfaces.",
      contactResearchStatus: "NOT_APPLICABLE_CATEGORY_WATCH",
      contactResearchNote:
        "No discrete demand activity or buyer entity to research. Keep as account-development watch only.",
      recommendedSalesAction:
        "Use hotel CRM / local BD to identify specific HQ-adjacent accounts; do not invent a named buyer for this category card.",
      hotelOpportunityThesis:
        "Corporate offsite/training demand near the Marriott HQ / Bethesda business corridor remains a category watch without a single verified buyer entity. Commercially useful as a monitoring prompt, not as a named-contact sales opportunity.",
    },
    sources: [],
  },
]);

function applyFactualFields(before, patch) {
  const next = { ...before };
  const changed = [];
  for (const [key, value] of Object.entries(patch.fields || {})) {
    if (key === "primaryContact" && value && typeof value === "object") {
      const merged = mergeContactAntiClobber(next, { primaryContact: value });
      Object.assign(next, merged);
      // Ensure top-level mirrors for list/detail
      const snap = extractContactSnapshot(next);
      next.primaryContactName = snap.named ? snap.name : next.primaryContactName || null;
      next.primaryContactRole = snap.title || next.primaryContactRole || null;
      next.primaryContactEmail = snap.hasDirectEmail ? snap.email : null;
      next.functionalContactEmail =
        snap.orgEmail || snap.orgRoute || next.functionalContactEmail || null;
      next.organizationContactEmail =
        snap.orgEmail || next.organizationContactEmail || null;
      changed.push("primaryContact");
      continue;
    }
    const prev = next[key];
    if (JSON.stringify(prev) === JSON.stringify(value)) continue;
    next[key] = value;
    changed.push(key);
  }
  next.salesReady = false;
  next.customerFacing = false;
  next.customerVisible = false;
  next.publicationApproved = false;
  next.publication = "NOT_AUTHORIZED";
  next.finalRemediationUpdate = {
    version: BETHESDA_88_FINAL_FULL_E2E_REMEDIATION_V1,
    appliedAt: new Date().toISOString(),
    founderAuthorized: true,
    sources: patch.sources || [],
    changedFields: changed,
  };
  // Never mutate maturity/priority unless already DISQUALIFIED destination honesty.
  next.commercialMaturity = before.commercialMaturity;
  next.priority = before.priority;
  return { next, changed };
}

export function buildFinalRemediationDryRun({ root = ROOT } = {}) {
  const store = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  const patches = [];
  for (const patch of FINAL_FACTUAL_FIELD_PATCHES) {
    const before = store.opportunities.find((o) => text(o.id) === patch.id);
    if (!before) {
      patches.push({ id: patch.id, ok: false, reason: "ID_NOT_IN_STORE" });
      continue;
    }
    const { next, changed } = applyFactualFields(before, patch);
    const beforeSnap = extractContactSnapshot(before);
    const afterSnap = extractContactSnapshot(next);
    patches.push({
      id: patch.id,
      ok: true,
      changedFields: changed,
      before: {
        name: beforeSnap.name,
        coverage: beforeSnap.coverage,
        hotelFitHonesty: before.hotelFitHonesty || null,
        priority: before.priority || null,
        eventLocation: before.eventLocation || null,
      },
      after: {
        name: afterSnap.name,
        coverage: afterSnap.coverage,
        hotelFitHonesty: next.hotelFitHonesty || null,
        priority: next.priority || null,
        eventLocation: next.eventLocation || null,
      },
    });
  }
  return {
    version: BETHESDA_88_FINAL_FULL_E2E_REMEDIATION_V1,
    authorized: false,
    storeSha256: store.sha256,
    expectedStoreSha256: WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256,
    approvalToken: FINAL_REMEDIATION_APPROVAL_TOKEN,
    patchCount: patches.filter((p) => p.ok).length,
    patches,
    acvnuDeferredContactApply: WAVE2_ACVNU_DEFERRED_ID,
  };
}

/**
 * Atomic final write: ACVNU corrected contact + destination/honesty factual patches.
 */
export function applyFinalFullRemediationAtomic({
  root = ROOT,
  founderAuthorization = false,
  approvalToken = null,
  includeAcvnuContact = true,
  dryRun = true,
} = {}) {
  if (
    !founderAuthorization ||
    approvalToken !== FINAL_REMEDIATION_APPROVAL_TOKEN
  ) {
    return { ok: false, code: "FOUNDER_AUTHORIZATION_REQUIRED", writeApplied: false };
  }
  const store = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  if (store.sha256 !== WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256) {
    const already = store.opportunities.some(
      (o) => o?.finalRemediationUpdate?.version === BETHESDA_88_FINAL_FULL_E2E_REMEDIATION_V1
    );
    if (!already) {
      return {
        ok: false,
        code: "UNEXPECTED_STORE_SHA",
        sha256: store.sha256,
        expected: WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256,
      };
    }
  }
  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      writeApplied: false,
      manifest: buildFinalRemediationDryRun({ root }),
    };
  }

  const backup = backupHotelOpportunityStore(BETHESDA_HOTEL_ID, {
    root,
    label: "pre-final-full-e2e-remediation-atomic",
  });
  const byId = new Map(store.opportunities.map((o) => [text(o.id), o]));
  const applied = [];

  // 1) ACVNU corrected named + org-route (no Gmail as business direct)
  if (includeAcvnuContact) {
    const id = WAVE2_ACVNU_DEFERRED_ID;
    const before = byId.get(id);
    const evidence = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[id];
    if (before && evidence && !extractContactSnapshot(before).named) {
      const preservedOfficialUrl =
        text(before.contactOfficialUrl) ||
        text(before.contactSourceUrl) ||
        null;
      let merged = mergeContactAntiClobber(before, evidence);
      if (evidence.hotelFitHonesty) {
        merged.hotelFitHonesty = evidence.hotelFitHonesty;
        merged.hotelFitNote = evidence.hotelFitNote || null;
      }
      if (preservedOfficialUrl) {
        merged.contactOfficialUrl = preservedOfficialUrl;
        merged.contactProvenanceUrls = Array.from(
          new Set(
            [
              preservedOfficialUrl,
              text(evidence.primaryContact?.sourceUrl),
              ...(Array.isArray(evidence.primaryContact?.provenanceUrls)
                ? evidence.primaryContact.provenanceUrls.map((u) => text(u))
                : []),
            ].filter(Boolean)
          )
        );
      }
      // Commercial description for ACVNU overflow honesty
      merged.hotelOpportunityThesis =
        "ACVNU Renal Week 2027 (Apr 12–17) uses The Bethesdan Hotel (Tapestry) as the official room block. Bethesda Marriott is overflow/residual lodging only. JD Foster is the verified Renal Week Chair / ACVNU President-Elect; outreach via info@acvnu.org (Gmail founder listing is not treated as verified business-direct email).";
      merged.recommendedSalesAction =
        "Contact info@acvnu.org referencing Renal Week overflow near The Bethesdan; ask whether residual room nights or attendee referrals remain open for Bethesda Marriott.";
      merged.whyThisContact =
        text(evidence.primaryContact?.whyRelevant) || merged.whyThisContact;
      const snap = extractContactSnapshot(merged);
      merged.primaryContactName = snap.named ? snap.name : null;
      merged.primaryContactRole = snap.title || null;
      merged.primaryContactEmail = snap.hasDirectEmail ? snap.email : null;
      merged.functionalContactEmail = snap.orgEmail || snap.orgRoute || null;
      merged.organizationContactEmail = snap.orgEmail || null;
      merged.salesReady = false;
      merged.customerFacing = false;
      merged.customerVisible = false;
      merged.publicationApproved = false;
      merged.publication = "NOT_AUTHORIZED";
      merged.priority = before.priority;
      merged.commercialMaturity = before.commercialMaturity;
      merged.approvedContactUpdate = {
        version: BETHESDA_88_FINAL_FULL_E2E_REMEDIATION_V1,
        wave: "wave2_20261112",
        appliedAt: new Date().toISOString(),
        founderAuthorized: true,
        note: "ACVNU corrected: named + org route; Gmail not business-direct",
      };
      merged.finalRemediationUpdate = {
        version: BETHESDA_88_FINAL_FULL_E2E_REMEDIATION_V1,
        appliedAt: new Date().toISOString(),
        founderAuthorized: true,
        changedFields: [
          "primaryContact",
          "hotelFitHonesty",
          "hotelOpportunityThesis",
          "recommendedSalesAction",
        ],
      };
      byId.set(id, merged);
      applied.push({
        id,
        kind: "ACVNU_CONTACT",
        after: snap,
        hotelFitHonesty: merged.hotelFitHonesty,
      });
    }
  }

  // 2) Destination / honesty / research-exhaustion factual patches
  for (const patch of FINAL_FACTUAL_FIELD_PATCHES) {
    const before = byId.get(patch.id);
    if (!before) continue;
    const { next, changed } = applyFactualFields(before, patch);
    if (!changed.length) continue;
    byId.set(patch.id, next);
    applied.push({
      id: patch.id,
      kind: "FACTUAL_FIELDS",
      changedFields: changed,
      after: extractContactSnapshot(next),
      hotelFitHonesty: next.hotelFitHonesty || null,
    });
  }

  const nextOps = store.opportunities.map((o) => byId.get(text(o.id)) || o);
  assertNoProtectedIdLoss({
    hotelId: BETHESDA_HOTEL_ID,
    nextOpportunities: nextOps,
    protectedIds: [...BETHESDA_PROTECTED_SCALING_IDS],
  });
  assertCountNotDropped({
    hotelId: BETHESDA_HOTEL_ID,
    priorCount: 59,
    nextCount: nextOps.length,
  });
  if (nextOps.length !== 59) {
    return { ok: false, code: "CANONICAL_COUNT_DRIFT", writeApplied: false };
  }
  const doc = {
    ...(store.doc || {}),
    opportunities: nextOps,
    updatedAt: new Date().toISOString(),
    finalFullE2eRemediation: {
      version: BETHESDA_88_FINAL_FULL_E2E_REMEDIATION_V1,
      appliedAt: new Date().toISOString(),
      appliedCount: applied.length,
      appliedIds: applied.map((a) => a.id),
    },
  };
  writeJsonAtomic(store.path, doc);
  return {
    ok: true,
    dryRun: false,
    writeApplied: true,
    appliedCount: applied.length,
    applied,
    backupPath: backup.backupPath,
    beforeSha256: store.sha256,
    afterSha256: sha256File(store.path),
  };
}

/** @deprecated use applyFinalFullRemediationAtomic */
export function applyFinalFactualFieldPatches(opts = {}) {
  return applyFinalFullRemediationAtomic({
    ...opts,
    includeAcvnuContact: false,
  });
}

function engineEligibility(opportunities = []) {
  let salesReadyEligible = 0;
  let accountDevelopmentEligible = 0;
  const salesReadyIds = [];
  const notes = [];
  for (const o of opportunities) {
    const sr = evaluateSharedCommercialEligibility(o);
    const ad = evaluateAccountDevelopmentEligibility(o);
    const srOk =
      sr?.eligible === true ||
      sr?.ok === true ||
      sr?.result?.eligible === true ||
      String(sr?.outcome || "").toUpperCase() === "PASS" ||
      String(sr?.status || "").toUpperCase() === "PASS";
    const adOk =
      ad?.eligible === true ||
      ad?.ok === true ||
      ad?.result?.eligible === true ||
      String(ad?.outcome || "").toUpperCase() === "PASS" ||
      String(ad?.status || "").toUpperCase() === "PASS";
    if (srOk) {
      salesReadyEligible += 1;
      salesReadyIds.push(o.id);
    }
    if (adOk) accountDevelopmentEligible += 1;
  }
  notes.push(
    "Engine eligibility recomputed from persisted bodies; product maturity/priority not auto-promoted."
  );
  return { salesReadyEligible, salesReadyIds, accountDevelopmentEligible, notes };
}

export function runBethesda88FinalFullE2eRemediation({
  root = ROOT,
  applyHistorical = true,
  applyAcvnuContact = false,
  applyFactualPatches = false,
  founderAuthorization = false,
  approvalToken = null,
} = {}) {
  const outDir = path.join(root, REPORT_REL);
  fs.mkdirSync(outDir, { recursive: true });

  const storeBefore = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  if (!ACCEPTABLE_59_ROW_SHA256S.includes(storeBefore.sha256)) {
    return { ok: false, code: "UNEXPECTED_STORE_SHA", sha256: storeBefore.sha256 };
  }

  const unionBefore = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    storeBefore.opportunities,
    { root }
  );
  const beforeCoverage = coverageCounts(unionBefore.opportunities);
  const beforeUnique = uniqueMissingNamed(unionBefore.opportunities);
  const beforeEligibility = engineEligibility(unionBefore.opportunities);

  writeJsonAtomic(path.join(outDir, "00-source-of-truth-lock.json"), {
    generatedAt: new Date().toISOString(),
    storeSha: storeBefore.sha256,
    canonicalCount: storeBefore.opportunities.length,
    union: unionBefore.opportunities.length,
    coverage: beforeCoverage,
    uniqueMissingNamed: beforeUnique.missingCount,
    credibleMissingNamed: beforeUnique.credibleMissing.length,
    gaps: beforeUnique.missing,
    scalingIntact: BETHESDA_PROTECTED_SCALING_IDS.every((id) =>
      storeBefore.ids.includes(id)
    ),
  });

  let historicalPersist = { writeApplied: false };
  if (applyHistorical) {
    historicalPersist = applyHistoricalContactEnrichment({ root, dryRun: false });
  }

  const dryRunManifest = buildFinalRemediationDryRun({ root });
  writeJsonAtomic(
    path.join(outDir, "02-final-factual-dry-run-manifest.json"),
    dryRunManifest
  );

  let atomicPersist = { writeApplied: false };
  if (applyAcvnuContact || applyFactualPatches) {
    atomicPersist = applyFinalFullRemediationAtomic({
      root,
      founderAuthorization,
      approvalToken,
      includeAcvnuContact: applyAcvnuContact === true,
      dryRun: false,
    });
  }
  const acvnuPersist = {
    writeApplied:
      atomicPersist.writeApplied === true &&
      (atomicPersist.applied || []).some((a) => a.id === WAVE2_ACVNU_DEFERRED_ID),
    applied: (atomicPersist.applied || []).filter(
      (a) => a.id === WAVE2_ACVNU_DEFERRED_ID
    ),
  };
  const factualPersist = {
    writeApplied:
      atomicPersist.writeApplied === true &&
      (atomicPersist.applied || []).some((a) => a.kind === "FACTUAL_FIELDS"),
    applied: (atomicPersist.applied || []).filter((a) => a.kind === "FACTUAL_FIELDS"),
    backupPath: atomicPersist.backupPath,
    beforeSha256: atomicPersist.beforeSha256,
    afterSha256: atomicPersist.afterSha256,
    code: atomicPersist.code,
  };

  const storeAfter = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  const unionAfter = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    storeAfter.opportunities,
    { root }
  );
  const afterCoverage = coverageCounts(unionAfter.opportunities);
  const afterUnique = uniqueMissingNamed(unionAfter.opportunities);
  const afterEligibility = engineEligibility(unionAfter.opportunities);

  const exceptions = [
    {
      type: "COMMERCIAL_MATURITY_PROMOTION",
      status: "STAGED_NOT_APPLIED",
      note: "Loudoun College Showcase remains engine-eligible Sales Ready; product priority MEDIUM_PRIORITY not auto-promoted.",
      ids: ["gdi_opp_loudoun_college_showcase_2027"],
    },
    {
      type: "COMMERCIAL_MATURITY_PROMOTION",
      status: "STAGED_NOT_APPLIED",
      note: "NADO 2028 may pass contact gates but DESTINATION_UNCONFIRMED — keep Research Continuation / MEDIUM; do not promote.",
      ids: ["gdi_opp_nado_ddaa_washcon_2028"],
    },
    {
      type: "COMMERCIAL_MATURITY_PROMOTION",
      status: "STAGED_NOT_APPLIED",
      note: "ACTS has named Adam Joyce but buyer-attribution gaps remain — do not auto-promote to Sales Ready.",
      ids: ["gdi_opp_acts_ts27"],
    },
    {
      type: "PAID_RESEARCH",
      status: "NOT_AUTHORIZED",
      note: "No paid research in this run. NCI RNA named lodging owner remains unpaid blocker if deeper discovery required.",
      ids: ["gdi_opp_2027_nci_rna_biology_symposium_7"],
      proposedCeilingUsd: 4,
      provider: "Exa Agent",
      question:
        "Identify current CCR Conferences lodging/housing coordinator for 2027 NCI RNA Biology Symposium beyond ccrconferences@mail.nih.gov (Carrie Wagerman previously rejected).",
    },
  ];

  writeJsonAtomic(path.join(outDir, "03-before-after-coverage.json"), {
    before: {
      raw: beforeCoverage,
      uniqueMissingNamed: beforeUnique.missingCount,
      credibleMissingNamed: beforeUnique.credibleMissing.length,
    },
    after: {
      raw: afterCoverage,
      uniqueMissingNamed: afterUnique.missingCount,
      credibleMissingNamed: afterUnique.credibleMissing.length,
    },
    storeShaBefore: storeBefore.sha256,
    storeShaAfter: storeAfter.sha256,
  });
  writeJsonAtomic(path.join(outDir, "04-remaining-credible-contact-gaps.json"), {
    count: afterUnique.credibleMissing.length,
    rows: afterUnique.credibleMissing,
    allUniqueMissing: afterUnique.missing,
  });
  writeJsonAtomic(path.join(outDir, "05-exception-package.json"), {
    authorizedMaturityPromotion: false,
    authorizedPaidResearch: false,
    authorizedCustomerPublication: false,
    exceptions,
  });
  writeJsonAtomic(path.join(outDir, "06-sr-ad-reconciliation.json"), {
    before: beforeEligibility,
    after: afterEligibility,
  });

  const fullE2e = runGdiOpportunityFullE2eQualityOrchestration({
    root,
    applyHistorical: false,
    applyCanonical: false,
    founderAuthorization: false,
    paidResearchAuthorized: false,
    reportRel: `${REPORT_REL}/full-e2e-recert`,
  });

  const summary = {
    version: BETHESDA_88_FINAL_FULL_E2E_REMEDIATION_V1,
    ok:
      fullE2e.ok === true &&
      (acvnuPersist.writeApplied === true || acvnuPersist.deferred === true) &&
      storeAfter.opportunities.length === 59,
    generatedAt: new Date().toISOString(),
    safety: {
      paidResearch: false,
      customerPublication: false,
      maturityPromotion: false,
      opportunitiesJsonWrites:
        acvnuPersist.writeApplied === true || factualPersist.writeApplied === true,
    },
    before: {
      raw: beforeCoverage,
      uniqueMissingNamed: beforeUnique.missingCount,
      credibleMissingNamed: beforeUnique.credibleMissing.length,
    },
    after: {
      raw: afterCoverage,
      uniqueMissingNamed: afterUnique.missingCount,
      credibleMissingNamed: afterUnique.credibleMissing.length,
    },
    historicalPersist,
    acvnuPersist,
    factualPersist,
    storeShaBefore: storeBefore.sha256,
    storeShaAfter: storeAfter.sha256,
    engineEligibility: afterEligibility,
    fullE2eRecert: {
      ok: fullE2e.ok === true,
      processedCount: fullE2e.execution?.processedCount ?? null,
      commercialCertification: fullE2e.commercialCertification || null,
      researchIntegrity: fullE2e.researchIntegrity || null,
      namedRelevantPrimary: afterCoverage.named,
    },
    exceptions,
  };
  writeJsonAtomic(path.join(outDir, "00-final-remediation-summary.json"), summary);
  return summary;
}
