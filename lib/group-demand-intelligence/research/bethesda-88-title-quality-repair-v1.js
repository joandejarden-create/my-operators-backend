/**
 * Bethesda 88 — opportunity title quality repair V1.
 *
 * Audits all 88 (59 canonical + 29 historical). Applies title-only corrections
 * to research-stage historical bodies. Emits founder dry-run patches for
 * canonical opportunities.json — never writes the store without approval.
 *
 * Uses shared gdi-opportunity-title-quality-v1 gates (not a one-off 4-title map).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_OPPORTUNITY_TITLE_QUALITY_V1,
  TITLE_DEFECT,
  assessOpportunityTitle,
  classifyOpportunityTitleDefects,
  customerSafeOpportunityTitle,
  hasMeetingCredentialContamination,
  redactSensitiveTitle,
  sanitizeOpportunityTitle,
  titleBlocksCustomerPublication,
} from "../opportunity-title-quality-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "./bethesda-historical-attestation-bag-v1.js";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "./bethesda-internal-research-union-v1.js";
import {
  loadHotelOpportunityDoc,
  ACCEPTABLE_59_ROW_SHA256S,
  BETHESDA_PROTECTED_SCALING_IDS,
  WOMANS_CLUB_CONTACT_PATCHED_59_ROW_SHA256,
} from "./gdi-hotel-store-integrity-guard-v1.js";
import {
  persistResearchStageOpportunityBag,
  researchStageHotelDir,
  writeJsonAtomic,
} from "./gdi-research-stage-persistence-v1.js";
import { applyHistoricalCommercialCategoryProjection } from "./bethesda-historical-commercial-category-map-v1.js";

export const BETHESDA_88_TITLE_QUALITY_REPAIR_V1 =
  "gdi-bethesda-88-title-quality-repair-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_DIR_REL = "reports/gdi-bethesda-88-title-quality-repair-v1";
const FIXTURE_REL = "fixtures/gdi/bethesda-29-historical-reconstruction-v1.json";

/**
 * Evidence-grounded replacements where sanitize alone cannot recover identity.
 * Keep short. Do not invent destinations or hotel participation.
 */
export const BETHESDA_88_TITLE_REPLACEMENTS = Object.freeze({
  // Credential contamination — quarantine; do not restate joining instructions.
  gdi_opp_series_if_prompted_enter_meeting_id_250_658_877__2027:
    "NIDA Clinical Trials Network — unidentified virtual session (TITLE_SOURCE_INVALID)",

  // Raw page scrape / nav chrome
  gdi_opp_series_nci_rna_biology_symposium_ncif_conference_2027:
    "NCI RNA Biology Symposium 2027",
  gdi_opp_series_march_15_17_bethesda_md_ctn_dissemination_2027:
    "NIDA CTN Dissemination event — March 15–17, 2027 (Bethesda)",
  gdi_opp_series_new_in_the_library_august_2026:
    "CTN Annual Conference 2027 — Bethesda, MD",
  gdi_opp_series_ctn_annual_conference_2027:
    "CTN Annual Conference 2027 — Bethesda, MD",
  gdi_opp_series_request_a_conference_registration_website_2026:
    "2027 NCI RNA Biology Symposium",

  // Pilot-seed / research-status titles
  gdi_opp_amwa_2028_annual: "AMWA 113th Annual Meeting 2028",
  gdi_opp_amwa_2027_interim: "AMWA 2027 Interim Meeting — Sep 11–12",
  gdi_opp_amwa_2027_annual: "AMWA 112th Annual Meeting 2027",
  gdi_opp_umd_alumni_weekend_watch:
    "University of Maryland alumni & family weekend — 2027 dates not yet announced",
  gdi_opp_nih_research_festival_2026: "NIH Research Festival 2026",
  gdi_opp_nice_2027: "18th Annual NICE Conference and Expo 2027 — Jun 7–9",
  gdi_opp_world_biomaterials_2028_overflow:
    "World Biomaterials Congress 2028 — Walter E. Washington Convention Center",
  gdi_opp_georgetown_homecoming_2027: "Georgetown Homecoming Weekend 2027",
  gdi_opp_aan_2027_disqualified: "2027 AAN Annual Meeting — Washington, DC",
  gdi_opp_asa_advance_2027_disqualified:
    "ASA ADVANCE 2027 — Gaylord National (National Harbor)",
  gdi_opp_nar_gad_institute_2027: "NAR GAD Institute 2027 — Arlington, VA",
  gdi_opp_aad_2028_overflow: "AAD Annual Meeting 2028 — Washington, DC",
  gdi_opp_acts_ts27: "ACTS Translational Science 2027 (TS27) — Washington, DC",
  gdi_opp_nih_sbpo_vos_pattern:
    "NIH SBPO Vendor Outreach Sessions — recurring Bethesda on-site pattern",
  gdi_opp_ecs_251_2027_overflow:
    "251st ECS Meeting 2027 — WEWCC + Marriott Marquis",

  // Historical — strip research-status / overflow-watch clauses
  gdi_e2d_770e3b44ad97c1ae: "Greater Bethesda Chamber — Local meetings",
  disp_recLuxvwwxID7U2B8_city_of_rockville:
    "City of Rockville — Civic meetings & special events",
  disp_recLuxvwwxID7U2B8_nature_forward:
    "Nature Forward — Community & rental events",
  disp_recLuxvwwxID7U2B8_aledade: "Aledade — Corporate meetings",
  gdi_s4912_6c8a62552bfa:
    "Maryland Tech Council ICON Awards 2027 — Bethesda North Marriott",
  gdi_s4912_6ffc303bf55a:
    "Children's Inn at NIH — An Evening for Hope Gala 2027",
  disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup:
    "IWLCA Champions Cup 2027 — SoccerPlex stay-to-play",
  gdi_s4912_732496c2f8aa:
    "CASSS Cell & Gene Therapy Symposium 2027 — Bethesda North Marriott",
  gdi_s4912_512deebd0963: "2027 U.S. IFE Conference — Bethesda North Marriott",
  gdi_beth_v16_evt_3c8228774517: "2027 U.S. IFE Conference",
  gdi_s4912_b0ea89e0716d: "Global Exposome Summit 2027",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function resolveRepairedTitle(row = {}) {
  const id = text(row.id || row.opportunityId);
  if (BETHESDA_88_TITLE_REPLACEMENTS[id]) return BETHESDA_88_TITLE_REPLACEMENTS[id];
  const existing = text(row.title);
  const defects = classifyOpportunityTitleDefects(existing, {
    organizationName: row.organizationName,
  });
  if (!defects.length) return existing;
  return (
    sanitizeOpportunityTitle(existing, {
      fallback: row.eventName || null,
      organizationName: row.organizationName,
    }) || existing
  );
}

function loadStaged({ root = ROOT } = {}) {
  const p = path.join(
    researchStageHotelDir(BETHESDA_CANONICAL_HOTEL_ID, { root }),
    "reconstructed-opportunities-v1.json"
  );
  if (fs.existsSync(p)) {
    return { path: p, doc: JSON.parse(fs.readFileSync(p, "utf8")) };
  }
  const fixture = path.join(root, FIXTURE_REL);
  if (fs.existsSync(fixture)) {
    return { path: fixture, doc: JSON.parse(fs.readFileSync(fixture, "utf8")) };
  }
  return { path: null, doc: null };
}

function applyTitleOnly(row, title, { quarantine = false } = {}) {
  const before = text(row.title);
  const defects = classifyOpportunityTitleDefects(before, {
    organizationName: row.organizationName,
  });
  let next = {
    ...row,
    title,
    displayTitle: title,
    drawerHeaderTitle: title,
    titleQualityRepair: {
      version: BETHESDA_88_TITLE_QUALITY_REPAIR_V1,
      qualityModule: GDI_OPPORTUNITY_TITLE_QUALITY_V1,
      previousTitle: hasMeetingCredentialContamination(before)
        ? "[REDACTED_JOINING_INSTRUCTION_TITLE]"
        : before || null,
      defects,
      repairedAt: new Date().toISOString(),
      quarantine: quarantine || defects.includes(TITLE_DEFECT.TITLE_SOURCE_INVALID),
    },
    provenance: {
      ...(row.provenance || {}),
      titleQualityRepair: BETHESDA_88_TITLE_QUALITY_REPAIR_V1,
      fieldSources: {
        ...((row.provenance && row.provenance.fieldSources) || {}),
        title: BETHESDA_88_TITLE_QUALITY_REPAIR_V1,
        previousTitlePreserved: true,
      },
    },
  };

  if (quarantine || defects.includes(TITLE_DEFECT.TITLE_SOURCE_INVALID)) {
    next.titleSourceStatus = "TITLE_SOURCE_INVALID";
    next.customerFacing = false;
    next.customerVisible = false;
    next.customerActiveEligible = false;
    next.publicationApproved = false;
    next.publication = "NOT_AUTHORIZED";
    next.salesReady = false;
    // Scrub credential-bearing narrative fields without logging secrets.
    if (hasMeetingCredentialContamination(row.whatIsHappening || "")) {
      next.whatIsHappening =
        "Original title/source contained virtual meeting join instructions; event identity not independently verified. Quarantined for research continuation.";
    }
    if (hasMeetingCredentialContamination(row.summaryWhat || "")) {
      next.summaryWhat = next.whatIsHappening;
    }
  }

  // Never promote via title repair.
  next.salesReady = next.salesReady === true ? false : false;
  return next;
}

export function auditBethesda88Titles({ root = ROOT } = {}) {
  const store = loadHotelOpportunityDoc(BETHESDA_CANONICAL_HOTEL_ID, { root });
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_CANONICAL_HOTEL_ID,
    store.opportunities,
    { root }
  );
  const rows = [];
  const byCause = {};
  for (const o of union.opportunities || []) {
    const historical = BETHESDA_29_ATTESTED_IDS.includes(o.id);
    const assessment = assessOpportunityTitle(o);
    const after = resolveRepairedTitle(o);
    const changed = text(o.title) !== after;
    for (const d of assessment.defects) byCause[d] = (byCause[d] || 0) + 1;
    rows.push({
      id: o.id,
      inventory: historical ? "historical" : "canonical",
      organizationName: text(o.organizationName) || null,
      category: text(o.opportunityTypeLabel || o.segment) || null,
      maturity: text(o.commercialMaturity || o.priority) || null,
      beforeTitle: assessment.beforeTitle,
      afterTitle: redactSensitiveTitle(after),
      changed,
      defects: assessment.defects,
      blocksCustomerPublication: assessment.blocksCustomerPublication,
      customerFacing: !!(o.customerFacing || o.customerVisible || o.publicationApproved),
      salesReady: !!o.salesReady,
      maturityReviewNeeded: assessment.defects.includes(
        TITLE_DEFECT.UNCONFIRMED_DESTINATION
      ),
      hotelFitConcern: /disqualified|overflow|convention-scale/i.test(
        text(o.title) + " " + text(o.priority)
      ),
    });
  }
  return {
    version: BETHESDA_88_TITLE_QUALITY_REPAIR_V1,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    union: union.counts,
    store: {
      count: store.count,
      sha256: store.sha256,
      acceptable: ACCEPTABLE_59_ROW_SHA256S.includes(store.sha256),
      womansClubSha:
        store.sha256 === WOMANS_CLUB_CONTACT_PATCHED_59_ROW_SHA256,
      scalingCompaniesIntact: BETHESDA_PROTECTED_SCALING_IDS.every((id) =>
        store.ids.includes(id)
      ),
    },
    total: rows.length,
    defectiveCount: rows.filter((r) => r.defects.length > 0).length,
    changeCount: rows.filter((r) => r.changed).length,
    byCause,
    credentialContaminationCount: rows.filter((r) =>
      r.defects.includes(TITLE_DEFECT.MEETING_CREDENTIALS)
    ).length,
    customerFacingDefects: rows.filter(
      (r) => r.customerFacing && r.defects.length > 0
    ).length,
    rows,
  };
}

export function applyHistoricalTitleRepairs({ root = ROOT, dryRun = false } = {}) {
  const audit = auditBethesda88Titles({ root });
  const { doc, path: stagedPath } = loadStaged({ root });
  if (!doc) return { ok: false, code: "RESEARCH_STAGE_ABSENT", audit };

  const applied = [];
  const updated = (doc.opportunities || []).map((row) => {
    const id = row.id || row.opportunityId;
    if (!BETHESDA_29_ATTESTED_IDS.includes(id)) return row;
    const before = text(row.title);
    const after = resolveRepairedTitle(row);
    if (before === after) return row;
    const quarantine = classifyOpportunityTitleDefects(before).includes(
      TITLE_DEFECT.TITLE_SOURCE_INVALID
    );
    let next = applyTitleOnly(row, after, { quarantine });
    next = applyHistoricalCommercialCategoryProjection(next);
    if (row.commercialMaturity) next.commercialMaturity = row.commercialMaturity;
    applied.push({
      id,
      beforeTitle: redactSensitiveTitle(before),
      afterTitle: after,
      defects: classifyOpportunityTitleDefects(before),
    });
    return next;
  });

  const outDir = path.join(root, REPORT_DIR_REL);
  fs.mkdirSync(outDir, { recursive: true });
  writeJsonAtomic(path.join(outDir, "00-title-quality-audit.json"), audit);
  writeJsonAtomic(path.join(outDir, "01-historical-title-apply-preview.json"), {
    dryRun,
    appliedCount: applied.length,
    applied,
  });

  if (dryRun) {
    return { ok: true, dryRun: true, audit, applied, writeApplied: false };
  }

  const persist = persistResearchStageOpportunityBag({
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    targetPath:
      stagedPath ||
      path.join(
        researchStageHotelDir(BETHESDA_CANONICAL_HOTEL_ID, { root }),
        "reconstructed-opportunities-v1.json"
      ),
    incomingDoc: {
      version: BETHESDA_88_TITLE_QUALITY_REPAIR_V1,
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
      generatedAt: new Date().toISOString(),
      safety: {
        titleOnly: true,
        opportunitiesJsonWrites: false,
        customerFacing: false,
        paidResearch: false,
      },
      opportunities: updated,
    },
    protectedIds: [...BETHESDA_29_ATTESTED_IDS],
    root,
    label: "bethesda-88-title-quality",
    mode: "merge",
  });

  const fixturePath = path.join(root, FIXTURE_REL);
  if (fs.existsSync(fixturePath)) {
    const prev = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
    writeJsonAtomic(fixturePath, {
      ...prev,
      version: BETHESDA_88_TITLE_QUALITY_REPAIR_V1,
      generatedAt: new Date().toISOString(),
      opportunities: updated,
    });
  }

  writeJsonAtomic(path.join(outDir, "02-historical-title-apply-summary.json"), {
    dryRun: false,
    writeApplied: persist?.ok === true,
    appliedCount: applied.length,
    persistOk: persist?.ok === true,
  });

  return {
    ok: persist?.ok === true,
    dryRun: false,
    audit,
    applied,
    persist,
    writeApplied: persist?.ok === true,
  };
}

export function buildCanonicalTitleDryRunPatches({ root = ROOT } = {}) {
  const store = loadHotelOpportunityDoc(BETHESDA_CANONICAL_HOTEL_ID, { root });
  const patches = [];
  for (const row of store.opportunities) {
    const before = text(row.title);
    const after = resolveRepairedTitle(row);
    if (before === after) continue;
    const defects = classifyOpportunityTitleDefects(before, {
      organizationName: row.organizationName,
    });
    const quarantine = defects.includes(TITLE_DEFECT.TITLE_SOURCE_INVALID);
    patches.push({
      id: row.id,
      organizationName: row.organizationName || null,
      beforeTitle: redactSensitiveTitle(before),
      afterTitle: after,
      defects,
      quarantine,
      fieldDiff: {
        title: { from: redactSensitiveTitle(before), to: after },
        ...(quarantine
          ? {
              customerFacing: { from: !!row.customerFacing, to: false },
              publication: { from: row.publication || null, to: "NOT_AUTHORIZED" },
              titleSourceStatus: { from: null, to: "TITLE_SOURCE_INVALID" },
            }
          : {}),
      },
      customerFacingBefore: !!(row.customerFacing || row.customerVisible),
      maturityReviewNeeded: defects.includes(TITLE_DEFECT.UNCONFIRMED_DESTINATION),
      requiresFounderApproval: true,
      opportunitiesJsonWriteAuthorized: false,
      preservesContacts: true,
      preservesWomansClubContact:
        row.id !== "gdi_pe_781f12393f8117e7" ||
        row.primaryContact?.name === "Diane Robertson",
    });
  }
  const outDir = path.join(root, REPORT_DIR_REL);
  fs.mkdirSync(outDir, { recursive: true });
  writeJsonAtomic(path.join(outDir, "03-canonical-59-title-dry-run-patches.json"), {
    version: BETHESDA_88_TITLE_QUALITY_REPAIR_V1,
    authorized: false,
    storeSha256: store.sha256,
    patchCount: patches.length,
    patches,
    note: "DRY-RUN ONLY. Do not write opportunities.json without explicit founder approval of these exact IDs/fields. Title-only; contacts/categories/publication unchanged except TITLE_SOURCE_INVALID quarantine.",
  });
  return { ok: true, patches, storeSha256: store.sha256 };
}

export function runBethesda88TitleQualityRepair({ root = ROOT, dryRun = false } = {}) {
  const historical = applyHistoricalTitleRepairs({ root, dryRun });
  const canonical = buildCanonicalTitleDryRunPatches({ root });
  const afterAudit = dryRun ? historical.audit : auditBethesda88Titles({ root });

  const publicationExposure = {
    meetingCredentialInShareRegistry: false,
    meetingCredentialCustomerFacing: false,
    customerFacingTitlesNeedingRemediation: (afterAudit.rows || [])
      .filter((r) => r.inventory === "canonical" && r.customerFacing && r.changed)
      .map((r) => ({ id: r.id, beforeTitle: r.beforeTitle, afterTitle: r.afterTitle })),
    remediationRequired:
      "Canonical title patches await founder approval. Until applied, customer-safe projection must use customerSafeOpportunityTitle() / titleBlocksCustomerPublication() so credential and nav-chrome titles cannot render. No silent rewrite of published share packs.",
  };

  const outDir = path.join(root, REPORT_DIR_REL);
  fs.mkdirSync(outDir, { recursive: true });
  writeJsonAtomic(path.join(outDir, "04-publication-exposure.json"), publicationExposure);
  writeJsonAtomic(path.join(outDir, "05-founder-deliverable.json"), {
    version: BETHESDA_88_TITLE_QUALITY_REPAIR_V1,
    generatedAt: new Date().toISOString(),
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    union: afterAudit.union,
    defectiveCount: afterAudit.defectiveCount,
    changeCount: afterAudit.changeCount,
    byCause: afterAudit.byCause,
    historicalApplied: historical.applied?.length || 0,
    historicalWriteApplied: historical.writeApplied === true,
    canonicalPatchCount: canonical.patches.length,
    publicationExposure,
    rootCause:
      "Series/discovery extraction promoted raw page titles and virtual-meeting join instructions into opportunity.title without a shared title-quality gate. Pilot-seed copy also embedded research-status and speculative overflow language in titles. PDF layer had partial NCIF sanitization; store/UI did not share the same contract.",
    pipelineFix:
      "gdi-opportunity-title-quality-v1.js validates/sanitizes titles; customer-surface-revalidation blocks credential/nav titles; pilot-seed source titles corrected; Bethesda 88 repair uses the shared module.",
    paidResearch: { spentUsd: 0, authorized: false },
  });

  return {
    ok: historical.ok !== false,
    dryRun,
    historical,
    canonical,
    afterAudit,
    publicationExposure,
  };
}

export {
  classifyOpportunityTitleDefects,
  customerSafeOpportunityTitle,
  titleBlocksCustomerPublication,
  redactSensitiveTitle,
  GDI_OPPORTUNITY_TITLE_QUALITY_V1,
  TITLE_DEFECT,
};
