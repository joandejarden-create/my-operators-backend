/**
 * Bethesda 29 historical opportunity title standardization V1.
 *
 * Aligns research-stage titles with the existing GDI commercial title convention
 * observed on strong canonical Bethesda opportunities:
 *   "{Specific event / activity} {year?} — {status / venue / lodging context}"
 *
 * Does NOT invent demand. Does NOT write opportunities.json.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { BETHESDA_29_ATTESTED_IDS } from "./bethesda-historical-attestation-bag-v1.js";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import { applyHistoricalCommercialCategoryProjection } from "./bethesda-historical-commercial-category-map-v1.js";
import {
  persistResearchStageOpportunityBag,
  researchStageHotelDir,
  writeJsonAtomic,
} from "./gdi-research-stage-persistence-v1.js";

export const BETHESDA_29_TITLE_STANDARDIZATION_V1 =
  "gdi-bethesda-29-title-standardization-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const FIXTURE_REL = "fixtures/gdi/bethesda-29-historical-reconstruction-v1.json";
const REPORT_DIR_REL = "reports/gdi-bethesda-29-title-standardization-v1";

/** Taxonomy / maturity suffixes that are not commercial activity descriptions. */
const CATEGORY_LABEL_RE =
  /^(Local Catering & Day Meetings|Corporate Meetings & Incentives|Corporate Celebrations & Private Events|Preferred Lodging Partnership|Travel Trade & Group Intermediaries|Professional Event Planner Partnership|Sports & Entertainment|Relocation & Extended Stay|Association \/ Conference \/ Events|Overflow \/ preferred lodging|Group Demand)(\s*\((Partnership|Overflow) Watch\))?$/i;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Detect org-only, taxonomy-appended, or internal placeholder titles.
 * Matches the cleanup-era pattern that produced "Org — Category Label".
 */
export function isGenericHistoricalTitle(title, organizationName = "") {
  const t = text(title);
  const org = text(organizationName);
  if (!t) return true;
  if (org && t === org) return true;
  if (/^Local catering \/ corporate celebrations/i.test(t)) return true;
  if (/^Corporate meetings\/training \+ education/i.test(t)) return true;
  if (/^Relocation & Extended Stay — deep research/i.test(t)) return true;
  if (/^2027 holiday \/ year-end celebrations/i.test(t)) return true;
  if (/Potential Business Opportunity|Strategic Opportunity|Partnership Prospect|Local Demand|Other\s*\/\s*Unclassified/i.test(t)) {
    return true;
  }
  // "Org — Category" / "Org — Category (Watch)"
  if (org && t.startsWith(`${org} — `)) {
    const rest = text(t.slice(org.length + 3));
    if (CATEGORY_LABEL_RE.test(rest)) return true;
  }
  // Multi-clause titles that still bolt taxonomy on the end:
  // e.g. "Montgomery College — AI Skills Summit — Local Catering & Day Meetings"
  // Two-clause activity titles may legitimately end with a segment that overlaps a
  // category word (e.g. "… — Relocation & extended stay") — do not flag those.
  const emParts = t.split(" — ");
  if (emParts.length >= 3) {
    const last = text(emParts[emParts.length - 1]);
    if (CATEGORY_LABEL_RE.test(last)) return true;
  }
  return false;
}

/**
 * Evidence-grounded titles for the 29 attested historical IDs.
 * Convention: specific activity first; honest watch/status in em-dash clause.
 */
export const BETHESDA_29_STANDARDIZED_TITLES = Object.freeze({
  disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce:
    "Greater Bethesda Chamber — Off the Clock & membership receptions",
  gdi_e2d_770e3b44ad97c1ae:
    "Greater Bethesda Chamber — Local meetings research continuation",
  disp_recLuxvwwxID7U2B8_montgomery_county_shrm:
    "Montgomery County SHRM — Chapter meetings & holiday programs",
  disp_recLuxvwwxID7U2B8_pmi_montgomery_county_maryland_chapter:
    "PMI Montgomery County — Monthly dinner meetings & FedTALKS",
  disp_recLuxvwwxID7U2B8_city_of_rockville:
    "City of Rockville — Civic meetings research continuation",
  disp_recLuxvwwxID7U2B8_nature_forward:
    "Nature Forward — Community meetings research continuation",
  gdi_s4912_59382d1e71a2:
    "Montgomery College AI Skills Summit — Future edition research",
  gdi_s4912_d55ee8a8e9ca:
    "NIDDK Meetings & Workshops — Scientific program portfolio",
  gdi_s4912_6c8a62552bfa:
    "Maryland Tech Council ICON Awards 2027 — Bethesda North host (overflow watch)",
  gdi_s4912_6ffc303bf55a:
    "Children's Inn at NIH — An Evening for Hope Gala (2027 venue TBD)",
  disp_recLuxvwwxID7U2B8_strathmore:
    "Strathmore — Preferred lodging partnership watch",
  disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_:
    "Cohera — DMV group intermediary partnership watch",
  disp_recLuxvwwxID7U2B8_linder_global_events:
    "LINDER Global Events — Travel-trade partnership watch",
  gdi_e2d_bc0a94d7c6007f36:
    "Visit Montgomery — Destination lodging partnership watch",
  gdi_plp_live_fa2063dc1195:
    "Rock Creek Mansion — Preferred lodging partnership watch",
  gdi_plp_live_4958569d826e:
    "The Cordell — Preferred lodging partnership watch",
  gdi_pep_live_af0711e711ba:
    "Howerton+Wooten Events — Planner partnership watch",
  gdi_pep_live_c165c897d233:
    "Potomac Meetings and Events — Planner partnership watch",
  disp_recLuxvwwxID7U2B8_aledade:
    "Aledade — Corporate meetings research continuation",
  disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup:
    "IWLCA Champions Cup 2027 — SoccerPlex stay-to-play (overflow watch)",
  disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_:
    "PDA TRI Bethesda training — December 2026 lodging research",
  gdi_s4912_66a1f89e5afb:
    "NIH OITE Postdoctoral Fellows — Relocation & extended stay",
  gdi_s4912_364713d41c67:
    "NIH Division of International Services — Relocation & extended stay",
  gdi_s4912_732496c2f8aa:
    "CASSS Cell & Gene Therapy Symposium 2027 — Bethesda North host (overflow watch)",
  gdi_s4912_512deebd0963:
    "2027 U.S. IFE Conference — Bethesda North host (overflow watch)",
  gdi_beth_v16_evt_3c8228774517:
    "2027 U.S. IFE Conference — Overflow lodging research",
  gdi_beth_v16_evt_90a6ddcb10ca:
    "Kappa Kappa Psi & Tau Beta Sigma National Convention 2027",
  gdi_beth_v16_evt_0ab91e208405:
    "IFPAC-2027 — Advanced Manufacturing Science & Innovation",
  gdi_s4912_b0ea89e0716d:
    "Global Exposome Summit 2027 — Overflow lodging research",
});

/** Titles kept relatively generic because evidence cannot support a sharper activity claim. */
export const INTENTIONALLY_GENERIC_TITLE_IDS = Object.freeze([
  "gdi_e2d_770e3b44ad97c1ae",
  "disp_recLuxvwwxID7U2B8_city_of_rockville",
  "disp_recLuxvwwxID7U2B8_nature_forward",
  "disp_recLuxvwwxID7U2B8_aledade",
]);

export function resolveStandardizedTitle(row = {}) {
  const id = text(row.id || row.opportunityId);
  const mapped = BETHESDA_29_STANDARDIZED_TITLES[id];
  if (mapped) return mapped;
  const org = text(row.organizationName);
  const existing = text(row.title);
  if (existing && !isGenericHistoricalTitle(existing, org)) return existing;
  return org || id;
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

export function auditBethesda29Titles({ root = ROOT } = {}) {
  const { doc } = loadStaged({ root });
  const rows = doc?.opportunities || [];
  const byId = new Map(rows.map((r) => [r.id || r.opportunityId, r]));
  const crosswalk = [];
  for (const id of BETHESDA_29_ATTESTED_IDS) {
    const row = byId.get(id);
    if (!row) {
      crosswalk.push({ id, issue: "MISSING_FROM_RESEARCH_STAGE" });
      continue;
    }
    const before = text(row.title);
    const after = resolveStandardizedTitle(row);
    const org = text(row.organizationName);
    crosswalk.push({
      id,
      organizationName: org,
      category: row.opportunityTypeLabel || null,
      maturity: row.commercialMaturity || null,
      beforeTitle: before,
      afterTitle: after,
      changed: before !== after,
      beforeWasGeneric: isGenericHistoricalTitle(before, org),
      intentionallyGeneric: INTENTIONALLY_GENERIC_TITLE_IDS.includes(id),
      evidenceSnippet: text(row.whatIsHappening || row.summaryWhat).slice(0, 180),
      reason:
        before === after
          ? "Already meets commercial title standard"
          : isGenericHistoricalTitle(before, org)
            ? "Replaced org—category / placeholder title with evidence-grounded activity title"
            : "Refined title to match GDI commercial convention",
    });
  }
  return {
    version: BETHESDA_29_TITLE_STANDARDIZATION_V1,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    count: crosswalk.length,
    changedCount: crosswalk.filter((r) => r.changed).length,
    intentionallyGenericCount: crosswalk.filter((r) => r.intentionallyGeneric).length,
    crosswalk,
  };
}

export function applyTitleToRow(row = {}) {
  const before = text(row.title);
  const title = resolveStandardizedTitle(row);
  let next = {
    ...row,
    title,
    displayTitle: title,
    drawerHeaderTitle: title,
    titleStandardization: {
      version: BETHESDA_29_TITLE_STANDARDIZATION_V1,
      previousTitle: before || null,
      standardizedAt: new Date().toISOString(),
      intentionallyGeneric: INTENTIONALLY_GENERIC_TITLE_IDS.includes(
        text(row.id || row.opportunityId)
      ),
    },
    provenance: {
      ...(row.provenance || {}),
      titleStandardization: BETHESDA_29_TITLE_STANDARDIZATION_V1,
      fieldSources: {
        ...((row.provenance && row.provenance.fieldSources) || {}),
        title: "bethesda-29-title-standardization-v1",
        previousTitlePreserved: true,
      },
    },
    salesReady: false,
    customerFacing: false,
    customerVisible: false,
    publicationApproved: false,
    publication: "NOT_AUTHORIZED",
  };
  next = applyHistoricalCommercialCategoryProjection(next);
  // Preserve maturity / stage from prior cleanup / Exa work.
  if (row.commercialMaturity) next.commercialMaturity = row.commercialMaturity;
  if (row.commercialStagePill) next.commercialStagePill = row.commercialStagePill;
  if (row.priorityPillLabel) next.priorityPillLabel = row.priorityPillLabel;
  return next;
}

/**
 * Persist title-only updates into research-stage + fixture (merge). Never touches opportunities.json.
 */
export function stageBethesda29TitleStandardization({
  root = ROOT,
  dryRun = false,
} = {}) {
  const audit = auditBethesda29Titles({ root });
  const { doc, path: stagedPath } = loadStaged({ root });
  if (!doc) {
    return { ok: false, code: "RESEARCH_STAGE_ABSENT", audit };
  }

  const updated = (doc.opportunities || []).map((row) => {
    const id = row.id || row.opportunityId;
    if (!BETHESDA_29_ATTESTED_IDS.includes(id)) return row;
    return applyTitleToRow(row);
  });

  const outDir = path.join(root, REPORT_DIR_REL);
  fs.mkdirSync(outDir, { recursive: true });
  writeJsonAtomic(path.join(outDir, "00-title-crosswalk.json"), audit);

  if (dryRun) {
    writeJsonAtomic(path.join(outDir, "01-dry-run-summary.json"), {
      dryRun: true,
      writeApplied: false,
      changedCount: audit.changedCount,
      convention:
        "Specific event/activity first; honest watch/status after em dash — matching strong canonical Bethesda titles.",
    });
    return { ok: true, dryRun: true, audit, writeApplied: false };
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
      version: BETHESDA_29_TITLE_STANDARDIZATION_V1,
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
      generatedAt: new Date().toISOString(),
      safety: {
        titleOnly: true,
        opportunitiesJsonWrites: false,
        customerFacing: false,
        salesReadyForced: false,
        paidResearch: false,
      },
      opportunities: updated,
    },
    protectedIds: [...BETHESDA_29_ATTESTED_IDS],
    root,
    label: "bethesda-29-title-standardization",
    mode: "merge",
  });

  const fixturePath = path.join(root, FIXTURE_REL);
  if (fs.existsSync(fixturePath)) {
    const prev = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
    writeJsonAtomic(fixturePath, {
      ...prev,
      version: BETHESDA_29_TITLE_STANDARDIZATION_V1,
      generatedAt: new Date().toISOString(),
      opportunities: updated,
    });
  }

  writeJsonAtomic(path.join(outDir, "01-apply-summary.json"), {
    dryRun: false,
    writeApplied: persist?.ok === true,
    persistOk: persist?.ok === true,
    changedCount: audit.changedCount,
    researchStageCount: updated.length,
  });

  return {
    ok: persist?.ok === true,
    dryRun: false,
    audit,
    persist,
    writeApplied: persist?.ok === true,
  };
}
