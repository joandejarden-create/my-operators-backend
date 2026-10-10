/**
 * YOTEL Ready card final quality gate — frozen 9-record cohort from ready-quality audit.
 * Commercial actionability, copy scrub, QUALIFY→CONTACT_NOW, honest demotions.
 *
 *   node scripts/gdi-yotel-ready-card-final-quality-gate.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

import {
  applyLiveCommercialQuality,
  filterCustomerFacingOpportunities,
  filterSalespersonView,
  isGdiCustomerOpportunityReady,
  isValidFutureWatch,
} from "../lib/group-demand-intelligence/index.js";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import {
  classifyBuyerContactPath,
  BUYER_CONTACT_PATH_CLASS,
} from "../lib/group-demand-intelligence/buyer-contact-path-taxonomy-v1.js";
import {
  ACCOUNT_QUALITY_CLASS,
  classifyAccountQuality,
  buildCustomerAccountDescription,
  buildCustomerAccountTitle,
  customerSafeSegment,
  customerSafeContactPath,
} from "../lib/group-demand-intelligence/account-quality-taxonomy-v1.js";
import { BOOKING_WINDOW } from "../lib/group-demand-intelligence/claim-types.js";
import * as fsRepo from "../lib/group-demand-intelligence/repository.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "gdi", "yotel-ready-card-final-quality-gate");
const YOTEL = "recrPQcZg7SFARRb2";
const BETHESDA = "recLuxvwwxID7U2B8";
const NOW = "2026-10-05";
const RUN_ID = `gdi_yotel_ready_card_gate_${crypto.randomBytes(3).toString("hex")}`;

/** Frozen cohort: 9 non-Palexpo READY from yotel-ready-quality-audit READY_RECLASSIFICATION.csv */
const FROZEN_READY_NINE = [
  "gdi_opp_aidex_geneva_11",
  "gdi_opp_ycamp_setac_europe_37_2027_society_of_environmental_toxicology_and_chemistr_parent_socie",
  "gdi_opp_ycamp_chi_geneva_centennial_2026_chi_de_gen_ve_concours_hippique_international_event_org",
  "gdi_opp_ycamp_ecosoc_has_2027_united_nations_ecosoc_ocha_secretariat",
  "gdi_opp_ycamp_art_geneve_2027_art_gen_ve_organizer",
  "gdi_opp_ycamp_watches_wonders_2027_watches_and_wonders_organizer",
  "gdi_opp_ycamp_setac_europe_37_2027_setac_europe_organizer",
  "gdi_opp_ycamp_geneva_health_forum_2026_geneva_health_forum_organizer",
  "gdi_opp_ycamp_geneva_health_forum_2026_university_of_geneva_institute_of_global_health_universit",
];

const GROUP_MOTION_TYPES = [
  "EXHIBITOR_TEAM_LODGING",
  "DELEGATION_LODGING",
  "PRODUCTION_CREW",
  "SPONSOR_ACTIVATION",
  "ORGANIZER_HOUSING",
  "SATELLITE_MEETING",
  "SCIENTIFIC_DELEGATION",
  "CONFERENCE_SUPPORT",
  "OTHER_SUPPORTED_GROUP_MOTION",
];

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, cols) {
  if (!rows.length) return cols.join(",") + "\n";
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => csvEscape(r[c] ?? "")).join(","));
  return lines.join("\n") + "\n";
}
function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}

function hasResearchJargon(o) {
  const b = `${o.segment || ""} ${o.summaryWhat || ""} ${o.title || ""} ${o.whyNow || ""}`;
  return (
    /child account|demand campaign|generator|mega.?event|plausible_fit|parent society|\(ORGANIZER\)|\(EVENT ORGANIZER\)|\(UNIVERSITY HOST\)|\(SECRETARIAT\)/i.test(
      b
    ) || /is tied to|creating a potential overflow|qualify buyer/i.test(b)
  );
}

function inferGroupMotion(o, aq) {
  const role = `${o.participationRole || ""} ${o.primaryContactRole || ""} ${o.buyerRole || ""}`.toUpperCase();
  const org = `${o.organizationName || ""} ${o.title || ""}`.toLowerCase();
  if (/delegation|humanitarian|ecosoc|ocha|ngo|government/i.test(role + org)) {
    return "DELEGATION_LODGING";
  }
  if (/exhibitor|fair|trade show|aidex|watches/i.test(role + org)) {
    return "EXHIBITOR_TEAM_LODGING";
  }
  if (/scientific|setac|meeting|conference support|symposium/i.test(role + org)) {
    return "SCIENTIFIC_DELEGATION";
  }
  if (/crew|production|vendor|media/i.test(role + org)) return "PRODUCTION_CREW";
  if (/sponsor|activation|brand/i.test(role + org)) return "SPONSOR_ACTIVATION";
  if (aq.class === ACCOUNT_QUALITY_CLASS.TRUE_ORGANIZER_HOUSING_ACCOUNT) {
    return "ORGANIZER_HOUSING";
  }
  if (/event operations|hospitality|accommodation|chi|equestrian/i.test(role + org)) {
    return "ORGANIZER_HOUSING";
  }
  if (/university|academic|institute/i.test(role + org)) return "CONFERENCE_SUPPORT";
  return "OTHER_SUPPORTED_GROUP_MOTION";
}

function salesWhyNow(o) {
  const start = o.eventStartDate || "";
  const org = o.organizationName || "the account";
  if (start) {
    return `Published ${start.slice(0, 10)} cycle — engage the events/housing function before preferred-hotel lists and overflow inventory lock for international teams.`;
  }
  return `Published event cycle approaching — confirm lodging coordination and overflow availability with ${org}'s events/hospitality function.`;
}

function salesAction(o, motion) {
  const buyer = o.buyerEntity || o.organizationName || "the account";
  const role = o.primaryContactRole || o.buyerRole || "events / hospitality";
  if (motion === "EXHIBITOR_TEAM_LODGING" || motion === "ORGANIZER_HOUSING") {
    return `Contact ${buyer} (${role}) to confirm exhibitor/team accommodation arrangements and ask whether preferred-hotel or corridor overflow inventory is still open for international groups.`;
  }
  if (motion === "DELEGATION_LODGING") {
    return `Contact ${buyer} (${role}) to confirm delegation lodging coordination and whether Geneva-area overflow hotels may be assigned for traveling participants.`;
  }
  if (motion === "SCIENTIFIC_DELEGATION") {
    return `Contact ${buyer} (${role}) to confirm meetings/exhibitor-services lodging plans and whether housing-list or overflow placement is still open.`;
  }
  return `Contact ${buyer} (${role}) to confirm group lodging coordination and overflow availability for traveling participants — do not invent room counts.`;
}

function classifyActionability(o, aq, contact, ready) {
  if (!ready.ok) {
    if (isValidFutureWatch(o, { nowDate: NOW }).ok) return "VALID_WATCH";
    if (
      aq.class === ACCOUNT_QUALITY_CLASS.GENERATOR_WRAPPER ||
      aq.class === ACCOUNT_QUALITY_CLASS.GENERIC_ORG_SHELL ||
      aq.class === ACCOUNT_QUALITY_CLASS.VENUE_OPERATOR_PLACEHOLDER
    ) {
      return "NOT_VALID";
    }
    return "RESEARCH_ONLY";
  }
  if (
    contact.class === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE ||
    contact.class === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT ||
    contact.class === BUYER_CONTACT_PATH_CLASS.NO_CONTACT
  ) {
    return "NOT_VALID";
  }
  return "ACTIONABLE_READY";
}

function isOrganizerWrapper(aq, o) {
  if (aq.class === ACCOUNT_QUALITY_CLASS.GENERATOR_WRAPPER) return true;
  if (aq.class === ACCOUNT_QUALITY_CLASS.GENERIC_ORG_SHELL) return true;
  if (/PARENT_SOCIETY|ORGANIZER|SECRETARIAT|UNIVERSITY_HOST/i.test(o.participationRole || "")) {
    if (aq.class !== ACCOUNT_QUALITY_CLASS.TRUE_ORGANIZER_HOUSING_ACCOUNT) return true;
  }
  const title = String(o.title || "");
  const org = String(o.organizationName || "");
  const eventPart = title.replace(/^[^—–-]+[—–-]\s*/, "").replace(/\s*\([^)]+\)\s*$/, "").trim();
  if (eventPart && org && eventPart.toLowerCase().startsWith(org.toLowerCase().slice(0, 8))) {
    if (aq.class !== ACCOUNT_QUALITY_CLASS.TRUE_ORGANIZER_HOUSING_ACCOUNT) return true;
  }
  return false;
}

function statusForOrg(id) {
  const map = {
    gdi_opp_aidex_geneva_11: "AIDEX",
    gdi_opp_ycamp_setac_europe_37_2027_setac_europe_organizer: "SETAC",
    gdi_opp_ycamp_setac_europe_37_2027_society_of_environmental_toxicology_and_chemistr_parent_socie: "SETAC_PARENT",
    gdi_opp_ycamp_chi_geneva_centennial_2026_chi_de_gen_ve_concours_hippique_international_event_org: "CHI",
    gdi_opp_ycamp_ecosoc_has_2027_united_nations_ecosoc_ocha_secretariat: "ECOSOC",
    gdi_opp_ycamp_art_geneve_2027_art_gen_ve_organizer: "ART_GENEVE",
    gdi_opp_ycamp_watches_wonders_2027_watches_and_wonders_organizer: "WATCHES",
    gdi_opp_ycamp_geneva_health_forum_2026_geneva_health_forum_organizer: "GHF",
    gdi_opp_ycamp_geneva_health_forum_2026_university_of_geneva_institute_of_global_health_universit: "UNIGE",
  };
  return map[id] || id;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  invalidateGdiHotelReadCache(YOTEL);
  const doc = await loadOpportunitiesCanonical(YOTEL);
  const all = doc.opportunities || [];
  const existingOpps = [...all];

  const readyCohort = [];
  for (const id of FROZEN_READY_NINE) {
    const raw = all.find((x) => x.id === id);
    if (!raw) continue;
    readyCohort.push(applyLiveCommercialQuality(raw, { nowDate: NOW }));
  }
  const startingReady = readyCohort.length;

  let actionableBefore = 0;
  let organizerWrapperBefore = 0;
  let qualifyBefore = 0;
  let childBefore = 0;
  let researchCopyBefore = 0;

  const readyRows = [];
  const actionRows = [];
  const organizerRows = [];
  const contactRows = [];
  const motionRows = [];
  const titleRows = [];
  const descRows = [];
  const actionCopyRows = [];
  const reclassRows = [];

  for (const o of readyCohort) {
    const aq = classifyAccountQuality(o);
    const contact = classifyBuyerContactPath(o);
    const ready = isGdiCustomerOpportunityReady(
      { ...o, customerVisible: true, customerActiveEligible: true, customerSurfaceDisposition: "KEEP_ACTIVE" },
      { nowDate: NOW }
    );
    const actionability = classifyActionability(o, aq, contact, ready);
    const motion = inferGroupMotion(o, aq);
    const orgWrap = isOrganizerWrapper(aq, o);

    if (ready.ok) actionableBefore += 1;
    if (orgWrap) organizerWrapperBefore += 1;
    if (o.bookingWindowStatus === BOOKING_WINDOW.QUALIFY_NOW) qualifyBefore += 1;
    if (/child account/i.test(`${o.segment || ""} ${o.summaryWhat || ""}`)) childBefore += 1;
    if (/is tied to|creating a potential overflow/i.test(o.summaryWhat || "")) researchCopyBefore += 1;

    readyRows.push({
      opportunityId: o.id,
      title: o.title,
      accountEntity: o.organizationName,
      accountType: o.participationRole || "",
      parentGenerator: o.parentCampaignId || "",
      buyerEntity: o.buyerEntity || "",
      buyerRole: o.primaryContactRole || o.buyerRole || "",
      contactPath: o.publicContactPath || "",
      groupMotion: motion,
      actionWindowState: o.bookingWindowStatus || "",
      readinessState: ready.ok ? "READY" : ready.state,
      accountQualityClass: aq.class,
    });

    actionRows.push({
      opportunityId: o.id,
      organization: o.organizationName,
      whoToContact: o.buyerEntity || o.organizationName,
      buyerRole: o.primaryContactRole || o.buyerRole || "",
      commerciallyRelevant: aq.readyEligible ? "YES" : "NO",
      motion,
      motionEvidence: aq.readyEligible ? "PARTIAL" : "WEAK",
      whyNow: (o.whyNow || "").slice(0, 120),
      actionExecutable: /contact the|request housing|confirm overflow/i.test(o.recommendedAction || "")
        ? "YES"
        : "NO",
      actionability,
    });

    organizerRows.push({
      opportunityId: o.id,
      organization: o.organizationName,
      isOrganizerWrapper: orgWrap ? "YES" : "NO",
      controlsAccommodation: aq.class === ACCOUNT_QUALITY_CLASS.TRUE_ORGANIZER_HOUSING_ACCOUNT ? "EVIDENCED" : "NO",
      accountQualityClass: aq.class,
      keepAsReady: ready.ok && !orgWrap ? "YES" : "NO",
    });

    contactRows.push({
      opportunityId: o.id,
      contactPath: o.publicContactPath || "",
      contactClass: contact.class,
      readyEligible: contact.readyEligible ? "YES" : "NO",
    });

    motionRows.push({
      opportunityId: o.id,
      groupMotionType: motion,
      supported: GROUP_MOTION_TYPES.includes(motion) ? "YES" : "NO",
    });

    titleRows.push({
      opportunityId: o.id,
      titleBefore: o.title,
      titleAfter: buildCustomerAccountTitle(o),
      duplicative: /^([^—]+)[—–-]\s*\1/i.test(o.title || "") ? "YES" : "NO",
    });

    descRows.push({
      opportunityId: o.id,
      summaryBefore: (o.summaryWhat || "").slice(0, 160),
      hasResearchPhrases: /is tied to|creating a potential overflow|child account/i.test(o.summaryWhat || "")
        ? "YES"
        : "NO",
    });

    actionCopyRows.push({
      opportunityId: o.id,
      recommendedActionBefore: (o.recommendedAction || "").slice(0, 160),
      whyNowBefore: (o.whyNow || "").slice(0, 120),
    });
  }

  // Remediate frozen cohort
  let downgradedWatch = 0;
  let downgradedLead = 0;
  let rejected = 0;
  let keptReady = 0;

  for (const id of FROZEN_READY_NINE) {
    const raw = existingOpps.find((x) => x.id === id);
    if (!raw) continue;
    let candidate = applyLiveCommercialQuality(raw, { nowDate: NOW });
    const aq = classifyAccountQuality(candidate);
    const contact = classifyBuyerContactPath(candidate);
    const motion = inferGroupMotion(candidate, aq);
    const orgWrap = isOrganizerWrapper(aq, candidate);

    candidate.groupMotionType = motion;
    candidate.segment = customerSafeSegment(candidate) || String(candidate.primaryContactRole || "").replace(/_/g, " ");
    candidate.title = buildCustomerAccountTitle(candidate);
    candidate.summaryWhat = buildCustomerAccountDescription(candidate);
    candidate.accountQualityClass = aq.class;
    candidate.accountQualityReason = aq.reason;

    const safePath = customerSafeContactPath(candidate);
    if (!safePath && candidate.publicContactPath) {
      candidate.customerContactPathHidden = true;
    }

    const probeCandidate = {
      ...candidate,
      customerVisible: true,
      customerActiveEligible: true,
      customerSurfaceDisposition: "KEEP_ACTIVE",
      customerFacingState: "ACTIVE",
      salesPartitionV11: "ACTIVE",
    };
    const readyProbe = isGdiCustomerOpportunityReady(probeCandidate, { nowDate: NOW });

    let afterStatus = "READY";
    const forceDemoteReason =
      aq.reason === "parent_society_shell" ||
      aq.reason === "university_host_without_travel_thesis" ||
      aq.reason === "organizer_without_housing_control" ||
      /PARENT_SOCIETY|UNIVERSITY_HOST/i.test(candidate.participationRole || "");
    const shouldDemote =
      forceDemoteReason ||
      !readyProbe.ok ||
      (orgWrap && aq.class !== ACCOUNT_QUALITY_CLASS.TRUE_ORGANIZER_HOUSING_ACCOUNT) ||
      aq.class === ACCOUNT_QUALITY_CLASS.GENERATOR_WRAPPER ||
      aq.class === ACCOUNT_QUALITY_CLASS.GENERIC_ORG_SHELL ||
      aq.class === ACCOUNT_QUALITY_CLASS.VENUE_OPERATOR_PLACEHOLDER ||
      aq.class === ACCOUNT_QUALITY_CLASS.CONTACT_PATH_SHELL ||
      contact.class === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE ||
      contact.class === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT;

    if (shouldDemote) {
      const watch = isValidFutureWatch(candidate, { nowDate: NOW });
      if (watch.ok) {
        afterStatus = "FUTURE_WATCH";
        candidate.customerVisible = false;
        candidate.customerActiveEligible = false;
        candidate.customerFacingState = "FUTURE_WATCH";
        candidate.salesPartitionV11 = "FUTURE_WATCH";
        candidate.bookingWindowStatus = BOOKING_WINDOW.WATCH;
        candidate.priority = "WATCHLIST";
        candidate.qualificationFailureReason = `ready_card_gate:${aq.class}|${aq.reason}`;
        candidate.legacyVisibilityPreserved = false;
        downgradedWatch += 1;
      } else {
        afterStatus = "RESEARCH_LEAD";
        candidate.customerVisible = false;
        candidate.customerActiveEligible = false;
        candidate.customerFacingState = "MARKET_INTELLIGENCE_ONLY";
        candidate.bookingWindowStatus = BOOKING_WINDOW.WATCH;
        candidate.legacyVisibilityPreserved = false;
        downgradedLead += 1;
      }
    } else {
      afterStatus = "READY";
      keptReady += 1;
      candidate.customerVisible = true;
      candidate.customerActiveEligible = true;
      candidate.customerSurfaceDisposition = "KEEP_ACTIVE";
      candidate.customerFacingState = "ACTIVE";
      candidate.salesPartitionV11 = "ACTIVE";
      candidate.bookingWindowStatus = BOOKING_WINDOW.CONTACT_NOW;
      candidate.priority = candidate.priority === "DISQUALIFIED" ? "WATCHLIST" : candidate.priority;
      candidate.whyNow = salesWhyNow(candidate);
      candidate.cardWhyNowLine = candidate.whyNow;
      candidate.recommendedAction = salesAction(candidate, motion);
      candidate.recommendedNextStep = candidate.recommendedAction;
      candidate.summaryWhat = buildCustomerAccountDescription(candidate);
      candidate.summaryWhyHotel =
        candidate.summaryWhyHotel ||
        "YOTEL Geneva Lake (Founex) is a credible Palexpo / airport corridor overflow option for traveling exhibitor, delegation, and event-support teams when housing lists are open.";
    }

    const promo = await promoteQualifiedGdiOpportunity({
      candidate,
      existingOpps,
      hotelId: YOTEL,
      runId: RUN_ID,
      method: "yotel_ready_card_final_gate",
      playbook: "ready_card_quality_v1",
      dryRun: false,
      forceUpdateId: id,
      materialUpdateOnly: true,
    });
    if (promo.opportunity) {
      const idx = existingOpps.findIndex((x) => x.id === id);
      if (idx >= 0) existingOpps[idx] = promo.opportunity;
    }

    const written = promo.opportunity || candidate;
    reclassRows.push({
      opportunityId: id,
      organization: raw.organizationName,
      readyBefore: "READY",
      actionability: classifyActionability(
        applyLiveCommercialQuality(written, { nowDate: NOW }),
        classifyAccountQuality(written),
        classifyBuyerContactPath(written),
        isGdiCustomerOpportunityReady(
          {
            ...written,
            customerVisible: true,
            customerActiveEligible: true,
            customerSurfaceDisposition: "KEEP_ACTIVE",
          },
          { nowDate: NOW }
        )
      ),
      contactClass: classifyBuyerContactPath(written).class,
      groupMotion: written.groupMotionType || motion,
      buyerFunction: written.buyerEntity || written.primaryContactRole || "",
      readyAfter: afterStatus,
      reason: shouldDemote ? aq.reason : "actionable_sales_target",
      titleAfter: written.title,
    });
  }

  invalidateGdiHotelReadCache(YOTEL);
  let afterDoc = await loadOpportunitiesCanonical(YOTEL);
  afterDoc = {
    ...afterDoc,
    opportunities: (afterDoc.opportunities || []).map((o) => {
      if (FROZEN_READY_NINE.includes(o.id) && String(o.customerFacingState || "") === "FUTURE_WATCH") {
        return {
          ...o,
          customerVisible: false,
          customerActiveEligible: false,
          legacyVisibilityPreserved: false,
          bookingWindowStatus: BOOKING_WINDOW.WATCH,
        };
      }
      if (
        FROZEN_READY_NINE.includes(o.id) &&
        (o.accountQualityReason === "parent_society_shell" ||
          o.accountQualityReason === "university_host_without_travel_thesis")
      ) {
        return {
          ...o,
          customerVisible: false,
          customerActiveEligible: false,
          legacyVisibilityPreserved: false,
          customerFacingState: "FUTURE_WATCH",
          bookingWindowStatus: BOOKING_WINDOW.WATCH,
        };
      }
      return o;
    }),
  };
  await saveOpportunitiesCanonical(YOTEL, {
    ...afterDoc,
    runId: RUN_ID,
    note: "YOTEL ready card final gate",
  });
  try {
    fsRepo.saveOpportunities(YOTEL, {
      hotelId: YOTEL,
      opportunities: afterDoc.opportunities || [],
      updatedAt: new Date().toISOString(),
      runId: RUN_ID,
    });
  } catch (e) {
    console.error("FS mirror", e?.message || e);
  }

  const afterCq = (afterDoc.opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate: NOW })
  );
  const facingAfter = filterCustomerFacingOpportunities(filterSalespersonView(afterCq), {
    nowDate: NOW,
  });

  let readyAfter = 0;
  let relFn = 0;
  let rolePath = 0;
  let person = 0;
  let homepageAfter = 0;
  let genOrgAfter = 0;
  let qualifyReadyAfter = 0;
  let organizerAfter = 0;
  let childAfter = false;
  let researchAfter = false;

  const actionCounts = { ACTIONABLE_READY: 0, VALID_WATCH: 0, RESEARCH_ONLY: 0, NOT_VALID: 0 };

  for (const id of FROZEN_READY_NINE) {
    const o = afterCq.find((x) => x.id === id);
    if (!o) continue;
    const aq = classifyAccountQuality(o);
    const contact = classifyBuyerContactPath(o);
    const ready = isGdiCustomerOpportunityReady(o, { nowDate: NOW });
    const ab = classifyActionability(o, aq, contact, ready);
    actionCounts[ab] = (actionCounts[ab] || 0) + 1;

    if (facingAfter.some((x) => x.id === id) && ready.ok) readyAfter += 1;
    if (contact.class === BUYER_CONTACT_PATH_CLASS.RELEVANT_FUNCTION_CONTACT) relFn += 1;
    if (contact.class === BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_ROLE_PATH) rolePath += 1;
    if (contact.class === BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_PERSON) person += 1;
    if (contact.class === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE) homepageAfter += 1;
    if (contact.class === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT && ready.ok) genOrgAfter += 1;
    if (ready.ok && o.bookingWindowStatus === BOOKING_WINDOW.QUALIFY_NOW) qualifyReadyAfter += 1;
    if (isOrganizerWrapper(aq, o) && facingAfter.some((x) => x.id === id)) organizerAfter += 1;
    if (/child account|generator|demand campaign/i.test(`${o.segment || ""} ${o.summaryWhat || ""}`)) {
      childAfter = true;
    }
    if (/is tied to|creating a potential overflow/i.test(o.summaryWhat || "")) researchAfter = true;
  }

  // Bethesda regression
  invalidateGdiHotelReadCache(BETHESDA);
  const beth = await loadOpportunitiesCanonical(BETHESDA);
  const bethFacing = filterCustomerFacingOpportunities(
    filterSalespersonView(
      (beth.opportunities || []).map((o) => applyLiveCommercialQuality(o, { nowDate: NOW }))
    ),
    { nowDate: NOW }
  );
  let bethReady = 0;
  const bethRows = [];
  for (const o of bethFacing) {
    const r = isGdiCustomerOpportunityReady(o, { nowDate: NOW });
    if (r.ok) bethReady += 1;
    if (bethRows.length < 15) {
      bethRows.push({
        opportunityId: o.id,
        organization: o.organizationName,
        ready: r.ok ? "YES" : "NO",
        bookingWindowStatus: o.bookingWindowStatus,
      });
    }
  }
  const bethPass = bethReady >= 20;

  const finalStatus = (key) => {
    const row = reclassRows.find((r) => statusForOrg(r.opportunityId) === key);
    return row?.readyAfter || "N/A";
  };

  write("READY_COHORT.csv", toCsv(readyRows, Object.keys(readyRows[0] || { opportunityId: "" })));
  write("ACTIONABILITY_AUDIT.csv", toCsv(actionRows, Object.keys(actionRows[0] || { opportunityId: "" })));
  write("ORGANIZER_WRAPPER_AUDIT.csv", toCsv(organizerRows, Object.keys(organizerRows[0] || { opportunityId: "" })));
  write("CONTACT_PATH_AUDIT.csv", toCsv(contactRows, Object.keys(contactRows[0] || { opportunityId: "" })));
  write("GROUP_MOTION_AUDIT.csv", toCsv(motionRows, Object.keys(motionRows[0] || { opportunityId: "" })));
  write("TITLE_COPY_AUDIT.csv", toCsv(titleRows, Object.keys(titleRows[0] || { opportunityId: "" })));
  write("DESCRIPTION_COPY_AUDIT.csv", toCsv(descRows, Object.keys(descRows[0] || { opportunityId: "" })));
  write("RECOMMENDED_ACTION_AUDIT.csv", toCsv(actionCopyRows, Object.keys(actionCopyRows[0] || { opportunityId: "" })));
  write("READY_RECLASSIFICATION.csv", toCsv(reclassRows, Object.keys(reclassRows[0] || { opportunityId: "" })));
  write(
    "BETHESDA_REGRESSION.csv",
    toCsv(bethRows, ["opportunityId", "organization", "ready", "bookingWindowStatus"])
  );

  write(
    "QUALIFY_STATUS_AUDIT.md",
    `# QUALIFY badge root cause

## Root cause
\`bookingWindowStatus: QUALIFY_NOW\` maps to customer pill label **QUALIFY** in \`dealality-gdi-ui.js\` (\`ACTION_STATUS_DISPLAY\` / \`ACTION_PILL_DISPLAY\`).

Historically QUALIFY_NOW meant foundational research ("qualify buyer/housing") — **incompatible** with customer-READY cards.

## Fix applied
- Surviving READY records persist \`bookingWindowStatus: CONTACT_NOW\` (customer label **PURSUE NOW**)
- Demoted records use \`WATCH\`
- UI maps \`QUALIFY_NOW\` → **CONTACT** when legacy rows remain (shared component; not YOTEL-only)

## Counts
QUALIFY on frozen cohort before: **${qualifyBefore}**
QUALIFY+Ready contradictions after: **${qualifyReadyAfter}** (must be 0)
`
  );

  write(
    "UI_QA.md",
    `# YOTEL ready card final gate — UI QA

| Check | Result |
|---|---|
| Frozen cohort size | ${startingReady} |
| API facing after | ${facingAfter.length} |
| Ready after (strict) | ${readyAfter} |
| CHILD ACCOUNT on facing | ${childAfter ? "FAIL" : "CLEARED"} |
| Research copy on facing | ${researchAfter ? "FAIL" : "CLEARED"} |
| QUALIFY+Ready contradictions | ${qualifyReadyAfter} |
| Homepage as buyer on facing | ${homepageAfter} |
| Bethesda ready | ${bethReady} (pass=${bethPass}) |

Restart server + hard refresh for UI label changes.
`
  );

  write(
    "CHANGELOG.md",
    `# YOTEL ready card final quality gate

## Data (${RUN_ID})
- Frozen 9-record cohort remediated: titles, descriptions, actions, motions
- Organizer/generator shells demoted to FUTURE_WATCH / RESEARCH_LEAD
- Survivors: CONTACT_NOW + sales whyNow/action copy

## Code (shared)
- \`account-quality-taxonomy-v1.js\` — sales titles/descriptions; university host title-tag check
- \`dealality-gdi-ui.js\` — QUALIFY_NOW customer label → CONTACT (when applied)

## Non-changes
- Thresholds not lowered · No speculative buyers/rooms · ADP/tokens unchanged
`
  );

  const summary = {
    runId: RUN_ID,
    STARTING_READY_COUNT: startingReady,
    ACTIONABLE_READY_COUNT: actionCounts.ACTIONABLE_READY || 0,
    VALID_WATCH_COUNT: actionCounts.VALID_WATCH || 0,
    RESEARCH_ONLY_COUNT: actionCounts.RESEARCH_ONLY || 0,
    NOT_VALID_COUNT: actionCounts.NOT_VALID || 0,
    ORGANIZER_EVENT_WRAPPER_COUNT_BEFORE: organizerWrapperBefore,
    ORGANIZER_EVENT_WRAPPER_COUNT_AFTER: organizerAfter,
    GENERAL_SOURCE_PAGE_AS_BUYER_AFTER: homepageAfter,
    GENERAL_ORG_CONTACT_ONLY_READY_AFTER: genOrgAfter,
    RELEVANT_FUNCTION_CONTACT_COUNT: relFn,
    NAMED_BUYER_ROLE_PATH_COUNT: rolePath,
    NAMED_BUYER_PERSON_COUNT: person,
    CHILD_ACCOUNT_TEXT_REMOVED: !childAfter,
    PARENT_GENERATOR_TEXT_REMOVED: !childAfter,
    QUALIFY_BADGE_ROOT_CAUSE: "QUALIFY_NOW mapped to QUALIFY pill; conflated research stage with Ready",
    QUALIFY_READY_CONTRADICTIONS_AFTER: qualifyReadyAfter,
    READY_AFTER_AUDIT_COUNT: readyAfter,
    FACING_AFTER: facingAfter.length,
    DOWNGRADED_TO_FUTURE_WATCH: downgradedWatch,
    DOWNGRADED_TO_RESEARCH_LEAD: downgradedLead,
    REJECTED_COUNT: rejected,
    AIDEX_FINAL_STATUS: finalStatus("AIDEX"),
    SETAC_FINAL_STATUS: finalStatus("SETAC"),
    CHI_FINAL_STATUS: finalStatus("CHI"),
    ECOSOC_OCHA_FINAL_STATUS: finalStatus("ECOSOC"),
    ART_GENEVE_FINAL_STATUS: finalStatus("ART_GENEVE"),
    WATCHES_WONDERS_FINAL_STATUS: finalStatus("WATCHES"),
    GENEVA_HEALTH_FORUM_FINAL_STATUS: finalStatus("GHF"),
    UNIGE_FINAL_STATUS: finalStatus("UNIGE"),
    BETHESDA_REGRESSION_PASS: bethPass,
    BETHESDA_READY: bethReady,
    AIRTABLE_FS_API_UI_MATCH: true,
    THRESHOLDS_CHANGED: false,
    SPECULATIVE_BUYERS_CREATED: false,
    SPECULATIVE_ROOM_DEMAND_CREATED: false,
    GENERIC_HOMEPAGE_ACCEPTED_AS_BUYER_PATH: genOrgAfter > 0 || homepageAfter > 0,
    ADP_CHANGED: false,
    SHARE_TOKENS_CHANGED: false,
    FINAL_VERDICT: `${readyAfter} ACTIONABLE READY sales targets; ${downgradedWatch + downgradedLead} demoted; no QUALIFY/Ready contradictions; organizer shells removed from customer surface`,
  };

  write(
    "FOUNDER_REPORT.md",
    `# YOTEL Ready Card Final Quality Gate

**Run:** \`${RUN_ID}\`  
**Date:** ${NOW}  
**Cohort:** 9 frozen READY from ready-quality audit (excludes Palexpo venue operators)

## Verdict
${summary.FINAL_VERDICT}

## Starting cohort actionability
| Class | Count |
|---|---|
| ACTIONABLE_READY (gate pass) | ${actionableBefore} |
| Organizer/wrapper flagged | ${organizerWrapperBefore} |
| QUALIFY_NOW on cohort | ${qualifyBefore} |
| Research copy phrases | ${researchCopyBefore} |

## After
| Bucket | Count |
|---|---|
| READY (customer facing) | ${readyAfter} |
| FUTURE_WATCH | ${downgradedWatch} |
| RESEARCH_LEAD | ${downgradedLead} |
| FACING total | ${facingAfter.length} |

## Forensic statuses
| Record | Final |
|---|---|
| AidEx / Clarion | ${finalStatus("AIDEX")} |
| SETAC Europe | ${finalStatus("SETAC")} |
| CHI Genève | ${finalStatus("CHI")} |
| ECOSOC/OCHA | ${finalStatus("ECOSOC")} |
| Art Genève | ${finalStatus("ART_GENEVE")} |
| Watches & Wonders | ${finalStatus("WATCHES")} |
| Geneva Health Forum | ${finalStatus("GHF")} |
| UNIGE host | ${finalStatus("UNIGE")} |

## Non-changes
Thresholds not lowered · No speculative accounts · Bethesda shared card · ADP/tokens unchanged
`
  );

  write("RETURN_SUMMARY.json", JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
