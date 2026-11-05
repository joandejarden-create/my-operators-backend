/**
 * Read-only GDI research preview.
 * A preview shows a frozen set of existing research records.
 * It does not change publication, customerVisible, or commercial readiness.
 */
import fs from "node:fs";
import path from "node:path";
import { classifyBuyerContactPath, hasLodgingContactEvidence } from "./buyer-contact-path-taxonomy-v1.js";
import { toOpportunityListDto } from "./opportunity-list-dto.js";

export const RESEARCH_PREVIEW_SCOPE = "research_preview";
export const RESEARCH_PREVIEW_TITLE = "GDI Research Preview — Preliminary Opportunities";
export const RESEARCH_PREVIEW_DISCLAIMER =
  "These opportunities are preliminary research leads. Buyer contact routes, hotel fit, accommodation demand and commercial readiness have not been fully verified. They should not be treated as confirmed bookings or active RFPs.";

export const YOTEL_RESEARCH_PREVIEW_TITLE = "GDI Research Preview | YOTEL Geneva Lake";
export const YOTEL_RESEARCH_PREVIEW_DISCLAIMER =
  "These opportunities are preliminary research leads. Buyer contact routes, group accommodation requirements, timing and commercial readiness have not been fully verified. They do not represent confirmed bookings, active RFPs or verified purchasing intent.";

export const YOTEL_ORIGINAL_REPORT_12 = Object.freeze({
  hotelId: "recrPQcZg7SFARRb2",
  manifestId: "yotel-geneva-lake-original-report-12",
  title: YOTEL_RESEARCH_PREVIEW_TITLE,
  disclaimer: YOTEL_RESEARCH_PREVIEW_DISCLAIMER,
  opportunityIds: Object.freeze([
    "gdi_opp_ycamp_watches_wonders_2027_porsche_design_exhibitor",
    "gdi_opp_ycamp_watches_wonders_2027_grand_seiko_exhibitor",
    "gdi_opp_aifg_2027_access_partnership_session_partner_2026",
    "gdi_opp_aifg_2027_ministry_of_internal_affairs_and_communications__gold_sponsor_2026",
    "gdi_opp_ycamp_chi_geneva_centennial_2026_chi_de_gen_ve_concours_hippique_international_event_org",
    "gdi_opp_ycamp_art_geneve_2027_lee_bae_exhibitor",
    "gdi_opp_ycamp_art_geneve_2027_tang_contemporary_art_exhibitor",
    "gdi_opp_ycamp_geneva_health_forum_2026_european_federation_of_allergy_and_airways_disea_speaker_",
    "gdi_opp_ycamp_watches_wonders_2027_nomos_glash_tte_exhibitor",
    "gdi_opp_ycamp_watches_wonders_2027_sinn_spezialuhren_exhibitor",
    "gdi_opp_aifg_2027_ministry_of_science_and_ict_of_the_republic_of_k_gold_sponsor_2026",
    "gdi_opp_aifg_2027_hp_inc_networking_partner_2026",
  ]),
});

export const MAYFLOWER_RESEARCH_PREVIEW_TITLE = "GDI Research Preview | The Mayflower Hotel";
export const MAYFLOWER_RESEARCH_PREVIEW_DISCLAIMER =
  "These are preliminary group-demand research opportunities. Some buyer contacts, lodging requirements, timing and commercial qualifications remain unverified. Listings do not indicate confirmed bookings, active RFPs or current purchasing intent.";

export const MAYFLOWER_ORIGINAL_REPORT_38 = Object.freeze({
  hotelId: "rec0eTEnCqlsGJJt9",
  manifestId: "mayflower-postdeploy-facing-38",
  title: MAYFLOWER_RESEARCH_PREVIEW_TITLE,
  disclaimer: MAYFLOWER_RESEARCH_PREVIEW_DISCLAIMER,
  preserveStoredResearch: true,
  source: "reports/gdi-use-case-qualification-integrity/release/postdeploy-verify.json hotelSnapshots.rec0eTEnCqlsGJJt9.facingIds",
  opportunityIds: Object.freeze([
    "gdi_lmc_9a30c08278a92953",
    "gdi_sc_43a6be41ced811a0",
    "gdi_sc_0fe98055e8df9f75",
    "gdi_lmc_ceab66971a48a9f8",
    "gdi_sc_056f479083d48dc1",
    "gdi_opp_prospect_mv8fnmju_53cfad28",
    "gdi_lmc_5b4d51be7726617c",
    "gdi_lmc_860cde704de88d7c",
    "gdi_sc_cde8141b891e922d",
    "gdi_sc_788dea695ea47723",
    "gdi_pe_6e20211edf688737",
    "gdi_pe_0d1c606f41f7847b",
    "gdi_opp_prospect_mv8fnmjw_b8f2b42f",
    "gdi_sc_f917b2850ebd5142",
    "gdi_opp_prospect_mv8fnmju_78ea97be",
    "gdi_sc_c9349cbe45c8e52d",
    "gdi_opp_prospect_mv8fnmk0_72500455",
    "gdi_opp_prospect_mv8fnmjt_565f3694",
    "gdi_pe_b4de27f6743c03c5",
    "gdi_opp_prospect_mv8fnmk2_1ced5dd8",
    "gdi_lmc_00cd663a9f282697",
    "gdi_opp_prospect_mv8fnmjz_49a8adab",
    "gdi_lmc_d670fde177ad418b",
    "gdi_opp_prospect_mv8fnmjz_4e1dc764",
    "gdi_opp_prospect_mv8fnmk1_121a66d4",
    "gdi_opp_prospect_mv8fnmjv_68ed714c",
    "gdi_lmc_6a144810692f8a23",
    "gdi_sc_dd73bb69885b426c",
    "gdi_pe_de84e0fdf4d690e3",
    "gdi_opp_prospect_mv8fnmjz_832ce0cf",
    "gdi_lmc_82918469d12cfbc1",
    "gdi_pe_8601eb15f47ad808",
    "gdi_lmc_8bf1156a3b6bc8dc",
    "gdi_pe_1e87cdb00b80fc5f",
    "gdi_lmc_6268043898e92fc3",
    "gdi_lmc_0ccfca01a22ed264",
    "gdi_lmc_5f7f334ef133fdb4",
    "gdi_sc_9a57aa6b68136b53",
  ]),
});

const MANIFESTS = Object.freeze({
  [YOTEL_ORIGINAL_REPORT_12.hotelId]: YOTEL_ORIGINAL_REPORT_12,
  [MAYFLOWER_ORIGINAL_REPORT_38.hotelId]: MAYFLOWER_ORIGINAL_REPORT_38,
});

export function researchPreviewManifestForHotel(hotelId) {
  return MANIFESTS[String(hotelId || "").trim()] || null;
}

export function researchPreviewPath(manifestId, repoRoot = process.cwd()) {
  return path.join(repoRoot, "data/group-demand-intelligence/research-previews", `${manifestId}.json`);
}

export function isResearchPreviewClaim(claims = {}) {
  return claims?.accessScope === RESEARCH_PREVIEW_SCOPE && Boolean(claims?.previewManifestId);
}

function text(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function sourceUrls(row) {
  const urls = [];
  for (const item of row.sources || []) {
    const url = typeof item === "string" ? item : item?.url;
    if (url && /^https?:\/\//i.test(url)) urls.push(url);
  }
  if (row.officialSource && /^https?:\/\//i.test(row.officialSource)) urls.push(row.officialSource);
  if (row.discoverySource && /^https?:\/\//i.test(row.discoverySource)) urls.push(row.discoverySource);
  return [...new Set(urls)];
}

function withoutBuyingClaims(value) {
  return text(value)
    .replace(/[^.]*\b(confirmed booking|active rfp|room block confirmed|open booking|seeking hotel)\b[^.]*\.?/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

function storedContact(row = {}) {
  const contact = row.primaryContact || {};
  return {
    name: text(contact.name || row.primaryContactName),
    email: text(contact.email || row.primaryContactEmail || row.contactEmail),
    phone: text(contact.phone || row.primaryContactPhone),
    url: text(row.publicContactPath || row.organizationContactUrl || row.officialContactPath || row.contactUrl || contact.organizationContactRoute),
  };
}

function researchClassification(row = {}) {
  const raw = [row.demandMechanism, row.commercialMechanism, row.prospectReviewSection, row.mechanism]
    .filter(Boolean)
    .join(" ");
  if (/MONITOR/i.test(raw)) return "Monitoring";
  return "Exploratory";
}

export function presentResearchPreviewOpportunity(row = {}, options = {}) {
  const contact = classifyBuyerContactPath(row);
  const lodging = hasLodgingContactEvidence(row);
  const verifiedRoute = contact.readyEligible === true;
  const clone = {
    ...row,
    customerVisible: false,
    commerciallyReady: false,
    customerReady: false,
    ready: false,
    priority: "WATCHLIST",
    bookingWindowStatus: "RESEARCH_FURTHER",
    openBookingClaimed: false,
    publicContactPath: verifiedRoute ? contact.contactUrl : "",
    organizationContactUrl: verifiedRoute ? contact.contactUrl : "",
    officialContactPath: verifiedRoute ? contact.contactUrl : "",
    primaryContactEmail: verifiedRoute ? row.primaryContactEmail : "",
    contactEmail: verifiedRoute ? row.contactEmail : "",
    primaryContactPhone: verifiedRoute ? row.primaryContactPhone : "",
  };
  const dto = toOpportunityListDto(clone);
  if (options.preserveStoredResearch === true) {
    const organization = row.organizationName || dto.organizationName || "This organization";
    const title = row.title || dto.title || "Stored research opportunity";
    const sources = sourceUrls(row);
    const stored = storedContact(row);
    const verifiedRoute = contact.readyEligible === true && stored.url;
    const classification = researchClassification(row);
    const storedSummary = withoutBuyingClaims(row.summary || row.summaryWhat || row.establishedFacts || "");
    const evidence = [
      storedSummary || `${organization} is stored as ${classification.toLowerCase()} research for ${title}.`,
      sources.length ? `Source references: ${sources.join(" ")}` : "No source URL is stored on this record.",
      stored.email || stored.phone || stored.url
        ? `Stored contact: ${[stored.name, stored.email, stored.phone, stored.url].filter(Boolean).join(" · ")}.${verifiedRoute ? "" : " This stored contact is not a verified organizer route."}`
        : "No contact is stored on this record.",
    ].join(" ");
    return {
      ...dto,
      id: row.id || row.opportunityId,
      organizationName: organization,
      title,
      displayTitle: title,
      commercialCategory: classification,
      opportunityTypeLabel: classification,
      summaryWhat: evidence,
      summaryWhyMatters: evidence,
      evidenceNote: evidence,
      evidenceStatusLabel: classification,
      whyNow: withoutBuyingClaims(row.whyNow || row.eventDateDisplay || "") || "Original timing is unchanged. No additional timing was added.",
      primaryContact: stored.email || stored.phone || stored.name ? { name: stored.name || null, email: stored.email || null, phone: stored.phone || null } : null,
      publicContactPath: stored.url || null,
      verifiedContactRoute: verifiedRoute ? stored.url : null,
      sourceReferences: sources,
      readinessState: "NOT_VERIFIED",
      customerFacingState: null,
      gdiMaturityState: null,
      prospectCustomerLabel: classification,
      prospectCommercialLabel: classification,
      commerciallyReady: false,
      customerReady: false,
      customerVisible: false,
      openBookingClaimed: false,
      canStartPursuit: false,
      researchPreview: true,
      preliminary: true,
      priority: "WATCHLIST",
      bookingWindowStatus: "RESEARCH_FURTHER",
      recommendedNextStep: "Review the stored evidence before any sales contact.",
      hotelFitScore: null,
      modeledRoomsMin: null,
      modeledRoomsMax: null,
      potentialRoomNights: null,
    };
  }
  const organization = row.organizationName || dto.organizationName || "This organization";
  const title = row.title || dto.title || "the stored event";
  const roleMatch = String(title).match(/\(([^)]+)\)\s*$/);
  const eventRole = roleMatch ? roleMatch[1] : "recorded event association";
  const timing = [row.eventStartDate, row.eventEndDate].filter(Boolean).join(" – ")
    || withoutBuyingClaims(row.eventDateDisplay || "");
  const sources = sourceUrls(row);
  const missingContact = verifiedRoute ? null : "No verified organizer contact route is stored.";
  const missingHousing = lodging ? null : "No verified accommodation or housing evidence is stored.";
  const nextStep = "Find an official meetings or housing page for this organization before any sales contact.";
  const evidence = [
    `${organization} is stored with the event association “${title}”.`,
    timing ? `Stored timing: ${timing}.` : "No event date is stored on this record.",
    missingContact,
    missingHousing,
    sources.length ? `Source references: ${sources.join(" ")}` : "No source URL is stored on this record.",
  ].filter(Boolean).join(" ");
  return {
    ...dto,
    id: row.id || row.opportunityId,
    organizationName: organization,
    title,
    displayTitle: title,
    opportunityTypeLabel: eventRole,
    commercialCategory: eventRole,
    summaryWhat: evidence,
    summaryWhyMatters: evidence,
    whyNow: timing || "No event date is stored on this record.",
    hotelOpportunityThesis: "",
    summaryWhyHotel: "",
    fitExplanation: "",
    recommendedAction: "",
    modeledRoomsMin: null,
    modeledRoomsMax: null,
    modeledDemandDisclaimer: missingHousing,
    potentialRoomNights: null,
    hotelFitScore: null,
    hotelFitRationale: missingHousing,
    priority: "WATCHLIST",
    bookingWindowStatus: "RESEARCH_FURTHER",
    readinessState: "NOT_VERIFIED",
    customerFacingState: null,
    gdiMaturityState: null,
    prospectCustomerLabel: "Preliminary research",
    prospectCommercialLabel: "Preliminary research",
    commerciallyReady: false,
    customerReady: false,
    customerVisible: false,
    openBookingClaimed: false,
    canStartPursuit: false,
    primaryContact: null,
    publicContactPath: verifiedRoute ? contact.contactUrl : null,
    contactPathClass: null,
    whoPathClass: null,
    buyerRole: eventRole,
    researchPreview: true,
    preliminary: true,
    evidenceNote: evidence,
    evidenceStatusLabel: [missingContact, missingHousing].filter(Boolean).join(" "),
    timingNote: timing || "No event date is stored on this record.",
    verifiedContactRoute: verifiedRoute ? contact.contactUrl : null,
    missingContact,
    missingHousing,
    sourceReferences: sources,
    nextStep,
    recommendedNextStep: nextStep,
  };
}

export function writeResearchPreviewSnapshot(rows = [], manifest, repoRoot = process.cwd()) {
  const byId = new Map(rows.map((row) => [row.id || row.opportunityId, row]));
  const opportunities = manifest.opportunityIds.map((id) => {
    const row = byId.get(id);
    if (!row) throw new Error(`preview source missing: ${id}`);
    return presentResearchPreviewOpportunity(row, { preserveStoredResearch: manifest.preserveStoredResearch === true });
  });
  const doc = {
    manifestId: manifest.manifestId,
    hotelId: manifest.hotelId,
    title: manifest.title || RESEARCH_PREVIEW_TITLE,
    disclaimer: manifest.disclaimer || RESEARCH_PREVIEW_DISCLAIMER,
    commerciallyReady: false,
    customerReady: false,
    publicationStatus: "not_published",
    opportunityIds: [...manifest.opportunityIds],
    opportunities,
    frozenAt: new Date().toISOString(),
  };
  const file = researchPreviewPath(manifest.manifestId, repoRoot);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(doc, null, 2)}\n`);
  return doc;
}

export function loadResearchPreviewSnapshot(manifestId, repoRoot = process.cwd()) {
  const file = researchPreviewPath(manifestId, repoRoot);
  if (!fs.existsSync(file)) return null;
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

export function researchPreviewForClaim(claims = {}, repoRoot = process.cwd()) {
  if (!isResearchPreviewClaim(claims)) return null;
  const doc = loadResearchPreviewSnapshot(claims.previewManifestId, repoRoot);
  if (!doc || doc.hotelId !== claims.hotelId) return null;
  return doc;
}
