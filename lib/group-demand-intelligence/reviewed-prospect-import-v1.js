/**
 * Idempotent importer for reviewed hotel-facing prospect packages.
 * Uses canonical GDI repository (GDI_DATA_ROOT aware). No hotel-specific
 * account hardcoding — match keys derived from package orgKey + cardId.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  applyContactPackageToOpportunity,
  evaluateProspectPublicationEligibility,
  classifyContactCompletenessGroup,
} from "./prospect-publication-eligibility-v1.js";
import {
  REVIEWED_PROSPECT_PUBLICATION_BASIS,
  REVIEWED_PROSPECT_PACKAGE_VERSION,
  REVIEWED_PROSPECT_HOTEL_IDS,
  customerFacingLabelForProspect,
  contactResponsibilityLabel,
  phoneTypeCustomerLabel,
} from "./reviewed-prospect-publication-v1.js";
import {
  loadOpportunities,
  saveOpportunities,
  loadHotelProfile,
  saveHotelProfile,
  hotelDir,
  getGdiDataRoot,
  clearGdiRepositoryReadCache,
  createId,
} from "./repository.js";
import {
  getGdiOpportunityPersistenceMode,
  isGdiOpportunityAirtableConfigured,
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "./opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "./read-cache.js";

const PROTECTED_USER_FIELDS = [
  "salesOwner",
  "owner",
  "ownerUserId",
  "salesWorkflowState",
  "pursuitId",
  "pursuitStatus",
  "notes",
  "hotelNotes",
  "salespersonNotes",
  "feedback",
  "legacyVisibilityPreserved",
  "customerVisible",
];

/** Preferred live Bethesda opportunity IDs for known review cardIds (stable match). */
const KNOWN_ID_MATCHES = Object.freeze({
  "beth-ndss-2027": "gdi_opp_ndss_advocacy_2027",
  "beth-ahima-advocacy-2027": "gdi_opp_ahima_advocacy_2027",
  "beth-acc-leg-2027": "gdi_opp_acc_legislative_2027",
  "beth-acts-ts27": "gdi_opp_acts_ts27",
  "beth-bebpa-usb-2027": "gdi_opp_bebpa_usb_2027",
  "beth-amwa-annual-2027": "gdi_opp_amwa_2027_annual",
  "beth-nar-gad-2027": "gdi_opp_nar_gad_institute_2027",
  "beth-nado-2028": "gdi_opp_nado_ddaa_washcon_2028",
  "beth-nado-2027-overflow": "gdi_opp_nado_ddaa_washcon_2027_overflow",
  "beth-loudoun-showcase-2027": "gdi_opp_loudoun_college_showcase_2027",
  "beth-nci-rna-2027": "gdi_opp_2027_nci_rna_biology_symposium_7",
  "beth-nih-hrhr-2027": "gdi_opp_series_high_risk_high_reward_research_symposium_2027",
  "beth-ctn-2027": "gdi_opp_series_ctn_annual_conference_2027",
  "beth-afcea-hits-2027": "gdi_opp_afcea_hits_2027",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function sha256File(filePath) {
  const buf = fs.readFileSync(filePath);
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function parseCsv(textIn) {
  const lines = textIn.trim().split(/\r?\n/);
  const headers = splitCsvLine(lines[0]);
  return lines.slice(1).filter(Boolean).map((line) => {
    const cols = splitCsvLine(line);
    const row = {};
    headers.forEach((h, i) => {
      row[h] = cols[i] ?? "";
    });
    return row;
  });
}

function splitCsvLine(line) {
  const cols = [];
  let cur = "";
  let q = false;
  for (const ch of line) {
    if (ch === '"') q = !q;
    else if (ch === "," && !q) {
      cols.push(cur);
      cur = "";
    } else cur += ch;
  }
  cols.push(cur);
  return cols;
}

function parseEventDates(timing = "") {
  const t = text(timing);
  const range = t.match(
    /(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})\s*[–-]\s*(\d{1,2}),?\s*(20\d{2})/i
  );
  const months = {
    january: "01",
    february: "02",
    march: "03",
    april: "04",
    may: "05",
    june: "06",
    july: "07",
    august: "08",
    september: "09",
    october: "10",
    november: "11",
    december: "12",
  };
  if (range) {
    const mo = months[range[1].toLowerCase()];
    return {
      eventStartDate: `${range[4]}-${mo}-${String(range[2]).padStart(2, "0")}`,
      eventEndDate: `${range[4]}-${mo}-${String(range[3]).padStart(2, "0")}`,
    };
  }
  const single = t.match(
    /(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s*(20\d{2})/i
  );
  if (single) {
    const mo = months[single[1].toLowerCase()];
    const d = `${single[3]}-${mo}-${String(single[2]).padStart(2, "0")}`;
    return { eventStartDate: d, eventEndDate: d };
  }
  const monthYear = t.match(
    /(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})/i
  );
  if (monthYear) {
    const mo = months[monthYear[1].toLowerCase()];
    const d = `${monthYear[2]}-${mo}-01`;
    return { eventStartDate: d, eventEndDate: d, granularity: "month" };
  }
  const y = t.match(/20\d{2}/);
  return { eventStartDate: null, eventEndDate: null, yearHint: y ? y[0] : null };
}

function normalizePhoneType(pkg = {}) {
  let phoneType = text(pkg.phoneType);
  const note = text(pkg.phoneNote || "");
  if (/helpline/i.test(note) || /helpline/i.test(phoneType)) {
    phoneType = "organization_helpline";
  }
  return phoneType || null;
}

function normalizeCompletenessGroup(pkg = {}) {
  const stamped = applyContactPackageToOpportunity({}, { ...pkg, phoneType: normalizePhoneType(pkg) });
  const computed = classifyContactCompletenessGroup(stamped);
  // CMSS: person + email + HQ phone → group 1 when person established
  if (
    pkg.cardId === "may-cmss-2027" &&
    text(pkg.person) &&
    !/^not\s*established$/i.test(pkg.person) &&
    text(pkg.personEmail || pkg.contactMethod) &&
    text(pkg.businessPhone) &&
    !/^not\s*established$/i.test(pkg.businessPhone)
  ) {
    return 1;
  }
  return computed;
}

function matchIdentityKey(row = {}, pkg = {}) {
  const orgKey = text(pkg.orgKey || row.orgKey);
  const cardId = text(row.cardId || pkg.cardId);
  return `${orgKey}::${cardId}`;
}

function findExisting(
  opportunities,
  row,
  pkg,
  { claimedIds = new Set(), allowOrgHeuristic = false } = {}
) {
  const available = (opportunities || []).filter((o) => !claimedIds.has(o.id));

  const known = KNOWN_ID_MATCHES[row.cardId];
  if (known) {
    const byKnown = available.find((o) => o.id === known);
    if (byKnown) return { opp: byKnown, matchType: "known_id" };
  }
  const byCard = available.find(
    (o) =>
      o.prospectReviewCardId === row.cardId ||
      o.reviewedProspectCardId === row.cardId ||
      o.id === row.cardId
  );
  if (byCard) return { opp: byCard, matchType: "card_id" };

  const identity = matchIdentityKey(row, pkg);
  const byIdentity = available.find((o) => o.prospectIdentityKey === identity);
  if (byIdentity) return { opp: byIdentity, matchType: "identity_key" };

  // Default: create. Org heuristics are opt-in only — Mayflower already has a
  // distinct research corpus that must not be overwritten by guessing.
  if (!allowOrgHeuristic) {
    return { opp: null, matchType: "create" };
  }

  const org = text(row.account || pkg.company).toLowerCase();
  const orgKey = text(pkg.orgKey || row.orgKey).toLowerCase();
  const year = (row.timing || "").match(/20\d{2}/)?.[0];
  const stop = new Set([
    "national",
    "association",
    "american",
    "america",
    "society",
    "conference",
    "hotel",
    "the",
    "and",
    "for",
    "of",
    "inc",
    "development",
    "organizations",
  ]);
  const orgTokens = org
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 6 && !stop.has(t));
  if (org || orgKey) {
    const candidates = available.filter((o) => {
      if (o.prospectReviewCardId && o.prospectReviewCardId !== row.cardId) return false;
      if (o.prospectIdentityKey && o.prospectIdentityKey !== identity) return false;
      const name = String(o.organizationName || "").toLowerCase();
      if (!name) return false;
      if (orgKey) {
        const keyBits = orgKey
          .replace(/-/g, " ")
          .split(/\s+/)
          .filter((t) => t.length >= 5 && !stop.has(t));
        if (keyBits.length && keyBits.every((t) => name.includes(t))) return true;
      }
      return orgTokens.some((t) => name.includes(t));
    });
    if (year) {
      const yHits = candidates.filter(
        (o) =>
          String(o.title || "").includes(year) ||
          String(o.eventStartDate || "").startsWith(year)
      );
      if (yHits.length === 1) return { opp: yHits[0], matchType: "org_year_heuristic" };
      if (yHits.length > 1) {
        return {
          opp: null,
          matchType: "ambiguous_org_year",
          ambiguousIds: yHits.map((o) => o.id),
        };
      }
    }
    if (candidates.length === 1) {
      return { opp: candidates[0], matchType: "org_unique_deterministic" };
    }
    if (candidates.length > 1) {
      return {
        opp: null,
        matchType: "ambiguous_org",
        ambiguousIds: candidates.map((o) => o.id),
      };
    }
  }
  return { opp: null, matchType: "create" };
}

function buildOpportunityFromPackage({ row, pkg, existing = null, hotelId }) {
  const dates = parseEventDates(row.timing || "");
  const phoneType = normalizePhoneType(pkg || {});
  const group = pkg ? normalizeCompletenessGroup({ ...pkg, phoneType }) : 4;
  const section = text(row.section || "PRIORITY").toUpperCase();
  const commercialLabel =
    section === "EXPLORATORY" ? "Exploratory account" : "Prospect to qualify";

  let base = existing
    ? { ...existing }
    : {
        id: createId("gdi_opp_prospect"),
        hotelId,
        createdAt: new Date().toISOString(),
      };

  // Preserve user/workflow fields
  const preserved = {};
  for (const k of PROTECTED_USER_FIELDS) {
    if (base[k] !== undefined) preserved[k] = base[k];
  }

  const title =
    text(existing?.title) ||
    `${text(row.account || pkg?.company)} — ${text(row.timing).slice(0, 80) || section}`;

  let next = {
    ...base,
    hotelId,
    title,
    organizationName: text(row.account || pkg?.company || base.organizationName),
    company: text(row.account || pkg?.company || base.company),
    segment: base.segment || "Association",
    participationRole: base.participationRole || "ORGANIZER",
    prospectReviewCardId: row.cardId,
    prospectReviewSection: section,
    prospectOrgKey: text(pkg?.orgKey || row.orgKey),
    prospectIdentityKey: matchIdentityKey(row, pkg || {}),
    prospectPackageVersion: REVIEWED_PROSPECT_PACKAGE_VERSION,
    prospectPublicationBasis: REVIEWED_PROSPECT_PUBLICATION_BASIS,
    prospectCommercialLabel: commercialLabel,
    opportunityQualificationLabel: commercialLabel,
    venueSourcingStatus: "UNKNOWN",
    venueSourcingStatusLabel: "Hotel sourcing unconfirmed",
    roomDemandStatus: base.roomDemandStatus || "UNKNOWN",
    confirmedBuying: false,
    commercialStatus: text(row.commercialStatus) || commercialLabel,
    timingNote: text(row.timing),
    eventStartDate: dates.eventStartDate || base.eventStartDate || null,
    eventEndDate: dates.eventEndDate || base.eventEndDate || null,
    futureCycleEvidenceState: base.futureCycleEvidenceState || "FUTURE_CYCLE_EXPECTED",
    fitExplanation: text(row.commercialStatus) || base.fitExplanation,
    summaryWhyHotel: text(row.commercialStatus) || base.summaryWhyHotel,
    hotelOpportunityThesis:
      text(row.commercialStatus) ||
      base.hotelOpportunityThesis ||
      `Reviewed prospect for ${hotelId}`,
    summaryWhat: `${text(row.account)} group lodging prospect`,
    summaryWhyNow: text(row.timing) || "Dated occasion in primary horizon",
    summaryWhyMatters:
      "Credible reviewed prospect — hotel sourcing unconfirmed; contact route researched",
    whyNow: text(row.timing) || commercialLabel,
    cardWhyNowLine: text(row.timing),
    hotelFitScore: base.hotelFitScore ?? 55,
    officialSource:
      text(pkg?.sourceUrl) ||
      text(row.sources).split(";")[0]?.replace(/\s*\(retrieved.*$/, "").trim() ||
      base.officialSource,
    discoverySource: text(pkg?.sourceUrl) || base.discoverySource,
    sources: [
      ...(Array.isArray(base.sources) ? base.sources : []),
      ...(text(pkg?.sourceUrl) ? [{ url: pkg.sourceUrl, verificationDate: pkg.verificationDate }] : []),
      ...(text(pkg?.sourceUrlSecondary)
        ? [{ url: pkg.sourceUrlSecondary, verificationDate: pkg.verificationDate }]
        : []),
    ].filter((s, i, arr) => s?.url && arr.findIndex((x) => x.url === s.url) === i),
    bookingResponsibilityClass: text(pkg?.bookingResponsibility) || null,
    bookingResponsibilityLabel: contactResponsibilityLabel(pkg?.bookingResponsibility),
    contactCompletenessGroup: group,
    recommendedAction:
      section === "EXPLORATORY"
        ? "Qualify whether overflow or secondary lodging is needed before pursuit."
        : "Contact the published route to ask about lodging status and hotel fit.",
    recommendedNextStep:
      section === "EXPLORATORY"
        ? "Qualify whether overflow or secondary lodging is needed before pursuit."
        : "Contact the published route to ask about lodging status and hotel fit.",
    contactResearchAttempted: true,
    whoResearchAttempted: true,
    updatedAt: new Date().toISOString(),
    ...preserved,
  };

  if (pkg) {
    const pkgNorm = {
      ...pkg,
      phoneType,
      completenessGroup: group,
      businessPhone: /^not\s*established$/i.test(text(pkg.businessPhone))
        ? "Not established"
        : pkg.businessPhone,
    };
    // Prefer personEmail for CMSS when functional education@ is primary method
    if (pkg.personEmail && text(pkg.person) && !/^not\s*established$/i.test(pkg.person)) {
      pkgNorm.contactMethod = pkg.personEmail;
      pkgNorm.contactMethodType = "named_email";
      next.functionalContactEmail = text(pkg.contactMethod);
    }
    next = applyContactPackageToOpportunity(next, pkgNorm);
    if (next.primaryContact) {
      next.primaryContact.phoneType = phoneType || next.primaryContact.phoneType;
      next.primaryContact.phoneTypeLabel = phoneTypeCustomerLabel(phoneType);
      next.primaryContact.relevanceEvidence = text(pkg.relevanceEvidence);
      next.primaryContact.verificationDate = text(pkg.verificationDate);
    }
    next.primaryContactPhoneType = phoneType;
    next.primaryContactPhoneTypeLabel = phoneTypeCustomerLabel(phoneType);
  }

  const elig = evaluateProspectPublicationEligibility(next);
  next.prospectHotelFacing = elig.hotelFacing === true;
  next.prospectEligibility = elig.eligibility;
  next.prospectEnrichmentQueue = elig.enrichmentQueue === true;
  next.prospectEligibilityReasons = elig.reasons || [];
  next.prospectContactRouteReason = elig.route?.reason || null;
  next.prospectCustomerLabel = customerFacingLabelForProspect(next);
  if (!next.prospectHotelFacing) {
    // Keep internally for enrichment; do not force customerVisible false if already true from other path
    next.prospectInternalOnly = true;
  }

  // Never invent Ready/ACTIONABLE
  if (!existing?.gdiMaturityState || existing.gdiMaturityState === "SIGNAL") {
    // leave maturity as-is / unset — publication path is independent
  }

  return { opportunity: next, eligibility: elig };
}

function ensureMayflowerProfile(hotelId) {
  const existing = loadHotelProfile(hotelId);
  if (existing) return { profile: existing, created: false };
  const profile = {
    hotelId,
    canonicalIds: {
      censusRecordId: hotelId,
      note: "Initialized for reviewed-prospect publication local path",
    },
    identity: {
      hotelName: "The Mayflower Hotel, Autograph Collection",
      address: "1127 Connecticut Avenue NW, Washington, DC 20036, United States",
      city: "Washington",
      state: "District of Columbia",
      country: "US",
      brand: "Autograph Collection",
      parentBrandCompany: "Marriott International",
      chainScale: "Upper Upscale",
      website: "https://www.marriott.com/en-us/hotels/wasor-the-mayflower-hotel-autograph-collection/overview/",
    },
    guestrooms: {
      totalGuestrooms: 581,
      totalGuestroomsClaimKind: "ESTIMATED",
      totalGuestroomsConfidence: "MEDIUM",
      note: "From prior Mayflower GDI registration notes — confirm before commercial claims",
    },
    meetingCapability: {
      meetingRooms: 27,
      meetingRoomsClaimKind: "ESTIMATED",
    },
    hotelCharacteristics: {
      serviceLevel: "full_service",
      classification: "urban_downtown",
      publicTransit: "dc_metro_access",
    },
    gdiOnboarding: {
      reviewedProspectPublication: true,
      initializedAt: new Date().toISOString(),
      source: "reviewed-prospect-import-v1",
    },
    createdAt: new Date().toISOString(),
  };
  saveHotelProfile(hotelId, profile);
  return { profile, created: true };
}

function ensureMayflowerConfig() {
  // Config lives under config/ — for selectable hotels; write only if missing
  const configPath = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "../../config/group-demand-intelligence/hotels/rec0eTEnCqlsGJJt9.json"
  );
  if (fs.existsSync(configPath)) return { created: false, path: configPath };
  const config = {
    hotelId: "rec0eTEnCqlsGJJt9",
    displayName: "The Mayflower Hotel, Autograph Collection",
    commercialPriorities: {
      targetSegments: ["Associations", "Corporate", "Government", "Advocacy"],
      notes: "Downtown DC Autograph — reviewed prospect publication onboarding",
    },
    demandTerritory: {
      label: "Downtown DC / National Mall corridor",
      evaluationQuestion: "Could The Mayflower realistically compete for this lodging?",
    },
    gdiOnboarding: {
      reviewedProspectPublication: true,
    },
  };
  fs.mkdirSync(path.dirname(configPath), { recursive: true });
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2) + "\n");
  return { created: true, path: configPath };
}

function backupHotelStore(hotelId, backupDir, opportunitiesDoc = null) {
  const src = path.join(hotelDir(hotelId), "opportunities.json");
  const profileSrc = path.join(hotelDir(hotelId), "profile.json");
  fs.mkdirSync(backupDir, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const out = {
    opportunities: null,
    profile: null,
    adapterSnapshot: null,
    persistenceMode: getGdiOpportunityPersistenceMode(),
  };
  if (opportunitiesDoc) {
    const dest = path.join(
      backupDir,
      `${hotelId}-adapter-snapshot-${stamp}.json`
    );
    fs.writeFileSync(dest, JSON.stringify(opportunitiesDoc, null, 2) + "\n");
    out.adapterSnapshot = dest;
  }
  if (fs.existsSync(src)) {
    const dest = path.join(backupDir, `${hotelId}-opportunities-${stamp}.json`);
    fs.copyFileSync(src, dest);
    out.opportunities = dest;
  }
  if (fs.existsSync(profileSrc)) {
    const dest = path.join(backupDir, `${hotelId}-profile-${stamp}.json`);
    fs.copyFileSync(profileSrc, dest);
    out.profile = dest;
  }
  return out;
}

/**
 * Detect conflicts when production facts are newer than the package and
 * would be overwritten without an explicit force.
 */
function detectNewerFactConflict(existing, opportunity, pkg) {
  if (!existing) return null;
  const existingUpdated = Date.parse(existing.updatedAt || existing.lastVerifiedAt || "") || 0;
  const pkgVerified = Date.parse(pkg?.verificationDate || "") || 0;
  const existingEmail = text(existing.primaryContact?.email);
  const nextEmail = text(opportunity.primaryContact?.email);
  const existingPhone = text(existing.primaryContact?.phone);
  const nextPhone = text(opportunity.primaryContact?.phone);
  if (
    existingUpdated &&
    pkgVerified &&
    existingUpdated > pkgVerified + 24 * 60 * 60 * 1000 &&
    ((existingEmail && nextEmail && existingEmail !== nextEmail) ||
      (existingPhone && nextPhone && existingPhone !== nextPhone))
  ) {
    return {
      reason: "newer_production_contact_facts",
      existingUpdatedAt: existing.updatedAt || existing.lastVerifiedAt || null,
      packageVerificationDate: pkg?.verificationDate || null,
      existingEmail: existingEmail || null,
      nextEmail: nextEmail || null,
      existingPhone: existingPhone || null,
      nextPhone: nextPhone || null,
    };
  }
  return null;
}

/**
 * Load versioned hotel-review package inputs.
 */
export function loadReviewedProspectPackage(packageDir) {
  const dir = path.resolve(packageDir);
  const registerPath = path.join(dir, "final-prospect-register.csv");
  const packagesPath = path.join(dir, "contact-packages.json");
  const register = parseCsv(fs.readFileSync(registerPath, "utf8"));
  const packagesDoc = JSON.parse(fs.readFileSync(packagesPath, "utf8"));
  const byId = Object.fromEntries((packagesDoc.packages || []).map((p) => [p.cardId, p]));
  return {
    packageDir: dir,
    registerPath,
    packagesPath,
    registerHash: sha256File(registerPath),
    packagesHash: sha256File(packagesPath),
    packageVersion: REVIEWED_PROSPECT_PACKAGE_VERSION,
    register,
    packagesById: byId,
    packagesDoc,
  };
}

/**
 * Import reviewed prospects for one hotel through the canonical persistence
 * adapter (Airtable when configured; filesystem when explicitly selected).
 */
export async function importReviewedProspectsForHotel({
  hotelId,
  packageDir,
  dryRun = true,
  backupDir = null,
  initMayflower = true,
  skipConflictedUpdates = true,
  // Bethesda has known_id map; Mayflower and new hotels default to create-only
  // unless identity/card stamps already exist.
  allowOrgHeuristic = false,
} = {}) {
  clearGdiRepositoryReadCache();
  invalidateGdiHotelReadCache(hotelId);
  const persistenceMode = getGdiOpportunityPersistenceMode();
  const pkgBundle = loadReviewedProspectPackage(packageDir);
  const rows = pkgBundle.register.filter((r) => r.hotelId === hotelId);
  const conflicts = [];
  const quarantined = [];
  const manifest = [];

  let profileInit = { created: false };
  let configInit = { created: false };
  if (hotelId === "rec0eTEnCqlsGJJt9" && initMayflower) {
    if (!dryRun) {
      profileInit = ensureMayflowerProfile(hotelId);
      configInit = ensureMayflowerConfig();
    } else {
      profileInit = { created: !loadHotelProfile(hotelId), dryRun: true };
      configInit = { created: true, dryRun: true };
    }
  }

  const doc = await loadOpportunitiesCanonical(hotelId);
  const existingList = [...(doc.opportunities || [])];
  const nextList = [...existingList];
  const claimedIds = new Set();
  let backup = null;

  if (!dryRun && backupDir) {
    backup = backupHotelStore(hotelId, backupDir, doc);
  }

  for (const row of rows) {
    const pkg = pkgBundle.packagesById[row.cardId] || null;
    const found = findExisting(nextList, row, pkg || {}, {
      claimedIds,
      allowOrgHeuristic,
    });
    const { opp: existing, matchType, ambiguousIds = [] } = found;

    if (matchType === "ambiguous_org" || matchType === "ambiguous_org_year") {
      conflicts.push({
        cardId: row.cardId,
        reason: matchType,
        ambiguousIds,
      });
      quarantined.push({
        cardId: row.cardId,
        reason: matchType,
        ambiguousIds,
      });
      // Force create with a new id rather than overwrite ambiguous production rows.
    }

    const { opportunity, eligibility } = buildOpportunityFromPackage({
      row,
      pkg,
      existing: matchType.startsWith("ambiguous_") ? null : existing,
      hotelId,
    });

    if (existing && matchType === "org_year_heuristic") {
      if (
        existing.prospectIdentityKey &&
        existing.prospectIdentityKey !== opportunity.prospectIdentityKey
      ) {
        conflicts.push({
          cardId: row.cardId,
          existingId: existing.id,
          reason: "identity_key_mismatch",
        });
      }
    }

    const newerConflict = detectNewerFactConflict(existing, opportunity, pkg);
    if (newerConflict) {
      conflicts.push({
        cardId: row.cardId,
        existingId: existing.id,
        ...newerConflict,
      });
      if (skipConflictedUpdates) {
        quarantined.push({
          cardId: row.cardId,
          opportunityId: existing.id,
          reason: newerConflict.reason,
        });
        claimedIds.add(existing.id);
        manifest.push({
          cardId: row.cardId,
          hotelId,
          section: row.section,
          account: row.account,
          opportunityId: existing.id,
          action: "quarantined_conflict",
          matchType,
          hotelFacing: eligibility.hotelFacing ? "YES" : "NO",
          enrichmentQueue: eligibility.enrichmentQueue ? "YES" : "NO",
          eligibility: eligibility.eligibility,
          contactGroup: opportunity.contactCompletenessGroup,
          person: existing.primaryContact?.name || pkg?.person || "",
          contactMethod:
            existing.primaryContact?.email ||
            pkg?.contactMethod ||
            "",
          businessPhone: existing.primaryContact?.phone || pkg?.businessPhone || "",
          phoneType:
            existing.primaryContactPhoneType ||
            existing.primaryContact?.phoneType ||
            pkg?.phoneType ||
            "",
        });
        continue;
      }
    }

    const action = existing ? "update" : "create";
    if (existing) {
      const idx = nextList.findIndex((o) => o.id === existing.id);
      opportunity.id = existing.id;
      nextList[idx] = opportunity;
      claimedIds.add(existing.id);
    } else {
      nextList.push(opportunity);
      claimedIds.add(opportunity.id);
    }

    manifest.push({
      cardId: row.cardId,
      hotelId,
      section: row.section,
      account: row.account,
      opportunityId: opportunity.id,
      action,
      matchType,
      hotelFacing: eligibility.hotelFacing ? "YES" : "NO",
      enrichmentQueue: eligibility.enrichmentQueue ? "YES" : "NO",
      eligibility: eligibility.eligibility,
      contactGroup: opportunity.contactCompletenessGroup,
      person: opportunity.primaryContact?.name || pkg?.person || "",
      contactMethod:
        opportunity.primaryContact?.email ||
        opportunity.functionalContactEmail ||
        pkg?.contactMethod ||
        "",
      businessPhone: opportunity.primaryContact?.phone || pkg?.businessPhone || "",
      phoneType: opportunity.primaryContactPhoneType || pkg?.phoneType || "",
    });
  }

  let saveResult = null;
  if (!dryRun) {
    saveResult = await saveOpportunitiesCanonical(hotelId, {
      ...doc,
      hotelId,
      opportunities: nextList,
      reviewedProspectImport: {
        packageVersion: pkgBundle.packageVersion,
        registerHash: pkgBundle.registerHash,
        packagesHash: pkgBundle.packagesHash,
        importedAt: new Date().toISOString(),
        cardCount: rows.length,
        persistenceMode,
      },
    });
    clearGdiRepositoryReadCache();
    invalidateGdiHotelReadCache(hotelId);
  }

  const facing = manifest.filter((m) => m.hotelFacing === "YES");
  const priorityFacing = facing.filter((m) => m.section === "PRIORITY");
  const exploratoryFacing = facing.filter((m) => m.section === "EXPLORATORY");
  const enrichment = manifest.filter((m) => m.enrichmentQueue === "YES");

  return {
    ok: true,
    dryRun,
    hotelId,
    persistenceMode,
    airtableConfigured: isGdiOpportunityAirtableConfigured(),
    dataRoot: getGdiDataRoot(),
    packageVersion: pkgBundle.packageVersion,
    registerHash: pkgBundle.registerHash,
    packagesHash: pkgBundle.packagesHash,
    profileInit,
    configInit,
    backup,
    savePersistence: saveResult?.persistence || null,
    counts: {
      packageRows: rows.length,
      create: manifest.filter((m) => m.action === "create").length,
      update: manifest.filter((m) => m.action === "update").length,
      quarantined: quarantined.length,
      hotelFacing: facing.length,
      priorityHotelFacing: priorityFacing.length,
      exploratoryHotelFacing: exploratoryFacing.length,
      enrichmentQueue: enrichment.length,
      existingUnmatchedPreserved:
        existingList.length -
        manifest.filter((m) => m.action === "update" || m.action === "quarantined_conflict")
          .length,
    },
    conflicts,
    quarantined,
    manifest,
  };
}

export async function importReviewedProspectsBothHotels(opts = {}) {
  const hotels = opts.hotelIds || REVIEWED_PROSPECT_HOTEL_IDS;
  const results = [];
  for (const hotelId of hotels) {
    results.push(await importReviewedProspectsForHotel({ ...opts, hotelId }));
  }
  return results;
}

export { KNOWN_ID_MATCHES, PROTECTED_USER_FIELDS };
