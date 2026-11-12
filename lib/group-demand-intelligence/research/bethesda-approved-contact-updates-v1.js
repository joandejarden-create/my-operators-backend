/**
 * Founder-approved Bethesda contact updates (zero-paid).
 *
 * 1) Woman's Club — canonical single-ID contact patch (role inbox, not direct person email)
 * 2) Strathmore — research-stage organization routes only
 *
 * No paid providers. No Sales Ready / publication changes.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_HOTEL_ID,
  BETHESDA_PROTECTED_SCALING_IDS,
  BETHESDA_EXPECTED_CANONICAL_COUNT,
  ACCEPTABLE_59_ROW_SHA256S,
  RESTORED_LIVE_59_ROW_SHA256,
  loadHotelOpportunityDoc,
  backupHotelOpportunityStore,
  assertNoProtectedIdLoss,
  assertCountNotDropped,
  sha256File,
} from "./gdi-hotel-store-integrity-guard-v1.js";
import {
  persistResearchStageOpportunityBag,
  researchStageHotelDir,
  writeJsonAtomic,
} from "./gdi-research-stage-persistence-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "./bethesda-historical-attestation-bag-v1.js";
import { applyHistoricalCommercialCategoryProjection } from "./bethesda-historical-commercial-category-map-v1.js";
import { CONTACT_ELIGIBILITY_CLASS } from "../contact-eligibility-standard-v1.js";
import { mergeContactAntiClobber } from "./bethesda-88-contact-quality-completion-v1.js";

export const BETHESDA_APPROVED_CONTACT_UPDATES_V1 =
  "gdi-bethesda-approved-contact-updates-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_DIR_REL = "reports/gdi-bethesda-approved-contact-updates-v1";
const WOMANS_CLUB_ID = "gdi_pe_781f12393f8117e7";
const STRATHMORE_ID = "disp_recLuxvwwxID7U2B8_strathmore";

export const WOMANS_CLUB_APPROVED_CONTACT = Object.freeze({
  id: WOMANS_CLUB_ID,
  primaryContact: {
    name: "Diane Robertson",
    fullName: "Diane Robertson",
    title: "Rental Coordinator",
    role: "Rental Coordinator",
    organization: "Woman's Club of Bethesda",
    email: "rentals@womansclubofbethesda.org",
    emailOwnership: "organization_or_department",
    organizationContactRoute: "rentals@womansclubofbethesda.org",
    phone: "301-530-1784",
    phoneOwnership: "organization_or_department",
    contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
    whyThisContact:
      "Named rental coordinator for clubhouse events — relevant commercial entry for preferred lodging / guestroom overflow from venue rentals. rentals@ is a role/organization inbox, not a verified individual mailbox.",
    sourceUrl: "https://www.womansclubofbethesda.org/contact/",
    verifiedAt: "2026-11-12",
  },
  secondaryContact: {
    name: "Mary Sherman",
    title: "Club President",
    email: "president@womansclubofbethesda.org",
    emailOwnership: "organization_or_department",
    contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
    sourceUrl: "https://www.womansclubofbethesda.org/contact/",
    verifiedAt: "2026-11-12",
  },
  functionalContactEmail: "rentals@womansclubofbethesda.org",
  organizationContactEmail: "rentals@womansclubofbethesda.org",
  commercialContactPath: "NAMED_CONTACT",
  contactOfficialUrl: "https://www.womansclubofbethesda.org/contact/",
  contactSourceUrl: "https://www.womansclubofbethesda.org/contact/",
  contactVerifiedAt: "2026-11-12",
  whyThisContact:
    "Named rental coordinator for clubhouse events — relevant commercial entry for preferred lodging / guestroom overflow from venue rentals. rentals@ is a role/organization inbox, not a verified individual mailbox.",
});

export const STRATHMORE_APPROVED_CONTACT = Object.freeze({
  id: STRATHMORE_ID,
  primaryContact: {
    name: null,
    title: "Strathmore Group Sales (organization route)",
    role: "Group Sales / Private Event Rentals desk",
    email: "groupsales@strathmore.org",
    emailOwnership: "organization_or_department",
    organizationContactRoute: "groupsales@strathmore.org",
    phone: "301-581-5199",
    phoneOwnership: "organization_or_department",
    contactClass: "FUNCTIONAL_INBOX",
    namedPerson: false,
    whyRelevant:
      "Public Strathmore Group Sales inbox and phone for group outings / lodging-adjacent partnership outreach. Not a verified named individual.",
    sourceUrl:
      "https://www.rockvillechamber.org/2026/01/30/plan-the-perfect-group-outing-at-strathmore/",
    verifiedAt: "2026-11-12",
  },
  secondaryContact: {
    name: null,
    title: "Strathmore venue rentals desk",
    email: "rentals@strathmore.org",
    emailOwnership: "organization_or_department",
    contactClass: "FUNCTIONAL_INBOX",
    sourceUrl: "https://directory.bizbash.com/venue/amp-by-strathmore/",
    verifiedAt: "2026-11-12",
  },
  contactRecovery: {
    namedPerson: false,
    usableRoute: true,
    email: "groupsales@strathmore.org",
    contactClass: "FUNCTIONAL_INBOX",
    raw: "groupsales@strathmore.org",
    note: "Zero-paid org routes 2026-11-12 — groupsales@ + rentals@ + 301-581-5199. No named buyer invented.",
  },
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function applyWomansClubFields(row) {
  const patch = WOMANS_CLUB_APPROVED_CONTACT;
  return {
    ...row,
    primaryContact: {
      ...(row.primaryContact && typeof row.primaryContact === "object"
        ? row.primaryContact
        : {}),
      ...patch.primaryContact,
    },
    secondaryContact: patch.secondaryContact,
    primaryContactName: patch.primaryContact.name,
    primaryContactRole: patch.primaryContact.title,
    primaryContactEmail: null, // role inbox must not count as person-direct email
    primaryContactPhone: patch.primaryContact.phone,
    functionalContactEmail: patch.functionalContactEmail,
    organizationContactEmail: patch.organizationContactEmail,
    contactName: patch.primaryContact.name,
    contactTitle: patch.primaryContact.title,
    contactEmail: patch.functionalContactEmail,
    contactPhone: patch.primaryContact.phone,
    contactOfficialUrl: patch.contactOfficialUrl,
    contactSourceUrl: patch.contactSourceUrl,
    contactVerifiedAt: patch.contactVerifiedAt,
    whyThisContact: patch.whyThisContact,
    commercialContactPath: patch.commercialContactPath,
    contactPathClass: "NAMED_PARTIAL",
    contactTier: "NAMED_PARTIAL",
    contactGrade: "B",
    contactGradeLabel: "Grade B — named person via organization route",
    contactQuality: "NAMED_VIA_ORG_ROUTE",
    // Preserve commercial / publication state
    salesReady: row.salesReady === true ? row.salesReady : false,
    customerFacing: row.customerFacing === true ? row.customerFacing : false,
    customerVisible: row.customerVisible === true ? row.customerVisible : false,
    publicationApproved:
      row.publicationApproved === true ? row.publicationApproved : false,
    priority: row.priority,
    commercialMaturity: row.commercialMaturity,
    approvedContactUpdate: {
      version: BETHESDA_APPROVED_CONTACT_UPDATES_V1,
      appliedAt: new Date().toISOString(),
      founderApproved: true,
      emailClassification: "ORGANIZATION_ROLE_INBOX",
      directPersonEmail: false,
    },
  };
}

/**
 * Apply founder-approved Woman's Club contact patch to canonical opportunities.json.
 */
export function applyApprovedWomansClubCanonicalPatch({
  root = ROOT,
  dryRun = false,
} = {}) {
  const loaded = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  if (!loaded.exists) return { ok: false, code: "STORE_ABSENT" };
  if (loaded.count !== BETHESDA_EXPECTED_CANONICAL_COUNT) {
    return {
      ok: false,
      code: "UNEXPECTED_COUNT",
      count: loaded.count,
      expected: BETHESDA_EXPECTED_CANONICAL_COUNT,
    };
  }
  if (
    !ACCEPTABLE_59_ROW_SHA256S.includes(loaded.sha256) &&
    loaded.sha256 !== RESTORED_LIVE_59_ROW_SHA256
  ) {
    // Allow continue if already patched in a prior run of this module (idempotent).
    const existing = loaded.opportunities.find(
      (o) => text(o.id) === WOMANS_CLUB_ID
    );
    if (existing?.primaryContact?.name !== "Diane Robertson") {
      return {
        ok: false,
        code: "UNEXPECTED_STORE_SHA",
        sha256: loaded.sha256,
      };
    }
  }

  const beforeIds = loaded.ids.slice().sort();
  const idx = loaded.opportunities.findIndex((o) => text(o.id) === WOMANS_CLUB_ID);
  if (idx < 0) return { ok: false, code: "TARGET_ID_MISSING", id: WOMANS_CLUB_ID };

  const beforeRow = loaded.opportunities[idx];
  const afterRow = applyWomansClubFields(beforeRow);
  const nextOps = loaded.opportunities.map((o, i) => (i === idx ? afterRow : o));

  const idGate = assertNoProtectedIdLoss({
    hotelId: BETHESDA_HOTEL_ID,
    nextOpportunities: nextOps,
    protectedIds: [...BETHESDA_PROTECTED_SCALING_IDS, WOMANS_CLUB_ID],
  });
  if (!idGate.ok) return { ok: false, ...idGate };

  const countGate = assertCountNotDropped({
    hotelId: BETHESDA_HOTEL_ID,
    priorCount: loaded.count,
    nextCount: nextOps.length,
  });
  if (!countGate.ok) return { ok: false, ...countGate };

  const unchangedOthers = nextOps.every((o, i) => {
    if (text(o.id) === WOMANS_CLUB_ID) return true;
    return o === loaded.opportunities[i];
  });
  if (!unchangedOthers) {
    return { ok: false, code: "OTHER_ROWS_MUTATED" };
  }

  const outDir = path.join(root, REPORT_DIR_REL);
  fs.mkdirSync(outDir, { recursive: true });
  const preview = {
    id: WOMANS_CLUB_ID,
    before: {
      name: beforeRow.primaryContact?.name || beforeRow.contactName || null,
      email: beforeRow.primaryContact?.email || beforeRow.contactEmail || null,
      path: beforeRow.commercialContactPath || null,
    },
    after: {
      name: afterRow.primaryContact.name,
      roleInbox: afterRow.functionalContactEmail,
      phone: afterRow.primaryContact.phone,
      secondary: afterRow.secondaryContact?.name || null,
      emailOwnership: afterRow.primaryContact.emailOwnership,
      directPersonEmail: false,
    },
  };
  writeJsonAtomic(path.join(outDir, "01-womans-club-patch-preview.json"), preview);

  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      writeApplied: false,
      preview,
      beforeSha256: loaded.sha256,
    };
  }

  const backup = backupHotelOpportunityStore(BETHESDA_HOTEL_ID, {
    root,
    label: "pre-womans-club-contact-patch",
  });
  if (!backup.ok) return { ok: false, ...backup };

  const doc = loaded.doc && typeof loaded.doc === "object" && !Array.isArray(loaded.doc)
    ? { ...loaded.doc }
    : { hotelId: BETHESDA_HOTEL_ID, opportunities: [] };
  doc.hotelId = BETHESDA_HOTEL_ID;
  doc.opportunities = nextOps;
  doc.updatedAt = new Date().toISOString();
  doc.approvedContactPatch = {
    version: BETHESDA_APPROVED_CONTACT_UPDATES_V1,
    id: WOMANS_CLUB_ID,
    appliedAt: doc.updatedAt,
    founderApproved: true,
  };

  writeJsonAtomic(loaded.path, doc);
  const afterSha = sha256File(loaded.path);
  const afterIds = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root }).ids.slice().sort();
  if (JSON.stringify(afterIds) !== JSON.stringify(beforeIds)) {
    return {
      ok: false,
      code: "ID_SET_CHANGED_AFTER_WRITE",
      backup,
      afterSha,
    };
  }

  writeJsonAtomic(path.join(outDir, "02-womans-club-apply-summary.json"), {
    ok: true,
    writeApplied: true,
    backupPath: backup.backupPath,
    beforeSha256: loaded.sha256,
    afterSha256: afterSha,
    count: nextOps.length,
    targetId: WOMANS_CLUB_ID,
    directPersonEmail: false,
    emailClassification: "ORGANIZATION_ROLE_INBOX",
  });

  return {
    ok: true,
    dryRun: false,
    writeApplied: true,
    backup,
    beforeSha256: loaded.sha256,
    afterSha256: afterSha,
    preview,
  };
}

/**
 * Apply Strathmore organization contact routes to research-stage historical body.
 */
export function applyApprovedStrathmoreResearchStageRoutes({
  root = ROOT,
  dryRun = false,
} = {}) {
  const stagedPath = path.join(
    researchStageHotelDir(BETHESDA_HOTEL_ID, { root }),
    "reconstructed-opportunities-v1.json"
  );
  if (!fs.existsSync(stagedPath)) {
    return { ok: false, code: "RESEARCH_STAGE_ABSENT", path: stagedPath };
  }
  const doc = JSON.parse(fs.readFileSync(stagedPath, "utf8"));
  const rows = doc.opportunities || [];
  const idx = rows.findIndex((o) => text(o.id) === STRATHMORE_ID);
  if (idx < 0) return { ok: false, code: "STRATHMORE_MISSING" };

  const before = rows[idx];
  const merged = mergeContactAntiClobber(before, STRATHMORE_APPROVED_CONTACT);
  // Force org-only classification — no invented named buyer.
  merged.primaryContact = {
    ...(merged.primaryContact || {}),
    ...STRATHMORE_APPROVED_CONTACT.primaryContact,
    name: null,
    namedPerson: false,
  };
  merged.secondaryContact = STRATHMORE_APPROVED_CONTACT.secondaryContact;
  merged.contactRecovery = {
    ...(merged.contactRecovery || {}),
    ...STRATHMORE_APPROVED_CONTACT.contactRecovery,
  };
  merged.functionalContactEmail = "groupsales@strathmore.org";
  merged.organizationContactEmail = "groupsales@strathmore.org";
  merged.primaryContactName = null;
  merged.primaryContactEmail = null;
  merged.companyPhone = "301-581-5199";
  merged.contactPhone = "301-581-5199";
  merged.contactSourceUrl =
    STRATHMORE_APPROVED_CONTACT.primaryContact.sourceUrl;
  merged.contactVerifiedAt = "2026-11-12";
  merged.whyThisContact =
    STRATHMORE_APPROVED_CONTACT.primaryContact.whyRelevant;
  // Preserve partnership watch
  merged.commercialMaturity = before.commercialMaturity || "PARTNERSHIP_WATCH";
  merged.historicalRecoveryDisposition =
    before.historicalRecoveryDisposition || "PARTNERSHIP_WATCH";
  merged.salesReady = false;
  merged.customerFacing = false;
  merged.customerVisible = false;
  merged.publicationApproved = false;
  merged.publication = "NOT_AUTHORIZED";

  let projected = applyHistoricalCommercialCategoryProjection(merged);
  projected.commercialMaturity =
    before.commercialMaturity || projected.commercialMaturity || "PARTNERSHIP_WATCH";

  const next = rows.map((r, i) => (i === idx ? projected : r));
  const outDir = path.join(root, REPORT_DIR_REL);
  fs.mkdirSync(outDir, { recursive: true });
  writeJsonAtomic(path.join(outDir, "03-strathmore-preview.json"), {
    id: STRATHMORE_ID,
    before: {
      primary: before.primaryContact || null,
      recovery: before.contactRecovery || null,
      maturity: before.commercialMaturity || null,
    },
    after: {
      groupsales: "groupsales@strathmore.org",
      rentals: "rentals@strathmore.org",
      phone: "301-581-5199",
      namedPerson: false,
      maturity: projected.commercialMaturity,
    },
  });

  if (dryRun) {
    return { ok: true, dryRun: true, writeApplied: false };
  }

  const persist = persistResearchStageOpportunityBag({
    hotelId: BETHESDA_HOTEL_ID,
    targetPath: stagedPath,
    incomingDoc: {
      ...doc,
      version: BETHESDA_APPROVED_CONTACT_UPDATES_V1,
      generatedAt: new Date().toISOString(),
      opportunities: next,
    },
    protectedIds: [...BETHESDA_29_ATTESTED_IDS],
    root,
    label: "strathmore-org-contact-routes",
    mode: "merge",
  });

  writeJsonAtomic(path.join(outDir, "04-strathmore-apply-summary.json"), {
    ok: persist?.ok === true,
    writeApplied: persist?.ok === true,
    namedPerson: false,
    maturityPreserved: projected.commercialMaturity,
  });

  return {
    ok: persist?.ok === true,
    dryRun: false,
    writeApplied: persist?.ok === true,
    persist,
  };
}

export function applyFounderApprovedContactUpdates({
  root = ROOT,
  dryRun = false,
} = {}) {
  const womans = applyApprovedWomansClubCanonicalPatch({ root, dryRun });
  const strathmore = applyApprovedStrathmoreResearchStageRoutes({ root, dryRun });
  return {
    ok: womans.ok === true && strathmore.ok === true,
    dryRun,
    womans,
    strathmore,
  };
}
