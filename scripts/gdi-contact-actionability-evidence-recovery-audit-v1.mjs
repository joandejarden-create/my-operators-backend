/**
 * READ-ONLY: Contact Actionability & Evidence Recovery V1 audit.
 * Frozen baseline = same 182 customer-visible opportunities from
 * gdi-contact-email-publication-leakage-audit-v1.
 *
 * No paid providers. No publication / Airtable / eligibility writes.
 * Does not fabricate or backfill source URLs.
 */
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import { createGdiPublishedInventoryRegistry } from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";
import { isGenericOrganizationEmail } from "../lib/group-demand-intelligence/scaling-companies-sales-presentation-v1.js";
import { classifyContactEligibility } from "../lib/group-demand-intelligence/contact-eligibility-standard-v1.js";
import { personIdentityKey } from "../lib/group-demand-intelligence/workflow/contact-field-provenance-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports", "gdi-contact-actionability-evidence-recovery-v1");
const LEAKAGE = path.join(
  ROOT,
  "reports",
  "gdi-contact-email-publication-leakage-audit-v1",
  "LEAKAGE_AUDIT.json",
);
const BETHESDA = "recLuxvwwxID7U2B8";

const HOTEL_NAMES = Object.freeze({
  recLuxvwwxID7U2B8: "Bethesda Marriott",
  recH9h3McgMNFQEla: "Radisson Blu Toronto Downtown",
  recrPQcZg7SFARRb2: "YOTEL Geneva Lake",
  recCAGqkERSXgiku4: "Sheraton Guadalajara Expo",
  recXh4NgF8sDbi9cQ: "Wyndham Alltra Punta Cana",
  recKRJjcPnb4tVDDS: "Spice Island Beach Resort",
});

const RELEVANT = new Set([
  "DIRECT_BUYER",
  "RELEVANT_COMMERCIAL_ENTRY_POINT",
  "VERIFIED_HOTEL_BUYER",
  "BOOKING_AUTHORITY",
]);

const ROLE_MAILBOX_RE =
  /^(info|contact|hello|office|admin|support|sales|inquiries|enquiry|reception|mail|team|press|media|customerservice|hq|summit|conference|congress|events?|housing|registration|registrations|bookings?|reservations?|rsvp|general|helpdesk|help)$/i;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function writeJson(file, data) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function isGenericMailbox(email) {
  if (!email || !String(email).includes("@")) return false;
  if (isGenericOrganizationEmail(email)) return true;
  return ROLE_MAILBOX_RE.test(text(email).split("@")[0] || "");
}

function extractEmail(contact = {}) {
  const raw = contact.email || contact.workEmail || contact.businessEmail || null;
  if (raw && typeof raw === "object") {
    return text(raw.email || raw.value || raw.address) || null;
  }
  const e = text(raw);
  if (!e || !e.includes("@") || /\[object Object\]/i.test(e)) return null;
  return e.toLowerCase();
}

function extractPhone(contact = {}) {
  const candidates = [
    contact.phone,
    contact.directPhone,
    contact.workPhone,
    contact.officePhone,
    Array.isArray(contact.phoneExtensions) && contact.phoneExtensions.length
      ? contact.phoneExtensions.join(" / ")
      : null,
  ]
    .map((v) => text(v))
    .filter(Boolean);
  const phone = candidates[0] || null;
  if (!phone) return null;
  if (!/\d/.test(phone)) return null;
  return phone;
}

function extractLinkedIn(contact = {}, opp = {}) {
  return (
    text(contact.linkedinUrl) ||
    text(contact.linkedInUrl) ||
    text(contact.profileUrl) ||
    text(opp.linkedinUrl) ||
    null
  );
}

function contactSlots(opp) {
  const slots = [];
  const primary =
    opp.primaryContact ||
    opp.contact ||
    (opp.contactName
      ? {
          name: opp.contactName || opp.primaryContactName,
          email: opp.contactEmail || opp.primaryContactEmail,
          phone: opp.contactPhone,
          title: opp.contactRole || opp.displayRole,
          role: opp.contactRole,
        }
      : null);
  if (primary) slots.push({ slot: "primary", contact: primary });
  if (opp.secondaryContact) slots.push({ slot: "secondary", contact: opp.secondaryContact });
  return slots;
}

function classifyRelevance(contact, opp) {
  const name = text(contact.name || contact.fullName);
  if (!looksLikePersonName(name)) {
    return {
      named: false,
      commercialRelevanceClass: "NO_NAMED_PERSON",
      buyerVsEntry: null,
    };
  }
  const klass = text(
    contact.contactClass || contact.contactClassification || "",
  ).toUpperCase();
  let eligClass = null;
  try {
    // Signature: classifyContactEligibility(person, opportunity)
    const elig = classifyContactEligibility(contact, opp);
    eligClass = text(elig?.class || elig?.contactEligibilityClass || "").toUpperCase();
  } catch {
    eligClass = null;
  }
  const resolved = RELEVANT.has(klass)
    ? klass
    : RELEVANT.has(eligClass)
      ? eligClass
      : eligClass || klass || "UNSPECIFIED";

  let buyerVsEntry = "UNSPECIFIED";
  if (resolved === "DIRECT_BUYER" || /DIRECT_BUYER|VERIFIED_HOTEL_BUYER|BOOKING/.test(resolved)) {
    buyerVsEntry = "DIRECT_BUYER";
  } else if (
    resolved === "RELEVANT_COMMERCIAL_ENTRY_POINT" ||
    /ENTRY_POINT|INFLUENCER|INTERMEDIARY|REFERRAL/.test(resolved)
  ) {
    buyerVsEntry = "RELEVANT_COMMERCIAL_ENTRY_POINT";
  } else if (resolved === "RESEARCH_CANDIDATE" || klass === "RESEARCH_CANDIDATE") {
    buyerVsEntry = "RESEARCH_CANDIDATE";
  }

  const title = text(contact.title || contact.role);
  const commerciallyRelevant =
    buyerVsEntry === "DIRECT_BUYER" ||
    buyerVsEntry === "RELEVANT_COMMERCIAL_ENTRY_POINT" ||
    (buyerVsEntry === "RESEARCH_CANDIDATE" &&
      /\b(meeting|meetings|event|events|housing|hotel|lodging|conference|congress|procurement|travel|planner|organizer|organiser|association manager|program manager)\b/i.test(
        title,
      ));

  return {
    named: true,
    name,
    title: title || null,
    organization: text(contact.organization) || null,
    commercialRelevanceClass: resolved || "UNSPECIFIED",
    buyerVsEntry,
    commerciallyRelevant: Boolean(commerciallyRelevant),
    eligibilityClass: eligClass,
  };
}

function stripContactChannelFields(contact) {
  if (!contact || typeof contact !== "object") return contact;
  const {
    email: _e,
    workEmail: _w,
    businessEmail: _b,
    emailAddress: _a,
    phone: _p,
    directPhone: _d,
    workPhone: _wp,
    officePhone: _o,
    phoneExtensions: _x,
    ...rest
  } = contact;
  return rest;
}

/**
 * Evidence blob for recovery detection — excludes the contact's own channel
 * fields so presence of email on primaryContact does not count as provenance.
 */
function collectOppEvidenceBlob(opp) {
  const parts = [];
  const push = (v) => {
    if (v == null) return;
    if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
      parts.push(String(v));
      return;
    }
    try {
      parts.push(JSON.stringify(v));
    } catch {
      /* ignore */
    }
  };
  for (const k of [
    "sources",
    "evidence",
    "supportingEvidenceDrawer",
    "sourceUrls",
    "contactEvidence",
    "salesActionReason",
    "whatIsHappening",
    "summaryWhat",
    "contactAuditNote",
    "researchNotes",
    "verificationNotes",
    "drawerSections",
    "overview",
    "recommendedNextStep",
  ]) {
    push(opp[k]);
  }
  // Include non-channel contact metadata (name/title/why) only
  push(stripContactChannelFields(opp.primaryContact));
  push(stripContactChannelFields(opp.secondaryContact));
  push(opp.primaryContact?.whyThisContact);
  push(opp.primaryContact?.whyRelevant);
  push(opp.primaryContact?.relevanceEvidence);
  push(opp.primaryContact?.relevance);
  return parts.join("\n");
}

function collectOppUrls(opp, contact = {}) {
  const urls = new Set();
  const add = (u) => {
    const s = text(u);
    if (!s) return;
    if (/^https?:\/\//i.test(s)) urls.add(s);
  };
  add(contact.sourceUrl);
  add(contact.channelSourceUrl);
  add(contact.officialContactUrl);
  add(opp.contactSourceUrl);
  add(opp.officialContactUrl);
  add(opp.eventDateEvidenceUrl);
  add(opp.attendanceEvidenceUrl);
  add(opp.peakRoomsEvidenceUrl);
  const walk = (node, depth = 0) => {
    if (depth > 6 || node == null) return;
    if (typeof node === "string") {
      add(node);
      return;
    }
    if (Array.isArray(node)) {
      for (const x of node) walk(x, depth + 1);
      return;
    }
    if (typeof node === "object") {
      for (const [k, v] of Object.entries(node)) {
        if (/url|href|source|link/i.test(k)) add(v);
        else walk(v, depth + 1);
      }
    }
  };
  walk(opp.sources);
  walk(opp.evidence);
  walk(opp.supportingEvidenceDrawer);
  return [...urls];
}

function emailAppearsInBlob(email, blob) {
  if (!email || !blob) return false;
  const lower = blob.toLowerCase();
  if (lower.includes(email.toLowerCase())) return true;
  // Also match escaped / HTML-entity forms without inventing
  const at = email.indexOf("@");
  if (at > 0) {
    const local = email.slice(0, at);
    const domain = email.slice(at + 1);
    if (lower.includes(`${local} [at] ${domain}`) || lower.includes(`${local}(at)${domain}`)) {
      return true;
    }
  }
  return false;
}

function channelProvenance(contact, email, opp) {
  const ownership = text(contact.emailOwnership).toLowerCase();
  const emailKind = text(contact.emailKind).toUpperCase();
  const origin = text(contact.contactDataOrigin || contact.dataOrigin);
  const verification = text(
    contact.channelVerificationStatus || contact.emailVerificationStatus || "",
  ).toUpperCase();
  const sourceUrl =
    text(contact.channelSourceUrl) ||
    text(contact.sourceUrl) ||
    text(contact.officialContactUrl) ||
    null;
  const providerBlocked =
    contact.surfeDerived === true ||
    /APOLLO|PROSPEO|PDL|SURFE|FULLENRICH|PROVIDER_ENRICHED/i.test(origin);

  if (!email) {
    return { status: "NO_EMAIL", sourceUrl, ownership, emailKind, origin, verification };
  }
  if (providerBlocked) {
    return {
      status: "PROVIDER_BLOCKED",
      sourceUrl,
      ownership,
      emailKind,
      origin,
      verification,
    };
  }
  if (isGenericMailbox(email) || emailKind === "ORGANIZER_MAILBOX" || ownership === "organization_or_department") {
    return {
      status: "GENERIC_OR_ORG_MAILBOX",
      sourceUrl,
      ownership,
      emailKind,
      origin,
      verification,
    };
  }
  if (
    verification === "PUBLICLY_VERIFIED" ||
    /^PUBLIC_SOURCE$/i.test(origin) ||
    (sourceUrl && (emailKind === "NAMED_DIRECT" || ownership === "person_specific" || !isGenericMailbox(email)))
  ) {
    return {
      status: "SOURCE_CONFIRMED",
      sourceUrl,
      ownership,
      emailKind,
      origin,
      verification,
    };
  }
  if (ownership === "person_specific" && !sourceUrl) {
    return {
      status: "PERSON_SPECIFIC_WITHOUT_SOURCE_URL",
      sourceUrl,
      ownership,
      emailKind,
      origin,
      verification,
    };
  }
  return {
    status: "UNVERIFIED_EMAIL",
    sourceUrl,
    ownership,
    emailKind,
    origin,
    verification,
  };
}

/**
 * Actionability tiers (founder-approved design — not a publication gate).
 * Opportunity eligibility / publication remain independent.
 */
function classifyActionability(opp) {
  const slots = contactSlots(opp).map(({ slot, contact }) => {
    const rel = classifyRelevance(contact, opp);
    const email = extractEmail(contact);
    const phone = extractPhone(contact);
    const linkedIn = extractLinkedIn(contact, opp);
    const prov = channelProvenance(contact, email, opp);
    const identityKey =
      rel.named && rel.name
        ? personIdentityKey({
            name: rel.name,
            organization: rel.organization || contact.organization,
            domain: email ? email.split("@")[1] : null,
          })
        : null;
    return {
      slot,
      ...rel,
      emailPresent: Boolean(email),
      emailDomain: email ? email.split("@")[1] : null,
      // Never echo full email into founder markdown unnecessarily — hash for join
      emailFingerprint: email
        ? createHash("sha256").update(email).digest("hex").slice(0, 12)
        : null,
      phonePresent: Boolean(phone),
      linkedInPresent: Boolean(linkedIn),
      provenance: prov,
      identityKey,
    };
  });

  const anyNamed = slots.some((s) => s.named);
  const anyRelevant = slots.some((s) => s.commerciallyRelevant);
  const anyConfirmedEmail = slots.some(
    (s) => s.emailPresent && s.provenance.status === "SOURCE_CONFIRMED" && s.commerciallyRelevant,
  );
  const anyReachablePhone = slots.some(
    (s) => s.phonePresent && s.commerciallyRelevant && s.named,
  );
  const anyPersonSpecificNoSource = slots.some(
    (s) => s.provenance.status === "PERSON_SPECIFIC_WITHOUT_SOURCE_URL",
  );

  let actionabilityTier = "CONTACT_IDENTIFICATION_REQUIRED";
  if (anyConfirmedEmail && anyRelevant) actionabilityTier = "SALES_READY";
  else if (anyReachablePhone && anyRelevant) actionabilityTier = "REACHABLE";
  else if (anyNamed) actionabilityTier = "CONTACT_RESEARCH_REQUIRED";
  else actionabilityTier = "CONTACT_IDENTIFICATION_REQUIRED";

  return {
    opportunityId: text(opp.opportunityId || opp.id),
    title: text(opp.title || opp.name || opp.displayTitle || ""),
    priority: text(opp.priority || opp.priorityFilterKey || ""),
    commercialClassification: text(
      opp.commercialClassification || opp.commercialInventoryClass || "",
    ),
    // Publication / eligibility stay independent of actionability
    customerPublicationEligible: true, // row is already in customer-visible frozen set
    opportunityEligibilityUnchanged: true,
    actionabilityTier,
    slots,
    flags: {
      personSpecificWithoutSource: anyPersonSpecificNoSource,
      hasConfirmedEmail: anyConfirmedEmail,
      hasReachablePhone: anyReachablePhone,
      hasNamedContact: anyNamed,
      hasCommerciallyRelevant: anyRelevant,
    },
  };
}

function assessBethesdaEmailRecovery(opp, slotEval) {
  const contact =
    slotEval.slot === "secondary" ? opp.secondaryContact : opp.primaryContact || opp.contact || {};
  const email = extractEmail(contact);
  const blob = collectOppEvidenceBlob(opp);
  const urls = collectOppUrls(opp, contact);
  const emailInFrozenEvidence = emailAppearsInBlob(email, blob);
  const emailInAnyUrlString = urls.some((u) => email && u.toLowerCase().includes(email));

  let recoveryClass = "UNSUPPORTED_IN_FROZEN_PACKAGE";
  let nextStep = "PUBLIC_REVERIFY_OR_HOLD_AS_UNVERIFIED";
  const reasons = [];

  if (!email) {
    recoveryClass = "NO_EMAIL";
    nextStep = "IDENTIFY_OR_RESEARCH_CHANNEL";
    reasons.push("no_email");
  } else if ((emailInFrozenEvidence || emailInAnyUrlString) && urls.length > 0) {
    recoveryClass = "RECOVERABLE_ATTACH_EXISTING_URL";
    nextStep = "ATTACH_EXISTING_SOURCE_URL_WITHOUT_INVENTION";
    reasons.push(
      emailInFrozenEvidence
        ? "email_literal_in_frozen_evidence_and_url_candidates_exist"
        : "email_embedded_in_url",
    );
  } else if (emailInFrozenEvidence && urls.length === 0) {
    // Email appears in narrative/prose but no URL to attribute — do NOT invent a source
    recoveryClass = "EMAIL_LITERAL_IN_PACKAGE_TEXT_NO_ATTRIBUTABLE_URL";
    nextStep = "PUBLIC_DISCOVERY_TO_FIND_ATTRIBUTABLE_SOURCE_PAGE";
    reasons.push("email_literal_in_non_channel_text_but_no_url_to_attach");
  } else if (urls.length > 0) {
    recoveryClass = "REVERIFIABLE_CANDIDATE_URLS_PRESENT";
    nextStep = "BOUNDED_PUBLIC_FETCH_OF_EXISTING_URLS_ONLY";
    reasons.push("opportunity_has_urls_but_email_not_literal_in_frozen_blob");
  } else if (
    text(contact.organization) ||
    text(slotEval.organization) ||
    (email && email.includes("@"))
  ) {
    recoveryClass = "REVERIFIABLE_VIA_PUBLIC_DISCOVERY";
    nextStep = "GOOGLE_FIRST_PUBLIC_DISCOVERY_THEN_VALIDATE";
    reasons.push("named_person_and_org_or_domain_present_no_frozen_source");
  } else {
    recoveryClass = "UNSUPPORTED_IN_FROZEN_PACKAGE";
    nextStep = "HOLD_UNVERIFIED_NO_CUSTOMER_VERIFIED_BADGE";
    reasons.push("insufficient_identity_anchors");
  }

  // Never claim recovery by pattern inference
  const forbidden = {
    inferredEmailPattern: false,
    fabricatedSourceUrl: false,
    providerSpendAuthorized: false,
  };

  return {
    opportunityId: text(opp.opportunityId || opp.id),
    title: text(opp.title || opp.name || ""),
    priority: text(opp.priority || ""),
    slot: slotEval.slot,
    personName: slotEval.name || null,
    organization: slotEval.organization || null,
    buyerVsEntry: slotEval.buyerVsEntry,
    commerciallyRelevant: slotEval.commerciallyRelevant,
    emailFingerprint: slotEval.emailFingerprint,
    emailDomain: slotEval.emailDomain,
    phonePresent: slotEval.phonePresent,
    linkedInPresent: slotEval.linkedInPresent,
    ownership: slotEval.provenance.ownership || null,
    frozenSourceUrlOnContact: slotEval.provenance.sourceUrl || null,
    candidateUrlsInOpportunity: urls.slice(0, 12),
    candidateUrlCount: urls.length,
    emailLiteralInFrozenEvidence: emailInFrozenEvidence,
    recoveryClass,
    nextStep,
    reasons,
    forbidden,
    potentialTierIfRecovered:
      slotEval.commerciallyRelevant && email
        ? "SALES_READY"
        : slotEval.phonePresent && slotEval.commerciallyRelevant
          ? "REACHABLE"
          : "CONTACT_RESEARCH_REQUIRED",
  };
}

function loadCustomerVisibleRows(store, registry) {
  const rows = [];
  const hotelIds = fs
    .readdirSync(
      path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages"),
      { withFileTypes: true },
    )
    .filter((d) => d.isDirectory() && d.name.startsWith("rec"))
    .map((d) => d.name);

  for (const hotelId of hotelIds) {
    const pointer = store.readPointer(hotelId, "CURRENT");
    if (!pointer || pointer.withheld) continue;
    const loaded = store.loadPackage(hotelId, pointer.publicationVersion);
    if (!loaded.ok) continue;
    const hotelName = text(loaded.package.hotelName) || HOTEL_NAMES[hotelId] || hotelId;
    for (const opp of loaded.package.opportunities || []) {
      rows.push({
        hotelId,
        hotelName,
        source: "CURRENT",
        publicationVersion: loaded.package.publicationVersion,
        opp,
        classified: classifyActionability(opp),
      });
    }
  }

  let listed = [];
  try {
    listed = registry.listAllHotelsPublished({ includeRevoked: false }) || [];
  } catch {
    listed = [];
  }
  for (const r of listed) {
    const hotelId = text(r.hotelId);
    if (rows.some((x) => x.hotelId === hotelId && x.classified.opportunityId === r.opportunityId)) {
      continue;
    }
    let snap = null;
    try {
      snap = registry.loadSnapshot(hotelId, r.opportunityId, r.publishedVersion);
    } catch {
      snap = null;
    }
    const nested = snap?.opportunity || snap?.snapshot || snap || {};
    const opp = {
      ...nested,
      opportunityId: r.opportunityId || nested.opportunityId || nested.id,
      commercialClassification: r.commercialClassification || nested.commercialClassification,
      primaryContact:
        nested.primaryContact ||
        nested.contact ||
        (nested.contactName
          ? {
              name: nested.contactName,
              email: nested.contactEmail,
              phone: nested.contactPhone,
              title: nested.contactRole,
            }
          : null),
    };
    rows.push({
      hotelId,
      hotelName: HOTEL_NAMES[hotelId] || hotelId,
      source: "REGISTRY_PUBLISHED",
      publicationVersion: `registry:v${r.publishedVersion}`,
      opp,
      classified: classifyActionability(opp),
    });
  }
  return rows;
}

function summarizeByHotel(rows) {
  const by = new Map();
  for (const row of rows) {
    if (!by.has(row.hotelId)) {
      by.set(row.hotelId, {
        hotelId: row.hotelId,
        hotelName: row.hotelName,
        total: 0,
        tiers: {
          SALES_READY: 0,
          REACHABLE: 0,
          CONTACT_RESEARCH_REQUIRED: 0,
          CONTACT_IDENTIFICATION_REQUIRED: 0,
        },
        personSpecificWithoutSource: 0,
        directBuyer: 0,
        entryPoint: 0,
        researchCandidate: 0,
      });
    }
    const h = by.get(row.hotelId);
    h.total += 1;
    h.tiers[row.classified.actionabilityTier] =
      (h.tiers[row.classified.actionabilityTier] || 0) + 1;
    if (row.classified.flags.personSpecificWithoutSource) h.personSpecificWithoutSource += 1;
    for (const s of row.classified.slots) {
      if (s.buyerVsEntry === "DIRECT_BUYER") h.directBuyer += 1;
      if (s.buyerVsEntry === "RELEVANT_COMMERCIAL_ENTRY_POINT") h.entryPoint += 1;
      if (s.buyerVsEntry === "RESEARCH_CANDIDATE") h.researchCandidate += 1;
    }
  }
  return [...by.values()];
}

function crossHotelDuplication(rows) {
  const byIdentity = new Map();
  for (const row of rows) {
    for (const s of row.classified.slots) {
      if (!s.identityKey || !s.named) continue;
      if (!byIdentity.has(s.identityKey)) {
        byIdentity.set(s.identityKey, {
          identityKey: s.identityKey,
          name: s.name,
          organization: s.organization,
          hotels: new Set(),
          opportunityIds: [],
        });
      }
      const rec = byIdentity.get(s.identityKey);
      rec.hotels.add(row.hotelId);
      rec.opportunityIds.push({
        hotelId: row.hotelId,
        hotelName: row.hotelName,
        opportunityId: row.classified.opportunityId,
        title: row.classified.title,
        actionabilityTier: row.classified.actionabilityTier,
      });
    }
  }
  const dupes = [...byIdentity.values()]
    .map((d) => ({
      identityKey: d.identityKey,
      name: d.name,
      organization: d.organization,
      hotelCount: d.hotels.size,
      hotels: [...d.hotels].map((id) => HOTEL_NAMES[id] || id),
      opportunityCount: d.opportunityIds.length,
      opportunities: d.opportunityIds,
    }))
    .filter((d) => d.hotelCount > 1 || d.opportunityCount > 1)
    .sort(
      (a, b) =>
        b.hotelCount - a.hotelCount ||
        b.opportunityCount - a.opportunityCount ||
        String(a.name).localeCompare(String(b.name)),
    );
  return {
    sharedAcrossHotels: dupes.filter((d) => d.hotelCount > 1),
    repeatedWithinOrAcross: dupes,
    estimatedDuplicateResearchPairs: dupes
      .filter((d) => d.hotelCount > 1)
      .reduce((n, d) => n + (d.opportunityCount - 1), 0),
  };
}

function main() {
  const leakage = fs.existsSync(LEAKAGE)
    ? JSON.parse(fs.readFileSync(LEAKAGE, "utf8"))
    : null;
  const store = createGdiPublicationPackageStore();
  const registry = createGdiPublishedInventoryRegistry();
  const rows = loadCustomerVisibleRows(store, registry);

  // Deduplicate hotel+opp
  const seen = new Set();
  const deduped = [];
  for (const row of rows) {
    const key = `${row.hotelId}::${row.classified.opportunityId}`;
    if (!row.classified.opportunityId || seen.has(key)) continue;
    seen.add(key);
    deduped.push(row);
  }

  const hotelSummary = summarizeByHotel(deduped);
  const portfolioTiers = {
    SALES_READY: 0,
    REACHABLE: 0,
    CONTACT_RESEARCH_REQUIRED: 0,
    CONTACT_IDENTIFICATION_REQUIRED: 0,
  };
  for (const r of deduped) {
    portfolioTiers[r.classified.actionabilityTier] += 1;
  }

  // Bethesda 34 cohort
  const bethRows = deduped.filter((r) => r.hotelId === BETHESDA);
  const bethRecovery = [];
  for (const row of bethRows) {
    for (const slot of row.classified.slots) {
      if (slot.provenance?.status === "PERSON_SPECIFIC_WITHOUT_SOURCE_URL") {
        bethRecovery.push(assessBethesdaEmailRecovery(row.opp, slot));
      }
    }
  }

  const recoveryCounts = bethRecovery.reduce((acc, r) => {
    acc[r.recoveryClass] = (acc[r.recoveryClass] || 0) + 1;
    return acc;
  }, {});

  const upgradeCandidates = {
    toSalesReadyIfEvidenceRecovered: bethRecovery.filter(
      (r) => r.potentialTierIfRecovered === "SALES_READY",
    ).length,
    alreadyReachableViaPhone: bethRecovery.filter((r) => r.phonePresent).length,
  };

  const duplication = crossHotelDuplication(deduped);

  // Cost model (free-first; paid only if separately authorized)
  const costModel = {
    currency: "USD",
    assumptions: {
      googleFirstDiscoveryPerContactUsd: 0.05,
      publicPageFetchValidateUsd: 0.0,
      analystMinutesRecoverableFromFrozen: 3,
      analystMinutesReverifyUrls: 8,
      analystMinutesPublicDiscovery: 15,
      analystMinutesUnsupportedCloseout: 5,
      analystHourlyUsd: 75,
      paidProviderPerSuccessUsd: null,
      paidProviderAuthorized: false,
    },
    estimatedCostPerSuccessfulUpgradeUsd: {
      recoverableFromFrozen: Number(((3 / 60) * 75).toFixed(2)),
      reverifyExistingUrls: Number(((8 / 60) * 75 + 0.05).toFixed(2)),
      publicDiscoveryThenValidate: Number(((15 / 60) * 75 + 0.05).toFixed(2)),
    },
    bethCohortBudgetCapSuggestionUsd: {
      phaseA_attachExistingUrlOnly: Number(
        (
          (recoveryCounts.RECOVERABLE_ATTACH_EXISTING_URL || 0) *
          ((3 / 60) * 75)
        ).toFixed(2),
      ),
      phaseB_publicDiscoveryForNarrativeLiteralsAndMissingSources: Number(
        (
          ((recoveryCounts.EMAIL_LITERAL_IN_PACKAGE_TEXT_NO_ATTRIBUTABLE_URL ||
            0) +
            (recoveryCounts.REVERIFIABLE_VIA_PUBLIC_DISCOVERY || 0) +
            (recoveryCounts.REVERIFIABLE_CANDIDATE_URLS_PRESENT || 0)) *
          ((15 / 60) * 75 + 0.05)
        ).toFixed(2),
      ),
      paidProviders: 0,
    },
  };

  const beforeAfter = {
    customerVisibleBefore: deduped.length,
    customerVisibleAfterIfTiersOnly: deduped.length,
    delta: 0,
    note:
      "EXISTING_RULES_WITH_CONTACT_TIERS does not remove opportunities from customer publication. Actionability tiers are labels/workflow only.",
    leakageBaselineVisible: leakage?.portfolioTotals?.currentlyCustomerVisible ?? null,
    baselineMatch: leakage
      ? leakage.portfolioTotals.currentlyCustomerVisible === deduped.length
      : null,
  };

  const contract = {
    schema: "gdi-contact-actionability-contract-v1",
    status: "PROPOSED_AWAITING_FOUNDER_APPROVAL",
    nonGoals: [
      "STRICT_EMAIL is NOT a customer-publication gate",
      "Actionability does NOT mutate opportunity eligibility",
      "Actionability does NOT change CURRENT/STAGED pointers",
      "Provider-enriched channels are not customer-verified by default",
    ],
    separationOfConcerns: {
      opportunityEligibility: "commercial demand + hotel fit + contact eligibility class",
      contactVerification: "named person identity + source-backed channels",
      contactActionability:
        "SALES_READY | REACHABLE | CONTACT_RESEARCH_REQUIRED | CONTACT_IDENTIFICATION_REQUIRED",
      customerPublication: "founder-authorized CURRENT / registry PUBLISHED only",
    },
    tiers: {
      SALES_READY: {
        requires: [
          "named person",
          "commercially relevant (DIRECT_BUYER or RELEVANT_COMMERCIAL_ENTRY_POINT — labeled distinctly)",
          "person-specific business email with independent source confirmation",
        ],
        customerLabel: "Sales-ready contact",
        forbids: [
          "inferred email patterns",
          "generic/org mailboxes counted as personal email",
          "fabricated source URLs",
          "provider-only channels without display rights",
        ],
      },
      REACHABLE: {
        requires: [
          "named commercially relevant person",
          "usable person-specific business phone or office extension with evidence",
        ],
        customerLabel: "Reachable contact",
        note: "Email may still be CONTACT_RESEARCH_REQUIRED while phone keeps REACHABLE",
      },
      CONTACT_RESEARCH_REQUIRED: {
        requires: ["named person identified"],
        customerLabel: "Contact research needed",
        includes: [
          "person_specific email without source URL",
          "LinkedIn-only",
          "generic mailbox only",
          "research candidates lacking remit evidence",
        ],
      },
      CONTACT_IDENTIFICATION_REQUIRED: {
        requires: [],
        customerLabel: "Contact not yet identified",
        when: "no verified named person",
      },
    },
    buyerVsEntryPoint: {
      DIRECT_BUYER: "Hotel/travel/meetings/lodging/purchasing remit",
      RELEVANT_COMMERCIAL_ENTRY_POINT:
        "Verified commercially relevant intermediary/referral — valid contact, never mislabeled as direct decision-maker",
      rule: "Both may achieve SALES_READY actionability when channels are source-confirmed; UI must show distinct role labels",
    },
    sharedContactModel: {
      identityKey: "personIdentityKey(name|organization|domain)",
      reuse: "Verified person channels reusable across hotels",
      hotelSpecific: "Commercial relevance / buyerVsEntry re-evaluated per opportunity-hotel pair",
    },
    researchLimits: {
      minAttemptsBeforeStop: 1,
      maxPublicDiscoveryPassesPerContact: 2,
      maxUrlReverifyFetchesPerContact: 5,
      stopWhen: [
        "two public discovery passes yield no person-specific channel",
        "all candidate official URLs fetched and email/phone absent",
        "identity mismatch / lookalike LinkedIn quarantine",
        "only generic mailbox remains after named-person search",
      ],
      paidProviders: "HOLD until separately justified bounded budget",
    },
  };

  const result = {
    schema: "gdi-contact-actionability-evidence-recovery-audit-v1",
    generatedAt: new Date().toISOString(),
    mode: "READ_ONLY",
    founderDecisionHonored: {
      existingRulesWithContactTiers: true,
      strictEmailPublicationGate: false,
    },
    baseline: {
      customerVisibleCount: deduped.length,
      leakageAuditGeneratedAt: leakage?.generatedAt || null,
      baselineMatch: beforeAfter.baselineMatch,
    },
    beforeAfterInventory: beforeAfter,
    portfolioActionability: portfolioTiers,
    hotelByHotel: hotelSummary,
    bethesdaEvidenceRecovery: {
      cohortSize: bethRecovery.length,
      leakageReportedPersonSpecificWithoutSource:
        leakage?.hotelByHotel?.find((h) => h.hotelId === BETHESDA)
          ?.personSpecificEmailWithoutSourceUrl ?? null,
      recoveryCounts,
      upgradeCandidates,
      records: bethRecovery,
    },
    crossHotelDuplication: {
      sharedAcrossHotelsCount: duplication.sharedAcrossHotels.length,
      estimatedDuplicateResearchPairs: duplication.estimatedDuplicateResearchPairs,
      topShared: duplication.sharedAcrossHotels.slice(0, 15),
    },
    costModel,
    contract,
    stop: true,
  };

  fs.mkdirSync(OUT, { recursive: true });
  writeJson(path.join(OUT, "AUDIT.json"), result);
  writeJson(path.join(OUT, "BETHESDA_EVIDENCE_RECOVERY.json"), {
    generatedAt: result.generatedAt,
    cohortSize: bethRecovery.length,
    recoveryCounts,
    upgradeCandidates,
    records: bethRecovery,
  });
  writeJson(path.join(OUT, "ACTIONABILITY_CONTRACT_V1.json"), contract);
  writeJson(path.join(OUT, "PORTFOLIO_IMPACT.json"), {
    beforeAfterInventory: beforeAfter,
    portfolioActionability: portfolioTiers,
    hotelByHotel: hotelSummary,
    crossHotelDuplication: result.crossHotelDuplication,
    costModel,
  });

  const md = renderFounderReport(result);
  fs.writeFileSync(path.join(OUT, "FOUNDER_DECISION_REPORT.md"), md, "utf8");

  console.log(
    JSON.stringify(
      {
        ok: true,
        outDir: OUT,
        customerVisible: deduped.length,
        baselineMatch: beforeAfter.baselineMatch,
        portfolioActionability: portfolioTiers,
        bethesdaCohort: bethRecovery.length,
        recoveryCounts,
        inventoryDelta: 0,
        stop: true,
      },
      null,
      2,
    ),
  );
}

function renderFounderReport(result) {
  const c = result.contract;
  const b = result.bethesdaEvidenceRecovery;
  const p = result.portfolioActionability;
  const lines = [];
  lines.push("# GDI Contact Actionability & Evidence Recovery V1 — Founder Decision Report");
  lines.push("");
  lines.push(`Generated: ${result.generatedAt}`);
  lines.push("");
  lines.push(
    "**Status:** READ-ONLY design + evidence audit · **STOP** before implementation or spending",
  );
  lines.push("");
  lines.push("## Founder decisions honored");
  lines.push("");
  lines.push("- APPROVED design direction: `EXISTING_RULES_WITH_CONTACT_TIERS`");
  lines.push("- REJECTED as publication gate: `STRICT_EMAIL`");
  lines.push("- No publication authorization changes");
  lines.push("- No paid provider calls in this phase");
  lines.push("- No fabricated / inferred / backfilled source evidence");
  lines.push("");
  lines.push("## Exact inventory (before / after)");
  lines.push("");
  lines.push(
    `| Measure | Count |`,
  );
  lines.push("|---|---:|");
  lines.push(
    `| Customer-visible opportunities (frozen baseline) | ${result.beforeAfterInventory.customerVisibleBefore} |`,
  );
  lines.push(
    `| After adopting tiers-only (no gate change) | ${result.beforeAfterInventory.customerVisibleAfterIfTiersOnly} |`,
  );
  lines.push(`| Unauthorized inventory delta | **${result.beforeAfterInventory.delta}** |`);
  lines.push(
    `| Matches leakage audit baseline (182) | ${result.baseline.baselineMatch === true ? "YES" : result.baseline.baselineMatch === false ? "NO — investigate" : "leakage file missing"} |`,
  );
  lines.push("");
  lines.push(result.beforeAfterInventory.note);
  lines.push("");
  lines.push("## 1. Proposed contact-actionability data contract");
  lines.push("");
  lines.push("### Separation of concerns");
  lines.push("");
  for (const [k, v] of Object.entries(c.separationOfConcerns)) {
    lines.push(`- **${k}:** ${v}`);
  }
  lines.push("");
  lines.push("### Tiers");
  lines.push("");
  lines.push("| Tier | Customer label | Requires |");
  lines.push("|---|---|---|");
  for (const [tier, def] of Object.entries(c.tiers)) {
    lines.push(
      `| \`${tier}\` | ${def.customerLabel} | ${(def.requires || def.when && [def.when] || ["—"]).join("; ")} |`,
    );
  }
  lines.push("");
  lines.push("### DIRECT_BUYER vs RELEVANT_COMMERCIAL_ENTRY_POINT");
  lines.push("");
  lines.push(`- **DIRECT_BUYER:** ${c.buyerVsEntryPoint.DIRECT_BUYER}`);
  lines.push(
    `- **RELEVANT_COMMERCIAL_ENTRY_POINT:** ${c.buyerVsEntryPoint.RELEVANT_COMMERCIAL_ENTRY_POINT}`,
  );
  lines.push(`- **Rule:** ${c.buyerVsEntryPoint.rule}`);
  lines.push("");
  lines.push("### Shared contact reuse");
  lines.push("");
  lines.push(`- Identity: \`${c.sharedContactModel.identityKey}\``);
  lines.push(`- ${c.sharedContactModel.reuse}`);
  lines.push(`- ${c.sharedContactModel.hotelSpecific}`);
  lines.push("");
  lines.push("### Hard forbids (Sales Ready)");
  lines.push("");
  for (const f of c.tiers.SALES_READY.forbids) lines.push(`- ${f}`);
  lines.push("");
  lines.push("Machine-readable: `ACTIONABILITY_CONTRACT_V1.json`");
  lines.push("");
  lines.push("## 2. Bethesda evidence-recovery findings");
  lines.push("");
  lines.push(
    `Cohort: **${b.cohortSize}** person-specific emails without frozen contact source URL (leakage reported ${b.leakageReportedPersonSpecificWithoutSource}).`,
  );
  lines.push("");
  lines.push("| Recovery class | Count |");
  lines.push("|---|---:|");
  for (const [k, v] of Object.entries(b.recoveryCounts).sort()) {
    lines.push(`| ${k} | ${v} |`);
  }
  lines.push("");
  lines.push(
    `- Potential SALES_READY if evidence recovered: **${b.upgradeCandidates.toSalesReadyIfEvidenceRecovered}**`,
  );
  lines.push(
    `- Already phone-reachable within cohort: **${b.upgradeCandidates.alreadyReachableViaPhone}**`,
  );
  lines.push("");
  lines.push("### Definitions used (no invention)");
  lines.push("");
  lines.push(
    "- **RECOVERABLE_ATTACH_EXISTING_URL:** email literal found in frozen evidence AND at least one candidate URL exists — attach real URL only, never invent.",
  );
  lines.push(
    "- **EMAIL_LITERAL_IN_PACKAGE_TEXT_NO_ATTRIBUTABLE_URL:** email appears in narrative/prose but no URL to attribute — public discovery required; do not fabricate sourceUrl.",
  );
  lines.push(
    "- **REVERIFIABLE_CANDIDATE_URLS_PRESENT:** opportunity has official/evidence URLs but email not literal in package — bounded public fetch of those URLs only.",
  );
  lines.push(
    "- **REVERIFIABLE_VIA_PUBLIC_DISCOVERY:** named person + org/domain anchors exist — Google-first public discovery then validate.",
  );
  lines.push(
    "- **UNSUPPORTED_IN_FROZEN_PACKAGE:** insufficient anchors; hold unverified; no customer verified badge.",
  );
  lines.push("");
  lines.push("Detail: `BETHESDA_EVIDENCE_RECOVERY.json` (emails fingerprinted; full addresses not required for founder review).");
  lines.push("");
  lines.push("## 3. Portfolio-wide opportunity impact");
  lines.push("");
  lines.push("| Actionability tier | Opportunities |");
  lines.push("|---|---:|");
  for (const [k, v] of Object.entries(p)) lines.push(`| ${k} | ${v} |`);
  lines.push("");
  lines.push("### By hotel");
  lines.push("");
  lines.push(
    "| Hotel | Visible | SALES_READY | REACHABLE | RESEARCH_REQUIRED | IDENTIFICATION_REQUIRED | Person email w/o source |",
  );
  lines.push("|---|---:|---:|---:|---:|---:|---:|");
  for (const h of result.hotelByHotel) {
    lines.push(
      `| ${h.hotelName} | ${h.total} | ${h.tiers.SALES_READY} | ${h.tiers.REACHABLE} | ${h.tiers.CONTACT_RESEARCH_REQUIRED} | ${h.tiers.CONTACT_IDENTIFICATION_REQUIRED} | ${h.personSpecificWithoutSource} |`,
    );
  }
  lines.push("");
  lines.push("### Cross-hotel duplicated research");
  lines.push("");
  lines.push(
    `- Shared identities across ≥2 hotels: **${result.crossHotelDuplication.sharedAcrossHotelsCount}**`,
  );
  lines.push(
    `- Estimated duplicate research pairs avoidable via shared contact model: **${result.crossHotelDuplication.estimatedDuplicateResearchPairs}**`,
  );
  if (result.crossHotelDuplication.topShared?.length) {
    lines.push("");
    for (const d of result.crossHotelDuplication.topShared.slice(0, 8)) {
      lines.push(
        `- ${d.name || "(unnamed)"} @ ${d.organization || "—"} — hotels: ${d.hotels.join(", ")} (${d.opportunityCount} opps)`,
      );
    }
  } else {
    lines.push("");
    lines.push("_No cross-hotel shared person identities detected in the frozen customer-visible set._");
  }
  lines.push("");
  lines.push("## 4. Contact-recovery workflow & cost controls");
  lines.push("");
  lines.push("```");
  lines.push("1) Identity preflight (named person + employer/domain/LinkedIn)");
  lines.push("2) Scan frozen package evidence for literal email/phone (free)");
  lines.push("3) Re-fetch only existing opportunity URLs (bounded, free)");
  lines.push("4) Google-first public discovery → validate on official HTML");
  lines.push("5) STOP if stop-rules hit");
  lines.push("6) Paid providers ONLY with separate founder budget (HOLD now)");
  lines.push("```");
  lines.push("");
  lines.push("| Upgrade path | Est. cost / success |");
  lines.push("|---|---:|");
  lines.push(
    `| Recover from frozen evidence | $${result.costModel.estimatedCostPerSuccessfulUpgradeUsd.recoverableFromFrozen} |`,
  );
  lines.push(
    `| Re-verify existing URLs | $${result.costModel.estimatedCostPerSuccessfulUpgradeUsd.reverifyExistingUrls} |`,
  );
  lines.push(
    `| Public discovery + validate | $${result.costModel.estimatedCostPerSuccessfulUpgradeUsd.publicDiscoveryThenValidate} |`,
  );
  lines.push(`| Paid providers | HOLD (not authorized) |`);
  lines.push("");
  lines.push("### Suggested Bethesda cohort caps (analyst time only)");
  lines.push("");
  lines.push(
    `- Phase A attach existing URL only: ~$${result.costModel.bethCohortBudgetCapSuggestionUsd.phaseA_attachExistingUrlOnly}`,
  );
  lines.push(
    `- Phase B public discovery (34 − attachable): ~$${result.costModel.bethCohortBudgetCapSuggestionUsd.phaseB_publicDiscoveryForNarrativeLiteralsAndMissingSources}`,
  );
  lines.push(`- Paid: $0`);
  lines.push("");
  lines.push("### Research limits / stop rules");
  lines.push("");
  lines.push(
    `- Min attempts before stop classification: ${c.researchLimits.minAttemptsBeforeStop}`,
  );
  lines.push(
    `- Max public discovery passes / contact: ${c.researchLimits.maxPublicDiscoveryPassesPerContact}`,
  );
  lines.push(
    `- Max URL reverify fetches / contact: ${c.researchLimits.maxUrlReverifyFetchesPerContact}`,
  );
  for (const s of c.researchLimits.stopWhen) lines.push(`- Stop when: ${s}`);
  lines.push("");
  lines.push("## 5. Recommended UI treatment");
  lines.push("");
  lines.push("| Tier | Customer-visible label | Treatment |");
  lines.push("|---|---|---|");
  lines.push(
    "| SALES_READY | Sales-ready contact | Show verified email; badge Direct buyer **or** Commercial entry point |",
  );
  lines.push(
    "| REACHABLE | Reachable contact | Show verified phone; email CTA = research/find if missing |",
  );
  lines.push(
    "| CONTACT_RESEARCH_REQUIRED | Contact research needed | Named person visible; channels not marked verified; no fake source |",
  );
  lines.push(
    "| CONTACT_IDENTIFICATION_REQUIRED | Contact not yet identified | Prompt identification; do not invent names |",
  );
  lines.push("");
  lines.push(
    "Unsupported person-specific emails (no source): **do not** present as verified Direct Business Email. Keep internal ownership metadata; customer copy stays honest.",
  );
  lines.push("");
  lines.push("## 6. Tests & permanent regression gates (proposed)");
  lines.push("");
  lines.push("1. `test:gdi-contact-actionability-contract-v1` — tier classification fixtures");
  lines.push("2. `test:gdi-contact-actionability-no-strict-publication-gate` — publication eligibility ≠ actionability");
  lines.push("3. `test:gdi-bethesda-person-specific-without-source-quarantine` — no verified badge without sourceUrl");
  lines.push("4. `test:gdi-buyer-vs-entry-point-labeling` — intermediaries never labeled Direct buyer");
  lines.push("5. `test:gdi-shared-contact-identity-reuse` — personIdentityKey reuse across hotels");
  lines.push("6. `test:gdi-actionability-inventory-freeze` — 182 baseline count unchanged by tier labeling");
  lines.push("7. Leakage audit regression: STRICT leakage reported, not enforced");
  lines.push("");
  lines.push("## 7. Implementation sequence (after founder GO)");
  lines.push("");
  lines.push("| Step | Work | Acceptance |");
  lines.push("|---|---|---|");
  lines.push(
    "| 0 | Founder approves this report | Explicit GO; still no STRICT gate |",
  );
  lines.push(
    "| 1 | Land contract module + unit tests (no UI yet) | Gates 1–2 green; inventory delta 0 |",
  );
  lines.push(
    "| 2 | Compute tiers on read path / DTO only | Customer labels honest; no CURRENT switch |",
  );
  lines.push(
    "| 3 | Bethesda Phase A: attach only frozen-recoverable sources | Recovery class counts drop; no invention |",
  );
  lines.push(
    "| 4 | Bethesda Phase B: bounded URL reverify (free) | Reverified emails gain sourceUrl; stop-rules enforced |",
  );
  lines.push(
    "| 5 | Shared contact store design (identity key) | Cross-hotel reuse without duplicating research |",
  );
  lines.push(
    "| 6 | Optional paid ladder | Separate budget PR; caps ≤ authorized credits |",
  );
  lines.push("");
  lines.push("## Recommendation");
  lines.push("");
  lines.push(
    "Proceed with **tiers-as-workflow-labels** on top of existing publication rules. Prioritize Bethesda frozen-evidence recovery and URL reverification before any paid spend. Keep DIRECT_BUYER and RELEVANT_COMMERCIAL_ENTRY_POINT visually distinct. Do not activate STRICT_EMAIL as a publication gate.",
  );
  lines.push("");
  lines.push("## STOP");
  lines.push("");
  lines.push(
    "STOP for founder review. No implementation wiring, no provider spend, no merge/deploy/publication in this phase.",
  );
  lines.push("");
  return `${lines.join("\n")}\n`;
}

main();
