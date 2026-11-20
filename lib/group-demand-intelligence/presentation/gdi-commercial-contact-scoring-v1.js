/**
 * Dual independent scores for Commercial Contact Intelligence.
 * Evidence gates remain authoritative — scores never certify buyer authority.
 */
import {
  COMMERCIAL_CONTACT_CLASS,
  classifyCommercialContact,
} from "./gdi-commercial-contact-classification-v1.js";

export const GDI_COMMERCIAL_CONTACT_SCORING_V1 =
  "gdi-commercial-contact-scoring-v1";

export const CHANNEL_SCORE_WEIGHTS = Object.freeze({
  verifiedNamedBusinessEmail: 40,
  verifiedPersonPhoneOrExtension: 30,
  officialProfileOrCredibleRoute: 20,
  sourceProvenanceFreshness: 10,
});

export const COMMERCIAL_SCORE_WEIGHTS = Object.freeze({
  relevantBuyingAuthorityEvidence: 40,
  directRelationshipToDemand: 30,
  credibleInfluenceOrReferral: 20,
  currentRoleAndTiming: 10,
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function clamp(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(100, Math.round(x)));
}

function isFresh(iso, maxDays = 180) {
  const t = Date.parse(String(iso || ""));
  if (!Number.isFinite(t)) return false;
  const age = (Date.now() - t) / (86400 * 1000);
  return age >= 0 && age <= maxDays;
}

/**
 * CONTACT_CHANNEL_RECOVERY_SCORE (0–100) with component reasons.
 */
export function scoreContactChannelRecovery(person = {}, channels = {}) {
  const email = text(
    channels.email || person.email || person.workEmail || "",
  );
  const phone = text(
    channels.phone || person.phone || person.directPhone || "",
  );
  const extensions = Array.isArray(person.phoneExtensions)
    ? person.phoneExtensions
    : [];
  const profileUrl = text(
    channels.profileUrl ||
      person.officialContactUrl ||
      person.profileUrl ||
      person.sourceUrl ||
      person.channelSourceUrl ||
      "",
  );
  const sourceUrl = text(
    channels.sourceUrl || person.channelSourceUrl || person.sourceUrl || "",
  );
  const verifiedAt = text(
    channels.verifiedAt ||
      person.channelLastVerifiedAt ||
      person.verifiedAt ||
      "",
  );
  const origin = text(
    channels.contactDataOrigin || person.contactDataOrigin || "",
  );

  const components = [];
  let total = 0;

  if (email && email.includes("@")) {
    total += CHANNEL_SCORE_WEIGHTS.verifiedNamedBusinessEmail;
    components.push({
      key: "verifiedNamedBusinessEmail",
      points: CHANNEL_SCORE_WEIGHTS.verifiedNamedBusinessEmail,
      reason: `Verified named business email (${email})`,
      link: sourceUrl || null,
    });
  } else {
    components.push({
      key: "verifiedNamedBusinessEmail",
      points: 0,
      reason: "No verified named business email",
      link: null,
    });
  }

  if (phone || extensions.length) {
    total += CHANNEL_SCORE_WEIGHTS.verifiedPersonPhoneOrExtension;
    components.push({
      key: "verifiedPersonPhoneOrExtension",
      points: CHANNEL_SCORE_WEIGHTS.verifiedPersonPhoneOrExtension,
      reason: extensions.length
        ? `Verified office phone with extension (${extensions.join(" / ")})`
        : `Verified phone (${phone})`,
      link: sourceUrl || null,
    });
  } else {
    components.push({
      key: "verifiedPersonPhoneOrExtension",
      points: 0,
      reason: "No verified person-specific phone or extension",
      link: null,
    });
  }

  if (profileUrl || sourceUrl) {
    total += CHANNEL_SCORE_WEIGHTS.officialProfileOrCredibleRoute;
    components.push({
      key: "officialProfileOrCredibleRoute",
      points: CHANNEL_SCORE_WEIGHTS.officialProfileOrCredibleRoute,
      reason: "Official profile / credible source route available",
      link: profileUrl || sourceUrl,
    });
  } else {
    components.push({
      key: "officialProfileOrCredibleRoute",
      points: 0,
      reason: "No official profile or credible alternate route",
      link: null,
    });
  }

  const provenanceOk =
    (origin === "PUBLIC_SOURCE" || Boolean(sourceUrl)) &&
    (isFresh(verifiedAt) || Boolean(sourceUrl));
  if (provenanceOk) {
    total += CHANNEL_SCORE_WEIGHTS.sourceProvenanceFreshness;
    components.push({
      key: "sourceProvenanceFreshness",
      points: CHANNEL_SCORE_WEIGHTS.sourceProvenanceFreshness,
      reason: verifiedAt
        ? `Public provenance with verification date ${verifiedAt}`
        : "Public provenance with source URL",
      link: sourceUrl || null,
    });
  } else {
    components.push({
      key: "sourceProvenanceFreshness",
      points: 0,
      reason: "Missing provenance / freshness",
      link: null,
    });
  }

  return {
    version: GDI_COMMERCIAL_CONTACT_SCORING_V1,
    scoreType: "CONTACT_CHANNEL_RECOVERY_SCORE",
    score: clamp(total),
    max: 100,
    weights: CHANNEL_SCORE_WEIGHTS,
    components,
  };
}

/**
 * COMMERCIAL_CONTACT_RELEVANCE_SCORE (0–100) with component reasons.
 * Does not grant buyer authority — classification gates remain separate.
 */
export function scoreCommercialContactRelevance(
  person = {},
  classification = null,
  opts = {},
) {
  const classified =
    classification ||
    classifyCommercialContact(person, {
      evidence: opts.evidence || person.commercialEvidence,
      organizationName: opts.organizationName,
    });
  const e = classified.evidence || {};
  const components = [];
  let total = 0;

  const authorityPts = e.confirmedHotelSourcingResponsibility
    ? COMMERCIAL_SCORE_WEIGHTS.relevantBuyingAuthorityEvidence
    : e.confirmedMeetingOrVenueBuyingResponsibility ||
        e.confirmedTravelProcurementResponsibility ||
        e.confirmedHousingAccommodationResponsibility
      ? Math.round(
          COMMERCIAL_SCORE_WEIGHTS.relevantBuyingAuthorityEvidence * 0.85,
        )
      : 0;
  total += authorityPts;
  components.push({
    key: "relevantBuyingAuthorityEvidence",
    points: authorityPts,
    reason: authorityPts
      ? "Evidence supports relevant hotel / meetings / travel buying responsibility"
      : "No verified buying-authority evidence (title alone insufficient)",
    link: (e.supportingSourceUrls || [])[0] || null,
  });

  const demandPts = e.connectedToDemandActivity
    ? COMMERCIAL_SCORE_WEIGHTS.directRelationshipToDemand
    : 0;
  total += demandPts;
  components.push({
    key: "directRelationshipToDemand",
    points: demandPts,
    reason: demandPts
      ? "Direct relationship to the specific demand activity"
      : "Connection to this demand activity not evidenced",
    link: (e.supportingSourceUrls || [])[0] || null,
  });

  const influencePts =
    e.credibleReferralRoute || e.relevantOperationalOrCommercialInfluence
      ? COMMERCIAL_SCORE_WEIGHTS.credibleInfluenceOrReferral
      : 0;
  total += influencePts;
  components.push({
    key: "credibleInfluenceOrReferral",
    points: influencePts,
    reason: influencePts
      ? "Credible commercial influence or referral route"
      : "No credible influence / referral evidence",
    link: null,
  });

  const rolePts = e.currentOrganizationAndRole
    ? COMMERCIAL_SCORE_WEIGHTS.currentRoleAndTiming
    : 0;
  total += rolePts;
  components.push({
    key: "currentRoleAndTiming",
    points: rolePts,
    reason: rolePts
      ? "Current organization and role supported"
      : "Current role / timing not verified",
    link: null,
  });

  return {
    version: GDI_COMMERCIAL_CONTACT_SCORING_V1,
    scoreType: "COMMERCIAL_CONTACT_RELEVANCE_SCORE",
    score: clamp(total),
    max: 100,
    weights: COMMERCIAL_SCORE_WEIGHTS,
    components,
    classificationClass: classified.class,
    buyerAuthorityClaimed: classified.buyerAuthorityClaimed === true,
    // Explicit: high channel score must not flip this
    scoreCannotOverrideAuthorityGate: true,
  };
}

export function scoreCommercialContactCandidate(person = {}, opts = {}) {
  const classification = classifyCommercialContact(person, opts);
  const channel = scoreContactChannelRecovery(person, opts.channels || {});
  const commercial = scoreCommercialContactRelevance(
    person,
    classification,
    opts,
  );
  return {
    version: GDI_COMMERCIAL_CONTACT_SCORING_V1,
    personName: text(person.name) || null,
    classification,
    channelRecovery: channel,
    commercialRelevance: commercial,
    // Selection uses commercial first; channel is secondary (see selection module)
    compositeForSort:
      commercial.score * 1000 +
      (classification.buyerAuthorityClaimed ? 500 : 0) +
      channel.score,
  };
}

/** Fixture helper — proves channel score cannot create a buyer claim. */
export function assertScoresDoNotOverrideAuthorityGates() {
  const person = {
    name: "Alejandra Vázquez",
    title: "Sales Manager",
    email: "alejandra.vazquez@igeco.mx",
    contactDataOrigin: "PUBLIC_SOURCE",
    channelSourceUrl: "https://ecomondomexico.com.mx/en/contact/",
    channelLastVerifiedAt: new Date().toISOString().slice(0, 10),
  };
  const scored = scoreCommercialContactCandidate(person, {
    evidence: {
      connectedToDemandActivity: true,
      currentOrganizationAndRole: true,
      relevantOperationalOrCommercialInfluence: true,
      credibleReferralRoute: true,
      unverifiedResponsibilityGaps: [
        "hotel_accommodation_responsibility_not_confirmed",
      ],
      supportingSourceUrls: ["https://ecomondomexico.com.mx/en/contact/"],
    },
  });
  return {
    ok:
      scored.classification.class ===
        COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT &&
      scored.channelRecovery.score >= 40 &&
      scored.commercialRelevance.buyerAuthorityClaimed === false,
    scored,
  };
}
