/**
 * Independent offline re-certification of staged Sales Ready opportunities.
 * Does NOT trust prior independentAcceptance.disposition === SALES_READY.
 * No paid research, Airtable writes, or customer publication.
 */

export const INDEPENDENT_SALES_READY_CERTIFICATION_V1 =
  "gdi-independent-sales-ready-certification-v1";

export const CERTIFICATION_CLASS = Object.freeze({
  CERTIFIED_SALES_READY: "CERTIFIED_SALES_READY",
  CERTIFIED_ACCOUNT_DEVELOPMENT: "CERTIFIED_ACCOUNT_DEVELOPMENT",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  WATCH: "WATCH",
  REJECTED: "REJECTED",
});

const HOTEL_MARKETS = Object.freeze({
  recCAGqkERSXgiku4: {
    slug: "sheraton-guadalajara-expo",
    name: "Sheraton Guadalajara Expo",
    marketTokens: ["guadalajara", "jalisco", "expo", "mexico"],
  },
  recTk5I8VOzsPhxDz: {
    slug: "voco-cancun-zona-hotelera",
    name: "voco Cancún Zona Hotelera",
    marketTokens: ["cancun", "cancún", "riviera maya", "quintana", "mexico"],
  },
  recXh4NgF8sDbi9cQ: {
    slug: "wyndham-alltra-punta-cana",
    name: "Wyndham Alltra Punta Cana",
    marketTokens: ["punta cana", "dominican", "cap cana", "bavaro", "bávaro"],
  },
  recH9h3McgMNFQEla: {
    slug: "radisson-blu-toronto-downtown",
    name: "Radisson Blu Toronto Downtown",
    marketTokens: ["toronto", "ontario", "canada", "queens quay"],
  },
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function blobOf(o = {}) {
  return text(
    [
      o.title,
      o.organizationName,
      o.hotelOpportunityThesis,
      o.programmeName,
      o.recommendedNextStep,
      o.independentAcceptance?.rationale?.join?.(" "),
      JSON.stringify(o.primaryContact || {}),
    ].join(" "),
  ).toLowerCase();
}

function namedBuyerOrEntry(o = {}) {
  const c = o.primaryContact || o.secondaryContact || null;
  const name = text(c?.name);
  const role = text(c?.title || c?.role);
  const entry =
    c?.isRelevantCommercialEntryPoint === true ||
    c?.commercialClass === "CONTACT_COMMERCIALLY_RELEVANT" ||
    o.independentAcceptance?.contact?.commercialContactPass === true;
  if (!name || name.length < 3) {
    return { pass: false, name: null, role: null, reason: "No named buyer/entry point" };
  }
  if (/unidentified|unknown|n\/a|tbd|placeholder/i.test(name)) {
    return { pass: false, name, role, reason: "Placeholder contact name" };
  }
  if (!entry && !role) {
    return { pass: false, name, role, reason: "Named person without commercial role/entry signal" };
  }
  return {
    pass: true,
    name,
    role,
    authority: c?.contractingAuthorityStatus || null,
    reason: entry
      ? "Named commercially relevant entry point"
      : "Named contact with role (authority not verified)",
  };
}

function thesisGrounded(o = {}) {
  const thesis = text(o.hotelOpportunityThesis);
  if (thesis.length < 80) {
    return { pass: false, reason: "Thesis missing or too thin for commercial grounding" };
  }
  const speculative =
    /not an? (?:qualified|live|verified|confirmed|open|available)|not confirmed .{0,40}demand|no (?:evidence|verified business|live|current .{0,30}lead|current .{0,30}booking)|historical (?:analogue|incentive|academic|resort|hospitality)|conditional(?:ly)?(?:\s|,)|plausible\b|prospecting (?:opportunity|hypothesis)|qualified (?:accommodation lead|historical|future-edition|prospecting)|future conversion remains unqualified|not (?:an? )?(?:identified|ready-to-book) .{0,40}lead|account-development prospect|intermediary-account|intermediary-development|but not an? open/i.test(
      thesis,
    );
  return {
    pass: true,
    speculative,
    reason: speculative
      ? "Thesis present but explicitly qualifies/limits live booking readiness"
      : "Evidence-grounded commercial demand thesis present",
    thesisPreview: thesis.slice(0, 220),
  };
}

function hotelRelevance(o = {}, hotelMeta) {
  const thesis = text(o.hotelOpportunityThesis).toLowerCase();
  const hotelName = text(o.hotelName || hotelMeta?.name).toLowerCase();
  const tokens = hotelMeta?.marketTokens || [];
  const hitHotel =
    (hotelName && thesis.includes(hotelName.split(" ")[0])) ||
    /for the |this (?:property|hotel)|radisson|wyndham|voco|sheraton|alltra/i.test(thesis);
  const hitMarket = tokens.some((t) => thesis.includes(t) || blobOf(o).includes(t));
  if (!hitHotel && !hitMarket) {
    return { pass: false, reason: "Hotel/market relevance not evidenced in thesis" };
  }
  return {
    pass: true,
    reason: hitHotel
      ? "Thesis references this hotel/product context"
      : "Thesis references destination/market relevant to hotel",
  };
}

function sourcesOk(o = {}) {
  const urls = [
    ...(o.evidenceUrls || []),
    ...(o.originalSourceUrls || []),
    ...(o.supportingSources || []).map((s) => s?.url || s),
  ].filter(Boolean);
  const n = new Set(urls.map((u) => String(u).trim().toLowerCase())).size;
  if (n < 2) return { pass: false, count: n, reason: "Fewer than 2 supporting sources" };
  return { pass: true, count: n, reason: `${n} supporting sources` };
}

function actionableApproach(o = {}) {
  const next = text(
    o.recommendedNextStep ||
      o.salesApproach ||
      o.independentAcceptance?.salesActivation ||
      o.primaryContact?.whyThisContact ||
      "",
  );
  if (next.length < 40) {
    return { pass: false, reason: "No actionable recommended sales approach" };
  }
  return { pass: true, reason: "Actionable commercial approach present", preview: next.slice(0, 160) };
}

function titleSegmentOk(o = {}) {
  const title = text(o.title);
  const seg = text(o.demandSegmentId || o.workflowId);
  if (!title || /unidentified/i.test(title)) {
    return { pass: false, reason: "Title unidentified / broken identity" };
  }
  if (!seg) return { pass: false, reason: "Segment missing" };
  return { pass: true, reason: `Title/segment ok (${seg})` };
}

function competitorExclusive(o = {}) {
  const b = blobOf(o);
  const thesis = text(o.hotelOpportunityThesis).toLowerCase();
  if (
    /exclusive (?:resort )?takeover|tied to an exclusive|hyatt ziva|hyatt zilara|meli[aá] room block|competitor host/i.test(
      `${b} ${thesis}`,
    ) &&
    /no (?:overflow|alternate)|exclusive|not (?:available|open)/i.test(thesis)
  ) {
    return {
      hit: true,
      reason: "Evidence points to competitor-exclusive or closed lodging path",
    };
  }
  if (/hyatt ziva and hyatt zilara/i.test(text(o.title))) {
    return { hit: true, reason: "Title is competitor-hosted programme; hotel is overflow/account only" };
  }
  return { hit: false };
}

function completedWithoutFuture(o = {}) {
  const thesis = text(o.hotelOpportunityThesis).toLowerCase();
  const title = text(o.title).toLowerCase();
  const blob = `${thesis} ${title}`;
  const hasFutureSignal = /2026|2027|2028|future[- ]cycle|future edition|next (?:edition|cycle)|recurring/i.test(
    blob,
  );
  if (
    /no live .+ opportunity remains|historical .+ no (?:current|live)|completed|2022\)|service period: may 2019|cita@40|esv\)/i.test(
      blob,
    ) &&
    !hasFutureSignal
  ) {
    return { hit: true, reason: "Completed/historical without credible future cycle" };
  }
  // Past-year titled programmes (e.g. "… 2025") without explicit future conversion path
  if (/\b2025\b/.test(title) && !hasFutureSignal && /historical|conditional future-property|analogue/i.test(thesis)) {
    return { hit: true, reason: "2025-titled programme without evidenced open future cycle" };
  }
  return { hit: false };
}

function commercialPotential(o = {}) {
  const thesis = text(o.hotelOpportunityThesis).toLowerCase();
  const imm = text(o.immediateVsRecurring).toUpperCase();
  if (/no commercial|zero demand|not commercially/i.test(thesis)) {
    return { pass: false, mode: null, reason: "No commercial potential stated" };
  }
  if (imm.includes("IMMEDIATE")) {
    return {
      pass: true,
      mode: "IMMEDIATE",
      reason: "Labeled immediate sales opportunity (subject to independent gates)",
    };
  }
  if (imm.includes("RECURRING") || imm.includes("ACCOUNT")) {
    return {
      pass: true,
      mode: "RECURRING",
      reason: "Recurring / account-development commercial potential",
    };
  }
  if (/2026|2027|future|annual|incentive|congress|symposium|meeting/i.test(thesis)) {
    return { pass: true, mode: "FUTURE", reason: "Credible future commercial potential in thesis" };
  }
  return { pass: true, mode: "UNSPECIFIED", reason: "Commercial potential assumed from thesis length" };
}

/**
 * Independently certify one staged opportunity object.
 * @returns {object} certification record
 */
export function independentlyCertifyStagedOpportunity(opportunity = {}, opts = {}) {
  const hotelId = opportunity.hotelId || opts.hotelId;
  const hotelMeta = HOTEL_MARKETS[hotelId] || {
    name: opportunity.hotelName,
    marketTokens: [],
    slug: opts.slug || null,
  };

  const checks = {
    hotelIdentity: {
      pass:
        Boolean(hotelId) &&
        text(opportunity.hotelName || hotelMeta.name).length > 3 &&
        (opportunity.hotelId == null || opportunity.hotelId === hotelId),
      reason: `Hotel ${text(opportunity.hotelName || hotelMeta.name)} / ${hotelId}`,
    },
    uniqueIdentity: {
      pass: Boolean(opportunity.opportunityId || opportunity.id),
      opportunityId: opportunity.opportunityId || opportunity.id || null,
      reason: "Opportunity id present",
    },
    demandThesis: thesisGrounded(opportunity),
    namedBuyerOrEntry: namedBuyerOrEntry(opportunity),
    hotelRelevance: hotelRelevance(opportunity, hotelMeta),
    commercialPotential: commercialPotential(opportunity),
    titleSegment: titleSegmentOk(opportunity),
    salesApproach: actionableApproach(opportunity),
    sources: sourcesOk(opportunity),
    pursuitMode: {
      pass: true,
      immediateVsRecurring: opportunity.immediateVsRecurring || null,
      reason: text(opportunity.immediateVsRecurring) || "unspecified",
    },
  };

  const exclusivity = competitorExclusive(opportunity);
  const historicalDead = completedWithoutFuture(opportunity);
  const reasons = [];
  const failKeys = Object.entries(checks)
    .filter(([, v]) => v && v.pass === false)
    .map(([k, v]) => {
      reasons.push(`${k}: ${v.reason}`);
      return k;
    });

  let classification = CERTIFICATION_CLASS.CERTIFIED_SALES_READY;
  const imm = text(opportunity.immediateVsRecurring).toUpperCase();
  const speculative = checks.demandThesis.speculative === true;
  const recurring =
    imm.includes("RECURRING") ||
    imm.includes("ACCOUNT_DEVELOPMENT") ||
    /account-development|intermediary|not a (?:qualified|verified) (?:live )?room-block|not a ready-to-book/i.test(
      text(opportunity.hotelOpportunityThesis),
    );

  if (failKeys.includes("hotelIdentity") || failKeys.includes("uniqueIdentity")) {
    classification = CERTIFICATION_CLASS.REJECTED;
    reasons.push("Identity failure");
  } else if (exclusivity.hit && speculative && !checks.namedBuyerOrEntry.pass) {
    classification = CERTIFICATION_CLASS.REJECTED;
    reasons.push(exclusivity.reason);
  } else if (failKeys.length >= 3) {
    classification = CERTIFICATION_CLASS.REJECTED;
    reasons.push("Multiple hard gate failures");
  } else if (
    failKeys.includes("namedBuyerOrEntry") ||
    failKeys.includes("demandThesis") ||
    failKeys.includes("sources")
  ) {
    classification = CERTIFICATION_CLASS.RESEARCH_CONTINUATION;
  } else if (historicalDead.hit && !recurring) {
    classification = CERTIFICATION_CLASS.WATCH;
    reasons.push(historicalDead.reason);
  } else if (exclusivity.hit) {
    classification = CERTIFICATION_CLASS.WATCH;
    reasons.push(exclusivity.reason);
    reasons.push("Monitor overflow / alternative lodging — not immediate Sales Ready");
  } else if (recurring) {
    classification = CERTIFICATION_CLASS.CERTIFIED_ACCOUNT_DEVELOPMENT;
    reasons.push("Recurring / intermediary pursuit mode");
    if (speculative) {
      reasons.push("Thesis explicitly limits live booking readiness → Account Development");
    }
  } else if (speculative) {
    // Immediate label + speculative thesis → AD (not Sales Ready), unless historical-dead already WATCH
    classification = CERTIFICATION_CLASS.CERTIFIED_ACCOUNT_DEVELOPMENT;
    reasons.push("Immediate label present but thesis is speculative/limited → Account Development");
  } else if (
    checks.commercialPotential.mode === "IMMEDIATE" &&
    checks.namedBuyerOrEntry.pass &&
    checks.hotelRelevance.pass &&
    checks.sources.pass &&
    checks.salesApproach.pass &&
    checks.demandThesis.pass &&
    !speculative &&
    !exclusivity.hit &&
    !historicalDead.hit
  ) {
    classification = CERTIFICATION_CLASS.CERTIFIED_SALES_READY;
    reasons.push("Meets independent Sales Ready bar for immediate pursuit");
  } else {
    classification = CERTIFICATION_CLASS.CERTIFIED_ACCOUNT_DEVELOPMENT;
    reasons.push("Commercially credible but not independently certified as immediate Sales Ready");
  }

  // Hard demotion: speculative / exclusivity / historical-dead never CERTIFIED_SALES_READY
  if (classification === CERTIFICATION_CLASS.CERTIFIED_SALES_READY) {
    if (speculative) {
      classification = CERTIFICATION_CLASS.CERTIFIED_ACCOUNT_DEVELOPMENT;
      reasons.push("Demoted: speculative thesis cannot be CERTIFIED_SALES_READY");
    } else if (exclusivity.hit) {
      classification = CERTIFICATION_CLASS.WATCH;
      reasons.push("Demoted: competitor-exclusive path");
    } else if (historicalDead.hit) {
      classification = CERTIFICATION_CLASS.WATCH;
      reasons.push("Demoted: historical without open future cycle");
    }
  }

  const suitableFor =
    classification === CERTIFICATION_CLASS.CERTIFIED_SALES_READY
      ? "IMMEDIATE_SALES_PURSUIT"
      : classification === CERTIFICATION_CLASS.CERTIFIED_ACCOUNT_DEVELOPMENT
        ? "RECURRING_ACCOUNT_DEVELOPMENT"
        : classification === CERTIFICATION_CLASS.WATCH
          ? "MONITOR_ONLY"
          : classification === CERTIFICATION_CLASS.RESEARCH_CONTINUATION
            ? "CONTINUE_RESEARCH"
            : "DO_NOT_PURSUE";

  const publicationCandidate =
    classification === CERTIFICATION_CLASS.CERTIFIED_SALES_READY ||
    classification === CERTIFICATION_CLASS.CERTIFIED_ACCOUNT_DEVELOPMENT;

  return {
    version: INDEPENDENT_SALES_READY_CERTIFICATION_V1,
    opportunityId: opportunity.opportunityId || opportunity.id,
    hotelId,
    hotelName: opportunity.hotelName || hotelMeta.name,
    organizationName: opportunity.organizationName || null,
    title: opportunity.title || null,
    segment: opportunity.demandSegmentId || opportunity.workflowId || null,
    priorStagedDisposition: opportunity.independentAcceptance?.disposition || null,
    priorPromoteEligible: opportunity.independentAcceptance?.promoteEligible === true,
    classification,
    suitableFor,
    publicationCandidate,
    customerVisibleAuthorized: false,
    checks,
    exclusivity,
    historicalDead,
    reasons,
    decidedAt: new Date().toISOString(),
  };
}

export { HOTEL_MARKETS };
