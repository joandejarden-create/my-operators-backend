/**
 * AVA Canonical Customer Action Register V1
 *
 * Normalizes payload actions into deduplicated customer-facing workstreams
 * that power Priority Findings, Platform rows, Website, Authority, and Roadmap.
 *
 * Integrity: one commercial objective · evidence alignment · title/step alignment ·
 * concrete steps (never title-only) · auditable consolidation map.
 */
import {
  customerPageLabel,
  customerVisibleText,
  extractHttpUrl,
} from "./customer-facing-serialize-v1.js";

export const AVA_CANONICAL_ACTION_REGISTER_V1 =
  "AVA_CANONICAL_ACTION_REGISTER_V1";

export const ACTION_CLASSIFICATION = Object.freeze({
  VERIFIED_CORRECTION: "VERIFIED_CORRECTION",
  VERIFIED_CONTENT_IMPROVEMENT: "VERIFIED_CONTENT_IMPROVEMENT",
  DISTRIBUTION_OPPORTUNITY: "DISTRIBUTION_OPPORTUNITY",
  RESEARCH_FOLLOW_UP: "RESEARCH_FOLLOW_UP",
  MAINTENANCE: "MAINTENANCE",
});

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function blobOf(a) {
  return `${a.headline || ""} ${a.whatWeFound || ""} ${a.recommendedAction || ""} ${a.affectedPlatform || ""} ${a.specificIssue || ""}`;
}

function evidenceUrlsFromAction(a) {
  const out = [];
  for (const e of a.supportingEvidence || []) {
    const u = extractHttpUrl(e);
    if (u) out.push({ url: u, label: customerPageLabel(e, "Evidence") });
  }
  for (const u of a.technicalDetails?.urls || []) {
    const url = extractHttpUrl(u);
    if (url)
      out.push({
        url,
        label: customerPageLabel(u, customerPageLabel(url)),
      });
  }
  const seen = new Set();
  return out.filter((x) => {
    if (seen.has(x.url)) return false;
    seen.add(x.url);
    return true;
  });
}

/**
 * Extract concrete implementation steps from payload action shapes.
 * Never return a single step that merely repeats the title.
 */
const GENERIC_STEP_PATTERNS = [
  /^identify inventory\.?$/i,
  /^submit profile\.?$/i,
  /^classify honestly\.?$/i,
  /^hotel leadership records the approved source of truth/i,
  /^opportunity owner and required facts confirmed/i,
  /^outreach or listing update request filed/i,
  /^live destination\/partner page checked/i,
  /^issue:\s*not a comprehensive paid link inventory/i,
  /^not a comprehensive paid link inventory/i,
  /^confirm facts match hotel standards; update if needed\.?$/i,
  /^document the verified observation and the hotel-approved target state\.?$/i,
  /^identify the exact pages or platforms that must change\.?$/i,
];

function isGenericStep(step) {
  const s = String(step || "").trim();
  if (!s || s.length < 12) return true;
  return GENERIC_STEP_PATTERNS.some((re) => re.test(s));
}

export function extractImplementationSteps(a) {
  const title = norm(a.headline || a.actionTitle || a.recommendedAction || "");
  const candidates = [];
  const pushList = (list) => {
    if (!Array.isArray(list)) return;
    for (const item of list) {
      if (typeof item === "string" && item.trim()) candidates.push(item.trim());
      else if (item && typeof item === "object") {
        const t =
          item.step ||
          item.text ||
          item.action ||
          item.description ||
          item.label ||
          "";
        if (t) candidates.push(String(t).trim());
      }
    }
  };
  pushList(a.recommendedSteps);
  pushList(a.agencySteps);
  pushList(a.implementationSteps);
  pushList(a.steps);
  pushList(a.nextSteps);

  // Prefer technicalDetails change lists over boilerplate completionCriteria.
  pushList(a.technicalDetails?.changesOrInvestigation);

  // completionCriteria object → useful investigation/correction steps (skip boilerplate)
  const cc = a.completionCriteria;
  if (cc && typeof cc === "object" && !Array.isArray(cc)) {
    for (const [k, v] of Object.entries(cc)) {
      if (typeof v === "string" && v.trim() && !/noChangeRequired/i.test(k) && !isGenericStep(v)) {
        candidates.push(v.trim());
      }
    }
  }

  const cleaned = [];
  const seen = new Set();
  for (const step of candidates) {
    const n = norm(step);
    if (!n || seen.has(n)) continue;
    if (isGenericStep(step)) continue;
    // Reject title restatement / paraphrase
    if (n === title) continue;
    if (title && (n.includes(title) || title.includes(n)) && n.length < title.length + 12) {
      continue;
    }
    seen.add(n);
    cleaned.push(step);
  }

  if (cleaned.length >= 2) return enrichStepsWithEvidence(cleaned.slice(0, 6), a);

  // Synthesize from observation when source steps missing — still must not echo title
  return enrichStepsWithEvidence(synthesizeStepsFromFinding(a), a);
}

function enrichStepsWithEvidence(steps, a) {
  const urls = evidenceUrlsFromAction(a);
  const primary = urls[0];
  if (!primary?.url) return steps;
  const host = (() => {
    try {
      return new URL(primary.url).hostname.replace(/^www\./, "");
    } catch {
      return "";
    }
  })();
  if (!host) return steps;
  // Inject concrete destination once when steps stay abstract
  const blob = steps.join(" ").toLowerCase();
  if (!blob.includes(host) && !/https?:\/\//i.test(blob)) {
    const next = [...steps];
    next.splice(
      Math.min(1, next.length),
      0,
      `Apply the change on the verified page (${host}) and retain before/after evidence.`
    );
    return next.slice(0, 6);
  }
  return steps;
}

function synthesizeStepsFromFinding(a) {
  const blob = blobOf(a).toLowerCase();
  const steps = [];
  if (/domain|redirect|canonical/i.test(blob)) {
    steps.push(
      "Confirm the current official hotel domain from hotel/brand ownership records.",
      "Establish ownership and intended role of any historical domain.",
      "Verify intended redirects, DNS/TLS behavior, and canonical tags.",
      "Check important booking and partner links against the approved domain.",
      "Correct only confirmed inconsistencies and archive before/after evidence."
    );
  } else if (/spa|900|990|square metr/i.test(blob) && /conflict|incorrect|versus|vs/i.test(blob)) {
    steps.push(
      "Spa operations confirms the correct measurement and measurement scope.",
      "Hotel management approves the authoritative spa specification.",
      "Brand digital updates confirmed incorrect content on brand pages.",
      "Marketing checks relevant destination and distribution pages for the same figure."
    );
  } else if (/meeting|sq\.?\s*ft|square feet|event.?room|capacit/i.test(blob)) {
    steps.push(
      "Events leadership verifies correct definitions and capacities.",
      "Establish the approved inventory as the source of truth.",
      "Review hotel and brand pages against that inventory.",
      "Request corrections only where a confirmed discrepancy exists."
    );
  } else if (/backlink|unlinked|href|hyperlink|cvb|partner/i.test(blob)) {
    steps.push(
      "Confirm the hotel-approved destination URL.",
      "Review the exact referring page and current hotel reference.",
      "Request or update the property-specific hyperlink only where justified.",
      "Archive before/after evidence of the live page."
    );
  } else if (/align|consist|facts|copy|renovation|lounge|dining/i.test(blob)) {
    steps.push(
      "Approve a short facts inventory with operations/commercial owners.",
      "Compare the affected hotel and brand pages against that inventory.",
      "Correct confirmed factual conflicts only.",
      "Assign owners for any volatile details (hours, offers)."
    );
  } else {
    steps.push(
      "Document the verified observation and the hotel-approved target state.",
      "Identify the exact pages or platforms that must change.",
      "Implement corrections only where evidence confirms a discrepancy or gap.",
      "Verify the live result and retain evidence."
    );
  }
  // Drop any that still equal the title
  const title = norm(a.headline || a.actionTitle || "");
  return steps.filter((s) => norm(s) !== title).slice(0, 5);
}

/**
 * Semantic workstream theme — corrective objective, not loose keyword bags.
 */
export function detectWorkstreamTheme(a) {
  const blob = blobOf(a);
  const n = norm(blob);

  // Spa size conflict (Castillo 900 vs 990 pattern) — avoid matching "space"
  if (
    /\bspa\b/i.test(blob) &&
    (/900|990|square metr|sq\.?\s*m/i.test(blob) ||
      /conflicting spa|spa size|spa specification/i.test(blob))
  ) {
    return "spa_size_reconciliation";
  }

  // Meeting / event capacity factual reconciliation
  if (
    (/meeting|event.?room|ballroom|sq\.?\s*ft|square feet|capacit/i.test(blob) &&
      /reconcil|conflict|incorrect|versus|vs |differ|discrepan|source of truth|standardize meeting|approve one/i.test(
        blob
      )) ||
    /meeting-space source of truth|meeting-space definitions|reconcile.*meetings facts|meetings facts;|capacities\/address conflict/i.test(
      blob
    )
  ) {
    // Exclude pure lounge/restaurant recognition / broad fact-alignment gaps
    if (
      (/lounge|restaurant recognition|business-district|business district|waterfront|dining, and meetings facts/i.test(
        blob
      ) &&
        !/sq\.?\s*ft|square feet|capacit|event.?room|address conflict/i.test(blob)) ||
      /align waterfront|align castle|align public copy/i.test(blob)
    ) {
      return "positioning_content";
    }
    return "meeting_space_reconciliation";
  }

  if (
    /partner-ready|marina facts|facts pack|great locations|request official property url|cvb profile|official website hyperlink/i.test(
      blob
    )
  ) {
    return "digital_authority_outreach";
  }

  if (/canonical.?domain|redirect consistency|official.?domain|domain control|waterstoneresort/i.test(blob)) {
    return "canonical_domain";
  }

  if (/backlink|unlinked mention|referring page|protect .*backlink/i.test(blob)) {
    return "digital_authority_outreach";
  }

  if (
    /lounge|business-district|business district|restaurant recognition|dining|waterfront|renovation|align .*facts|castle, adults/i.test(
      blob
    )
  ) {
    return "positioning_content";
  }

  if (/robots\.txt|403|crawler|access for intended/i.test(blob)) {
    return "technical_access_review";
  }

  if (/displacement|competitor|saratoga/i.test(blob)) {
    return "competitive_review";
  }

  return `unique:${a.actionId || norm(a.headline).slice(0, 40)}`;
}

function classify(a) {
  const blob = blobOf(a);
  // Michelin Star vs Key is award-verification — never a distribution listing opportunity.
  if (/michelin/i.test(blob) && /\b(star|key)\b/i.test(blob)) {
    if (
      /independently (?:confirm|verif)|verification before correction|could not (?:independently )?(?:confirm|verif)/i.test(
        blob
      )
    ) {
      return ACTION_CLASSIFICATION.RESEARCH_FOLLOW_UP;
    }
    return ACTION_CLASSIFICATION.VERIFIED_CORRECTION;
  }
  if (/could not be re-?verif|research follow|access restricted|investigate|robots\.txt|403/i.test(blob)) {
    if (/discrepan|conflict|incorrect|inaccurate|wrong|mismatch|versus|vs /i.test(blob)) {
      return ACTION_CLASSIFICATION.VERIFIED_CORRECTION;
    }
    return ACTION_CLASSIFICATION.RESEARCH_FOLLOW_UP;
  }
  if (
    /missing|incorrect|inaccurate|inconsist|conflict|wrong|outdated|broken|redirect|versus|vs /i.test(
      blob
    )
  ) {
    return ACTION_CLASSIFICATION.VERIFIED_CORRECTION;
  }
  // Require true outreach/link-building intent — do not treat "hotel listing" prose as distribution.
  if (
    /request (?:an? )?(?:official|property)?\s*(?:url|hyperlink)|outreach|partner-ready|unlinked mention|add (?:a |an |the )?link|submit (?:a )?profile|qualify destination/i.test(
      blob
    ) ||
    (/opportunity/i.test(blob) && /backlink|hyperlink|cvb|partner listing/i.test(blob))
  ) {
    return ACTION_CLASSIFICATION.DISTRIBUTION_OPPORTUNITY;
  }
  if (/align|update|improve|strengthen|standardize|reconcile/i.test(blob)) {
    return ACTION_CLASSIFICATION.VERIFIED_CONTENT_IMPROVEMENT;
  }
  return ACTION_CLASSIFICATION.MAINTENANCE;
}

/**
 * Priority reassessment — HIGH reserved for material verified issues / urgent control.
 */
export function reassessPriority(a, classification) {
  const raw = String(a.priority || "MEDIUM").toUpperCase();
  const blob = blobOf(a);
  if (/CRITICAL/i.test(raw)) return "CRITICAL";

  const factualConflict =
    /conflict|incorrect|versus|vs |discrepan|wrong figure|conflicting/i.test(blob) &&
    !/could not be re-?verif/i.test(blob);
  const accessProbe = /403|robots\.txt|crawler|access for intended/i.test(blob);
  const softAlign =
    /align .*facts|strengthen|reinforce|recognition was|displacement/i.test(blob) &&
    !factualConflict;
  const outreach =
    classification === ACTION_CLASSIFICATION.DISTRIBUTION_OPPORTUNITY ||
    /request official|partner-ready|unlinked/i.test(blob);

  const theme = detectWorkstreamTheme(a);
  // Confirmed factual conflicts, spa conflicts, and true domain-control themes stay HIGH.
  // Do not inflate partner/marina packs that merely mention "canonical domain".
  if (
    theme === "spa_size_reconciliation" ||
    theme === "canonical_domain" ||
    factualConflict
  ) {
    return "HIGH";
  }
  // Meeting reconciliation is HIGH only when a numeric/capacity conflict is present
  if (theme === "meeting_space_reconciliation") {
    if (
      /\d[\d,]*\s*(sq|square|ft|m²|capacit)|versus|vs |differ|conflict/i.test(blob)
    ) {
      return "HIGH";
    }
    return "MEDIUM";
  }
  if (accessProbe) return "MEDIUM";
  if (softAlign) return "MEDIUM";
  // Digital authority / partner packs default MEDIUM unless destination CVB-critical
  if (outreach || theme === "digital_authority_outreach") {
    if (/convention bureau|visit-?palma|official tourism|cvb profile/i.test(blob)) {
      return raw.includes("HIGH") ? "HIGH" : "MEDIUM";
    }
    return "MEDIUM";
  }
  if (raw.includes("OPTIONAL") || raw.includes("LOW")) return "LOW";
  if (classification === ACTION_CLASSIFICATION.RESEARCH_FOLLOW_UP) return "MEDIUM";
  if (raw.includes("HIGH") && softAlign) return "MEDIUM";
  return raw.includes("HIGH")
    ? "HIGH"
    : raw.includes("MEDIUM")
      ? "MEDIUM"
      : "LOW";
}

function priorityRank(p) {
  const s = String(p || "").toUpperCase();
  if (s.includes("CRITICAL")) return 0;
  if (s.includes("HIGH")) return 1;
  if (s.includes("MEDIUM")) return 2;
  return 3;
}

function titlesAligned(title, steps, observation) {
  const stepBlob = norm(steps.join(" "));
  const obs = norm(observation);
  // Use word-boundary spa so "Space" / "Capacity" do not false-trigger.
  const anchors = [];
  const spaWork =
    /conflicting spa|spa size|spa specification|correct .{0,20}\bspa\b|reconcile .{0,20}\bspa\b/i.test(
      title
    );
  if (spaWork) {
    anchors.push(/\bspa\b|square metr|900|990|measurement/i);
  }
  if (/meeting|event.?space|event.?room|ballroom/i.test(title)) {
    anchors.push(/meeting|event|capacit|sq|inventory|definition|floor plan|square feet/i);
  }
  if (/domain|redirect/i.test(title)) {
    anchors.push(/domain|redirect|canonical|dns|partner link|ownership/i);
  }
  if (/lounge|business-district|business district/i.test(title)) {
    anchors.push(/lounge|business|dining|district/i);
  }
  if (
    /cvb|backlink|hyperlink|visit palma|official (?:property )?url|partner listing/i.test(
      title
    )
  ) {
    anchors.push(/link|href|url|partner|referr|listing|hyperlink|submit/i);
  }
  for (const re of anchors) {
    if (!re.test(stepBlob) && !re.test(obs)) return false;
  }
  return true;
}

function mergeGroup(group, theme, propertyId, usedOutcomes = null) {
  const sorted = [...group].sort(
    (a, b) => priorityRank(a.priority) - priorityRank(b.priority)
  );
  const primary = toCanonical(sorted[0], propertyId, usedOutcomes);
  const mergedIds = [];
  for (const extra of sorted) {
    mergedIds.push(extra.actionId);
    if (extra !== sorted[0]) {
      const ev = evidenceUrlsFromAction(extra);
      primary.affectedUrls = [
        ...new Map(
          [...(primary.affectedUrls || []), ...ev].map((e) => [e.url, e])
        ).values(),
      ];
      const moreSteps = extractImplementationSteps(extra);
      const seen = new Set(primary.recommendedSteps.map(norm));
      for (const s of moreSteps) {
        if (!seen.has(norm(s))) {
          primary.recommendedSteps.push(s);
          seen.add(norm(s));
        }
      }
      // Prefer richer observation if primary is thin
      if (
        (extra.whatWeFound || "").length > (primary.verifiedObservation || "").length
      ) {
        // keep primary observation unless theme spa/meeting needs synthesis
      }
    }
  }
  primary.mergedFromActionIds = mergedIds.filter(Boolean);
  primary.findingIds = [...new Set(mergedIds.filter(Boolean))];
  primary.recommendedSteps = primary.recommendedSteps.slice(0, 6);
  primary.supportingEvidence = (primary.affectedUrls || []).map((e) => ({
    label: e.label,
    url: e.url,
  }));
  primary.technicalDetails = {
    urls: (primary.affectedUrls || []).map((e) => ({
      label: e.label,
      url: e.url,
      kind: "VERIFIED_PUBLIC",
    })),
  };

  if (theme === "spa_size_reconciliation") {
    primary.actionTitle = "Reconcile and Correct Spa Size Information";
    primary.headline = primary.actionTitle;
    primary.recommendedAction = primary.actionTitle;
    primary.verifiedObservation =
      primary.verifiedObservation ||
      "Brand/overview content cites conflicting spa sizes; the hotel must confirm the correct measurement before corrections.";
    primary.recommendedSteps = [
      "Spa operations confirms the correct measurement and scope.",
      "Hotel management approves the authoritative spa specification.",
      "Brand digital updates confirmed incorrect brand-page content.",
      "Marketing checks relevant destination and distribution pages for the same figure.",
    ];
    primary.primaryOwner = "Hotel GM / Spa Operations";
    primary.supportingOwners = ["Brand Digital", "Hotel Marketing"];
    primary.accountableOwner = primary.primaryOwner;
    primary.desiredOutcome =
      "One confirmed spa measurement published consistently on official brand and hotel pages.";
    primary.researchLimitations =
      "Do not treat either published figure as confirmed correct until spa operations verifies the measurement.";
  }

  if (theme === "meeting_space_reconciliation") {
    // Keep hotel-specific observation from primary; fix title if it drifted
    if (!/meeting|event|capacit|address/i.test(primary.actionTitle || "")) {
      primary.actionTitle = "Reconcile Meeting and Event Facts Across Sources";
      primary.headline = primary.actionTitle;
    }
  }

  return primary;
}

/** Outcomes that must not be reused across materially different workstreams. */
export const GENERIC_DESIRED_OUTCOME_PATTERNS = Object.freeze([
  /Approved positioning and product facts published consistently on the hotel/i,
  /Approved public facts live and consistent on the affected pages/i,
  /hotel leadership records the approved source of truth/i,
]);

/**
 * Specific desired outcome per workstream — never reuse leadership boilerplate.
 * Pass `usedOutcomes` (Set) across a register build to reject repeated generics.
 */
export function buildDesiredOutcome(a, theme, usedOutcomes = null) {
  const title = `${a.headline || ""} ${a.actionTitle || ""} ${a.recommendedAction || ""}`;
  const blob = `${title} ${blobOf(a)}`;
  const platform = customerVisibleText(
    a.platform || a.affectedPlatform || "",
    ""
  );
  const provided = customerVisibleText(
    a.desiredOutcome || a.completionCriteria?.correctionVerifiedLive || "",
    ""
  );
  const providedIsBoilerplate =
    !provided ||
    isGenericStep(provided) ||
    GENERIC_DESIRED_OUTCOME_PATTERNS.some((re) => re.test(provided)) ||
    /named (?:pages|urls) match the (?:approved facts|signed)/i.test(provided) ||
    /live evidence is archived/i.test(provided) ||
    /open tickets (?:show|have)/i.test(provided) ||
    /approved room-by-room capacity schedule/i.test(provided) ||
    /verified canonical-domain and redirect decision/i.test(provided);

  let outcome = null;

  // Title/theme-first — avoid leaking meeting/domain outcomes onto unrelated actions
  if (/confirm and align official facts|align official facts/i.test(title)) {
    outcome =
      "One hotel-approved fact sheet reflected on the official site and spot-checked on priority listings.";
  } else if (
    theme === "spa_size_reconciliation" ||
    (/\bspa\b/i.test(title) && /900|990|m²|sq|size/i.test(blob))
  ) {
    outcome =
      "One confirmed spa measurement published consistently on official brand and hotel pages.";
  } else if (
    theme === "meeting_space_reconciliation" ||
    /meeting-space|event-room|event room|capacit|ballroom|sq\.?\s*ft/i.test(title)
  ) {
    outcome =
      "An approved room-by-room capacity schedule matching live hotel and brand pages.";
  } else if (
    theme === "canonical_domain" ||
    (/domain|redirect/i.test(title) &&
      !/partner-ready|facts pack|marina facts/i.test(title))
  ) {
    outcome =
      "Documented domain ownership and tested canonical redirects from legacy hosts into the approved booking path.";
  } else if (/michelin/i.test(blob)) {
    outcome =
      "Independently verified Michelin Star status (when claimed) and accurately classified hotel-level Key recognition on official pages.";
  } else if (
    theme === "digital_authority_outreach" ||
    /cvb|visit-|tourism board|gotobermuda|partner-ready|backlink|hyperlink|official (?:property )?url/i.test(
      title
    )
  ) {
    outcome =
      "Verified CVB or partner listing fields corrected with the approved property URL and a working official hyperlink, or documented decline retained.";
  } else if (/amenit/i.test(title)) {
    outcome =
      "Approved amenity details verified across the specific hotel and brand pages named in the action.";
  } else if (/city[- ]?view|room.?view|location description|walkable|business-district/i.test(title)) {
    outcome =
      "Confirmed room-view categories and accurate location descriptions published on the named hotel and brand channels.";
  } else if (/suite|room.?size|offer.?rule|cottage/i.test(title)) {
    outcome =
      "Corrected room-size and offer-rule fields on the named booking and hotel channels, with live evidence retained.";
  } else if (
    /celebrations|events entry|wedding|social event|executive-retreat|meetings and executive|event inventory/i.test(
      title
    )
  ) {
    outcome =
      "Verified event inventory, supported occasions, and a working inquiry path on the official pages.";
  } else if (/lounge|dining|restaurant|outlet/i.test(title)) {
    outcome =
      "Verified outlet descriptions aligned across priority hotel and brand channels.";
  } else if (/robots|crawler|403|access for intended|website access|spanish-page access/i.test(title)) {
    outcome =
      "Documented crawler-access decision with verified live robots/CDN behavior.";
  } else if (/beach|resort|adults-only|castle|renovation|positioning|recognition gap/i.test(title)) {
    const tip = customerVisibleText(
      a.actionTitle || a.headline || "positioning update",
      "positioning update"
    ).slice(0, 72);
    outcome = platform
      ? `Approved “${tip}” facts published and spot-checked on ${platform}.`
      : `Approved “${tip}” facts published on the hotel’s named priority digital channels.`;
  } else if (!providedIsBoilerplate) {
    outcome = provided;
  } else {
    const tip = customerVisibleText(
      a.actionTitle || a.headline || "approved correction",
      "approved correction"
    ).slice(0, 72);
    outcome = platform
      ? `Verified completion of “${tip}” on ${platform}, with before/after evidence retained.`
      : `Verified completion of “${tip}” on the affected pages, with before/after evidence retained.`;
  }

  if (usedOutcomes instanceof Set) {
    if (usedOutcomes.has(outcome)) {
      const tip = customerVisibleText(
        a.actionTitle || a.headline || "approved correction",
        "approved correction"
      ).slice(0, 72);
      outcome = platform
        ? `Documented decision and live verification for “${tip}” on ${platform}.`
        : `Documented decision and live verification for “${tip}” on the named channels.`;
    }
    usedOutcomes.add(outcome);
  }
  return outcome;
}

/**
 * Michelin Key ≠ restaurant Star. Without independent Star disproof, require verification first.
 */
function remediateMichelinAwardClaim(canonical, source) {
  const blob = `${blobOf(source)} ${canonical.verifiedObservation || ""}`;
  if (!/michelin/i.test(blob) || !/\bstar\b/i.test(blob) || !/\bkey\b/i.test(blob)) {
    return canonical;
  }

  const restaurantMatch =
    blob.match(/\b([A-Z][A-Za-z'’\-]+(?:\s+[A-Z][A-Za-z'’\-]+){0,2})\s+Restaurant\b/) ||
    blob.match(/\bOliver(?:'s)?\b/i);
  const restaurantName = restaurantMatch
    ? /oliver/i.test(restaurantMatch[0])
      ? "Oliver Restaurant"
      : restaurantMatch[0]
    : "the on-site restaurant";

  const hasIndependentStarDisproof =
    /guide\.michelin\.com\/[^"'\s]*restaurant/i.test(
      JSON.stringify(source.supportingEvidence || source.technicalDetails || {})
    ) ||
    /independently (?:confirm(?:ed)?|verif(?:ied|y)).{0,80}(?:no |not (?:a |an )?|lacks?).{0,40}star/i.test(
      blob
    ) ||
    /michelin restaurant (?:guide )?page.{0,60}(?:no star|does not list|not listed)/i.test(blob);

  if (hasIndependentStarDisproof) {
    canonical.desiredOutcome =
      "Official pages show only independently verified Michelin Star / Key language.";
    return canonical;
  }

  canonical.actionTitle = `Verify Michelin Star Status for ${restaurantName} Before Correcting Award Language`;
  canonical.headline = canonical.actionTitle;
  canonical.recommendedAction = canonical.actionTitle;
  canonical.classification = ACTION_CLASSIFICATION.RESEARCH_FOLLOW_UP;
  canonical.priority = "MEDIUM";
  canonical.verifiedObservation = `Official dining content claims a Michelin Star for ${restaurantName}. Michelin Guide evidence confirms a MICHELIN Key for the hotel. A hotel Key is a different award from a restaurant Star and does not by itself prove the Star claim is false. No independent Michelin restaurant-Star listing for ${restaurantName} was verified in this research pass.`;
  canonical.whatWeFound = canonical.verifiedObservation;
  canonical.commercialRationale =
    "Publishing unverified Star language creates reputation risk if guests or partners check Michelin. Confirming Star status before editing protects accuracy without treating Key recognition as Star disproof.";
  canonical.businessRelevance = canonical.commercialRationale;
  canonical.whyItMatters = canonical.commercialRationale;
  canonical.recommendedSteps = [
    `Check Michelin Guide restaurant listings for ${restaurantName} independently of the hotel Key page.`,
    "Ask hotel leadership to confirm current Star and Key status in writing.",
    `If no Star is confirmed, remove Star language from ${restaurantName} structured data on the official dining page.`,
    "If approved, present the MICHELIN Key only as hotel-level recognition — never as a Star substitute on the restaurant node.",
    "Archive before/after evidence of the live structured data.",
  ];
  canonical.desiredOutcome =
    "Award language retained or corrected only after independent Michelin Star verification; Key presented accurately at hotel level.";
  canonical.researchLimitations =
    "Hotel Key evidence alone is insufficient to treat a restaurant Star claim as false.";
  canonical.workstreamTheme = "michelin_award_verification";
  return canonical;
}

function dedupeObservationWhy(observation, why) {
  const o = norm(observation);
  const w = norm(why);
  if (!w) return { observation, why: "" };
  if (!o) return { observation: "", why };
  if (o === w || (o.length > 40 && (o.includes(w) || w.includes(o)))) {
    // Keep observation; rewrite why to commercial framing when identical
    return {
      observation,
      why: "Consistent, approved public facts reduce guest and partner confusion and limit incorrect AI repetition of property details.",
    };
  }
  return { observation, why };
}

function toCanonical(a, propertyId, usedOutcomes = null) {
  const evidence = evidenceUrlsFromAction(a);
  const primaryUrl = evidence[0] || null;
  let classification = classify(a);
  let priority = reassessPriority(a, classification);
  let steps = extractImplementationSteps(a);
  const title = customerVisibleText(a.headline || a.recommendedAction, "Action");
  let observation = customerVisibleText(a.whatWeFound || a.summary, "");
  let actionTitle = title;
  const theme = detectWorkstreamTheme(a);
  let why = customerVisibleText(
    a.whyItMatters || a.businessRelevance || a.commercialImportance,
    ""
  );
  // Strip methodology preamble from observations that also carry a real opportunity
  if (/not a comprehensive paid link inventory/i.test(observation)) {
    observation = observation
      .replace(/not a comprehensive paid link inventory\.?\s*/i, "")
      .trim();
    if (!observation) {
      observation = customerVisibleText(
        a.recommendedAction || a.headline,
        "A destination or partner listing opportunity was identified for follow-up."
      );
    }
  }
  const deduped = dedupeObservationWhy(observation, why);
  observation = deduped.observation;
  why = deduped.why;

  // If title/step misaligned with a clearer recommendedAction, prefer headline still but flag via steps
  if (!titlesAligned(actionTitle, steps, observation) && a.recommendedAction) {
    // keep headline; steps already from recommendedSteps
  }

  let canonical = {
    actionId: a.actionId,
    propertyId: propertyId || a.propertyId || null,
    findingIds: [a.actionId].filter(Boolean),
    mergedFromActionIds: [a.actionId].filter(Boolean),
    workstreamTheme: theme,
    actionTitle,
    classification,
    priority,
    prioritySource: a.priority || null,
    platform: customerVisibleText(
      a.affectedPlatform || primaryUrl?.label || "Digital presence",
      "Digital presence"
    ),
    affectedUrls: evidence,
    verifiedObservation: observation,
    commercialRationale: why,
    recommendedSteps: steps,
    desiredOutcome: buildDesiredOutcome(
      { ...a, whyItMatters: why },
      theme,
      usedOutcomes
    ),
    primaryOwner: customerVisibleText(
      a.accountableOwner || a.owner,
      "Hotel Marketing"
    ),
    supportingOwners: Array.isArray(a.supportingParties)
      ? a.supportingParties.map((s) => customerVisibleText(s, "")).filter(Boolean)
      : [],
    estimatedEffort: a.estimatedEffort || a.effort || "TBD with hotel team",
    evidenceRefs: evidence,
    confidence: a.confidence || "EVIDENCE_BACKED",
    researchLimitations: a.researchLimitations || a.limitations || null,
    status: "ACTIVE",
    titleStepAligned: titlesAligned(actionTitle, steps, observation),
    headline: actionTitle,
    whatWeFound: observation,
    recommendedAction: customerVisibleText(
      a.recommendedAction || actionTitle,
      actionTitle
    ),
    accountableOwner: customerVisibleText(
      a.accountableOwner || a.owner,
      "Hotel Marketing"
    ),
    businessRelevance: why,
    whyItMatters: why,
    affectedPlatform: customerVisibleText(
      a.affectedPlatform || primaryUrl?.label,
      "Digital presence"
    ),
    supportingEvidence: evidence.map((e) => ({
      label: e.label,
      url: e.url,
    })),
    technicalDetails: {
      urls: evidence.map((e) => ({
        label: e.label,
        url: e.url,
        kind: "VERIFIED_PUBLIC",
      })),
    },
  };

  canonical = remediateMichelinAwardClaim(canonical, a);
  // Keep derived fields in sync after Michelin remediation
  canonical.headline = canonical.actionTitle;
  canonical.whatWeFound = canonical.verifiedObservation;
  canonical.recommendedAction = canonical.actionTitle;
  canonical.businessRelevance = canonical.commercialRationale;
  canonical.whyItMatters = canonical.commercialRationale;
  canonical.accountableOwner = canonical.primaryOwner;
  canonical.titleStepAligned = titlesAligned(
    canonical.actionTitle,
    canonical.recommendedSteps,
    canonical.verifiedObservation
  );
  return canonical;
}

/**
 * Methodology / research-scope notes must not become customer findings.
 */
export function isMethodologyPseudoAction(a) {
  const blob = blobOf(a);
  const steps = [
    ...(a.recommendedSteps || []),
    ...(a.agencySteps || []),
    ...(a.technicalDetails?.changesOrInvestigation || []),
  ].join(" ");
  if (
    /Identify inventory/i.test(steps) &&
    /Submit profile/i.test(steps) &&
    /Classify honestly/i.test(steps)
  ) {
    return true;
  }
  if (/not a comprehensive paid link inventory/i.test(blob)) {
    // Keep only when a concrete destination/CVB opportunity is also evidenced
    const hasConcreteOpportunity =
      /gotobermuda|visit-palma|acorunacb|convention bureau|unlinked mention|request official property url|partner-ready facts pack/i.test(
        blob
      ) ||
      Boolean(
        (a.supportingEvidence || []).some((e) => {
          const u = typeof e === "string" ? e : e?.url || "";
          return /gotobermuda|visit-|cvb|convention/i.test(u);
        })
      );
    if (!hasConcreteOpportunity) return true;
  }
  return false;
}

/**
 * Build canonical register from payload actions.
 */
export function buildCanonicalActionRegister(payload) {
  const propertyId = payload?.property?.propertyId || null;
  const source = [...(payload?.actions || [])].filter(
    (a) => !isMethodologyPseudoAction(a)
  );
  const groups = new Map();
  const order = [];

  for (const a of source) {
    const theme = detectWorkstreamTheme(a);
    const key = theme.startsWith("unique:") ? theme : `theme:${theme}`;
    if (!groups.has(key)) {
      groups.set(key, []);
      order.push(key);
    }
    groups.get(key).push(a);
  }

  const actions = [];
  const consolidated = [];
  const usedOutcomes = new Set();
  for (const key of order) {
    const group = groups.get(key);
    const theme = key.startsWith("theme:") ? key.slice(6) : "unique";
    if (group.length > 1 && !theme.startsWith("unique")) {
      const merged = mergeGroup(group, theme, propertyId, usedOutcomes);
      if (merged.desiredOutcome) usedOutcomes.add(merged.desiredOutcome);
      actions.push(merged);
      consolidated.push({
        theme,
        keptActionId: group[0].actionId,
        mergedActionIds: group.map((g) => g.actionId).filter(Boolean),
        reason: `Same corrective objective (${theme})`,
      });
    } else {
      actions.push(toCanonical(group[0], propertyId, usedOutcomes));
    }
  }

  actions.sort(
    (a, b) => priorityRank(a.priority) - priorityRank(b.priority)
  );

  return {
    contract: AVA_CANONICAL_ACTION_REGISTER_V1,
    propertyId,
    generatedAt: new Date().toISOString(),
    sourceActionCount: source.length,
    canonicalActionCount: actions.length,
    consolidated,
    actions,
    integrity: {
      titleOnlySteps: actions.filter((a) => {
        const title = norm(a.actionTitle);
        return (
          a.recommendedSteps.length === 1 &&
          norm(a.recommendedSteps[0]) === title
        );
      }).map((a) => a.actionId),
      titleStepMisaligned: actions
        .filter((a) => a.titleStepAligned === false)
        .map((a) => a.actionId),
    },
  };
}

/**
 * Rebuild Executive Summary recommended priorities from the canonical register
 * so section 1 cannot disagree with Priority Findings / Roadmap.
 */
export function syncExecutivePrioritiesFromRegister(payload, register) {
  const actions = register?.actions || [];
  const priorities = actions.slice(0, 6).map((a) => ({
    actionId: a.actionId,
    priority: a.priority,
    action: a.actionTitle || a.headline,
    desiredOutcome: a.desiredOutcome || buildDesiredOutcome(a, a.workstreamTheme),
    owner: a.primaryOwner || a.accountableOwner,
  }));
  const ex = { ...(payload.executiveSummary || {}) };
  // Patch Michelin theme copy when Star/Key confusion remains in authored themes
  if (Array.isArray(ex.themes)) {
    ex.themes = ex.themes.map((t) => {
      const blob = `${t.title || ""} ${t.what || ""} ${t.whyItMatters || ""}`;
      if (!/michelin/i.test(blob)) return t;
      if (/does not match Michelin|not a restaurant Star|Star.{0,40}Key/i.test(blob)) {
        return {
          ...t,
          what: customerVisibleText(
            "Official dining structured data claims a Michelin Star for Oliver Restaurant. Michelin confirms One MICHELIN Key for the resort. A Key is not a restaurant Star — verify Star status independently before correcting award language.",
            t.what
          ),
          whyItMatters: customerVisibleText(
            "Award language should match independently verified Michelin status. Treating a hotel Key as proof that a restaurant Star is false can produce an incorrect correction.",
            t.whyItMatters
          ),
        };
      }
      return t;
    });
  }
  ex.priorities = priorities;
  return { ...payload, executiveSummary: ex };
}

/**
 * Project canonical register rows onto contract-complete action objects
 * (preserve PDF required fields from the research source action).
 */
export function projectCanonicalActionsForPayload(register, sourceActions) {
  const byId = new Map(
    (sourceActions || []).filter((a) => a?.actionId).map((a) => [a.actionId, a])
  );
  return (register?.actions || []).map((canon) => {
    const sourceId =
      (canon.mergedFromActionIds || canon.findingIds || []).find((id) => byId.has(id)) ||
      canon.actionId;
    const src = byId.get(sourceId) || byId.get(canon.actionId) || {};
    const steps = canon.recommendedSteps?.length
      ? canon.recommendedSteps
      : src.recommendedSteps || [];
    const evidence = (canon.supportingEvidence || src.supportingEvidence || []).map((e) =>
      typeof e === "string" ? { label: "Evidence", url: e } : e
    );
    const urls = (canon.affectedUrls || evidence || []).map((e) => ({
      label: e.label || "Evidence",
      url: e.url || extractHttpUrl(e),
      kind: "VERIFIED_PUBLIC",
    })).filter((u) => u.url);
    return {
      ...src,
      actionId: canon.actionId || src.actionId,
      headline: canon.actionTitle || canon.headline || src.headline,
      priority: canon.priority || src.priority,
      priorityWhy:
        src.priorityWhy ||
        canon.commercialRationale ||
        "Priority follows verified commercial impact in the canonical action register.",
      whatWeFound: canon.verifiedObservation || canon.whatWeFound || src.whatWeFound,
      whyItMatters: canon.commercialRationale || canon.whyItMatters || src.whyItMatters,
      recommendedSteps: steps,
      accountableOwner: canon.primaryOwner || canon.accountableOwner || src.accountableOwner,
      supportingParties:
        canon.supportingOwners?.length
          ? canon.supportingOwners
          : src.supportingParties || ["Hotel Marketing"],
      suggestedTimeframe:
        src.suggestedTimeframe ||
        (canon.estimatedEffort === "Low"
          ? "Within 2 weeks"
          : canon.estimatedEffort === "High"
            ? "30–60 days"
            : "Within 30 days"),
      completionCriteria: src.completionCriteria || {
        investigationCompleted: "Hotel confirms the verified observation and approved target state.",
        correctionSubmitted: "Responsible party applies the approved correction on the affected page(s).",
        correctionVerifiedLive: canon.desiredOutcome || "Live page matches the approved facts; evidence archived.",
        noChangeRequired: "Only if leadership documents that no change is required, with evidence retained.",
      },
      requiresHotelApproval:
        src.requiresHotelApproval != null ? src.requiresHotelApproval : true,
      hotelApprovalNotes:
        src.hotelApprovalNotes ||
        "Confirm approved facts before publishing corrections on brand or partner channels.",
      supportingEvidence: evidence.length ? evidence : src.supportingEvidence || [],
      technicalDetails: {
        urls: urls.length
          ? urls
          : src.technicalDetails?.urls || [],
        changesOrInvestigation:
          steps.length
            ? steps
            : src.technicalDetails?.changesOrInvestigation || steps,
        evidence: evidence.length
          ? evidence
          : src.technicalDetails?.evidence || evidence,
        verificationMethod:
          src.technicalDetails?.verificationMethod ||
          "Live-page QA with archived screenshots and hotel confirmation where required.",
        dependencies: src.technicalDetails?.dependencies || [],
        notes: src.technicalDetails?.notes || canon.researchLimitations || "",
      },
      implementationStatus: src.implementationStatus || "Awaiting hotel decision",
      claimType: src.claimType || "REQUIRES_HOTEL_CONFIRMATION",
      // Canonical overlays
      actionTitle: canon.actionTitle,
      desiredOutcome: canon.desiredOutcome,
      classification: canon.classification,
      workstreamTheme: canon.workstreamTheme,
      primaryOwner: canon.primaryOwner,
      affectedPlatform: canon.platform || canon.affectedPlatform || src.affectedPlatform,
      businessRelevance: canon.commercialRationale || src.businessRelevance,
      recommendedAction: canon.actionTitle || src.recommendedAction,
    };
  });
}

/**
 * Attach register to payload for report generation (non-destructive to research record).
 * Customer-facing action surfaces use the canonical register exclusively.
 */
export function applyCanonicalActionRegisterToPayload(payload) {
  const sourceActions = payload.actionsResearchRecord || payload.actions || [];
  const register = buildCanonicalActionRegister({
    ...payload,
    actions: sourceActions,
  });
  const projected = projectCanonicalActionsForPayload(register, sourceActions);
  let next = {
    ...payload,
    customerActionRegister: register,
    actionsResearchRecord: sourceActions,
    // Contract-complete projected actions (canonical priority/title/steps/outcome).
    actions: projected,
    actionOverview: register.actions.map((a) => ({
      actionId: a.actionId,
      priority: a.priority,
      headline: a.actionTitle || a.headline,
      businessRelevance: a.commercialRationale || a.businessRelevance,
      accountableOwner: a.primaryOwner || a.accountableOwner,
      desiredOutcome: a.desiredOutcome,
      suggestedTimeframe:
        a.estimatedEffort === "Low"
          ? "Within 2 weeks"
          : a.estimatedEffort === "High"
            ? "30–60 days"
            : "Within 30 days",
    })),
    actionOverviewIntro: `${register.canonicalActionCount} hotel-specific actions at a glance; detailed evidence and agency steps follow.`,
  };
  next = syncExecutivePrioritiesFromRegister(next, register);
  return { payload: next, register };
}

/**
 * Integrity defects for CQA / stage 11.5.
 */
export function detectCanonicalActionIntegrityDefects(payload) {
  const register =
    payload.customerActionRegister || buildCanonicalActionRegister(payload);
  const defects = [];
  for (const id of register.integrity?.titleOnlySteps || []) {
    defects.push({
      type: "TITLE_ONLY_IMPLEMENTATION_STEPS",
      severity: "CRITICAL",
      section: `actions:${id}`,
      message: "Implementation steps merely repeat the action title",
      actionId: id,
    });
  }
  for (const id of register.integrity?.titleStepMisaligned || []) {
    defects.push({
      type: "ACTION_TITLE_STEP_MISMATCH",
      severity: "HIGH",
      section: `actions:${id}`,
      message: "Action title is not semantically aligned with implementation steps",
      actionId: id,
    });
  }
  // Duplicate spa / meeting themes remaining
  const byTheme = new Map();
  for (const a of register.actions) {
    const t = a.workstreamTheme || detectWorkstreamTheme(a);
    if (String(t).startsWith("unique:")) continue;
    if (!byTheme.has(t)) byTheme.set(t, []);
    byTheme.get(t).push(a.actionId);
  }
  for (const [theme, ids] of byTheme) {
    if (ids.length > 1) {
      defects.push({
        type: "SEMANTIC_DUPLICATE_WORKSTREAM",
        severity: "HIGH",
        section: "customerActionRegister",
        message: `Multiple canonical actions share theme ${theme}`,
        actionIds: ids,
      });
    }
  }

  for (const a of register.actions) {
    const steps = a.recommendedSteps || [];
    const genericCount = steps.filter((s) => isGenericStep(s)).length;
    if (genericCount > 0 || steps.length < 2) {
      defects.push({
        type: "INCOMPLETE_OR_GENERIC_IMPLEMENTATION_STEPS",
        severity: "HIGH",
        section: `actions:${a.actionId}`,
        message: "Canonical action has incomplete or generic implementation steps",
        actionId: a.actionId,
      });
    }
    if (
      !a.desiredOutcome ||
      GENERIC_DESIRED_OUTCOME_PATTERNS.some((re) => re.test(a.desiredOutcome))
    ) {
      defects.push({
        type: "BOILERPLATE_DESIRED_OUTCOME",
        severity: "HIGH",
        section: `actions:${a.actionId}`,
        message: "Canonical action lacks a specific desired outcome",
        actionId: a.actionId,
      });
    }
  }

  // Reject the same generic outcome reused across different workstreams
  const outcomeBuckets = new Map();
  for (const a of register.actions) {
    const o = String(a.desiredOutcome || "").trim();
    if (!o) continue;
    if (!outcomeBuckets.has(o)) outcomeBuckets.set(o, []);
    outcomeBuckets.get(o).push(a.actionId);
  }
  for (const [outcome, ids] of outcomeBuckets) {
    if (
      ids.length >= 2 &&
      GENERIC_DESIRED_OUTCOME_PATTERNS.some((re) => re.test(outcome))
    ) {
      defects.push({
        type: "REPEATED_GENERIC_DESIRED_OUTCOME",
        severity: "HIGH",
        section: "customerActionRegister",
        message: `Generic desired outcome repeated across ${ids.length} actions`,
        actionIds: ids,
      });
    }
  }

  defects.push(...detectCrossSectionActionIntegrityDefects(payload, register));
  return defects;
}

/**
 * Fail when Executive Summary / overview priorities disagree with the register.
 */
export function detectCrossSectionActionIntegrityDefects(payload, registerIn) {
  const register =
    registerIn ||
    payload.customerActionRegister ||
    buildCanonicalActionRegister(payload);
  const defects = [];
  const byId = new Map(register.actions.map((a) => [a.actionId, a]));

  const checkRow = (row, section) => {
    if (!row) return;
    const id = row.actionId;
    const title = norm(row.action || row.headline || row.actionTitle || "");
    let canon = id ? byId.get(id) : null;
    if (!canon && title) {
      canon = register.actions.find(
        (a) =>
          norm(a.actionTitle) === title ||
          norm(a.actionTitle).includes(title.slice(0, 28)) ||
          title.includes(norm(a.actionTitle).slice(0, 28))
      );
    }
    if (!canon) return;
    const rowPri = String(row.priority || "").toUpperCase().replace(/PRIORITY/g, "").trim();
    const canonPri = String(canon.priority || "").toUpperCase();
    if (rowPri && canonPri && !rowPri.includes(canonPri) && !canonPri.includes(rowPri.split(/\s+/)[0])) {
      defects.push({
        type: "CROSS_SECTION_PRIORITY_CONFLICT",
        severity: "CRITICAL",
        section,
        message: `Priority conflict for "${canon.actionTitle}": ${section}=${row.priority}, register=${canon.priority}`,
        actionId: canon.actionId,
      });
    }
    const rowOwner = norm(row.owner || row.accountableOwner || row.primaryOwner || "");
    const canonOwner = norm(canon.primaryOwner || canon.accountableOwner || "");
    if (
      rowOwner &&
      canonOwner &&
      rowOwner !== canonOwner &&
      !rowOwner.includes(canonOwner.slice(0, 18)) &&
      !canonOwner.includes(rowOwner.slice(0, 18))
    ) {
      defects.push({
        type: "CROSS_SECTION_OWNER_CONFLICT",
        severity: "HIGH",
        section,
        message: `Owner conflict for "${canon.actionTitle}"`,
        actionId: canon.actionId,
      });
    }
  };

  for (const p of payload.executiveSummary?.priorities || []) {
    checkRow(p, "executiveSummary.priorities");
  }
  for (const p of payload.actionOverview || []) {
    checkRow(p, "actionOverview");
  }
  return defects;
}
