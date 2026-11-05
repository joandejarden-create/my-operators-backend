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

  // completionCriteria object → useful investigation/correction steps
  const cc = a.completionCriteria;
  if (cc && typeof cc === "object" && !Array.isArray(cc)) {
    for (const [k, v] of Object.entries(cc)) {
      if (typeof v === "string" && v.trim() && !/noChangeRequired/i.test(k)) {
        candidates.push(v.trim());
      }
    }
  }

  const cleaned = [];
  const seen = new Set();
  for (const step of candidates) {
    const n = norm(step);
    if (!n || seen.has(n)) continue;
    // Reject title restatement / paraphrase
    if (n === title) continue;
    if (title && (n.includes(title) || title.includes(n)) && n.length < title.length + 12) {
      continue;
    }
    seen.add(n);
    cleaned.push(step);
  }

  if (cleaned.length) return cleaned.slice(0, 6);

  // Synthesize from observation when source steps missing — still must not echo title
  return synthesizeStepsFromFinding(a);
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
  if (/could not be re-?verif|research follow|access restricted|investigate|robots\.txt|403/i.test(blob)) {
    if (/discrepan|conflict|incorrect|wrong|mismatch|versus|vs /i.test(blob)) {
      return ACTION_CLASSIFICATION.VERIFIED_CORRECTION;
    }
    return ACTION_CLASSIFICATION.RESEARCH_FOLLOW_UP;
  }
  if (/missing|incorrect|inconsist|conflict|wrong|outdated|broken|redirect|versus|vs /i.test(blob)) {
    return ACTION_CLASSIFICATION.VERIFIED_CORRECTION;
  }
  if (/opportunity|request|outreach|include|add (a |an |the )?link|listing|hyperlink/i.test(blob)) {
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

function mergeGroup(group, theme, propertyId) {
  const sorted = [...group].sort(
    (a, b) => priorityRank(a.priority) - priorityRank(b.priority)
  );
  const primary = toCanonical(sorted[0], propertyId);
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

function toCanonical(a, propertyId) {
  const evidence = evidenceUrlsFromAction(a);
  const primaryUrl = evidence[0] || null;
  const classification = classify(a);
  const priority = reassessPriority(a, classification);
  const steps = extractImplementationSteps(a);
  const title = customerVisibleText(a.headline || a.recommendedAction, "Action");
  const observation = customerVisibleText(a.whatWeFound || a.summary, "");
  let actionTitle = title;

  // If title/step misaligned with a clearer recommendedAction, prefer headline still but flag via steps
  if (!titlesAligned(actionTitle, steps, observation) && a.recommendedAction) {
    // keep headline; steps already from recommendedSteps
  }

  return {
    actionId: a.actionId,
    propertyId: propertyId || a.propertyId || null,
    findingIds: [a.actionId].filter(Boolean),
    mergedFromActionIds: [a.actionId].filter(Boolean),
    workstreamTheme: detectWorkstreamTheme(a),
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
    commercialRationale: customerVisibleText(
      a.businessRelevance || a.whyItMatters || a.commercialImportance,
      ""
    ),
    recommendedSteps: steps,
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
    businessRelevance: customerVisibleText(
      a.businessRelevance || a.whyItMatters || a.commercialImportance,
      ""
    ),
    whyItMatters: customerVisibleText(
      a.whyItMatters || a.businessRelevance || a.commercialImportance,
      ""
    ),
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
}

/**
 * Build canonical register from payload actions.
 */
export function buildCanonicalActionRegister(payload) {
  const propertyId = payload?.property?.propertyId || null;
  const source = [...(payload?.actions || [])];
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
  for (const key of order) {
    const group = groups.get(key);
    const theme = key.startsWith("theme:") ? key.slice(6) : "unique";
    if (group.length > 1 && !theme.startsWith("unique")) {
      const merged = mergeGroup(group, theme, propertyId);
      actions.push(merged);
      consolidated.push({
        theme,
        keptActionId: group[0].actionId,
        mergedActionIds: group.map((g) => g.actionId).filter(Boolean),
        reason: `Same corrective objective (${theme})`,
      });
    } else {
      actions.push(toCanonical(group[0], propertyId));
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
 * Attach register to payload for report generation (non-destructive).
 */
export function applyCanonicalActionRegisterToPayload(payload) {
  const sourceActions = payload.actionsResearchRecord || payload.actions || [];
  const register = buildCanonicalActionRegister({
    ...payload,
    actions: sourceActions,
  });
  const next = {
    ...payload,
    customerActionRegister: register,
    actionsResearchRecord: sourceActions,
    actions: sourceActions,
    actionOverview: register.actions.map((a) => ({
      actionId: a.actionId,
      priority: a.priority,
      headline: a.actionTitle || a.headline,
      businessRelevance: a.commercialRationale || a.businessRelevance,
      accountableOwner: a.primaryOwner || a.accountableOwner,
      suggestedTimeframe:
        a.estimatedEffort === "Low"
          ? "Within 2 weeks"
          : a.estimatedEffort === "High"
            ? "30–60 days"
            : "Within 30 days",
    })),
    actionOverviewIntro: `${register.canonicalActionCount} hotel-specific actions at a glance; detailed evidence and agency steps follow.`,
  };
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
  return defects;
}
