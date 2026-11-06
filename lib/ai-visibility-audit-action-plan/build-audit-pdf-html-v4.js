/**
 * AVA Customer Report V4 — concise management PDF HTML.
 * Preserves approved Executive Audit Summary; sections are views of the
 * canonical customer action register (not independently rewritten actions).
 */
import {
  AI_VISIBILITY_AUDIT_RENDERER,
  AI_VISIBILITY_AUDIT_REPORT_FAMILY,
  assertAuditPdfPayload,
} from "./audit-pdf-contract-v1.js";
import {
  renderCover,
  renderExecutive,
} from "./build-audit-pdf-html-v1.js";
import { LISTING_EXISTENCE } from "./platform-listing-discovery-v3.js";
import {
  applyCanonicalActionRegisterToPayload,
  syncExecutivePrioritiesFromRegister,
} from "./canonical-action-register-v1.js";
import {
  customerPageLabel,
  customerVisibleText,
  extractHttpUrl,
} from "./customer-facing-serialize-v1.js";
import { buildDigitalAuthorityCustomerCards } from "./digital-authority-customer-v1.js";
import { replaceInternalLanguage } from "./customer-facing-language-v1.js";

export const AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V4 =
  "AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V4";

function esc(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function escAttr(s) {
  return esc(s).replace(/'/g, "&#39;");
}

function evidenceLink(url, label) {
  const u = extractHttpUrl(url);
  if (!u) return "";
  return `<a class="ava-evidence-link" href="${escAttr(u)}" target="_blank" rel="noopener">${esc(
    customerVisibleText(label, "View source")
  )}</a>`;
}

/** Map internal destination enums to customer-facing link labels. */
function customerDestinationLabel(status) {
  switch (String(status || "").toUpperCase()) {
    case "CURRENT_CANONICAL_OR_OFFICIAL":
      return "Official / approved hotel URL";
    case "CURRENT_BRAND_PROPERTY":
      return "Brand property URL";
    case "HISTORICAL_OR_LEGACY_DOMAIN":
      return "Historical / legacy domain";
    case "THIRD_PARTY_OR_UNRESOLVED":
      return "Third-party or unresolved URL";
    case "ACCESS_RESTRICTED":
      return "Access-restricted destination";
    case "OPPORTUNITY":
      return "Opportunity destination";
    case "NONE":
    case "UNLINKED_OR_UNKNOWN":
      return "Destination URL";
    default:
      return "Destination URL";
  }
}

function priorityRank(p) {
  const s = String(p || "").toUpperCase();
  if (s.includes("CRITICAL")) return 0;
  if (s.includes("HIGH")) return 1;
  if (s.includes("MEDIUM")) return 2;
  return 3;
}

function ensureRegister(audit) {
  // Always rebuild from research actions — never trust a stale customerActionRegister
  // attached to payload from a prior register version.
  const researchActions =
    audit.actionsResearchRecord ||
    (audit.customerActionRegister ? audit.actionsResearchRecord : null) ||
    audit.actions ||
    [];
  // Prefer original research record when present; avoid re-canonicalizing already-canonical rows.
  const source =
    Array.isArray(audit.actionsResearchRecord) && audit.actionsResearchRecord.length
      ? audit.actionsResearchRecord
      : researchActions;
  const { payload: base, register } = applyCanonicalActionRegisterToPayload({
    ...audit,
    actions: source,
    actionsResearchRecord: source,
  });
  const synced = syncExecutivePrioritiesFromRegister(base, register);
  return {
    ...synced,
    _researchActions: source,
    // Prefer projected contract-complete actions; keep register for integrity metadata.
    actions: synced.actions?.length ? synced.actions : register.actions,
    customerActionRegister: register,
  };
}

const PLATFORM_REGISTRY_LABELS = Object.freeze({
  BRAND_PROPERTY_PAGE: "Brand property page",
  HOTEL_OPERATED_WEBSITE: "Hotel-operated website",
  GOOGLE_BUSINESS_PROFILE: "Google Business Profile / Maps",
  TRIPADVISOR: "Tripadvisor",
  OTA_BOOKING: "Booking.com",
  OTA_EXPEDIA: "Expedia",
  APPLE_MAPS: "Apple Maps",
  BING_PLACES: "Bing Places",
  CVENT: "Cvent",
  DESTINATION_CVB: "Destination / CVB listing",
  SPECIALIST_DIRECTORY: "Specialist directory",
});

const PLATFORM_STATUS_CUSTOMER = Object.freeze({
  ASSESSED: "Verified listing — content reviewed in scope",
  IDENTIFIED_NOT_ASSESSED: "Identified — content not fully assessed in this pass",
  IDENTIFIED_NOT_FULLY_ASSESSED: "Identified — content not fully assessed in this pass",
  ACCESS_RESTRICTED: "Verified listing — content partially restricted",
  NOT_FOUND: "Listing requiring further verification",
  NOT_RELEVANT: "Not applicable",
  VERIFIED_CORRECT: "Verified listing — no material issue in scope",
  RESEARCH_INCOMPLETE: "Listing requiring further verification",
});

function registryFallbackPlatformRows(audit) {
  const dcr = audit.digitalContentReadiness || {};
  const registry =
    dcr.platformCoverageRegistry ||
    audit.platformListingDiscoveryV3?.platformCoverageRegistry ||
    audit.platformListingDiscoveryV2?.platformCoverageRegistry ||
    {};
  const inventory = Array.isArray(dcr.sourceInventory) ? dcr.sourceInventory : [];
  const corrections = [
    ...(Array.isArray(dcr.platformCorrections) ? dcr.platformCorrections : []),
    ...(Array.isArray(audit.digitalPlatformCorrectionRegister?.corrections)
      ? audit.digitalPlatformCorrectionRegister.corrections
      : []),
  ];
  const rows = [];

  for (const [key, status] of Object.entries(registry)) {
    if (!status) continue;
    const label = PLATFORM_REGISTRY_LABELS[key] || String(key).replace(/_/g, " ");
    const inv = inventory.find(
      (s) =>
        String(s.sourceCategory || "").toUpperCase() === key ||
        normLoose(s.platformName).includes(normLoose(label).slice(0, 12))
    );
    const corr = corrections.find((c) =>
      normLoose(c.platform || "").includes(normLoose(label).slice(0, 10))
    );
    const listingUrl = extractHttpUrl(inv?.url || inv?.evidenceUrl || corr?.url);
    const statusLabel =
      PLATFORM_STATUS_CUSTOMER[String(status).toUpperCase()] ||
      customerVisibleText(replaceInternalLanguage(String(status)), "Assessment status");
    let finding =
      "Listing status recorded from the digital-content research inventory for this hotel.";
    let action =
      "No content correction prescribed from the completed assessment scope.";
    let assessmentClass = "PARTIALLY_ASSESSED";
    let actionable = false;

    if (/NOT_RELEVANT|NOT_APPLICABLE/i.test(status)) {
      assessmentClass = "NOT_APPLICABLE";
      finding = "Not applicable to this hotel within the AVA platform scope.";
      action = "No action required.";
    } else if (corr) {
      // (3) Verified issue or improvement opportunity
      assessmentClass = "VERIFIED_CONTENT_IMPROVEMENT";
      actionable = true;
      finding = customerVisibleText(
        corr.discrepancyOrOpportunity || corr.currentlyPublished,
        finding
      );
      action = customerVisibleText(
        corr.recommendedCorrection || corr.completionCriteria,
        "Confirm approved facts, then update the live listing."
      );
    } else if (/ASSESSED|VERIFIED_CORRECT/i.test(status)) {
      // (1)+(2) Exact listing + field-level review → (6) no material issue in scope
      assessmentClass = "VERIFIED_AND_CONSISTENT";
      finding = listingUrl
        ? `Exact listing verified (${listingUrl.replace(/^https?:\/\//i, "").split("/")[0]}). Field-level content reviewed in scope — no material discrepancy confirmed.`
        : "Exact listing verified and field-level content reviewed in scope — no material discrepancy confirmed.";
      action = "No customer action required from this assessment scope.";
    } else if (/ACCESS_RESTRICTED/i.test(status)) {
      // (4) Partial assessment due to access restrictions
      assessmentClass = "PARTIALLY_ASSESSED";
      finding = listingUrl
        ? "Exact listing verified; important page content could not be fully audited due to access restrictions. Listing existence is not proof that content fields are correct."
        : "Listing reachable but important page content could not be fully audited due to access restrictions.";
      action =
        "Hotel may spot-check key facts when authenticated access is available.";
    } else if (/IDENTIFIED_NOT|RESEARCH_INCOMPLETE|NOT_FOUND/i.test(status)) {
      // (5) Outstanding research follow-up — distinguish URL inventoried vs not
      assessmentClass = "RESEARCH_FOLLOW_UP";
      if (listingUrl) {
        finding =
          "Exact listing URL inventoried; field-level content was not fully assessed in this unpaid research pass. Do not treat listing presence as verified content accuracy.";
        action =
          "Complete field-level content verification before prescribing a correction; confirm critical facts on the official site first.";
      } else {
        finding =
          "Platform category identified in research inventory; exact listing URL not yet confirmed. Content was not field-assessed.";
        action =
          "Confirm the exact property listing URL before treating as a content defect.";
      }
    }

    rows.push({
      platform: label,
      url: listingUrl,
      status: statusLabel,
      finding: customerVisibleText(replaceInternalLanguage(finding)),
      action: customerVisibleText(replaceInternalLanguage(action)),
      assessmentClass,
      actionable,
      owner: corr?.responsibleOrganization || null,
      label: customerVisibleText(inv?.platformName || "View listing", "View listing"),
      confidence: actionable ? "EVIDENCE_BACKED" : "LISTING_ONLY",
    });
  }

  // Always surface hotel-operated / brand inventory URLs even if registry sparse
  for (const inv of inventory) {
    const plat = customerVisibleText(inv.platformName || inv.sourceCategory, "Official source");
    if (rows.some((r) => normLoose(r.platform) === normLoose(plat))) continue;
    rows.push({
      platform: plat,
      url: extractHttpUrl(inv.url || inv.evidenceUrl),
      status: "Verified listing — content reviewed in scope",
      finding: customerVisibleText(
        inv.intendedPurpose || "Official discovery path inventoried for this hotel."
      ),
      action: "Keep approved facts aligned on this official source before cascading corrections.",
      assessmentClass: "VERIFIED_AND_CONSISTENT",
      actionable: false,
      owner: inv.controllingOrganization || null,
      label: "View source",
      confidence: "LISTING_ONLY",
    });
  }

  return rows;
}

function normLoose(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function pickPriorityFindings(audit) {
  return (audit.actions || [])
    .filter((a) => /HIGH|CRITICAL|MEDIUM/i.test(String(a.priority || "")))
    .sort((a, b) => priorityRank(a.priority) - priorityRank(b.priority))
    .slice(0, 6)
    .map((a) => ({
      actionId: a.actionId,
      priority: a.priority || "HIGH",
      platform: customerVisibleText(a.platform || a.affectedPlatform, "Digital presence"),
      problem: customerVisibleText(a.verifiedObservation || a.whatWeFound || a.headline),
      implication: customerVisibleText(a.commercialRationale || a.businessRelevance || a.whyItMatters),
      action: customerVisibleText(a.actionTitle || a.recommendedAction || a.headline),
      owner: customerVisibleText(a.primaryOwner || a.accountableOwner, "Hotel Marketing"),
      evidenceUrl: extractHttpUrl(a.evidenceRefs?.[0] || a.supportingEvidence?.[0] || a.affectedUrls?.[0]),
      evidenceLabel: customerPageLabel(
        a.evidenceRefs?.[0] || a.supportingEvidence?.[0] || "View evidence",
        "View evidence"
      ),
      steps: a.recommendedSteps || [],
      desiredOutcome: customerVisibleText(
        a.desiredOutcome,
        "Approved public facts live and consistent on the affected pages."
      ),
    }));
}

/**
 * Platform assessment classes A–F from the quality standard.
 */
function classifyPlatformRow(assessment, matchingActions) {
  const listing = assessment.listingExistence;
  const content = String(assessment.contentAssessment || "");
  const notes = String(assessment.notes || "");
  const actionHit = matchingActions[0] || null;

  if (listing === LISTING_EXISTENCE.RESEARCH_INCOMPLETE || !assessment.url) {
    return {
      assessmentClass: "RESEARCH_FOLLOW_UP",
      statusLabel: "Research follow-up",
      finding:
        "Exact listing not yet confirmed — additional verification required before prescribing a correction.",
      action: "Complete listing verification before treating as a defect.",
      actionable: false,
    };
  }

  if (/ACCESS_RESTRICTED/i.test(content) || /access restricted/i.test(notes)) {
    return {
      assessmentClass: "PARTIALLY_ASSESSED",
      statusLabel: "Partially assessed",
      finding:
        "Exact listing verified; important page content could not be fully audited due to access restrictions. This is not evidence of incorrect hotel content.",
      action: actionHit
        ? customerVisibleText(actionHit.actionTitle || actionHit.recommendedAction)
        : "No content correction prescribed from restricted page body — hotel may spot-check key facts when logged in.",
      actionable: Boolean(actionHit),
      owner: actionHit?.primaryOwner || null,
    };
  }

  if (actionHit && /CORRECTION/i.test(actionHit.classification || "")) {
    return {
      assessmentClass: "VERIFIED_CORRECTION",
      statusLabel: "Verified correction",
      finding: customerVisibleText(actionHit.verifiedObservation || actionHit.whatWeFound),
      action: customerVisibleText(actionHit.actionTitle || actionHit.recommendedAction),
      actionable: true,
      owner: actionHit.primaryOwner || actionHit.accountableOwner,
    };
  }

  if (actionHit && /CONTENT_IMPROVEMENT|DISTRIBUTION/i.test(actionHit.classification || "")) {
    return {
      assessmentClass: /DISTRIBUTION/i.test(actionHit.classification || "")
        ? "DISTRIBUTION_OPPORTUNITY"
        : "VERIFIED_CONTENT_IMPROVEMENT",
      statusLabel: /DISTRIBUTION/i.test(actionHit.classification || "")
        ? "Distribution opportunity"
        : "Content improvement",
      finding: customerVisibleText(actionHit.verifiedObservation || actionHit.whatWeFound),
      action: customerVisibleText(actionHit.actionTitle || actionHit.recommendedAction),
      actionable: true,
      owner: actionHit.primaryOwner || actionHit.accountableOwner,
    };
  }

  if (listing === LISTING_EXISTENCE.EXACT_LISTING_VERIFIED) {
    return {
      assessmentClass: "VERIFIED_AND_CONSISTENT",
      statusLabel: "Verified — no material issue in scope",
      finding:
        "Exact property listing verified. No material discrepancy was confirmed within the completed assessment scope.",
      action: "No customer action required from this audit pass.",
      actionable: false,
    };
  }

  return {
    assessmentClass: "PARTIALLY_ASSESSED",
    statusLabel: "Partially assessed",
    finding: customerVisibleText(assessment.notes || "Listing present; content review incomplete."),
    action: "Hotel confirmation recommended for remaining fields.",
    actionable: false,
  };
}

function platformRows(audit) {
  const fromV3 =
    audit.platformListingDiscoveryV3?.assessments ||
    audit.platformListingDiscoveryV2?.assessments ||
    audit.platformCoverageAssessment?.assessments ||
    audit.digitalPlatformReadinessV3?.rows ||
    [];
  const actions = audit.actions || [];

  let rows = fromV3
    .filter((a) => a.relevant !== false)
    .filter(
      (a) =>
        a.listingExistence === LISTING_EXISTENCE.EXACT_LISTING_VERIFIED ||
        a.listingExistence === LISTING_EXISTENCE.RESEARCH_INCOMPLETE ||
        a.assessmentStatus ||
        (a.url && a.listingExistence !== LISTING_EXISTENCE.NOT_APPLICABLE)
    )
    .map((a) => {
      if (a.assessmentStatus && !a.listingExistence) {
        // digitalPlatformReadinessV3 row shape
        return {
          platform: customerVisibleText(a.platform || a.platformName, "Platform"),
          url: extractHttpUrl(a.url),
          status: customerVisibleText(
            a.assessmentStatusLabel ||
              PLATFORM_STATUS_CUSTOMER[a.assessmentStatus] ||
              a.assessmentStatus,
            "Assessment"
          ),
          finding: customerVisibleText(
            replaceInternalLanguage(a.verifiedFinding || a.finding || ""),
            "Listing inventoried in digital platform research."
          ),
          action: customerVisibleText(
            replaceInternalLanguage(a.recommendedCorrection || a.nextAction || ""),
            "No material correction prescribed from this assessment scope."
          ),
          assessmentClass: /CORRECTION|IMPROVEMENT|OPPORTUNITY/i.test(
            a.recommendedCorrection || a.group || ""
          )
            ? "VERIFIED_CONTENT_IMPROVEMENT"
            : /IDENTIFIED_NOT|RESEARCH/i.test(a.assessmentStatus || "")
              ? "RESEARCH_FOLLOW_UP"
              : "VERIFIED_AND_CONSISTENT",
          actionable: Boolean(a.recommendedCorrection),
          owner: a.responsibleParty || null,
          label: customerVisibleText(a.evidenceLabel, "View listing"),
          confidence: a.recommendedCorrection ? "EVIDENCE_BACKED" : "LISTING_ONLY",
        };
      }
      const plat = String(a.platform || a.category || "");
      const matching = actions.filter((act) => {
        const blob = `${act.platform || ""} ${act.affectedPlatform || ""} ${act.actionTitle || ""} ${act.verifiedObservation || ""}`.toLowerCase();
        const urls = (act.affectedUrls || act.supportingEvidence || [])
          .map((e) => extractHttpUrl(e) || "")
          .join(" ");
        if (/GOOGLE/i.test(plat) && /google|maps|gbp/i.test(blob + urls)) return true;
        if (/TRIPADVISOR/i.test(plat) && /tripadvisor/i.test(blob + urls)) return true;
        if (/BOOKING/i.test(plat) && /booking\.com/i.test(blob + urls)) return true;
        if (/EXPEDIA|HOTELS\.COM/i.test(plat) && /expedia|hotels\.com/i.test(blob + urls)) return true;
        if (/BRAND|HILTON|MARRIOTT/i.test(plat) && /brand|hilton|marriott|curio|luxury collection/i.test(blob + urls)) return true;
        if (/HOTEL.?OPERATED|WEBSITE/i.test(plat) && /website|hotel-operated|domain|meetings page|waterstoneboca|official/i.test(blob + urls)) return true;
        if (/APPLE/i.test(plat) && /apple/i.test(blob + urls)) return true;
        if (/CVENT/i.test(plat) && /cvent/i.test(blob + urls)) return true;
        if (/DESTINATION|CVB/i.test(plat) && /destination|cvb|visit-|tourism/i.test(blob + urls)) return true;
        return false;
      });
      const classified = classifyPlatformRow(a, matching);
      return {
        platform: customerVisibleText(a.platform || a.category, "Platform"),
        url: extractHttpUrl(a.url),
        status: classified.statusLabel,
        finding: classified.finding,
        action: classified.action,
        assessmentClass: classified.assessmentClass,
        actionable: classified.actionable,
        owner: classified.owner || null,
        label: customerVisibleText(a.evidenceLabel, "View listing"),
        confidence: classified.actionable ? "EVIDENCE_BACKED" : "LISTING_ONLY",
      };
    });

  if (!rows.length) {
    rows = registryFallbackPlatformRows(audit);
  }

  // Prefer actionable + incomplete first; keep verified-consistent compact
  const actionable = rows.filter((r) => r.actionable || r.assessmentClass === "RESEARCH_FOLLOW_UP" || r.assessmentClass === "PARTIALLY_ASSESSED");
  const clean = rows.filter(
    (r) =>
      r.assessmentClass === "VERIFIED_AND_CONSISTENT" ||
      r.assessmentClass === "NOT_APPLICABLE"
  );
  const other = rows.filter(
    (r) => !actionable.includes(r) && !clean.includes(r)
  );
  return [...actionable, ...other, ...clean].slice(0, 12);
}

function websiteImprovements(audit) {
  return (audit.actions || [])
    .filter((a) =>
      /website|brand|domain|meeting|content|page|official/i.test(
        `${a.platform || ""} ${a.affectedPlatform || ""} ${a.actionTitle || ""} ${a.headline || ""} ${a.whatWeFound || ""}`
      )
    )
    .filter((a) => !/DISTRIBUTION_OPPORTUNITY/i.test(a.classification || "") || /domain|website|meeting/i.test(`${a.actionTitle}${a.platform}`))
    .slice(0, 5)
    .map((a) => {
      const pageRef =
        a.affectedUrls?.[0] ||
        a.technicalDetails?.urls?.[0] ||
        a.supportingEvidence?.[0] ||
        null;
      const pageUrl = extractHttpUrl(pageRef);
      const pageLabel = customerPageLabel(
        pageRef || a.platform || a.affectedPlatform,
        "Hotel / brand page"
      );
      return {
        pageLabel,
        pageUrl,
        issue: customerVisibleText(a.verifiedObservation || a.whatWeFound || a.headline),
        improvement: customerVisibleText(a.actionTitle || a.recommendedAction || a.headline),
        why: customerVisibleText(a.commercialRationale || a.businessRelevance || a.whyItMatters),
        owner: customerVisibleText(a.primaryOwner || a.accountableOwner, "Hotel Marketing"),
        evidenceUrl: pageUrl || extractHttpUrl(a.evidenceRefs?.[0]),
        steps: a.recommendedSteps || [],
        actionId: a.actionId,
      };
    });
}

function authorityBrief(audit) {
  const built = buildDigitalAuthorityCustomerCards(audit);
  return {
    summary: built.summary,
    examples: built.cards.map((c) => ({
      kind: c.kind,
      source: c.source,
      sourceTypeLabel: c.sourceTypeLabel || null,
      detail: c.observation || c.detail,
      observation: c.observation || c.detail,
      why: c.why,
      action: c.action,
      owner: c.owner,
      url: c.url,
      evidenceUrls: c.evidenceUrls || [],
      destinationUrl: c.destinationUrl,
      destinationStatus: c.destinationStatus,
      destinationNote: c.destinationNote,
      researchLimitations: c.researchLimitations,
      consolidatedPageCount: c.consolidatedPageCount,
    })),
  };
}

function roadmap(audit) {
  const actions = [...(audit.actions || [])].sort(
    (a, b) => priorityRank(a.priority) - priorityRank(b.priority)
  );
  const map = (a) => ({
    actionId: a.actionId,
    what: customerVisibleText(a.actionTitle || a.recommendedAction || a.headline),
    where: customerVisibleText(a.platform || a.affectedPlatform, "See evidence"),
    who: customerVisibleText(a.primaryOwner || a.accountableOwner, "Hotel Marketing"),
    priority: a.priority || "—",
    effort: customerVisibleText(a.estimatedEffort || a.effort, "TBD with hotel team"),
    outcome: customerVisibleText(
      a.desiredOutcome || a.commercialRationale || a.businessRelevance,
      "Clearer, consistent public facts"
    ),
    evidenceUrl: extractHttpUrl(
      a.evidenceRefs?.[0] || a.supportingEvidence?.[0] || a.affectedUrls?.[0]
    ),
  });
  return {
    immediate: actions
      .filter((a) => /HIGH|CRITICAL/i.test(String(a.priority || "")))
      .map(map),
    next30: actions
      .filter((a) => /MEDIUM/i.test(String(a.priority || "")))
      .map(map),
    subsequent: actions
      .filter((a) => !/HIGH|CRITICAL|MEDIUM/i.test(String(a.priority || "")))
      .map(map),
  };
}

function renderPriorityFindings(items) {
  const cards = items
    .map((it) => {
      const steps =
        Array.isArray(it.steps) && it.steps.length
          ? `<ol style="margin:6px 0 8px 18px;padding:0;">${it.steps
              .slice(0, 5)
              .map((s) => `<li style="margin:0 0 2px;">${esc(customerVisibleText(s))}</li>`)
              .join("")}</ol>`
          : "";
      return (
        `<div class="ava-v4-action-card" style="border:1px solid #d8d2c8;padding:12px 14px;margin:0 0 12px;border-radius:4px;page-break-inside:avoid;">` +
        `<p style="margin:0 0 6px;font-size:11px;letter-spacing:.04em;text-transform:uppercase;color:#6b6358;"><strong>Priority:</strong> ${esc(
          it.priority
        )} · <strong>Platform:</strong> ${esc(it.platform)}</p>` +
        `<p style="margin:0 0 6px;"><strong>Issue:</strong> ${esc(it.problem)}</p>` +
        `<p style="margin:0 0 6px;"><strong>Why it matters:</strong> ${esc(it.implication)}</p>` +
        `<p style="margin:0 0 4px;"><strong>Recommended action:</strong> ${esc(it.action)}</p>` +
        steps +
        `<p style="margin:0 0 6px;"><strong>Desired outcome:</strong> ${esc(
          it.desiredOutcome || ""
        )}</p>` +
        `<p style="margin:0 0 6px;"><strong>Responsible party:</strong> ${esc(it.owner)}</p>` +
        `<p style="margin:0;">${
          it.evidenceUrl
            ? `<strong>Evidence:</strong> ${evidenceLink(it.evidenceUrl, it.evidenceLabel)}`
            : `<strong>Evidence:</strong> Requires hotel confirmation / further investigation`
        }</p>` +
        `</div>`
      );
    })
    .join("");
  return (
    `<section class="drs-section ava-section" data-ava-section="priority-findings">` +
    `<h2 class="drs-section-h">2. Priority Findings &amp; Opportunities</h2>` +
    `<p class="drs-body">The most important verified corrections and opportunities for this hotel. Full research detail remains in the AVA record.</p>` +
    (cards || `<p class="drs-body">No high-priority actions packaged for this draft.</p>`) +
    `</section>`
  );
}

function renderPlatformHealth(rows) {
  const actionable = rows.filter(
    (r) =>
      r.actionable ||
      (r.assessmentClass !== "VERIFIED_AND_CONSISTENT" &&
        r.assessmentClass !== "NOT_APPLICABLE")
  );
  const consistent = rows.filter((r) => r.assessmentClass === "VERIFIED_AND_CONSISTENT");
  const tableRows = actionable.length ? actionable : rows;

  const body = tableRows
    .map(
      (r) =>
        `<tr><td>${r.url ? evidenceLink(r.url, r.platform) : esc(r.platform)}</td><td>${
          r.url ? evidenceLink(r.url, r.label) : "—"
        }</td><td>${esc(r.status)}</td><td>${esc(r.finding)}</td><td>${esc(r.action)}${
          r.owner ? `<br><span class="drs-muted">Owner: ${esc(r.owner)}</span>` : ""
        }</td></tr>`
    )
    .join("");

  const consistentNote = consistent.length && actionable.length
    ? `<p class="drs-muted" style="margin-top:8px;">Also verified with no material issue in this assessment scope: ${esc(
        consistent.map((r) => r.platform).join("; ")
      )}.</p>`
    : "";

  const emptyFallback =
    `<tr><td colspan="5">Platform assessment inventory was not available for this hotel. Research readiness requires a documented listing pass before content corrections can be prescribed. See Methodology &amp; Limitations.</td></tr>`;

  return (
    `<section class="drs-section ava-section" data-ava-section="platform-health">` +
    `<h2 class="drs-section-h">3. Digital Platform Assessment</h2>` +
    `<p class="drs-body">Exact verified listings where available. Rows distinguish corrections, content improvements, distribution opportunities, verified-consistent listings, partial assessments, and research follow-ups. Access restrictions are not treated as content defects. Listing verification and content verification are tracked separately.</p>` +
    `<div class="drs-table-wrap"><table class="drs-table bas-brief-table">` +
    `<thead><tr><th>Platform</th><th>Listing</th><th>Assessment</th><th>Finding</th><th>Next action</th></tr></thead>` +
    `<tbody>${body || emptyFallback}</tbody></table></div>` +
    consistentNote +
    `</section>`
  );
}

function renderWebsite(items) {
  const cards = items
    .map((it) => {
      const steps =
        Array.isArray(it.steps) && it.steps.length
          ? `<ol style="margin:6px 0 8px 18px;padding:0;">${it.steps
              .slice(0, 4)
              .map((s) => `<li>${esc(customerVisibleText(s))}</li>`)
              .join("")}</ol>`
          : "";
      return (
        `<div class="ava-v4-action-card" style="border:1px solid #d8d2c8;padding:12px 14px;margin:0 0 10px;border-radius:4px;page-break-inside:avoid;">` +
        `<p style="margin:0 0 6px;"><strong>Page:</strong> ${
          it.pageUrl
            ? evidenceLink(it.pageUrl, it.pageLabel)
            : esc(it.pageLabel)
        }</p>` +
        `<p style="margin:0 0 6px;"><strong>Issue:</strong> ${esc(it.issue)}</p>` +
        `<p style="margin:0 0 4px;"><strong>Recommended action:</strong> ${esc(it.improvement)}</p>` +
        steps +
        `<p style="margin:0 0 6px;"><strong>Why it matters:</strong> ${esc(it.why)}</p>` +
        `<p style="margin:0 0 6px;"><strong>Responsible party:</strong> ${esc(it.owner)}</p>` +
        `<p style="margin:0;">${
          it.evidenceUrl
            ? `<strong>Evidence:</strong> ${evidenceLink(it.evidenceUrl, "View page")}`
            : ""
        }</p>` +
        `</div>`
      );
    })
    .join("");

  return (
    `<section class="drs-section ava-section" data-ava-section="website-content">` +
    `<h2 class="drs-section-h">4. Website &amp; Content Improvements</h2>` +
    `<p class="drs-body">Specific hotel or brand pages that affect how travelers understand and select the property. This section does not repeat ADP traveler-intent analysis.</p>` +
    (cards ||
      `<p class="drs-body">No website-specific high-priority gaps packaged beyond the Executive Summary priorities.</p>`) +
    `</section>`
  );
}

function renderAuthority(brief) {
  const cards = brief.examples
    .map((e) => {
      const extraLinks = (e.evidenceUrls || [])
        .slice(1, 4)
        .map((u, i) => evidenceLink(u, `Related page ${i + 2}`))
        .filter(Boolean)
        .join(" · ");
      return (
        `<div class="ava-v4-action-card" style="border:1px solid #d8d2c8;padding:12px 14px;margin:0 0 10px;border-radius:4px;page-break-inside:avoid;">` +
        `<p style="margin:0 0 4px;font-size:11px;letter-spacing:.04em;text-transform:uppercase;color:#6b6358;">${esc(
          e.kind
        )}${
          e.consolidatedPageCount > 1
            ? ` · ${e.consolidatedPageCount} pages`
            : ""
        }</p>` +
        `<p style="margin:0 0 6px;"><strong>Source:</strong> ${
          e.url ? evidenceLink(e.url, e.source) : esc(e.source)
        }${extraLinks ? ` · ${extraLinks}` : ""}</p>` +
        `<p style="margin:0 0 6px;"><strong>Observation:</strong> ${esc(
          e.observation || e.detail
        )}</p>` +
        `<p style="margin:0 0 6px;"><strong>Commercial relevance:</strong> ${esc(
          e.why || ""
        )}</p>` +
        (e.destinationUrl
          ? `<p style="margin:0 0 6px;"><strong>Destination:</strong> ${evidenceLink(
              e.destinationUrl,
              customerDestinationLabel(e.destinationStatus)
            )}${
              e.destinationNote
                ? ` — ${esc(e.destinationNote)}`
                : ""
            }</p>`
          : "") +
        `<p style="margin:0 0 6px;"><strong>Recommended action:</strong> ${esc(e.action)}</p>` +
        `<p style="margin:0 0 6px;"><strong>Responsible party:</strong> ${esc(e.owner || "Hotel Marketing")}</p>` +
        (e.researchLimitations
          ? `<p class="drs-muted" style="margin:0;">Limitation: ${esc(e.researchLimitations)}</p>`
          : "") +
        `</div>`
      );
    })
    .join("");

  return (
    `<section class="drs-section ava-section" data-ava-section="digital-authority">` +
    `<h2 class="drs-section-h">5. Digital Authority &amp; Backlink Opportunities</h2>` +
    `<p class="drs-body">${esc(brief.summary)}</p>` +
    cards +
    `<p class="drs-muted">Full referring-domain inventory is retained in AVA research records — not printed here. Directory presence is not counted as a backlink. Healthy existing backlinks are not listed as problems.</p>` +
    `</section>`
  );
}

function renderRoadmap(rm) {
  const rows = [...rm.immediate, ...rm.next30, ...rm.subsequent];
  const timing = (it) => {
    if (rm.immediate.includes(it)) return "Immediate";
    if (rm.next30.includes(it)) return "Next 30 days";
    return "Subsequent";
  };
  const body = rows
    .map(
      (it) =>
        `<tr>` +
        `<td>${esc(it.priority)}</td>` +
        `<td>${esc(it.what)}</td>` +
        `<td>${esc(it.who)}</td>` +
        `<td>${esc(timing(it))}</td>` +
        `<td>${esc(it.effort)}</td>` +
        `<td>${
          it.evidenceUrl
            ? evidenceLink(it.evidenceUrl, "View")
            : "Hotel confirm"
        }</td>` +
        `</tr>`
    )
    .join("");

  return (
    `<section class="drs-section ava-section" data-ava-section="roadmap" style="page-break-inside:auto;">` +
    `<h2 class="drs-section-h">6. Implementation Roadmap</h2>` +
    `<p class="drs-body">Same canonical actions as Priority Findings — execution view only. Escalate brand-controlled edits through the brand digital team when the hotel cannot edit the page directly.</p>` +
    `<div class="drs-table-wrap"><table class="drs-table bas-brief-table">` +
    `<thead><tr><th>Priority</th><th>Action</th><th>Owner</th><th>Timing</th><th>Effort</th><th>Evidence</th></tr></thead>` +
    `<tbody>${body || `<tr><td colspan="6">No roadmap actions packaged.</td></tr>`}</tbody></table></div>` +
    `</section>`
  );
}

function renderMethod(audit) {
  const readyRaw =
    audit.platformListingDiscoveryV3?.customerResearchReadiness?.verdict || "";
  const ready = customerVisibleText(
    replaceInternalLanguage(readyRaw),
    readyRaw
      ? String(readyRaw).replace(/_/g, " ").toLowerCase()
      : "completed within the documented public-research scope"
  );
  const assessed =
    audit.reporting?.generatedAt ||
    audit.platformListingDiscoveryV3?.generatedAt ||
    audit.digitalContentReadiness?.generatedAt ||
    "see report footer";
  const dcrLimits = Array.isArray(audit.digitalContentReadiness?.limitations)
    ? audit.digitalContentReadiness.limitations
        .map((l) => customerVisibleText(replaceInternalLanguage(l), ""))
        .filter(Boolean)
        .slice(0, 2)
    : [];
  const limitNote = dcrLimits.length
    ? ` Research limits: ${dcrLimits.join(" ")}`
    : "";
  // Keep methodology a single unbreakable block after the roadmap so Chromium
  // cannot orphan the final word onto a near-empty trailing page.
  return (
    `<section class="drs-section ava-section ava-v4-methodology" data-ava-section="methodology">` +
    `<h2 class="drs-section-h">7. Methodology &amp; Limitations</h2>` +
    `<p class="drs-body ava-v4-methodology__body">Dealality AVA assesses the hotel’s public digital presence. Listing existence and content assessment are tracked separately; Google Travel entities are not treated as Google Business Profile / Maps place listings. Assessment date: ${esc(
      assessed
    )}. Research coverage: ${esc(
      ready
    )}.${esc(limitNote)} ADP traveler-intent detail is limited to the Executive Audit Summary. Methodology limits are not listed as hotel findings.</p>` +
    `</section>`
  );
}

/**
 * @param {object} audit
 */
export function buildAiVisibilityAuditPdfHtmlV4(audit) {
  const gate = assertAuditPdfPayload(audit);
  if (!gate.ok) return { ok: false, errors: gate.errors };

  const cleaned = ensureRegister(audit);

  const findings = pickPriorityFindings(cleaned);
  const platforms = platformRows(cleaned);
  const website = websiteImprovements(cleaned);
  const authority = authorityBrief(cleaned);
  const rm = roadmap(cleaned);

  const inner =
    `<div class="drs-report ava-report ava-report-v4 brand-alignment-snapshot" data-report-family="${AI_VISIBILITY_AUDIT_REPORT_FAMILY}" data-ava-template="${AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V4}" data-ava-renderer="${AI_VISIBILITY_AUDIT_RENDERER}">` +
    renderCover(cleaned) +
    `<div class="adp-mr-report-body ava-report-body">` +
    renderExecutive(cleaned) +
    renderPriorityFindings(findings) +
    renderPlatformHealth(platforms) +
    renderWebsite(website) +
    renderAuthority(authority) +
    `<div class="ava-v4-closing-block">` +
    renderRoadmap(rm) +
    renderMethod(cleaned) +
    `</div>` +
    `</div></div>`;

  const evidenceUrls = [];
  const push = (u) => {
    const url = extractHttpUrl(u);
    if (url) evidenceUrls.push(url);
  };
  for (const f of findings) push(f.evidenceUrl);
  for (const p of platforms) push(p.url);
  for (const w of website) push(w.evidenceUrl);
  for (const e of authority.examples) push(e.url);
  for (const band of [rm.immediate, rm.next30, rm.subsequent]) {
    for (const a of band) push(a.evidenceUrl);
  }

  return {
    ok: true,
    html: inner,
    templateId: AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V4,
    evidenceUrls: [...new Set(evidenceUrls)],
    canonicalActionCount: cleaned.customerActionRegister?.canonicalActionCount ?? null,
    consolidated: cleaned.customerActionRegister?.consolidated || [],
  };
}
