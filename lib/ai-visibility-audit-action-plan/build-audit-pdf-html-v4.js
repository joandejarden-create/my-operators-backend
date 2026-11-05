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
} from "./canonical-action-register-v1.js";
import {
  customerPageLabel,
  customerVisibleText,
  extractHttpUrl,
} from "./customer-facing-serialize-v1.js";
import { buildDigitalAuthorityCustomerCards } from "./digital-authority-customer-v1.js";

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
    audit.actionsResearchRecord || audit.actions || [];
  const { payload: base, register } = applyCanonicalActionRegisterToPayload({
    ...audit,
    actions: researchActions,
  });
  return {
    ...base,
    _researchActions: researchActions,
    actions: register.actions,
    customerActionRegister: register,
  };
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
    [];
  const actions = audit.actions || [];

  const rows = fromV3
    .filter((a) => a.relevant !== false)
    .filter(
      (a) =>
        a.listingExistence === LISTING_EXISTENCE.EXACT_LISTING_VERIFIED ||
        a.listingExistence === LISTING_EXISTENCE.RESEARCH_INCOMPLETE ||
        (a.url && a.listingExistence !== LISTING_EXISTENCE.NOT_APPLICABLE)
    )
    .map((a) => {
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

  // Prefer actionable + incomplete first; keep verified-consistent compact
  const actionable = rows.filter((r) => r.actionable || r.assessmentClass === "RESEARCH_FOLLOW_UP" || r.assessmentClass === "PARTIALLY_ASSESSED");
  const clean = rows.filter((r) => r.assessmentClass === "VERIFIED_AND_CONSISTENT");
  return [...actionable, ...clean].slice(0, 10);
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
      a.commercialRationale || a.businessRelevance,
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
  const actionable = rows.filter((r) => r.actionable || r.assessmentClass !== "VERIFIED_AND_CONSISTENT");
  const consistent = rows.filter((r) => r.assessmentClass === "VERIFIED_AND_CONSISTENT");

  const body = actionable
    .map(
      (r) =>
        `<tr><td>${r.url ? evidenceLink(r.url, r.platform) : esc(r.platform)}</td><td>${
          r.url ? evidenceLink(r.url, r.label) : "—"
        }</td><td>${esc(r.status)}</td><td>${esc(r.finding)}</td><td>${esc(r.action)}${
          r.owner ? `<br><span class="drs-muted">Owner: ${esc(r.owner)}</span>` : ""
        }</td></tr>`
    )
    .join("");

  const consistentNote = consistent.length
    ? `<p class="drs-muted" style="margin-top:8px;">Also verified with no material issue in this assessment scope: ${esc(
        consistent.map((r) => r.platform).join("; ")
      )}.</p>`
    : "";

  return (
    `<section class="drs-section ava-section" data-ava-section="platform-health">` +
    `<h2 class="drs-section-h">3. Digital Platform Assessment</h2>` +
    `<p class="drs-body">Exact verified listings where available. Rows distinguish corrections, content improvements, distribution opportunities, verified-consistent listings, partial assessments, and research follow-ups. Access restrictions are not treated as content defects.</p>` +
    `<div class="drs-table-wrap"><table class="drs-table bas-brief-table">` +
    `<thead><tr><th>Platform</th><th>Listing</th><th>Assessment</th><th>Finding</th><th>Next action</th></tr></thead>` +
    `<tbody>${body || `<tr><td colspan="5">No platform rows packaged.</td></tr>`}</tbody></table></div>` +
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
  const ready =
    audit.platformListingDiscoveryV3?.customerResearchReadiness?.verdict || "";
  const assessed =
    audit.reporting?.generatedAt ||
    audit.platformListingDiscoveryV3?.generatedAt ||
    "see report footer";
  // Keep methodology a single unbreakable block after the roadmap so Chromium
  // cannot orphan the final word onto a near-empty trailing page.
  return (
    `<section class="drs-section ava-section ava-v4-methodology" data-ava-section="methodology">` +
    `<h2 class="drs-section-h">7. Methodology &amp; Limitations</h2>` +
    `<p class="drs-body ava-v4-methodology__body">Dealality AVA assesses the hotel’s public digital presence. Listing existence and content assessment are tracked separately; Google Travel entities are not treated as Google Business Profile / Maps place listings. Assessment date: ${esc(
      assessed
    )}. Readiness: ${esc(
      ready || "see certification"
    )}. ADP traveler-intent detail is limited to the Executive Audit Summary.</p>` +
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
