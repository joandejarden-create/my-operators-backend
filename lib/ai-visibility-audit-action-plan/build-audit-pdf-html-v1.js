/**
 * Build AI Visibility Audit & Action Plan HTML using Dealality report-system CSS.
 * Cover: shared DealalityReportCoverV1 primitive (same as ADP Performance Review).
 * Progressive detail: business explanation first; agency technical panel second.
 */

import { renderAiVisibilityAuditCover } from "../dealality-report-family/dealality-report-cover-v1.js";
import {
  AI_VISIBILITY_AUDIT_RENDERER,
  AI_VISIBILITY_AUDIT_REPORT_FAMILY,
  AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V1,
  assertAuditPdfPayload,
} from "./audit-pdf-contract-v1.js";
import { collectDigitalAuthorityEvidenceUrls } from "./digital-authority-v1.js";
import { collectDigitalContentReadinessEvidenceUrls } from "./digital-content-readiness-v1.js";
import {
  claimTypeCustomerLabel,
  evidenceKindCustomerLabel,
  findingClassCustomerLabel,
  replaceInternalLanguage,
} from "./customer-facing-language-v1.js";

function sectionNumberFor(audit, key) {
  const hasDa = !!audit.digitalAuthority;
  const hasDcr = !!audit.digitalContentReadiness;
  const hasImpl = Array.isArray(audit.implementationByOwnerV3) && audit.implementationByOwnerV3.length > 0;
  const afterFindings = 5;
  const daNum = afterFindings;
  const dcrNum = afterFindings + (hasDa ? 1 : 0);
  const actionsNum = afterFindings + (hasDa ? 1 : 0) + (hasDcr ? 1 : 0);
  const implNum = actionsNum + 1;
  const methodNum = actionsNum + (hasImpl ? 1 : 0) + 1;
  const map = {
    digitalAuthority: daNum,
    digitalContentReadiness: dcrNum,
    actions: actionsNum,
    implementation: implNum,
    methodology: methodNum,
  };
  return map[key];
}

function esc(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function escAttr(s) {
  return esc(s);
}

function claimTypeLabel(t) {
  return claimTypeCustomerLabel(t);
}

function priorityPill(p) {
  const x = String(p || "").toUpperCase();
  if (x === "HIGH") return { label: "HIGH PRIORITY", cls: "drs-pill--high" };
  if (x === "MEDIUM") return { label: "MEDIUM", cls: "" };
  if (x === "OPTIONAL") return { label: "OPTIONAL", cls: "drs-pill--monitor" };
  return { label: x || "OPEN", cls: "" };
}

function statusPill(st) {
  const s = String(st || "");
  if (/VERIFIED/i.test(s)) return { label: "VERIFIED", cls: "drs-pill--done" };
  if (/COMPLETE/i.test(s)) return { label: "COMPLETED", cls: "drs-pill--done" };
  if (/PROGRESS/i.test(s)) return { label: "IN PROGRESS", cls: "drs-pill--progress" };
  if (/AWAITING|DECISION/i.test(s)) {
    return { label: "AWAITING HOTEL DECISION", cls: "drs-pill--monitor" };
  }
  return { label: "NOT STARTED", cls: "drs-pill--open" };
}

function evidenceKindLabel(kind) {
  return evidenceKindCustomerLabel(kind);
}

function cust(text) {
  return esc(replaceInternalLanguage(text));
}

function evidenceItemHtml(item) {
  if (!item) return "";
  const label = esc(item.label || item.url || "Source");
  const kind = item.kind
    ? `<span class="ava-evidence-kind">${esc(evidenceKindLabel(item.kind))}</span>`
    : "";
  if (item.url && /^https?:\/\//i.test(String(item.url))) {
    const href = esc(item.url);
    return (
      `<li class="ava-evidence-item">${kind}` +
      `<a class="ava-evidence-link" href="${href}" target="_blank" rel="noopener noreferrer">${label}</a>` +
      (item.note ? `<span class="ava-evidence-note"> — ${esc(item.note)}</span>` : "") +
      `</li>`
    );
  }
  return (
    `<li class="ava-evidence-item">${kind}` +
    `<span>${label}</span>` +
    (item.note
      ? `<span class="ava-evidence-note"> — ${esc(item.note)}</span>`
      : "") +
    `</li>`
  );
}

function evidenceListHtml(items) {
  if (!Array.isArray(items) || !items.length) return "";
  return (
    `<ul class="ava-evidence-list">` +
    items.map(evidenceItemHtml).join("") +
    `</ul>`
  );
}

function listHtml(items, ordered) {
  if (!Array.isArray(items) || !items.length) return "";
  const tag = ordered ? "ol" : "ul";
  return (
    `<${tag}>` +
    items.map((i) => `<li>${cust(i)}</li>`).join("") +
    `</${tag}>`
  );
}

function field(label, bodyHtml) {
  if (!bodyHtml) return "";
  return (
    `<div class="drs-action__field"><p class="drs-label">${esc(label)}</p>${bodyHtml}</div>`
  );
}

function para(text) {
  if (!text) return "";
  return `<p>${cust(text)}</p>`;
}

function renderCompletionCriteria(cc) {
  if (!cc || typeof cc !== "object") return "";
  const rows = [
    ["Investigation Completed", cc.investigationCompleted],
    ["Correction Submitted", cc.correctionSubmitted],
    ["Correction Verified Live", cc.correctionVerifiedLive],
    ["No Change Required", cc.noChangeRequired],
  ];
  return (
    `<ul class="ava-completion-list">` +
    rows
      .map(
        ([label, text]) =>
          `<li><strong>${esc(label)}.</strong> ${esc(text)}</li>`
      )
      .join("") +
    `</ul>`
  );
}

export function renderCover(audit) {
  return renderAiVisibilityAuditCover(audit);
}

function renderKpis(audit) {
  const kpis = audit.kpis || [];
  if (!kpis.length) return "";
  return (
    `<div class="drs-kpi-band">` +
    kpis
      .map(
        (k) =>
          `<div class="drs-kpi">` +
          `<p class="drs-kpi__label">${esc(k.label)}</p>` +
          `<p class="drs-kpi__value">${esc(k.value)}</p>` +
          (k.meta ? `<p class="drs-kpi__meta">${esc(k.meta)}</p>` : "") +
          `</div>`
      )
      .join("") +
    `</div>`
  );
}

function renderExecutiveThemes(themes) {
  if (!Array.isArray(themes) || !themes.length) return "";
  return (
    `<div class="ava-exec-themes">` +
    themes
      .map(
        (t, i) =>
          `<div class="ava-exec-theme">` +
          `<h4 class="ava-exec-theme__title">${esc(i + 1)}. ${esc(t.title)}</h4>` +
          `<p class="ava-exec-theme__what">${esc(t.what)}</p>` +
          (t.whyItMatters
            ? `<p class="ava-exec-theme__why"><span class="ava-exec-theme__why-label">Why it matters.</span> ${esc(
                t.whyItMatters
              )}</p>`
            : "") +
          `</div>`
      )
      .join("") +
    `</div>`
  );
}

function renderExecutivePriorities(rows) {
  if (!Array.isArray(rows) || !rows.length) return "";
  const body = rows
    .map(
      (r) =>
        `<tr>` +
        `<td>${esc(r.priority)}</td>` +
        `<td>${esc(r.action)}</td>` +
        `<td>${esc(r.desiredOutcome)}</td>` +
        `</tr>`
    )
    .join("");
  return (
    `<div class="drs-table-wrap ava-exec-priority-table"><table class="drs-table bas-brief-table">` +
    `<thead><tr><th>Priority</th><th>Action</th><th>Desired Outcome</th></tr></thead>` +
    `<tbody>${body}</tbody></table></div>`
  );
}

export function renderExecutive(audit) {
  const ex = audit.executiveSummary || {};
  return (
    `<section class="drs-section ava-section ava-exec-page" data-ava-section="executive">` +
    `<h2 class="drs-section-h">1. Executive Audit Summary</h2>` +
    `<div class="ava-exec-assessment">` +
    `<p class="ava-exec-assessment__headline">${esc(
      ex.headline ||
        "Strong AI Visibility, With Opportunities to Improve Digital Accuracy and Control"
    )}</p>` +
    (ex.lead ? `<p class="drs-body ava-exec-assessment__lead">${esc(ex.lead)}</p>` : "") +
    (ex.leadSupport
      ? `<p class="drs-body ava-exec-assessment__lead">${esc(ex.leadSupport)}</p>`
      : "") +
    `</div>` +
    `<h3 class="drs-subhead ava-exec-subhead">Performance Snapshot</h3>` +
    renderKpis(audit) +
    (ex.metricsNote
      ? `<p class="drs-muted ava-exec-metrics-note">${esc(ex.metricsNote)}</p>`
      : "") +
    `<h3 class="drs-subhead ava-exec-subhead">Key Findings</h3>` +
    renderExecutiveThemes(ex.themes || []) +
    `<h3 class="drs-subhead ava-exec-subhead">Recommended Priorities</h3>` +
    renderExecutivePriorities(ex.priorities || []) +
    `<div class="ava-exec-takeaway">` +
    `<p class="ava-exec-takeaway__label">Executive Takeaway</p>` +
    (ex.closing
      ? `<p class="ava-exec-takeaway__text">${esc(ex.closing)}</p>`
      : "") +
    `</div>` +
    `</section>`
  );
}

function renderActionOverview(audit) {
  const rows = audit.actionOverview || audit.actions || [];
  if (!rows.length) return "";
  const body = rows
    .map((r) => {
      const action = r.headline || r.action || "";
      const relevance = r.businessRelevance || r.whyItMatters || "";
      const owner = r.accountableOwner || r.owner || "";
      const timing = r.suggestedTimeframe || r.target || "";
      const priority = r.priority || "";
      return (
        `<tr>` +
        `<td>${esc(priority)}</td>` +
        `<td><strong>${esc(action)}</strong>` +
        (relevance
          ? `<br><span class="ava-overview-relevance">${cust(relevance)}</span>`
          : "") +
        `</td>` +
        `<td>${esc(owner)}</td>` +
        `<td>${esc(timing)}</td>` +
        `</tr>`
      );
    })
    .join("");

  return (
    `<section class="drs-section ava-section" data-ava-section="overview">` +
    `<div class="ava-overview-keep">` +
    `<h2 class="drs-section-h">2. Action Overview</h2>` +
    `<p class="drs-body">${cust(
      audit.actionOverviewIntro ||
        "Actions for leadership. Details and agency steps follow. Suggested timings require hotel agreement."
    )}</p>` +
    `<div class="drs-table-wrap ava-overview-table"><table class="drs-table bas-brief-table">` +
    `<thead><tr>` +
    `<th>Priority</th><th>Action &amp; Business Relevance</th>` +
    `<th>Accountable Owner</th><th>Suggested Timing</th>` +
    `</tr></thead><tbody>${body}</tbody></table></div>` +
    `</div>` +
    `</section>`
  );
}

function renderDecisions(audit) {
  const d = audit.hotelDecisions || {};
  const items = d.items || [];
  if (!items.length) return "";
  return (
    `<section class="drs-section ava-section" data-ava-section="decisions">` +
    `<h2 class="drs-section-h">3. Decisions Requested from Hotel Leadership</h2>` +
    `<p class="drs-body">${cust(
      d.intro ||
        "None of the items below has been approved. Each requires an explicit hotel decision before public-facing changes proceed."
    )}</p>` +
    `<ol class="ava-decision-list">` +
    items
      .map(
        (item) =>
          `<li class="ava-decision-item">` +
          `<p class="ava-decision-title">${esc(item.title)}</p>` +
          `<p class="drs-body">${cust(item.detail)}</p>` +
          `</li>`
      )
      .join("") +
    `</ol>` +
    `</section>`
  );
}

function renderFindings(audit) {
  const findings = audit.keyFindings || [];
  return (
    `<section class="drs-section ava-section" data-ava-section="findings">` +
    `<h2 class="drs-section-h">4. Key Digital Presence Findings</h2>` +
    `<p class="drs-body">${cust(
      audit.findingsIntro ||
        "Evidence links open the supporting source. Full recommendations follow in the action plan."
    )}</p>` +
    `<div class="adp-mr-callout-stack">` +
    findings
      .map((f) => {
        const themes = Array.isArray(f.diagnosisThemes) ? f.diagnosisThemes : [];
        return (
          `<aside class="drs-callout">` +
          `<p class="drs-callout__eyebrow">${esc(claimTypeLabel(f.claimType))}</p>` +
          `<p class="ava-finding-title">${esc(f.title)}</p>` +
          `<p class="drs-body">${cust(f.summary)}</p>` +
          (f.commercialRelevance
            ? field("Commercial relevance", para(f.commercialRelevance))
            : "") +
          (f.affectedPlatform
            ? field("Primary surface", para(f.affectedPlatform))
            : "") +
          (themes.length
            ? field(
                "Diagnosis themes",
                `<p class="drs-body">${esc(themes.join(" · "))}</p>`
              )
            : "") +
          field("Supporting Evidence", evidenceListHtml(f.evidence || [])) +
          `</aside>`
        );
      })
      .join("") +
    `</div>` +
    `</section>`
  );
}

function renderTechnicalPanel(a) {
  const td = a.technicalDetails || {};
  const changes = Array.isArray(td.changesOrInvestigation)
    ? td.changesOrInvestigation
    : [td.changesOrInvestigation].filter(Boolean);
  const deps = Array.isArray(td.dependencies)
    ? td.dependencies
    : [td.dependencies].filter(Boolean);
  const evidence =
    td.evidence || a.supportingEvidence || [];

  return (
    `<div class="ava-tech-panel">` +
    `<p class="ava-tech-panel__title">Agency Implementation Details</p>` +
    `<p class="drs-muted">For the digital marketing agency, website developer, or CDN partner.</p>` +
    field("Affected Pages / Platforms", evidenceListHtml(td.urls || [])) +
    field("Required Changes or Investigation", listHtml(changes, true)) +
    field("Source Evidence", evidenceListHtml(evidence)) +
    field("Verification Method", para(td.verificationMethod)) +
    (deps.length ? field("Dependencies", listHtml(deps, false)) : "") +
    (td.notes ? field("Technical Notes", para(td.notes)) : "") +
    `</div>`
  );
}

function renderActionCard(a, index) {
  const pri = priorityPill(a.priority);
  const st = statusPill(a.implementationStatus);
  const n = String(index + 1).padStart(2, "0");
  const supporting = a.supportingParties || a.supportingParty || "";

  return (
    `<article class="drs-action ava-action" data-ava-action="${esc(a.actionId || "")}">` +
    `<div class="drs-action__keep">` +
    `<p class="drs-action__eyebrow">Action ${n}</p>` +
    `<h3 class="drs-action__title">${esc(a.headline)}</h3>` +
    `<div class="drs-action__scan">` +
    `<span class="drs-pill ${pri.cls}">${esc(pri.label)}</span>` +
    `<span class="drs-pill ${st.cls}">${esc(st.label)}</span>` +
    `<span class="drs-pill">${esc(claimTypeLabel(a.claimType))}</span>` +
    `<span class="drs-action__scan-meta">Owner: ${esc(
      a.accountableOwner || a.responsibleParty
    )}</span>` +
    `<span class="drs-action__scan-meta">Timing: ${esc(a.suggestedTimeframe)}</span>` +
    `</div>` +
    `</div>` +
    (a.affectedPlatform
      ? field("Exact website or platform", para(a.affectedPlatform))
      : "") +
    (a.specificIssue ? field("Specific issue", para(a.specificIssue)) : "") +
    field("What We Found", para(a.whatWeFound)) +
    field("Why It Matters / commercial importance", para(a.whyItMatters || a.commercialImportance)) +
    field("Recommended Action", listHtml(a.recommendedSteps, true)) +
    field("Accountable Owner", para(a.accountableOwner || a.responsibleParty)) +
    (supporting ? field("Supporting Parties", para(supporting)) : "") +
    field(
      "Suggested Timing",
      para(
        `${a.suggestedTimeframe}${
          a.timeframeNote ? `. ${a.timeframeNote}` : ""
        }`
      )
    ) +
    field("Measurable Completion Criteria", renderCompletionCriteria(a.completionCriteria)) +
    (a.requiresHotelApproval
      ? `<p class="adp-mr-disclosure"><strong>Hotel Decision Required.</strong> ${esc(
          a.hotelApprovalNotes ||
            "Confirm before public-facing edits or partner outreach."
        )}</p>`
      : "") +
    renderTechnicalPanel(a) +
    `</article>`
  );
}

function isInternalStrategyNoise(text) {
  return /^(A_|B_|C_|D_|E_)[A-Z0-9_]+$/.test(String(text || "").trim()) ||
    /DIRECT_HOTEL_BACKLINKS|HOTEL_MENTIONS|COMPETITOR_BACKLINKS|INDUSTRY_DESTINATION|SEED_KNOWN/i.test(
      String(text || "")
    );
}

function commercialRelevanceForSource(s) {
  const raw = String(s.commercialRelevance || s.note || "").trim();
  if (raw && !isInternalStrategyNoise(raw)) return cust(raw);
  const dest = String(s.destinationUrlClass || s.hotelDestinationUrl || "");
  if (/HOTEL_OWNED|hotel.?operated|waterstone|castillo/i.test(dest)) {
    return "Links travelers to the hotel-operated website.";
  }
  if (/BRAND|marriott|hilton|ihg/i.test(dest)) {
    return "Links travelers to the brand property page.";
  }
  return "Third-party site that can help travelers discover this hotel.";
}

function sanitizeDaExecutiveFinding(da, backlinkCount, mentionCount) {
  const raw = String(da.executiveFinding || "");
  // Prefer commercial summary when research jargon dominates.
  if (
    !raw ||
    /Public V2|first-party brand-family|low-quality referrers|discovery verified/i.test(raw)
  ) {
    if (backlinkCount > 0) {
      return `Trusted third-party websites currently link to this hotel’s official pages (${backlinkCount} verified link${backlinkCount === 1 ? "" : "s"} in this review). ${mentionCount} additional third-party page${mentionCount === 1 ? "" : "s"} mention the hotel without a clear property link. Strengthening destination and industry links can improve discoverability — it does not, by itself, change measured AI visibility.`;
    }
    return `This public review did not confirm third-party websites linking to the hotel’s official pages. ${mentionCount} third-party reference${mentionCount === 1 ? "" : "s"} mention the hotel without a property hyperlink. That is a visibility opportunity to pursue selectively — not proof of weak AI performance.`;
  }
  return cust(raw);
}

function hostLabel(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, "");
  } catch {
    return url || "";
  }
}

function renderDigitalAuthority(audit) {
  const da = audit.digitalAuthority;
  if (!da || typeof da !== "object") return "";
  const opps = Array.isArray(da.opportunities) ? da.opportunities.slice(0, 5) : [];
  const actions = Array.isArray(da.actions) ? da.actions.slice(0, 5) : [];
  const gaps = Array.isArray(da.competitorGaps) ? da.competitorGaps.slice(0, 4) : [];
  // Customer PDF uses customer-facing sources only (not Admin researchInventory).
  const backlinks = (Array.isArray(da.sources) ? da.sources : []).filter(
    (s) => s.findingClass === "VERIFIED_BACKLINK"
  );
  const mentions = (Array.isArray(da.sources) ? da.sources : []).filter(
    (s) => s.findingClass === "VERIFIED_EXTERNAL_MENTION"
  );
  const competitorBacklinks = Array.isArray(da.competitorBacklinks)
    ? da.competitorBacklinks.slice(0, 5)
    : [];
  const v3 = da.v3Extras || {};
  const uniqueHosts =
    v3.uniqueReferringWebsites != null
      ? v3.uniqueReferringWebsites
      : new Set(
          backlinks
            .map((s) => hostLabel(s.linkingPageUrl || s.evidenceUrl))
            .filter(Boolean)
        ).size;

  const snapshotHtml =
    `<h3 class="drs-subhead">External visibility snapshot</h3>` +
    `<table class="ava-table ava-da-snapshot"><thead><tr>` +
    `<th>Metric</th><th>In this report</th><th>What it means</th>` +
    `</tr></thead><tbody>` +
    `<tr><td>Verified website links</td><td><strong>${backlinks.length}</strong></td>` +
    `<td>Third-party pages that hyperlink to an official hotel or brand property URL</td></tr>` +
    `<tr><td>Unique referring websites</td><td><strong>${uniqueHosts}</strong></td>` +
    `<td>Distinct domains among verified links (deduped hosts)</td></tr>` +
    `<tr><td>Third-party references (no link)</td><td><strong>${mentions.length}</strong></td>` +
    `<td>Pages that name the hotel but do not clearly link to the official site</td></tr>` +
    `<tr><td>High-value destination / industry sources</td><td><strong>${
      v3.highValueDestinationOrIndustryCount != null
        ? v3.highValueDestinationOrIndustryCount
        : "—"
    }</strong></td>` +
    `<td>Verified links from tourism, CVB, destination, or specialist hosts in this review</td></tr>` +
    `<tr><td>Competitor link examples</td><td><strong>${competitorBacklinks.length}</strong></td>` +
    `<td>Evidence that peers appear on useful third-party pages (not a full backlink ranking)</td></tr>` +
    `</tbody></table>` +
    `<p class="drs-muted">This is a <strong>partial public review</strong>, not a paid backlink index. ${esc(
      v3.distinguishNote ||
        "Verified website links differ from mentions without hyperlinks, and both differ from measured AI citations in ADP."
    )} Counts below are the verified findings shown to you — not a claim about AI ranking or search volume.</p>`;

  const backlinkHtml = backlinks.length
    ? `<h3 class="drs-subhead">Verified website links</h3>` +
      `<table class="ava-table ava-da-backlinks"><thead><tr>` +
      `<th>Referring website</th><th>Links to</th><th>Why it matters</th><th>Evidence</th>` +
      `</tr></thead><tbody>` +
      backlinks
        .slice(0, 8)
        .map((s) => {
          const destLabel = /marriott|hilton|ihg|brand/i.test(
            String(s.hotelDestinationUrl || s.destinationUrlClass || "")
          )
            ? "Brand property page"
            : "Hotel website";
          return (
            `<tr>` +
            `<td>${esc(s.referringOrganization || hostLabel(s.linkingPageUrl) || "Source")}</td>` +
            `<td>${esc(destLabel)}</td>` +
            `<td>${esc(commercialRelevanceForSource(s))}</td>` +
            `<td>` +
            (s.linkingPageUrl
              ? `<a class="ava-evidence-link" href="${escAttr(
                  s.linkingPageUrl
                )}" target="_blank" rel="noopener noreferrer">Open page</a>`
              : "—") +
            `</td></tr>`
          );
        })
        .join("") +
      `</tbody></table>` +
      (backlinks.length > 8
        ? `<p class="drs-muted">${backlinks.length - 8} additional verified link(s) retained in Admin evidence.</p>`
        : "")
    : `<h3 class="drs-subhead">Verified website links</h3>` +
      `<p class="drs-body">No third-party hyperlinks to the hotel’s official pages were confirmed in this partial public review. Mentions without hyperlinks are listed separately below.</p>`;

  const mentionHtml = mentions.length
    ? `<h3 class="drs-subhead">Third-party references without a clear hotel link</h3>` +
      `<table class="ava-table ava-da-mentions"><thead><tr>` +
      `<th>Source</th><th>Finding</th><th>Evidence</th></tr></thead><tbody>` +
      mentions
        .slice(0, 5)
        .map(
          (s) =>
            `<tr>` +
            `<td>${esc(s.referringOrganization || hostLabel(s.linkingPageUrl) || "Source")}</td>` +
            `<td>Hotel is mentioned; no verified property hyperlink in this review</td>` +
            `<td>` +
            (s.linkingPageUrl
              ? `<a class="ava-evidence-link" href="${escAttr(
                  s.linkingPageUrl
                )}" target="_blank" rel="noopener noreferrer">Open page</a>`
              : "—") +
            `</td></tr>`
        )
        .join("") +
      `</tbody></table>`
    : "";

  const competitorBlHtml = competitorBacklinks.length
    ? `<h3 class="drs-subhead">Where competitors appear externally</h3>` +
      `<ul class="ava-da-gap-list">` +
      competitorBacklinks
        .map(
          (b) =>
            `<li><strong>${esc(b.competitorName || "Competitor")}</strong> — ${esc(
              cust(b.summary || b.linkContext || "Appears on a relevant third-party page")
            )}` +
            (b.linkingPageUrl
              ? ` <a class="ava-evidence-link" href="${escAttr(
                  b.linkingPageUrl
                )}" target="_blank" rel="noopener noreferrer">Evidence</a>`
              : "") +
            `</li>`
        )
        .join("") +
      `</ul>`
    : "";

  const gapHtml = gaps.length
    ? `<ul class="ava-da-gap-list">` +
      gaps
        .map(
          (g) =>
            `<li><strong>${esc(g.competitorName || "Competitor")}</strong> — ${esc(
              cust(g.gapSummary || g.summary || "")
            )}` +
            (g.evidenceUrl
              ? ` <a class="ava-evidence-link" href="${escAttr(
                  g.evidenceUrl
                )}" target="_blank" rel="noopener noreferrer">Evidence</a>`
              : "") +
            `</li>`
        )
        .join("") +
      `</ul>`
    : "";

  const oppRows = opps
    .map((o) => {
      return (
        `<div class="ava-da-opp">` +
        `<h4 class="ava-da-opp__title">${esc(o.title || o.organization || "Opportunity")}</h4>` +
        (o.whyItMatters
          ? `<p class="drs-body"><strong>Why it matters.</strong> ${cust(o.whyItMatters)}</p>`
          : "") +
        (o.recommendedAction
          ? `<p class="drs-body"><strong>What to do.</strong> ${cust(o.recommendedAction)}</p>`
          : "") +
        (o.targetOrganizationUrl
          ? `<p class="ava-da-opp__link"><a class="ava-evidence-link" href="${escAttr(
              o.targetOrganizationUrl
            )}" target="_blank" rel="noopener noreferrer">${esc(
              hostLabel(o.targetOrganizationUrl)
            )}</a></p>`
          : "") +
        `</div>`
      );
    })
    .join("");

  const actionHtml = actions.length
    ? `<ol class="ava-da-action-list">` +
      actions
        .map(
          (a) =>
            `<li><strong>${esc(a.headline || "Action")}</strong>` +
            (a.accountableOwner ? ` · ${esc(a.accountableOwner)}` : "") +
            (a.priority ? ` · ${esc(a.priority)} priority` : "") +
            (a.whatWeFound
              ? `<div class="ava-muted">${cust(a.whatWeFound)}</div>`
              : "") +
            `</li>`
        )
        .join("") +
      `</ol>`
    : "";

  const sectionNum = sectionNumberFor(audit, "digitalAuthority");
  const execText = sanitizeDaExecutiveFinding(da, backlinks.length, mentions.length);

  return (
    `<section class="drs-section ava-section ava-da-section" data-ava-section="digital-authority">` +
    `<h2 class="drs-section-h">${sectionNum}. Digital Authority &amp; External Visibility</h2>` +
    `<p class="drs-muted ava-da-mode">Partial public review of third-party websites — not a comprehensive paid backlink inventory, and not a measure of AI ranking.</p>` +
    `<div class="ava-da-exec"><p class="drs-callout__eyebrow">What this means commercially</p><p class="drs-body">${esc(
      execText
    )}</p></div>` +
    snapshotHtml +
    backlinkHtml +
    mentionHtml +
    (da.competitiveComparison || gapHtml || competitorBlHtml
      ? `<h3 class="drs-subhead">Competitor visibility notes</h3>` +
        (da.competitiveComparison
          ? `<p class="drs-body">${cust(da.competitiveComparison)}</p>`
          : "") +
        competitorBlHtml +
        gapHtml
      : "") +
    (oppRows
      ? `<h3 class="drs-subhead">Highest-value external opportunities</h3>` +
        `<div class="ava-da-opps">${oppRows}</div>`
      : "") +
    (actionHtml
      ? `<h3 class="drs-subhead">Recommended authority actions</h3>${actionHtml}`
      : "") +
    `<p class="drs-muted ava-da-disclaimer">Website links and third-party mentions are different from measured AI visibility. Improving a destination listing does not guarantee a change in AI recommendations, bookings, or revenue.</p>` +
    `</section>`
  );
}

function renderDigitalContentReadiness(audit) {
  const dcr = audit.digitalContentReadiness;
  if (!dcr || typeof dcr !== "object") return "";
  const sectionNum = sectionNumberFor(audit, "digitalContentReadiness");
  const rs = dcr.readinessSummary || {};
  const corrections = Array.isArray(dcr.platformCorrections)
    ? dcr.platformCorrections.slice(0, 5)
    : [];
  const questions = (Array.isArray(dcr.travelerQuestionCoverage)
    ? dcr.travelerQuestionCoverage
    : []
  ).filter((q) =>
    [
      "MISSING_INFORMATION",
      "INCONSISTENT_INFORMATION",
      "PARTIALLY_ANSWERED",
    ].includes(q.classification)
  );
  const sources = Array.isArray(dcr.sourceInventory) ? dcr.sourceInventory : [];
  const brand = sources.find((s) => s.sourceCategory === "BRAND_PROPERTY_PAGE");
  const hotelSite = sources.find(
    (s) => s.sourceCategory === "HOTEL_OPERATED_WEBSITE"
  );

  const readinessRows = [
    ["Website accessibility", rs.websiteAccessibility],
    ["Content completeness", rs.contentCompleteness],
    ["Structured information", rs.structuredInformation],
    ["Traveler question coverage", rs.travelerQuestionCoverage],
    ["Third-party consistency", rs.thirdPartyConsistency],
  ]
    .filter(([, v]) => v)
    .map(
      ([label, value]) =>
        `<tr><td>${esc(label)}</td><td>${esc(
          String(value).replace(/_/g, " ")
        )}</td></tr>`
    )
    .join("");

  const correctionRows = corrections
    .map(
      (c) =>
        `<tr>` +
        `<td>${esc(c.platform || "")}</td>` +
        `<td>${esc(
          c.discrepancyOrOpportunity || c.issue || c.attribute || ""
        )}</td>` +
        `<td>${esc(c.recommendedCorrection || "")}</td>` +
        `<td>${esc(
          c.responsibleOrganization === "RESPONSIBLE_PARTY_TO_CONFIRM"
            ? "Owner to confirm"
            : c.responsibleOrganization || "Owner to confirm"
        )}</td>` +
        `<td>${esc(c.priority || "")}</td>` +
        `</tr>`
    )
    .join("");

  const registry = dcr.platformCoverageRegistry || {};
  const PLATFORM_LABELS = {
    BRAND_PROPERTY_PAGE: "Brand property page",
    HOTEL_OPERATED_WEBSITE: "Hotel-operated website",
    GOOGLE_BUSINESS_PROFILE: "Google Business Profile / Google Maps",
    TRIPADVISOR: "Tripadvisor",
    OTA_BOOKING: "Booking.com",
    OTA_EXPEDIA: "Expedia / Hotels.com",
    APPLE_MAPS: "Apple Maps",
    BING_PLACES: "Bing Places / Bing Maps",
    CVENT: "Cvent / meetings directories",
    DESTINATION_CVB: "Tourism board / convention bureau",
    SPECIALIST_DIRECTORY: "Specialist directories",
    TOURISM_BOARD: "Tourism board",
  };
  const STATUS_LABELS = {
    ASSESSED: "Assessed",
    IDENTIFIED_NOT_ASSESSED: "Identified — not field-assessed",
    ACCESS_RESTRICTED: "Access restricted",
    NOT_FOUND: "Not found",
    NOT_RELEVANT: "Not applicable to this hotel",
    VERIFIED_CORRECT: "Verified reachable",
  };
  const detailRows =
    (Array.isArray(audit.digitalPlatformReadinessV3?.rows) &&
      audit.digitalPlatformReadinessV3.rows) ||
    (Array.isArray(dcr.platformDetailRows) && dcr.platformDetailRows) ||
    null;
  const groups = audit.digitalPlatformReadinessV3?.groups || null;
  const correctionMap =
    Array.isArray(audit.digitalPlatformReadinessV3?.correctionMap) &&
    audit.digitalPlatformReadinessV3.correctionMap.length
      ? audit.digitalPlatformReadinessV3.correctionMap
      : null;

  function platformRowHtml(r) {
    const urlCell = r.url
      ? `<a class="ava-evidence-link" href="${escAttr(
          r.url
        )}" target="_blank" rel="noopener noreferrer">${esc(
          r.evidenceLabel || hostLabel(r.url) || "Open listing"
        )}</a>`
      : "—";
    return (
      `<tr>` +
      `<td>${esc(r.platform || "")}</td>` +
      `<td>${urlCell}</td>` +
      `<td>${esc(r.assessmentStatusLabel || STATUS_LABELS[r.assessmentStatus] || r.assessmentStatus || "")}</td>` +
      `<td>${esc(r.verifiedFinding || "")}</td>` +
      `<td>${esc(r.recommendedCorrection || "")}</td>` +
      `<td>${esc(r.responsibleParty || "")}</td>` +
      `</tr>`
    );
  }

  let platformRows = "";
  if (groups) {
    const blocks = [
      ["Verified corrections & investigations", groups.corrections],
      ["Actionable inclusion / outreach opportunities", groups.opportunities],
      ["Confirmed reachable platforms", groups.consistent],
      ["Additional platforms (researched; not fully field-assessed)", groups.additional],
    ];
    for (const [title, rows] of blocks) {
      if (!rows?.length) continue;
      platformRows +=
        `<h4 class="drs-subhead">${esc(title)}</h4>` +
        `<table class="ava-table ava-dcr-platforms ava-dcr-platforms--v3"><thead><tr>` +
        `<th>Platform</th><th>Listing</th><th>Status</th><th>Verified finding</th><th>Recommended action</th><th>Owner</th>` +
        `</tr></thead><tbody>${rows.map(platformRowHtml).join("")}</tbody></table>`;
    }
  } else if (detailRows) {
    platformRows = detailRows.map(platformRowHtml).join("");
    platformRows =
      `<table class="ava-table ava-dcr-platforms ava-dcr-platforms--v3"><thead><tr>` +
      `<th>Platform</th><th>Listing</th><th>Status</th><th>Verified finding</th><th>Recommended action</th><th>Owner</th>` +
      `</tr></thead><tbody>${platformRows}</tbody></table>`;
  } else {
    platformRows = Object.entries(registry)
      .map(([k, v]) => {
        const label = PLATFORM_LABELS[k] || String(k).replace(/_/g, " ");
        const status = STATUS_LABELS[v] || String(v).replace(/_/g, " ");
        return `<tr><td>${esc(label)}</td><td>—</td><td>${esc(status)}</td><td>—</td><td>—</td><td>—</td></tr>`;
      })
      .join("");
    if (platformRows) {
      platformRows =
        `<table class="ava-table ava-dcr-platforms"><thead><tr>` +
        `<th>Platform</th><th>Listing</th><th>Status</th><th>Verified finding</th><th>Recommended action</th><th>Owner</th>` +
        `</tr></thead><tbody>${platformRows}</tbody></table>`;
    }
  }

  const correctionMapHtml = correctionMap
    ? `<h3 class="drs-subhead">Digital Platform Correction Map</h3>` +
      `<table class="ava-table ava-dcr-corrections ava-correction-map"><thead><tr>` +
      `<th>Platform</th><th>Affected page</th><th>Finding</th><th>Change</th><th>Priority</th><th>Owner</th><th>Evidence</th>` +
      `</tr></thead><tbody>` +
      correctionMap
        .map((r) => {
          const ev = r.evidenceUrl
            ? `<a class="ava-evidence-link" href="${escAttr(
                r.evidenceUrl
              )}" target="_blank" rel="noopener noreferrer">${esc(
                r.evidenceLabel || hostLabel(r.evidenceUrl) || "View listing"
              )}</a>`
            : "—";
          const page = r.exactPage
            ? `<a class="ava-evidence-link" href="${escAttr(
                r.exactPage
              )}" target="_blank" rel="noopener noreferrer">${esc(
                hostLabel(r.exactPage) || "Open"
              )}</a>`
            : "—";
          return (
            `<tr>` +
            `<td>${esc(r.platform || "")}</td>` +
            `<td>${page}</td>` +
            `<td>${esc(r.verifiedFinding || "")}</td>` +
            `<td>${esc(r.recommendedChange || "")}</td>` +
            `<td>${esc(r.priority || "")}</td>` +
            `<td>${esc(r.responsibleParty || "")}</td>` +
            `<td>${ev}</td>` +
            `</tr>`
          );
        })
        .join("") +
      `</tbody></table>`
    : "";

  const questionHtml = questions.length
    ? `<h3 class="drs-subhead">Traveler question gaps</h3><ul class="ava-da-gap-list">` +
      questions
        .slice(0, 4)
        .map(
          (q) =>
            `<li><strong>${esc(q.travelerIntent || "")}</strong>` +
            (q.verifiedGaps
              ? `<div class="ava-muted">${esc(q.verifiedGaps)}</div>`
              : "") +
            (q.recommendedAction
              ? `<div class="ava-muted"><strong>Action.</strong> ${esc(
                  q.recommendedAction
                )}</div>`
              : "") +
            `</li>`
        )
        .join("") +
      `</ul>`
    : "";

  const presenceBits = [];
  if (brand) presenceBits.push("brand property page identified");
  if (hotelSite) presenceBits.push("hotel-operated website identified");
  if (!brand && !hotelSite) presenceBits.push("primary digital sources inventoried");

  const sectionTitle =
    dcr.sectionTitleCustomer ||
    audit.digitalPlatformReadinessV3?.title ||
    "Digital Platform & Website Readiness";

  return (
    `<section class="drs-section ava-section ava-dcr-section" data-ava-section="digital-content-readiness">` +
    `<h2 class="drs-section-h">${sectionNum}. ${esc(sectionTitle)}</h2>` +
    (dcr.executiveAssessment
      ? `<div class="ava-da-exec"><p class="drs-callout__eyebrow">Executive assessment</p><p class="drs-body">${esc(
          dcr.executiveAssessment
        )}</p></div>`
      : "") +
    `<p class="drs-muted">Digital presence: ${esc(
      presenceBits.join("; ")
    )}. Brand and hotel-operated sources are assessed independently when both exist. Measured ADP traveler-intent detail is not repeated here.</p>` +
    (readinessRows
      ? `<h3 class="drs-subhead">Digital discoverability readiness</h3>` +
        `<table class="ava-table ava-dcr-ready"><thead><tr><th>Dimension</th><th>Status</th></tr></thead><tbody>${readinessRows}</tbody></table>`
      : "") +
    (platformRows
      ? `<h3 class="drs-subhead">Platform-by-platform assessment</h3>` +
        platformRows +
        `<p class="drs-muted">Additional / incomplete research rows are not verified defects. Access-restricted brand pages are not treated as broken customer sites.</p>`
      : "") +
    correctionMapHtml +
    (correctionRows && !correctionMap
      ? `<h3 class="drs-subhead">Priority platform corrections</h3>` +
        `<table class="ava-table ava-dcr-corrections"><thead><tr><th>Platform</th><th>Finding</th><th>Recommended action</th><th>Owner</th><th>Priority</th></tr></thead><tbody>${correctionRows}</tbody></table>`
      : "") +
    questionHtml +
    `<p class="drs-muted ava-da-disclaimer">Missing or inconsistent content is a readiness opportunity unless causal evidence shows it drove a specific AI recommendation. Bot-blocked access is not treated as proof a search engine cannot reach a page. Completing an action does not guarantee a change in AI recommendations or revenue.</p>` +
    `</section>`
  );
}

function renderImplementationByOwner(audit) {
  const groups = audit.implementationByOwnerV3;
  if (!Array.isArray(groups) || !groups.length) return "";
  const sectionNum = sectionNumberFor(audit, "implementation");
  return (
    `<section class="drs-section ava-section" data-ava-section="implementation-by-owner">` +
    `<h2 class="drs-section-h">${sectionNum}. Implementation by Responsible Party</h2>` +
    `<p class="drs-body">Page-specific execution notes grouped for hotel teams, brand partners, agencies, and platform owners. Unassessed platforms are not treated as confirmed defects.</p>` +
    groups
      .map((g) => {
        return (
          `<h3 class="drs-subhead">${esc(g.label)}</h3>` +
          `<ol class="ava-da-action-list">` +
          (g.items || [])
            .map((item) => {
              return (
                `<li>` +
                `<strong>${esc(item.headline || item.actionId || "Action")}</strong>` +
                (item.affectedPlatform
                  ? `<div class="ava-muted"><strong>Platform.</strong> ${esc(
                      item.affectedPlatform
                    )}</div>`
                  : "") +
                (item.specificIssue
                  ? `<div class="ava-muted"><strong>Issue.</strong> ${esc(
                      item.specificIssue
                    )}</div>`
                  : "") +
                (item.steps?.length
                  ? `<div class="ava-muted"><strong>Do this.</strong> ${esc(
                      item.steps.join(" ")
                    )}</div>`
                  : "") +
                (item.evidenceUrls?.length
                  ? `<div class="ava-muted">` +
                    item.evidenceUrls
                      .map(
                        (u) =>
                          `<a class="ava-evidence-link" href="${escAttr(
                            u
                          )}" target="_blank" rel="noopener noreferrer">${esc(
                            hostLabel(u)
                          )}</a>`
                      )
                      .join(" · ") +
                    `</div>`
                  : "") +
                `</li>`
              );
            })
            .join("") +
          `</ol>`
        );
      })
      .join("") +
    `</section>`
  );
}

function renderPrioritizedPlan(audit) {
  const actions = audit.actions || [];
  const sectionNum = sectionNumberFor(audit, "actions");
  return (
    `<section class="drs-section ava-section" data-ava-section="actions">` +
    `<h2 class="drs-section-h">${sectionNum}. Prioritized Action Plan</h2>` +
    `<p class="drs-body">${esc(
      audit.actionPlanIntro ||
        "Executive explanation first; agency implementation details follow. Evidence links are in the agency panels."
    )}</p>` +
    actions.map((a, i) => renderActionCard(a, i)).join("") +
    `</section>`
  );
}

function renderMethod(audit) {
  const m = audit.methodology || {};
  const sectionNum = sectionNumberFor(audit, "methodology");
  return (
    `<section class="drs-section ava-section" data-ava-section="methodology">` +
    `<h2 class="drs-section-h">${sectionNum}. Methodology, Responsibilities &amp; Next Steps</h2>` +
    (m.scope ? `<p class="drs-body">${esc(m.scope)}</p>` : "") +
    `<h3 class="drs-subhead">How to Read Findings</h3>` +
    listHtml(m.claimTypeGuide || [], false) +
    `<h3 class="drs-subhead">Responsibilities</h3>` +
    listHtml(m.responsibilities || [], false) +
    `<h3 class="drs-subhead">How This Relates to Monthly Monitoring</h3>` +
    `<p class="drs-body">${esc(
      m.monthlyReviewRelationship ||
        "Approved actions can later be tracked in the hotel’s monthly AI Demand Performance Review. Completing an action does not, by itself, prove a change in AI recommendations or revenue."
    )}</p>` +
    // Confidentiality line lives only in the shared print footer (every page).
    // Do not repeat it in the body after Suggested Next Step.
    `<div class="ava-method-close">` +
    `<h3 class="drs-subhead">Suggested Next Step</h3>` +
    `<p class="drs-body">${esc(m.nextStep || "")}</p>` +
    `</div>` +
    `</section>`
  );
}

/**
 * @param {object} audit
 * @returns {{ ok: boolean, html?: string, errors?: string[], evidenceUrls?: string[] }}
 */
export function buildAiVisibilityAuditPdfHtml(audit) {
  const gate = assertAuditPdfPayload(audit);
  if (!gate.ok) return { ok: false, errors: gate.errors };

  const inner =
    `<div class="drs-report ava-report brand-alignment-snapshot" data-report-family="${AI_VISIBILITY_AUDIT_REPORT_FAMILY}" data-ava-template="${AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V1}" data-ava-renderer="${AI_VISIBILITY_AUDIT_RENDERER}">` +
    renderCover(audit) +
    `<div class="adp-mr-report-body ava-report-body">` +
    renderExecutive(audit) +
    renderActionOverview(audit) +
    renderDecisions(audit) +
    renderFindings(audit) +
    renderDigitalAuthority(audit) +
    renderDigitalContentReadiness(audit) +
    renderPrioritizedPlan(audit) +
    renderImplementationByOwner(audit) +
    renderMethod(audit) +
    `</div></div>`;

  const evidenceUrls = [];
  const collect = (items) => {
    for (const it of items || []) {
      if (it?.url && /^https?:\/\//i.test(String(it.url))) {
        evidenceUrls.push(String(it.url));
      }
      if (typeof it === "string" && /^https?:\/\//i.test(it)) {
        evidenceUrls.push(it);
      }
    }
  };
  for (const f of audit.keyFindings || []) collect(f.evidence);
  for (const a of audit.actions || []) {
    collect(a.supportingEvidence);
    collect(a.technicalDetails?.evidence);
    collect(a.technicalDetails?.urls);
  }
  if (audit.digitalAuthority) {
    evidenceUrls.push(
      ...collectDigitalAuthorityEvidenceUrls(audit.digitalAuthority)
    );
  }
  if (audit.digitalContentReadiness) {
    evidenceUrls.push(
      ...collectDigitalContentReadinessEvidenceUrls(audit.digitalContentReadiness)
    );
  }

  return {
    ok: true,
    html: inner,
    templateId: AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V1,
    evidenceUrls: [...new Set(evidenceUrls)],
  };
}
