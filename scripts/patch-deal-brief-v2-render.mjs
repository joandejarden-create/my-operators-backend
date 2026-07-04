import fs from "fs";

const path = "public/deal-summary.html";
let html = fs.readFileSync(path, "utf8");

const renderDetailOld = `            function renderDetailSection(fields, section, skipTitle) {
                var visible = [];
                section.rows.forEach(function(r) {
                    var label = r[0], keys = r[1];
                    var val = getField(fields, keys);
                    if (val === 'Franchise + Third-Party Management') val = 'Franchise + 3rd Party Mgmt.';
                    if (label === 'Current brand/operator') {
                        var parent = (fields['Parent Company Name'] != null && fields['Parent Company Name'] !== '') ? String(fields['Parent Company Name']).trim() : '';
                        var brand = (fields['Current Brand Affiliation'] != null && fields['Current Brand Affiliation'] !== '') ? String(fields['Current Brand Affiliation']).trim() : '';
                        var op = (fields['Operator Name Current'] != null && fields['Operator Name Current'] !== '') ? String(fields['Operator Name Current']).trim() : '';
                        val = [parent && ('Parent: ' + parent), brand && ('Brand: ' + brand), op && ('Operator: ' + op)].filter(Boolean).join(' · ') || '';
                    }
                    if (label === 'Room breakdown') {
                        var std = (fields['Number of Standard Rooms'] != null && fields['Number of Standard Rooms'] !== '') ? String(fields['Number of Standard Rooms']) : '';
                        var suite = (fields['Number of Suites'] != null && fields['Number of Suites'] !== '') ? String(fields['Number of Suites']) : '';
                        val = [std && (std + ' Standard'), suite && (suite + ' Suites')].filter(Boolean).join(', ') || '';
                    }
                    if (label === 'Meeting Space') {
                        val = (fields['Meeting Space'] != null && fields['Meeting Space'] !== '') ? (String(fields['Meeting Space']) + (fields['Meeting Space Unit'] ? ' ' + formatFieldValue(fields['Meeting Space Unit']) : '')) : '';
                    }
                    if (!hasValue(val)) return;
                    visible.push({ label: label, val: val });
                });
                if (visible.length === 0) return '';
                var html = skipTitle ? '' : '<div class="brochure-detail-section-title">' + escapeHtml(section.title) + '</motion>';
                visible.forEach(function(item, i) {
                    var label = item.label, val = item.val;
                    var isExecSummaryRow = (label === 'Executive summary');
                    var rowClass = isExecSummaryRow ? ' brochure-detail-row exec-summary-text' : ' brochure-detail-row';
                    if (i === visible.length - 1) rowClass += ' last-in-section';
                    if (isExecSummaryRow) {
                        html += '<div class="' + rowClass.trim() + '"><span class="v">' + escapeHtml(val) + '</span></div>';
                    } else {
                        html += '<div class="' + rowClass.trim() + '"><span class="l">' + escapeHtml(label) + '</span><span class="v">' + escapeHtml(val) + '</span></div>';
                    }
                });
                return html;
            }`;

const renderDetailNew = `            function isOwnerDraftMode(ctx) {
                return ctx && ctx.mode === (window.DealBriefV2 && DealBriefV2.MODES.OWNER_DRAFT);
            }
            function renderDetailSection(fields, section, skipTitle, ctx) {
                ctx = ctx || {};
                var V2 = window.DealBriefV2;
                var ownerDraft = isOwnerDraftMode(ctx);
                var visible = [];
                section.rows.forEach(function(r) {
                    var label = r[0], keys = r[1];
                    var airtableKey = keys[0] || '';
                    if (V2 && !V2.isFieldShownInBrief(airtableKey, fields, ctx.readiness)) return;
                    var val = getField(fields, keys);
                    if (val === 'Franchise + Third-Party Management') val = 'Franchise + 3rd Party Mgmt.';
                    if (label === 'Current brand/operator') {
                        var parent = (fields['Parent Company Name'] != null && fields['Parent Company Name'] !== '') ? String(fields['Parent Company Name']).trim() : '';
                        var brand = (fields['Current Brand Affiliation'] != null && fields['Current Brand Affiliation'] !== '') ? String(fields['Current Brand Affiliation']).trim() : '';
                        var op = (fields['Operator Name Current'] != null && fields['Operator Name Current'] !== '') ? String(fields['Operator Name Current']).trim() : '';
                        val = [parent && ('Parent: ' + parent), brand && ('Brand: ' + brand), op && ('Operator: ' + op)].filter(Boolean).join(' · ') || '';
                    }
                    if (label === 'Room breakdown') {
                        var std = (fields['Number of Standard Rooms'] != null && fields['Number of Standard Rooms'] !== '') ? String(fields['Number of Standard Rooms']) : '';
                        var suite = (fields['Number of Suites'] != null && fields['Number of Suites'] !== '') ? String(fields['Number of Suites']) : '';
                        val = [std && (std + ' Standard'), suite && (suite + ' Suites')].filter(Boolean).join(', ') || '';
                    }
                    if (label === 'Meeting Space') {
                        val = (fields['Meeting Space'] != null && fields['Meeting Space'] !== '') ? (String(fields['Meeting Space']) + (fields['Meeting Space Unit'] ? ' ' + formatFieldValue(fields['Meeting Space Unit']) : '')) : '';
                    }
                    if (!hasValue(val)) {
                        if (!ownerDraft) return;
                        val = 'Not provided';
                    }
                    var displayLabel = V2 ? V2.executiveFieldLabel(airtableKey || label) : label;
                    visible.push({ label: displayLabel, val: val });
                });
                if (visible.length === 0) return '';
                var block = skipTitle ? '' : '<div class="brochure-detail-section-title">' + escapeHtml(section.title) + '</div>';
                visible.forEach(function(item, i) {
                    var label = item.label, val = item.val;
                    var isExecSummaryRow = (label === 'Executive summary');
                    var rowClass = isExecSummaryRow ? ' brochure-detail-row exec-summary-text' : ' brochure-detail-row';
                    if (i === visible.length - 1) rowClass += ' last-in-section';
                    var valClass = val === 'Not provided' ? ' v empty' : ' v';
                    if (isExecSummaryRow) {
                        block += '<div class="' + rowClass.trim() + '"><span class="v">' + escapeHtml(val) + '</span></div>';
                    } else {
                        block += '<motion class="' + rowClass.trim() + '"><span class="l">' + escapeHtml(label) + '</span><span class="' + valClass.trim() + '">' + escapeHtml(val) + '</span></div>';
                    }
                });
                return block;
            }`;

const renderDetailNewFixed = renderDetailNew
  .replace(/<motion class="/g, '<motion class="'.replace("<motion", "<div"))
  .replace("</motion>", "");

// fix the botched replace - rewrite renderDetailNewFixed properly
const renderDetailNewClean = renderDetailNew.replace(
  "block += '<motion class=\"' + rowClass.trim() + '\"><span class=\"l\">'",
  "block += '<div class=\"' + rowClass.trim() + '\"><span class=\"l\">'"
);

if (!html.includes("function renderDetailSection(fields, section, skipTitle) {")) {
  console.error("renderDetailSection not found");
  process.exit(1);
}

html = html.replace(
  /            function renderDetailSection\(fields, section, skipTitle\) \{[\s\S]*?                return html;\n            \}/,
  renderDetailNewClean
);

const renderSig = "            function render(fields, normalized) {";
const renderReplacement = `            function applyBriefModeToDocument(ctx) {
                var V2 = window.DealBriefV2;
                if (!V2) return;
                var ownerDraft = ctx.mode === V2.MODES.OWNER_DRAFT;
                document.body.classList.toggle('brief-mode-owner', ownerDraft);
                document.body.classList.toggle('brief-mode-recipient', !ownerDraft);
                var cover = V2.coverCopyForMode(ctx.mode);
                document.getElementById('coverConfidential').textContent = cover.confidential;
                document.getElementById('coverSub').textContent = cover.sub;
                var disc = document.querySelector('.brochure-cover-disclaimer');
                if (disc) disc.textContent = cover.disclaimer;
                var contact = V2.contactCopyForMode(ctx.mode);
                var ctaEl = document.getElementById('briefContactCta');
                if (ctaEl) ctaEl.textContent = contact.cta;
                var propCta = document.getElementById('briefProposalCta');
                if (propCta) propCta.hidden = !contact.showProposalCta;
                var ownerPanel = document.getElementById('briefOwnerStatus');
                if (ownerPanel) ownerPanel.hidden = !ownerDraft;
                var statusCard = document.getElementById('briefStatusCard');
                if (statusCard) statusCard.hidden = !ownerDraft;
            }
            function renderOwnerReadinessPanel(readiness) {
                var V2 = window.DealBriefV2;
                if (!V2 || !readiness) return;
                var stage = readiness.readinessStage || '—';
                var score = readiness.dealReadinessScore;
                var mapped = V2.mapBriefStatusFromStage(stage);
                document.getElementById('briefReadinessScore').textContent =
                  score != null && score !== '' ? score + ' / 100' : '—';
                document.getElementById('briefReadinessStage').textContent = stage;
                document.getElementById('briefStatusLabel').textContent = mapped.briefStatus;
                document.getElementById('briefExternalSharing').textContent = mapped.externalSharing;
                var cardBrief = document.getElementById('cardBriefStatus');
                if (cardBrief) {
                  cardBrief.innerHTML =
                    '<div class="card-line"><span class="card-label">Brief:</span> ' + escapeHtml(mapped.briefStatus) + '</div>' +
                    '<div class="card-line"><span class="card-label">Sharing:</span> ' + escapeHtml(mapped.externalSharing) + '</div>' +
                    '<div class="card-line"><span class="card-label">Review:</span> ' + escapeHtml(mapped.reviewStatusLabel) + '</div>';
                }
                var items = V2.buildValidationItems(readiness);
                var block = document.getElementById('briefValidationBlock');
                var list = document.getElementById('briefValidationList');
                if (block && list) {
                  if (items.length) {
                    block.hidden = false;
                    list.innerHTML = items.map(function (l) { return '<li>' + escapeHtml(l) + '</li>'; }).join('');
                  } else {
                    block.hidden = true;
                    list.innerHTML = '';
                  }
                }
            }
            function render(fields, normalized, ctx) {
                ctx = ctx || { mode: (window.DealBriefV2 && DealBriefV2.MODES.OWNER_DRAFT) };
                var V2 = window.DealBriefV2;
                var ownerDraft = ctx.mode === (V2 && V2.MODES.OWNER_DRAFT);
                applyBriefModeToDocument(ctx);`;

if (!html.includes(renderSig)) {
  console.error("render() not found");
  process.exit(1);
}
html = html.replace(renderSig, renderReplacement);

html = html.replace(
  `                function buildDealalityExecutiveSummary() {
                    var keysVal = getField(fields, ['Total Number of Rooms/Keys']) || normalized.totalKeys || '';
                    var locationVal = getField(fields, ['City & State', 'Country', 'Hotel Submarket & Location']) || normalized.hotelLocation || 'the identified market';
                    var currentStatus = getField(fields, ['Current Brand Affiliation', 'Operator Name Current']) || 'current operating setup';
                    var dealPath = getField(fields, ['Preferred Deal Path', 'Preferred Deal Structure', 'Who should receive bids for this project?']) || normalized.dealStructureLabel || 'the preferred deal path';
                    var goals = getField(fields, ['Primary Goal for the Hotel', 'Top Priorities for Project']) || 'long-term asset value and execution certainty';
                    return 'Dealality reviewed this ' + (keysVal ? (keysVal + '-key ') : '') + 'opportunity in ' + locationVal + '; the asset is currently under ' + currentStatus + ' and the owner is evaluating ' + dealPath + ' options. ' +
                        'Respondents should focus on economics, flexibility, and guest positioning aligned with owner priorities around ' + goals + '.';
                }`,
  `                function buildRecipientOpportunitySummary() {
                    if (!V2) return getField(fields, ['Company Executive Summary']) || '';
                    var meta = {
                      keyCount: getField(fields, ['Total Number of Rooms/Keys']) || normalized.totalKeys || '',
                      marketLine: getField(fields, ['City & State', 'Country', 'Hotel Submarket & Location']) || normalized.hotelLocation || 'the identified market',
                      projectType: normalized.projectType || getField(fields, ['Project Type']) || ''
                    };
                    return V2.buildRecipientOpportunityLead(meta, normalized);
                }
                function buildOwnerOpportunitySummary() {
                    if (!V2) return getField(fields, ['Company Executive Summary']) || '';
                    var meta = {
                      projectType: normalized.projectType || getField(fields, ['Project Type']) || '—',
                      targetPositioning: getField(fields, ['Brand Positioning', 'Target Chain Scale', 'Hotel Chain Scale', 'Preferred Chain Scales']) || ''
                    };
                    return V2.buildOwnerOpportunityLead(meta);
                }`
);

html = html.replace(
  `                if (roomKeys) dealHookParts.push(roomKeys + ' keys');
                if (projectType && projectType !== '—') dealHookParts.push(projectType);
                if (opening && opening !== '—') dealHookParts.push('Target ' + opening);
                if (normalized.dealStructureLabel) dealHookParts.push(normalized.dealStructureLabel);
                document.getElementById('coverDealHook').textContent = dealHookParts.length ? dealHookParts.join(' · ') : '—';
                var prepDate = new Date();
                document.getElementById('coverDatePrepared').textContent = 'Prepared ' + prepDate.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });`,
  `                var positioning = getField(fields, ['Brand Positioning', 'Target Chain Scale', 'Hotel Chain Scale', 'Preferred Chain Scales']);
                if (roomKeys) dealHookParts.push(roomKeys + ' keys');
                if (projectType && projectType !== '—') dealHookParts.push(projectType);
                if (positioning) dealHookParts.push(positioning);
                document.getElementById('coverDealHook').textContent = dealHookParts.length ? dealHookParts.join(' · ') : '—';
                var prepDate = new Date();
                var cover = V2 ? V2.coverCopyForMode(ctx.mode) : null;
                document.getElementById('coverDatePrepared').textContent =
                  (ownerDraft ? 'Generated ' : 'Prepared ') +
                  prepDate.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' }) +
                  (cover && ownerDraft ? ' · ' + cover.dateSuffix : '');`
);

html = html.replace(
  `                /* Opportunity card: Dealality executive narrative (avoid repetitive field list) */
                var execSummaryInCard = getField(fields, ['Company Executive Summary']) || buildDealalityExecutiveSummary();`,
  `                if (ownerDraft && ctx.readiness) renderOwnerReadinessPanel(ctx.readiness);
                var execSummaryInCard = getField(fields, ['Company Executive Summary']);
                if (!execSummaryInCard) {
                  execSummaryInCard = ownerDraft ? buildOwnerOpportunitySummary() : buildRecipientOpportunitySummary();
                }`
);

html = html.replace(
  `                if (!existingExecSummary) {
                    fields['Company Executive Summary'] = buildDealalityExecutiveSummary();
                }`,
  `                if (!existingExecSummary && ownerDraft) {
                    fields['Company Executive Summary'] = buildOwnerOpportunitySummary();
                } else if (!existingExecSummary && !ownerDraft) {
                    fields['Company Executive Summary'] = buildRecipientOpportunitySummary();
                }`
);

html = html.replace(
  `                    var block = renderDetailSection(fields, s, skipTitle);`,
  `                    var block = renderDetailSection(fields, s, skipTitle, ctx);`
);

html = html.replace(
  `                render(alcoveGloriaDemo.fields, alcoveGloriaDemo.normalized);`,
  `                var demoCtx = { mode: (window.DealBriefV2 && DealBriefV2.resolveBriefMode(new URLSearchParams(window.location.search))) };
                render(alcoveGloriaDemo.fields, alcoveGloriaDemo.normalized, demoCtx);`
);

const loadPatch = `                    render(fields, data.normalized || {});`;
const loadReplacement = `                    var params = new URLSearchParams(window.location.search || '');
                    var briefCtx = {
                      mode: window.DealBriefV2
                        ? DealBriefV2.resolveBriefMode(params)
                        : 'ownerDraft',
                      readiness: null
                    };
                    if (briefCtx.mode === (DealBriefV2 && DealBriefV2.MODES.OWNER_DRAFT)) {
                      try {
                        var rr = await auth.fetchMyDealsApi('/api/ai/deal-readiness-review', {
                          method: 'POST',
                          headers: { 'Content-Type': 'application/json' },
                          body: JSON.stringify({ dealId: dealId })
                        });
                        var rd = await rr.json();
                        if (rd && rd.success) briefCtx.readiness = rd;
                      } catch (readinessErr) { /* optional */ }
                    }
                    render(fields, data.normalized || {}, briefCtx);`;

if (!html.includes(loadPatch)) {
  console.error("load render call not found");
  process.exit(1);
}
html = html.replace(loadPatch, loadReplacement);

fs.writeFileSync(path, html);
console.log("Patched render logic in", path);
