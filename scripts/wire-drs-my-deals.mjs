import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const p = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "public", "my-deals.html");
let html = fs.readFileSync(p, "utf8");

if (!html.includes("/css/deal-readiness-snapshot.css")) {
  html = html.replace(
    "<title>My Deals - Dealality</title>",
    "<title>My Deals - Dealality</title>\n    <link rel=\"stylesheet\" href=\"/css/deal-readiness-snapshot.css\">"
  );
}

if (!html.includes("deal-readiness-snapshot.js")) {
  html = html.replace(
    '<script src="/js/deal-package-validation.js"></script>',
    '<script src="/js/deal-package-validation.js"></script>\n    <script src="/js/deal-readiness-snapshot.js"></script>'
  );
}

html = html.replace(
  /<h2 id="myDealsReadinessModalTitle">Deal Readiness Review<\/h2>/,
  '<h2 id="myDealsReadinessModalTitle">Deal Readiness Snapshot</h2>'
);

html = html.replace(
  /Deterministic score and gaps from your saved fields; narrative from AI when configured\./,
  "Readiness signals from saved Deal Setup fields. Draft output for owner/advisor validation."
);

const start = html.indexOf("                function renderReadinessBody(data) {");
const end = html.indexOf("                function runReview(dealId) {");
if (start < 0 || end < 0) {
  console.error("Could not find renderReadinessBody bounds", start, end);
  process.exit(1);
}

const replacement = `                function buildUiHighlightFieldsFromReadiness(data) {
                    var highlightSeen = {};
                    var uiHighlightFields = [];
                    function pushHighlight(name) {
                        var key = String(name || '').trim();
                        if (!key || highlightSeen[key]) return;
                        highlightSeen[key] = true;
                        uiHighlightFields.push(key);
                    }
                    (data.missingInformation || []).forEach(function (m) { if (m && m.field) pushHighlight(m.field); });
                    (data.weakInformation || []).forEach(function (w) { if (w && w.field) pushHighlight(w.field); });
                    var plan = data.scoreImprovementPlan || {};
                    (plan.priorityActions || []).forEach(function (a) { if (a && a.relatedField) pushHighlight(a.relatedField); });
                    data.uiHighlightFields = uiHighlightFields;
                    return uiHighlightFields;
                }

                function renderReadinessBody(data) {
                    if (!contentEl) return;
                    if (!window.DealReadinessSnapshot || typeof window.DealReadinessSnapshot.render !== 'function') {
                        contentEl.innerHTML = '<div class="my-deals-readiness-error">Snapshot renderer failed to load.</div>';
                        contentEl.style.display = 'block';
                        if (loadingEl) loadingEl.style.display = 'none';
                        return;
                    }
                    var dealContext = (typeof getDealByIdLocal === 'function' && activeDealId) ? getDealByIdLocal(activeDealId) : null;
                    buildUiHighlightFieldsFromReadiness(data);
                    var missingCount = (data.missingInformation || []).length;
                    var blockingCount = (data.blockingIssues || []).length;
                    data.dealReadinessMissingCount = missingCount;
                    data.dealReadinessBlockingCount = blockingCount;
                    var editHref = activeDealId ? newDealSetupEditUrl(activeDealId, 'highlightReadinessGaps=1') : '';
                    var fullPageHref = activeDealId
                        ? '/deal-readiness-snapshot.html?dealId=' + encodeURIComponent(activeDealId) + '&embed=1'
                        : '';
                    window.DealReadinessSnapshot.render(contentEl, data, {
                        embed: true,
                        dealId: activeDealId,
                        listDeal: dealContext,
                        editDealHref: editHref,
                        fullPageHref: fullPageHref,
                        footerHtml: '<p class="my-deals-readiness-save-status" id="myDealsReadinessSaveStatus" aria-live="polite"></p>',
                        generatedAt: data.savedAt || new Date().toISOString(),
                    });
                    contentEl.style.display = 'block';
                    if (loadingEl) loadingEl.style.display = 'none';

                    (function autoSaveReadinessReviewToAirtable() {
                        if (!activeDealId || !lastResponse) return;
                        var statusEl = document.getElementById('myDealsReadinessSaveStatus');
                        if (statusEl) statusEl.textContent = 'Saving review to deal record…';
                        var score = lastResponse.dealReadinessScore;
                        var stage = lastResponse.readinessStage;
                        fetch('/api/ai/deal-readiness-review/save', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                dealId: activeDealId,
                                review: {
                                    dealReadinessScore: score,
                                    readinessStage: stage,
                                    missingInformation: lastResponse.missingInformation,
                                    weakInformation: lastResponse.weakInformation,
                                    blockingIssues: lastResponse.blockingIssues,
                                    sectionScores: lastResponse.sectionScores,
                                    ai: lastResponse.ai,
                                    workflowRecommendation: lastResponse.workflowRecommendation,
                                    scoreImprovementPlan: lastResponse.scoreImprovementPlan,
                                    humanReadableSummary: lastResponse.humanReadableSummary || '',
                                },
                            }),
                        })
                            .then(function(r) { return r.json().then(function(j) { return { ok: r.ok, status: r.status, body: j }; }); })
                            .then(function(pack) {
                                var res = pack.body;
                                if (!res.success) {
                                    if (statusEl) {
                                        if (res.code === 'SAVE_NOT_CONFIGURED' || pack.status === 501 ||
                                                (res.error && /not configured/i.test(res.error))) {
                                            statusEl.textContent = 'Review complete. Saving to Airtable is not configured yet.';
                                        } else {
                                            statusEl.textContent = res.error || 'Could not save review.';
                                        }
                                    }
                                    return;
                                }
                                if (statusEl) statusEl.textContent = 'Saved to deal record.';
                                if (lastResponse && typeof lastResponse === 'object') {
                                    lastResponse.savedAt = res.savedAt || lastResponse.savedAt || '';
                                }
                                updateReadinessModalMetaChips(res.savedAt || '', missingCount, blockingCount);
                                patchDealAfterSave(activeDealId, res, res.savedAt);
                            })
                            .catch(function(err) {
                                if (statusEl) statusEl.textContent = err.message || 'Save failed.';
                            });
                    })();
                }

`;

let out = html.slice(0, start) + replacement + html.slice(end);
out = out.replace(/'<motionless><\/motionless>'\.slice\(0, 0\) \+ '/g, "'");
out = out.replace(/<motionless><\/motionless>/g, "");
fs.writeFileSync(p, out);
console.log("my-deals.html wired");
