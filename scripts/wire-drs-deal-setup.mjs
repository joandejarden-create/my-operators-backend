import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const p = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "public", "new-deal-setup.html");
let html = fs.readFileSync(p, "utf8");

if (!html.includes("/css/deal-readiness-snapshot.css")) {
  html = html.replace(
    /<title>[\s\S]*?<\/title>/,
    (m) => m + '\n    <link rel="stylesheet" href="/css/deal-readiness-snapshot.css">'
  );
}

if (!html.includes("deal-readiness-snapshot.js")) {
  html = html.replace(
    '<script src="/js/dealality-memberstack-auth.js"></script>',
    '<script src="/js/dealality-memberstack-auth.js"></script>\n    <script src="/js/deal-readiness-snapshot.js"></script>'
  );
}

html = html.replace(
  /<h2 id="dealReadinessModalTitle">Deal Readiness Review<\/h2>/,
  '<h2 id="dealReadinessModalTitle">Deal Readiness Snapshot</h2>'
);

html = html.replace(
  /Deterministic score and gaps from your saved fields; narrative from AI when configured\./,
  "Readiness signals from saved Deal Setup fields. Draft output for owner/advisor validation."
);

const start = html.indexOf("        function renderReadiness(data) {");
const end = html.indexOf("        function runDealReadinessReviewFromSetup() {");
if (start < 0 || end < 0) {
  console.error("bounds not found", start, end);
  process.exit(1);
}

const replacement = `        function renderReadiness(data) {
            if (!contentEl) return;
            if (!window.DealReadinessSnapshot || typeof window.DealReadinessSnapshot.render !== "function") {
                contentEl.innerHTML = '<div class="deal-readiness-error">Snapshot renderer failed to load.</div>';
                contentEl.classList.remove("hidden");
                if (loadingEl) loadingEl.classList.add("hidden");
                return;
            }
            var fullPageHref = dealId
                ? "/deal-readiness-snapshot.html?dealId=" + encodeURIComponent(dealId) + "&embed=1"
                : "";
            window.DealReadinessSnapshot.render(contentEl, data, {
                embed: true,
                dealId: dealId,
                fullPageHref: fullPageHref,
                footerHtml: '<p class="deal-readiness-save-status" id="dealReadinessSaveStatus" aria-live="polite"></p>',
                generatedAt: data.savedAt || new Date().toISOString(),
            });
            contentEl.classList.remove("hidden");
            if (loadingEl) loadingEl.classList.add("hidden");

            (function autoSaveReadinessReviewToAirtable() {
                if (!dealId || !lastDealReadinessResponse) return;
                var statusEl = document.getElementById("dealReadinessSaveStatus");
                if (statusEl) statusEl.textContent = "Saving review to deal record…";
                fetch("/api/ai/deal-readiness-review/save", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        dealId: dealId,
                        review: {
                            dealReadinessScore: lastDealReadinessResponse.dealReadinessScore,
                            readinessStage: lastDealReadinessResponse.readinessStage,
                            missingInformation: lastDealReadinessResponse.missingInformation,
                            weakInformation: lastDealReadinessResponse.weakInformation,
                            blockingIssues: lastDealReadinessResponse.blockingIssues,
                            sectionScores: lastDealReadinessResponse.sectionScores,
                            ai: lastDealReadinessResponse.ai,
                            workflowRecommendation: lastDealReadinessResponse.workflowRecommendation,
                            scoreImprovementPlan: lastDealReadinessResponse.scoreImprovementPlan,
                            humanReadableSummary: lastDealReadinessResponse.humanReadableSummary,
                        },
                    }),
                })
                    .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, status: r.status, body: j }; }); })
                    .then(function (pack) {
                        var res = pack.body;
                        if (!res.success) {
                            if (statusEl) {
                                if (res.code === "SAVE_NOT_CONFIGURED" || pack.status === 501 ||
                                        (res.error && /not configured/i.test(res.error))) {
                                    statusEl.textContent = "Review complete. Saving to Airtable is not configured yet.";
                                } else {
                                    statusEl.textContent = res.error || "Could not save review.";
                                }
                            }
                            setHeaderRunBadge(lastDealReadinessResponse, false);
                            return;
                        }
                        if (statusEl) statusEl.textContent = "Saved to deal record.";
                        setHeaderRunBadge(lastDealReadinessResponse, true);
                    })
                    .catch(function (err) {
                        if (statusEl) statusEl.textContent = err.message || "Save failed.";
                        setHeaderRunBadge(lastDealReadinessResponse, false);
                    });
            })();
        }

`;

let out = html.slice(0, start) + replacement + html.slice(end);
out = out.replace(/Snapshot renderer failed to load\.<\/motionless><\/motionless>'\.slice\(0, 0\) \+ "<\/motionless><\/motionless>"\.slice\(0, 0\) \+ "<\/motionless><\/motionless>"\.slice\(0, 0\) \+ "<\/div>"/g,
  'Snapshot renderer failed to load.</motionless></motionless>'.slice(0,0) + '</div>');
out = out.replace(/Snapshot renderer failed to load\.<\/motionless><\/motionless>'\.slice\(0, 0\) \+ "<\/motionless><\/motionless>"\.slice\(0, 0\) \+ "<\/div>"/g,
  "Snapshot renderer failed to load.</motionless></motionless>".slice(0,0));
out = out.replace(/Snapshot renderer failed to load\.<\/motionless><\/motionless>'\.slice\(0, 0\) \+ "<\/motionless><\/motionless>"\.slice\(0, 0\) \+ "<\/div>"/, 'Snapshot renderer failed to load.</div>');
out = out.replace(/contentEl\.innerHTML = '<div class="deal-readiness-error">Snapshot renderer failed to load\.[^;]+;/,
  'contentEl.innerHTML = \'<motionless></motionless>\'.slice(0,0);\n                contentEl.innerHTML = \'<div class="deal-readiness-error">Snapshot renderer failed to load.</div>\';');
out = out.replace(/contentEl\.innerHTML = '<motionless><\/motionless>'\.slice\(0,0\);\s*contentEl\.innerHTML = '<div class="deal-readiness-error">Snapshot renderer failed to load\.<\/div>';/,
  'contentEl.innerHTML = \'<div class="deal-readiness-error">Snapshot renderer failed to load.</div>\';');
out = out.replace(/'<motionless><\/motionless>'\.slice\(0, 0\) \+ '/g, "'");
out = out.replace(/<motionless><\/motionless>/g, "");
fs.writeFileSync(p, out);
console.log("new-deal-setup.html wired");
