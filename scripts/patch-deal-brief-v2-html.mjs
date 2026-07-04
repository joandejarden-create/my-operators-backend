import fs from "fs";

const path = "public/deal-summary.html";
let html = fs.readFileSync(path, "utf8");
const t = "div";

const marker = `            <${t} class="brochure-highlights">
                <${t} class="brochure-icon-strip">`;

const insert = `            <${t} class="brochure-highlights">
                <${t} id="briefOwnerStatus" class="brochure-owner-status" hidden>
                    <${t} class="brochure-owner-status-title">Brief Status &amp; Readiness</${t}>
                    <${t} class="brochure-owner-status-grid">
                        <${t} class="brochure-owner-status-item"><label>Readiness Score</label><span id="briefReadinessScore">—</span></${t}>
                        <${t} class="brochure-owner-status-item"><label>Readiness Stage</label><span id="briefReadinessStage">—</span></${t}>
                        <${t} class="brochure-owner-status-item"><label>Brief Status</label><span id="briefStatusLabel">—</span></${t}>
                        <${t} class="brochure-owner-status-item"><label>External Sharing</label><span id="briefExternalSharing">—</span></${t}>
                    </${t}>
                    <${t} id="briefValidationBlock" class="brochure-validation-block" hidden>
                        <h4>Key Validation Items</h4>
                        <ul id="briefValidationList" class="brochure-validation-list"></ul>
                    </${t}>
                </${t}>
                <${t} class="brochure-icon-strip">`;

if (!html.includes(marker)) {
  console.error("Marker not found");
  process.exit(1);
}
html = html.replace(marker, insert);

html = html.replace(
  `<${t} class="brochure-icon-item"><span class="icon"></span> <strong id="hlBids">—</strong> Who Receives Bids</${t}>`,
  `<${t} class="brochure-icon-item brochure-recipient-only"><span class="icon"></span> <strong id="hlBids">—</strong> Who Receives Bids</${t}>`
);

html = html.replace(
  `                <${t} class="brochure-cards">
                    <${t} class="brochure-card">
                        <${t} class="brochure-card-title">The Opportunity</${t}>`,
  `                <${t} class="brochure-cards">
                    <${t} class="brochure-card" id="briefStatusCard" hidden>
                        <${t} class="brochure-card-title">Brief Status</${t}>
                        <${t} class="brochure-card-body" id="cardBriefStatus">—</${t}>
                    </${t}>
                    <${t} class="brochure-card">
                        <${t} class="brochure-card-title">The Opportunity</${t}>`
);

html = html.replace(
  '<p class="brochure-cta">Visit Dealality to learn more and submit a proposal.</p>',
  '<p class="brochure-cta" id="briefContactCta">Visit Dealality to learn more and submit a proposal.</p>\n                            <a href="#" class="brochure-recipient-cta-btn brochure-recipient-only" id="briefProposalCta" hidden>Submit proposal in Dealality</a>'
);

if (!html.includes("deal-brief-v2.js")) {
  html = html.replace(
    '<script src="/js/dealality-memberstack-auth.js"></script>',
    '<script src="/js/dealality-memberstack-auth.js"></script>\n    <script src="/js/deal-brief-v2.js"></script>'
  );
}

if (!html.includes("Brand / Operator Review Path")) {
  html = html.replace(
    "{ title: 'Strategic & Priorities', col: 3, rows: [",
    `{ title: 'Brand / Operator Review Path', col: 3, rows: [
                    ['Who receives bids', ['Who should receive bids for this project?']],
                    ['Preferred brands', ['Preferred Brands (up to 4)']],
                    ['Preferred chain scales', ['Preferred Chain Scales']],
                    ['Franchise vs management', ['Franchise vs Management Preference']]
                ]},
                { title: 'Strategic Priorities', col: 3, rows: [`
  );
}

fs.writeFileSync(path, html);
console.log("Patched", path);
