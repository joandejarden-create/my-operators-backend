/**
 * Align static brand-education popup Commercial tab with owner-facing combined explorer copy.
 *
 *   node scripts/patch-brand-education-commercial-tab.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC = path.join(__dirname, "../public");

const FILES = [
  "brand-education-atelier-north.html",
  "brand-education-voco.html",
  "brand-education-summit-house.html",
  "brand-education-velvet-crown.html",
];

const NEW_BLOCK = `              <section class="oe-section">
                <h2 class="oe-section-title">Commercial Strengths</h2>
                <p class="oe-section-hint">Brand-specific benefits and channel levers (illustrative; not property-specific performance)</p>
                <div class="explorer-detail-card" style="margin-bottom:14px;">
                  <h3 class="explorer-detail-card__label">How this brand can lift your project</h3>
                  <p class="explorer-detail-card__body">This profile shows how affiliation can affect <strong style="color:var(--text);font-weight:600;">demand, rate, and channel mix</strong> on your asset. <strong style="color:var(--text);font-weight:600;">Project impact</strong> lines describe benefits for this brand—not a generic checklist. Illustrative only; not a performance guarantee.</p>
                </div>
                <div class="brand-markets-kpi" style="margin-bottom:16px;" aria-label="Illustrative commercial footprint">
                  <div class="brand-markets-kpi__card">
                    <div class="brand-markets-kpi__label">Channels in franchise materials</div>
                    <div class="brand-markets-kpi__value">Brand.com · major OTAs · GDS · metasearch</div>
                  </div>
                  <div class="brand-markets-kpi__card">
                    <div class="brand-markets-kpi__label">Campaign rhythm</div>
                    <div class="brand-markets-kpi__value">Always-on + seasonal / market bursts</div>
                  </div>
                  <div class="brand-markets-kpi__card">
                    <div class="brand-markets-kpi__label">B2B programs</div>
                    <div class="brand-markets-kpi__value">RFP &amp; account programs (where active)</div>
                  </div>
                  <div class="brand-markets-kpi__card">
                    <div class="brand-markets-kpi__label">Owner underwriting lens</div>
                    <div class="brand-markets-kpi__value">Net contribution after fees and channel costs</div>
                  </div>
                </div>
                <div class="scenario-card-grid" style="grid-template-columns: repeat(3, 1fr);">
                  <div class="scenario-card">
                    <h4>Distribution &amp; Retail Reach</h4>
                    <p>Branded retail paths guests already use—CRS connectivity, brand.com and app, retail OTA relationships, and packages—so the property shows up in consideration sets where independents often under-index.</p>
                    <p><span class="scenario-card__label">Project impact</span>Evaluate whether you get more qualified demand without funding a global platform alone—portfolio campaigns and rate plans that match how guests shop in your segment.</p>
                  </div>
                  <div class="scenario-card">
                    <h4>Revenue Management &amp; Pricing Discipline</h4>
                    <p>Forecasting tools, competitive sets, restriction strategies, and brand-level playbooks tuned to the chain scale—not only discounting.</p>
                    <p><span class="scenario-card__label">Project impact</span>Underwrite ADR protection in peak windows and escalation support during shocks; confirm what is included in your agreement tier.</p>
                  </div>
                  <div class="scenario-card">
                    <h4>Digital Marketing &amp; Performance Media</h4>
                    <p>Paid and owned media, search, social, and retargeting at portfolio scale, with creative templates that can still carry property-level story.</p>
                    <p><span class="scenario-card__label">Project impact</span>Pooled spend can lower acquisition cost at the margin; expect always-on brand search and seasonal bursts aligned to holidays, events, and city calendars.</p>
                  </div>
                  <div class="scenario-card">
                    <h4>Corporate, SME &amp; Group Pull</h4>
                    <p>Contracted travelers, small meetings, and negotiated programs where the flag acts as a trusted filter—especially in urban and gateway mixed-use assets.</p>
                    <p><span class="scenario-card__label">Project impact</span>RFP tools, account coverage, and standard proposals can open corporate doors; size addressable demand directionally for your market tier.</p>
                  </div>
                  <div class="scenario-card">
                    <h4>Leisure &amp; Destination Visibility</h4>
                    <p>Inspiration content, packages, partnerships, and destination narratives for high-intent leisure shoppers—when rate premium depends on aspiration and uniqueness.</p>
                    <p><span class="scenario-card__label">Project impact</span>Local design, F&amp;B, and ties still matter; distribution should convert the story—earlier visibility to the right guests in the booking journey.</p>
                  </div>
                  <div class="scenario-card">
                    <h4>International &amp; Feeder Markets</h4>
                    <p>Inbound and cross-border feeders where global recognition reduces perceived risk—gateways, hubs, and resorts with international mix.</p>
                    <p><span class="scenario-card__label">Project impact</span>Language, currency, and channel coverage in feeder countries; portfolio campaigns tied to holidays and routes—performance varies by market maturity and airlift.</p>
                  </div>
                  <div class="scenario-card">
                    <h4>Sales &amp; Catering Brand Pull</h4>
                    <p>Brand credibility, central inquiry flow, and proposal tools for weddings, SMERF, and small corporate meetings.</p>
                    <p><span class="scenario-card__label">Project impact</span>Compare lead quality and trust transfer against going independent; weigh brand contribution vs. in-house sales and catering cost.</p>
                  </div>
                  <div class="scenario-card">
                    <h4>Reputation, Reviews &amp; QA Lift</h4>
                    <p>Recognizable flags improve post-click conversion; QA and service standards reduce variance that hurts reviews and repeat visits.</p>
                    <p><span class="scenario-card__label">Project impact</span>Review response, recovery playbooks, and brand-led offers can protect long-term rate power when executed consistently on property.</p>
                  </div>
                  <div class="scenario-card">
                    <h4>Data, Analytics &amp; Experimentation</h4>
                    <p>Portfolio benchmarks, test-and-learn, and guest insights to refine offers, room types, and channel mix.</p>
                    <p><span class="scenario-card__label">Project impact</span>Network learning, test campaigns, and reporting many lenders expect—treat as commercial intelligence for decisions, not vanity metrics.</p>
                  </div>
                </div>
                <div class="oe-cluster" style="margin-top:4px;">
                  <h3>Recurring themes in franchise materials</h3>
                  <ul>
                    <li><strong style="color:var(--text);font-weight:600;">More demand at the top of the funnel</strong> — retail presence, search, and inspiration media guests see before they choose a city or date.</li>
                    <li><strong style="color:var(--text);font-weight:600;">Better conversion at the bottom</strong> — trust, reviews, loyalty, and frictionless booking paths that turn lookers into stays.</li>
                    <li><strong style="color:var(--text);font-weight:600;">Repeat and higher-quality guests</strong> — loyalty, corporate accounts, and recognition that increase lifetime value versus one-off OTA transactions.</li>
                    <li><strong style="color:var(--text);font-weight:600;">Commercial systems, not just a logo</strong> — pricing, sales support, and analytics that should earn the fee in your pro forma.</li>
                  </ul>
                </div>
              </section>`;

const START_MARKERS = [
  /<section class="oe-section">\s*<h2 class="oe-section-title">Commercial Strengths<\/h2>/,
];
const END_MARKER = /<\/section>\s*<section class="oe-section">\s*<h2 class="oe-section-title">Demand Scenario View<\/h2>/;

for (const file of FILES) {
  const full = path.join(PUBLIC, file);
  let html = fs.readFileSync(full, "utf8");
  const startMatch = html.search(START_MARKERS[0]);
  const endMatch = html.search(END_MARKER);
  if (startMatch < 0 || endMatch < 0) {
    console.warn(`Skip ${file}: commercial section markers not found`);
    continue;
  }
  html = html.slice(0, startMatch) + NEW_BLOCK + "\n              " + html.slice(endMatch);
  fs.writeFileSync(full, html, "utf8");
  console.log("patched", file);
}

console.log("Done.");
