/**
 * Runtime smoke: opportunity tile HTML must not render V5A card-contract prose.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const uiPath = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "../public/js/group-demand-intelligence/dealality-gdi-ui.js"
);
const src = fs.readFileSync(uiPath, "utf8");
const start = src.indexOf("function isGenericOrgEmailLocal");
const tileStart = src.indexOf("function opportunityTileHtml");
assert.ok(start > 0 && tileStart > start, "compact helpers before opportunityTileHtml");
const afterTile = src.indexOf("\n  function opportunityCardsGridHtml", tileStart);
assert.ok(afterTile > tileStart, "opportunityCardsGridHtml after tile");
const block = src.slice(start, afterTile);

const helpers = `
function esc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");}
function trunc(s,n){s=String(s||""); return s.length>n?s.slice(0,n-1)+"…":s;}
function scrubEventDatesFromText(s){return String(s||"");}
function isAssessmentStatusProse(){return false;}
function isSafeHttpUrl(u){return /^https?:\\/\\//i.test(String(u||""));}
function priorityToneClass(){return "tone-watch";}
function commercialClassPillHtml(){return "";}
function weeklyDeltaPillHtml(){return "";}
function tileActionPill(){return "<span class=\\"gdi-tile-pill\\">WATCH</span>";}
function tileEventDatePill(){return "<span class=\\"gdi-tile-pill\\">2027</span>";}
function commercialProgressionCompactPill(){return "";}
function weeklyDeltaMetaLine(){return "";}
function readinessPillHtml(){return "";}
function tileClassificationMeta(it, linked){return String((linked&&linked.demandSignalTypeLabel)||(it&&it.segment)||"").toUpperCase();}
`;

const fn = new Function(`${helpers}\n${block}\nreturn opportunityTileHtml;`)();
const html = fn(
  {
    opportunityId: "x",
    title: "AMWA 112th Annual Meeting 2027",
    organizationName: "AMWA",
    segment: "Medical",
    summaryWhat:
      "AMWA annual meeting announced for DC area; host hotel not named.",
    bookingWindowStatus: "CONTACT_NOW",
    primaryContact: { name: "Jane Doe", email: "a@b.com" },
  },
  {
    priority: "HIGH_PRIORITY",
    bookingWindowStatus: "CONTACT_NOW",
    demandSignalTypeLabel: "ASSOCIATION",
    commercialSummary: "SHOULD NOT APPEAR",
    cardWhyNowLine: "WHY NOW SHOULD NOT APPEAR",
    cardHotelFitLine: "FIT SHOULD NOT APPEAR",
    commercialMotionLabel: "OVERFLOW MOTION",
  }
);

assert.match(html, /host hotel not named/i);
assert.doesNotMatch(html, /SHOULD NOT APPEAR/);
assert.doesNotMatch(html, /WHY NOW SHOULD NOT APPEAR/);
assert.doesNotMatch(html, /FIT SHOULD NOT APPEAR/);
assert.doesNotMatch(html, /OVERFLOW MOTION/);
assert.doesNotMatch(html, /brand-card__meta--fit/);
assert.doesNotMatch(html, /brand-card__meta--why-now/);
assert.match(html, /ASSOCIATION|MEDICAL/);
assert.match(html, /View Details/);
assert.match(html, /Jane Doe/);
assert.match(html, /a@b\.com/);

console.log("test:gdi-card-tile-runtime-smoke OK");
