/**
 * Build fixtures/brand-explorer-presentation-radisson-blu.example.json from Radisson core template.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const src = JSON.parse(
  fs.readFileSync(path.join(ROOT, "fixtures/brand-explorer-presentation-radisson.example.json"), "utf8")
);

function transform(s) {
  if (!s) return s;
  return (
    s
      .replace(/Radisson —/g, "Radisson Blu —")
      .replace(/Radisson balances/g, "Radisson Blu balances")
      .replace(/Radisson is commonly/g, "Radisson Blu is commonly")
      .replace(/Radisson competes/g, "Radisson Blu competes")
      .replace(/Radisson pitches/g, "Radisson Blu pitches")
      .replace(/Radisson is most compelling/g, "Radisson Blu is most compelling")
      .replace(/Radisson's/g, "Radisson Blu's")
      .replace(/Radisson /g, "Radisson Blu ")
      .replace(/upscale full-service/gi, "upper-upscale full-service")
      .replace(/Upscale conversion/g, "Upper-upscale conversion")
      .replace(/upscale consumers/gi, "upper-upscale consumers")
      .replace(/upscale positioning/gi, "upper-upscale positioning")
      .replace(/upscale or full-service/gi, "upper-upscale or full-service")
      .replace(/upscale retail/gi, "upper-upscale retail")
      .replace(/A Century Young/g, "Think in Black & White Blu")
      .replace(/Charming simplicity/g, "Enticing Moments")
      .replace(/Contemporary classic/g, "Nordic Nouveau")
      .replace(/Gracious hospitality/g, "Curatorial Warmth")
      .replace(/kit-of-parts/gi, "design-forward prototype")
  );
}

const out = {
  targetBrandBasicsName: "Radisson Blu (Choice)",
  brandNameFallback: "Radisson Blu (Choice)",
  instructions:
    'Core Brand Explorer slots (same keys as Radisson (Choice)). After clone from Choice, patch: npm run apply-brand-explorer-presentation -- --brand-name "Radisson Blu" --fixture fixtures/brand-explorer-presentation-radisson-blu.example.json --replace-slot-prefix overview. (see docs/radisson-blu-choice-reference.md)',
  rows: src.rows.map((r) => ({
    ...r,
    title: r.title ? transform(r.title) : r.title,
    body: r.body ? transform(r.body) : r.body,
  })),
};

const bySlot = Object.fromEntries(out.rows.map((r) => [r.slotKey, r]));
bySlot["hero.benefit_zones"].body =
  "Tagline: Think in Black & White Blu. Upper-upscale positioning for top urban and resort destinations—Nordic Nouveau design, Enticing Moments experience, Curatorial Warmth service. Americas presence: 10 open (3 domestic, 7 international); pipeline per Choice development materials (verify counts). Loyalty: Choice Privileges under Choice-affiliated positioning.";
bySlot["hero.operator_compat"].body =
  "Purpose fit: redefine upper-upscale hospitality—style with substance, innovation with comfort, belonging in an elevated environment. Operators who deliver design-forward full-service with gallery-curator service warmth and can run meetings, F&B theater, and tech-enabled guest rooms.";
bySlot["overview.typical_use_case"].body =
  "Top urban gateways, iconic resort markets, and design-forward conversions where guests expect memorable public spaces, fine-dining theater, and Scandinavian-inspired rooms—not limited-service or economy extended-stay.";
bySlot["overview.relative_positioning"].body =
  "Upper-upscale flag within the Choice portfolio—iconic design heritage (Arne Jacobsen lineage), distinctive public spaces, and enterprise distribution; above core Radisson upscale, not ultra-luxury or select-service.";
bySlot["overview.development_model"].body =
  "New construction, adaptive reuse, and conversion where the asset can support upper-upscale casegoods, baths, and signature F&B—top urban and resort destinations per development criteria.";
bySlot["operations.flexibility.design"].body = "Moderate";
bySlot["operations.flexibility.conversion"].body = "Moderate";
bySlot["operations.flexibility.prototype"].body = "High";
bySlot["operations.standards_philosophy"].body =
  "Radisson Blu balances upper-upscale design discipline—Nordic Nouveau interiors, memorable public spaces, and curatorial service warmth—with market-appropriate conversion paths. Iconic, stimulating environments should feel purposeful, not sterile; safety, cleanliness, and repeatable service benchmarks stay non-negotiable under Choice Privileges and portfolio QA where Choice-affiliated.";
bySlot["overview.scenario.1"].title = "Iconic Urban Flagship";
bySlot["overview.scenario.1"].body =
  "Strong fit where the owner wants a design-forward upper-upscale box in a top urban market—meetings, signature F&B, and Inspired Professional guests who reject boring big-box experiences.";
bySlot["overview.scenario.2"].title = "Resort and Leisure Destination";
bySlot["overview.scenario.2"].body =
  "Resort or mixed-use leisure settings where memorable public spaces, wellness, and distinctive design justify upper-upscale ADR—not a mainstream upscale conversion play.";
bySlot["overview.scenario.3"].title = "Adaptive Reuse and Conversion";
bySlot["overview.scenario.3"].body =
  "Adaptive reuse or conversion where structure and ceiling heights can support Blu public-space and room standards; pair with realistic capex and operator depth for gallery-curator service delivery.";
bySlot["loyalty.hero_title"].body =
  "Radisson Blu — Think in Black & White Blu · Loyalty (Choice Privileges under Choice-affiliated positioning)";
bySlot["insight.summary"].body =
  "Radisson Blu is most compelling for owners targeting top urban and resort destinations who want recognizable upper-upscale design heritage, Choice distribution, and loyalty-led retail for the Inspired Professional—guests allergic to boring, big-box experiences. Strongest with operators who can deliver design-forward F&B, meetings product, and gallery-curator service without waivers. Weaker where the physical plant cannot support upper-upscale casegoods and public-space investment, or where the market is better served by core Radisson upscale conversion economics.";

const dest = path.join(ROOT, "fixtures/brand-explorer-presentation-radisson-blu.example.json");
fs.writeFileSync(dest, JSON.stringify(out, null, 2) + "\n");
console.log("Wrote", dest, "rows:", out.rows.length);
