/**
 * One-time maintainer: embed executive profile-detail fields in Operator Setup HTML
 * (static, no JS injection required). Safe to re-run — skips rows that already have the block.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { LEADERSHIP_MEMBER_SELECT_OPTIONS } from "../api/lib/operator-leadership-member-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const htmlPath = path.join(__dirname, "../public/third-party-operator-setup-new-two.html");

function optionsHtml(optionKey) {
  const opts = LEADERSHIP_MEMBER_SELECT_OPTIONS[optionKey] || [];
  return opts
    .map((label) => `<option value="${label.replace(/"/g, "&quot;")}">${label}</option>`)
    .join("\n            ");
}

function buildExecProfileFieldsHtml(n) {
  const prefix = `exec_${n}_`;
  return `
        <div class="exec-profile-detail" data-exec-profile-detail="1">
        <h3 class="project-fit-subheader case-study-row-subheader">Profile detail (Explorer card)</h3>
        <p class="subsection-hint">Maps to Credentials, Languages, Markets, Expertise, and Asset types on each executive card in Operator Explorer. Summary and bio above remain the narrative layer.</p>
        <div class="exec-profile-detail__grid">
    <div class="field-wrap">
    <label class="form-label label-spacing" for="${prefix}hospitality_experience_years">Hospitality experience (years)</label>
    <input class="form-input" type="number" min="0" max="80" step="0.5" name="${prefix}hospitality_experience_years" id="${prefix}hospitality_experience_years" placeholder="e.g. 28" data-explorer-payload="1">
    </div>
    <div class="field-wrap">
    <label class="form-label label-spacing" for="${prefix}company_tenure_years">Company tenure (years)</label>
    <input class="form-input" type="number" min="0" max="80" step="0.5" name="${prefix}company_tenure_years" id="${prefix}company_tenure_years" placeholder="e.g. 9" data-explorer-payload="1">
    </div>
    <div class="field-wrap exec-profile-detail__full">
    <label class="form-label label-spacing" for="${prefix}prior_background">Prior background</label>
    <input class="form-input" type="text" name="${prefix}prior_background" id="${prefix}prior_background" placeholder="e.g. Hilton CALA development — 350+ management agreements" data-explorer-payload="1">
    </div>
    <div class="field-wrap">
    <label class="form-label label-spacing" for="${prefix}languages">Languages</label>
    <span class="help-text">Hold Ctrl/Cmd to select multiple.</span>
    <select class="form-select exec-profile-multiselect" name="${prefix}languages" id="${prefix}languages" multiple size="5" data-explorer-payload="1">
            ${optionsHtml("languages")}
    </select>
    </div>
    <div class="field-wrap">
    <label class="form-label label-spacing" for="${prefix}market_experience">Market experience</label>
    <span class="help-text">Hold Ctrl/Cmd to select multiple.</span>
    <select class="form-select exec-profile-multiselect" name="${prefix}market_experience" id="${prefix}market_experience" multiple size="5" data-explorer-payload="1">
            ${optionsHtml("marketExperience")}
    </select>
    </div>
    <div class="field-wrap exec-profile-detail__full">
    <label class="form-label label-spacing" for="${prefix}core_expertise">Core expertise</label>
    <span class="help-text">Hold Ctrl/Cmd to select multiple.</span>
    <select class="form-select exec-profile-multiselect" name="${prefix}core_expertise" id="${prefix}core_expertise" multiple size="6" data-explorer-payload="1">
            ${optionsHtml("coreExpertise")}
    </select>
    </div>
    <div class="field-wrap exec-profile-detail__full">
    <label class="form-label label-spacing" for="${prefix}relevant_asset_types">Relevant asset types</label>
    <span class="help-text">Hold Ctrl/Cmd to select multiple.</span>
    <select class="form-select exec-profile-multiselect" name="${prefix}relevant_asset_types" id="${prefix}relevant_asset_types" multiple size="5" data-explorer-payload="1">
            ${optionsHtml("relevantAssetTypes")}
    </select>
    </div>
        </div>
        </div>
        `;
}

let html = fs.readFileSync(htmlPath, "utf8");
if (html.includes('data-exec-profile-detail="1"')) {
  console.log("Profile detail blocks already present — re-run with --force to replace.");
  if (!process.argv.includes("--force")) process.exit(0);
}

let count = 0;
html = html.replace(
  /(<div class="case-block repeater-row" data-repeater-row="executives">[\s\S]*?<h4>Executive (\d+)<\/h4>[\s\S]*?)(<div class="exec-headshot-section">)/g,
  (match, before, execNum, headshot) => {
    if (before.includes('data-exec-profile-detail="1"')) return match;
    count += 1;
    return before + buildExecProfileFieldsHtml(execNum) + headshot;
  }
);

fs.writeFileSync(htmlPath, html, "utf8");
console.log(`Embedded profile detail blocks for ${count} executive row(s) in ${htmlPath}`);
