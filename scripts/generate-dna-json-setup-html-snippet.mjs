#!/usr/bin/env node
/** Prints HTML snippet blocks for third-party-operator-setup-new-two.html (stdout). */
import { DNA_EXPLORER_JSON_FIELD_SPECS } from "../lib/operator-dna-explorer-json-fields.js";

function blockForSpecs(title, hint, specs) {
  const lines = [
    `    <h3 class="project-fit-subheader">${title}</h3>`,
    `    <p class="subsection-hint">${hint}</p>`,
  ];
  for (const s of specs) {
    const ph = s.jsonShape.replace(/"/g, "&quot;");
    lines.push(`    <div class="field-wrap">`);
    lines.push(
      `    <label class="form-label label-spacing" for="${s.formKey}">${s.label} (JSON)</label>`
    );
    lines.push(
      `    <textarea class="form-textarea tall explorer-story-field" name="${s.formKey}" id="${s.formKey}" rows="6" data-explorer-payload="1" placeholder="${ph}"></textarea>`
    );
    lines.push(`    </div>`);
  }
  return lines.join("\n");
}

const groups = {
  op: DNA_EXPLORER_JSON_FIELD_SPECS.filter((s) => s.formKey.startsWith("op_")),
  brand: DNA_EXPLORER_JSON_FIELD_SPECS.filter((s) => s.formKey.startsWith("brand_") && s.formKey.endsWith("_json")),
  ov: DNA_EXPLORER_JSON_FIELD_SPECS.filter((s) => s.formKey.startsWith("ov_") && s.formKey.endsWith("_json")),
  mkt: DNA_EXPLORER_JSON_FIELD_SPECS.filter((s) => s.formKey.startsWith("mkt_")),
  bf: DNA_EXPLORER_JSON_FIELD_SPECS.filter((s) => s.formKey.startsWith("bf_") && s.formKey.endsWith("_json")),
};

console.log("===OP===");
console.log(
  blockForSpecs(
    "Explorer DNA — Operating Platform pillars (JSON)",
    "Optional. Up to six custom capability tiles per pillar on the Operator Explorer DNA profile. Legacy multiline fields above still work as fallbacks until these are filled.",
    groups.op
  )
);
console.log("===BRAND===");
console.log(
  blockForSpecs(
    "Explorer DNA — Brand &amp; Relationships subsections (JSON)",
    "Optional. Powers portfolio mix, relationship depth, execution, and governance cards on the DNA Brand tab.",
    groups.brand
  )
);
console.log("===MKT===");
console.log(
  blockForSpecs(
    "Explorer DNA — Markets subsections (JSON)",
    "Optional. Powers regional expertise and market fit signal rows on the DNA Markets tab.",
    groups.mkt
  )
);
console.log("===OV===");
console.log(
  blockForSpecs(
    "Explorer DNA — Owner Engagement subsections (JSON)",
    "Optional. Powers Owner Engagement &amp; Reporting subsections on the DNA profile (replaces demo defaults when populated).",
    groups.ov
  )
);
console.log("===BF===");
console.log(
  blockForSpecs(
    "Explorer DNA — Project Fit subsections (JSON)",
    "Optional. Powers Project Fit &amp; Deal Profile subsections on the DNA profile.",
    groups.bf
  )
);
