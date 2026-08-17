import fs from "node:fs";

const schema = JSON.parse(
  fs.readFileSync(new URL("./wh-schema-only.json", import.meta.url), "utf8")
);

const prompt = `Follow the attached research brief (dealality-mexico-owner-intel-test1-brief.txt) EXACTLY.

This is Dealality's first $5 Mexico Owner Intelligence discovery test using the included free run.

Hard requirements from the brief (do not dilute):
- FIND THE OWNER WHILE THE FUTURE OF THE HOTEL IS STILL BEING DECIDED.
- Mexico only. Target 8–12 high-quality Class 1/Class 2 rows; prefer 5–7 excellent over stretching.
- Discovery quality beats schema completeness.
- Complete TIER 1 fields substantially before any TIER 2 enrichment.
- DO NOT spend meaningful time on Tier 2 for one company if that prevents finding/validating more strong Class 1/2 opportunities.
- Prioritize government triangulation: OWNER/SPV + SITE/ASSET + PUBLIC FILING/PLANNING SIGNAL + HOSPITALITY INTENT + NO PUBLIC FINAL BRAND/OPERATOR COMMITMENT.
- Run the too-late check on every Class 1. Never invent ownership %, emails, phones, or completed decisions.
- Fill the provided dataset schema; leave Tier 2 sparse when needed.

The attached brief is the full research protocol. Obey it.`;

const args = {
  prompt,
  schema,
  budget: 5,
  title:
    "Dealality Mexico Owner Intelligence — Early Pre-Decision Opportunities (Test 1)",
  use_free_run_when_available: true,
  file_ids: ["634de8bd-768f-43ec-89ad-84d5ce11a53a"],
};

fs.writeFileSync(
  new URL("./wh-mcp-call-compact.json", import.meta.url),
  JSON.stringify(args)
);

console.log(
  JSON.stringify({
    prompt_len: prompt.length,
    attrs: schema.attributes.length,
    total: Buffer.byteLength(JSON.stringify(args)),
  })
);
