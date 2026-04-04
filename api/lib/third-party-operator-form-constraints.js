import fs from "fs";
import { load } from "cheerio";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/** Labels written to the hidden `regions` field by the intake form geo grid (must match third-party-operator-intake.html). */
export const REGIONS_HIDDEN_ALLOWED = new Set([
  "North America (NA)",
  "Caribbean & Latin America (CALA)",
  "Europe (EU)",
  "Middle East & Africa (MEA)",
  "Asia Pacific (APAC)",
]);

/**
 * Parse third-party-operator-intake.html for constrained fields:
 * - select / multiselect: non-empty option values
 * - checkbox groups: distinct name + value pairs
 * @returns {{ fields: Map<string, { type: 'select' | 'multiselect' | 'checkbox', allowed: Set<string> }> }}
 */
export function parseThirdPartyOperatorFormConstraints(html) {
  const $ = load(html);
  /** @type {Map<string, { type: 'select' | 'multiselect' | 'checkbox', allowed: Set<string> }>} */
  const fields = new Map();

  $("select[name]").each((_, el) => {
    const name = $(el).attr("name");
    if (!name) return;
    const multiple = $(el).attr("multiple") != null;
    const allowed = new Set();
    $(el)
      .find("option")
      .each((__, opt) => {
        const v = $(opt).attr("value");
        if (v != null && String(v).trim() !== "") allowed.add(String(v).trim());
      });
    if (allowed.size === 0) return;

    const prev = fields.get(name);
    const type = multiple ? "multiselect" : "select";
    if (!prev) {
      fields.set(name, { type, allowed });
      return;
    }
    if (prev.type !== type) return;
    for (const x of allowed) prev.allowed.add(x);
  });

  $('input[type="checkbox"][name][value]').each((_, el) => {
    const name = $(el).attr("name");
    const value = $(el).attr("value");
    if (!name || value == null || String(value).trim() === "") return;
    const v = String(value).trim();
    const prev = fields.get(name);
    if (!prev) {
      fields.set(name, { type: "checkbox", allowed: new Set([v]) });
      return;
    }
    if (prev.type !== "checkbox") return;
    prev.allowed.add(v);
  });

  return { fields };
}

export function loadDefaultThirdPartyOperatorFormConstraints() {
  const htmlPath = path.join(__dirname, "..", "..", "public", "third-party-operator-intake.html");
  const html = fs.readFileSync(htmlPath, "utf8");
  return parseThirdPartyOperatorFormConstraints(html);
}
