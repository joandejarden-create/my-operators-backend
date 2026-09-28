/**
 * Extract exhibitor / sponsor / participant entities from HTML directories.
 * Generic — no hotel hardcodes.
 */

import { ENTITY_TYPE, PARTICIPATION_ROLE, SOURCE_TYPE } from "./v2-constants.js";
import { normalizeOrganizationName } from "./entity-normalize.js";
import { isPlausibleOrganizationName } from "./quality-gate.js";

function clean(s) {
  return String(s || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&nbsp;/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Pull company-like names from HTML tables and list structures.
 */
export function extractEntitiesFromHtmlDirectory(html = "", meta = {}) {
  const sourceType = meta.sourceType || SOURCE_TYPE.EXHIBITOR_DIRECTORY;
  const sourceURL = meta.sourceURL || null;
  const demandGeneratorId = meta.demandGeneratorId || null;
  const demandGeneratorName = meta.demandGeneratorName || null;
  const year = meta.year || null;
  const family = meta.family || null;

  const entities = [];
  const seen = new Set();

  const push = (name, extra = {}) => {
    const { displayName, normalizeKey } = normalizeOrganizationName(name);
    if (!isPlausibleOrganizationName(displayName)) return;
    if (seen.has(normalizeKey)) return;
    seen.add(normalizeKey);
    const role =
      sourceType === SOURCE_TYPE.SPONSOR_DIRECTORY
        ? PARTICIPATION_ROLE.SPONSOR
        : sourceType === SOURCE_TYPE.PARTICIPANT_LIST
          ? PARTICIPATION_ROLE.PARTICIPANT
          : PARTICIPATION_ROLE.EXHIBITOR;
    entities.push({
      entityName: displayName,
      normalizeKey,
      entityType:
        role === PARTICIPATION_ROLE.SPONSOR
          ? ENTITY_TYPE.SPONSOR
          : ENTITY_TYPE.COMPANY,
      sourceType,
      sourceURL,
      sourceAuthority: meta.sourceAuthority || "official_or_event",
      demandGeneratorId,
      demandGeneratorName,
      participationRole: role,
      boothNumber: extra.booth || null,
      category: extra.category || null,
      country: extra.country || null,
      sponsorLevel: extra.sponsorLevel || null,
      companyUrl: extra.companyUrl || null,
      futureTiming: Boolean(year) || meta.futureTiming === true,
      year,
      eventCycleId: meta.eventCycleId || (year ? String(year) : null),
      evidenceSnippet: extra.snippet || displayName,
      confidence: extra.confidence ?? 0.55,
      family,
      participationType: role,
    });
  };

  // Table rows: <td>Company</td>
  const tableCellRe = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
  let m;
  const cells = [];
  while ((m = tableCellRe.exec(html)) && cells.length < 800) {
    const t = clean(m[1]);
    if (t && t.length >= 3 && t.length <= 100) cells.push(t);
  }
  for (const c of cells) {
    // Skip headers / numbers-only
    if (/^(booth|company|exhibitor|sponsor|name|country|category|hall|#)$/i.test(c)) continue;
    if (/^\d{1,4}[A-Z]?$/i.test(c)) continue;
    push(c, { confidence: 0.6, snippet: c });
  }

  // List items / card titles
  const liRe = /<(?:li|h[2-4]|div)[^>]*class="[^"]*(?:exhibitor|sponsor|company|participant|booth)[^"]*"[^>]*>([\s\S]*?)<\/(?:li|h[2-4]|div)>/gi;
  while ((m = liRe.exec(html)) && entities.length < 400) {
    push(clean(m[1]), { confidence: 0.65 });
  }

  // Links that look like exhibitor profile pages
  const linkRe =
    /<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/gi;
  while ((m = linkRe.exec(html)) && entities.length < 500) {
    const href = m[1];
    const text = clean(m[2]);
    if (!text || text.length < 3) continue;
    if (/exhibitor|booth|sponsor|company|directory/i.test(href) || /exhibitor|booth/i.test(text)) {
      if (/^(view|more|details|website|visit|click|here|login|register)$/i.test(text)) continue;
      push(text, { companyUrl: href.startsWith("http") ? href : null, confidence: 0.58 });
    }
  }

  // Embedded JSON / JSON-LD exhibitor arrays
  const jsonBlocks = html.match(/<script[^>]*type=["']application\/(?:ld\+)?json["'][^>]*>([\s\S]*?)<\/script>/gi) || [];
  for (const block of jsonBlocks) {
    const inner = block.replace(/^[\s\S]*?>/, "").replace(/<\/script>$/i, "");
    try {
      const data = JSON.parse(inner);
      walkJsonForOrgs(data, (name, extra) => push(name, { ...extra, confidence: 0.7 }));
    } catch {
      /* ignore */
    }
  }

  // window.__EXHIBITORS__ / similar inline JSON arrays
  const inline = html.match(
    /(?:exhibitors?|sponsors?|companies)\s*[:=]\s*(\[[\s\S]{20,80000}?\])/i
  );
  if (inline) {
    try {
      const arr = JSON.parse(inline[1]);
      if (Array.isArray(arr)) {
        for (const row of arr.slice(0, 300)) {
          const name = row.name || row.company || row.title || row.exhibitorName || row.Company;
          if (name) {
            push(String(name), {
              booth: row.booth || row.boothNumber || null,
              country: row.country || row.Country || null,
              category: row.category || row.Category || null,
              confidence: 0.75,
            });
          }
        }
      }
    } catch {
      /* ignore */
    }
  }

  return entities;
}

function walkJsonForOrgs(node, push, depth = 0) {
  if (depth > 8 || node == null) return;
  if (Array.isArray(node)) {
    for (const x of node.slice(0, 400)) walkJsonForOrgs(x, push, depth + 1);
    return;
  }
  if (typeof node === "object") {
    const name = node.name || node.legalName || node.company || node.organization;
    const type = String(node["@type"] || node.type || "");
    if (name && (/Organization|Corporation|Company|Exhibitor|Sponsor/i.test(type) || node.booth || node.boothNumber)) {
      push(String(name), {
        booth: node.booth || node.boothNumber || null,
        companyUrl: node.url || node.website || null,
      });
    }
    for (const v of Object.values(node)) walkJsonForOrgs(v, push, depth + 1);
  }
}
