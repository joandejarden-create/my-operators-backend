/** Bounded search/reading helpers for the existing Context.dev handoff, not a new crawler.
 * Search hits are leads. Candidate extraction never proves ownership or authorizes enrichment.
 */
import { createHash } from 'node:crypto';
import { mentions } from './research-evidence.js';
export function ownershipQueries(hotel = {}) {
  const full = String(hotel.hotel_name || '').replace(/["\n\r]/g, ' ').trim();
  const core = full.replace(/,?\s+(?:an? |by )?(?:autograph collection|tapestry collection|lxr hotels|all[- ]inclusive|adults only).*$/i, '').trim() || full;
  const location = [hotel.city, hotel.country].filter(Boolean).join(' ');
  const local = hotel.language === 'pt' ? '(proprietário OR adquiriu OR aquisição OR vendido)' : hotel.language === 'es' ? '(propietario OR adquirió OR adquisición OR comprado)' : '(owner OR acquired OR acquisition OR purchased)';
  return [...new Set([
    `"${core}" ${location} ${local}`,
    `"${core}" (acquired OR acquisition OR purchased OR investor OR sale)`,
    `"${full}" (ownership OR propietario OR proprietario OR annual report)`,
  ])];
}
export function ownershipSearchResults(data) {
  // Known named envelopes only; unexpected shape is an execution failure, not zero evidence.
  const rows = Array.isArray(data?.results) ? data.results : Array.isArray(data?.data?.results) ? data.data.results : null;
  return rows === null ? null : rows.map((r) => ({ title:r.title || '', url:r.url || r.link, snippet:r.description || r.snippet || '' })).filter((r) => /^https?:\/\//.test(r.url || ''));
}
export function rankOwnershipSources(rows, hotel = {}) {
  const seen = new Set();
  return rows.filter((r) => { if (seen.has(r.url)) return false; seen.add(r.url); return true; }).map((r) => {
    const blob = `${r.title} ${r.snippet} ${r.url}`;
    let score = /acquir|acquis|purchas|owned|propietari|adquiri|compra|vendid/i.test(blob) ? 5 : 0;
    if (/press|news-release|investor|annual|filing|relatorio|\.pdf(?:\?|$)/i.test(blob)) score += 3;
    if (mentions(blob, hotel.hotel_name)) score += 2;
    if (/booking\.com|tripadvisor|whoistheownerof|whoownsthebrand|careers|jobs/i.test(r.url)) score -= 20;
    return {...r, score};
  }).filter((r) => r.score >= 0).sort((a,b) => b.score-a.score);
}
export function ownershipDocumentPassages(text, hotel = {}) {
  const raw = String(text || '');
  const core = String(hotel.hotel_name || '').split(/,| an Autograph| by | lxr hotels/i)[0].trim();
  const matches = [];
  for (const hit of raw.matchAll(/owned|owner|acquir|acquis|purchas|propiedad|propietari|adquiri|compra|portfolio/gi)) {
    const start = Math.max(0, hit.index-350), end = Math.min(raw.length, hit.index+900);
    const excerpt = raw.slice(start,end);
    if (mentions(excerpt, core) && !matches.some((p) => Math.abs(p.start-start)<100)) matches.push({start,end,excerpt});
  }
  return { sha256:createHash('sha256').update(raw).digest('hex'), characters:raw.length, passages:matches.slice(0,8) };
}
export function ownershipCandidates(document) {
  const out = [];
  // Capitalized names with bounded tokens, distinct from nearby prose. Leads only.
  const name = "([A-ZÀ-Ý][\\p{L}\\d&'’-]+(?:[ \\t]+(?:[A-ZÀ-Ý][\\p{L}\\d&'’-]+|&)){0,5})";
  for (const p of document.passages || []) {
    for (const re of [new RegExp('(?:owned by|acquired by|purchased by|propiedad de|adquirido por|adquirida por)\\s+'+name,'gu'),new RegExp(name+'\\s+(?:has |today )?(?:acquired|purchased|adquiriu|adquirió|compró)\\b','gu')]) {
      for (const m of p.excerpt.matchAll(re)) out.push({name:m[1].trim(),excerpt:p.excerpt,classification:'OWNER_CANDIDATE',currentness:'UNRESOLVED'});
    }
  }
  return out;
}
