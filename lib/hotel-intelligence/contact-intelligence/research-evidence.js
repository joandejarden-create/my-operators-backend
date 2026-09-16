/** Pure, offline checks over captured passages. Not a truth/accuracy verifier.
 * URL-only references, provider claims and caller status flags are insufficient.
 * Keep original captures outside this module; do not turn retrieval dates into verification dates.
 */
export const normalizeEvidenceText = (v) => String(v || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
const esc = (v) => String(v).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
export function mentions(text, name) {
  const n = normalizeEvidenceText(name);
  return n.length >= 3 && (` ${normalizeEvidenceText(text)} `).includes(` ${n} `);
}
export function evidenceRecords(refs = []) {
  return (Array.isArray(refs) ? refs : [refs]).filter((r) => {
    if (!r || typeof r !== 'object') return false;
    try { if (!['http:', 'https:'].includes(new URL(r.source_url || r.url).protocol)) return false; } catch { return false; }
    const text = r.excerpt || r.passage;
    if (typeof text !== 'string' || text.trim().length < 20) return false;
    if (/PROVIDER|DIRECTORY|SNIPPET|HYPOTHESIS/i.test(r.source_type || r.source_class || '')) return false;
    if (/SURFE|CONTACTOUT|APOLLO|ZOOMINFO/i.test(new URL(r.source_url || r.url).hostname)) return false;
    if (r.source_text != null && !String(r.source_text).includes(text)) return false;
    return true;
  });
}
export function ownershipPassageSupport(text, hotel, owner) {
  if (!mentions(text, hotel) || !mentions(text, owner)) return false;
  const s = normalizeEvidenceText(text), h = esc(normalizeEvidenceText(hotel)), o = esc(normalizeEvidenceText(owner));
  // A candidate mention elsewhere on the page is not a binding ownership statement.
  const passive = new RegExp(`${h}.{0,100}\\b(?:owned by|acquired by|purchased by|propiedad de|adquirido por|adquirida por|pertence a) ${o}\\b`);
  const active = new RegExp(`${o} (?:has |have |today |recently |today announced it has |announced it has )?(?:acquired|purchased|owns|adquiriu|adquirio|compro) (?:the |el |o |a |hotel |resort ){0,2}${h}\\b`);
  const negated = /\b(not|never|no longer|formerly|previously|proposed|plans to|agreed to|will acquire|subject to|nao|anteriormente|pretende)\b/i.test(s);
  return !negated && (passive.test(s) || active.test(s));
}
export function supportedOwnershipEvidence(refs, hotel, owner) {
  return evidenceRecords(refs).filter((r) => ownershipPassageSupport(r.excerpt || r.passage, hotel, owner));
}
export function supportedAffiliationEvidence(refs, person, owner, title) {
  return evidenceRecords(refs).filter((r) => {
    const text = r.excerpt || r.passage;
    return mentions(text, person) && mentions(text, owner) && mentions(text, title) &&
      !/\b(former|formerly|retired|deceased|left the company|ex director|fallecid|falecido)\b/i.test(text);
  });
}
export function domainEvidenceSupported(refs, domain, owner) {
  const host = (v) => { try { return new URL(v.includes('://') ? v : `https://${v}`).hostname.replace(/^www\./,'').toLowerCase(); } catch { return ''; } };
  const target = host(domain || '');
  return Boolean(target) && evidenceRecords(refs).some((r) =>
    host(r.source_url || r.url) === target && mentions(r.excerpt || r.passage, owner));
}
export function classifyContactPurpose(email = '') {
  const local = String(email).split('@')[0];
  if (/media|press|prensa|imprensa|presse/i.test(local)) return 'MEDIA_PURPOSE';
  if (/privacy|derechosarco|sostenibilidad|ouvidoria/i.test(local)) return 'PURPOSE_RESTRICTED';
  return 'UNDETERMINED';
}
