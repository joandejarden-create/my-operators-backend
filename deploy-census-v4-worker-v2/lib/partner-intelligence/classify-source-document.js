/**
 * Classify Partner Intelligence sources for merge priority + parsing strategy.
 */

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

/**
 * @param {{ sourceTitle?: string, sourceUrl?: string, localFilePath?: string, sourceType?: string }} source
 * @returns {{ role: string, parseAsDeck: boolean, language: 'en'|'es'|'unknown' }}
 */
export function classifySourceDocument(source) {
  const blob = [source.localFilePath, source.sourceTitle, source.sourceUrl]
    .map(nz)
    .join(" ")
    .toLowerCase();

  if (/\bfdd\b|franchise disclosure/i.test(blob)) {
    return { role: "fdd", parseAsDeck: false, language: "en" };
  }
  if (/development brochure|brand summary|americas kimpton|emeaa kimpton/i.test(blob)) {
    return { role: "development_brochure", parseAsDeck: false, language: "en" };
  }
  if (/kimpton hotels.*\.html|hotel-brands\/kimpton/i.test(blob)) {
    return { role: "brand_web", parseAsDeck: false, language: "en" };
  }
  if (/regional experience|experiencia regional/i.test(blob)) {
    return {
      role: "regional_deck",
      parseAsDeck: true,
      language: /experiencia|junio 2026\.pdf$/i.test(blob) && !/june/i.test(blob) ? "es" : "en",
    };
  }
  if (/overview.*english|english.*overview|overview - english/i.test(blob)) {
    return { role: "overview_en", parseAsDeck: false, language: "en" };
  }
  if (/overview.*spanish|spanish.*overview|español|spanish - case/i.test(blob)) {
    return { role: "overview_es", parseAsDeck: false, language: "es" };
  }
  if (source.sourceUrl && /^https?:\/\//i.test(source.sourceUrl)) {
    return { role: "public_web", parseAsDeck: false, language: "en" };
  }
  if (source.localFilePath) {
    return { role: "overview_en", parseAsDeck: false, language: "unknown" };
  }
  return { role: "any", parseAsDeck: false, language: "unknown" };
}
