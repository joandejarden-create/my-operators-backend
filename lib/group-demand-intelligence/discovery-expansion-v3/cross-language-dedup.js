/**
 * Cross-language / cross-scout deduplication for discovery expansion V3.
 */

const CONGRESS_ALIASES = [
  [/congr[eè]s/i, "congress"],
  [/kongress/i, "congress"],
  [/congreso/i, "congress"],
  [/convegno/i, "congress"],
  [/assemblée\s+annuelle/i, "annual_meeting"],
  [/asamblea\s+anual/i, "annual_meeting"],
  [/jahresversammlung/i, "annual_meeting"],
  [/hébergement|alojamiento|aloxamento|unterkunft|alloggio/i, "lodging"],
  [/appel\s+d['']offres|licitación|ausschreibung|appalto/i, "procurement"],
];

export function normalizeDiscoveryKey(text = "") {
  let s = String(text || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  for (const [re, token] of CONGRESS_ALIASES) {
    s = s.replace(re, token);
  }
  return s;
}

export function opportunityDedupFingerprint(opp = {}) {
  const org = normalizeDiscoveryKey(opp.organizationName || opp.company || "");
  const title = normalizeDiscoveryKey(opp.title || opp.opportunityName || "");
  const city = normalizeDiscoveryKey(opp.city || opp.eventLocation || opp.destinationStatus || "");
  const date = String(opp.eventStartDate || opp.eventYear || "").slice(0, 10);
  const domain = (() => {
    try {
      const u = opp.officialSource || opp.discoverySource || "";
      return u ? new URL(u).hostname.replace(/^www\./, "") : "";
    } catch {
      return "";
    }
  })();
  return [org, title.slice(0, 80), city, date, domain].filter(Boolean).join("|");
}

/**
 * Deduplicate candidate list; preserve original-language evidence on keeper.
 * @returns {{ unique, removed, aliasPairs }}
 */
export function dedupeCrossLanguageCandidates(candidates = []) {
  const byFp = new Map();
  const removed = [];
  const aliasPairs = [];
  for (const c of candidates) {
    const fp = opportunityDedupFingerprint(c);
    if (!fp) {
      removed.push({ candidate: c, reason: "empty_fingerprint" });
      continue;
    }
    if (!byFp.has(fp)) {
      byFp.set(fp, {
        ...c,
        originalLanguageEvidence: [
          {
            language: c.queryLanguage || c.sourceLanguage || null,
            title: c.title,
            source: c.officialSource || c.discoverySource || null,
          },
        ],
      });
      continue;
    }
    const keeper = byFp.get(fp);
    keeper.originalLanguageEvidence = keeper.originalLanguageEvidence || [];
    keeper.originalLanguageEvidence.push({
      language: c.queryLanguage || c.sourceLanguage || null,
      title: c.title,
      source: c.officialSource || c.discoverySource || null,
    });
    // Prefer candidate with lodging / WHO / better source
    const score = (x) =>
      (x.lodgingEvidence ? 2 : 0) +
      (x.primaryContact ? 2 : 0) +
      (x.officialSource ? 1 : 0) +
      (x.eventStartDate ? 1 : 0);
    if (score(c) > score(keeper)) {
      aliasPairs.push({ kept: c.id || c.title, dropped: keeper.id || keeper.title, fp });
      byFp.set(fp, {
        ...c,
        originalLanguageEvidence: keeper.originalLanguageEvidence,
      });
      removed.push({ candidate: keeper, reason: "cross_language_duplicate_weaker" });
    } else {
      aliasPairs.push({ kept: keeper.id || keeper.title, dropped: c.id || c.title, fp });
      removed.push({ candidate: c, reason: "cross_language_duplicate" });
    }
  }
  return {
    unique: [...byFp.values()],
    removed,
    aliasPairs,
    crossLanguageAliases: CONGRESS_ALIASES.map(([re, token]) => ({
      pattern: String(re),
      token,
    })),
  };
}
