/**
 * Questions Missing intelligence — prompt-family rollup + peer-present/subject-missing.
 * Presence-derived only. No win/loss language.
 */

export const QUESTIONS_MISSING_INTEL_VERSION =
  "ai_visibility_questions_missing_intelligence_v1";

function familyOf(obs) {
  return (
    obs.promptFamily ||
    obs.intentTerritory ||
    obs.intent ||
    obs.payload?.promptFamily ||
    "Unspecified"
  );
}

function presentIds(obs) {
  const fromField = obs.presentEntityIds || [];
  if (fromField.length) return new Set(fromField.filter(Boolean));
  const mentions = obs.mentions || obs.payload?.mentions || [];
  return new Set(
    mentions
      .map((m) => m.entityId || m.resolvedEntityId || m.canonicalEntityId)
      .filter(Boolean)
  );
}

function nameFromObservationMentions(obs, entityId) {
  const mentions = obs.mentions || obs.payload?.mentions || [];
  for (const m of mentions) {
    const mid = m.entityId || m.resolvedEntityId || m.canonicalEntityId;
    if (mid !== entityId) continue;
    const name =
      m.entityName || m.name || m.canonicalEntityName || m.matchedText || null;
    if (name && String(name).trim()) return String(name).trim();
  }
  return null;
}

/**
 * Aggregate Questions Missing by prompt family for a subject brand.
 */
export function buildPromptFamilyMissingRollup(observations = [], subjectBrandId) {
  const relevant = (observations || []).filter((o) => o && o.success !== false);
  const byFamily = new Map();

  for (const obs of relevant) {
    const family = familyOf(obs);
    if (!byFamily.has(family)) {
      byFamily.set(family, {
        promptFamily: family,
        monitored: 0,
        withPresence: 0,
        missing: 0,
        promptIds: new Set(),
      });
    }
    const row = byFamily.get(family);
    const promptId = obs.promptId || obs.responseId || `${family}:${row.monitored}`;
    // Count unique prompts per family when possible
    if (row.promptIds.has(promptId) && obs.promptId) continue;
    if (obs.promptId) row.promptIds.add(promptId);
    row.monitored += 1;
    const present = presentIds(obs).has(subjectBrandId);
    if (present) row.withPresence += 1;
    else row.missing += 1;
  }

  const families = [...byFamily.values()]
    .map((r) => {
      const monitored = r.monitored;
      const missingRate = monitored > 0 ? r.missing / monitored : null;
      return {
        promptFamily: r.promptFamily,
        MONITORED_QUESTIONS: monitored,
        QUESTIONS_WITH_PRESENCE: r.withPresence,
        QUESTIONS_MISSING: r.missing,
        MISSING_RATE: missingRate,
        missingRateDisplay:
          missingRate == null
            ? "—"
            : `${(Math.round(missingRate * 1000) / 10).toFixed(1)}%`,
      };
    })
    .sort((a, b) => (b.QUESTIONS_MISSING || 0) - (a.QUESTIONS_MISSING || 0));

  return {
    version: QUESTIONS_MISSING_INTEL_VERSION,
    subjectBrandId,
    families,
    TOTAL_MISSING: families.reduce((s, f) => s + (f.QUESTIONS_MISSING || 0), 0),
    TOTAL_MONITORED: families.reduce((s, f) => s + (f.MONITORED_QUESTIONS || 0), 0),
    COMPOSITE_GAP_SCORE: false,
  };
}

/**
 * Subject ABSENT + ≥1 governed peer PRESENT in same response/prompt observation.
 */
export function buildPeerPresentSubjectMissing(observations = [], opts = {}) {
  const subjectBrandId = opts.subjectBrandId;
  const peerIds = new Set((opts.peerEntityIds || []).filter(Boolean));
  const peerNames = opts.peerNamesById || {};
  if (!subjectBrandId || !peerIds.size) {
    return {
      version: QUESTIONS_MISSING_INTEL_VERSION,
      rows: [],
      PEER_PRESENT_SUBJECT_MISSING_N: 0,
      READY: false,
    };
  }

  const rows = [];
  for (const obs of observations || []) {
    if (!obs || obs.success === false) continue;
    const present = presentIds(obs);
    if (present.has(subjectBrandId)) continue;
    const peersPresent = [...present].filter((id) => peerIds.has(id));
    if (!peersPresent.length) continue;
    rows.push({
      QUESTION: obs.promptText || obs.question || obs.promptId || "—",
      promptId: obs.promptId || null,
      PROMPT_FAMILY: familyOf(obs),
      PROVIDER: obs.provider || null,
      REGION: obs.geographyKey || obs.commercialRegion || obs.geography || null,
      LANGUAGE: obs.language || null,
      PEERS_PRESENT: peersPresent.map((id) => ({
        entityId: id,
        entityName:
          peerNames[id] || nameFromObservationMentions(obs, id) || null,
      })),
      SUBJECT_PRESENT: false,
      evidenceId: obs.evidenceId || null,
      responseId: obs.responseId || null,
    });
  }

  return {
    version: QUESTIONS_MISSING_INTEL_VERSION,
    subjectBrandId,
    rows,
    PEER_PRESENT_SUBJECT_MISSING_N: rows.length,
    READY: true,
    CLIENT_COPY:
      "Brand was not observed in these monitored owner questions while one or more peers were observed.",
    FORBIDDEN_LANGUAGE: ["lost", "won", "failure", "recommended"],
  };
}

/**
 * Group missing rows by prompt family / provider / region / language.
 */
export function groupQuestionsMissingWatchlist(rows = [], dimension = "promptFamily") {
  const keyFn = {
    promptFamily: (r) => r.PROMPT_FAMILY || r.promptFamily || "Unspecified",
    provider: (r) => r.PROVIDER || r.provider || "—",
    region: (r) => r.REGION || r.region || "—",
    language: (r) => r.LANGUAGE || r.language || "—",
  }[dimension] || ((r) => r.PROMPT_FAMILY || "Unspecified");

  const map = new Map();
  for (const r of rows) {
    const k = keyFn(r);
    if (!map.has(k)) map.set(k, []);
    map.get(k).push(r);
  }
  return [...map.entries()]
    .map(([key, list]) => ({ key, count: list.length, rows: list }))
    .sort((a, b) => b.count - a.count);
}
