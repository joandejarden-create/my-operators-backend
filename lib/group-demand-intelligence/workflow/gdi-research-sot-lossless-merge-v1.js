/**
 * GDI Research SoT lossless semantic merge V1
 *
 * Evidence-aware merge: live fat Airtable payload is the base.
 * Certified Full E2E overlay may enrich certification-owned fields and
 * verified contact channels — never wholesale-replace researchPayloadJson.
 *
 * Fail closed on information loss, unsafe downgrades, or unresolved conflicts.
 */

import {
  GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
  SOT_CERTIFICATION_OWNED_KEYS,
  SOT_TELEMETRY_DROP_KEYS,
  SOT_DERIVED_PRESENTATION_KEYS,
  REVENUE_STREAM_STRENGTH,
  classifySotPayloadKey,
} from "./gdi-research-sot-field-ownership-v1.js";

export const GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1 =
  "gdi-research-sot-lossless-merge-v1";

export const RESEARCH_PAYLOAD_MAX_CHARS_DEFAULT = 95_000;

function text(v) {
  if (v == null) return "";
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
    return String(v).replace(/\s+/g, " ").trim();
  }
  return "";
}

function isPlainObject(v) {
  return Boolean(v) && typeof v === "object" && !Array.isArray(v);
}

function parsePayload(raw) {
  if (raw == null || raw === "") return {};
  if (typeof raw === "object" && !Array.isArray(raw)) return { ...raw };
  if (typeof raw !== "string") return {};
  try {
    const p = JSON.parse(raw);
    return isPlainObject(p) ? p : {};
  } catch {
    return {};
  }
}

function stripTelemetry(payload) {
  const out = { ...payload };
  for (const k of SOT_TELEMETRY_DROP_KEYS) delete out[k];
  return out;
}

function contactIdentity(c) {
  if (!c || typeof c !== "object") return "";
  const name = text(c.name || c.fullName || c.contactName).toLowerCase();
  const email = text(c.email || c.workEmail).toLowerCase();
  if (name && email) return `n:${name}|e:${email}`;
  if (name) return `n:${name}`;
  if (email) return `e:${email}`;
  return "";
}

function contactRichness(c) {
  if (!c || typeof c !== "object") return 0;
  let score = 0;
  for (const k of [
    "email",
    "workEmail",
    "phone",
    "mobile",
    "linkedinUrl",
    "linkedInUrl",
    "role",
    "title",
    "authorityBasis",
    "contactGrade",
    "grade",
    "verificationStatus",
    "verificationAudit",
    "researchAudit",
    "contactResearchAudit",
    "whyThisContact",
    "phoneExtensions",
  ]) {
    const v = c[k];
    if (v == null || v === "") continue;
    if (Array.isArray(v)) score += v.length > 0 ? 2 : 0;
    else if (typeof v === "object") score += Object.keys(v).length > 0 ? 3 : 0;
    else score += 1;
  }
  return score;
}

function mergeContactObjects(live, proposed) {
  const base = { ...(live || {}) };
  const prop = proposed || {};
  // Overlay verified channels when proposed has values; never clear live.
  for (const k of [
    "email",
    "workEmail",
    "phone",
    "mobile",
    "linkedinUrl",
    "linkedInUrl",
    "role",
    "title",
    "authorityBasis",
    "name",
    "fullName",
  ]) {
    if (text(prop[k]) && !text(base[k])) base[k] = prop[k];
    else if (
      text(prop[k]) &&
      text(base[k]) &&
      text(prop[k]).toLowerCase() !== text(base[k]).toLowerCase()
    ) {
      // Keep live; note alternate under _proposedOverlay when different.
      if (!base._proposedChannelOverlay) base._proposedChannelOverlay = {};
      base._proposedChannelOverlay[k] = prop[k];
    }
  }
  // Preserve all audit / grade / history keys from live; add missing from prop.
  for (const [k, v] of Object.entries(prop)) {
    if (k.startsWith("_")) continue;
    if (base[k] == null || base[k] === "") base[k] = v;
  }
  return base;
}

function mergeContactLists(liveList, proposedList) {
  const live = Array.isArray(liveList) ? liveList.filter(isPlainObject) : [];
  const prop = Array.isArray(proposedList)
    ? proposedList.filter(isPlainObject)
    : [];
  const byKey = new Map();
  const order = [];
  for (const c of live) {
    const key = contactIdentity(c) || `anon:${order.length}`;
    if (!byKey.has(key)) {
      byKey.set(key, c);
      order.push(key);
    } else {
      byKey.set(key, mergeContactObjects(byKey.get(key), c));
    }
  }
  for (const c of prop) {
    const key = contactIdentity(c);
    if (!key) continue;
    if (byKey.has(key)) {
      byKey.set(key, mergeContactObjects(byKey.get(key), c));
    } else {
      byKey.set(key, c);
      order.push(key);
    }
  }
  return order.map((k) => byKey.get(k));
}

function evidenceClaimText(e) {
  if (!e || typeof e !== "object") return "";
  return text(
    e.claim || e.statement || e.fact || e.value || e.extractedText || e.text,
  );
}

function evidenceIdentity(e) {
  if (!e || typeof e !== "object") return "";
  const id = text(e.id);
  if (id) return `id:${id}`;
  const url = text(e.url || e.sourceUrl || e.href).toLowerCase();
  const field = text(e.field || e.supportsFact).toLowerCase();
  const claim = evidenceClaimText(e).toLowerCase().slice(0, 120);
  if (field && url) return `f:${field}|u:${url}`;
  if (url && claim) return `u:${url}|c:${claim}`;
  if (url) return `u:${url}`;
  if (claim) return `c:${claim}`;
  return "";
}

function evidenceRichness(e) {
  if (!e || typeof e !== "object") return 0;
  let score = 0;
  if (evidenceClaimText(e)) score += 5;
  if (text(e.claimKind || e.kind).toUpperCase() === "FACT") score += 3;
  if (e.confidence != null && e.confidence !== "") score += 2;
  if (text(e.url || e.sourceUrl)) score += 1;
  if (text(e.field || e.supportsFact)) score += 1;
  if (text(e.observedAt || e.date || e.asOf || e.accessDate)) score += 1;
  if (text(e.provenance || e.source || e.sourceTitle)) score += 1;
  score += Object.keys(e).length;
  return score;
}

function mergeEvidenceObjects(live, proposed) {
  // Prefer richer live evidence; never collapse FACT+confidence to URL stub.
  if (evidenceRichness(live) >= evidenceRichness(proposed)) {
    const out = { ...live };
    for (const [k, v] of Object.entries(proposed || {})) {
      if (out[k] == null || out[k] === "") out[k] = v;
    }
    return out;
  }
  const out = { ...proposed };
  for (const [k, v] of Object.entries(live || {})) {
    if (out[k] == null || out[k] === "") out[k] = v;
  }
  // Never drop live FACT claim/value when proposed is thinner.
  const liveClaim = evidenceClaimText(live);
  const outClaim = evidenceClaimText(out);
  if (liveClaim && liveClaim.length > outClaim.length) {
    if (live.claim != null) out.claim = live.claim;
    else if (live.statement != null) out.statement = live.statement;
    else if (live.value != null) out.value = live.value;
    if (live.claimKind) out.claimKind = live.claimKind;
    if (live.confidence != null) out.confidence = live.confidence;
    if (live.field != null && out.field == null) out.field = live.field;
  }
  return out;
}

function mergeEvidenceLists(liveList, proposedList) {
  const live = Array.isArray(liveList) ? liveList.filter(isPlainObject) : [];
  const prop = Array.isArray(proposedList)
    ? proposedList.filter(isPlainObject)
    : [];
  const byKey = new Map();
  const order = [];
  const urlToKey = new Map();
  for (const e of live) {
    const key = evidenceIdentity(e) || `anon:${order.length}`;
    if (!byKey.has(key)) {
      byKey.set(key, e);
      order.push(key);
      const url = text(e.url || e.sourceUrl).toLowerCase();
      const field = text(e.field || e.supportsFact).toLowerCase();
      if (url) {
        const uk = field ? `f:${field}|u:${url}` : `u:${url}`;
        if (!urlToKey.has(uk)) urlToKey.set(uk, key);
        if (!urlToKey.has(`u:${url}`)) urlToKey.set(`u:${url}`, key);
      }
    } else {
      byKey.set(key, mergeEvidenceObjects(byKey.get(key), e));
    }
  }
  for (const e of prop) {
    const key = evidenceIdentity(e);
    if (!key) continue;
    if (byKey.has(key)) {
      byKey.set(key, mergeEvidenceObjects(byKey.get(key), e));
      continue;
    }
    // Match thinner proposed stubs onto richer live rows by URL/field.
    const url = text(e.url || e.sourceUrl).toLowerCase();
    const field = text(e.field || e.supportsFact).toLowerCase();
    const urlKey = field && url ? `f:${field}|u:${url}` : url ? `u:${url}` : "";
    const existingKey = urlKey ? urlToKey.get(urlKey) || urlToKey.get(`u:${url}`) : null;
    if (existingKey && byKey.has(existingKey)) {
      byKey.set(existingKey, mergeEvidenceObjects(byKey.get(existingKey), e));
      continue;
    }
    // Only add proposed evidence that has substantive claim or URL.
    if (evidenceClaimText(e) || url) {
      byKey.set(key, e);
      order.push(key);
      if (urlKey) urlToKey.set(urlKey, key);
    }
  }
  return order.map((k) => byKey.get(k));
}

function meetingIdentity(m) {
  if (!m || typeof m !== "object") return "";
  const year = text(m.year || m.eventYear || m.date || "").slice(0, 4);
  const venue = text(m.venue || m.hotel || m.property || m.city).toLowerCase();
  const name = text(m.eventName || m.name || m.title).toLowerCase().slice(0, 80);
  if (year && venue) return `y:${year}|v:${venue}`;
  if (year && name) return `y:${year}|n:${name}`;
  if (venue && name) return `v:${venue}|n:${name}`;
  return name || venue || year || "";
}

function mergeMeetingLists(liveList, proposedList) {
  const live = Array.isArray(liveList) ? liveList.filter(isPlainObject) : [];
  const prop = Array.isArray(proposedList)
    ? proposedList.filter(isPlainObject)
    : [];
  // Live meetings are authoritative; proposed only fills empty slots / adds new.
  const byKey = new Map();
  const order = [];
  for (const m of live) {
    const key = meetingIdentity(m) || `anon:${order.length}`;
    if (!byKey.has(key)) {
      byKey.set(key, { ...m });
      order.push(key);
    }
  }
  for (const m of prop) {
    const key = meetingIdentity(m);
    if (!key) continue;
    if (byKey.has(key)) {
      const base = byKey.get(key);
      for (const [k, v] of Object.entries(m)) {
        if (base[k] == null || base[k] === "") base[k] = v;
      }
    } else {
      byKey.set(key, { ...m });
      order.push(key);
    }
  }
  return order.map((k) => byKey.get(k));
}

function competitorIdentity(c) {
  if (typeof c === "string") return text(c).toLowerCase();
  if (!c || typeof c !== "object") return "";
  return text(c.name || c.hotelName || c.property || c.id).toLowerCase();
}

function mergeCompetitorLists(liveList, proposedList) {
  const live = Array.isArray(liveList) ? liveList : [];
  const prop = Array.isArray(proposedList) ? proposedList : [];
  const out = [];
  const seen = new Set();
  for (const c of [...live, ...prop]) {
    const key = competitorIdentity(c);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(c);
  }
  return out;
}

function isExhibitorTemplateNarrative(s) {
  const t = text(s).toLowerCase();
  if (!t) return false;
  return (
    t.includes("exhibitors and attendees") ||
    (t.includes("create guestroom") && t.includes("networking demand")) ||
    t.includes("annual cycle creates guestroom")
  );
}

function narrativeStrength(s) {
  const t = text(s);
  if (!t) return 0;
  let score = Math.min(t.length, 500);
  if (isExhibitorTemplateNarrative(t)) score = Math.min(score, 40);
  // Prefer longer, more specific narratives.
  if (/\b(NICE|National Association of Insurance)\b/i.test(t)) score += 50;
  if (/\b(STR|competitor|headquarters|Bethesda)\b/i.test(t)) score += 20;
  return score;
}

/**
 * Detect unsafe classification / narrative conflicts.
 */
export function detectSotMergeConflicts(livePayload, proposedPayload) {
  const live = isPlainObject(livePayload) ? livePayload : {};
  const prop = isPlainObject(proposedPayload) ? proposedPayload : {};
  const conflicts = [];

  const liveRev = text(live.revenueStreamClass);
  const propRev = text(prop.revenueStreamClass);
  if (
    liveRev &&
    propRev &&
    liveRev !== propRev &&
    (REVENUE_STREAM_STRENGTH[propRev] || 0) <
      (REVENUE_STREAM_STRENGTH[liveRev] || 0)
  ) {
    conflicts.push({
      code: "REVENUE_STREAM_DOWNGRADE",
      path: "revenueStreamClass",
      live: liveRev,
      proposed: propRev,
      resolution: "KEEP_LIVE_REJECT_DOWNGRADE",
    });
  }

  const liveCdt = text(live.commercialDemandType).toUpperCase();
  const propCdt = text(prop.commercialDemandType).toUpperCase();
  if (liveCdt === "BOTH" && propCdt && propCdt !== "BOTH") {
    conflicts.push({
      code: "COMMERCIAL_DEMAND_TYPE_DOWNGRADE",
      path: "commercialDemandType",
      live: liveCdt,
      proposed: propCdt,
      resolution: "KEEP_LIVE_REJECT_DOWNGRADE",
    });
  }

  for (const path of ["whatIsHappening", "summaryWhat", "fitExplanation"]) {
    const lv = text(live[path]);
    const pv = text(prop[path]);
    if (!lv || !pv) continue;
    if (
      narrativeStrength(pv) < narrativeStrength(lv) * 0.5 ||
      (isExhibitorTemplateNarrative(pv) && !isExhibitorTemplateNarrative(lv))
    ) {
      conflicts.push({
        code: "NARRATIVE_UNSUPPORTED_REPLACEMENT",
        path,
        liveChars: lv.length,
        proposedChars: pv.length,
        proposedLooksTemplate: isExhibitorTemplateNarrative(pv),
        resolution: "KEEP_LIVE_FLAG_CONFLICT",
      });
    }
  }

  const liveEv = Array.isArray(live.evidence) ? live.evidence : [];
  const propEv = Array.isArray(prop.evidence) ? prop.evidence : [];
  const liveFactCount = liveEv.filter(
    (e) =>
      text(e?.claimKind || e?.kind).toUpperCase() === "FACT" ||
      evidenceClaimText(e).length > 20,
  ).length;
  const propFactCount = propEv.filter(
    (e) =>
      text(e?.claimKind || e?.kind).toUpperCase() === "FACT" ||
      evidenceClaimText(e).length > 20,
  ).length;
  if (liveFactCount >= 3 && propFactCount === 0 && propEv.length > 0) {
    conflicts.push({
      code: "EVIDENCE_COLLAPSED_TO_STUBS",
      path: "evidence",
      liveFactCount,
      proposedEvidenceCount: propEv.length,
      resolution: "KEEP_LIVE_EVIDENCE",
    });
  }

  const liveMeetings = Array.isArray(live.meetingHistory)
    ? live.meetingHistory.length
    : 0;
  if (
    liveMeetings > 0 &&
    Object.prototype.hasOwnProperty.call(prop, "meetingHistory") &&
    (!Array.isArray(prop.meetingHistory) || prop.meetingHistory.length === 0)
  ) {
    conflicts.push({
      code: "MEETING_HISTORY_ERASURE",
      path: "meetingHistory",
      liveCount: liveMeetings,
      resolution: "KEEP_LIVE",
    });
  }

  return conflicts;
}

/**
 * Extract only certification-owned + safe contact enrichment from proposed.
 */
export function extractCertificationOverlay(proposedPayload = {}) {
  const prop = isPlainObject(proposedPayload) ? proposedPayload : {};
  const overlay = {};
  for (const k of SOT_CERTIFICATION_OWNED_KEYS) {
    if (prop[k] !== undefined) overlay[k] = prop[k];
  }
  // Allow named contact enrichment (merge, not replace).
  for (const k of [
    "primaryContact",
    "secondaryContact",
    "contacts",
    "backupContacts",
    "namedStakeholders",
  ]) {
    if (prop[k] !== undefined) overlay[k] = prop[k];
  }
  // Allow additive evidence / sources (merged by identity).
  if (Array.isArray(prop.evidence)) overlay.evidence = prop.evidence;
  if (Array.isArray(prop.sources)) overlay.sources = prop.sources;
  // Fill-empty only for a few identity fields when live blank.
  for (const k of [
    "organizationName",
    "eventStartDate",
    "eventEndDate",
    "eventDateDisplay",
    "segment",
    "priority",
  ]) {
    if (text(prop[k])) overlay[`__fillEmpty__${k}`] = prop[k];
  }
  return overlay;
}

function collectPathDiff(before, after, prefix = "", acc = { added: [], updated: [], removed: [], preserved: [] }) {
  const bKeys = new Set(Object.keys(before || {}));
  const aKeys = new Set(Object.keys(after || {}));
  for (const k of aKeys) {
    const path = prefix ? `${prefix}.${k}` : k;
    if (!bKeys.has(k)) {
      acc.added.push(path);
      continue;
    }
    const bv = before[k];
    const av = after[k];
    if (JSON.stringify(bv) === JSON.stringify(av)) {
      acc.preserved.push(path);
    } else if (isPlainObject(bv) && isPlainObject(av)) {
      collectPathDiff(bv, av, path, acc);
    } else {
      acc.updated.push(path);
    }
  }
  for (const k of bKeys) {
    if (!aKeys.has(k)) {
      acc.removed.push(prefix ? `${prefix}.${k}` : k);
    }
  }
  return acc;
}

/**
 * Semantic lossless merge of live SoT payload with certified overlay.
 *
 * @returns {{
 *   ok: boolean,
 *   code: string|null,
 *   payload: object|null,
 *   json: string|null,
 *   length: number,
 *   conflicts: array,
 *   unresolvedConflicts: array,
 *   diff: object,
 *   classification: string,
 *   blockers: array,
 * }}
 */
export function mergeResearchSotLossless({
  livePayloadJson = null,
  livePayload = null,
  proposedPayloadJson = null,
  proposedPayload = null,
  maxChars = RESEARCH_PAYLOAD_MAX_CHARS_DEFAULT,
  failOnUnresolvedConflicts = true,
} = {}) {
  const liveRaw = livePayload
    ? { ...livePayload }
    : parsePayload(livePayloadJson);
  const propRaw = proposedPayload
    ? { ...proposedPayload }
    : parsePayload(proposedPayloadJson);

  const live = stripTelemetry(liveRaw);
  const conflicts = detectSotMergeConflicts(live, propRaw);

  // Auto-resolve keep-live conflicts (do not mutate live values).
  const unresolved = [];
  const autoResolved = [];
  for (const c of conflicts) {
    if (
      c.resolution === "KEEP_LIVE" ||
      c.resolution === "KEEP_LIVE_REJECT_DOWNGRADE" ||
      c.resolution === "KEEP_LIVE_EVIDENCE" ||
      c.resolution === "KEEP_LIVE_FLAG_CONFLICT"
    ) {
      autoResolved.push(c);
    } else {
      unresolved.push(c);
    }
  }

  if (failOnUnresolvedConflicts && unresolved.length > 0) {
    return {
      ok: false,
      code: "CONFLICT_REQUIRES_REVIEW",
      payload: null,
      json: null,
      length: 0,
      conflicts,
      unresolvedConflicts: unresolved,
      autoResolvedConflicts: autoResolved,
      diff: null,
      classification: "CONFLICT_REQUIRES_REVIEW",
      blockers: unresolved.map((u) => u.code),
      ownershipContract: GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
      mergeVersion: GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
    };
  }

  // Start from complete live SoT (unknown keys preserved).
  const merged = { ...live };
  const overlay = extractCertificationOverlay(propRaw);

  // Apply certification-owned keys (overlay wins when present).
  for (const k of SOT_CERTIFICATION_OWNED_KEYS) {
    if (overlay[k] !== undefined) merged[k] = overlay[k];
  }

  // Fill-empty identity only.
  for (const [ok, v] of Object.entries(overlay)) {
    if (!ok.startsWith("__fillEmpty__")) continue;
    const k = ok.slice("__fillEmpty__".length);
    if (!text(merged[k]) && text(v)) merged[k] = v;
  }

  // Contacts — identity merge.
  if (overlay.contacts !== undefined || live.contacts) {
    merged.contacts = mergeContactLists(live.contacts, overlay.contacts);
  }
  if (overlay.backupContacts !== undefined || live.backupContacts) {
    merged.backupContacts = mergeContactLists(
      live.backupContacts,
      overlay.backupContacts,
    );
  }
  if (overlay.namedStakeholders !== undefined || live.namedStakeholders) {
    merged.namedStakeholders = mergeContactLists(
      live.namedStakeholders,
      overlay.namedStakeholders,
    );
  }
  if (overlay.primaryContact || live.primaryContact) {
    if (isPlainObject(live.primaryContact) && isPlainObject(overlay.primaryContact)) {
      merged.primaryContact = mergeContactObjects(
        live.primaryContact,
        overlay.primaryContact,
      );
    } else if (isPlainObject(live.primaryContact)) {
      merged.primaryContact = live.primaryContact;
    } else if (isPlainObject(overlay.primaryContact)) {
      merged.primaryContact = overlay.primaryContact;
    }
  }
  if (overlay.secondaryContact || live.secondaryContact) {
    if (
      isPlainObject(live.secondaryContact) &&
      isPlainObject(overlay.secondaryContact)
    ) {
      merged.secondaryContact = mergeContactObjects(
        live.secondaryContact,
        overlay.secondaryContact,
      );
    } else if (isPlainObject(live.secondaryContact)) {
      merged.secondaryContact = live.secondaryContact;
    } else if (isPlainObject(overlay.secondaryContact)) {
      merged.secondaryContact = overlay.secondaryContact;
    }
  }

  // Evidence / sources — identity merge; never collapse FACT statements.
  if (overlay.evidence !== undefined || live.evidence) {
    merged.evidence = mergeEvidenceLists(live.evidence, overlay.evidence);
  }
  if (overlay.sources !== undefined || live.sources) {
    const liveSources = (Array.isArray(live.sources) ? live.sources : []).map(
      (s) => (typeof s === "string" ? { url: s } : s),
    );
    const propSources = (
      Array.isArray(overlay.sources) ? overlay.sources : []
    ).map((s) => (typeof s === "string" ? { url: s } : s));
    const mergedSourceObjs = mergeEvidenceLists(liveSources, propSources);
    // Preserve string form when live used bare URL strings.
    const liveUsedStrings = (Array.isArray(live.sources) ? live.sources : []).every(
      (s) => typeof s === "string",
    );
    merged.sources = liveUsedStrings
      ? mergedSourceObjs
          .map((s) => text(s?.url || s?.sourceUrl || (typeof s === "string" ? s : "")))
          .filter(Boolean)
      : mergedSourceObjs;
  }

  // Meeting history & competitors — preserve live; additive only.
  if (live.meetingHistory || overlay.meetingHistory) {
    merged.meetingHistory = mergeMeetingLists(
      live.meetingHistory,
      propRaw.meetingHistory,
    );
  }
  for (const ck of [
    "competitors",
    "marketCompetitors",
  ]) {
    if (live[ck] || propRaw[ck]) {
      merged[ck] = mergeCompetitorLists(live[ck], propRaw[ck]);
    }
  }

  // Authoritative classifications — keep live when conflict auto-resolved.
  if (text(live.revenueStreamClass)) {
    const propRev = text(propRaw.revenueStreamClass);
    const liveRev = text(live.revenueStreamClass);
    if (
      !propRev ||
      (REVENUE_STREAM_STRENGTH[propRev] || 0) <=
        (REVENUE_STREAM_STRENGTH[liveRev] || 0)
    ) {
      merged.revenueStreamClass = liveRev;
      if (live.revenueStreamLabel) merged.revenueStreamLabel = live.revenueStreamLabel;
      if (live.revenueStreamIcons) merged.revenueStreamIcons = live.revenueStreamIcons;
    }
  }
  if (text(live.commercialDemandType)) {
    const liveCdt = text(live.commercialDemandType);
    const propCdt = text(propRaw.commercialDemandType);
    if (!propCdt || (liveCdt.toUpperCase() === "BOTH" && propCdt.toUpperCase() !== "BOTH")) {
      merged.commercialDemandType = live.commercialDemandType;
      if (live.commercialDemandTypeLabel) {
        merged.commercialDemandTypeLabel = live.commercialDemandTypeLabel;
      }
    }
  }

  // Narratives — never replace stronger live with template/weaker proposed.
  for (const path of [
    "whatIsHappening",
    "summaryWhat",
    "summaryWhyHotel",
    "summaryWhyMatters",
    "summaryWhyNow",
    "fitExplanation",
    "whyThisHotel",
    "whyNow",
    "guestroomThesis",
    "eventFnbThesis",
    "hotelOpportunityThesis",
  ]) {
    const lv = text(live[path]);
    const pv = text(propRaw[path]);
    if (lv && (!pv || narrativeStrength(pv) < narrativeStrength(lv) * 0.5)) {
      merged[path] = live[path];
    } else if (lv && isExhibitorTemplateNarrative(pv) && !isExhibitorTemplateNarrative(lv)) {
      merged[path] = live[path];
    } else if (!lv && pv && !isExhibitorTemplateNarrative(pv)) {
      merged[path] = propRaw[path];
    }
    // Never inject unsupported exhibitor-template narratives into SoT.
  }

  // Never let derived presentation keys from slim wipe authoritative unknowns.
  for (const k of SOT_DERIVED_PRESENTATION_KEYS) {
    if (live[k] !== undefined && propRaw[k] !== undefined) {
      // Prefer live unless live empty.
      if (live[k] != null && live[k] !== "") merged[k] = live[k];
    }
  }

  // Information-loss gate: critical live arrays must not shrink to empty.
  const blockers = [];
  const criticalArrayPaths = [
    "meetingHistory",
    "evidence",
    "contacts",
    "competitors",
  ];
  for (const p of criticalArrayPaths) {
    const beforeN = Array.isArray(live[p]) ? live[p].length : 0;
    const afterN = Array.isArray(merged[p]) ? merged[p].length : 0;
    if (beforeN > 0 && afterN === 0) {
      blockers.push({
        code: "BLOCKED_INFORMATION_LOSS",
        path: p,
        beforeCount: beforeN,
        afterCount: afterN,
      });
    }
  }
  // Evidence rows must survive by stable id/url (enrichment may change claim identity).
  function stableEvidenceKey(e) {
    if (!e || typeof e !== "object") return "";
    const id = text(e.id);
    if (id) return `id:${id}`;
    const url = text(e.url || e.sourceUrl).toLowerCase();
    const field = text(e.field || e.supportsFact).toLowerCase();
    if (field && url) return `f:${field}|u:${url}`;
    if (url) return `u:${url}`;
    return evidenceIdentity(e);
  }
  const liveEvidenceKeys = (Array.isArray(live.evidence) ? live.evidence : [])
    .map((e) => stableEvidenceKey(e))
    .filter(Boolean);
  const mergedEvidenceKeys = new Set(
    (Array.isArray(merged.evidence) ? merged.evidence : [])
      .map((e) => stableEvidenceKey(e))
      .filter(Boolean),
  );
  // Also index merged by bare URL so enriched claim identities still match.
  for (const e of Array.isArray(merged.evidence) ? merged.evidence : []) {
    const url = text(e?.url || e?.sourceUrl).toLowerCase();
    if (url) mergedEvidenceKeys.add(`u:${url}`);
    const id = text(e?.id);
    if (id) mergedEvidenceKeys.add(`id:${id}`);
  }
  const lostEvidenceIds = liveEvidenceKeys.filter((id) => !mergedEvidenceKeys.has(id));
  if (lostEvidenceIds.length > 0) {
    blockers.push({
      code: "BLOCKED_EVIDENCE_FACT_LOSS",
      lostCount: lostEvidenceIds.length,
      sample: lostEvidenceIds.slice(0, 5),
    });
  }

  if (blockers.length > 0) {
    return {
      ok: false,
      code: "BLOCKED_INFORMATION_LOSS",
      payload: null,
      json: null,
      length: 0,
      conflicts,
      unresolvedConflicts: unresolved,
      autoResolvedConflicts: autoResolved,
      diff: null,
      classification: "BLOCKED_INFORMATION_LOSS",
      blockers,
      ownershipContract: GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
      mergeVersion: GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
    };
  }

  let json;
  try {
    json = JSON.stringify(merged);
  } catch (err) {
    return {
      ok: false,
      code: "SERIALIZE_FAILED",
      payload: null,
      json: null,
      length: 0,
      conflicts,
      classification: "BLOCKED_INFORMATION_LOSS",
      blockers: [{ code: "SERIALIZE_FAILED", error: String(err?.message || err) }],
      ownershipContract: GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
      mergeVersion: GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
    };
  }

  if (json.length > maxChars) {
    return {
      ok: false,
      code: "RESEARCH_PAYLOAD_TOO_LARGE",
      payload: null,
      json: null,
      length: json.length,
      conflicts,
      classification: "BLOCKED_INFORMATION_LOSS",
      blockers: [
        {
          code: "STORAGE_LIMIT",
          length: json.length,
          maxChars,
          note: "Refuse truncate; fail closed rather than degrade SoT",
        },
      ],
      ownershipContract: GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
      mergeVersion: GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
    };
  }

  const diff = collectPathDiff(live, merged);
  // Top-level only summary for classification
  const topAdded = diff.added.filter((p) => !p.includes("."));
  const topUpdated = diff.updated.filter((p) => !p.includes("."));
  const topRemoved = diff.removed.filter((p) => !p.includes("."));

  let classification = "UNCHANGED";
  const certTouched = SOT_CERTIFICATION_OWNED_KEYS.some(
    (k) => JSON.stringify(live[k]) !== JSON.stringify(merged[k]),
  );
  const researchTouched = topUpdated.some(
    (k) => classifySotPayloadKey(k) !== "CERTIFICATION" && k !== "contacts",
  );
  if (topRemoved.length > 0) {
    // Telemetry strip only is OK
    const nonTelemetryRemoved = topRemoved.filter(
      (k) => !SOT_TELEMETRY_DROP_KEYS.includes(k),
    );
    if (nonTelemetryRemoved.length > 0) {
      classification = "BLOCKED_INFORMATION_LOSS";
    }
  }
  if (classification !== "BLOCKED_INFORMATION_LOSS") {
    if (researchTouched || topAdded.some((k) => !SOT_CERTIFICATION_OWNED_KEYS.includes(k))) {
      classification = "VERIFIED_ENRICHMENT";
    } else if (certTouched || topAdded.length > 0) {
      classification = "CERTIFICATION_METADATA_ONLY";
    } else if (autoResolved.length > 0 && topUpdated.length === 0) {
      classification = "UNCHANGED";
    }
  }

  if (classification === "BLOCKED_INFORMATION_LOSS") {
    return {
      ok: false,
      code: "BLOCKED_INFORMATION_LOSS",
      payload: null,
      json: null,
      length: json.length,
      conflicts,
      autoResolvedConflicts: autoResolved,
      diff: {
        added: topAdded,
        updated: topUpdated,
        removed: topRemoved,
        preservedCount: diff.preserved.length,
        full: diff,
      },
      classification,
      blockers: [
        {
          code: "UNEXPECTED_KEY_REMOVAL",
          removed: topRemoved.filter((k) => !SOT_TELEMETRY_DROP_KEYS.includes(k)),
        },
      ],
      ownershipContract: GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
      mergeVersion: GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
    };
  }

  return {
    ok: true,
    code: null,
    payload: merged,
    json,
    length: json.length,
    conflicts,
    unresolvedConflicts: unresolved,
    autoResolvedConflicts: autoResolved,
    diff: {
      added: topAdded,
      updated: topUpdated,
      removed: topRemoved.filter((k) => SOT_TELEMETRY_DROP_KEYS.includes(k)),
      preservedCount: diff.preserved.length,
      full: diff,
    },
    classification,
    blockers: [],
    ownershipContract: GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
    mergeVersion: GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
    storageLimitCompatible: json.length <= maxChars,
  };
}

export default {
  GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
  mergeResearchSotLossless,
  detectSotMergeConflicts,
  extractCertificationOverlay,
  RESEARCH_PAYLOAD_MAX_CHARS_DEFAULT,
};
