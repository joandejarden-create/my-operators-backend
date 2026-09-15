/**
 * GDI signed share capabilities — mirrors ADP/BAI share pattern.
 * Token scoped to hotelId; read-only surfaces only.
 */
import { createHmac, randomBytes, timingSafeEqual } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";

export const GDI_SHARE_TOKEN_VERSION = 1;
export const GDI_SHARE_TOKEN_PREFIX = "gdishare.v1.";
export const GDI_SIGNED_SHARE_CAPABILITY = "GDI_SIGNED_SHARE_CAPABILITY";

export const DEFAULT_GDI_SHARE_SURFACES = Object.freeze([
  "brief",
  "opportunities",
  "opportunity_detail",
  "summary",
]);

const REGISTRY_DIR =
  (process.env.GDI_SHARE_REGISTRY_DIR && String(process.env.GDI_SHARE_REGISTRY_DIR).trim()) ||
  join(process.cwd(), "config/client-share/gdi-share-registry");
const REGISTRY_PATH = join(REGISTRY_DIR, "active-tokens.json");

function b64url(buf) {
  return Buffer.from(buf)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function b64urlJson(obj) {
  return b64url(Buffer.from(JSON.stringify(obj), "utf8"));
}

function fromB64url(str) {
  const pad = str.length % 4 === 0 ? "" : "=".repeat(4 - (str.length % 4));
  const b64 = str.replace(/-/g, "+").replace(/_/g, "/") + pad;
  return Buffer.from(b64, "base64");
}

function safeEqualStr(a, b) {
  const ba = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ba.length !== bb.length) return false;
  return timingSafeEqual(ba, bb);
}

export function getGdiShareCapabilitySecret() {
  const secret = process.env.GDI_SHARE_CAPABILITY_SECRET || "";
  if (secret && secret.length >= 32) return secret;
  if (
    process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET === "1" ||
    process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET === "true"
  ) {
    return "gdi-share-dev-secret-do-not-use-in-production-32b";
  }
  return null;
}

export function gdiShareEnforcementEnabled() {
  if (process.env.GDI_SHARE_CAPABILITY_ENFORCE === "0") return false;
  if (process.env.GDI_SHARE_CAPABILITY_ENFORCE === "false") return false;
  return true;
}

function loadRegistry() {
  if (!existsSync(REGISTRY_PATH)) {
    return { version: "gdi_share_registry_v1", tokens: {} };
  }
  return JSON.parse(readFileSync(REGISTRY_PATH, "utf8"));
}

function saveRegistry(reg) {
  mkdirSync(REGISTRY_DIR, { recursive: true });
  writeFileSync(REGISTRY_PATH, JSON.stringify(reg, null, 2) + "\n");
}

export function readGdiShareRegistry() {
  return loadRegistry();
}

export function issueGdiShareCapability({
  hotelId,
  label = null,
  surfaces = DEFAULT_GDI_SHARE_SURFACES,
  expiresAt = null,
  tokenId = null,
} = {}) {
  const secret = getGdiShareCapabilitySecret();
  if (!secret) {
    throw new Error(
      "GDI_SHARE_CAPABILITY_SECRET missing (min 32 chars). Set env or GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET=1 for local only."
    );
  }
  if (!hotelId) throw new Error("hotelId required");
  const tid = tokenId || `gdisht_${randomBytes(12).toString("hex")}`;
  const iat = Math.floor(Date.now() / 1000);
  const payload = {
    v: GDI_SHARE_TOKEN_VERSION,
    tid,
    hotelId,
    surfaces: [...surfaces],
    mode: "read_only",
    iat,
    exp: expiresAt ? Math.floor(new Date(expiresAt).getTime() / 1000) : null,
  };
  const body = b64urlJson(payload);
  const sig = b64url(createHmac("sha256", secret).update(body).digest());
  const token = `${GDI_SHARE_TOKEN_PREFIX}${body}.${sig}`;

  const reg = loadRegistry();
  reg.tokens[tid] = {
    tokenId: tid,
    hotelId,
    label: label || hotelId,
    surfaces: payload.surfaces,
    mode: "read_only",
    status: "ACTIVE",
    issuedAt: new Date(iat * 1000).toISOString(),
    expiresAt: expiresAt || null,
    revokedAt: null,
  };
  saveRegistry(reg);

  return {
    token,
    tokenId: tid,
    hotelId,
    sharePath: `/group-demand-intelligence-share.html?share=${encodeURIComponent(token)}`,
    meta: reg.tokens[tid],
  };
}

export function revokeGdiShareCapability(tokenId, reason = "revoked") {
  const reg = loadRegistry();
  const row = reg.tokens[tokenId];
  if (!row) return { ok: false, error: "token_not_found" };
  row.status = "REVOKED";
  row.revokedAt = new Date().toISOString();
  row.revokeReason = reason;
  saveRegistry(reg);
  return { ok: true, tokenId, meta: row };
}

export function verifyGdiShareCapability(
  token,
  { expectedHotelId = null, requiredSurface = null } = {}
) {
  const secret = getGdiShareCapabilitySecret();
  if (!secret) {
    return { ok: false, error: "share_secret_not_configured", code: "SHARE_SECRET_MISSING" };
  }
  if (!token || typeof token !== "string") {
    return { ok: false, error: "share_capability_required", code: "SHARE_MISSING" };
  }
  let raw = token.trim();
  if (raw.startsWith(GDI_SHARE_TOKEN_PREFIX)) {
    raw = raw.slice(GDI_SHARE_TOKEN_PREFIX.length);
  }
  const parts = raw.split(".");
  if (parts.length !== 2) {
    return { ok: false, error: "malformed_share_capability", code: "SHARE_MALFORMED" };
  }
  const [body, sig] = parts;
  const expectedSig = b64url(createHmac("sha256", secret).update(body).digest());
  if (!safeEqualStr(sig, expectedSig)) {
    return { ok: false, error: "invalid_share_signature", code: "SHARE_BAD_SIGNATURE" };
  }
  let payload;
  try {
    payload = JSON.parse(fromB64url(body).toString("utf8"));
  } catch {
    return { ok: false, error: "malformed_share_payload", code: "SHARE_MALFORMED" };
  }
  if (payload.v !== GDI_SHARE_TOKEN_VERSION) {
    return { ok: false, error: "unsupported_share_version", code: "SHARE_VERSION" };
  }
  if (!payload.tid || !payload.hotelId) {
    return { ok: false, error: "incomplete_share_payload", code: "SHARE_MALFORMED" };
  }
  if (payload.exp != null && Number(payload.exp) < Math.floor(Date.now() / 1000)) {
    return { ok: false, error: "share_capability_expired", code: "SHARE_EXPIRED" };
  }
  const reg = loadRegistry();
  const row = reg.tokens[payload.tid];
  if (!row) {
    return { ok: false, error: "share_capability_unknown", code: "SHARE_UNKNOWN" };
  }
  if (row.status !== "ACTIVE") {
    return { ok: false, error: "share_capability_revoked", code: "SHARE_REVOKED" };
  }
  if (row.hotelId !== payload.hotelId) {
    return { ok: false, error: "share_registry_mismatch", code: "SHARE_REGISTRY_MISMATCH" };
  }
  if (expectedHotelId && expectedHotelId !== payload.hotelId) {
    return {
      ok: false,
      error: "share_hotel_scope_mismatch",
      code: "SHARE_HOTEL_SCOPE",
      tokenHotelId: payload.hotelId,
      requestedHotelId: expectedHotelId,
    };
  }
  if (requiredSurface && !(payload.surfaces || []).includes(requiredSurface)) {
    return { ok: false, error: "share_surface_not_allowed", code: "SHARE_SURFACE" };
  }
  return {
    ok: true,
    claims: payload,
    meta: row,
    gate: GDI_SIGNED_SHARE_CAPABILITY,
  };
}

export function extractGdiShareCapabilityFromRequest(req) {
  const q = req?.query || {};
  if (typeof q.share === "string" && q.share) return q.share;
  const h = req?.headers || {};
  if (typeof h["x-gdi-share-capability"] === "string" && h["x-gdi-share-capability"]) {
    return h["x-gdi-share-capability"];
  }
  const auth = h.authorization || h.Authorization;
  if (typeof auth === "string" && auth.toLowerCase().startsWith("bearer ")) {
    const t = auth.slice(7).trim();
    if (t.startsWith(GDI_SHARE_TOKEN_PREFIX)) return t;
  }
  return null;
}

/**
 * Strip internal/admin fields for external share payloads.
 */
export function sanitizeOpportunityForShare(opp) {
  if (!opp) return null;
  const evidence = (opp.evidence || [])
    .filter((e) => e.claimKind === "FACT" || e.sourceAuthority === "Tier_A" || e.sourceAuthority === "Tier_B")
    .slice(0, 8)
    .map((e) => ({
      field: e.field,
      value: e.value,
      claimKind: e.claimKind,
      sourceTitle: e.sourceTitle,
      sourceUrl: e.sourceUrl,
      sourceDomain: e.sourceDomain,
      confidence: e.confidence,
    }));
  return {
    id: opp.id,
    title: opp.title,
    organizationName: opp.organizationName,
    segment: opp.segment,
    demandType: opp.demandType,
    demandStatus: opp.demandStatus,
    priority: opp.priority,
    eventStartDate: opp.eventStartDate,
    eventEndDate: opp.eventEndDate,
    destinationStatus: opp.destinationStatus,
    venueStatus: opp.venueStatus,
    estimatedAttendance: opp.estimatedAttendance,
    estimatedAttendanceClaimKind: opp.estimatedAttendanceClaimKind,
    estimatedPeakRooms: opp.estimatedPeakRooms,
    estimatedPeakRoomsClaimKind: opp.estimatedPeakRoomsClaimKind,
    estimatedNights: opp.estimatedNights,
    estimatedNightsClaimKind: opp.estimatedNightsClaimKind,
    hotelFitScore: opp.hotelFitScore,
    physicalFitScore: opp.physicalFitScore,
    geographyFitScore: opp.geographyFitScore,
    timingScore: opp.timingScore,
    commercialValueScore: opp.commercialValueScore,
    historicalFitScore: opp.historicalFitScore,
    competitiveAccessibilityScore: opp.competitiveAccessibilityScore,
    contactabilityScore: opp.contactabilityScore,
    evidenceConfidence: opp.evidenceConfidence,
    evidenceConfidenceExplanation: opp.evidenceConfidenceExplanation || null,
    evidenceConfidenceTooltip: opp.evidenceConfidenceTooltip || null,
    hotelFitComponentLabels: opp.hotelFitComponentLabels || null,
    bookingWindowStatus: opp.bookingWindowStatus,
    bookingWindowLabel: opp.bookingWindowLabel || null,
    demandTerritoryFit: opp.demandTerritoryFit || null,
    demandTerritoryFitLabel: opp.demandTerritoryFitLabel || null,
    demandTerritoryRationale: opp.demandTerritoryRationale || null,
    bethesdaWinThesis: opp.bethesdaWinThesis || null,
    hotelOpportunityThesis: opp.hotelOpportunityThesis || opp.bethesdaWinThesis || null,
    opportunityType: opp.opportunityType || null,
    opportunityTypeLabel: opp.opportunityTypeLabel || null,
    venueSourcingStatus: opp.venueSourcingStatus || null,
    venueSourcingStatusLabel: opp.venueSourcingStatusLabel || null,
    venueSourcingRationale: opp.venueSourcingRationale || null,
    eventLocationStatus: opp.eventLocationStatus || null,
    eventLocationStatusLabel: opp.eventLocationStatusLabel || null,
    eventLocationSummary: opp.eventLocationSummary || null,
    roomDemandStatus: opp.roomDemandStatus || null,
    roomDemandStatusLabel: opp.roomDemandStatusLabel || null,
    roomDemandRationale: opp.roomDemandRationale || null,
    roomDemandConfidence: opp.roomDemandConfidence ?? null,
    publishedAttendance: opp.publishedAttendance ?? null,
    publishedPeakRooms: opp.publishedPeakRooms ?? null,
    opportunityQualification: opp.opportunityQualification || null,
    opportunityQualificationLabel: opp.opportunityQualificationLabel || null,
    contactQuality: opp.contactQuality || null,
    contactQualityLabel: opp.contactQualityLabel || null,
    contactRole: opp.contactRole || null,
    relationshipToEvent: opp.relationshipToEvent || null,
    relationshipConfidence: opp.relationshipConfidence || null,
    reactivationSignal: opp.reactivationSignal || null,
    reactivationSignalLabel: opp.reactivationSignalLabel || null,
    reactivationThesis: opp.reactivationThesis || null,
    marketCompetitors: (opp.marketCompetitors || []).slice(0, 8),
    sourcingStatus: opp.sourcingStatus || "UNKNOWN",
    sourcingStatusLabel: opp.sourcingStatusLabel || "Unknown — requires hotel validation",
    alreadySourcedToHotelDisplay:
      opp.alreadySourcedToHotelDisplay || "Unknown — requires hotel validation",
    incrementalValueStatus: opp.incrementalValueStatus || "UNKNOWN",
    incrementalValueStatusLabel: opp.incrementalValueStatusLabel || null,
    whyNow: opp.whyNow,
    fitExplanation: opp.fitExplanation,
    summaryWhat: opp.summaryWhat,
    summaryWhyMatters: opp.summaryWhyMatters,
    summaryWhyHotel: opp.summaryWhyHotel,
    recommendedAction: opp.recommendedAction,
    likelyCompetitor: opp.likelyCompetitor,
    likelyStrCompetitor: opp.likelyStrCompetitor || null,
    likelyGroupCompetitor: opp.likelyGroupCompetitor || null,
    competitorRationale: opp.competitorRationale,
    sources: (opp.sources || []).slice(0, 8).map((s) => ({
      name: s.name,
      sourceType: s.sourceType,
      date: s.date,
      url: s.url,
      supportsFact: s.supportsFact,
      claimKind: s.claimKind,
    })),
    knownVsEstimated: opp.knownVsEstimated
      ? {
          verified: (opp.knownVsEstimated.verified || []).slice(0, 8),
          estimated: (opp.knownVsEstimated.estimated || []).slice(0, 8),
          inferred: (opp.knownVsEstimated.inferred || []).slice(0, 8),
        }
      : null,
    competitors: (opp.competitors || []).slice(0, 8).map((c) => ({
      name: c.name,
      class: c.class,
      classLabel: c.classLabel,
      reason: c.reason,
    })),
    contactGrade: opp.contactGrade || null,
    contactGradeLabel: opp.contactGradeLabel || null,
    contactConfidence: opp.contactConfidence ?? null,
    contactRoleMatch: opp.contactRoleMatch || null,
    contactRoleMatchLabel: opp.contactRoleMatchLabel || null,
    whyThisContact: opp.whyThisContact || null,
    relationshipToEvent: opp.relationshipToEvent || null,
    primaryContact: opp.primaryContact
      ? {
          name: opp.primaryContact.name,
          role: opp.primaryContact.role,
          title: opp.primaryContact.title || opp.primaryContact.role,
          organization: opp.primaryContact.organization,
          email: opp.primaryContact.email,
          emailType: opp.primaryContact.emailType || null,
          emailTypeLabel: opp.primaryContact.emailTypeLabel || null,
          emailVerificationStatus: opp.primaryContact.emailVerificationStatus || null,
          emailVerificationStatusLabel:
            opp.primaryContact.emailVerificationStatusLabel || null,
          phone: opp.primaryContact.phone || null,
          phoneType: opp.primaryContact.phoneType || null,
          phoneTypeLabel: opp.primaryContact.phoneTypeLabel || null,
          contactGrade: opp.primaryContact.contactGrade || opp.contactGrade || null,
          contactGradeLabel:
            opp.primaryContact.contactGradeLabel || opp.contactGradeLabel || null,
          contactConfidence:
            opp.primaryContact.contactConfidence ?? opp.contactConfidence ?? null,
          targetRoleMatch: opp.primaryContact.targetRoleMatch || null,
          targetRoleMatchLabel: opp.primaryContact.targetRoleMatchLabel || null,
          relationshipToEvent: opp.primaryContact.relationshipToEvent || null,
          whyThisContact: opp.primaryContact.whyThisContact || opp.whyThisContact || null,
          lastVerifiedAt: opp.primaryContact.lastVerifiedAt || null,
          claimKind: opp.primaryContact.claimKind,
        }
      : null,
    backupContacts: (opp.backupContacts || []).slice(0, 3).map((c) => ({
      name: c.name,
      role: c.role,
      organization: c.organization,
      email: c.email,
      phone: c.phone || null,
      phoneTypeLabel: c.phoneTypeLabel || null,
      emailVerificationStatusLabel: c.emailVerificationStatusLabel || null,
      contactGrade: c.contactGrade || null,
      whyThisContact: c.whyThisContact || null,
    })),
    meetingHistory: (opp.meetingHistory || []).map((h) => ({
      year: h.year,
      city: h.city,
      venue: h.venue,
      claimKind: h.claimKind,
    })),
    evidence,
    plannerConsideration: opp.plannerConsideration
      ? {
          experimental: true,
          label: "EXPERIMENTAL",
          prompt: opp.plannerConsideration.prompt,
          bethesdaAppeared: opp.plannerConsideration.bethesdaAppeared,
          bethesdaRank: opp.plannerConsideration.bethesdaRank,
          observations: opp.plannerConsideration.observations,
        }
      : null,
  };
}
