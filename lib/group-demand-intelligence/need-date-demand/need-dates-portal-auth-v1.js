/**
 * Hotel-scoped authorization for Need Dates Management portal.
 * Do not trust client-supplied HotelId alone.
 */

import { WATERSTONE_NEED_DATE_PILOT_HOTEL_ID } from "./need-date-demand-contract-v1.js";

function text(v) {
  return String(v || "").trim();
}

function parseAllowlist(env = process.env) {
  const raw = text(env.GDI_NEED_DATES_AUTHORIZED_HOTELS);
  if (!raw) return null;
  return new Set(
    raw
      .split(",")
      .map((s) => text(s))
      .filter(Boolean),
  );
}

/**
 * Hotels the Dealality user may access for need-date management.
 * Prefer explicit user fields; fall back to env allowlist / local portal test mode.
 */
export function resolveAuthorizedNeedDateHotels(req, env = process.env) {
  const user = req?.dealalityUser || null;
  const fromUser = [
    ...(Array.isArray(user?.authorizedHotelIds) ? user.authorizedHotelIds : []),
    ...(Array.isArray(user?.hotelIds) ? user.hotelIds : []),
    ...(Array.isArray(user?.gdiHotelIds) ? user.gdiHotelIds : []),
    text(user?.primaryHotelId),
  ]
    .map(text)
    .filter(Boolean);

  if (fromUser.length) return [...new Set(fromUser)];

  const allow = parseAllowlist(env);
  if (allow) return [...allow];

  // Local zero-paid portal certification only
  if (
    text(env.GDI_NEED_DATES_PORTAL_LOCAL_WRITE) === "1" ||
    text(env.GDI_NEED_DATES_PORTAL_LOCAL_WRITE).toLowerCase() === "true"
  ) {
    return [WATERSTONE_NEED_DATE_PILOT_HOTEL_ID];
  }

  // Authenticated Dealality users without hotel binding: deny (fail closed for writes)
  if (user) return [];

  return [];
}

export function assertNeedDatesHotelAccess(req, hotelId, { write = false } = {}) {
  const id = text(hotelId);
  if (!/^rec[a-zA-Z0-9]+$/.test(id)) {
    const err = new Error("invalid_hotel_id");
    err.code = "INVALID_HOTEL_ID";
    err.status = 400;
    throw err;
  }

  const isLocalWrite =
    text(process.env.GDI_NEED_DATES_PORTAL_LOCAL_WRITE) === "1" ||
    text(process.env.GDI_NEED_DATES_PORTAL_LOCAL_WRITE).toLowerCase() ===
      "true";

  if (write) {
    const hasUser = Boolean(req?.dealalityUser);
    if (!hasUser && !isLocalWrite) {
      const err = new Error("authentication_required");
      err.code = "AUTHENTICATION_REQUIRED";
      err.status = 401;
      throw err;
    }
    const authorized = resolveAuthorizedNeedDateHotels(req);
    // If user has no hotel bindings and no allowlist, fail closed unless local write + Waterstone test
    if (!authorized.length) {
      if (isLocalWrite && id === WATERSTONE_NEED_DATE_PILOT_HOTEL_ID) {
        return { hotelId: id, actor: "local_portal_test", mode: "local_write" };
      }
      const err = new Error("hotel_access_denied");
      err.code = "HOTEL_ACCESS_DENIED";
      err.status = 403;
      throw err;
    }
    if (!authorized.includes(id)) {
      const err = new Error("hotel_access_denied");
      err.code = "HOTEL_ACCESS_DENIED";
      err.status = 403;
      throw err;
    }
    return {
      hotelId: id,
      actor: req.dealalityUser?.email || req.dealalityUser?.id || "local_portal_test",
      mode: hasUser ? "memberstack" : "local_write",
    };
  }

  // Reads: if Bearer user present, enforce allowlist when available
  if (req?.dealalityUser) {
    const authorized = resolveAuthorizedNeedDateHotels(req);
    if (authorized.length && !authorized.includes(id)) {
      const err = new Error("hotel_access_denied");
      err.code = "HOTEL_ACCESS_DENIED";
      err.status = 403;
      throw err;
    }
  }

  return {
    hotelId: id,
    actor: req?.dealalityUser?.email || null,
    mode: req?.dealalityUser ? "memberstack" : "pilot_read",
  };
}

export function actorUserFromReq(req) {
  const u = req?.dealalityUser;
  if (!u) {
    return { id: "local_portal_test", email: null, name: "Local Portal Test" };
  }
  return {
    id: u.id || u.memberId || null,
    email: u.email || null,
    name: u.name || u.fullName || null,
  };
}
