/**
 * Keep in sync with api/brand-library.js normalizeFeeBasisValue + resolveFeeBasisPercentRevenue.
 */
const FEE_BASIS_GROSS = "% of Gross Revenue";
const FEE_BASIS_ROOMS = "% of Rooms Revenue";
const FEE_BASIS_TOTAL = "% of Total Revenue";

function resolveFeeBasisPercentRevenue(val) {
  if (val === undefined || val === null || val === "") return val;
  const s = String(val).replace(/["\u201C\u201D]/g, "").trim().toLowerCase();
  if (s.includes("total")) return FEE_BASIS_TOTAL;
  if (s.includes("rooms") || s.includes("room")) return FEE_BASIS_ROOMS;
  if (s.includes("gross") || s.includes("revenue")) return FEE_BASIS_GROSS;
  return val;
}

export function normalizeFeeBasisValue(val) {
  if (val === undefined || val === null || val === "") return val;
  let s = String(val)
    .replace(/\\"/g, "")
    .replace(/["\u201C\u201D\u201E\u201F\u2033\u2036]/g, "")
    .replace(/\u00A0/g, " ")
    .trim();
  const lower = s.toLowerCase();
  if (lower.includes("% of") && lower.includes("revenue")) return resolveFeeBasisPercentRevenue(s);
  const map = {
    "one-time": "One-Time",
    "one time": "One-Time",
    included: "Included",
    "per application": "Per Application",
    "per property": "Per Property",
    "per room / year": "Per Room / Year",
    "per room/year": "Per Room / Year",
    "per room": "Per Room",
    "base + per room over threshold": "Base + Per Room Over Threshold",
    "per reservation / per booking": "Per Reservation / Per Booking",
    "per reservation": "Per Reservation / Per Booking",
    "per room / month": "Per Room / Month",
    "per room/month": "Per Room / Month",
    "fixed fee": "Fixed Fee",
    fixed: "Fixed Fee",
  };
  return map[lower] !== undefined ? map[lower] : s;
}

export function pickBasis(allowed, want) {
  if (!Array.isArray(allowed) || !allowed.length) return want;
  const w = normalizeFeeBasisValue(want);
  const exact = allowed.find((c) => String(c).trim().toLowerCase() === String(w).trim().toLowerCase());
  if (exact) return exact;
  return allowed[0];
}
