/**
 * Masked logging helpers — never print full emails/phones or SURFE_API_KEY.
 */

export function maskEmail(email) {
  const s = String(email || "").trim();
  if (!s || !s.includes("@")) return s ? "***" : null;
  const [user, domain] = s.split("@");
  const u = user.length <= 2 ? "*" : `${user[0]}***${user[user.length - 1]}`;
  return `${u}@${domain}`;
}

export function maskPhone(phone) {
  const s = String(phone || "").replace(/\s+/g, "");
  if (!s) return null;
  if (s.length <= 4) return "****";
  return `${"*".repeat(Math.max(0, s.length - 4))}${s.slice(-4)}`;
}

export function logContactOp(event, meta = {}) {
  try {
    const safe = { ...meta };
    if (safe.email) safe.email = maskEmail(safe.email);
    if (safe.phone) safe.phone = maskPhone(safe.phone);
    if ("apiKey" in safe) delete safe.apiKey;
    if ("SURFE_API_KEY" in safe) delete safe.SURFE_API_KEY;
    if (process.env.NODE_ENV !== "production" || process.env.CONTACT_ENRICHMENT_DEBUG === "true") {
      console.log(`[market-alerts-contact] ${event}`, JSON.stringify(safe));
    }
  } catch {
    // never throw from logger
  }
}
