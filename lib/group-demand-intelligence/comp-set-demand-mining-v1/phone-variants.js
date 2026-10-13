/**
 * Public business phone normalization — discovery pivots only, never proof of stay.
 */

export function digitsOnly(phone = "") {
  return String(phone || "").replace(/\D+/g, "");
}

/**
 * Build searchable formatted variants from a public business phone.
 * Returns [] when phone unknown — do not invent.
 */
export function buildFormattedPhoneVariants(phone = "", countryHint = "") {
  const raw = String(phone || "").trim();
  if (!raw) return [];
  const d = digitsOnly(raw);
  if (d.length < 8) return [];

  const variants = new Set();
  variants.add(raw);
  variants.add(d);
  if (raw.startsWith("+")) variants.add(raw);
  else variants.add(`+${d}`);

  // Spaced international-ish
  if (d.length >= 10) {
    variants.add(`+${d.slice(0, 2)} ${d.slice(2, 4)} ${d.slice(4, 7)} ${d.slice(7, 9)} ${d.slice(9)}`);
    variants.add(`+${d.slice(0, 2)} ${d.slice(2)}`);
  }

  // Switzerland
  if (countryHint === "CH" || d.startsWith("41")) {
    const local = d.startsWith("41") ? d.slice(2) : d;
    if (local.length >= 9) {
      variants.add(`+41 ${local.slice(0, 2)} ${local.slice(2, 5)} ${local.slice(5, 7)} ${local.slice(7)}`);
      variants.add(`+41${local}`);
      variants.add(`0${local.slice(0, 2)} ${local.slice(2, 5)} ${local.slice(5, 7)} ${local.slice(7)}`);
      variants.add(`0${local}`);
    }
  }

  // Spain
  if (countryHint === "ES" || d.startsWith("34")) {
    const local = d.startsWith("34") ? d.slice(2) : d;
    if (local.length >= 9) {
      variants.add(`+34 ${local.slice(0, 3)} ${local.slice(3, 5)} ${local.slice(5, 7)} ${local.slice(7)}`);
      variants.add(`+34${local}`);
      variants.add(`${local.slice(0, 3)} ${local.slice(3, 6)} ${local.slice(6)}`);
    }
  }

  // US / Caribbean / Bermuda style +1
  if (countryHint === "US" || countryHint === "GD" || countryHint === "BM" || d.startsWith("1")) {
    const local = d.startsWith("1") && d.length === 11 ? d.slice(1) : d.length === 10 ? d : null;
    if (local && local.length === 10) {
      variants.add(`+1 ${local.slice(0, 3)}-${local.slice(3, 6)}-${local.slice(6)}`);
      variants.add(`+1${local}`);
      variants.add(`(${local.slice(0, 3)}) ${local.slice(3, 6)}-${local.slice(6)}`);
      variants.add(`${local.slice(0, 3)}-${local.slice(3, 6)}-${local.slice(6)}`);
    }
  }

  return [...variants].filter(Boolean).slice(0, 12);
}

/** Extract first plausible phone from page text (public listing). Prefer +country format. */
export function extractPublicPhoneFromText(text = "", countryHint = "") {
  const t = String(text || "");
  // Prefer international + formats to avoid garbage digit runs
  const preferred = [
    /\+\d{1,3}[\s\-.]?\(?\d{1,4}\)?[\s\-.]?\d{2,4}[\s\-.]?\d{2,4}[\s\-.]?\d{0,4}/g,
    /\b\(\d{3}\)\s*\d{3}[\s\-]?\d{4}\b/g,
    /\b0\d{2}[\s\-.]?\d{3}[\s\-.]?\d{2}[\s\-.]?\d{2}\b/g,
  ];
  for (const re of preferred) {
    const m = t.match(re);
    if (!m?.length) continue;
    for (const candidate of m) {
      const d = digitsOnly(candidate);
      // Reject toll-free / too short / absurd
      if (d.length < 10 || d.length > 15) continue;
      if (/^1800|^1888|^1877|^0800/.test(d)) continue;
      return candidate.trim();
    }
  }
  return null;
}
