/** Strip grouping separators (commas) and parse a nonnegative integer; empty → null. */
export function parseFormattedInt(value) {
  if (value == null || value === "") return null;
  const s = String(value).replace(/\D/g, "");
  if (s === "") return null;
  const n = parseInt(s, 10);
  return Number.isFinite(n) ? n : null;
}

export function formatListValue(val) {
  if (val == null) return "";
  if (typeof val === "string") return val.trim();
  if (typeof val === "number" && Number.isFinite(val)) return String(val);
  if (Array.isArray(val)) {
    return val
      .map((v) => {
        if (typeof v === "string") return v.trim();
        if (v && typeof v === "object" && typeof v.name === "string") return v.name.trim();
        return "";
      })
      .filter(Boolean)
      .join(", ");
  }
  if (typeof val === "object" && val !== null && typeof val.name === "string") {
    return val.name.trim();
  }
  return String(val).trim();
}

export function safeParseJsonArray(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

export function parseMultiValue(value) {
  if (value == null || value === "") return [];

  function splitTopLevelCommas(str) {
    const out = [];
    let cur = "";
    let depth = 0;

    for (let i = 0; i < str.length; i += 1) {
      const ch = str[i];
      if (ch === "(") depth += 1;
      if (ch === ")") depth = Math.max(0, depth - 1);

      if (ch === "," && depth === 0) {
        const t = cur.trim();
        if (t) out.push(t);
        cur = "";
        continue;
      }
      cur += ch;
    }

    const t = cur.trim();
    if (t) out.push(t);
    return out;
  }

  if (Array.isArray(value)) {
    return value
      .map((v) => formatListValue(v))
      .map((v) => String(v).trim())
      .filter(Boolean);
  }
  return splitTopLevelCommas(String(value))
    .map((v) => String(v).trim())
    .filter(Boolean);
}
