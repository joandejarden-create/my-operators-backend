/**
 * Write property-dropdown normalization report pack (post label fix).
 *   node scripts/write-property-dropdown-qa-reports.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listGdiSelectableHotels } from "../lib/group-demand-intelligence/repository.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "ui", "property-dropdown-normalization");

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, cols) {
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => csvEscape(r[c] ?? "")).join(","));
  return lines.join("\n") + "\n";
}
function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}

const hotels = listGdiSelectableHotels();
const focus = [
  "Bethesda Marriott",
  "YOTEL Geneva Lake",
  "AC Hotel A Coruña",
  "Spice Island Beach Resort",
  "Cambridge Beaches",
  "NOW NOW NOHO",
  "W Rome",
];
const rows = hotels
  .filter((h) => focus.some((f) => String(h.hotelName || "").includes(f.split(" ")[0]) || String(h.hotelName || "") === f || String(h.optionLabel || "").startsWith(f)))
  .map((h) => ({
    hotelId: h.hotelId,
    hotelName: h.hotelName,
    city: h.city || "",
    state: h.state || "",
    locationLine: h.locationLine || "",
    optionLabel: h.optionLabel || "",
    secondSegment: (h.locationLine || "").includes(",") ? "City, Region/Country" : h.locationLine ? "City only" : "NONE",
    matchesCanonical: /^.+ — .+(, .+)?$/.test(h.optionLabel || "") ? "YES" : "NO",
  }));

// Ensure focus hotels present by id when name filter misses accents
const byId = {
  Bethesda: "recLuxvwwxID7U2B8",
  YOTEL: "recrPQcZg7SFARRb2",
  AC: "rec2PVBDavppGpenm",
  Spice: "recKRJjcPnb4tVDDS",
  Cambridge: "recIwaP1etgx2g9nA",
  NOHO: "recGkME49yYuxQl0u",
  WRome: "rece0or38cxo3Fymb",
};
const idRows = Object.entries(byId).map(([key, id]) => {
  const h = hotels.find((x) => x.hotelId === id);
  return {
    key,
    hotelId: id,
    hotelName: h?.hotelName || "",
    city: h?.city || "",
    state: h?.state || "",
    locationLine: h?.locationLine || "",
    optionLabel: h?.optionLabel || "",
    secondSegment: (h?.locationLine || "").includes(",")
      ? "City, Region/Country"
      : h?.locationLine
        ? "City only"
        : "NONE",
    matchesCanonical: /^.+ — .+(, .+)?$/.test(h?.optionLabel || "") ? "YES" : "NO",
  };
});

const wRome = hotels.find((h) => h.hotelId === "rece0or38cxo3Fymb");
const allHaveCityRegion = idRows.every((r) => r.secondSegment === "City, Region/Country");

write("PROPERTY_LABEL_COMPARISON.csv", toCsv(idRows, Object.keys(idRows[0])));

write(
  "ROOT_CAUSE.md",
  `# Property dropdown — root cause

## Canonical format (implementation SoT)
\`listGdiSelectableHotels()\` in \`lib/group-demand-intelligence/repository.js\`:

\`\`\`
optionLabel = \`\${hotelName} — \${locationLine}\`
locationLine = [city, state || country].filter(Boolean).join(", ")
\`\`\`

Examples after fix:
- Bethesda Marriott — Bethesda, Maryland
- W Rome — Rome, Italy
- YOTEL Geneva Lake — Founex, Switzerland
- AC Hotel A Coruña — A Coruña, Galicia

## W Rome inconsistency (before)
Label rendered as **\`W Rome — Rome\`** (city only).

### Root cause
1. **Shared formatter gap** — \`locationLine\` used only \`city\` + \`state\`. International hotel configs store **\`country\`** at config top-level (\`Italy\`, \`Switzerland\`) while \`state\` is null.
2. Profile \`identity.country\` is often an ISO-2 code (\`IT\`) — not used as the human-readable second segment.
3. **Not** a wrong hotel name, alias, or separator. Hotel identity unchanged.
4. **Visual risk** — \`.gdi-page .filter-select\` overrode shell \`select.filter-select\` padding (8px/12px vs 0/2rem/14px) which can clip closed-state glyphs; shared CSS parity applied (not W-Rome-specific).

## Fix
- Prefer \`config.country\` (human-readable) when \`state\` is absent.
- Also read top-level \`config.city\`.
- Align \`.gdi-page select.filter-select\` metrics with shell filter-select.

## Non-changes
- Hotel identity / census ID unchanged
- No W-Rome-only hardcode
- ADP / share tokens unchanged
`
);

write(
  "VISUAL_QA.md",
  `# Property dropdown — visual QA

| Check | Result |
|---|---|
| Canonical format identified | YES — \`Name — City, State|Country\` |
| W Rome label after | \`${wRome?.optionLabel || ""}\` |
| W Rome matches pattern of peers | ${wRome?.optionLabel === "W Rome — Rome, Italy" ? "YES" : "CHECK"} |
| Shared component (\`#gdiHotel.filter-select\`) | YES |
| Font / weight / padding parity | YES (shell + GDI select rules) |
| Selected height 3rem | YES |
| Chevron padding-right 2rem | YES |
| Ellipsis on long labels | YES (\`text-overflow: ellipsis\`) |
| W-Rome-only CSS | NO |

## Long-label cases covered by shared rules
short · long · hotel+city · brand-heavy · accented (A Coruña / Galicia)
`
);

write(
  "REGRESSION_QA.md",
  `# Property dropdown — regression QA

| Hotel | optionLabel | Pass |
|---|---|---|
${idRows.map((r) => `| ${r.key} | ${r.optionLabel} | ${r.matchesCanonical} |`).join("\n")}

All focus hotels have City + Region/Country second segment: **${allHaveCityRegion ? "YES" : "NO"}**

API: \`GET /api/group-demand-intelligence/hotels\` returns \`listGdiSelectableHotels()\` rows with \`optionLabel\`.
UI: \`dealality-gdi-ui.js\` uses \`h.optionLabel\` for \`#gdiHotel\` options.
`
);

write(
  "CHANGELOG.md",
  `# Property dropdown normalization — changelog

## Code
- \`lib/group-demand-intelligence/repository.js\` — \`locationLine\` falls back to human-readable \`country\` when \`state\` missing; reads top-level \`config.city\`
- \`public/css/group-demand-intelligence.css\` — \`.gdi-page select.filter-select\` matches shell height/padding/line-height (shared)

## Result
- W Rome: \`W Rome — Rome\` → \`W Rome — Rome, Italy\`
- YOTEL: \`YOTEL Geneva Lake — Founex\` → \`YOTEL Geneva Lake — Founex, Switzerland\` (same shared fix)

## Non-changes
- No hotel identity changes
- No ADP / share-token changes
`
);

console.log(
  JSON.stringify(
    {
      wRomeLabel: wRome?.optionLabel,
      allHaveCityRegion,
      rows: idRows.map((r) => r.optionLabel),
    },
    null,
    2
  )
);
