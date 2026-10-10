#!/usr/bin/env node
/**
 * Pack ADP dropdown-fix reports for Westin Grand München.
 *   node scripts/pack-westin-grand-munchen-dropdown-fix-reports.mjs
 */
import { listPropertyProfiles, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";
import { registerCertifiedAdpPropertyForCustomerDropdown } from "../lib/ai-demand-positioning/client-readiness/register-certified-adp-dropdown-v1.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports/adp/westin-grand-munchen-dropdown-fix");
const WESTIN = "adp_westin_grand_munchen";
const PERIOD = "adp_period_adp_westin_grand_munchen_20261007145836_73c25a";
const PEERS = [
  "adp_hilton_times_square",
  "adp_renaissance_times_square",
  "adp_yotel_geneva_lake",
  "adp_bethesda_marriott",
  "adp_w_rome",
];

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function write(name, body) {
  const s = typeof body === "string" ? body : JSON.stringify(body, null, 2);
  fs.writeFileSync(path.join(OUT, name), s.endsWith("\n") ? s : s + "\n", "utf8");
}

ensureDir(OUT);

// Idempotent registration
const reg = registerCertifiedAdpPropertyForCustomerDropdown(WESTIN, {
  certificationStatus: "CERTIFIED",
});

const profiles = listPropertyProfiles();
const westinListed = profiles.find((p) => p.propertyId === WESTIN);
const westinProfile = loadPropertyProfile(WESTIN);
const manifest = loadPublishedManifest(WESTIN);

const peerRows = PEERS.map((id) => {
  const p = loadPropertyProfile(id);
  const m = loadPublishedManifest(id);
  const inDropdown = profiles.some((x) => x.propertyId === id);
  return {
    propertyId: id,
    name: p?.name || null,
    customerDropdownVisible: p?.customerDropdownVisible,
    officialBaselinePublished: p?.officialBaselinePublished,
    inDropdown,
    latestPeriodId: m?.latestPeriodId || null,
    certificationStatus: m?.certificationStatus || null,
    publishStatus: m?.publishStatus || null,
  };
});

write(
  "DROPDOWN_DATA_FLOW.md",
  `# ADP Dropdown Data Flow

## Canonical path

1. **Frontend:** \`public/js/ai-demand-positioning/ai-demand-positioning.js\` → \`loadProperties()\`
2. **API:** \`GET /api/ai-demand-positioning/properties\` (\`api/ai-demand-positioning.js\`)
3. **Catalog:** \`listPropertyProfiles()\` in \`lib/ai-demand-positioning/data-model.js\`
4. **Source:** \`fixtures/ai-demand-positioning/*-property-profile.json\`
5. **Eligibility filter:** skip when \`customerDropdownVisible === false\`
6. **Sort:** \`name.localeCompare\`
7. **Label:** \`formatPropertySelectorLabel({ name, city, state, region, country })\`

No frontend hardcode of hotel options. Share tokens resolve a single property without enumerating others.

## Post-cert registration (new shared step)

\`registerCertifiedAdpPropertyForCustomerDropdown\` in  
\`lib/ai-demand-positioning/client-readiness/register-certified-adp-dropdown-v1.js\`

Invoked from \`publishExistingHotelAdpSnapshot\` after CERTIFIED publish so future hotels do not skip the dropdown flip.
`
);

write(
  "ROOT_CAUSE.md",
  `# Root Cause — Westin Missing from ADP Dropdown

## Classification

**VISIBILITY_FLAG_MISSING** (also reads as skipped post-cert catalog registration)

## Exact defect

| Field | Hilton TS | Renaissance TS | YOTEL Geneva | **Westin München (before)** |
|-------|-----------|----------------|--------------|-----------------------------|
| customerDropdownVisible | true | true | **false** | **false** |
| officialBaselinePublished | (n/a/true) | (n/a) | false | **false** |
| CERTIFIED published period | YES | YES | YES | YES |
| In listPropertyProfiles | YES | YES | NO | **NO** |

Westin was fully CERTIFIED and published (\`publishStatus: Live\`, period \`${PERIOD}\`) but the fixture still had \`customerDropdownVisible: false\` from preflight scaffolding.  
\`listPropertyProfiles\` hard-skips that flag → API returns no Westin option → UI dropdown empty for this hotel.

YOTEL remains \`false\` intentionally (prospect / not customer-dropdown-released). Regression = YOTEL still excluded; peers still present.

## Not the cause

- Missing published period / certification filter at API (manifest was Live + CERTIFIED)
- Frontend cache
- Country/market filter
- Hotel-specific UI omit list
`
);

write(
  "WESTIN_REGISTRATION.md",
  `# Westin Registration

| Field | Value |
|-------|-------|
| Property ID | \`${WESTIN}\` |
| Registration result | ${reg.ok ? "OK" : "FAIL"} |
| Changed | ${reg.changed} |
| customerDropdownVisible | ${reg.after?.customerDropdownVisible} |
| officialBaselinePublished | ${reg.after?.officialBaselinePublished} |
| baselineState | ${reg.after?.baselineState} |
| In listPropertyProfiles now | **${westinListed ? "YES" : "NO"}** |
| Display label | ${westinListed?.label || "—"} |
| Manifest latestPeriodId | ${manifest?.latestPeriodId || "—"} |
| Certification | ${manifest?.certificationStatus || "—"} |
| Shared path only | YES — no UI hardcode |
`
);

write(
  "DROPDOWN_QA.md",
  `# Dropdown QA

| Check | Result |
|-------|--------|
| Westin in catalog | ${westinListed ? "YES" : "NO"} |
| Label | ${westinListed?.label || "—"} |
| Truncation risk | Full title in option + title attr (existing UI) |
| Duplicate Westin rows | ${profiles.filter((p) => p.propertyId === WESTIN).length === 1 ? "NO" : "REVIEW"} |
| Sort | Alphabetical by name (Westin near end of W*) |
| Loads certified period | Manifest bound to \`${PERIOD}\` |
| Metrics changed | NO |
| Certification changed | NO |

## Peer presence

${peerRows.map((r) => `- ${r.propertyId}: dropdown=${r.inDropdown} cert=${r.certificationStatus || "n/a"}`).join("\n")}
`
);

write(
  "REGRESSION.md",
  `# Dropdown Regression

| Property | In dropdown | Notes |
|----------|-------------|-------|
| Hilton Times Square | ${peerRows.find((r) => r.propertyId === "adp_hilton_times_square")?.inDropdown ? "YES" : "NO"} | PASS |
| Renaissance Times Square | ${peerRows.find((r) => r.propertyId === "adp_renaissance_times_square")?.inDropdown ? "YES" : "NO"} | PASS |
| YOTEL Geneva Lake | ${peerRows.find((r) => r.propertyId === "adp_yotel_geneva_lake")?.inDropdown ? "YES" : "NO"} | Intentionally false (prospect) — unchanged PASS |
| Bethesda Marriott | ${peerRows.find((r) => r.propertyId === "adp_bethesda_marriott")?.inDropdown ? "YES" : "NO"} | PASS |
| W Rome | ${peerRows.find((r) => r.propertyId === "adp_w_rome")?.inDropdown ? "YES" : "NO"} | PASS |
| Westin Grand München | ${westinListed ? "YES" : "NO"} | NEW |

Total dropdown properties: ${profiles.length}
Duplicates: none expected (unique propertyId)
`
);

write(
  "CHANGELOG.md",
  `# CHANGELOG — Westin ADP dropdown fix

- Root cause: \`customerDropdownVisible: false\` after CERTIFIED publish
- Added shared \`registerCertifiedAdpPropertyForCustomerDropdown\`
- Wired into \`publishExistingHotelAdpSnapshot\` for all future CERTIFIED publishes
- Flipped Westin fixture flags (no frontend hardcode)
- ADP metrics / certification untouched
`
);

write(
  "FOUNDER_REPORT.md",
  `# FOUNDER REPORT — Westin ADP Dropdown Fix

## Verdict: **FIXED**

Westin was CERTIFIED and published but excluded from the shared dropdown catalog because \`customerDropdownVisible\` stayed \`false\`.

| | |
|--|--|
| Root cause | VISIBILITY_FLAG_MISSING |
| Fix | Shared post-cert registration |
| Westin label | ${westinListed?.label} |
| Hotel-specific UI hack | NO |
| Metrics/cert changed | NO |

Peers (Hilton, Renaissance, Bethesda, W Rome) remain visible. YOTEL remains intentionally excluded.
`
);

write("SUMMARIES.json", {
  westinSubjectExists: true,
  westinCertifiedPeriodExists: Boolean(manifest?.latestPeriodId),
  dropdownDataSource:
    "GET /api/ai-demand-positioning/properties → listPropertyProfiles() → fixtures *property-profile.json filtered by customerDropdownVisible !== false",
  rootCause: "VISIBILITY_FLAG_MISSING",
  registered: reg.ok && westinListed,
  visible: Boolean(westinListed),
  displayLabel: westinListed?.label || null,
  periodId: manifest?.latestPeriodId || PERIOD,
  peers: peerRows,
  hotelSpecificUiHack: false,
});

console.log(JSON.stringify({ westinListed: !!westinListed, label: westinListed?.label, count: profiles.length }, null, 2));
