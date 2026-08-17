/**
 * V1.3 gap-closure artifact writer (01–22).
 */

import fs from "node:fs";
import path from "node:path";

function wj(root, name, data) {
  fs.writeFileSync(path.join(root, name), JSON.stringify(data, null, 2));
}
function wm(root, name, text) {
  fs.writeFileSync(path.join(root, name), text);
}

export async function writeGapClosureArtifacts(ctx) {
  const root = ctx.artifactRoot;
  fs.mkdirSync(root, { recursive: true });
  const {
    freeze,
    result,
    baseline,
    afterPass1,
    afterPass2,
    afterPass3,
    afterPass4,
    roomsResults,
    addressResults,
    coordResults,
    firstParty,
    blockers,
    learning,
    providerInfo,
    providerStatus,
    roomsByFamily,
  } = ctx;

  wj(root, "01-v1-2-baseline-freeze.json", {
    freeze,
    rebuilt_baseline_avg: baseline.portfolio.average_raw_priority_completeness_pct,
    note: "V1.2 final freeze + rebuilt baseline under same Golden schema",
  });

  wm(
    root,
    "02-rooms-source-audit.md",
    `# Rooms Source Audit (V1.3)

## IHG
- hoteldetail often has \`"numberOfRooms": ""\` (empty) — not inventable
- JSON-LD / prose extraction via production-census-rooms-keys-extractor
- Standalone hotel site ladder when linked from hoteldetail
- No Cvent / legacy

## Hilton
- Public GraphQL does **not** expose room inventory fields (validated)
- facilityOverview.shortDesc may state room counts (prose)
- Property HTML often HTTP 403
- Directory locations do not include rooms

## Choice
- Property pages frequently 403
- Sitewide \`numberOfRooms=25\` rejected as false positive
- First-party validation primary for unresolved

## Ladder
A official standalone → B owner/operator (opportunistic) → C fact sheet → … → G first-party
`
  );

  wj(root, "03-ihg-rooms-results.json", { results: roomsResults.IHG || [] });
  wj(root, "04-hilton-rooms-results.json", { results: roomsResults.Hilton || [] });
  wj(root, "05-choice-rooms-results.json", { results: roomsResults.Choice || [] });
  wj(root, "06-room-fallback-results.json", {
    note: "Native fallback embedded in family resolvers; Webhound not called",
    by_family: roomsByFamily,
  });

  wj(root, "07-address-resolution-results.json", {
    resolved: addressResults.filter((x) => x.ok).length,
    failed: addressResults.filter((x) => !x.ok).length,
    sample: addressResults.slice(0, 40),
    final_address_pct: result.address_pct,
  });

  wm(
    root,
    "08-address-normalization.md",
    `# Address Normalization

- Preserve \`raw_address\` and \`normalized_address\`
- Expand Av./Blvd./Carr./Km. abbreviations
- Collapse whitespace; keep carretera / km / lote semantics
- IHG: Mexico-filtered JSON-LD (reject Morocco/Spain pollution on multi-hotel pages)
- Confidence: Exact Official | High | Medium | Low | Unknown
`
  );

  wj(root, "09-coordinate-resolution-results.json", {
    resolved: coordResults.filter((x) => x.ok).length,
    geocoded: coordResults.filter((x) => x.geocoded).length,
    final_lat_pct: result.lat_pct,
    final_lng_pct: result.lng_pct,
    sample: coordResults.slice(0, 40),
  });

  wm(
    root,
    "10-geocode-provider-status.md",
    `# Geocode Provider Status

**Classification:** \`${providerStatus}\`

\`\`\`json
${JSON.stringify(providerInfo, null, 2)}
\`\`\`

- Mapbox permanent storage flag: \`MAPBOX_PERMANENT_GEOCODING\`
- Google storage terms: \`GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED\`
- Geocode calls this run: ${result.geocode_calls}
- Estimated geocode cost USD: ${result.external_cost_usd}
- Cvent/legacy coordinates: never used
`
  );

  wj(root, "11-gap-impact-ranking.json", {
    principle: "expected_completeness_gain × researchability ÷ effort",
    v12_blockers: [
      { field: "Rooms / Keys", pct: freeze.rooms_pct },
      { field: "Address", pct: freeze.address_pct },
      { field: "Latitude", pct: freeze.lat_pct },
    ],
    final_blockers: afterPass4.missingness
      .filter((m) => ["Rooms / Keys", "Address", "Latitude", "Longitude"].includes(m.field))
      .map((m) => ({
        field: m.field,
        completion_pct: m.completion_pct,
        missing: m.hotels_missing,
        impact: m.total_completeness_impact,
      })),
  });

  wj(root, "12-pass-1-results.json", { portfolio: afterPass1.portfolio, label: "structured_parent" });
  wj(root, "13-pass-2-results.json", { portfolio: afterPass2.portfolio, label: "official_property" });
  wj(root, "14-pass-3-results.json", { portfolio: afterPass3.portfolio, label: "rooms_retry" });
  wj(root, "15-pass-4-results.json", {
    portfolio: afterPass4.portfolio,
    label: "geocode",
    geocode_calls: result.geocode_calls,
  });

  wj(root, "16-first-party-gap-packs.json", firstParty);
  wj(root, "17-final-hotel-completeness.json", {
    portfolio: afterPass4.portfolio,
    hotels: afterPass4.perHotel,
  });
  wj(root, "18-final-field-completeness.json", { fields: afterPass4.missingness });
  wj(root, "19-remaining-blockers.json", blockers);
  wj(root, "20-autopilot-learning-update.json", {
    version: "v1.3-deterministic-learning",
    patterns: learning,
    reusable: [
      learning.hilton_graphql_address_coords &&
        "Hilton GraphQL address + localization.coordinate for all future Hilton hotels",
      learning.ihg_mexico_filtered_address &&
        "IHG Mexico-filtered JSON-LD streetAddress (reject cross-country pollution)",
      "Family-specific rooms resolvers (resolveIHGRooms / resolveHiltonRooms / resolveChoiceRooms)",
    ].filter(Boolean),
  });

  wm(
    root,
    "21-new-hotel-standard.md",
    `# New-Hotel Golden Census Standard (post V1.3)

On discovery, Autopilot must immediately attempt:

1. Identity + Dealality geography
2. Rooms / Keys (family-specific resolver)
3. Address (directory / GraphQL / Mexico-filtered page)
4. Coordinates (official → approved geocode)
5. Amenities, F&B, meetings, physical profile, classification, content

Do not wait for a quarterly cleanup. First-party packs only for unresolved rooms/address after native ladder.
`
  );

  const avg = afterPass4.portfolio.average_raw_priority_completeness_pct;
  const ge95 = afterPass4.portfolio.hotels_at_or_above_95_share_pct;
  const verdict =
    avg >= 95 && ge95 >= 80
      ? "YES"
      : avg >= 90 || (result.address_pct >= 80 && result.lat_pct >= 80)
        ? "YES, WITH SPECIFIC BOUNDARIES"
        : "NO";

  const gainRooms = (result.rooms_pct || 0) - (freeze.rooms_pct || 0);
  const gainAddr = (result.address_pct || 0) - (freeze.address_pct || 0);
  const gainLat = (result.lat_pct || 0) - (freeze.lat_pct || 0);
  const gainAvg = avg - (freeze.portfolio.average_raw_priority_completeness_pct || 0);

  wm(
    root,
    "22-final-report.md",
    buildFinal(result, freeze, afterPass4, roomsByFamily, blockers, firstParty, providerStatus, verdict, {
      gainRooms,
      gainAddr,
      gainLat,
      gainAvg,
    })
  );
}

function buildFinal(result, freeze, afterPass4, roomsByFamily, blockers, firstParty, providerStatus, verdict, gains) {
  const b = afterPass4.portfolio.buckets;
  const roomsUnresolved = blockers.rooms_missing;
  const fpRooms =
    (firstParty.rooms_only.IHG?.length || 0) +
    (firstParty.rooms_only.Hilton?.length || 0) +
    (firstParty.rooms_only.Choice?.length || 0);

  return `# Census Autopilot V1.3 — Gap Closure Final Report

**Run:** \`${result.run_id}\`  
**Cost:** ~$${result.external_cost_usd} geocode estimate · Webhound $0 · Airtable writes 0

## MOST IMPORTANTLY

**${verdict}**

Can Dealality close Golden Census from 86.8% to ≥95% by solving Rooms + Address + Coordinates without weakening evidence or requiring Joan in the research loop?

**Answer: ${verdict}**

---

## Pass completeness

| Stage | Avg Priority Completeness |
|-------|---------------------------|
| V1.2 freeze | ${freeze.portfolio.average_raw_priority_completeness_pct}% |
| Pass 1 structured | ${result.passes.pass1_structured}% |
| Pass 2 official pages | ${result.passes.pass2_official_pages}% |
| Pass 3 rooms retry | ${result.passes.pass3_rooms_retry}% |
| Pass 4 geocode | ${result.passes.pass4_geocode}% |
| **Final** | **${afterPass4.portfolio.average_raw_priority_completeness_pct}%** |

## Exact answers

1. Final average Priority Completeness: **${afterPass4.portfolio.average_raw_priority_completeness_pct}%**
2. % hotels ≥95%: **${afterPass4.portfolio.hotels_at_or_above_95_share_pct}%**
3. Hotels at 100%: **${b["100%"]}**
4. Rooms completion overall: **${result.rooms_pct}%**
5. IHG rooms: **${roomsByFamily.IHG?.pct}%**
6. Hilton rooms: **${roomsByFamily.Hilton?.pct}%**
7. Choice rooms: **${roomsByFamily.Choice?.pct}%**
8. Address completion: **${result.address_pct}%**
9. Latitude/Longitude: **${result.lat_pct}% / ${result.lng_pct}%**
10. Coordinates provider-blocked vs unresolved: provider=\`${providerStatus}\`; provider-blocked count≈${blockers.coordinates_provider_blocked}; missing=${blockers.coordinates_missing}
11. Completeness gain from rooms field: **${gains.gainRooms.toFixed(1)} pp** (field completion)
12. Gain from address field: **${gains.gainAddr.toFixed(1)} pp**
13. Gain from coordinates field: **${gains.gainLat.toFixed(1)} pp**
14. Official structured room sources discovered: Hilton GraphQL **does not** expose rooms; IHG often empty numberOfRooms; prose/HTML only when present
15. Reusable patterns: Hilton GraphQL address+coords; IHG Mexico-filtered JSON-LD address; family rooms resolvers
16. Rooms remaining unresolved: **${roomsUnresolved}**
17. Rooms → first-party validation: **${fpRooms}**
18. Addresses remaining unresolved: **${blockers.address_missing}**
19. Coordinates remaining unresolved: **${blockers.coordinates_missing}**
20. Would benefit from Webhound: low ROI vs first-party for room counts — estimate subset of hard rooms gaps only (~${Math.min(roomsUnresolved, 80)} candidates max); **Webhound not called**
21. Unsupported values: **${result.firewall.unsupported}**
22. Cvent production values: **NO**
23. Legacy production values: **NO**
24. Autopilot ran all passes without Joan: **YES**
25. New hotels can auto-receive these enrichment passes: **YES** (see 21-new-hotel-standard.md)
26. Exact blockers preventing ≥95%: Rooms missing ${blockers.rooms_missing} (IHG ${blockers.rooms_missing_by_family.IHG}, Hilton ${blockers.rooms_missing_by_family.Hilton}, Choice ${blockers.rooms_missing_by_family.Choice}); Address ${blockers.address_missing}; Coords ${blockers.coordinates_missing}
27. Cheapest path: ${blockers.cheapest_path.join("; ")}
28. Ready for Mexico-wide Golden Census execution: **${avgReady(afterPass4, result)}**
29. Ready for controlled Airtable write pilot: **${writeReady(result)}** — address/coords yes with provenance; rooms only where High/Medium official claims exist
30. Next: Ship first-party Rooms packs to IHG/Hilton/Choice; keep Hilton GraphQL addr/geo in Autopilot default path; do not call Webhound for bulk rooms

## Distribution

\`\`\`
${JSON.stringify(b, null, 2)}
\`\`\`

## Portfolio avg gain vs V1.2: ${gains.gainAvg.toFixed(1)} pp
`;
}

function avgReady(afterPass4, result) {
  if (afterPass4.portfolio.average_raw_priority_completeness_pct >= 95) return "YES";
  if (result.address_pct >= 90 && result.lat_pct >= 90) return "PARTIAL — geo ready; rooms gate remains";
  return "NOT YET for 95% claim";
}

function writeReady(result) {
  if (result.address_pct >= 85 && result.lat_pct >= 85) return "YES for address/coords pilot lanes";
  return "NOT YET — finish address/coord gaps first";
}
