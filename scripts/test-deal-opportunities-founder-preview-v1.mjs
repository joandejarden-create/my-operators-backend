#!/usr/bin/env node
import {
  applyDealOpportunitiesFounderPreview,
  CM_PERMANENT_QUALIFIED_HOTELS,
  isDealOpportunitiesFounderPreviewRequested,
} from "../lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-founder-preview-v1.js";

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    process.exitCode = 1;
  } else {
    console.log("OK:", msg);
  }
}

const fixtures = [
  {
    id: "find_ds_cm_v2_voir_beach_brand_open_2025",
    name: "Voir Beach Resort — Cable Beach",
    group: "qualified",
    kind: "BCT",
    country: "BS",
  },
  {
    id: "find_ds_cm_v2_baha_mar_brand_open_2026",
    name: "Baha Mar — fourth hotel",
    group: "watch",
    kind: "BCT",
    country: "BS",
    status: "Historical — committed",
  },
  {
    id: "find_ds_cm_v3_oromana_mct_open_2026",
    name: "Hotel Oromana",
    group: "watch",
    kind: "MCT",
    country: "ES",
  },
  {
    id: "find_ds_cm_v3_agueda_operator_open_2026",
    name: "Águeda Passivhaus eco-hotel",
    group: "watch",
    kind: "MCT",
    country: "PT",
  },
  {
    id: "find_tb_cb_cb_gap_areia_branca_vortal",
    name: "Antiga Colónia Balnear da Areia Branca (REVIVE)",
    group: "watch",
    kind: "MCT",
    country: "PT",
  },
  {
    id: "find_cb2_hotel_do_mel_gois_operator_2026",
    name: "Hotel do Mel – Casa da Costeira",
    group: "watch",
    kind: "OTHER",
    country: "PT",
  },
  {
    id: "find_ds_cm_v4_leaburu_mct_open_2026",
    name: "Ostatu de Leaburu — municipal hostelero",
    group: "watch",
    kind: "OTHER",
    country: "ES",
  },
  {
    id: "find_ds_cm_v4_santa_eulalia_mct_open_2026",
    name: "Santa Eulalia del Campo — 12-room alojamiento",
    group: "watch",
    kind: "OTHER",
    country: "ES",
  },
  {
    id: "find_ds_cm_v4_pau_dalho_concession_open_2026",
    name: "Revive Brasil — Fazenda Pau D'Alho",
    group: "watch",
    kind: "OTHER",
    country: "BR",
    status: "Historical — committed",
  },
  {
    id: "noise_watch",
    name: "Unrelated watch hotel",
    group: "watch",
    kind: "BCT",
    country: "US",
  },
];

assert(CM_PERMANENT_QUALIFIED_HOTELS.length === 9, "baseline hotel count is 9");
assert(
  isDealOpportunitiesFounderPreviewRequested({ founderPreview: "1" }),
  "founderPreview=1 requested",
);
assert(
  !isDealOpportunitiesFounderPreviewRequested({}),
  "default does not request preview",
);

const off = applyDealOpportunitiesFounderPreview(fixtures, {
  requested: true,
  authorized: false,
});
assert(!off.preview.enabled, "unauthorized preview stays disabled");
assert(
  off.opportunities.filter((o) => o.group === "qualified").length === 1,
  "unauthorized leaves only natural Qualified",
);

const on = applyDealOpportunitiesFounderPreview(fixtures, {
  requested: true,
  authorized: true,
});
assert(on.preview.enabled, "authorized+requested enables preview");
assert(on.preview.classifierUnchanged === true, "classifier marked unchanged");
assert(
  on.preview.publicationState === "CUSTOMER_BLOCKED",
  "publication remains CUSTOMER_BLOCKED",
);
assert(on.preview.matchedHotels.length === 9, "all 9 hotels matched");
assert(on.preview.missingHotels.length === 0, "no missing hotels");

const q = on.opportunities.filter((o) => o.group === "qualified");
assert(q.length === 9, "exactly 9 Qualified after preview (noise stays watch)");
assert(
  q.every((o) => o.publicationState === "CUSTOMER_BLOCKED" || o.founderPreview),
  "promoted rows remain CUSTOMER_BLOCKED / founderPreview flagged",
);
assert(
  on.opportunities.find((o) => o.id === "noise_watch").group === "watch",
  "unrelated opportunities not promoted",
);
assert(
  on.opportunities.find((o) => /Hotel do Mel/.test(o.name)).kind === "MCT",
  "OTHER Mel promoted with MCT kind hint",
);

if (process.exitCode) {
  console.error("\nFounder preview tests FAILED");
  process.exit(1);
}
console.log("\nAll founder preview tests passed.");
