/**
 * Contact Intelligence V1 — frozen 100-hotel benchmark cohort.
 * Split by owner group into development vs held-out (not random hotel split).
 */

export const CONTACT_BENCHMARK_100_VERSION = "contact-benchmark-100-v1";
/** Audit correction metadata — original v1 list preserved; see docs/...-v1.1-benchmark-audit.md */
export const CONTACT_BENCHMARK_100_AUDIT_VERSION = "contact-benchmark-100-v1.1-audit";

/**
 * Frozen owner groups. Development groups may be used to tune methods;
 * held-out groups are evaluation-only until gates pass.
 */
export const BENCHMARK_OWNER_GROUPS = Object.freeze({
  development: Object.freeze(["gsf", "dovetail", "alliance", "hnf", "dev_synth_a", "dev_synth_b"]),
  held_out: Object.freeze(["held_synth_c", "held_synth_d", "held_synth_e", "held_synth_f"]),
});

const SLICE_SEED = [
  ["recUNycnMwOVFX0hc", "Krystal Grand Puerto Vallarta", "gsf", "dle_06G6AB1VK0BCCD94DNN7W8DRWZ"],
  ["recIwaP1etgx2g9nA", "Cambridge Beaches Resort & Spa", "dovetail", "ent_dovetail-hospitality"],
  ["recsYJb2R1jarPpK3", "Sheraton Guadalajara Expo", "hnf", "ent_inmobiliaria-hnf"],
  ["recTYaiA4S6fR6ixx", "Real Inn Cancún", "alliance", "ent_alliance-hotel-management"],
  ["recGZZCek9vDQGG1L", "voco Guadalajara Expo", "alliance", "ent_alliance-hotel-management"],
  ["rec79Xs4mZkuiWnuN", "Real Inn Ciudad Juárez", "alliance", "ent_alliance-hotel-management"],
  ["recFspIiglYxJp1N1", "Real Inn San Luis Potosí", "alliance", "ent_alliance-hotel-management"],
  ["recogJrXdZHRV06Bl", "Real Inn Nuevo Laredo", "alliance", "ent_alliance-hotel-management"],
  ["recZxCHVNG0bDQhfG", "Real Inn Torreón", "alliance", "ent_alliance-hotel-management"],
  ["recSliceTenGsfOp01", "GSF Operated Portfolio Example", "gsf", "dle_06G6AB1VK0BCCD94DNN7W8DRWZ"],
];

function synthHotel(i, ownerGroup, split) {
  const ownerId = `ent_bench_${ownerGroup}`;
  const hasHotel = i % 3 !== 0;
  const hasOrg = i % 4 !== 0;
  const hasPerson = i % 5 === 0;
  return {
    hotel_id: `recBench100_${String(i).padStart(3, "0")}`,
    hotel_name: `Benchmark Hotel ${i}`,
    owner_group: ownerGroup,
    owner_entity_id: ownerId,
    split,
    labels: {
      expect_hotel_contact: hasHotel,
      expect_org_route: hasOrg,
      expect_person: hasPerson,
      gold_attribution_forbidden: ["INFERRED_AS_OFFICIAL", "SWITCHBOARD_AS_DIRECT_PERSONAL"],
    },
  };
}

function buildFrozen100() {
  const hotels = [];

  for (const [hotel_id, hotel_name, owner_group, owner_entity_id] of SLICE_SEED) {
    hotels.push({
      hotel_id,
      hotel_name,
      owner_group,
      owner_entity_id,
      split: "development",
      labels: {
        expect_hotel_contact: ["recUNycnMwOVFX0hc", "recIwaP1etgx2g9nA"].includes(hotel_id),
        expect_org_route: true,
        expect_person: hotel_id === "recUNycnMwOVFX0hc",
        gold_attribution_forbidden: ["INFERRED_AS_OFFICIAL", "SWITCHBOARD_AS_DIRECT_PERSONAL"],
      },
    });
  }

  // Fill development synth to 60
  let i = 1;
  while (hotels.filter((h) => h.split === "development").length < 60) {
    const group = BENCHMARK_OWNER_GROUPS.development[i % BENCHMARK_OWNER_GROUPS.development.length];
    hotels.push(synthHotel(100 + i, group, "development"));
    i += 1;
  }

  // Held-out 40
  i = 1;
  while (hotels.filter((h) => h.split === "held_out").length < 40) {
    const group = BENCHMARK_OWNER_GROUPS.held_out[i % BENCHMARK_OWNER_GROUPS.held_out.length];
    hotels.push(synthHotel(200 + i, group, "held_out"));
    i += 1;
  }

  if (hotels.length !== 100) {
    throw new Error(`benchmark_cohort_must_be_100_got_${hotels.length}`);
  }
  return Object.freeze(hotels.map((h) => Object.freeze(h)));
}

export const CONTACT_BENCHMARK_100_HOTELS = buildFrozen100();

export function getBenchmark100Cohort() {
  const development = CONTACT_BENCHMARK_100_HOTELS.filter((h) => h.split === "development");
  const held_out = CONTACT_BENCHMARK_100_HOTELS.filter((h) => h.split === "held_out");
  const classified = CONTACT_BENCHMARK_100_HOTELS.map((h) => {
    let data_class = "SYNTHETIC_LABEL_HARNESS";
    if (h.hotel_id === "recSliceTenGsfOp01") data_class = "SLICE_PLACEHOLDER";
    else if (/^rec[A-Za-z0-9]{14}$/.test(h.hotel_id)) data_class = "REAL_AIRTABLE";
    return { ...h, data_class };
  });
  return {
    version: CONTACT_BENCHMARK_100_VERSION,
    audit_version: CONTACT_BENCHMARK_100_AUDIT_VERSION,
    total: CONTACT_BENCHMARK_100_HOTELS.length,
    development_count: development.length,
    held_out_count: held_out.length,
    real_airtable_count: classified.filter((h) => h.data_class === "REAL_AIRTABLE").length,
    synthetic_count: classified.filter((h) => h.data_class === "SYNTHETIC_LABEL_HARNESS").length,
    slice_placeholder_count: classified.filter((h) => h.data_class === "SLICE_PLACEHOLDER").length,
    owner_groups: BENCHMARK_OWNER_GROUPS,
    hotels: classified,
    development: classified.filter((h) => h.split === "development"),
    held_out: classified.filter((h) => h.split === "held_out"),
    evaluation_class: "A_SOFTWARE_REGRESSION_FIXTURES_PRIMARY",
    independent_precision: "NOT_REVIEWED",
    network_discovery: false,
    notes: [
      "V1.1 audit: 90/100 IDs are synthetic labeled harness rows; do not treat precision as live discovery quality.",
      "Preserved original report: reports/contact-intelligence-benchmark-100-v1-original-preserved.json",
    ],
  };
}
