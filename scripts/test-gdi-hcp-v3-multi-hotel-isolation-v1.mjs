#!/usr/bin/env node
/**
 * Multi-hotel Tab 2 commercial-profile isolation + research bridge (zero paid).
 * Uses LOCAL_SHADOW isolation only — does not mutate production Airtable customer prefs.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  saveCommercialProfile,
  loadCommercialProfile,
  loadLocalCommercialProfile,
} from "../lib/group-demand-intelligence/need-date-demand/hotel-commercial-profile-store-v1.js";
import {
  mapSavedProfileToCommercialPriorities,
  resolveHotelConfigWithSavedCommercialProfile,
} from "../lib/group-demand-intelligence/need-date-demand/hotel-saved-commercial-profile-research-bridge-v1.js";
import { buildHotelSegmentResearchPlanWithSavedProfile } from "../lib/group-demand-intelligence/workflow/hotel-segment-research-plan-v1.js";
import { selectStrategiesForCampaign } from "../lib/group-demand-intelligence/need-date-demand/need-period-strategy-selection-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { listGdiSelectableHotels } from "../lib/group-demand-intelligence/repository.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const HOTELS = [
  {
    hotelId: "recLuxvwwxID7U2B8",
    name: "Bethesda Marriott",
    marker: "HCP_V3_CERT_BETHESDA",
    dayPreference: "WEEKDAYS",
    prioritySegments: ["associations_conferences_events", "corporate_meetings_incentives"],
    weekendFocus: false,
  },
  {
    hotelId: "recnPB3Q02NO8Ma1o",
    name: "Wyndham San Diego Bayside",
    marker: "HCP_V3_CERT_WYNDHAM_SD",
    dayPreference: "WEEKENDS",
    prioritySegments: ["sports_entertainment", "social_special_events"],
    weekendFocus: true,
  },
  {
    hotelId: "recgMYovrrZDJMqzX",
    name: "Waterstone Resort & Marina",
    marker: "HCP_V3_CERT_WATERSTONE",
    dayPreference: "BOTH",
    prioritySegments: ["local_catering_day_meetings", "social_special_events"],
    weekendFocus: false,
  },
  {
    hotelId: "recrPQcZg7SFARRb2",
    name: "YOTEL Geneva Lake",
    marker: "HCP_V3_CERT_YOTEL_GENEVA",
    dayPreference: "WEEKDAYS",
    prioritySegments: ["corporate_meetings_incentives", "relocation_extended_stay"],
    weekendFocus: false,
  },
];

let passed = 0;
let failed = 0;
function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

async function checkAsync(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

const selectable = listGdiSelectableHotels();
check("registry_lists_cert_hotels", () => {
  for (const h of HOTELS) {
    assert.ok(
      selectable.some((x) => x.hotelId === h.hotelId),
      `missing selectable ${h.hotelId}`
    );
  }
  assert.ok(selectable.length >= 4);
});

check("tab2_js_is_original_v3", () => {
  const src = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/hotel-need-times-priorities-tab.js"),
    "utf8"
  );
  assert.match(src, /ORIGINAL UI recovered/);
  assert.match(src, /Revenue Priorities/);
  assert.match(src, /Hotel Team/);
  assert.match(src, /hcp-chip/);
  assert.match(src, /_draftsByHotelId/);
  assert.match(src, /exportDraft/);
});

check("html_links_hcp_css", () => {
  const html = fs.readFileSync(
    path.join(ROOT, "public/group-demand-intelligence.html"),
    "utf8"
  );
  assert.match(html, /hotel-commercial-priorities\.css/);
  assert.match(html, /dealality-intake-form-shell\.css/);
});

check("bethesda_customer_feed_still_19", () => {
  const doc = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json"
      ),
      "utf8"
    )
  );
  const facing = filterCustomerFacingOpportunities(doc.opportunities || [], {
    hotelId: "recLuxvwwxID7U2B8",
  });
  assert.equal(facing.length, 19);
});

{
  const { assertNeedDatesHotelAccess } = await import(
    "../lib/group-demand-intelligence/need-date-demand/need-dates-portal-auth-v1.js"
  );
  check("cross_hotel_write_denied_by_auth", () => {
    let denied = false;
    try {
      assertNeedDatesHotelAccess(
        {
          dealalityUser: {
            authorizedHotelIds: ["recLuxvwwxID7U2B8"],
            email: "hotel-a@dealality.test",
          },
        },
        "recnPB3Q02NO8Ma1o",
        { write: true }
      );
    } catch (err) {
      denied =
        err.status === 403 || err.code === "HOTEL_ACCESS_DENIED";
    }
    assert.equal(denied, true);
  });
}

const baselines = new Map();
const localDir = path.join(
  ROOT,
  "data/group-demand-intelligence/hotel-commercial-priorities"
);

for (const h of HOTELS) {
  await checkAsync(`save_local_isolated_${h.hotelId}`, async () => {
    const localPath = path.join(localDir, `${h.hotelId}.json`);
    baselines.set(
      h.hotelId,
      fs.existsSync(localPath) ? fs.readFileSync(localPath, "utf8") : null
    );
    const before = await loadCommercialProfile(h.hotelId, {
      useAirtable: false,
    });
    const saved = await saveCommercialProfile(
      h.hotelId,
      {
        hotelName: h.name,
        dayPreference: h.dayPreference,
        prioritySegments: h.prioritySegments,
        revenuePriorities: ["GROUP_ROOM_NIGHTS"],
        demandGeneratorCategories: [h.marker],
        restrictionNotes: `${h.marker} isolated cert — safe to delete`,
        isSyntheticTest: true,
        provenance: "DEALALITY_TEST",
      },
      {
        useAirtable: false,
        reason: h.marker,
        actorUser: { id: "hcp_v3_cert", email: "cert@dealality.test" },
      }
    );
    assert.equal(saved.ok, true);
    const loaded = await loadCommercialProfile(h.hotelId, { useAirtable: false });
    assert.equal(loaded.dayPreference, h.dayPreference);
    assert.ok(
      (loaded.demandGeneratorCategories || []).includes(h.marker) ||
        (loaded.restrictionNotes || "").includes(h.marker)
    );
    assert.ok(before);
  });
}

await checkAsync("no_cross_hotel_contamination", async () => {
  for (const a of HOTELS) {
    const loaded = await loadCommercialProfile(a.hotelId, { useAirtable: false });
    for (const b of HOTELS) {
      if (a.hotelId === b.hotelId) continue;
      assert.ok(
        !(loaded.restrictionNotes || "").includes(b.marker),
        `${a.hotelId} leaked ${b.marker}`
      );
      assert.ok(
        !(loaded.demandGeneratorCategories || []).includes(b.marker),
        `${a.hotelId} category leaked ${b.marker}`
      );
    }
  }
});

await checkAsync("research_bridge_soft_signals_differ_by_hotel", async () => {
  const plans = {};
  for (const h of HOTELS) {
    const profile = await loadCommercialProfile(h.hotelId, { useAirtable: false });
    const mapped = mapSavedProfileToCommercialPriorities(profile);
    assert.ok(mapped, `mapped ${h.hotelId}`);
    assert.equal(mapped.softSignalOnly, true);
    const plan = await buildHotelSegmentResearchPlanWithSavedProfile({
      hotelId: h.hotelId,
      commercialProfile: profile,
      useAirtable: false,
      opportunities: [],
    });
    plans[h.hotelId] = plan;
    assert.equal(plan.savedCommercialProfileApplied, true);
    assert.equal(
      plan.hotelProfile.commercialPrioritiesSource,
      "HOTEL_CONFIRMED_TAB2_PROFILE"
    );
  }
  const beth = await resolveHotelConfigWithSavedCommercialProfile(
    "recLuxvwwxID7U2B8",
    {},
    {
      commercialProfile: await loadCommercialProfile("recLuxvwwxID7U2B8", {
        useAirtable: false,
      }),
    }
  );
  const wyn = await resolveHotelConfigWithSavedCommercialProfile(
    "recnPB3Q02NO8Ma1o",
    {},
    {
      commercialProfile: await loadCommercialProfile("recnPB3Q02NO8Ma1o", {
        useAirtable: false,
      }),
    }
  );
  assert.notEqual(
    beth.mappedCommercialPriorities.weekendFocus,
    wyn.mappedCommercialPriorities.weekendFocus
  );
});

await checkAsync("strategy_selection_uses_profile_soft_boost", async () => {
  const profile = await loadCommercialProfile("recLuxvwwxID7U2B8", {
    useAirtable: false,
  });
  const plan = selectStrategiesForCampaign({
    hotelId: "recLuxvwwxID7U2B8",
    commercialProfile: profile,
    needPeriods: [
      {
        needPeriodId: "np_cert_test",
        startDate: "2027-03-01",
        endDate: "2027-03-31",
        dayPreference: "weekdays",
        priority: "HIGH",
      },
    ],
  });
  assert.equal(plan.commercialProfileApplied, true);
});

// Restore baselines (best-effort local file restore — no Airtable writes)
for (const h of HOTELS) {
  await checkAsync(`cleanup_restore_${h.hotelId}`, async () => {
    const localPath = path.join(localDir, `${h.hotelId}.json`);
    const priorRaw = baselines.get(h.hotelId);
    if (priorRaw == null) {
      if (fs.existsSync(localPath)) fs.unlinkSync(localPath);
    } else {
      fs.writeFileSync(localPath, priorRaw);
    }
    // Cert marker must not remain after cleanup when prior had none
    const after = loadLocalCommercialProfile(h.hotelId);
    if (priorRaw == null) {
      assert.ok(
        !after ||
          !(after.restrictionNotes || "").includes(h.marker)
      );
    } else {
      assert.ok(after);
      assert.ok(!(after.restrictionNotes || "").includes(h.marker));
    }
  });
}

console.log(
  JSON.stringify(
    {
      suite: "gdi-hcp-v3-multi-hotel-isolation-v1",
      selectableHotels: selectable.length,
      certHotels: HOTELS.map((h) => h.hotelId),
      passed,
      failed,
    },
    null,
    2
  )
);
process.exit(failed ? 1 : 0);
