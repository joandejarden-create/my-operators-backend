import test from "node:test";
import assert from "node:assert/strict";

import {
  assertRoomsSourcePolicy,
  classifyNullFill,
  FILL_CLASS,
  buildPropertyFundamentalsPatch,
  scorePropertyUsefulness,
  MAP_PF,
} from "../lib/research-engine-v2/property-fundamentals-enrichment-v1.js";

test("HBX rooms[] cannot pass rooms source policy", () => {
  const r = assertRoomsSourcePolicy({
    source_kind: "hbx_rooms_array",
    method: "hbx_rooms_array",
    from_hbx_rooms_array: true,
    count: 120,
  });
  assert.equal(r.ok, false);
  assert.ok(r.blockers.some((b) => /HBX/.test(b)));
  assert.equal(r.HBX_ROOMS_ARRAY_WRITES, 0);
});

test("Cvent-alone cannot pass rooms source policy", () => {
  const r = assertRoomsSourcePolicy({
    source_kind: "cvent_alone",
    from_cvent_only: true,
    count: 80,
  });
  assert.equal(r.ok, false);
  assert.ok(r.blockers.some((b) => /cvent/i.test(b)));
});

test("NULL_FILL does not overwrite populated values", () => {
  assert.equal(classifyNullFill("", 120).class, FILL_CLASS.NULL_FILL);
  assert.equal(classifyNullFill("", 120).write, true);
  assert.equal(classifyNullFill(100, 120).class, FILL_CLASS.CONFLICT_REVIEW);
  assert.equal(classifyNullFill(100, 120).write, false);
  assert.equal(classifyNullFill(100, 100).class, FILL_CLASS.CONFIRMED_EXISTING);
  assert.equal(classifyNullFill(100, 100).write, false);
});

test("buildPropertyFundamentalsPatch blocks HBX rooms and null-fills High official", () => {
  const rec = {
    id: "recTEST",
    fields: {
      [MAP_PF.propertyName]: "Test Hotel",
      [MAP_PF.country]: "Mexico",
      [MAP_PF.city]: "Cancún",
      [MAP_PF.roomsKeys]: null,
      [MAP_PF.phone]: null,
    },
  };
  const blocked = buildPropertyFundamentalsPatch(rec, {
    rooms: {
      count: 99,
      confidence: "High",
      from_hbx_rooms_array: true,
      source_kind: "hbx_rooms_array",
    },
  });
  assert.equal(blocked.patch[MAP_PF.roomsKeys], undefined);
  assert.equal(blocked.HBX_ROOMS_ARRAY_WRITES, 0);

  const ok = buildPropertyFundamentalsPatch(rec, {
    rooms: {
      count: 180,
      confidence: "High",
      source_kind: "official_html",
      source_url: "https://example.com/hotels/test",
      from_hbx_rooms_array: false,
      from_cvent_only: false,
    },
    phone: "+52 998 000 0000",
  });
  assert.equal(ok.patch[MAP_PF.roomsKeys], 180);
  assert.equal(ok.patch[MAP_PF.phone], "+52 998 000 0000");
  assert.equal(ok.HBX_ROOMS_ARRAY_WRITES, 0);

  const noOverwrite = buildPropertyFundamentalsPatch(
    { ...rec, fields: { ...rec.fields, [MAP_PF.roomsKeys]: 200 } },
    {
      rooms: {
        count: 180,
        confidence: "High",
        source_kind: "official_html",
        from_hbx_rooms_array: false,
      },
    }
  );
  assert.equal(noOverwrite.patch[MAP_PF.roomsKeys], undefined);
});

test("usefulness score prefers missing rooms + official URL", () => {
  const a = scorePropertyUsefulness({
    [MAP_PF.roomsKeys]: null,
    [MAP_PF.officialUrl]: "https://hilton.com/en/hotels/mexmx-test/",
    [MAP_PF.city]: "Cancún",
    [MAP_PF.country]: "Mexico",
  });
  const b = scorePropertyUsefulness({
    [MAP_PF.roomsKeys]: 100,
    [MAP_PF.city]: "Cancún",
    [MAP_PF.country]: "Mexico",
    [MAP_PF.stateRegion]: "Quintana Roo",
    [MAP_PF.address]: "x",
    [MAP_PF.phone]: "1",
    [MAP_PF.officialUrl]: "https://hilton.com/en/hotels/mexmx-test/",
  });
  assert.ok(a > b);
});
