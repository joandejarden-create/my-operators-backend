#!/usr/bin/env node
/**
 * Founder/admin owner-app inventory must include canonical ADP hotels
 * independently of customerDropdownVisible.
 * npm run test:adp-owner-app-admin-inventory-v1
 */

import "../load-env.js";
import assert from "assert";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { listPropertyProfiles } from "../lib/ai-demand-positioning/data-model.js";
import { CANONICAL_ADP_31_PROPERTY_IDS } from "../lib/ai-demand-positioning/governance/canonical-adp-31-property-scope-v1.js";
import {
  listInternalOwnerAppPropertyInventory,
  resolveOwnerAppReportReadiness,
  assessOwnerAppReportArtifacts,
} from "../lib/ai-demand-positioning/share/adp-owner-app-admin-inventory-v1.js";
import {
  resolveOwnerAppPropertyAccess,
  filterPropertiesForOwnerApp,
  ownerAppCanAccessProperty,
} from "../lib/ai-demand-positioning/share/adp-owner-app-property-access-v1.js";

const OUT = join(
  process.cwd(),
  "reports/ai-demand-positioning/owner-app-admin-inventory-v1"
);
const results = {
  stamp: new Date().toISOString(),
  gates: {},
  counts: {},
};

function mark(gate, pass, detail = null) {
  results.gates[gate] = pass ? "PASS" : "FAIL";
  if (!pass) throw new Error(`${gate}${detail ? `: ${detail}` : ""}`);
}

function main() {
  mkdirSync(OUT, { recursive: true });

  const customerVisible = listPropertyProfiles();
  const adminInventory = listInternalOwnerAppPropertyInventory();
  const adminIds = adminInventory.map((p) => p.propertyId);

  results.counts = {
    customerDropdownVisible: customerVisible.length,
    adminInternalInventory: adminInventory.length,
    canonical31: CANONICAL_ADP_31_PROPERTY_IDS.length,
  };

  // Admin inventory must include every canonical 31 hotel.
  const missingCanonical = CANONICAL_ADP_31_PROPERTY_IDS.filter(
    (id) => !adminIds.includes(id)
  );
  mark(
    "ADMIN_INVENTORY_INCLUDES_CANONICAL_31",
    missingCanonical.length === 0,
    missingCanonical.join(",")
  );

  // Sheraton + Wyndham must appear even if customer surface shrinks.
  mark(
    "SHERATON_IN_ADMIN_INVENTORY",
    adminIds.includes("adp_sheraton_mallorca_arabella_golf")
  );
  mark(
    "WYNDHAM_IN_ADMIN_INVENTORY",
    adminIds.includes("adp_wyndham_san_diego_bayside")
  );
  mark(
    "BETHESDA_IN_ADMIN_INVENTORY",
    adminIds.includes("adp_bethesda_marriott")
  );
  mark(
    "YOTEL_IN_ADMIN_INVENTORY_DESPITE_CUSTOMER_HIDDEN",
    adminIds.includes("adp_yotel_geneva_lake")
  );

  const customerIds = new Set(customerVisible.map((p) => p.propertyId));
  mark(
    "YOTEL_EXCLUDED_FROM_CUSTOMER_SURFACE",
    !customerIds.has("adp_yotel_geneva_lake")
  );

  const canonicalRows = adminInventory.filter((p) => p.canonicalAdp31Scope === true);
  const outsideRows = adminInventory.filter((p) => p.canonicalAdp31Scope === false);
  mark("CANONICAL_31_ROW_COUNT", canonicalRows.length === 31, String(canonicalRows.length));
  mark(
    "TORONTO_MARKED_OUTSIDE_CANONICAL_31",
    outsideRows.some(
      (p) =>
        p.propertyId === "adp_radisson_blu_toronto_downtown" &&
        p.executionScope === "INTERNAL_ADDITIONAL_OUTSIDE_CANONICAL_31"
    )
  );
  mark(
    "TORONTO_LABEL_MARKS_OUTSIDE_SCOPE",
    String(
      adminInventory.find((p) => p.propertyId === "adp_radisson_blu_toronto_downtown")
        ?.label || ""
    ).includes("Outside canonical 31")
  );

  // Aimbridge CALA — outside canonical 31, founder/admin only
  const aimbridgeIds = [
    "adp_sheraton_guadalajara_expo",
    "adp_wyndham_alltra_punta_cana",
    "adp_voco_cancun_zona_hotelera",
  ];
  for (const id of aimbridgeIds) {
    const row = adminInventory.find((p) => p.propertyId === id);
    mark(`AIMBRIDGE_IN_ADMIN_${id}`, Boolean(row));
    mark(
      `AIMBRIDGE_OUTSIDE_CANONICAL_${id}`,
      row?.canonicalAdp31Scope === false &&
        row?.executionScope === "INTERNAL_ADDITIONAL_OUTSIDE_CANONICAL_31"
    );
    mark(
      `AIMBRIDGE_NOT_CUSTOMER_SURFACE_${id}`,
      !customerIds.has(id)
    );
    const art = assessOwnerAppReportArtifacts(id);
    // CURRENT pointer alone must not force "available" without a snapshot report.
    if (art.hasCurrentPointer && !art.hasDurableSnapshotReport && !art.stagedStoreVersionId) {
      mark(
        `AIMBRIDGE_POINTER_ONLY_NOT_AVAILABLE_${id}`,
        resolveOwnerAppReportReadiness(id, row) === "incomplete"
      );
    } else if (art.hasDurableSnapshotReport) {
      mark(
        `AIMBRIDGE_SNAPSHOT_AVAILABLE_${id}`,
        resolveOwnerAppReportReadiness(id, row) === "available" &&
          row?.reportReadSource === "durable_snapshot" &&
          row?.presentationReady === false
      );
    }
    mark(
      `AIMBRIDGE_OUTSTANDING_WORK_${id}`,
      Array.isArray(row?.outstandingWork) && row.outstandingWork.length > 0
    );
  }
  const alltra = adminInventory.find(
    (p) => p.propertyId === "adp_wyndham_alltra_punta_cana"
  );
  mark(
    "ALLTRA_DENOMINATOR_FLAGGED",
    Array.isArray(alltra?.outstandingWork) &&
      alltra.outstandingWork.includes("MEASUREMENT_DENOMINATOR_RECONCILIATION")
  );
  mark(
    "ADMIN_INVENTORY_35_WITH_AIMBRIDGE",
    adminInventory.length === 35,
    String(adminInventory.length)
  );
  // Canonical execution cohort must not silently expand.
  mark(
    "NO_EXTRA_CANONICAL_FLAGS",
    outsideRows.every((p) => !CANONICAL_ADP_31_PROPERTY_IDS.includes(p.propertyId))
  );

  // Admin access path
  const adminReq = {
    memberstackEmail: "joandejarden@hotmail.com",
    dealalityUser: { isAdmin: false, email: "joandejarden@hotmail.com" },
  };
  const adminAccess = resolveOwnerAppPropertyAccess(adminReq);
  assert.ok(adminAccess.isAdmin);
  mark(
    "ADMIN_ALLOWED_INCLUDES_SHERATON_WYNDHAM",
    adminAccess.allowedPropertyIds.includes("adp_sheraton_mallorca_arabella_golf") &&
      adminAccess.allowedPropertyIds.includes("adp_wyndham_san_diego_bayside")
  );
  mark(
    "ADMIN_ALLOWED_COVERS_CANONICAL_31",
    CANONICAL_ADP_31_PROPERTY_IDS.every((id) =>
      adminAccess.allowedPropertyIds.includes(id)
    )
  );

  // Customer isolation — unassigned non-admin cannot access hotels
  const limited = {
    memberstackEmail: "limited@hotel.com",
    dealalityUser: { isAdmin: false, email: "limited@hotel.com" },
  };
  const limitedAccess = resolveOwnerAppPropertyAccess(limited);
  mark("CUSTOMER_ZERO_ASSIGNMENT_EMPTY", limitedAccess.allowedPropertyIds.length === 0);
  mark(
    "CUSTOMER_CANNOT_ENUMERATE_ADMIN_INVENTORY",
    limitedAccess.allowedPropertyIds.length !== adminInventory.length &&
      limitedAccess.allowedPropertyIds.length === 0
  );
  mark(
    "CUSTOMER_CANNOT_ACCESS_UNASSIGNED_SHERATON",
    ownerAppCanAccessProperty(limited, "adp_sheraton_mallorca_arabella_golf")
      .status === 403
  );
  mark(
    "CUSTOMER_CANNOT_ACCESS_UNPUBLISHED_YOTEL",
    ownerAppCanAccessProperty(limited, "adp_yotel_geneva_lake").status === 403 &&
      !customerIds.has("adp_yotel_geneva_lake")
  );
  const limitedFiltered = filterPropertiesForOwnerApp(limited, customerVisible);
  mark("CUSTOMER_FILTER_EMPTY", limitedFiltered.length === 0);

  // Assigned customer stays on customer-visible intersection only (not full admin 32).
  const assignedCustomer = {
    memberstackEmail: "assigned@hotel.com",
    dealalityUser: { isAdmin: false, email: "assigned@hotel.com" },
    // Inject via temporary assignment simulation: only Waterstone if visible.
  };
  // Without assignment rows, non-admin remains empty — never inherits admin inventory.
  mark(
    "ASSIGNMENT_ENFORCEMENT_NO_ADMIN_FALLBACK",
    resolveOwnerAppPropertyAccess(assignedCustomer).isAdmin === false &&
      resolveOwnerAppPropertyAccess(assignedCustomer).allowedPropertyIds.length === 0
  );

  // Readiness metadata present; incomplete/blocked never pretend customer-ready
  for (const row of adminInventory) {
    assert.ok(row.reportReadiness, `missing readiness for ${row.propertyId}`);
    assert.notStrictEqual(row.reportReadiness, "customer_ready");
  }
  mark("READINESS_METADATA_PRESENT", true);

  const sheraton = adminInventory.find(
    (p) => p.propertyId === "adp_sheraton_mallorca_arabella_golf"
  );
  mark("SHERATON_HAS_NAME", Boolean(sheraton?.name));
  mark(
    "SHERATON_READINESS_RESOLVES",
    ["available", "staged", "incomplete", "blocked"].includes(
      resolveOwnerAppReportReadiness(
        "adp_sheraton_mallorca_arabella_golf",
        sheraton
      )
    )
  );

  writeFileSync(join(OUT, "RESULT.json"), JSON.stringify(results, null, 2));
  console.log(JSON.stringify(results, null, 2));
  console.log("PASS test-adp-owner-app-admin-inventory-v1");
}

main();
