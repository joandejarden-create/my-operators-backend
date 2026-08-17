#!/usr/bin/env node
/**
 * Unit tests for image role-match taxonomy.
 */
import assert from "assert";
import {
  parseAssignedRoleFromCaption,
  detectVisualCategory,
  evaluateImageRoleMatch,
  evaluateBrandImageRoleMatch,
  IMAGE_ROLES,
} from "../lib/partner-intelligence/brand-explorer-image-role-match.js";

function testGuestRoomOnDiningFails() {
  const r = evaluateImageRoleMatch({
    slotKey: "materials.gallery.2",
    title: "Guest Room / Suite - Palladio Hotel Buenos Aires MGallery Collection",
    imageUrl: "https://www.ahstatic.com/photos/b1r7_ba_00_p_1024x768.jpg",
    imageFilename: "b1r7_ba_00_p_1024x768.jpg",
  });
  assert.equal(r.currentRole, IMAGE_ROLES.guest_room_suite);
  assert.equal(r.detectedVisualCategory, IMAGE_ROLES.food_beverage_experience);
  assert.equal(r.matchStatus, "wrong_role");
  assert.equal(r.hardFail, true);
}

function testRoomCodePassesGuestRoom() {
  const r = evaluateImageRoleMatch({
    slotKey: "materials.gallery.2",
    title: "Guest Room / Suite - Palladio",
    imageUrl: "https://www.ahstatic.com/photos/b1r7_ro_00_p_1024x768.jpg",
    imageFilename: "b1r7_ro_00_p_1024x768.jpg",
  });
  assert.equal(r.matchStatus, "pass");
  assert.equal(r.hardFail, false);
}

function testExteriorCaptionOnRoomFails() {
  const r = evaluateImageRoleMatch({
    slotKey: "materials.gallery.1",
    title: "Exterior / Arrival - Hotel",
    imageUrl: "https://www.ahstatic.com/photos/b1r7_ro_01_p_1024x768.jpg",
  });
  assert.equal(r.matchStatus, "wrong_role");
}

function testDesignDetailOnExteriorFails() {
  const r = evaluateImageRoleMatch({
    slotKey: "materials.gallery.5",
    title: "Design Detail - Costanero",
    imageUrl: "https://www.ahstatic.com/photos/b3g3_ho_01_p_1024x768.jpg",
  });
  assert.equal(r.detectedVisualCategory, IMAGE_ROLES.exterior_arrival);
  assert.equal(r.matchStatus, "wrong_role");
}

function testPropertySettingOnRoomFails() {
  const r = evaluateImageRoleMatch({
    slotKey: "materials.gallery.6",
    title: "Property Setting - Costanero",
    imageUrl: "https://www.ahstatic.com/photos/b3g3_ro_02_p_1024x768.jpg",
  });
  assert.equal(r.matchStatus, "wrong_role");
}

function testSixRoleMatchedPass() {
  const rows = [
    {
      slotKey: "materials.gallery.1",
      title: "Exterior / Arrival - A",
      imageUrl: "https://www.ahstatic.com/photos/b1r7_ho_00_p_1024x768.jpg",
    },
    {
      slotKey: "materials.gallery.2",
      title: "Guest Room / Suite - A",
      imageUrl: "https://www.ahstatic.com/photos/b1r7_ro_00_p_1024x768.jpg",
    },
    {
      slotKey: "materials.gallery.3",
      title: "Wellness / Pool / Spa - B",
      imageUrl: "https://www.ahstatic.com/photos/b3g3_sp_00_p_1024x768.jpg",
    },
    {
      slotKey: "materials.gallery.4",
      title: "F&B / Bar / Restaurant / Local Experience - A",
      imageUrl: "https://www.ahstatic.com/photos/b1r7_ba_00_p_1024x768.jpg",
    },
    {
      slotKey: "materials.gallery.5",
      title: "Design Detail / Interior Detail - B",
      imageUrl: "https://www.ahstatic.com/photos/b3g3_sp_01_p_1024x768.jpg",
    },
    {
      slotKey: "materials.gallery.6",
      title: "Property Setting / Destination Context - C",
      imageUrl: "https://www.ahstatic.com/photos/a1x5_ho_01_p_1024x768.jpg",
    },
    { slotKey: "overview.scenario.1", title: "S1", imageUrl: "https://cdn.example.com/s1.jpg" },
    { slotKey: "overview.scenario.2", title: "S2", imageUrl: "https://cdn.example.com/s2.jpg" },
    { slotKey: "overview.scenario.3", title: "S3", imageUrl: "https://cdn.example.com/s3.jpg" },
    {
      slotKey: "footprint.openings",
      title: "P1",
      imageUrl: "https://www.ahstatic.com/photos/b1r7_ho_02_p_1024x768.jpg",
      recordId: "r1",
    },
    {
      slotKey: "footprint.openings",
      title: "P2",
      imageUrl: "https://www.ahstatic.com/photos/b3g3_ho_02_p_1024x768.jpg",
      recordId: "r2",
    },
    {
      slotKey: "footprint.openings",
      title: "P3",
      imageUrl: "https://www.ahstatic.com/photos/a1x5_ho_02_p_1024x768.jpg",
      recordId: "r3",
    },
  ];
  const result = evaluateBrandImageRoleMatch({ brandSlug: "fixture", presentationRows: rows });
  assert.equal(result.pass, true, JSON.stringify(result.unresolved, null, 2));
}

function main() {
  assert.equal(parseAssignedRoleFromCaption("Guest Room / Suite - X"), IMAGE_ROLES.guest_room_suite);
  assert.equal(
    detectVisualCategory({ imageUrl: "https://www.ahstatic.com/photos/b1r7_ba_00_p_1024x768.jpg" })
      .category,
    IMAGE_ROLES.food_beverage_experience
  );
  testGuestRoomOnDiningFails();
  testRoomCodePassesGuestRoom();
  testExteriorCaptionOnRoomFails();
  testDesignDetailOnExteriorFails();
  testPropertySettingOnRoomFails();
  testSixRoleMatchedPass();
  console.log("[PASS] brand-explorer image role-match (unit)");
}

main();
