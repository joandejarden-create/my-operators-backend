import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";

/** Independently executable Association Rotation hunter (wave 2 stub + cache). */
export const associationRotationHunter = createCacheBackedHunter(
  SPECIALIST_HUNTER.ASSOCIATION_ROTATION
);

export const runAssociationRotationHunter = associationRotationHunter.run;
export const discoverAssociationRotation = associationRotationHunter.discoverFn;
