/**
 * Built-but-blocked remediation — target / protected registries.
 */
import { RADISSON_BUILT_BLOCKED_CONTENT } from "./brand-explorer-built-blocked-content-radisson.js";
import { RADISSON_BLU_BUILT_BLOCKED_CONTENT } from "./brand-explorer-built-blocked-content-radisson-blu.js";
import { RADISSON_RED_BUILT_BLOCKED_CONTENT } from "./brand-explorer-built-blocked-content-radisson-red.js";
import { COUNTRY_BUILT_BLOCKED_CONTENT } from "./brand-explorer-built-blocked-content-country.js";
import { QUALITY_INN_BUILT_BLOCKED_CONTENT } from "./brand-explorer-built-blocked-content-quality-inn.js";
import { SUBURBAN_BUILT_BLOCKED_CONTENT } from "./brand-explorer-built-blocked-content-suburban.js";
import { WOODSPRING_BUILT_BLOCKED_CONTENT } from "./brand-explorer-built-blocked-content-woodspring.js";

export const BUILT_BLOCKED_TARGETS = Object.freeze([
  "country-inn-suites",
  "quality-inn",
  "radisson",
  "radisson-blu",
  "radisson-red",
  "suburban-studios",
  "woodspring-suites",
]);

/** Wave 1: failFindings &lt; 35 and images already pass. */
export const BUILT_BLOCKED_WAVE1 = Object.freeze(["radisson", "radisson-blu"]);

/** Wave 2: higher field debt and/or image gaps. */
export const BUILT_BLOCKED_WAVE2 = Object.freeze([
  "radisson-red",
  "quality-inn",
  "country-inn-suites",
  "woodspring-suites",
  "suburban-studios",
]);

export const BUILT_BLOCKED_PROTECTED_PUBLIC_FULL = Object.freeze([
  "ascend",
  "comfort-inn-suites",
  "curio-collection",
  "design-hotels",
  "everhome-suites",
  "hotel-indigo",
  "kimpton",
  "mgallery-collection",
  "radisson-individuals-by-choice",
  "small-luxury-hotels-of-the-world",
  "tribute-portfolio",
]);

export const BUILT_BLOCKED_TRUE_INCOMPLETE = Object.freeze([
  "autograph-collection",
  "handwritten-collection",
  "radisson-collection",
  "tapestry-collection-by-hilton",
  "vignette-collection",
]);

export const BUILT_BLOCKED_IDENTITIES = Object.freeze({
  "country-inn-suites": {
    recordId: "recaayt9u7YYg8h7Y",
    name: "Country Inn & Suites by Choice",
  },
  "quality-inn": { recordId: "recd8o4k1JddhkRWW", name: "Quality Inn" },
  radisson: { recordId: "recywbx1YQSTCPqW1", name: "Radisson by Choice" },
  "radisson-blu": { recordId: "recWPEvxBQxVVzSq3", name: "Radisson Blu by Choice" },
  "radisson-red": { recordId: "recmKqo7M7mLZgRqQ", name: "Radisson RED by Choice" },
  "suburban-studios": { recordId: "reclcjg5Foa9Vs5TC", name: "Suburban Studios" },
  "woodspring-suites": { recordId: "recsOd51NzRPYsMko", name: "WoodSpring Suites" },
});

export const BUILT_BLOCKED_CONTENT_BY_SLUG = Object.freeze({
  radisson: RADISSON_BUILT_BLOCKED_CONTENT,
  "radisson-blu": RADISSON_BLU_BUILT_BLOCKED_CONTENT,
  "radisson-red": RADISSON_RED_BUILT_BLOCKED_CONTENT,
  "country-inn-suites": COUNTRY_BUILT_BLOCKED_CONTENT,
  "quality-inn": QUALITY_INN_BUILT_BLOCKED_CONTENT,
  "suburban-studios": SUBURBAN_BUILT_BLOCKED_CONTENT,
  "woodspring-suites": WOODSPRING_BUILT_BLOCKED_CONTENT,
});

/** Limited Basics patches for observed Basics-sourced thin fields (audience/positioning). */
export const BUILT_BLOCKED_BASICS_BY_SLUG = Object.freeze({
  "quality-inn": {
    "Key Brand Differentiators":
      "Founding Choice Hotels brand with deep awareness.\n\nValue Qs amenity framework.\n\nConversion-ready within brand standards growth model for owners.\n\nGlobal footprint with dominant U.S. presence.",
  },
  "country-inn-suites": {
    "Guest Psychographics Description":
      "Business and leisure travelers seeking reliable upper-midscale select-service stays with consistent comfort, breakfast convenience, and Choice Privileges participation—Country Inn & Suites fits when predictability and value matter more than lifestyle programming.",
    "Key Brand Differentiators":
      "Complimentary breakfast and suite-friendly comfort.\n\nUpper-midscale select-service consistency for suburban and highway corridors.\n\nChoice Privileges and enterprise distribution participation.\n\nWelcoming residential-feel product without lifestyle-collection positioning.",
  },
  "suburban-studios": {
    "Brand Customer Promise":
      "Flexible extended-stay accommodations with in-room kitchen capability and Choice extended-stay expertise—supported by conversion-ready within brand standards prototypes that help owners meet growing longer-stay demand.",
  },
  "radisson-blu": {
    "Brand Positioning":
      "Radisson Blu by Choice is the upper-upscale full-service design-forward Radisson flag for gateway and destination hotels—distinct from core Radisson, Radisson RED’s social lifestyle path, Radisson Collection’s curated luxury, and Radisson Individuals’ soft-collection independents.",
  },
  "radisson-red": {
    "Brand Positioning":
      "Radisson RED by Choice is the upscale lifestyle / select-service Radisson flag built around social energy and flex F&B—distinct from Radisson Blu’s full-service design posture, core Radisson, and Collection or Individuals paths.",
    "Guest Psychographics Description":
      "Urban and leisure travelers seeking a lively upscale stay with social public spaces and approachable design—RED fits when energy and community matter more than formal full-service ritual.",
  },
});
