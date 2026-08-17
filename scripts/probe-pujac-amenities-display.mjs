import { buildHiltonAmenitiesDisplay } from "../lib/hilton-amenity-display.js";

const pujac =
  "Sustainability; Complimentary Wi-Fi; Restaurant; Bar; Outdoor Pool; Fitness Center; Business Center; Meeting Space; Children's Recreation; Convenience Store; Room Service; Wake-Up Calls; Daily; Mobile Key; Service Request";

const beforeIdsOnly = pujac
  .split(";")
  .map((l) => l.trim())
  .filter(Boolean);

const display = buildHiltonAmenitiesDisplay({ amenities: pujac });
console.log("Input labels:", beforeIdsOnly.length);
console.log("Display items:", display.length);
console.log(display.map((d) => d.label).join("\n"));
