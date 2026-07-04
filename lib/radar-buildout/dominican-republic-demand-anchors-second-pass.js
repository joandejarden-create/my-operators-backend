/**
 * Dominican Republic Demand Anchors — second pass (gap fill only).
 */

const COUNTRY = "Dominican Republic";
const REGION = "Caribbean";

function pt(opts) {
  return {
    country: COUNTRY,
    region: REGION,
    source: "Public Source",
    visibility: "Internal Only",
    includeOnRadarMap: true,
    dataConfidence: opts.dataConfidence || "High",
    notes: opts.notes || `Submarket: ${opts.submarket}.`,
    ...opts,
  };
}

export const DR_DEMAND_ANCHORS_SECOND_PASS = [
  pt({
    name: "Acropolis Convention Center",
    pointType: "Convention Center",
    pointSubtype: "Meetings / Events",
    latitude: 18.4612,
    longitude: -69.9408,
    city: "Santo Domingo",
    submarket: "Santo Domingo Metro",
    sourceReference: "https://www.acropolis.com.do/",
    hotelDemandRationale:
      "Major Santo Domingo convention venue; supports group, corporate, and meeting-hotel demand in the capital.",
  }),
  pt({
    name: "Hotel El Embajador Convention Center",
    pointType: "Convention Center",
    pointSubtype: "Hotel Convention Center",
    latitude: 18.4689,
    longitude: -69.9425,
    city: "Santo Domingo",
    submarket: "Santo Domingo Metro",
    sourceReference: "https://www.hotelembajador.com/en/convention-center",
    hotelDemandRationale:
      "Large hotel-attached convention facility in metro SD; group and banquet compression for full-service hotels.",
  }),
  pt({
    name: "Fiesta Resort Convention & Casino",
    pointType: "Convention Center",
    pointSubtype: "Resort Convention",
    latitude: 18.436,
    longitude: -69.43,
    city: "Juan Dolio",
    submarket: "Boca Chica / Juan Dolio",
    sourceReference: "https://www.fiestahotelgroup.com/en/hotels/dominican-republic/fiesta-resort",
    hotelDemandRationale:
      "South-coast resort convention and casino complex; group leisure and meeting demand between SD and Juan Dolio.",
  }),
  pt({
    name: "Calle El Conde — Colonial Zone Entertainment Corridor",
    pointType: "Entertainment District",
    pointSubtype: "Historic Dining / Retail",
    latitude: 18.4735,
    longitude: -69.8855,
    city: "Santo Domingo",
    submarket: "Santo Domingo Metro",
    sourceReference: "https://www.godominicanrepublic.com/places-to-visit/calle-el-conde",
    hotelDemandRationale:
      "Primary pedestrian dining and retail spine in Colonial Zone; heritage-hotel and lifestyle urban demand driver.",
  }),
  pt({
    name: "Coco Bongo Punta Cana",
    pointType: "Entertainment District",
    pointSubtype: "Nightlife / Show Venue",
    latitude: 18.6352,
    longitude: -68.395,
    city: "Punta Cana",
    submarket: "Punta Cana / Bávaro / Cap Cana",
    sourceReference: "https://www.cocobongo.com/punta-cana/",
    hotelDemandRationale:
      "Signature resort-corridor nightlife show venue; supports entertainment-driven overnight stays in Bávaro.",
  }),
  pt({
    name: "Avenida Winston Churchill Business Corridor",
    pointType: "Business District",
    pointSubtype: "Corporate Office Corridor",
    latitude: 18.4755,
    longitude: -69.9385,
    city: "Santo Domingo",
    submarket: "Santo Domingo Metro",
    sourceReference: "https://www.godominicanrepublic.com/things-to-do/shopping",
    dataConfidence: "Medium",
    hotelDemandRationale:
      "Dense corporate office and banking corridor in Piantini/Naco; weekday transient hotel demand anchor.",
    notes: "Submarket: Santo Domingo Metro. Winston Churchill / Piantini corporate corridor centroid.",
  }),
  pt({
    name: "Universidad Iberoamericana (UNIBE)",
    pointType: "University / College",
    pointSubtype: "Private University",
    latitude: 18.4589,
    longitude: -69.9422,
    city: "Santo Domingo",
    submarket: "Santo Domingo Metro",
    sourceReference: "https://www.unibe.edu.do/",
    hotelDemandRationale:
      "Major private university campus; academic travel, conferences, and visiting-family hotel demand.",
  }),
  pt({
    name: "Plaza Lama Santiago Commercial Hub",
    pointType: "Business District",
    pointSubtype: "Regional Retail / Commercial",
    latitude: 19.456,
    longitude: -70.701,
    city: "Santiago",
    submarket: "Santiago / Cibao",
    sourceReference: "https://www.plazalama.com.do/",
    dataConfidence: "Medium",
    hotelDemandRationale:
      "Regional commercial hub in Cibao capital; weekday corporate and select-service hotel demand.",
    notes: "Submarket: Santiago / Cibao. Downtown Santiago retail/commercial node.",
  }),
];
