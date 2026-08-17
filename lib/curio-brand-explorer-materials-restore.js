/**
 * Curio Brand Explorer — materials.file rows with public PDF URLs for restore after
 * accidental --replace-slot-prefix "materials." wipe (attachments are not in text fixtures).
 */
export const CURIO_MATERIALS_FILE_ROWS = [
  {
    slotKey: "materials.file",
    title: "2026 US Curio Collection FDD",
    body: "PDF · ~5.7 MB · Hilton development disclosure (2026)\nhttps://hmd-wp.go-vip.net/wp-content/uploads/2026/03/2026-US-FDD-Curio.pdf",
    sort: 0,
    imageUrl: "https://hmd-wp.go-vip.net/wp-content/uploads/2026/03/2026-US-FDD-Curio.pdf",
  },
  {
    slotKey: "materials.file",
    title: "Curio Collection Brand Brochure",
    body: "PDF · ~1.0 MB · Brand overview deck\nhttps://griffinstafford.com/wp-content/uploads/2019/02/Curio-Brochure.pdf",
    sort: 1,
    imageUrl: "https://griffinstafford.com/wp-content/uploads/2019/02/Curio-Brochure.pdf",
  },
  {
    slotKey: "materials.file",
    title: "Hilton Brand Portfolio Grid",
    body: "PDF · Portfolio ladder reference\nhttps://assets.hiltonstatic.com/hilton-asset-cache/image/upload/Multimedia/Travel%20Agent/Hilton_Brand_Portfolio_Grid.pdf",
    sort: 2,
    imageUrl:
      "https://assets.hiltonstatic.com/hilton-asset-cache/image/upload/Multimedia/Travel%20Agent/Hilton_Brand_Portfolio_Grid.pdf",
  },
];

/** Match materials.gallery slot → footprint.openings title keyword. */
export const CURIO_GALLERY_TO_OPENING_KEY = {
  "materials.gallery.1": "Nacar",
  "materials.gallery.2": "Zemi",
  "materials.gallery.3": "Indura",
  "materials.gallery.4": "Royal Palm",
  "materials.gallery.5": "Anselmo",
};

/** Hilton property pages for gallery slots missing a footprint.openings image. */
export const CURIO_GALLERY_PROPERTY_URL = {
  "materials.gallery.1": "https://www.hilton.com/en/hotels/ctgncqq-nacar-hotel-cartagena/",
  "materials.gallery.2": "https://www.hilton.com/en/hotels/pujmiqq-zemi-miches-all-inclusive-resort/",
  "materials.gallery.3": "https://www.hilton.com/en/hotels/teaibqq-indura-beach-and-golf-resort/",
  "materials.gallery.4": "https://www.hilton.com/en/hotels/gpsrpqq-royal-palm-galapagos/",
  "materials.gallery.5": "https://www.hilton.com/en/hotels/buebaqq-anselmo-buenos-aires/",
  "materials.gallery.6": "https://curiocollection.com/",
};
