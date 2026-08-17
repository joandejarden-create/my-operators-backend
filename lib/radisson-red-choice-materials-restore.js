/**
 * Radisson RED (Choice) — materials.gallery image sources.
 * @see fixtures/choice-media-center-text/Radisson-Red-press-kit.txt
 */

export const RADISSON_RED_CHOICE_BRAND_AIRTABLE_NAME = "Radisson RED by Choice";

/** Press-kit download URLs (use hoteldam fallback when >5MB). */
export const RADISSON_RED_CHOICE_GALLERY_IMAGE_URL = {
  "materials.gallery.1":
    "https://media.choicehotels.com/download/Exterior+-+Minneapolis+.jpg",
  "materials.gallery.2":
    "https://media.choicehotels.com/download/Lobby+-+Miami+Airport+.jpg",
  "materials.gallery.3":
    "https://media.choicehotels.com/download/Lounge+R%C3%98D+Grainne%E2%80%99s+-+Brazil+Campinas.jpg",
  "materials.gallery.4":
    "https://media.choicehotels.com/download/OUIBar+%2B+KTCHN+-+Minneapolis+.jpg",
  "materials.gallery.5":
    "https://www.choicehotels.com/hoteldam/aa/aa024/images/1280/AA024ExteriorTemp1.jpg",
  "materials.gallery.6":
    "https://www.choicehotels.com/hoteldam/aa/aa024/images/480/AA024ExteriorTemp1.jpg",
};

/** hoteldam fallback when press-kit file exceeds Airtable 5MB limit. */
export const RADISSON_RED_CHOICE_GALLERY_HOTELDAM_URL = {
  "materials.gallery.1":
    "https://www.choicehotels.com/hoteldam/mn/mn290/images/1280/MN290ExteriorTemp01_1.jpg",
  "materials.gallery.3":
    "https://www.choicehotels.com/hoteldam/br/br157/images/1280/BR157ExteriorTemp01_1.jpg",
  "materials.gallery.4":
    "https://www.choicehotels.com/hoteldam/mn/mn290/images/1280/MN290LobbyTemp01_1.jpg",
};

/** Reuse footprint.openings hero when slot maps to opening title keyword. */
export const RADISSON_RED_CHOICE_GALLERY_TO_OPENING_KEY = {
  "materials.gallery.1": "Minneapolis",
  "materials.gallery.2": "Miami",
  "materials.gallery.3": "Campinas",
  "materials.gallery.5": "Rosario",
  "materials.gallery.6": "Miraflores",
};

/** Property page for hoteldam fetch referer. */
export const RADISSON_RED_CHOICE_GALLERY_PROPERTY_URL = {
  "materials.gallery.1":
    "https://www.choicehotels.com/minnesota/minneapolis/radisson-red-hotels/mn290",
  "materials.gallery.2":
    "https://www.choicehotels.com/florida/miami/radisson-red-hotels/flj13",
  "materials.gallery.3":
    "https://www.choicehotels.com/brazil/campinas/radisson-red-hotels/br157",
  "materials.gallery.4":
    "https://www.choicehotels.com/minnesota/minneapolis/radisson-red-hotels/mn290",
  "materials.gallery.5":
    "https://www.choicehotels.com/argentina/rosario/radisson-red-hotels/aa024",
  "materials.gallery.6":
    "https://www.choicehotels.com/peru/miraflores/radisson-red-hotels/pe012",
};
