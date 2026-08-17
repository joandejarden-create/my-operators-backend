/**
 * Seed Cvent venue URLs for Choice CALA properties (manual + discovered).
 * Used to skip slow search when identity is already known.
 */
export const CHOICE_CVENT_URL_SEEDS = Object.freeze({
  ind_choice_mx_mx092:
    "https://www.cvent.com/venues/irapuato/hotel/comfort-inn-irapuato/venue-f264d80b-e323-4365-842d-c91a18430d72",
  ind_choice_mx_mx226:
    "https://www.cvent.com/venues/es-ES/queretaro/hotel/comfort-inn-queretaro-tecnologico/venue-cd252652-75b1-454d-9360-bd48fb9000b1",
  ind_choice_mx_mx086:
    "https://www.cvent.com/venues/santiago-de-queretaro/hotel/comfort-inn-queretaro/venue-8fb0ab70-ed58-45ea-b18f-5c3ba2eb9f88",
});

/** Extra discover seeds by normalized name/city token sets. */
export const CHOICE_CVENT_NAME_SEEDS = Object.freeze([
  {
    match: ["comfort inn", "puerto vallarta"],
    url: "https://www.cvent.com/venues/puerto-vallarta/hotel/comfort-inn-puerto-vallarta/venue-7fca2251-bf71-4396-9b4e-db43c06ee524",
  },
  {
    match: ["comfort inn", "cabo"],
    url: "https://www.cvent.com/venues/cabo-san-lucas/hotel/comfort-inn-and-suites-los-cabos/venue-1bfb9b6c-5432-4e9c-8e29-f8aea19dd881",
  },
  {
    match: ["quality inn", "obregon"],
    url: "https://www.cvent.com/venues/obregon/hotel/quality-inn-ciudad-obregon/venue-99cb369f-7e59-4e2a-8d37-bb7c30208d2d",
  },
  {
    match: ["radisson", "paraiso"],
    url: "https://www.cvent.com/venues/mexico-city/hotel/radisson-paraiso-hotel-mexico-city/venue-c143a206-3bbb-4b5a-9fd7-e87c8048e88b",
  },
  {
    match: ["sleep inn", "queretaro"],
    url: "https://www.cvent.com/venues/queretaro/hotel/sleep-inn-queretaro/venue-bf90b9aa-fcf8-449a-a59d-7ed8f577991a",
  },
  {
    match: ["radisson", "toluca"],
    url: "https://www.cvent.com/venues/es-ES/toluca/hotel/radisson-hotel-convention-center-toluca/venue-7fa910df-bf51-4a69-b687-af10910f1534",
  },
  {
    match: ["radisson", "tapatio"],
    url: "https://www.cvent.com/venues/tlaquepaque/resort/radisson-hotel-tapatio-guadalajara/venue-dc134dd4-e9aa-46c1-a1b1-d21586d92a92",
  },
  {
    match: ["radisson", "guadalajara"],
    url: "https://www.cvent.com/venues/tlaquepaque/resort/radisson-hotel-tapatio-guadalajara/venue-dc134dd4-e9aa-46c1-a1b1-d21586d92a92",
  },
  {
    match: ["park inn", "mazatlan"],
    url: "https://www.cvent.com/venues/mazatlan/hotel/park-inn-by-radisson-mazatlan/venue-e8ce5320-bda8-484a-8254-fdc2e6a6f552",
  },
  {
    match: ["comfort inn", "delicias"],
    url: "https://www.cvent.com/venues/es-ES/ciudad-delicias/hotel/comfort-inn-delicias/venue-f975d93e-3ae8-4635-ba1d-27457e8c9cdc",
  },
  {
    match: ["comfort inn", "san luis"],
    url: "https://www.cvent.com/venues/san-luis-potosi/hotel/comfort-inn-san-luis-potosi/venue-fcfe57af-5c26-4a11-b159-de805e0f02b1",
  },
  {
    match: ["sleep inn", "paseo", "damas"],
    url: "https://www.cvent.com/venues/san-jose/hotel/sleep-inn-paseo-las-damas/venue-fc01c1ec-d360-4154-a8da-a6a8ced4bca2",
  },
  {
    match: ["comfort inn", "queretaro"],
    url: "https://www.cvent.com/venues/santiago-de-queretaro/hotel/comfort-inn-queretaro/venue-8fb0ab70-ed58-45ea-b18f-5c3ba2eb9f88",
  },
]);
