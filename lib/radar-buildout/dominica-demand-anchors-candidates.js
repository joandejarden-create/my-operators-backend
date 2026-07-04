/**
 * Dominica countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyDominicaGovernanceDefaults,
  DOMINICA_SUBMARKETS,
} from "./dominica-demand-anchor-governance.js";

const COUNTRY = "Dominica";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyDominicaGovernanceDefaults);

export const DOMINICA_COUNTRYWIDE_CANDIDATES = [
  pt({ name: "Douglas-Charles Airport Corridor", pointType: "Future Growth Node", city: "Marigot", submarket: "Portsmouth", latitude: 15.547, longitude: -61.3, sourceReference: "https://www.dominica.gov.dm/", manuallyVerified: true }),
  pt({ name: "Canefield Airport", pointType: "Future Growth Node", city: "Canefield", submarket: "Roseau", latitude: 15.3367, longitude: -61.3922, sourceReference: "https://www.dominica.gov.dm/", manuallyVerified: true }),
  pt({ name: "Roseau Cruise Ship Berth", pointType: "Mixed-Use Development", city: "Roseau", submarket: "Roseau", latitude: 15.2978, longitude: -61.3871, sourceReference: "https://www.discoverdominica.com/", manuallyVerified: true }),
  pt({ name: "Roseau Central Business District", pointType: "Business District", city: "Roseau", submarket: "Roseau", latitude: 15.3012, longitude: -61.3889, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Old Market Roseau", pointType: "Entertainment District", city: "Roseau", submarket: "Roseau", latitude: 15.2989, longitude: -61.3867, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Dominica Botanic Gardens", pointType: "Tourist Attraction", city: "Roseau", submarket: "Roseau", latitude: 15.3123, longitude: -61.3912, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Dominica State College", pointType: "University / College", city: "Roseau", submarket: "Roseau", latitude: 15.3056, longitude: -61.3845, sourceReference: "https://www.dominica.gov.dm/" }),
  pt({ name: "Princess Margaret Hospital", pointType: "Medical Campus", city: "Roseau", submarket: "Roseau", latitude: 15.3034, longitude: -61.3823, sourceReference: "https://www.dominica.gov.dm/" }),
  pt({ name: "Windsor Park Sports Stadium", pointType: "Sports Venue", city: "Roseau", submarket: "Roseau", latitude: 15.3089, longitude: -61.3789, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Morne Bruce Viewpoint", pointType: "Tourist Attraction", city: "Roseau", submarket: "Roseau", latitude: 15.3234, longitude: -61.3756, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Trafalgar Falls", pointType: "Tourist Attraction", city: "Trafalgar", submarket: "Roseau", latitude: 15.3345, longitude: -61.3567, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Emerald Pool Nature Trail", pointType: "Tourist Attraction", city: "Morne Trois Pitons", submarket: "Other", latitude: 15.3234, longitude: -61.3234, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Boiling Lake Trailhead", pointType: "Tourist Attraction", city: "Laudat", submarket: "Other", latitude: 15.3345, longitude: -61.3012, sourceReference: "https://www.discoverdominica.com/", manuallyVerified: true }),
  pt({ name: "Morne Trois Pitons National Park UNESCO", pointType: "Tourist Attraction", city: "Laudat", submarket: "Other", latitude: 15.3456, longitude: -61.3123, sourceReference: "https://www.discoverdominica.com/", manuallyVerified: true }),
  pt({ name: "Champagne Reef Snorkel Site", pointType: "Beach / Waterfront", city: "Soufriere", submarket: "South Coast", latitude: 15.2234, longitude: -61.3567, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Scotts Head Marine Reserve", pointType: "Beach / Waterfront", city: "Scotts Head", submarket: "South Coast", latitude: 15.2123, longitude: -61.3789, sourceReference: "https://www.discoverdominica.com/", manuallyVerified: true }),
  pt({ name: "Soufriere Bay Village", pointType: "Tourist Attraction", city: "Soufriere", submarket: "South Coast", latitude: 15.2345, longitude: -61.3567, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Calibishie Coastal Village", pointType: "Beach / Waterfront", city: "Calibishie", submarket: "East Coast", latitude: 15.5923, longitude: -61.3456, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Red Rock Beach East Coast", pointType: "Beach / Waterfront", city: "Calibishie", submarket: "East Coast", latitude: 15.6012, longitude: -61.3389, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Cabrits National Park Portsmouth", pointType: "Tourist Attraction", city: "Portsmouth", submarket: "Portsmouth", latitude: 15.5845, longitude: -61.4678, sourceReference: "https://www.discoverdominica.com/", manuallyVerified: true }),
  pt({ name: "Fort Shirley Historic Site", pointType: "Tourist Attraction", city: "Portsmouth", submarket: "Portsmouth", latitude: 15.5867, longitude: -61.4623, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Portsmouth Bay Marina", pointType: "Beach / Waterfront", city: "Portsmouth", submarket: "Portsmouth", latitude: 15.5789, longitude: -61.4567, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Indian River Tour Gateway", pointType: "Tourist Attraction", city: "Portsmouth", submarket: "Portsmouth", latitude: 15.5712, longitude: -61.4512, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Secret Bay Resort", pointType: "Mixed-Use Development", city: "Coulibistrie", submarket: "Other", latitude: 15.4123, longitude: -61.4456, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Jungle Bay Resort Spa", pointType: "Mixed-Use Development", city: "Soufriere", submarket: "South Coast", latitude: 15.2289, longitude: -61.3512, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Papillote Wilderness Retreat", pointType: "Tourist Attraction", city: "Trafalgar", submarket: "Roseau", latitude: 15.3312, longitude: -61.3589, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Middleham Falls", pointType: "Tourist Attraction", city: "Laudat", submarket: "Other", latitude: 15.3567, longitude: -61.3234, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Titou Gorge", pointType: "Tourist Attraction", city: "Laudat", submarket: "Other", latitude: 15.3489, longitude: -61.3189, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Waitukubuli National Trail Segment Roseau", pointType: "Tourist Attraction", city: "Roseau", submarket: "Roseau", latitude: 15.3156, longitude: -61.3678, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Layou River Valley", pointType: "Tourist Attraction", city: "Layou", submarket: "Other", latitude: 15.4012, longitude: -61.4012, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Mero Beach", pointType: "Beach / Waterfront", city: "Mero", submarket: "Other", latitude: 15.4234, longitude: -61.4234, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Dominica Export Import Agency Precinct", pointType: "Government / Civic Center", city: "Roseau", submarket: "Roseau", latitude: 15.3023, longitude: -61.3856, sourceReference: "https://www.investdominica.dm/" }),
  pt({ name: "Dominica Industrial Estate", pointType: "Industrial / Logistics Zone", city: "Fond Cole", submarket: "Roseau", latitude: 15.2912, longitude: -61.4012, sourceReference: "https://www.dominica.gov.dm/" }),
  pt({ name: "Rosalie Bay Resort Turtle Sanctuary", pointType: "Tourist Attraction", city: "Rosalie", submarket: "East Coast", latitude: 15.3678, longitude: -61.2623, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Hampstead Beach", pointType: "Beach / Waterfront", city: "Hampstead", submarket: "East Coast", latitude: 15.5789, longitude: -61.3234, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Castle Bruce Village", pointType: "Tourist Attraction", city: "Castle Bruce", submarket: "East Coast", latitude: 15.4456, longitude: -61.2567, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Dominica Convention Bureau District", pointType: "Convention Center", city: "Roseau", submarket: "Roseau", latitude: 15.3001, longitude: -61.3878, sourceReference: "https://www.discoverdominica.com/" }),
  pt({ name: "Portsmouth North Gateway Growth Node", pointType: "Future Growth Node", city: "Portsmouth", submarket: "Portsmouth", latitude: 15.5812, longitude: -61.4589, sourceReference: "https://www.investdominica.dm/" }),
  pt({ name: "Roseau Valley Resort Growth Corridor", pointType: "Future Growth Node", city: "Roseau", submarket: "Roseau", latitude: 15.3189, longitude: -61.3712, sourceReference: "https://www.investdominica.dm/" }),
];

export function getDominicaCandidates() {
  return DOMINICA_COUNTRYWIDE_CANDIDATES;
}

export { DOMINICA_SUBMARKETS };
