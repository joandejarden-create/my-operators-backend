import {
  marriottDirectoryNameSimilarity,
  marriottOpenMatchScore,
} from "../lib/marriott-name-match.js";
import { nameSimilarity } from "../lib/independent-census/match-current-census.js";

const PAIRS = [
  ["Treasure Beach By Elegant Hotels - All-Inclusive, Adults Only", "Treasure Beach Art Hotel, Barbados, An Autograph Collection All-Inclusive Resort"],
  ["Crystal Cove by Elegant Hotels - All Inclusive", "Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort"],
  ["Courtyard Bridgetown Barbados", "Courtyard by Marriott Bridgetown, Barbados"],
  ["Luxury Collection Park Tower", "Park Tower, a Luxury Collection Hotel, Buenos Aires"],
  ["The Brown Guatape", "The Brown, Guatape, Autograph Collection"],
  ["Planet Hollywood Beach Resort Cancun", "Planet Hollywood Cancun by Royalton, An Autograph Collection All-Inclusive Resort"],
  ["Hideaway at Royalton Negril All-Inclusive Resort & Spa - Adults Only", "Royalton Hideaway Negril, An Autograph Collection All-Inclusive Resort - Adults Only"],
  ["The Ritz-Carlton St. Thomas", "The Ritz Carlton St Thomas"],
  ["The Ritz-Carlton Aruba", "The Ritz Carlton Aruba"],
  ["The Ritz-Carlton Grand Cayman", "The Ritz Carlton Grand Cayman"],
  ["The Ritz-Carlton Santiago", "The Ritz Carlton Santiago"],
  ["The Ritz-Carlton Turks & Caicos", "The Ritz Carlton Turks And Caicos"],
  ["Dorado Beach a Ritz-Carlton Reserve", "Dorado Beach A Ritz Carlton Reserve"],
  ["Zadun A Ritz-Carlton Reserve", "Zadun Los Cabos A Ritz Carlton Reserve"],
  ["La Concha Renaissance San Juan Resort", "La Concha Resort, Puerto Rico, Autograph Collection"],
  ["Sheraton Buganvilias Resort & Convention Center", "Sheraton Buganvilias Puerto Vallarta"],
  ["AC Hotels by Marriott Kingston Jamaica", "AC Hotel Kingston, Jamaica"],
  ["Courtyard Kingston Jamaica", "Courtyard by Marriott Kingston, Jamaica"],
  ["AC Hotels by Marriott Guadalajara", "AC Hotel Guadalajara, Mexico"],
  ["City Express Suites Santa Fe", "City Express Plus by Marriott Ciudad de México Santa Fe"],
];

for (const [a, b] of PAIRS) {
  const old = nameSimilarity(a, b);
  const neu = marriottDirectoryNameSimilarity(a, b);
  console.log(neu.toFixed(2), `(was ${old.toFixed(2)})`, "|", a.slice(0, 40));
}
