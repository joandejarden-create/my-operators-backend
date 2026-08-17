/**
 * LinkedIn connection triage — Pilot Target List bulk import seed data.
 * Source: founder LinkedIn connection screenshots (Jul 2026).
 *
 * @typedef {Object} LinkedInPilotContact
 * @property {string} name
 * @property {string} [role]
 * @property {string} [company]
 * @property {string} segment - VAL_PILOT_OUTREACH_SEGMENT value
 * @property {"P1"|"P2"|"P3"} [priority]
 * @property {"A"|"B"|"C"|"D"|"E"} tier
 * @property {string} [skipReason] - tier E only
 */

/** @type {LinkedInPilotContact[]} */
export const LINKEDIN_PILOT_CONTACTS = [
  // Tier A — owner / investor / capital
  { name: "Vamsi Bonthala", role: "CEO", company: "Arbor Lodging Partners", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Jorge Margain", role: "Real estate investor & investment advisor", company: "", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Jorge Ayala Treviño", role: "Desarrollador inmobiliario", company: "", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Abraham Alanis", role: "Co-founder", company: "Grupo RIE / Mirmich Inmobiliaria", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Alejandro Rodriguez Rivero", role: "CIO & portfolio manager", company: "MREIT", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Juan Carlos Suarez", role: "EVP, growth & capital origination", company: "", segment: "Capital Partner", priority: "P1", tier: "A" },
  { name: "David Figueroa Zurita", role: "Presidente del consejo de administración", company: "", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Jonathon S. Zink", role: "COO", company: "Hotel Investment Group (Northstar)", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Roberto Durán", role: "Founder/CEO", company: "Ascentis Mexico", segment: "Owner / Investor", priority: "P2", tier: "A" },
  { name: "César Chávez González", role: "Head of Development", company: "FibraHotel", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Nicolas Valle, CFA", role: "Hospitality investments, RE, dev & asset mgmt", company: "", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Juan Camilo Castaño", role: "Private equity", company: "Columbia MBA", segment: "Capital Partner", priority: "P1", tier: "A" },
  { name: "David Cummings", role: "VP, head of real estate & project finance", company: "", segment: "Capital Partner", priority: "P1", tier: "A" },
  { name: "Julienne Smith", role: "Hotel development executive & investor", company: "", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Andres Fajardo", role: "CEO, hospitality entrepreneur", company: "", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Luis Mota Sousa", role: "Development & acquisitions", company: "", segment: "Owner / Investor", priority: "P2", tier: "A" },
  { name: "Linda Yau", role: "SVP Investments", company: "Pebblebrook Hotel Trust", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Jay Shah", role: "Principal", company: "Shamin Hotels", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Jay Bhakta", role: "Managing Partner", company: "JR Hospitality", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Fernando Mulet", role: "EVP / CIO", company: "Playa Hotels & Resorts", segment: "Owner / Investor", priority: "P1", tier: "A" },
  { name: "Mohammed Ramadan, CFA", role: "Corporate finance, capital markets, RE", company: "", segment: "Capital Partner", priority: "P2", tier: "A" },
  { name: "Rick West", role: "CEO / Partner", company: "", segment: "Owner / Investor", priority: "P2", tier: "A" },

  // Tier B — advisors
  { name: "John Fareed", role: "Managing Partner", company: "Horwath HTL USA", segment: "Advisor / Consultant / Broker", priority: "P1", tier: "B" },
  { name: "Francisco Alberti", role: "Hospitality corporate development advisor", company: "", segment: "Advisor / Consultant / Broker", priority: "P1", tier: "B" },
  { name: "Adam McGaughy", role: "Senior MD", company: "JLL Hotels & Hospitality", segment: "Advisor / Consultant / Broker", priority: "P2", tier: "B" },
  { name: "Gustavo Cumella de Montserrat", role: "Director", company: "CBRE Hotels", segment: "Advisor / Consultant / Broker", priority: "P2", tier: "B" },
  { name: "Charis Atwood", role: "Owner's rep / hospitality project consultant", company: "", segment: "Advisor / Consultant / Broker", priority: "P2", tier: "B" },
  { name: "Juanjo Ripoll", role: "Real estate portfolio & investment mgmt", company: "", segment: "Owner / Investor", priority: "P2", tier: "B" },
  { name: "Carlos Enrique Monteros Garcia", role: "Senior consultant", company: "Colliers", segment: "Advisor / Consultant / Broker", priority: "P2", tier: "B" },
  { name: "Thomas Bour, MRICS", role: "RE project manager, Caribbean", company: "", segment: "Advisor / Consultant / Broker", priority: "P2", tier: "B" },
  { name: "David Camhi", role: "Partner", company: "CAM Law Group", segment: "Advisor / Consultant / Broker", priority: "P2", tier: "B" },
  { name: "Roger A. Allen", role: "Group CEO", company: "RLA Global", segment: "Advisor / Consultant / Broker", priority: "P2", tier: "B" },
  { name: "Charlie Shi, AACI", role: "Managing Director", company: "HVS Toronto", segment: "Advisor / Consultant / Broker", priority: "P2", tier: "B" },
  { name: "Steve Rushmore", role: "Founder", company: "HVS", segment: "Advisor / Consultant / Broker", priority: "P2", tier: "B" },
  { name: "Lissette Elias", role: "Foreign direct investment counsel", company: "", segment: "Advisor / Consultant / Broker", priority: "P2", tier: "B" },
  { name: "Ken Gooz", role: "Principal/CEO, franchise & advisory", company: "", segment: "Advisor / Consultant / Broker", priority: "P2", tier: "B" },
  { name: "Jeremy Bratcher", role: "CEO", company: "Landingplace Hotels", segment: "Operator", priority: "P2", tier: "B" },
  { name: "Thibault Catala", role: "Founder", company: "Catala Consulting", segment: "Advisor / Consultant / Broker", priority: "P2", tier: "B" },
  { name: "Carmine Iommazzo", role: "Hospitality development consulting", company: "", segment: "Advisor / Consultant / Broker", priority: "P2", tier: "B" },
  { name: "Zachary Bayman", role: "Partner", company: "", segment: "Owner / Investor", priority: "P2", tier: "B" },
  { name: "Vasilis Halakos", role: "Director", company: "JLL", segment: "Advisor / Consultant / Broker", priority: "P2", tier: "B" },

  // Tier C — brand / referral / feedback
  { name: "Nicolas Rodriguez Alvarez", role: "Sr Director Development", company: "Hilton CALA", segment: "Brand / Referral Source", priority: "P2", tier: "C" },
  { name: "Pamela Vasquez", role: "CALA Development", company: "Hilton", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Melissa Vargas", role: "Feasibility & Development", company: "Marriott Mexico/Caribbean/LATAM", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Laurent de Kousemaeker", role: "CDO", company: "Marriott Caribbean & LATAM", segment: "Brand / Referral Source", priority: "P2", tier: "C" },
  { name: "Nicolas Martinez Matallana", role: "SVP Development", company: "Accor Luxury CALA", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Duncan Chiu", role: "VP Lodging Development", company: "Marriott Canada", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Brandon Harris", role: "Director Development", company: "Marriott Midscale", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Ludwig Bouldoukian", role: "RVP Development", company: "Hyatt MEA", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Antonio Fungairino", role: "RVP Growth", company: "Hyatt Inclusive CALA", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Felipe Mihovilovic", role: "Development Director", company: "Wyndham", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Leonardo Lido", role: "Sr Director Development", company: "Hilton Brazil", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Christian De Polignac", role: "Development Manager", company: "Mexico & Central America", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Fabiana Salvi, PhD", role: "Global Development Director", company: "Iberostar", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Guillermo Ortega", role: "Director BD", company: "TUI Hotels Americas", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Íñigo Cumella de Montserrat", role: "VP Development Europe", company: "Minor Hotels", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Hylko Versteeg", role: "Head of Dev Southern Europe", company: "IHG", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Kathryn Wallin", role: "Sr Director International Development", company: "Marriott", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Dimitris Manikis", role: "President EMEA", company: "Wyndham", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Ruben Amor", role: "VP Development", company: "Global hotel brands", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Genna Panagopoulos", role: "VP Development", company: "US & Canada", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Joyce Tan", role: "Head Global Development", company: "Seibu Prince", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "John Alarcon", role: "Development executive", company: "Luxury/lifestyle brands", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Wendy Chan", role: "CFO", company: "Marriott CALA", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Jordan Hollander", role: "Founder", company: "HotelTechReport", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Luis-Rene Sanchez", role: "", company: "Aimbridge Hospitality", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Brittney Jones", role: "CDO", company: "Brittain Resorts & Hotels", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Philippe Bassoul", role: "Development Director", company: "Hotels & F&B", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Camila Ortiz", role: "Development Manager EMEA", company: "", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Stéphanie Segaux", role: "VP Development EMEA", company: "", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "María del Pilar Ruiz Rufián", role: "Global Product & Brand Developer", company: "Meliá", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Alejandro García Menéndez", role: "Senior Feasibility Manager", company: "Accor", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Hugo Mirabal", role: "Director of Development, Caribbean", company: "", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Chema Basterrechea", role: "Global President/COO", company: "Radisson", segment: "Brand / Referral Source", priority: "P3", tier: "C" },
  { name: "Rohan Thakkar", role: "Brand development", company: "", segment: "Brand / Referral Source", priority: "P3", tier: "C" },

  // Tier D — operator / mixed
  { name: "Juan Carlos Rodo", role: "VP Ops", company: "Grupo Marta Hospitality", segment: "Operator", priority: "P3", tier: "D" },
  { name: "Sandor B. Tupi", role: "CEO", company: "Elite Hotels & Resorts", segment: "Operator", priority: "P3", tier: "D" },
  { name: "Keith Oltchick", role: "CDO", company: "Remington Hospitality", segment: "Operator", priority: "P3", tier: "D" },
  { name: "Ricardo Losada Revol", role: "SVP & GM, international division", company: "", segment: "Operator", priority: "P3", tier: "D" },
  { name: "Nick Nizar Comaty", role: "COO / F&B / development", company: "", segment: "Operator", priority: "P3", tier: "D" },
  { name: "Shadi Omeish", role: "SVP Operations, Americas", company: "", segment: "Operator", priority: "P3", tier: "D" },
  { name: "Daniel Chavarria", role: "CEO", company: "Enchanting Hotels Collection", segment: "Operator", priority: "P3", tier: "D" },
  { name: "Piers Thackray", role: "TH1.ai + boutique hotel partner", company: "", segment: "Other", priority: "P3", tier: "D" },
  { name: "Steven Ortmann", role: "Cignara / Gratitude AI / recovering hotelier", company: "", segment: "Other", priority: "P3", tier: "D" },
  { name: "Laura Santoni", role: "General Manager", company: "", segment: "Operator", priority: "P3", tier: "D" },

  // Tier E — skip / low fit (included for edit-down in Airtable)
  { name: "Félix Rosselló Gili", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Sourabh Khare", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Amisha William", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Ramy Fouda", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Carlo Loi", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Calvin Goh", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Ricardo Pantaleón", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Pierre Sierralta", role: "Supply chain", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Not hospitality deal flow" },
  { name: "Nalin Gupta", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Gatik Trivedi", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Julien Frachisse", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Julian Sanchez", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Preetam Mallick", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Daniel Williams", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Ricardo Souza", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Fabio De Vero", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Luis Barberi", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Fwangmun Haggai", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Neil Kolton", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Lara Shaw", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Laura Nichols", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Sebastian Lang", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Brian Caravello", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Will Simone", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Sophie Driessens", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Christian Nikles", role: "", company: "Marriott", segment: "Brand / Referral Source", priority: "P3", tier: "E", skipReason: "Corp dev — deprioritized wave 1" },
  { name: "Tomas Dieuzeide", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Andre Rodrigues Pereira", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Camilo Andres Gutierrez Cuervo", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Low wave-1 fit from triage" },
  { name: "Franklin Piñerúa", role: "", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Unclear fit from triage" },
  { name: "Reto Stöckenius", role: "Hospitality development", company: "", segment: "Other", priority: "P3", tier: "E", skipReason: "Unclear deal flow from triage" },
];

/** Names already manually seeded in Pilot Target List before LinkedIn bulk import. */
export const EXISTING_PILOT_TARGET_NAMES = new Set([
  "paul adan",
  "michael michael",
  "michael jones",
  "ryan forde",
  "german fernandez del busto",
]);
