/**
 * Google Places review corrections for Colombia candidates.
 */
const REVIEW_TAG = "[Google review correction applied]";

/** @type {Record<string, object>} */
export const COLOMBIA_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Bocagrande Waterfront Strip": {
    "name": "Bocagrande Beach",
    "googleSearchQuery": "Playa Bocagrande Cartagena de Indias Colombia",
    "latitude": 10.3993,
    "longitude": -75.5556,
    "reviewAction": "rename_search"
  },
  "Playa Blanca Barú": {
    "name": "Playa Blanca Barú",
    "googleSearchQuery": "Playa Blanca Isla Barú Cartagena Colombia",
    "latitude": 10.2173264,
    "longitude": -75.6133453,
    "reviewAction": "search_query"
  },
  "Getsemaní Entertainment District": {
    "name": "Barrio Getsemaní",
    "latitude": 10.4201663,
    "longitude": -75.5449241,
    "reviewAction": "google_canonical"
  },
  "Mamonal Industrial Corridor": {
    "name": "Parque Industrial Mamonal",
    "googleSearchQuery": "Parque Industrial Mamonal Cartagena Colombia",
    "latitude": 10.3336,
    "longitude": -75.5159,
    "reviewAction": "rename_search"
  },
  "Centro Administrativo Distrital de Cartagena": {
    "name": "Centro Administrativo Departamental CAD",
    "googleSearchQuery": "Centro Administrativo Departamental CAD Cartagena Colombia",
    "latitude": 10.4220463,
    "longitude": -75.5500304,
    "reviewAction": "google_canonical"
  },
  "Avenida San Martín Business Corridor": {
    "name": "Avenida San Martín Bocagrande",
    "googleSearchQuery": "Avenida San Martín Bocagrande Cartagena Colombia",
    "latitude": 10.4041,
    "longitude": -75.5545,
    "reviewAction": "rename_search"
  },
  "La Serrezuela Mixed-Use Complex": {
    "name": "La Serrezuela Mall",
    "latitude": 10.4283724,
    "longitude": -75.5458251,
    "reviewAction": "google_canonical"
  },
  "Puerto de Cartagena Cruise Terminal": {
    "name": "Cruise Terminal Pier 3 Cartagena",
    "latitude": 10.4036351,
    "longitude": -75.5322725,
    "reviewAction": "google_canonical"
  },
  "Serena del Mar Urban Expansion Node": {
    "name": "Conjunto Residencial Serena del Mar",
    "googleSearchQuery": "Conjunto Residencial Serena del Mar Cartagena Colombia",
    "latitude": 10.4874,
    "longitude": -75.4863,
    "reviewAction": "search_query"
  },
  "Corferias Bogotá Convention Center": {
    "name": "Corferias",
    "latitude": 4.6291803,
    "longitude": -74.0899575,
    "reviewAction": "google_canonical"
  },
  "La Candelaria Historic District": {
    "name": "La Candelaria",
    "latitude": 4.5953005,
    "longitude": -74.0735738,
    "reviewAction": "google_canonical"
  },
  "Zona T / Zona Rosa Entertainment District": {
    "name": "Zona T",
    "latitude": 4.6676124,
    "longitude": -74.0541186,
    "reviewAction": "google_canonical"
  },
  "Parque de la 93 Entertainment District": {
    "name": "Parque de la 93",
    "latitude": 4.6766342,
    "longitude": -74.0482757,
    "reviewAction": "google_canonical"
  },
  "Fundación Santa Fe de Bogotá": {
    "name": "Fundación Santa Fe de Bogotá",
    "googleSearchQuery": "Fundación Santa Fe de Bogotá Bogotá Colombia",
    "latitude": 4.6956426,
    "longitude": -74.033027,
    "reviewAction": "search_query"
  },
  "Universidad de los Andes": {
    "name": "Universidad de los Andes",
    "googleSearchQuery": "Universidad de los Andes Bogotá Colombia",
    "latitude": 4.6014581,
    "longitude": -74.0661334,
    "reviewAction": "search_query"
  },
  "Estadio Nemesio Camacho El Campín": {
    "name": "Estadio Nemesio Camacho El Campín",
    "googleSearchQuery": "Estadio El Campín Bogotá Colombia",
    "latitude": 4.645942,
    "longitude": -74.0775185,
    "reviewAction": "search_query"
  },
  "Centro Internacional Business District": {
    "name": "Centro Internacional Bogotá",
    "googleSearchQuery": "Centro Internacional Bogotá Colombia",
    "latitude": 4.6157218,
    "longitude": -74.0712277,
    "reviewAction": "google_canonical"
  },
  "Calle 100 / Chicó Business Corridor": {
    "name": "Avenida Calle 100",
    "googleSearchQuery": "Avenida Calle 100 Bogotá Colombia",
    "latitude": 4.6856708,
    "longitude": -74.0528113,
    "reviewAction": "google_canonical"
  },
  "Salitre Plaza Mixed-Use Hub": {
    "name": "Salitre Plaza Centro Comercial",
    "latitude": 4.6531882,
    "longitude": -74.1099341,
    "reviewAction": "google_canonical"
  },
  "Paloquemao Logistics Market Zone": {
    "name": "Paloquemao Fruit Market",
    "latitude": 4.6160795,
    "longitude": -74.084109,
    "reviewAction": "google_canonical"
  },
  "CAN Government Complex (Centro Administrativo Nacional)": {
    "name": "CAN",
    "googleSearchQuery": "CAN Centro Administrativo Nacional Bogotá Colombia",
    "latitude": 4.6460222,
    "longitude": -74.0974704,
    "reviewAction": "search_query"
  },
  "Plaza Mayor Medellín Convention Center": {
    "name": "Plaza Mayor Medellín",
    "latitude": 6.2431811,
    "longitude": -75.575787,
    "reviewAction": "google_canonical"
  },
  "Poblado Entertainment District (Parque Lleras)": {
    "name": "Parque Lleras",
    "latitude": 6.208881,
    "longitude": -75.5677791,
    "reviewAction": "google_canonical"
  },
  "Comuna 13 Tourism Corridor": {
    "name": "Comuna 13",
    "googleSearchQuery": "Comuna 13 Medellín Colombia graffiti",
    "latitude": 6.2568,
    "longitude": -75.6232,
    "reviewAction": "search_query"
  },
  "Botero Plaza": {
    "name": "Plaza Botero Medellín",
    "googleSearchQuery": "Plaza Botero Medellín Colombia",
    "latitude": 6.2521184,
    "longitude": -75.5686141,
    "reviewAction": "search_query"
  },
  "Hospital Universitario San Vicente Fundación": {
    "name": "Hospital San Vicente Fundación",
    "latitude": 6.262662,
    "longitude": -75.5656043,
    "reviewAction": "google_canonical"
  },
  "Universidad EAFIT": {
    "name": "Universidad EAFIT",
    "googleSearchQuery": "Universidad EAFIT Medellín Colombia",
    "latitude": 6.1996012,
    "longitude": -75.5792139,
    "reviewAction": "manual_corridor",
    "manuallyVerified": true
  },
  "Milla de Oro Business District": {
    "name": "Milla de Oro El Poblado",
    "googleSearchQuery": "Milla de Oro Avenida El Poblado Medellín Colombia",
    "latitude": 6.2017,
    "longitude": -75.5719,
    "reviewAction": "search_query"
  },
  "Ruta N Innovation District": {
    "name": "Ruta N",
    "latitude": 6.2649637,
    "longitude": -75.5668122,
    "reviewAction": "google_canonical"
  },
  "Zona Franca Rionegro Logistics Hub": {
    "name": "Zona Franca Rionegro",
    "city": "Rionegro",
    "latitude": 6.1578892,
    "longitude": -75.4127335,
    "reviewAction": "google_coords_city"
  },
  "Puerta de Oro Convention Center": {
    "name": "Puerta de Oro Centro de Eventos",
    "latitude": 11.0246407,
    "longitude": -74.8003691,
    "reviewAction": "google_canonical"
  },
  "Gran Malecón del Río": {
    "name": "Gran Malecón del Río Barranquilla",
    "googleSearchQuery": "Gran Malecón del Río Barranquilla Colombia",
    "latitude": 11.0262312,
    "longitude": -74.7995889,
    "reviewAction": "search_query"
  },
  "Barranquilla Carnival Zone": {
    "name": "Carnaval de Barranquilla",
    "googleSearchQuery": "Carnaval de Barranquilla Colombia",
    "latitude": 10.9878,
    "longitude": -74.8019,
    "reviewAction": "rename_search"
  },
  "Clínica Portoazul Auna": {
    "name": "Clínica Portoazul",
    "googleSearchQuery": "Clínica Portoazul Barranquilla Colombia",
    "latitude": 11.0146,
    "longitude": -74.8426,
    "reviewAction": "search_query"
  },
  "Estadio Metropolitano Roberto Meléndez": {
    "name": "Estadio Metropolitano Roberto Meléndez",
    "googleSearchQuery": "Estadio Metropolitano Roberto Meléndez Barranquilla Colombia",
    "latitude": 10.9269606,
    "longitude": -74.8005364,
    "reviewAction": "search_query"
  },
  "Prado / Alto Prado Business District": {
    "name": "Barrio El Prado Barranquilla",
    "googleSearchQuery": "Barrio El Prado Barranquilla Atlántico Colombia",
    "latitude": 10.9968716,
    "longitude": -74.8012976,
    "city": "Barranquilla",
    "reviewAction": "search_query"
  },
  "Zona Franca Barranquilla": {
    "name": "Zona Franca Barranquilla",
    "googleSearchQuery": "Zona Franca Barranquilla Colombia",
    "latitude": 10.955472,
    "longitude": -74.7615033,
    "reviewAction": "google_coords"
  },
  "Malambo Airport Corridor Growth Node": {
    "name": "Aeropuerto Internacional Ernesto Cortissoz",
    "googleSearchQuery": "Aeropuerto Internacional Ernesto Cortissoz Soledad Colombia",
    "city": "Soledad",
    "latitude": 10.8865373,
    "longitude": -74.776479,
    "reviewAction": "search_query"
  },
  "Centro de Eventos Valle del Pacífico": {
    "name": "Centro de Eventos Valle del Pacífico",
    "city": "Jamundí",
    "latitude": 3.5280918,
    "longitude": -76.4978725,
    "reviewAction": "google_coords_city"
  },
  "San Antonio Historic District": {
    "name": "Barrio San Antonio Cali",
    "googleSearchQuery": "Barrio San Antonio Cali Colombia",
    "latitude": 3.4473512,
    "longitude": -76.541512,
    "reviewAction": "search_query"
  },
  "Universidad del Valle (Meléndez)": {
    "name": "Universidad del Valle",
    "googleSearchQuery": "Universidad del Valle Cali Colombia",
    "latitude": 3.3767679,
    "longitude": -76.5343197,
    "reviewAction": "search_query"
  },
  "Ciudad Jardín Business Corridor": {
    "name": "Ciudad Jardín",
    "googleSearchQuery": "Ciudad Jardín Cali Colombia",
    "latitude": 3.3695,
    "longitude": -76.5388,
    "reviewAction": "rename_search"
  },
  "Yumbo Industrial Zone": {
    "name": "Yumbo Industrial Zone",
    "city": "Yumbo",
    "latitude": 3.52083,
    "longitude": -76.499491,
    "reviewAction": "google_coords_city"
  },
  "Cali Administrative Center (CAM)": {
    "name": "Centro Administrativo Municipal de Cali",
    "googleSearchQuery": "Centro Administrativo Municipal Cali Colombia",
    "latitude": 3.4541264,
    "longitude": -76.5341021,
    "reviewAction": "search_query"
  },
  "Rodadero Beach District": {
    "name": "Playa El Rodadero",
    "latitude": 11.2036394,
    "longitude": -74.2276856,
    "reviewAction": "google_canonical"
  },
  "Parque Tayrona Gateway": {
    "name": "Parque Nacional Natural Tayrona",
    "googleSearchQuery": "Parque Nacional Natural Tayrona Santa Marta Colombia",
    "city": "Santa Marta",
    "latitude": 11.3064409,
    "longitude": -74.0657561,
    "reviewAction": "search_query"
  },
  "Puerto de Santa Marta Logistics Zone": {
    "name": "Puerto de Santa Marta",
    "googleSearchQuery": "Puerto de Santa Marta Colombia",
    "latitude": 11.2477233,
    "longitude": -74.2139889,
    "reviewAction": "search_query"
  },
  "Expofuturo Convention Center": {
    "name": "Expofuturo Event Center",
    "latitude": 4.8059592,
    "longitude": -75.7558553,
    "reviewAction": "google_canonical"
  },
  "Viaducto César Gaviria Trujillo": {
    "name": "Viaducto de Pereira Dosquebradas",
    "googleSearchQuery": "Viaducto César Gaviria Trujillo Pereira Colombia",
    "latitude": 4.8178952,
    "longitude": -75.6859867,
    "reviewAction": "google_canonical"
  },
  "Ukumarí Biopark": {
    "name": "Ukumari Park",
    "googleSearchQuery": "Bioparque Ukumarí Pereira Colombia",
    "latitude": 4.8017692,
    "longitude": -75.8123206,
    "reviewAction": "google_canonical"
  },
  "Clínica Los Rosales Pereira": {
    "name": "Clínica Los Rosales",
    "latitude": 4.8129163,
    "longitude": -75.6993839,
    "reviewAction": "google_canonical"
  },
  "Pereira CBD (Centro Financiero)": {
    "name": "Centro de Pereira",
    "googleSearchQuery": "Centro de Pereira Colombia",
    "latitude": 4.8133,
    "longitude": -75.6961,
    "reviewAction": "rename_search"
  },
  "Coffee Landscape Growth Node (Filandia Corridor)": {
    "name": "Filandia",
    "city": "Filandia",
    "submarket": "Coffee Region / Pereira",
    "googleSearchQuery": "Filandia Quindío Colombia",
    "latitude": 4.6762,
    "longitude": -75.6579,
    "reviewAction": "rename_search"
  },
  "Johnny Cay Regional Park": {
    "name": "Johnny Cay",
    "googleSearchQuery": "Acuario Johnny Cay San Andrés Colombia",
    "latitude": 12.6008,
    "longitude": -81.6881,
    "reviewAction": "search_query"
  },
  "West View Waterfront": {
    "name": "Eco Parque West View",
    "googleSearchQuery": "Eco Parque West View San Andrés Colombia",
    "latitude": 12.5210032,
    "longitude": -81.7290783,
    "reviewAction": "google_canonical"
  },
  "Hoyo Soplador Coastal Attraction": {
    "name": "Hoyo Soplador",
    "latitude": 12.4814156,
    "longitude": -81.7312115,
    "reviewAction": "google_canonical"
  },
  "Hospital Departamental Clarence Lynd Newball": {
    "name": "Hospital Clarence Lynd Newball",
    "latitude": 12.5715797,
    "longitude": -81.7092486,
    "reviewAction": "google_canonical"
  },
  "Punta Norte Entertainment Strip": {
    "name": "Punta Norte",
    "googleSearchQuery": "Punta Norte San Andrés Colombia",
    "latitude": 12.5848,
    "longitude": -81.6918,
    "reviewAction": "rename_search"
  },
  "Muelle Portofino Commercial Pier": {
    "name": "Muelle Portofino",
    "latitude": 12.5804767,
    "longitude": -81.6924239,
    "reviewAction": "google_canonical"
  },
  "Gustavo Rojas Pinilla Airport Corridor": {
    "name": "Aeropuerto Gustavo Rojas Pinilla",
    "googleSearchQuery": "Aeropuerto Gustavo Rojas Pinilla San Andrés Colombia",
    "latitude": 12.5845522,
    "longitude": -81.7092814,
    "reviewAction": "search_query"
  },
  "Zona Franca la Candelaria": {
    "name": "Zona Franca La Candelaria",
    "googleSearchQuery": "Zona Franca La Candelaria Cartagena Colombia",
    "latitude": 10.3538,
    "longitude": -75.4914,
    "reviewAction": "search_query"
  },
  "Parque Industrial Mamonal": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.3336,
    "longitude": -75.5159
  },
  "Comuna 13": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 6.2568,
    "longitude": -75.6232
  },
  "Milla de Oro El Poblado": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 6.2017,
    "longitude": -75.5719
  },
  "Gran Malecón del Río Barranquilla": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 11.0262312,
    "longitude": -74.7995889
  },
  "Barrio El Prado Barranquilla": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.9968716,
    "longitude": -74.8012976
  },
  "Barrio San Antonio Cali": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 3.4473512,
    "longitude": -76.541512
  },
  "Centro Administrativo Municipal de Cali": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 3.4541264,
    "longitude": -76.5341021
  },
  "Johnny Cay": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.6008,
    "longitude": -81.6881
  },
  "Aeropuerto Gustavo Rojas Pinilla": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.5845522,
    "longitude": -81.7092814
  }
};

export function applyColombiaPlaceReviewCorrection(point) {
  const fix = COLOMBIA_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyColombiaPlaceReviewCorrections(points) {
  return points.map(applyColombiaPlaceReviewCorrection);
}
