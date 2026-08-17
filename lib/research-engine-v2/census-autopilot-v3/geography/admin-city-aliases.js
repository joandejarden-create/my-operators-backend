/**
 * City / destination / municipality aliases → State / Region (Dealality-owned).
 * Used after address parse and before coordinate bbox. No Cvent/legacy/STR.
 */

function norm(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** @type {Record<string, Record<string, string>>} country → cityNorm → admin label */
export const CITY_TO_ADMIN = Object.freeze({
  Brazil: {
    belem: "Pará",
    "belem para": "Pará",
    "belem, para": "Pará",
    manaus: "Amazonas",
    "sao paulo": "São Paulo",
    "cidade moncoes": "São Paulo",
    pinheiros: "São Paulo",
    jardins: "São Paulo",
    bauru: "São Paulo",
    campinas: "São Paulo",
    santos: "São Paulo",
    guarulhos: "São Paulo",
    "rio de janeiro": "Rio de Janeiro",
    curitiba: "Paraná",
    "porto alegre": "Rio Grande do Sul",
    farroupilha: "Rio Grande do Sul",
    salvador: "Bahia",
    recife: "Pernambuco",
    fortaleza: "Ceará",
    brasilia: "Distrito Federal",
    goiania: "Goiás",
    "belo horizonte": "Minas Gerais",
    florianopolis: "Santa Catarina",
    "foz do iguacu": "Paraná",
  },
  Argentina: {
    "buenos aires": "Buenos Aires",
    pilar: "Buenos Aires",
    neuquen: "Neuquén",
    "villa la angostura": "Neuquén",
    cordoba: "Córdoba",
    mendoza: "Mendoza",
    rosario: "Santa Fe",
    "santiago del estero": "Santiago del Estero",
    "san miguel de tucuman": "Tucumán",
    tucuman: "Tucumán",
    salta: "Salta",
    "villa san lorenzo": "Salta",
    "san lorenzo": "Salta",
    ituzaingo: "Corrientes",
    corrientes: "Corrientes",
    bariloche: "Río Negro",
    "san carlos de bariloche": "Río Negro",
  },
  "Costa Rica": {
    "san jose": "San José",
    "san josé": "San José",
    guanacaste: "Guanacaste",
    "santa cruz": "Guanacaste",
    "playa conchal": "Guanacaste",
    "reserva conchal": "Guanacaste",
    papagayo: "Guanacaste",
    liberia: "Guanacaste",
    tamarindo: "Guanacaste",
    puntarenas: "Puntarenas",
    herradura: "Puntarenas",
    "playa herradura": "Puntarenas",
    "los suenos": "Puntarenas",
    "manuel antonio": "Puntarenas",
    jaco: "Puntarenas",
    monteverde: "Puntarenas",
    alajuela: "Alajuela",
    heredia: "Heredia",
    cartago: "Cartago",
    limon: "Limón",
  },
  Jamaica: {
    "montego bay": "St. James",
    "st james": "St. James",
    "saint james": "St. James",
    "rose hall": "St. James",
    kingston: "Kingston",
    "port antonio": "Portland",
    portland: "Portland",
    westmoreland: "Westmoreland",
    bluefields: "Westmoreland",
    "ocho rios": "St. Ann",
    negril: "Westmoreland",
    "runaway bay": "St. Ann",
  },
  Barbados: {
    bridgetown: "Saint Michael",
    "bridgetown christ church": "Christ Church",
    "christ church": "Christ Church",
    "st philip": "Saint Philip",
    "st. philip": "Saint Philip",
    "saint philip": "Saint Philip",
    "st michael": "Saint Michael",
    "saint michael": "Saint Michael",
    speightstown: "Saint Peter",
    holetown: "Saint James",
  },
  Mexico: {
    cancun: "Quintana Roo",
    "cancún": "Quintana Roo",
    "playa del carmen": "Quintana Roo",
    tulum: "Quintana Roo",
    "cabo san lucas": "Baja California Sur",
    "san jose del cabo": "Baja California Sur",
    "los cabos": "Baja California Sur",
    "puerto vallarta": "Jalisco",
    "mexico city": "Ciudad de México",
    "ciudad de mexico": "Ciudad de México",
    guadalajara: "Jalisco",
    monterrey: "Nuevo León",
    merida: "Yucatán",
  },
  "Dominican Republic": {
    "punta cana": "La Altagracia",
    "santo domingo": "Distrito Nacional",
    sosua: "Puerto Plata",
    "sosúa": "Puerto Plata",
    "puerto plata": "Puerto Plata",
    "costa norte": "Puerto Plata",
    "cabarete": "Puerto Plata",
  },
  Colombia: {
    bogota: "Cundinamarca",
    "bogotá": "Cundinamarca",
    cartagena: "Bolívar",
    medellin: "Antioquia",
    "medellín": "Antioquia",
    barranquilla: "Atlántico",
    cali: "Valle del Cauca",
    "santa marta": "Magdalena",
  },
  Cuba: {
    havana: "La Habana",
    habana: "La Habana",
    "la habana": "La Habana",
    vedado: "La Habana",
    varadero: "Matanzas",
    "cayo coco": "Ciego de Ávila",
    "cayo guillermo": "Ciego de Ávila",
    "cayo santa maria": "Villa Clara",
    holguin: "Holguín",
    guardalavaca: "Holguín",
    "santiago de cuba": "Santiago de Cuba",
    trinidad: "Sancti Spíritus",
    cienfuegos: "Cienfuegos",
    vinales: "Pinar del Río",
    camaguey: "Camagüey",
    "cayo largo": "Isla de la Juventud",
    baracoa: "Guantánamo",
  },
  Peru: {
    lima: "Lima",
    cusco: "Cusco",
    "machu picchu": "Cusco",
    arequipa: "Arequipa",
  },
  Chile: {
    santiago: "Región Metropolitana",
    valparaiso: "Valparaíso",
    "vina del mar": "Valparaíso",
  },
  Ecuador: {
    quito: "Pichincha",
    guayaquil: "Guayas",
  },
  Uruguay: {
    montevideo: "Montevideo",
    "punta del este": "Maldonado",
  },
});

/** Name tokens that imply admin unit when city missing (deterministic brand/destination cues). */
export const NAME_TO_ADMIN = Object.freeze({
  "Costa Rica": [
    { re: /guanacaste|papagayo|conchal|costa elena|tamarindo|liberia|planet hollywood/i, admin: "Guanacaste" },
    { re: /los sue[nñ]os|herradura|manuel antonio|jac[oó]|puntarenas|belmar|monteverde/i, admin: "Puntarenas" },
    { re: /san jos[eé]|central valley/i, admin: "San José" },
    { re: /\besh\b.*hotel|esh hotel/i, admin: "Puntarenas" },
  ],
  Brazil: [{ re: /amaz[oô]nia|manaus|bel[eé]m|belem/i, admin: null }], // handled per match below
  "Dominican Republic": [{ re: /sos[uú]a|puerto plata|costa norte|ocean club/i, admin: "Puerto Plata" }],
  Argentina: [{ re: /corrientes/i, admin: "Corrientes" }, { re: /neuqu[eé]n/i, admin: "Neuquén" }],
});

export function lookupCityAdmin(country, city) {
  const map = CITY_TO_ADMIN[country];
  if (!map) return null;
  const n = norm(city);
  if (!n) return null;
  if (map[n]) return { value: map[n], method: "city_alias", city_norm: n };
  // partial: "Belem, Pará" already in map; also try first token / last token
  for (const [k, v] of Object.entries(map)) {
    if (n.includes(k) || k.includes(n)) return { value: v, method: "city_alias_partial", city_norm: n };
  }
  return null;
}

export function lookupNameAdmin(country, name) {
  const rules = NAME_TO_ADMIN[country];
  if (!rules) return null;
  for (const r of rules) {
    if (r.admin && r.re.test(String(name || ""))) {
      return { value: r.admin, method: "name_destination_cue" };
    }
  }
  // Brazil Amazonia hotel in Manaus
  if (country === "Brazil" && /amaz[oô]nia|manaus/i.test(String(name || ""))) {
    return { value: "Amazonas", method: "name_destination_cue" };
  }
  if (country === "Brazil" && /bel[eé]m|belem|ananindeua|maiorana/i.test(String(name || ""))) {
    return { value: "Pará", method: "name_destination_cue" };
  }
  return null;
}

export { norm as normGeoLabel };
