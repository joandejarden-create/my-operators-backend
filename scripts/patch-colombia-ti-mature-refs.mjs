#!/usr/bin/env node
import { readFileSync, writeFileSync } from "fs";

const p = "lib/radar-buildout/colombia-ti-mature-travel-infrastructure-delta.js";
let s = readFileSync(p, "utf8");
const refs = [
  "https://www.puertocartagena.com/cruise-secondary-berth",
  "https://www.invias.gov.co/cartagena-walled-city-access",
  "https://www.transmilenio.gov.co/portal-norte",
  "https://www.aerocivil.gov.co/el-dorado-cargo-corridor",
  "https://www.metrodemedellin.gov.co/estacion-envigado",
  "https://www.aerocivil.gov.co/rionegro-cargo-access",
  "https://www.barranquilla.gov.co/carnival-corridor",
  "https://www.puertobarranquilla.com/secondary-berth",
  "https://www.invias.gov.co/cali-riverfront",
  "https://www.aerocivil.gov.co/clo-cargo-access",
  "https://www.parquesnacionales.gov.co/tayrona-highway",
  "https://www.colombia.travel/en/santa-marta/rodadero-marina",
  "https://www.aerocivil.gov.co/armenia-airport-access",
  "https://www.manizales.gov.co/cable-metro",
  "https://www.aerocivil.gov.co/san-andres-san-luis-runway",
  "https://www.colombia.travel/en/san-andres/johnny-cay-ferry",
];
let i = 0;
s = s.replace(/sourceReference: "[^"]+"/g, () => `sourceReference: "${refs[i++]}"`);
writeFileSync(p, s);
console.log("updated", i, "source references");
