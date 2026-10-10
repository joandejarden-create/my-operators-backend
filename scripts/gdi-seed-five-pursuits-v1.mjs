#!/usr/bin/env node
/**
 * Seed five GDI Pursuit records for AC Coruña + Radisson SD Watches.
 * Does NOT change Ready/Watch gates. Does NOT mark contacted.
 *
 * Usage: node scripts/gdi-seed-five-pursuits-v1.mjs [--apply]
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadOpportunitiesCanonical,
  upsertSingleOpportunity,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  startPursuitFromOpportunity,
  listPursuits,
  toCustomerPursuitDto,
} from "../lib/group-demand-intelligence/pursuit/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi/pursuit-workflow-v1");
const APPLY = process.argv.includes("--apply");
const NOW = "2026-10-07";

const AC = "rec2PVBDavppGpenm";
const RAD = "recUOyzOXn2Zdp98I";
const BETHESDA = "recLuxvwwxID7U2B8";
const YOTEL = "recrPQcZg7SFARRb2";

const DRAFTS = {
  gdi_opp_biocultura_a_coruna_2027: {
    language: "es",
    subject: "AC Hotel A Coruña — alojamiento expositores BioCultura 2027",
    message: `Hola,

Nos gustaría que AC Hotel A Coruña fuera considerado para el alojamiento de expositores y visitantes de BioCultura A Coruña 2027 (5–7 de marzo, EXPOCoruña).

Estamos en el corredor Expocoruña / Matogrande y apoyamos grupos de feria de tamaño medio. ¿Podrían indicarnos si se publicará un listado de hoteles preferentes y cómo se seleccionan?

Quedamos a disposición para enviar tarifas y condiciones de grupo.

Un cordial saludo`,
  },
  gdi_opp_international_symposium_6: {
    language: "es",
    subject: "AC Hotel A Coruña — interés en alojamiento IAPS Spaces in Transition 2027",
    message: `Estimados profesores García Mira y García Fontán,

Enhorabuena por el simposio IAPS Spaces in Transition en A Coruña (14–16 de junio de 2027).

AC Hotel A Coruña (AC Hotels by Marriott) desearía ser considerado cuando se definan las opciones de alojamiento preferente. Estamos en la zona Matogrande / Expocoruña y apoyamos grupos académicos y profesionales.

¿Podrían indicarnos cómo se gestionará la selección de hoteles y cuándo prevé publicarse la información de alojamiento?

Quedamos a su disposición.

Un cordial saludo`,
  },
  gdi_opp_rif_filosofia_sd_2027: {
    language: "es",
    subject: "Radisson Santo Domingo — convenio de alojamiento VII Congreso RIF 2027",
    message: `Estimados miembros del Comité Organizador,

Hemos visto que se están gestionando tarifas preferenciales para el VII Congreso Iberoamericano de Filosofía (15–19 de marzo de 2027) y que el listado de hoteles en convenio se publicará en una próxima circular.

Radisson Hotel Santo Domingo (Naco) desea ser considerado para ese listado. Podemos apoyar a ponentes y participantes con tarifas de grupo y ubicación en el Distrito Nacional.

¿Quién es el contacto adecuado para la selección de hoteles y cuándo se espera la circular?

Un cordial saludo`,
  },
  gdi_opp_cielo_laboral_sd_2026: {
    language: "es",
    subject: "Radisson Santo Domingo — listado de hoteles recomendados CIELO Laboral 2026",
    message: `Hola,

Hemos revisado el listado de hoteles recomendados para el 6º Congreso Mundial CIELO Laboral (2–4 de diciembre de 2026, PUCMM Santo Domingo).

Radisson Hotel Santo Domingo desea ser incluido en el listado recomendado para participantes internacionales. Podemos facilitar tarifas y ubicación respecto al campus.

¿Pueden confirmar el contacto adecuado y si el listado aún puede actualizarse?

Un cordial saludo`,
  },
  gdi_opp_autoamericas_2027: {
    language: "es",
    subject: "Radisson Santo Domingo — hotel asociado AUTOAMERICAS 2027",
    message: `Estimado Andrés,

Vemos que Hotel Dominican Fiesta es el hotel y sede oficial de AUTOAMERICAS 2027 (23–24 de abril). Radisson Santo Domingo figuró entre las opciones de alojamiento asociadas en 2026 y desearíamos ser considerados de nuevo como hotel asociado secundario para 2027.

¿Podrían indicarnos el proceso y calendario para convenios de hoteles asociados mientras las tarifas 2027 están pendientes de anuncio?

Un cordial saludo`,
  },
};

const SEED = [
  { hotelId: AC, opportunityId: "gdi_opp_biocultura_a_coruna_2027", key: "BIOCULTURA" },
  { hotelId: AC, opportunityId: "gdi_opp_international_symposium_6", key: "IAPS" },
  { hotelId: RAD, opportunityId: "gdi_opp_rif_filosofia_sd_2027", key: "RIF" },
  { hotelId: RAD, opportunityId: "gdi_opp_cielo_laboral_sd_2026", key: "CIELO" },
  { hotelId: RAD, opportunityId: "gdi_opp_autoamericas_2027", key: "AUTOAMERICAS" },
];

async function countReady(hotelId) {
  const ops = (await loadOpportunitiesCanonical(hotelId)).opportunities || [];
  return ops.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok).length;
}

const bethReadyBefore = await countReady(BETHESDA);
const yotelReadyBefore = await countReady(YOTEL);

const created = [];
for (const row of SEED) {
  const doc = await loadOpportunitiesCanonical(row.hotelId);
  const opp = (doc.opportunities || []).find((o) => o.id === row.opportunityId);
  if (!opp) {
    created.push({ ...row, ok: false, error: "opportunity_missing" });
    continue;
  }
  const draft = DRAFTS[row.opportunityId];
  if (!APPLY) {
    created.push({
      ...row,
      ok: true,
      dryRun: true,
      title: opp.title,
      outreachReadiness: opp.outreachReadiness,
      expectedStatus:
        String(opp.outreachReadiness).toUpperCase() === "OUTREACH_PREPARE"
          ? "PREPARE"
          : "OUTREACH_READY",
    });
    continue;
  }
  // Stamp drafts onto opp payload for pursuit create (intelligence fields untouched)
  const stamped = {
    ...opp,
    draftSubject: draft.subject,
    draftMessage: draft.message,
    draftLanguage: draft.language,
  };
  const result = startPursuitFromOpportunity(row.hotelId, stamped, {
    actor: "seed_pursuit_workflow_v1",
    language: draft.language,
    draftSubject: draft.subject,
    draftMessage: draft.message,
  });
  if (result.ok && result.pursuit) {
    try {
      await upsertSingleOpportunity(row.hotelId, {
        ...stamped,
        pursuitId: result.pursuit.pursuitId,
        pursuitStatus: result.pursuit.pursuitStatus,
      });
    } catch {
      /* store remains SoT */
    }
  }
  created.push({
    ...row,
    ok: result.ok,
    created: result.created,
    title: opp.title,
    pursuitId: result.pursuit?.pursuitId,
    pursuitStatus: result.pursuit?.pursuitStatus,
    contactEmail: result.pursuit?.contactEmail,
    nextAction: result.pursuit?.nextAction,
  });
}

const bethReadyAfter = await countReady(BETHESDA);
const yotelReadyAfter = await countReady(YOTEL);
const acReady = await countReady(AC);
const radReady = await countReady(RAD);

fs.mkdirSync(OUT, { recursive: true });

const csvRows = created.map((r) => ({
  key: r.key,
  hotelId: r.hotelId,
  opportunityId: r.opportunityId,
  title: r.title || "",
  pursuitId: r.pursuitId || "",
  status: r.pursuitStatus || r.expectedStatus || "",
  ok: r.ok,
}));
const cols = Object.keys(csvRows[0] || { key: "" });
fs.writeFileSync(
  path.join(OUT, "FIVE_INITIAL_PURSUITS.csv"),
  [cols.join(",")]
    .concat(
      csvRows.map((r) =>
        cols
          .map((c) => {
            const s = String(r[c] ?? "");
            return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
          })
          .join(",")
      )
    )
    .join("\n") + "\n"
);

fs.writeFileSync(
  path.join(OUT, "OUTREACH_DRAFTS.md"),
  `# Outreach drafts (Spanish)\n\n${Object.entries(DRAFTS)
    .map(
      ([id, d]) => `## ${id}\n**Language:** ${d.language}\n**Subject:** ${d.subject}\n\n\`\`\`\n${d.message}\n\`\`\`\n`
    )
    .join("\n")}`
);

const acP = APPLY ? listPursuits(AC).map(toCustomerPursuitDto) : [];
const radP = APPLY ? listPursuits(RAD).map(toCustomerPursuitDto) : [];

console.log(
  JSON.stringify(
    {
      apply: APPLY,
      created,
      readyCounts: {
        ac: acReady,
        rad: radReady,
        bethesdaBefore: bethReadyBefore,
        bethesdaAfter: bethReadyAfter,
        yotelBefore: yotelReadyBefore,
        yotelAfter: yotelReadyAfter,
        bethesdaChanged: bethReadyBefore !== bethReadyAfter,
        yotelChanged: yotelReadyBefore !== yotelReadyAfter,
      },
      acPursuitCount: acP.length,
      radPursuitCount: radP.length,
    },
    null,
    2
  )
);
