/**
 * Strategy-aligned Founder Project Plan updates (master to-do + phase rollup hygiene).
 * Applied by scripts/sync-founder-project-plan-state.mjs
 */
import { MAP_MASTER_TODO as F } from "./master-todo-field-map.js";

/** Master to-do rows matched by Seed ID in Success Metric. */
export const MASTER_TODO_SYNC_UPDATES = [
  {
    seedId: "mt-04",
    fields: {
      [F.status]: "Needs Review",
      [F.nextAction]: "Final read-through; save final version for copy/paste use.",
      [F.dueDate]: "2026-07-05",
    },
  },
  {
    seedId: "mt-05",
    fields: {
      [F.status]: "Needs Review",
      [F.nextAction]: "Polish templates; store final version in master GTM notes.",
      [F.dueDate]: "2026-07-05",
    },
  },
  {
    seedId: "mt-06",
    fields: {
      [F.status]: "Needs Review",
      [F.nextAction]: "Review call script; confirm ready for first booked calls.",
      [F.dueDate]: "2026-07-05",
    },
  },
  {
    seedId: "mt-07",
    fields: {
      [F.progress]: "55%",
      [F.nextAction]:
        "Wave 1 (11 contacts, Jul 2) is fully tracked. Clean up ~15 June 16 sends: add Mail Merge Batch, Send Channel, and Next Follow-Up Date where missing.",
    },
  },
  {
    seedId: "mt-08",
    fields: {
      [F.progress]: "90%",
      [F.nextAction]:
        "Diego Fernández (Altiplano) call held Jul 15 on live San José del Cabo / Mijares 32 project; Brand/Operator/F&B proposal drafted (AO Word doc Jul 15). Send proposal, track signature/deposit, and continue monitoring Wave 1 replies (Abelardo Spain referrals, Miguel pending).",
      [F.dueDate]: "2026-07-25",
    },
  },
  {
    seedId: "mt-10",
    fields: {
      [F.status]: "In Progress",
      [F.nextAction]:
        "Tailor pilot intake questions to Altiplano Mijares 32 (brand purpose, operator role, residential association, F&B structure, design/permitting timing).",
      [F.dueDate]: "2026-07-22",
    },
  },
  {
    seedId: "mt-11",
    fields: {
      [F.dueDate]: "2026-07-22",
      [F.nextAction]:
        "Draft Altiplano-facing materials checklist aligned to Brand/Operator/F&B proposal assumptions (deck, program, timing, known brand/operator conversations).",
    },
  },
  {
    seedId: "mt-14",
    fields: {
      [F.dueDate]: "2026-07-18",
      [F.nextAction]:
        "Smoke test owner/advisor access before provisioning Diego / Altiplano as first real pilot user.",
    },
  },
  {
    seedId: "mt-15",
    fields: {
      [F.dueDate]: "2026-07-22",
      [F.nextAction]:
        "Document repeatable pilot invite checklist; use Altiplano / Diego as first live provisioning case once engagement is accepted.",
    },
  },
  {
    seedId: "mt-20",
    fields: {
      [F.dueDate]: "2026-07-25",
      [F.nextAction]:
        "Include Altiplano / Diego Jul 15 call + proposal path in Wave 1 performance summary alongside Abelardo, Daniel Shamah, and other reply signals.",
    },
  },
  {
    seedId: "mt-22",
    fields: {
      [F.dueDate]: "2026-08-14",
      [F.nextAction]:
        "Run first pilot opportunity QA against Altiplano Mijares 32 once engagement is accepted and Diego is provisioned.",
    },
  },
];

/**
 * Phase rollup rows — intentionally empty unless a fresh audit sets targets.
 * Stale Jul 3 rollup status patches were regressing Completed phases.
 */
export const PHASE_ROLLUP_SYNC_UPDATES = [];
