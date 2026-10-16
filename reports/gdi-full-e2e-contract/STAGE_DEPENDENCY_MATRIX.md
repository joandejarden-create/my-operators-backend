# Full GDI E2E — Stage / dependency matrix

Source of truth: `lib/group-demand-intelligence/workflow/full-e2e-contract-v1.js` (`E2E_STAGE_DEFINITIONS`).

| Letter | Stage | Coordinator stages | Depends on | Side effects / auth |
|--------|-------|--------------------|------------|---------------------|
| A | Resolve hotel identity | `validate_config` | — | none |
| B | Verify profile / catchment | `validate_config`, `reuse_evidence` | A | none |
| C | Freeze inventory / reuse | `reuse_evidence` | A–B | run-dir artifacts |
| D | Segment applicability + scope | `reuse_evidence`, `discover_gaps` | B–C | scope freeze |
| E | Substantive multisegment research | `discover_gaps`, `paid_multisegment` | D + budget auth | paid when authorized |
| F | Qualify / dedupe | `evaluate_eligibility`, `assess_fit` | E (or reuse) | artifacts |
| G | Contact depth | `identify_people`, `enrich_contacts`, `buyer_contact_enrichment` | F candidates | Surfe if `--authorize-surfe` |
| H | Hotel fit / narratives | `synthesize_hotel_relevance`, `assess_fit` | F | artifacts |
| I | Cards / drawers QC | `generate_cards` | H | artifacts |
| J | Publication diff / backups | `prepare_manifest`, `preview_verify` | I | backups under run |
| K | Canonical persistence | `apply_corporate_packages`, `apply_pe_canonical`, `apply_publish` | J | production writes when authorized |
| L | Reports / external links | `reconcile_run`, `evaluate_completion` | K (qualified set) | report artifacts; no token commit |
| M | Local delivery verify | `local_delivery_verify`, `preview_verify` | L inventory | none |
| N | Production delivery verify | `production_delivery_verify` | deploy if needed + K/L | deploy only when code requires |
| O | Reconcile + computed status | `reconcile_run`, `evaluate_completion` | all prior | writes completion JSON |

**Resume:** stages marked `completed` in `stage-completion-ledger.json` are skipped on `--resume-run`.  
**Fail closed:** missing required budget ledgers, dry-run with paid/write attempts, unsupported `NOT_APPLICABLE`.

Machine-readable copy: `full-e2e-contract.json`.
