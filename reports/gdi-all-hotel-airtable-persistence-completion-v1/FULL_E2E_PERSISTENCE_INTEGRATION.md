# Full E2E persistence integration

## Target pipeline

Discovery → Screening → Research → Identity Resolution → **Research Persistence** → Commercial Qualification → **GDO Persistence** → Read-Back → Weekly Change → Controlled Publication

## Adapter posture

- Extend existing Full E2E persistence adapter / eligibility — do not build a parallel engine
- Research persistence writes target proposed research table once schema-approved
- GDO writes remain gated by commercial eligibility
- Customer publication remains outside automatic persistence
- Production mutations remain fail-closed until exact allowlist authorization

## This package

- Implements classification + all-hotel dry-run manifests
- Does **not** enable bulk apply
- Schema create blocked (`SCHEMA_CHANGES_REQUIRE_FOUNDER_APPROVAL`)
