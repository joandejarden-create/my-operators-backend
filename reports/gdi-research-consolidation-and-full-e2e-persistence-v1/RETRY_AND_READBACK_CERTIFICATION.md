# Retry and read-back certification

| Capability | Status |
|------------|--------|
| Idempotent upsert by compositeKey | Existing store (`hotelId::opportunityId`) |
| UNCHANGED when fingerprint matches | Existing |
| Read-back helper | `readBackResearchInventory` |
| Bridge read-back option | `readBack: true` / `--readback-research-persistence` |
| Partial failure row isolation | Per-row try/catch in bridge; run continues |
| Durable ledger | Written under run `outDir` |
| Production apply | Fail-closed without env + founder token |

Deterministic bridge test: `npm run test:gdi-full-e2e-research-persistence-bridge-v1`.

