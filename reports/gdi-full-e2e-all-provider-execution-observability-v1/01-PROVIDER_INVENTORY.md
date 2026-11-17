# Provider inventory

See `lib/group-demand-intelligence/observability/gdi-provider-inventory-v1.js` for the authoritative inventory consumed by the admin monitor.

Key rule: **an installed package or enabled Actor is not proof of execution.** The provider matrix records `callsSubmitted` from the durable ledger only.
