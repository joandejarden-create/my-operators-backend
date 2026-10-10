# Path Selection Engine

Deterministic `selectNextDiscoveryPath`:

1. Hotel-supplied history available → HOTEL_HISTORY_FIRST  
2. Recurring congress + prior cycle, no controller → HISTORICAL_PROCESS_FIRST  
3. List unpublished / PUBLIC_DATA_CEILING, no controller → DEMAND_CONTROLLER_FIRST  
4. Controller resolved, accounts unknown → ACCOUNT_FIRST (participant later)  
5. Else market-profile weighted default  

Pilot defaults:
- **AC Hotel A Coruña**: `HISTORICAL_PROCESS_FIRST` (recurring_congress_prior_cycle)
- **Radisson Hotel Santo Domingo**: `HISTORICAL_PROCESS_FIRST` (recurring_congress_prior_cycle)
- **The Westin Grand München**: `HISTORICAL_PROCESS_FIRST` (recurring_congress_prior_cycle)
