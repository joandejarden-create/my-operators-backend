# DURABILITY_QA

| Check | Result |
|-------|--------|
| Classic published manifest on disk | YES (`data/ai-demand-positioning/published/adp_radisson_blu_toronto_downtown/`) |
| Share registry token ACTIVE | YES (`sht_a07147cf287fc81fa9afca26`) |
| Local server restart (:8091) resolves Toronto | YES |
| Production share resolve after deploy | YES (prod-HMAC token) |
| Core/Loyalty period refs persist | YES (`c8bd8b` / `308ba1`) |
| No local-only dependency for customer URL | YES (Railway production) |
