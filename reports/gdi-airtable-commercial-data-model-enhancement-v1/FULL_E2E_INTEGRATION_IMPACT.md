# Full E2E integration impact

Persistence adapter (PR #363) already:

- Routes only approved tables  
- Defers GDO without CONFIRMED/RELIABLE + entry point  
- Denies need-date / commercial / Decisions / facility / ADP  
- Uses hashed DG/Venue IDs  
- Fail-closes production apply  

## Before activating writes

| Requirement | Status |
|-------------|--------|
| Table ownership approved | D1 yes |
| Critical identity gaps | YOTEL disjoint IDs documented; Bethesda disc_* clear |
| Duplicate prevention | Idempotent upsert by identity |
| Qualification reliable | Eligibility module |
| Global vs hotel-private | Enforced in routing |
| Material-change tracking | Populate existing fields (CD-1) |
| Read-back | Required on apply |

Optional P2 enhancements must **not** block activation of the eligible Bethesda DG/PE pilot set once founder authorizes the exact write manifest.
