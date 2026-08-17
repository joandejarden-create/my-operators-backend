# Brand Explorer Active Profile Readiness Prioritization Audit vv28A

- Generated: 2026-07-10T02:35:33.019Z
- Mode: **dry-run**
- All six contract 100: **yes**
- Tribute active-profile ready: **yes**
- Recommended next brand: **Radisson by Choice**
- Multi-brand apply-approved safe: **no**
- Airtable modified: **no**

## Contract status (all active brands)
| Brand | Contract | Active-profile | Final QA | Ease score |
| --- | ---: | --- | ---: | ---: |
| Tribute Portfolio | 100 | ready | 95 | 208.6 |
| Curio Collection by Hilton | 100 | blocked | 61 | -37.2 |
| Kimpton Hotels | 100 | blocked | 46 | 0.7 |
| Radisson Blu by Choice | 100 | blocked | 57 | 50.2 |
| Radisson by Choice | 100 | blocked | 55 | 53.1 |
| Ascend Hotel Collection | 100 | blocked | 58 | 52.4 |

## Ranked blocked brands (easiest path first)
1. **Radisson by Choice** (`radisson`) — Final QA 55, ease 53.1, primary blocker: `source_fact_governance`, next writer: `v24C_source_evidence_work`
2. **Ascend Hotel Collection** (`ascend`) — Final QA 58, ease 52.4, primary blocker: `source_fact_governance`, next writer: `v24C_source_evidence_work`
3. **Radisson Blu by Choice** (`radisson-blu`) — Final QA 57, ease 50.2, primary blocker: `source_fact_governance`, next writer: `v24C_source_evidence_work`
4. **Kimpton Hotels** (`kimpton`) — Final QA 46, ease 0.7, primary blocker: `true_content_gaps`, next writer: `v24C_source_evidence_work`
5. **Curio Collection by Hilton** (`curio-collection`) — Final QA 61, ease -37.2, primary blocker: `true_content_gaps`, next writer: `v24C_source_evidence_work`

### Radisson by Choice
- Critical/high defects: 2/4
- Visual defects (total): 7
- Carryover defects: 0
- Pending facts: 3 · FDD: 0 · Internal: 0
- Missing images / empty cards: 0/0
- Governed platform ready: yes
- Blocker buckets:
  - **visual_image_work**: 7 item(s)
  - **source_fact_governance**: 2 item(s)

### Ascend Hotel Collection
- Critical/high defects: 2/4
- Visual defects (total): 8
- Carryover defects: 1
- Pending facts: 1 · FDD: 0 · Internal: 0
- Missing images / empty cards: 0/0
- Governed platform ready: yes
- Blocker buckets:
  - **visual_image_work**: 7 item(s)
  - **copy_carryover_cleanup**: 1 item(s)
  - **source_fact_governance**: 2 item(s)

### Radisson Blu by Choice
- Critical/high defects: 2/4
- Visual defects (total): 8
- Carryover defects: 1
- Pending facts: 3 · FDD: 0 · Internal: 0
- Missing images / empty cards: 0/0
- Governed platform ready: yes
- Blocker buckets:
  - **visual_image_work**: 7 item(s)
  - **copy_carryover_cleanup**: 1 item(s)
  - **source_fact_governance**: 2 item(s)

### Kimpton Hotels
- Critical/high defects: 1/5
- Visual defects (total): 7
- Carryover defects: 0
- Pending facts: 44 · FDD: 0 · Internal: 0
- Missing images / empty cards: 0/0
- Governed platform ready: no
- Blocker buckets:
  - **visual_image_work**: 7 item(s)
  - **source_fact_governance**: 3 item(s)
  - **true_content_gaps**: 1 item(s)

### Curio Collection by Hilton
- Critical/high defects: 1/4
- Visual defects (total): 7
- Carryover defects: 0
- Pending facts: 123 · FDD: 99 · Internal: 0
- Missing images / empty cards: 0/0
- Governed platform ready: no
- Blocker buckets:
  - **visual_image_work**: 6 item(s)
  - **source_fact_governance**: 4 item(s)
  - **true_content_gaps**: 1 item(s)

## Recommendations
- **Next brand end-to-end:** Radisson by Choice
- **Next writer to build/run:** v26A_copy_carryover_cleanup (carryover on Ascend/Radisson Blu) then v24C_source_evidence_work visual/thin-copy batch
- **Multi-brand visual cleanup safe:** no
- **Apply-approved safe:** no

```bash
npm run brand-explorer-complete-build -- --brand radisson --dry-run --target-quality active-profile
```