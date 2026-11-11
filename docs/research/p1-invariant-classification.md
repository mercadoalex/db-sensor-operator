# P1 (sub-question 2): which business invariants does the database already enforce, and which could CDC content inspection see?
Investigation date: 2026-11-11 (read-only research on public source code; no repo code changed, nothing run against a database).
Feeds: R9 kill criterion and sub-question 2 in [`STATUS.md`](STATUS.md); companion to [`p0-public-postmortems.md`](p0-public-postmortems.md).
Language note: written in English like the other research capture files.

## 1. Summary answer

**Question.** In real open-source applications with public schemas, what share of business invariants is (A) already enforced by a database constraint or trigger, (B) enforced only in application code, or (C) not findably enforced anywhere? For B and C, could a WAL change event (row before/after + commit time, no code path) detect a violation?

**Short answer.** I read 4 projects (all PostgreSQL: Mastodon, Discourse, Saleor, Odoo) and classified **56 invariants**, each cited to a file and line.

| Class | Count | Share of 56 |
|---|---|---|
| A: DB already enforces it | 18 | 32% |
| B, CDC catchable from one row's before/after (R) | 6 | 11% |
| B, CDC catchable only with extra state: cross-row, cross-table, history (S) | 23 | 41% |
| B, CDC could not catch either (N) | 3 | 5% |
| C, CDC catchable from one row (R) | 6 | 11% |
| C, needs extra state (S) | 0 | 0% |
| C, not catchable (N) | 0 | 0% |

Three results matter for R9, and two of them cut against the project:

1. **Most of what the DB does not enforce needs more than a per-row content policy.** Of the 38 non-A invariants, 12 (32%) are decidable from a single row event, 23 (61%) need state the event does not carry (a join, an aggregate, or earlier events), and 3 (8%) are not decidable at all from WAL. A "content policy over before/after rows" covers about one third of the gap, not most of it.
2. **Even the 12 row-local ones are mostly things a CHECK constraint could express.** 9 of the 12 are single-row predicates (`quantity_fulfilled <= quantity`, `counter >= 0`, ...). Only 3 are real before/after transition rules that a CHECK cannot express (a trigger could): Saleor `charge_status` transitions, Odoo hash-protected posted entries, Odoo sale order `state` transitions. So CDC's expressiveness edge over database-side prevention is thin in this sample (3 of 56, 5%). Its real edge, in these data, is operational: it can watch without blocking writes and without a schema change. Discourse is a concrete case: it added `CHECK (topic_count >= 0)` on `user_stats` and removed it about a month later (section 3.2, D7).
3. **Derived-data drift is real, known to the maintainers, and mostly needs aggregation.** Discourse, Odoo and Mastodon each ship repair or clamp code for counters and denormalized data (`PostRevision.ensure_consistency!`, `Topic.reset_all_highest!`, `stock.quant._clean_reservations`, read-time clamping of `account_stats`). That is evidence the invariants break in production. But 5 of the 6 such cases are aggregate equalities (S), which is exactly where a counter-based alert or a periodic reconciliation query is the natural competitor, not a row-level content policy.

**Verdict for sub-question 2.** The evidence supports a narrow applicability claim: CDC content inspection is useful for single-row and transition rules on tables where the owner will not (or cannot) add a constraint or trigger. It does not support a broad claim. It does not move R9 by itself; it sharpens what R9 should measure (section 5).

**Confidence and bias (read before quoting numbers).** One reader, 4 projects, hand-selected rules. Invariants were found mostly where they are written down (constraints, validators), so A and B are over-represented and C is under-counted: a real C is an absence, and absences are found by looking at adjacent columns, not by grep. "C = 6 of 56" should be read as a floor. The R/S/N judgments are mine and some are arguable (flagged inline). Treat the fractions as ±10 points.

## 2. Method

**Projects and why.** All four have a public schema and visible business-rule code, and are PostgreSQL-only. No MySQL project is in the sample.

| Project | Domain | Source read | Revision |
|---|---|---|---|
| Mastodon (Rails) | social graph, polls, counters | `db/schema.rb`, `app/models`, `app/validators`, `app/services` | `main`, SHA near `b31c484` |
| Discourse (Rails) | forum | `db/migrate` (1780 files, grepped for CHECK/trigger), `app/models`, `lib/validators` | `ae2b267b` (sparse clone) |
| Saleor (Django) | e-commerce, payments, stock | `saleor/{payment,order,warehouse,giftcard,discount}/*.py` | `main`, SHA near `ea4a69f` |
| Odoo (Python) | ERP, accounting, stock | `addons/{account,sale,stock}/models/*.py` | `master`, SHA near `ad3de26` |

SHAs for Mastodon, Saleor and Odoo were read from the branch head right after fetching, so they are approximate. Discourse is exact. Not examined: Saleor's GraphQL mutation layer and its migrations; Odoo's `account_move_line` write paths beyond the lines cited; any hidden DB trigger not visible in the files above.

**Categories (exactly one per invariant).**
- **A**: a CHECK, UNIQUE, FK, or trigger exists in the schema/migrations/model constraint declarations.
- **B**: only application code checks or maintains it (validator, service, callback, job). A direct SQL write bypasses it.
- **C**: I found no enforcement in DB or app layer after a reasonable search. Stated as "not found", not "does not exist".

**CDC verdict for B and C (what a WAL event carries: columns before and after, commit time; not the code path, not the actor, not the session).**
- **R** (row-local): decidable from the old and new image of one row plus static constants.
- **S** (needs state): decidable only with another table, an aggregate over other rows, history of earlier events, or reference data. A stateful processor could do it; a per-row policy cannot.
- **N** (not catchable): depends on the actor, a bypass path invisible in the row, external state (Redis, API), or information destroyed by the write itself.

Caveats on A: Django's `PositiveIntegerField` emits `"col" >= 0` on PostgreSQL (`django/db/backends/postgresql/base.py:133-137`); I did not open the Saleor migrations, so those A rows are marked "inferred". Discourse index entries come from the `# Indexes` annotation blocks at the bottom of the model files (annotate gem output), not from migrations.

## 3. Evidence by project

### 3.1 Mastodon (15 invariants: A 3, B 11, C 1)

Observation: `db/schema.rb` contains no `check_constraint` and no triggers (0 matches). Constraints are unique indexes and foreign keys only.

| ID | Invariant | Cls | Evidence | CDC |
|---|---|---|---|---|
| M1 | One follow per (account, target) | A | `db/schema.rb:691` unique index `index_follows_on_account_id_and_target_account_id` | n/a |
| M2 | Deleting a poll deletes its votes | A | `db/schema.rb:1669` `add_foreign_key "poll_votes", "polls", on_delete: :cascade` | n/a |
| M3 | Username unique per domain, case-insensitive | A | `db/schema.rb:228` unique index on `lower(username), COALESCE(lower(domain),'')` | n/a |
| M4 | One vote per account on a single-choice poll; one per choice on multiple-choice | B | `app/validators/vote_validator.rb:14-48`. `poll_votes` has only non-unique indexes (`db/schema.rb:1013-1022`) | S (needs `polls.multiple` and prior votes) |
| M5 | Vote choice index within `[0, options.size)` | B | `vote_validator.rb:26-28` | S (needs `polls.options` from another table) |
| M6 | No voting on your own poll | B | `vote_validator.rb:30-32` | S (`polls.account_id`) |
| M7 | No vote after the poll expired | B | `vote_validator.rb:5` via `poll_expired?` | S (`polls.expires_at` is in another table) |
| M8 | Local poll expiry between 5 minutes and 1 month from now | B | `app/validators/poll_expiration_validator.rb:4-5,13-14`; applied only `if: local?` (`app/models/poll.rb:43`) | S (arithmetic is row-local with `created_at`, but "local" needs an account lookup; without it remote polls false-positive) |
| M9 | Local status text <= 500 grapheme clusters, URLs count as 23 | B | `app/validators/status_length_validator.rb:4-6,9,16-18` | R (columns `text`, `spoiler_text`, `local` are in the `statuses` row). Must replicate the URL/mention counting or it will misfire |
| M10 | One reblog of a status per account | B | `app/models/status.rb:124`; index on `(reblog_of_id, account_id)` is not unique (`db/schema.rb:1352`) | S (duplicate detection over earlier rows) |
| M11 | `rule_ids` only on `violation` reports | B | `app/models/report.rb:54`; both columns exist in `reports` | R |
| M12 | `followers_count`/`following_count` equal the number of follow rows | B | maintained by callbacks `app/models/follow.rb:46,62-70` and `app/models/concerns/account/counters.rb:23-45`; no constraint (`db/schema.rb:114-122`) | S (aggregate over `follows`) |
| M13 | A block implies no follow in either direction | B | `app/services/block_service.rb:26-30` calls `UnfollowService` before `account.block!` (line 17) | S (cross-row: `blocks` vs `follows`) |
| M14 | Follow limit per local account | B | `app/validators/follow_limit_validator.rb:3-11`, `follow_limitable.rb:7` `unless: :bypass_follow_limit`; `follow_request.rb:35` passes `bypass_limit: true` on approval | N (legitimate approved follows exceed the limit; the bypass flag is not in the row, so a count rule would false-positive) |
| M15 | Counters never negative | C | `account_stats` columns are `default: 0, null: false` with no CHECK (`db/schema.rb:117-120`). App masks instead of preventing: reads clamp `[attributes[...], 0].max` (`app/models/account_stat.rb:27-37`) and writes clamp initial values (`counters.rb:66`) | R |

Not found for M15: any job that repairs negative counters (not searched exhaustively).

### 3.2 Discourse (13 invariants: A 6, B 6, C 1)

Observation: out of 1780 migrations, only 7 files add a CHECK and 9 create trigger/function objects. DB enforcement is sparse and mostly unique indexes.

| ID | Invariant | Cls | Evidence | CDC |
|---|---|---|---|---|
| D1 | A private message has no category | A | `db/migrate/20140911065449_private_messages_have_no_category_id.rb:6` `ADD CONSTRAINT pm_has_no_category CHECK (category_id IS NULL OR archetype <> 'private_message')` | n/a |
| D2 | A regular topic must have a category | A | `db/migrate/20131022045114_add_uncategorized_category.rb:31` `CHECK (category_id IS NOT NULL OR archetype <> 'regular')` | n/a |
| D3 | One `topic_users` row per (topic, user) | A | `app/models/topic_user.rb` Indexes block: `index_topic_users_on_topic_id_and_user_id (topic_id,user_id) UNIQUE` | n/a |
| D4 | Post numbers unique within a topic | A | `app/models/post.rb` Indexes block: `index_posts_on_topic_id_and_post_number (topic_id,post_number) UNIQUE` | n/a |
| D5 | A badge is granted once per (user, post) or (user, seq) | A | `app/models/user_badge.rb` Indexes block: two partial UNIQUE indexes; plus validator at `user_badge.rb:47` | n/a |
| D6 | A post revision is never both hidden and unlisted | A | `db/migrate/20261021180714_add_unlisted_to_post_revisions.rb:8-11` `add_check_constraint ... "NOT (hidden AND unlisted)"` | n/a |
| D7 | `user_stats.topic_count` and `post_count` non-negative | **C (was A)** | Added `db/migrate/20220118065658_add_constraints_to_user_stat.rb:17-18` (`CHECK (topic_count >= 0)`), removed by `db/migrate/20220215015538_drop_user_stat_count_constraints.rb:5-6`. No later re-add found by grepping all migrations for CHECK | R |
| D8 | Posts editable only within a time limit for non-trusted users | B | `app/models/concerns/limited_edit.rb:6-10` (`post_edit_time_limit`, `tl2_post_edit_time_limit`) | N (the time part is row-local via `updated_at`, but the rule exempts trusted users and staff, and the actor's trust level is not in the post row) |
| D9 | `posts.version = 1 + count(post_revisions)` and revision numbers are contiguous | B | repair job `PostRevision.ensure_consistency!` `app/models/post_revision.rb:16-33`; revision index `(post_id, number)` is not unique | S (aggregate) |
| D10 | `topics.posts_count`, `highest_post_number`, `word_count`, `last_posted_at` match the visible posts | B | repair job `Topic.reset_all_highest!` `app/models/topic.rb:1058-1100` | S (aggregate) |
| D11 | No identical post by the same user within `unique_posts_mins` | B | `lib/validators/post_validator.rb:75-82`; the dedup key lives in Redis (`app/models/post.rb:316-317`) | S (history of earlier posts; the enforcing state is outside the DB; staff exempt) |
| D12 | Post body length range depends on context (PM, first post, reply) | B | `post_validator.rb:41-57` | S (needs topic archetype/first-post lookup and site settings) |
| D13 | New users limited in replies per topic | B | `post_validator.rb:66-73` | S (count per user per topic, plus trust level) |

D7 is the sharpest single data point in the study: a DB constraint on a counter was added and then dropped within 28 days. I did not find the reason in the files I read; I do not claim one.

### 3.3 Saleor (14 invariants: A 4, B 7, C 3)

| ID | Invariant | Cls | Evidence | CDC |
|---|---|---|---|---|
| S1 | Gift card balance never negative | A | `saleor/giftcard/models.py:141-148` `CheckConstraint(condition=Q(current_balance_amount__gte=0), name="giftcard_current_balance_non_negative")`. Comment there: writers also clamp to zero | n/a |
| S2 | Transaction idempotency key unique per app | A | `saleor/payment/models.py:216-221` `UniqueConstraint(fields=["app_identifier","idempotency_key"])`; events at `:283-288` | n/a |
| S3 | One allocation per (order line, stock) | A | `saleor/warehouse/models.py:593` `unique_together` | n/a |
| S4 | `Allocation.quantity_allocated >= 0` | A (inferred) | `saleor/warehouse/models.py:588` `PositiveIntegerField`; Django emits `CHECK (col >= 0)` on PostgreSQL | n/a |
| S5 | Captured amount never exceeds payment total | B | request-time only: `saleor/payment/utils.py:530-536` (`clean_capture`). The webhook/gateway path adds `payment.captured_amount += transaction.amount` with no re-check (`utils.py:595-602` region, `update_payment_charge_status` starts at `:587`) | R (`captured_amount` and `total` are both in the `payment_payment` row) |
| S6 | Refund never exceeds captured amount | B | `saleor/payment/gateway.py:541-543` (`_validate_refund_amount`) | N (after an over-refund the code clamps `captured_amount` to 0 and sets `FULLY_REFUNDED`, `utils.py:612-615`, so the overshoot is erased from the row) |
| S7 | Allocation does not exceed available stock | B | `saleor/warehouse/management.py:183-198` raises `InsufficientStock`. `Stock.quantity`/`quantity_allocated` are plain `IntegerField`, no CHECK (`warehouse/models.py:532-533`) | S (whether oversell is legitimate depends on variant `track_inventory`, not in the stock row; I did not verify all legitimate over-allocation cases) |
| S8 | Voucher usage limited by `usage_limit` | B | check `saleor/discount/utils/voucher.py:256-262` (sum over codes); increment `:101` `F("used") + 1` | S (aggregate over `VoucherCode.used` vs `Voucher.usage_limit`) |
| S9 | Order status is consistent with fulfillments | B | `saleor/order/utils.py:159,192,210` (`determine_order_status` computes from line quantities) | S (aggregate over order lines/fulfillments) |
| S10 | Order line `quantity >= 1`, `quantity_fulfilled >= 0` | B (weak) | `saleor/order/models.py:598-601` `MinValueValidator`. Django does not run field validators on `save()`, so this only guards forms/`full_clean()` | R |
| S11 | `Stock.quantity_allocated` equals the sum of its allocations | B | maintained by `F()` updates `management.py:211-213` and `:403-405`; no constraint | S (aggregate) |
| S12 | Payment `charge_status` follows a legal transition graph | C | `update_payment_charge_status` sets `PENDING` for a PENDING transaction with no source-state guard (`utils.py:617-618`); `can_capture`/`can_refund` guard requests only (`payment/models.py:456-470`). Not found: any transition table | R (before/after `charge_status`). Needs the legal-transition table, which I did not find; false-positive rate unknown |
| S13 | `TransactionItem` charged/authorized/refunded amounts non-negative | C | `saleor/payment/models.py:48-60` bare `DecimalField`, no validators or CHECK. The GraphQL layer was not read | R |
| S14 | `quantity_fulfilled <= quantity` on order lines | C | `order/models.py:598-601` (no cross-field check); `quantity_unfulfilled` property computes the difference at `:773`, so a violation yields a negative number silently | R |

### 3.4 Odoo (14 invariants: A 5, B 8, C 1)

Odoo is the most DB-defended sample for single-row accounting rules and the least for aggregates.

| ID | Invariant | Cls | Evidence | CDC |
|---|---|---|---|---|
| O1 | A journal line has debit or credit, not both | A | `addons/account/models/account_move_line.py:548-551` `CHECK(... credit * debit=0)` | n/a |
| O2 | `amount_currency` and `balance` have the same sign | A | `account_move_line.py:552-555` `_check_amount_currency_balance_sign` | n/a |
| O3 | Payment amount non-negative | A | `addons/account/models/account_payment.py:235-238` `CHECK(amount >= 0.0)` | n/a |
| O4 | A confirmed sale order has a confirmation date | A | `addons/sale/models/sale_order.py:51-54` `CHECK((state = 'sale' AND date_order IS NOT NULL) OR state != 'sale')` | n/a |
| O5 | Posted entry names unique per journal | A | `addons/account/models/account_move.py:809-812` `UniqueIndex("(name, journal_id) WHERE (state = 'posted'AND name != '/')")` | n/a |
| O6 | A move's total debit equals total credit | B | `account_move.py:2907-2943` `_check_balanced` runs an aggregate query on create/write; skippable via `check_move_validity` context (`:2910-2913`). Not a deferred constraint trigger | S (aggregate over lines of one `move_id`; a processor must group events per transaction) |
| O7 | A hash-protected posted entry's integrity fields cannot change | B | `account_move.py:4008-4014` (`inalterable_hash` plus `_get_integrity_hash_fields()`) | R for header fields in `account_move` (old hash non-null and a hashed column changed). Line-level fields live in `account_move_line` (would need S). Transition rule |
| O8 | Cannot edit date/name or un-post a move inside a locked period | B | `account_move.py:4030-4038` and `_check_fiscal_lock_dates` `:2949-2966`; bypassable by context key `bypass_lock_check` (`:2950`) | S (lock dates are on `res_company`) |
| O9 | Sale order can be confirmed only from draft/sent | B | `sale_order.py:1662-1666` ("not in a state requiring confirmation") | R (transition rule `state` old -> new) |
| O10 | A serial number is on at most one unit | B | `addons/stock/models/stock_quant.py:630-646` `check_quantity` (aggregate by lot) | S |
| O11 | A confirmed payment with an outstanding account has a journal entry | B | `account_payment.py:945-952` `_check_move_id` | S (the `outstanding_account_id` condition is not shown to be stored in the payment row; without it the rule over-fires) |
| O12 | Receivable/payable account types match document type | B | `account_move_line.py:1882-1900` `_check_payable_receivable` | S (needs `account_account.account_type` and move type) |
| O13 | `quant.reserved_quantity` equals the sum of move-line reservations | B | repair job `_clean_reservations` `stock_quant.py:1185-1213` | S (aggregate) |
| O14 | `reserved_quantity <= quantity` on a quant | C | Writes clamp at zero only (`stock_quant.py:1131-1132`, `max(0, ...)`). Code explicitly tolerates `quantity - reserved_quantity < 0` (`:929-941`). Not found: any check or CHECK | R |

## 4. Cross-cutting classification

### 4.1 Totals

| Project | A | B-R | B-S | B-N | C-R | C-S | C-N | Total |
|---|---|---|---|---|---|---|---|---|
| Mastodon | 3 | 2 | 8 | 1 | 1 | 0 | 0 | 15 |
| Discourse | 6 | 0 | 5 | 1 | 1 | 0 | 0 | 13 |
| Saleor | 4 | 2 | 4 | 1 | 3 | 0 | 0 | 14 |
| Odoo | 5 | 2 | 6 | 0 | 1 | 0 | 0 | 14 |
| **Total** | **18** | **6** | **23** | **3** | **6** | **0** | **0** | **56** |
| Share | 32% | 11% | 41% | 5% | 11% | 0% | 0% | |

### 4.2 Recount by ID (checkable)

- A (18): M1 M2 M3, D1 D2 D3 D4 D5 D6, S1 S2 S3 S4, O1 O2 O3 O4 O5.
- B-R (6): M9, M11, S5, S10, O7, O9.
- B-S (23): M4 M5 M6 M7 M8 M10 M12 M13 (8), D9 D10 D11 D12 D13 (5), S7 S8 S9 S11 (4), O6 O8 O10 O11 O12 O13 (6).
- B-N (3): M14, D8, S6.
- C-R (6): M15, D7, S12, S13, S14, O14.

18 + 6 + 23 + 3 + 6 = 56. Per project: Mastodon 15, Discourse 13, Saleor 14, Odoo 14.

### 4.3 What the numbers mean

| Cut | Count | Share |
|---|---|---|
| DB already enforces (A) | 18 / 56 | 32% |
| Not enforced by DB (B + C) | 38 / 56 | 68% |
| ...of which decidable from one row (R) | 12 / 38 | 32% |
| ...of which needs state (S) | 23 / 38 | 61% |
| ...of which not decidable from WAL (N) | 3 / 38 | 8% |
| R that a CHECK could express (single-row predicate) | 9 / 12 | 75% |
| R that is a true before/after transition (CHECK cannot; trigger can) | 3 / 12 | 25% (5% of all 56) |

R items that a CHECK could express: M9, M11, M15, D7, S5, S10, S13, S14, O14. Transition rules: S12, O7, O9.

### 4.4 Where I was skeptical on purpose

- M8 and O11 look row-local at first glance; each needs a lookup (account locality, outstanding account), so I counted them S.
- D8 (edit time limit) has a row-local time comparison, but the exemption depends on who edits. WAL carries no actor identity beyond what a `last_editor_id`-style column holds, so N.
- S6 (over-refund): the application's own clamp destroys the evidence, so even a perfect CDC consumer sees a normal-looking `FULLY_REFUNDED` row.
- M14: a legitimate bypass path means a "following_count > limit" rule would alert on correct data.
- S12 and S7: catchable in principle, but I could not establish the legal transition table or all legitimate over-allocation cases from the code read. The false-positive rate is unknown, so "R/S" here means "evaluable", not "useful".
- I counted nothing as CDC-catchable that needs a Kubernetes-side join. S means "possible with a stateful stream processor", which is outside what the project's content policy does today.

## 5. Conclusions and recommendations

1. **For R9 and sub-question 2, use these numbers as the base rate for applicability.** About 21% of invariants in this sample (12 of 56) are checkable from a single row event, and only 5% are checkable there but not by a CHECK constraint. That is far below a broad "CDC beats database-side prevention" claim. It is consistent with the P0 conclusion that real incidents rarely line up with what row-level CDC can see.
2. **Narrow the positioning to what the data supports.** The defensible niche is: (a) single-row and transition rules (3 found) on tables whose owner will not add a trigger; (b) invariants teams deliberately do not enforce in the DB or removed (Discourse D7, Mastodon M15, Odoo O14, Saleor S13/S14), where detection without blocking writes is the only acceptable control; (c) legacy tables where adding a constraint needs a full-table validation (Discourse's own migration for D6 says it validates in a separate migration to avoid the scan lock).
3. **Do not claim the S class without building state.** 23 of 38 uncovered invariants need a join or an aggregate. Covering them means a stateful stream layer, which is a different product from a per-row content policy, and it competes directly with counter-based alerts and periodic reconciliation queries (the same shape as `ensure_consistency!`, `reset_all_highest!`, `_clean_reservations`). Decide explicitly whether that is in scope; I recommend recording it as out of scope until a user conversation (see `user-conversations.md`) asks for it.
4. **Follow-up experiments that would turn this from reading into measurement** (nothing implemented here):
   - Take the 12 R items and the Saleor/Odoo schemas and run them as the project's policy against a synthetic bad write, to measure detection latency and the false-positive rate for S12 and M9, the two I flagged as uncertain.
   - Of the 23 S items, classify which need bounded state (a per-key counter or a short window, such as M4, M10, D11) versus unbounded aggregates (O6 is bounded per transaction). Bounded-state ones are the cheapest scope extension.
   - Re-run the C hunt differently: profile real data (or fuzz in a test instance) for the 6 C items instead of reading code, since reading underestimates C.
5. **Treat D7 as a research lead, not a conclusion.** A constraint added and dropped in 28 days suggests prevention has a cost that detection avoids. Reading the Discourse commit history and discussion for the reason would test that directly.

## 6. Limitations

- Single reader, 56 hand-picked invariants from 4 projects; no inter-rater check. The R/S/N split in particular is judgment.
- Selection bias toward invariants that are written down (see section 1). C is a floor.
- No code was executed; no trigger or constraint was exercised. A-class row S4 is inferred from Django behavior, not from a migration.
- Saleor GraphQL layer and migrations, and Odoo write paths outside the cited lines, were not read, so some B items could really be A (a DB constraint added in a migration I did not open) or have a second application guard.
- Discourse index citations come from annotation comments, which can lag the real schema.
- All four projects are PostgreSQL. Nothing here speaks to MySQL.
