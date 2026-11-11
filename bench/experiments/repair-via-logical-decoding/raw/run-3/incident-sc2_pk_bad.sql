set enable_seqscan = off;
set enable_bitmapscan = off;
begin;
select txid_current() as tx \gset
\echo TX :tx
explain (costs off) delete from public.sc2_pk_bad where state_group = any (array[10,60,110]);
\echo REMOVED_BEGIN
with d as (delete from public.sc2_pk_bad where state_group = any (array[10,60,110]) returning *)
select coalesce(json_agg(d order by id), '[]'::json) from d;
\echo REMOVED_END
commit;
