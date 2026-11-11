\set ON_ERROR_STOP on
begin; select txid_current() as tx \gset
\echo BADTX :tx
with u as (update public.sc_inventory set qty = 100 where server_id in (3,4,5) and id between 1001 and 1150 and qty > 100 returning id) insert into public.sc_bad_ledger select id, :tx from u;
commit;
select pg_sleep(0.3);
begin; select txid_current() as tx \gset
\echo BADTX :tx
with u as (update public.sc_inventory set qty = 100 where server_id in (3,4,5) and id between 1151 and 1300 and qty > 100 returning id) insert into public.sc_bad_ledger select id, :tx from u;
commit;
select pg_sleep(0.3);
begin; select txid_current() as tx \gset
\echo BADTX :tx
with u as (update public.sc_inventory set qty = 100 where server_id in (3,4,5) and id between 1301 and 1450 and qty > 100 returning id) insert into public.sc_bad_ledger select id, :tx from u;
commit;
select pg_sleep(0.3);
begin; select txid_current() as tx \gset
\echo BADTX :tx
with u as (update public.sc_inventory set qty = 100 where server_id in (3,4,5) and id between 1451 and 1600 and qty > 100 returning id) insert into public.sc_bad_ledger select id, :tx from u;
commit;
select pg_sleep(0.3);
begin; select txid_current() as tx \gset
\echo BADTX :tx
with u as (update public.sc_inventory set qty = 100 where server_id in (3,4,5) and id between 1601 and 1750 and qty > 100 returning id) insert into public.sc_bad_ledger select id, :tx from u;
commit;
select pg_sleep(0.3);
begin; select txid_current() as tx \gset
\echo BADTX :tx
with u as (update public.sc_inventory set qty = 100 where server_id in (3,4,5) and id between 1751 and 1900 and qty > 100 returning id) insert into public.sc_bad_ledger select id, :tx from u;
commit;
select pg_sleep(0.3);
begin; select txid_current() as tx \gset
\echo BADTX :tx
with u as (update public.sc_inventory set qty = 100 where server_id in (3,4,5) and id between 1901 and 2050 and qty > 100 returning id) insert into public.sc_bad_ledger select id, :tx from u;
commit;
select pg_sleep(0.3);
begin; select txid_current() as tx \gset
\echo BADTX :tx
with u as (update public.sc_inventory set qty = 100 where server_id in (3,4,5) and id between 2051 and 2200 and qty > 100 returning id) insert into public.sc_bad_ledger select id, :tx from u;
commit;
select pg_sleep(0.3);
begin; select txid_current() as tx \gset
\echo BADTX :tx
with u as (update public.sc_inventory set qty = 100 where server_id in (3,4,5) and id between 2201 and 2350 and qty > 100 returning id) insert into public.sc_bad_ledger select id, :tx from u;
commit;
select pg_sleep(0.3);
begin; select txid_current() as tx \gset
\echo BADTX :tx
with u as (update public.sc_inventory set qty = 100 where server_id in (3,4,5) and id between 2351 and 2500 and qty > 100 returning id) insert into public.sc_bad_ledger select id, :tx from u;
commit;
select pg_sleep(0.3);
