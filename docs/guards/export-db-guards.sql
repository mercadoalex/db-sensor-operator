-- export-db-guards.sql
--
-- Lists the rules a PostgreSQL database already enforces by itself: triggers (with the mode in
-- which they fire), constraints, enum values and the replica identity of each table. It is the
-- raw material for writing the invariants you want Lactifluus to watch; see README.md here.
--
-- READ-ONLY. It queries the system catalogs only. It returns no row data. It does return schema,
-- table, trigger and constraint names and their definitions, so read the output before you share it.
--
-- Usage:
--   psql -d YOUR_DB -v schema=public -f export-db-guards.sql
--   psql -d YOUR_DB -v schema=public -A -F $'\t' -f export-db-guards.sql > guards.tsv
--
-- Status: run on 2026-11-08 against PostgreSQL 16.15 (kind, bench/experiments/trigger-bypass): no errors;
-- sections 1, 2 and 4 returned rows for a table with a trigger, a check constraint and FULL replica identity,
-- section 3 was empty because that database has no enum. The optional section 5 was not run. Other versions
-- were not tried; if a query fails on yours, please report it.

\echo '== 1. Triggers (mode: origin = default; always = also fires under session_replication_role=replica; replica only; disabled)'
select c.relname                      as table_name,
       t.tgname                       as trigger_name,
       case t.tgenabled
            when 'O' then 'origin (default)'
            when 'A' then 'always'
            when 'R' then 'replica only'
            when 'D' then 'DISABLED'
            else t.tgenabled::text
       end                            as mode,
       pg_get_triggerdef(t.oid)       as definition
from pg_trigger t
join pg_class c      on c.oid = t.tgrelid
join pg_namespace n  on n.oid = c.relnamespace
where not t.tgisinternal
  and n.nspname = :'schema'
order by c.relname, t.tgname;

\echo '== 2. Constraints (check, foreign key, unique, exclusion)'
select conrelid::regclass             as table_name,
       conname                        as constraint_name,
       case contype
            when 'c' then 'check'
            when 'f' then 'foreign key'
            when 'u' then 'unique'
            when 'x' then 'exclusion'
       end                            as kind,
       pg_get_constraintdef(oid)      as definition
from pg_constraint
where connamespace = (select oid from pg_namespace where nspname = :'schema')
  and contype in ('c', 'f', 'u', 'x')
order by conrelid::regclass::text, conname;

\echo '== 3. Enum types and their values'
select t.typname                      as enum_type,
       string_agg(e.enumlabel, ', ' order by e.enumsortorder) as enum_values
from pg_type t
join pg_enum e       on e.enumtypid = t.oid
join pg_namespace n  on n.oid = t.typnamespace
where n.nspname = :'schema'
group by t.typname
order by t.typname;

\echo '== 4. Replica identity per table (rules that compare the old and new value need FULL)'
select c.relname                      as table_name,
       case c.relreplident
            when 'd' then 'default (primary key)'
            when 'n' then 'nothing'
            when 'f' then 'FULL'
            when 'i' then 'index'
       end                            as replica_identity
from pg_class c
join pg_namespace n  on n.oid = c.relnamespace
where c.relkind in ('r', 'p')
  and n.nspname = :'schema'
order by c.relname;

-- OPTIONAL, off by default: the source of the functions that triggers call. It can be long and may
-- contain business logic you do not want to share. Remove the leading "--" on each line to run it.
-- \echo '== 5. Source of the functions called by triggers'
-- select distinct p.oid::regprocedure as function, pg_get_functiondef(p.oid) as source
-- from pg_trigger t
-- join pg_class c      on c.oid = t.tgrelid
-- join pg_namespace n  on n.oid = c.relnamespace
-- join pg_proc p       on p.oid = t.tgfoid
-- where not t.tgisinternal and n.nspname = :'schema';
