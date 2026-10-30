-- Workload_Generator: deterministic insert/update/delete sequence.
--
-- Determinism: the procedure calls setseed(p_seed) first, so the sequence of
-- table choices, operation choices, and row targets is a pure function of
-- (seed, mix, rate, duration) for a given starting data state. Every Approach
-- therefore sees the same mutation sequence (Requirements 6.2, 8.1).
--
-- Each operation is committed on its own (COMMIT inside the loop) so that CDC
-- sees one transaction per mutation and the triggers/stats see realistic
-- commit boundaries. Call it OUTSIDE an explicit transaction block.
--
-- Pacing: operation i is scheduled at start + i/rate seconds. If the database
-- cannot keep up, operations run late instead of being dropped, and the
-- procedure reports the achieved rate so the report can disclose it.
--
--   CALL bench.run_workload(
--     p_seed       => 42,    -- fixed, documented seed
--     p_insert_pct => 50,    -- mix, must sum to 100
--     p_update_pct => 40,
--     p_delete_pct => 10,
--     p_rate       => 50,    -- operations per second
--     p_duration_s => 60);

CREATE SCHEMA IF NOT EXISTS bench;

CREATE OR REPLACE PROCEDURE bench.run_workload(
    p_seed       double precision,
    p_insert_pct integer,
    p_update_pct integer,
    p_delete_pct integer,
    p_rate       integer,
    p_duration_s integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    tables     constant text[] := ARRAY['orders', 'users', 'payments'];
    total_ops  integer;
    i          integer;
    r          double precision;
    tbl        text;
    max_id     bigint;
    target_id  bigint;
    started    timestamptz;
    due        timestamptz;
    n_ins      integer := 0;
    n_upd      integer := 0;
    n_del      integer := 0;
    n_noop     integer := 0;
    elapsed_s  double precision;
    affected   integer;
BEGIN
    IF p_insert_pct < 0 OR p_update_pct < 0 OR p_delete_pct < 0
       OR p_insert_pct + p_update_pct + p_delete_pct <> 100 THEN
        RAISE EXCEPTION 'mix must be non-negative and sum to 100 (got %/%/%)',
            p_insert_pct, p_update_pct, p_delete_pct;
    END IF;
    IF p_seed < -1 OR p_seed > 1 THEN
        -- setseed() only accepts [-1, 1]; map an integer seed into that range.
        PERFORM setseed(((p_seed::bigint % 2000001) - 1000000) / 1000000.0);
    ELSE
        PERFORM setseed(p_seed);
    END IF;
    IF p_rate < 1 OR p_duration_s < 1 THEN
        RAISE EXCEPTION 'rate and duration must be >= 1';
    END IF;

    total_ops := p_rate * p_duration_s;
    started   := clock_timestamp();

    FOR i IN 0 .. total_ops - 1 LOOP
        tbl := tables[1 + floor(random() * array_length(tables, 1))::int];
        r   := random() * 100;

        IF r < p_insert_pct THEN
            -- INSERT
            IF tbl = 'orders' THEN
                INSERT INTO public.orders (customer, amount)
                VALUES (1 + floor(random() * 100)::int, 1 + floor(random() * 500)::int);
            ELSIF tbl = 'users' THEN
                INSERT INTO public.users (name, score)
                VALUES ('user-' || i, floor(random() * 1000)::int);
            ELSE
                INSERT INTO public.payments (order_id, amount)
                VALUES (1 + floor(random() * 1000)::int, 1 + floor(random() * 500)::int);
            END IF;
            n_ins := n_ins + 1;

        ELSE
            -- UPDATE or DELETE: pick the first existing id at or after a random point.
            EXECUTE format('SELECT coalesce(max(id), 0) FROM public.%I', tbl) INTO max_id;
            target_id := NULL;
            IF max_id > 0 THEN
                EXECUTE format(
                    'SELECT id FROM public.%I WHERE id >= $1 ORDER BY id LIMIT 1', tbl)
                INTO target_id
                USING 1 + floor(random() * max_id)::bigint;
            END IF;

            IF target_id IS NULL THEN
                n_noop := n_noop + 1;  -- nothing to touch; keeps the sequence deterministic
            ELSIF r < p_insert_pct + p_update_pct THEN
                EXECUTE format(
                    'UPDATE public.%I SET updated_at = now() WHERE id = $1', tbl)
                USING target_id;
                GET DIAGNOSTICS affected = ROW_COUNT;
                n_upd := n_upd + affected;
            ELSE
                EXECUTE format('DELETE FROM public.%I WHERE id = $1', tbl)
                USING target_id;
                GET DIAGNOSTICS affected = ROW_COUNT;
                n_del := n_del + affected;
            END IF;
        END IF;

        COMMIT;

        -- Pace: sleep until this operation's scheduled slot (never negative).
        due := started + make_interval(secs => (i + 1)::double precision / p_rate);
        IF clock_timestamp() < due THEN
            PERFORM pg_sleep(extract(epoch FROM (due - clock_timestamp())));
        END IF;
    END LOOP;

    elapsed_s := extract(epoch FROM (clock_timestamp() - started));
    RAISE NOTICE 'workload done: seed=% mix=%/%/% target_rate=%/s duration=%s ops=% inserts=% updates=% deletes=% noops=% achieved_rate=%/s elapsed=%s',
        p_seed, p_insert_pct, p_update_pct, p_delete_pct, p_rate, p_duration_s,
        total_ops, n_ins, n_upd, n_del, n_noop,
        round((total_ops / greatest(elapsed_s, 0.001))::numeric, 2),
        round(elapsed_s::numeric, 2);
END;
$$;
