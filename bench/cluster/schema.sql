-- Benchmark schema: the monitored tables, identical for every Approach.
-- Applied by `make -C bench setup`. Idempotent.

CREATE TABLE IF NOT EXISTS public.orders (
    id         bigserial PRIMARY KEY,
    customer   integer        NOT NULL,
    amount     numeric(12, 2) NOT NULL,
    updated_at timestamptz    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.users (
    id         bigserial PRIMARY KEY,
    name       text        NOT NULL,
    score      integer     NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.payments (
    id         bigserial PRIMARY KEY,
    order_id   integer        NOT NULL,
    amount     numeric(12, 2) NOT NULL,
    updated_at timestamptz    NOT NULL DEFAULT now()
);

-- Deterministic initial rows so every run starts from the same state.
-- Only seeds a table when it is empty, keeping setup idempotent.
INSERT INTO public.orders (customer, amount)
SELECT (g % 100) + 1, (g % 500) + 1
FROM generate_series(1, 1000) AS g
WHERE NOT EXISTS (SELECT 1 FROM public.orders);

INSERT INTO public.users (name, score)
SELECT 'user-' || g, g % 1000
FROM generate_series(1, 1000) AS g
WHERE NOT EXISTS (SELECT 1 FROM public.users);

INSERT INTO public.payments (order_id, amount)
SELECT (g % 1000) + 1, (g % 500) + 1
FROM generate_series(1, 1000) AS g
WHERE NOT EXISTS (SELECT 1 FROM public.payments);
