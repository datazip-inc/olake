-- Creates NEW objects only. Does not modify or delete any existing source object.
-- Run through sql/01_create_source.sh, which passes :base, :src, :smoke and :pub from bench.env.
-- Safe to run again: each step is skipped when its object already exists.
\timing on
CREATE TABLE IF NOT EXISTS public.:"smoke" (LIKE :base INCLUDING DEFAULTS);
INSERT INTO public.:"smoke" SELECT * FROM :base WHERE c_custkey <= 2000
  AND NOT EXISTS (SELECT 1 FROM public.:"smoke");
SELECT NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = format('public.%I', :'smoke')::regclass AND contype = 'p') AS need_pk \gset
\if :need_pk
ALTER TABLE public.:"smoke" ADD PRIMARY KEY (c_custkey);
\endif

CREATE TABLE IF NOT EXISTS public.:"src" (LIKE :base INCLUDING DEFAULTS);
INSERT INTO public.:"src" SELECT * FROM :base
  WHERE NOT EXISTS (SELECT 1 FROM public.:"src");
SELECT NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = format('public.%I', :'src')::regclass AND contype = 'p') AS need_pk \gset
\if :need_pk
ALTER TABLE public.:"src" ADD PRIMARY KEY (c_custkey);
\endif
ANALYZE public.:"src";

SELECT NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = :'pub') AS need_pub \gset
\if :need_pub
CREATE PUBLICATION :"pub" FOR TABLE public.:"src", public.:"smoke";
\endif
SELECT NOT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = :'pub' AND schemaname = 'public' AND tablename = :'src') AS add_src \gset
\if :add_src
ALTER PUBLICATION :"pub" ADD TABLE public.:"src";
\endif
SELECT NOT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = :'pub' AND schemaname = 'public' AND tablename = :'smoke') AS add_smoke \gset
\if :add_smoke
ALTER PUBLICATION :"pub" ADD TABLE public.:"smoke";
\endif
SELECT count(*) FROM public.:"src";
