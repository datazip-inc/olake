-- Creates NEW objects only. Does not modify or delete any existing source object.
-- Run through sql/01_create_source.sh, which passes :base, :src, :smoke and :pub from bench.env.
\timing on
CREATE TABLE IF NOT EXISTS public.:"smoke" (LIKE :base INCLUDING DEFAULTS);
INSERT INTO public.:"smoke" SELECT * FROM :base WHERE c_custkey <= 2000
  AND NOT EXISTS (SELECT 1 FROM public.:"smoke");
ALTER TABLE public.:"smoke" ADD PRIMARY KEY (c_custkey);

CREATE TABLE IF NOT EXISTS public.:"src" (LIKE :base INCLUDING DEFAULTS);
INSERT INTO public.:"src" SELECT * FROM :base
  WHERE NOT EXISTS (SELECT 1 FROM public.:"src");
ALTER TABLE public.:"src" ADD PRIMARY KEY (c_custkey);
ANALYZE public.:"src";

CREATE PUBLICATION :"pub" FOR TABLE public.:"src", public.:"smoke";
SELECT count(*) FROM public.:"src";
