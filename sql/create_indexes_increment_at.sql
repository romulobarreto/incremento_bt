-- Índices para incremento AT (Gestão Centralizada)

CREATE INDEX IF NOT EXISTS idx_increment_at_instalacao
ON public.increment_at ("INSTALACAO");

CREATE INDEX IF NOT EXISTS idx_increment_at_irregularidade
ON public.increment_at ("IRREGULARIDADE");

CREATE INDEX IF NOT EXISTS idx_increment_at_data_exec
ON public.increment_at ("DATA_EXECUCAO");

CREATE INDEX IF NOT EXISTS idx_increment_at_data_baixa
ON public.increment_at ("DATA_BAIXA");

CREATE INDEX IF NOT EXISTS idx_increment_at_classif
ON public.increment_at ("CLASSIFICACAO_IRREG");

CREATE INDEX IF NOT EXISTS idx_increment_at_matchok
ON public.increment_at ("MATCH_OK");

CREATE INDEX IF NOT EXISTS idx_increment_at_motivo_pendencia
ON public.increment_at ("MOTIVO_PENDENCIA");