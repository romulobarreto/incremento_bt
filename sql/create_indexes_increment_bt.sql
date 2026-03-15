-- Ajuste o schema se necessário (default: public)
CREATE INDEX IF NOT EXISTS idx_increment_bt_instalacao   ON public.increment_bt ("INSTALACAO");
CREATE INDEX IF NOT EXISTS idx_increment_bt_irregularidade ON public.increment_bt ("IRREGULARIDADE");
CREATE INDEX IF NOT EXISTS idx_increment_bt_data_exec     ON public.increment_bt ("DATA_EXECUCAO");
CREATE INDEX IF NOT EXISTS idx_increment_bt_data_baixa    ON public.increment_bt ("DATA_BAIXA");
CREATE INDEX IF NOT EXISTS idx_increment_bt_classif       ON public.increment_bt ("CLASSIFICACAO_IRREG");
CREATE INDEX IF NOT EXISTS idx_increment_bt_matchok       ON public.increment_bt ("MATCH_OK");
