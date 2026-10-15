-- 02_check_fct_snapshot.sql
-- Проверки фактовой таблицы

SELECT
    (SELECT count(*) > 0 FROM dds.fct_model_snapshot)                           AS has_rows,

    -- нет дублей по (business_date, hf_id)
    (SELECT count(*) = 0
     FROM (
         SELECT business_date, hf_id
         FROM dds.fct_model_snapshot
         GROUP BY business_date, hf_id
         HAVING count(*) > 1
     ) t
    )                                                                           AS no_duplicates,

    -- все hf_id из фактов существуют в dim_model
    (SELECT count(*) = 0
     FROM dds.fct_model_snapshot f
     WHERE NOT EXISTS (
         SELECT 1 FROM dds.dim_model d WHERE d.hf_id = f.hf_id
     )
    )                                                                           AS all_hf_id_in_dim;
