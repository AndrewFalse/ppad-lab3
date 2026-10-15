-- 01_check_dim_model.sql
-- Проверки слоя dds.dim_model

SELECT
    -- таблица не пустая
    (SELECT count(*) > 0 FROM dds.dim_model)                                    AS has_rows,

    -- у каждой модели ровно одна текущая версия
    (SELECT count(*) = 0
     FROM (
         SELECT hf_id
         FROM dds.dim_model
         WHERE is_current
         GROUP BY hf_id
         HAVING count(*) > 1
     ) t
    )                                                                           AS one_current_per_model,

    -- нет дыр в истории (valid_to + 1 day = следующий valid_from)
    (SELECT count(*) = 0
     FROM dds.dim_model d1
     JOIN dds.dim_model d2
       ON d1.hf_id = d2.hf_id
      AND d1.valid_to + INTERVAL '1 day' = d2.valid_from
     WHERE d1.is_current = false
       AND d1.valid_to IS NOT NULL
       AND d2.valid_from IS NULL          -- такого быть не должно, просто доп.защита
    )                                                                           AS no_null_valid_from,

    -- hf_id не пустой
    (SELECT count(*) = 0 FROM dds.dim_model WHERE hf_id IS NULL OR hf_id = '')  AS hf_id_not_empty;
