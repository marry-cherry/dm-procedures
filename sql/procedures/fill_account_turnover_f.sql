--ПРОЦЕДУРА ДЛЯ ВИТРИНЫ ОБОРОТОВ
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    f_start_time TIMESTAMP := now();
BEGIN
	--удаление данных
    DELETE FROM dm.dm_account_turnover_f
    WHERE on_date = i_OnDate;

    -- Вставка данных
    INSERT INTO dm.dm_account_turnover_f (
    on_date,
    account_rk,
    credit_amount,
    credit_amount_rub,
    debet_amount,
    debet_amount_rub
)
SELECT
    i_OnDate,
    COALESCE(c.account_rk, d.account_rk) AS account_rk,
    COALESCE(c.credit_amount, 0),
    COALESCE(c.credit_amount_rub, 0),
    COALESCE(d.debet_amount, 0),
    COALESCE(d.debet_amount_rub, 0)
FROM
    -- Кредитовые обороты
    (
        SELECT
            f.credit_account_rk AS account_rk,
            SUM(f.credit_amount) AS credit_amount,
            SUM(f.credit_amount * COALESCE(r.reduced_cource, 1)) AS credit_amount_rub
        FROM ds.ft_posting_f f
        JOIN ds.md_account_d acc ON acc.account_rk = f.credit_account_rk
        LEFT JOIN ds.md_exchange_rate_d r ON r.currency_rk = acc.currency_rk AND r.data_actual_date = i_OnDate
        WHERE f.oper_date = i_OnDate
          AND i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date
        GROUP BY f.credit_account_rk
    ) c
FULL OUTER JOIN
    -- Дебетовые обороты
    (
        SELECT
            f.debet_account_rk AS account_rk,
            SUM(f.debet_amount) AS debet_amount,
            SUM(f.debet_amount * COALESCE(r.reduced_cource, 1)) AS debet_amount_rub
        FROM ds.ft_posting_f f
        JOIN ds.md_account_d acc ON acc.account_rk = f.debet_account_rk
        LEFT JOIN ds.md_exchange_rate_d r ON r.currency_rk = acc.currency_rk AND r.data_actual_date = i_OnDate
        WHERE f.oper_date = i_OnDate
          AND i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date
        GROUP BY f.debet_account_rk
    ) d
ON c.account_rk = d.account_rk;

    -- Лог завершения
    INSERT INTO logs.etl_log (process_name, start_time, end_time, commentt)
    VALUES ('fill_account_turnover_f', f_start_time, now(), 'Completed for ' || i_OnDate);
END;
$$;
