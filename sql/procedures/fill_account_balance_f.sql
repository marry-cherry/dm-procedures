--ПРОЦЕДУРА ДЛЯ ВИТРИНЫ ОСТАТКОВ
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    f_start_time TIMESTAMP := now();
    prev_date DATE := i_OnDate - INTERVAL '1 day';
BEGIN
    DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;

    INSERT INTO dm.dm_account_balance_f (
        on_date,
        account_rk,
        balance_out,
        balance_out_rub
    )
    SELECT
        i_OnDate,
        acc.account_rk,

        CASE acc.char_type
            WHEN 'А' THEN COALESCE(b_prev.balance_out, 0)
                         + COALESCE(t.debet_amount, 0)
                         - COALESCE(t.credit_amount, 0)
            WHEN 'П' THEN COALESCE(b_prev.balance_out, 0)
                         - COALESCE(t.debet_amount, 0)
                         + COALESCE(t.credit_amount, 0)
        END AS balance_out,

        CASE acc.char_type
            WHEN 'А' THEN COALESCE(b_prev.balance_out_rub, 0)
                         + COALESCE(t.debet_amount_rub, 0)
                         - COALESCE(t.credit_amount_rub, 0)
            WHEN 'П' THEN COALESCE(b_prev.balance_out_rub, 0)
                         - COALESCE(t.debet_amount_rub, 0)
                         + COALESCE(t.credit_amount_rub, 0)
        END AS balance_out_rub

    FROM ds.md_account_d acc
    LEFT JOIN dm.dm_account_balance_f b_prev
        ON b_prev.account_rk = acc.account_rk AND b_prev.on_date = prev_date
    LEFT JOIN dm.dm_account_turnover_f t
        ON t.account_rk = acc.account_rk AND t.on_date = i_OnDate
    WHERE i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date;

    -- Лог завершения
    INSERT INTO logs.etl_log (process_name, start_time, end_time, commentt)
    VALUES ('fill_account_balance_f', f_start_time, now(), 'Completed for ' || i_OnDate);
END;
$$;

