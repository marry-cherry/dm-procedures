Ссылка на видео демонстрации работы: https://disk.yandex.ru/i/BLrppzFLq3nqJA

sql/tables/create_tables.sql - скрипт создания таблиц схемы dm 

sql/procedures/fill_account_balance_f.sql - процедура витрины остатков

sql/procedures/fill_account_turnover_f.sql - процедура витрины оборотов

dags/fill_balance_january.py - DAG для запуска функции fill_account_balance_f на каждый день января 2018 года и 31.12.2017

dags/fill_turnover_january.py - DAG для запуска функции fill_account_turnover_f на каждый день января 2018 года
