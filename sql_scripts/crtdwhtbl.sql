
CREATE TABLE if not exists DWH_FACT_transactions(
			trans_id varchar(128),
			trans_date date,
			card_num varchar(128),
			oper_type varchar(128),
			amt decimal,
			oper_result varchar(128),
			terminal varchar(128)
			);

CREATE TABLE if not exists DWH_FACT_passport_blacklist(
			passport_num varchar(128),
			entry_dt date
			);

CREATE TABLE if not exists DWH_DIM_terminal_HIST(
			terminal_id varchar(128),
			terminal_type varchar(128),
			terminal_city varchar(128),
			terminal_address varchar(128),
			effective_from datetime default current_timestamp,
			effective_to datetime default(datetime('2999-12-31 23:59:59')),
			delete_flg integer default 0 
			);
CREATE TABLE if not exists STG_terminal_new as
			SELECT
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			FROM STG_terminals t1
			LEFT JOIN DWH_DIM_terminal_HIST t2
			On t1.terminal_id = t2.terminal_id
			WHERE t2.terminal_id is null;

CREATE TABLE if not exists STG_terminal_delete as
			SELECT
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			FROM STG_terminals t1
			LEFT JOIN DWH_DIM_terminal_HIST t2
			On t1.terminal_id = t2.terminal_id
			WHERE t1.terminal_id is null;
CREATE TABLE if not exists STG_terminal_change as
			SELECT
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			FROM STG_terminals t1
			LEFT JOIN DWH_DIM_terminal_HIST t2
			On t1.terminal_id = t2.terminal_id
			and (
			t1.terminal_type <> t2.terminal_type or
			t1.terminal_city <> t2.terminal_city or
			t1.terminal_address <> t2.terminal_address);
DROP TABLE STG_terminals;