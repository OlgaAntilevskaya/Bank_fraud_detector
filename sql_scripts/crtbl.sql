

CREATE TABLE if not exists DWH_DIM_client_HIST(
			client_id varchar(128),
			last_name varchar(128),
			first_name varchar(128),
			patronymic varchar(128),
			date_of_birth date,
			passport_num varchar(128),
			passport_valid_to date,
			phone varchar(128),
			effective_from datetime ,
			effective_to datetime default(datetime('2999-12-31 23:59:59')),
			delete_flg integer default 0 
			);

CREATE TABLE if not exists DWH_DIM_account_HIST(
			account_num varchar(128),
			valid_to date,
			client varchar(128),
			effective_from datetime,
			effective_to datetime default(datetime('2999-12-31 23:59:59')),
			delete_flg integer default 0 
			);

CREATE TABLE if not exists DWH_DIM_card_HIST(
			card_num varchar(128),
			account_num varchar(128),
			effective_from datetime,
			effective_to datetime default(datetime('2999-12-31 23:59:59')),
			delete_flg integer default 0 
			);

INSERT INTO DWH_DIM_client_HIST(
		 	client_id,
			last_name,
			first_name,
			patronymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone,
			effective_from,
			effective_to)
		SELECT 
			client_id,
			last_name,
			first_name,
			patronymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone,
			create_dt,
			update_dt
		FROM clients;
		DROP TABLE clients;

INSERT INTO DWH_DIM_account_HIST(
			account_num,
			valid_to,
			client,
			effective_from,
			effective_to
			)
		SELECT
		 	account,
			valid_to,
			client,
			create_dt,
			update_dt
		FROM accounts;
		DROP TABLE accounts;

INSERT INTO DWH_DIM_card_HIST(
			card_num,
			account_num,
			effective_from,
			effective_to)
		SELECT
			card_num,
			account,
			create_dt,
			update_dt
		FROM cards;
		DROP TABLE cards;