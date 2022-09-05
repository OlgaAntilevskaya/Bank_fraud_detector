INSERT INTO DWH_FACT_transactions(
			trans_id,
			trans_date,
			card_num,
			oper_type,
			amt,
			oper_result,
			terminal
			)
			select
			transaction_id,
			transaction_date,
			card_num,
			oper_type,
			amount,
			oper_result,
			terminal			
			from STG_transactions;
		DROP TABLE STG_transactions;

INSERT INTO DWH_FACT_passport_blacklist(
			passport_num,
			entry_dt)
			select
			passport,
			date
			from STG_passport_blacklist;
		DROP TABLE STG_passport_blacklist;

INSERT INTO DWH_DIM_terminal_HIST(
	 		terminal_id,
			terminal_type,
	 		terminal_city,
			terminal_address)
	 		Select
		 		terminal_id,
		 		terminal_type,
		 		terminal_city,
				terminal_address
	 		from STG_terminal_new;
	 	DROP TABLE STG_terminal_new;

UPDATE DWH_DIM_terminal_HIST
		set effective_to = datetime('now', '-1 second')
		where terminal_id in (select terminal_id from STG_terminal_change)
		and effective_to = datetime('2999-12-31 23:59:59');
		INSERT INTO DWH_DIM_terminal_HIST(
	 		terminal_id,
			terminal_type,
	 		terminal_city,
			terminal_address)
	 		Select
		 		terminal_id,
		 		terminal_type,
		 		terminal_city,
				terminal_address
	 		from STG_terminal_change;
	 	DROP TABLE STG_terminal_change;
	 	
UPDATE DWH_DIM_terminal_HIST
		set effective_to = datetime('now', '-1 second')
		where terminal_id in (select terminal_id from STG_terminal_delete)
		and effective_to = datetime('2999-12-31 23:59:59');
		INSERT INTO DWH_DIM_terminal_HIST(
	 		terminal_id,
			terminal_type,
	 		terminal_city,
			terminal_address,
			delete_flg)
	 		Select
		 		terminal_id,
		 		terminal_type,
		 		terminal_city,
				terminal_address,
				1
	 		from STG_terminal_delete;
	 	DROP TABLE STG_terminal_delete;