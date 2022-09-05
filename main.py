import sqlite3
import pandas as pd
import os

#Read script and create historical tables: client, card,account

def executeScriptFile(filename):
	if os.path.exists('database.db'):
		return
	conn = sqlite3.connect('database.db')
	cursor = conn.cursor()
	with open(filename, 'r') as sql_file:
		conn.executescript(sql_file.read())

def executeFile(filename):
	conn = sqlite3.connect('database.db')
	cursor = conn.cursor()
	with open(filename, 'r') as sql_file:
		conn.executescript(sql_file.read())
	conn.commit()


#Read input files passport_blacklist, terminals and transactions

def ImportBackupFiles():
	import os
	import shutil
	import datetime
	conn = sqlite3.connect('database.db')
	cursor = conn.cursor()
	date_list = []
	files_list = os.listdir('data')
	if files_list == []:
		print('Папка пуста!Загрузите файлы!')
		import sys
		sys.exit()
	for el in files_list:
		if el.endswith('.txt'):
			date_list.append(datetime.datetime.strptime(el[-12:-4], '%d%m%Y').date())
	date_list = sorted(date_list)
	dtm = (date_list[0].strftime('%d%m%Y'))
	passport_blacklist = pd.read_excel('data/passport_blacklist_'+dtm+'.xlsx',index_col = None)
	terminals = pd.read_excel('data/terminals_'+dtm+'.xlsx',index_col = None)
	transactions = pd.read_csv('data/transactions_'+dtm+'.txt',index_col = None, sep =';')

	passport_blacklist.to_sql('STG_passport_blacklist', conn, if_exists='replace', index = False)
	terminals.to_sql('STG_terminals', conn, if_exists='replace', index = False)
	transactions.to_sql('STG_transactions', conn, if_exists='replace', index = False)

	if not os.path.isdir('archive'):
		os.mkdir('archive')
	shutil.move('data/transactions_'+dtm+'.txt', 'archive/transactions_'+dtm+'.txt.backup')
	shutil.move('data/terminals_'+dtm+'.xlsx', 'archive/terminals_'+dtm+'.xlsx.backup')
	shutil.move('data/passport_blacklist_'+dtm+'.xlsx','archive/passport_blacklist_'+dtm+'.xlsx.backup')


#Create report table	

def createReport():
	conn = sqlite3.connect('database.db')
	cursor = conn.cursor()
	cursor.execute('''
		CREATE TABLE if not exists REP_FRAUD(
			event_dt datetime,
			passport varchar(128),
			fio varchar(128),
			phone varchar(128),
			event_type varchar(128),
			report_dt datetime default current_timestamp
			)
	''')
# Find and upload output passport fraud into report table

def fraudPassport():
	conn = sqlite3.connect('database.db')
	cursor = conn.cursor()
	cursor.execute('''
		INSERT INTO REP_FRAUD(
			event_dt,
			passport,
			fio,
			phone,
			event_type)

		SELECT
			min(t1.trans_date),
			t4.passport_num,
			t4.last_name || ' ' || t4.first_name || ' ' || t4.patronymic as fio,
			t4.phone,
			'passport_fraud' as event_type
		FROM DWH_FACT_transactions t1
		Left join DWH_DIM_card_HIST t2
		on t1.card_num = t2.card_num
		Left join DWH_DIM_account_HIST t3
		on t2.account_num = t3.account_num
		Left join DWH_DIM_client_HIST t4
		On t3.client = t4.client_id
		where date(t4.passport_valid_to, '+1 day') < t1.trans_date
		or t4.passport_num in(
		select
			passport_num
		from DWH_FACT_passport_blacklist)
		and oper_result = 'SUCCESS'
		and t1.trans_date > 
		(
select
date(max(trans_date))
from DWH_FACT_transactions
)
		Group by passport_num
	''')

	conn.commit()

# Find and upload output acccount fraud into report table

def fraudAccount():
	conn = sqlite3.connect('database.db')
	cursor = conn.cursor()
	cursor.execute('''
		INSERT INTO REP_FRAUD(
			event_dt,
			passport,
			fio,
			phone,
			event_type
			)
		SELECT
			min(t1.trans_date),
			t4.passport_num,
			t4.last_name || ' ' || t4.first_name,
			t4.phone,
			'account_fraud' as event_type
		FROM DWH_FACT_transactions t1
		Left join DWH_DIM_card_HIST t2
		on t1.card_num = t2.card_num
		Left join DWH_DIM_account_HIST t3
		on t2.account_num = t3.account_num
		Left join DWH_DIM_client_HIST t4
		On t3.client = t4.client_id
		where date(t3.valid_to, '+1 day') < t1.trans_date
		and t1.oper_result = 'SUCCESS'
		Group by passport_num
		''')

	conn.commit()

# Find and upload output transactional fraud into report table

def fraudtrans():
	conn = sqlite3.connect('database.db')
	cursor = conn.cursor()
	cursor.execute('''
		INSERT INTO REP_FRAUD(
			event_dt,
			passport,
			fio,
			phone,
			event_type)
		SELECT
			min(trans_date),
			passport_num,
			fio,
			phone,
			'transaction_fraud' as event_type
		FROM
			(
			SELECT
				t1.trans_date,
				terminal_city != lag(t5.terminal_city) over (partition by t2.account_num order by t1.trans_date) as dif_city,
				strftime('%s', t1.trans_date) - lag(strftime('%s', t1.trans_date)) over (partition by t2.account_num) as timedelta,
				t4.passport_num,
				t4.last_name || ' ' || t4.first_name || ' ' || t4.patronymic as fio,
				t4.phone
			FROM DWH_FACT_transactions t1
			Left join DWH_DIM_card_HIST t2
			on t1.card_num = t2.card_num
			Left join DWH_DIM_account_HIST t3
			on t2.account_num = t3.account_num
			Left join DWH_DIM_client_HIST t4
			On t3.client = t4.client_id
            Left join DWH_DIM_terminal_HIST t5
			On t5.terminal_id = t1.terminal
			)
		where dif_city = 1 and timedelta < 3600
		and trans_date > (
select
date(max(trans_date))
from DWH_FACT_transactions
)
		Group by passport_num;
		''')
	conn.commit()

# Find and upload output amount fraud into report table

def fraudamount():
	conn = sqlite3.connect('database.db')
	cursor = conn.cursor()
	cursor.execute('''
		INSERT INTO REP_FRAUD(
			event_dt,
			passport,
			fio,
			phone,
			event_type)
		SELECT
            trans_date as event_dt,
            passport_num as passport,
            fio,
            phone,
            'amount_fraud' as event_type
        FROM 
            (SELECT
                trans_date,
                passport_num,
                fio,
                phone,
                oper_type,

                lag(amt,3) over (partition by account_num order by trans_date) as oper_1,
                lag(amt,2) over (partition by account_num order by trans_date) as oper_2,
                lag(amt) over (partition by account_num order by trans_date) as oper_3,
                amt as oper_4,

                lag(oper_result,3) over (partition by account_num order by trans_date) as res_1,
                lag(oper_result,2) over (partition by account_num order by trans_date) as res_2,
                lag(oper_result) over (partition by account_num order by trans_date) as res_3,
                oper_result as res_4,

                lag(strftime('%s', trans_date),3) over (partition by account_num order by trans_date) as t1,
                strftime('%s', trans_date) as t2

            FROM (SELECT distinct
            t1.trans_id,
            t1.trans_date,
            t1.oper_result,
            t1.card_num,
            t1.amt,
            t1.oper_type,
            t3.client,
            t3.account_num,
            t4.passport_num,
            t4.last_name||' '||t4.first_name||' '||t4.patronymic as fio,
            t4.phone
        FROM DWH_FACT_transactions t1
        LEFT JOIN DWH_DIM_card_HIST t2
        ON t1.card_num = t2.card_num
        LEFT JOIN DWH_DIM_account_HIST t3
        ON t2.account_num = t3.account_num
        LEFT JOIN DWH_DIM_client_HIST t4
        ON t3.client = t4.client_id
        LEFT JOIN DWH_DIM_terminal_HIST t5 on t1.terminal = t5.terminal_id
        WHERE t1.trans_date between 
            (select max(date(trans_date)) FROM DWH_FACT_transactions) 
            and
            (SELECT date(max(date(trans_date)), '+1 day') FROM DWH_FACT_transactions)))

        WHERE oper_1 > oper_2 and oper_2 > oper_3 and oper_3 > oper_4
        and res_1 = 'REJECT' and res_2 = 'REJECT' and res_3 = 'REJECT' and res_4 = 'SUCCESS'
        and oper_type in ('WITHDRAW', 'PAYMENT')
        and t2-t1<1200
			
		''')
	conn.commit()

#Read script and create historical tables: client, card,account
executeScriptFile('sql_scripts/ddl_dml.sql')

#Create fact tables: transactions and passport_blacklist and historical one - terminals
executeFile('sql_scripts/crtbl.sql') 

#Read input files passport_blacklist, terminals and transactions
ImportBackupFiles()

#Create fact tables: transactions and passport_blacklist and historical one - terminals
executeFile('sql_scripts/crtdwhtbl.sql')

#Upload input files
executeFile('sql_scripts/insertb.sql')
createReport()
fraudPassport()
fraudAccount()
fraudtrans()
fraudamount()








