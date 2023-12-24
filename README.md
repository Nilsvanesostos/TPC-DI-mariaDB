# TPC-DS-mariadb (INFO-H-419)

Development of a TPC-DS benchmark for mariaDB.

You can find the project report at the root of the repository.

To reproduce the benchmark:

1. First download the tpcds-kit from https://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPC-DI&bm_vers=1.1.0&mode=CURRENT-ONLY and follow the instructions in TPC-DI_v1.1.0.pdf

2. Then download MariaDB

3. Create the DB and populate it using the scripts described bellow.

# Order of file running

There are multiples files to be executed in a certain order, 
since some tables depend on others. The following order must
thus be followed:

1. In step1 folder, we have to insert the data in the tables which only 
require from reading a txt file. We start executing the schema
of the database, schema_db.sql. Then, we create the tables DimDate, DimTime, 
Industry, StatusType, TaxRate and TradeType. In order to insert
all the data you just have to execute BatchDate.sql, dimTime.sql,
Industry.sql, StatusType.sql, TaxRate.sql and TradeType.sql, respectively.  
All this files are located in the folder "schemas".

2. After, move to the folder "step2" and proceed. You have load_data.py, load_company.py, 
load_financial and load_security.py, respectively. Then all
the data is inserted by running s_company.sql, s_financial.sql,
s_security.sql, load_company.sql, load_financial.sql, 
load_security.sql.

3. In step3 folder, you have to insert the info for DimBroker and the control
file from Prospect since you will use for DimCustomer and 
DimAccount. This is done by executing dimBroker.sql and Prospect.sql from the folder schemas. 
After this, we have to execute load_broker.py and load_broker.sql, and load_prospect.py and load_prospect.sql.

4. In step4 folder, load all the data for DimCustomer and DimAccount. This is done by 
executing load_customer_account.py.

5. In step5 folder, finish the second part of Prospect by executing update_prospect.py and update_prospect.sql.

6. In step6 folder, load the indormation of S_Trade, S_Cash_Balances, S_Watches and S_Holdings by executing dimTrade.sql, 
FactCashBalance.sql, FactWatches.sql and FactHoldings.sql Make transformations in S_Trade, S_Cash_Balances, S_Watches and S_Holdings
by executing the files load_trade.py, load_cash_balance.py, load_watches.py and
load_holdings.py, and then, load_trade.sql, load_cash_balances.sql load_watches.sql and
load_holdings.sql.

7. In step7 folder, finally, execute DailyMarket.sql, FactMarketHistory.py and FactMarketHistory.sql.

