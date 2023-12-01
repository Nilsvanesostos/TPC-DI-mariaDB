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

1. First, we have to insert the data in the tables which only 
require from reading a txt file. These are DimDate, DimTime, 
Industry, StatusType, TaxRate and TradeType. In order to insert
all the data you just have to execute its respective sql file.

2. After, you have load_data.py, load_company.py, 
load_financial and load_security.py, respectively. Then all
the data is inserted by running s_company.sql, s_financial.sql,
s_security.sql, load_company.sql, load_financial.sql, 
load_security.sql.

