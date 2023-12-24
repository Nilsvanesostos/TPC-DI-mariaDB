LOAD DATA INFILE 'C:/ULB/MA2/tpc-di/staging/7/Batch1/CashTransaction.txt'
INTO TABLE S_Cash_Balances
FIELDS TERMINATED BY '|'
LINES STARTING BY '' 
TERMINATED BY '\r\n'
(
CT_CA_ID ,
CT_DTS ,
CT_AMT ,
CT_NAME
);

