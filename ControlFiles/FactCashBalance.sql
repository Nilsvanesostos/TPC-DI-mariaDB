LOAD DATA INFILE 'D:/Documentos/tpc-di-tool/Tools/staging/3/Batch1/CashTransaction.txt'
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

--THIS IS UPLOADED