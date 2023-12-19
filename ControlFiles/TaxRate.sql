LOAD DATA
INFILE 'D:/Documentos/tpc-di-tool/Tools/Loading/staging/3/Batch1/TaxRate.txt'
INTO TABLE TaxRate
FIELDS TERMINATED BY '|'
LINES STARTING BY '' 
TERMINATED BY '\r\n'
(
TX_ID ,
TX_NAME ,
TX_RATE
);

