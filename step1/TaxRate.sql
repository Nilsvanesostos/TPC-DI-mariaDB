LOAD DATA
INFILE 'C:/ULB/MA2/tpc-di/staging/7/Batch1/TaxRate.txt'
INTO TABLE TaxRate
FIELDS TERMINATED BY '|'
LINES STARTING BY '' 
TERMINATED BY '\r\n'
(
TX_ID ,
TX_NAME ,
TX_RATE
);

