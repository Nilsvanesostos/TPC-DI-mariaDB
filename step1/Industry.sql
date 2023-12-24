LOAD DATA
INFILE 'C:/ULB/MA2/tpc-di/staging/7/Batch1/Industry.txt'
INTO TABLE Industry
FIELDS TERMINATED BY '|'
LINES STARTING BY '' 
TERMINATED BY '\r\n'
(
    IN_ID ,
    IN_NAME ,
    IN_SC_ID
);
