LOAD DATA
INFILE 'D:/Documentos/tpc-di-tool/Tools/Loading/staging/3/Batch1/Industry.txt'
INTO TABLE Industry
FIELDS TERMINATED BY '|'
LINES STARTING BY '' 
TERMINATED BY '\r\n'
(
    IN_ID ,
    IN_NAME ,
    IN_SC_ID
);
