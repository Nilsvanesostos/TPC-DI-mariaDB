LOAD DATA
INFILE 'D:/Documentos/tpc-di-tool/Tools/staging/3/Batch1/StatusType.txt'

INTO TABLE StatusType
FIELDS TERMINATED BY '|'
LINES STARTING BY '' 
TERMINATED BY '\r\n'
(
ST_ID ,
ST_NAME
);


--THIS ONE WORKS