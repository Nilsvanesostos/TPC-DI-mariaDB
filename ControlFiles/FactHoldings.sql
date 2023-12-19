LOAD DATA INFILE 'D:/Documentos/tpc-di-tool/Tools/Loading/staging/3/Batch1/HoldingHistory.txt'
INTO TABLE S_Holdings
FIELDS TERMINATED BY '|'
LINES STARTING BY '' 
TERMINATED BY '\r\n'
(
HH_H_T_ID ,
HH_T_ID ,
HH_BEFORE_QTY ,
HH_AFTER_QTY
);

