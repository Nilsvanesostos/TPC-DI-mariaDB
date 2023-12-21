LOAD DATA INFILE 'D:/Documentos/tpc-di-tool/Tools/Loading/staging/3/Batch1/WatchHistory.txt'
INTO TABLE S_Watches
FIELDS TERMINATED BY '|'
LINES STARTING BY '' 
TERMINATED BY '\r\n'
(
W_C_ID ,
W_S_SYMB ,
W_DTS ,
W_ACTION
);
