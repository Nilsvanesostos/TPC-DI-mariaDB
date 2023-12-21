LOAD DATA
INFILE 'D:/Documentos/tpc-di-tool/Tools/Loading/staging/3/Batch1/TradeType.txt'
INTO TABLE TradeType
FIELDS TERMINATED BY '|'
LINES STARTING BY '' 
TERMINATED BY '\r\n'
(
TT_ID ,
TT_NAME ,
TT_IS_SELL ,
TT_IS_MRKT
);


