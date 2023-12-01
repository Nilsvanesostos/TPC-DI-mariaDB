LOAD DATA INFILE 'D:/Documentos/tpc-di-tool/Tools/staging/3/Batch1/Trade.txt'
INTO TABLE S_Trade
FIELDS TERMINATED BY '|'
LINES STARTING BY '' 
TERMINATED BY '\r\n'
(
t_id,       
@var5 ,
t_st_id ,
t_tt_id ,
t_is_cash ,
t_s_symb ,
t_qty ,
t_bid_price ,
t_ca_id ,
t_exec_name ,
@var1 ,
@var2 ,
@var3 ,
@var4
)

SET t_trade_price = NULLIF(@var1,''),
t_chrg = NULLIF(@var2,''),
t_comm = NULLIF(@var3,''),
t_tax = NULLIF(@var4,''),
t_dts = NULLIF(@var5,'');

--THIS ONE IS UPLOADED