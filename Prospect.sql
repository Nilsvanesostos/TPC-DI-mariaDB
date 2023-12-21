LOAD DATA INFILE 'D:/Documentos/tpc-di-tool/Tools/Loading/staging/3/Batch1/Prospect.csv'
INTO TABLE S_Prospect
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
(
AGENCY_ID ,
LAST_NAME ,
FIRST_NAME ,
MIDDLE_INITIAL ,
GENDER ,
ADDRESS_LINE_1 ,
ADDRESS_LINE_2 ,
POSTAL_CODE ,
CITY,
STATE ,
COUNTRY ,
PHONE ,
@var5 ,
@var1 ,
@var4 ,
MARITAL_STATUS ,
@var2,
@var3 ,
OWN_OR_RENT_FLAG ,
EMPLOYER ,
@var2 ,
@var6
)

SET NUMBER_CARS = NULLIF(@var1, ''),
AGE = NULLIF(@var2, ''),
CREDIT_RATING = NULLIF(@var3, ''),
NUMBER_CHILDREM = NULLIF(@var4, ''),
INCOME = NULLIF(@var5, ''),
NET_WORTH = NULLIF(@var6, '');

-- DELIMITER //

-- CREATE OR REPLACE FUNCTION get_marketing_template(
--     net_worth DECIMAL,
--     income DECIMAL,
--     number_credit_cards DECIMAL,
--     number_children DECIMAL,
--     age DECIMAL,
--     credit_rating DECIMAL,
--     number_cars DECIMAL
-- )
-- RETURNS VARCHAR(100)
-- BEGIN
--     DECLARE marketing_template VARCHAR(100);
    
--     IF (net_worth > 1000000 OR income > 200000) THEN
--         SET marketing_template = CONCAT(marketing_template, 'HighValue+');
--     END IF;
    
--     IF (number_credit_cards > 5 OR number_children > 3) THEN
--         SET marketing_template = CONCAT(marketing_template, 'Expenses+');
--     END IF;
    
--     IF (age > 45) THEN
--         SET marketing_template = CONCAT(marketing_template, 'Boomer+');
--     END IF;
    
--     IF (credit_rating < 600 OR income < 5000 OR net_worth < 100000) THEN
--         SET marketing_template = CONCAT(marketing_template, 'MoneyAlert+');
--     END IF;
    
--     IF (number_cars > 3 OR number_credit_cards > 7) THEN
--         SET marketing_template = CONCAT(marketing_template, 'Spender+');
--     END IF;
    
--     IF (age > 25 OR net_worth > 1000000) THEN
--         SET marketing_template = CONCAT(marketing_template, 'Inherited+');
--     END IF;
    
--     RETURN SUBSTRING(marketing_template, 1, LENGTH(marketing_template) - 1);
-- END

-- //

-- DELIMITER ;



-- INSERT INTO Prospect
-- SELECT SP.AGENCY_ID, (SELECT SK_DateID FROM DimDate WHERE DateValue IN (SELECT BatchDate FROM BatchDate WHERE BatchNumber = 1)) SK_RecordDateID,
--       (SELECT SK_DateID FROM DimDate WHERE DateValue IN (SELECT BatchDate FROM BatchDate WHERE BatchNumber = 1)) SK_UpdateDateID, 1 BatchID, 
--       FALSE IsCustomer, SP.LAST_NAME, SP.FIRST_NAME, SP.MIDDLE_INITIAL, SP.GENDER, SP.ADDRESS_LINE_1, SP.ADDRESS_LINE_2, SP.POSTAL_CODE, SP.CITY,
--       SP.STATE, SP.COUNTRY, SP.PHONE, SP.INCOME, SP.NUMBER_CARS,SP.NUMBER_CHILDREM, SP.MARITAL_STATUS, SP.AGE,
--       SP.CREDIT_RATING, SP.OWN_OR_RENT_FLAG, SP.EMPLOYER,SP.NUMBER_CREDIT_CARDS, SP.NET_WORTH, 
--       get_marketing_template(SP.NET_WORTH, SP.INCOME, SP.NUMBER_CREDIT_CARDS, SP.NUMBER_CHILDREM, SP.AGE, SP.CREDIT_RATING, SP.NUMBER_CARS) MarketingNameplate
-- FROM S_Prospect SP;

