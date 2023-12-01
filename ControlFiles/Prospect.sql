LOAD DATA INFILE 'D:/Documentos/tpc-di-tool/Tools/staging/3/Batch1/Prospect.csv'
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


--THIS ONE WORKS

CREATE OR REPLACE FUNCTION get_marketing_template(net_worth NUMBER, income NUMBER,
number_credit_cards NUMBER, number_children NUMBER, age NUMBER,
credit_rating NUMBER, number_cars NUMBER)
RETURN VARCHAR AS
    marketing_template VARCHAR(100);
BEGIN
    IF (net_worth>1000000 OR income>200000) THEN
        marketing_template :=  CONCAT(marketing_template, 'HighValue+');
    END IF;
    IF (number_credit_cards>5 OR number_children>3) THEN
      marketing_template :=  CONCAT(marketing_template, 'Expenses+');
    END IF;
    IF (age>45) THEN
      marketing_template :=  CONCAT(marketing_template, 'Boomer+');
    END IF;
    IF (credit_rating<600 or income <5000 or net_worth < 100000) THEN
      marketing_template :=  CONCAT(marketing_template, 'MoneyAlert+');
    END IF;
    IF (number_cars>3 or number_credit_cards>7) THEN
      marketing_template :=  CONCAT(marketing_template, 'Spender+');
    END IF;
    IF (age>25 or net_worth>1000000) THEN
      marketing_template :=  CONCAT(marketing_template, 'Inherited+');
    END IF;
    RETURN SUBSTR(marketing_template, 1, LENGTH(marketing_template) - 1);
END;

INSERT INTO Prospect
SELECT SP.AGENCY_ID, (SELECT SK_DateID FROM DimDate WHERE DateValue IN (SELECT BatchDate FROM BatchDate WHERE BatchNumber = {0})) SK_RecordDateID,
      (SELECT SK_DateID FROM DimDate WHERE DateValue IN (SELECT BatchDate FROM BatchDate WHERE BatchNumber = {0})) SK_UpdateDateID, {0} BatchID, 
      'false' IsCustomer, SP.LAST_NAME, SP.FIRST_NAME, SP.MIDDLE_INITIAL, SP.GENDER, SP.ADDRESS_LINE_1, SP.ADDRESS_LINE_2, SP.POSTAL_CODE, SP.CITY,
      SP.STATE, SP.COUNTRY, SP.PHONE, SP.INCOME, SP.NUMBER_CARS,SP.NUMBER_CHILDREM, SP.MARITAL_STATUS, SP.AGE,
      SP.CREDIT_RATING, SP.OWN_OR_RENT_FLAG, SP.EMPLOYER,SP.NUMBER_CREDIT_CARDS, SP.NET_WORTH, 
      get_marketing_template(SP.NET_WORTH, SP.INCOME, SP.NUMBER_CREDIT_CARDS, SP.NUMBER_CHILDREM, SP.AGE, SP.CREDIT_RATING, SP.NUMBER_CARS) MarketingNameplate
FROM S_Prospect SP;

--I still have to run this queries

UPDATE Prospect P
SET P.IsCustomer  = 'true'
WHERE EXISTS (
  SELECT *
  FROM DimCustomer C
  WHERE UPPER(P.LastName) = UPPER(C.LastName) AND
        TRIM(UPPER(P.AddressLine1)) = TRIM(UPPER(C.AddressLine1)) AND
        TRIM(UPPER(P.AddressLine2)) = TRIM(UPPER(C.AddressLine2)) AND
        TRIM(UPPER(P.PostalCode)) = TRIM(UPPER(C.PostalCode)))

--This last query has to be runned after dimCustomer is created