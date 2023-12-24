import os
import glob

batch_number = 1


print('Loading prospect...')

# If the following query gives error just add a delimiter

marketing_nameplate_func = """
DELIMITER //

CREATE OR REPLACE FUNCTION get_marketing_template(
    net_worth DECIMAL,
    income DECIMAL,
    number_credit_cards DECIMAL,
    number_children DECIMAL,
    age DECIMAL,
    credit_rating DECIMAL,
    number_cars DECIMAL
)
RETURNS VARCHAR(100)
BEGIN
    DECLARE marketing_template VARCHAR(100);
    
    IF (net_worth > 1000000 OR income > 200000) THEN
        SET marketing_template = CONCAT(marketing_template, 'HighValue+');
    END IF;
    
    IF (number_credit_cards > 5 OR number_children > 3) THEN
        SET marketing_template = CONCAT(marketing_template, 'Expenses+');
    END IF;
    
    IF (age > 45) THEN
        SET marketing_template = CONCAT(marketing_template, 'Boomer+');
    END IF;
    
    IF (credit_rating < 600 OR income < 5000 OR net_worth < 100000) THEN
        SET marketing_template = CONCAT(marketing_template, 'MoneyAlert+');
    END IF;
    
    IF (number_cars > 3 OR number_credit_cards > 7) THEN
        SET marketing_template = CONCAT(marketing_template, 'Spender+');
    END IF;
    
    IF (age > 25 OR net_worth > 1000000) THEN
        SET marketing_template = CONCAT(marketing_template, 'Inherited+');
    END IF;
    
    RETURN SUBSTRING(marketing_template, 1, LENGTH(marketing_template) - 1);
END

//

DELIMITER ;\n
"""

load_prospect_query = """
INSERT INTO Prospect
SELECT SP.AGENCY_ID, (SELECT SK_DateID FROM DimDate WHERE DateValue IN (SELECT BatchDate FROM BatchDate WHERE BatchNumber = 1)) SK_RecordDateID,
      (SELECT SK_DateID FROM DimDate WHERE DateValue IN (SELECT BatchDate FROM BatchDate WHERE BatchNumber = 1)) SK_UpdateDateID, 1 BatchID, 
      FALSE IsCustomer, SP.LAST_NAME, SP.FIRST_NAME, SP.MIDDLE_INITIAL, SP.GENDER, SP.ADDRESS_LINE_1, SP.ADDRESS_LINE_2, SP.POSTAL_CODE, SP.CITY,
      SP.STATE, SP.COUNTRY, SP.PHONE, SP.INCOME, SP.NUMBER_CARS,SP.NUMBER_CHILDREM, SP.MARITAL_STATUS, SP.AGE,
      SP.CREDIT_RATING, SP.OWN_OR_RENT_FLAG, SP.EMPLOYER,SP.NUMBER_CREDIT_CARDS, SP.NET_WORTH, 
      get_marketing_template(SP.NET_WORTH, SP.INCOME, SP.NUMBER_CREDIT_CARDS, SP.NUMBER_CHILDREM, SP.AGE, SP.CREDIT_RATING, SP.NUMBER_CARS) MarketingNameplate
FROM S_Prospect SP;\n
"""  

with open("load_prospect.sql", "w") as f:
    f.write(marketing_nameplate_func)
    f.write(load_prospect_query)

print('Done.')