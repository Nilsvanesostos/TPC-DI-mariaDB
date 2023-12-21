import os
import glob
import json
import xmltodict
import mariadb
import sys

batch_number = 1

#from utils import char_insert

f = open('load_customer_account.sql','w')


# database_name = input('What is the database name where you want to load the data?\n')
# user_name = input('What is the user name?\n')
database_name = 'tpcdi'
user_name = 'root'
# password_name = input('What is your password in postgres?\n') 
password_name = 'conejolibra899'
host_name = '127.0.0.1'
port_name = '3306'


# This is the load_staging_customer_account function, which uploads 
# the first data into S_Customer and S_Account.
print("Loading staging customer and account...")
customer_inserts = []
customer_inserts_values = []
account_inserts = []
account_inserts_values = []
max_packet = 150
with open('D:/Documentos/tpc-di-tool/Tools/loading/staging/3/Batch1/CustomerMgmt.xml') as fd:
  doc = xmltodict.parse(fd.read())
  actions = doc['TPCDI:Actions']['TPCDI:Action']
  for action in actions:
    action_type = action['@ActionType']
    # Customer fields
    try:
      c_id = action['Customer']['@C_ID']
    except:
      c_id = None
    try:
      c_tax_id = action['Customer']['@C_TAX_ID']
    except:
      c_tax_id = None
    try:
      c_l_name = action['Customer']['Name']['C_L_NAME']
    except:
      c_l_name = None
    try:
      c_f_name = action['Customer']['Name']['C_F_NAME']
    except:
      c_f_name = None
    try:
      c_m_name = action['Customer']['Name']['C_M_NAME']
    except:
      c_m_name = None
    try:
      c_tier = action['Customer']['@C_TIER']
      if c_tier == '':
        c_tier = 0
    except:
      c_tier = 0
    try:
      c_dob = action['Customer']['@C_DOB']
    except:
      c_dob = None
    try:
      c_prim_email = action['Customer']['ContactInfo']['C_PRIM_EMAIL']
    except:
      c_prim_email = None
    try:
      c_alt_email = action['Customer']['ContactInfo']['C_ALT_EMAIL']
    except:
      c_alt_email = None
    try:
      c_gndr = action['Customer']['@C_GNDR'].upper()
    except:
      c_gndr = 'U'
    if c_gndr != 'M' and c_gndr != 'F':
      c_gndr = 'U'
    try:
      c_adline1 = action['Customer']['Address']['C_ADLINE1']
    except:
      c_adline1 = None
    try:
      c_adline2 = action['Customer']['Address']['C_ADLINE2']
    except:
      c_adline2 = None
    try:
      c_zipcode = action['Customer']['Address']['C_ZIPCODE']
    except:
      c_zipcode = None
    try:
      c_city = action['Customer']['Address']['C_CITY']
    except:
      c_city = None
    try:
      c_state_prov = action['Customer']['Address']['C_STATE_PROV']
    except:
      c_state_prov = None
    try:
      c_ctry = action['Customer']['Address']['C_CTRY']
    except:
      c_ctry = None
    c_status = 'ACTIVE'
    phones = []
    try:
      phones.append(action['Customer']['ContactInfo']['C_PHONE_1'])
    except:
      phones.append(None)
    try:
      phones.append(action['Customer']['ContactInfo']['C_PHONE_2'])
    except:
      phones.append(None)
    try:
      phones.append(action['Customer']['ContactInfo']['C_PHONE_3'])
    except:
      phones.append(None)
    phone_numbers = []
    for phone in phones:
      try:
        c_ctry_code = phone['C_CTRY_CODE']
      except:
        c_ctry_code = None
      try:
        c_area_code = phone['C_AREA_CODE']
      except:
        c_area_code = None
      try:
        c_local = phone['C_LOCAL']
      except:
        c_local = None
      phone_number = None
      if c_ctry_code is not None and c_area_code is not None and c_local is not None:
        phone_number = '+' + c_ctry_code + '(' + c_area_code + ')' + c_local
      elif c_ctry_code is None and c_area_code is not None and c_local is not None:
        phone_number = '(' + c_area_code + ')' + c_local
      elif c_ctry_code is None and c_area_code is None and c_local is not None:
        phone_number = c_local
      if phone_number is not None and phone['C_EXT'] is not None:
        phone_number = phone_number + phone['C_EXT']
      phone_numbers.append(phone_number)
      try:
        c_nat_tx_id = action['Customer']['TaxInfo']['C_NAT_TX_ID']
      except:
        c_nat_tx_id = None
      try:
        c_lcl_tx_id = action['Customer']['TaxInfo']['C_LCL_TX_ID']  
      except:
        c_lcl_tx_id = None
      try:
        action_ts_date = action['@ActionTS'][0:10]
      except:
        action_ts_date = None
    if c_id is not None:
      insert_customer = """
      INSERT INTO S_Customer (ActionType, CustomerID, TaxID, Status, LastName, FirstName, MiddleInitial, Gender, Tier, DOB, AddressLine1, AddressLine2, PostalCode,
        City, StateProv, Country, Phone1, Phone2, Phone3, Email1, Email2, NationalTaxRateDesc, NationalTaxRate, LocalTaxRateDesc, LocalTaxRate, EffectiveDate, EndDate, BatchId)
      VALUES (?, ?, ?, ?, ?, ?, ?, 
        ?, ?, STR_TO_DATE(?, '%Y-%m-%d'), ?, ?, ?, 
        ?, ?, ?, ?, ?, 
        ?, ?, ?, 
        (SELECT TX_NAME FROM TaxRate WHERE TX_ID = ?), (SELECT TX_RATE FROM TaxRate WHERE TX_ID = ?),
        (SELECT TX_NAME FROM TaxRate WHERE TX_ID = ?), (SELECT TX_RATE FROM TaxRate WHERE TX_ID = ?),
        STR_TO_DATE(?, '%Y-%m-%d'), STR_TO_DATE('9999-12-31', '%Y-%m-%d'), ?)
      """
      insert_customer_values = (action_type,c_id,c_tax_id,c_status,c_l_name,c_f_name,c_m_name,c_gndr,c_tier,c_dob,c_adline1,c_adline2,c_zipcode,c_city,c_state_prov,c_ctry,phone_numbers[0],phone_numbers[1],phone_numbers[2],c_prim_email,c_alt_email,c_nat_tx_id,c_nat_tx_id,c_lcl_tx_id,c_lcl_tx_id,action_ts_date,batch_number)
      customer_inserts.append(insert_customer)
      customer_inserts_values.append(insert_customer_values)
    # Account fields
    try:
      a_id = action['Customer']['Account']['@CA_ID']
    except:
      a_id = None
    try:
      a_Desc = action['Customer']['Account']['CA_NAME']
    except:
      a_Desc = None
    try:
      a_taxStatus = action['Customer']['Account']['@CA_TAX_ST']
    except:
      a_taxStatus = None
    try:
      a_brokerID = action['Customer']['Account']['CA_B_ID']
    except:
      a_brokerID = None
    # action_type we have it already
    try:
      action_ts_date = action['@ActionTS'][0:10]
    except:
      action_ts_date = None
    if a_id is not None:
      insert_account = """
      INSERT INTO S_Account (ActionType, AccountID, Status, BrokerID, CustomerID, AccountDesc, TaxStatus, EffectiveDate, EndDate, BatchId)
      VALUES (?, ?, 'Active', ?, ?, ?, ?,
        STR_TO_DATE(?, '%Y-%m-%d'), STR_TO_DATE('9999-12-31', '%Y-%m-%d'), ?)
      """
      insert_account_values = (action_type,a_id,a_brokerID,c_id,a_Desc,a_taxStatus,action_ts_date,batch_number)
      account_inserts.append(insert_account)
      account_inserts_values.append(insert_account_values)
    if len(customer_inserts) + len(account_inserts) >= max_packet:
      # Connect to MariaDB Platform
      try:
          conn = mariadb.connect(
              user=user_name,
              password=password_name,
              host="127.0.0.1",
              port=3306,
              database=database_name

          )
      except mariadb.Error as e:
          print(f"Error connecting to MariaDB Platform: {e}")
          sys.exit(1)

      # Get Cursor
      cur = conn.cursor()

      # Execute queries
      for ins,val in zip(customer_inserts,customer_inserts_values):
        cur.execute(ins,val)
      conn.commit()
      customer_inserts = []
      customer_inserts_values = []
      for ins,val in zip(account_inserts,account_inserts_values):
        cur.execute(ins,val)
      conn.commit()
      account_inserts = []
      account_inserts_values = []
      conn.close()
      
      
print('Done.')

"""
Load NEW customers into DimCustomer table
"""
print('Loading new customers into DimCustomer table...')
# Afterwards, we insert all NEW customers into the DimCustomer table. This is the load_new_customer function
load_query_customer = """
  INSERT INTO DimCustomer(CustomerID, TaxID, Status, LastName, FirstName, MiddleInitial, Gender, Tier, DOB, AddressLine1, AddressLine2, PostalCode,
            City, StateProv, Country, Phone1, Phone2, Phone3, Email1, Email2, NationalTaxRateDesc, NationalTaxRate, LocalTaxRateDesc, LocalTaxRate, EffectiveDate, 
            EndDate, BatchId, AgencyID, CreditRating, NetWorth, MarketingNameplate, IsCurrent)
  WITH Copied AS (
      SELECT C.CustomerID, C.TaxID, C.Status, C.LastName, C.FirstName, C.MiddleInitial, C.Gender, C.Tier, C.DOB, C.AddressLine1, C.AddressLine2, C.PostalCode,
          C.City, C.StateProv, C.Country, C.Phone1, C.Phone2, C.Phone3, C.Email1, C.Email2, C.NationalTaxRateDesc, C.NationalTaxRate, C.LocalTaxRateDesc, C.LocalTaxRate, C.EffectiveDate, 
          C.EndDate, C.BatchId, P.AgencyID, P.CreditRating, P.NetWorth, P.MarketingNameplate
      FROM Prospect P, S_Customer C
      WHERE C.ActionType = 'NEW' AND
          P.FirstName = C.FirstName AND 
          UPPER(P.LastName) = UPPER(C.LastName) AND
          TRIM(UPPER(P.AddressLine1)) = TRIM(UPPER(C.AddressLine1)) AND
          TRIM(UPPER(P.AddressLine2)) = TRIM(UPPER(C.AddressLine2)) AND
          TRIM(UPPER(P.PostalCode)) = TRIM(UPPER(C.PostalCode)) AND
          NOT EXISTS (SELECT * 
              FROM S_Customer C1 
              WHERE C.CustomerID = C1.CustomerID AND
                  (C1.ActionType = 'UPDCUST' OR C1.ActionType = 'INACT') AND
                  C1.EffectiveDate > C.EffectiveDate
          )
  )
  SELECT C.CustomerID, C.TaxID, C.Status, C.LastName, C.FirstName, C.MiddleInitial, C.Gender, C.Tier, C.DOB, C.AddressLine1, C.AddressLine2, C.PostalCode,
          C.City, C.StateProv, C.Country, C.Phone1, C.Phone2, C.Phone3, C.Email1, C.Email2, C.NationalTaxRateDesc, C.NationalTaxRate, C.LocalTaxRateDesc, C.LocalTaxRate, C.EffectiveDate, 
          C.EndDate, C.BatchId, CP.AgencyID, CP.CreditRating, CP.NetWorth, CP.MarketingNameplate, 'true'
  FROM S_Customer C LEFT OUTER JOIN Copied CP ON (C.CustomerID = CP.CustomerID)
  WHERE C.ActionType = 'NEW'
"""

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=user_name,
        password=password_name,
        host="127.0.0.1",
        port=3306,
        database=database_name
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()
# Execute queries
cur.execute(load_query_customer)
conn.commit()
conn.close()


print('Done.')

 
"""
Load NEW accounts in S_Account into DimAccount table in the target database.
"""
print('Loading new accounts...')
# Then, we insert all NEW rows from S_Account into DimAccount. This is the load_new_account function
load_query_account = """
  INSERT INTO DimAccount (AccountID, SK_BrokerID, SK_CustomerID, Status, AccountDesc, TaxStatus, IsCurrent, BatchID, EffectiveDate, EndDate)
  WITH Copied AS (
    SELECT A.AccountID, B.SK_BrokerID, C.SK_CustomerID, A.Status, A.AccountDesc, A.TaxStatus, A.BatchID, A.EffectiveDate, A.EndDate
    FROM S_Account A
    JOIN DimBroker B ON A.BrokerID = B.BrokerID
    JOIN DimCustomer C ON A.CustomerID = C.CustomerID
    WHERE A.ActionType IN ('NEW', 'ADDACCT') AND
      NOT EXISTS (
        SELECT * FROM S_Account A1
        WHERE A.AccountID = A1.AccountID AND
          (A1.ActionType = 'UPDACC' OR A1.ActionType = 'INACT') AND
          A1.EffectiveDate > A.EffectiveDate
      )
  )
  SELECT A.AccountID, CP.SK_BrokerID, CP.SK_CustomerID, A.Status, A.AccountDesc, A.TaxStatus, 'true', A.BatchID, A.EffectiveDate, A.EndDate
  FROM S_Account A
  LEFT OUTER JOIN Copied CP ON (A.AccountID = CP.AccountID)
  WHERE A.ActionType IN ('NEW', 'ADDACCT')
"""

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=user_name,
        password=password_name,
        host="127.0.0.1",
        port=3306,
        database=database_name
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()
# Execute queries
cur.execute(load_query_account)
conn.commit()
conn.close()

print('Done.')

# This is the function create_trigger_UPD_customer_account

"""
    Create a trigger that will update the DimAccount table when a customer is updated.
"""

print('Creating trigger UPD_CUSTOMER_ACCOUNT...')
create_trigger_query = """
  CREATE OR REPLACE TRIGGER UPD_CUSTOMER_ACCOUNT
  AFTER UPDATE OF SK_CustomerID ON DimAccount
  FOR EACH ROW
  BEGIN
    UPDATE DimAccount
    SET SK_CustomerID = :new.SK_CustomerID
    WHERE AccountID = :old.AccountID;
  END;
"""

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=user_name,
        password=password_name,
        host="127.0.0.1",
        port=3306,
        database=database_name
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()
# Execute queries
cur.execute(create_trigger_query)
conn.commit()
conn.close()

print('Done.')

# Let us go with the load_update_customer function:

# Now we update all fields in the DimCustomer table with the latest values from S_Customer
base_update_query = """
  UPDATE DimCustomer C
  SET %s = (
    SELECT MAX(%s)
    FROM S_Customer C1
    WHERE C1.CustomerID = C.CustomerID AND
      C1.%s IS NOT NULL AND
      C1.ActionType = 'UPDCUST' AND
      NOT EXISTS (
        SELECT * FROM S_Customer C2 
        WHERE C2.CustomerID = C1.CustomerID 
        AND C2.ActionType = 'UPDCUST' AND C2.EffectiveDate > C1.EffectiveDate)
  )
  WHERE EXISTS (
    SELECT * FROM S_Customer C1
    WHERE C1.CustomerID = C.CustomerID AND
      C1.%s IS NOT NULL AND
      C1.ActionType = 'UPDCUST' AND
      NOT EXISTS (
        SELECT * FROM S_Customer C2
        WHERE C2.CustomerID = C1.CustomerID AND
          C2.ActionType = 'UPDCUST' AND C2.EffectiveDate > C1.EffectiveDate)
  )
"""
update_query_status = base_update_query % ('C.Status', 'C1.Status', 'Status', 'Status')
update_query_last_name = base_update_query % ('C.LastName', 'C1.LastName', 'LastName', 'LastName')
update_query_first_name = base_update_query % ('C.FirstName', 'C1.FirstName', 'FirstName', 'FirstName')
update_query_middle_initial = base_update_query % ('C.MiddleInitial', 'C1.MiddleInitial', 'MiddleInitial', 'MiddleInitial')
update_query_gender = base_update_query % ('C.Gender', 'C1.Gender', 'Gender', 'Gender')
update_query_tier = base_update_query % ('C.Tier', 'C1.Tier', 'Tier', 'Tier')
update_query_dob = base_update_query % ('C.DOB', 'C1.DOB', 'DOB', 'DOB')
update_query_address_line1 = base_update_query % ('C.AddressLine1', 'C1.AddressLine1', 'AddressLine1', 'AddressLine1')
update_query_address_line2 = base_update_query % ('C.AddressLine2', 'C1.AddressLine2', 'AddressLine2', 'AddressLine2')
update_query_postal_code = base_update_query % ('C.PostalCode', 'C1.PostalCode', 'PostalCode', 'PostalCode')
update_query_city = base_update_query % ('C.City', 'C1.City', 'City', 'City')
update_query_state_prov = base_update_query % ('C.StateProv', 'C1.StateProv', 'StateProv', 'StateProv')
update_query_country = base_update_query % ('C.Country', 'C1.Country', 'Country', 'Country')
print('Updating DimCustomer table...')
update_query_phone1 = base_update_query % ('C.Phone1', 'C1.Phone1', 'Phone1', 'Phone1')
update_query_phone2 = base_update_query % ('C.Phone2', 'C1.Phone2', 'Phone2', 'Phone2')
update_query_phone3 = base_update_query % ('C.Phone3', 'C1.Phone3', 'Phone3', 'Phone3')
update_query_email1 = base_update_query % ('C.Email1', 'C1.Email1', 'Email1', 'Email1')
update_query_email2 = base_update_query % ('C.Email2', 'C1.Email2', 'Email2', 'Email2')
update_query_national_tax_rate_desc = base_update_query % ('C.NationalTaxRateDesc', 'C1.NationalTaxRateDesc', 'NationalTaxRateDesc', 'NationalTaxRateDesc')
update_query_national_tax_rate = base_update_query % ('C.NationalTaxRate', 'C1.NationalTaxRate', 'NationalTaxRate', 'NationalTaxRate')
update_query_local_tax_rate_desc = base_update_query % ('C.LocalTaxRateDesc', 'C1.LocalTaxRateDesc', 'LocalTaxRateDesc', 'LocalTaxRateDesc')
update_query_local_tax_rate = base_update_query % ('C.LocalTaxRate', 'C1.LocalTaxRate', 'LocalTaxRate', 'LocalTaxRate')
# To finalize the update, we need to update the values from Prospect
base_update_prospect_query = """
UPDATE DimCustomer C
  SET C.AgencyID = (
    SELECT MAX(CP.AgencyID)
    FROM (SELECT P.AgencyID
          FROM Prospect P
          WHERE P.FirstName = C.FirstName AND 
              UPPER(P.LastName) = UPPER(C.LastName) AND
              TRIM(UPPER(P.AddressLine1)) = TRIM(UPPER(C.AddressLine1)) AND
              TRIM(UPPER(P.AddressLine2)) = TRIM(UPPER(C.AddressLine2)) AND
              TRIM(UPPER(P.PostalCode)) = TRIM(UPPER(C.PostalCode))
          ) CP),
      C.CreditRating = (
      SELECT MAX(CP.CreditRating)
      FROM (SELECT P.CreditRating
            FROM Prospect P
            WHERE P.FirstName = C.FirstName AND 
                UPPER(P.LastName) = UPPER(C.LastName) AND
                TRIM(UPPER(P.AddressLine1)) = TRIM(UPPER(C.AddressLine1)) AND
                TRIM(UPPER(P.AddressLine2)) = TRIM(UPPER(C.AddressLine2)) AND
                TRIM(UPPER(P.PostalCode)) = TRIM(UPPER(C.PostalCode))
            ) CP),
      C.NetWorth = (
      SELECT MAX(CP.NetWorth)
      FROM (SELECT P.NetWorth
            FROM Prospect P
            WHERE P.FirstName = C.FirstName AND 
                UPPER(P.LastName) = UPPER(C.LastName) AND
                TRIM(UPPER(P.AddressLine1)) = TRIM(UPPER(C.AddressLine1)) AND
                TRIM(UPPER(P.AddressLine2)) = TRIM(UPPER(C.AddressLine2)) AND
                TRIM(UPPER(P.PostalCode)) = TRIM(UPPER(C.PostalCode))
            ) CP),
      C.MarketingNameplate = (
      SELECT MAX(CP.MarketingNameplate)
      FROM (SELECT P.MarketingNameplate
            FROM Prospect P
            WHERE P.FirstName = C.FirstName AND
                UPPER(P.LastName) = UPPER(C.LastName) AND
                TRIM(UPPER(P.AddressLine1)) = TRIM(UPPER(C.AddressLine1)) AND
                TRIM(UPPER(P.AddressLine2)) = TRIM(UPPER(C.AddressLine2)) AND
                TRIM(UPPER(P.PostalCode)) = TRIM(UPPER(C.PostalCode))
            ) CP)
  WHERE EXISTS (
    SELECT * FROM S_Customer C1
    WHERE C1.CustomerID = C.CustomerID AND
      C1.ActionType = 'UPDCUST' AND
      NOT EXISTS (
        SELECT * FROM S_Customer C2
        WHERE C2.CustomerID = C1.CustomerID AND
          C2.ActionType = 'UPDCUST' AND C2.EffectiveDate > C1.EffectiveDate)
  )
"""

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=user_name,
        password=password_name,
        host="127.0.0.1",
        port=3306,
        database=database_name
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()
# Execute queries
print('...')
cur.execute(update_query_status)
cur.execute(update_query_last_name)
cur.execute(update_query_first_name)
print('...')
cur.execute(update_query_middle_initial)
cur.execute(update_query_gender)
cur.execute(update_query_tier)
print('...')
cur.execute(update_query_dob)
cur.execute(update_query_address_line1)
cur.execute(update_query_address_line2)
print('...')
cur.execute(update_query_postal_code)
cur.execute(update_query_city)
cur.execute(update_query_state_prov)
print('...')
cur.execute(update_query_country)
cur.execute(update_query_phone1)
cur.execute(update_query_phone2)
print('...')
cur.execute(update_query_phone3)
cur.execute(update_query_email1)
cur.execute(update_query_email2)
print('...')
cur.execute(update_query_national_tax_rate_desc)
cur.execute(update_query_national_tax_rate)
cur.execute(update_query_local_tax_rate_desc)
cur.execute(update_query_local_tax_rate)
print('...')
cur.execute(base_update_prospect_query)
conn.commit()
conn.close()

print('Done.')

# The next function is the load_update_account:


# Now we update the DimAccount table
print('Updating accounts...')
base_update_query = """
UPDATE DimAccount A
  SET A.%s = (
    SELECT MAX(A1.%s)
    FROM S_Account A1
    WHERE A1.AccountID = A.AccountID AND
      A1.%s IS NOT NULL AND
      A1.ActionType = 'UPDACCT' AND
      NOT EXISTS (
        SELECT * FROM S_Account A2
        WHERE A2.AccountID = A1.AccountID AND
          A2.ActionType = 'UPDACCT' AND A2.EffectiveDate > A1.EffectiveDate)
  )
  WHERE EXISTS (
    SELECT * FROM S_Account A1
    WHERE A1.AccountID = A.AccountID AND
      A1.%s IS NOT NULL AND
      A1.ActionType = 'UPDACCT' AND
      NOT EXISTS (
        SELECT * FROM S_Account A2
        WHERE A2.AccountID = A1.AccountID AND
          A2.ActionType = 'UPDACCT' AND A2.EffectiveDate > A1.EffectiveDate)
  )
"""
update_query_status = base_update_query % ('Status', 'Status', 'Status', 'Status')
update_query_account_desc = base_update_query % ('AccountDesc', 'AccountDesc', 'AccountDesc', 'AccountDesc')
update_query_account_taxstatus = base_update_query % ('TaxStatus', 'TaxStatus', 'TaxStatus', 'TaxStatus')


# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=user_name,
        password=password_name,
        host="127.0.0.1",
        port=3306,
        database=database_name
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)
    
cur = conn.cursor() 
cur.execute(update_query_status)
cur.execute(update_query_account_desc)
cur.execute(update_query_account_taxstatus)
conn.commit()
conn.close()

print('Done.')


# And the next one is load_close_account:

# Now, we update the Status field for all rows in the DimAccount table
# for which there is a row in S_Account with an ActionType of 'CLOSEACCT'
print('Closing accounts...')
update_query_status = """
  UPDATE DimAccount A
  SET A.Status = 'Inactive'
  WHERE EXISTS (
    SELECT * FROM S_Account A1
    WHERE A1.AccountID = A.AccountID AND
      A1.ActionType = 'CLOSEACCT' AND
      NOT EXISTS (
        SELECT * FROM S_Account A2
        WHERE A2.AccountID = A1.AccountID AND
          A2.ActionType = 'UPDACCT' AND A2.EffectiveDate > A1.EffectiveDate)
  )
"""

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=user_name,
        password=password_name,
        host="127.0.0.1",
        port=3306,
        database=database_name
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()
# Execute queries
cur.execute(update_query_status)
conn.commit()
conn.close()


# The next one is load_inact_customer

# Finally, we update the EndDate field and the isCurrent field for all rows in the DimCustomer table
# for which there is a row in S_Customer with an ActionType of 'INACT'
print('Updating inactive customers...')
update_query_end_date = """
  UPDATE DimCustomer C
  SET C.EndDate = (
    SELECT MAX(C1.EffectiveDate)
    FROM S_Customer C1
    WHERE C1.CustomerID = C.CustomerID AND
      C1.ActionType = 'INACT' AND
      NOT EXISTS (
        SELECT * FROM S_Customer C2
        WHERE C2.CustomerID = C1.CustomerID AND
          C2.ActionType = 'INACT' AND C2.EffectiveDate > C1.EffectiveDate)
  ),
  C.isCurrent = 'false'
  WHERE EXISTS (
    SELECT * FROM S_Customer C1
    WHERE C1.CustomerID = C.CustomerID AND
      C1.ActionType = 'INACT' AND
      NOT EXISTS (
        SELECT * FROM S_Customer C2
        WHERE C2.CustomerID = C1.CustomerID AND
          C2.ActionType = 'INACT' AND C2.EffectiveDate > C1.EffectiveDate)
  )
"""
# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=user_name,
        password=password_name,
        host="127.0.0.1",
        port=3306,
        database=database_name
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()
# Execute queries
cur.execute(update_query_end_date)
conn.commit()
conn.close()

print('Done.')
print('Adding invalid Customer records to DImessages...')
query = """INSERT INTO DImessages 
    (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
    SELECT CURRENT_TIMESTAMP, C.BatchID, 'DimCustomer', 'Invalid customer tier', 'Alert', 'C_ID = ' || C.CustomerID || ', C_TIER = ' || C.Tier
    FROM DimCustomer C
    WHERE C.Tier not in (1, 2, 3)"""

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=user_name,
        password=password_name,
        host="127.0.0.1",
        port=3306,
        database=database_name
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()
# Execute queries
cur.execute(query)
conn.commit()
conn.close()

### I DO NOT REALLY UNDERSTAND THIS PART

query = "select BatchDate, BatchDate-NUMTOYMINTERVAL(100, 'year') from BatchDate where BatchNumber = 1"
        
# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=user_name,
        password=password_name,
        host="127.0.0.1",
        port=3306,
        database=database_name
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)
  
# Get cursor
cur = conn.cursor() 
# Execute query
cur.execute(query)
for (batchdate,batchdateint) in cur:
  b_date, b_date_100 = batchdate, batchdateint
  break
b_date_100 = b_date_100.strftime('%Y-%m-%d')
b_date = b_date.strftime('%Y-%m-%d')
conn.commit()
conn.close()

query = """insert into DImessages (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
          SELECT CURRENT_TIMESTAMP, C.BatchID, 'DimCustomer', 'DOB is out of range', 'Alert', 'C_ID = ' || C.CustomerID || ', C_DOB = ' || C.DOB
          FROM DimCustomer C
          where ( (TO_DATE(\'%s\', 'yyyy-mm-dd') > DOB) or (DOB > (TO_DATE(\'%s\', 'yyyy-mm-dd'))) )"""%(b_date_100, b_date)

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=user_name,
        password=password_name,
        host="127.0.0.1",
        port=3306,
        database=database_name
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get cursor
cur = conn.cursor() 
cur.execute(query)
conn.commit()
conn.close()

print("Done.")

print('Done.')

# This is the load_inact_account function

# Finally, we update the EndDate field, the Status and the isCurrent field for all rows in the DimAccount table
# for which there is a row in S_Account with an ActionType of 'INACT'
print('Updating inactive accounts...')
update_query_end_date = """
  UPDATE DimAccount A
  SET A.EndDate = (
    SELECT MAX(A1.EffectiveDate)
    FROM S_Account A1
    WHERE A1.AccountID = A.AccountID AND
      A1.ActionType = 'INACT' AND
      NOT EXISTS (
        SELECT * FROM S_Account A2
        WHERE A2.AccountID = A1.AccountID AND
          A2.ActionType = 'INACT' AND A2.EffectiveDate > A1.EffectiveDate)
  ),
  A.Status = 'Inactive',
  A.isCurrent = 'false'
  WHERE EXISTS (
    SELECT * FROM S_Account A1
    WHERE A1.AccountID = A.AccountID AND
      A1.ActionType = 'INACT' AND
      NOT EXISTS (
        SELECT * FROM S_Account A2
        WHERE A2.AccountID = A1.AccountID AND
          A2.ActionType = 'INACT' AND A2.EffectiveDate > A1.EffectiveDate)
  )
"""
# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=user_name,
        password=password_name,
        host="127.0.0.1",
        port=3306,
        database=database_name
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)
  

cur = conn.cursor() 
cur.execute(update_query_end_date)
conn.commit()
conn.close()

print('Done.')


