### This program updates the prospect table and must be 
### executed after executing load_customer_account.py

upd_query = """
  UPDATE Prospect P
  SET P.IsCustomer  = 'true'
  WHERE EXISTS (
    SELECT *
    FROM DimCustomer C
    WHERE UPPER(P.LastName) = UPPER(C.LastName) AND
          TRIM(UPPER(P.AddressLine1)) = TRIM(UPPER(C.AddressLine1)) AND
          TRIM(UPPER(P.AddressLine2)) = TRIM(UPPER(C.AddressLine2)) AND
          TRIM(UPPER(P.PostalCode)) = TRIM(UPPER(C.PostalCode));\n
  )
""" 
print('Filling DImessages for Prospect')
query = """INSERT INTO DImessages
    (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
    SELECT CURRENT_TIMESTAMP,1,'Prospect', 'Inserted rows', 'Status', COUNT(*) FROM Prospect;\n
"""
with open("update_prospect.sql") as f:
    f.write(upd_query)
    f.write(query)
print('Done.')
