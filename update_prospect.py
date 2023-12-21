import os
import glob

batch_number = 1

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
query = f"""INSERT INTO DImessages
    (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
    SELECT CURRENT_TIMESTAMP,{batch_number},'Prospect', 'Inserted rows', 'Status', COUNT(*) FROM Prospect;\n
"""
with open("update_prospect.sql", "w") as f:
    f.write(upd_query)
    f.write(query)
print('Done.')
