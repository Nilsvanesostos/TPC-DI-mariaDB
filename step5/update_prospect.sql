
  UPDATE Prospect P
  SET P.IsCustomer = 1
  WHERE EXISTS (
    SELECT *
    FROM DimCustomer C
    WHERE UPPER(P.LastName) = UPPER(C.LastName) AND
          TRIM(UPPER(P.AddressLine1)) = TRIM(UPPER(C.AddressLine1)) AND
          TRIM(UPPER(P.AddressLine2)) = TRIM(UPPER(C.AddressLine2)) AND
          TRIM(UPPER(P.PostalCode)) = TRIM(UPPER(C.PostalCode))
  );
INSERT INTO DImessages
    (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
    SELECT CURRENT_TIMESTAMP,1,'Prospect', 'Inserted rows', 'Status', COUNT(*) FROM Prospect;

