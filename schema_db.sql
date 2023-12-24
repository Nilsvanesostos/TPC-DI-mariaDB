CREATE TABLE BatchDate (
    BatchNumber TINYINT AUTO_INCREMENT PRIMARY KEY,
    BatchDate DATE NOT NULL
);

CREATE TABLE DimBroker (
    SK_BrokerID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    BrokerID INT NOT NULL,
    ManagerID INT,
    FirstName CHAR(50) NOT NULL,
    LastName CHAR(50) NOT NULL,
    MiddleInitial CHAR(1),
    Branch CHAR(50),
    Office CHAR(50),
    Phone CHAR(14),
    IsCurrent BOOLEAN NOT NULL,
    BatchID SMALLINT NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL
);

CREATE TABLE DimCustomer (
    SK_CustomerID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    CustomerID INT NOT NULL,
    TaxID CHAR(20) NOT NULL,
    Status CHAR(10) NOT NULL,
    LastName CHAR(30) NOT NULL,
    FirstName CHAR(30) NOT NULL,
    MiddleInitial CHAR(1),
    Gender CHAR(1),
    Tier TINYINT,
    DOB DATE NOT NULL,
    AddressLine1 VARCHAR(80) NOT NULL,
    AddressLine2 VARCHAR(80),
    PostalCode CHAR(12) NOT NULL,
    City CHAR(25) NOT NULL,
    StateProv CHAR(20) NOT NULL,
    Country CHAR(24),
    Phone1 CHAR(30),
    Phone2 CHAR(30),
    Phone3 CHAR(30),
    Email1 CHAR(50),
    Email2 CHAR(50),
    NationalTaxRateDesc VARCHAR(50),
    NationalTaxRate DECIMAL(6,5),
    LocalTaxRateDesc VARCHAR(50),
    LocalTaxRate DECIMAL(6,5),
    AgencyID CHAR(30),
    CreditRating SMALLINT,
    NetWorth INT,
    MarketingNameplate VARCHAR(100),
    IsCurrent BOOLEAN NOT NULL,
    BatchID SMALLINT NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL
);

CREATE TABLE DimAccount (
    SK_AccountID INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
    AccountID INT NOT NULL,
    SK_BrokerID INT NOT NULL,
    SK_CustomerID INT NOT NULL,
    Status CHAR(10) NOT NULL,
    AccountDesc VARCHAR(50),
    TaxStatus TINYINT NOT NULL CHECK (TaxStatus = 0 OR TaxStatus = 1 OR TaxStatus = 2),
    IsCurrent BOOLEAN NOT NULL,
    BatchID INT NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL,
    FOREIGN KEY (SK_BrokerID) REFERENCES DimBroker(SK_BrokerID),
    FOREIGN KEY (SK_CustomerID) REFERENCES DimCustomer(SK_CustomerID)
);

CREATE TABLE DimCompany ( SK_CompanyID INT NOT NULL AUTO_INCREMENT
    PRIMARY KEY,
    CompanyID INT NOT NULL ,
    Status CHAR(10) NOT NULL ,
    Name CHAR(60) NOT NULL ,
    Industry CHAR(50) NOT NULL ,
    SPrating CHAR(4),
    isLowGrade BOOLEAN NOT NULL,
    CEO CHAR(100) NOT NULL ,
    AddressLine1 CHAR(80),
    AddressLine2 CHAR(80),
    PostalCode CHAR(12) NOT NULL ,
    City CHAR(25) NOT NULL ,
    StateProv CHAR(20) NOT NULL ,
    Country CHAR(24),
    Description CHAR(150) NOT NULL ,
    FoundingDate DATE ,
    IsCurrent BOOLEAN NOT NULL ,
    BatchID SMALLINT NOT NULL ,
    EffectiveDate DATE NOT NULL ,
    EndDate DATE NOT NULL
);

CREATE TABLE DimDate ( SK_DateID INT(11) NOT NULL PRIMARY KEY,
    DateValue DATE NOT NULL ,
    DateDesc CHAR(20) NOT NULL ,
    CalendarYearID SMALLINT NOT NULL ,
    CalendarYearDesc CHAR(20) NOT NULL ,
    CalendarQtrID DECIMAL(5) NOT NULL ,
    CalendarQtrDesc CHAR(20) NOT NULL ,
    CalendarMonthID INT NOT NULL ,
    CalendarMonthDesc CHAR(20) NOT NULL ,
    CalendarWeekDesc CHAR(20) NOT NULL ,
    CalendarWeekID INT NOT NULL ,
    DayOfWeekDesc CHAR(10) NOT NULL ,
    DayOfWeeknumeric TINYINT NOT NULL ,
    FiscalYearDesc CHAR(20) NOT NULL ,
    FiscalYearID SMALLINT NOT NULL ,
    FiscalQtrID DECIMAL(5) NOT NULL ,
    FiscalQtrDesc CHAR(20) NOT NULL ,
    HolidayFlag BOOLEAN NOT NULL
);

CREATE TABLE DimSecurity (
    SK_SecurityID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Symbol CHAR(15) NOT NULL,
    Issue CHAR(6) NOT NULL,
    Status CHAR(10) NOT NULL,
    Name CHAR(70) NOT NULL,
    ExchangeID CHAR(6) NOT NULL,
    SK_CompanyID INT NOT NULL,
    SharesOutstanding INT NOT NULL,
    FirstTrade DATE NOT NULL,
    FirstTradeOnExchange DATE NOT NULL,
    Dividend DECIMAL(10,2) NOT NULL,
    IsCurrent BOOLEAN NOT NULL,
    BatchID DECIMAL(5) NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL,
    FOREIGN KEY (SK_CompanyID) REFERENCES DimCompany(SK_CompanyID)
);


CREATE TABLE DimTime ( SK_TimeID INT(14) NOT NULL PRIMARY KEY,
    TimeValue TIME NOT NULL ,
    HourID TINYINT NOT NULL ,
    HourDesc CHAR(20) NOT NULL ,
    MinuteID TINYINT NOT NULL ,
    MinuteDesc CHAR(20) NOT NULL ,
    SecondID TINYINT NOT NULL ,
    SecondDesc CHAR(20) NOT NULL ,
    MarketHoursFlag BOOLEAN NOT NULL,
    OfficeHoursFlag BOOLEAN NOT NULL
);


CREATE TABLE DimTrade (
    TradeID INT NOT NULL,
    SK_BrokerID INT NOT NULL,
    SK_CreateDateID INT NOT NULL,
    SK_CreateTimeID INT NOT NULL,
    SK_CloseDateID INT NOT NULL,
    SK_CloseTimeID INT NOT NULL,
    Status CHAR(10) NOT NULL,
    Type CHAR(12) NOT NULL,
    CashFlag BOOLEAN NOT NULL,
    SK_SecurityID INT NOT NULL ,
    SK_CompanyID INT NOT NULL ,
    Quantity INT NOT NULL,
    BidPrice DECIMAL(8,2) NOT NULL,
    SK_CustomerID INT NOT NULL ,
    SK_AccountID INT NOT NULL ,
    ExecutedBy CHAR(64) NOT NULL,
    TradePrice DECIMAL(8,2),
    Fee DECIMAL(10,2),
    Commission DECIMAL(10,2),
    Tax DECIMAL(10,2),
    BatchID DECIMAL(5) NOT NULL,
    FOREIGN KEY (SK_BrokerID) REFERENCES DimBroker(SK_BrokerID),
    FOREIGN KEY (SK_CreateDateID) REFERENCES DimDate(SK_DateID),
    FOREIGN KEY (SK_CreateTimeID) REFERENCES DimTime(SK_TimeID),
    FOREIGN KEY (SK_CloseDateID) REFERENCES DimDate(SK_DateID),
    FOREIGN KEY (SK_CloseTimeID) REFERENCES DimTime(SK_TimeID),
    FOREIGN KEY (SK_SecurityID) REFERENCES DimSecurity(SK_SecurityID),
    FOREIGN KEY (SK_CompanyID) REFERENCES DimCompany(SK_CompanyID),
    FOREIGN KEY (SK_CustomerID) REFERENCES DimCustomer(SK_CustomerID),
    FOREIGN KEY (SK_AccountID) REFERENCES DimAccount(SK_AccountID)
);



CREATE TABLE DImessages ( MessageDateAndTime DATETIME NOT NULL ,
    BatchID DECIMAL(5) NOT NULL ,
    MessageSource CHAR(30),
    MessageText CHAR(50) NOT NULL ,
    MessageType CHAR(12) NOT NULL ,
    MessageData CHAR(100)
);

CREATE TABLE FactCashBalances (
    SK_CustomerID INT NOT NULL,
    SK_AccountID INT NOT NULL,
    SK_DateID INT NOT NULL,
    Cash DECIMAL(15,2) NOT NULL,
    BatchID DECIMAL(5) NOT NULL,
    FOREIGN KEY (SK_CustomerID) REFERENCES DimCustomer(SK_CustomerID),
    FOREIGN KEY (SK_AccountID) REFERENCES DimAccount(SK_AccountID),
    FOREIGN KEY (SK_DateID) REFERENCES DimDate(SK_DateID)
);

CREATE TABLE FactHoldings (
    TradeID INT NOT NULL,
    CurrentTradeID INT NOT NULL,
    SK_CustomerID INT NOT NULL,
    SK_AccountID INT NOT NULL,
    SK_SecurityID INT NOT NULL,
    SK_CompanyID INT NOT NULL,
    SK_DateID INT NOT NULL,
    SK_TimeID INT NOT NULL,
    CurrentPrice DECIMAL(8,2) CHECK (CurrentPrice > 0),
    CurrentHolding DECIMAL(6) NOT NULL,
    BatchID DECIMAL(5) NOT NULL,
    FOREIGN KEY (SK_CustomerID) REFERENCES DimCustomer(SK_CustomerID),
    FOREIGN KEY (SK_AccountID) REFERENCES DimAccount(SK_AccountID),
    FOREIGN KEY (SK_SecurityID) REFERENCES DimSecurity(SK_SecurityID),
    FOREIGN KEY (SK_CompanyID) REFERENCES DimCompany(SK_CompanyID),
    FOREIGN KEY (SK_DateID) REFERENCES DimDate(SK_DateID),
    FOREIGN KEY (SK_TimeID) REFERENCES DimTime(SK_TimeID)
);


CREATE TABLE FactMarketHistory (
    SK_SecurityID INT NOT NULL,
    SK_CompanyID INT NOT NULL,
    SK_DateID INT NOT NULL,
    PERatio DECIMAL(10,2),
    Yield DECIMAL(5,2) NOT NULL,
    FiftyTwoWeekHigh DECIMAL(8,2) NOT NULL,
    SK_FiftyTwoWeekHighDate INT NOT NULL,
    FiftyTwoWeekLow DECIMAL(8,2) NOT NULL,
    SK_FiftyTwoWeekLowDate INT NOT NULL,
    ClosePrice DECIMAL(8,2) NOT NULL,
    DayHigh DECIMAL(8,2) NOT NULL,
    DayLow DECIMAL(8,2) NOT NULL,
    Volume DECIMAL(12) NOT NULL,
    BatchID DECIMAL(5) NOT NULL,
    FOREIGN KEY (SK_SecurityID) REFERENCES DimSecurity(SK_SecurityID),
    FOREIGN KEY (SK_CompanyID) REFERENCES DimCompany(SK_CompanyID),
    FOREIGN KEY (SK_DateID) REFERENCES DimDate(SK_DateID)
);


CREATE TABLE FactWatches (
    SK_CustomerID INT NOT NULL,
    SK_SecurityID INT NOT NULL,
    SK_DateID_DatePlaced INT NOT NULL,
    SK_DateID_DateRemoved INT,
    BatchID DECIMAL(5) NOT NULL,
    FOREIGN KEY (SK_CustomerID) REFERENCES DimCustomer(SK_CustomerID),
    FOREIGN KEY (SK_SecurityID) REFERENCES DimSecurity(SK_SecurityID),
    FOREIGN KEY (SK_DateID_DatePlaced) REFERENCES DimDate(SK_DateID),
    FOREIGN KEY (SK_DateID_DateRemoved) REFERENCES DimDate(SK_DateID)
);


CREATE TABLE Industry ( IN_ID CHAR(4) Not NULL ,
    IN_NAME CHAR(50) Not NULL ,
    IN_SC_ID CHAR(4) Not NULL
);

CREATE TABLE Financial (
    SK_CompanyID INT NOT NULL,
    FI_YEAR DECIMAL(4) NOT NULL,
    FI_QTR DECIMAL(1) NOT NULL,
    FI_QTR_START_DATE DATE NOT NULL,
    FI_REVENUE DECIMAL(15,2) NOT NULL,
    FI_NET_EARN DECIMAL(15,2) NOT NULL,
    FI_BASIC_EPS DECIMAL(10,2) NOT NULL,
    FI_DILUT_EPS DECIMAL(10,2) NOT NULL,
    FI_MARGIN DECIMAL(10,2) NOT NULL,
    FI_INVENTORY DECIMAL(15,2) NOT NULL,
    FI_ASSETS DECIMAL(15,2) NOT NULL,
    FI_LIABILITY DECIMAL(15,2) NOT NULL,
    FI_OUT_BASIC DECIMAL(12) NOT NULL,
    FI_OUT_DILUT DECIMAL(12) NOT NULL,
    FOREIGN KEY (SK_CompanyID) REFERENCES DimCompany(SK_CompanyID)
);

CREATE TABLE Prospect (
    AgencyID CHAR(30) NOT NULL,
    SK_RecordDateID INT NULL,
    SK_UpdateDateID INT NOT NULL,
    BatchID DECIMAL(5) NOT NULL,
    IsCustomer BOOLEAN NOT NULL,
    LastName CHAR(30) NOT NULL,
    FirstName CHAR(30) NOT NULL,
    MiddleInitial CHAR(1),
    Gender CHAR(1),
    AddressLine1 CHAR(80),
    AddressLine2 CHAR(80),
    PostalCode CHAR(12),
    City CHAR(25) NOT NULL,
    State CHAR(20) NOT NULL,
    Country CHAR(24),
    Phone CHAR(30),
    Income DECIMAL(9),
    numericberCars DECIMAL(2),
    numericberChildren DECIMAL(2),
    MaritalStatus CHAR(1),
    Age DECIMAL(3),
    CreditRating DECIMAL(4),
    OwnOrRentFlag CHAR(1),
    Employer CHAR(30),
    numericberCreditCards DECIMAL(2),
    NetWorth DECIMAL(12),
    MarketingNameplate CHAR(100),
    FOREIGN KEY (SK_UpdateDateID) REFERENCES DimDate(SK_DateID)
);


CREATE TABLE StatusType ( ST_ID CHAR(4) Not NULL ,
    ST_NAME CHAR(10) Not NULL
);

CREATE TABLE TaxRate ( TX_ID CHAR(4) Not NULL ,
    TX_NAME CHAR(50) Not NULL ,
    TX_RATE DECIMAL(6,5) Not NULL
);

CREATE TABLE TradeType ( TT_ID CHAR(3) Not NULL ,
    TT_NAME CHAR(12) Not NULL ,
    TT_IS_SELL DECIMAL(1) Not NULL ,
    TT_IS_MRKT DECIMAL(1) Not NULL
);

CREATE TABLE Audit_ ( DataSet CHAR(20) Not Null ,
    BatchID DECIMAL(5),
    Date_ DATE ,
    Attribute CHAR(50) not null ,
    Value DECIMAL(15),
    DValue DECIMAL(15,5)
);

 -- staging tables

CREATE TABLE S_Company (
PTS CHAR(15) NOT NULL ,
REC_TYPE CHAR(3) NOT NULL ,
COMPANY_NAME CHAR(60),
CIK CHAR(10) NOT NULL ,
STATUS CHAR(4) NOT NULL ,
INDUSTRY_ID CHAR(2) NOT NULL ,
SP_RATING CHAR(4),
FOUNDING_DATE CHAR(8),
ADDR_LINE_1 CHAR(80),
ADDR_LINE_2 CHAR(80),
POSTAL_CODE CHAR(12),
CITY CHAR(25),
STATE_PROVINCE CHAR(20),
COUNTRY CHAR(24),
CEO_NAME CHAR(46),
DESCRIPTION CHAR(150)
);

CREATE TABLE S_Security (
PTS CHAR(15) NOT NULL ,
REC_TYPE CHAR(3) NOT NULL ,
SYMBOL CHAR(15) NOT NULL ,
ISSUE_TYPE CHAR(6) NOT NULL ,
STATUS CHAR(4) NOT NULL ,
NAME CHAR(70) NOT NULL ,
EX_ID CHAR(6) NOT NULL ,
SH_OUT CHAR(13) NOT NULL ,
FIRST_TRADE_DATE CHAR(8) NOT NULL ,
FIRST_TRADE_EXCHANGE CHAR(8) NOT NULL ,
DIVIDEN CHAR(12) NOT NULL ,
COMPANY_NAME_OR_CIK CHAR(60) NOT NULL);

CREATE TABLE S_Financial(
PTS CHAR(15),
REC_TYPE CHAR(3),
YEAR CHAR(4),
QUARTER CHAR(1),
QTR_START_DATE CHAR(8),
POSTING_DATE CHAR(8),
REVENUE CHAR(17),
EARNINGS CHAR(17),
EPS CHAR(12),
DILUTED_EPS CHAR(12),
MARGIN CHAR(12),
INVENTORY CHAR(17),
ASSETS CHAR(17),
LIABILITIES CHAR(17),
SH_OUT CHAR(13),
DILUTED_SH_OUT CHAR(13),
CO_NAME_OR_CIK CHAR(60));

CREATE TABLE S_Prospect(
AGENCY_ID CHAR(30) NOT NULL ,
LAST_NAME CHAR(30) NOT NULL ,
FIRST_NAME CHAR(30) NOT NULL ,
MIDDLE_INITIAL CHAR(1),
GENDER CHAR(1),
ADDRESS_LINE_1 CHAR(80),
ADDRESS_LINE_2 CHAR(80),
POSTAL_CODE CHAR(12),
CITY CHAR(25) NOT NULL ,
STATE CHAR(20) NOT NULL ,
COUNTRY CHAR(24),
PHONE CHAR(30),
INCOME DECIMAL(9),
NUMBER_CARS SMALLINT,
NUMBER_CHILDREM DECIMAL(2),
MARITAL_STATUS CHAR(1),
AGE DECIMAL(3),
CREDIT_RATING DECIMAL(4),
OWN_OR_RENT_FLAG CHAR(1),
EMPLOYER CHAR(30),
NUMBER_CREDIT_CARDS DECIMAL(2),
NET_WORTH DECIMAL(12));

CREATE TABLE S_Watches(
W_C_ID INT NOT NULL ,
W_S_SYMB CHAR(15) NOT NULL ,
W_DTS DATETIME NOT NULL ,
W_ACTION CHAR(4) NOT NULL);


CREATE TABLE S_Holdings(
    HH_H_T_ID INT NOT NULL,
    HH_T_ID INT NOT NULL,
    HH_BEFORE_QTY INT NOT NULL,
    HH_AFTER_QTY INT NOT NULL
);


CREATE TABLE S_Cash_Balances(
CT_CA_ID INT NOT NULL ,
CT_DTS DATETIME NOT NULL ,
CT_AMT CHAR(20) NOT NULL ,
CT_NAME CHAR(100) NOT NULL);

CREATE TABLE S_Broker(
EmployeeID INT NOT NULL ,
ManagerID INT NOT NULL ,
EmployeeFirstName CHAR(30) NOT NULL ,
EmployeeLastName CHAR(30) NOT NULL ,
EmployeeMI CHAR(1),
EmployeeJobCode SMALLINT,
EmployeeBranch CHAR(30),
EmployeeOffice CHAR(10),
EmployeePhone CHAR(14));

CREATE TABLE S_Account ( SK_AccountID INT,
AccountID INT,
BrokerID INT,
CustomerID INT,
Status CHAR(10) NOT NULL ,
AccountDesc VARCHAR(50),
TaxStatus TINYINT,
BatchID SMALLINT,
EffectiveDate DATE ,
EndDate DATE ,
ActionType CHAR(10));

CREATE TABLE S_Customer ( ActionType CHAR(10),
CustomerID INT,
TaxID CHAR(20),
Status CHAR(10),
LastName CHAR(30),
FirstName CHAR(30),
MiddleInitial CHAR(1),
Gender CHAR(1),
Tier TINYINT,
DOB DATE ,
AddressLine1 VARCHAR(80),
AddressLine2 VARCHAR(80),
PostalCode CHAR(12),
City CHAR(25),
StateProv CHAR(20),
Country CHAR(24),
Phone1 CHAR(30),
Phone2 CHAR(30),
Phone3 CHAR(30),
Email1 CHAR(50),
Email2 CHAR(50),
NationalTaxRateDesc VARCHAR(50),
NationalTaxRate DECIMAL(6,5),
LocalTaxRateDesc VARCHAR(50),
LocalTaxRate DECIMAL(6,5),
BatchID SMALLINT,
EffectiveDate DATE ,
EndDate DATE);

CREATE TABLE S_Trade (
cdc_flag CHAR(1),
cdc_dsn DECIMAL(12),
t_id DECIMAL(15),
t_dts DATETIME ,
t_st_id CHAR(4),
t_tt_id CHAR(3),
t_is_cash CHAR(3),
t_s_symb CHAR(15) NOT NULL ,
t_qty DECIMAL(6) NOT NULL ,
t_bid_price DECIMAL(8),
t_ca_id DECIMAL(11),
t_exec_name CHAR(49),
t_trade_price DECIMAL(8),
t_chrg DECIMAL(10),
t_comm DECIMAL(10),
t_tax DECIMAL(10));

CREATE TABLE S_Trade_History (
th_t_id DECIMAL(15),
th_dts DATE ,
th_st_id CHAR(4));

CREATE TABLE S_Daily_Market (
DM_DATE DATE NOT NULL,
DM_S_SYMB CHAR(15) NOT NULL,
DM_CLOSE DECIMAL(8) NOT NULL,
DM_HIGH DECIMAL(8) NOT NULL,
DM_LOW DECIMAL(8) NOT NULL,
DM_VOL BIGINT NOT NULL
);
