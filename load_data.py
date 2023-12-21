import os
import glob

"""
Create S_Company and S_Security table in the staging database and then load rows in FINWIRE files with the type of CMP
"""
ctxt=open('s_company.sql', 'w')
stxt=open('s_security.sql', 'w')
ftxt=open('s_financial.sql', 'w')

print('Loading staging company, security and financial...')
base_path = "staging/3/Batch1/"
s_company_base_query = "INSERT INTO S_Company"
s_security_base_query = "INSERT INTO S_Security"
s_financial_base_query = "INSERT INTO S_Financial"
s_company_values = []
s_security_values = []
s_financial_values = []
max_packet = 150
for fname in os.listdir(base_path):
  if("FINWIRE" in fname and "audit" not in fname):
    with open(base_path+fname, 'r') as finwire_file:
      for line in finwire_file:
        pts = line[:15] #0
        rec_type=line[15:18] #1
        if rec_type=="CMP":
          company_name = line[18:78] #2
          # check if company name is made of blanks
          if company_name.strip() == "":
            company_name = "NULL"
          cik = line[78:88] #3
          status = line[88:92] #4
          industry_id = line[92:94] #5
          sp_rating = line[94:98] # 6
          # check if sp rating is made of blanks
          if sp_rating.strip() == "":
            sp_rating = "NULL"
          founding_date = line[98:106] #7
          # check if founding date is made of blanks
          if founding_date.strip() == "":
            founding_date = "NULL"
          addr_line_1 = line[106:186] #8
          # check if address line 1 is made of blanks
          if addr_line_1.strip() == "":
            addr_line_1 = "NULL"
          addr_line_2 = line[186:266] #9
          # check if address line 2 is made of blanks
          if addr_line_2.strip() == "":
            addr_line_2 = "NULL"
          postal_code = line[266:278] #10
          # check if postal code is made of blanks
          if postal_code.strip() == "":
            postal_code = "NULL"
          city = line[278:303] #10
          # check if city is made of blanks
          if city.strip() == "":
            city = "NULL"
          state_province = line[303:323] #11
          # check if state province is made of blanks
          if state_province.strip() == "":
            state_province = "NULL"
          country = line[323:347] #12
          # check if country is made of blanks
          if country.strip() == "":
            country = "NULL"
          ceo_name = line[347:393] #13
          # check if ceo name is made of blanks
          if ceo_name.strip() == "":
            ceo_name = "NULL"
          description = line[393:][:-1] #14
          # check if description is made of blanks
          if description.strip() == "":
            description = "NULL"
          query = "%s (PTS,REC_TYPE,COMPANY_NAME,CIK,STATUS,INDUSTRY_ID,SP_RATING,FOUNDING_DATE,ADDR_LINE_1," % s_company_base_query
          query += "ADDR_LINE_2,POSTAL_CODE,CITY,STATE_PROVINCE,COUNTRY,CEO_NAME,DESCRIPTION) "
          query += "VALUES ('%s','%s'," % (pts, rec_type)
          query = "%s (PTS,REC_TYPE,COMPANY_NAME,CIK,STATUS,INDUSTRY_ID,SP_RATING,FOUNDING_DATE,ADDR_LINE_1," % s_company_base_query
          query += "ADDR_LINE_2,POSTAL_CODE,CITY,STATE_PROVINCE,COUNTRY,CEO_NAME,DESCRIPTION) "
          query += "VALUES ('%s','%s'," % (pts, rec_type)
          
          # now we add all the values which are not "NULL"
          if company_name != "NULL":
            query += "'%s'," % company_name
          else:
            query += "NULL,"
          query += "'%s'," % cik
          query += "'%s'," % status
          query += "'%s'," % industry_id
          if sp_rating != "NULL":
            query += "'%s'," % sp_rating
          else:
            query += "NULL,"
          if founding_date != "NULL":
            query += "'%s'," % founding_date
          else:
            query += "NULL,"
          if addr_line_1 != "NULL":
            query += "'%s'," % addr_line_1
          else:
            query += "NULL,"
          if addr_line_2 != "NULL":
            query += "'%s'," % addr_line_2
          else:
            query += "NULL,"
          if postal_code != "NULL":
            query += "'%s'," % postal_code
          else:
            query += "NULL,"
          if city != "NULL":
            query += "'%s'," % city
          else:
            query += "NULL,"
          if state_province != "NULL":
            query += "'%s'," % state_province
          else:
            query += "NULL,"
          if country != "NULL":
            query += "'%s'," % country
          else:
            query += "NULL,"
          if ceo_name != "NULL":
            query += "'%s'," % ceo_name
          else:
            query += "NULL,"
          if description != "NULL":
            query += "'%s'" % description
          else:
            query += "NULL"
          query += ")"
          s_company_values.append(query)
          if len(s_company_values)>=max_packet:
            # Create query to load text data into tradeType table
            
            for query in s_company_values:
                ctxt.write(query)
                ctxt.write(';\n')
            s_company_values = []
                  
        elif rec_type == "SEC":
          symbol = line[18:33]
          issue_type = line[33:39]
          status = line[39:43]
          name = line[43:113]
          ex_id = line[113:119]
          sh_out = line[119:132]
          first_trade_date = line[132:140]
          first_trade_exchange = line[140:148]
          dividen = line[148:160]
          company_name = line[160:][:-1]
          
          s_security_values.append(
            "%s (PTS,REC_TYPE,SYMBOL,ISSUE_TYPE,STATUS,NAME,EX_ID,SH_OUT,FIRST_TRADE_DATE,FIRST_TRADE_EXCHANGE,"
            "DIVIDEN,COMPANY_NAME_OR_CIK)"
            "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
            %(
              s_security_base_query,
              pts,rec_type,symbol,issue_type,status,name,ex_id,sh_out,first_trade_date,
              first_trade_exchange,dividen,company_name))
          if len(s_security_values)>=max_packet:
            # Create query to load text data into tradeType table
            for query in s_security_values:
               stxt.write(query)
               stxt.write(';\n')
            s_security_values = []
        elif rec_type == "FIN":
          year = line[18:22]
          quarter = line[22:23]
          qtr_start_date = line[23:31]
          posting_date = line[31:39]
          revenue = line[39:56]
          earnings = line[56:73]
          eps = line[73:85]
          diluted_eps = line[85:97]
          margin = line[97:109]
          inventory = line[109:126]
          assets = line[126:143]
          liabilities = line[143:160]
          sh_out = line[160:173]
          diluted_sh_out = line[173:186]
          co_name_or_cik = line[186:][:-1]
          s_financial_values.append(
            "%s (PTS, REC_TYPE, YEAR,QUARTER,QTR_START_DATE,POSTING_DATE,REVENUE,EARNINGS,EPS,DILUTED_EPS,"
            "MARGIN,INVENTORY,ASSETS,LIABILITIES,SH_OUT,DILUTED_SH_OUT,CO_NAME_OR_CIK) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
            %(s_financial_base_query,
              pts, rec_type, year,quarter,qtr_start_date,posting_date, revenue, earnings, eps, diluted_eps, 
              margin, inventory, assets, liabilities, sh_out,diluted_sh_out,co_name_or_cik
              ))
          if len(s_financial_values)>=max_packet:
            # Create query to load text data into tradeType table
            for query in s_financial_values:
                ftxt.write(query)
                ftxt.write(';\n')
            s_financial_values = []
    # after reading each line, save all the records that are left in the arrays (<150)
    for query in s_company_values:
        ctxt.write(query)
        ctxt.write(';\n')
    s_company_values = []
    
    for query in s_security_values:
        stxt.write(query)
        stxt.write(';\n')
    s_security_values = []

    for query in s_financial_values:
        ftxt.write(query)
        ftxt.write(';\n')
    s_financial_values = []
print('Done.')