Feature: Verify the db records for manifestations in PMX and EPH

Scenario: Verify count of manifestations records in PMX and EPH staging is equal
Given We get the count of the manifestations records in PMX
When We get the count of the manifestations records in PMX STG
Then The number of the records in PMX and EPH staging table is equal

Scenario: Verify count of manifestations records in EPH staging and EPH is equal
Given We get the count of the manifestations records in PMX STG
When The manifestations are transferred to EPH
Then The number of the records in EPH staging table and SA_MANIFESTATION is equal


  Scenario: Verify count of manifestations records in EPH staging and EPH golden data is equal
    Given The manifestations are transferred to EPH
    When The manifestations are transferred to the golden data table
    Then The number of the records in EPH staging table and SA_MANIFESTATION is equal

Scenario: Verify db records for manifestations in PMX and EPH are equal
Given We have manifestations in PMX
When We have manifestations in PMX STG
Then The data for manifestations in PMX and PMX STG is equal
And We get the manifestations in EPH
And We get compare the manifestations in PMX STG and EPH


#1)	JPR – Print Journal   - WORK_TYPE_ID IN (4,3,102) F_PRODUCT_MANIFESTATION_TYP = 1
#2)	JEL – Electronic Journal -  WORK_TYPE_ID IN (4,3,102) F_PRODUCT_MANIFESTATION_TYP != 1
#3)	PHB – Print Hard Back Book - MANIFESTATION_SUBTYPE = 424
#4)	PSB - Print Softback Book - MANIFESTATION_SUBTYPE = 425
#5)	EBK – Electronic Book - COMMODITY = 'EB'
