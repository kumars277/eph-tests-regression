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
    Then The number of the records in EPH staging table and GD_MANIFESTATION is equal


  Scenario Outline: Verify db records for manifestations for books in PMX and EPH is equal
    Given We get <numberOfRecords> random ISBNs for <book_type>
    When We get the manifestation ids for these books
    Then We get the manifestations records from PMX
    Then We have the manifestations in PMX STG
    And The data for manifestations in PMX and PMX STG is equal
    And We get the manifestations in EPH
    And We compare the manifestations in PMX STG and EPH
    And We get the manifestations in EPH golden data
    And We compare the manifestations in EPH and EPH golden data
    Examples:
      | numberOfRecords | book_type |
      | 5               | PHB       |
      | 5               | PSB       |
      | 5               | EBK       |

  Scenario Outline: Verify db records for manifestations for journals in PMX and EPH is equal
    Given We get <numberOfRecords> random records for <journal_type>
    When We get the manifestations records from PMX
    Then We have the manifestations in PMX STG
    And The data for manifestations in PMX and PMX STG is equal
    And We get the manifestations in EPH
    And We compare the manifestations in PMX STG and EPH
    And We get the manifestations in EPH golden data
    And We compare the manifestations in EPH and EPH golden data
    Examples:
      | numberOfRecords | journal_type |
      | 5               | JPR          |
      | 5               | JEL          |


#1)	JPR – Print Journal   - WORK_TYPE_ID IN (4,3,102) F_PRODUCT_MANIFESTATION_TYP = 1
#2)	JEL – Electronic Journal -  WORK_TYPE_ID IN (4,3,102) F_PRODUCT_MANIFESTATION_TYP != 1
#3)	PHB – Print Hard Back Book - MANIFESTATION_SUBTYPE = 424
#4)	PSB - Print Softback Book - MANIFESTATION_SUBTYPE = 425
#5)	EBK – Electronic Book - COMMODITY = 'EB'
