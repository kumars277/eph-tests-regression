Feature: Entity - Manifestation Identifier - Count & Data Mapping Check - Validate data between PMX and EPH - Talend Load

  @Regression
  Scenario Outline: Verify count of records in STG_PMX_MANIFESTATION and SA_MANIFESTATION_IDENTIFIER is equal
    Given We get the count of records with <identifier> in STG_PMX_MANIFESTATION
    When We get the count of records with <identifier> in SA_MANIFESTATION_IDENTIFIER
    Then Check the count of the records in STG_PMX_MANIFESTATION and SA_MANIFESTATION_IDENTIFIER is equal for <identifier>

    Examples:
      | identifier |
      | ISBN       |
      | ISSN       |

  @Regression
  Scenario Outline: Verify count of records in SA_MANIFESTATION_IDENTIFIER and GD_MANIFESTATION_IDENTIFIER is equal
    Given We get the count of records with <identifier> in SA_MANIFESTATION_IDENTIFIER
    When We get the count of records with <identifier> in GD_MANIFESTATION_IDENTIFIER
    Then Check the count of the records in SA_MANIFESTATION_IDENTIFIER and GD_MANIFESTATION_IDENTIFIER is equal for <identifier>

    Examples:
      | identifier |
      | ISBN       |
      | ISSN       |

  @Regression
  Scenario: Verify sum of counts of manifestations in EPH GD and EPH AE is equal to count of manifestation idenfintifiers in EPH SA
    Given We get the count of records with <identifier> in SA_MANIFESTATION_IDENTIFIER
    Given Get the count of records for manifestation identifiers in EPH AE
    When We get the count of records with <identifier> in GD_MANIFESTATION_IDENTIFIER
    Then Verify sum of records for manifestation identifiers in EPH GD and EPH AE is equal to number of records in EPH SA



  @Regression
  Scenario Outline: Check the mapping of data between STG_PMX_MANIFESTATION and SA_MANIFESTATION_IDENTIFIER
    Given We get the manifestation ids of <numberOfRecords> random records from STG_PMX_MANIFESTATION that have <identifier> for <type>
    When We get the records from SA_MANIFESTATION_IDENTIFIER
    Then Verify that data in SA_MANIFESTATION_IDENTIFIER is populated correctly for <identifier>
    And We get the records from GD_MANIFESTATION_IDENTIFIER
    And Verify the data in SA_MANIFESTATION_IDENTIFIER and GD_MANIFESTATION_IDENTIFIER is equal for <identifier>

    Examples:
      | numberOfRecords | identifier | type |
      | 2               | ISBN       | PHB  |
      | 2               | ISBN       | PSB  |
      | 2               | ISBN       | EBK  |
      | 2               | ISSN       | JPR  |
      | 2               | ISSN       | JEL  |

  @Regression
  Scenario: Verify that existing records end-dated properly for books
    Given Get ISBN from STG and GD for end dated records in GD
    Then Check the manifestation identifiers are updated properly


  @Regression
  Scenario: Verify that existing records end-dated properly for journals
    Given Get ISSN from STG and GD for end dated records in GD
    Then Check the manifestation identifiers are updated properly for ISSN


