Feature: Entity - Manifestation Identifier - Count & Data Mapping Check - Validate data between PMX and EPH - Talend Full Load

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
  Scenario Outline: Verify that existing records end-dated properly
    Given We get the manifestation ids of all records with set updated effective_end_date in SA for <identifier>
    Then Check the manifestation identifiers are updated for <identifier>
    Examples:
      | identifier |
      | ISBN       |
      | ISSN       |



