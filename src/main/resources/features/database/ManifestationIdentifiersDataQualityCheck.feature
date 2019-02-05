Feature: Check db records for manifestation identifiers in EPH

  Scenario Outline: Verify count of records in STG_PMX_MANIFESTATION and SA_MANIFESTATION_IDENTIFIER is equal
    Given We get the count of records with <identifier> in STG_PMX_MANIFESTATION
    When We get the count of records with <identifier> in SA_MANIFESTATION_IDENTIFIER
    Then Check the count of the records in STG_PMX_MANIFESTATION and SA_MANIFESTATION_IDENTIFIER is equal

    Examples:
      | identifier |
      | ISBN       |
      | ISSN       |


  Scenario Outline: Verify count of records in SA_MANIFESTATION_IDENTIFIER and GD_MANIFESTATION_IDENTIFIER is equal
    Given We get the count of records with <identifier> in SA_MANIFESTATION_IDENTIFIER
    When We get the count of records with <identifier> in GD_MANIFESTATION_IDENTIFIER
    Then Check the count of the records in SA_MANIFESTATION_IDENTIFIER and GD_MANIFESTATION_IDENTIFIER is equal

    Examples:
      | identifier |
      | ISBN       |
      | ISSN       |


  Scenario Outline: Check the mapping of data between STG_PMX_MANIFESTATION and SA_MANIFESTATION_IDENTIFIER
    Given We get the manifestation ids of <numberOfRecords> random records from STG_PMX_MANIFESTATION that have <identifier> for <type>
    When We get the records from SA_MANIFESTATION_IDENTIFIER
    Then Verify that data in SA_MANIFESTATION_IDENTIFIER is populated correctly for <identifier>
    And We get the records from GD_MANIFESTATION_IDENTIFIER
    And Verify the data in SA_MANIFESTATION_IDENTIFIER and GD_MANIFESTATION_IDENTIFIER is equal

    Examples:
      | numberOfRecords | identifier | type |
      | 1              | ISBN       | PHB  |
      | 1             | ISBN       | PSB  |
      | 1             | ISBN       | EBK  |
      | 1             | ISSN       | JPR  |
      | 1             | ISSN       | JEL  |




