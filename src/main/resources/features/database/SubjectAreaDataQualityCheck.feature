Feature: Entity - SUBJECT AREA - Count And Data Mapping Check - Validate data between PMX and EPH - Talend Load

  @Regression
  Scenario: Compare count of subject area data between PMX and EPH STG
    Given We get the count of subject area data from PMX
    When We get the count of subject area data from EPH STG
    Then Compare the count of subject area data in PMX and EPH STG

  @Regression
  Scenario: Compare count of subject area data between EPH STG and EPH SA
    Given We get the count of subject area data from EPH STG
    When We get the count of subject area data from EPH SA
    Then Compare the count of subject area data in EPH STG and EPH SA

  @Regression
  Scenario: Compare count of subject area data between EPH SA and EPH GD
    Given We get the count of subject area data from EPH SA
    When We get the count of subject area data from EPH GD
    Then Compare the count of subject area data in EPH SA and EPH GD

  @Regression
  Scenario Outline: Validate subject area data is transferred from PMX to EPH STG
    Given We get <countOfRandomIds> random ids of subject area data
    When We get the subject area data from PMX
    Then We get the subject area data from EPH STG
    And Compare the subject area data in PMX and EPH STG
    Examples:
      | countOfRandomIds |
      | 10               |

  @Regression
  Scenario Outline: Check mandatory columns for subject link are populated in SA
    Given We get <countOfRandomIds> random ids of subject area data
    Then We get the subject area data from EPH STG
    When We get the subject area data from EPH SA
    And Check the mandatory columns are populated for subject link
    Examples:
      | countOfRandomIds |
      | 10               |

  @Regression
  Scenario Outline: Validate subject area data is transferred from EPH STG to EPH SA
    Given We get <countOfRandomIds> random ids of subject area data
    When We get the subject area data from EPH STG
    Then We get the subject area data from EPH SA
    And Compare the subject area data in EPH STG and EPH SA
    Examples:
      | countOfRandomIds |
      | 10               |

  @Regression
  Scenario Outline: Validate subject area data is transferred from EPH SA to EPH GD
    Given We get <countOfRandomIds> random ids of subject area data
    When We get the subject area data from EPH SA
    Then We get the subject area data from EPH GD
    And Compare the subject area data in EPH SA and EPH GD
    Examples:
      | countOfRandomIds |
      | 10               |

