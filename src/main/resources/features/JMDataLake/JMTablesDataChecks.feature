Feature:Validate data for JM between MYsql and Data Lake - Inbound

  @JMDL
  Scenario Outline: Validate Allocation Changes data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Allocation records from JMF MySQL of <table>
    Then We get the JMF Allocation records from DL of <table>
    And Compare JMF Allocation records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000             | JMF_ALLOCATION_CHANGE|

  @JMDL
  Scenario Outline: Validate Application properties data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random application key of <table>
    When We get the JMF Application properties records from JMF MySQL of <table>
    Then We get the JMF Application properties records from DL of <table>
    And Compare JMF Application properties records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000              | JMF_APPLICATION_PROPERTIES|

  @JMDL
  Scenario Outline: Validate Approval Attachment data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Approval Attachment records from JMF MySQL of <table>
    Then We get the JMF Approval Attachment records from DL of <table>
    And Compare JMF Approval Attachment records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000             | JMF_APPROVAL_ATTACHMENT|

  @JMDL
  Scenario Outline: Validate Approval Request data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Approval Request records from JMF MySQL of <table>
    Then We get the JMF Approval Request records from DL of <table>
    And Compare JMF Approval Request records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000             | JMF_APPROVAL_REQUEST|

  @JMDL
  Scenario Outline: Validate Chronicle Scenario data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Chronicle Scenario records from JMF MySQL of <table>
    Then We get the JMF Chronicle Scenario records from DL of <table>
    And Compare JMF Chronicle Scenario records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000             | JMF_CHRONICLE_SCENARIO|

  @JMDL
  Scenario Outline: Validate Chronicle Status data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Chronicle Status records from JMF MySQL of <table>
    Then We get the JMF Chronicle Status records from DL of <table>
    And Compare JMF Chronicle Status records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000             | JMF_CHRONICLE_STATUS|

  @JMDL
  Scenario Outline: Validate Family Resource data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Family Resource records from JMF MySQL of <table>
    Then We get the JMF Family Resource records from DL of <table>
    And Compare JMF Family Resource records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000             | JMF_FAMILY_RESOURCE_DETAILS|

  @JMDL
  Scenario Outline: Validate Finanacial Info data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Finanacial Info records from JMF MySQL of <table>
    Then We get the JMF Finanacial Info records from DL of <table>
    And Compare JMF Finanacial Info records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000             | JMF_FINANCIAL_INFORMATION|

  @JMDL
  Scenario Outline: Validate Legal Info data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Legal Info records from JMF MySQL of <table>
    Then We get the JMF Legal Info records from DL of <table>
    And Compare JMF Legal Info records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000            | JMF_LEGAL_INFORMATION|

  @JMDL
  Scenario Outline: Validate Manifestation data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Manifestation Info records from JMF MySQL of <table>
    Then We get the JMF Manifestation Info records from DL of <table>
    And Compare JMF Manifestation Info records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000                 | JMF_MANIFESTATION_ELECTRONIC_DETAILS|

  @JMDL
  Scenario Outline: Validate Manifestation Print is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Manifestation Print records from JMF MySQL of <table>
    Then We get the JMF Manifestation Print records from DL of <table>
    And Compare JMF Manifestation Print records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000                 | JMF_MANIFESTATION_PRINT_DETAILS|

  @JMDL
  Scenario Outline: Validate Party Product is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Party Product records from JMF MySQL of <table>
    Then We get the JMF Party Product records from DL of <table>
    And Compare JMF Party Product records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000                 | JMF_PARTY_IN_PRODUCT|

  @JMDL
  Scenario Outline: Validate Product Availability is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Product Availability records from JMF MySQL of <table>
    Then We get the JMF Product Availability records from DL of <table>
    And Compare JMF Product Availability records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000                 | JMF_PRODUCT_AVAILABILITY|

  @JMDL
  Scenario Outline: Validate Product Chronicle is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Product Chronicle records from JMF MySQL of <table>
    Then We get the JMF Product Chronicle records from DL of <table>
    And Compare JMF Product Chronicle records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000               | JMF_PRODUCT_CHRONICLE|

  @JMDL
  Scenario Outline: Validate Product Family is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Product Family records from JMF MySQL of <table>
    Then We get the JMF Product Family records from DL of <table>
    And Compare JMF Product Family records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000               | JMF_PRODUCT_FAMILY|

  @JMDL
  Scenario Outline: Validate Product Manifestation is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Product Manifestation records from JMF MySQL of <table>
    Then We get the JMF Product Manifestation records from DL of <table>
    And Compare JMF Product Manifestation records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000               | JMF_PRODUCT_MANIFESTATION|

  @JMDL
  Scenario Outline: Validate Product Subject is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Product Subject records from JMF MySQL of <table>
    Then We get the JMF Product Subject records from DL of <table>
    And Compare JMF Product Subject records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000               | JMF_PRODUCT_SUBJECT_AREA|

  @JMDL
  Scenario Outline: Validate Review Comment is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Review Comment records from JMF MySQL of <table>
    Then We get the JMF Review Comment records from DL of <table>
    And Compare JMF Review Comment records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000              | JMF_REVIEW_COMMENT|

  @JMDL
  Scenario Outline: Validate Product Work is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Product Work records from JMF MySQL of <table>
    Then We get the JMF Product Work records from DL of <table>
    And Compare JMF Product Work records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000              | JMF_PRODUCT_WORK|

  @JMDL
  Scenario Outline: Validate Product Information is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Production Information records from JMF MySQL of <table>
    Then We get the JMF Production Information records from DL of <table>
    And Compare JMF Production Information records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1000                 | JMF_PRODUCTION_INFORMATION|


