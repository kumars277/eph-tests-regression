Feature: Customer Search API: Works
  As an EIP-MS Integration User
  I would like to search works from Enterprise Customer Hub using EIP Search
  So that I can use the details to validate business needs


  @searchAPI @workSearchAPI
  Scenario: search work by ID
    Given We get 1 random search ids for works default
    And We get the work search data from EPH GD
    Then the work details are retrieved and compared

   #new search fields included as part of Journal Finder #EPR-W-108TJK
  @searchAPI @workSearchAPI @JFSearch
  Scenario Outline: Search journal by search option
    Given We get 1 random journal ids for search
    And We get the work search data from EPH GD
    Then the journal by search <option> details are retrieved and compared
    Examples:
      | option            |
      |TITLE              |
      |EPR_ID             |
      |JOURNAL_ACRONYM    |
      |JOURNAL_NUMBER     |
      |ISSN               |

  @searchAPI @workSearchAPI @JFSearch
  Scenario Outline: search journal by Person id
    Given We get 1 random journal ids for search
    And We get the work search data from EPH GD
    Then work response is compared with the DB for <options>
    Examples:
      |options            |

      |PERSON_NAME        |
      |PEOPLE_HUB_ID      |
      |PERSON_ID      |
      #updated below as per EPHD-1414 by Nishant @ 08 Jul 2020
      |personFullNameCurrent|
      |personIdCurrent|

  @searchAPI @workSearchAPI
  Scenario Outline: search work by title
    Given We get 1 random search ids for works <tType>
    And We get the work search data from EPH GD
    Then the work details are retrieved by title <tType> and compared
    Examples:
    | tType |
    |WORK_PRODUCT_SUMMARY_NAME|
    |WORK_TITLE |
    |WORK_MANIFESTATION_TITLE|
    |WORK_MANIFESTATION_PRODUCT_SUMMARY_NAME|

  @searchAPI @workSearchAPI
  Scenario Outline: Search works by identifier
    Given We get 1 random search ids for works <idType>
    And We get the work search data from EPH GD
    Then the works search by identifier <idType> details are retrieved and compared
    Examples:
      | idType                        |
      | WORK_IDENTIFIER               |
      | WORK_MANIFESTATION_IDENTIFIER |
      | WORK_ID                       |
      | WORK_MANIFESTATION_ID         |

  @searchAPI @workSearchAPI
  Scenario Outline: Search works by identifier and Type
    Given We get 1 random search ids for works <idType>
    And We get the work search data from EPH GD
    Then the work search by identifier <idType> and type details are retrieved and compared
    Examples:
      | idType                        |
      | WORK_IDENTIFIER          |
      | WORK_MANIFESTATION_IDENTIFIER |

  @searchAPI @workSearchAPI
  Scenario Outline: Search works by search option
    Given We get 1 random search ids for works <option>
    And We get the work search data from EPH GD
    Then the works retrieved by search <option> details are retrieved and compared
    Examples:
      | option                                 |
      | WORK_ID                                |
      | WORK_TITLE                             |
      | WORK_IDENTIFIER                        |
      | WORK_MANIFESTATION_ID                  |
      | WORK_MANIFESTATION_TITLE               |
      | WORK_MANIFESTATION_IDENTIFIER          |
      | WORK_PRODUCT_ID                        |
      | WORK_PRODUCT_SUMMARY_NAME              |
      | WORK_PERSONS_FULLNAME                  |
      | WORK_MANIFESTATION_PRODUCT_ID          |
      | WORK_MANIFESTATION_PRODUCT_SUMMARY_NAME|

  @searchAPI @workSearchAPI
  Scenario: search work by random Person id
    Given We get 1 random search ids for person roles
    Then the work response count is compared with the count in the DB for Person Id

  @searchAPI @workSearchAPI
  Scenario: search work by PMC Code
    Given We get 1 random search ids for works default
    And We get the work search data from EPH GD
    Then the work details are retrieved by PMC Code and compared

  @searchAPI @workSearchAPI
  Scenario: search work by PMG Code
    Given We get 1 random search ids for works default
    And We get the work search data from EPH GD
    Then the work details are retrieved by PMG Code and compared

  @searchAPI @workSearchAPI
  Scenario: search work by accountableProduct
    Given We get 1 random search ids for works default
    And We get the work search data from EPH GD
    Then the work details are retrieved by accountableProduct and compared

  @searchAPI @workSearchAPI
  Scenario: search work by workStatus
    Given We get 1 random search ids for works default
    And We get the work search data from EPH GD
    Then the work details are retrieved by workStatus and compared

  @searchAPI @workSearchAPI
  Scenario: search work by workType
    Given We get 1 random search ids for works default
    And We get the work search data from EPH GD
    Then the work details are retrieved by workType and compared

  @searchAPI @workSearchAPI
  Scenario: search work by manifestationType
    Given We get 1 random search ids for works default
    And We get the work search data from EPH GD
    Then the work details are retrieved by manifestationType and compared

  @searchAPI @workSearchAPI
  Scenario: search work by Search with PMCCode
    Given We get 1 random search ids for works default
    And We get the work search data from EPH GD
    Then the work details are retrieved by search with PMC code and compared

  @searchAPI @workSearchAPI
  Scenario: search work by Search with PMGCode
    Given We get 1 random search ids for works default
    And We get the work search data from EPH GD
    Then the work details are retrieved by search with PMG code and compared

  #{'datafile':'C:\Users\Chitren\Office Work\Project doc\EPH sprint testing\Elastic search,APIv3 and JRBI data/stch_work_ext_json_202006181758.csv'}

  @searchAPI @workSearchAPI
  Scenario Outline: Search E2E
    Given We get the work data from EPH GD for <id>
    Then the work details are retrieved and compared
    Examples:
      | id                        |
      | EPR-W-102S7C              |

# covered in above tests
  @APIv3
  Scenario: verify Manifestation Extended specific ID
    Given We get 2 random search ids for Extended manifestation
    And get work by manifestation
    And We get the work search data from EPH GD
    Then the work details are retrieved and compared

  @APIv3
  Scenario: verify Work Extended by specific ID
    Given We get 1 random search ids for Extended works
    And We get the work search data from EPH GD
    Then the work details are retrieved and compared