Feature: Entity - Work Identifier - Count & Data Mapping Check - Validate data between PMX and EPH - Talend Full Load

  @Regression
  Scenario Outline: Check if all of the identifiers are stored in the identifiers table
    Given We have a work from type <type> to check
    When We get the data from Staging, SA and Work Identifiers
    Then All of the identifiers are stored
    And The identifiers data is correct
    And The identifiers data between SA and GD is identical

    Examples:
      |type       |
      |STAB       |
      |SERMEM     |
      |BOOKSET    |
      #|BOOKSERIES |
      |CONT       |
      |JNL        |
      |CABS       |

    Scenario Outline: Verify count of Work Identifier records
      Given We know the work identifiers count in staging from column <stg_type> and <type>
      When We get the work identifier count from SA and GD <type>
      Then The counts between staging and SA are matching
      And The counts between SA and GD are matching

      Examples:
      |type                   | stg_type              |
      |ISSN-L                 |ISSN_L                 |
      |PROJECT-NUM            |PROJECT_NUM            |
      |ELSEVIER JOURNAL NUMBER|JOURNAL_NUMBER         |
      |JOURNAL ACRONYM        |JOURNAL_ACRONYM        |
      |DAC-K                  |DAC_KEY                |

      Scenario: Verify that an old entry is end-dated properly
        Given We have an end-dated identifier in GD
        When We get the data for the identifier from staging
        Then There is a change to the work identifier