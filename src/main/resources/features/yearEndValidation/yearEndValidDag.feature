Feature:Validate EPH Year End Validation DAG

  #Confluence:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/119601028011908/EPH+Year-End+Data+Validation+Design
  #https://elsevier.atlassian.net/browse/EPHD-4641 - JIRA Ticket

  @SDRM
  Scenario Outline: Check the count and data for pmc and opco rc changes
    Given Get the total count from year_end_full table for <tableName>
    Then  We get the total count from <tableName>
    And   Compare count of year_end_full and <tableName>
  #  Given We get the <countOfRandomIds> from year_end_full
  #  When  Get the data from year_end_full table
  #  Then  Get the data from <tableName>
  #  And   Compare the data between year_end_full and <tableName>
    Examples:
      | tableName                                                |countOfRandomIds |
      |pmc_changes_full_v                                        |50               |
      |opco_rc_changes_full_v                                    |50               |