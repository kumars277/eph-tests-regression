Feature:Validate data count for SDBooks in Data Lake Access Layer

  #confluence link:https://confluence.cbsels.com/display/EPH/SDBooks

  @SD
  Scenario Outline: Verify Data for SDBooks transform_Current_tables is transferred from Source Table
    Given Get the total count of SD Data from Full Load
    Then  We know the total count of Current SD data
    And Compare count of SD Full load with current table are identical
    Given We get the <countOfRandomIds> random ISBN ids <tableName>
    When Get the records from data inbound for URL
    Then Get the records from transform SD current URL
    And  we compare records of Inbound and current URL
    Examples:
      | tableName                        |    countOfRandomIds|
      |sdbooks_inbound_part              |50                |

  @notUsed
  Scenario Outline: Verify Data for SD transform_current_history tables are transferred from transformed_current tables
    Given We know the total count of Current SD data
    Then Get the count of SD transform_current_history
    And Check count of SD current table and SD current history are identical
    Given We get the <countOfRandomIds> random ISBN ids <sourceTable>
    When Get the records from transform SD current URL
    Then We Get the records from transform SD url History <sourceTable>
    And we compare records of SD current url and SD current url history
    Examples:
      | sourceTable                   | countOfRandomIds      |
      |sdbooks_transform_current_urls |   50                 |

  @SD
  Scenario Outline: Verify Data for SD transform_previous_history tables are transferred from transformed_previous tables
    Given We know the total count of Current SD data
    Then Get the count of SD transform_file
    And Check count of SD current table and SD tranform_file are identical
    Given We get the <countOfRandomIds> random ISBN ids <sourceTable>
    When Get the records from transform SD current URL
    Then We Get the records from transform File SD url <sourceTable>
    And we compare records of SD current url and SD transform file url history
    Examples:
      | sourceTable                             | countOfRandomIds      |
      |sdbooks_transform_file_history_urls_part |   50                 |

   @notUsed
     Scenario Outline: Verify Data for SDBooks delta_current tables are transferred from Current and Previous tables
      Given Get the difference of total count between current and previous time stamps of transform_file of SDbooks data
      Then We know the delta current count for Sdbooks tables
      And Compare SDbooks delta count with tranform_file identical
       Given We get the <countOfRandomIds> random ISBN ids <sourceTable>
       When Get the records from SDBooks for Delta Current Url
       Then We Get the data from the difference of SD Current and Previous transform_file table
       And compare the records of SD delta url with difference of current and previous transform_file
       Examples:
         | sourceTable              | countOfRandomIds|
         |sdbooks_delta_current_urls| 50               |

    @notUsed
      Scenario Outline: Verify Data count for SDBooks delta_current_history tables are transferred from delta_current_work tables
        Given We know the delta current count for Sdbooks tables
        Then Get the count of SDBook delta current history table
        And Compare SD delta current table and delta history are identical
        Given We get the <countOfRandomIds> random ISBN ids <sourceTable>
        When Get the records from SDBooks for Delta Current Url
        Then We Get the records from SD transform Delta Current History
        And we compare the records of SDBooks delta Current and delta Current history
        Examples:
          | sourceTable                   | countOfRandomIds|
          |sdbooks_delta_history_urls_part| 50                 |

    @SD
     Scenario Outline: Verify Data count for SDBooks delta_latest tables are transferred from delta_current and Current_Exclude tables
      Given Get the sum of total count between SDBooks delta current and and Current_Exclude Table
      Then Get the SDbooks latest data count
      And Compare SDBooks latest counts and exclude with delta are identical
      Given We get the <countOfRandomIds> random ISBN ids <tableName>
      When Get the records from the addition of SDbooks Delta_URL and URL_Exclude
      Then Get the records from SDbooks URL latest table
      And we compare records of SDbooks URL Latest with addition of Delta_current_Person and Person_Exclude
      Examples:
        |tableName                                | countOfRandomIds|
        |sdbooks_transform_latest_urls               |50                 |

  @notUsed
  Scenario Outline: Verify Data count for SDBooks delta_current_exclude are transferred from delta_current and current_history tables
    Given Get the SDBooks total count difference between delta current and transform current history Table
    Then Get the SDBooks exclude data count
    And Compare SDBooks exclude count and delta current with current history are identical
    Given We get the <countOfRandomIds> random ISBN ids <tableName>
    When we get the records from the difference of SD Delta_current_url and url_history
    Then We know the records from SDBooks URL Excl Table
    And we compare the records of SD url Exclude with difference of Delta_current_url and url_history
    Examples:
      |tableName                                | countOfRandomIds|
      |sdbooks_transform_history_excl_delta     |50                |

  @SD
  Scenario Outline: Verify Duplicate Entry for SDBoks in transform latest tables
    Given Get the SDBooks Duplicate count in <SourceTableName> table
    Then Check the SDBooks count should be equal to Zero <SourceTableName>
    Examples:
      |SourceTableName                      |
      |sdbooks_transform_latest_urls           |