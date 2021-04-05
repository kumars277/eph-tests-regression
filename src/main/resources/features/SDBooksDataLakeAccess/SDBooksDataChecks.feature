Feature:Validate data count for SDBooks in Data Lake Access Layer

#  Created by Dinesh on 08/09/2020
  #confunece link: https://confluence.cbsels.com/display/EPH/SDBooks



  @SD
  Scenario Outline: Verify Data for SD transform_current_url is transferred from sdbooks_inbound_part
    Given We get the <countOfRandomIds> random ISBN ids <tableName>
    When  Get the records from data inbound for URL
    Then  Get the records from transform SD current URL
    And   Compare the records of Inbound and current URL
    Examples:
      | tableName                        |    countOfRandomIds|
      |sdbooks_inbound_part              |50                |

    @SD
  Scenario Outline: Verify Data for SD transform_Current_URL_history is transferred from Current URL
      Given We get the <countOfRandomIds> random ISBN ids <sourceTable>
      When Get the records from transform SD current URL
      Then We Get the records from transform SD url History <sourceTable>
      And Compare the records of SD current url and SD current url history
     Examples:
      | sourceTable                   | countOfRandomIds      |
      |sdbooks_transform_current_urls |   50                 |

  @SD
  Scenario Outline: Verify Data for SD transform_File_URL_history is transferred from Current URL
    Given We get the <countOfRandomIds> random ISBN ids <sourceTable>
    When Get the records from transform SD current URL
    Then We Get the records from transform File SD url <sourceTable>
    And Compare the records of SD current url and SD transform file url history
    Examples:
      | sourceTable                   | countOfRandomIds      |
      |sdbooks_transform_file_history_urls_part |   50                 |

  @SD
  Scenario Outline: Verify Data for SD transform_Delta_URL_history is transferred from Delta URL
    Given We get the <countOfRandomIds> random ISBN ids <sourceTable>
    When Get the records from SDBooks for Delta Current Url
    Then We Get the records from SD transform Delta Current History
    And Compare the records of SDBooks delta Current and delta Current history
    Examples:
      | sourceTable                   | countOfRandomIds|
      |sdbooks_delta_history_urls_part| 50                 |


  @SD
    Scenario Outline: Verify Data from the difference of Current and Previous is transffered to delta current table
      Given We get the <countOfRandomIds> random ISBN ids <sourceTable>
      When Get the records from SDBooks for Delta Current Url
      Then We Get the data from the difference of SD Current and Previous transform_file table
      And Compare the records of SD delta url with difference of current and previous transform_file
      Examples:
        | sourceTable              | countOfRandomIds|
        |sdbooks_delta_current_urls| 50               |

  @SD
  Scenario Outline: Verify Data from the difference of SD Delta_URL and URL_history is transferred to url exclude table
    Given We get the <countOfRandomIds> random ISBN ids <tableName>
    When Get the records from the difference of SD Delta_current_url and url_history
    Then We know the records from SDBooks URL Excl Table
    And  Compare the records of SD url Exclude with difference of Delta_current_url and url_history
    Examples:
      |tableName                                | countOfRandomIds|
      |sdbooks_transform_history_excl_delta     |50                |

  @SD
  Scenario Outline: Verify Data from the addition of SD Delta_current_url and url_Exclude is transferred to url Latest table
    Given We get the <countOfRandomIds> random ISBN ids <tableName>
    When Get the records from the addition of SDbooks Delta_URL and URL_Exclude
    Then Get the records from SDbooks URL latest table
    And  Compare the records of SDbooks URL Latest with addition of Delta_current_Person and Person_Exclude
    Examples:
      |tableName                                | countOfRandomIds|
      |sdbooks_transform_latest_urls               |50                 |



