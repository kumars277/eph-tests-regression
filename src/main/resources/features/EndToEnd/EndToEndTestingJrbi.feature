Feature:Validate EndToEnd Testing for JRBI

#  Created by Dinesh S on 06/05/2021


    #Phase-1 Scenarios
   #To check the Data in S3 file is transffered to jrbi fulload table
   #Glue Script used to transfer the data
   #Confluence: https://confluence.cbsels.com/display/EPH/JRBI+Inbound+V2

  #Below Scenarios checks whether the count and Data in the jrbi full load matching with the source file in S3 bucket
    Scenario Outline: Verify the Glue job picking all the Data count from the source file in s3 bucket matching with the jrbi_journal_data_full
      Given Get the total count of JRBI Data from source file <fileLocation><fileName>
      Then  Get total count of JRBI Data from the table <tableName>
      And   Compare count of JRBI data between <tableName> and source file
    Examples:
      | tableName                    |fileLocation                        |fileName     |
      |  jrbi_journal_data_full      |C:\\Users\\sureshkumard\\Downloads\\    |jrbi.csv     |

  Scenario Outline: Verify the Glue job picking all the Data value from the source file in s3 bucket matching with the jrbi_journal_data_full
    Given Get the data of JRBI Data from source file <fileLocation><fileName>
    Then  Get JRBI Data from the table <tableName>
    And   Compare JRBI data between <tableName> and source file
    Examples:
      | tableName                    |fileLocation                        |fileName     |
      |  jrbi_journal_data_full      |C:\\Users\\sureshkumard\\Downloads\\    |jrbi.csv     |




