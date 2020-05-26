Feature:Validate data count for JRBI Work,Manifestation and Person tables in Data Lake

#  Created by Dinesh on 26/05/2020

    @JRBI
  Scenario Outline: Verify Data Count for JRBI Current Work is transferred from Source Table
    Given We know the total count of JRBI data from Source table <tableName>
    Then  Get the JRBI <tableName> work data count in the DL
    And Compare count <tableName> table and JRBI Source table are identical
    Examples:
      | tableName                           |
      | jrbi_transform_current_work               |
      |jrbi_transform_current_manifestation       |
      |jrbi_transform_current_person              |

