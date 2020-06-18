Feature:Validate data for DPP tables between EPH and Data Lake - Outbound

#  Created by Thomas Kruck on 12/03//2020

  @DL
  Scenario Outline: Verify that all DPP data is transferred from DPP to DL
    Given We know the number of DPP <tableName> data in EPH
    When The DPP <tableName> data is in the DL
    Then The DPP count for <tableName> table between EPH and DL are identical
    Examples:
      | tableName          |
      | comment            |
      | eph_user           |
      | package            |
      | package_approval   |
      | package_approvals  |
      | package_have_items |
      | package_item       |
      | package_item_audit |

  @DL
  Scenario Outline: Validate DPP Table data is transferred from EPH to DL Outbound
    Given We get <countOfRandomIds> random DPP ids of <tableName>
    When We get the DPP <tableName> records from EPH
    Then We get the DPP <tableName> records from DL
    And Compare DPP <tableName> records in EPH and DL
    Examples:
      | countOfRandomIds |   tableName        |
      | 10                | comment            |
      | 10                | eph_user           |
      | 10                | package            |
      | 10                | package_approval   |
      | 10                | package_approvals  |
      | 10                | package_have_items |
      | 10                | package_item       |
      | 10                | package_item_audit |