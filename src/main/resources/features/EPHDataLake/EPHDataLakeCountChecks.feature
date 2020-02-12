Feature: Validate data count for all tables between EPH and Data Lake - Outbound


  @DL
  Scenario: Verify that all the work data is transferred from EPH to DL Outbound
    Given We know the number of Works in EPH
    When The works are in DL Outbound
    Then The work count between EPH and DL are identical



