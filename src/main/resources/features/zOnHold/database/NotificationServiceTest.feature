Feature: Entity - Notifications - Validate a notification is created for every change

  @Regression
  Scenario Outline: Verify all notifications are created after a full-load
    Given We know the number of <table> GD records after a full-load
    When We check the created notifications by <notification_type> and by <table>
    Then A notification is created for each GD record
    And The <notification_type> id is the same as the id from table <table>
    And The notifications for <notification_type> is successfully processed

    Examples:
       | notification_type |table        |
       | PRODUCT           |product      |
       | WORK              |wwork        |
       | MANIFESTATION     |manifestation|

    @Regression
    Scenario: Creating test data for notification test
      Given A full load was performed
      When The test data is inserted
      Then The test data is created successfully

    @Regression
    Scenario Outline: Verify notification is processed successfully
      Given A <type> is updated
      When A <type> notification is created
      Then The notification is processed
      And The <type> notification is in the payload table

      Examples:
       | type                     |
#       | work                     |
       | product                  |
#       | manifestation            |
#       | work_identifier          |
#       | manifestation_identifier |
#       | translation              |
#       | person_role              |
#       | mirror                   |
#       | financial_attribute      |
#        | work_subject_area_link   |
#        | product_person_role      |
#        | product_package_relationship |


      ######## Negative Tests
  @Regression
  Scenario: Work - Succeeds , Product - Fails
    Given A incorrect product data is inserted
    When The data and the notifications are created
    Then The product notification is not processed

  @Regression
  Scenario: Work - Fails , Product - Fails
    Given A incorrect work and product data is inserted
    When The incorrect data and the notifications are created
    Then The work and product notifications are not processed

  @Regression
  Scenario: Manifestation update, Work - Succeeds , Product - Fails
    Given A correct manifestation is updated connected to incorrect product
    When The data and the notifications are created
    Then The product notification is not processed

  @Regression
  Scenario: Manifestation update, Work - Fails , Product - Fails
    Given A correct manifestation is updated connected to incorrect work and product
    When The incorrect data and the notifications are created
    Then The work and product notifications are not processed

  @Regression
  Scenario: Product update, Work - Succeeds , Product - Fails
    Given An incorrect product is updated connected to correct work
    When The data and the notifications are created
    Then The failed product notification is not processed