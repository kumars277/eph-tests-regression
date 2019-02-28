Feature: Entity - Notifications - Validate a notification is created for every change

  @Regression
  Scenario Outline: Verify all notifications are created after a full-load
    Given We know the number of <table> GD records after a full-load
    When We check the created notifications by <notification_type>
    Then A notification is created for each GD record
    And The <notification_type> id is the same as the id from table <table>

    Examples:
      | notification_type |table        |
      | PRODUCT           |product      |
      | WORK              |wwork        |
      | MANIFESTATION     |manifestation|