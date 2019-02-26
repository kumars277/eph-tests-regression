Feature: Entity - Notifications - Validate a notification is created for every change

  @WIP
  Scenario Outline: Verify all notifications are created after a full-load
    Given We know the number of <table> GD records after a full-load
    When We check the created notifications by <notification_type>
    Then A notification is created for each GD record

    Examples:
      | notification_type |table        |
      | PRODUCT           |product      |
      | WORK              |wwork        |
      | MANIFESTATION     |manifestation|
      #| ISSN_L            |BOOKSERIES |