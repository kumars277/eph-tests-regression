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

    @WIP
    Scenario Outline: Verify notification is processed successfully
      Given A <type> is updated
      When A notification is created
      Then The notification is processed
      And The <type> notification is in the payload table

      Examples:
       | type          |
       | work          |
       #| product       |
       #| manifestation |

