#created by Nishant @ 29 Sep 2020
Feature:Validate data check for BCS tables in Data Lake

  @BCS
Scenario Outline: Verify Data for BCS is transferred from initial ingest
Given We get the <countOfRandomIds> random ids from initial ingest <targetTable>
When Get the data records from initial ingest for <targetTable>
Then Get the records from current tables <targetTable>
And Compare the records of initial ingest and current table <targetTable>
Examples:
| targetTable               | countOfRandomIds|
|stg_current_classification |       1         |
|stg_current_content        |       1         |