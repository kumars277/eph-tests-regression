#created by Nishant @ 29 Sep 2020
Feature:Validate data check for BCS tables in Data Lake

  @BCSDebug
Scenario Outline: Verify Data for BCS is transferred from initial ingest
Given We get the <countOfRandomIds> random ids from initial ingest <targetTable>
When Get the data records from initial ingest
Then Get the records from transform current manifestation
And Compare the records of manifestation full load and current manifestation
Examples:
| targetTable         | countOfRandomIds|
|jrbi_journal_data_full|       3        |