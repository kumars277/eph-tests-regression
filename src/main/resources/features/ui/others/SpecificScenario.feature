Feature: Specially requested Business scenarios

  @PFDMC @PFProddd
  Scenario: Search specific Journal and verify link tab
    Given We set specific journal ids for search
    Then  search work and verify links

  @PFDMC
  Scenario: Search the Journal and verify link tab
    Given We get 2 random journal ids for search
    Then  search work and verify links


  Scenario Outline:verify links from file
    Given <filePath> have a valid file available <datafile> to read
    Then verify links from <filePath> and write result to <filePath>
    Examples:
    |filePath|datafile|
    |C:\Users\Chitren\Office Work\Project doc\EPH sprint testing\EPHD-3127 link verification\      |Image URLs for parties.csv|


  Scenario: Read file from S3 location
      Given Read file from S3 bucket com-elsevier-jrbi-nonprod/sit/staging and key ops-10001-journal-metadata-eph-sit
      And print file data

@linkTesting
  Scenario Outline:verify links from S3 file
    Given Read file from S3 bucket <s3Bucket> and key <s3file>
    Then verify links and write result to <ResultPath>
    And upload result <ResultPath> to s3 <s3Bucket>
    Examples:
      |s3Bucket           |s3file                       |ResultPath|
      |eph-test-data/QA   |Image URLs for parties.csv   |target\   |
