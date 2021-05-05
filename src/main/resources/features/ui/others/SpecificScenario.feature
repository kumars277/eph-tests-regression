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
Given We have a valid file available <filePath> <datafile> to read
Then verify links from file <filePath> <datafile>
Examples:
|filePath|datafile|
|C:\Users\Chitren\Office Work\Project doc\EPH sprint testing\EPHD-3127 link verification\      |Image URLs for parties.csv|
