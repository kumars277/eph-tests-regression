Feature:Validate data between GD Tables and PackageItems table or Research Packages

  #Confluence:https://elsevier.atlassian.net/wiki/spaces/IN/pages/88416868939/EPHMS011+-+EPH+Product+Notification+Collections+API+Update


  @rpMicroService
  Scenario Outline: Check between GD Tables and Package Items of Research Package UI
    Given We get the <countOfRandomIds> random EPR Prod ids from Semarchy tables
    When  Get the data from semarchy tables
    Then  Get the data from package item table of research package
    And we compare the records between semarchy and research packages
    Examples:
      |countOfRandomIds |
      |10               |

