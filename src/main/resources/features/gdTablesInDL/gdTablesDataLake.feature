Feature:Validate data for GD Tables in DataLake from PostgreSQL

  @gdDL
  Scenario Outline: Verify that GD tables in the PostgreSQL and DL

    Given We get the count <SourceTable> from postgreSQL
    Then Get the count of <SourceTable> from the DataLake
    And we compare both the counts are identical for the table <SourceTable>
    #Given Get <numberOfRecords> random ids of <SourceTable> from the postgreSQL
    #When We get the records for <SourceTable> from postgreSQL
    #Then Get the records for <SourceTable> from the DataLake
    #And Compare the records of <SourceTable> from postgreSQL and DataLake
    Examples:
      |numberOfRecords  |SourceTable                 |
      | 5               |gd_accountable_product      |
      | 5               |gd_event                    |
      | 5               |gd_legal_owner              |
      | 5               |gd_manifestation            |
      | 5               |gd_manifestation_identifier |
      | 5               |gd_person                   |
      | 5               |gd_price                    |
      | 5               |gd_product                   |
      | 5               |gd_product_financial_attribs |
      | 5               |gd_product_hierarchy      |
      | 5               |gd_product_line      |
      | 5               |gd_product_person_role      |
      | 5               |gd_product_product_hchy_link      |
      | 5               |gd_product_rel_package      |
      | 5               |gd_product_relationship      |
      | 5               |gd_subject_area      |
      | 5               |gd_work_access_model      |
      | 5               |gd_work_business_model      |
      | 5               |gd_work_financial_attribs      |
      | 5               |gd_work_hierarchy      |
      | 5               |gd_work_identifier      |
      | 5               |gd_work_legal_owner      |
      | 5               |gd_work_metric      |
      | 5               |gd_work_person_role      |
      | 5               |gd_work_rel_package      |
      | 5               |gd_work_relationship      |
      | 5               |gd_work_subject_area_link      |
      | 5               |gd_work_work_hchy_link      |
      | 5               |gd_wwork                   |











