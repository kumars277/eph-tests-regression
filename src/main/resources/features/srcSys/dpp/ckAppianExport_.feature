Feature:Validate data for DPP_Long_List_dag tables

#confluence link:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/119483632526334/DPP-INT-13+Long+List+Generation

    @DPPLONGLIST
    Scenario Outline: Verify that all data is transferred between DPP LongList VIEW and DPP LongList tables
        Given We know the number of <DPPLongListView> data in LongList View
        Then Get the count for <DPPLongListTable> LongList Table
        And Compare the count for <DPPLongListTable> table between LongList View and LongList Table
        Given We get the <numberOfRecords> random CK LongList View ids of <DPPLongListView>
        When We get the LongList View Records from <DPPLongListView>
        Then We get the LongList Table records from <DPPLongListTable>
        And Compare records in LongList View and Table of <DPPLongListTable>

        Examples:
            |numberOfRecords |DPPLongListView        |DPPLongListTable  |
            | 5              |ck_long_list2_v        |ck_long_list      |


        #ck_long_list1_v
        #ck_long_list2_v
        #ck_long_list_part
        #ck_long_list_json_src
        #ck_long_list_json
        #ck_long_list
    #ck_durable_url
    #ck_durable_url_json
    #ck_durable_url_json_src
    #ck_durable_url_part

