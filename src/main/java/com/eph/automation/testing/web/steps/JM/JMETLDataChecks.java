package com.eph.automation.testing.web.steps.JM;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import cucumber.api.java.en.*;
import com.eph.automation.testing.services.db.JMDataLakeSQL.JMETLDataChecksSQL;
import java.util.*;
import java.util.stream.Collectors;

public class JMETLDataChecks {
    private static String sql;
    private static List<String> Ids;

    @Given("^We get the (.*) random JM ids of (.*)$")
    public void getRandomPromisIds(String numberOfRecords, String Currenttable) {
//  numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        switch (Currenttable) {
            case "jmf_work":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkIds.stream().map(m -> (Integer) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_manifestation":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_MANIFESTATION_IDs, numberOfRecords);
                List<Map<?, ?>> randomManifestationIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomManifestationIds.stream().map(m -> (Integer) m.get("MANIFESTATION_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_work_person_role":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_PERSON_ROLE_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkPersonRolesIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkPersonRolesIds.stream().map(m -> (Integer) m.get("WORK_PERSON_ROLE_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_work_subject_area":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_SUBJECT_AREA_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkSubjectAreaIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkSubjectAreaIds.stream().map(m -> (Integer) m.get("WORK_SUBJECT_AREA_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_work_chronicle":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_CHRONICLE_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkChronicleIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkChronicleIds.stream().map(m -> (Integer) m.get("WORK_CHRONICLE_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_pmg_pubdir_allocation":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_PMG_PUBDIR_ALLOCATION_IDs, numberOfRecords);
                List<Map<?, ?>> randomPmgPubdirAllocationIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomPmgPubdirAllocationIds.stream().map(m -> (Integer) m.get("PMG_PUBDIR_ALLOCATION_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_approval_attachment":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPROVAL_ATTACHMENT_IDs, numberOfRecords);
                List<Map<?, ?>> randomApprovalAttachmentIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomApprovalAttachmentIds.stream().map(m -> (Integer) m.get("APPROVAL_ATTACHMENT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_review_comment":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_REVIEW_COMMENT_IDs, numberOfRecords);
                List<Map<?, ?>> randomReviewCommentIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomReviewCommentIds.stream().map(m -> (Integer) m.get("REVIEW_COMMENT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_approval_request":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPROVAL_REQUEST_IDs, numberOfRecords);
                List<Map<?, ?>> randomApprovalRequestIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomApprovalRequestIds.stream().map(m -> (Integer) m.get("APPROVAL_REQUEST_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_application_properties":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPLICATION_PROPERTIES_IDs, numberOfRecords);
                List<Map<?, ?>> randomApplicationPropertiesIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomApplicationPropertiesIds.stream().map(m -> (Integer) m.get("APPLICATION_PROPERTY_KEY")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }
}


