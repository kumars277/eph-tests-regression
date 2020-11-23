package com.eph.automation.testing.services.db.JMDataLakeSQL;

public class JMETLDataChecksSQL {
    public static String GET_JMF_WORK_IDs = "select work_id as WORK_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work limit %s";
    public static String GET_JMF_MANIFESTATION_IDs = "select manifestation_id as MANIFESTATION_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_manifestation limit %s";
    public static String GET_JMF_WORK_PERSON_ROLE_IDs = "select work_person_role_id as WORK_PERSON_ROLE_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work_person_role limit %s";
    public static String GET_JMF_WORK_SUBJECT_AREA_IDs = "select work_subject_area_id as WORK_SUBJECT_AREA_ROLE_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work_subject_area limit %s";
    public static String GET_JMF_WORK_CHRONICLE_IDs = "select work_chronicle_id as WORK_CHRONICLE_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_work_chronicle limit %s";
    public static String GET_JMF_PMG_PUBDIR_ALLOCATION_IDs = "select pmg_pubdir_allocation_id as PMG_PUBDIR_ALLOCATION_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_pmg_pubdir_allocation limit %s";
    public static String GET_JMF_APPROVAL_ATTACHMENT_IDs = "select approval_attachment_id as APPROVAL_ATTACHMENT_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_approval_attachment limit %s";
    public static String GET_JMF_REVIEW_COMMENT_IDs = "select review_comment_id as REVIEW_COMMENT_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_review_comment limit %s";
    public static String GET_JMF_APPROVAL_REQUEST_IDs = "select approval_id as APPROVAL_ID from "+ GetJMDLDBUser.getJMDB()+".jmf_approval_request limit %s";
    public static String GET_JMF_APPLICATION_PROPERTIES_IDs = "select application_property_key as APPLICATION_PROPERTY_KEY from "+ GetJMDLDBUser.getJMDB()+".jmf_application_property limit %s";
}