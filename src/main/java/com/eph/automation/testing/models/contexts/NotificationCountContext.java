package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.models.dao.NotificationDataObject;

import java.util.List;

public class NotificationCountContext {
    public static List<NotificationDataObject> gdCountNumber;
    public static List<NotificationDataObject> notificationCountNumber;
    public static List<NotificationDataObject> failedNotifications;
    public static List<NotificationDataObject> notificationID;
    public static List<NotificationDataObject> createdNotification;
    public static List<NotificationDataObject> processedNotification;
    public static List<NotificationDataObject> payloadNotification;
    public static List<NotificationDataObject> payloadResult;
    public static List<NotificationDataObject> writeAttemptsBefore;
    public static List<NotificationDataObject> writeAttemptsAfter;
    public static List<NotificationDataObject> checkTestData;
    public static List<NotificationDataObject> getWorkTD;
    public static List<NotificationDataObject> getProductTD;
    public static List<NotificationDataObject> getManifestationTD;

    //negative test
    public static List<NotificationDataObject> getWorkTDNegative;
    public static List<NotificationDataObject> getProductTDNegative;
    public static List<NotificationDataObject> getManifestationTDNegative;
    public static List<NotificationDataObject> getStatusSTNotification;
    public static List<NotificationDataObject> getWriteAttemptsBeforeNegWork;
    public static List<NotificationDataObject> getWriteAttemptsBeforeNegProduct1;
    public static List<NotificationDataObject> getWriteAttemptsBeforeNegProduct2;
    public static List<NotificationDataObject> getWriteAttemptsAfterNegWork;
    public static List<NotificationDataObject> getWriteAttemptsAfterNegProduct1;
    public static List<NotificationDataObject> getWriteAttemptsAfterNegProduct2;
    public static List<NotificationDataObject> getFailedPayloadNot;
    public static List<NotificationDataObject> getFailedPayloadNotBefore;

    public static boolean manifestationIdentifier=false;
    public static boolean translation=false;
    public static boolean personRole=false;
    public static boolean mirror=false;
    public static boolean finAttribute =false;
    public static boolean productPersonRole =false;
    public static boolean productPackRel =false;
    public static boolean workSubjectAreaLink=false;
}
