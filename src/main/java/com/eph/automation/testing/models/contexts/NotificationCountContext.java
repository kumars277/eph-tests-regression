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
}
