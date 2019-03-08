package com.eph.automation.testing.models.dao;

public class NotificationDataObject {
    public int getEphGDCount() {
        return ephGDCount;
    }

    public void setEphGDCount(int ephGDCount) {
        this.ephGDCount = ephGDCount;
    }

    public int getNotificationCount() {
        return notificationCount;
    }

    public void setNotificationCount(int notificationCount) {
        this.notificationCount = notificationCount;
    }

    public int ephGDCount;
    public int notificationCount;

    public String gdID;

    public String getGdID() {
        return gdID;
    }

    public void setGdID(String gdID) {
        this.gdID = gdID;
    }

    public String getNotificationID() {
        return notificationID;
    }

    public void setNotificationID(String notificationID) {
        this.notificationID = notificationID;
    }

    public String notificationID;
}
