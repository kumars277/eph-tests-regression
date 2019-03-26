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

    public int getProcessed() {
        return processed;
    }

    public void setProcessed(int processed) {
        this.processed = processed;
    }

    public int getPayloadcount() {
        return payloadcount;
    }

    public void setPayloadcount(int payloadcount) {
        this.payloadcount = payloadcount;
    }

    public int processed;
    public int payloadcount;

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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String status;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String timestamp;
    public String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String key;


    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public int attempts;
}
