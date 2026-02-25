package com.example.qryaw.db;

import java.util.Date;

public class QRRegistrationRecord {

    public long id;
    public String qrPayload;
    public double yawDegrees;
    public double normalX;
    public double normalY;
    public double normalZ;
    public double toleranceDegrees;
    public String deviceModel;
    public String appVersion;
    public Date registeredAt;
    public String notes;

    public QRRegistrationRecord(long id,
                                String qrPayload,
                                double yawDegrees,
                                double normalX,
                                double normalY,
                                double normalZ,
                                double toleranceDegrees,
                                String deviceModel,
                                String appVersion,
                                Date registeredAt,
                                String notes) {
        this.id = id;
        this.qrPayload = qrPayload;
        this.yawDegrees = yawDegrees;
        this.normalX = normalX;
        this.normalY = normalY;
        this.normalZ = normalZ;
        this.toleranceDegrees = toleranceDegrees;
        this.deviceModel = deviceModel;
        this.appVersion = appVersion;
        this.registeredAt = registeredAt;
        this.notes = notes;
    }
}