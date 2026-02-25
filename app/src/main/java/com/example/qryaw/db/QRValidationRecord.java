package com.example.qryaw.db;

import java.util.Date;

public class QRValidationRecord {

    public long id;
    public long registrationId;
    public String qrPayload;
    public double currentYaw;
    public double registeredYaw;
    public double deltaDegrees;
    public boolean isWithinTolerance;
    public double toleranceDegrees;
    public Date validatedAt;
    public String deviceModel;
    public String appVersion;

    public QRValidationRecord(long id,
                              long registrationId,
                              String qrPayload,
                              double currentYaw,
                              double registeredYaw,
                              double deltaDegrees,
                              boolean isWithinTolerance,
                              double toleranceDegrees,
                              Date validatedAt,
                              String deviceModel,
                              String appVersion) {

        this.id = id;
        this.registrationId = registrationId;
        this.qrPayload = qrPayload;
        this.currentYaw = currentYaw;
        this.registeredYaw = registeredYaw;
        this.deltaDegrees = deltaDegrees;
        this.isWithinTolerance = isWithinTolerance;
        this.toleranceDegrees = toleranceDegrees;
        this.validatedAt = validatedAt;
        this.deviceModel = deviceModel;
        this.appVersion = appVersion;
    }
}