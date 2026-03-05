package com.example.qryaw;

import android.content.Context;
import android.hardware.Sensor;
import android.hardware.SensorEvent;
import android.hardware.SensorEventListener;
import android.hardware.SensorManager;

import com.google.ar.core.Camera;
import com.google.ar.core.Frame;
import com.google.ar.core.Pose;

import java.util.ArrayDeque;
import java.util.Queue;

public class NorthAlignedYawManager implements SensorEventListener {

    private final SensorManager sensorManager;
    private final Sensor accelerometer;
    private final Sensor magnetometer;
    private final Sensor gyroscope;

    private final float[] gravity = new float[3];
    private final float[] geomagnetic = new float[3];
    private final float[] gyroValues = new float[3];

    private boolean hasAccel = false;
    private boolean hasMag = false;

    // Circular buffer for compass samples
    private final Queue<Double> compassSamples = new ArrayDeque<>();
    private static final int COMPASS_SAMPLE_SIZE = 40;

    // Fusion state
    private double northOffset = 0.0;
    private boolean offsetInitialized = false;

    // Tuning parameters
    private static final double VARIANCE_THRESHOLD = 0.02;  // circular variance
    private static final double GYRO_STABLE_THRESHOLD = 0.02; // rad/s
    private static final double OFFSET_ADJUST_SPEED = 0.02;  // slow correction
    private static final double SMOOTHING_ALPHA = 0.1;

    private double smoothedCompass = 0.0;

    public NorthAlignedYawManager(Context context) {
        sensorManager = (SensorManager) context.getSystemService(Context.SENSOR_SERVICE);

        accelerometer = sensorManager.getDefaultSensor(Sensor.TYPE_ACCELEROMETER);
        magnetometer = sensorManager.getDefaultSensor(Sensor.TYPE_MAGNETIC_FIELD);
        gyroscope = sensorManager.getDefaultSensor(Sensor.TYPE_GYROSCOPE);
    }

    public void start() {
        sensorManager.registerListener(this, accelerometer, SensorManager.SENSOR_DELAY_GAME);
        sensorManager.registerListener(this, magnetometer, SensorManager.SENSOR_DELAY_GAME);
        sensorManager.registerListener(this, gyroscope, SensorManager.SENSOR_DELAY_GAME);
    }

    public void stop() {
        sensorManager.unregisterListener(this);
    }

    @Override
    public void onSensorChanged(SensorEvent event) {

        if (event.sensor.getType() == Sensor.TYPE_ACCELEROMETER) {
            System.arraycopy(event.values, 0, gravity, 0, 3);
            hasAccel = true;
        }

        if (event.sensor.getType() == Sensor.TYPE_MAGNETIC_FIELD) {
            System.arraycopy(event.values, 0, geomagnetic, 0, 3);
            hasMag = true;
        }

        if (event.sensor.getType() == Sensor.TYPE_GYROSCOPE) {
            System.arraycopy(event.values, 0, gyroValues, 0, 3);
        }

        if (hasAccel && hasMag) {
            float[] R = new float[9];
            float[] I = new float[9];

            boolean success = SensorManager.getRotationMatrix(R, I, gravity, geomagnetic);
            if (success) {
                float[] orientation = new float[3];
                SensorManager.getOrientation(R, orientation);

                double azimuth = Math.toDegrees(orientation[0]);
                azimuth = (azimuth + 360) % 360;

                addCompassSample(azimuth);
            }
        }
    }

    @Override
    public void onAccuracyChanged(Sensor sensor, int accuracy) {}

    private void addCompassSample(double value) {
        if (compassSamples.size() >= COMPASS_SAMPLE_SIZE) {
            compassSamples.poll();
        }
        compassSamples.add(value);

        double mean = computeCircularMean();
        smoothedCompass = SMOOTHING_ALPHA * mean + (1 - SMOOTHING_ALPHA) * smoothedCompass;
    }

    private double computeCircularMean() {
        double sumSin = 0;
        double sumCos = 0;

        for (double angle : compassSamples) {
            double rad = Math.toRadians(angle);
            sumSin += Math.sin(rad);
            sumCos += Math.cos(rad);
        }

        return (Math.toDegrees(Math.atan2(sumSin, sumCos)) + 360) % 360;
    }

    private double computeCircularVariance() {
        double sumSin = 0;
        double sumCos = 0;

        for (double angle : compassSamples) {
            double rad = Math.toRadians(angle);
            sumSin += Math.sin(rad);
            sumCos += Math.cos(rad);
        }

        double R = Math.sqrt(sumSin * sumSin + sumCos * sumCos) / compassSamples.size();
        return 1 - R;
    }

    private boolean isDeviceStable() {
        double gyroMagnitude = Math.sqrt(
                gyroValues[0] * gyroValues[0] +
                        gyroValues[1] * gyroValues[1] +
                        gyroValues[2] * gyroValues[2]
        );
        return gyroMagnitude < GYRO_STABLE_THRESHOLD;
    }

    public double getNorthAlignedYaw(Frame frame) {

        Camera camera = frame.getCamera();
        Pose pose = camera.getPose();

        float[] zAxis = pose.getZAxis();
        double arYaw = Math.toDegrees(Math.atan2(zAxis[0], zAxis[2]));
        arYaw = (arYaw + 360) % 360;

        if (compassSamples.size() < COMPASS_SAMPLE_SIZE) {
            return arYaw; // not enough compass data yet
        }

        double variance = computeCircularVariance();

        if (variance < VARIANCE_THRESHOLD && isDeviceStable()) {

            double targetOffset = normalizeAngle(smoothedCompass - arYaw);

            if (!offsetInitialized) {
                northOffset = targetOffset;
                offsetInitialized = true;
            } else {
                northOffset = normalizeAngle(
                        northOffset + OFFSET_ADJUST_SPEED * normalizeAngle(targetOffset - northOffset)
                );
            }
        }

        return normalizeAngle(arYaw + northOffset);
    }

    private double normalizeAngle(double angle) {
        angle = angle % 360;
        if (angle < 0) angle += 360;
        return angle;
    }
}