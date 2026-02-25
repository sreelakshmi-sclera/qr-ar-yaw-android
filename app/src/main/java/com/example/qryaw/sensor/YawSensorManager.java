package com.example.qryaw.sensor;

import android.content.Context;
import android.hardware.Sensor;
import android.hardware.SensorEvent;
import android.hardware.SensorEventListener;
import android.hardware.SensorManager;

public class YawSensorManager implements SensorEventListener {

    public interface YawListener {
        void onYawChanged(double yawDegrees);
    }

    private SensorManager sensorManager;
    private Sensor rotationSensor;
    private YawListener listener;

    public YawSensorManager(Context context, YawListener listener) {
        this.listener = listener;
        sensorManager = (SensorManager) context.getSystemService(Context.SENSOR_SERVICE);
        rotationSensor = sensorManager.getDefaultSensor(Sensor.TYPE_ROTATION_VECTOR);
    }

    public void start() {
        sensorManager.registerListener(this, rotationSensor,
                SensorManager.SENSOR_DELAY_GAME);
    }

    public void stop() {
        sensorManager.unregisterListener(this);
    }

    @Override
    public void onSensorChanged(SensorEvent event) {

        float[] rotationMatrix = new float[9];
        float[] orientation = new float[3];

        SensorManager.getRotationMatrixFromVector(rotationMatrix, event.values);
        SensorManager.getOrientation(rotationMatrix, orientation);

        double yawRadians = orientation[0];
        double yawDegrees = Math.toDegrees(yawRadians);

        if (listener != null) {
            listener.onYawChanged(yawDegrees);
        }
    }

    @Override
    public void onAccuracyChanged(Sensor sensor, int accuracy) {}
}