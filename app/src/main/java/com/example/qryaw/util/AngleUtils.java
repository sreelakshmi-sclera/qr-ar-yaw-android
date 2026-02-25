package com.example.qryaw.util;

public class AngleUtils {

    public static double normalize(double angle) {
        angle = angle % 360;
        if (angle < 0) angle += 360;
        return angle;
    }

    public static double circularDelta(double a, double b) {
        double diff = Math.abs(normalize(a) - normalize(b));
        return Math.min(diff, 360 - diff);
    }

    public static double circularMean(double[] angles) {
        double sumSin = 0;
        double sumCos = 0;

        for (double angle : angles) {
            double rad = Math.toRadians(angle);
            sumSin += Math.sin(rad);
            sumCos += Math.cos(rad);
        }

        return normalize(Math.toDegrees(Math.atan2(sumSin, sumCos)));
    }
}