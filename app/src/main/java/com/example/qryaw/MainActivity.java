package com.example.qryaw;

import android.Manifest;
import android.content.pm.PackageManager;
import android.hardware.Sensor;
import android.hardware.SensorManager;
import android.os.Bundle;
import android.util.Log;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import androidx.activity.result.ActivityResultLauncher;
import androidx.activity.result.contract.ActivityResultContracts;
import androidx.annotation.OptIn;
import androidx.appcompat.app.AlertDialog;
import androidx.appcompat.app.AppCompatActivity;
import androidx.camera.core.ExperimentalGetImage;
import androidx.core.content.ContextCompat;

import com.example.qryaw.db.QRDatabaseHelper;
import com.google.ar.core.ArCoreApk;

import io.github.sceneview.ar.ARSceneView;
import kotlin.Unit;

import com.google.ar.core.CameraIntrinsics;
import com.google.ar.core.Frame;
import com.google.ar.core.TrackingState;
import com.google.mlkit.vision.barcode.BarcodeScanner;
import com.google.mlkit.vision.barcode.BarcodeScannerOptions;
import com.google.mlkit.vision.barcode.BarcodeScanning;
import com.google.mlkit.vision.barcode.common.Barcode;
import com.google.mlkit.vision.common.InputImage;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class MainActivity extends AppCompatActivity {

    private static final String TAG = "QRYaws";

    // ── Sampling config (mirrors iOS constants) ──
    private static final int REQUIRED_SAMPLES = 8;
    private static final double MIN_SAMPLE_WEIGHT = 0.15;
    private static final double DEFAULT_TOLERANCE = 15.0;

    private long compassStartTime = 0;
    private double compassSin = 0;
    private double compassCos = 0;
    private int compassSamples = 0;

    // ── AR / UI ──
    private ARSceneView arSceneView;
    private QROverlayView overlayView;

    private TextView statusText, infoText, modeText, dbBadge;
    private Button btnRegister;
    private Button btnValidate;

    private final BarcodeScanner scanner =
            BarcodeScanning.getClient(
                    new BarcodeScannerOptions.Builder()
                            .setBarcodeFormats(Barcode.FORMAT_QR_CODE)
                            .build()
            );
    // ── State (exact mirror of iOS ivars) ──
    /**
     * Last decoded QR text — used as the SQLite primary key
     */
    private String lastPayload;
    private QRDatabaseHelper.Registration currentRegistration;   // == registration in iOS
    private boolean isSampling = false;
    private SamplingPurpose purpose = SamplingPurpose.REGISTER;
    private final List<YawSample> sampleBuffer = new ArrayList<>();
    private long lastSampleTime = 0;
    /**
     * Max gap between consecutive samples — if exceeded, restart sampling
     * because the QR was lost long enough that the user may have shifted.
     */
    private static final long MAX_SAMPLE_GAP_MS = 400;
    /**
     * Last detected screen corners — updated when QR is found, cleared when lost
     */
    private List<float[]> lastDetectedCorners;

    // ── QR-lost debounce ──
    // ML Kit is async and can miss QR on individual frames (motion blur, focus, etc.)
    // unlike iOS Vision which runs synchronously and is more frame-consistent.
    // We debounce the "QR lost" signal: only clear after several consecutive misses.
    private long lastQrSeenTime = 0;
    private static final long QR_LOST_TIMEOUT_MS = 500;

    private enum SamplingPurpose {REGISTER, VALIDATE}

    // ── Inner data classes ──
    private static class YawSample {
        final double yaw;
        final double weight;

        YawSample(double y, double w) {
            yaw = y;
            weight = w;
        }
    }

    private static class QRMeasurement {
        final double yaw;
        final double confidence;
        final String method;

        QRMeasurement(double yaw, double confidence, String method) {
            this.yaw = yaw;
            this.confidence = confidence;
            this.method = method;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Vector Math Helpers  (port of iOS SIMD calls)
    // ─────────────────────────────────────────────────────────────────────────

    private static float[] vec3Scale(float[] v, float s) {
        return new float[]{v[0] * s, v[1] * s, v[2] * s};
    }

    private static float vec3Dot(float[] a, float[] b) {
        return a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
    }

    private static float vec3Len(float[] v) {
        return (float) Math.sqrt(vec3Dot(v, v));
    }

    private static float[] vec3Normalize(float[] v) {
        float len = vec3Len(v);
        return (len < 1e-6f) ? new float[]{0, 0, 0} : vec3Scale(v, 1f / len);
    }

    private static float[] vec3Cross(float[] a, float[] b) {
        return new float[]{
                a[1] * b[2] - a[2] * b[1],
                a[2] * b[0] - a[0] * b[2],
                a[0] * b[1] - a[1] * b[0]
        };
    }

    /**
     * Rotate a camera-space vector into world space using the ARCore camera pose matrix.
     */
    private static float[] rotateCamToWorld(float[] camVec, float[] m) {
        return new float[]{
                m[0] * camVec[0] + m[4] * camVec[1] + m[8] * camVec[2],
                m[1] * camVec[0] + m[5] * camVec[1] + m[9] * camVec[2],
                m[2] * camVec[0] + m[6] * camVec[1] + m[10] * camVec[2]
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Permission launcher
    // ─────────────────────────────────────────────────────────────────────────

    private final ActivityResultLauncher<String> permissionLauncher =
            registerForActivityResult(new ActivityResultContracts.RequestPermission(), granted -> {
                if (granted) {
                    checkArCoreAndStart();
                } else {
                    Toast.makeText(this, "Camera permission required", Toast.LENGTH_LONG).show();
                    finish();
                }
            });

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    // ── Compass for North alignment (iOS gravityAndHeading equivalent) ──
    private android.hardware.SensorManager sensorManager;
    private final float[] orientation = new float[3];

    private double arNorthOffsetDeg = Double.NaN;
    private boolean compassReady = false;

    private final android.hardware.SensorEventListener compassListener =
            new android.hardware.SensorEventListener() {

                @Override
                public void onSensorChanged(android.hardware.SensorEvent event) {
                    if (event.sensor.getType() == Sensor.TYPE_ROTATION_VECTOR) {

                        float[] rotationMatrix = new float[9];
                        SensorManager.getRotationMatrixFromVector(rotationMatrix, event.values);

                        float[] remappedRotationMatrix = new float[9];
                        SensorManager.remapCoordinateSystem(
                                rotationMatrix,
                                SensorManager.AXIS_X,
                                SensorManager.AXIS_Z,
                                remappedRotationMatrix);

                        SensorManager.getOrientation(remappedRotationMatrix, orientation);

                        double azimuthDeg = Math.toDegrees(orientation[0]);
                        if (azimuthDeg < 0) azimuthDeg += 360.0;

                        long now = System.currentTimeMillis();
                        if (compassStartTime == 0) {
                            compassStartTime = now;
                        }

                        // Keep your old 3-second locking logic!
                        if (!compassReady) {
                            Frame frame = arSceneView.getFrame();
                            if (frame != null && frame.getCamera().getTrackingState() == TrackingState.TRACKING) {

                                double rad = Math.toRadians(azimuthDeg);
                                compassSin += Math.sin(rad);
                                compassCos += Math.cos(rad);
                                compassSamples++;

                                if (now - compassStartTime > 3000 && compassSamples > 15) {
                                    double meanRad = Math.atan2(compassSin / compassSamples, compassCos / compassSamples);
                                    double meanDeg = Math.toDegrees(meanRad);
                                    if (meanDeg < 0) meanDeg += 360.0;

                                    double arYaw = getARCameraYawDegrees(frame);
                                    arNorthOffsetDeg = meanDeg - arYaw;
                                    while (arNorthOffsetDeg < 0) arNorthOffsetDeg += 360;
                                    while (arNorthOffsetDeg >= 360) arNorthOffsetDeg -= 360;

                                    compassReady = true;
                                    log("Compass locked at " + meanDeg + "° using ROTATION_VECTOR");

                                    runOnUiThread(() -> {
                                        dbBadge.setText("✅ Compass ready — North-anchored");
                                        if (currentRegistration != null) {
                                            setMode(ScanMode.VALIDATE);
                                        } else if (lastPayload != null) {
                                            setMode(ScanMode.REGISTER);
                                        } else {
                                            showStatus("Scan a QR code to begin");
                                        }
                                    });
                                }
                            }
                        }
                    }
                }

                @Override
                public void onAccuracyChanged(android.hardware.Sensor sensor, int accuracy) {}
            };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        sensorManager = (android.hardware.SensorManager) getSystemService(SENSOR_SERVICE);

        arSceneView = findViewById(R.id.ar_scene_view);
        overlayView = findViewById(R.id.overlay_view);
        statusText = findViewById(R.id.status_text);
        infoText = findViewById(R.id.info_text);
        modeText = findViewById(R.id.mode_text);
        dbBadge = findViewById(R.id.db_badge);
        btnRegister = findViewById(R.id.btn_register);
        btnValidate = findViewById(R.id.btn_validate);
        Button btnHistory = findViewById(R.id.btn_history);

        btnRegister.setOnClickListener(v -> registerTapped());
        btnValidate.setOnClickListener(v -> validateTapped());
        btnHistory.setOnClickListener(v -> historyTapped());

        log("DB path: " + QRDatabaseHelper.getInstance(this).getDatabasePath());

        if (ContextCompat.checkSelfPermission(this, Manifest.permission.CAMERA)
                == PackageManager.PERMISSION_GRANTED) {
            checkArCoreAndStart();
        } else {
            permissionLauncher.launch(Manifest.permission.CAMERA);
        }
    }

    @Override
    protected void onResume() {
        super.onResume();
        resetCompass();

        sensorManager.registerListener(
                compassListener,
                sensorManager.getDefaultSensor(Sensor.TYPE_ROTATION_VECTOR),
                android.hardware.SensorManager.SENSOR_DELAY_UI);
    }

    @Override
    protected void onPause() {
        super.onPause();
        sensorManager.unregisterListener(compassListener);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // AR Setup  (mirrors iOS setupAR + startARSession)
    // ─────────────────────────────────────────────────────────────────────────

    private void checkArCoreAndStart() {
        ArCoreApk.getInstance().checkAvailabilityAsync(this, availability -> {
            if (availability.isTransient()) {
                arSceneView.postDelayed(this::checkArCoreAndStart, 200);
                return;
            }
            if (availability.isSupported()) {
                initializeArSession();
                startArSession();
            } else {
                statusText.setText("ARCore not supported on this device.");
            }
        });
    }

    private void resetCompass() {
        compassReady = false;
        compassStartTime = 0;
        compassSin = 0;
        compassCos = 0;
        compassSamples = 0;
        arNorthOffsetDeg = Double.NaN;
    }

    private void initializeArSession() {
        try {
            arSceneView.getPlaneRenderer().setEnabled(false);
        } catch (Exception e) {
            Log.e(TAG, "AR setup failed", e);
            statusText.setText("AR init failed");
        }
    }

    private void startArSession() {
        resetCompass();
        btnRegister.setEnabled(false);
        btnRegister.setAlpha(0.5f);
        btnValidate.setEnabled(false);
        btnValidate.setAlpha(0.5f);

        showStatus("⏳ Compass stabilizing...\nPlease hold still for a moment.");
        dbBadge.setText("📡 Aligning to North...");

        arSceneView.setOnFrame(frameTime -> {
            Frame frame = arSceneView.getFrame();
            if (frame == null) return Unit.INSTANCE;
            if (frame.getCamera().getTrackingState() != TrackingState.TRACKING)
                return Unit.INSTANCE;

            processARFrame(frame);
            return Unit.INSTANCE;
        });

        arSceneView.postDelayed(() -> {
            dbBadge.setText("✅ Compass ready — North-anchored");
            showStatus("Scan a QR code to begin");

            if (lastDetectedCorners != null) {
                btnRegister.setEnabled(true);
                btnRegister.setAlpha(1f);
            }
            if (currentRegistration != null) {
                btnValidate.setEnabled(true);
                btnValidate.setAlpha(1f);
            }
        }, 2000);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QR Payload Detected → Auto-load from DB
    // ─────────────────────────────────────────────────────────────────────────

    private void onQRPayloadDetected(String payload) {
        if (payload.equals(lastPayload)) return;
        lastPayload = payload;
        currentRegistration = null;

        QRDatabaseHelper.Registration record =
                QRDatabaseHelper.getInstance(this).fetchRegistration(payload);

        if (record != null) {
            currentRegistration = record;

            setMode(ScanMode.VALIDATE);
            dbBadge.setText("📦 Loaded from DB  (registered " + relativeTime(record.registeredAt) + ")");
            showStatus("QR Detected ✓  —  Registration found in DB!\nPress VALIDATE to check orientation.");
            updateInfoLabel(null, record, null, InfoSource.DATABASE);

            log("DB hit → payload=" + payload + " yaw=" + record.yaw + "°");
        } else {
            setMode(ScanMode.REGISTER);
            dbBadge.setText("🆕 New QR — not yet registered");
            showStatus("QR Detected ✓\nNo prior registration found.\nPress REGISTER to save yaw.");
            infoText.setText("Payload : " + payload.substring(0, Math.min(42, payload.length()))
                    + "\nStatus  : Not registered");

            log("DB miss → payload=" + payload);
        }

        // Only enable buttons if compass is actually ready
        if (compassReady) {
            btnRegister.setEnabled(true);
            btnRegister.setAlpha(1f);
            if (currentRegistration != null) {
                btnValidate.setEnabled(true);
                btnValidate.setAlpha(1f);
            }
        } else {
            btnRegister.setEnabled(false);
            btnValidate.setEnabled(false);
            showStatus("⏳ Waiting for Compass lock...");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Button Actions
    // ─────────────────────────────────────────────────────────────────────────

    private void registerTapped() {
        if (!compassReady) {
            showStatus("⏳ Compass still aligning. Please wait...");
            return;
        }
        if (lastDetectedCorners == null) {
            showStatus("⚠️ No QR code in frame. Point camera at QR code.");
            return;
        }
        if (lastPayload == null) {
            showStatus("⚠️ QR payload not decoded yet.");
            return;
        }
        if (isSampling) return;
        startSampling(SamplingPurpose.REGISTER);
    }

    private void validateTapped() {
        if (!compassReady) {
            showStatus("⏳ Compass still aligning. Please wait...");
            return;
        }
        if (lastDetectedCorners == null) {
            showStatus("⚠️ No QR code in frame.");
            return;
        }
        if (lastPayload == null) {
            showStatus("⚠️ QR payload not decoded.");
            return;
        }
        if (QRDatabaseHelper.getInstance(this).fetchRegistration(lastPayload) == null) {
            showStatus("⚠️ No registration in DB for this QR.\nPress REGISTER first.");
            setMode(ScanMode.REGISTER);
            return;
        }
        if (isSampling) return;
        startSampling(SamplingPurpose.VALIDATE);
    }

    private void historyTapped() {
        if (lastPayload == null) {
            new AlertDialog.Builder(this)
                    .setTitle("No QR Scanned")
                    .setMessage("Scan a QR code first to see its history.")
                    .setPositiveButton("OK", null)
                    .show();
            return;
        }

        List<QRDatabaseHelper.Validation> validations = QRDatabaseHelper.getInstance(this).fetchValidations(lastPayload, 20);
        List<QRDatabaseHelper.Registration> allRegs = QRDatabaseHelper.getInstance(this).fetchAllRegistrations();

        StringBuilder msg = new StringBuilder("═══ REGISTRATION ═══\n");
        QRDatabaseHelper.Registration reg = null;
        for (QRDatabaseHelper.Registration r : allRegs) {
            if (r.payload.equals(lastPayload)) {
                reg = r;
                break;
            }
        }
        if (reg != null) {
            msg.append(String.format(Locale.US, "Yaw      : %.1f°\n", reg.yaw));
            msg.append(String.format(Locale.US, "Tolerance: ±%.0f°\n", reg.tolerance));
            msg.append("Saved    : ").append(relativeTime(reg.registeredAt)).append("\n");
            msg.append("Device   : ").append(android.os.Build.MODEL).append("\n");
            // FIX #1: Mirror iOS which shows appVersion in history
            msg.append("App      : ").append(getAppVersion()).append("\n");
        } else {
            msg.append("None found.\n");
        }

        msg.append("\n═══ LAST ").append(validations.size()).append(" VALIDATIONS ═══\n");
        if (validations.isEmpty()) {
            msg.append("No validations yet.");
        } else {
            for (QRDatabaseHelper.Validation v : validations) {
                String icon = v.withinTolerance ? "✅" : "❌";
                msg.append(String.format(Locale.US, "%s %s   Δ%+.1f°\n",
                        icon, relativeTime(v.validatedAt), v.delta));
            }
        }

        new AlertDialog.Builder(this)
                .setTitle("QR History")
                .setMessage(msg.toString())
                .setPositiveButton("Close", null)
                .setNegativeButton("🗑 Delete This QR", (d, w) -> {
                    QRDatabaseHelper.getInstance(this).deleteRegistration(lastPayload);
                    currentRegistration = null;
                    lastPayload = null;
                    modeText.setText("MODE: WAITING");
                    dbBadge.setText("");
                    infoText.setText("");
                    showStatus("Registration deleted.\nScan a QR code to begin.");
                    btnRegister.setEnabled(false);
                    btnRegister.setAlpha(0.5f);
                    btnValidate.setEnabled(false);
                    btnValidate.setAlpha(0.5f);
                })
                .show();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Mode Switching  (mirrors iOS setMode(_:))
    // ─────────────────────────────────────────────────────────────────────────

    private enum ScanMode {REGISTER, VALIDATE}

    private void setMode(ScanMode mode) {
        boolean canClick = compassReady && !isSampling;

        switch (mode) {
            case REGISTER:
                modeText.setText("MODE: REGISTER");
                modeText.setTextColor(0xFFFFEB3B);
                btnRegister.setEnabled(canClick);
                btnRegister.setAlpha(canClick ? 1f : 0.5f);
                btnValidate.setEnabled(false);
                btnValidate.setAlpha(0.5f);
                break;
            case VALIDATE:
                modeText.setText("MODE: VALIDATE");
                modeText.setTextColor(0xFF4CAF50);
                btnRegister.setEnabled(canClick);
                btnRegister.setAlpha(canClick ? 0.8f : 0.5f);
                btnValidate.setEnabled(canClick);
                btnValidate.setAlpha(canClick ? 1f : 0.5f);
                break;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Sampling Engine
    // ─────────────────────────────────────────────────────────────────────────

    private void startSampling(SamplingPurpose p) {
        sampleBuffer.clear();
        purpose = p;
        isSampling = true;
        lastSampleTime = 0;   // reset — first sample has no gap check
        String action = (p == SamplingPurpose.REGISTER) ? "Registering" : "Validating";
        showStatus("📐 " + action + "... hold still\n(collecting " + REQUIRED_SAMPLES + " samples)");
        dbBadge.setText("🔄 Sampling 0/" + REQUIRED_SAMPLES + "...");
    }

    private void collectSample(Frame frame, float[] imagePoints) {
        QRMeasurement result = computeQRYawFromTopEdge(frame, imagePoints);
        if (result == null) return;

        if (result.confidence < MIN_SAMPLE_WEIGHT) {
            log(String.format(Locale.US,
                    "Sample rejected (confidence %.2f < %.2f)", result.confidence, MIN_SAMPLE_WEIGHT));
            return;
        }

        log("method=" + result.method);

        // ── Staleness check ──
        // If there was a big gap since the last accepted sample, the QR was
        // intermittently lost and the user may have shifted position. Discard
        // all prior samples and restart the buffer from this sample.
        long now = System.currentTimeMillis();
        if (lastSampleTime > 0 && (now - lastSampleTime) > MAX_SAMPLE_GAP_MS) {
            int discarded = sampleBuffer.size();
            sampleBuffer.clear();
            log(String.format(Locale.US,
                    "Sample gap %dms > %dms — discarded %d stale samples, restarting",
                    (now - lastSampleTime), MAX_SAMPLE_GAP_MS, discarded));
            runOnUiThread(() ->
                    dbBadge.setText("🔄 Gap detected — resampling 0/" + REQUIRED_SAMPLES));
        }
        lastSampleTime = now;

        if (!sampleBuffer.isEmpty()) {

            double referenceYaw = circularWeightedMean(sampleBuffer);
            double diff = Math.abs(angleDifference(referenceYaw, result.yaw));

            if (diff > 45) {
                log("Rejected sample due to yaw jump: " + result.yaw);
                return;
            }
        }

        sampleBuffer.add(new YawSample(result.yaw, result.confidence));
        log(String.format(Locale.US, "  Sample %d: yaw=%.1f° conf=%.2f [%s]",
                sampleBuffer.size(), result.yaw, result.confidence, result.method));

        final String methodCopy = result.method;
        final int countCopy = sampleBuffer.size();
        runOnUiThread(() ->
                dbBadge.setText("🔄 Sampling " + countCopy + "/" + REQUIRED_SAMPLES
                        + " [" + methodCopy + "]"));

        if (sampleBuffer.size() >= REQUIRED_SAMPLES) {
            isSampling = false;
            double finalYaw = circularWeightedMean(sampleBuffer);
            log(String.format(Locale.US,
                    "Sampling complete. Averaged yaw = %.2f° from %d samples",
                    finalYaw, sampleBuffer.size()));
            sampleBuffer.clear();
            commitReading(finalYaw);
        }
    }

    private double circularWeightedMean(List<YawSample> samples) {
        double sumSin = 0, sumCos = 0, totalWeight = 0;
        for (YawSample s : samples) {
            double rad = Math.toRadians(s.yaw);
            sumSin += Math.sin(rad) * s.weight;
            sumCos += Math.cos(rad) * s.weight;
            totalWeight += s.weight;
        }
        if (totalWeight < 1e-6) return 0;
        double mean = Math.toDegrees(Math.atan2(sumSin / totalWeight, sumCos / totalWeight));
        if (mean < 0) mean += 360.0;
        return mean;
    }

    /**
     * Called once sampling is complete with the final averaged yaw.
     * <p>
     * FIX #3: Now applies North offset so yaw values are absolute compass bearings,
     * matching iOS .gravityAndHeading behavior. Without this, registrations from
     * one ARCore session could not be validated in another session because ARCore
     * uses an arbitrary heading each time.
     */
    private void commitReading(double rawYaw) {
        if (lastPayload == null) return;

        double yaw = rawYaw;

        // APPLY THE COMPASS OFFSET HERE
        if (!Double.isNaN(arNorthOffsetDeg)) {
            yaw = (rawYaw + arNorthOffsetDeg) % 360.0;
            if (yaw < 0) yaw += 360.0;
        }

        // Synthetic normal for DB compatibility (mirrors iOS)
        double yawRad = Math.toRadians(yaw);
        float normalX = (float) Math.sin(yawRad);
        float normalZ = (float) Math.cos(yawRad);

        if (purpose == SamplingPurpose.REGISTER) {
            // ── SAVE TO SQLite ──
            Long dbId = QRDatabaseHelper.getInstance(this).saveRegistration(
                    lastPayload, yaw, normalX, 0.0, normalZ, DEFAULT_TOLERANCE);

            if (dbId == null) {
                showStatus("⚠️ Database save failed.");
                return;
            }

            QRDatabaseHelper.Registration reg = new QRDatabaseHelper.Registration();
            reg.id = dbId;
            reg.payload = lastPayload;
            reg.yaw = yaw;
            reg.normalX = normalX;
            reg.normalY = 0.0;
            reg.normalZ = normalZ;
            reg.tolerance = DEFAULT_TOLERANCE;
            reg.registeredAt = new Date();
            currentRegistration = reg;

            double finalYaw = yaw;
            runOnUiThread(() -> {
                setMode(ScanMode.VALIDATE);
                dbBadge.setText("💾 Saved to DB  (id=" + dbId + ")");
                showStatus(String.format(Locale.US,
                        "✅ Registered & Saved!\nYaw = %.1f°  |  id=%d", finalYaw, dbId));
                updateInfoLabel(finalYaw, reg, null, InfoSource.FRESH_REGISTER);
            });

            log(String.format(Locale.US,
                    "Registered → payload=%s…  yaw=%.1f°  dbId=%d",
                    lastPayload.substring(0, Math.min(20, lastPayload.length())), yaw, dbId));

        } else {
            // ── VALIDATE AGAINST DB ──
            QRDatabaseHelper.Registration record =
                    QRDatabaseHelper.getInstance(this).fetchRegistration(lastPayload);
            if (record == null) {
                showStatus("⚠️ No previous registration found.\nPress REGISTER first.");
                setMode(ScanMode.REGISTER);
                return;
            }

            double delta = angleDifference(record.yaw, yaw);
            boolean within = Math.abs(delta) <= DEFAULT_TOLERANCE;

            QRDatabaseHelper.getInstance(this).saveValidation(
                    record.id, lastPayload, yaw, record.yaw, delta, within, DEFAULT_TOLERANCE);

            double finalYaw1 = yaw;
            runOnUiThread(() -> {
                String icon = within ? "✅" : "❌";
                String moved = within ? "NOT moved" : "MOVED";
                showStatus(String.format(Locale.US,
                        "%s QR has %s\nΔYaw = %+.1f°   (±%.0f° tolerance)",
                        icon, moved, delta, DEFAULT_TOLERANCE));
                dbBadge.setText("📝 Validation logged to DB");
                updateInfoLabel(finalYaw1, record, delta, InfoSource.DATABASE);
            });

            log(String.format(Locale.US,
                    "Validate → payload=%s…\n           current=%.1f°\n           registered=%.1f°\n           Δ=%+.1f°\n           yawOK=%b",
                    lastPayload.substring(0, Math.min(20, lastPayload.length())),
                    yaw, record.yaw, delta, within));
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Core: Compute QR World Yaw via ARCore  (mirrors iOS computeQRYawFromTopEdge)
    // ─────────────────────────────────────────────────────────────────────────

    private QRMeasurement computeQRYawFromTopEdge(Frame frame, float[] imagePoints) {
        com.google.ar.core.Camera camera = frame.getCamera();
        if (camera.getTrackingState() != TrackingState.TRACKING) return null;

        CameraIntrinsics intr = camera.getImageIntrinsics();
        float[] focal = intr.getFocalLength();
        float[] pp = intr.getPrincipalPoint();

        float fx = focal[0];
        float fy = focal[1];
        float cx = pp[0];
        float cy = pp[1];

        float[] camPose = new float[16];
        camera.getPose().toMatrix(camPose, 0);

        float[] camOrigin = {
                camPose[12],
                camPose[13],
                camPose[14]
        };

        float[][] imgPts = {
                {imagePoints[0], imagePoints[1]}, // TL
                {imagePoints[2], imagePoints[3]}, // TR
                {imagePoints[4], imagePoints[5]}, // BR
                {imagePoints[6], imagePoints[7]}  // BL
        };

        // ── Build world rays ──
        float[][] rays = new float[4][];
        for (int i = 0; i < 4; i++) {
            float xn = (imgPts[i][0] - cx) / fx;
            float yn = (imgPts[i][1] - cy) / fy;
            float[] camRay = vec3Normalize(new float[]{xn, -yn, -1f});
            rays[i] = vec3Normalize(rotateCamToWorld(camRay, camPose));
        }

        float[] planeNormal = null;
        float[] planePoint = null;
        String method = "angle-solve";

        if (planeNormal == null) {
            planeNormal = computeNormalViaVanishingPoint(
                    imgPts[0], imgPts[1], imgPts[2], imgPts[3],
                    focal, pp, camPose);

            if (planeNormal != null) {
                float[] camFwd = {-camPose[8], -camPose[9], -camPose[10]};
                planePoint = new float[]{
                        camOrigin[0] + camFwd[0],
                        camOrigin[1] + camFwd[1],
                        camOrigin[2] + camFwd[2]
                };
                method = "vanishing-point";
            }
        }

        if (planeNormal == null || planePoint == null) return null;

        // ── Ensure normal faces camera (Points OUT of the wall) ──
        float[] camFwd = {-camPose[8], -camPose[9], -camPose[10]};
        if (vec3Dot(planeNormal, camFwd) > 0) {
            planeNormal = vec3Scale(planeNormal, -1f);
        }

        // ── Ray-plane intersection ──
        float[] tl3D = intersectPlane(camOrigin, rays[0], planePoint, planeNormal);
        float[] bl3D = intersectPlane(camOrigin, rays[3], planePoint, planeNormal);

        if (tl3D == null || bl3D == null) return null;

        // ── Extract yaw ──
        float nx = planeNormal[0];
        float nz = planeNormal[2];
        double nxzLen = Math.hypot(nx, nz);

        double hx, hz;

        if (nxzLen > 0.15) {
            hx = nx;
            hz = nz;
        } else {
            float[] qrUp = {
                    tl3D[0] - bl3D[0],
                    tl3D[1] - bl3D[1],
                    tl3D[2] - bl3D[2]
            };
            qrUp = vec3Normalize(qrUp);
            hx = qrUp[0];
            hz = qrUp[2];
            if (Math.hypot(hx, hz) < 0.15) return null;
        }

        double yaw = Math.toDegrees(Math.atan2(hx, hz));
        if (yaw < 0) yaw += 360;

        double baseConf = method.equals("plane-detected") ? 1.0 :
                method.equals("lidar-depth") ? 0.7 : 0.4;
        double vertConf = nxzLen > 0.15 ? nxzLen : 1.0;
        double confidence = baseConf * vertConf;

        return new QRMeasurement(yaw, confidence, method);
    }

    private float[] intersectPlane(
            float[] camOrigin, float[] rayDir,
            float[] planePoint, float[] planeNormal) {

        float denom = vec3Dot(rayDir, planeNormal);
        if (Math.abs(denom) < 1e-5) return null;

        float[] diff = {
                planePoint[0] - camOrigin[0],
                planePoint[1] - camOrigin[1],
                planePoint[2] - camOrigin[2]
        };

        float t = vec3Dot(diff, planeNormal) / denom;
        if (t <= 0) return null;

        return new float[]{
                camOrigin[0] + rayDir[0] * t,
                camOrigin[1] + rayDir[1] * t,
                camOrigin[2] + rayDir[2] * t
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Vanishing Point Normal Solve
    // ─────────────────────────────────────────────────────────────────────────

    private float[] computeNormalViaVanishingPoint(
            float[] imgTL, float[] imgTR, float[] imgBR, float[] imgBL,
            float[] focalLength, float[] principalPt,
            float[] camTransform) {
        try {
            float fx = focalLength[0];
            float fy = focalLength[1];
            float cx = principalPt[0];
            float cy = principalPt[1];

// Use the perfectly mapped coordinates directly
            float[] hTL = {imgTL[0], imgTL[1], 1f};
            float[] hTR = {imgTR[0], imgTR[1], 1f};
            float[] hBR = {imgBR[0], imgBR[1], 1f};
            float[] hBL = {imgBL[0], imgBL[1], 1f};

            float[] lineTop = vec3Cross(hTL, hTR);
            float[] lineBottom = vec3Cross(hBL, hBR);
            float[] lineLeft = vec3Cross(hTL, hBL);
            float[] lineRight = vec3Cross(hTR, hBR);

            float[] vpX = vec3Cross(lineTop, lineBottom);
            float[] vpY = vec3Cross(lineLeft, lineRight);

            if (Math.abs(vpX[2]) < 1e-6f || Math.abs(vpY[2]) < 1e-6f) {
                Log.d(TAG, "Vanishing points at infinity — QR is square-on, skipping VP solve");
                return null;
            }

            float[] rayX = toCameraRay(vpX, fx, fy, cx, cy);
            float[] rayY = toCameraRay(vpY, fx, fy, cx, cy);

            if (vec3Len(rayX) < 0.001f || vec3Len(rayY) < 0.001f) {
                Log.d(TAG, "Degenerate VP rays");
                return null;
            }

            float[] normalCam = vec3Normalize(vec3Cross(rayX, rayY));

            if (normalCam[2] < 0) normalCam = vec3Scale(normalCam, -1f);

            float[] normalWorld = rotateCamToWorld(normalCam, camTransform);

            Log.d(TAG, String.format(Locale.US,
                    "VanishingPoint: normalCam=(%.3f,%.3f,%.3f) normalWorld=(%.3f,%.3f,%.3f)",
                    normalCam[0], normalCam[1], normalCam[2],
                    normalWorld[0], normalWorld[1], normalWorld[2]));

            return normalWorld;

        } catch (Exception e) {
            Log.e(TAG, "Vanishing point solve failed", e);
            return null;
        }
    }

    private static float[] toCameraRay(float[] v, float fx, float fy, float cx, float cy) {
        float w = v[2];
        float x = (v[0] - w * cx) / fx;
        float y = (v[1] - w * cy) / fy;
        return vec3Normalize(new float[]{x, -y, -w});
    }


    // ─────────────────────────────────────────────────────────────────────────
    // Yaw Math
    // ─────────────────────────────────────────────────────────────────────────

    private double angleDifference(double a, double b) {
        double diff = b - a;
        while (diff > 180) diff -= 360;
        while (diff < -180) diff += 360;
        return diff;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Continuous QR Detection via ML Kit
    // ─────────────────────────────────────────────────────────────────────────

    private boolean isProcessingFrame = false;

    private void processARFrame(Frame frame) {
        if (isProcessingFrame) return;
        isProcessingFrame = true;
        scanQrFromArFrame(frame);
    }

    @OptIn(markerClass = ExperimentalGetImage.class)
    private void scanQrFromArFrame(Frame frame) {
        try {
            long frameTimestamp = frame.getTimestamp();
            android.media.Image cameraImage = frame.acquireCameraImage();

            InputImage inputImage = InputImage.fromMediaImage(cameraImage, 0);

            scanner.process(inputImage)
                    .addOnSuccessListener(barcodes -> {
                        Barcode qr = null;
                        for (Barcode b : barcodes) {
                            if (b.getFormat() == Barcode.FORMAT_QR_CODE
                                    && b.getRawValue() != null) {
                                qr = b;
                                break;
                            }
                        }

                        if (qr == null) {
                            // ML Kit is async and can miss the QR on individual frames
                            // (motion blur, focus shift, partial occlusion). iOS Vision
                            // is synchronous and more frame-consistent, so it can clear
                            // immediately. Here we debounce: only treat the QR as truly
                            // lost after QR_LOST_TIMEOUT_MS of consecutive misses.
                            long now = System.currentTimeMillis();
                            boolean qrTrulyLost = (now - lastQrSeenTime) > QR_LOST_TIMEOUT_MS;

                            if (qrTrulyLost) {
                                runOnUiThread(() -> {
                                    overlayView.setCorners(null);
                                    lastDetectedCorners = null;

                                    if (lastPayload == null) {
                                        showStatus("Scan a QR code to begin");
                                    }

                                    // Cancel sampling if QR lost (mirrors iOS)
                                    if (isSampling) {
                                        isSampling = false;
                                        sampleBuffer.clear();
                                        showStatus("⚠️ QR lost during sampling. Try again.");
                                        dbBadge.setText("");
                                    }
                                });
                            }

                            return;
                        }

                        // ── QR found — update timestamp for debounce ──
                        lastQrSeenTime = System.currentTimeMillis();
                        String payload = qr.getRawValue();

                        android.graphics.Point[] pts = qr.getCornerPoints();
                        if (pts == null || pts.length != 4) return;

                        // These are perfectly native IMAGE_PIXELS
                        float[] imagePoints = new float[]{
                                pts[0].x, pts[0].y, pts[1].x, pts[1].y,
                                pts[2].x, pts[2].y, pts[3].x, pts[3].y
                        };

                        // 2. Ask ARCore to perfectly map the pixels to the screen for the UI overlay
                        float[] viewPoints = new float[8];
                        frame.transformCoordinates2d(
                                com.google.ar.core.Coordinates2d.IMAGE_PIXELS, imagePoints,
                                com.google.ar.core.Coordinates2d.VIEW, viewPoints
                        );

                        List<float[]> cornersList = new ArrayList<>();
                        for (int i = 0; i < 4; i++) {
                            cornersList.add(new float[]{viewPoints[i * 2], viewPoints[i * 2 + 1]});
                        }

                        runOnUiThread(() -> {
                            lastDetectedCorners = cornersList;
                            overlayView.setCorners(cornersList);
                            onQRPayloadDetected(payload);

                            if (isSampling) {
                                Frame currentFrame = arSceneView.getFrame();
                                if (currentFrame != null && currentFrame.getTimestamp() == frameTimestamp) {
                                    // 3. Pass the RAW imagePoints directly to the math!
                                    collectSample(currentFrame, imagePoints);
                                }
                            }
                        });
                    })
                    .addOnFailureListener(e ->
                            Log.w(TAG, "ML Kit barcode scan failed", e))
                    .addOnCompleteListener(task -> {
                        cameraImage.close();
                        isProcessingFrame = false;
                    });

        } catch (Exception e) {
            isProcessingFrame = false;
            Log.w(TAG, "scanQrFromArFrame error: " + e.getMessage());
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // UI Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private enum InfoSource {DATABASE, FRESH_REGISTER}

    private void updateInfoLabel(Double currentYaw,
                                 QRDatabaseHelper.Registration reg,
                                 Double delta,
                                 InfoSource source) {
        List<String> lines = new ArrayList<>();
        if (lastPayload != null)
            lines.add("QR      : " + lastPayload.substring(0, Math.min(38, lastPayload.length())));
        if (reg != null) {
            lines.add(String.format(Locale.US, "Reg Yaw : %.1f°", reg.yaw));
            lines.add("DB id   : " + reg.id);
            lines.add("Source  : " + (source == InfoSource.DATABASE ? "📦 DB" : "💾 New")
                    + "  (" + relativeTime(reg.registeredAt) + ")");
        }
        if (currentYaw != null)
            lines.add(String.format(Locale.US, "Cur Yaw : %.1f°", currentYaw));
        if (delta != null) {
            lines.add(String.format(Locale.US,
                    "Δ Delta : %+.1f°  (tol ±%.0f°)", delta, DEFAULT_TOLERANCE));
            lines.add("Result  : " + (Math.abs(delta) <= DEFAULT_TOLERANCE
                    ? "✅ SAME POSITION" : "❌ MOVED"));
        }
        String text = String.join("\n", lines);
        runOnUiThread(() -> infoText.setText(text));
    }

    private void showStatus(String text) {
        runOnUiThread(() -> statusText.setText(text));
    }

    private void log(String msg) {
        Log.d(TAG, "[QRYaw] " + msg);
    }

    private String relativeTime(Date date) {
        long diff = System.currentTimeMillis() - date.getTime();
        if (diff < 60_000) return "just now";
        if (diff < 3_600_000) return (diff / 60_000) + "m ago";
        if (diff < 86_400_000) return (diff / 3_600_000) + "h ago";
        return (diff / 86_400_000) + "d ago";
    }

    /**
     * Mirrors iOS rec.appVersion shown in history dialog
     */
    private String getAppVersion() {
        try {
            return getPackageManager().getPackageInfo(getPackageName(), 0).versionName;
        } catch (Exception e) {
            return "unknown";
        }
    }

    private double getARCameraYawDegrees(Frame frame) {
        float[] m = new float[16];
        frame.getCamera().getPose().toMatrix(m, 0);

        float fx = -m[8];
        float fz = -m[10];

        double yaw = Math.toDegrees(Math.atan2(fx, fz));
        if (yaw < 0) yaw += 360;

        return yaw;
    }
}