package com.example.qryaw;

import android.Manifest;
import android.content.pm.PackageManager;
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
import com.google.ar.core.Config;
import com.google.ar.core.Frame;
import com.google.ar.core.HitResult;
import com.google.ar.core.Pose;
import com.google.ar.core.Session;
import com.google.ar.core.TrackingState;
import com.google.ar.core.exceptions.CameraNotAvailableException;
import com.google.ar.core.exceptions.UnavailableArcoreNotInstalledException;
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

    private static final String TAG = "QRYaw";

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
    private Session arSession;
    private QROverlayView overlayView;
    private long lastQrSeenTime = 0;
    private static final long QR_LOST_TIMEOUT_MS = 800;
    private double currentYawNorth = 0.0;

    private TextView statusText, infoText, modeText, dbBadge;
    private Button btnRegister, btnValidate, btnHistory;

    public NorthAlignedYawManager northAlignedYawManager;

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
    /**
     * Last detected screen corners — updated when QR is found, cleared when lost
     */
    private List<float[]> lastDetectedCorners;

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
     * <p>
     * ARCore's getPose().toMatrix() gives a column-major 4×4 matrix where the
     * rotation columns are:
     * col 0 = world-space direction of camera +X (right)
     * col 1 = world-space direction of camera +Y (up)
     * col 2 = world-space direction of camera +Z (INTO the scene, i.e. forward)
     * <p>
     * This is OPPOSITE to ARKit where col 2 = camera BACK (+Z = away from scene).
     * Standard matrix multiply handles this correctly as long as our camera-space
     * vectors use the same convention — +Z forward (into scene) — which is what
     * toCameraRay produces after the Y-flip (no Z-flip needed for ARCore).
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
    private final float[] gravity = new float[3];
    private final float[] geomagnetic = new float[3];
    private final float[] rotationMatrix = new float[9];
    private final float[] orientation = new float[3];

    private double arNorthOffsetDeg = Double.NaN;
    private boolean compassReady = false;

    private final android.hardware.SensorEventListener compassListener =
            new android.hardware.SensorEventListener() {

                @Override
                public void onSensorChanged(android.hardware.SensorEvent event) {

                    if (event.sensor.getType() == android.hardware.Sensor.TYPE_ACCELEROMETER) {
                        System.arraycopy(event.values, 0, gravity, 0, 3);
                    } else if (event.sensor.getType() == android.hardware.Sensor.TYPE_MAGNETIC_FIELD) {
                        System.arraycopy(event.values, 0, geomagnetic, 0, 3);
                    }

                    if (android.hardware.SensorManager.getRotationMatrix(
                            rotationMatrix, null, gravity, geomagnetic)) {
                        SensorManager.remapCoordinateSystem(
                                rotationMatrix,
                                SensorManager.AXIS_X,
                                SensorManager.AXIS_Z,
                                rotationMatrix);
                        android.hardware.SensorManager.getOrientation(rotationMatrix, orientation);

                        double azimuthDeg = Math.toDegrees(orientation[0]);
                        if (azimuthDeg < 0) azimuthDeg += 360.0;

                        long now = System.currentTimeMillis();

                        if (compassStartTime == 0) {
                            compassStartTime = now;
                        }

                        if (!compassReady) {
                            double rad = Math.toRadians(azimuthDeg);
                            compassSin += Math.sin(rad);
                            compassCos += Math.cos(rad);
                            compassSamples++;

                            if (now - compassStartTime > 1500 && compassSamples > 5) {

                                double meanRad = Math.atan2(compassSin / compassSamples,
                                        compassCos / compassSamples);

                                double meanDeg = Math.toDegrees(meanRad);
                                if (meanDeg < 0) meanDeg += 360.0;

                                Frame frame = arSceneView.getFrame();

                                if (frame != null &&
                                        frame.getCamera().getTrackingState() == TrackingState.TRACKING) {

                                    double arYaw = getARCameraYawDegrees(frame);

                                    arNorthOffsetDeg = meanDeg - arYaw;

                                    // normalize
                                    while (arNorthOffsetDeg < 0) arNorthOffsetDeg += 360;
                                    while (arNorthOffsetDeg >= 360) arNorthOffsetDeg -= 360;

                                    compassReady = true;

                                    log("Compass locked at " + meanDeg + "°");
                                    log("AR yaw at lock " + arYaw + "°");
                                    log("North offset " + arNorthOffsetDeg + "°");
                                }

                                log("Compass locked (stable) at " + arNorthOffsetDeg + "°");
                            }
                        }
                    }
                }

                @Override
                public void onAccuracyChanged(android.hardware.Sensor sensor, int accuracy) {
                }
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
        btnHistory = findViewById(R.id.btn_history);
        northAlignedYawManager = new NorthAlignedYawManager(this);
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
        if (arSession != null) {
            try {
                arSession.resume();
            } catch (CameraNotAvailableException e) {
                Toast.makeText(this, "Camera unavailable", Toast.LENGTH_LONG).show();
            }
        }

        sensorManager.registerListener(
                compassListener,
                sensorManager.getDefaultSensor(android.hardware.Sensor.TYPE_ACCELEROMETER),
                android.hardware.SensorManager.SENSOR_DELAY_UI);

        sensorManager.registerListener(
                compassListener,
                sensorManager.getDefaultSensor(android.hardware.Sensor.TYPE_MAGNETIC_FIELD),
                android.hardware.SensorManager.SENSOR_DELAY_UI);
    }

    @Override
    protected void onPause() {
        super.onPause();
        if (arSession != null) arSession.pause();
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
                startArSession();         // mirrors startARSession()
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
            arSession = new Session(this);
            Config config = new Config(arSession);
            config.setUpdateMode(Config.UpdateMode.LATEST_CAMERA_IMAGE);
            // NOTE: ARCore has no setWorldAlignment() API (unlike ARKit's .gravityAndHeading).
            // World-space yaw from ARCore is relative to an arbitrary heading at session start.
            // We compensate in applyNorthOffset() using the device magnetometer, producing
            // absolute compass bearings that match what iOS stores with .gravityAndHeading.
            config.setPlaneFindingMode(Config.PlaneFindingMode.HORIZONTAL_AND_VERTICAL);
            if (arSession.isDepthModeSupported(Config.DepthMode.AUTOMATIC)) {
                config.setDepthMode(Config.DepthMode.AUTOMATIC);
            }
            arSession.configure(config);
            arSceneView.getPlaneRenderer().setEnabled(true);
        } catch (UnavailableArcoreNotInstalledException e) {
            statusText.setText("Please install ARCore");
        } catch (Exception e) {
            Log.e(TAG, "AR session failed", e);
            statusText.setText("AR init failed");
        }
    }

    /**
     * Mirrors iOS startARSession():
     * - Disables Register/Validate buttons for 2 s while compass stabilises.
     * - Shows a "Compass stabilizing…" message then re-enables appropriately.
     */
    private void startArSession() {
        resetCompass();
        // Disable buttons immediately
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

        // Re-enable after 2 s, exactly as iOS does
        arSceneView.postDelayed(() -> {
            dbBadge.setText("✅ Compass ready — North-anchored");
            showStatus("Scan a QR code to begin");

            // Re-enable based on current state (mirror of iOS logic)
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
    // Mirrors iOS onQRPayloadDetected(_:)
    // ─────────────────────────────────────────────────────────────────────────

    private void onQRPayloadDetected(String payload) {
        // Guard: ignore duplicate payloads (same as iOS)
        if (payload.equals(lastPayload)) return;
        lastPayload = payload;
        currentRegistration = null;   // reset just like iOS sets registration = nil

        QRDatabaseHelper.Registration record =
                QRDatabaseHelper.getInstance(this).fetchRegistration(payload);

        if (record != null) {
            // ── FOUND IN DB ──
            currentRegistration = record;

            setMode(ScanMode.VALIDATE);
            dbBadge.setText("📦 Loaded from DB  (registered " + relativeTime(record.registeredAt) + ")");
            showStatus("QR Detected ✓  —  Registration found in DB!\nPress VALIDATE to check orientation.");
            updateInfoLabel(null, record, null, InfoSource.DATABASE);

            log("DB hit → payload=" + payload + " yaw=" + record.yaw + "°");
        } else {
            // ── NOT IN DB ──
            setMode(ScanMode.REGISTER);
            dbBadge.setText("🆕 New QR — not yet registered");
            showStatus("QR Detected ✓\nNo prior registration found.\nPress REGISTER to save yaw.");
            infoText.setText("Payload : " + payload.substring(0, Math.min(42, payload.length()))
                    + "\nStatus  : Not registered");

            log("DB miss → payload=" + payload);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Button Actions  (mirrors iOS @objc registerTapped / validateTapped / historyTapped)
    // ─────────────────────────────────────────────────────────────────────────

    private void registerTapped() {
        if (lastDetectedCorners == null) {
            showStatus("⚠️ No QR code in frame. Point camera at QR code.");
            return;
        }
        if (lastPayload == null) {
            showStatus("⚠️ QR payload not decoded yet.");
            return;
        }
        if (isSampling) return;   // already collecting
        startSampling(SamplingPurpose.REGISTER);
    }

    private void validateTapped() {
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
            msg.append(String.format(Locale.US, "Normal   : (%.3f, %.3f, %.3f)\n", reg.normalX, reg.normalY, reg.normalZ));
            msg.append(String.format(Locale.US, "Tolerance: ±%.0f°\n", reg.tolerance));
            msg.append("Saved    : ").append(relativeTime(reg.registeredAt)).append("\n");
            msg.append("Device   : ").append(android.os.Build.MODEL).append("\n");
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
                    // Reset buttons (mirror iOS)
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
        switch (mode) {
            case REGISTER:
                modeText.setText("MODE: REGISTER");
                btnRegister.setEnabled(true);
                btnRegister.setAlpha(1f);
                btnValidate.setEnabled(false);
                btnValidate.setAlpha(0.5f);
                break;
            case VALIDATE:
                modeText.setText("MODE: VALIDATE");
                btnRegister.setEnabled(true);    // allow re-registration
                btnRegister.setAlpha(0.8f);
                btnValidate.setEnabled(true);
                btnValidate.setAlpha(1f);
                break;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Sampling Engine  (mirrors iOS startSampling / collectSample / circularWeightedMean)
    // ─────────────────────────────────────────────────────────────────────────

    private void startSampling(SamplingPurpose p) {
        sampleBuffer.clear();
        purpose = p;
        isSampling = true;
        String action = (p == SamplingPurpose.REGISTER) ? "Registering" : "Validating";
        showStatus("📐 " + action + "... hold still\n(collecting " + REQUIRED_SAMPLES + " samples)");
        dbBadge.setText("🔄 Sampling 0/" + REQUIRED_SAMPLES + "...");
    }

    /**
     * Called from processARFrame while isSampling == true.
     * Feeds one frame into the buffer; fires commitReading when full.
     * Mirrors iOS collectSample(corners:arFrame:).
     */
//    private void collectSample(Frame frame, List<float[]> corners) {
//        QRMeasurement result = computeQRYawFromTopEdge(frame, corners);
//
//        if (result == null) return;
//
//        // Skip frames where the QR edge has insufficient horizontal content
//        if (result.confidence < MIN_SAMPLE_WEIGHT) {
//            log(String.format(Locale.US,
//                    "Sample rejected (confidence %.2f < %.2f)", result.confidence, MIN_SAMPLE_WEIGHT));
//            return;
//        }
//
//        sampleBuffer.add(new YawSample(result.yaw, result.confidence));
//        log(String.format(Locale.US, "  Sample %d: yaw=%.1f° conf=%.2f [%s]",
//                sampleBuffer.size(), result.yaw, result.confidence, result.method));
//
//        final String methodCopy = result.method;
//        final int countCopy = sampleBuffer.size();
//        runOnUiThread(() ->
//                dbBadge.setText("🔄 Sampling " + countCopy + "/" + REQUIRED_SAMPLES
//                        + " [" + methodCopy + "]"));
//
//        if (sampleBuffer.size() >= REQUIRED_SAMPLES) {
//            isSampling = false;
//            double finalYaw = circularWeightedMean(sampleBuffer);
//            log(String.format(Locale.US,
//                    "Sampling complete. Averaged yaw = %.2f° from %d samples",
//                    finalYaw, sampleBuffer.size()));
//            sampleBuffer.clear();
//            commitReading(finalYaw);
//        }
//    }

    private void collectSample(Frame frame, List<float[]> corners) {

        // 🔥 Use stabilized north-aligned device yaw
        double yawNorth = currentYawNorth;

//        // Optional: reject unstable compass
//        if (northAlignedYawManager.isUnstable()) {
//            log("Sample rejected (compass unstable)");
//            return;
//        }

        sampleBuffer.add(new YawSample(yawNorth, 1.0)); // weight = 1.0

        log(String.format(Locale.US,
                "  Sample %d: yawNorth=%.1f°",
                sampleBuffer.size(), yawNorth));

        final int countCopy = sampleBuffer.size();
        runOnUiThread(() ->
                dbBadge.setText("🔄 Sampling " + countCopy + "/" + REQUIRED_SAMPLES
                        + " [North aligned]"));

        if (sampleBuffer.size() >= REQUIRED_SAMPLES) {

            isSampling = false;

            double finalYaw = circularWeightedMean(sampleBuffer);

            log(String.format(Locale.US,
                    "Sampling complete. Averaged north yaw = %.2f° from %d samples",
                    finalYaw, sampleBuffer.size()));

            sampleBuffer.clear();

            commitReading(finalYaw);
        }
    }

    /**
     * Weighted circular mean — handles 0°/360° wraparound correctly.
     * Mirrors iOS circularWeightedMean(samples:).
     */
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
     * Mirrors iOS commitReading(yaw:arFrame:).
     * <p>
     * NOTE ON COORDINATE FRAME:
     * ARCore's world coordinate system has Y=up (gravity-aligned) but an
     * arbitrary heading.  That is fine — both REGISTER and VALIDATE happen
     * inside the SAME ARCore session, so the yaw values are self-consistent.
     * The QR face normal comparison (delta) is valid even without North-anchoring.
     * We do NOT apply any compass offset here; doing so was the primary cause
     * of false "MOVED" results because the device heading changes between the
     * register tap and the validate tap.
     */
    private void commitReading(double yaw) {
        if (lastPayload == null) return;

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

            // Build in-memory registration (mirrors iOS QRYawRegistration)
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

            runOnUiThread(() -> {
                setMode(ScanMode.VALIDATE);
                dbBadge.setText("💾 Saved to DB  (id=" + dbId + ")");
                showStatus(String.format(Locale.US,
                        "✅ Registered & Saved!\nYaw = %.1f°  |  id=%d", yaw, dbId));
                updateInfoLabel(yaw, reg, null, InfoSource.FRESH_REGISTER);
            });

            log(String.format(Locale.US, "Registered → yaw=%.1f° dbId=%d", yaw, dbId));

        } else {
            // ── VALIDATE AGAINST DB ──
            QRDatabaseHelper.Registration record =
                    QRDatabaseHelper.getInstance(this).fetchRegistration(lastPayload);
            if (record == null) {
                showStatus("⚠️ No registration found. Press REGISTER first.");
                setMode(ScanMode.REGISTER);
                return;
            }

//            double delta = angleDifference(record.yaw, yaw);
            double delta = normalizeDelta(yaw - record.yaw);
            boolean within = Math.abs(delta) <= DEFAULT_TOLERANCE;
            QRDatabaseHelper.getInstance(this).saveValidation(
                    record.id, lastPayload, yaw, record.yaw, delta, within, DEFAULT_TOLERANCE);

            runOnUiThread(() -> {
                String icon = within ? "✅" : "❌";
                String moved = within ? "NOT moved" : "MOVED";
                showStatus(String.format(Locale.US,
                        "%s QR has %s\nΔYaw = %+.1f°   (±%.0f° tolerance)",
                        icon, moved, delta, DEFAULT_TOLERANCE));
                dbBadge.setText("📝 Validation logged to DB");
                updateInfoLabel(yaw, record, delta, InfoSource.DATABASE);
            });

            log(String.format(Locale.US,
                    "Validate → current=%.1f° registered=%.1f° delta=%.1f° within=%b",
                    yaw, record.yaw, delta, within));
        }
    }
    private double normalizeDelta(double delta) {
        delta = (delta + 180) % 360;
        if (delta < 0)
            delta += 360;
        return delta - 180;
    }
    // ─────────────────────────────────────────────────────────────────────────
    // Core: Compute QR World Yaw via ARCore  (mirrors iOS computeQRYawFromTopEdge)
    //
    // ROOT CAUSE OF ALL PREVIOUS FAILURES:
    // Both the normal-vector method and the ray-subtraction method work with
    // angular rays from the camera origin — NOT with actual 3D world positions.
    // When you scan from the side, the rays diverge at different angles and
    // the geometry is completely wrong. There is no mathematical fix for this
    // using rays alone — you need real 3D points.
    //
    // THE CORRECT APPROACH:
    // Use ARCore's hitTest to hit each QR corner against detected world
    // geometry (planes). This gives actual 3D world coordinates for each corner,
    // independent of camera angle.
    //
    // FALLBACK (when hitTest misses):
    // Vanishing Point (Homography) solve — mathematically robust way to find
    // the normal from 2D projection without needing depth.
    //
    // CONFIDENCE:
    // 1.0 = plane-detected, 0.7 = depth-estimate, 0.4 = vanishing-point.
    // Also weighted by how vertical the QR is (nxzLen).
    // ─────────────────────────────────────────────────────────────────────────

    private QRMeasurement computeQRYawFromTopEdge(Frame frame, List<float[]> corners) {
        if (corners == null || corners.size() != 4) return null;

        // ML Kit corners: [TL=0, TR=1, BR=2, BL=3]
        float[] tl = corners.get(0);
        float[] tr = corners.get(1);
        float[] br = corners.get(2);
        float[] bl = corners.get(3);

        com.google.ar.core.Camera arCamera = frame.getCamera();
        if (arCamera.getTrackingState() != TrackingState.TRACKING) return null;

        float viewW = arSceneView.getWidth();
        float viewH = arSceneView.getHeight();
        if (viewW <= 0 || viewH <= 0) return null;

        CameraIntrinsics intrinsics = arCamera.getImageIntrinsics();
        float[] focalLength = intrinsics.getFocalLength();    // [fx, fy]
        float[] principalPt = intrinsics.getPrincipalPoint(); // [cx, cy]
        int[] imageSize = intrinsics.getImageDimensions();// [w, h]

        float[] camTransform = new float[16];
        arCamera.getPose().toMatrix(camTransform, 0);

        // Center of QR in screen pixel coords
        float centerScreenX = (tl[0] + tr[0] + br[0] + bl[0]) / 4f;
        float centerScreenY = (tl[1] + tr[1] + br[1] + bl[1]) / 4f;

        float[] faceNormal = null;
        String method = "vanishing-point";

        // ════════════════════════════════════════════════════════════
        // Priority 1: ARCore Plane Detection  (≡ iOS .existingPlaneInfinite)
        // frame.hitTest takes pixel coords in the display viewport.
        // The hit pose's Y axis is the plane normal (ARCore convention for planes).
        // ════════════════════════════════════════════════════════════
        try {
            List<HitResult> hits = frame.hitTest(centerScreenX, centerScreenY);
            if (!hits.isEmpty()) {
                Pose hitPose = hits.get(0).getHitPose();
                // ARCore plane normal = Y axis of the hit pose
                float[] yAxis = hitPose.getYAxis();
                faceNormal = vec3Normalize(new float[]{yAxis[0], yAxis[1], yAxis[2]});
                method = "plane-detected";
            }
        } catch (Exception e) {
            Log.w(TAG, "hitTest failed: " + e.getMessage());
        }

        // ════════════════════════════════════════════════════════════
        // Priority 2: Scene Depth (ARCore Depth API)  (≡ iOS sceneDepth)
        // Sample depth at QR centre; use camera-forward as initial normal.
        // ════════════════════════════════════════════════════════════
        if (faceNormal == null) {
            try {
                android.media.Image depthImage = frame.acquireRawDepthImage16Bits();
                int dw = depthImage.getWidth();
                int dh = depthImage.getHeight();

                // Convert screen centre to depth-buffer coords with rotation handling
                // (mirrors iOS viewToImagePoint)
                float[] centerImg = viewToImagePoint(centerScreenX, centerScreenY,
                        viewW, viewH, dw, dh);
                int dx = Math.min(Math.max((int) centerImg[0], 0), dw - 1);
                int dy = Math.min(Math.max((int) centerImg[1], 0), dh - 1);

                android.media.Image.Plane depthPlane = depthImage.getPlanes()[0];
                java.nio.ShortBuffer buffer = depthPlane.getBuffer().asShortBuffer();
                int rowStride = depthPlane.getRowStride() / 2; // in shorts
                int offset = dy * rowStride + dx;

                if (offset >= 0 && offset < buffer.capacity()) {
                    float depthM = (buffer.get(offset) & 0xFFFF) / 1000.0f; // mm → m
                    if (depthM > 0.05f && depthM < 10.0f) {
                        // ARCore camera forward = +Z column of pose matrix
                        float[] camFwd = vec3Normalize(new float[]{
                                camTransform[8], camTransform[9], camTransform[10]
                        });
                        faceNormal = camFwd;
                        method = "lidar-depth";
                    }
                }
                depthImage.close();
            } catch (Exception e) {
                Log.d(TAG, "Depth acquisition failed: " + e.getMessage());
            }
        }

        // ════════════════════════════════════════════════════════════
        // Priority 3: Vanishing Point (Homography) Solve
        // Mirrors iOS Priority 3 block exactly.
        // ════════════════════════════════════════════════════════════
        if (faceNormal == null) {
            faceNormal = computeNormalViaVanishingPoint(
                    tl, tr, br, bl, viewW, viewH,
                    focalLength, principalPt, imageSize, camTransform);
            if (faceNormal != null) method = "vanishing-point";
        }

        if (faceNormal == null) {
            log("Could not determine QR plane normal");
            return null;
        }

        // ── Ensure normal faces camera ──
        // ARCore camera forward (into scene) = +Z = column 2 of the pose matrix.
        // (ARKit uses -Z column 2, which is why iOS negates it. ARCore does not.)
        float[] camFwd = vec3Normalize(new float[]{
                camTransform[8], camTransform[9], camTransform[10]
        });
        if (vec3Dot(faceNormal, camFwd) < 0) {
            faceNormal = vec3Scale(faceNormal, -1f);
        }

        // ── Extract yaw: project face normal onto horizontal XZ plane ──
        // yaw = atan2(nx, nz) — clockwise from North (Z axis)
        double nx = faceNormal[0];
        double nz = faceNormal[2];
        double nxzLen = Math.hypot(nx, nz);

        if (nxzLen < 0.15) {
            log("QR is nearly horizontal — yaw undefined");
            return null;
        }

        double nxzNormX = nx / nxzLen;
        double nxzNormZ = nz / nxzLen;

        double yaw = Math.toDegrees(Math.atan2(nxzNormX, nxzNormZ));
        if (yaw < 0) yaw += 360.0;

        if (compassReady && !Double.isNaN(arNorthOffsetDeg)) {
            yaw = (yaw + arNorthOffsetDeg) % 360.0;
            if (yaw < 0) yaw += 360.0;
        }

        // Confidence mirrors iOS: baseConf × nxzLen
        double baseConf = method.equals("plane-detected") ? 1.0
                : method.equals("lidar-depth") ? 0.7
                : 0.4;
        double confidence = baseConf * nxzLen;

        return new QRMeasurement(yaw, confidence, method);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Vanishing Point Normal Solve  (mirrors iOS Priority 3 block)
    // ─────────────────────────────────────────────────────────────────────────

    private float[] computeNormalViaVanishingPoint(
            float[] tl, float[] tr, float[] br, float[] bl,
            float viewW, float viewH,
            float[] focalLength, float[] principalPt, int[] imageSize,
            float[] camTransform) {
        try {
            float fx = focalLength[0];
            float fy = focalLength[1];
            float cx = principalPt[0];
            float cy = principalPt[1];
            int imgW = imageSize[0];
            int imgH = imageSize[1];

            // The corners are in view (screen) pixel space.
            // The intrinsics cx/cy/fx/fy are in SENSOR image pixel space.
            // We must map view coords → sensor image coords so the spaces match.
            // viewToImagePoint handles the orientation rotation (portrait ↔ landscape).
            float[] imgTL = viewToImagePoint(tl[0], tl[1], viewW, viewH, imgW, imgH);
            float[] imgTR = viewToImagePoint(tr[0], tr[1], viewW, viewH, imgW, imgH);
            float[] imgBR = viewToImagePoint(br[0], br[1], viewW, viewH, imgW, imgH);
            float[] imgBL = viewToImagePoint(bl[0], bl[1], viewW, viewH, imgW, imgH);

            // Homogeneous image coordinates (x, y, 1)
            float[] hTL = {imgTL[0], imgTL[1], 1f};
            float[] hTR = {imgTR[0], imgTR[1], 1f};
            float[] hBR = {imgBR[0], imgBR[1], 1f};
            float[] hBL = {imgBL[0], imgBL[1], 1f};

            // Lines through each pair of collinear corners (projective cross product)
            float[] lineTop = crossHomogeneous(hTL, hTR);
            float[] lineBottom = crossHomogeneous(hBL, hBR);
            float[] lineLeft = crossHomogeneous(hTL, hBL);
            float[] lineRight = crossHomogeneous(hTR, hBR);

            // Vanishing points = intersection of the two pairs of parallel edges
            float[] vpX = crossHomogeneous(lineTop, lineBottom); // horizontal VP
            float[] vpY = crossHomogeneous(lineLeft, lineRight);  // vertical VP

            // Reject degenerate cases (VP at infinity = square-on view, use depth/plane instead)
            if (Math.abs(vpX[2]) < 1e-6f || Math.abs(vpY[2]) < 1e-6f) {
                Log.d(TAG, "Vanishing points at infinity — QR is square-on, skipping VP solve");
                return null;
            }

            // Back-project each VP through K^-1 to get a direction ray in camera space.
            // K^-1 * [u, v, 1]^T = [(u-cx)/fx,  (v-cy)/fy,  1]
            // Then flip Y and Z to convert from image convention (+Y down, +Z fwd)
            // to ARCore camera convention (+Y up, +Z back).
            float[] rayX = toCameraRay(vpX, fx, fy, cx, cy);
            float[] rayY = toCameraRay(vpY, fx, fy, cx, cy);

            if (vec3Len(rayX) < 0.001f || vec3Len(rayY) < 0.001f) {
                Log.d(TAG, "Degenerate VP rays");
                return null;
            }

            // Face normal in camera space = cross product of the two edge directions
            float[] normalCam = vec3Normalize(vec3Cross(rayX, rayY));

            // In ARCore camera space +Z is INTO the scene (forward).
            // A normal pointing TOWARD the camera has POSITIVE Z.
            // Ensure it points toward the camera (positive Z).
            if (normalCam[2] < 0) normalCam = vec3Scale(normalCam, -1f);

            // Rotate into world space using camera pose rotation matrix
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

    /**
     * Cross product of two points/lines in homogeneous coordinates
     */
    private float[] crossHomogeneous(float[] a, float[] b) {
        return vec3Cross(a, b);   // identical operation — reuse helper
    }

    /**
     * Convert a homogeneous image point to a camera-space direction ray.
     * <p>
     * Image/intrinsics convention:  +X right,  +Y DOWN,  +Z forward (into scene)
     * ARCore camera convention:     +X right,  +Y UP,    +Z forward (into scene)
     * <p>
     * So we only need to flip Y.  We do NOT flip Z — ARCore's camera +Z already
     * points into the scene, matching image convention.
     * (ARKit is different: its camera +Z points BACK, so iOS flips both Y and Z.)
     */
    private static float[] toCameraRay(float[] v, float fx, float fy, float cx, float cy) {
        float w = v[2];
        float x = (v[0] - w * cx) / fx;
        float y = (v[1] - w * cy) / fy;
        // Image +Y is down, ARCore camera +Y is up → flip Y only
        return vec3Normalize(new float[]{x, -y, w});
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Coordinate Conversion  (mirrors iOS viewToImagePoint)
    // Maps a screen-space point to image-buffer pixel coordinates,
    // accounting for device orientation.
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Convert a view (screen) pixel coordinate to sensor image pixel coordinates,
     * so that the result can be used with ARCore's CameraIntrinsics (which are
     * always in the sensor's native/landscape coordinate space).
     * <p>
     * For a typical rear camera (sensor natural orientation = landscape, mounted
     * so SENSOR_ORIENTATION = 90°):
     * <p>
     * ROTATION_0  (portrait, home-button down):
     * screen-right  (+X) = sensor-down   (+Y)  → px = (1-ny)*imgW, py = nx*imgH
     * ROTATION_90  (landscape, home-button right):
     * screen = sensor exactly             → px = nx*imgW, py = ny*imgH
     * ROTATION_180 (portrait upside-down):
     * → px = ny*imgW, py = (1-nx)*imgH
     * ROTATION_270 (landscape, home-button left):
     * → px = (1-nx)*imgW, py = (1-ny)*imgH
     */
    private float[] viewToImagePoint(float sx, float sy,
                                     float viewW, float viewH,
                                     int imgW, int imgH) {
        float nx = sx / viewW;
        float ny = sy / viewH;
        float px, py;
        int rotation = getWindowManager().getDefaultDisplay().getRotation();
        switch (rotation) {
            case android.view.Surface.ROTATION_90:   // Landscape-right
                px = nx * imgW;
                py = ny * imgH;
                break;
            case android.view.Surface.ROTATION_270:  // Landscape-left
                px = (1f - nx) * imgW;
                py = (1f - ny) * imgH;
                break;
            case android.view.Surface.ROTATION_180:  // Portrait upside-down
                px = ny * imgW;
                py = (1f - nx) * imgH;
                break;
            default:                                  // ROTATION_0 = Portrait (most common)
                px = (1f - ny) * imgW;
                py = nx * imgH;
                break;
        }
        return new float[]{px, py};
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Yaw Math  (mirrors iOS angleDifference)
    // ─────────────────────────────────────────────────────────────────────────

    private double angleDifference(double a, double b) {
        double diff = b - a;
        while (diff > 180) diff -= 360;
        while (diff < -180) diff += 360;
        return diff;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Continuous QR Detection via ML Kit  (mirrors iOS processARFrame + detectQRCode)
    // ─────────────────────────────────────────────────────────────────────────

    private boolean isProcessingFrame = false;   // mirrors iOS isProcessingFrame

    private void processARFrame(Frame frame) {
        if (isProcessingFrame) return;
        if (northAlignedYawManager == null) return;
        isProcessingFrame = true;

        currentYawNorth = northAlignedYawManager.getNorthAlignedYaw(frame);
        scanQrFromArFrame(frame);
    }

    @OptIn(markerClass = ExperimentalGetImage.class)
    private void scanQrFromArFrame(Frame frame) {
        try {
            android.media.Image cameraImage = frame.acquireCameraImage();
            int rotDeg = getSensorToDisplayRotation();   // correct sensor→display rotation

            InputImage inputImage = InputImage.fromMediaImage(cameraImage, rotDeg);

            scanner.process(inputImage)
                    .addOnSuccessListener(barcodes -> {
                        // Mirror iOS: find the first QR barcode
                        lastQrSeenTime = System.currentTimeMillis();
                        Barcode qr = null;
                        for (Barcode b : barcodes) {
                            if (b.getFormat() == Barcode.FORMAT_QR_CODE
                                    && b.getRawValue() != null) {
                                qr = b;
                                break;
                            }
                        }

                        if (qr == null) {

                            long now = System.currentTimeMillis();
                            boolean qrRecentlySeen = (now - lastQrSeenTime) < QR_LOST_TIMEOUT_MS;

                            runOnUiThread(() -> {
                                if (!qrRecentlySeen) {
                                    overlayView.setCorners(null);
                                }

                                if (!qrRecentlySeen) {
                                    lastDetectedCorners = null;

                                    if (lastPayload == null) {
                                        showStatus("Scan a QR code to begin");
                                    }

                                    // Cancel only if really gone
                                    if (isSampling) {
                                        isSampling = false;
                                        sampleBuffer.clear();
                                        showStatus("⚠️ QR lost during sampling. Try again.");
                                        dbBadge.setText("");
                                    }
                                }
                            });

                            return;
                        }

                        // ── QR found — convert ML Kit pixel coords → view coords ──
                        String payload = qr.getRawValue();
                        List<float[]> cornersList = extractScreenCorners(qr, cameraImage, rotDeg);

                        runOnUiThread(() -> {
                            lastDetectedCorners = cornersList;
                            overlayView.setCorners(cornersList);

                            // Enable Register button when corners present (mirrors iOS)
                            btnRegister.setEnabled(true);
                            btnRegister.setAlpha(1f);

                            onQRPayloadDetected(payload);

                            // Feed sample if actively collecting (mirrors iOS)
                            if (isSampling) {
                                List<float[]> cornersForSampling =
                                        (cornersList != null) ? cornersList : lastDetectedCorners;

                                if (cornersForSampling != null) {
                                    collectSample(frame, cornersForSampling);
                                }
                            }
                        });
                    })
                    .addOnFailureListener(e ->
                            Log.w(TAG, "ML Kit barcode scan failed", e))
                    .addOnCompleteListener(task -> {
                        cameraImage.close();
                        isProcessingFrame = false;   // mirrors iOS defer { isProcessingFrame = false }
                    });

        } catch (Exception e) {
            isProcessingFrame = false;
            Log.w(TAG, "scanQrFromArFrame error: " + e.getMessage());
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper: extract SCREEN-space corners from ML Kit barcode
    // Corner order: [TL, TR, BR, BL]  (same as iOS Vision)
    //
    // ML Kit returns corners in the coordinate space of the InputImage that was
    // passed to it.  InputImage.fromMediaImage(image, rotationDegrees) tells ML
    // Kit to rotate the image before analysis, so its corners come back in
    // *screen-upright* pixel coordinates — (0,0) = top-left of the screen,
    // dimensions = the image size AFTER the rotation is applied:
    //   rotDeg 90 or 270 → analysisW = sensorH, analysisH = sensorW
    //   rotDeg 0  or 180 → analysisW = sensorW, analysisH = sensorH
    //
    // We normalise by those analysis dimensions then scale to view pixels so
    // the overlay draws exactly on the QR code and hitTest gets the right point.
    // ─────────────────────────────────────────────────────────────────────────

    private List<float[]> extractScreenCorners(Barcode barcode,
                                               android.media.Image cameraImage,
                                               int rotationDeg) {
        int sensorW = cameraImage.getWidth();
        int sensorH = cameraImage.getHeight();
        int analysisW = (rotationDeg == 90 || rotationDeg == 270) ? sensorH : sensorW;
        int analysisH = (rotationDeg == 90 || rotationDeg == 270) ? sensorW : sensorH;

        float viewW = arSceneView.getWidth();
        float viewH = arSceneView.getHeight();

        android.graphics.Point[] pts = barcode.getCornerPoints();
        if (pts == null || pts.length != 4) {
            android.graphics.Rect rect = barcode.getBoundingBox();
            if (rect == null) return null;
            pts = new android.graphics.Point[]{
                    new android.graphics.Point(rect.left, rect.top),
                    new android.graphics.Point(rect.right, rect.top),
                    new android.graphics.Point(rect.right, rect.bottom),
                    new android.graphics.Point(rect.left, rect.bottom)
            };
        }

        List<float[]> list = new ArrayList<>();
        for (android.graphics.Point p : pts) {
            float nx = p.x / (float) analysisW;   // normalise to [0,1]
            float ny = p.y / (float) analysisH;
            list.add(new float[]{nx * viewW, ny * viewH});  // scale to view pixels
        }
        return list;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // UI Helpers  (mirrors iOS updateInfoLabel / showStatus / log / relativeDate)
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
     * Returns the rotation that must be applied to a rear-camera image so that
     * it appears upright on screen — i.e. the value to pass to
     * InputImage.fromMediaImage(image, rotationDegrees).
     * <p>
     * Most Android phones have a rear sensor that is mounted in landscape
     * (its natural axis is 90° from the display's natural axis).  The
     * surface rotation tells us how the display is currently oriented relative
     * to its own natural (portrait) position.  We combine both to get the
     * angle needed to make the image upright.
     * <p>
     * Typical rear-camera sensor orientation = 90° (stored in
     * CameraCharacteristics.SENSOR_ORIENTATION).  Rather than querying
     * Camera2 every frame we use the well-known default and handle the
     * display rotation on top of it.
     * <p>
     * Formula:  (sensorOrientation - displayRotationDeg + 360) % 360
     * For sensor = 90°:
     * ROTATION_0   (portrait)              → (90 -   0 + 360) % 360 = 90
     * ROTATION_90  (landscape, home right) → (90 -  90 + 360) % 360 = 0
     * ROTATION_270 (landscape, home left)  → (90 - 270 + 360) % 360 = 180
     * ROTATION_180 (portrait upside-down)  → (90 - 180 + 360) % 360 = 270
     */
    private int getSensorToDisplayRotation() {
        // Query the actual sensor orientation via Camera2 so we handle
        // devices where the sensor is mounted at 270° (front camera, tablets).
        int sensorOrientation = 90; // safe default for rear camera on phones
        try {
            android.hardware.camera2.CameraManager cm =
                    (android.hardware.camera2.CameraManager) getSystemService(CAMERA_SERVICE);
            for (String id : cm.getCameraIdList()) {
                android.hardware.camera2.CameraCharacteristics chars =
                        cm.getCameraCharacteristics(id);
                Integer facing = chars.get(
                        android.hardware.camera2.CameraCharacteristics.LENS_FACING);
                if (facing != null &&
                        facing == android.hardware.camera2.CameraCharacteristics.LENS_FACING_BACK) {
                    Integer so = chars.get(
                            android.hardware.camera2.CameraCharacteristics.SENSOR_ORIENTATION);
                    if (so != null) sensorOrientation = so;
                    break;
                }
            }
        } catch (Exception e) {
            Log.w(TAG, "Could not query sensor orientation, using default 90°");
        }

        int displayRotationDeg;
        switch (getWindowManager().getDefaultDisplay().getRotation()) {
            case android.view.Surface.ROTATION_90:
                displayRotationDeg = 90;
                break;
            case android.view.Surface.ROTATION_180:
                displayRotationDeg = 180;
                break;
            case android.view.Surface.ROTATION_270:
                displayRotationDeg = 270;
                break;
            default:
                displayRotationDeg = 0;
                break;
        }
        return (sensorOrientation - displayRotationDeg + 360) % 360;
    }

    private double getARCameraYawDegrees(Frame frame) {

        float[] m = new float[16];
        frame.getCamera().getPose().toMatrix(m, 0);

        // ARCore forward vector (+Z)
        float fx = m[8];
        float fz = m[10];

        double yaw = Math.toDegrees(Math.atan2(fx, fz));
        if (yaw < 0) yaw += 360.0;

        return yaw;
    }
}