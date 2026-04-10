package com.example.qryaw;

import android.Manifest;
import android.annotation.SuppressLint;
import android.content.pm.PackageManager;
import android.hardware.GeomagneticField;
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
import com.google.android.gms.location.DeviceOrientation;
import com.google.android.gms.location.DeviceOrientationListener;
import com.google.android.gms.location.DeviceOrientationRequest;
import com.google.android.gms.location.FusedLocationProviderClient;
import com.google.android.gms.location.FusedOrientationProviderClient;
import com.google.android.gms.location.LocationServices;
import com.google.android.gms.location.Priority;
import com.google.ar.core.ArCoreApk;
import com.google.ar.core.CameraIntrinsics;
import com.google.ar.core.Config;
import com.google.ar.core.Frame;
import com.google.ar.core.TrackingState;
import com.king.wechat.qrcode.WeChatQRCodeDetector;

import org.opencv.OpenCV;
import org.opencv.calib3d.Calib3d;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfDouble;
import org.opencv.core.MatOfPoint2f;
import org.opencv.core.MatOfPoint3f;
import org.opencv.core.Point;
import org.opencv.core.Point3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.github.sceneview.ar.ARSceneView;
import kotlin.Unit;

public class MainActivity extends AppCompatActivity {

    private static final String TAG = "QRYaws";

    // ── Configuration Constants ──
    private static final int REQUIRED_SAMPLES = 8;
    private static final double MIN_SAMPLE_WEIGHT = 0.15;
    private static final double DEFAULT_TOLERANCE = 15.0;
    private static final float HEADING_ERROR_GATE = 90.0f;
    private static final int MAG_SAMPLES_REQUIRED = 15;
    private static final long MAX_SAMPLE_GAP_MS = 400;
    private static final long QR_LOST_TIMEOUT_MS = 500;

    // ── UI Components ──
    private ARSceneView arSceneView;
    private QROverlayView overlayView;
    private TextView statusText, infoText, modeText, dbBadge;
    private Button btnRegister;
    private Button btnValidate;

    // ── Runtime State ──
    private String magSamplesDisplay = null;
    private final List<Double> magValidationBuffer = new ArrayList<>();
    private String lastMeasurementMethod = null;
    private boolean isCollectingMag = false;
    private double samplingOffsetAnchor = Double.NaN;
    private double lastRawMagHeading = Double.NaN;
    private float[] lastFopAttitude = null;
    private String lastPayload;
    private QRDatabaseHelper.Registration currentRegistration;
    private boolean isSampling = false;
    private SamplingPurpose purpose = SamplingPurpose.REGISTER;
    private final List<YawSample> sampleBuffer = new ArrayList<>();
    private long lastSampleTime = 0;
    private List<float[]> lastDetectedCorners;
    private long lastQrSeenTime = 0;
    private boolean isProcessingFrame = false;

    private final ExecutorService qrExecutor = Executors.newSingleThreadExecutor();

    // ── Services ──
    private FusedOrientationProviderClient fusedOrientationClient;
    private FusedLocationProviderClient locationClient;

    private enum SamplingPurpose {REGISTER, VALIDATE}

    private enum ScanMode {REGISTER, VALIDATE}

    private enum InfoSource {DATABASE, FRESH_REGISTER}

    // ── Inner Data Classes ──
    private static class YawSample {
        final double yaw;
        final double northYaw;
        final double weight;

        YawSample(double yaw, double northYaw, double weight) {
            this.yaw = yaw;
            this.northYaw = northYaw;
            this.weight = weight;
        }
    }

    private static class QRMeasurement {
        final double yaw;
        final double pitch;
        final double confidence;
        final String method;

        QRMeasurement(double yaw, double pitch, double confidence, String method) {
            this.yaw = yaw;
            this.pitch = pitch;
            this.confidence = confidence;
            this.method = method;
        }
    }

    private void flushSensors() {
        log("Manual Recalibration Triggered: Flushing sensors.");

        stopFOP();

        resetAlignment();
        lastPayload = null;
        currentRegistration = null;
        isSampling = false;
        isCollectingMag = false;
        sampleBuffer.clear();
        lastFopAttitude = null;
        magValidationBuffer.clear();
        magSamplesDisplay = null;
        lastMeasurementMethod = null;
        lastSampleTime = 0;
        lastDetectedCorners = null;
        lastQrSeenTime = 0;
        isProcessingFrame = false;

        android.view.ViewGroup parent = (android.view.ViewGroup) arSceneView.getParent();
        int index = parent.indexOfChild(arSceneView);
        android.view.ViewGroup.LayoutParams params = arSceneView.getLayoutParams();

        arSceneView.destroy();
        parent.removeView(arSceneView);

        arSceneView = new ARSceneView(this);
        parent.addView(arSceneView, index, params);

        arSceneView.setSessionConfiguration((session, config) -> {
            config.setPlaneFindingMode(Config.PlaneFindingMode.HORIZONTAL_AND_VERTICAL);
            return null;
        });

        runOnUiThread(() -> {
            overlayView.setCorners(null);
            statusText.setText("♻️ Full Reset.\nPlease hold still to re-align.");
            dbBadge.setText("Recalibrated ✓");
            infoText.setText("");
            modeText.setText("MODE: WAITING");
            btnRegister.setEnabled(false);
            btnValidate.setEnabled(false);
            btnRegister.setAlpha(0.5f);
            btnValidate.setAlpha(0.5f);
        });

        arSceneView.postDelayed(() -> {
            checkArCoreAndStart();
            startFOP();
            log("Fresh session + FOP started.");
        }, 500);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Device Orientation Listener
    // ─────────────────────────────────────────────────────────────────────────
    private final DeviceOrientationListener fopListener = new DeviceOrientationListener() {
        @SuppressLint("SetTextI18n")
        @Override
        public void onDeviceOrientationChanged(DeviceOrientation orientation) {
            float headingError = orientation.getHeadingErrorDegrees();

            // Always store the attitude for floor direct path
            float[] attitude = orientation.getAttitude();
            if (attitude.length >= 4) {
                lastFopAttitude = attitude.clone();
            }

            if (headingError > HEADING_ERROR_GATE) {
                if (isCollectingMag) {
                    runOnUiThread(() -> showStatus("⚠️ Compass interference.\nPlease move phone in a figure-8."));
                }
                return;
            }

            runOnUiThread(() -> {
                lastRawMagHeading = getCameraHeadingDegrees(orientation);

                if (isCollectingMag) {
                    Frame frame = arSceneView.getFrame();
                    if (frame == null || frame.getCamera().getTrackingState() != TrackingState.TRACKING)
                        return;

                    // Camera-forward offset (used only for wall QR)
                    double arYaw = getARCameraYawDegrees(frame);
                    double targetOffset = (lastRawMagHeading - arYaw + 360.0) % 360.0;

                    magValidationBuffer.add(targetOffset);

                    dbBadge.setText("🧭 Mag Samples: " + magValidationBuffer.size() + "/" + MAG_SAMPLES_REQUIRED);

                    if (magValidationBuffer.size() >= MAG_SAMPLES_REQUIRED) {
                        isCollectingMag = false;
                        StringBuilder sb = new StringBuilder("\n===10 MAG SAMPLES CAPTURED ===\n");
                        for (int i = 0; i < magValidationBuffer.size(); i++) {
                            sb.append(String.format(Locale.US, "Mag Sample #%02d: %.2f°\n", i + 1, magValidationBuffer.get(i)));
                        }
                        Log.i(TAG, sb.toString());

                        StringBuilder uiSb = new StringBuilder("🧲 10 MAG SAMPLES:\n");
                        for (int i = 0; i < magValidationBuffer.size(); i++) {
                            uiSb.append(String.format(Locale.US, "%5.1f°", magValidationBuffer.get(i)));
                            if (i < magValidationBuffer.size() - 1) uiSb.append(", ");
                            if ((i + 1) % 5 == 0 && i != magValidationBuffer.size() - 1) {
                                uiSb.append("\n");
                            }
                        }
                        magSamplesDisplay = uiSb.toString();

                        List<Double> filteredMagSamples = filterCircularOutliers(magValidationBuffer);
                        samplingOffsetAnchor = circularMeanDegrees(filteredMagSamples);

                        showStatus("📐 North locked. Measuring QR...");
                        dbBadge.setText("🔄 QR Sampling 0/" + REQUIRED_SAMPLES);

                        int rejectedSamples = magValidationBuffer.size() - filteredMagSamples.size();
                        if (rejectedSamples > 0) {
                            log(String.format(Locale.US,
                                    "Filtered %d/%d magnetic outliers before averaging",
                                    rejectedSamples, magValidationBuffer.size()));
                        }
                        log("Averaged " + filteredMagSamples.size() + " mag samples. Final Anchor = " + String.format(Locale.US, "%.2f°", samplingOffsetAnchor));
                    }
                }
            });
        }
    };

    // ─────────────────────────────────────────────────────────────────────────
    // Vector Math Helpers
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

    private static float[] rotateCamToWorld(float[] camVec, float[] m) {
        return new float[]{
                m[0] * camVec[0] + m[4] * camVec[1] + m[8] * camVec[2],
                m[1] * camVec[0] + m[5] * camVec[1] + m[9] * camVec[2],
                m[2] * camVec[0] + m[6] * camVec[1] + m[10] * camVec[2]
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle & Permissions
    // ─────────────────────────────────────────────────────────────────────────
    private final ActivityResultLauncher<String[]> permissionLauncher =
            registerForActivityResult(new ActivityResultContracts.RequestMultiplePermissions(), permissions -> {
                Boolean cameraGranted = permissions.getOrDefault(Manifest.permission.CAMERA, false);
                if (cameraGranted != null && cameraGranted) {
                    checkArCoreAndStart();
                    startFOP();
                } else {
                    Toast.makeText(this, "Camera permission required", Toast.LENGTH_LONG).show();
                    finish();
                }
            });

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        arSceneView = findViewById(R.id.ar_scene_view);
        overlayView = findViewById(R.id.overlay_view);
        statusText = findViewById(R.id.status_text);
        infoText = findViewById(R.id.info_text);
        modeText = findViewById(R.id.mode_text);
        dbBadge = findViewById(R.id.db_badge);
        btnRegister = findViewById(R.id.btn_register);
        btnValidate = findViewById(R.id.btn_validate);
        Button btnRecalibrate = findViewById(R.id.btn_recalibrate);
        btnRecalibrate.setOnClickListener(v -> flushSensors());
        Button btnHistory = findViewById(R.id.btn_history);

        btnRegister.setOnClickListener(v -> registerTapped());
        btnValidate.setOnClickListener(v -> validateTapped());
        btnHistory.setOnClickListener(v -> historyTapped());

        OpenCV.initOpenCV();
        WeChatQRCodeDetector.init(this);

        log("DB path: " + QRDatabaseHelper.getInstance(this).getDatabasePath());

        fusedOrientationClient = LocationServices.getFusedOrientationProviderClient(this);
        locationClient = LocationServices.getFusedLocationProviderClient(this);

        arSceneView.setSessionConfiguration((session, config) -> {
            config.setPlaneFindingMode(Config.PlaneFindingMode.HORIZONTAL_AND_VERTICAL);
            log("ARCore sessionConfiguration: HORIZONTAL_AND_VERTICAL enabled");
            return null;
        });

        boolean hasCamera = ContextCompat.checkSelfPermission(this, Manifest.permission.CAMERA)
                == PackageManager.PERMISSION_GRANTED;

        if (hasCamera) {
            checkArCoreAndStart();
        } else {
            permissionLauncher.launch(new String[]{
                    Manifest.permission.CAMERA,
                    Manifest.permission.ACCESS_FINE_LOCATION,
                    Manifest.permission.ACCESS_COARSE_LOCATION
            });
        }
    }

    @Override
    protected void onResume() {
        super.onResume();
        resetAlignment();
        startFOP();
    }

    @Override
    protected void onPause() {
        super.onPause();
        stopFOP();
    }

    @SuppressLint("MissingPermission")
    private void startFOP() {
        locationClient.getCurrentLocation(Priority.PRIORITY_BALANCED_POWER_ACCURACY, null)
                .addOnSuccessListener(location -> {
                    if (location != null) {
                        GeomagneticField field = new GeomagneticField(
                                (float) location.getLatitude(),
                                (float) location.getLongitude(),
                                (float) location.getAltitude(),
                                System.currentTimeMillis()
                        );
                        float declination = field.getDeclination();
                        log("Location secured. Magnetic Declination is: " + String.format(Locale.US, "%.2f°", declination));
                    } else {
                        log("⚠️ Location is null. FOP may temporarily fall back to Magnetic North.");
                    }
                });

        DeviceOrientationRequest request = new DeviceOrientationRequest.Builder(
                DeviceOrientationRequest.OUTPUT_PERIOD_FAST
        ).build();

        fusedOrientationClient
                .requestOrientationUpdates(request, Executors.newSingleThreadExecutor(), fopListener)
                .addOnSuccessListener(unused -> log("FOP: registration success"))
                .addOnFailureListener(e -> {
                    Log.e(TAG, "FOP: registration failed", e);
                    runOnUiThread(() -> dbBadge.setText("⚠️ FOP unavailable — check Play Services"));
                });
    }

    private void stopFOP() {
        if (fusedOrientationClient != null) {
            fusedOrientationClient.removeOrientationUpdates(fopListener);
        }
    }

    private void resetAlignment() {
        samplingOffsetAnchor = Double.NaN;
        lastRawMagHeading = Double.NaN;
        lastFopAttitude = null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // AR Setup
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

    private void initializeArSession() {
        try {
            arSceneView.getPlaneRenderer().setEnabled(false);
        } catch (Exception e) {
            Log.e(TAG, "AR setup failed", e);
            statusText.setText("AR init failed");
        }
    }

    private void startArSession() {
        resetAlignment();
        btnRegister.setEnabled(false);
        btnRegister.setAlpha(0.5f);
        btnValidate.setEnabled(false);
        btnValidate.setAlpha(0.5f);

        showStatus("⏳ Compass stabilizing...\nPlease hold still for a moment.");
        dbBadge.setText("");

        arSceneView.setOnFrame(frameTime -> {
            Frame frame = arSceneView.getFrame();
            if (frame == null) return Unit.INSTANCE;

            if (frame.getCamera().getTrackingState() == TrackingState.TRACKING) {
                if (isAlignmentReady() && lastPayload != null) {
                    runOnUiThread(() -> setMode(currentRegistration != null ? ScanMode.VALIDATE : ScanMode.REGISTER));
                }
                processARFrame(frame);
            }
            return Unit.INSTANCE;
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QR Payload Detected
    // ─────────────────────────────────────────────────────────────────────────
    @SuppressLint("SetTextI18n")
    private void onQRPayloadDetected(String payload) {
        if (payload.equals(lastPayload)) return;
        lastPayload = payload;
        currentRegistration = null;

        QRDatabaseHelper.Registration record = QRDatabaseHelper.getInstance(this).fetchRegistration(payload);

        if (record != null) {
            currentRegistration = record;
            setMode(ScanMode.VALIDATE);
            dbBadge.setText("📦 Loaded from DB  (registered " + relativeTime(record.registeredAt) + ")");
            showStatus("QR Detected ✓  —  Registration found in DB!\nPress VALIDATE to check orientation.");
            updateInfoLabel(null, null, record, null, InfoSource.DATABASE);
            log("DB hit → payload=" + payload + " yaw=" + record.yaw + "°");
        } else {
            setMode(ScanMode.REGISTER);
            dbBadge.setText("🆕 New QR — not yet registered");
            showStatus("QR Detected ✓\nNo prior registration found.\nPress REGISTER to save yaw.");
            infoText.setText("Payload : " + payload.substring(0, Math.min(42, payload.length()))
                    + "\nStatus  : Not registered");
            log("DB miss → payload=" + payload);
        }

        if (isAlignmentReady()) {
            btnRegister.setEnabled(true);
            btnRegister.setAlpha(1f);
            if (currentRegistration != null) {
                btnValidate.setEnabled(true);
                btnValidate.setAlpha(1f);
            }
        } else {
            btnRegister.setEnabled(false);
            btnValidate.setEnabled(false);
            showStatus("⏳ Waiting for North alignment...");
        }
    }

    private boolean isAlignmentReady() {
        return !Double.isNaN(lastRawMagHeading);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Button Actions
    // ─────────────────────────────────────────────────────────────────────────
    private void registerTapped() {
        if (!isAlignmentReady()) {
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
        if (!isAlignmentReady()) {
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

    private void setMode(ScanMode mode) {
        boolean canClick = isAlignmentReady() && !isSampling;
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
        magValidationBuffer.clear();
        magSamplesDisplay = null;
        lastMeasurementMethod = null;
        purpose = p;
        isSampling = true;
        isCollectingMag = true;
        lastSampleTime = 0;

        String action = (p == SamplingPurpose.REGISTER) ? "Registering" : "Validating";
        showStatus("🧭 " + action + "...\nFinding Magnetic North (0/" + MAG_SAMPLES_REQUIRED + ")");
        dbBadge.setText("🔄 Mag Init...");

        arSceneView.postDelayed(() -> {
            if (!isSampling) return;
            isCollectingMag = true;
            showStatus("🧭 " + action + "...\nFinding Magnetic North (0/" + MAG_SAMPLES_REQUIRED + ")");
            dbBadge.setText("🔄 Mag Init...");
        }, 1500);
    }

    private void collectSampleWithPose(float[] imagePoints, float[] camPose,
                                       CameraIntrinsics intrinsics,
                                       float[] fopAttitude) {
        log("[collectSampleWithPose] Using pre-captured pose. camOrigin=("
                + String.format(Locale.US, "%.3f, %.3f, %.3f", camPose[12], camPose[13], camPose[14]) + ")");
        QRMeasurement result = computeQRYawFromTopEdgeWithPose(imagePoints, camPose, intrinsics, fopAttitude);

        if (result == null) {
            log("[collectSampleWithPose] computeQRYaw returned null — skipping");
            return;
        }

        log(String.format(Locale.US, "[collectSampleWithPose] yaw=%.1f° pitch=%.1f° conf=%.2f method=%s",
                result.yaw, result.pitch, result.confidence, result.method));
        processSample(result);
    }

    private void logSampleBuffer() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n=== QR NORTH SAMPLES [").append(sampleBuffer.size()).append("/").append(REQUIRED_SAMPLES).append("] ===\n");
        sb.append(String.format(Locale.US, "%-10s | %-10s | %-8s | %-8s\n", "ID", "NORTH", "RAW AR", "WEIGHT"));
        sb.append("--------------------------------------------------\n");
        for (int i = 0; i < sampleBuffer.size(); i++) {
            YawSample s = sampleBuffer.get(i);
            sb.append(String.format(Locale.US, "#%-9d | %-8.2f°  | %-8.2f° | %-6.2f\n",
                    i + 1, s.northYaw, s.yaw, s.weight));
        }
        Log.i("SAMPLES", sb.toString());
    }

    private void updateFullSampleListUI() {
        StringBuilder sb = new StringBuilder();
        sb.append("🛰️ SAMPLING NORTH BEARING\n");
        sb.append("──────────────────────────\n");
        for (int i = 0; i < sampleBuffer.size(); i++) {
            YawSample s = sampleBuffer.get(i);
            String indicator = (i == sampleBuffer.size() - 1) ? "▶ " : "  ";
            sb.append(String.format(Locale.US, "%sSample #%d: %.2f°  (w: %.2f)\n",
                    indicator, i + 1, s.northYaw, s.weight));
        }
        int progress = sampleBuffer.size();
        sb.append("\nProgress: [");
        for (int i = 0; i < REQUIRED_SAMPLES; i++) {
            sb.append(i < progress ? "●" : "○");
        }
        sb.append("] ").append(progress).append("/").append(REQUIRED_SAMPLES);
        runOnUiThread(() -> infoText.setText(sb.toString()));
    }

    private void processSample(QRMeasurement result) {
        if (result.confidence < MIN_SAMPLE_WEIGHT) {
            log(String.format(Locale.US,
                    "Sample rejected (confidence %.2f < %.2f)",
                    result.confidence, MIN_SAMPLE_WEIGHT));
            return;
        }

        if (lastMeasurementMethod == null) {
            lastMeasurementMethod = result.method;
            log(String.format(Locale.US, "[processSample] Locked method=%s", result.method));
        }

        boolean isFloor = result.method.equals("floor-solvepnp-direct");

        // For floor: yaw is already north-referenced, so sampleNorth = yaw
        // For wall: sampleNorth is computed but NOT used for outlier check (old behavior)
        double sampleNorth;
        if (isFloor) {
            sampleNorth = result.yaw;
        } else {
            double activeOffset = samplingOffsetAnchor;
            sampleNorth = (result.yaw + activeOffset) % 360.0;
            if (sampleNorth < 0) sampleNorth += 360.0;
        }

        // Outlier check
        if (!sampleBuffer.isEmpty()) {
            // Wall: compare raw AR yaws (old behavior)
            // Floor: compare north-referenced yaws
            double referenceValue = isFloor
                    ? circularWeightedMeanNorth(sampleBuffer)
                    : circularWeightedMeanRaw(sampleBuffer);
            double compareValue = isFloor ? sampleNorth : result.yaw;
            double diff = Math.abs(angleDifference(referenceValue, compareValue));

            if (diff > 45) {
                if (result.confidence >= 1.0 && sampleBuffer.get(0).weight < 0.5) {
                    log("[processSample] 🌟 High-confidence plane found! Discarding old low-confidence buffer.");
                    sampleBuffer.clear();
                } else {
                    log(String.format(Locale.US,
                            "[processSample] OUTLIER REJECTED: value=%.1f° ref=%.1f° diff=%.1f°",
                            compareValue, referenceValue, diff));
                    return;
                }
            } else {
                log(String.format(Locale.US,
                        "[processSample] Sample OK: value=%.1f° ref=%.1f° diff=%.1f°",
                        compareValue, referenceValue, diff));
            }
        }

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

        sampleBuffer.add(new YawSample(result.yaw, sampleNorth, result.confidence));

        log(String.format(Locale.US,
                "  Sample %d: raw=%.1f° north=%.1f° conf=%.2f [%s]",
                sampleBuffer.size(), result.yaw, sampleNorth,
                result.confidence, result.method));

        logSampleBuffer();
        updateFullSampleListUI();

        final String methodCopy = result.method;
        final int countCopy = sampleBuffer.size();
        runOnUiThread(() ->
                dbBadge.setText("🔄 Sampling " + countCopy + "/" + REQUIRED_SAMPLES
                        + " [" + methodCopy + "]"));

        if (sampleBuffer.size() >= REQUIRED_SAMPLES) {
            isSampling = false;

            StringBuilder sb = new StringBuilder("[processSample] ALL SAMPLES: ");
            for (int i = 0; i < sampleBuffer.size(); i++) {
                YawSample s = sampleBuffer.get(i);
                sb.append(String.format(Locale.US,
                        "#%d raw=%.1f° north=%.1f°(w=%.2f) ",
                        i + 1, s.yaw, s.northYaw, s.weight));
            }
            log(sb.toString());

            // Wall: average raw yaws, then pass to commitReading which applies offset
            // Floor: average north yaws, pass directly
            double finalValue;
            if (isFloor) {
                finalValue = circularWeightedMeanNorth(sampleBuffer);
            } else {
                finalValue = circularWeightedMeanRaw(sampleBuffer);
            }

            log(String.format(Locale.US,
                    "Sampling complete. Averaged %s yaw = %.2f° from %d samples",
                    isFloor ? "north" : "raw", finalValue, sampleBuffer.size()));

            sampleBuffer.clear();
            commitReading(finalValue, isFloor);
        }
    }

    private double circularWeightedMean(List<YawSample> samples) {
        double sumSin = 0;
        double sumCos = 0;
        double totalWeight = 0;

        for (YawSample s : samples) {
            double rad = Math.toRadians(s.northYaw);
            sumSin += Math.sin(rad) * s.weight;
            sumCos += Math.cos(rad) * s.weight;
            totalWeight += s.weight;
        }

        if (totalWeight < 1e-6) return 0;

        double mean = Math.toDegrees(Math.atan2(
                sumSin / totalWeight,
                sumCos / totalWeight
        ));

        if (mean < 0) mean += 360.0;
        return mean;
    }

    private double circularMeanDegrees(List<Double> samples) {
        if (samples.isEmpty()) return 0.0;
        double sumSin = 0.0, sumCos = 0.0;
        for (double value : samples) {
            double rad = Math.toRadians(value);
            sumSin += Math.sin(rad);
            sumCos += Math.cos(rad);
        }
        double mean = Math.toDegrees(Math.atan2(sumSin / samples.size(), sumCos / samples.size()));
        if (mean < 0) mean += 360.0;
        return mean;
    }

    private List<Double> filterCircularOutliers(List<Double> samples) {
        if (samples.size() < 5) return new ArrayList<>(samples);

        double center = circularMeanDegrees(samples);
        List<Double> deviations = new ArrayList<>(samples.size());
        for (double sample : samples) {
            deviations.add(Math.abs(angleDifference(center, sample)));
        }
        Collections.sort(deviations);

        double medianDeviation = deviations.get(deviations.size() / 2);
        double threshold = Math.max(3.0, Math.min(20.0, medianDeviation * 3.0 + 2.0));

        List<Double> filtered = new ArrayList<>(samples.size());
        for (double sample : samples) {
            if (Math.abs(angleDifference(center, sample)) <= threshold) {
                filtered.add(sample);
            }
        }

        int minimumKept = Math.max(8, samples.size() / 2);
        if (filtered.size() < minimumKept) {
            log(String.format(Locale.US,
                    "Mag outlier filter kept only %d/%d samples; falling back to all samples",
                    filtered.size(), samples.size()));
            return new ArrayList<>(samples);
        }
        return filtered;
    }

    private void commitReading(double inputYaw, boolean isFloor) {
        if (lastPayload == null) return;

        double offsetUsed;
        double finalNorthYaw;

        if (isFloor) {
            // Floor: inputYaw is already north-referenced
            offsetUsed = 0.0;
            finalNorthYaw = inputYaw;
        } else {
            // Wall: inputYaw is raw AR yaw, apply offset here (old behavior)
            offsetUsed = samplingOffsetAnchor;
            if (!Double.isNaN(offsetUsed)) {
                finalNorthYaw = (inputYaw + offsetUsed) % 360.0;
                if (finalNorthYaw < 0) finalNorthYaw += 360.0;
            } else {
                finalNorthYaw = inputYaw;
            }
        }

        log("COMMIT READING DEBUG:");
        log(" -> Input Yaw: " + inputYaw + "°");
        log(" -> Offset USED: " + offsetUsed + "°");
        log(" -> FINAL COMPASS BEARING: " + finalNorthYaw + "°");

        samplingOffsetAnchor = Double.NaN;

        double yawRad = Math.toRadians(finalNorthYaw);
        float normalX = (float) Math.sin(yawRad);
        float normalZ = (float) Math.cos(yawRad);


        if (purpose == SamplingPurpose.REGISTER) {
            getSharedPreferences("qryaw_offsets", MODE_PRIVATE)
                    .edit()
                    .putFloat("offset_" + lastPayload, (float) offsetUsed)
                    .apply();

            Long dbId = QRDatabaseHelper.getInstance(this).saveRegistration(
                    lastPayload, finalNorthYaw, normalX, 0.0, normalZ, DEFAULT_TOLERANCE);

            if (dbId == null) {
                showStatus("⚠️ Database save failed.");
                return;
            }

            QRDatabaseHelper.Registration reg = new QRDatabaseHelper.Registration();
            reg.id = dbId;
            reg.payload = lastPayload;
            reg.yaw = finalNorthYaw;
            reg.normalX = normalX;
            reg.normalY = 0.0;
            reg.normalZ = normalZ;
            reg.tolerance = DEFAULT_TOLERANCE;
            reg.registeredAt = new Date();
            currentRegistration = reg;

            double finalYaw = finalNorthYaw;
            runOnUiThread(() -> {
                setMode(ScanMode.VALIDATE);
                dbBadge.setText("💾 Saved to DB  (id=" + dbId + ")");
                showStatus(String.format(Locale.US, "✅ Registered & Saved!\nYaw = %.1f°  |  id=%d", finalYaw, dbId));
                updateInfoLabel(finalYaw, offsetUsed, reg, null, InfoSource.FRESH_REGISTER);
            });

            log(String.format(Locale.US, "Registered → payload=%s…  yaw=%.1f°  dbId=%d",
                    lastPayload.substring(0, Math.min(20, lastPayload.length())), finalNorthYaw, dbId));

        } else {
            QRDatabaseHelper.Registration record = QRDatabaseHelper.getInstance(this).fetchRegistration(lastPayload);
            if (record == null) {
                showStatus("⚠️ No previous registration found.\nPress REGISTER first.");
                setMode(ScanMode.REGISTER);
                return;
            }

            float regOffset = getSharedPreferences("qryaw_offsets", MODE_PRIVATE).getFloat("offset_" + lastPayload, Float.NaN);
            if (!Float.isNaN(regOffset)) {
                double offsetDelta = Math.abs(angleDifference(regOffset, offsetUsed));
                boolean offsetDrifted = offsetDelta > 10.0;
                log(String.format(Locale.US, "[VALIDATE] Offset check: regOffset=%.1f° currentOffset=%.1f° delta=%.1f° drifted=%b",
                        regOffset, offsetUsed, offsetDelta, offsetDrifted));
            }

            double delta = angleDifference(record.yaw, finalNorthYaw);
            boolean within = Math.abs(delta) <= DEFAULT_TOLERANCE;

            log(String.format(Locale.US, "[VALIDATE] registeredYaw=%.1f° currentYaw=%.1f° delta=%+.1f° tolerance=±%.0f° result=%s",
                    record.yaw, finalNorthYaw, delta, DEFAULT_TOLERANCE, within ? "PASS" : "FAIL"));

            QRDatabaseHelper.getInstance(this).saveValidation(
                    record.id, lastPayload, finalNorthYaw, record.yaw, delta, within, DEFAULT_TOLERANCE);

            double finalYaw1 = finalNorthYaw;
            runOnUiThread(() -> {
                String icon = within ? "✅" : "❌";
                String moved = within ? "NOT moved" : "MOVED";
                showStatus(String.format(Locale.US, "%s QR has %s\nΔYaw = %+.1f°   (±%.0f° tolerance)",
                        icon, moved, delta, DEFAULT_TOLERANCE));
                dbBadge.setText("📝 Validation logged to DB");
                updateInfoLabel(finalYaw1, offsetUsed, record, delta, InfoSource.DATABASE);
            });

            log(String.format(Locale.US, "Validate → payload=%s…\n           current=%.1f°\n           registered=%.1f°\n           Δ=%+.1f°\n           yawOK=%b",
                    lastPayload.substring(0, Math.min(20, lastPayload.length())),
                    finalNorthYaw, record.yaw, delta, within));
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Core: Compute QR World Yaw via ARCore (Wall + Floor)
    // ─────────────────────────────────────────────────────────────────────────
    private QRMeasurement computeQRYawFromTopEdgeWithPose(float[] imagePoints, float[] camPose,
                                                          CameraIntrinsics intr, float[] fopAttitude) {
        float[] focal = intr.getFocalLength();
        float[] pp = intr.getPrincipalPoint();
        float fx = focal[0], fy = focal[1], cx = pp[0], cy = pp[1];

        // ML Kit quad corners (used for wall VP normal calculation)
        float[][] imgPts = {
                {imagePoints[0], imagePoints[1]},
                {imagePoints[2], imagePoints[3]},
                {imagePoints[4], imagePoints[5]},
                {imagePoints[6], imagePoints[7]}
        };

        String method = "vanishing-point";
        double yaw;
        double confidence = 0.8;

        // 1. Calculate Normal from Vanishing Points
        float[] planeNormal = computeNormalViaVanishingPoint(
                imgPts[0], imgPts[1], imgPts[2], imgPts[3], focal, pp, camPose);

        if (planeNormal == null) {
            // VP failed — check if camera is looking down (floor QR viewed straight-on)
            float camFwdY = -camPose[9];
            if (camFwdY < -0.3f) {
                log(String.format(Locale.US,
                        "[yawCalc] VP null but camera looking down (camFwdY=%.3f) — assuming floor",
                        camFwdY));
                planeNormal = new float[]{0f, 1f, 0f};
            } else {
                log("[yawCalc] Vanishing-point solve failed — no normal available");
                return null;
            }
        }

        // 2. Ensure Normal is facing the camera
        float[] camFwd = {-camPose[8], -camPose[9], -camPose[10]};
        float dotNormalCamFwd = vec3Dot(planeNormal, camFwd);
        if (dotNormalCamFwd < 0) {
            planeNormal = vec3Scale(planeNormal, -1f);
            log(String.format(Locale.US, "[yawCalc] Normal flipped (dot=%.3f) → now=(%.3f,%.3f,%.3f)",
                    dotNormalCamFwd, planeNormal[0], planeNormal[1], planeNormal[2]));
        }

        // 3. Pitch: surface tilt (Wall=0°, Floor=+90°, Ceiling=-90°)
        double clampedY = Math.max(-1.0, Math.min(1.0, planeNormal[1]));
        double pitch = Math.toDegrees(Math.asin(clampedY));

        // 4. Yaw: Wall vs Floor split
        double hx = planeNormal[0];
        double hz = planeNormal[2];
        double horizLen = Math.hypot(hx, hz);

        if (horizLen > 0.50) {
            // ─── 🧱 WALL CASE ───
            // Use the face normal projected onto horizontal plane.
            // ML Kit corners are fine here — yaw comes from normal direction, not corner identity.
            yaw = Math.toDegrees(Math.atan2(hx, -hz));
            if (yaw < 0) yaw += 360;
            method = "vp-wall";

        } else {
            // ─── 🪩 FLOOR CASE ───
            // Use ZXing finder pattern centers directly.
            // ZXing structurally identifies TL and BL from the QR's internal bit layout,
            // so these are always the correct physical corners regardless of camera position.
            // This is the Android equivalent of what iOS Vision does automatically.

            QRMeasurement pnpResult = computeFloorYawWithSolvePnP(
                    imagePoints,
                    intr,
                    camPose,
                    fopAttitude
            );
            if (pnpResult != null) {
                return pnpResult;
            }

            log("[yawCalc] Floor QR detected, but solvePnP failed. Dropping frame.");
            return null;
        }

        log(String.format(Locale.US, "[yawCalc] RESULT: rawYaw=%.1f° pitch=%.1f° conf=%.2f method=%s",
                yaw, pitch, confidence, method));

        return new QRMeasurement(yaw, pitch, confidence, method);
    }

    private float[] computeNormalViaVanishingPoint(
            float[] imgTL, float[] imgTR, float[] imgBR, float[] imgBL,
            float[] focalLength, float[] principalPt, float[] camTransform) {
        try {
            float fx = focalLength[0], fy = focalLength[1];
            float cx = principalPt[0], cy = principalPt[1];

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
    // Yaw Math & Corner Helpers
    // ─────────────────────────────────────────────────────────────────────────
    private double angleDifference(double a, double b) {
        double diff = b - a;
        while (diff > 180) diff -= 360;
        while (diff < -180) diff += 360;
        return diff;
    }

    private List<float[]> toCornerList(float[] points) {
        List<float[]> corners = new ArrayList<>(4);
        for (int i = 0; i < 4; i++) {
            corners.add(new float[]{points[i * 2], points[i * 2 + 1]});
        }
        return corners;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Continuous QR Detection via ML Kit + ZXing
    // ─────────────────────────────────────────────────────────────────────────
    private void processARFrame(Frame frame) {
        if (isProcessingFrame) return;
        isProcessingFrame = true;
        scanQrFromArFrame(frame);
    }

    @OptIn(markerClass = ExperimentalGetImage.class)
    private void scanQrFromArFrame(Frame frame) {
        try {
            android.media.Image cameraImage = frame.acquireCameraImage();

            CameraIntrinsics capturedIntrinsics = frame.getCamera().getImageIntrinsics();
            float[] capturedPose = new float[16];
            frame.getCamera().getPose().toMatrix(capturedPose, 0);
            final Frame capturedFrame = frame;

            // 1. Extract Grayscale (Y) plane SAFELY (Strips hardware padding)
            android.media.Image.Plane yPlane = cameraImage.getPlanes()[0];
            java.nio.ByteBuffer yBuffer = yPlane.getBuffer();
            int yRowStride = yPlane.getRowStride();
            int width = cameraImage.getWidth();
            int height = cameraImage.getHeight();

            byte[] yData = new byte[width * height];

            if (yRowStride == width) {
                // Tightly packed memory (No padding)
                yBuffer.get(yData);
            } else {
                // Padded memory - strip it row by row so OpenCV doesn't crash
                byte[] rowBuffer = new byte[yRowStride];
                for (int row = 0; row < height; row++) {
                    yBuffer.position(row * yRowStride);
                    yBuffer.get(rowBuffer, 0, Math.min(yRowStride, yBuffer.remaining()));
                    System.arraycopy(rowBuffer, 0, yData, row * width, width);
                }
            }

            cameraImage.close(); // Close immediately to free ARCore

            // 2. Offload to background thread
            qrExecutor.execute(() -> {
                try {
                    // Initialize Mat with the perfectly sized byte array
                    org.opencv.core.Mat grayMat = new org.opencv.core.Mat(height, width, org.opencv.core.CvType.CV_8UC1);
                    grayMat.put(0, 0, yData);

                    List<org.opencv.core.Mat> points = new ArrayList<>();
                    List<String> results = com.king.wechat.qrcode.WeChatQRCodeDetector.detectAndDecode(grayMat, points);

                    // --- QR LOST LOGIC ---
                    if (results == null || results.isEmpty() || points.isEmpty()) {
                        long now = System.currentTimeMillis();
                        boolean qrTrulyLost = (now - lastQrSeenTime) > QR_LOST_TIMEOUT_MS;
                        if (qrTrulyLost) {
                            runOnUiThread(() -> {
                                overlayView.setCorners(null);
                                lastDetectedCorners = null;
                                if (lastPayload == null) showStatus("Scan a QR code to begin");
                                if (isSampling) {
                                    isSampling = false;
                                    isCollectingMag = false;
                                    sampleBuffer.clear();
                                    magValidationBuffer.clear();
                                    showStatus("⚠️ QR lost during sampling. Try again.");
                                    dbBadge.setText("");
                                }
                            });
                        }

                        // Release C++ memory
                        grayMat.release();
                        return; // Jumps to 'finally' block
                    }

                    // --- QR FOUND LOGIC ---
                    lastQrSeenTime = System.currentTimeMillis();
                    String payload = results.get(0);

                    // Extract Intrinsic Corners
                    org.opencv.core.Mat cornersMat = points.get(0);
                    float[] cornerData = new float[8];
                    cornersMat.get(0, 0, cornerData);

                    // [0,1]=TL | [2,3]=TR | [4,5]=BR | [6,7]=BL
                    float[] imagePoints = new float[]{
                            cornerData[0], cornerData[1],
                            cornerData[2], cornerData[3],
                            cornerData[4], cornerData[5],
                            cornerData[6], cornerData[7]
                    };

                    // Project to AR View for overlay
                    float[] viewPoints = new float[8];
                    try {
                        capturedFrame.transformCoordinates2d(
                                com.google.ar.core.Coordinates2d.IMAGE_PIXELS, imagePoints,
                                com.google.ar.core.Coordinates2d.VIEW, viewPoints
                        );
                    } catch (Exception e) {
                        Log.w(TAG, "transformCoordinates2d failed", e);
                        grayMat.release();
                        cornersMat.release();
                        return; // Jumps to 'finally' block
                    }

                    List<float[]> cornersList = toCornerList(viewPoints);

                    runOnUiThread(() -> {
                        lastDetectedCorners = cornersList;
                        overlayView.setCorners(cornersList);
                        onQRPayloadDetected(payload);

                        if (isSampling && !isCollectingMag) {
                            final float[] capturedAttitude = lastFopAttitude != null ? lastFopAttitude.clone() : null;
                            collectSampleWithPose(imagePoints, capturedPose, capturedIntrinsics, capturedAttitude);
                        }
                    });

                    // Free C++ Memory
                    grayMat.release();
                    for (org.opencv.core.Mat m : points) m.release();

                } catch (Exception e) {
                    // If anything inside the thread crashes, log it here!
                    Log.e(TAG, "FATAL ERROR inside QR Executor thread!", e);
                } finally {
                    // 🚨 GUARANTEES THE APP NEVER DEADLOCKS 🚨
                    isProcessingFrame = false;
                }
            });

        } catch (Exception e) {
            isProcessingFrame = false;
            Log.w(TAG, "scanQrFromArFrame outer error: " + e.getMessage());
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // UI Helpers
    // ─────────────────────────────────────────────────────────────────────────
    private void updateInfoLabel(Double finalYaw, Double currentOffset,
                                 QRDatabaseHelper.Registration reg, Double delta, InfoSource source) {
        List<String> lines = new ArrayList<>();

        if (magSamplesDisplay != null) {
            lines.add(magSamplesDisplay);
            lines.add("──────────────────────────");
        }

        if (lastPayload != null) {
            lines.add("QR      : " + lastPayload.substring(0, Math.min(38, lastPayload.length())));
            lines.add("──────────────────────────");
        }

        if (reg != null) {
            float regOffset = getSharedPreferences("qryaw_offsets", MODE_PRIVATE)
                    .getFloat("offset_" + lastPayload, Float.NaN);

            lines.add("💾 REGISTRATION DATA:");
            lines.add("Source  : " + (source == InfoSource.DATABASE ? "📦 DB" : "🆕 New")
                    + "  (" + relativeTime(reg.registeredAt) + ")");

            if (!Float.isNaN(regOffset)) {
                double rawRegYaw = (reg.yaw - regOffset + 360.0) % 360.0;
                lines.add(String.format(Locale.US, "Math    : [%.1f° raw] + [%.1f° offset]", rawRegYaw, regOffset));
            }
            lines.add(String.format(Locale.US, "Reg Yaw : %.1f°", reg.yaw));
            lines.add("──────────────────────────");
        }

        if (finalYaw != null && null != null && currentOffset != null) {
            lines.add("🧭 CURRENT SCAN DATA:");
            lines.add(String.format(Locale.US, "Math    : [%.1f° raw] + [%.1f° offset]", null, currentOffset));
            lines.add(String.format(Locale.US, "Cur Yaw : %.1f°", finalYaw));
        }

        if (delta != null) {
            lines.add("──────────────────────────");
            lines.add(String.format(Locale.US, "Δ Delta : %+.1f°  (tol ±%.0f°)", delta, DEFAULT_TOLERANCE));
            lines.add("Result  : " + (Math.abs(delta) <= DEFAULT_TOLERANCE ? "✅ SAME POSITION" : "❌ MOVED"));
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
        float fx = -m[8], fz = -m[10];
        double yaw = Math.toDegrees(Math.atan2(fx, -fz));
        if (yaw < 0) yaw += 360;
        return yaw;
    }

    private double getCameraHeadingDegrees(DeviceOrientation orientation) {
        float[] attitude = orientation.getAttitude();
        if (attitude.length < 4) {
            return orientation.getHeadingDegrees();
        }

        float[] rotationMatrix = new float[9];
        SensorManager.getRotationMatrixFromVector(rotationMatrix, attitude);

        float east = -rotationMatrix[2];
        float north = -rotationMatrix[5];

        if (Math.hypot(east, north) < 1e-5) {
            return orientation.getHeadingDegrees();
        }

        double heading = Math.toDegrees(Math.atan2(east, north));
        if (heading < 0) heading += 360.0;
        return heading;
    }

    private QRMeasurement computeFloorYawWithSolvePnP(
            float[] imagePoints,
            CameraIntrinsics intrinsics,
            float[] camPose,
            float[] fopAttitude) {

        try {
            float[] focal = intrinsics.getFocalLength();
            float[] pp = intrinsics.getPrincipalPoint();

            double fx = focal[0];
            double fy = focal[1];
            double cx = pp[0];
            double cy = pp[1];

            Mat cameraMatrix = new Mat(3, 3, CvType.CV_64F);
            cameraMatrix.put(0, 0,
                    fx, 0, cx,
                    0, fy, cy,
                    0, 0, 1);

            MatOfDouble distCoeffs = new MatOfDouble(0, 0, 0, 0, 0);

            double half = (float) 0.1 / 2.0;

            MatOfPoint3f objectPoints = new MatOfPoint3f(
                    new Point3(-half, -half, 0),
                    new Point3( half, -half, 0),
                    new Point3( half,  half, 0),
                    new Point3(-half,  half, 0)
            );

            MatOfPoint2f imagePts = new MatOfPoint2f(
                    new Point(imagePoints[0], imagePoints[1]),
                    new Point(imagePoints[2], imagePoints[3]),
                    new Point(imagePoints[4], imagePoints[5]),
                    new Point(imagePoints[6], imagePoints[7])
            );

            List<Mat> rvecs = new ArrayList<>();
            List<Mat> tvecs = new ArrayList<>();
            Mat reprojErrors = new Mat();

            int numSolutions = Calib3d.solvePnPGeneric(
                    objectPoints, imagePts, cameraMatrix, distCoeffs,
                    rvecs, tvecs, false, Calib3d.SOLVEPNP_IPPE_SQUARE,
                    new Mat(), new Mat(), reprojErrors
            );

            if (numSolutions == 0) {
                log("[solvePnP] Failed — no solutions");
                return null;
            }

            // === DETERMINE WHICH PATH TO USE ===
            boolean useDirect = (fopAttitude != null);

            double bestScore = Double.MAX_VALUE;
            double bestYaw = 0, bestPitch = 0;

            for (int solIdx = 0; solIdx < numSolutions; solIdx++) {
                Mat rotationMatrix = new Mat();
                Calib3d.Rodrigues(rvecs.get(solIdx), rotationMatrix);

                // QR normal (Z column) in OpenCV camera space
                double normalCamX = rotationMatrix.get(0, 2)[0];
                double normalCamY = rotationMatrix.get(1, 2)[0];
                double normalCamZ = rotationMatrix.get(2, 2)[0];

                // QR "up" (-Y column) in OpenCV camera space
                double upCamX = -rotationMatrix.get(0, 1)[0];
                double upCamY = -rotationMatrix.get(1, 1)[0];
                double upCamZ = -rotationMatrix.get(2, 1)[0];

                // Convert to Android camera convention (flip Y and Z)
                float[] upCamAndroid = new float[]{
                        (float) upCamX,
                        (float) -upCamY,
                        (float) -upCamZ
                };
                float[] normalCamAndroid = new float[]{
                        (float) normalCamX,
                        (float) -normalCamY,
                        (float) -normalCamZ
                };

                // Transform to world space using ARCore pose (for disambiguation)
                float[] uWorld = rotateCamToWorld(upCamAndroid, camPose);
                float[] nWorld = rotateCamToWorld(normalCamAndroid, camPose);

                // Force normal to point UP away from floor
                if (nWorld[1] < 0) {
                    uWorld[0] = -uWorld[0];
                    uWorld[1] = -uWorld[1];
                    uWorld[2] = -uWorld[2];
                }

                double solPitch = Math.toDegrees(Math.asin(
                        Math.max(-1.0, Math.min(1.0, nWorld[1]))));

                // Select solution where "Up" is closest to horizontal
                double score = Math.abs(uWorld[1]);

                double solYaw;

                if (useDirect) {
                    // === DIRECT FOP PATH (compass yaw, session-independent) ===
                    // Rotate QR "up" by FOP attitude to get earth frame direction
                    float[] rotMatrix = new float[9];
                    SensorManager.getRotationMatrixFromVector(rotMatrix, fopAttitude);

                    // Apply rotation: upEarth = rotMatrix * upCamAndroid
                    // But we need to handle the normal flip too
                    float[] upCamFinal = upCamAndroid;
                    if (nWorld[1] < 0) {
                        // Normal was flipped, so flip up too (in camera space)
                        upCamFinal = new float[]{-upCamAndroid[0], -upCamAndroid[1], -upCamAndroid[2]};
                    }

                    float upEastRaw = rotMatrix[0] * upCamFinal[0] + rotMatrix[1] * upCamFinal[1] + rotMatrix[2] * upCamFinal[2];
                    float upNorthRaw = rotMatrix[3] * upCamFinal[0] + rotMatrix[4] * upCamFinal[1] + rotMatrix[5] * upCamFinal[2];

                    solYaw = Math.toDegrees(Math.atan2(upEastRaw, upNorthRaw));
                    if (solYaw < 0) solYaw += 360.0;

                } else {
                    // === FALLBACK: ARCore world path (same as before) ===
                    solYaw = Math.toDegrees(Math.atan2(uWorld[0], uWorld[2]));
                    if (solYaw < 0) solYaw += 360.0;
                }

                if (score < bestScore) {
                    bestScore = score;
                    bestYaw = solYaw;
                    bestPitch = solPitch;
                }

                rotationMatrix.release();

                Log.d(TAG, String.format(Locale.US,
                        "[solvePnP] Solution %d: yaw=%.1f° pitch=%.1f° score=%.3f upWorld=(%.3f,%.3f,%.3f)",
                        solIdx, solYaw, solPitch, score, uWorld[0], uWorld[1], uWorld[2]));
            }

            for (Mat r : rvecs) r.release();
            for (Mat t : tvecs) t.release();
            reprojErrors.release();

            // Use different method name so processSample knows to skip offset
            String method = useDirect ? "floor-solvepnp-direct" : "floor-solvepnp";
            return new QRMeasurement(bestYaw, bestPitch, 1.0, method);

        } catch (Exception e) {
            log("[solvePnP] Exception: " + e.getMessage());
            return null;
        }
    }

    // Averages .yaw (raw AR yaw) — used for wall path (matches old code exactly)
    private double circularWeightedMeanRaw(List<YawSample> samples) {
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

    // Averages .northYaw (compass-referenced) — used for floor path
    private double circularWeightedMeanNorth(List<YawSample> samples) {
        double sumSin = 0, sumCos = 0, totalWeight = 0;
        for (YawSample s : samples) {
            double rad = Math.toRadians(s.northYaw);
            sumSin += Math.sin(rad) * s.weight;
            sumCos += Math.cos(rad) * s.weight;
            totalWeight += s.weight;
        }
        if (totalWeight < 1e-6) return 0;
        double mean = Math.toDegrees(Math.atan2(sumSin / totalWeight, sumCos / totalWeight));
        if (mean < 0) mean += 360.0;
        return mean;
    }
}