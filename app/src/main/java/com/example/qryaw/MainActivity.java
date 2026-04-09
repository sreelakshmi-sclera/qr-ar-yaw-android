package com.example.qryaw;

import android.Manifest;
import android.annotation.SuppressLint;
import android.content.pm.PackageManager;
import android.hardware.GeomagneticField;
import android.hardware.SensorManager;
import android.hardware.camera2.CameraCharacteristics;
import android.hardware.camera2.CameraManager;
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
import com.google.mlkit.vision.barcode.BarcodeScanner;
import com.google.mlkit.vision.barcode.BarcodeScannerOptions;
import com.google.mlkit.vision.barcode.BarcodeScanning;
import com.google.mlkit.vision.barcode.common.Barcode;
import com.google.mlkit.vision.common.InputImage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;

import io.github.sceneview.ar.ARSceneView;
import kotlin.Unit;

public class MainActivity extends AppCompatActivity {

    private static final String TAG = "QRYaws";

    // ── Configuration Constants ──
    private static final int REQUIRED_SAMPLES = 8;
    private static final double MIN_SAMPLE_WEIGHT = 0.15;
    private static final double DEFAULT_TOLERANCE = 15.0;
    private static final double PITCH_TOLERANCE = 20.0;
    private static final float HEADING_ERROR_GATE = 90.0f;
    private static final int MAG_SAMPLES_REQUIRED = 40;
    private static final long MAX_SAMPLE_GAP_MS = 400;
    private static final long QR_LOST_TIMEOUT_MS = 500;

    // ── UI Components ──
    private ARSceneView arSceneView;
    private QROverlayView overlayView;
    private TextView statusText, infoText, modeText, dbBadge;
    private Button btnRecalibrate;
    private Button btnRegister;
    private Button btnValidate;

    // ── Runtime State ──
    private String magSamplesDisplay = null;
    private final List<Double> magValidationBuffer = new ArrayList<>();
    private boolean isCollectingMag = false;
    private double samplingOffsetAnchor = Double.NaN;
    private double lastRawMagHeading = Double.NaN;
    private float lastHeadingErrorDegrees = Float.NaN;
    private String lastPayload;
    private QRDatabaseHelper.Registration currentRegistration;
    private boolean isSampling = false;
    private SamplingPurpose purpose = SamplingPurpose.REGISTER;
    private final List<YawSample> sampleBuffer = new ArrayList<>();
    private long lastSampleTime = 0;
    private List<float[]> lastDetectedCorners;
    private float[] lastAcceptedImagePoints;
    private float[] lastAcceptedViewPoints;
    private long lastQrSeenTime = 0;
    private boolean isProcessingFrame = false;

    // ── Attitude (ENU) for floor QR ──
    private final float[] lastAttitudeRotMat = new float[9]; // device→ENU, row-major
    private boolean attitudeValid = false;
    private int sensorOrientation = 90; // default, read at startup

    // ── Services ──
    private FusedOrientationProviderClient fusedOrientationClient;
    private FusedLocationProviderClient locationClient;
    private final BarcodeScanner scanner = BarcodeScanning.getClient(
            new BarcodeScannerOptions.Builder()
                    .setBarcodeFormats(Barcode.FORMAT_QR_CODE)
                    .build()
    );

    private enum SamplingPurpose {REGISTER, VALIDATE}

    private enum ScanMode {REGISTER, VALIDATE}

    private enum InfoSource {DATABASE, FRESH_REGISTER}

    // ── Inner Data Classes ──
    private static class YawSample {
        final double yaw;
        final double pitch;    // degrees: 0=wall, +90=floor
        final double northYaw;
        final double weight;

        YawSample(double yaw, double pitch, double northYaw, double weight) {
            this.yaw = yaw;
            this.pitch = pitch;
            this.northYaw = northYaw;
            this.weight = weight;
        }
    }

    private static class QRMeasurement {
        final double yaw;
        final double pitch;    // degrees: 0=wall, +90=floor
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

        // 1. Stop FOP
        stopFOP();

        // 2. Reset ALL app state
        resetAlignment();
        lastPayload = null;
        currentRegistration = null;
        isSampling = false;
        isCollectingMag = false;
        sampleBuffer.clear();
        magValidationBuffer.clear();
        magSamplesDisplay = null;
        lastSampleTime = 0;
        lastDetectedCorners = null;
        resetTrackedQrCorners();
        lastQrSeenTime = 0;
        isProcessingFrame = false;

        // 3. Destroy and recreate ARSceneView for a fresh session
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

        // 4. Update UI
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

        // 5. Restart with delay to let camera release
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
        @Override
        public void onDeviceOrientationChanged(DeviceOrientation orientation) {
            float headingError = orientation.getHeadingErrorDegrees();
            lastHeadingErrorDegrees = headingError;

            if (headingError == 180.0f || headingError > HEADING_ERROR_GATE) {
                if (isCollectingMag) {
                    runOnUiThread(() -> showStatus("⚠️ Compass interference.\nPlease move phone in a figure-8."));
                }
                return;
            }

            // Store full attitude rotation matrix for floor QR ENU projection
            float[] attitude = orientation.getAttitude();
            if (attitude != null && attitude.length >= 4) {
                SensorManager.getRotationMatrixFromVector(lastAttitudeRotMat, attitude);
                attitudeValid = true;
            }

            runOnUiThread(() -> {
                double rawHeading = getCameraHeadingDegrees(orientation);
                lastRawMagHeading = rawHeading;

                // Collect 40 samples strictly ON DEMAND
                if (isCollectingMag) {
                    Frame frame = arSceneView.getFrame();
                    if (frame == null || frame.getCamera().getTrackingState() != TrackingState.TRACKING)
                        return;

                    double arYaw = getARCameraYawDegrees(frame);
                    double targetOffset = (lastRawMagHeading - arYaw + 360.0) % 360.0;

                    magValidationBuffer.add(targetOffset);

                    dbBadge.setText("🧭 Mag Samples: " + magValidationBuffer.size() + "/" + MAG_SAMPLES_REQUIRED);

                    if (magValidationBuffer.size() >= MAG_SAMPLES_REQUIRED) {
                        isCollectingMag = false;
                        StringBuilder sb = new StringBuilder("\n=== 40 MAG SAMPLES CAPTURED ===\n");
                        for (int i = 0; i < magValidationBuffer.size(); i++) {
                            sb.append(String.format(Locale.US, "Mag Sample #%02d: %.2f°\n", i + 1, magValidationBuffer.get(i)));
                        }
                        Log.i(TAG, sb.toString());

                        StringBuilder uiSb = new StringBuilder("🧲 40 MAG SAMPLES:\n");
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

                        isCollectingMag = false;

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

    /**
     * Transform a camera-space vector to ENU (East-North-Up) world space using
     * the device attitude rotation matrix and sensor orientation.
     * Camera space: X=right-in-image, Y=up-in-image, Z=backward.
     * Result: [East, North, Up].
     */
    private float[] rotateCamToENU(float[] camVec, float[] R, int sensorOri) {
        float cx = camVec[0], cy = camVec[1], cz = camVec[2];
        // Camera → Device rotation based on sensor orientation
        float dx, dy, dz;
        switch (sensorOri) {
            case 90:  dx = -cy; dy = cx;  dz = cz; break;
            case 270: dx = cy;  dy = -cx; dz = cz; break;
            case 180: dx = -cx; dy = -cy; dz = cz; break;
            default:  dx = cx;  dy = cy;  dz = cz; break; // 0°
        }
        // Device → ENU (R is row-major 3×3 from SensorManager.getRotationMatrixFromVector)
        return new float[]{
                R[0] * dx + R[1] * dy + R[2] * dz,  // East
                R[3] * dx + R[4] * dy + R[5] * dz,  // North
                R[6] * dx + R[7] * dy + R[8] * dz   // Up
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
        btnRecalibrate = findViewById(R.id.btn_recalibrate);
        btnRecalibrate.setOnClickListener(v -> flushSensors());
        Button btnHistory = findViewById(R.id.btn_history);

        btnRegister.setOnClickListener(v -> registerTapped());
        btnValidate.setOnClickListener(v -> validateTapped());
        btnHistory.setOnClickListener(v -> historyTapped());

        log("DB path: " + QRDatabaseHelper.getInstance(this).getDatabasePath());

        fusedOrientationClient = LocationServices.getFusedOrientationProviderClient(this);
        locationClient = LocationServices.getFusedLocationProviderClient(this);

        // Read camera sensor orientation for attitude→ENU floor yaw
        try {
            CameraManager cm = (CameraManager) getSystemService(CAMERA_SERVICE);
            String[] ids = cm.getCameraIdList();
            for (String id : ids) {
                Integer facing = cm.getCameraCharacteristics(id)
                        .get(CameraCharacteristics.LENS_FACING);
                if (facing != null && facing == CameraCharacteristics.LENS_FACING_BACK) {
                    Integer ori = cm.getCameraCharacteristics(id)
                            .get(CameraCharacteristics.SENSOR_ORIENTATION);
                    if (ori != null) sensorOrientation = ori;
                    break;
                }
            }
        } catch (Exception e) {
            log("Could not read sensor orientation, defaulting to 90: " + e.getMessage());
        }
        log("Sensor orientation: " + sensorOrientation + "°");

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
        // 1. Wake up the GPS to guarantee FOP has the data it needs for True North
        locationClient.getCurrentLocation(Priority.PRIORITY_BALANCED_POWER_ACCURACY, null)
                .addOnSuccessListener(location -> {
                    if (location != null) {
                        // 2. Explicitly calculate the declination for logging
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

        // 3. Start your existing FOP listener
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
        lastHeadingErrorDegrees = Float.NaN;
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

            // Ensure we are tracking before we allow button interaction
            if (frame.getCamera().getTrackingState() == TrackingState.TRACKING) {

                // Check if compass is ready. If it is, and we have a payload, enable buttons.
                if (isAlignmentReady() && lastPayload != null) {
                    runOnUiThread(() -> {
                        // This forces the buttons to wake up if a QR is still in front of the lens
                        setMode(currentRegistration != null ? ScanMode.VALIDATE : ScanMode.REGISTER);
                    });
                }

                processARFrame(frame);
            }
            return Unit.INSTANCE;
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QR Payload Detected
    // ─────────────────────────────────────────────────────────────────────────
    private void onQRPayloadDetected(String payload) {
        if (payload.equals(lastPayload)) return;
        resetTrackedQrCorners();
        lastPayload = payload;
        currentRegistration = null;

        QRDatabaseHelper.Registration record = QRDatabaseHelper.getInstance(this).fetchRegistration(payload);

        if (record != null) {
            currentRegistration = record;
            setMode(ScanMode.VALIDATE);
            dbBadge.setText("📦 Loaded from DB  (registered " + relativeTime(record.registeredAt) + ")");
            showStatus("QR Detected ✓  —  Registration found in DB!\nPress VALIDATE to check orientation.");
            updateInfoLabel(null, null, null, record, null, null, InfoSource.DATABASE);
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
            msg.append(String.format(Locale.US, "Pitch    : %.1f° (%s)\n", reg.pitchDegrees, pitchLabel(reg.pitchDegrees)));
            msg.append(String.format(Locale.US, "Tolerance: yaw ±%.0f°  pitch ±%.0f°\n", reg.tolerance, PITCH_TOLERANCE));
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
                msg.append(String.format(Locale.US, "%s %s   Δyaw%+.1f° Δpitch%+.1f°\n",
                        icon, relativeTime(v.validatedAt), v.delta, v.pitchDelta));
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
        purpose = p;
        isSampling = true;
        isCollectingMag = true;
        lastSampleTime = 0;

        String action = (p == SamplingPurpose.REGISTER) ? "Registering" : "Validating";
        showStatus("🧭 " + action + "...\nFinding Magnetic North (0/" + MAG_SAMPLES_REQUIRED + ")");
        dbBadge.setText("🔄 Mag Init...");

        arSceneView.postDelayed(() -> {
            if (!isSampling) return; // User might have cancelled
            isCollectingMag = true;
            showStatus("🧭 " + action + "...\nFinding Magnetic North (0/" + MAG_SAMPLES_REQUIRED + ")");
            dbBadge.setText("🔄 Mag Init...");
        }, 1500);
    }

    private void collectSampleWithPose(float[] imagePoints, float[] rawImagePoints,
                                       float[] camPose, CameraIntrinsics intrinsics, Frame frame) {
        log("[collectSampleWithPose] Using pre-captured pose. camOrigin=("
                + String.format(Locale.US, "%.3f, %.3f, %.3f", camPose[12], camPose[13], camPose[14]) + ")");
        QRMeasurement result = computeQRYawFromTopEdgeWithPose(imagePoints, rawImagePoints, camPose, intrinsics);

        if (result == null) {
            log("[collectSampleWithPose] computeQRYaw returned null — skipping");
            return;
        }

        log(String.format(Locale.US, "[collectSampleWithPose] yaw=%.1f° conf=%.2f method=%s",
                result.yaw, result.confidence, result.method));
        processSample(result);
    }

    private void logSampleBuffer() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n=== QR NORTH SAMPLES [").append(sampleBuffer.size()).append("/").append(REQUIRED_SAMPLES).append("] ===\n");
        sb.append(String.format(Locale.US, "%-10s | %-10s | %-8s | %-8s | %-8s\n", "ID", "NORTH", "RAW AR", "PITCH", "WEIGHT"));
        sb.append("--------------------------------------------------------------\n");

        for (int i = 0; i < sampleBuffer.size(); i++) {
            YawSample s = sampleBuffer.get(i);
            sb.append(String.format(Locale.US, "#%-9d | %-8.2f°  | %-8.2f° | %-8.1f° | %-6.2f\n",
                    i + 1, s.northYaw, s.yaw, s.pitch, s.weight));
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
            sb.append(String.format(Locale.US, "%sSample #%d: %.2f° pitch:%.1f° (w: %.2f)\n",
                    indicator, i + 1, s.northYaw, s.pitch, s.weight));
        }

        int progress = sampleBuffer.size();
        sb.append("\nProgress: [");
        for (int i = 0; i < REQUIRED_SAMPLES; i++) {
            sb.append(i < progress ? "●" : "○");
        }
        sb.append("] " + progress + "/" + REQUIRED_SAMPLES);

        runOnUiThread(() -> infoText.setText(sb.toString()));
    }

    private void processSample(QRMeasurement result) {
        if (result.confidence < MIN_SAMPLE_WEIGHT) {
            log(String.format(Locale.US, "Sample rejected (confidence %.2f < %.2f)", result.confidence, MIN_SAMPLE_WEIGHT));
            return;
        }

        // Attitude-based floor yaw is already in ENU (global) space — skip magnetic offset
        double sampleNorth;
        if ("attitude-VP".equals(result.method)) {
            sampleNorth = result.yaw; // Already global ENU-based
        } else {
            sampleNorth = (result.yaw + samplingOffsetAnchor) % 360.0;
            if (sampleNorth < 0) sampleNorth += 360.0;
        }

        // ── Outlier check BEFORE adding to buffer ──
        if (!sampleBuffer.isEmpty()) {
            double referenceYaw = circularWeightedMean(sampleBuffer);
            double diff = Math.abs(angleDifference(referenceYaw, result.yaw));

            if (diff > 45) {
                if (result.confidence >= 1.0 && sampleBuffer.get(0).weight < 0.5) {
                    log("[processSample] 🌟 High-confidence plane found! Discarding old low-confidence buffer.");
                    sampleBuffer.clear();
                } else {
                    log(String.format(Locale.US,
                            "[processSample] OUTLIER REJECTED: sample=%.1f° ref=%.1f° diff=%.1f°",
                            result.yaw, referenceYaw, diff));
                    return;
                }
            } else {
                log(String.format(Locale.US,
                        "[processSample] Sample OK: yaw=%.1f° ref=%.1f° diff=%.1f°",
                        result.yaw, referenceYaw, diff));
            }
        }

        long now = System.currentTimeMillis();
        if (lastSampleTime > 0 && (now - lastSampleTime) > MAX_SAMPLE_GAP_MS) {
            int discarded = sampleBuffer.size();
            sampleBuffer.clear();
            log(String.format(Locale.US,
                    "Sample gap %dms > %dms — discarded %d stale samples, restarting",
                    (now - lastSampleTime), MAX_SAMPLE_GAP_MS, discarded));
            runOnUiThread(() -> dbBadge.setText("🔄 Gap detected — resampling 0/" + REQUIRED_SAMPLES));
        }
        lastSampleTime = now;

        sampleBuffer.add(new YawSample(result.yaw, result.pitch, sampleNorth, result.confidence));
        logSampleBuffer();
        updateFullSampleListUI();
        log("method=" + result.method);

        log(String.format(Locale.US, "  Sample %d: yaw=%.1f° conf=%.2f [%s]",
                sampleBuffer.size(), result.yaw, result.confidence, result.method));

        final String methodCopy = result.method;
        final int countCopy = sampleBuffer.size();
        runOnUiThread(() -> dbBadge.setText("🔄 Sampling " + countCopy + "/" + REQUIRED_SAMPLES + " [" + methodCopy + "]"));

        if (sampleBuffer.size() >= REQUIRED_SAMPLES) {
            isSampling = false;
            StringBuilder sb = new StringBuilder("[processSample] ALL SAMPLES: ");
            for (int i = 0; i < sampleBuffer.size(); i++) {
                YawSample s = sampleBuffer.get(i);
                sb.append(String.format(Locale.US, "#%d=%.1f°(w=%.2f) ", i + 1, s.yaw, s.weight));
            }
            log(sb.toString());

            double finalYaw = circularWeightedMean(sampleBuffer);
            double finalPitch = weightedPitchMean(sampleBuffer);
            log(String.format(Locale.US, "Sampling complete. Averaged yaw=%.2f° pitch=%.1f° (%s) from %d samples",
                    finalYaw, finalPitch, pitchLabel(finalPitch), sampleBuffer.size()));
            sampleBuffer.clear();
            commitReading(finalYaw, finalPitch);
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

    /** Weighted arithmetic mean for pitch (no wraparound needed, range is -90 to +90). */
    private double weightedPitchMean(List<YawSample> samples) {
        double sum = 0, totalWeight = 0;
        for (YawSample s : samples) {
            sum += s.pitch * s.weight;
            totalWeight += s.weight;
        }
        return totalWeight < 1e-6 ? 0 : sum / totalWeight;
    }

    private double circularMeanDegrees(List<Double> samples) {
        if (samples.isEmpty()) return 0.0;

        double sumSin = 0.0;
        double sumCos = 0.0;
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
        if (samples.size() < 5) {
            return new ArrayList<>(samples);
        }

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

    private void commitReading(double rawYaw, double pitch) {
        if (lastPayload == null) return;

        double offsetToUse = samplingOffsetAnchor;
        boolean isFloor = Math.abs(pitch) >= 65.0;

        log("COMMIT READING DEBUG:");
        log(" -> Raw AR Yaw (from math): " + rawYaw + "°");
        log(" -> Pitch: " + String.format(Locale.US, "%.1f° (%s)", pitch, pitchLabel(pitch)));
        log(" -> isFloor: " + isFloor + " (attitude-VP yaw is already ENU-based)");
        log(" -> Offset USED: " + (isFloor ? "SKIPPED (floor)" : offsetToUse + "°"));

        double yaw = rawYaw;

        // Floor QR yaw from attitude-VP is already in ENU global space — skip magnetic offset
        if (!isFloor && !Double.isNaN(offsetToUse)) {
            yaw = (rawYaw + offsetToUse) % 360.0;
            if (yaw < 0) yaw += 360.0;
        }

        samplingOffsetAnchor = Double.NaN;
        log(" -> FINAL COMPASS BEARING: " + yaw + "°");

        double yawRad = Math.toRadians(yaw);
        float normalX = (float) Math.sin(yawRad);
        float normalZ = (float) Math.cos(yawRad);

        if (purpose == SamplingPurpose.REGISTER) {
            getSharedPreferences("qryaw_offsets", MODE_PRIVATE)
                    .edit()
                    .putFloat("offset_" + lastPayload, (float) offsetToUse)
                    .apply();

            Long dbId = QRDatabaseHelper.getInstance(this).saveRegistration(
                    lastPayload, yaw, pitch, normalX, 0.0, normalZ, DEFAULT_TOLERANCE);

            if (dbId == null) {
                showStatus("⚠️ Database save failed.");
                return;
            }

            QRDatabaseHelper.Registration reg = new QRDatabaseHelper.Registration();
            reg.id = dbId;
            reg.payload = lastPayload;
            reg.yaw = yaw;
            reg.pitchDegrees = pitch;
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
                showStatus(String.format(Locale.US, "✅ Registered & Saved!\nYaw = %.1f°  Pitch = %.1f° (%s)\nid=%d",
                        finalYaw, pitch, pitchLabel(pitch), dbId));
                updateInfoLabel(rawYaw, finalYaw, offsetToUse, reg, null, null, InfoSource.FRESH_REGISTER);
            });

            log(String.format(Locale.US, "Registered → payload=%s…  yaw=%.1f°  pitch=%.1f° (%s)  dbId=%d",
                    lastPayload.substring(0, Math.min(20, lastPayload.length())), yaw, pitch, pitchLabel(pitch), dbId));

        } else {
            QRDatabaseHelper.Registration record = QRDatabaseHelper.getInstance(this).fetchRegistration(lastPayload);
            if (record == null) {
                showStatus("⚠️ No previous registration found.\nPress REGISTER first.");
                setMode(ScanMode.REGISTER);
                return;
            }

            float regOffset = getSharedPreferences("qryaw_offsets", MODE_PRIVATE).getFloat("offset_" + lastPayload, Float.NaN);
            boolean offsetDrifted = false;
            double offsetDelta = 0;
            if (!Float.isNaN(regOffset)) {
                offsetDelta = Math.abs(angleDifference(regOffset, offsetToUse));
                offsetDrifted = offsetDelta > 10.0;
                log(String.format(Locale.US, "[VALIDATE] Offset check: regOffset=%.1f° currentOffset=%.1f° delta=%.1f° drifted=%b",
                        regOffset, offsetToUse, offsetDelta, offsetDrifted));
            }

            // Yaw validation
            double yawDelta = angleDifference(record.yaw, yaw);
            boolean yawOK = Math.abs(yawDelta) <= DEFAULT_TOLERANCE;

            // Pitch validation
            double pitchDelta = pitch - record.pitchDegrees;
            boolean pitchOK = Math.abs(pitchDelta) <= PITCH_TOLERANCE;

            // Both must pass
            boolean overallOK = yawOK && pitchOK;

            log(String.format(Locale.US,
                    "[VALIDATE] yaw: reg=%.1f° cur=%.1f° Δ=%+.1f° (±%.0f°) %s | pitch: reg=%.1f° cur=%.1f° Δ=%+.1f° (±%.0f°) %s | overall=%s",
                    record.yaw, yaw, yawDelta, DEFAULT_TOLERANCE, yawOK ? "PASS" : "FAIL",
                    record.pitchDegrees, pitch, pitchDelta, PITCH_TOLERANCE, pitchOK ? "PASS" : "FAIL",
                    overallOK ? "PASS" : "FAIL"));

            QRDatabaseHelper.getInstance(this).saveValidation(
                    record.id, lastPayload, yaw, record.yaw, yawDelta, overallOK, DEFAULT_TOLERANCE,
                    pitch, record.pitchDegrees, pitchDelta, pitchOK);

            double finalYaw1 = yaw;

            runOnUiThread(() -> {
                String yawIcon = yawOK ? "✅" : "❌";
                String pitchIcon = pitchOK ? "✅" : "❌";

                String statusMsg;
                if (overallOK) {
                    statusMsg = "✅ QR has NOT moved";
                } else if (!pitchOK && yawOK) {
                    statusMsg = "❌ SURFACE CHANGED (" + pitchLabel(record.pitchDegrees) + " → " + pitchLabel(pitch) + "?)";
                } else if (!yawOK && pitchOK) {
                    statusMsg = "❌ DIRECTION CHANGED";
                } else {
                    statusMsg = "❌ QR has MOVED (yaw + surface)";
                }

                showStatus(String.format(Locale.US,
                        "%s\n\n%s Yaw  Δ = %+.1f°  (±%.0f°)\n%s Pitch Δ = %+.1f°  (±%.0f°)\n[%s → %s]",
                        statusMsg,
                        yawIcon, yawDelta, DEFAULT_TOLERANCE,
                        pitchIcon, pitchDelta, PITCH_TOLERANCE,
                        pitchLabel(record.pitchDegrees), pitchLabel(pitch)));
                dbBadge.setText("📝 Validation logged to DB");
                updateInfoLabel(rawYaw, finalYaw1, offsetToUse, record, yawDelta, pitchDelta, InfoSource.DATABASE);
            });

            log(String.format(Locale.US,
                    "Validate → payload=%s…\n           currentYaw=%.1f°  registeredYaw=%.1f°  Δyaw=%+.1f°  yawOK=%b\n           currentPitch=%.1f°  registeredPitch=%.1f°  Δpitch=%+.1f°  pitchOK=%b\n           overall=%b",
                    lastPayload.substring(0, Math.min(20, lastPayload.length())),
                    yaw, record.yaw, yawDelta, yawOK,
                    pitch, record.pitchDegrees, pitchDelta, pitchOK,
                    overallOK));
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Core: Compute QR World Yaw via ARCore
    // ─────────────────────────────────────────────────────────────────────────
    private QRMeasurement computeQRYawFromTopEdgeWithPose(float[] imagePoints, float[] rawImagePoints,
                                                          float[] camPose, CameraIntrinsics intr) {
        float[] focal = intr.getFocalLength();
        float[] pp = intr.getPrincipalPoint();

        float fx = focal[0], fy = focal[1], cx = pp[0], cy = pp[1];

        float[][] imgPts = {
                {imagePoints[0], imagePoints[1]},
                {imagePoints[2], imagePoints[3]},
                {imagePoints[4], imagePoints[5]},
                {imagePoints[6], imagePoints[7]}
        };

        String method = "vanishing-point";

        // 1. Calculate Normal directly from Vanishing Points (No Hit Testing)
        float[] planeNormal = computeNormalViaVanishingPoint(imgPts[0], imgPts[1], imgPts[2], imgPts[3], focal, pp, camPose);

        if (planeNormal == null) {
            log("[yawCalc] Vanishing-point solve failed — no normal available");
            return null;
        }

        // 2. Ensure Normal is facing the camera
        float[] camFwd = {-camPose[8], -camPose[9], -camPose[10]};
        float dotNormalCamFwd = vec3Dot(planeNormal, camFwd);
        if (dotNormalCamFwd < 0) {
            planeNormal = vec3Scale(planeNormal, -1f);
            log(String.format(Locale.US, "[yawCalc] Normal flipped (dot=%.3f) → now=(%.3f,%.3f,%.3f)",
                    dotNormalCamFwd, planeNormal[0], planeNormal[1], planeNormal[2]));
        }

        // 3. Compute pitch from normal's vertical component
        //    normal.y ≈ 0 → wall, ≈ +1 → floor
        double clampedY = Math.max(-1.0, Math.min(1.0, (double) planeNormal[1]));
        double pitch = Math.toDegrees(Math.asin(clampedY));

        // 4. Branch on wall vs floor based on horizontal magnitude of normal
        double hx = planeNormal[0];
        double hz = planeNormal[2];
        double hMag = Math.hypot(hx, hz);

        double yaw;
        double confidence;
        String yawSource;

        if (hMag > 0.15) {
            // ── WALL CASE: unchanged — use face normal projected onto horizontal plane ──
            yawSource = "WALL (plane normal)";
            yaw = Math.toDegrees(Math.atan2(hx, -hz));
            if (yaw < 0) yaw += 360;
            confidence = 0.8;
        } else {
            // ── FLOOR CASE: use attitude-based VP for position-independent yaw ──
            Double floorYaw = computeFloorYawViaAttitude(imagePoints, intr.getFocalLength(), intr.getPrincipalPoint());
            if (floorYaw != null) {
                yawSource = "FLOOR (attitude-VP)";
                yaw = floorYaw;
                confidence = 0.6;
            } else {
                // Fallback to ray-plane method if attitude is unavailable
                log("[yawCalc] Attitude floor yaw failed, falling back to ray-plane");
                yawSource = "FLOOR (ray-plane fallback)";
                floorYaw = computeFloorYaw(rawImagePoints, planeNormal, camPose, intr);
                if (floorYaw == null) {
                    log("[yawCalc] Floor yaw computation failed");
                    return null;
                }
                yaw = floorYaw;
                confidence = 0.5;
            }
        }

        log(String.format(Locale.US, "[yawCalc] RESULT: rawYaw=%.1f° pitch=%.1f° (%s) conf=%.2f method=%s src=%s",
                yaw, pitch, pitchLabel(pitch), confidence, method, yawSource));

        String resultMethod = yawSource.startsWith("FLOOR (attitude") ? "attitude-VP" : method;
        return new QRMeasurement(yaw, pitch, confidence, resultMethod);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Vanishing Point Normal Solve
    // ─────────────────────────────────────────────────────────────────────────
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
    // Floor Yaw Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Cast a ray from camera through an image pixel and intersect with a world-space plane.
     * Returns the 3D intersection point, or null if the ray is parallel / behind camera.
     */
    private float[] intersectRayPlane(float imgX, float imgY,
                                      float fx, float fy, float cx, float cy,
                                      float[] camPose, float[] camOrigin,
                                      float[] planeNormal, float[] planePoint) {
        // Camera-space ray: unproject pixel through intrinsics
        float[] camRay = vec3Normalize(new float[]{
                (imgX - cx) / fx,
                -((imgY - cy) / fy),   // negate Y: image Y-down → camera Y-up
                -1.0f                   // camera looks along -Z
        });
        // World-space ray
        float[] worldRay = rotateCamToWorld(camRay, camPose);

        float denom = vec3Dot(worldRay, planeNormal);
        if (Math.abs(denom) < 1e-5f) return null;  // ray parallel to plane

        float[] diff = {
                planePoint[0] - camOrigin[0],
                planePoint[1] - camOrigin[1],
                planePoint[2] - camOrigin[2]
        };
        float t = vec3Dot(diff, planeNormal) / denom;
        if (t <= 0) return null;  // intersection behind camera

        return new float[]{
                camOrigin[0] + worldRay[0] * t,
                camOrigin[1] + worldRay[1] * t,
                camOrigin[2] + worldRay[2] * t
        };
    }

    /**
     * Compute yaw for a floor QR code using the QR's printed "up" edge direction.
     * Uses RAW (unstabilized) ML Kit corners so TL/BL identity is consistent regardless
     * of camera viewing angle — critical for scanning floor QR codes from any direction.
     *
     * @param rawImagePoints RAW ML Kit corners (8 floats: TL, TR, BR, BL)
     * @param planeNormal    world-space face normal (pointing up for floor)
     * @param camPose        4x4 column-major camera pose matrix
     * @param intr           camera intrinsics
     * @return yaw in degrees [0,360), or null on failure
     */
    private Double computeFloorYaw(float[] rawImagePoints, float[] planeNormal,
                                   float[] camPose, CameraIntrinsics intr) {
        float[] focal = intr.getFocalLength();
        float[] pp = intr.getPrincipalPoint();
        float fx = focal[0], fy = focal[1], cx = pp[0], cy = pp[1];

        // Camera origin from pose (column-major: translation at m[12], m[13], m[14])
        float[] camOrigin = {camPose[12], camPose[13], camPose[14]};

        // Plane point: camOrigin + camForward * 1.0m
        // The actual distance doesn't affect the direction of (tl3D - bl3D) projected to XZ
        // for a floor QR (normal ≈ [0,1,0]), because shifting the plane along Y scales both
        // intersection points equally in Y while leaving their XZ difference unchanged.
        float[] camForward = {-camPose[8], -camPose[9], -camPose[10]};
        float[] planePoint = {
                camOrigin[0] + camForward[0],
                camOrigin[1] + camForward[1],
                camOrigin[2] + camForward[2]
        };

        // Intersect TL (index 0) and BL (index 3) with the plane
        float[] tl3D = intersectRayPlane(rawImagePoints[0], rawImagePoints[1],
                fx, fy, cx, cy, camPose, camOrigin, planeNormal, planePoint);
        float[] bl3D = intersectRayPlane(rawImagePoints[6], rawImagePoints[7],
                fx, fy, cx, cy, camPose, camOrigin, planeNormal, planePoint);

        if (tl3D == null || bl3D == null) {
            log("[floorYaw] Ray-plane intersection failed for TL or BL");
            return null;
        }

        // QR "up" direction = TL - BL (left edge pointing from bottom-left to top-left)
        float[] qrUp = {tl3D[0] - bl3D[0], tl3D[1] - bl3D[1], tl3D[2] - bl3D[2]};

        // Project onto horizontal (XZ) plane
        double ux = qrUp[0];
        double uz = qrUp[2];
        double uxzLen = Math.hypot(ux, uz);

        if (uxzLen < 0.15) {
            log("[floorYaw] QR up-edge has no horizontal extent — degenerate (looking straight down?)");
            return null;
        }

        double yaw = Math.toDegrees(Math.atan2(ux, -uz));
        if (yaw < 0) yaw += 360;

        log(String.format(Locale.US,
                "[floorYaw] tl3D=(%.3f,%.3f,%.3f) bl3D=(%.3f,%.3f,%.3f) qrUp=(%.3f,%.3f,%.3f) yaw=%.1f°",
                tl3D[0], tl3D[1], tl3D[2], bl3D[0], bl3D[1], bl3D[2],
                qrUp[0], qrUp[1], qrUp[2], yaw));

        return yaw;
    }

    /**
     * Compute floor QR yaw using getAttitude() rotation matrix + vanishing point edge direction.
     * Projects the QR's "up" edge direction directly into ENU (East-North-Up) global space,
     * making the result position-independent. Uses only 2D corners + intrinsics + attitude.
     *
     * @param imagePoints  ML Kit corners (8 floats: TL, TR, BR, BL) in image coordinates
     * @param focalLength  camera intrinsics focal length [fx, fy]
     * @param principalPt  camera intrinsics principal point [cx, cy]
     * @return yaw in degrees [0,360) in ENU space, or null on failure
     */
    private Double computeFloorYawViaAttitude(float[] imagePoints, float[] focalLength, float[] principalPt) {
        if (!attitudeValid) {
            log("[attitudeFloorYaw] Attitude not ready");
            return null;
        }

        float fx = focalLength[0], fy = focalLength[1];
        float cx = principalPt[0], cy = principalPt[1];

        // Vanishing points from 2D corners (homogeneous coordinates)
        float[] hTL = {imagePoints[0], imagePoints[1], 1f};
        float[] hTR = {imagePoints[2], imagePoints[3], 1f};
        float[] hBR = {imagePoints[4], imagePoints[5], 1f};
        float[] hBL = {imagePoints[6], imagePoints[7], 1f};

        // VP of left/right edges = QR "up" direction in projective space
        float[] lineLeft  = vec3Cross(hTL, hBL);
        float[] lineRight = vec3Cross(hTR, hBR);
        float[] vpY = vec3Cross(lineLeft, lineRight);

        if (Math.abs(vpY[2]) < 1e-6f) {
            log("[attitudeFloorYaw] VP at infinity — edges are parallel in image");
            return null;
        }

        // Convert VP to camera-space ray (QR "up" direction in camera space)
        float[] rayY = toCameraRay(vpY, fx, fy, cx, cy);
        if (vec3Len(rayY) < 0.001f) {
            log("[attitudeFloorYaw] Degenerate VP ray");
            return null;
        }

        // Resolve 180° VP ambiguity: ensure rayY points in the QR's "up" direction
        // (from BL toward TL). Use image-space TL-BL direction as ground truth
        // since ML Kit corner ordering is based on finder patterns, not camera angle.
        float vpImgX = vpY[0] / vpY[2];
        float vpImgY = vpY[1] / vpY[2];
        float qrCenterX = (imagePoints[0] + imagePoints[2] + imagePoints[4] + imagePoints[6]) / 4f;
        float qrCenterY = (imagePoints[1] + imagePoints[3] + imagePoints[5] + imagePoints[7]) / 4f;
        float imgUpX = imagePoints[0] - imagePoints[6]; // TL - BL in image X
        float imgUpY = imagePoints[1] - imagePoints[7]; // TL - BL in image Y
        float toVpX = vpImgX - qrCenterX;
        float toVpY = vpImgY - qrCenterY;
        if (imgUpX * toVpX + imgUpY * toVpY < 0) {
            rayY = vec3Scale(rayY, -1f);
            log("[attitudeFloorYaw] VP flipped to match TL-BL direction");
        }

        // Transform to ENU using attitude rotation matrix
        float[] edgeENU = rotateCamToENU(vec3Normalize(rayY), lastAttitudeRotMat, sensorOrientation);

        // Project onto horizontal (East-North) plane
        double eMag = Math.hypot(edgeENU[0], edgeENU[1]);
        if (eMag < 0.15) {
            log("[attitudeFloorYaw] QR up-edge has no horizontal extent in ENU — degenerate");
            return null;
        }

        // Yaw = atan2(East, North) — compass bearing convention
        double yaw = Math.toDegrees(Math.atan2(edgeENU[0], edgeENU[1]));
        if (yaw < 0) yaw += 360.0;

        log(String.format(Locale.US,
                "[attitudeFloorYaw] edgeENU=(%.3f,%.3f,%.3f) hMag=%.3f yaw=%.1f°",
                edgeENU[0], edgeENU[1], edgeENU[2], eMag, yaw));

        return yaw;
    }

    /**
     * Human-readable label for a pitch angle.
     */
    private static String pitchLabel(double pitch) {
        double abs = Math.abs(pitch);
        if (abs < 25) return "Wall";
        if (abs < 65) return "Tilted";
        return "Floor";
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

    private List<float[]> toCornerList(float[] points) {
        List<float[]> corners = new ArrayList<>(4);
        for (int i = 0; i < 4; i++) {
            corners.add(new float[]{points[i * 2], points[i * 2 + 1]});
        }
        return corners;
    }

    private void resetTrackedQrCorners() {
        lastAcceptedImagePoints = null;
        lastAcceptedViewPoints = null;
    }

    private float[] copyPointPairs(float[] points) {
        float[] copy = new float[points.length];
        System.arraycopy(points, 0, copy, 0, points.length);
        return copy;
    }

    private float[] rotatePointPairs(float[] points, int offset) {
        int pairCount = points.length / 2;
        float[] rotated = new float[points.length];
        for (int i = 0; i < pairCount; i++) {
            int srcIndex = ((i + offset) % pairCount) * 2;
            rotated[i * 2] = points[srcIndex];
            rotated[i * 2 + 1] = points[srcIndex + 1];
        }
        return rotated;
    }

    private double scoreCornerRotation(float[] candidateViewPoints, float[] referenceViewPoints) {
        double score = 0.0;
        for (int i = 0; i < candidateViewPoints.length; i += 2) {
            double dx = candidateViewPoints[i] - referenceViewPoints[i];
            double dy = candidateViewPoints[i + 1] - referenceViewPoints[i + 1];
            score += (dx * dx) + (dy * dy);
        }
        return score;
    }

    private float[][] stabilizeCornerOrder(float[] imagePoints, float[] viewPoints) {
        if (lastAcceptedViewPoints == null || lastAcceptedImagePoints == null) {
            lastAcceptedImagePoints = copyPointPairs(imagePoints);
            lastAcceptedViewPoints = copyPointPairs(viewPoints);
            log("[QR] corner order seeded from detector");
            return new float[][]{lastAcceptedImagePoints, lastAcceptedViewPoints};
        }

        double bestScore = Double.POSITIVE_INFINITY;
        int bestRotation = 0;
        float[] bestImagePoints = null;
        float[] bestViewPoints = null;

        for (int rotation = 0; rotation < 4; rotation++) {
            float[] rotatedImagePoints = rotatePointPairs(imagePoints, rotation);
            float[] rotatedViewPoints = rotatePointPairs(viewPoints, rotation);
            double score = scoreCornerRotation(rotatedViewPoints, lastAcceptedViewPoints);

            if (score < bestScore) {
                bestScore = score;
                bestRotation = rotation;
                bestImagePoints = rotatedImagePoints;
                bestViewPoints = rotatedViewPoints;
            }
        }

        lastAcceptedImagePoints = bestImagePoints;
        lastAcceptedViewPoints = bestViewPoints;
        return new float[][]{bestImagePoints, bestViewPoints};
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Continuous QR Detection via ML Kit
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

            InputImage inputImage = InputImage.fromMediaImage(cameraImage, 0);
            final Frame capturedFrame = frame;

            scanner.process(inputImage)
                    .addOnSuccessListener(barcodes -> {
                        if (arSceneView == null || arSceneView.getSession() == null) {
                            return;
                        }

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
                            boolean qrTrulyLost = (now - lastQrSeenTime) > QR_LOST_TIMEOUT_MS;

                            if (qrTrulyLost) {
                                runOnUiThread(() -> {
                                    overlayView.setCorners(null);
                                    lastDetectedCorners = null;
                                    resetTrackedQrCorners();
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
                            return;
                        }

                        lastQrSeenTime = System.currentTimeMillis();
                        String payload = qr.getRawValue();
                        if (lastPayload != null && !payload.equals(lastPayload)) {
                            resetTrackedQrCorners();
                        }

                        android.graphics.Point[] pts = qr.getCornerPoints();
                        if (pts == null || pts.length != 4) return;

                        float[] imagePoints = new float[]{
                                pts[0].x, pts[0].y, pts[1].x, pts[1].y,
                                pts[2].x, pts[2].y, pts[3].x, pts[3].y
                        };

                        // Preserve raw ML Kit corners BEFORE stabilization.
                        // For floor QR yaw, we need the original corner identity
                        // (TL/BL based on finder patterns) which stabilization can rotate.
                        final float[] rawImagePointsCopy = imagePoints.clone();

                        float[] viewPoints = new float[8];
                        try {
                            capturedFrame.transformCoordinates2d(
                                    com.google.ar.core.Coordinates2d.IMAGE_PIXELS, imagePoints,
                                    com.google.ar.core.Coordinates2d.VIEW, viewPoints
                            );
                        } catch (Exception e) {
                            Log.w(TAG, "transformCoordinates2d failed (stale frame)", e);
                            return;
                        }

                        float[][] stabilizedPoints = stabilizeCornerOrder(imagePoints, viewPoints);
                        float[] stabilizedImagePoints = stabilizedPoints[0];
                        float[] stabilizedViewPoints = stabilizedPoints[1];
                        List<float[]> cornersList = toCornerList(stabilizedViewPoints);

                        runOnUiThread(() -> {
                            lastDetectedCorners = cornersList;
                            overlayView.setCorners(cornersList);
                            onQRPayloadDetected(payload);

                            if (isSampling && !isCollectingMag) {
                                collectSampleWithPose(stabilizedImagePoints, rawImagePointsCopy, capturedPose, capturedIntrinsics, capturedFrame);
                            }
                        });
                    })
                    .addOnFailureListener(e -> Log.w(TAG, "ML Kit barcode scan failed", e))
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
// ─────────────────────────────────────────────────────────────────────────
// UI Helpers
// ─────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────
// UI Helpers
// ─────────────────────────────────────────────────────────────────────────
    private void updateInfoLabel(Double rawYaw, Double finalYaw, Double currentOffset,
                                 QRDatabaseHelper.Registration reg, Double yawDelta, Double pitchDelta,
                                 InfoSource source) {
        List<String> lines = new ArrayList<>();

        // 1. Raw Mag Samples Dump
        if (magSamplesDisplay != null) {
            lines.add(magSamplesDisplay);
            lines.add("──────────────────────────");
        }

        // 2. QR Info
        if (lastPayload != null) {
            lines.add("QR      : " + lastPayload.substring(0, Math.min(38, lastPayload.length())));
            lines.add("──────────────────────────");
        }

        // 3. REGISTRATION MATH (Historical)
        if (reg != null) {
            float regOffset = getSharedPreferences("qryaw_offsets", MODE_PRIVATE)
                    .getFloat("offset_" + lastPayload, Float.NaN);

            lines.add("💾 REGISTRATION DATA:");
            lines.add("Source  : " + (source == InfoSource.DATABASE ? "📦 DB" : "🆕 New")
                    + "  (" + relativeTime(reg.registeredAt) + ")");

            // Reverse-engineer the raw AR yaw from the DB using the saved offset
            if (!Float.isNaN(regOffset)) {
                double rawRegYaw = (reg.yaw - regOffset + 360.0) % 360.0;
                lines.add(String.format(Locale.US, "Math    : [%.1f° raw] + [%.1f° offset]", rawRegYaw, regOffset));
            }
            lines.add(String.format(Locale.US, "Reg Yaw : %.1f°", reg.yaw));
            lines.add(String.format(Locale.US, "Reg Pitch: %.1f° (%s)", reg.pitchDegrees, pitchLabel(reg.pitchDegrees)));
            lines.add("──────────────────────────");
        }

        // 4. CURRENT SCAN MATH (Live)
        if (finalYaw != null && rawYaw != null && currentOffset != null) {
            lines.add("🧭 CURRENT SCAN DATA:");
            lines.add(String.format(Locale.US, "Math    : [%.1f° raw] + [%.1f° offset]", rawYaw, currentOffset));
            lines.add(String.format(Locale.US, "Cur Yaw : %.1f°", finalYaw));
        }

        // 5. FINAL RESULT / DELTA
        if (yawDelta != null) {
            lines.add("──────────────────────────");
            lines.add(String.format(Locale.US, "Δ Yaw   : %+.1f°  (tol ±%.0f°)", yawDelta, DEFAULT_TOLERANCE));
            boolean yawOK = Math.abs(yawDelta) <= DEFAULT_TOLERANCE;
            if (pitchDelta != null) {
                lines.add(String.format(Locale.US, "Δ Pitch : %+.1f°  (tol ±%.0f°)", pitchDelta, PITCH_TOLERANCE));
                boolean pitchOK = Math.abs(pitchDelta) <= PITCH_TOLERANCE;
                if (yawOK && pitchOK) {
                    lines.add("Result  : ✅ SAME POSITION & ORIENTATION");
                } else if (!pitchOK && yawOK) {
                    lines.add("Result  : ❌ SURFACE CHANGED");
                } else if (!yawOK && pitchOK) {
                    lines.add("Result  : ❌ DIRECTION CHANGED");
                } else {
                    lines.add("Result  : ❌ MOVED (yaw + surface)");
                }
            } else {
                lines.add("Result  : " + (yawOK ? "✅ SAME POSITION" : "❌ MOVED"));
            }
        }

        // Push to UI
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
        if (attitude == null || attitude.length < 4) {
            return orientation.getHeadingDegrees();
        }

        float[] rotationMatrix = new float[9];
        SensorManager.getRotationMatrixFromVector(rotationMatrix, attitude);

        // Android device axes: +X right, +Y up, +Z out of the screen.
        // The rear camera looks along -Z in device coordinates.
        float east = -rotationMatrix[2];
        float north = -rotationMatrix[5];

        if (Math.hypot(east, north) < 1e-5) {
            return orientation.getHeadingDegrees();
        }

        double heading = Math.toDegrees(Math.atan2(east, north));
        if (heading < 0) heading += 360.0;
        return heading;
    }
}
