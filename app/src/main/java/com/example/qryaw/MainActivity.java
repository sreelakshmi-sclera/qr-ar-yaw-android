package com.example.qryaw;

import android.Manifest;
import android.annotation.SuppressLint;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.hardware.GeomagneticField;
import android.hardware.SensorManager;
import android.hardware.camera2.CameraCharacteristics;
import android.hardware.camera2.CameraManager;
import android.os.Bundle;
import android.os.SystemClock;
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
import org.opencv.core.Size;
import org.opencv.core.TermCriteria;
import org.opencv.imgproc.Imgproc;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.sceneview.ar.ARSceneView;
import kotlin.Unit;

public class MainActivity extends AppCompatActivity {

    private static final String TAG = "QRYaws";

    // ── Configuration Constants ──
    private static final int REQUIRED_SAMPLES = 8;
    private static final double MIN_SAMPLE_WEIGHT = 0.15;
    private static final double YAW_TOLERANCE = 30.0;   // Loose: Absorbs magnetic drift
    private static final double PITCH_TOLERANCE = 20.0; // Tight: Gravity is stable
    private static final double ROLL_TOLERANCE = 20.0;  // Tight: PnP optics are stable
    private static final float HEADING_ERROR_GATE = 90.0f;
    private static final int MAG_SAMPLES_REQUIRED = 15;
    private static final long MAX_SAMPLE_GAP_MS = 400;
    private static final long QR_LOST_TIMEOUT_MS = 500;
    private static final int SUBPIX_WINDOW_RADIUS = 5;
    private static final int SUBPIX_MAX_ITER = 20;
    private static final double SUBPIX_EPSILON = 0.03;
    private static final long SUBPIX_LOG_INTERVAL_MS = 500;
    private static final int MAX_FOP_HISTORY_SAMPLES = 128;
    private static final long MAX_FOP_INTERPOLATION_SPAN_NS = 75_000_000L;
    private static final long MAX_FOP_NEAREST_SAMPLE_AGE_NS = 50_000_000L;
    private static final double DIRECT_METHOD_OUTLIER_THRESHOLD_DEG = 8.0;
    private static final double DIRECT_SAMPLE_RESET_CONFIDENCE = 0.85;
    private static final double DIRECT_REPROJECTION_CONFIDENCE_SCALE_PX = 1.2;
    private static final double WALL_DIRECT_REPROJECTION_GATE_PX = 1.8;
    private static final double WALL_AMBIGUITY_YAW_SPLIT_DEG = 4.0;
    private static final double WALL_AMBIGUITY_MAX_SCORE_MARGIN = 0.20;
    private static final int WALL_EARLY_SAMPLE_CAP_COUNT = 2;
    private static final double WALL_EARLY_SAMPLE_CONFIDENCE_CAP = 0.70;
    private static final double PITCH_OUTLIER_THRESHOLD_DEG = 20.0;
    private static final int NON_DIRECT_FALLBACKS_BEFORE_MAG = 3;
    private static final double WALL_BACKFACING_PENALTY = 1000.0;
    private static final double WALL_CONTINUITY_PENALTY_PER_DEG = 0.01;
    private static final String OFFSET_PREFS_NAME = "qryaw_offsets";
    private static final String OFFSET_KEY_PREFIX = "offset_";
    private static final String OFFSET_MODE_KEY_PREFIX = "offset_mode_";

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
    private volatile boolean isCollectingMag = false;
    private double samplingOffsetAnchor = Double.NaN;
    private volatile double lastRawMagHeading = Double.NaN;
    private volatile TimedAttitude lastFopSample = null;
    private volatile String lastPayload;
    private volatile QRDatabaseHelper.Registration currentRegistration;
    private volatile boolean isSampling = false;
    private SamplingPurpose purpose = SamplingPurpose.REGISTER;
    private final List<YawSample> sampleBuffer = new ArrayList<>();
    private long lastSampleTime = 0;
    private volatile List<float[]> lastDetectedCorners;
    private long lastQrSeenTime = 0;
    private long lastSubPixLogTime = 0;
    private final AtomicBoolean isProcessingFrame = new AtomicBoolean(false);
    private int backCameraSensorOrientationDegrees = 90;
    private int consecutiveNonDirectFallbacks = 0;
    private long nextSamplingSessionId = 1L;
    private SamplingSession activeSamplingSession = null;

    private final ExecutorService qrExecutor = Executors.newSingleThreadExecutor();
    private ExecutorService fopExecutor;
    private volatile boolean isFopRegistered = false;
    private volatile int activeFopGeneration = 0;
    private final AtomicInteger nextFopGeneration = new AtomicInteger(1);
    private DeviceOrientationListener activeFopListener = null;
    private final Object fopHistoryLock = new Object();
    private final ArrayDeque<TimedAttitude> fopAttitudeHistory = new ArrayDeque<>();

    // ── Services ──
    private FusedOrientationProviderClient fusedOrientationClient;
    private FusedLocationProviderClient locationClient;

    private enum SamplingPurpose {REGISTER, VALIDATE}

    private enum ScanMode {REGISTER, VALIDATE}

    private enum InfoSource {DATABASE, FRESH_REGISTER}

    private enum OffsetMode {RELATIVE, ABSOLUTE, UNKNOWN}

    // ── Inner Data Classes ──
    private static class YawSample {
        final double yaw;
        final double northYaw;
        final double pitch;
        final double roll;
        final double weight;

        YawSample(double yaw, double northYaw, double pitch, double roll, double weight) {
            this.yaw = yaw;
            this.northYaw = northYaw;
            this.pitch = pitch;
            this.roll = roll;
            this.weight = weight;
        }
    }

    private static class QRMeasurement {
        final double yaw;
        final double pitch;
        final double roll;
        final double confidence;
        final String method;
        final boolean rollAbsolute;

        QRMeasurement(double yaw, double pitch, double roll, double confidence, String method, boolean rollAbsolute) {
            this.yaw = yaw;
            this.pitch = pitch;
            this.roll = roll;
            this.confidence = confidence;
            this.method = method;
            this.rollAbsolute = rollAbsolute;
        }
    }

    private static class VpEstimate {
        final float[] normalCam;
        final float[] normalWorld;

        VpEstimate(float[] normalCam, float[] normalWorld) {
            this.normalCam = normalCam;
            this.normalWorld = normalWorld;
        }
    }

    private static class TimedAttitude {
        final long elapsedRealtimeNs;
        final float[] attitude;

        TimedAttitude(long elapsedRealtimeNs, float[] attitude) {
            this.elapsedRealtimeNs = elapsedRealtimeNs;
            this.attitude = attitude;
        }
    }

    private static class ResolvedFopAttitude {
        final float[] attitude;
        final long targetTimestampNs;
        final long referenceTimestampNs;
        final String strategy;

        ResolvedFopAttitude(float[] attitude, long targetTimestampNs, long referenceTimestampNs, String strategy) {
            this.attitude = attitude;
            this.targetTimestampNs = targetTimestampNs;
            this.referenceTimestampNs = referenceTimestampNs;
            this.strategy = strategy;
        }
    }

    private static class ValidationSummary {
        final double yawDelta;
        final double pitchDelta;
        final double rollDelta;
        final double tolerance;
        final boolean withinTolerance;

        ValidationSummary(double yawDelta, double pitchDelta, double rollDelta, double tolerance, boolean withinTolerance) {
            this.yawDelta = yawDelta;
            this.pitchDelta = pitchDelta;
            this.rollDelta = rollDelta;
            this.tolerance = tolerance;
            this.withinTolerance = withinTolerance;
        }
    }

    private static class SamplingSession {
        final long id;
        final String payload;
        final SamplingPurpose purpose;
        final long startedAtMs;
        String lockedMethod;
        String lockedMethodFamily;
        Boolean absoluteNorth;
        double offsetAnchor = Double.NaN;

        SamplingSession(long id, String payload, SamplingPurpose purpose, long startedAtMs) {
            this.id = id;
            this.payload = payload;
            this.purpose = purpose;
            this.startedAtMs = startedAtMs;
        }
    }

    private void flushSensors() {
        log("Manual Recalibration Triggered: Flushing sensors.");

        stopFOP();

        resetAlignment();
        lastPayload = null;
        currentRegistration = null;
        resetSamplingState();
        lastDetectedCorners = null;
        lastQrSeenTime = 0;
        isProcessingFrame.set(false);
        clearFopHistory();

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
    private DeviceOrientationListener createFopListener(int generation) {
        return new DeviceOrientationListener() {
            @SuppressLint("SetTextI18n")
            @Override
            public void onDeviceOrientationChanged(DeviceOrientation orientation) {
                if (!isFopRegistered || generation != activeFopGeneration) {
                    return;
                }

                float headingError = orientation.getHeadingErrorDegrees();

                if (headingError == 180.0f || headingError > HEADING_ERROR_GATE) {
                    if (isCollectingMag) {
                        runOnUiThread(() -> showStatus(
                                String.format(Locale.US, "⚠️ Compass interference (Error: %.1f°).\nPlease move phone in a figure-8.", headingError)
                        ));
                    }
                    return;
                }

                float[] attitude = orientation.getAttitude();
                if (attitude.length >= 4) {
                    float[] normalizedAttitude = normalizeQuaternionCopy(attitude);
                    if (normalizedAttitude != null) {
                        long sampleElapsedRealtimeNs = orientation.getElapsedRealtimeNs();
                        lastFopSample = new TimedAttitude(sampleElapsedRealtimeNs, normalizedAttitude.clone());
                        addFopAttitudeSample(sampleElapsedRealtimeNs, normalizedAttitude);
                    }
                }

                runOnUiThread(() -> {
                    if (!isFopRegistered || generation != activeFopGeneration) {
                        return;
                    }

                    lastRawMagHeading = getCameraHeadingDegrees(orientation);

                    if (isCollectingMag) {
                        SamplingSession session = activeSamplingSession;
                        if (!isSampling || session == null) {
                            isCollectingMag = false;
                            return;
                        }

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
                            session.offsetAnchor = samplingOffsetAnchor;

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
    }

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

    private int resolveBackCameraSensorOrientation() {
        try {
            CameraManager cameraManager = getSystemService(CameraManager.class);
            if (cameraManager == null) return 90;

            for (String cameraId : cameraManager.getCameraIdList()) {
                CameraCharacteristics characteristics = cameraManager.getCameraCharacteristics(cameraId);
                Integer lensFacing = characteristics.get(CameraCharacteristics.LENS_FACING);
                if (lensFacing == null || lensFacing != CameraCharacteristics.LENS_FACING_BACK) {
                    continue;
                }

                Integer sensorOrientation = characteristics.get(CameraCharacteristics.SENSOR_ORIENTATION);
                if (sensorOrientation != null) {
                    return sensorOrientation;
                }
            }
        } catch (Exception e) {
            Log.w(TAG, "Failed to resolve back camera sensor orientation", e);
        }
        return 90;
    }

    private float[] mapOpenCvCameraVectorToDeviceFrame(double camX, double camY, double camZ) {
        int orientation = ((backCameraSensorOrientationDegrees % 360) + 360) % 360;

        float deviceX;
        float deviceY;

        switch (orientation) {
            case 0:
                deviceX = (float) camX;
                deviceY = (float) -camY;
                break;
            case 90:
                deviceX = (float) -camY;
                deviceY = (float) -camX;
                break;
            case 180:
                deviceX = (float) -camX;
                deviceY = (float) camY;
                break;
            case 270:
                deviceX = (float) camY;
                deviceY = (float) camX;
                break;
            default:
                Log.w(TAG, "Unexpected sensor orientation " + orientation + "°, assuming 90° mapping");
                deviceX = (float) -camY;
                deviceY = (float) -camX;
                break;
        }

        return new float[]{deviceX, deviceY, (float) -camZ};
    }

    private void addFopAttitudeSample(long elapsedRealtimeNs, float[] attitude) {
        synchronized (fopHistoryLock) {
            fopAttitudeHistory.addLast(new TimedAttitude(elapsedRealtimeNs, attitude.clone()));
            while (fopAttitudeHistory.size() > MAX_FOP_HISTORY_SAMPLES) {
                fopAttitudeHistory.removeFirst();
            }
        }
    }

    private void clearFopHistory() {
        synchronized (fopHistoryLock) {
            fopAttitudeHistory.clear();
        }
        lastFopSample = null;
    }

    private static float[] normalizeQuaternionCopy(float[] quaternion) {
        if (quaternion == null || quaternion.length < 4) return null;

        double norm = Math.sqrt(
                quaternion[0] * quaternion[0]
                        + quaternion[1] * quaternion[1]
                        + quaternion[2] * quaternion[2]
                        + quaternion[3] * quaternion[3]);
        if (norm < 1e-9) return null;

        return new float[]{
                (float) (quaternion[0] / norm),
                (float) (quaternion[1] / norm),
                (float) (quaternion[2] / norm),
                (float) (quaternion[3] / norm)
        };
    }

    private static float[] slerpQuaternion(float[] q0, float[] q1, double alpha) {
        float[] start = normalizeQuaternionCopy(q0);
        float[] end = normalizeQuaternionCopy(q1);
        if (start == null || end == null) return null;

        double dot = start[0] * end[0] + start[1] * end[1] + start[2] * end[2] + start[3] * end[3];
        if (dot < 0.0) {
            dot = -dot;
            end = new float[]{-end[0], -end[1], -end[2], -end[3]};
        }

        if (dot > 0.9995) {
            float[] blended = new float[4];
            for (int i = 0; i < 4; i++) {
                blended[i] = (float) ((1.0 - alpha) * start[i] + alpha * end[i]);
            }
            return normalizeQuaternionCopy(blended);
        }

        double theta0 = Math.acos(Math.max(-1.0, Math.min(1.0, dot)));
        double sinTheta0 = Math.sin(theta0);
        if (Math.abs(sinTheta0) < 1e-6) {
            return start.clone();
        }

        double theta = theta0 * alpha;
        double sinTheta = Math.sin(theta);
        double s0 = Math.sin(theta0 - theta) / sinTheta0;
        double s1 = sinTheta / sinTheta0;

        return new float[]{
                (float) (s0 * start[0] + s1 * end[0]),
                (float) (s0 * start[1] + s1 * end[1]),
                (float) (s0 * start[2] + s1 * end[2]),
                (float) (s0 * start[3] + s1 * end[3])
        };
    }

    private ResolvedFopAttitude resolveFopAttitudeAt(long targetTimestampNs, String strategyBase) {
        if (targetTimestampNs <= 0) return null;

        synchronized (fopHistoryLock) {
            if (fopAttitudeHistory.isEmpty()) return null;

            TimedAttitude prev = null;
            TimedAttitude next = null;

            for (TimedAttitude sample : fopAttitudeHistory) {
                if (sample.elapsedRealtimeNs <= targetTimestampNs) {
                    prev = sample;
                }
                if (sample.elapsedRealtimeNs >= targetTimestampNs) {
                    next = sample;
                    break;
                }
            }

            if (prev != null && next != null) {
                if (prev.elapsedRealtimeNs == next.elapsedRealtimeNs) {
                    long deltaNs = Math.abs(targetTimestampNs - prev.elapsedRealtimeNs);
                    if (deltaNs <= MAX_FOP_NEAREST_SAMPLE_AGE_NS) {
                        return new ResolvedFopAttitude(
                                prev.attitude.clone(),
                                targetTimestampNs,
                                prev.elapsedRealtimeNs,
                                strategyBase + "-exact");
                    }
                } else {
                    long spanNs = next.elapsedRealtimeNs - prev.elapsedRealtimeNs;
                    if (spanNs > 0 && spanNs <= MAX_FOP_INTERPOLATION_SPAN_NS) {
                        double alpha = (double) (targetTimestampNs - prev.elapsedRealtimeNs) / (double) spanNs;
                        float[] interpolated = slerpQuaternion(prev.attitude, next.attitude, alpha);
                        if (interpolated != null) {
                            return new ResolvedFopAttitude(
                                    interpolated,
                                    targetTimestampNs,
                                    targetTimestampNs,
                                    strategyBase + "-interp");
                        }
                    }
                }
            }

            TimedAttitude nearest = null;
            long nearestDeltaNs = Long.MAX_VALUE;
            if (prev != null) {
                nearest = prev;
                nearestDeltaNs = Math.abs(targetTimestampNs - prev.elapsedRealtimeNs);
            }
            if (next != null) {
                long nextDeltaNs = Math.abs(targetTimestampNs - next.elapsedRealtimeNs);
                if (nextDeltaNs < nearestDeltaNs) {
                    nearest = next;
                    nearestDeltaNs = nextDeltaNs;
                }
            }

            if (nearest != null && nearestDeltaNs <= MAX_FOP_NEAREST_SAMPLE_AGE_NS) {
                return new ResolvedFopAttitude(
                        nearest.attitude.clone(),
                        targetTimestampNs,
                        nearest.elapsedRealtimeNs,
                        strategyBase + "-nearest");
            }

            return null;
        }
    }

    private ResolvedFopAttitude resolveFopAttitudeForFrame(long imageTimestampNs, long acquireElapsedRealtimeNs) {
        ResolvedFopAttitude resolved = resolveFopAttitudeAt(imageTimestampNs, "image");
        if (resolved != null) return resolved;

        resolved = resolveFopAttitudeAt(acquireElapsedRealtimeNs, "acquire");
        if (resolved != null) return resolved;

        TimedAttitude latestSample = lastFopSample;
        if (latestSample == null) return null;

        return new ResolvedFopAttitude(
                latestSample.attitude.clone(),
                acquireElapsedRealtimeNs,
                latestSample.elapsedRealtimeNs,
                "latest");
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
        backCameraSensorOrientationDegrees = resolveBackCameraSensorOrientation();
        log("Back camera sensor orientation: " + backCameraSensorOrientationDegrees + "°");

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
        if (isSampling) {
            abortActiveSampling("Sampling paused. Please scan again.");
        }
        stopFOP();
    }

    @Override
    protected void onDestroy() {
        stopFOP();
        qrExecutor.shutdownNow();
        if (fopExecutor != null) {
            fopExecutor.shutdownNow();
            fopExecutor = null;
        }
        super.onDestroy();
    }

    private ExecutorService ensureFopExecutor() {
        if (fopExecutor == null || fopExecutor.isShutdown()) {
            fopExecutor = Executors.newSingleThreadExecutor();
        }
        return fopExecutor;
    }

    @SuppressLint("MissingPermission")
    private void startFOP() {
        if (fusedOrientationClient == null) return;
        if (isFopRegistered) {
            log("FOP already started; skipping duplicate registration.");
            return;
        }
        int generation = nextFopGeneration.getAndIncrement();
        DeviceOrientationListener listener = createFopListener(generation);
        activeFopGeneration = generation;
        activeFopListener = listener;
        isFopRegistered = true;

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
                .requestOrientationUpdates(request, ensureFopExecutor(), listener)
                .addOnSuccessListener(unused -> {
                    if (generation == activeFopGeneration && listener == activeFopListener) {
                        log("FOP: registration success");
                    }
                })
                .addOnFailureListener(e -> {
                    if (generation == activeFopGeneration && listener == activeFopListener) {
                        isFopRegistered = false;
                        activeFopGeneration = 0;
                        activeFopListener = null;
                        clearFopHistory();
                    }
                    Log.e(TAG, "FOP: registration failed", e);
                    runOnUiThread(() -> dbBadge.setText("⚠️ FOP unavailable — check Play Services"));
                });
    }

    private void stopFOP() {
        DeviceOrientationListener listener = activeFopListener;
        if (fusedOrientationClient != null && listener != null) {
            fusedOrientationClient.removeOrientationUpdates(listener);
        }
        isFopRegistered = false;
        activeFopGeneration = 0;
        activeFopListener = null;
        clearFopHistory();
    }

    private void resetAlignment() {
        samplingOffsetAnchor = Double.NaN;
        lastRawMagHeading = Double.NaN;
        clearFopHistory();
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
        SamplingSession session = activeSamplingSession;
        if (session != null && !payload.equals(session.payload)) {
            log(String.format(Locale.US,
                    "[sampling] Payload changed mid-session: session=%s detected=%s -> aborting session %d",
                    session.payload,
                    payload,
                    session.id));
            abortActiveSampling("QR changed during sampling. Try again.");
        }

        if (payload.equals(lastPayload)) return;
        lastPayload = payload;
        currentRegistration = null;

        QRDatabaseHelper.Registration record = QRDatabaseHelper.getInstance(this).fetchRegistration(payload);

        if (record != null) {
            currentRegistration = record;
            setMode(ScanMode.VALIDATE);
            dbBadge.setText("📦 Loaded from DB  (registered " + relativeTime(record.registeredAt) + ")");
            showStatus("QR Detected ✓  —  Registration found in DB!\nPress VALIDATE to check orientation.");
            updateInfoLabel(null, null, null, null, record, null, InfoSource.DATABASE);
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
        startSampling(SamplingPurpose.REGISTER, lastPayload);
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
        QRDatabaseHelper.Registration record = QRDatabaseHelper.getInstance(this).fetchRegistration(lastPayload);
        if (record == null) {
            showStatus("⚠️ No registration in DB for this QR.\nPress REGISTER first.");
            setMode(ScanMode.REGISTER);
            return;
        }
        if (!hasFullOrientation(record)) {
            showStatus("⚠️ This QR was registered before pitch/roll tracking was added.\nPress REGISTER to save full orientation again.");
            setMode(ScanMode.REGISTER);
            return;
        }
        if (isSampling) return;
        startSampling(SamplingPurpose.VALIDATE, lastPayload);
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
            msg.append(formatAngleLine("Pitch", reg.pitch, "n/a (legacy)").replace(":", " :")).append("\n");
            msg.append(formatAngleLine("Roll", reg.roll, "n/a (legacy)").replace(":", " :")).append("\n");
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
                msg.append(String.format(Locale.US, "%s %s   ΔYaw %+.1f°",
                        icon, relativeTime(v.validatedAt), v.delta));
                if (!Double.isNaN(v.deltaPitch)) {
                    msg.append(String.format(Locale.US, "  ΔPitch %+.1f°", v.deltaPitch));
                }
                if (!Double.isNaN(v.deltaRoll)) {
                    msg.append(String.format(Locale.US, "  ΔRoll %+.1f°", v.deltaRoll));
                }
                msg.append("\n");
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
    private String samplingActionLabel() {
        return (purpose == SamplingPurpose.REGISTER) ? "Registering" : "Validating";
    }

    private SharedPreferences getOffsetPreferences() {
        return getSharedPreferences(OFFSET_PREFS_NAME, MODE_PRIVATE);
    }

    private String offsetKey(String payload) {
        return OFFSET_KEY_PREFIX + payload;
    }

    private String offsetModeKey(String payload) {
        return OFFSET_MODE_KEY_PREFIX + payload;
    }

    private OffsetMode readStoredOffsetMode(String payload) {
        if (payload == null) return OffsetMode.UNKNOWN;
        String rawValue = getOffsetPreferences().getString(offsetModeKey(payload), null);
        if (rawValue == null) return OffsetMode.UNKNOWN;
        try {
            return OffsetMode.valueOf(rawValue);
        } catch (IllegalArgumentException ignored) {
            return OffsetMode.UNKNOWN;
        }
    }

    private Double readStoredRelativeOffset(String payload) {
        if (payload == null) return null;
        SharedPreferences prefs = getOffsetPreferences();
        String key = offsetKey(payload);
        if (!prefs.contains(key)) return null;

        float offset = prefs.getFloat(key, Float.NaN);
        return Float.isNaN(offset) ? null : (double) offset;
    }

    private void persistOffsetMetadata(String payload, boolean isAbsoluteNorth, double offsetUsed) {
        if (payload == null) return;

        SharedPreferences.Editor editor = getOffsetPreferences().edit()
                .putString(offsetModeKey(payload),
                        isAbsoluteNorth ? OffsetMode.ABSOLUTE.name() : OffsetMode.RELATIVE.name());

        if (isAbsoluteNorth || Double.isNaN(offsetUsed)) {
            editor.remove(offsetKey(payload));
        } else {
            editor.putFloat(offsetKey(payload), (float) offsetUsed);
        }
        editor.apply();
    }

    private String normalizeMeasurementFamily(String method) {
        if (method == null) return "unknown";

        boolean absoluteNorth = isAbsoluteNorthMethod(method);
        if (method.startsWith("wall-")) {
            return absoluteNorth ? "wall-direct" : "wall-relative";
        }
        if (method.startsWith("floor-")) {
            return absoluteNorth ? "floor-direct" : "floor-relative";
        }
        return absoluteNorth ? "direct" : "relative";
    }

    private double resolveActiveOffsetAnchor(SamplingSession session) {
        if (session != null && !Double.isNaN(session.offsetAnchor)) {
            return session.offsetAnchor;
        }
        return samplingOffsetAnchor;
    }

    private void resetSamplingState() {
        sampleBuffer.clear();
        magValidationBuffer.clear();
        magSamplesDisplay = null;
        lastMeasurementMethod = null;
        lastSampleTime = 0;
        consecutiveNonDirectFallbacks = 0;
        samplingOffsetAnchor = Double.NaN;
        activeSamplingSession = null;
        isSampling = false;
        isCollectingMag = false;
    }

    private void abortActiveSampling(String reason) {
        if (!isSampling && activeSamplingSession == null && sampleBuffer.isEmpty() && magValidationBuffer.isEmpty()) {
            return;
        }

        resetSamplingState();
        if (reason != null && !reason.isEmpty()) {
            showStatus(reason);
        }
        runOnUiThread(() -> dbBadge.setText(""));
    }

    private void beginMagAnchorCollection(String reason) {
        sampleBuffer.clear();
        magValidationBuffer.clear();
        magSamplesDisplay = null;
        lastMeasurementMethod = null;
        lastSampleTime = 0;
        consecutiveNonDirectFallbacks = 0;
        isCollectingMag = true;

        log("[sampling] Starting magnetic north collection: " + reason);

        String action = samplingActionLabel();
        showStatus(action + "...\nFinding Magnetic North (0/" + MAG_SAMPLES_REQUIRED + ")");
        dbBadge.setText("Mag Init...");
    }

    private void startSampling(SamplingPurpose p, String payloadSnapshot) {
        if (payloadSnapshot == null) {
            showStatus("⚠️ QR payload not decoded yet.");
            return;
        }

        resetSamplingState();
        purpose = p;
        activeSamplingSession = new SamplingSession(
                nextSamplingSessionId++,
                payloadSnapshot,
                p,
                System.currentTimeMillis());
        isSampling = true;
        isCollectingMag = true;

        String action = (p == SamplingPurpose.REGISTER) ? "Registering" : "Validating";
        showStatus("🧭 " + action + "...\nFinding Magnetic North (0/" + MAG_SAMPLES_REQUIRED + ")");
        dbBadge.setText("🔄 Mag Init...");

        arSceneView.postDelayed(() -> {
            SamplingSession session = activeSamplingSession;
            if (!isSampling || session == null || !payloadSnapshot.equals(session.payload)) return;
            isCollectingMag = true;
            showStatus("🧭 " + action + "...\nFinding Magnetic North (0/" + MAG_SAMPLES_REQUIRED + ")");
            dbBadge.setText("🔄 Mag Init...");
        }, 1500);
    }

    private void collectSampleWithPose(float[] imagePoints, float[] camPose,
                                       CameraIntrinsics intrinsics,
                                       ResolvedFopAttitude resolvedFopAttitude,
                                       String framePayload) {
        SamplingSession session = activeSamplingSession;
        if (session == null || !isSampling) return;

        if (resolvedFopAttitude == null) {
            log("[collectSampleWithPose] Waiting for aligned FOP before accepting samples");
            return;
        }
        if (!isSampling || session == null) {
            log("[collectSampleWithPose] No active sampling session - skipping frame");
            return;
        }
        if (!session.payload.equals(framePayload)) {
            log(String.format(Locale.US,
                    "[collectSampleWithPose] Frame payload=%s does not match active session payload=%s - skipping frame",
                    framePayload,
                    session.payload));
            return;
        }

        log("[collectSampleWithPose] Using pre-captured pose. camOrigin=("
                + String.format(Locale.US, "%.3f, %.3f, %.3f", camPose[12], camPose[13], camPose[14]) + ")");
        if (resolvedFopAttitude != null) {
            double syncDeltaMs = Math.abs(resolvedFopAttitude.targetTimestampNs - resolvedFopAttitude.referenceTimestampNs) / 1_000_000.0;
            log(String.format(Locale.US,
                    "[collectSampleWithPose] FOP sync strategy=%s delta=%.2fms",
                    resolvedFopAttitude.strategy,
                    syncDeltaMs));
        } else {
            log("[collectSampleWithPose] No time-aligned FOP sample available");
        }

        QRMeasurement result = computeQRYawFromTopEdgeWithPose(
                imagePoints,
                camPose,
                intrinsics,
                resolvedFopAttitude != null ? resolvedFopAttitude.attitude : null);

        if (result == null) {
            log("[collectSampleWithPose] computeQRYaw returned null — skipping");
            return;
        }

        log(String.format(Locale.US, "[collectSampleWithPose] yaw=%.1f° pitch=%.1f° roll=%.1f° conf=%.2f method=%s",
                result.yaw, result.pitch, result.roll, result.confidence, result.method));
        processSample(result);
    }

    private void logSampleBuffer() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n=== QR NORTH SAMPLES [").append(sampleBuffer.size()).append("/").append(REQUIRED_SAMPLES).append("] ===\n");
        sb.append(String.format(Locale.US, "%-6s | %-10s | %-8s | %-8s | %-8s | %-8s\n",
                "ID", "NORTH", "RAW", "PITCH", "ROLL", "WEIGHT"));
        sb.append("----------------------------------------------------------------\n");
        for (int i = 0; i < sampleBuffer.size(); i++) {
            YawSample s = sampleBuffer.get(i);
            sb.append(String.format(Locale.US, "#%-5d | %-8.2f°  | %-8.2f° | %-8.2f° | %-8.2f° | %-6.2f\n",
                    i + 1, s.northYaw, s.yaw, s.pitch, s.roll, s.weight));
        }
        Log.i("SAMPLES", sb.toString());
    }

    private void updateFullSampleListUI() {
        StringBuilder sb = new StringBuilder();
        sb.append("Sampling QR orientation\n");
        sb.append("-----------------------\n");
        for (int i = 0; i < sampleBuffer.size(); i++) {
            YawSample s = sampleBuffer.get(i);
            String indicator = (i == sampleBuffer.size() - 1) ? "> " : "  ";
            sb.append(String.format(Locale.US, "%sSample #%d: yaw %.2f°  roll %.2f°  pitch %.2f°  (w: %.2f)\n",
                    indicator, i + 1, s.northYaw, s.roll, s.pitch, s.weight));
        }
        int progress = sampleBuffer.size();
        sb.append("\nProgress: [");
        for (int i = 0; i < REQUIRED_SAMPLES; i++) {
            sb.append(i < progress ? "*" : ".");
        }
        sb.append("] ").append(progress).append("/").append(REQUIRED_SAMPLES);
        runOnUiThread(() -> infoText.setText(sb.toString()));
    }

    private void processSample(QRMeasurement result) {
        SamplingSession session = activeSamplingSession;
        if (!isSampling || session == null) {
            log("[processSample] No active sampling session - dropping sample");
            return;
        }

        double sampleConfidence = result.confidence;
        if (isWallDirectMethod(result.method) && sampleBuffer.size() < WALL_EARLY_SAMPLE_CAP_COUNT) {
            double cappedConfidence = Math.min(sampleConfidence, WALL_EARLY_SAMPLE_CONFIDENCE_CAP);
            if (cappedConfidence < sampleConfidence) {
                log(String.format(Locale.US,
                        "[processSample] Early wall direct sample confidence capped: raw=%.2f capped=%.2f sampleIndex=%d",
                        sampleConfidence, cappedConfidence, sampleBuffer.size() + 1));
                sampleConfidence = cappedConfidence;
            }
        }

        if (sampleConfidence < MIN_SAMPLE_WEIGHT) {
            log(String.format(Locale.US,
                    "Sample rejected (confidence %.2f < %.2f)",
                    sampleConfidence, MIN_SAMPLE_WEIGHT));
            return;
        }

        if (Double.isNaN(result.roll)) {
            log("[processSample] Sample rejected — roll unavailable for this frame");
            return;
        }

        boolean sampleAbsoluteNorth = isAbsoluteNorthMethod(result.method);
        String sampleMethodFamily = normalizeMeasurementFamily(result.method);
        if (session.lockedMethod == null) {
            session.lockedMethod = result.method;
            session.lockedMethodFamily = sampleMethodFamily;
            session.absoluteNorth = sampleAbsoluteNorth;
            lastMeasurementMethod = result.method;
            log(String.format(Locale.US,
                    "[processSample] Locked session=%d method=%s family=%s payload=%s",
                    session.id,
                    result.method,
                    sampleMethodFamily,
                    session.payload));
        } else if (!sampleMethodFamily.equals(session.lockedMethodFamily)
                || session.absoluteNorth == null
                || session.absoluteNorth != sampleAbsoluteNorth) {
            log(String.format(Locale.US,
                    "[processSample] Rejected method mismatch: session=%d locked=%s/%s incoming=%s/%s",
                    session.id,
                    session.lockedMethod,
                    session.lockedMethodFamily,
                    result.method,
                    sampleMethodFamily));
            return;
        }

        boolean isAbsoluteNorth = Boolean.TRUE.equals(session.absoluteNorth);
        double activeOffset = resolveActiveOffsetAnchor(session);

        double sampleNorth;
        if (isAbsoluteNorth) {
            sampleNorth = result.yaw;
        } else {
            if (Double.isNaN(activeOffset)) {
                sampleNorth = result.yaw;
            } else {
                sampleNorth = (result.yaw + activeOffset) % 360.0;
                if (sampleNorth < 0) sampleNorth += 360.0;
            }
        }

        double sampleRoll;
        if (result.rollAbsolute) {
            sampleRoll = normalizeDegrees(result.roll);
        } else {
            sampleRoll = Double.isNaN(activeOffset)
                    ? normalizeDegrees(result.roll)
                    : normalizeDegrees(result.roll + activeOffset);
        }

        if (!sampleBuffer.isEmpty()) {
            double yawReference = isAbsoluteNorth
                    ? circularWeightedMeanNorth(sampleBuffer)
                    : circularWeightedMeanRaw(sampleBuffer);
            double yawCompareValue = isAbsoluteNorth ? sampleNorth : result.yaw;
            double yawDiff = Math.abs(angleDifference(yawReference, yawCompareValue));
            double rollReference = circularWeightedMeanRoll(sampleBuffer);
            double rollDiff = Math.abs(angleDifference(rollReference, sampleRoll));
            double pitchReference = weightedMeanPitch(sampleBuffer);
            double pitchDiff = Math.abs(pitchReference - result.pitch);
            double outlierThreshold = isAbsoluteNorth
                    ? DIRECT_METHOD_OUTLIER_THRESHOLD_DEG
                    : 45.0;

            if (yawDiff > outlierThreshold
                    || rollDiff > outlierThreshold
                    || pitchDiff > PITCH_OUTLIER_THRESHOLD_DEG) {
                if (sampleConfidence >= DIRECT_SAMPLE_RESET_CONFIDENCE
                        && sampleBuffer.get(0).weight < 0.5) {
                    log("[processSample] 🌟 High-confidence plane found! Discarding old low-confidence buffer.");
                    sampleBuffer.clear();
                } else {
                    log(String.format(Locale.US,
                            "[processSample] OUTLIER REJECTED: yaw %.1f°/%.1f° diff=%.1f°, roll %.1f°/%.1f° diff=%.1f°, pitch %.1f°/%.1f° diff=%.1f°",
                            yawCompareValue, yawReference, yawDiff,
                            sampleRoll, rollReference, rollDiff,
                            result.pitch, pitchReference, pitchDiff));
                    return;
                }
            } else {
                log(String.format(Locale.US,
                        "[processSample] Sample OK: yaw %.1f°/%.1f° diff=%.1f°, roll %.1f°/%.1f° diff=%.1f°, pitch %.1f°/%.1f° diff=%.1f°",
                        yawCompareValue, yawReference, yawDiff,
                        sampleRoll, rollReference, rollDiff,
                        result.pitch, pitchReference, pitchDiff));
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

        sampleBuffer.add(new YawSample(result.yaw, sampleNorth, result.pitch, sampleRoll, sampleConfidence));

        log(String.format(Locale.US,
                "  Sample %d: raw=%.1f° north=%.1f° pitch=%.1f° roll=%.1f° conf=%.2f [%s]",
                sampleBuffer.size(), result.yaw, sampleNorth,
                result.pitch, sampleRoll, sampleConfidence, result.method));

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
                        "#%d raw=%.1f° north=%.1f° pitch=%.1f° roll=%.1f°(w=%.2f) ",
                        i + 1, s.yaw, s.northYaw, s.pitch, s.roll, s.weight));
            }
            log(sb.toString());

            // Direct FOP methods average absolute north; fallback methods still average raw yaw.
            // Floor: average north yaws, pass directly
            double finalYaw;
            if (isAbsoluteNorth) {
                finalYaw = circularWeightedMeanNorth(sampleBuffer);
            } else {
                finalYaw = circularWeightedMeanRaw(sampleBuffer);
            }
            double finalPitch = weightedMeanPitch(sampleBuffer);
            double finalRoll = circularWeightedMeanRoll(sampleBuffer);

            log(String.format(Locale.US,
                    "Sampling complete. Averaged %s yaw = %.2f°, pitch = %.2f°, roll = %.2f° from %d samples",
                    isAbsoluteNorth ? "north" : "raw", finalYaw, finalPitch, finalRoll, sampleBuffer.size()));

            sampleBuffer.clear();
            commitReading(session, finalYaw, finalPitch, finalRoll);
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

    private void commitReading(SamplingSession session, double inputYaw, double inputPitch, double inputRoll) {
        if (session == null || session.payload == null) return;

        String payload = session.payload;
        boolean isAbsoluteNorth = Boolean.TRUE.equals(session.absoluteNorth);
        OffsetMode currentOffsetMode = isAbsoluteNorth ? OffsetMode.ABSOLUTE : OffsetMode.RELATIVE;

        double offsetUsed;
        double finalNorthYaw;
        double finalPitch = inputPitch;
        double finalRoll = normalizeDegrees(inputRoll);

        if (isAbsoluteNorth) {
            offsetUsed = 0.0;
            finalNorthYaw = inputYaw;
        } else {
            offsetUsed = resolveActiveOffsetAnchor(session);
            if (!Double.isNaN(offsetUsed)) {
                finalNorthYaw = (inputYaw + offsetUsed) % 360.0;
                if (finalNorthYaw < 0) finalNorthYaw += 360.0;
            } else {
                finalNorthYaw = inputYaw;
            }
        }

        log("COMMIT READING DEBUG:");
        log(" -> Session ID: " + session.id);
        log(" -> Payload: " + payload);
        log(" -> Input Yaw: " + inputYaw + "°");
        log(" -> Input Pitch: " + inputPitch + "°");
        log(" -> Input Roll: " + inputRoll + "°");
        log(" -> Offset USED: " + offsetUsed + "°");
        log(" -> FINAL COMPASS BEARING: " + finalNorthYaw + "°");
        log(" -> FINAL PITCH: " + finalPitch + "°");
        log(" -> FINAL ROLL: " + finalRoll + "°");

        activeSamplingSession = null;
        lastMeasurementMethod = null;
        samplingOffsetAnchor = Double.NaN;

        double yawRad = Math.toRadians(finalNorthYaw);
        double pitchRad = Math.toRadians(finalPitch);
        double horizScale = Math.cos(pitchRad);
        float normalX = (float) (Math.sin(yawRad) * horizScale);
        float normalY = (float) Math.sin(pitchRad);
        float normalZ = (float) (Math.cos(yawRad) * horizScale);

        Double displayOffset = (currentOffsetMode == OffsetMode.RELATIVE && !Double.isNaN(offsetUsed))
                ? offsetUsed
                : null;

        if (session.purpose == SamplingPurpose.REGISTER) {
            persistOffsetMetadata(payload, isAbsoluteNorth, offsetUsed);

            Long dbId = QRDatabaseHelper.getInstance(this).saveRegistration(
                    payload, finalNorthYaw, finalPitch, finalRoll, normalX, normalY, normalZ, YAW_TOLERANCE);

            if (dbId == null) {
                showStatus("⚠️ Database save failed.");
                return;
            }

            QRDatabaseHelper.Registration reg = new QRDatabaseHelper.Registration();
            reg.id = dbId;
            reg.payload = payload;
            reg.yaw = finalNorthYaw;
            reg.pitch = finalPitch;
            reg.roll = finalRoll;
            reg.normalX = normalX;
            reg.normalY = normalY;
            reg.normalZ = normalZ;
            reg.tolerance = YAW_TOLERANCE;
            reg.registeredAt = new Date();
            currentRegistration = reg;

            double finalYaw = finalNorthYaw;
            double finalPitchValue = finalPitch;
            double finalRollValue = finalRoll;
            runOnUiThread(() -> {
                setMode(ScanMode.VALIDATE);
                dbBadge.setText("💾 Saved to DB  (id=" + dbId + ")");
                showStatus(String.format(Locale.US,
                        "✅ Registered & Saved!\nYaw = %.1f°  Pitch = %.1f°  Roll = %.1f°  |  id=%d",
                        finalYaw, finalPitchValue, finalRollValue, dbId));
                updateInfoLabel(finalYaw, finalPitchValue, finalRollValue, displayOffset, reg, null, InfoSource.FRESH_REGISTER);
            });

            log(String.format(Locale.US, "Registered → payload=%s…  yaw=%.1f°  pitch=%.1f°  roll=%.1f°  dbId=%d",
                    payload.substring(0, Math.min(20, payload.length())), finalNorthYaw, finalPitch, finalRoll, dbId));

        } else {
            QRDatabaseHelper.Registration record = QRDatabaseHelper.getInstance(this).fetchRegistration(payload);
            if (record == null) {
                showStatus("⚠️ No previous registration found.\nPress REGISTER first.");
                setMode(ScanMode.REGISTER);
                return;
            }

            if (!hasFullOrientation(record)) {
                showStatus("⚠️ This QR was registered before pitch/roll tracking was added.\nPress REGISTER to save full orientation again.");
                setMode(ScanMode.REGISTER);
                return;
            }

            OffsetMode storedOffsetMode = readStoredOffsetMode(payload);
            Double storedOffset = readStoredRelativeOffset(payload);
            if (storedOffsetMode == OffsetMode.RELATIVE
                    && currentOffsetMode == OffsetMode.RELATIVE
                    && storedOffset != null
                    && !Double.isNaN(offsetUsed)) {
                double offsetDelta = Math.abs(angleDifference(storedOffset, offsetUsed));
                boolean offsetDrifted = offsetDelta > 10.0;
                log(String.format(Locale.US, "[VALIDATE] Offset check: regOffset=%.1f° currentOffset=%.1f° delta=%.1f° drifted=%b",
                        storedOffset, offsetUsed, offsetDelta, offsetDrifted));
            } else {
                log(String.format(Locale.US,
                        "[VALIDATE] Offset check skipped: storedMode=%s currentMode=%s regOffset=%s currentOffset=%s",
                        storedOffsetMode,
                        currentOffsetMode,
                        formatAngleForLog(storedOffset != null ? storedOffset : Double.NaN),
                        formatAngleForLog(offsetUsed)));
            }

            double yawDelta = angleDifference(record.yaw, finalNorthYaw);
            double pitchDelta = finalPitch - record.pitch;
            double rollDelta = angleDifference(record.roll, finalRoll);

// Apply separate tolerances per axis
            boolean yawWithin = Math.abs(yawDelta) <= YAW_TOLERANCE;
            boolean pitchWithin = Math.abs(pitchDelta) <= PITCH_TOLERANCE;
            boolean rollWithin = Math.abs(rollDelta) <= ROLL_TOLERANCE;
            boolean within = yawWithin && pitchWithin && rollWithin;

            log(String.format(Locale.US,
                    "[VALIDATE] reg(yaw=%.1f° pitch=%.1f° roll=%.1f°) current(yaw=%.1f° pitch=%.1f° roll=%.1f°) " +
                            "delta(yaw=%+.1f° pitch=%+.1f° roll=%+.1f°) tolerance=±%.0f° result=%s",
                    record.yaw, record.pitch, record.roll,
                    finalNorthYaw, finalPitch, finalRoll,
                    yawDelta, pitchDelta, rollDelta,
                    YAW_TOLERANCE, within ? "PASS" : "FAIL"));

            QRDatabaseHelper.getInstance(this).saveValidation(
                    record.id, payload,
                    finalNorthYaw, record.yaw, yawDelta,
                    finalPitch, record.pitch, pitchDelta,
                    finalRoll, record.roll, rollDelta,
                    within, YAW_TOLERANCE);

            double finalYaw1 = finalNorthYaw;
            double finalPitch1 = finalPitch;
            double finalRoll1 = finalRoll;
            ValidationSummary validationSummary = new ValidationSummary(
                    yawDelta, pitchDelta, rollDelta, YAW_TOLERANCE, within);
            runOnUiThread(() -> {
                String icon = within ? "✅" : "❌";
                String moved = within ? "NOT moved or rotated" : "MOVED / ROTATED";

                // Show exact tolerance for each axis in the UI
                showStatus(String.format(Locale.US,
                        "%s QR has %s\nΔYaw: %+.1f° (±%.0f°)\nΔPitch: %+.1f° (±%.0f°)\nΔRoll: %+.1f° (±%.0f°)",
                        icon, moved,
                        yawDelta, YAW_TOLERANCE,
                        pitchDelta, PITCH_TOLERANCE,
                        rollDelta, ROLL_TOLERANCE));

                dbBadge.setText("📝 Validation logged to DB");
                updateInfoLabel(finalYaw1, finalPitch1, finalRoll1, displayOffset, record, validationSummary, InfoSource.DATABASE);
            });

            log(String.format(Locale.US,
                    "Validate → payload=%s…\n           current=(%.1f°, %.1f°, %.1f°)\n           registered=(%.1f°, %.1f°, %.1f°)\n           Δ=(%+.1f°, %+.1f°, %+.1f°)\n           ok=(yaw=%b pitch=%b roll=%b)",
                    payload.substring(0, Math.min(20, payload.length())),
                    finalNorthYaw, finalPitch, finalRoll,
                    record.yaw, record.pitch, record.roll,
                    yawDelta, pitchDelta, rollDelta,
                    yawWithin, pitchWithin, rollWithin));
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
        VpEstimate vpEstimate = computeNormalViaVanishingPoint(
                imgPts[0], imgPts[1], imgPts[2], imgPts[3], focal, pp, camPose);
        float[] planeNormal = vpEstimate != null ? vpEstimate.normalWorld.clone() : null;
        float[] vpNormalCam = vpEstimate != null ? vpEstimate.normalCam.clone() : null;

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
            if (vpNormalCam != null) {
                vpNormalCam = vec3Scale(vpNormalCam, -1f);
            }
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
        double vpWallRawYaw = Double.NaN;
        double vpWallNorthYaw = Double.NaN;

        if (horizLen > 0.50) {
            // ─── 🧱 WALL CASE ───
            // Use the face normal projected onto horizontal plane.
            // ML Kit corners are fine here — yaw comes from normal direction, not corner identity.
            vpWallRawYaw = normalizeDegrees(Math.toDegrees(Math.atan2(hx, -hz)));
            if (!Double.isNaN(samplingOffsetAnchor)) {
                vpWallNorthYaw = normalizeDegrees(vpWallRawYaw + samplingOffsetAnchor);
            }
            QRMeasurement wallResult = computeWallYawWithSolvePnP(
                    imagePoints,
                    intr,
                    camPose,
                    fopAttitude,
                    vpNormalCam,
                    vpWallRawYaw,
                    vpWallNorthYaw
            );
            if (wallResult != null) {
                return wallResult;
            }

            QRMeasurement wallFallback = buildWallVpFallbackMeasurement(
                    imagePoints,
                    focal,
                    pp,
                    camPose,
                    fopAttitude,
                    planeNormal,
                    vpNormalCam,
                    pitch,
                    vpWallRawYaw
            );
            if (wallFallback != null) {
                return wallFallback;
            }

            log("[yawCalc] Wall QR detected, but solvePnP failed and no safe fallback was available. Dropping frame.");
            return null;

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

    }

    private VpEstimate computeNormalViaVanishingPoint(
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

            return new VpEstimate(normalCam, normalWorld);
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

    private boolean isAbsoluteNorthMethod(String method) {
        return method != null && method.endsWith("-direct");
    }

    private boolean isWallDirectMethod(String method) {
        return method != null && method.startsWith("wall-") && method.endsWith("-direct");
    }

    private double confidenceFromReprojectionError(double reprojErrorPx) {
        if (Double.isNaN(reprojErrorPx) || Double.isInfinite(reprojErrorPx) || reprojErrorPx < 0.0) {
            return 0.0;
        }

        double normalizedError = reprojErrorPx / DIRECT_REPROJECTION_CONFIDENCE_SCALE_PX;
        return 1.0 / (1.0 + normalizedError * normalizedError);
    }

    private double getWallContinuityReferenceYaw() {
        if (sampleBuffer.isEmpty()) return Double.NaN;
        if (!isWallDirectMethod(lastMeasurementMethod)) return Double.NaN;
        return circularWeightedMeanNorth(sampleBuffer);
    }

    private QRMeasurement buildWallVpFallbackMeasurement(
            float[] imagePoints,
            float[] focalLength,
            float[] principalPoint,
            float[] camPose,
            float[] fopAttitude,
            float[] planeNormalWorld,
            float[] planeNormalCam,
            double pitch,
            double vpWallRawYaw) {
        if (planeNormalCam == null) {
            log("[yawCalc] Wall VP fallback unavailable - camera-space plane normal missing");
            return null;
        }

        float[] vpUpCam = computeWallVpUpVectorInCamera(
                imagePoints,
                focalLength,
                principalPoint,
                planeNormalCam
        );
        if (vpUpCam == null) {
            log("[yawCalc] Wall VP fallback unavailable - could not derive VP up vector");
            return null;
        }

        if (fopAttitude != null) {
            log("[yawCalc] Wall solvePnP failed - skipping VP fallback because VP yaw is ARCore-relative, not FOP-aligned");
            return null;
        }

        float[] vpUpWorld = rotateCamToWorld(vpUpCam, camPose);
        double roll = computeWallRollDegrees(vpUpWorld, planeNormalWorld);
        if (Double.isNaN(roll)) {
            log("[yawCalc] Wall VP fallback unavailable - roll math returned NaN");
            return null;
        }

        // Use the 180° aligned wall-normal math so the VP fallback matches the direct wall path.
        double alignedVpYaw = normalizeDegrees(vpWallRawYaw + 180.0);
        double finalYaw = alignedVpYaw;

        // Legacy 2D offset mode still needs the sampled anchor baked in here.
        if (fopAttitude == null && !Double.isNaN(samplingOffsetAnchor)) {
            finalYaw = normalizeDegrees(alignedVpYaw + samplingOffsetAnchor);
        }

        log(String.format(Locale.US,
                "[yawCalc] Wall solvePnP failed - using VP fallback yaw=%.1f deg pitch=%.1f deg roll=%.1f deg rawVp=%.1f deg alignedVp=%.1f deg mode=%s",
                finalYaw,
                pitch,
                roll,
                vpWallRawYaw,
                alignedVpYaw,
                "legacy-offset"));
        return new QRMeasurement(finalYaw, pitch, roll, 0.8, "wall-vp", true);
    }

    private float[] computeWallVpUpVectorInCamera(
            float[] imagePoints,
            float[] focalLength,
            float[] principalPoint,
            float[] planeNormalCam) {
        float fx = focalLength[0];
        float fy = focalLength[1];
        float cx = principalPoint[0];
        float cy = principalPoint[1];

        float centerX = (imagePoints[0] + imagePoints[2] + imagePoints[4] + imagePoints[6]) * 0.25f;
        float centerY = (imagePoints[1] + imagePoints[3] + imagePoints[5] + imagePoints[7]) * 0.25f;
        float topMidX = (imagePoints[0] + imagePoints[2]) * 0.5f;
        float topMidY = (imagePoints[1] + imagePoints[3]) * 0.5f;

        float[] normalCam = vec3Normalize(planeNormalCam);
        float[] centerRay = toCameraRay(new float[]{centerX, centerY, 1f}, fx, fy, cx, cy);
        float[] topMidRay = toCameraRay(new float[]{topMidX, topMidY, 1f}, fx, fy, cx, cy);

        float centerDenom = vec3Dot(centerRay, normalCam);
        float topDenom = vec3Dot(topMidRay, normalCam);
        if (Math.abs(centerDenom) < 1e-6f || Math.abs(topDenom) < 1e-6f) {
            return null;
        }

        // Plane scale is arbitrary here; n·X = 1 is enough to recover an in-plane direction.
        float[] centerPoint = vec3Scale(centerRay, 1f / centerDenom);
        float[] topPoint = vec3Scale(topMidRay, 1f / topDenom);
        float[] upCam = new float[]{
                topPoint[0] - centerPoint[0],
                topPoint[1] - centerPoint[1],
                topPoint[2] - centerPoint[2]
        };

        float normalComponent = vec3Dot(upCam, normalCam);
        upCam = new float[]{
                upCam[0] - normalCam[0] * normalComponent,
                upCam[1] - normalCam[1] * normalComponent,
                upCam[2] - normalCam[2] * normalComponent
        };

        if (vec3Len(upCam) < 1e-5f) {
            return null;
        }
        return vec3Normalize(upCam);
    }

    private List<float[]> toCornerList(float[] points) {
        List<float[]> corners = new ArrayList<>(4);
        for (int i = 0; i < 4; i++) {
            corners.add(new float[]{points[i * 2], points[i * 2 + 1]});
        }
        return corners;
    }

    private float[] captureImageToViewBasis(Frame frame, int imageWidth, int imageHeight) {
        float[] srcBasis = new float[]{
                0f, 0f,
                (float) imageWidth, 0f,
                0f, (float) imageHeight
        };
        float[] dstBasis = new float[6];
        try {
            frame.transformCoordinates2d(
                    com.google.ar.core.Coordinates2d.IMAGE_PIXELS, srcBasis,
                    com.google.ar.core.Coordinates2d.VIEW, dstBasis
            );
            return dstBasis;
        } catch (Exception e) {
            Log.w(TAG, "captureImageToViewBasis failed", e);
            return null;
        }
    }

    private float[] mapImagePointsToView(float[] imagePoints, float[] viewBasis, int imageWidth, int imageHeight) {
        if (viewBasis == null || imagePoints == null || imagePoints.length != 8 || imageWidth <= 0 || imageHeight <= 0) {
            return null;
        }

        float originX = viewBasis[0];
        float originY = viewBasis[1];
        float basisXX = viewBasis[2] - originX;
        float basisXY = viewBasis[3] - originY;
        float basisYX = viewBasis[4] - originX;
        float basisYY = viewBasis[5] - originY;

        float[] viewPoints = new float[imagePoints.length];
        for (int i = 0; i < imagePoints.length; i += 2) {
            float u = imagePoints[i] / (float) imageWidth;
            float v = imagePoints[i + 1] / (float) imageHeight;
            viewPoints[i] = originX + basisXX * u + basisYX * v;
            viewPoints[i + 1] = originY + basisXY * u + basisYY * v;
        }
        return viewPoints;
    }

    private float[] refineCornersSubPixel(Mat grayMat, float[] imagePoints) {
        for (int i = 0; i < imagePoints.length; i += 2) {
            float x = imagePoints[i];
            float y = imagePoints[i + 1];
            if (x < SUBPIX_WINDOW_RADIUS || y < SUBPIX_WINDOW_RADIUS
                    || x >= grayMat.cols() - SUBPIX_WINDOW_RADIUS
                    || y >= grayMat.rows() - SUBPIX_WINDOW_RADIUS) {
                log("[subpix] Skipping refinement - corner too close to image edge");
                return imagePoints;
            }
        }

        MatOfPoint2f corners = new MatOfPoint2f(
                new Point(imagePoints[0], imagePoints[1]),
                new Point(imagePoints[2], imagePoints[3]),
                new Point(imagePoints[4], imagePoints[5]),
                new Point(imagePoints[6], imagePoints[7])
        );

        try {
            Imgproc.cornerSubPix(
                    grayMat,
                    corners,
                    new Size(SUBPIX_WINDOW_RADIUS, SUBPIX_WINDOW_RADIUS),
                    new Size(-1, -1),
                    new TermCriteria(
                            TermCriteria.MAX_ITER + TermCriteria.EPS,
                            SUBPIX_MAX_ITER,
                            SUBPIX_EPSILON)
            );

            Point[] refined = corners.toArray();
            float[] refinedPoints = new float[imagePoints.length];
            double totalShift = 0.0;
            double maxShift = 0.0;
            for (int i = 0; i < refined.length; i++) {
                int xIndex = i * 2;
                int yIndex = xIndex + 1;
                refinedPoints[xIndex] = (float) refined[i].x;
                refinedPoints[yIndex] = (float) refined[i].y;

                double dx = refinedPoints[xIndex] - imagePoints[xIndex];
                double dy = refinedPoints[yIndex] - imagePoints[yIndex];
                double shift = Math.hypot(dx, dy);
                totalShift += shift;
                maxShift = Math.max(maxShift, shift);
            }

            long now = System.currentTimeMillis();
            if (now - lastSubPixLogTime >= SUBPIX_LOG_INTERVAL_MS) {
                lastSubPixLogTime = now;
                log(String.format(Locale.US,
                        "[subpix] Refined %d corners avgShift=%.3fpx maxShift=%.3fpx",
                        refined.length, totalShift / refined.length, maxShift));
            }
            return refinedPoints;
        } catch (Exception e) {
            Log.w(TAG, "cornerSubPix failed; using detector corners", e);
            return imagePoints;
        } finally {
            corners.release();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Continuous QR Detection via ML Kit + ZXing
    // ─────────────────────────────────────────────────────────────────────────
    private void processARFrame(Frame frame) {
        if (!isProcessingFrame.compareAndSet(false, true)) return;
        scanQrFromArFrame(frame);
    }

    @OptIn(markerClass = ExperimentalGetImage.class)
    private void scanQrFromArFrame(Frame frame) {
        android.media.Image cameraImage = null;
        try {
            cameraImage = frame.acquireCameraImage();
            final long capturedImageTimestampNs = cameraImage.getTimestamp();
            final long capturedAcquireElapsedRealtimeNs = SystemClock.elapsedRealtimeNanos();

            CameraIntrinsics capturedIntrinsics = frame.getCamera().getImageIntrinsics();
            float[] capturedPose = new float[16];
            frame.getCamera().getPose().toMatrix(capturedPose, 0);

            // 1. Extract Grayscale (Y) plane SAFELY (Strips hardware padding)
            android.media.Image.Plane yPlane = cameraImage.getPlanes()[0];
            java.nio.ByteBuffer yBuffer = yPlane.getBuffer();
            int yRowStride = yPlane.getRowStride();
            int width = cameraImage.getWidth();
            int height = cameraImage.getHeight();
            final float[] capturedImageToViewBasis = captureImageToViewBasis(frame, width, height);

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

            // 2. Offload to background thread
            qrExecutor.execute(() -> {
                org.opencv.core.Mat grayMat = null;
                List<org.opencv.core.Mat> points = new ArrayList<>();
                try {
                    // Initialize Mat with the perfectly sized byte array
                    grayMat = new org.opencv.core.Mat(height, width, org.opencv.core.CvType.CV_8UC1);
                    grayMat.put(0, 0, yData);

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
                                    abortActiveSampling("⚠️ QR lost during sampling. Try again.");
                                }
                            });
                        }

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
                    float[] detectorImagePoints = new float[]{
                            cornerData[0], cornerData[1],
                            cornerData[2], cornerData[3],
                            cornerData[4], cornerData[5],
                            cornerData[6], cornerData[7]
                    };
                    final float[] imagePoints = refineCornersSubPixel(grayMat, detectorImagePoints);

                    float[] viewPoints = mapImagePointsToView(imagePoints, capturedImageToViewBasis, width, height);
                    List<float[]> cornersList = viewPoints != null ? toCornerList(viewPoints) : null;

                    final ResolvedFopAttitude resolvedFop = isSampling && !isCollectingMag
                            ? resolveFopAttitudeForFrame(capturedImageTimestampNs, capturedAcquireElapsedRealtimeNs)
                            : null;

                    runOnUiThread(() -> {
                        lastDetectedCorners = cornersList;
                        overlayView.setCorners(cornersList);
                        onQRPayloadDetected(payload);

                        if (isSampling && !isCollectingMag) {
                            collectSampleWithPose(imagePoints, capturedPose, capturedIntrinsics, resolvedFop, payload);
                        }
                    });

                } catch (Exception e) {
                    // If anything inside the thread crashes, log it here!
                    Log.e(TAG, "FATAL ERROR inside QR Executor thread!", e);
                } finally {
                    if (grayMat != null) grayMat.release();
                    for (org.opencv.core.Mat m : points) m.release();
                    // 🚨 GUARANTEES THE APP NEVER DEADLOCKS 🚨
                    isProcessingFrame.set(false);
                }
            });

        } catch (Exception e) {
            isProcessingFrame.set(false);
            Log.w(TAG, "scanQrFromArFrame outer error: " + e.getMessage());
        } finally {
            if (cameraImage != null) {
                try {
                    cameraImage.close();
                } catch (Exception closeError) {
                    Log.w(TAG, "cameraImage.close failed", closeError);
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // UI Helpers
    // ─────────────────────────────────────────────────────────────────────────
    private void updateInfoLabel(Double finalYaw, Double finalPitch, Double finalRoll, Double currentOffset,
                                 QRDatabaseHelper.Registration reg, ValidationSummary validation, InfoSource source) {
        List<String> lines = new ArrayList<>();
        String payloadForDisplay = reg != null && reg.payload != null ? reg.payload : lastPayload;

        if (magSamplesDisplay != null) {
            lines.add(magSamplesDisplay);
            lines.add("──────────────────────────");
        }

        if (payloadForDisplay != null) {
            lines.add("QR      : " + payloadForDisplay.substring(0, Math.min(38, payloadForDisplay.length())));
            lines.add("──────────────────────────");
        }

        if (reg != null) {
            OffsetMode regOffsetMode = readStoredOffsetMode(reg.payload);
            Double regOffset = readStoredRelativeOffset(reg.payload);

            lines.add("💾 REGISTRATION DATA:");
            lines.add("Source  : " + (source == InfoSource.DATABASE ? "📦 DB" : "🆕 New")
                    + "  (" + relativeTime(reg.registeredAt) + ")");

            if (regOffsetMode == OffsetMode.RELATIVE && regOffset != null) {
                double rawRegYaw = (reg.yaw - regOffset + 360.0) % 360.0;
                lines.add(String.format(Locale.US, "Math    : [%.1f° raw] + [%.1f° offset]", rawRegYaw, regOffset));
            }
            lines.add(String.format(Locale.US, "Reg Yaw : %.1f°", reg.yaw));
            lines.add(formatAngleLine("Reg Pitch", reg.pitch, "n/a (legacy registration)"));
            lines.add(formatAngleLine("Reg Roll", reg.roll, "n/a (re-register to enable rotation checks)"));
            lines.add("──────────────────────────");
        }

        if (finalYaw != null || finalPitch != null || finalRoll != null) {
            lines.add("CURRENT SCAN DATA:");
            if (currentOffset != null) {
                lines.add(String.format(Locale.US, "Offset  : %.1f°", currentOffset));
            }
            if (finalYaw != null) {
                lines.add(String.format(Locale.US, "Cur Yaw : %.1f°", finalYaw));
            }
            if (finalPitch != null) {
                lines.add(String.format(Locale.US, "Cur Pitch: %.1f°", finalPitch));
            }
            if (finalRoll != null) {
                lines.add(String.format(Locale.US, "Cur Roll : %.1f°", finalRoll));
            }
        }

        if (validation != null) {
            lines.add("──────────────────────────");
            lines.add(String.format(Locale.US, "Δ Yaw   : %+.1f°  (tol ±%.0f°)", validation.yawDelta, YAW_TOLERANCE));
            lines.add(String.format(Locale.US, "Δ Pitch : %+.1f°  (tol ±%.0f°)", validation.pitchDelta, PITCH_TOLERANCE));
            lines.add(String.format(Locale.US, "Δ Roll  : %+.1f°  (tol ±%.0f°)", validation.rollDelta, ROLL_TOLERANCE));
            lines.add("Result  : " + (validation.withinTolerance ? "✅ SAME POSITION + ORIENTATION" : "❌ MOVED OR ROTATED"));
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

    private double normalizeDegrees(double angle) {
        angle %= 360.0;
        if (angle < 0) angle += 360.0;
        return angle;
    }

    private String formatAngleForLog(double angle) {
        return Double.isNaN(angle)
                ? "n/a"
                : String.format(Locale.US, "%.1f°", angle);
    }

    private boolean hasFullOrientation(QRDatabaseHelper.Registration registration) {
        return registration != null
                && !Double.isNaN(registration.pitch)
                && !Double.isNaN(registration.roll);
    }

    private String formatAngleLine(String label, double angle, String fallback) {
        return Double.isNaN(angle)
                ? String.format(Locale.US, "%-9s: %s", label, fallback)
                : String.format(Locale.US, "%-9s: %.1f°", label, angle);
    }

    private double computeWallRollDegrees(float[] upWorld, float[] normalWorld) {
        float[] referenceUp = new float[]{
                -normalWorld[0] * normalWorld[1],
                1f - normalWorld[1] * normalWorld[1],
                -normalWorld[2] * normalWorld[1]
        };
        if (vec3Len(referenceUp) < 1e-4f) {
            return Double.NaN;
        }

        referenceUp = vec3Normalize(referenceUp);
        float[] referenceRight = vec3Normalize(vec3Cross(normalWorld, referenceUp));
        if (vec3Len(referenceRight) < 1e-4f) {
            return Double.NaN;
        }

        double cosAngle = vec3Dot(upWorld, referenceUp);
        double sinAngle = vec3Dot(upWorld, referenceRight);
        return normalizeDegrees(Math.toDegrees(Math.atan2(sinAngle, cosAngle)));
    }

    private QRMeasurement computeFloorYawWithSolvePnP(
            float[] imagePoints,
            CameraIntrinsics intrinsics,
            float[] camPose,
            float[] fopAttitude) {
        Mat cameraMatrix = null;
        MatOfDouble distCoeffs = null;
        MatOfPoint3f objectPoints = null;
        MatOfPoint2f imagePts = null;
        Mat pnpAux1 = null;
        Mat pnpAux2 = null;
        Mat pnpAux3 = null;
        List<Mat> rvecs = new ArrayList<>();
        List<Mat> tvecs = new ArrayList<>();
        try {
            float[] focal = intrinsics.getFocalLength();
            float[] pp = intrinsics.getPrincipalPoint();

            double fx = focal[0];
            double fy = focal[1];
            double cx = pp[0];
            double cy = pp[1];

            cameraMatrix = new Mat(3, 3, CvType.CV_64F);
            cameraMatrix.put(0, 0,
                    fx, 0, cx,
                    0, fy, cy,
                    0, 0, 1);

            distCoeffs = new MatOfDouble(0, 0, 0, 0, 0);

            double half = (float) 0.1 / 2.0;

            // IPPE_SQUARE expects points in this exact square coordinate order:
            // top-left, top-right, bottom-right, bottom-left in object space.
            objectPoints = new MatOfPoint3f(
                    new Point3(-half,  half, 0),
                    new Point3( half,  half, 0),
                    new Point3( half, -half, 0),
                    new Point3(-half, -half, 0)
            );

            imagePts = new MatOfPoint2f(
                    new Point(imagePoints[0], imagePoints[1]),
                    new Point(imagePoints[2], imagePoints[3]),
                    new Point(imagePoints[4], imagePoints[5]),
                    new Point(imagePoints[6], imagePoints[7])
            );

            pnpAux1 = new Mat();
            pnpAux2 = new Mat();
            pnpAux3 = new Mat();

            int numSolutions = Calib3d.solvePnPGeneric(
                    objectPoints, imagePts, cameraMatrix, distCoeffs,
                    rvecs, tvecs, false, Calib3d.SOLVEPNP_IPPE_SQUARE,
                    pnpAux1, pnpAux2, pnpAux3
            );

            if (numSolutions == 0) {
                log("[solvePnP] Failed — no solutions");
                return null;
            }

            // === DETERMINE WHICH PATH TO USE ===
            boolean useDirect = (fopAttitude != null);

            double bestScore = Double.MAX_VALUE;
            double bestYaw = 0, bestPitch = 0, bestRoll = 0;
            double bestReprojectionError = Double.NaN;
            int bestSolutionIdx = -1;

            for (int solIdx = 0; solIdx < numSolutions; solIdx++) {
                Mat rotationMatrix = new Mat();
                try {
                    Calib3d.Rodrigues(rvecs.get(solIdx), rotationMatrix);

                    // QR normal (Z column) in OpenCV camera space
                    double normalCamX = rotationMatrix.get(0, 2)[0];
                    double normalCamY = rotationMatrix.get(1, 2)[0];
                    double normalCamZ = rotationMatrix.get(2, 2)[0];

                    // QR "up" (+Y column) in OpenCV camera space.
                    double upCamX = rotationMatrix.get(0, 1)[0];
                    double upCamY = rotationMatrix.get(1, 1)[0];
                    double upCamZ = rotationMatrix.get(2, 1)[0];

                    // === VECTOR 1: ARCORE SENSOR FRAME ===
                    // Used strictly for ARCore to disambiguate the solvePnP pose
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

                    // Force normal to point UP away from floor.
                    boolean flippedForFloor = nWorld[1] < 0;
                    if (flippedForFloor) {
                        uWorld = vec3Scale(uWorld, -1f);
                        nWorld = vec3Scale(nWorld, -1f);
                    }

                    double solPitch = Math.toDegrees(Math.asin(
                            Math.max(-1.0, Math.min(1.0, nWorld[1]))));

                    // Select solution where "Up" is closest to horizontal
                    double score = Math.abs(uWorld[1]);
                    double reprojError = computeReprojectionRmse(
                            objectPoints, imagePts, cameraMatrix, distCoeffs, rvecs.get(solIdx), tvecs.get(solIdx));

                    double solYaw;

                    if (useDirect) {
                        // === VECTOR 2: DEVICE PORTRAIT FRAME ===
                        // Used strictly for FOP absolute compass math
                        float[] upCamDevice = mapOpenCvCameraVectorToDeviceFrame(upCamX, upCamY, upCamZ);

                        float[] rotMatrix = new float[9];
                        SensorManager.getRotationMatrixFromVector(rotMatrix, fopAttitude);

                        float[] upCamFinal = upCamDevice;
                        if (flippedForFloor) {
                            upCamFinal = new float[]{-upCamDevice[0], -upCamDevice[1], -upCamDevice[2]};
                        }

                        // 1. Apply FOP rotation to get absolute earth bearing
                        float upEast = rotMatrix[0] * upCamFinal[0] + rotMatrix[1] * upCamFinal[1] + rotMatrix[2] * upCamFinal[2];
                        float upNorth = rotMatrix[3] * upCamFinal[0] + rotMatrix[4] * upCamFinal[1] + rotMatrix[5] * upCamFinal[2];

                        // 2. Project onto horizontal plane via atan2 (ignores the Sky/Z vector to eliminate tilt bleed)
                        solYaw = Math.toDegrees(Math.atan2(upEast, upNorth));
                        if (solYaw < 0) solYaw += 360.0;

                    } else {
                        // === FALLBACK: ARCore world path ===
                        solYaw = normalizeDegrees(Math.toDegrees(Math.atan2(uWorld[0], uWorld[2])));
                    }
                    double solRoll = solYaw;

                    if (score < bestScore) {
                        bestScore = score;
                        bestYaw = solYaw;
                        bestPitch = solPitch;
                        bestRoll = solRoll;
                        bestReprojectionError = reprojError;
                        bestSolutionIdx = solIdx;
                    }

                    Log.d(TAG, String.format(Locale.US,
                            "[solvePnP] Solution %d: yaw=%.1f° pitch=%.1f° roll=%.1f° score=%.3f upWorld=(%.3f,%.3f,%.3f)",
                            solIdx, solYaw, solPitch, solRoll, score, uWorld[0], uWorld[1], uWorld[2]));
                } finally {
                    rotationMatrix.release();
                }
            }

            // Use different method name so processSample knows to skip offset
            String method = useDirect ? "floor-solvepnp-direct" : "floor-solvepnp";
            double confidence = useDirect
                    ? confidenceFromReprojectionError(bestReprojectionError)
                    : 1.0;
            Log.d(TAG, String.format(Locale.US,
                    "[solvePnP] Selected solution %d -> yaw=%.1f deg pitch=%.1f deg roll=%.1f deg reproj=%.4f conf=%.2f method=%s",
                    bestSolutionIdx, bestYaw, bestPitch, bestRoll, bestReprojectionError, confidence, method));
            return new QRMeasurement(bestYaw, bestPitch, bestRoll, confidence, method, useDirect);

        } catch (Exception e) {
            log("[solvePnP] Exception: " + e.getMessage());
            return null;
        } finally {
            if (cameraMatrix != null) cameraMatrix.release();
            if (distCoeffs != null) distCoeffs.release();
            if (objectPoints != null) objectPoints.release();
            if (imagePts != null) imagePts.release();
            if (pnpAux1 != null) pnpAux1.release();
            if (pnpAux2 != null) pnpAux2.release();
            if (pnpAux3 != null) pnpAux3.release();
            for (Mat r : rvecs) r.release();
            for (Mat t : tvecs) t.release();
        }
    }

    // Reprojection RMSE for a candidate PnP pose.
    private double computeReprojectionRmse(MatOfPoint3f objectPoints,
                                           MatOfPoint2f imagePoints,
                                           Mat cameraMatrix,
                                           MatOfDouble distCoeffs,
                                           Mat rvec,
                                           Mat tvec) {
        MatOfPoint2f projectedPoints = new MatOfPoint2f();
        try {
            Calib3d.projectPoints(objectPoints, rvec, tvec, cameraMatrix, distCoeffs, projectedPoints);
            Point[] projected = projectedPoints.toArray();
            Point[] observed = imagePoints.toArray();
            int count = Math.min(projected.length, observed.length);
            if (count == 0) return Double.MAX_VALUE;

            double sumSq = 0.0;
            for (int i = 0; i < count; i++) {
                double dx = projected[i].x - observed[i].x;
                double dy = projected[i].y - observed[i].y;
                sumSq += dx * dx + dy * dy;
            }
            return Math.sqrt(sumSq / count);
        } finally {
            projectedPoints.release();
        }
    }

    private QRMeasurement computeWallYawWithSolvePnP(
            float[] imagePoints,
            CameraIntrinsics intrinsics,
            float[] camPose,
            float[] fopAttitude,
            float[] vpNormalCam,
            double vpWallRawYaw,
            double vpWallNorthYaw) {
        Mat cameraMatrix = null;
        MatOfDouble distCoeffs = null;
        MatOfPoint3f objectPoints = null;
        MatOfPoint2f imagePts = null;
        Mat pnpAux1 = null;
        Mat pnpAux2 = null;
        Mat pnpAux3 = null;
        List<Mat> rvecs = new ArrayList<>();
        List<Mat> tvecs = new ArrayList<>();
        try {
            float[] focal = intrinsics.getFocalLength();
            float[] pp = intrinsics.getPrincipalPoint();

            double fx = focal[0];
            double fy = focal[1];
            double cx = pp[0];
            double cy = pp[1];

            cameraMatrix = new Mat(3, 3, CvType.CV_64F);
            cameraMatrix.put(0, 0,
                    fx, 0, cx,
                    0, fy, cy,
                    0, 0, 1);

            distCoeffs = new MatOfDouble(0, 0, 0, 0, 0);
            double half = (float) 0.1 / 2.0;
            objectPoints = new MatOfPoint3f(
                    new Point3(-half, half, 0),
                    new Point3(half, half, 0),
                    new Point3(half, -half, 0),
                    new Point3(-half, -half, 0)
            );

            imagePts = new MatOfPoint2f(
                    new Point(imagePoints[0], imagePoints[1]),
                    new Point(imagePoints[2], imagePoints[3]),
                    new Point(imagePoints[4], imagePoints[5]),
                    new Point(imagePoints[6], imagePoints[7])
            );

            pnpAux1 = new Mat();
            pnpAux2 = new Mat();
            pnpAux3 = new Mat();

            int numSolutions = Calib3d.solvePnPGeneric(
                    objectPoints, imagePts, cameraMatrix, distCoeffs,
                    rvecs, tvecs, false, Calib3d.SOLVEPNP_IPPE_SQUARE,
                    pnpAux1, pnpAux2, pnpAux3
            );

            if (numSolutions == 0) {
                log("[wall-solvePnP] Failed - no solutions");
                return null;
            }

            boolean useDirect = (fopAttitude != null);
            float[] fopRotMatrix = new float[9];
            if (useDirect) {
                SensorManager.getRotationMatrixFromVector(fopRotMatrix, fopAttitude);
            }
            double continuityReferenceYaw = getWallContinuityReferenceYaw();

            double bestScore = Double.MAX_VALUE;
            double bestYaw = 0.0;
            double bestPitch = 0.0;
            double bestRoll = 0.0;
            double bestReprojectionError = Double.NaN;
            int bestSolutionIdx = -1;
            double bestNormalCamX = 0.0;
            double bestNormalCamY = 0.0;
            double bestNormalCamZ = 0.0;
            double secondBestScore = Double.MAX_VALUE;
            double secondBestYaw = 0.0;
            double secondBestPitch = 0.0;
            double secondBestReprojectionError = Double.NaN;
            int secondBestSolutionIdx = -1;

            for (int solIdx = 0; solIdx < numSolutions; solIdx++) {
                Mat rotationMatrix = new Mat();
                try {
                    Calib3d.Rodrigues(rvecs.get(solIdx), rotationMatrix);

                    double normalCamX = rotationMatrix.get(0, 2)[0];
                    double normalCamY = rotationMatrix.get(1, 2)[0];
                    double normalCamZ = rotationMatrix.get(2, 2)[0];
                    double upCamX = rotationMatrix.get(0, 1)[0];
                    double upCamY = rotationMatrix.get(1, 1)[0];
                    double upCamZ = rotationMatrix.get(2, 1)[0];

                    // ── Step 1: Camera-space front-facing disambiguation ──
                    // In OpenCV camera convention, Z points out of the camera.
                    // A QR facing the camera has its normal pointing back toward
                    // the camera, so normalCamZ must be negative.
                    // This is purely geometric — no ARCore or FOP dependency.
                    boolean frontFacing = normalCamZ < 0.0;
                    if (!frontFacing) {
                        normalCamX = -normalCamX;
                        normalCamY = -normalCamY;
                        normalCamZ = -normalCamZ;
                        upCamX = -upCamX;
                        upCamY = -upCamY;
                        upCamZ = -upCamZ;
                    }

                    double solYaw;
                    double solPitch;
                    double solRoll;

                    if (useDirect) {
                        // ── Step 2: Map normal and up to device portrait frame ──
                        float[] normalDevice = mapOpenCvCameraVectorToDeviceFrame(normalCamX, normalCamY, normalCamZ);
                        float[] upDevice = mapOpenCvCameraVectorToDeviceFrame(upCamX, upCamY, upCamZ);

                        // ── Step 3: Rotate normal to Earth frame via FOP ──
                        // SensorManager rotation matrix rows:
                        //   [0-2] = East,  [3-5] = North,  [6-8] = Up (gravity-opposite)
                        float normalEast  = fopRotMatrix[0] * normalDevice[0] + fopRotMatrix[1] * normalDevice[1] + fopRotMatrix[2] * normalDevice[2];
                        float normalNorth = fopRotMatrix[3] * normalDevice[0] + fopRotMatrix[4] * normalDevice[1] + fopRotMatrix[5] * normalDevice[2];
                        float normalUp    = fopRotMatrix[6] * normalDevice[0] + fopRotMatrix[7] * normalDevice[1] + fopRotMatrix[8] * normalDevice[2];

                        double horizLenEarth = Math.hypot(normalEast, normalNorth);
                        if (horizLenEarth < 1e-5) {
                            log(String.format(Locale.US,
                                    "[wall-solvePnP] Solution %d skipped - horizontal normal too small",
                                    solIdx));
                            continue;
                        }

                        // ── Step 4: Yaw and pitch entirely from FOP-rotated normal ──
                        solYaw = Math.toDegrees(Math.atan2(normalEast, normalNorth));
                        if (solYaw < 0) solYaw += 360.0;

                        solPitch = Math.toDegrees(Math.atan2(normalUp, horizLenEarth));

                        // ── Step 5: Viewpoint-invariant roll ──
                        //
                        // Roll = rotation of the QR's top edge relative to gravity's
                        // projection onto the wall plane. This is a physical property
                        // of the QR on the wall and is independent of camera position.
                        //
                        // Strategy:
                        //   1. Project gravity-up onto the wall plane → wallRefUp
                        //      (the direction "up" means on this particular wall surface)
                        //   2. Rotate PnP up-vector to Earth frame, then project
                        //      onto the same wall plane → qrUpOnWall
                        //   3. Measure the signed angle between them in-plane

                        float[] normalEarthVec = new float[]{ normalEast, normalNorth, normalUp };

                        // Gravity-up in the FOP Earth frame is (East=0, North=0, Up=1)
                        float[] gravityUp = new float[]{ 0f, 0f, 1f };

                        // Project gravity-up onto the wall plane:
                        //   projected = gravityUp - (gravityUp · normal) * normal
                        float dotGravNormal = vec3Dot(gravityUp, normalEarthVec);
                        float[] wallRefUp = new float[]{
                                gravityUp[0] - dotGravNormal * normalEarthVec[0],
                                gravityUp[1] - dotGravNormal * normalEarthVec[1],
                                gravityUp[2] - dotGravNormal * normalEarthVec[2]
                        };

                        if (vec3Len(wallRefUp) < 1e-4f) {
                            // Wall normal is pointing straight up/down — this is a
                            // ceiling or floor, not a wall. Roll is undefined.
                            log(String.format(Locale.US,
                                    "[wall-solvePnP] Solution %d skipped - wall normal too vertical for roll (FOP path)",
                                    solIdx));
                            continue;
                        }
                        wallRefUp = vec3Normalize(wallRefUp);

                        // Rotate the PnP up-vector to Earth frame via FOP
                        float upEast  = fopRotMatrix[0] * upDevice[0] + fopRotMatrix[1] * upDevice[1] + fopRotMatrix[2] * upDevice[2];
                        float upNorth = fopRotMatrix[3] * upDevice[0] + fopRotMatrix[4] * upDevice[1] + fopRotMatrix[5] * upDevice[2];
                        float upUp    = fopRotMatrix[6] * upDevice[0] + fopRotMatrix[7] * upDevice[1] + fopRotMatrix[8] * upDevice[2];
                        float[] qrUpEarth = new float[]{ upEast, upNorth, upUp };

                        // Project QR up onto the same wall plane
                        float dotQrNormal = vec3Dot(qrUpEarth, normalEarthVec);
                        float[] qrUpOnWall = new float[]{
                                qrUpEarth[0] - dotQrNormal * normalEarthVec[0],
                                qrUpEarth[1] - dotQrNormal * normalEarthVec[1],
                                qrUpEarth[2] - dotQrNormal * normalEarthVec[2]
                        };

                        if (vec3Len(qrUpOnWall) < 1e-4f) {
                            // QR up-vector is parallel to wall normal — degenerate
                            log(String.format(Locale.US,
                                    "[wall-solvePnP] Solution %d skipped - QR up degenerate on wall plane (FOP path)",
                                    solIdx));
                            continue;
                        }
                        qrUpOnWall = vec3Normalize(qrUpOnWall);

                        // Build an in-plane right axis: wallRefRight = normal × wallRefUp
                        // This gives us a signed 2D coordinate system on the wall face.
                        float[] wallRefRight = vec3Normalize(vec3Cross(normalEarthVec, wallRefUp));
                        if (vec3Len(wallRefRight) < 1e-4f) {
                            log(String.format(Locale.US,
                                    "[wall-solvePnP] Solution %d skipped - wall right vector degenerate (FOP path)",
                                    solIdx));
                            continue;
                        }

                        // Signed angle from wallRefUp to qrUpOnWall, measured in-plane
                        double cosRoll = vec3Dot(qrUpOnWall, wallRefUp);
                        double sinRoll = vec3Dot(qrUpOnWall, wallRefRight);
                        solRoll = normalizeDegrees(Math.toDegrees(Math.atan2(sinRoll, cosRoll)));

                    } else {
                        // ── Legacy ARCore path (non-direct) ──
                        // ARCore is acceptable here because this path does not claim
                        // absolute north — it uses the mag anchor offset downstream.
                        float[] normalCamAndroid = new float[]{
                                (float) normalCamX,
                                (float) -normalCamY,
                                (float) -normalCamZ
                        };
                        float[] upCamAndroid = new float[]{
                                (float) upCamX,
                                (float) -upCamY,
                                (float) -upCamZ
                        };
                        float[] normalWorld = rotateCamToWorld(normalCamAndroid, camPose);
                        float[] upWorld = rotateCamToWorld(upCamAndroid, camPose);

                        double horizLenWorld = Math.hypot(normalWorld[0], normalWorld[2]);
                        if (horizLenWorld < 1e-5) {
                            log(String.format(Locale.US,
                                    "[wall-solvePnP] Solution %d skipped - horizontal world normal too small",
                                    solIdx));
                            continue;
                        }

                        solYaw = normalizeDegrees(Math.toDegrees(Math.atan2(normalWorld[0], normalWorld[2])));
                        solPitch = Math.toDegrees(Math.asin(
                                Math.max(-1.0, Math.min(1.0, normalWorld[1]))));
                        solRoll = computeWallRollDegrees(upWorld, normalWorld);
                        if (Double.isNaN(solRoll)) {
                            log(String.format(Locale.US,
                                    "[wall-solvePnP] Solution %d skipped - roll unavailable",
                                    solIdx));
                            continue;
                        }
                    }

                    double reprojError = computeReprojectionRmse(
                            objectPoints, imagePts, cameraMatrix, distCoeffs, rvecs.get(solIdx), tvecs.get(solIdx));
                    if (reprojError > WALL_DIRECT_REPROJECTION_GATE_PX) {
                        log(String.format(Locale.US,
                                "[wall-solvePnP] Solution %d skipped - reproj %.4f px > %.2f px gate",
                                solIdx, reprojError, WALL_DIRECT_REPROJECTION_GATE_PX));
                        continue;
                    }

                    // Facing penalty: camera-space check already resolved the flip
                    // above, so frontFacing should always be true after the flip.
                    // We still penalize the originally-back-facing solution mildly
                    // because the flipped pose is geometrically less trustworthy.
                    double facingPenalty = frontFacing ? 0.0 : WALL_BACKFACING_PENALTY;
                    double continuityPenalty = Double.isNaN(continuityReferenceYaw)
                            ? 0.0
                            : WALL_CONTINUITY_PENALTY_PER_DEG
                            * Math.abs(angleDifference(continuityReferenceYaw, solYaw));
                    double score = reprojError + facingPenalty + continuityPenalty;

                    if (score < bestScore) {
                        secondBestScore = bestScore;
                        secondBestYaw = bestYaw;
                        secondBestPitch = bestPitch;
                        secondBestReprojectionError = bestReprojectionError;
                        secondBestSolutionIdx = bestSolutionIdx;

                        bestScore = score;
                        bestYaw = solYaw;
                        bestPitch = solPitch;
                        bestRoll = solRoll;
                        bestReprojectionError = reprojError;
                        bestSolutionIdx = solIdx;
                        bestNormalCamX = normalCamX;
                        bestNormalCamY = normalCamY;
                        bestNormalCamZ = normalCamZ;
                    } else if (score < secondBestScore) {
                        secondBestScore = score;
                        secondBestYaw = solYaw;
                        secondBestPitch = solPitch;
                        secondBestReprojectionError = reprojError;
                        secondBestSolutionIdx = solIdx;
                    }

                    log(String.format(Locale.US,
                            "[wall-solvePnP] Solution %d: yaw=%.1f deg pitch=%.1f deg roll=%.1f deg reproj=%.4f facing=%b continuityPenalty=%.3f score=%.4f normalCam=(%.3f,%.3f,%.3f)",
                            solIdx, solYaw, solPitch, solRoll, reprojError, frontFacing,
                            continuityPenalty, score, normalCamX, normalCamY, normalCamZ));
                } finally {
                    rotationMatrix.release();
                }
            }

            if (bestSolutionIdx < 0) {
                log("[wall-solvePnP] No usable wall solutions");
                return null;
            }

            if (secondBestSolutionIdx >= 0) {
                double yawSplit = Math.abs(angleDifference(bestYaw, secondBestYaw));
                double scoreMargin = secondBestScore - bestScore;
                if (yawSplit >= WALL_AMBIGUITY_YAW_SPLIT_DEG
                        && scoreMargin <= WALL_AMBIGUITY_MAX_SCORE_MARGIN) {
                    log(String.format(Locale.US,
                            "[wall-solvePnP] Ambiguous wall solutions: best=%d yaw=%.1f deg reproj=%.4f score=%.4f second=%d yaw=%.1f deg reproj=%.4f score=%.4f yawSplit=%.1f deg scoreMargin=%.4f -> suppressing sample",
                            bestSolutionIdx, bestYaw, bestReprojectionError, bestScore,
                            secondBestSolutionIdx, secondBestYaw, secondBestReprojectionError, secondBestScore,
                            yawSplit, scoreMargin));
                    return new QRMeasurement(bestYaw, bestPitch, bestRoll, 0.0, useDirect ? "wall-solvepnp-direct" : "wall-solvepnp", true);
                }
            }

            double confidence = confidenceFromReprojectionError(bestReprojectionError);
            log(String.format(Locale.US,
                    "[wall-solvePnP] Selected solution %d -> yaw=%.1f deg pitch=%.1f deg roll=%.1f deg reproj=%.4f score=%.4f conf=%.2f",
                    bestSolutionIdx, bestYaw, bestPitch, bestRoll, bestReprojectionError, bestScore, confidence));

            double vpVsPnpNormalAxisDeltaDeg = Double.NaN;
            if (vpNormalCam != null) {
                double vpLen = Math.sqrt(
                        vpNormalCam[0] * vpNormalCam[0]
                                + vpNormalCam[1] * vpNormalCam[1]
                                + vpNormalCam[2] * vpNormalCam[2]);
                double pnpLen = Math.sqrt(
                        bestNormalCamX * bestNormalCamX
                                + bestNormalCamY * bestNormalCamY
                                + bestNormalCamZ * bestNormalCamZ);
                if (vpLen > 1e-6 && pnpLen > 1e-6) {
                    double dot = (vpNormalCam[0] * bestNormalCamX
                            + vpNormalCam[1] * bestNormalCamY
                            + vpNormalCam[2] * bestNormalCamZ) / (vpLen * pnpLen);
                    dot = Math.max(-1.0, Math.min(1.0, Math.abs(dot)));
                    vpVsPnpNormalAxisDeltaDeg = Math.toDegrees(Math.acos(dot));
                }
            }
            return new QRMeasurement(bestYaw, bestPitch, bestRoll, confidence, useDirect ? "wall-solvepnp-direct" : "wall-solvepnp", true);

        } catch (Exception e) {
            log("[wall-solvePnP] Exception: " + e.getMessage());
            return null;
        } finally {
            if (cameraMatrix != null) cameraMatrix.release();
            if (distCoeffs != null) distCoeffs.release();
            if (objectPoints != null) objectPoints.release();
            if (imagePts != null) imagePts.release();
            if (pnpAux1 != null) pnpAux1.release();
            if (pnpAux2 != null) pnpAux2.release();
            if (pnpAux3 != null) pnpAux3.release();
            for (Mat r : rvecs) r.release();
            for (Mat t : tvecs) t.release();
        }
    }

    private double weightedMeanPitch(List<YawSample> samples) {
        double weightedSum = 0.0;
        double totalWeight = 0.0;
        for (YawSample s : samples) {
            weightedSum += s.pitch * s.weight;
            totalWeight += s.weight;
        }
        if (totalWeight < 1e-6) return 0.0;
        return weightedSum / totalWeight;
    }

    private double circularWeightedMeanRoll(List<YawSample> samples) {
        double sumSin = 0.0, sumCos = 0.0, totalWeight = 0.0;
        for (YawSample s : samples) {
            double rad = Math.toRadians(s.roll);
            sumSin += Math.sin(rad) * s.weight;
            sumCos += Math.cos(rad) * s.weight;
            totalWeight += s.weight;
        }
        if (totalWeight < 1e-6) return 0.0;
        double mean = Math.toDegrees(Math.atan2(sumSin / totalWeight, sumCos / totalWeight));
        if (mean < 0) mean += 360.0;
        return mean;
    }

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
