package com.example.qryaw;

import android.Manifest;
import android.annotation.SuppressLint;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.hardware.GeomagneticField;
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
import androidx.annotation.NonNull;
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
import org.opencv.core.Mat;
import org.opencv.core.MatOfPoint2f;
import org.opencv.core.Point;
import org.opencv.core.Size;
import org.opencv.core.TermCriteria;
import org.opencv.imgproc.Imgproc;

import java.util.ArrayDeque;
import java.util.ArrayList;
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
    private static final double MIN_SAMPLE_WEIGHT = 0.05;
    private static final double YAW_TOLERANCE = 30.0; // Loose: Absorbs magnetic drift
    private static final double PITCH_TOLERANCE = 20.0; // Tight: Gravity is stable
    private static final double ROLL_TOLERANCE = 20.0; // Tight: PnP optics are stable
    private static final float HEADING_ERROR_GATE = 90.0f;
    private static final long MAX_SAMPLE_GAP_MS = 1000;
    private static final long QR_LOST_TIMEOUT_MS = 1500;
    private static final int SUBPIX_WINDOW_RADIUS = 5;
    private static final int SUBPIX_MAX_ITER = 20;
    private static final double SUBPIX_EPSILON = 0.03;
    private static final long SUBPIX_LOG_INTERVAL_MS = 500;
    private static final int MAX_FOP_HISTORY_SAMPLES = 128;
    private static final long MAX_FOP_INTERPOLATION_SPAN_NS = 75_000_000L;
    private static final long MAX_FOP_NEAREST_SAMPLE_AGE_NS = 50_000_000L;
    // Hard freshness cap for the unconditional `latest` FOP fallback.
    // If lastFopSample is older than this, treat it as no FOP rather than
    // poisoning the wall yaw with a 15–19 second stale compass heading.
    private static final long MAX_FOP_LATEST_FALLBACK_AGE_NS = 500_000_000L; // 500 ms
    private static final double DIRECT_METHOD_OUTLIER_THRESHOLD_DEG = 8.0;
    private static final double DIRECT_SAMPLE_RESET_CONFIDENCE = 0.85;
    private static final double PITCH_OUTLIER_THRESHOLD_DEG = 20.0;
    private static final String OFFSET_PREFS_NAME = "qryaw_offsets";
    private static final String OFFSET_KEY_PREFIX = "offset_";
    private static final String OFFSET_MODE_KEY_PREFIX = "offset_mode_";
    private static final double UNIFIED_WALL_FLOOR_THRESHOLD = 0.15;
    // Bootstrap: require N provisional samples before trusting any anchor
    private static final int BOOTSTRAP_REQUIRED = 3;
    private static final double BOOTSTRAP_YAW_SPREAD_DEG = 10.0;
    private static final double BOOTSTRAP_PITCH_SPREAD_DEG = 15.0;
    private static final double BOOTSTRAP_ROLL_SPREAD_DEG = 10.0;
    // QR frame-quality gate thresholds
    private static final double QR_MIN_EDGE_PX = 20.0;
    private static final double QR_MIN_AREA_PX2 = 400.0;
    private static final double QR_MIN_EDGE_RATIO = 0.3;
    private static final double QR_MAX_SUBPIX_SHIFT_PX = 6.0;
    // Canonical yaw continuity gate
    private static final int CANONICAL_YAW_HISTORY_SIZE = 5;
    private static final double CANONICAL_YAW_SPIKE_THRESHOLD_DEG = 45.0;
    // Set to true to enable per-frame diagnostic logs (noisy, ~30/sec)
    private static final boolean VERBOSE_FRAME_LOGS = false;


    // ── UI Components ──
    private ARSceneView arSceneView;
    private QROverlayView overlayView;
    private TextView statusText, infoText, modeText, dbBadge;
    private Button btnRegister;
    private Button btnValidate;

    // ── Runtime State ──
    private String magSamplesDisplay = null;

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
    private long nextSamplingSessionId = 1L;
    private SamplingSession activeSamplingSession = null;
    private final ArrayDeque<Double> recentCanonicalYaws = new ArrayDeque<>();

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

    private enum SamplingPurpose {
        REGISTER, VALIDATE
    }

    private enum ScanMode {
        REGISTER, VALIDATE
    }

    private enum InfoSource {
        DATABASE, FRESH_REGISTER
    }

    private enum OffsetMode {
        RELATIVE, ABSOLUTE, UNKNOWN
    }

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

        ValidationSummary(double yawDelta, double pitchDelta, double rollDelta, double tolerance,
                          boolean withinTolerance) {
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
        // Bootstrap: collect provisional samples before trusting any anchor
        final List<QRMeasurement> provisionalBuffer = new ArrayList<>();
        boolean bootstrapPromoted = false;
        String lockedSubtype = null; // exact method locked after bootstrap (e.g. "unified-wall")

        SamplingSession(long id, String payload, SamplingPurpose purpose, long startedAtMs) {
            this.id = id;
            this.payload = payload;
            this.purpose = purpose;
            this.startedAtMs = startedAtMs;
        }
    }

    private static class QRFrameQuality {
        final double topEdge, bottomEdge, leftEdge, rightEdge;
        final double minEdge;
        final double ratioTB, ratioLR;
        final double area;
        final double avgShift, maxShift;
        final boolean convex;

        QRFrameQuality(double topEdge, double bottomEdge, double leftEdge, double rightEdge,
                       double minEdge, double ratioTB, double ratioLR,
                       double area, double avgShift, double maxShift, boolean convex) {
            this.topEdge = topEdge;
            this.bottomEdge = bottomEdge;
            this.leftEdge = leftEdge;
            this.rightEdge = rightEdge;
            this.minEdge = minEdge;
            this.ratioTB = ratioTB;
            this.ratioLR = ratioLR;
            this.area = area;
            this.avgShift = avgShift;
            this.maxShift = maxShift;
            this.convex = convex;
        }
    }

    private void flushSensors() {
        log("Manual Recalibration Triggered: Flushing sensors.");

        stopFOP();

        resetAlignment();
        lastPayload = null;
        currentRegistration = null;
        resetSamplingState();
        recentCanonicalYaws.clear();
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
            public void onDeviceOrientationChanged(@NonNull DeviceOrientation orientation) {
                if (!isFopRegistered || generation != activeFopGeneration) {
                    return;
                }

                float headingError = orientation.getHeadingErrorDegrees();
                if (headingError <= HEADING_ERROR_GATE) {
                    float[] attitude = orientation.getAttitude();
                    if (attitude.length >= 4) {
                        float[] normalizedAttitude = normalizeQuaternionCopy(attitude);
                        if (normalizedAttitude != null) {
                            long sampleElapsedRealtimeNs = orientation.getElapsedRealtimeNs();
                            lastFopSample = new TimedAttitude(sampleElapsedRealtimeNs, normalizedAttitude.clone());
                            addFopAttitudeSample(sampleElapsedRealtimeNs, normalizedAttitude);
                        }
                    }
                }
            }
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Vector Math Helpers
    // ─────────────────────────────────────────────────────────────────────────
    private static float[] vec3Scale(float[] v, float s) {
        return new float[] { v[0] * s, v[1] * s, v[2] * s };
    }

    private static float vec3Dot(float[] a, float[] b) {
        return a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
    }

    private static float vec3Len(float[] v) {
        return (float) Math.sqrt(vec3Dot(v, v));
    }

    private static float[] vec3Normalize(float[] v) {
        float len = vec3Len(v);
        return (len < 1e-6f) ? new float[] { 0, 0, 0 } : vec3Scale(v, 1f / len);
    }

    private static float[] vec3Cross(float[] a, float[] b) {
        return new float[] {
                a[1] * b[2] - a[2] * b[1],
                a[2] * b[0] - a[0] * b[2],
                a[0] * b[1] - a[1] * b[0]
        };
    }

    private static float[] rotateCamToWorld(float[] camVec, float[] m) {
        return new float[] {
                m[0] * camVec[0] + m[4] * camVec[1] + m[8] * camVec[2],
                m[1] * camVec[0] + m[5] * camVec[1] + m[9] * camVec[2],
                m[2] * camVec[0] + m[6] * camVec[1] + m[10] * camVec[2]
        };
    }

    private int resolveBackCameraSensorOrientation() {
        try {
            CameraManager cameraManager = getSystemService(CameraManager.class);
            if (cameraManager == null)
                return 90;

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
        if (quaternion == null || quaternion.length < 4)
            return null;

        double norm = Math.sqrt(
                quaternion[0] * quaternion[0]
                        + quaternion[1] * quaternion[1]
                        + quaternion[2] * quaternion[2]
                        + quaternion[3] * quaternion[3]);
        if (norm < 1e-9)
            return null;

        return new float[] {
                (float) (quaternion[0] / norm),
                (float) (quaternion[1] / norm),
                (float) (quaternion[2] / norm),
                (float) (quaternion[3] / norm)
        };
    }

    private static float[] slerpQuaternion(float[] q0, float[] q1, double alpha) {
        float[] start = normalizeQuaternionCopy(q0);
        float[] end = normalizeQuaternionCopy(q1);
        if (start == null || end == null)
            return null;

        double dot = start[0] * end[0] + start[1] * end[1] + start[2] * end[2] + start[3] * end[3];
        if (dot < 0.0) {
            dot = -dot;
            end = new float[] { -end[0], -end[1], -end[2], -end[3] };
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

        return new float[] {
                (float) (s0 * start[0] + s1 * end[0]),
                (float) (s0 * start[1] + s1 * end[1]),
                (float) (s0 * start[2] + s1 * end[2]),
                (float) (s0 * start[3] + s1 * end[3])
        };
    }

    private ResolvedFopAttitude resolveFopAttitudeAt(long targetTimestampNs, String strategyBase) {
        if (targetTimestampNs <= 0)
            return null;

        synchronized (fopHistoryLock) {
            if (fopAttitudeHistory.isEmpty())
                return null;

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
        if (resolved != null)
            return resolved;

        resolved = resolveFopAttitudeAt(acquireElapsedRealtimeNs, "acquire");
        if (resolved != null)
            return resolved;

        TimedAttitude latestSample = lastFopSample;
        if (latestSample == null)
            return null;

        long latestAgeNs = Math.abs(acquireElapsedRealtimeNs - latestSample.elapsedRealtimeNs);
        if (latestAgeNs > MAX_FOP_LATEST_FALLBACK_AGE_NS) {
            Log.w(TAG, String.format(Locale.US,
                    "[resolveFop] latest FOP sample is %.0f ms old — rejecting stale fallback",
                    latestAgeNs / 1_000_000.0));
            return null;
        }

        return new ResolvedFopAttitude(
                latestSample.attitude.clone(),
                acquireElapsedRealtimeNs,
                latestSample.elapsedRealtimeNs,
                "latest");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle & Permissions
    // ─────────────────────────────────────────────────────────────────────────
    private final ActivityResultLauncher<String[]> permissionLauncher = registerForActivityResult(
            new ActivityResultContracts.RequestMultiplePermissions(), permissions -> {
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
        int backCameraSensorOrientationDegrees = resolveBackCameraSensorOrientation();
        log("Back camera sensor orientation: " + backCameraSensorOrientationDegrees + "°");

        log("DB path: " + QRDatabaseHelper.getInstance(this).getDatabasePath());

        fusedOrientationClient = LocationServices.getFusedOrientationProviderClient(this);
        locationClient = LocationServices.getFusedLocationProviderClient(this);

        arSceneView.setSessionConfiguration((session, config) -> {
            config.setPlaneFindingMode(Config.PlaneFindingMode.HORIZONTAL_AND_VERTICAL);
            log("ARCore sessionConfiguration: HORIZONTAL_AND_VERTICAL enabled");
            return null;
        });

        boolean hasCamera = ContextCompat.checkSelfPermission(this,
                Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED;

        if (hasCamera) {
            checkArCoreAndStart();
        } else {
            permissionLauncher.launch(new String[] {
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
        if (fusedOrientationClient == null)
            return;
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
                                System.currentTimeMillis());
                        float declination = field.getDeclination();
                        log("Location secured. Magnetic Declination is: "
                                + String.format(Locale.US, "%.2f°", declination));
                    } else {
                        log("⚠️ Location is null. FOP may temporarily fall back to Magnetic North.");
                    }
                });

        DeviceOrientationRequest request = new DeviceOrientationRequest.Builder(
                DeviceOrientationRequest.OUTPUT_PERIOD_FAST).build();

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
            if (frame == null)
                return Unit.INSTANCE;

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

        if (payload.equals(lastPayload))
            return;
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
        return lastFopSample != null;
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
        if (isSampling)
            return;
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
            showStatus(
                    "⚠️ This QR was registered before pitch/roll tracking was added.\nPress REGISTER to save full orientation again.");
            setMode(ScanMode.REGISTER);
            return;
        }
        if (isSampling)
            return;
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

        List<QRDatabaseHelper.Validation> validations = QRDatabaseHelper.getInstance(this).fetchValidations(lastPayload,
                20);
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
        if (payload == null)
            return OffsetMode.UNKNOWN;
        String rawValue = getOffsetPreferences().getString(offsetModeKey(payload), null);
        if (rawValue == null)
            return OffsetMode.UNKNOWN;
        try {
            return OffsetMode.valueOf(rawValue);
        } catch (IllegalArgumentException ignored) {
            return OffsetMode.UNKNOWN;
        }
    }

    private Double readStoredRelativeOffset(String payload) {
        if (payload == null)
            return null;
        SharedPreferences prefs = getOffsetPreferences();
        String key = offsetKey(payload);
        if (!prefs.contains(key))
            return null;

        float offset = prefs.getFloat(key, Float.NaN);
        return Float.isNaN(offset) ? null : (double) offset;
    }

    private void persistOffsetMetadata(String payload, boolean isAbsoluteNorth, double offsetUsed) {
        if (payload == null)
            return;

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
        if (method == null)
            return "unknown";

        if (method.startsWith("unified-")) {
            return "unified";
        }

        boolean absoluteNorth = isAbsoluteNorthMethod(method);
        if (method.startsWith("wall-")) {
            return absoluteNorth ? "wall-direct" : "wall-relative";
        }
        if (method.startsWith("floor-")) {
            return absoluteNorth ? "floor-direct" : "floor-relative";
        }
        return absoluteNorth ? "direct" : "relative";
    }

    private void resetSamplingState() {
        sampleBuffer.clear();
        magSamplesDisplay = null;
        lastSampleTime = 0;
        activeSamplingSession = null;
        isSampling = false;
    }

    private void abortActiveSampling(String reason) {
        if (!isSampling && activeSamplingSession == null && sampleBuffer.isEmpty()) {
            return;
        }

        resetSamplingState();
        if (reason != null && !reason.isEmpty()) {
            showStatus(reason);
        }
        runOnUiThread(() -> dbBadge.setText(""));
    }

    private void startSampling(SamplingPurpose p, String payloadSnapshot) {
        if (payloadSnapshot == null) {
            showStatus("⚠️ QR payload not decoded yet.");
            return;
        }

        resetSamplingState();
        recentCanonicalYaws.clear();
        purpose = p;
        activeSamplingSession = new SamplingSession(
                nextSamplingSessionId++,
                payloadSnapshot,
                p,
                System.currentTimeMillis());
        isSampling = true;

        String action = (p == SamplingPurpose.REGISTER) ? "Registering" : "Validating";
        showStatus("🧭 " + action + "...\nMeasuring orientation...");
        dbBadge.setText("🔄 QR Sampling 0/" + REQUIRED_SAMPLES);
    }

    private void collectSampleWithPose(float[] imagePoints, float[] camPose,
                                       CameraIntrinsics intrinsics,
                                       ResolvedFopAttitude resolvedFopAttitude,
                                       String framePayload) {
        SamplingSession session = activeSamplingSession;
        if (session == null || !isSampling)
            return;

        if (resolvedFopAttitude == null) {
            log("[collectSampleWithPose] Waiting for aligned FOP before accepting samples");
            return;
        }
        // Secondary freshness gate: if resolution still returned a `latest` fallback
        // whose delta exceeds the cap, drop it here as a safety net.
        if ("latest".equals(resolvedFopAttitude.strategy)) {
            long ageNs = Math.abs(resolvedFopAttitude.targetTimestampNs - resolvedFopAttitude.referenceTimestampNs);
            if (ageNs > MAX_FOP_LATEST_FALLBACK_AGE_NS) {
                log(String.format(Locale.US,
                        "[collectSampleWithPose] DROPPED: `latest` FOP fallback is %.0f ms old — too stale for production sample",
                        ageNs / 1_000_000.0));
                return;
            }
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

        if (VERBOSE_FRAME_LOGS) {
            log("[collectSampleWithPose] Using pre-captured pose. camOrigin=("
                    + String.format(Locale.US, "%.3f, %.3f, %.3f", camPose[12], camPose[13], camPose[14]) + ")");
            if (resolvedFopAttitude != null) {
                double syncDeltaMs = Math.abs(
                        resolvedFopAttitude.targetTimestampNs - resolvedFopAttitude.referenceTimestampNs) / 1_000_000.0;
                log(String.format(Locale.US,
                        "[collectSampleWithPose] FOP sync strategy=%s delta=%.2fms",
                        resolvedFopAttitude.strategy,
                        syncDeltaMs));
            } else {
                log("[collectSampleWithPose] No time-aligned FOP sample available");
            }
        }

        QRMeasurement result = calculateUnifiedQRMeasurement(
                imagePoints,
                camPose,
                intrinsics,
                resolvedFopAttitude != null ? resolvedFopAttitude.attitude : null);

        if (result == null) {
            log("[collectSampleWithPose] computeQRYaw returned null — skipping");
            return;
        }

        if (VERBOSE_FRAME_LOGS) {
            log(String.format(Locale.US, "[collectSampleWithPose] yaw=%.1f° pitch=%.1f° roll=%.1f° conf=%.2f method=%s",
                    result.yaw, result.pitch, result.roll, result.confidence, result.method));
        }
        processSample(result);
    }

    private void logSampleBuffer() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n=== QR NORTH SAMPLES [").append(sampleBuffer.size()).append("/").append(REQUIRED_SAMPLES)
                .append("] ===\n");
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

    private boolean isAbsoluteNorthMethod(String method) {
        return method != null && (method.endsWith("-direct") || method.startsWith("unified-"));
    }

    private void processSample(QRMeasurement result) {
        SamplingSession session = activeSamplingSession;
        if (!isSampling || session == null) {
            log("[processSample] No active sampling session - dropping sample");
            return;
        }

        double sampleConfidence = result.confidence;

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

        double sampleNorth = result.yaw;
        double sampleRoll = normalizeDegrees(result.roll);

        // ── Time gap check (applies to both bootstrap and post-bootstrap) ──
        long now = System.currentTimeMillis();
        if (lastSampleTime > 0 && (now - lastSampleTime) > MAX_SAMPLE_GAP_MS) {
            if (!session.bootstrapPromoted) {
                int discarded = session.provisionalBuffer.size();
                session.provisionalBuffer.clear();
                log(String.format(Locale.US,
                        "[bootstrap] Gap %dms > %dms — discarded %d provisional samples, restarting bootstrap",
                        (now - lastSampleTime), MAX_SAMPLE_GAP_MS, discarded));
            } else {
                int discarded = sampleBuffer.size();
                sampleBuffer.clear();
                session.provisionalBuffer.clear();
                session.bootstrapPromoted = false;
                session.lockedSubtype = null;
                log(String.format(Locale.US,
                        "[bootstrap] Gap %dms > %dms — discarded %d post-bootstrap samples, restarting bootstrap",
                        (now - lastSampleTime), MAX_SAMPLE_GAP_MS, discarded));
            }
            runOnUiThread(() -> dbBadge.setText("🔄 Gap detected — resampling 0/" + REQUIRED_SAMPLES));
        }
        lastSampleTime = now;

        // ── Bootstrap phase: collect provisional samples before trusting any anchor ──
        if (!session.bootstrapPromoted) {
            session.provisionalBuffer.add(result);
            log(String.format(Locale.US,
                    "[bootstrap] Provisional sample %d/%d: yaw=%.1f° pitch=%.1f° roll=%.1f° method=%s",
                    session.provisionalBuffer.size(), BOOTSTRAP_REQUIRED,
                    result.yaw, result.pitch, result.roll, result.method));

            final int bootCount = session.provisionalBuffer.size();
            runOnUiThread(() -> dbBadge.setText("🔄 Bootstrap " + bootCount + "/" + BOOTSTRAP_REQUIRED));

            if (session.provisionalBuffer.size() < BOOTSTRAP_REQUIRED) {
                return;
            }

            // Evaluate bootstrap stability
            List<QRMeasurement> promoted = evaluateBootstrap(session.provisionalBuffer);
            if (promoted == null) {
                session.provisionalBuffer.clear();
                log("[bootstrap] RESET: no stable cluster found — restarting bootstrap");
                runOnUiThread(() -> dbBadge.setText("🔄 Bootstrap reset — resampling"));
                return;
            }

            // Lock subtype from promoted samples
            session.lockedSubtype = promoted.get(0).method;
            session.bootstrapPromoted = true;
            log(String.format(Locale.US,
                    "[bootstrap] PROMOTED %d/%d samples → sampleBuffer, locked subtype=%s",
                    promoted.size(), session.provisionalBuffer.size(), session.lockedSubtype));

            // Add promoted samples to sampleBuffer
            for (QRMeasurement m : promoted) {
                sampleBuffer.add(new YawSample(m.yaw, m.yaw, m.pitch,
                        normalizeDegrees(m.roll), m.confidence));
            }

            logSampleBuffer();
            updateFullSampleListUI();

            final int countAfterPromo = sampleBuffer.size();
            final String subtypeCopy = session.lockedSubtype;
            runOnUiThread(() -> dbBadge.setText("🔄 Sampling " + countAfterPromo + "/" + REQUIRED_SAMPLES
                    + " [" + subtypeCopy + "]"));

            if (sampleBuffer.size() >= REQUIRED_SAMPLES) {
                completeSampling(session);
            }
            return;
        }

        // ── Post-bootstrap: enforce subtype lock ──
        if (session.lockedSubtype != null && !session.lockedSubtype.equals(result.method)) {
            log(String.format(Locale.US,
                    "[processSample] Subtype mismatch after bootstrap lock: session=%d locked=%s incoming=%s — rejected",
                    session.id, session.lockedSubtype, result.method));
            return;
        }

        // ── Outlier detection against established buffer ──
        if (!sampleBuffer.isEmpty()) {
            double yawReference = circularWeightedMeanNorth(sampleBuffer);
            double yawCompareValue = sampleNorth;
            double yawDiff = Math.abs(angleDifference(yawReference, yawCompareValue));
            double rollReference = circularWeightedMeanRoll(sampleBuffer);
            double rollDiff = Math.abs(angleDifference(rollReference, sampleRoll));
            double pitchReference = weightedMeanPitch(sampleBuffer);
            double pitchDiff = Math.abs(pitchReference - result.pitch);
            boolean isWallLike = "unified-wall".equals(result.method) || "unified-tilted".equals(result.method);
            double yawOutlierThreshold = isWallLike ? 4.0 : DIRECT_METHOD_OUTLIER_THRESHOLD_DEG;
            double rollOutlierThreshold = isWallLike ? 6.0 : DIRECT_METHOD_OUTLIER_THRESHOLD_DEG;

            if (yawDiff > yawOutlierThreshold
                    || rollDiff > rollOutlierThreshold
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

        sampleBuffer.add(new YawSample(result.yaw, sampleNorth, result.pitch, sampleRoll, sampleConfidence));

        log(String.format(Locale.US,
                "  Sample %d: raw=%.1f° north=%.1f° pitch=%.1f° roll=%.1f° conf=%.2f [%s]",
                sampleBuffer.size(), result.yaw, sampleNorth,
                result.pitch, sampleRoll, sampleConfidence, result.method));

        logSampleBuffer();
        updateFullSampleListUI();

        final String methodCopy = result.method;
        final int countCopy = sampleBuffer.size();
        runOnUiThread(() -> dbBadge.setText("🔄 Sampling " + countCopy + "/" + REQUIRED_SAMPLES
                + " [" + methodCopy + "]"));

        if (sampleBuffer.size() >= REQUIRED_SAMPLES) {
            completeSampling(session);
        }
    }

    private void completeSampling(SamplingSession session) {
        isSampling = false;

        StringBuilder sb = new StringBuilder("[processSample] ALL SAMPLES: ");
        for (int i = 0; i < sampleBuffer.size(); i++) {
            YawSample s = sampleBuffer.get(i);
            sb.append(String.format(Locale.US,
                    "#%d raw=%.1f° north=%.1f° pitch=%.1f° roll=%.1f°(w=%.2f) ",
                    i + 1, s.yaw, s.northYaw, s.pitch, s.roll, s.weight));
        }
        log(sb.toString());

        // Direct FOP methods average absolute north; fallback methods still average raw
        // yaw.
        // Floor: average north yaws, pass directly
        double finalYaw = circularWeightedMeanNorth(sampleBuffer);
        double finalPitch = weightedMeanPitch(sampleBuffer);
        double finalRoll = circularWeightedMeanRoll(sampleBuffer);

        log(String.format(Locale.US,
                "Sampling complete. Averaged north yaw = %.2f°, pitch = %.2f°, roll = %.2f° from %d samples",
                finalYaw, finalPitch, finalRoll, sampleBuffer.size()));

        sampleBuffer.clear();
        commitReading(session, finalYaw, finalPitch, finalRoll);
    }

    /**
     * Evaluates a provisional bootstrap buffer for internal consistency.
     * Returns the subset of samples to promote, or null if no stable cluster is found.
     * - If all samples agree: returns all.
     * - If excluding one sample makes the rest agree (bad seed): returns the agreeing subset.
     * - If no majority found: returns null (caller should reset bootstrap).
     */
    private List<QRMeasurement> evaluateBootstrap(List<QRMeasurement> provisional) {
        int n = provisional.size();

        // Check unified subtype consistency: all unified-* samples must share the same subtype
        String subtype = provisional.get(0).method;
        if (subtype != null && subtype.startsWith("unified-")) {
            for (QRMeasurement m : provisional) {
                if (!subtype.equals(m.method)) {
                    log(String.format(Locale.US,
                            "[bootstrap] Mixed unified subtypes (%s vs %s) — rejecting set",
                            subtype, m.method));
                    return null;
                }
            }
        }

        double[] yaws = new double[n];
        double[] pitches = new double[n];
        double[] rolls = new double[n];
        for (int i = 0; i < n; i++) {
            yaws[i] = provisional.get(i).yaw;
            pitches[i] = provisional.get(i).pitch;
            rolls[i] = normalizeDegrees(provisional.get(i).roll);
        }

        // Compute pairwise max spread
        double maxYawSpread = 0, maxPitchSpread = 0, maxRollSpread = 0;
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                maxYawSpread = Math.max(maxYawSpread, Math.abs(angleDifference(yaws[i], yaws[j])));
                maxPitchSpread = Math.max(maxPitchSpread, Math.abs(pitches[i] - pitches[j]));
                maxRollSpread = Math.max(maxRollSpread, Math.abs(angleDifference(rolls[i], rolls[j])));
            }
        }

        log(String.format(Locale.US,
                "[bootstrap] Spread: yaw=%.1f° pitch=%.1f° roll=%.1f° (thresholds: %.0f/%.0f/%.0f)",
                maxYawSpread, maxPitchSpread, maxRollSpread,
                BOOTSTRAP_YAW_SPREAD_DEG, BOOTSTRAP_PITCH_SPREAD_DEG, BOOTSTRAP_ROLL_SPREAD_DEG));

        // All samples agree — promote all
        if (maxYawSpread <= BOOTSTRAP_YAW_SPREAD_DEG
                && maxPitchSpread <= BOOTSTRAP_PITCH_SPREAD_DEG
                && maxRollSpread <= BOOTSTRAP_ROLL_SPREAD_DEG) {
            log(String.format(Locale.US,
                    "[bootstrap] All %d samples agree — promoting all", n));
            return new ArrayList<>(provisional);
        }

        // Try to find a bad seed: check if excluding one sample makes the rest agree
        for (int exclude = 0; exclude < n; exclude++) {
            double pairMaxYaw = 0, pairMaxPitch = 0, pairMaxRoll = 0;
            List<Integer> remaining = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                if (i != exclude) remaining.add(i);
            }
            for (int a = 0; a < remaining.size(); a++) {
                for (int b = a + 1; b < remaining.size(); b++) {
                    int ia = remaining.get(a), ib = remaining.get(b);
                    pairMaxYaw = Math.max(pairMaxYaw, Math.abs(angleDifference(yaws[ia], yaws[ib])));
                    pairMaxPitch = Math.max(pairMaxPitch, Math.abs(pitches[ia] - pitches[ib]));
                    pairMaxRoll = Math.max(pairMaxRoll, Math.abs(angleDifference(rolls[ia], rolls[ib])));
                }
            }

            if (pairMaxYaw <= BOOTSTRAP_YAW_SPREAD_DEG
                    && pairMaxPitch <= BOOTSTRAP_PITCH_SPREAD_DEG
                    && pairMaxRoll <= BOOTSTRAP_ROLL_SPREAD_DEG) {
                log(String.format(Locale.US,
                        "[bootstrap] Bad seed at provisional[%d] (yaw=%.1f° pitch=%.1f° roll=%.1f°) — discarding, promoting %d remaining",
                        exclude, yaws[exclude], pitches[exclude], rolls[exclude], remaining.size()));
                List<QRMeasurement> promoted = new ArrayList<>();
                for (int i : remaining) {
                    promoted.add(provisional.get(i));
                }
                return promoted;
            }
        }

        // No majority found — all samples disagree
        log(String.format(Locale.US,
                "[bootstrap] No majority agreement — all %d samples disagree", n));
        return null;
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

        if (totalWeight < 1e-6)
            return 0;

        double mean = Math.toDegrees(Math.atan2(
                sumSin / totalWeight,
                sumCos / totalWeight));

        if (mean < 0)
            mean += 360.0;
        return mean;
    }



    private void commitReading(SamplingSession session, double inputYaw, double inputPitch, double inputRoll) {
        if (session == null || session.payload == null)
            return;

        String payload = session.payload;
        boolean isAbsoluteNorth = Boolean.TRUE.equals(session.absoluteNorth);
        OffsetMode currentOffsetMode = isAbsoluteNorth ? OffsetMode.ABSOLUTE : OffsetMode.RELATIVE;

        double offsetUsed = 0.0;
        double finalNorthYaw = inputYaw;
        double finalPitch = inputPitch;
        double finalRoll = normalizeDegrees(inputRoll);

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
                updateInfoLabel(finalYaw, finalPitchValue, finalRollValue, displayOffset, reg, null,
                        InfoSource.FRESH_REGISTER);
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
                showStatus(
                        "⚠️ This QR was registered before pitch/roll tracking was added.\nPress REGISTER to save full orientation again.");
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
                log(String.format(Locale.US,
                        "[VALIDATE] Offset check: regOffset=%.1f° currentOffset=%.1f° delta=%.1f° drifted=%b",
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
                updateInfoLabel(finalYaw1, finalPitch1, finalRoll1, displayOffset, record, validationSummary,
                        InfoSource.DATABASE);
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


    private double unifiedComputeNorthOffset(float[] camPose, float[] fopAttitude) {
        if (fopAttitude == null)
            return Double.NaN;

        float arFwdX = -camPose[8];
        float arFwdZ = -camPose[10];
        double arCamYaw = Math.toDegrees(Math.atan2(arFwdX, -arFwdZ));
        if (arCamYaw < 0)
            arCamYaw += 360.0;

        float[] rotMatrix = new float[9];
        android.hardware.SensorManager.getRotationMatrixFromVector(rotMatrix, fopAttitude);
        float east = -rotMatrix[2];
        float north = -rotMatrix[5];
        double fopHeading;
        if (Math.hypot(east, north) < 1e-5) {
            return Double.NaN;
        }
        fopHeading = Math.toDegrees(Math.atan2(east, north));
        if (fopHeading < 0)
            fopHeading += 360.0;

        double offset = angleDifference(arCamYaw, fopHeading);
        return offset;
    }

    private double angleDifference(double a, double b) {
        double diff = b - a;
        while (diff > 180)
            diff -= 360;
        while (diff < -180)
            diff += 360;
        return diff;
    }

    private VpEstimate computeNormalViaVanishingPoint(
            float[] imgTL, float[] imgTR, float[] imgBR, float[] imgBL,
            float[] focalLength, float[] principalPt, float[] camTransform) {
        try {
            float fx = focalLength[0], fy = focalLength[1];
            float cx = principalPt[0], cy = principalPt[1];

            float[] hTL = { imgTL[0], imgTL[1], 1f };
            float[] hTR = { imgTR[0], imgTR[1], 1f };
            float[] hBR = { imgBR[0], imgBR[1], 1f };
            float[] hBL = { imgBL[0], imgBL[1], 1f };

            float[] lineTop = vec3Cross(hTL, hTR);
            float[] lineBottom = vec3Cross(hBL, hBR);
            float[] lineLeft = vec3Cross(hTL, hBL);
            float[] lineRight = vec3Cross(hTR, hBR);

            float[] vpX = vec3Cross(lineTop, lineBottom);
            float[] vpY = vec3Cross(lineLeft, lineRight);

            if (Math.abs(vpX[2]) < 1e-6f || Math.abs(vpY[2]) < 1e-6f) {
                return null;
            }

            float[] rayX = toCameraRay(vpX, fx, fy, cx, cy);
            float[] rayY = toCameraRay(vpY, fx, fy, cx, cy);

            if (vec3Len(rayX) < 0.001f || vec3Len(rayY) < 0.001f) {
                return null;
            }

            float[] normalCam = vec3Normalize(vec3Cross(rayX, rayY));
            if (normalCam[2] < 0)
                normalCam = vec3Scale(normalCam, -1f);

            float[] normalWorld = rotateCamToWorld(normalCam, camTransform);
            return new VpEstimate(normalCam, normalWorld);
        } catch (Exception e) {
            return null;
        }
    }

    private static float[] toCameraRay(float[] v, float fx, float fy, float cx, float cy) {
        float w = v[2];
        float x = (v[0] - w * cx) / fx;
        float y = (v[1] - w * cy) / fy;
        return vec3Normalize(new float[] { x, -y, -w });
    }

    private float[] unifiedGetARCoreNorth(float[] camPose, float[] fopAttitude) {
        double offset = unifiedComputeNorthOffset(camPose, fopAttitude);
        if (Double.isNaN(offset))
            return null;

        double northArYawRad = Math.toRadians(-offset);
        return new float[] {
                (float) Math.sin(northArYawRad),
                0f,
                (float) -Math.cos(northArYawRad)
        };
    }

    private static float[] unifiedIntersectRayPlane(float[] rayOrigin, float[] rayDir,
                                                    float[] planePoint, float[] planeNormal) {
        float denom = vec3Dot(rayDir, planeNormal);
        if (Math.abs(denom) < 1e-5f)
            return null;

        float[] diff = {
                planePoint[0] - rayOrigin[0],
                planePoint[1] - rayOrigin[1],
                planePoint[2] - rayOrigin[2]
        };
        float t = vec3Dot(diff, planeNormal) / denom;
        if (t <= 0)
            return null;

        return new float[] {
                rayOrigin[0] + rayDir[0] * t,
                rayOrigin[1] + rayDir[1] * t,
                rayOrigin[2] + rayDir[2] * t
        };
    }

    private static float[] unifiedProjectOntoPlane(float[] v, float[] planeNormal) {
        float d = vec3Dot(v, planeNormal);
        return new float[] {
                v[0] - planeNormal[0] * d,
                v[1] - planeNormal[1] * d,
                v[2] - planeNormal[2] * d
        };
    }

    private static float[] unifiedBuildWorldRay(float imgX, float imgY,
                                                float fx, float fy, float cx, float cy,
                                                float[] camPose) {
        float xn = (imgX - cx) / fx;
        float yn = -(imgY - cy) / fy;
        float[] camDir = vec3Normalize(new float[] { xn, yn, -1.0f });
        return vec3Normalize(rotateCamToWorld(camDir, camPose));
    }

    private QRMeasurement calculateUnifiedQRMeasurement(float[] imagePoints,
                                                        float[] camPose,
                                                        com.google.ar.core.CameraIntrinsics intrinsics,
                                                        float[] fopAttitude) {
        final String P = "[unified]";

        float[] focal = intrinsics.getFocalLength();
        float[] pp = intrinsics.getPrincipalPoint();
        float fx = focal[0], fy = focal[1], cx = pp[0], cy = pp[1];

        float[] camOrigin = { camPose[12], camPose[13], camPose[14] };
        float[] camFwd = { -camPose[8], -camPose[9], -camPose[10] };

        float[][] rays = new float[4][];
        for (int i = 0; i < 4; i++) {
            rays[i] = unifiedBuildWorldRay(imagePoints[i * 2], imagePoints[i * 2 + 1], fx, fy, cx, cy, camPose);
        }

        float[][] imgPts = {
                { imagePoints[0], imagePoints[1] }, { imagePoints[2], imagePoints[3] },
                { imagePoints[4], imagePoints[5] }, { imagePoints[6], imagePoints[7] }
        };

        VpEstimate vpEstimate = computeNormalViaVanishingPoint(
                imgPts[0], imgPts[1], imgPts[2], imgPts[3], focal, pp, camPose);

        float[] planeNormal;
        String planeSource;

        if (vpEstimate != null) {
            planeNormal = vpEstimate.normalWorld.clone();
            planeSource = "VP";
        } else {
            float camFwdY = camFwd[1];
            if (camFwdY < -0.3f) {
                planeNormal = new float[] { 0f, 1f, 0f };
                planeSource = "assumed-floor";
            } else {
                Log.d(TAG, String.format(Locale.US, "%s ABORT: VP failed, camFwdY=%.3f", P, camFwdY));
                return null;
            }
        }

        float dotNF = vec3Dot(planeNormal, camFwd);
        if (dotNF < 0) {
            planeNormal = vec3Scale(planeNormal, -1f);
        }

        double hx = planeNormal[0];
        double hz = planeNormal[2];
        double horizLen = Math.hypot(hx, hz);

        String surfaceClass;
        if (horizLen > 0.70)
            surfaceClass = "WALL";
        else if (horizLen < UNIFIED_WALL_FLOOR_THRESHOLD)
            surfaceClass = "FLOOR";
        else
            surfaceClass = "TILTED";

        if (surfaceClass.equals("FLOOR")) {
            // Lock floor normal perfectly to gravity (DOWN) to prevent oblique
            // dotNF hemisphere flips which reverse the roll basis (East/West).
            // This guarantees an immutable, perfect horizontal projection plane for corners.
            planeNormal = new float[]{0f, -1f, 0f};
        }

        double clampedY = Math.max(-1.0, Math.min(1.0, planeNormal[1]));
        double pitch = Math.toDegrees(Math.asin(clampedY));

        float[] planePt = { camOrigin[0] + camFwd[0], camOrigin[1] + camFwd[1], camOrigin[2] + camFwd[2] };
        float[][] corners3D = new float[4][];
        for (int i = 0; i < 4; i++) {
            corners3D[i] = unifiedIntersectRayPlane(camOrigin, rays[i], planePt, planeNormal);
            if (corners3D[i] == null)
                return null;
        }

        double arYaw;
        String yawSource;
        if (horizLen > UNIFIED_WALL_FLOOR_THRESHOLD) {
            // Negate horizontal components to use the OUTWARD-facing normal
            // (away from wall, toward camera) for yaw. The dotNF flip above
            // resolves the VP normal to point INTO the wall (aligned with
            // camFwd); iOS uses the outward normal, so we negate hx/hz here
            // for yaw only — pitch and roll still use planeNormal as-is.
            arYaw = Math.toDegrees(Math.atan2(-hx / horizLen, hz / horizLen));
            if (arYaw < 0)
                arYaw += 360.0;
            yawSource = "face-normal";
        } else {
            // ── Floor: yaw from the QR's intrinsic "up" direction ──────────────
            // The QR detector identifies finder patterns and returns corners in a
            // QR-intrinsic order (TL/TR/BR/BL relative to the printed code).
            // corners3D[0]-corners3D[3] = TL-BL = left edge = QR "up" direction.
            // We MUST extract this BEFORE canonicalizeFloorCorners(), which
            // reorders corners by world azimuth and erases QR orientation.
            float[] qrUp = { corners3D[0][0] - corners3D[3][0],
                             corners3D[0][1] - corners3D[3][1],
                             corners3D[0][2] - corners3D[3][2] };
            double ux = qrUp[0], uz = qrUp[2];
            double uLen = Math.hypot(ux, uz);
            if (uLen < 0.01)
                return null;
            arYaw = Math.toDegrees(Math.atan2(ux / uLen, -uz / uLen));
            if (arYaw < 0)
                arYaw += 360.0;
            yawSource = "qr-up-vector";

            // Canonicalize corners for stable ROLL computation only.
            // topEdge3D and qrUp3D (used for roll at lines 2034-2039) still
            // benefit from north-anchored ordering to prevent roll jumps.
            float[] arNorthForCanon = unifiedGetARCoreNorth(camPose, fopAttitude);
            if (arNorthForCanon != null) {
                corners3D = canonicalizeFloorCorners(corners3D, arNorthForCanon);
            }
        }

        double northOffset = unifiedComputeNorthOffset(camPose, fopAttitude);
        double compassYaw = (arYaw + (Double.isNaN(northOffset) ? 0 : northOffset)) % 360.0;
        if (compassYaw < 0)
            compassYaw += 360.0;

        float[] worldUp = { 0f, 1f, 0f };
        float[] refCandidate = unifiedProjectOntoPlane(worldUp, planeNormal);
        float refLen = vec3Len(refCandidate);
        String rollRefUsed = "world-up";

        if (refLen < 0.15f) {
            float[] arNorth = unifiedGetARCoreNorth(camPose, fopAttitude);
            if (arNorth != null) {
                refCandidate = unifiedProjectOntoPlane(arNorth, planeNormal);
                refLen = vec3Len(refCandidate);
                rollRefUsed = (refLen >= 0.01) ? "world-north" : "arcore-minus-z-fallback";
            }
            if (refLen < 0.01f) {
                refCandidate = unifiedProjectOntoPlane(new float[] { 0f, 0f, -1f }, planeNormal);
                refLen = vec3Len(refCandidate);
                rollRefUsed = "arcore-minus-z-fallback";
            }
        }

        if (refLen < 0.01f)
            return null;

        float[] topEdge3D = vec3Normalize(new float[] { corners3D[1][0] - corners3D[0][0],
                corners3D[1][1] - corners3D[0][1], corners3D[1][2] - corners3D[0][2] });
        float[] qrUp3D = vec3Normalize(new float[] { corners3D[0][0] - corners3D[3][0], corners3D[0][1] - corners3D[3][1],
                corners3D[0][2] - corners3D[3][2] });

        double rollTopEdge = computeInPlaneAngle(topEdge3D, refCandidate, planeNormal);

        // ── Floor-only diagnostic logging ──────────────────────────────────────────
        if (VERBOSE_FRAME_LOGS && surfaceClass.equals("FLOOR")) {
            Log.d(TAG, String.format(Locale.US,
                    "[floor-diag] planeNormal=(%.3f,%.3f,%.3f) rollRef=(%.3f,%.3f,%.3f) rollRefUsed=%s",
                    planeNormal[0], planeNormal[1], planeNormal[2],
                    refCandidate[0], refCandidate[1], refCandidate[2], rollRefUsed));
            Log.d(TAG, String.format(Locale.US,
                    "[floor-diag] qrUp3D=(%.3f,%.3f,%.3f) topEdge3D=(%.3f,%.3f,%.3f)",
                    qrUp3D[0], qrUp3D[1], qrUp3D[2], topEdge3D[0], topEdge3D[1], topEdge3D[2]));
            Log.d(TAG, String.format(Locale.US,
                    "[floor-diag] arYaw=%.2f northOffset=%.2f compassYaw=%.2f roll=%.2f yawSrc=%s",
                    arYaw, northOffset, compassYaw, rollTopEdge, yawSource));
        }

        double baseConf = planeSource.equals("VP") ? 0.70 : 0.40;
        double vertConf = horizLen > UNIFIED_WALL_FLOOR_THRESHOLD ? Math.min(horizLen, 1.0) : 1.0;
        double confidence = baseConf * vertConf;

        if (VERBOSE_FRAME_LOGS) {
            Log.i(TAG, String.format(Locale.US,
                    "%s [CANONICAL] yaw=%.1f° pitch=%.1f° roll=%.1f° conf=%.2f",
                    P, compassYaw, pitch, rollTopEdge, confidence));
        }

        return new QRMeasurement(compassYaw, pitch, rollTopEdge, confidence, "unified-" + surfaceClass.toLowerCase(),
                true);
    }

    /**
     * Reorders 4 floor corners (already in world space on a gravity-locked plane) into a
     * canonical CCW ordering when viewed from above, anchored so that corners3D[0] is always
     * the corner whose world azimuth from the centroid is closest to true north.
     * This eliminates the view-heading-dependent corner-label assignment that causes
     * yaw and roll to shift together between sessions from different camera headings.
     */
    private static float[][] canonicalizeFloorCorners(float[][] corners3D, float[] arNorth) {
        // Centroid in XZ
        float cx = 0, cz = 0;
        for (float[] c : corners3D) { cx += c[0]; cz += c[2]; }
        cx /= 4; cz /= 4;

        // Azimuth of each corner from centroid, measured CCW from arNorth in the horizontal plane
        double[] azimuths = new double[4];
        for (int i = 0; i < 4; i++) {
            double dx = corners3D[i][0] - cx;
            double dz = corners3D[i][2] - cz;
            // Angle of corner relative to arNorth direction (CCW positive)
            // arNorth = (nx, 0, nz); perpendicular CCW = (-nz, 0, nx)
            double along  = arNorth[0] * dx + arNorth[2] * dz;  // component along north
            double perp   = -arNorth[2] * dx + arNorth[0] * dz; // component CCW-perp to north
            azimuths[i] = Math.toDegrees(Math.atan2(perp, along));
            if (azimuths[i] < 0) azimuths[i] += 360.0;
        }

        // Sort corners by azimuth CCW (starting from northernmost)
        Integer[] idx = {0, 1, 2, 3};
        java.util.Arrays.sort(idx, (a, b) -> Double.compare(azimuths[a], azimuths[b]));

        float[][] sorted = new float[4][];
        for (int i = 0; i < 4; i++) sorted[i] = corners3D[idx[i]];
        return sorted;
    }


    private static double computeInPlaneAngle(float[] directionWorld,
                                              float[] refProjected,
                                              float[] planeNormal) {
        float[] refDir = vec3Normalize(refProjected);
        float[] refPerp = vec3Normalize(vec3Cross(planeNormal, refDir));
        if (vec3Len(refDir) < 1e-4f || vec3Len(refPerp) < 1e-4f) {
            return Double.NaN;
        }
        double cosA = vec3Dot(directionWorld, refDir);
        double sinA = vec3Dot(directionWorld, refPerp);
        double angle = Math.toDegrees(Math.atan2(sinA, cosA));
        if (angle < 0)
            angle += 360.0;
        return angle;
    }


    private List<float[]> toCornerList(float[] points) {
        List<float[]> corners = new ArrayList<>(4);
        for (int i = 0; i < 4; i++) {
            corners.add(new float[] { points[i * 2], points[i * 2 + 1] });
        }
        return corners;
    }

    private float[] captureImageToViewBasis(Frame frame, int imageWidth, int imageHeight) {
        float[] srcBasis = new float[] {
                0f, 0f,
                (float) imageWidth, 0f,
                0f, (float) imageHeight
        };
        float[] dstBasis = new float[6];
        try {
            frame.transformCoordinates2d(
                    com.google.ar.core.Coordinates2d.IMAGE_PIXELS, srcBasis,
                    com.google.ar.core.Coordinates2d.VIEW, dstBasis);
            return dstBasis;
        } catch (Exception e) {
            Log.w(TAG, "captureImageToViewBasis failed", e);
            return null;
        }
    }

    private float[] mapImagePointsToView(float[] imagePoints, float[] viewBasis, int imageWidth, int imageHeight) {
        if (viewBasis == null || imagePoints == null || imagePoints.length != 8 || imageWidth <= 0
                || imageHeight <= 0) {
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
                new Point(imagePoints[6], imagePoints[7]));

        try {
            Imgproc.cornerSubPix(
                    grayMat,
                    corners,
                    new Size(SUBPIX_WINDOW_RADIUS, SUBPIX_WINDOW_RADIUS),
                    new Size(-1, -1),
                    new TermCriteria(
                            TermCriteria.MAX_ITER + TermCriteria.EPS,
                            SUBPIX_MAX_ITER,
                            SUBPIX_EPSILON));

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
    // QR Frame Quality
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Computes geometry quality metrics from detector and refined QR corners.
     * Always logs diagnostics. Corner layout: [TL_x, TL_y, TR_x, TR_y, BR_x, BR_y, BL_x, BL_y].
     */
    private QRFrameQuality evaluateQRFrameQuality(float[] detectorPoints, float[] refinedPoints) {
        float tlX = refinedPoints[0], tlY = refinedPoints[1];
        float trX = refinedPoints[2], trY = refinedPoints[3];
        float brX = refinedPoints[4], brY = refinedPoints[5];
        float blX = refinedPoints[6], blY = refinedPoints[7];

        double top    = Math.hypot(trX - tlX, trY - tlY);
        double right  = Math.hypot(brX - trX, brY - trY);
        double bottom = Math.hypot(blX - brX, blY - brY);
        double left   = Math.hypot(tlX - blX, tlY - blY);
        double minEdge = Math.min(Math.min(top, bottom), Math.min(left, right));

        double maxTB = Math.max(top, bottom);
        double maxLR = Math.max(left, right);
        double ratioTB = maxTB > 0 ? Math.min(top, bottom) / maxTB : 0;
        double ratioLR = maxLR > 0 ? Math.min(left, right) / maxLR : 0;

        // Shoelace area
        double area = 0.5 * Math.abs(
                tlX * trY - trX * tlY
              + trX * brY - brX * trY
              + brX * blY - blX * brY
              + blX * tlY - tlX * blY);

        // Convexity: all consecutive-edge cross products must share the same sign
        float[][] corners = {{tlX, tlY}, {trX, trY}, {brX, brY}, {blX, blY}};
        boolean convex = true;
        int sign = 0;
        for (int i = 0; i < 4; i++) {
            float[] a = corners[i];
            float[] b = corners[(i + 1) % 4];
            float[] c = corners[(i + 2) % 4];
            float cross = (b[0] - a[0]) * (c[1] - b[1]) - (b[1] - a[1]) * (c[0] - b[0]);
            int s = cross > 0 ? 1 : (cross < 0 ? -1 : 0);
            if (s != 0) {
                if (sign == 0) sign = s;
                else if (s != sign) { convex = false; break; }
            }
        }

        // Subpixel shift (detector → refined)
        double totalShift = 0, maxShift = 0;
        for (int i = 0; i < 8; i += 2) {
            double dx = refinedPoints[i] - detectorPoints[i];
            double dy = refinedPoints[i + 1] - detectorPoints[i + 1];
            double shift = Math.hypot(dx, dy);
            totalShift += shift;
            maxShift = Math.max(maxShift, shift);
        }
        double avgShift = totalShift / 4.0;

        QRFrameQuality q = new QRFrameQuality(top, bottom, left, right, minEdge,
                ratioTB, ratioLR, area, avgShift, maxShift, convex);

        if (VERBOSE_FRAME_LOGS) {
            Log.d(TAG, String.format(Locale.US,
                    "[qr-quality] area=%.0f top=%.1f bot=%.1f left=%.1f right=%.1f ratioTB=%.2f ratioLR=%.2f avgShift=%.2f maxShift=%.2f convex=%b",
                    q.area, q.topEdge, q.bottomEdge, q.leftEdge, q.rightEdge,
                    q.ratioTB, q.ratioLR, q.avgShift, q.maxShift, q.convex));
        }

        return q;
    }

    /**
     * Returns true if the frame's QR geometry is good enough for sampling.
     * Logs the specific rejection reason when returning false.
     */
    private boolean isFrameAcceptableForSampling(QRFrameQuality q) {
        if (!q.convex) {
            log("[qr-quality] REJECTED frame for sampling: reason=non-convex quad");
            return false;
        }
        if (q.minEdge < QR_MIN_EDGE_PX) {
            log(String.format(Locale.US,
                    "[qr-quality] REJECTED frame for sampling: reason=minEdge %.1fpx < %.0fpx",
                    q.minEdge, QR_MIN_EDGE_PX));
            return false;
        }
        if (q.area < QR_MIN_AREA_PX2) {
            log(String.format(Locale.US,
                    "[qr-quality] REJECTED frame for sampling: reason=area %.0f < %.0f",
                    q.area, QR_MIN_AREA_PX2));
            return false;
        }
        if (q.ratioTB < QR_MIN_EDGE_RATIO) {
            log(String.format(Locale.US,
                    "[qr-quality] REJECTED frame for sampling: reason=ratioTB %.2f < %.2f",
                    q.ratioTB, QR_MIN_EDGE_RATIO));
            return false;
        }
        if (q.ratioLR < QR_MIN_EDGE_RATIO) {
            log(String.format(Locale.US,
                    "[qr-quality] REJECTED frame for sampling: reason=ratioLR %.2f < %.2f",
                    q.ratioLR, QR_MIN_EDGE_RATIO));
            return false;
        }
        if (q.maxShift > QR_MAX_SUBPIX_SHIFT_PX) {
            log(String.format(Locale.US,
                    "[qr-quality] REJECTED frame for sampling: reason=maxShift %.2fpx > %.1fpx",
                    q.maxShift, QR_MAX_SUBPIX_SHIFT_PX));
            return false;
        }
        return true;
    }

    /**
     * Returns true if the current canonical yaw is consistent with the recent rolling history.
     * Rejects single-frame yaw spikes that exceed the threshold from the recent circular mean.
     * Always passes if the history buffer has fewer than 2 entries (not enough context).
     */
    private boolean isCanonicalYawContinuous(double currentYaw) {
        if (Double.isNaN(currentYaw)) {
            return true; // no measurement — let other gates handle it
        }
        if (recentCanonicalYaws.size() < 2) {
            return true; // not enough history to judge
        }

        // Circular mean of recent canonical yaws (excluding the one we just added)
        double sumSin = 0, sumCos = 0;
        int count = 0;
        for (Double yaw : recentCanonicalYaws) {
            // Skip the last entry — it's the current frame we just pushed
            if (count >= recentCanonicalYaws.size() - 1) break;
            double rad = Math.toRadians(yaw);
            sumSin += Math.sin(rad);
            sumCos += Math.cos(rad);
            count++;
        }
        if (count == 0) return true;

        double recentMean = Math.toDegrees(Math.atan2(sumSin / count, sumCos / count));
        if (recentMean < 0) recentMean += 360.0;

        double diff = Math.abs(angleDifference(recentMean, currentYaw));

        if (diff > CANONICAL_YAW_SPIKE_THRESHOLD_DEG) {
            log(String.format(Locale.US,
                    "[canonical-gate] REJECTED frame for sampling: recentMean=%.1f° current=%.1f° diff=%.1f° > threshold=%.0f°",
                    recentMean, currentYaw, diff, CANONICAL_YAW_SPIKE_THRESHOLD_DEG));
            return false;
        }

        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Continuous QR Detection via ML Kit + ZXing
    // ─────────────────────────────────────────────────────────────────────────
    private void processARFrame(Frame frame) {
        if (!isProcessingFrame.compareAndSet(false, true))
            return;
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
                                if (lastPayload == null)
                                    showStatus("Scan a QR code to begin");
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
                    float[] detectorImagePoints = new float[] {
                            cornerData[0], cornerData[1],
                            cornerData[2], cornerData[3],
                            cornerData[4], cornerData[5],
                            cornerData[6], cornerData[7]
                    };
                    final float[] imagePoints = refineCornersSubPixel(grayMat, detectorImagePoints);
                    final QRFrameQuality frameQuality = evaluateQRFrameQuality(detectorImagePoints, imagePoints);

                    float[] viewPoints = mapImagePointsToView(imagePoints, capturedImageToViewBasis, width, height);
                    List<float[]> cornersList = viewPoints != null ? toCornerList(viewPoints) : null;

                    // ── Always resolve FOP for unified diagnostic (even when not sampling) ──
                    final ResolvedFopAttitude resolvedFopForDiag = resolveFopAttitudeForFrame(capturedImageTimestampNs,
                            capturedAcquireElapsedRealtimeNs);

                    // ── Unified Attitude Resolve ──
                    final ResolvedFopAttitude resolvedFop = isSampling
                            ? resolvedFopForDiag
                            : null;

                    // ── Unified Production Orientation Pipeline ──
                    double diagCanonicalYaw = Double.NaN;
                    {
                        float[] uFop = null;
                        if (resolvedFopForDiag != null) {
                            uFop = resolvedFopForDiag.attitude;
                        } else {
                            TimedAttitude latest = lastFopSample;
                            if (latest != null)
                                uFop = latest.attitude;
                        }
                        try {
                            QRMeasurement diagResult = calculateUnifiedQRMeasurement(
                                    imagePoints,
                                    capturedPose,
                                    capturedIntrinsics,
                                    uFop);
                            if (diagResult != null) {
                                diagCanonicalYaw = diagResult.yaw;
                            }
                        } catch (Exception e) {
                            Log.e("QRYaws", "Unified production pipeline error", e);
                        }
                    }
                    final double frameCanonicalYaw = diagCanonicalYaw;

                    runOnUiThread(() -> {
                        lastDetectedCorners = cornersList;
                        overlayView.setCorners(cornersList);
                        onQRPayloadDetected(payload);

                        // Feed canonical yaw into rolling buffer (every frame, not just sampling)
                        if (!Double.isNaN(frameCanonicalYaw)) {
                            recentCanonicalYaws.addLast(frameCanonicalYaw);
                            while (recentCanonicalYaws.size() > CANONICAL_YAW_HISTORY_SIZE) {
                                recentCanonicalYaws.removeFirst();
                            }
                        }

                        if (isSampling) {
                            if (isFrameAcceptableForSampling(frameQuality)
                                    && isCanonicalYawContinuous(frameCanonicalYaw)) {
                                collectSampleWithPose(imagePoints, capturedPose, capturedIntrinsics, resolvedFop, payload);
                            }
                        }
                    });

                } catch (Exception e) {
                    // If anything inside the thread crashes, log it here!
                    Log.e(TAG, "FATAL ERROR inside QR Executor thread!", e);
                } finally {
                    if (grayMat != null)
                        grayMat.release();
                    for (org.opencv.core.Mat m : points)
                        m.release();
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
            lines.add(
                    String.format(Locale.US, "Δ Pitch : %+.1f°  (tol ±%.0f°)", validation.pitchDelta, PITCH_TOLERANCE));
            lines.add(String.format(Locale.US, "Δ Roll  : %+.1f°  (tol ±%.0f°)", validation.rollDelta, ROLL_TOLERANCE));
            lines.add("Result  : "
                    + (validation.withinTolerance ? "✅ SAME POSITION + ORIENTATION" : "❌ MOVED OR ROTATED"));
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
        if (diff < 60_000)
            return "just now";
        if (diff < 3_600_000)
            return (diff / 60_000) + "m ago";
        if (diff < 86_400_000)
            return (diff / 3_600_000) + "h ago";
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
        if (yaw < 0)
            yaw += 360;
        return yaw;
    }

    private double normalizeDegrees(double angle) {
        angle %= 360.0;
        if (angle < 0)
            angle += 360.0;
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


    private double weightedMeanPitch(List<YawSample> samples) {
        double weightedSum = 0.0;
        double totalWeight = 0.0;
        for (YawSample s : samples) {
            weightedSum += s.pitch * s.weight;
            totalWeight += s.weight;
        }
        if (totalWeight < 1e-6)
            return 0.0;
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
        if (totalWeight < 1e-6)
            return 0.0;
        double mean = Math.toDegrees(Math.atan2(sumSin / totalWeight, sumCos / totalWeight));
        if (mean < 0)
            mean += 360.0;
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
        if (totalWeight < 1e-6)
            return 0;
        double mean = Math.toDegrees(Math.atan2(sumSin / totalWeight, sumCos / totalWeight));
        if (mean < 0)
            mean += 360.0;
        return mean;
    }
}
