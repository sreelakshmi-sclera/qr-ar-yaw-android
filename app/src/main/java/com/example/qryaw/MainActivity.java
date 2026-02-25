package com.example.qryaw;

import android.Manifest;
import android.content.pm.PackageManager;
import android.hardware.Sensor;
import android.hardware.SensorEvent;
import android.hardware.SensorEventListener;
import android.hardware.SensorManager;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import androidx.activity.result.ActivityResultLauncher;
import androidx.activity.result.contract.ActivityResultContracts;
import androidx.annotation.NonNull;
import androidx.annotation.OptIn;
import androidx.appcompat.app.AlertDialog;
import androidx.appcompat.app.AppCompatActivity;
import androidx.camera.core.CameraSelector;
import androidx.camera.core.ExperimentalGetImage;
import androidx.camera.core.ImageAnalysis;
import androidx.camera.core.ImageProxy;
import androidx.camera.core.Preview;
import androidx.camera.lifecycle.ProcessCameraProvider;
import androidx.core.content.ContextCompat;
import androidx.lifecycle.LifecycleOwner;

import com.example.qryaw.db.QRDatabaseHelper;
import com.google.ar.core.ArCoreApk;
import com.google.ar.core.ArCoreApk.Availability;
import com.google.ar.core.Config.DepthMode;
import io.github.sceneview.ar.ARSceneView;
import kotlin.Unit;

import com.google.ar.core.Config;
import com.google.ar.core.Frame;
import com.google.ar.core.HitResult;
import com.google.ar.core.Pose;
import com.google.ar.core.Session;
import com.google.ar.core.TrackingState;
import com.google.ar.core.exceptions.CameraNotAvailableException;
import com.google.ar.core.exceptions.UnavailableArcoreNotInstalledException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.mlkit.vision.barcode.BarcodeScanning;
import com.google.mlkit.vision.barcode.common.Barcode;
import com.google.mlkit.vision.common.InputImage;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MainActivity extends AppCompatActivity implements SensorEventListener {

    private static final String TAG = "QRYaw";
    private static final int REQUIRED_SAMPLES = 8;
    private static final double MIN_SAMPLE_WEIGHT = 0.15;
    private static final double DEFAULT_TOLERANCE = 15.0;

    private ARSceneView arSceneView;
    private Session arSession;
    private QROverlayView overlayView;

    private TextView statusText, infoText, modeText, dbBadge;
    private Button btnRegister, btnValidate, btnHistory;

    private final com.google.mlkit.vision.barcode.BarcodeScanner scanner = BarcodeScanning.getClient();
    private final ExecutorService visionExecutor = Executors.newSingleThreadExecutor();

    // State
    private String lastPayload;
    private QRDatabaseHelper.Registration currentRegistration;
    private boolean isSampling = false;
    private SamplingPurpose purpose = SamplingPurpose.REGISTER;
    private final List<YawSample> sampleBuffer = new ArrayList<>();
    private List<float[]> lastDetectedCorners;   // updated when QR is detected
    private enum SamplingPurpose { REGISTER, VALIDATE }

    private static class YawSample {
        double yaw;
        double weight;
        YawSample(double y, double w) { yaw = y; weight = w; }
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
    private double deviceHeadingDegrees = 0.0; // from compass

    private SensorManager sensorManager;
    private float[] gravity = new float[3];
    private float[] geomagnetic = new float[3];


    private final ActivityResultLauncher<String> permissionLauncher =
            registerForActivityResult(new ActivityResultContracts.RequestPermission(), granted -> {
                if (granted) {
                    startArAndCamera();
                } else {
                    Toast.makeText(this, "Camera permission required", Toast.LENGTH_LONG).show();
                    finish();
                }
            });

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

//        arSceneView = findViewById(R.id.ar_scene_view).findViewById(android.R.id.content); // wait for fragment
        arSceneView = findViewById(R.id.ar_scene_view);
        overlayView = findViewById(R.id.overlay_view);
        statusText = findViewById(R.id.status_text);
        infoText = findViewById(R.id.info_text);
        modeText = findViewById(R.id.mode_text);
        dbBadge = findViewById(R.id.db_badge);
        btnRegister = findViewById(R.id.btn_register);
        btnValidate = findViewById(R.id.btn_validate);
        btnHistory = findViewById(R.id.btn_history);

        btnRegister.setOnClickListener(v -> startSampling(SamplingPurpose.REGISTER));
        btnValidate.setOnClickListener(v -> startSampling(SamplingPurpose.VALIDATE));
        btnHistory.setOnClickListener(v -> showHistory());

        sensorManager = (SensorManager) getSystemService(SENSOR_SERVICE);

        if (ContextCompat.checkSelfPermission(this, Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED) {
            checkArCoreAndStart();
        } else {
            permissionLauncher.launch(Manifest.permission.CAMERA);
        }
    }

    private void checkArCoreAndStart() {
        ArCoreApk.getInstance().checkAvailabilityAsync(this, availability -> {
            if (availability.isTransient()) {
                // Retry soon (post delayed)
                arSceneView.postDelayed(this::checkArCoreAndStart, 200);
                return;
            }
            if (availability.isSupported()) {
                initializeArSession();
                startArAndCamera();
            } else {
                statusText.setText("ARCore not supported on this device.");
            }
        });
    }

    private void initializeArSession() {
        try {
            arSession = new Session(this);
            Config config = new Config(arSession);
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

    private void startArAndCamera() {
        arSceneView.setOnFrame(frameTime -> {

            Frame frame = arSceneView.getFrame();
            if (frame == null) {
                return Unit.INSTANCE;
            }

            if (frame.getCamera().getTrackingState() != TrackingState.TRACKING) {
                return Unit.INSTANCE;
            }

            processFrame(frame);
            return Unit.INSTANCE;
        });

//        startCameraXAnalysis();
        registerSensorListeners();
    }

    private void registerSensorListeners() {
        Sensor accel = sensorManager.getDefaultSensor(Sensor.TYPE_ACCELEROMETER);
        Sensor mag = sensorManager.getDefaultSensor(Sensor.TYPE_MAGNETIC_FIELD);
        if (accel != null) sensorManager.registerListener(this, accel, SensorManager.SENSOR_DELAY_GAME);
        if (mag != null) sensorManager.registerListener(this, mag, SensorManager.SENSOR_DELAY_GAME);
    }

    @Override
    public void onSensorChanged(SensorEvent event) {
        if (event.sensor.getType() == Sensor.TYPE_ACCELEROMETER) {
            gravity = event.values.clone();
        } else if (event.sensor.getType() == Sensor.TYPE_MAGNETIC_FIELD) {
            geomagnetic = event.values.clone();
        }

        float[] rot = new float[9];
        if (SensorManager.getRotationMatrix(rot, null, gravity, geomagnetic)) {
            float[] orientation = new float[3];
            SensorManager.getOrientation(rot, orientation);
            double azimuthRad = orientation[0];
            deviceHeadingDegrees = Math.toDegrees(azimuthRad);
            if (deviceHeadingDegrees < 0) deviceHeadingDegrees += 360;
        }
    }

    @Override
    public void onAccuracyChanged(Sensor sensor, int accuracy) {}

    private void startCameraXAnalysis() {
        ListenableFuture<ProcessCameraProvider> cameraProviderFuture =
                ProcessCameraProvider.getInstance(this);

        cameraProviderFuture.addListener(() -> {
            try {
                ProcessCameraProvider provider = cameraProviderFuture.get();

                ImageAnalysis analysis = new ImageAnalysis.Builder()
                        .setBackpressureStrategy(ImageAnalysis.STRATEGY_KEEP_ONLY_LATEST)
                        .build();

                analysis.setAnalyzer(visionExecutor, this::analyzeImage);

                CameraSelector selector = new CameraSelector.Builder()
                        .requireLensFacing(CameraSelector.LENS_FACING_BACK)
                        .build();

                provider.unbindAll();
                // Only bind ImageAnalysis — ARCore / ArSceneView handles the preview itself
                provider.bindToLifecycle(this, selector, analysis);

            } catch (Exception e) {
                Log.e(TAG, "CameraX setup failed", e);
                statusText.setText("Camera initialization failed");
            }
        }, ContextCompat.getMainExecutor(this));
    }

    @OptIn(markerClass = ExperimentalGetImage.class) private void analyzeImage(@NonNull ImageProxy imageProxy) {
        InputImage image = InputImage.fromMediaImage(imageProxy.getImage(), imageProxy.getImageInfo().getRotationDegrees());

        scanner.process(image)
                .addOnSuccessListener(barcodes -> {
                    Barcode best = null;
                    for (Barcode b : barcodes) {
                        if (b.getFormat() == Barcode.FORMAT_QR_CODE && b.getRawValue() != null) {
                            best = b; // take first for simplicity
                            break;
                        }
                    }
                    if (best != null) {
                        String payload = best.getRawValue();
                        List<float[]> screenCorners = extractScreenCorners(best);
                        runOnUiThread(() -> {
                            overlayView.setCorners(screenCorners);
                            onQrDetected(payload, screenCorners);
                        });
                    }
                })
                .addOnCompleteListener(t -> imageProxy.close());
    }

    private List<float[]> extractScreenCorners(Barcode barcode) {
        // ML Kit gives boundingBox — for real corners use barcode.getCornerPoints() if available
        // Here we approximate with bounding box corners (less accurate but works)
        android.graphics.Rect rect = barcode.getBoundingBox();
        if (rect == null) return null;

        List<float[]> points = new ArrayList<>();
        points.add(new float[]{rect.left, rect.top});     // TL
        points.add(new float[]{rect.right, rect.top});    // TR
        points.add(new float[]{rect.right, rect.bottom}); // BR
        points.add(new float[]{rect.left, rect.bottom});  // BL
        return points;
    }

    private void onQrDetected(String payload, List<float[]> corners) {
        if (payload.equals(lastPayload)) return;
        lastPayload = payload;

        QRDatabaseHelper.Registration reg = QRDatabaseHelper.getInstance(this).fetchRegistration(payload);
        if (reg != null) {
            currentRegistration = reg;
            modeText.setText("MODE: VALIDATE");
            dbBadge.setText("Loaded from DB");
            statusText.setText("Registration found. Press VALIDATE");
            btnValidate.setEnabled(true);
            btnValidate.setAlpha(1f);
        } else {
            currentRegistration = null;
            modeText.setText("MODE: REGISTER");
            dbBadge.setText("New QR — not registered");
            statusText.setText("No prior registration.\nPress REGISTER");
            btnRegister.setEnabled(true);
            btnRegister.setAlpha(1f);
        }
        updateInfoLabel(null, reg, null);
    }

    private void startSampling(SamplingPurpose p) {
        if (isSampling || lastPayload == null) return;
        sampleBuffer.clear();
        purpose = p;
        isSampling = true;
        statusText.setText("Sampling... hold still (" + REQUIRED_SAMPLES + " samples)");
        dbBadge.setText("Sampling 0/" + REQUIRED_SAMPLES);
    }

    private void processFrame(Frame frame) {
        if (!isSampling) {
            return;
        }

        if (lastPayload == null || lastDetectedCorners == null) {
            return;
        }

        // This is the direct equivalent of calling collectSample in Swift
        collectSample(frame, lastDetectedCorners);
    }
    private void collectSample(Frame frame, List<float[]> corners) {
        QRMeasurement result = computeQrYawFromTopEdge(frame, corners);

        if (result == null) {
            return;
        }

        // Skip low-confidence frames (e.g. QR too oblique / edge not clear)
        if (result.confidence < MIN_SAMPLE_WEIGHT) {
            Log.d(TAG, String.format(Locale.US,
                    "Sample rejected (confidence %.2f < %.2f) [%s]",
                    result.confidence, MIN_SAMPLE_WEIGHT, result.method));
            return;
        }

        // Add good sample
        sampleBuffer.add(new YawSample(result.yaw, result.confidence));

        Log.d(TAG, String.format(Locale.US,
                " Sample %d: yaw=%.1f° conf=%.2f [%s]",
                sampleBuffer.size(), result.yaw, result.confidence, result.method));

        // Update UI label on main thread
        runOnUiThread(() -> {
            if (dbBadge != null) {
                dbBadge.setText(String.format(Locale.US,
                        "🔄 Sampling %d/%d [%s]",
                        sampleBuffer.size(), REQUIRED_SAMPLES, result.method));
            }
        });

        // When buffer is full → compute final yaw & commit
        if (sampleBuffer.size() >= REQUIRED_SAMPLES) {
            isSampling = false;

            double finalYaw = circularWeightedMean(sampleBuffer);

            Log.d(TAG, String.format(Locale.US,
                    "Sampling complete. Averaged yaw = %.2f° from %d samples",
                    finalYaw, sampleBuffer.size()));

            // Clean up buffer
            sampleBuffer.clear();

            // Save / validate result
            commitReading(finalYaw, frame);
        }
    }
    private double circularWeightedMean(List<YawSample> samples) {
        double sumSin = 0.0;
        double sumCos = 0.0;
        double totalWeight = 0.0;

        for (YawSample s : samples) {
            double rad = Math.toRadians(s.yaw);
            sumSin += Math.sin(rad) * s.weight;
            sumCos += Math.cos(rad) * s.weight;
            totalWeight += s.weight;
        }

        if (totalWeight < 1e-6) {
            return 0.0;
        }

        double meanRad = Math.atan2(sumSin / totalWeight, sumCos / totalWeight);
        double meanDeg = Math.toDegrees(meanRad);
        if (meanDeg < 0) meanDeg += 360.0;

        return meanDeg;
    }

    private void commitReading(double yaw, Frame frame) {
        if (lastPayload == null) {
            Log.w(TAG, "commitReading called without lastPayload");
            runOnUiThread(() -> statusText.setText("No QR payload available"));
            return;
        }

        // Approximate synthetic normal (for DB compatibility / reference only)
        // In real-world use, you'd compute this more accurately from plane/corners
        double yawRad = Math.toRadians(yaw);
        float normalX = (float) Math.sin(yawRad);
        float normalZ = (float) Math.cos(yawRad);
        // normalY usually ≈ 0 for vertical QR codes

        if (purpose == SamplingPurpose.REGISTER) {
            // ── SAVE NEW REGISTRATION TO DATABASE ──
            Long dbId = QRDatabaseHelper.getInstance(this).saveRegistration(
                    lastPayload,
                    yaw,
                    normalX,      // normal X
                    0.0,          // normal Y (usually near 0)
                    normalZ,      // normal Z
                    DEFAULT_TOLERANCE   // e.g. 15.0
            );

            if (dbId != null) {
                // Create in-memory registration object
                currentRegistration = new QRDatabaseHelper.Registration();
                currentRegistration.id = dbId;
                currentRegistration.payload = lastPayload;
                currentRegistration.yaw = yaw;
                currentRegistration.normalX = normalX;
                currentRegistration.normalY = 0.0;
                currentRegistration.normalZ = normalZ;
                currentRegistration.tolerance = DEFAULT_TOLERANCE;
                currentRegistration.registeredAt = new Date();

                runOnUiThread(() -> {
                    modeText.setText("MODE: VALIDATE");
                    dbBadge.setText("💾 Saved to DB (id=" + dbId + ")");
                    statusText.setText(String.format(Locale.US,
                            "✅ Registered & Saved!\nYaw = %.1f°  |  id=%d",
                            yaw, dbId));

                    btnRegister.setAlpha(0.8f);
                    btnValidate.setEnabled(true);
                    btnValidate.setAlpha(1f);

                    updateInfoLabel(yaw, currentRegistration, null);
                });

                Log.d(TAG, String.format("Registered → yaw=%.1f° dbId=%d", yaw, dbId));
            } else {
                runOnUiThread(() ->
                        statusText.setText("⚠️ Database save failed"));
            }
        } else {
            // ── VALIDATION MODE ──
            if (currentRegistration == null) {
                runOnUiThread(() -> {
                    statusText.setText("⚠️ No registration found. Press REGISTER first.");
                    modeText.setText("MODE: REGISTER");
                });
                return;
            }

            double delta = angleDiff(currentRegistration.yaw, yaw);
            boolean withinTolerance = Math.abs(delta) <= DEFAULT_TOLERANCE;

            // Save validation result to DB
            QRDatabaseHelper.getInstance(this).saveValidation(
                    currentRegistration.id,
                    lastPayload,
                    yaw,
                    currentRegistration.yaw,
                    delta,
                    withinTolerance,
                    DEFAULT_TOLERANCE
            );

            runOnUiThread(() -> {
                String icon = withinTolerance ? "✅" : "❌";
                String status = withinTolerance ? "NOT moved" : "MOVED";

                statusText.setText(String.format(Locale.US,
                        "%s QR has %s\nΔYaw = %+.1f°   (±%.0f° tolerance)",
                        icon, status, delta, DEFAULT_TOLERANCE));

                dbBadge.setText("📝 Validation logged to DB");

                updateInfoLabel(yaw, currentRegistration, delta);
            });

            Log.d(TAG, String.format("Validate → current=%.1f° registered=%.1f° delta=%.1f° within=%b",
                    yaw, currentRegistration.yaw, delta, withinTolerance));
        }
    }
    private QRMeasurement computeQrYawFromTopEdge(Frame frame, List<float[]> corners) {
        if (corners == null || corners.size() != 4) {
            return null;
        }

        // Approximate center in normalized screen coords [0,1]
        float sumX = 0, sumY = 0;
        for (float[] p : corners) {
            sumX += p[0];
            sumY += p[1];
        }
        float centerX = sumX / 4f / arSceneView.getWidth();
        float centerY = sumY / 4f / arSceneView.getHeight();

        String method = "fallback";
        double confidence = 0.45;

        // Try to hit-test the center against planes
        List<HitResult> hits = frame.hitTest(centerX, centerY);
        if (!hits.isEmpty()) {
            HitResult hit = hits.get(0);
            Pose pose = hit.getHitPose();

            // Use plane's Z-axis as approximate normal
            float[] zAxis = pose.getZAxis();  // normal direction

            // Project to horizontal plane (ignore Y/up component)
            double nx = zAxis[0];
            double nz = zAxis[2];
            double len = Math.hypot(nx, nz);

            if (len > 0.12) {  // avoid near-vertical planes
                double yawRad = Math.atan2(nx, nz);  // atan2(x,z) → clockwise from north-ish
                double yawDeg = Math.toDegrees(yawRad);
                if (yawDeg < 0) yawDeg += 360.0;

                method = "plane-normal";
                confidence = 0.70 + (len - 0.12) * 0.6;   // better when normal is strong horizontal
                confidence = Math.min(confidence, 0.95);

                return new QRMeasurement(yawDeg, confidence, method);
            }
        }

        // Very rough fallback: device heading + screen top-edge angle
        float[] tl = corners.get(0); // top-left
        float[] tr = corners.get(1); // top-right
        float dx = tr[0] - tl[0];
        float dy = tr[1] - tl[1];

        double screenAngle = Math.toDegrees(Math.atan2(dy, dx));
        double qrAdjustment = -screenAngle; // heuristic: left-leaning top → facing more north-ish

        double estimatedYaw = (deviceHeadingDegrees + qrAdjustment + 360.0) % 360.0;

        // Basic size-based confidence
        float qrWidthPx = Math.abs(dx);
        float screenWidth = arSceneView.getWidth();
        double sizeConf = Math.min(qrWidthPx / screenWidth * 4.0, 1.0);

        confidence = 0.35 + sizeConf * 0.35;

        return new QRMeasurement(estimatedYaw, confidence, method);
    }
    private double angleDiff(double a, double b) {
        double diff = b - a;
        while (diff > 180) diff -= 360;
        while (diff < -180) diff += 360;
        return diff;
    }

    private void updateInfoLabel(Double curYaw, QRDatabaseHelper.Registration reg, Double delta) {
        List<String> lines = new ArrayList<>();
        if (lastPayload != null) {
            lines.add("QR      : " + lastPayload.substring(0, Math.min(38, lastPayload.length())));
        }
        if (reg != null) {
            lines.add(String.format("Reg Yaw : %.1f°", reg.yaw));
            lines.add("DB id   : " + reg.id);
            lines.add("Source  : DB   (" + relativeTime(reg.registeredAt) + ")");
        }
        if (curYaw != null) {
            lines.add(String.format("Cur Yaw : %.1f°", curYaw));
        }
        if (delta != null) {
            lines.add(String.format("Δ Delta : %+.1f°  (tol ±%.0f°)", delta, DEFAULT_TOLERANCE));
            lines.add("Result  : " + (Math.abs(delta) <= DEFAULT_TOLERANCE ? "✅ SAME" : "❌ MOVED"));
        }
        infoText.setText(String.join("\n", lines));
    }

    private String relativeTime(Date date) {
        long diff = System.currentTimeMillis() - date.getTime();
        if (diff < 60_000) return "just now";
        if (diff < 3_600_000) return (diff / 60_000) + "m ago";
        if (diff < 86_400_000) return (diff / 3_600_000) + "h ago";
        return (diff / 86_400_000) + "d ago";
    }

    private void showHistory() {
        if (lastPayload == null) {
            new AlertDialog.Builder(this)
                    .setTitle("No QR")
                    .setMessage("Scan a QR first.")
                    .setPositiveButton("OK", null)
                    .show();
            return;
        }

        QRDatabaseHelper.Registration reg = QRDatabaseHelper.getInstance(this).fetchRegistration(lastPayload);
        List<QRDatabaseHelper.Validation> vals = QRDatabaseHelper.getInstance(this).fetchValidations(lastPayload, 20);

        StringBuilder msg = new StringBuilder("═══ REGISTRATION ═══\n");
        if (reg != null) {
            msg.append(String.format("Yaw      : %.1f°\n", reg.yaw));
            msg.append(String.format("Tolerance: ±%.0f°\n", reg.tolerance));
            msg.append("Saved    : " + relativeTime(reg.registeredAt) + "\n");
            msg.append("Device   : " + android.os.Build.MODEL + "\n");
        } else {
            msg.append("None found.\n");
        }

        msg.append("\n═══ LAST ").append(vals.size()).append(" VALIDATIONS ═══\n");
        if (vals.isEmpty()) {
            msg.append("No validations yet.");
        } else {
            for (QRDatabaseHelper.Validation v : vals) {
                String icon = v.withinTolerance ? "✅" : "❌";
                msg.append(String.format("%s %s   Δ%+.1f°\n", icon, relativeTime(v.validatedAt), v.delta));
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
                    statusText.setText("Registration deleted.\nScan a QR code.");
                    btnRegister.setEnabled(false);
                    btnValidate.setEnabled(false);
                })
                .show();
    }

    @Override
    protected void onResume() {
        super.onResume();
        if (arSession != null) {
            try {
                arSession.resume();
            } catch (CameraNotAvailableException e) {
                Toast.makeText(this, "Camera unavailable", Toast.LENGTH_LONG).show();
            }
        }
        registerSensorListeners();
    }

    @Override
    protected void onPause() {
        super.onPause();
        if (arSession != null) arSession.pause();
        sensorManager.unregisterListener(this);
    }
}