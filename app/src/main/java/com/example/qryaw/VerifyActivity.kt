package com.example.qryaw

import android.Manifest
import android.util.Log
import android.annotation.SuppressLint
import android.content.SharedPreferences
import android.content.pm.PackageManager
import android.hardware.SensorManager
import android.hardware.camera2.CameraCharacteristics
import android.hardware.camera2.CameraManager
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.os.SystemClock
import android.view.ViewGroup
import android.widget.Toast
import androidx.activity.result.ActivityResultLauncher
import androidx.activity.result.contract.ActivityResultContracts
import androidx.annotation.OptIn
import androidx.appcompat.app.AppCompatActivity
import androidx.camera.core.ExperimentalGetImage
import androidx.core.content.ContextCompat
import com.google.android.gms.location.DeviceOrientation
import com.google.android.gms.location.DeviceOrientationListener
import com.google.android.gms.location.DeviceOrientationRequest
import com.google.android.gms.location.FusedOrientationProviderClient
import com.google.android.gms.location.LocationServices
import com.google.android.gms.tasks.OnFailureListener
import com.google.android.gms.tasks.OnSuccessListener
import com.google.ar.core.ArCoreApk
import com.google.ar.core.ArCoreApk.Availability
import com.google.ar.core.CameraIntrinsics
import com.google.ar.core.Config
import com.google.ar.core.Coordinates2d
import com.google.ar.core.Frame
import com.google.ar.core.Session
import com.google.ar.core.TrackingState
import com.king.wechat.qrcode.WeChatQRCodeDetector
import io.github.sceneview.ar.ARSceneView
import org.opencv.OpenCV
import org.opencv.calib3d.Calib3d
import org.opencv.core.CvType
import org.opencv.core.Mat
import org.opencv.core.MatOfDouble
import org.opencv.core.MatOfPoint2f
import org.opencv.core.MatOfPoint3f
import org.opencv.core.Point
import org.opencv.core.Point3
import org.opencv.core.Size
import org.opencv.core.TermCriteria
import org.opencv.imgproc.Imgproc
import java.util.ArrayDeque
import java.util.Collections
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer
import kotlin.math.abs
import kotlin.math.acos
import kotlin.math.asin
import kotlin.math.atan2
import kotlin.math.cos
import kotlin.math.hypot
import kotlin.math.max
import kotlin.math.min
import kotlin.math.sin
import kotlin.math.sqrt

class VerifyActivity : AppCompatActivity() {
    // ── UI Components ──
    private var arSceneView: ARSceneView? = null
//    private var overlayView: QROverlayView? = null
//    private var blueBorder: QrBlueBorder? = null

    // ── Timeout ──
    private val timeoutHandler = Handler(Looper.getMainLooper())
    private val timeoutRunnable = Runnable {
        setResult(RESULT_CANCELED)
        finish()
    }

    // ── Runtime State ──
    private val magValidationBuffer: MutableList<Double?> = ArrayList<Double?>()
    private var lastMeasurementMethod: String? = null

    @Volatile
    private var isCollectingMag = false
    private var samplingOffsetAnchor = Double.NaN

    @Volatile
    private var lastRawMagHeading = Double.NaN

    @Volatile
    private var lastFopAttitude: FloatArray? = null

    @Volatile
    private var lastPayload: String? = null

    @Volatile
    private var isSampling = false

    @Volatile
    private var committedPayload: String? = null
    private val sampleBuffer: MutableList<YawSample> = ArrayList<YawSample>()
    private var lastSampleTime: Long = 0

    @Volatile
    private var lastDetectedCorners: MutableList<FloatArray?>? = null
    private var lastQrSeenTime: Long = 0
    private var lastSubPixLogTime: Long = 0
    private val isProcessingFrame = AtomicBoolean(false)

    @Volatile
    private var lastFopElapsedRealtimeNs = 0L
    private var backCameraSensorOrientationDegrees = 90
    private var nextSamplingSessionId = 1L
    private var activeSamplingSession: SamplingSession? = null

    private val qrExecutor: ExecutorService = Executors.newSingleThreadExecutor()
    private var fopExecutor: ExecutorService? = null
    private var isFopRegistered = false
    private val fopHistoryLock = Any()
    private val fopAttitudeHistory: ArrayDeque<TimedAttitude> = ArrayDeque<TimedAttitude>()

    // ── Services ──
    private var fusedOrientationClient: FusedOrientationProviderClient? = null

    private enum class SamplingPurpose {
        REGISTER, VALIDATE
    }

    private enum class OffsetMode {
        RELATIVE, ABSOLUTE, UNKNOWN
    }

    // ── Inner Data Classes ──
    private class YawSample(
        val yaw: Double,
        val northYaw: Double,
        val pitch: Double,
        val roll: Double,
        val weight: Double
    )

    private class QRMeasurement(
        val yaw: Double,
        val pitch: Double,
        val roll: Double,
        val confidence: Double,
        val method: String?,
        val rollAbsolute: Boolean
    )

    private class VpEstimate(val normalCam: FloatArray, val normalWorld: FloatArray)

    private class TimedAttitude(val elapsedRealtimeNs: Long, val attitude: FloatArray)

    private class ResolvedFopAttitude(
        val attitude: FloatArray?,
        val targetTimestampNs: Long,
        val referenceTimestampNs: Long,
        val strategy: String?
    )

    private class SamplingSession(
        val id: Long,
        val payload: String?,
        val purpose: SamplingPurpose?,
        val startedAtMs: Long
    ) {
        var lockedMethod: String? = null
        var lockedMethodFamily: String? = null
        var absoluteNorth: Boolean? = null
        var offsetAnchor: Double = Double.NaN
    }

    private fun flushSensors() {
        stopFOP()

        resetAlignment()
        lastPayload = null
        resetSamplingState()
        lastDetectedCorners = null
        lastQrSeenTime = 0
        isProcessingFrame.set(false)
        clearFopHistory()

        val parent = arSceneView!!.getParent() as ViewGroup
        val index = parent.indexOfChild(arSceneView)
        val params = arSceneView!!.getLayoutParams()

        arSceneView!!.destroy()
        parent.removeView(arSceneView)

        arSceneView = ARSceneView(this)
        parent.addView(arSceneView, index, params)

        arSceneView!!.sessionConfiguration = { session: Session?, config: Config? ->
            config!!.setPlaneFindingMode(Config.PlaneFindingMode.HORIZONTAL_AND_VERTICAL)
        }

        arSceneView!!.postDelayed({
            checkArCoreAndStart()
            startFOP()
        }, 500)
    }

    private val fopListener: DeviceOrientationListener = object : DeviceOrientationListener {
        @SuppressLint("SetTextI18n")
        override fun onDeviceOrientationChanged(orientation: DeviceOrientation) {
            val headingError = orientation.getHeadingErrorDegrees()

            if (headingError == 180.0f || headingError > HEADING_ERROR_GATE) {
                return
            }

            val attitude = orientation.attitude
            if (attitude.size >= 4) {
                val normalizedAttitude: FloatArray? = normalizeQuaternionCopy(attitude)
                if (normalizedAttitude != null) {
                    val sampleElapsedRealtimeNs = orientation.getElapsedRealtimeNs()
                    lastFopAttitude = normalizedAttitude.clone()
                    lastFopElapsedRealtimeNs = sampleElapsedRealtimeNs
                    addFopAttitudeSample(sampleElapsedRealtimeNs, normalizedAttitude)
                }
            }

            runOnUiThread(Runnable {
                lastRawMagHeading = getCameraHeadingDegrees(orientation)
                if (isCollectingMag) {
                    val session = activeSamplingSession
                    if (!isSampling || session == null) {
                        isCollectingMag = false
                        return@Runnable
                    }

                    val frame = arSceneView!!.frame
                    if (frame == null || frame.getCamera()
                            .getTrackingState() != TrackingState.TRACKING
                    ) return@Runnable

                    // Camera-forward offset (used only for wall QR)
                    val arYaw = getARCameraYawDegrees(frame)
                    val targetOffset = (lastRawMagHeading - arYaw + 360.0) % 360.0

                    magValidationBuffer.add(targetOffset)

                    if (magValidationBuffer.size >= MAG_SAMPLES_REQUIRED) {
                        isCollectingMag = false
                        val filteredMagSamples = filterCircularOutliers(magValidationBuffer)
                        samplingOffsetAnchor = circularMeanDegrees(filteredMagSamples)
                        session.offsetAnchor = samplingOffsetAnchor
                    }
                }
            })
        }
    }

    private fun resolveBackCameraSensorOrientation(): Int {
        try {
            val cameraManager =
                getSystemService<CameraManager?>(CameraManager::class.java) ?: return 90

            for (cameraId in cameraManager.cameraIdList) {
                val characteristics = cameraManager.getCameraCharacteristics(cameraId)
                val lensFacing = characteristics.get<Int?>(CameraCharacteristics.LENS_FACING)
                if (lensFacing == null || lensFacing != CameraCharacteristics.LENS_FACING_BACK) {
                    continue
                }

                val sensorOrientation =
                    characteristics.get<Int?>(CameraCharacteristics.SENSOR_ORIENTATION)
                if (sensorOrientation != null) {
                    return sensorOrientation
                }
            }
        } catch (e: Exception) {
        }
        return 90
    }

    private fun mapOpenCvCameraVectorToDeviceFrame(
        camX: Double,
        camY: Double,
        camZ: Double
    ): FloatArray {
        val orientation = ((backCameraSensorOrientationDegrees % 360) + 360) % 360

        val deviceX: Float
        val deviceY: Float

        when (orientation) {
            0 -> {
                deviceX = camX.toFloat()
                deviceY = -camY.toFloat()
            }

            90 -> {
                deviceX = -camY.toFloat()
                deviceY = -camX.toFloat()
            }

            180 -> {
                deviceX = -camX.toFloat()
                deviceY = camY.toFloat()
            }

            270 -> {
                deviceX = camY.toFloat()
                deviceY = camX.toFloat()
            }

            else -> {
                deviceX = -camY.toFloat()
                deviceY = -camX.toFloat()
            }
        }

        return floatArrayOf(deviceX, deviceY, -camZ.toFloat())
    }

    private fun addFopAttitudeSample(elapsedRealtimeNs: Long, attitude: FloatArray) {
        synchronized(fopHistoryLock) {
            fopAttitudeHistory.addLast(TimedAttitude(elapsedRealtimeNs, attitude.clone()))
            while (fopAttitudeHistory.size > MAX_FOP_HISTORY_SAMPLES) {
                fopAttitudeHistory.removeFirst()
            }
        }
    }

    private fun clearFopHistory() {
        synchronized(fopHistoryLock) {
            fopAttitudeHistory.clear()
        }
        lastFopAttitude = null
        lastFopElapsedRealtimeNs = 0L
    }

    private fun resolveFopAttitudeAt(
        targetTimestampNs: Long,
        strategyBase: String?
    ): ResolvedFopAttitude? {
        if (targetTimestampNs <= 0) return null

        synchronized(fopHistoryLock) {
            if (fopAttitudeHistory.isEmpty()) return null
            var prev: TimedAttitude? = null
            var next: TimedAttitude? = null

            for (sample in fopAttitudeHistory) {
                if (sample.elapsedRealtimeNs <= targetTimestampNs) {
                    prev = sample
                }
                if (sample.elapsedRealtimeNs >= targetTimestampNs) {
                    next = sample
                    break
                }
            }

            if (prev != null && next != null) {
                if (prev.elapsedRealtimeNs == next.elapsedRealtimeNs) {
                    val deltaNs = abs(targetTimestampNs - prev.elapsedRealtimeNs)
                    if (deltaNs <= MAX_FOP_NEAREST_SAMPLE_AGE_NS) {
                        return ResolvedFopAttitude(
                            prev.attitude.clone(),
                            targetTimestampNs,
                            prev.elapsedRealtimeNs,
                            strategyBase + "-exact"
                        )
                    }
                } else {
                    val spanNs = next.elapsedRealtimeNs - prev.elapsedRealtimeNs
                    if (spanNs > 0 && spanNs <= MAX_FOP_INTERPOLATION_SPAN_NS) {
                        val alpha =
                            (targetTimestampNs - prev.elapsedRealtimeNs).toDouble() / spanNs.toDouble()
                        val interpolated: FloatArray? =
                            slerpQuaternion(prev.attitude, next.attitude, alpha)
                        if (interpolated != null) {
                            return ResolvedFopAttitude(
                                interpolated,
                                targetTimestampNs,
                                targetTimestampNs,
                                strategyBase + "-interp"
                            )
                        }
                    }
                }
            }

            var nearest: TimedAttitude? = null
            var nearestDeltaNs = Long.MAX_VALUE
            if (prev != null) {
                nearest = prev
                nearestDeltaNs = abs(targetTimestampNs - prev.elapsedRealtimeNs)
            }
            if (next != null) {
                val nextDeltaNs = abs(targetTimestampNs - next.elapsedRealtimeNs)
                if (nextDeltaNs < nearestDeltaNs) {
                    nearest = next
                    nearestDeltaNs = nextDeltaNs
                }
            }

            if (nearest != null && nearestDeltaNs <= MAX_FOP_NEAREST_SAMPLE_AGE_NS) {
                return ResolvedFopAttitude(
                    nearest.attitude.clone(),
                    targetTimestampNs,
                    nearest.elapsedRealtimeNs,
                    "$strategyBase-nearest"
                )
            }
            return null
        }
    }

    private fun resolveFopAttitudeForFrame(
        imageTimestampNs: Long,
        acquireElapsedRealtimeNs: Long
    ): ResolvedFopAttitude? {
        var resolved = resolveFopAttitudeAt(imageTimestampNs, "image")
        if (resolved != null) return resolved

        resolved = resolveFopAttitudeAt(acquireElapsedRealtimeNs, "acquire")
        if (resolved != null) return resolved

        val latest = lastFopAttitude
        if (latest == null) return null

        return ResolvedFopAttitude(
            latest.clone(),
            acquireElapsedRealtimeNs,
            lastFopElapsedRealtimeNs,
            "latest"
        )
    }

    private val permissionLauncher: ActivityResultLauncher<Array<String?>> =
        registerForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) { permissions ->
            val cameraGranted = permissions.getOrDefault(Manifest.permission.CAMERA, false)
            if (cameraGranted) {
                checkArCoreAndStart()
                startFOP()
            } else {
                Toast.makeText(this, "Camera permission required", Toast.LENGTH_LONG).show()
                finish()
            }
        } as ActivityResultLauncher<Array<String?>>

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_verify)
        timeoutHandler.postDelayed(timeoutRunnable, 15_000L)

        arSceneView = findViewById(R.id.ar_scene_view)
//        overlayView = findViewById(R.id.overlay_view)
//        blueBorder = findViewById(R.id.blue_border)
//        statusText = findViewById(R.id.status_text)

        OpenCV.initOpenCV()
        WeChatQRCodeDetector.init(this)
        backCameraSensorOrientationDegrees = resolveBackCameraSensorOrientation()

        fusedOrientationClient = LocationServices.getFusedOrientationProviderClient(this)

        arSceneView!!.sessionConfiguration = { _: Session?, config: Config? ->
            config!!.setPlaneFindingMode(Config.PlaneFindingMode.HORIZONTAL_AND_VERTICAL)
            null
        }

        resetSamplingState()

        val hasCamera = ContextCompat.checkSelfPermission(this, Manifest.permission.CAMERA) ==
                PackageManager.PERMISSION_GRANTED
        if (hasCamera) {
            checkArCoreAndStart()
        } else {
            permissionLauncher.launch(
                arrayOf(
                    Manifest.permission.CAMERA,
                    Manifest.permission.ACCESS_FINE_LOCATION,
                    Manifest.permission.ACCESS_COARSE_LOCATION
                )
            )
        }
    }

    override fun onResume() {
        super.onResume()
        resetSamplingState()
        resetAlignment()
        startFOP()
    }

    override fun onPause() {
        super.onPause()
        if (isSampling) {
            abortActiveSampling("Sampling paused. Please scan again.")
        }
        stopFOP()
    }

    override fun onDestroy() {
        timeoutHandler.removeCallbacks(timeoutRunnable)
        stopFOP()
        qrExecutor.shutdownNow()
        if (fopExecutor != null) {
            fopExecutor!!.shutdownNow()
            fopExecutor = null
        }
        super.onDestroy()
    }

    private fun ensureFopExecutor(): ExecutorService? {
        if (fopExecutor == null || fopExecutor!!.isShutdown()) {
            fopExecutor = Executors.newSingleThreadExecutor()
        }
        return fopExecutor
    }

    @SuppressLint("MissingPermission")
    private fun startFOP() {
        if (fusedOrientationClient == null || isFopRegistered) return
        isFopRegistered = true

        val request =
            DeviceOrientationRequest.Builder(DeviceOrientationRequest.OUTPUT_PERIOD_FAST).build()
        fusedOrientationClient!!
            .requestOrientationUpdates(request, ensureFopExecutor()!!, fopListener)
            .addOnSuccessListener(OnSuccessListener { _: Void? -> })
            .addOnFailureListener(OnFailureListener { _: Exception? -> isFopRegistered = false })
    }

    private fun stopFOP() {
        if (fusedOrientationClient != null && isFopRegistered) {
            fusedOrientationClient!!.removeOrientationUpdates(fopListener)
        }
        isFopRegistered = false
        clearFopHistory()
    }

    private fun resetAlignment() {
        samplingOffsetAnchor = Double.NaN
        lastRawMagHeading = 0.0
        clearFopHistory()
    }

    private fun checkArCoreAndStart() {
        ArCoreApk.getInstance()
            .checkAvailabilityAsync(this, Consumer { availability: Availability? ->
                if (availability!!.isTransient()) {
                    arSceneView!!.postDelayed({ checkArCoreAndStart() }, 200)
                    return@Consumer
                }
                if (availability.isSupported()) {
                    initializeArSession()
                    startArSession()
                }
            })
    }

    private fun initializeArSession() {
        try {
            arSceneView!!.planeRenderer.isEnabled = false
        } catch (e: Exception) {
        }
    }

    private fun startArSession() {
        resetAlignment()
        arSceneView!!.onFrame = label@{ frameTime: Long? ->
            val frame = arSceneView!!.frame
            if (frame == null) return@label Unit

            if (frame.getCamera().getTrackingState() == TrackingState.TRACKING) {
                processARFrame(frame)
            }
            Unit
        }
    }

    private fun onQRPayloadDetected(payload: String) {
        Log.d("test", "onQRPayloadDetected: payload=$payload isSampling=$isSampling")
        if (isFinishing) return
        val session = activeSamplingSession
        if (session != null && payload != session.payload) {
            Log.d(
                "test",
                "onQRPayloadDetected: QR changed, aborting: old=${session.payload} new=$payload"
            )
            abortActiveSampling("QR changed during sampling. Try again.")
        }
        if (isSampling) return
        if (payload == committedPayload) return
        lastPayload = payload
        startSampling(SamplingPurpose.REGISTER, payload)
    }

    private val isAlignmentReady: Boolean
        get() = !lastRawMagHeading.isNaN()


    private val offsetPreferences: SharedPreferences
        get() = getSharedPreferences(
            OFFSET_PREFS_NAME,
            MODE_PRIVATE
        )

    private fun offsetKey(payload: String): String {
        return OFFSET_KEY_PREFIX + payload
    }

    private fun offsetModeKey(payload: String): String {
        return OFFSET_MODE_KEY_PREFIX + payload
    }

    private fun readStoredRelativeOffset(payload: String?): Double? {
        if (payload == null) return null
        val prefs = this.offsetPreferences
        val key = offsetKey(payload)
        if (!prefs.contains(key)) return null

        val offset = prefs.getFloat(key, Float.NaN)
        return if (offset.isNaN()) null else offset.toDouble()
    }

    private fun persistOffsetMetadata(
        payload: String?,
        isAbsoluteNorth: Boolean,
        offsetUsed: Double
    ) {
        if (payload == null) return

        val editor = this.offsetPreferences.edit()
            .putString(
                offsetModeKey(payload),
                if (isAbsoluteNorth) OffsetMode.ABSOLUTE.name else OffsetMode.RELATIVE.name
            )

        if (isAbsoluteNorth || offsetUsed.isNaN()) {
            editor.remove(offsetKey(payload))
        } else {
            editor.putFloat(offsetKey(payload), offsetUsed.toFloat())
        }
        editor.apply()
    }

    private fun normalizeMeasurementFamily(method: String?): String {
        if (method == null) return "unknown"

        val absoluteNorth = isAbsoluteNorthMethod(method)
        if (method.startsWith("wall-")) {
            return if (absoluteNorth) "wall-direct" else "wall-relative"
        }
        if (method.startsWith("floor-")) {
            return if (absoluteNorth) "floor-direct" else "floor-relative"
        }
        return if (absoluteNorth) "direct" else "relative"
    }

    private fun resolveActiveOffsetAnchor(session: SamplingSession?): Double {
        if (session != null && !session.offsetAnchor.isNaN()) {
            return session.offsetAnchor
        }
        return samplingOffsetAnchor
    }

    private fun resetSamplingState() {
        sampleBuffer.clear()
        magValidationBuffer.clear()
        lastMeasurementMethod = null
        lastSampleTime = 0
        samplingOffsetAnchor = Double.NaN
        activeSamplingSession = null
        isSampling = false
        isCollectingMag = false
    }

    private fun abortActiveSampling(reason: String?) {
        if (!isSampling && activeSamplingSession == null && sampleBuffer.isEmpty() && magValidationBuffer.isEmpty()) {
            return
        }
        resetSamplingState()
    }

    private fun startSampling(p: SamplingPurpose?, payloadSnapshot: String?) {
        if (payloadSnapshot == null) {
            return
        }

        resetSamplingState()
        activeSamplingSession = SamplingSession(
            nextSamplingSessionId++,
            payloadSnapshot,
            p,
            System.currentTimeMillis()
        )
        isSampling = true
        isCollectingMag = true
        Log.d("test", "startSampling: purpose=$p payload=$payloadSnapshot")

        val action = if (p == SamplingPurpose.REGISTER) "Registering" else "Validating"
    }

    private fun collectSampleWithPose(
        imagePoints: FloatArray, camPose: FloatArray,
        intrinsics: CameraIntrinsics,
        resolvedFopAttitude: ResolvedFopAttitude?,
        framePayload: String?
    ) {
        val session = activeSamplingSession
        Log.d(
            "test",
            "collectSampleWithPose: isSampling=$isSampling session=${session?.payload} framePayload=$framePayload fopAttitude=${resolvedFopAttitude?.attitude != null}"
        )
        if (!isSampling || session == null) {
            Log.d("test", "collectSampleWithPose: skipped – not sampling or no session")
            return
        }
        if (session.payload != framePayload) {
            Log.d("test", "collectSampleWithPose: skipped – payload mismatch")
            return
        }
        val result = computeQRYawFromTopEdgeWithPose(
            imagePoints,
            camPose,
            intrinsics,
            resolvedFopAttitude?.attitude
        )
        Log.d(
            "test",
            "computeQRYaw result: yaw=${result?.yaw} pitch=${result?.pitch} roll=${result?.roll} method=${result?.method}"
        )

        if (result == null) {
            Log.d("test", "collectSampleWithPose: computeQRYaw returned null")
            return
        }
        processSample(result)
    }

    private fun processSample(result: QRMeasurement) {
        val session = activeSamplingSession
        if (!isSampling || session == null) {
            return
        }

        var sampleConfidence = result.confidence
        if (isWallDirectMethod(result.method) && sampleBuffer.size < WALL_EARLY_SAMPLE_CAP_COUNT) {
            val cappedConfidence = min(sampleConfidence, WALL_EARLY_SAMPLE_CONFIDENCE_CAP)
            if (cappedConfidence < sampleConfidence) {
                sampleConfidence = cappedConfidence
            }
        }

        if (sampleConfidence < MIN_SAMPLE_WEIGHT) {
            return
        }

        if (result.roll.isNaN()) {
            return
        }

        val sampleAbsoluteNorth = isAbsoluteNorthMethod(result.method)
        val sampleMethodFamily = normalizeMeasurementFamily(result.method)
        if (session.lockedMethod == null) {
            session.lockedMethod = result.method
            session.lockedMethodFamily = sampleMethodFamily
            session.absoluteNorth = sampleAbsoluteNorth
            lastMeasurementMethod = result.method
        } else if ((sampleMethodFamily != session.lockedMethodFamily) || session.absoluteNorth == null || session.absoluteNorth != sampleAbsoluteNorth) {
            return
        }

        val isAbsoluteNorth = session.absoluteNorth == true
        val activeOffset = resolveActiveOffsetAnchor(session)

        var sampleNorth: Double
        if (isAbsoluteNorth) {
            sampleNorth = result.yaw
        } else {
            if (activeOffset.isNaN()) {
                sampleNorth = result.yaw
            } else {
                sampleNorth = (result.yaw + activeOffset) % 360.0
                if (sampleNorth < 0) sampleNorth += 360.0
            }
        }

        val sampleRoll: Double
        if (result.rollAbsolute) {
            sampleRoll = normalizeDegrees(result.roll)
        } else {
            sampleRoll = if (activeOffset.isNaN())
                normalizeDegrees(result.roll)
            else
                normalizeDegrees(result.roll + activeOffset)
        }

        if (!sampleBuffer.isEmpty()) {
            val yawReference = if (isAbsoluteNorth)
                circularWeightedMeanNorth(sampleBuffer)
            else
                circularWeightedMeanRaw(sampleBuffer)
            val yawCompareValue = if (isAbsoluteNorth) sampleNorth else result.yaw
            val yawDiff = abs(angleDifference(yawReference, yawCompareValue))
            val rollReference = circularWeightedMeanRoll(sampleBuffer)
            val rollDiff = abs(angleDifference(rollReference, sampleRoll))
            val pitchReference = weightedMeanPitch(sampleBuffer)
            val pitchDiff = abs(pitchReference - result.pitch)
            val outlierThreshold = if (isAbsoluteNorth)
                DIRECT_METHOD_OUTLIER_THRESHOLD_DEG
            else
                45.0

            if (yawDiff > outlierThreshold || rollDiff > outlierThreshold || pitchDiff > PITCH_OUTLIER_THRESHOLD_DEG) {
                Log.w(
                    "test",
                    "processSample: outlier yawDiff=$yawDiff rollDiff=$rollDiff pitchDiff=$pitchDiff (thresholds: yaw/roll=$outlierThreshold pitch=$PITCH_OUTLIER_THRESHOLD_DEG)"
                )
                if (sampleConfidence >= DIRECT_SAMPLE_RESET_CONFIDENCE
                    && sampleBuffer.get(0).weight < 0.5
                ) {
                    Log.w("test", "processSample: high-confidence outlier — clearing buffer")
                    sampleBuffer.clear()
                } else {
                    return
                }
            }
        }

        val now = System.currentTimeMillis()
        if (lastSampleTime > 0 && (now - lastSampleTime) > MAX_SAMPLE_GAP_MS) {
            Log.w(
                "test",
                "processSample: sample gap ${now - lastSampleTime}ms > ${MAX_SAMPLE_GAP_MS}ms — buffer cleared (had ${sampleBuffer.size} samples)"
            )
            sampleBuffer.clear()
        }
        lastSampleTime = now

        sampleBuffer.add(
            YawSample(
                result.yaw,
                sampleNorth,
                result.pitch,
                sampleRoll,
                sampleConfidence
            )
        )

        Log.d(
            "test",
            "processSample: sample added buffer=${sampleBuffer.size}/$REQUIRED_SAMPLES  yaw=$sampleNorth  pitch=${result.pitch}  roll=$sampleRoll"
        )
        if (sampleBuffer.size >= REQUIRED_SAMPLES) {
            isSampling = false

            val finalYaw: Double
            if (isAbsoluteNorth) {
                finalYaw = circularWeightedMeanNorth(sampleBuffer)
            } else {
                finalYaw = circularWeightedMeanRaw(sampleBuffer)
            }
            val finalPitch = weightedMeanPitch(sampleBuffer)
            val finalRoll = circularWeightedMeanRoll(sampleBuffer)

            sampleBuffer.clear()
            commitReading(session, finalYaw, finalPitch, finalRoll)
        }
    }

    private fun circularMeanDegrees(samples: MutableList<Double?>): Double {
        if (samples.isEmpty()) return 0.0
        var sumSin = 0.0
        var sumCos = 0.0
        for (value in samples) {
            val rad = Math.toRadians(value!!)
            sumSin += sin(rad)
            sumCos += cos(rad)
        }
        var mean = Math.toDegrees(atan2(sumSin / samples.size, sumCos / samples.size))
        if (mean < 0) mean += 360.0
        return mean
    }

    private fun filterCircularOutliers(samples: MutableList<Double?>): MutableList<Double?> {
        if (samples.size < 5) return ArrayList<Double?>(samples)

        val center = circularMeanDegrees(samples)
        val deviations: MutableList<Double> = ArrayList<Double>(samples.size)
        for (sample in samples) {
            deviations.add(abs(angleDifference(center, sample!!)))
        }
        Collections.sort(deviations)

        val medianDeviation: Double = deviations.get(deviations.size / 2)
        val threshold = max(3.0, min(20.0, medianDeviation * 3.0 + 2.0))

        val filtered: MutableList<Double?> = ArrayList<Double?>(samples.size)
        for (sample in samples) {
            if (abs(angleDifference(center, sample!!)) <= threshold) {
                filtered.add(sample)
            }
        }

        val minimumKept = max(8, samples.size / 2)
        if (filtered.size < minimumKept) {
            return ArrayList<Double?>(samples)
        }
        return filtered
    }

    private fun commitReading(
        session: SamplingSession?,
        inputYaw: Double,
        inputPitch: Double,
        inputRoll: Double
    ) {
        if (session == null || session.payload == null) return

        val payload = session.payload
        val isAbsoluteNorth = session.absoluteNorth == true
        val finalRoll = normalizeDegrees(inputRoll)

        val offsetUsed: Double
        var finalNorthYaw: Double
        if (isAbsoluteNorth) {
            offsetUsed = 0.0
            finalNorthYaw = inputYaw
        } else {
            offsetUsed = resolveActiveOffsetAnchor(session)
            if (!offsetUsed.isNaN()) {
                finalNorthYaw = (inputYaw + offsetUsed) % 360.0
                if (finalNorthYaw < 0) finalNorthYaw += 360.0
            } else {
                finalNorthYaw = inputYaw
            }
        }

        activeSamplingSession = null
        lastMeasurementMethod = null
        samplingOffsetAnchor = Double.NaN
        committedPayload = payload

        Log.d(
            "test",
            "commitReading: purpose=${session.purpose} payload=$payload yaw=$finalNorthYaw pitch=$inputPitch roll=$finalRoll"
        )
        if (session.purpose == SamplingPurpose.REGISTER) {
            persistOffsetMetadata(payload, isAbsoluteNorth, offsetUsed)
            val resultIntent = android.content.Intent().apply {
                putExtra("url", payload)
                putExtra("yaw", finalNorthYaw)
                putExtra("pitch", inputPitch)
                putExtra("roll", finalRoll)
            }
            runOnUiThread {
                setResult(RESULT_OK, resultIntent)
                finish()
            }
        } else {
            val storedOffset = readStoredRelativeOffset(payload)
            if (storedOffset != null && !offsetUsed.isNaN()) {
                val offsetDelta = abs(angleDifference(storedOffset, offsetUsed))
                val offsetDrifted = offsetDelta > 10.0
            }
        }
    }

    private fun computeQRYawFromTopEdgeWithPose(
        imagePoints: FloatArray, camPose: FloatArray,
        intr: CameraIntrinsics, fopAttitude: FloatArray?
    ): QRMeasurement? {
        val focal = intr.getFocalLength()
        val pp = intr.getPrincipalPoint()

        val imgPts = arrayOf(
            floatArrayOf(imagePoints[0], imagePoints[1]),
            floatArrayOf(imagePoints[2], imagePoints[3]),
            floatArrayOf(imagePoints[4], imagePoints[5]),
            floatArrayOf(imagePoints[6], imagePoints[7])
        )

        val vpEstimate = computeNormalViaVanishingPoint(
            imgPts[0], imgPts[1], imgPts[2], imgPts[3], focal, pp, camPose
        )
        var planeNormal = if (vpEstimate != null) vpEstimate.normalWorld.clone() else null
        var vpNormalCam = if (vpEstimate != null) vpEstimate.normalCam.clone() else null

        if (planeNormal == null) {
            val camFwdY = -camPose[9]
            if (camFwdY < -0.3f) {
                planeNormal = floatArrayOf(0f, 1f, 0f)
            } else {
                return null
            }
        }

        // 2. Ensure Normal is facing the camera
        val camFwd = floatArrayOf(-camPose[8], -camPose[9], -camPose[10])
        val dotNormalCamFwd: kotlin.Float = vec3Dot(planeNormal, camFwd)
        if (dotNormalCamFwd < 0) {
            planeNormal = vec3Scale(planeNormal, -1f)
            if (vpNormalCam != null) {
                vpNormalCam = vec3Scale(vpNormalCam, -1f)
            }
        }

        val clampedY = max(-1.0, min(1.0, planeNormal[1].toDouble()))
        val pitch = Math.toDegrees(asin(clampedY))

        val hx = planeNormal[0].toDouble()
        val hz = planeNormal[2].toDouble()
        val horizLen = hypot(hx, hz)
        var vpWallRawYaw = Double.NaN
        var vpWallNorthYaw = Double.NaN

        if (horizLen > 0.50) {
            vpWallRawYaw = normalizeDegrees(Math.toDegrees(atan2(hx, -hz)))
            if (!samplingOffsetAnchor.isNaN()) {
                vpWallNorthYaw = normalizeDegrees(vpWallRawYaw + samplingOffsetAnchor)
            }
            val wallResult = computeWallYawWithSolvePnP(
                imagePoints,
                intr,
                camPose,
                fopAttitude,
                vpNormalCam,
                vpWallRawYaw,
                vpWallNorthYaw
            )
            if (wallResult != null) {
                return wallResult
            }

            val wallFallback = buildWallVpFallbackMeasurement(
                imagePoints,
                focal,
                pp,
                camPose,
                fopAttitude,
                planeNormal,
                vpNormalCam,
                pitch,
                vpWallRawYaw
            )
            if (wallFallback != null) {
                return wallFallback
            }

            return null
        } else {
            val pnpResult = computeFloorYawWithSolvePnP(
                imagePoints,
                intr,
                camPose,
                fopAttitude
            )
            if (pnpResult != null) {
                return pnpResult
            }

            return null
        }
    }

    private fun computeNormalViaVanishingPoint(
        imgTL: FloatArray, imgTR: FloatArray, imgBR: FloatArray, imgBL: FloatArray,
        focalLength: FloatArray, principalPt: FloatArray, camTransform: FloatArray
    ): VpEstimate? {
        try {
            val fx = focalLength[0]
            val fy = focalLength[1]
            val cx = principalPt[0]
            val cy = principalPt[1]

            val hTL = floatArrayOf(imgTL[0], imgTL[1], 1f)
            val hTR = floatArrayOf(imgTR[0], imgTR[1], 1f)
            val hBR = floatArrayOf(imgBR[0], imgBR[1], 1f)
            val hBL = floatArrayOf(imgBL[0], imgBL[1], 1f)

            val lineTop: FloatArray = vec3Cross(hTL, hTR)
            val lineBottom: FloatArray = vec3Cross(hBL, hBR)
            val lineLeft: FloatArray = vec3Cross(hTL, hBL)
            val lineRight: FloatArray = vec3Cross(hTR, hBR)

            val vpX: FloatArray = vec3Cross(lineTop, lineBottom)
            val vpY: FloatArray = vec3Cross(lineLeft, lineRight)

            if (abs(vpX[2]) < 1e-6f || abs(vpY[2]) < 1e-6f) {
                return null
            }

            val rayX: FloatArray = toCameraRay(vpX, fx, fy, cx, cy)
            val rayY: FloatArray = toCameraRay(vpY, fx, fy, cx, cy)

            if (vec3Len(rayX) < 0.001f || vec3Len(rayY) < 0.001f) {
                return null
            }
            var normalCam: FloatArray = vec3Normalize(vec3Cross(rayX, rayY))
            if (normalCam[2] < 0) normalCam = vec3Scale(normalCam, -1f)

            val normalWorld: FloatArray = rotateCamToWorld(normalCam, camTransform)
            return VpEstimate(normalCam, normalWorld)
        } catch (e: Exception) {
            return null
        }
    }

    private fun isViewingAngleAcceptable(
        imagePoints: FloatArray,
        camPose: FloatArray,
        intrinsics: CameraIntrinsics
    ): Boolean {
        val focal = intrinsics.getFocalLength()
        val pp = intrinsics.getPrincipalPoint()
        val tl = floatArrayOf(imagePoints[0], imagePoints[1])
        val tr = floatArrayOf(imagePoints[2], imagePoints[3])
        val br = floatArrayOf(imagePoints[4], imagePoints[5])
        val bl = floatArrayOf(imagePoints[6], imagePoints[7])
        val vp = computeNormalViaVanishingPoint(tl, tr, br, bl, focal, pp, camPose) ?: return false
        // normalCam[2] is dot product with camera forward (0,0,1); already forced positive
        return vp.normalCam[2] >= 0.4f  // reject angles > ~66° off-axis
    }

    private fun angleDifference(a: Double, b: Double): Double {
        var diff = b - a
        while (diff > 180) diff -= 360.0
        while (diff < -180) diff += 360.0
        return diff
    }

    private fun isAbsoluteNorthMethod(method: String?): Boolean {
        return method != null && method.endsWith("-direct")
    }

    private fun isWallDirectMethod(method: String?): Boolean {
        return method != null && method.startsWith("wall-") && method.endsWith("-direct")
    }

    private fun confidenceFromReprojectionError(reprojErrorPx: Double): Double {
        if (reprojErrorPx.isNaN() || reprojErrorPx.isInfinite() || reprojErrorPx < 0.0) {
            return 0.0
        }

        val normalizedError: Double = reprojErrorPx / DIRECT_REPROJECTION_CONFIDENCE_SCALE_PX
        return 1.0 / (1.0 + normalizedError * normalizedError)
    }

    private val wallContinuityReferenceYaw: Double
        get() {
            if (sampleBuffer.isEmpty()) return Double.NaN
            if (!isWallDirectMethod(lastMeasurementMethod)) return Double.NaN
            return circularWeightedMeanNorth(sampleBuffer)
        }

    private fun buildWallVpFallbackMeasurement(
        imagePoints: FloatArray,
        focalLength: FloatArray,
        principalPoint: FloatArray,
        camPose: FloatArray,
        fopAttitude: FloatArray?,
        planeNormalWorld: FloatArray,
        planeNormalCam: FloatArray?,
        pitch: Double,
        vpWallRawYaw: Double
    ): QRMeasurement? {
        if (planeNormalCam == null) {
            return null
        }

        val vpUpCam = computeWallVpUpVectorInCamera(
            imagePoints,
            focalLength,
            principalPoint,
            planeNormalCam
        )
        if (vpUpCam == null) {
            return null
        }

        val vpUpWorld: FloatArray = rotateCamToWorld(vpUpCam, camPose)
        val roll = computeWallRollDegrees(vpUpWorld, planeNormalWorld)
        if (roll.isNaN()) {
            return null
        }

        // Use the 180° aligned wall-normal math so the VP fallback matches the direct wall path.
        val alignedVpYaw = normalizeDegrees(vpWallRawYaw + 180.0)
        var finalYaw = alignedVpYaw

        // Legacy 2D offset mode still needs the sampled anchor baked in here.
        if (fopAttitude == null && !samplingOffsetAnchor.isNaN()) {
            finalYaw = normalizeDegrees(alignedVpYaw + samplingOffsetAnchor)
        }
        return QRMeasurement(finalYaw, pitch, roll, 0.8, "wall-vp-direct", true)
    }

    private fun computeWallVpUpVectorInCamera(
        imagePoints: FloatArray,
        focalLength: FloatArray,
        principalPoint: FloatArray,
        planeNormalCam: FloatArray
    ): FloatArray? {
        val fx = focalLength[0]
        val fy = focalLength[1]
        val cx = principalPoint[0]
        val cy = principalPoint[1]

        val centerX = (imagePoints[0] + imagePoints[2] + imagePoints[4] + imagePoints[6]) * 0.25f
        val centerY = (imagePoints[1] + imagePoints[3] + imagePoints[5] + imagePoints[7]) * 0.25f
        val topMidX = (imagePoints[0] + imagePoints[2]) * 0.5f
        val topMidY = (imagePoints[1] + imagePoints[3]) * 0.5f

        val normalCam: FloatArray = vec3Normalize(planeNormalCam)
        val centerRay: FloatArray = toCameraRay(floatArrayOf(centerX, centerY, 1f), fx, fy, cx, cy)
        val topMidRay: FloatArray = toCameraRay(floatArrayOf(topMidX, topMidY, 1f), fx, fy, cx, cy)

        val centerDenom: kotlin.Float = vec3Dot(centerRay, normalCam)
        val topDenom: kotlin.Float = vec3Dot(topMidRay, normalCam)
        if (abs(centerDenom) < 1e-6f || abs(topDenom) < 1e-6f) {
            return null
        }

        // Plane scale is arbitrary here; n·X = 1 is enough to recover an in-plane direction.
        val centerPoint: FloatArray = vec3Scale(centerRay, 1f / centerDenom)
        val topPoint: FloatArray = vec3Scale(topMidRay, 1f / topDenom)
        var upCam = floatArrayOf(
            topPoint[0] - centerPoint[0],
            topPoint[1] - centerPoint[1],
            topPoint[2] - centerPoint[2]
        )

        val normalComponent: kotlin.Float = vec3Dot(upCam, normalCam)
        upCam = floatArrayOf(
            upCam[0] - normalCam[0] * normalComponent,
            upCam[1] - normalCam[1] * normalComponent,
            upCam[2] - normalCam[2] * normalComponent
        )

        if (vec3Len(upCam) < 1e-5f) {
            return null
        }
        return vec3Normalize(upCam)
    }

    private fun toCornerList(points: FloatArray): MutableList<FloatArray?> {
        val corners: MutableList<FloatArray?> = ArrayList<FloatArray?>(4)
        for (i in 0..3) {
            corners.add(floatArrayOf(points[i * 2], points[i * 2 + 1]))
        }
        return corners
    }

    private fun refineCornersSubPixel(grayMat: Mat, imagePoints: FloatArray): FloatArray {
        var i = 0
        while (i < imagePoints.size) {
            val x = imagePoints[i]
            val y = imagePoints[i + 1]
            if (x < SUBPIX_WINDOW_RADIUS || y < SUBPIX_WINDOW_RADIUS || x >= grayMat.cols() - SUBPIX_WINDOW_RADIUS || y >= grayMat.rows() - SUBPIX_WINDOW_RADIUS) {
                return imagePoints
            }
            i += 2
        }

        val corners = MatOfPoint2f(
            Point(imagePoints[0].toDouble(), imagePoints[1].toDouble()),
            Point(imagePoints[2].toDouble(), imagePoints[3].toDouble()),
            Point(imagePoints[4].toDouble(), imagePoints[5].toDouble()),
            Point(imagePoints[6].toDouble(), imagePoints[7].toDouble())
        )

        try {
            Imgproc.cornerSubPix(
                grayMat,
                corners,
                Size(SUBPIX_WINDOW_RADIUS.toDouble(), SUBPIX_WINDOW_RADIUS.toDouble()),
                Size(-1.0, -1.0),
                TermCriteria(
                    TermCriteria.MAX_ITER + TermCriteria.EPS,
                    SUBPIX_MAX_ITER,
                    SUBPIX_EPSILON
                )
            )

            val refined = corners.toArray()
            val refinedPoints = FloatArray(imagePoints.size)
            var totalShift = 0.0
            var maxShift = 0.0
            for (i in refined.indices) {
                val xIndex = i * 2
                val yIndex = xIndex + 1
                refinedPoints[xIndex] = refined[i]!!.x.toFloat()
                refinedPoints[yIndex] = refined[i]!!.y.toFloat()

                val dx = (refinedPoints[xIndex] - imagePoints[xIndex]).toDouble()
                val dy = (refinedPoints[yIndex] - imagePoints[yIndex]).toDouble()
                val shift = hypot(dx, dy)
                totalShift += shift
                maxShift = max(maxShift, shift)
            }

            val now = System.currentTimeMillis()
            if (now - lastSubPixLogTime >= SUBPIX_LOG_INTERVAL_MS) {
                lastSubPixLogTime = now
            }
            return refinedPoints
        } catch (e: Exception) {
            return imagePoints
        } finally {
            corners.release()
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Continuous QR Detection via ML Kit + ZXing
    // ─────────────────────────────────────────────────────────────────────────
    private fun processARFrame(frame: Frame) {
        if (!isProcessingFrame.compareAndSet(false, true)) return
        scanQrFromArFrame(frame)
    }

    @OptIn(ExperimentalGetImage::class)
    private fun scanQrFromArFrame(frame: Frame) {
        try {
            val cameraImage = frame.acquireCameraImage()
            val capturedImageTimestampNs = cameraImage.getTimestamp()
            val capturedAcquireElapsedRealtimeNs = SystemClock.elapsedRealtimeNanos()

            val capturedIntrinsics = frame.getCamera().getImageIntrinsics()
            val capturedPose = FloatArray(16)
            frame.getCamera().getPose().toMatrix(capturedPose, 0)
            val capturedFrame = frame

            // 1. Extract Grayscale (Y) plane SAFELY (Strips hardware padding)
            val yPlane = cameraImage.getPlanes()[0]
            val yBuffer = yPlane.getBuffer()
            val yRowStride = yPlane.getRowStride()
            val width = cameraImage.getWidth()
            val height = cameraImage.getHeight()

            val yData = ByteArray(width * height)

            if (yRowStride == width) {
                // Tightly packed memory (No padding)
                yBuffer.get(yData)
            } else {
                // Padded memory - strip it row by row so OpenCV doesn't crash
                val rowBuffer = ByteArray(yRowStride)
                for (row in 0..<height) {
                    yBuffer.position(row * yRowStride)
                    yBuffer.get(rowBuffer, 0, min(yRowStride, yBuffer.remaining()))
                    System.arraycopy(rowBuffer, 0, yData, row * width, width)
                }
            }

            cameraImage.close() // Close immediately to free ARCore

            // 2. Offload to background thread
            qrExecutor.execute {
                try {
                    // Initialize Mat with the perfectly sized byte array
                    val grayMat = Mat(height, width, CvType.CV_8UC1)
                    grayMat.put(0, 0, yData)

                    val points: MutableList<Mat> = ArrayList<Mat>()
                    val results = WeChatQRCodeDetector.detectAndDecode(grayMat, points)

                    // --- QR LOST LOGIC ---
                    if (results == null || results.isEmpty() || points.isEmpty()) {
                        val now = System.currentTimeMillis()
                        val qrTrulyLost = (now - lastQrSeenTime) > QR_LOST_TIMEOUT_MS
                        if (qrTrulyLost) {
                            runOnUiThread {
                                lastDetectedCorners = null
                                if (isSampling) {
                                    abortActiveSampling("⚠️ QR lost during sampling. Try again.")
                                }
                            }
                        }

                        // Release C++ memory
                        grayMat.release()
                        return@execute  // Jumps to 'finally' block
                    }

                    // --- QR FOUND LOGIC ---
                    lastQrSeenTime = System.currentTimeMillis()
                    val payload = results.get(0)
                    Log.d("test", "processARFrame: QR detected payload=$payload")

                    // Extract Intrinsic Corners
                    val cornersMat = points.get(0)
                    val cornerData = FloatArray(8)
                    cornersMat.get(0, 0, cornerData)

                    // [0,1]=TL | [2,3]=TR | [4,5]=BR | [6,7]=BL
                    val detectorImagePoints = floatArrayOf(
                        cornerData[0], cornerData[1],
                        cornerData[2], cornerData[3],
                        cornerData[4], cornerData[5],
                        cornerData[6], cornerData[7]
                    )
                    val imagePoints = refineCornersSubPixel(grayMat, detectorImagePoints)

                    // Project to AR View for overlay
                    val viewPoints = FloatArray(8)
                    try {
                        capturedFrame.transformCoordinates2d(
                            Coordinates2d.IMAGE_PIXELS, imagePoints,
                            Coordinates2d.VIEW, viewPoints
                        )
                    } catch (e: Exception) {
                        grayMat.release()
                        cornersMat.release()
                        return@execute  // Jumps to 'finally' block
                    }

                    val cornersList = toCornerList(viewPoints)

                    runOnUiThread {
                        lastDetectedCorners = cornersList

                        Log.d(
                            "test",
                            "processARFrame: calling onQRPayloadDetected isSampling=$isSampling"
                        )
                        onQRPayloadDetected(payload)

                        Log.d(
                            "test",
                            "processARFrame: after onQRPayloadDetected isSampling=$isSampling isCollectingMag=$isCollectingMag samplingOffsetAnchor=$samplingOffsetAnchor"
                        )
                        if (isSampling && !isCollectingMag && !samplingOffsetAnchor.isNaN()) {
                            Log.d("test", "processARFrame: calling collectSampleWithPose")
                            val resolvedFopAttitude = resolveFopAttitudeForFrame(
                                capturedImageTimestampNs,
                                capturedAcquireElapsedRealtimeNs
                            )
                            collectSampleWithPose(
                                imagePoints,
                                capturedPose,
                                capturedIntrinsics,
                                resolvedFopAttitude,
                                payload
                            )
                        }
                    }

                    grayMat.release()
                    for (m in points) m.release()
                } catch (e: Exception) {
                    Log.e("test", "processARFrame: exception in QR executor", e)
                } finally {
                    isProcessingFrame.set(false)
                }
            }
        } catch (e: Exception) {
            Log.e("test", "processARFrame: exception starting QR executor", e)
            isProcessingFrame.set(false)
        }
    }

    private fun getARCameraYawDegrees(frame: Frame): Double {
        val m = FloatArray(16)
        frame.getCamera().getPose().toMatrix(m, 0)
        val fx = -m[8]
        val fz = -m[10]
        var yaw = Math.toDegrees(atan2(fx.toDouble(), -fz.toDouble()))
        if (yaw < 0) yaw += 360.0
        return yaw
    }

    private fun getCameraHeadingDegrees(orientation: DeviceOrientation): Double {
        val attitude = orientation.getAttitude()
        if (attitude.size < 4) {
            return orientation.getHeadingDegrees().toDouble()
        }

        val rotationMatrix = FloatArray(9)
        SensorManager.getRotationMatrixFromVector(rotationMatrix, attitude)

        val east = -rotationMatrix[2]
        val north = -rotationMatrix[5]

        if (hypot(east.toDouble(), north.toDouble()) < 1e-5) {
            return orientation.getHeadingDegrees().toDouble()
        }

        var heading = Math.toDegrees(atan2(east.toDouble(), north.toDouble()))
        if (heading < 0) heading += 360.0
        return heading
    }

    private fun normalizeDegrees(angle: Double): Double {
        var angle = angle
        angle %= 360.0
        if (angle < 0) angle += 360.0
        return angle
    }

    private fun computeWallRollDegrees(upWorld: FloatArray, normalWorld: FloatArray): Double {
        var referenceUp = floatArrayOf(
            -normalWorld[0] * normalWorld[1],
            1f - normalWorld[1] * normalWorld[1],
            -normalWorld[2] * normalWorld[1]
        )
        if (vec3Len(referenceUp) < 1e-4f) {
            return Double.NaN
        }

        referenceUp = vec3Normalize(referenceUp)
        val referenceRight: FloatArray = vec3Normalize(vec3Cross(normalWorld, referenceUp))
        if (vec3Len(referenceRight) < 1e-4f) {
            return Double.NaN
        }

        val cosAngle = vec3Dot(upWorld, referenceUp).toDouble()
        val sinAngle = vec3Dot(upWorld, referenceRight).toDouble()
        return normalizeDegrees(Math.toDegrees(atan2(sinAngle, cosAngle)))
    }

    private fun computeFloorYawWithSolvePnP(
        imagePoints: FloatArray,
        intrinsics: CameraIntrinsics,
        camPose: FloatArray,
        fopAttitude: FloatArray?
    ): QRMeasurement? {
        try {
            val focal = intrinsics.getFocalLength()
            val pp = intrinsics.getPrincipalPoint()

            val fx = focal[0].toDouble()
            val fy = focal[1].toDouble()
            val cx = pp[0].toDouble()
            val cy = pp[1].toDouble()

            val cameraMatrix = Mat(3, 3, CvType.CV_64F)
            cameraMatrix.put(
                0, 0,
                fx, 0.0, cx,
                0.0, fy, cy,
                0.0, 0.0, 1.0
            )

            val distCoeffs = MatOfDouble(0.0, 0.0, 0.0, 0.0, 0.0)

            val half = 0.1.toFloat() / 2.0

            // IPPE_SQUARE expects points in this exact square coordinate order:
            // top-left, top-right, bottom-right, bottom-left in object space.
            val objectPoints = MatOfPoint3f(
                Point3(-half, half, 0.0),
                Point3(half, half, 0.0),
                Point3(half, -half, 0.0),
                Point3(-half, -half, 0.0)
            )

            val imagePts = MatOfPoint2f(
                Point(imagePoints[0].toDouble(), imagePoints[1].toDouble()),
                Point(imagePoints[2].toDouble(), imagePoints[3].toDouble()),
                Point(imagePoints[4].toDouble(), imagePoints[5].toDouble()),
                Point(imagePoints[6].toDouble(), imagePoints[7].toDouble())
            )

            val rvecs: MutableList<Mat> = ArrayList<Mat>()
            val tvecs: MutableList<Mat> = ArrayList<Mat>()

            val numSolutions = Calib3d.solvePnPGeneric(
                objectPoints, imagePts, cameraMatrix, distCoeffs,
                rvecs, tvecs, false, Calib3d.SOLVEPNP_IPPE_SQUARE,
                Mat(), Mat(), Mat()
            )

            if (numSolutions == 0) {
                return null
            }

            // === DETERMINE WHICH PATH TO USE ===
            val useDirect = (fopAttitude != null)

            var bestScore = Double.MAX_VALUE
            var bestYaw = 0.0
            var bestPitch = 0.0
            var bestRoll = 0.0
            var bestReprojectionError = Double.NaN

            for (solIdx in 0..<numSolutions) {
                val rotationMatrix = Mat()
                Calib3d.Rodrigues(rvecs.get(solIdx), rotationMatrix)

                val normalCamX = rotationMatrix.get(0, 2)[0]
                val normalCamY = rotationMatrix.get(1, 2)[0]
                val normalCamZ = rotationMatrix.get(2, 2)[0]

                val upCamX = rotationMatrix.get(0, 1)[0]
                val upCamY = rotationMatrix.get(1, 1)[0]
                val upCamZ = rotationMatrix.get(2, 1)[0]

                val upCamAndroid = floatArrayOf(
                    upCamX.toFloat(),
                    -upCamY.toFloat(),
                    -upCamZ.toFloat()
                )
                val normalCamAndroid = floatArrayOf(
                    normalCamX.toFloat(),
                    -normalCamY.toFloat(),
                    -normalCamZ.toFloat()
                )

                val uWorld: FloatArray = rotateCamToWorld(upCamAndroid, camPose)
                val nWorld: FloatArray = rotateCamToWorld(normalCamAndroid, camPose)

                if (nWorld[1] < 0) {
                    uWorld[0] = -uWorld[0]
                    uWorld[1] = -uWorld[1]
                    uWorld[2] = -uWorld[2]
                }

                val solPitch = Math.toDegrees(
                    asin(
                        max(-1.0, min(1.0, nWorld[1].toDouble()))
                    )
                )

                val score = abs(uWorld[1]).toDouble()
                val reprojError = computeReprojectionRmse(
                    objectPoints,
                    imagePts,
                    cameraMatrix,
                    distCoeffs,
                    rvecs.get(solIdx),
                    tvecs.get(solIdx)
                )

                var solYaw: Double

                if (useDirect) {
                    val upCamDevice = mapOpenCvCameraVectorToDeviceFrame(upCamX, upCamY, upCamZ)

                    val rotMatrix = FloatArray(9)
                    SensorManager.getRotationMatrixFromVector(rotMatrix, fopAttitude)

                    var upCamFinal = upCamDevice
                    if (nWorld[1] < 0) {
                        upCamFinal = floatArrayOf(-upCamDevice[0], -upCamDevice[1], -upCamDevice[2])
                    }
                    val upEast =
                        rotMatrix[0] * upCamFinal[0] + rotMatrix[1] * upCamFinal[1] + rotMatrix[2] * upCamFinal[2]
                    val upNorth =
                        rotMatrix[3] * upCamFinal[0] + rotMatrix[4] * upCamFinal[1] + rotMatrix[5] * upCamFinal[2]
                    solYaw = Math.toDegrees(atan2(upEast.toDouble(), upNorth.toDouble()))
                    if (solYaw < 0) solYaw += 360.0
                } else {
                    solYaw = normalizeDegrees(
                        Math.toDegrees(
                            atan2(
                                uWorld[0].toDouble(),
                                uWorld[2].toDouble()
                            )
                        )
                    )
                }
                val solRoll = solYaw

                if (score < bestScore) {
                    bestScore = score
                    bestYaw = solYaw
                    bestPitch = solPitch
                    bestRoll = solRoll
                    bestReprojectionError = reprojError
                }
                rotationMatrix.release()
            }
            for (r in rvecs) r.release()
            for (t in tvecs) t.release()

            val method = if (useDirect) "floor-solvepnp-direct" else "floor-solvepnp"
            val confidence = if (useDirect)
                confidenceFromReprojectionError(bestReprojectionError)
            else
                1.0
            return QRMeasurement(bestYaw, bestPitch, bestRoll, confidence, method, useDirect)
        } catch (e: Exception) {
            return null
        }
    }

    // Reprojection RMSE for a candidate PnP pose.
    private fun computeReprojectionRmse(
        objectPoints: MatOfPoint3f,
        imagePoints: MatOfPoint2f,
        cameraMatrix: Mat,
        distCoeffs: MatOfDouble,
        rvec: Mat,
        tvec: Mat
    ): Double {
        val projectedPoints = MatOfPoint2f()
        try {
            Calib3d.projectPoints(
                objectPoints,
                rvec,
                tvec,
                cameraMatrix,
                distCoeffs,
                projectedPoints
            )
            val projected = projectedPoints.toArray()
            val observed = imagePoints.toArray()
            val count = min(projected.size, observed.size)
            if (count == 0) return Double.MAX_VALUE

            var sumSq = 0.0
            for (i in 0..<count) {
                val dx = projected[i]!!.x - observed[i]!!.x
                val dy = projected[i]!!.y - observed[i]!!.y
                sumSq += dx * dx + dy * dy
            }
            return sqrt(sumSq / count)
        } finally {
            projectedPoints.release()
        }
    }

    private fun computeWallYawWithSolvePnP(
        imagePoints: FloatArray,
        intrinsics: CameraIntrinsics,
        camPose: FloatArray,
        fopAttitude: FloatArray?,
        vpNormalCam: FloatArray?,
        vpWallRawYaw: Double,
        vpWallNorthYaw: Double
    ): QRMeasurement? {
        try {
            val focal = intrinsics.getFocalLength()
            val pp = intrinsics.getPrincipalPoint()

            val fx = focal[0].toDouble()
            val fy = focal[1].toDouble()
            val cx = pp[0].toDouble()
            val cy = pp[1].toDouble()

            val cameraMatrix = Mat(3, 3, CvType.CV_64F)
            cameraMatrix.put(
                0, 0,
                fx, 0.0, cx,
                0.0, fy, cy,
                0.0, 0.0, 1.0
            )

            val distCoeffs = MatOfDouble(0.0, 0.0, 0.0, 0.0, 0.0)
            val half = 0.1.toFloat() / 2.0
            val objectPoints = MatOfPoint3f(
                Point3(-half, half, 0.0),
                Point3(half, half, 0.0),
                Point3(half, -half, 0.0),
                Point3(-half, -half, 0.0)
            )

            val imagePts = MatOfPoint2f(
                Point(imagePoints[0].toDouble(), imagePoints[1].toDouble()),
                Point(imagePoints[2].toDouble(), imagePoints[3].toDouble()),
                Point(imagePoints[4].toDouble(), imagePoints[5].toDouble()),
                Point(imagePoints[6].toDouble(), imagePoints[7].toDouble())
            )

            val rvecs: MutableList<Mat> = ArrayList<Mat>()
            val tvecs: MutableList<Mat> = ArrayList<Mat>()

            val numSolutions = Calib3d.solvePnPGeneric(
                objectPoints, imagePts, cameraMatrix, distCoeffs,
                rvecs, tvecs, false, Calib3d.SOLVEPNP_IPPE_SQUARE,
                Mat(), Mat(), Mat()
            )

            if (numSolutions == 0) {
                cameraMatrix.release()
                distCoeffs.release()
                objectPoints.release()
                imagePts.release()
                return null
            }

            val useDirect = (fopAttitude != null)
            val rotMatrix = FloatArray(9)
            if (useDirect) {
                SensorManager.getRotationMatrixFromVector(rotMatrix, fopAttitude)
            }
            val continuityReferenceYaw = this.wallContinuityReferenceYaw

            var bestScore = Double.MAX_VALUE
            var bestYaw = 0.0
            var bestPitch = 0.0
            var bestRoll = 0.0
            var bestReprojectionError = Double.NaN
            var bestSolutionIdx = -1
            var bestNormalCamX = 0.0
            var bestNormalCamY = 0.0
            var bestNormalCamZ = 0.0
            var secondBestScore = Double.MAX_VALUE
            var secondBestYaw = 0.0
            var secondBestSolutionIdx = -1

            for (solIdx in 0..<numSolutions) {
                val rotationMatrix = Mat()
                try {
                    Calib3d.Rodrigues(rvecs.get(solIdx), rotationMatrix)

                    val normalCamX = rotationMatrix.get(0, 2)[0]
                    val normalCamY = rotationMatrix.get(1, 2)[0]
                    val normalCamZ = rotationMatrix.get(2, 2)[0]
                    val upCamX = rotationMatrix.get(0, 1)[0]
                    val upCamY = rotationMatrix.get(1, 1)[0]
                    val upCamZ = rotationMatrix.get(2, 1)[0]

                    val normalCamAndroid = floatArrayOf(
                        normalCamX.toFloat(),
                        -normalCamY.toFloat(),
                        -normalCamZ.toFloat()
                    )
                    val upCamAndroid = floatArrayOf(
                        upCamX.toFloat(),
                        -upCamY.toFloat(),
                        -upCamZ.toFloat()
                    )
                    val normalWorld: FloatArray = rotateCamToWorld(normalCamAndroid, camPose)
                    val upWorld: FloatArray = rotateCamToWorld(upCamAndroid, camPose)

                    var solYaw: Double
                    if (useDirect) {
                        val normalDevice =
                            mapOpenCvCameraVectorToDeviceFrame(normalCamX, normalCamY, normalCamZ)
                        val normalEast =
                            rotMatrix[0] * normalDevice[0] + rotMatrix[1] * normalDevice[1] + rotMatrix[2] * normalDevice[2]
                        val normalNorth =
                            rotMatrix[3] * normalDevice[0] + rotMatrix[4] * normalDevice[1] + rotMatrix[5] * normalDevice[2]

                        val horizLenEarth = hypot(normalEast.toDouble(), normalNorth.toDouble())
                        if (horizLenEarth < 1e-5) {
                            continue
                        }

                        solYaw =
                            Math.toDegrees(atan2(normalEast.toDouble(), normalNorth.toDouble()))
                        if (solYaw < 0) solYaw += 360.0
                    } else {
                        val horizLenWorld =
                            hypot(normalWorld[0].toDouble(), normalWorld[2].toDouble())
                        if (horizLenWorld < 1e-5) {
                            continue
                        }
                        solYaw = normalizeDegrees(
                            Math.toDegrees(
                                atan2(
                                    normalWorld[0].toDouble(),
                                    normalWorld[2].toDouble()
                                )
                            )
                        )
                    }

                    val solPitch = Math.toDegrees(
                        asin(
                            max(-1.0, min(1.0, normalWorld[1].toDouble()))
                        )
                    )
                    val solRoll = computeWallRollDegrees(upWorld, normalWorld)
                    if (solRoll.isNaN()) {
                        continue
                    }

                    val reprojError = computeReprojectionRmse(
                        objectPoints,
                        imagePts,
                        cameraMatrix,
                        distCoeffs,
                        rvecs.get(solIdx),
                        tvecs.get(solIdx)
                    )
                    if (reprojError > WALL_DIRECT_REPROJECTION_GATE_PX) {
                        continue
                    }

                    val frontFacing = normalCamZ < 0.0
                    val facingPenalty = if (frontFacing) 0.0 else WALL_BACKFACING_PENALTY
                    val continuityPenalty = if (continuityReferenceYaw.isNaN())
                        0.0
                    else
                        (WALL_CONTINUITY_PENALTY_PER_DEG
                                * abs(angleDifference(continuityReferenceYaw, solYaw)))
                    val score = reprojError + facingPenalty + continuityPenalty

                    if (score < bestScore) {
                        secondBestScore = bestScore
                        secondBestYaw = bestYaw
                        secondBestSolutionIdx = bestSolutionIdx

                        bestScore = score
                        bestYaw = solYaw
                        bestPitch = solPitch
                        bestRoll = solRoll
                        bestReprojectionError = reprojError
                        bestSolutionIdx = solIdx
                        bestNormalCamX = normalCamX
                        bestNormalCamY = normalCamY
                        bestNormalCamZ = normalCamZ
                    } else if (score < secondBestScore) {
                        secondBestScore = score
                        secondBestYaw = solYaw
                        secondBestSolutionIdx = solIdx
                    }
                } finally {
                    rotationMatrix.release()
                }
            }

            cameraMatrix.release()
            distCoeffs.release()
            objectPoints.release()
            imagePts.release()
            for (r in rvecs) r.release()
            for (t in tvecs) t.release()

            if (bestSolutionIdx < 0) {
                return null
            }

            if (secondBestSolutionIdx >= 0) {
                val yawSplit = abs(angleDifference(bestYaw, secondBestYaw))
                val scoreMargin = secondBestScore - bestScore
                if (yawSplit >= WALL_AMBIGUITY_YAW_SPLIT_DEG
                    && scoreMargin <= WALL_AMBIGUITY_MAX_SCORE_MARGIN
                ) {
                    return QRMeasurement(
                        bestYaw,
                        bestPitch,
                        bestRoll,
                        0.0,
                        if (useDirect) "wall-solvepnp-direct" else "wall-solvepnp",
                        true
                    )
                }
            }

            val confidence = confidenceFromReprojectionError(bestReprojectionError)

            return QRMeasurement(
                bestYaw,
                bestPitch,
                bestRoll,
                confidence,
                if (useDirect) "wall-solvepnp-direct" else "wall-solvepnp",
                true
            )
        } catch (e: Exception) {
            return null
        }
    }

    private fun weightedMeanPitch(samples: MutableList<YawSample>): Double {
        var weightedSum = 0.0
        var totalWeight = 0.0
        for (s in samples) {
            weightedSum += s.pitch * s.weight
            totalWeight += s.weight
        }
        if (totalWeight < 1e-6) return 0.0
        return weightedSum / totalWeight
    }

    private fun circularWeightedMeanRoll(samples: MutableList<YawSample>): Double {
        var sumSin = 0.0
        var sumCos = 0.0
        var totalWeight = 0.0
        for (s in samples) {
            val rad = Math.toRadians(s.roll)
            sumSin += sin(rad) * s.weight
            sumCos += cos(rad) * s.weight
            totalWeight += s.weight
        }
        if (totalWeight < 1e-6) return 0.0
        var mean = Math.toDegrees(atan2(sumSin / totalWeight, sumCos / totalWeight))
        if (mean < 0) mean += 360.0
        return mean
    }

    private fun circularWeightedMeanRaw(samples: MutableList<YawSample>): Double {
        var sumSin = 0.0
        var sumCos = 0.0
        var totalWeight = 0.0
        for (s in samples) {
            val rad = Math.toRadians(s.yaw)
            sumSin += sin(rad) * s.weight
            sumCos += cos(rad) * s.weight
            totalWeight += s.weight
        }
        if (totalWeight < 1e-6) return 0.0
        var mean = Math.toDegrees(atan2(sumSin / totalWeight, sumCos / totalWeight))
        if (mean < 0) mean += 360.0
        return mean
    }

    private fun circularWeightedMeanNorth(samples: MutableList<YawSample>): Double {
        var sumSin = 0.0
        var sumCos = 0.0
        var totalWeight = 0.0
        for (s in samples) {
            val rad = Math.toRadians(s.northYaw)
            sumSin += sin(rad) * s.weight
            sumCos += cos(rad) * s.weight
            totalWeight += s.weight
        }
        if (totalWeight < 1e-6) return 0.0
        var mean = Math.toDegrees(atan2(sumSin / totalWeight, sumCos / totalWeight))
        if (mean < 0) mean += 360.0
        return mean
    }

    companion object {
        private const val TAG = "QRYaws"

        private const val REQUIRED_SAMPLES = 8
        private const val MIN_SAMPLE_WEIGHT = 0.15
        private const val HEADING_ERROR_GATE = 40.0f
        private const val MAG_SAMPLES_REQUIRED = 15
        private const val MAX_SAMPLE_GAP_MS: Long = 2000
        private const val QR_LOST_TIMEOUT_MS: Long = 500
        private const val SUBPIX_WINDOW_RADIUS = 5
        private const val SUBPIX_MAX_ITER = 20
        private const val SUBPIX_EPSILON = 0.03
        private const val SUBPIX_LOG_INTERVAL_MS: Long = 500
        private const val MAX_FOP_HISTORY_SAMPLES = 128
        private const val MAX_FOP_INTERPOLATION_SPAN_NS = 75000000L
        private const val MAX_FOP_NEAREST_SAMPLE_AGE_NS = 50000000L
        private const val DIRECT_METHOD_OUTLIER_THRESHOLD_DEG = 8.0
        private const val DIRECT_SAMPLE_RESET_CONFIDENCE = 0.85
        private const val DIRECT_REPROJECTION_CONFIDENCE_SCALE_PX = 1.2
        private const val WALL_DIRECT_REPROJECTION_GATE_PX = 1.8
        private const val WALL_AMBIGUITY_YAW_SPLIT_DEG = 4.0
        private const val WALL_AMBIGUITY_MAX_SCORE_MARGIN = 0.20
        private const val WALL_EARLY_SAMPLE_CAP_COUNT = 2
        private const val WALL_EARLY_SAMPLE_CONFIDENCE_CAP = 0.70
        private const val PITCH_OUTLIER_THRESHOLD_DEG = 20.0
        private const val WALL_BACKFACING_PENALTY = 1000.0
        private const val WALL_CONTINUITY_PENALTY_PER_DEG = 0.01
        private const val OFFSET_PREFS_NAME = "qryaw_offsets"
        private const val OFFSET_KEY_PREFIX = "offset_"
        private const val OFFSET_MODE_KEY_PREFIX = "offset_mode_"

        private fun vec3Scale(v: FloatArray, s: kotlin.Float): FloatArray {
            return floatArrayOf(v[0] * s, v[1] * s, v[2] * s)
        }

        private fun vec3Dot(a: FloatArray, b: FloatArray): kotlin.Float {
            return a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
        }

        private fun vec3Len(v: FloatArray): kotlin.Float {
            return sqrt(vec3Dot(v, v).toDouble()).toFloat()
        }

        private fun vec3Normalize(v: FloatArray): FloatArray {
            val len: kotlin.Float = vec3Len(v)
            return if (len < 1e-6f) floatArrayOf(0f, 0f, 0f) else vec3Scale(v, 1f / len)
        }

        private fun vec3Cross(a: FloatArray, b: FloatArray): FloatArray {
            return floatArrayOf(
                a[1] * b[2] - a[2] * b[1],
                a[2] * b[0] - a[0] * b[2],
                a[0] * b[1] - a[1] * b[0]
            )
        }

        private fun rotateCamToWorld(camVec: FloatArray, m: FloatArray): FloatArray {
            return floatArrayOf(
                m[0] * camVec[0] + m[4] * camVec[1] + m[8] * camVec[2],
                m[1] * camVec[0] + m[5] * camVec[1] + m[9] * camVec[2],
                m[2] * camVec[0] + m[6] * camVec[1] + m[10] * camVec[2]
            )
        }

        private fun normalizeQuaternionCopy(quaternion: FloatArray?): FloatArray? {
            if (quaternion == null || quaternion.size < 4) return null

            val norm = sqrt(
                (quaternion[0] * quaternion[0] + quaternion[1] * quaternion[1] + quaternion[2] * quaternion[2] + quaternion[3] * quaternion[3]).toDouble()
            )
            if (norm < 1e-9) return null

            return floatArrayOf(
                (quaternion[0] / norm).toFloat(),
                (quaternion[1] / norm).toFloat(),
                (quaternion[2] / norm).toFloat(),
                (quaternion[3] / norm).toFloat()
            )
        }

        private fun slerpQuaternion(q0: FloatArray?, q1: FloatArray?, alpha: Double): FloatArray? {
            val start: FloatArray? = normalizeQuaternionCopy(q0)
            var end: FloatArray? = normalizeQuaternionCopy(q1)
            if (start == null || end == null) return null

            var dot =
                (start[0] * end[0] + start[1] * end[1] + start[2] * end[2] + start[3] * end[3]).toDouble()
            if (dot < 0.0) {
                dot = -dot
                end = floatArrayOf(-end[0], -end[1], -end[2], -end[3])
            }

            if (dot > 0.9995) {
                val blended = FloatArray(4)
                for (i in 0..3) {
                    blended[i] = ((1.0 - alpha) * start[i] + alpha * end[i]).toFloat()
                }
                return normalizeQuaternionCopy(blended)
            }

            val theta0 = acos(max(-1.0, min(1.0, dot)))
            val sinTheta0 = sin(theta0)
            if (abs(sinTheta0) < 1e-6) {
                return start.clone()
            }

            val theta = theta0 * alpha
            val sinTheta = sin(theta)
            val s0 = sin(theta0 - theta) / sinTheta0
            val s1 = sinTheta / sinTheta0

            return floatArrayOf(
                (s0 * start[0] + s1 * end[0]).toFloat(),
                (s0 * start[1] + s1 * end[1]).toFloat(),
                (s0 * start[2] + s1 * end[2]).toFloat(),
                (s0 * start[3] + s1 * end[3]).toFloat()
            )
        }

        private fun toCameraRay(
            v: FloatArray,
            fx: Float,
            fy: Float,
            cx: Float,
            cy: Float
        ): FloatArray {
            val w = v[2]
            val x = (v[0] - w * cx) / fx
            val y = (v[1] - w * cy) / fy
            return vec3Normalize(floatArrayOf(x, -y, -w))
        }
    }
}
