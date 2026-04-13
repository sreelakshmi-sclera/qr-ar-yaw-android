plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.android)
}

android {
    namespace = "com.example.qryaw"
    compileSdk = 35

    defaultConfig {
        applicationId = "com.example.qryaw"
        minSdk = 24
        targetSdk = 34
        versionCode = 1
        versionName = "1.0"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
    kotlinOptions {
        jvmTarget = "11"
    }
    // Jetifier rewrites old support → androidx imports at build time
    // (very important when old libs are present)
}

dependencies {

    implementation(libs.appcompat)
    implementation(libs.material)
    implementation(libs.activity)
    implementation(libs.constraintlayout)
    implementation(libs.camera.core)
    implementation(libs.camera.lifecycle)
    implementation(libs.camera.view)
    implementation(libs.camera.camera2)
    implementation(libs.mlkit.barcode.scanning)
    implementation ("com.google.ar:core:1.46.0")
    implementation("com.google.android.gms:play-services-location:21.2.0")
    implementation("io.github.sceneview:sceneview:2.3.0")
    implementation("io.github.sceneview:arsceneview:2.3.0")
    implementation(libs.play.services.location)
    implementation(libs.core.ktx)
    testImplementation(libs.junit)
    androidTestImplementation(libs.ext.junit)
    androidTestImplementation(libs.espresso.core)
    implementation("com.google.zxing:core:3.5.3")
    // 1. The Core OpenCV library (pre-compiled by Jenly)
    implementation("com.github.jenly1314.WeChatQRCode:opencv:2.4.0")

    // 2. The native C++ binaries for modern Android phones (add x86_64 if using an emulator)
    implementation("com.github.jenly1314.WeChatQRCode:opencv-armv64:2.4.0")

    // 3. The WeChat QRCode specific wrapper
    implementation("com.github.jenly1314.WeChatQRCode:wechat-qrcode:2.4.0")
}
configurations.all {
    resolutionStrategy {
//        force("androidx.activity:activity:1.8.2")
        force("com.google.ar:core:1.46.0")
        force("androidx.core:core:1.15.0")
        force("androidx.core:core-ktx:1.15.0")
    }
}