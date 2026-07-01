plugins {
    alias(libs.plugins.android.application)
}

android {
    namespace = "com.example.servicelab"
    compileSdk = 34

    defaultConfig {
        applicationId = "com.example.servicelab"
        minSdk = 26
        targetSdk = 34
        versionCode = 1
        versionName = "1.0"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"

        multiDexEnabled = true

        packaging {
            resources {
                excludes.add("META-INF/*")
            }
        }
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
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }




}

dependencies {

    implementation(libs.appcompat)
    implementation(libs.material)
    implementation(libs.activity)
    implementation(libs.constraintlayout)
    testImplementation(libs.junit)
    androidTestImplementation(libs.ext.junit)
    androidTestImplementation(libs.espresso.core)
    implementation(libs.multidex)
    //debugImplementation(libs.leakcanary.android)






    // needed by lombok
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    // needed by CDDL
    implementation(libs.eventbus)
    implementation(libs.org.eclipse.paho.client.mqttv3)
    implementation(libs.play.services.location)
    implementation(libs.commons.lang3)
    implementation(libs.gson)

    // needed by microbroker
    implementation(libs.netty.common)
    implementation(libs.netty.buffer)
    implementation(libs.netty.transport)
    implementation(libs.netty.handler)
    implementation(libs.netty.codec)
    implementation(libs.netty.codec.http)
    implementation(libs.netty.codec.mqtt)
    implementation(libs.netty.transport.native.epoll)

    // needed by CDAL
    //implementation(files("libs/polar-ble-sdk.aar"))
    implementation(libs.protobuf.javalite)
    implementation(libs.rxjava)
    implementation(libs.rxandroid)
    implementation(libs.commons.io)

    implementation(files("libs/cddl-1.1-release.aar"))
    implementation(files("libs/cdal-1.0.4-release.aar"))
    implementation(files("libs/microbroker-1.1-release.aar"))
    implementation(files("libs/security-service-1.1-release.aar"))

}