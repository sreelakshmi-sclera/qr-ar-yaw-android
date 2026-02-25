package com.example.qryaw.camera;

import android.annotation.SuppressLint;
import android.media.Image;

import androidx.camera.core.ImageAnalysis;
import androidx.camera.core.ImageProxy;

import com.google.mlkit.vision.barcode.common.Barcode;
import com.google.mlkit.vision.barcode.BarcodeScanner;
import com.google.mlkit.vision.barcode.BarcodeScanning;
import com.google.mlkit.vision.common.InputImage;

import java.util.List;

public class CameraAnalyzer implements ImageAnalysis.Analyzer {

    public interface QRListener {
        void onQRDetected(String payload);
    }

    private final QRListener listener;
    private final BarcodeScanner scanner;

    public CameraAnalyzer(QRListener listener) {
        this.listener = listener;

        scanner = BarcodeScanning.getClient();
    }

    @SuppressLint("UnsafeOptInUsageError")
    @Override
    public void analyze(ImageProxy imageProxy) {

        Image mediaImage = imageProxy.getImage();
        if (mediaImage != null) {

            InputImage image =
                    InputImage.fromMediaImage(
                            mediaImage,
                            imageProxy.getImageInfo().getRotationDegrees()
                    );

            scanner.process(image)
                    .addOnSuccessListener(barcodes -> {
                        for (Barcode barcode : barcodes) {

                            if (barcode.getValueType() == Barcode.TYPE_TEXT ||
                                    barcode.getValueType() == Barcode.TYPE_URL) {

                                String rawValue = barcode.getRawValue();
                                if (rawValue != null) {
                                    listener.onQRDetected(rawValue);
                                }
                            }
                        }
                    })
                    .addOnFailureListener(e -> {
                        e.printStackTrace();
                    })
                    .addOnCompleteListener(task -> {
                        imageProxy.close();
                    });

        } else {
            imageProxy.close();
        }
    }
}