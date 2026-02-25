package com.example.qryaw;

import android.content.Context;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.util.AttributeSet;
import android.view.View;

import java.util.ArrayList;
import java.util.List;

public class QROverlayView extends View {

    private List<float[]> corners; // 4 points: [x,y] each
    private final Paint linePaint = new Paint();
    private final Paint pointPaint = new Paint();

    public QROverlayView(Context context) { this(context, null); }
    public QROverlayView(Context context, AttributeSet attrs) { this(context, attrs, 0); }
    public QROverlayView(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
        setWillNotDraw(false);

        linePaint.setStyle(Paint.Style.STROKE);
        linePaint.setColor(Color.YELLOW);
        linePaint.setStrokeWidth(6f);
        linePaint.setAntiAlias(true);

        pointPaint.setStyle(Paint.Style.FILL);
        pointPaint.setAntiAlias(true);
    }

    public void setCorners(List<float[]> points) {
        this.corners = points;
        invalidate();
    }

    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);
        if (corners == null || corners.size() != 4) return;

        // Draw quadrilateral
        for (int i = 0; i < 4; i++) {
            float[] a = corners.get(i);
            float[] b = corners.get((i + 1) % 4);
            canvas.drawLine(a[0], a[1], b[0], b[1], linePaint);
        }

        // Draw colored dots on corners
        int[] colors = {Color.RED, Color.GREEN, Color.BLUE, Color.parseColor("#FFA500")};
        for (int i = 0; i < 4; i++) {
            pointPaint.setColor(colors[i]);
            float[] p = corners.get(i);
            canvas.drawCircle(p[0], p[1], 12, pointPaint);
        }

        // Center cross
        float cx = 0, cy = 0;
        for (float[] p : corners) {
            cx += p[0]; cy += p[1];
        }
        cx /= 4; cy /= 4;
        Paint crossPaint = new Paint();
        crossPaint.setColor(Color.CYAN);
        crossPaint.setStrokeWidth(4f);
        canvas.drawLine(cx - 16, cy, cx + 16, cy, crossPaint);
        canvas.drawLine(cx, cy - 16, cx, cy + 16, crossPaint);
    }
}
