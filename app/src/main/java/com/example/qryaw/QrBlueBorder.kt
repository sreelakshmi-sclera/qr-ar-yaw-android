package com.example.qryaw

import android.content.Context
import android.graphics.*
import android.util.AttributeSet
import android.view.View
import kotlin.math.min
import androidx.core.graphics.toColorInt

class QrBlueBorder @JvmOverloads constructor(
    context: Context,
    attrs: AttributeSet? = null,
    defStyleAttr: Int = 0
) : View(context, attrs, defStyleAttr) {

    private val paint = Paint().apply {
        color = "#3185E6".toColorInt()
        style = Paint.Style.STROKE
        strokeWidth = dpToPx(5f)
        isAntiAlias = true
        strokeCap = Paint.Cap.ROUND
    }

    private val rect = RectF()

    fun getScanRect(): RectF {
        val size = min(width, height) * 0.6f
        val left = (width - size) / 2
        val top = (height - size) / 2
        return RectF(left, top, left + size, top + size)
    }

    private val cornerLength = dpToPx(40f)
    private val cornerRadius = dpToPx(30f)

    override fun onDraw(canvas: Canvas) {
        super.onDraw(canvas)

        val size = min(width, height) * 0.6f

        val left = (width - size) / 2
        val top = (height - size) / 2
        val right = left + size
        val bottom = top + size

        rect.set(left, top, right, bottom)

        drawCorners(canvas, rect)
    }

    private fun drawCorners(canvas: Canvas, rect: RectF) {

        val path = Path()

        path.moveTo(rect.left, rect.top + cornerRadius)
        path.quadTo(rect.left, rect.top, rect.left + cornerRadius, rect.top)
        path.lineTo(rect.left + cornerLength, rect.top)

        path.moveTo(rect.left, rect.top + cornerRadius)
        path.lineTo(rect.left, rect.top + cornerLength)

        path.moveTo(rect.right - cornerRadius, rect.top)
        path.quadTo(rect.right, rect.top, rect.right, rect.top + cornerRadius)
        path.lineTo(rect.right, rect.top + cornerLength)

        path.moveTo(rect.right - cornerRadius, rect.top)
        path.lineTo(rect.right - cornerLength, rect.top)

        path.moveTo(rect.left, rect.bottom - cornerRadius)
        path.quadTo(rect.left, rect.bottom, rect.left + cornerRadius, rect.bottom)
        path.lineTo(rect.left + cornerLength, rect.bottom)

        path.moveTo(rect.left, rect.bottom - cornerRadius)
        path.lineTo(rect.left, rect.bottom - cornerLength)

        path.moveTo(rect.right - cornerRadius, rect.bottom)
        path.quadTo(rect.right, rect.bottom, rect.right, rect.bottom - cornerRadius)
        path.lineTo(rect.right, rect.bottom - cornerLength)

        path.moveTo(rect.right - cornerRadius, rect.bottom)
        path.lineTo(rect.right - cornerLength, rect.bottom)

        canvas.drawPath(path, paint)
    }

    private fun dpToPx(dp: Float): Float {
        return dp * resources.displayMetrics.density
    }
}