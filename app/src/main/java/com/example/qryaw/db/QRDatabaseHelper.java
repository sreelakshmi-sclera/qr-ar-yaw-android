package com.example.qryaw.db;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class QRDatabaseHelper extends SQLiteOpenHelper {

    private static final String DB_NAME = "qryaw.db";
    private static final int DB_VERSION = 1;

    private static QRDatabaseHelper instance;

    public static synchronized QRDatabaseHelper getInstance(Context context) {
        if (instance == null) {
            instance = new QRDatabaseHelper(context.getApplicationContext());
        }
        return instance;
    }

    private QRDatabaseHelper(Context context) {
        super(context, DB_NAME, null, DB_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE registrations (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "payload TEXT UNIQUE NOT NULL," +
                "yaw REAL NOT NULL," +
                "normal_x REAL," +
                "normal_y REAL," +
                "normal_z REAL," +
                "tolerance REAL," +
                "registered_at INTEGER," +
                "device TEXT," +
                "app_version TEXT" +
                ")");

        db.execSQL("CREATE TABLE validations (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "registration_id INTEGER," +
                "payload TEXT," +
                "current_yaw REAL," +
                "registered_yaw REAL," +
                "delta REAL," +
                "within_tolerance INTEGER," +
                "tolerance REAL," +
                "validated_at INTEGER," +
                "FOREIGN KEY(registration_id) REFERENCES registrations(id)" +
                ")");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS validations");
        db.execSQL("DROP TABLE IF EXISTS registrations");
        onCreate(db);
    }

    public Long saveRegistration(String payload, double yaw, double nx, double ny, double nz, double tolerance) {
        SQLiteDatabase db = getWritableDatabase();
        ContentValues cv = new ContentValues();
        cv.put("payload", payload);
        cv.put("yaw", yaw);
        cv.put("normal_x", nx);
        cv.put("normal_y", ny);
        cv.put("normal_z", nz);
        cv.put("tolerance", tolerance);
        cv.put("registered_at", System.currentTimeMillis());
        cv.put("device", android.os.Build.MODEL);
        cv.put("app_version", "1.0");

        long id = db.insert("registrations", null, cv);
        db.close();
        return id != -1 ? id : null;
    }

    public Registration fetchRegistration(String payload) {
        SQLiteDatabase db = getReadableDatabase();
        Cursor c = db.query("registrations", null, "payload=?", new String[]{payload}, null, null, null);
        if (c.moveToFirst()) {
            Registration r = new Registration();
            r.id = c.getLong(c.getColumnIndexOrThrow("id"));
            r.payload = c.getString(c.getColumnIndexOrThrow("payload"));
            r.yaw = c.getDouble(c.getColumnIndexOrThrow("yaw"));
            r.normalX = c.getDouble(c.getColumnIndexOrThrow("normal_x"));
            r.normalY = c.getDouble(c.getColumnIndexOrThrow("normal_y"));
            r.normalZ = c.getDouble(c.getColumnIndexOrThrow("normal_z"));
            r.tolerance = c.getDouble(c.getColumnIndexOrThrow("tolerance"));
            r.registeredAt = new Date(c.getLong(c.getColumnIndexOrThrow("registered_at")));
            c.close();
            db.close();
            return r;
        }
        c.close();
        db.close();
        return null;
    }

    public void saveValidation(long regId, String payload, double curYaw, double regYaw, double delta,
                               boolean within, double tolerance) {
        SQLiteDatabase db = getWritableDatabase();
        ContentValues cv = new ContentValues();
        cv.put("registration_id", regId);
        cv.put("payload", payload);
        cv.put("current_yaw", curYaw);
        cv.put("registered_yaw", regYaw);
        cv.put("delta", delta);
        cv.put("within_tolerance", within ? 1 : 0);
        cv.put("tolerance", tolerance);
        cv.put("validated_at", System.currentTimeMillis());
        db.insert("validations", null, cv);
        db.close();
    }

    public List<Validation> fetchValidations(String payload, int limit) {
        List<Validation> list = new ArrayList<>();
        SQLiteDatabase db = getReadableDatabase();
        Cursor c = db.query("validations", null, "payload=?", new String[]{payload},
                null, null, "validated_at DESC", String.valueOf(limit));
        while (c.moveToNext()) {
            Validation v = new Validation();
            v.validatedAt = new Date(c.getLong(c.getColumnIndexOrThrow("validated_at")));
            v.delta = c.getDouble(c.getColumnIndexOrThrow("delta"));
            v.withinTolerance = c.getInt(c.getColumnIndexOrThrow("within_tolerance")) == 1;
            list.add(v);
        }
        c.close();
        db.close();
        return list;
    }

    public void deleteRegistration(String payload) {
        SQLiteDatabase db = getWritableDatabase();
        db.delete("registrations", "payload=?", new String[]{payload});
        db.delete("validations", "payload=?", new String[]{payload});
        db.close();
    }

    public static class Registration {
        public long id;
        public String payload;
        public double yaw;
        public double normalX, normalY, normalZ;
        public double tolerance;
        public Date registeredAt;
    }

    public static class Validation {
        public Date validatedAt;
        public double delta;
        public boolean withinTolerance;
    }
}