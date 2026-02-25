package com.example.qryaw.db;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Android equivalent of iOS QRDatabase.swift.
 *
 * Design decisions mirroring iOS:
 *  - Singleton with a single persistent SQLiteDatabase connection
 *    (never closed after open — safe for app lifetime, avoids "closed" crashes).
 *  - saveRegistration uses INSERT OR REPLACE so re-registering the same QR
 *    payload overwrites the old row (same as iOS upsert behaviour).
 *  - fetchAllRegistrations() provided for historyTapped().
 *  - getDatabasePath() provided for the startup log line.
 *  - Registration carries deviceModel + appVersion to match iOS columns.
 *  - Validation carries all columns written by saveValidation.
 */
public class QRDatabaseHelper extends SQLiteOpenHelper {

    private static final String DB_NAME    = "qryaw.db";
    private static final int    DB_VERSION = 1;

    // ── Singleton ──────────────────────────────────────────────────────────────
    private static QRDatabaseHelper instance;

    public static synchronized QRDatabaseHelper getInstance(Context context) {
        if (instance == null) {
            instance = new QRDatabaseHelper(context.getApplicationContext());
        }
        return instance;
    }

    // Store app context for PackageManager access in getAppVersion()
    private final Context appContext;

    private QRDatabaseHelper(Context context) {
        super(context, DB_NAME, null, DB_VERSION);
        this.appContext = context;   // already getApplicationContext() from getInstance()
    }

    // ── Persistent connection (mirrors iOS: db opened once, never explicitly closed) ──
    // SQLiteOpenHelper already caches the database internally; we just ensure
    // callers never call db.close() so the singleton stays open.
    private SQLiteDatabase getDb() {
        return getWritableDatabase();   // returns the cached instance
    }

    // ── Path helper (used in MainActivity log line) ───────────────────────────
    public String getDatabasePath() {
        return getDb().getPath();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Schema
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(
                "CREATE TABLE registrations (" +
                        "  id             INTEGER PRIMARY KEY AUTOINCREMENT," +
                        "  payload        TEXT    UNIQUE NOT NULL," +
                        "  yaw            REAL    NOT NULL," +
                        "  normal_x       REAL," +
                        "  normal_y       REAL," +
                        "  normal_z       REAL," +
                        "  tolerance      REAL," +
                        "  registered_at  INTEGER," +       // epoch-ms
                        "  device         TEXT," +           // Build.MODEL  (≡ iOS deviceModel)
                        "  app_version    TEXT" +            // ≡ iOS appVersion
                        ")"
        );

        db.execSQL(
                "CREATE TABLE validations (" +
                        "  id                INTEGER PRIMARY KEY AUTOINCREMENT," +
                        "  registration_id   INTEGER," +
                        "  payload           TEXT," +
                        "  current_yaw       REAL," +
                        "  registered_yaw    REAL," +
                        "  delta             REAL," +
                        "  within_tolerance  INTEGER," +     // 0 or 1
                        "  tolerance         REAL," +
                        "  validated_at      INTEGER," +     // epoch-ms
                        "  FOREIGN KEY(registration_id) REFERENCES registrations(id)" +
                        ")"
        );
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS validations");
        db.execSQL("DROP TABLE IF EXISTS registrations");
        onCreate(db);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Registrations
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Insert or replace a registration for the given payload.
     * Mirrors iOS saveRegistration (upsert — same payload overwrites old row).
     *
     * @return the row-id of the inserted/replaced row, or null on failure.
     */
    public Long saveRegistration(String payload,
                                 double yaw,
                                 double nx, double ny, double nz,
                                 double tolerance) {
        ContentValues cv = new ContentValues();
        cv.put("payload",       payload);
        cv.put("yaw",           yaw);
        cv.put("normal_x",      nx);
        cv.put("normal_y",      ny);
        cv.put("normal_z",      nz);
        cv.put("tolerance",     tolerance);
        cv.put("registered_at", System.currentTimeMillis());
        cv.put("device",        android.os.Build.MODEL);
        cv.put("app_version",   getAppVersion());

        // INSERT OR REPLACE mirrors iOS "INSERT OR REPLACE INTO registrations …"
        long id = getDb().insertWithOnConflict(
                "registrations", null, cv, SQLiteDatabase.CONFLICT_REPLACE);
        return id != -1 ? id : null;
    }

    /**
     * Fetch the registration for a specific QR payload.
     * Returns null if not found (mirrors iOS fetchRegistration returning nil).
     */
    public Registration fetchRegistration(String payload) {
        Cursor c = getDb().query(
                "registrations", null,
                "payload=?", new String[]{ payload },
                null, null, null);
        try {
            if (c.moveToFirst()) return rowToRegistration(c);
            return null;
        } finally {
            c.close();
        }
    }

    /**
     * Fetch all registrations ordered by newest first.
     * Mirrors iOS fetchAllRegistrations() used in historyTapped().
     */
    public List<Registration> fetchAllRegistrations() {
        List<Registration> list = new ArrayList<>();
        Cursor c = getDb().query(
                "registrations", null,
                null, null, null, null,
                "registered_at DESC");
        try {
            while (c.moveToNext()) list.add(rowToRegistration(c));
        } finally {
            c.close();
        }
        return list;
    }

    /**
     * Delete a registration and all its associated validations.
     * Mirrors iOS deleteRegistration(for:).
     */
    public void deleteRegistration(String payload) {
        getDb().delete("validations",   "payload=?", new String[]{ payload });
        getDb().delete("registrations", "payload=?", new String[]{ payload });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Validations
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Log a validation result to the database.
     * Mirrors iOS saveValidation(registrationId:qrPayload:currentYaw:…).
     */
    public void saveValidation(long registrationId,
                               String payload,
                               double currentYaw,
                               double registeredYaw,
                               double delta,
                               boolean withinTolerance,
                               double tolerance) {
        ContentValues cv = new ContentValues();
        cv.put("registration_id",  registrationId);
        cv.put("payload",          payload);
        cv.put("current_yaw",      currentYaw);
        cv.put("registered_yaw",   registeredYaw);
        cv.put("delta",            delta);
        cv.put("within_tolerance", withinTolerance ? 1 : 0);
        cv.put("tolerance",        tolerance);
        cv.put("validated_at",     System.currentTimeMillis());
        getDb().insert("validations", null, cv);
    }

    /**
     * Fetch the most recent `limit` validations for a payload, newest first.
     * Mirrors iOS fetchValidations(for:limit:).
     */
    public List<Validation> fetchValidations(String payload, int limit) {
        List<Validation> list = new ArrayList<>();
        Cursor c = getDb().query(
                "validations", null,
                "payload=?", new String[]{ payload },
                null, null,
                "validated_at DESC",
                String.valueOf(limit));
        try {
            while (c.moveToNext()) {
                Validation v = new Validation();
                v.id             = c.getLong   (c.getColumnIndexOrThrow("id"));
                v.registrationId = c.getLong   (c.getColumnIndexOrThrow("registration_id"));
                v.payload        = c.getString (c.getColumnIndexOrThrow("payload"));
                v.currentYaw     = c.getDouble (c.getColumnIndexOrThrow("current_yaw"));
                v.registeredYaw  = c.getDouble (c.getColumnIndexOrThrow("registered_yaw"));
                v.delta          = c.getDouble (c.getColumnIndexOrThrow("delta"));
                v.withinTolerance= c.getInt    (c.getColumnIndexOrThrow("within_tolerance")) == 1;
                v.tolerance      = c.getDouble (c.getColumnIndexOrThrow("tolerance"));
                v.validatedAt    = new Date    (c.getLong(c.getColumnIndexOrThrow("validated_at")));
                list.add(v);
            }
        } finally {
            c.close();
        }
        return list;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Internal helpers
    // ─────────────────────────────────────────────────────────────────────────

    private Registration rowToRegistration(Cursor c) {
        Registration r = new Registration();
        r.id           = c.getLong  (c.getColumnIndexOrThrow("id"));
        r.payload      = c.getString(c.getColumnIndexOrThrow("payload"));
        r.yaw          = c.getDouble(c.getColumnIndexOrThrow("yaw"));
        r.normalX      = c.getDouble(c.getColumnIndexOrThrow("normal_x"));
        r.normalY      = c.getDouble(c.getColumnIndexOrThrow("normal_y"));
        r.normalZ      = c.getDouble(c.getColumnIndexOrThrow("normal_z"));
        r.tolerance    = c.getDouble(c.getColumnIndexOrThrow("tolerance"));
        r.registeredAt = new Date   (c.getLong(c.getColumnIndexOrThrow("registered_at")));
        r.deviceModel  = c.getString(c.getColumnIndexOrThrow("device"));
        r.appVersion   = c.getString(c.getColumnIndexOrThrow("app_version"));
        return r;
    }

    /** Reads versionName from the package manifest (≡ iOS Bundle.main.appVersion). */
    private String getAppVersion() {
        try {
            return appContext.getPackageManager()
                    .getPackageInfo(appContext.getPackageName(), 0)
                    .versionName;
        } catch (Exception e) {
            return "1.0";
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Data classes  (mirrors iOS structs)
    // ─────────────────────────────────────────────────────────────────────────

    /** Mirrors iOS QRRegistrationRecord */
    public static class Registration {
        public long   id;
        public String payload;
        public double yaw;
        public double normalX, normalY, normalZ;
        public double tolerance;
        public Date   registeredAt;
        public String deviceModel;   // ≡ iOS deviceModel
        public String appVersion;    // ≡ iOS appVersion
    }

    /** Mirrors iOS QRValidationRecord */
    public static class Validation {
        public long    id;
        public long    registrationId;
        public String  payload;
        public double  currentYaw;
        public double  registeredYaw;
        public double  delta;
        public boolean withinTolerance;
        public double  tolerance;
        public Date    validatedAt;
    }
}