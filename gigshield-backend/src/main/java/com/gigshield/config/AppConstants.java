package com.gigshield.config;

public final class  AppConstants {

    private AppConstants() {}

    // ── Payout Caps (₹) ───────────────────────────────────────────────────────
    public static final int    WAR_PAYOUT_CAP_INR            = 500;
    public static final int    TRAFFIC_PAYOUT_CAP_INR        = 200;
    public static final int    ORDER_CANCELLED_PAYOUT_CAP_INR = 150;


    public static final long   ORDER_CANCELLED_REPORT_WINDOW_HOURS = 72;
    public static final long   ORDER_CANCELLED_FUTURE_TOLERANCE_MINUTES = 5;

    public static final int    PREMIUM_STANDARD_INR          = 15;
    public static final int    PREMIUM_GOLD_INR              = 22;
    public static final int    PREMIUM_PREMIUM_INR           = 30;

    public static final int    PREMIUM_MIN_INR               = 15;
    public static final int    PREMIUM_MAX_INR               = 30;

    public static final double PAYOUT_RATIO_STANDARD         = 0.30;
    public static final double PAYOUT_RATIO_GOLD             = 0.35;
    public static final double PAYOUT_RATIO_PREMIUM          = 0.40;


    public static final double RISK_PREMIUM_MULTIPLIER_FLOOR = 0.80;
    public static final double RISK_PREMIUM_MULTIPLIER_CEIL  = 1.35;

    public static final double PAYOUT_INCOME_RATIO_MIN       = 0.30;
    public static final double PAYOUT_INCOME_RATIO_MAX       = 0.40;

    public static final int    FRAUD_AUTO_APPROVE_THRESHOLD  = 40;
    public static final int    FRAUD_STRIKE_BAN_COUNT        = 3;

    public static final double EVENT_GENUINE_MIN_RISK_SCORE  = 20.0;

    public static final long   JWT_EXPIRY_MS                 = 86_400_000L;
    public static final String JWT_HEADER                    = "Authorization";
    public static final String JWT_PREFIX                    = "Bearer ";

    public static final long   SESSION_TTL_SEC               = 86_400L;
    public static final long   RISK_SCORE_CACHE_TTL_SEC      = 3_600L;
    public static final long   TRIGGER_CACHE_TTL_SEC         = 900L;

    public static final String ML_RISK_SCORE_PATH            = "/risk-score";
    public static final String ML_FRAUD_CHECK_PATH           = "/fraud-check";
    public static final String ML_TRIGGER_CHECK_PATH         = "/trigger-check";

    public static final int    DEFAULT_PAGE_SIZE             = 20;

    public static final int    POLICY_DURATION_DAYS          = 7;
    public static final double DEFAULT_WEEKLY_INCOME_INR     = 3_500.0;
}