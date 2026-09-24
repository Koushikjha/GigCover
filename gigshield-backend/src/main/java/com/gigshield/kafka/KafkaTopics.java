package com.gigshield.kafka;


public final class KafkaTopics {

    private KafkaTopics() {}


    public static final String DISRUPTION_EVENT_CREATED = "gigshield.events.disruption";

    public static final String PAYOUT_REQUESTED = "gigshield.payments.payout-requested";

    public static final String PAYOUT_COMPLETED = "gigshield.payments.payout-completed";

    public static final String GROUP_CLAIMS_AUTOMATION  = "gigshield-claims-automation";
    public static final String GROUP_CLAIM_STATUS_SYNC  = "gigshield-claim-status-sync";
    public static final String GROUP_PAYMENTS            = "gigshield-payments";
    public static final String GROUP_AUDIT                = "gigshield-audit";
}
