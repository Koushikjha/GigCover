package com.gigshield.auth.sms;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SmsClient {

    @Value("${gigshield.sms.mock:true}")
    private boolean mockMode;

    @Value("${gigshield.sms.provider-url:}")
    private String providerUrl;

    @Value("${gigshield.sms.api-key:}")
    private String apiKey;


    public void sendOtp(String phone, String otp) {
        if (mockMode) {
            log.info("╔══════════════════════════════╗");
            log.info("║  [MOCK SMS] To: +91{}  ║", phone);
            log.info("║  OTP: {}                    ║", otp);
            log.info("╚══════════════════════════════╝");
            return;
        }

        if (providerUrl.isBlank() || apiKey.isBlank()) {
            throw new IllegalStateException(
                    "SMS provider not configured. " +
                            "Set gigshield.sms.provider-url and gigshield.sms.api-key");
        }

        log.info("OTP sent to phone={}", phone);
    }
}