package com.gigshield.auth.controller;

import com.gigshield.auth.dto.AuthResponse;
import com.gigshield.auth.dto.RefreshTokenRequest;
import com.gigshield.auth.dto.SendOtpRequest;
import com.gigshield.auth.dto.VerifyOtpRequest;
import com.gigshield.auth.service.AuthService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;


    @PostMapping("/send-otp")
    public ResponseEntity<Void> sendOtp(
            @Valid @RequestBody SendOtpRequest request) {
        authService.sendOtp(request);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/verify-otp")
    public ResponseEntity<AuthResponse> verifyOtp(
            @Valid @RequestBody VerifyOtpRequest request) {
        return ResponseEntity.ok(authService.verifyOtp(request));
    }

    @PostMapping("/refresh")
    public ResponseEntity<AuthResponse> refresh(
            @Valid @RequestBody RefreshTokenRequest request) {
        return ResponseEntity.ok(authService.refresh(request));
    }

    @PostMapping("/logout")
    public ResponseEntity<Void> logout(
            @AuthenticationPrincipal UserDetails userDetails) {
        authService.logout(userDetails.getUsername());
        return ResponseEntity.noContent().build();
    }
}