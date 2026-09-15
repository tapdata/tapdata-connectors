package io.tapdata.pdk.cli.services;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UploadFileServiceTest {

    @Test
    void createLoginRequestEncryptsPasswordForAdmin() throws Exception {
        JSONObject request = JSON.parseObject(UploadFileService.createLoginRequest("admin@admin.com", "secret"));

        assertEquals("admin@admin.com", request.getString("email"));
        assertNotEquals("secret", request.getString("password"));
        assertEquals("secret", decryptPassword(request.getString("password")));
    }

    @Test
    void parseAccessTokenReturnsToken() {
        assertEquals("token-id", UploadFileService.parseAccessToken(
                "{\"code\":\"ok\",\"data\":{\"id\":\"token-id\"}}"));
    }

    @Test
    void parseAccessTokenRejectsFailedLogin() {
        assertThrows(IllegalStateException.class, () -> UploadFileService.parseAccessToken(
                "{\"code\":\"Incorrect.Password\",\"message\":\"Incorrect password\"}"));
    }

    @Test
    void encryptPasswordUsesRandomSalt() {
        assertNotEquals(UploadFileService.encryptPassword("secret"), UploadFileService.encryptPassword("secret"));
    }

    @Test
    void loginRejectsNonAdminUser() {
        assertThrows(IllegalArgumentException.class,
                () -> UploadFileService.login("http://localhost:3000", "user@example.com", "secret"));
    }

    @Test
    void loginRejectsBlankPassword() {
        assertThrows(IllegalArgumentException.class,
                () -> UploadFileService.login("http://localhost:3000", "admin@admin.com", " "));
    }

    @Test
    void parseAccessTokenRejectsBlankResponse() {
        assertThrows(IllegalStateException.class, () -> UploadFileService.parseAccessToken(" "));
    }

    @Test
    void parseAccessTokenRejectsMissingToken() {
        assertThrows(IllegalStateException.class,
                () -> UploadFileService.parseAccessToken("{\"code\":\"ok\",\"data\":{}}"));
    }

    private String decryptPassword(String ciphertext) throws Exception {
        byte[] encrypted = Base64.getDecoder().decode(ciphertext);
        byte[] salt = Arrays.copyOfRange(encrypted, 8, 16);
        byte[] passAndSalt = concat("Gotapd8".getBytes(StandardCharsets.US_ASCII), salt);
        byte[] hash = new byte[0];
        byte[] keyAndIv = new byte[0];
        for (int index = 0; index < 3 && keyAndIv.length < 48; index++) {
            hash = MessageDigest.getInstance("MD5").digest(concat(hash, passAndSalt));
            keyAndIv = concat(keyAndIv, hash);
        }
        SecretKeySpec key = new SecretKeySpec(Arrays.copyOfRange(keyAndIv, 0, 32), "RC4");
        Cipher cipher = Cipher.getInstance("RC4");
        cipher.init(Cipher.DECRYPT_MODE, key, (AlgorithmParameterSpec) null);
        return new String(cipher.doFinal(encrypted, 16, encrypted.length - 16), StandardCharsets.UTF_8);
    }

    private byte[] concat(byte[] first, byte[] second) {
        byte[] result = new byte[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }
}
