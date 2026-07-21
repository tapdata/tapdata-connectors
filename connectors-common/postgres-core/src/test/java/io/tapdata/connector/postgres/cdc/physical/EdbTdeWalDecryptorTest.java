package io.tapdata.connector.postgres.cdc.physical;

import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class EdbTdeWalDecryptorTest {

    @Test
    void decryptsEdbAs17WalSegmentHeader() {
        byte[] key = Base64.getDecoder().decode(
                "EECRuRwqVNOA1T+ZhGhqHlsO8omEt4BB4raeuiL1WLR5BW96BgskGhqhyfXgzC5F"
                        + "ndIC6nzYRu8CkFdO9GmHZgcClEPtRDFryDEXLsWy2GwdzQq8bVPjzSblLlzZ"
                        + "9pLhGGnUialLjlrbBqZrzrvUcWMy7nGIo1S90E9ivrYLBJY=");
        byte[] encrypted = hex(
                "c5 6d 22 5a 99 2a 78 90 e5 94 ce da 54 0c 61 3b"
                        + "44 d7 07 21 ca 0a ac b6 df 93 66 16 50 76 5a 9a"
                        + "26 f8 d2 31 cf 98 a9 80 3f ce 73 f3 8c c1 5f 37"
                        + "52 16 84 1e 25 92 26 f5 b5 5c cd 36 00 b2 aa a5");

        EdbTdeWalDecryptor decryptor = new EdbTdeWalDecryptor(key, 256, 0x05000000L);
        byte[] plain = decryptor.decrypt(encrypted, 0, encrypted.length);

        assertArrayEquals(hex("16 d1 06 00"), copy(plain, 0, 4));
        assertEquals(0x05000000L, littleEndianLong(plain, 8));
        assertArrayEquals(hex("95 05 00 00"), copy(plain, 40, 4));
        assertEquals(1429, littleEndianInt(plain, 40));
    }

    @Test
    void decryptsFromNonBlockAlignedLsn() {
        byte[] key = Base64.getDecoder().decode(
                "EECRuRwqVNOA1T+ZhGhqHlsO8omEt4BB4raeuiL1WLR5BW96BgskGhqhyfXgzC5F"
                        + "ndIC6nzYRu8CkFdO9GmHZgcClEPtRDFryDEXLsWy2GwdzQq8bVPjzSblLlzZ"
                        + "9pLhGGnUialLjlrbBqZrzrvUcWMy7nGIo1S90E9ivrYLBJY=");
        byte[] encrypted = hex("50 76 5a 9a 26 f8 d2 31 cf 98 a9 80 3f ce 73 f3");

        EdbTdeWalDecryptor decryptor = new EdbTdeWalDecryptor(key, 256, 0x0500001cL);
        byte[] plain = decryptor.decrypt(encrypted, 0, encrypted.length);

        assertArrayEquals(hex("dd 49 5e 6a 00 00 00 01 00 20 00 00 95 05 00 00"), plain);
    }

    @Test
    void unwrapsUploadedOpenSslWrappedKeyWithPassword() throws Exception {
        byte[] rawKey = Base64.getDecoder().decode(
                "EECRuRwqVNOA1T+ZhGhqHlsO8omEt4BB4raeuiL1WLR5BW96BgskGhqhyfXgzC5F"
                        + "ndIC6nzYRu8CkFdO9GmHZgcClEPtRDFryDEXLsWy2GwdzQq8bVPjzSblLlzZ"
                        + "9pLhGGnUialLjlrbBqZrzrvUcWMy7nGIo1S90E9ivrYLBJY=");
        byte[] salt = hex("01 02 03 04 05 06 07 08");
        byte[] wrapped = openSslWrap(rawKey, "tapdata", salt);
        String uploaded = Base64.getUrlEncoder().encodeToString(wrapped);

        byte[] unwrapped = EdbTdeWalDecryptor.unwrapUploadedKey(uploaded, "tapdata");

        assertArrayEquals(rawKey, unwrapped);
    }

    @Test
    void acceptsUploadedRawKeyWhenPasswordIsBlank() throws Exception {
        byte[] rawKey = new byte[96];
        new SecureRandom(new byte[] {1, 2, 3, 4}).nextBytes(rawKey);
        String uploaded = Base64.getUrlEncoder().encodeToString(rawKey);

        byte[] unwrapped = EdbTdeWalDecryptor.unwrapUploadedKey(uploaded, "");

        assertArrayEquals(rawKey, unwrapped);
    }

    private static byte[] copy(byte[] src, int offset, int length) {
        byte[] out = new byte[length];
        System.arraycopy(src, offset, out, 0, length);
        return out;
    }

    private static int littleEndianInt(byte[] bytes, int offset) {
        return (bytes[offset] & 0xff)
                | ((bytes[offset + 1] & 0xff) << 8)
                | ((bytes[offset + 2] & 0xff) << 16)
                | ((bytes[offset + 3] & 0xff) << 24);
    }

    private static long littleEndianLong(byte[] bytes, int offset) {
        long value = 0;
        for (int i = 7; i >= 0; i--) {
            value = (value << 8) | (bytes[offset + i] & 0xffL);
        }
        return value;
    }

    private static byte[] hex(String text) {
        String clean = text.replace(" ", "");
        byte[] out = new byte[clean.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(clean.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    private static byte[] openSslWrap(byte[] rawKey, String password, byte[] salt) throws Exception {
        byte[] keyAndIv = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
                .generateSecret(new PBEKeySpec(password.toCharArray(), salt, 10_000, (32 + 16) * 8))
                .getEncoded();
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE,
                new SecretKeySpec(Arrays.copyOfRange(keyAndIv, 0, 32), "AES"),
                new IvParameterSpec(Arrays.copyOfRange(keyAndIv, 32, 48)));
        byte[] encrypted = cipher.doFinal(rawKey);
        byte[] magic = "Salted__".getBytes(StandardCharsets.US_ASCII);
        byte[] out = new byte[magic.length + salt.length + encrypted.length];
        System.arraycopy(magic, 0, out, 0, magic.length);
        System.arraycopy(salt, 0, out, magic.length, salt.length);
        System.arraycopy(encrypted, 0, out, magic.length + salt.length, encrypted.length);
        return out;
    }
}
