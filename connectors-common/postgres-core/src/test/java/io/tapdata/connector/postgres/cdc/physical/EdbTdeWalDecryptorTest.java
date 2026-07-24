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
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        byte[] plain = decryptor.decrypt(0x05000000L, encrypted, 0, encrypted.length);

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
        byte[] plain = decryptor.decrypt(0x0500001cL, encrypted, 0, encrypted.length);

        assertArrayEquals(hex("dd 49 5e 6a 00 00 00 01 00 20 00 00 95 05 00 00"), plain);
    }

    @Test
    void decryptsNonContinuousChunksFromExplicitLsn() {
        byte[] key = Base64.getDecoder().decode(
                "EECRuRwqVNOA1T+ZhGhqHlsO8omEt4BB4raeuiL1WLR5BW96BgskGhqhyfXgzC5F"
                        + "ndIC6nzYRu8CkFdO9GmHZgcClEPtRDFryDEXLsWy2GwdzQq8bVPjzSblLlzZ"
                        + "9pLhGGnUialLjlrbBqZrzrvUcWMy7nGIo1S90E9ivrYLBJY=");
        byte[] encrypted = hex(
                "41 c0 f4 89 87 3a ef 09 53 82 24 20 d6 31 70 5d"
                        + "df 2b 45 35 1b 17 6d 4a");

        EdbTdeWalDecryptor decryptor = new EdbTdeWalDecryptor(key, 256, 0x05000000L);
        byte[] plain = decryptor.decrypt(0x06000028L, encrypted, 0, encrypted.length);

        assertArrayEquals(hex(
                "22 00 00 00 4b 03 00 00 c8 06 00 05 00 00 00 00"
                        + "00 01 00 00 00 88 f8 7d"), plain);
    }

    @Test
    void decryptsWalWithAes128DataEncryptionKey() throws Exception {
        byte[] key = hex(
                "00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f"
                        + "10 11 12 13 14 15 16 17 18 19 1a 1b 1c 1d 1e 1f"
                        + "20 21 22 23 24 25 26 27 28 29 2a 2b 2c 2d 2e 2f"
                        + "30 31 32 33 34 35 36 37 38 39 3a 3b 3c 3d 3e 3f");
        byte[] walKey = Arrays.copyOfRange(key, 32, 48);
        byte[] plain = hex("16 d1 06 00 00 00 00 00 00 00 00 05 00 00 00 00");
        byte[] encrypted = aesCtr(walKey, 0x05000000L, plain);

        EdbTdeWalDecryptor decryptor = new EdbTdeWalDecryptor(key, 128, 0x05000000L);
        byte[] decrypted = decryptor.decrypt(0x05000000L, encrypted, 0, encrypted.length);

        assertArrayEquals(plain, decrypted);
    }

    @Test
    void decryptsRealEdbAs17Aes128WalPageHeader() throws Exception {
        byte[] unwrappedKey = unwrapRealAes128Key();
        byte[] encrypted = hex(
                "27 19 c1 07 df f7 52 99 75 1c 38 90 29 72 ea 93"
                        + "40 8d 97 20 d6 c8 07 6c 67 10 33 73 2d e8 2b bc"
                        + "e4 8b 53 b9 1e 4a 4c a2 5b 69 70 2f 80 2f 9f 44"
                        + "cd a9 70 5e 29 3e 2a e2 a2 1c 40 c9 c9 b3 e4 f7");

        EdbTdeWalDecryptor decryptor = new EdbTdeWalDecryptor(unwrappedKey, 128, 0x04B28000L);
        byte[] plain = decryptor.decrypt(0x04B28000L, encrypted, 0, encrypted.length);

        assertEquals(0xD116, littleEndianInt(new byte[] {plain[0], plain[1], 0, 0}, 0));
        assertEquals(0x0005, littleEndianInt(new byte[] {plain[2], plain[3], 0, 0}, 0));
        assertEquals(0x04B28000L, littleEndianLong(plain, 8));
        assertEquals(4, littleEndianInt(plain, 16));
    }

    @Test
    void decryptsRealEdbAs17Aes128WalContinuationPageHeader() throws Exception {
        byte[] unwrappedKey = unwrapRealAes128Key();
        byte[] encrypted = hex(
                "fd b6 b7 4a b7 25 f9 c8 77 71 df 01 6d 8a 35 df"
                        + "52 18 66 6a 5d 05 b8 ee ec b6 52 79 81 f6 26 30"
                        + "26 fd ea b9 b6 ae fe 6b 91 b5 66 41 39 7c e4 ca"
                        + "f5 e2 9e 42 2a 28 bd 9f 52 12 b1 73 09 bf cd 37");

        EdbTdeWalDecryptor decryptor = new EdbTdeWalDecryptor(unwrappedKey, 128, 0x0500C000L);
        byte[] plain = decryptor.decrypt(0x0500C000L, encrypted, 0, encrypted.length);

        assertEquals(0xD116, littleEndianInt(new byte[] {plain[0], plain[1], 0, 0}, 0));
        assertEquals(0x0005, littleEndianInt(new byte[] {plain[2], plain[3], 0, 0}, 0));
        assertEquals(0x0500C000L, littleEndianLong(plain, 8));
        assertEquals(1765, littleEndianInt(plain, 16));
    }

    @Test
    void decryptsRealEdbAs17Aes128FromNonBlockAlignedLsn() throws Exception {
        byte[] unwrappedKey = unwrapRealAes128Key();
        byte[] encrypted = hex("67 10 33 73 2d e8 2b bc e4 8b 53 b9 1e 4a 4c a2");

        EdbTdeWalDecryptor decryptor = new EdbTdeWalDecryptor(unwrappedKey, 128, 0x04B28000L);
        byte[] plain = decryptor.decrypt(0x04B28018L, encrypted, 0, encrypted.length);

        assertArrayEquals(hex("41 00 02 00 00 00 00 00 36 00 00 00 4c 03 00 00"), plain);
    }

    @Test
    void rejectsShortAes128DataEncryptionKey() {
        byte[] key = new byte[47];

        assertThrows(IllegalArgumentException.class, () -> new EdbTdeWalDecryptor(key, 128, 0L));
    }

    @Test
    void unwrapsUploadedOpenSslWrappedKeyWithPassword() throws Exception {
        byte[] rawKey = Base64.getDecoder().decode(
                "EECRuRwqVNOA1T+ZhGhqHlsO8omEt4BB4raeuiL1WLR5BW96BgskGhqhyfXgzC5F"
                        + "ndIC6nzYRu8CkFdO9GmHZgcClEPtRDFryDEXLsWy2GwdzQq8bVPjzSblLlzZ"
                        + "9pLhGGnUialLjlrbBqZrzrvUcWMy7nGIo1S90E9ivrYLBJY=");
        byte[] salt = hex("01 02 03 04 05 06 07 08");
        byte[] wrapped = openSslWrap(rawKey, "tapdata", salt, 32);
        String uploaded = Base64.getUrlEncoder().encodeToString(wrapped);

        byte[] unwrapped = EdbTdeWalDecryptor.unwrapUploadedKey(uploaded, "tapdata");

        assertArrayEquals(rawKey, unwrapped);
    }

    @Test
    void unwrapsUploadedOpenSslAes128WrappedKeyWithPassword() throws Exception {
        byte[] rawKey = Base64.getDecoder().decode(
                "EECRuRwqVNOA1T+ZhGhqHlsO8omEt4BB4raeuiL1WLR5BW96BgskGhqhyfXgzC5F"
                        + "ndIC6nzYRu8CkFdO9GmHZgcClEPtRDFryDEXLsWy2GwdzQq8bVPjzSblLlzZ"
                        + "9pLhGGnUialLjlrbBqZrzrvUcWMy7nGIo1S90E9ivrYLBJY=");
        byte[] salt = hex("01 02 03 04 05 06 07 08");
        byte[] wrapped = openSslWrap(rawKey, "tapdata", salt, 16);
        String uploaded = Base64.getUrlEncoder().encodeToString(wrapped);

        byte[] unwrapped = EdbTdeWalDecryptor.unwrapUploadedKey(uploaded, "tapdata");

        assertArrayEquals(rawKey, unwrapped);
    }

    @Test
    void unwrapsUploadedOpenSslAes128WrappedKeyWithExplicitAlgorithm() throws Exception {
        byte[] rawKey = Base64.getDecoder().decode(
                "EECRuRwqVNOA1T+ZhGhqHlsO8omEt4BB4raeuiL1WLR5BW96BgskGhqhyfXgzC5F"
                        + "ndIC6nzYRu8CkFdO9GmHZgcClEPtRDFryDEXLsWy2GwdzQq8bVPjzSblLlzZ"
                        + "9pLhGGnUialLjlrbBqZrzrvUcWMy7nGIo1S90E9ivrYLBJY=");
        byte[] salt = hex("01 02 03 04 05 06 07 08");
        byte[] wrapped = openSslWrap(rawKey, "tapdata", salt, 16);
        String uploaded = Base64.getUrlEncoder().encodeToString(wrapped);

        byte[] unwrapped = EdbTdeWalDecryptor.unwrapUploadedKey(uploaded, "tapdata", "aes-128-cbc");

        assertArrayEquals(rawKey, unwrapped);
    }

    @Test
    void rejectsUnsupportedKeyWrapAlgorithm() throws Exception {
        byte[] rawKey = new byte[96];
        new SecureRandom(new byte[] {1, 2, 3, 4}).nextBytes(rawKey);
        byte[] wrapped = openSslWrap(rawKey, "tapdata", hex("01 02 03 04 05 06 07 08"), 16);
        String uploaded = Base64.getUrlEncoder().encodeToString(wrapped);

        assertThrows(IllegalArgumentException.class,
                () -> EdbTdeWalDecryptor.unwrapUploadedKey(uploaded, "tapdata", "aes-192-cbc"));
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

    private static byte[] openSslWrap(byte[] rawKey, String password, byte[] salt, int keyLength) throws Exception {
        byte[] keyAndIv = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
                .generateSecret(new PBEKeySpec(password.toCharArray(), salt, 10_000, (keyLength + 16) * 8))
                .getEncoded();
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE,
                new SecretKeySpec(Arrays.copyOfRange(keyAndIv, 0, keyLength), "AES"),
                new IvParameterSpec(Arrays.copyOfRange(keyAndIv, keyLength, keyLength + 16)));
        byte[] encrypted = cipher.doFinal(rawKey);
        byte[] magic = "Salted__".getBytes(StandardCharsets.US_ASCII);
        byte[] out = new byte[magic.length + salt.length + encrypted.length];
        System.arraycopy(magic, 0, out, 0, magic.length);
        System.arraycopy(salt, 0, out, magic.length, salt.length);
        System.arraycopy(encrypted, 0, out, magic.length + salt.length, encrypted.length);
        return out;
    }

    private static byte[] unwrapRealAes128Key() throws Exception {
        byte[] wrapped = Base64.getDecoder().decode(
                "U2FsdGVkX1/mjX4mF9kGLRBBVwgY6YSXGUyWxIBEy1rvkzZ07hVnOfkuvH2e638r"
                        + "SAZKuVawrcwV9msXqevVpYVJR1skqQbU0kphKIt8/pvSiTB68MKe5Z14fNCEB5Sb");
        String uploaded = Base64.getEncoder().encodeToString(wrapped);
        return EdbTdeWalDecryptor.unwrapUploadedKey(uploaded, "CreatedBySJMDBA", "aes-128-cbc");
    }

    private static byte[] aesCtr(byte[] key, long startLsn, byte[] input) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new IvParameterSpec(counterIv(startLsn / 16)));
        return cipher.doFinal(input);
    }

    private static byte[] counterIv(long block) {
        byte[] iv = new byte[16];
        for (int i = 15; i >= 8; i--) {
            iv[i] = (byte) block;
            block >>>= 8;
        }
        return iv;
    }
}
