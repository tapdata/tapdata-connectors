package io.tapdata.connector.postgres.cdc.physical;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Base64;

/**
 * Decrypts physical WAL bytes produced by EDB Postgres Advanced Server TDE.
 *
 * <p>EDB AS 17 encrypts WAL with AES-CTR. The counter is the absolute WAL byte
 * position divided by the AES block size, encoded as a 128-bit big-endian
 * integer. The unwrapped EDB data-encryption key contains multiple subkeys; WAL
 * uses the third 32-byte subkey for 256-bit TDE.</p>
 */
class EdbTdeWalDecryptor {

    private static final int AES_BLOCK_SIZE = 16;
    private static final int WAL_KEY_OFFSET = 64;
    private static final byte[] OPENSSL_SALTED_MAGIC = new byte[] {
            'S', 'a', 'l', 't', 'e', 'd', '_', '_'
    };
    private static final int OPENSSL_SALT_LENGTH = 8;
    private static final int OPENSSL_PBKDF2_ITERATIONS = 10_000;
    private static final int OPENSSL_AES_256_KEY_LENGTH = 32;
    private static final int OPENSSL_AES_BLOCK_LENGTH = 16;

    private final byte[] walKey;

    EdbTdeWalDecryptor(byte[] unwrappedKey, int dataEncryptionBits, long startLsn) {
        int keyLength = dataEncryptionBits == 128 ? 16 : 32;
        if (dataEncryptionBits != 128 && dataEncryptionBits != 256) {
            throw new IllegalArgumentException("Unsupported EDB TDE key length: " + dataEncryptionBits);
        }
        if (unwrappedKey == null || unwrappedKey.length < WAL_KEY_OFFSET + keyLength) {
            throw new IllegalArgumentException("EDB TDE unwrapped key is too short for WAL decryption");
        }
        this.walKey = Arrays.copyOfRange(unwrappedKey, WAL_KEY_OFFSET, WAL_KEY_OFFSET + keyLength);
    }

    byte[] decrypt(long startLsn, byte[] encrypted, int offset, int length) {
        if (length <= 0) {
            return new byte[0];
        }
        try {
            long blockStartLsn = startLsn - (startLsn % AES_BLOCK_SIZE);
            int skip = (int) (startLsn - blockStartLsn);
            Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE,
                    new SecretKeySpec(walKey, "AES"),
                    new IvParameterSpec(counterIv(blockStartLsn / AES_BLOCK_SIZE)));
            byte[] input;
            if (skip == 0) {
                input = Arrays.copyOfRange(encrypted, offset, offset + length);
            } else {
                input = new byte[skip + length];
                System.arraycopy(encrypted, offset, input, skip, length);
            }
            byte[] output = cipher.update(input);
            if (output == null) {
                output = cipher.doFinal(input);
            }
            byte[] decrypted = skip == 0 ? output : Arrays.copyOfRange(output, skip, skip + length);
            return decrypted;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to decrypt EDB TDE WAL stream at LSN "
                    + PhysicalWalLogMiner.lsnStr(startLsn), e);
        }
    }

    static byte[] unwrapUploadedKey(String uploadedKey, String password) throws IOException {
        byte[] keyFileBytes = decodeUploadedKey(uploadedKey);
        if (keyFileBytes.length == 0) {
            throw new IOException("EDB TDE key file is empty");
        }
        if (password == null || password.trim().isEmpty()) {
            return keyFileBytes;
        }
        try {
            return decryptOpenSslAes256CbcPbkdf2(keyFileBytes, password.toCharArray());
        } catch (GeneralSecurityException e) {
            throw new IOException("Failed to unwrap EDB TDE key file with the configured password", e);
        }
    }

    private static byte[] decodeUploadedKey(String uploadedKey) {
        if (uploadedKey == null || uploadedKey.trim().isEmpty()) {
            throw new IllegalArgumentException("EDB TDE key file is required");
        }
        String value = uploadedKey.trim();
        int dataUrlMarker = value.indexOf(";base64,");
        if (dataUrlMarker >= 0) {
            value = value.substring(dataUrlMarker + ";base64,".length());
        }
        try {
            return Base64.getUrlDecoder().decode(value);
        } catch (IllegalArgumentException ignored) {
            try {
                return Base64.getDecoder().decode(value);
            } catch (IllegalArgumentException ignoredAgain) {
                return Base64.getMimeDecoder().decode(value);
            }
        }
    }

    private static byte[] decryptOpenSslAes256CbcPbkdf2(byte[] encrypted, char[] password)
            throws GeneralSecurityException, IOException {
        if (encrypted.length <= OPENSSL_SALTED_MAGIC.length + OPENSSL_SALT_LENGTH
                || !startsWith(encrypted, OPENSSL_SALTED_MAGIC)) {
            throw new IOException("Unsupported EDB TDE key wrapper format; expected OpenSSL salted format");
        }
        byte[] salt = Arrays.copyOfRange(encrypted,
                OPENSSL_SALTED_MAGIC.length,
                OPENSSL_SALTED_MAGIC.length + OPENSSL_SALT_LENGTH);
        byte[] cipherText = Arrays.copyOfRange(encrypted,
                OPENSSL_SALTED_MAGIC.length + OPENSSL_SALT_LENGTH,
                encrypted.length);
        byte[] keyAndIv = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
                .generateSecret(new PBEKeySpec(password, salt, OPENSSL_PBKDF2_ITERATIONS,
                        (OPENSSL_AES_256_KEY_LENGTH + OPENSSL_AES_BLOCK_LENGTH) * 8))
                .getEncoded();
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE,
                new SecretKeySpec(Arrays.copyOfRange(keyAndIv, 0, OPENSSL_AES_256_KEY_LENGTH), "AES"),
                new IvParameterSpec(Arrays.copyOfRange(keyAndIv,
                        OPENSSL_AES_256_KEY_LENGTH,
                        OPENSSL_AES_256_KEY_LENGTH + OPENSSL_AES_BLOCK_LENGTH)));
        return cipher.doFinal(cipherText);
    }

    private static boolean startsWith(byte[] value, byte[] prefix) {
        if (value.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (value[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static byte[] counterIv(long counter) {
        byte[] raw = BigInteger.valueOf(counter).toByteArray();
        byte[] iv = new byte[AES_BLOCK_SIZE];
        int copy = Math.min(raw.length, AES_BLOCK_SIZE);
        System.arraycopy(raw, raw.length - copy, iv, AES_BLOCK_SIZE - copy, copy);
        return iv;
    }
}
