package io.tapdata.common.util;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;
import java.security.SecureRandom;
import java.util.jar.JarInputStream;
import java.util.logging.Logger;

public class JarEncryptor {
    private static final Logger logger = Logger.getLogger(JarEncryptor.class.getName());
    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/CBC/PKCS5Padding";
    private static final String SECRET_KEY = "Gotapd!!Gotapd!!"; // 16字节密钥

    /**
     * 加密JAR文件
     *
     * @param jarPath 要加密的JAR文件路径
     */
    public static void encryptJar(String jarPath) throws Exception {
        // 读取原始JAR
        byte[] jarContent = Files.readAllBytes(Paths.get(jarPath));

        // 生成随机IV
        byte[] iv = new byte[16];
        SecureRandom random = new SecureRandom();
        random.nextBytes(iv);
        IvParameterSpec ivSpec = new IvParameterSpec(iv);

        // 加密
        Key key = new SecretKeySpec(SECRET_KEY.getBytes(), ALGORITHM);
        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.ENCRYPT_MODE, key, ivSpec);
        byte[] encryptedBytes = cipher.doFinal(jarContent);

        // 将IV和加密数据合并
        byte[] result = new byte[iv.length + encryptedBytes.length];
        System.arraycopy(iv, 0, result, 0, iv.length);
        System.arraycopy(encryptedBytes, 0, result, iv.length, encryptedBytes.length);

        // 保存加密后的文件（覆盖原文件）
        Files.write(Paths.get(jarPath), result);
    }

    /**
     * 解密JAR文件并原地保存
     *
     * @param jarPath 加密的JAR文件路径
     */
    public static void decryptJar(String jarPath) throws Exception {
        // 读取加密的JAR
        byte[] encryptedData = Files.readAllBytes(Paths.get(jarPath));

        // 提取IV
        byte[] iv = new byte[16];
        System.arraycopy(encryptedData, 0, iv, 0, iv.length);
        IvParameterSpec ivSpec = new IvParameterSpec(iv);

        // 提取加密数据
        byte[] encryptedBytes = new byte[encryptedData.length - iv.length];
        System.arraycopy(encryptedData, iv.length, encryptedBytes, 0, encryptedBytes.length);

        // 解密
        Key key = new SecretKeySpec(SECRET_KEY.getBytes(), ALGORITHM);
        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.DECRYPT_MODE, key, ivSpec);
        byte[] decryptedBytes = cipher.doFinal(encryptedBytes);

        // 验证JAR格式
        if (!isValidJarFile(decryptedBytes)) {
            throw new IllegalStateException("The decrypted file is not in a valid JAR format");
        }

        // 保存解密后的文件（覆盖原文件）
        Files.write(Paths.get(jarPath), decryptedBytes);
    }

    /**
     * 解密字节数组形式的JAR
     *
     * @param encryptedData 加密的JAR数据
     * @return 解密后的JAR数据
     */
    public static byte[] decryptJarBytes(byte[] encryptedData) throws Exception {
        // 提取IV
        byte[] iv = new byte[16];
        System.arraycopy(encryptedData, 0, iv, 0, iv.length);
        IvParameterSpec ivSpec = new IvParameterSpec(iv);

        // 提取加密数据
        byte[] encryptedBytes = new byte[encryptedData.length - iv.length];
        System.arraycopy(encryptedData, iv.length, encryptedBytes, 0, encryptedBytes.length);

        // 解密
        Key key = new SecretKeySpec(SECRET_KEY.getBytes(), ALGORITHM);
        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.DECRYPT_MODE, key, ivSpec);
        return cipher.doFinal(encryptedBytes);
    }

    /**
     * 验证解密后的内容是否为有效的JAR文件
     */
    private static boolean isValidJarFile(byte[] content) {
        try (JarInputStream jis = new JarInputStream(new ByteArrayInputStream(content))) {
            // 尝试读取第一个条目，如果成功则认为是有效的JAR
            return jis.getNextEntry() != null;
        } catch (Exception e) {
            logger.warning("JAR验证失败: " + e.getMessage());
            return false;
        }
    }
}