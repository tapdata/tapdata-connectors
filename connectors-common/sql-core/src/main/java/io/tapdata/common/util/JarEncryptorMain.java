package io.tapdata.common.util;

import io.tapdata.encryptor.JarEncryptor;

public class JarEncryptorMain {
    public static void main(String[] args) {
        try {
            if (args.length < 1) {
                System.exit(1);
            }

            String jarPath = args[0];

            System.out.println("Start encrypting JAR: " + jarPath);
            JarEncryptor.encryptJar(jarPath);
            System.out.println("JAR encryption is complete");

        } catch (Exception e) {
            System.err.println("Error in encryption process : " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}