package com.github.shyiko.mysql.binlog.network.protocol.command;

import com.github.shyiko.mysql.binlog.network.AuthenticationException;
import com.github.shyiko.mysql.binlog.io.ByteArrayOutputStream;

import javax.crypto.Cipher;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

public class AuthenticateSHA2RSAPasswordCommand implements Command {
    private static final String RSA_METHOD = "RSA/ECB/OAEPWithSHA-1AndMGF1Padding";
    private final String rsaKey;
    private final String password;
    private final String scramble;

    public AuthenticateSHA2RSAPasswordCommand(String rsaKey, String password, String scramble) {
        this.rsaKey = rsaKey;
        this.password = password;
        this.scramble = scramble;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        RSAPublicKey key = decodeKey(rsaKey);

        ByteArrayOutputStream passBuffer = new ByteArrayOutputStream();
        passBuffer.writeZeroTerminatedString(password);

        byte[] xorBuffer = CommandUtils.xor(passBuffer.toByteArray(), scramble.getBytes());
        return encrypt(xorBuffer, key, RSA_METHOD);
    }

    private RSAPublicKey decodeKey(String key) throws AuthenticationException {
        int beginIndex = key.indexOf("\n") + 1;
        int endIndex = key.indexOf("-----END PUBLIC KEY-----");
        String innerKey = key.substring(beginIndex, endIndex).replaceAll("\\n", "");

        Base64.Decoder decoder = Base64.getDecoder();
        byte[] certificateData = decoder.decode(innerKey.getBytes());

        X509EncodedKeySpec spec = new X509EncodedKeySpec(certificateData);
        try {
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return (RSAPublicKey) kf.generatePublic(spec);
        } catch (Exception e) {
            throw new AuthenticationException("Unable to decode public key: " + key);
        }
    }

    private byte[] encrypt(byte[] source, RSAPublicKey key, String transformation) throws AuthenticationException {
        try {
            Cipher cipher = Cipher.getInstance(transformation);
            cipher.init(Cipher.ENCRYPT_MODE, key);
            return cipher.doFinal(source);
        } catch (Exception e) {
            throw new AuthenticationException("couldn't encrypt password: " + e.getMessage());
        }
    }

}
