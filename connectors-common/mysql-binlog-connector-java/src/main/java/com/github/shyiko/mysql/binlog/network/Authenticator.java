package com.github.shyiko.mysql.binlog.network;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.io.ByteArrayOutputStream;
import com.github.shyiko.mysql.binlog.network.protocol.ErrorPacket;
import com.github.shyiko.mysql.binlog.network.protocol.GreetingPacket;
import com.github.shyiko.mysql.binlog.network.protocol.PacketChannel;
import com.github.shyiko.mysql.binlog.network.protocol.command.AuthenticateNativePasswordCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.AuthenticateSHA2Command;
import com.github.shyiko.mysql.binlog.network.protocol.command.AuthenticateSHA2RSAPasswordCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.AuthenticateSecurityPasswordCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.ByteArrayCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.Command;
import com.github.shyiko.mysql.binlog.network.protocol.command.SSLRequestCommand;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Authenticator {
    private enum AuthMethod {
        NATIVE,
        CACHING_SHA2
    };

    private final GreetingPacket greetingPacket;
    private String scramble;
    private final PacketChannel channel;
    private final String schema;
    private final String username;
    private final String password;

    private final Logger logger = Logger.getLogger(getClass().getName());

    private final String SHA2_PASSWORD = "caching_sha2_password";
    private final String MYSQL_NATIVE = "mysql_native_password";

    private AuthMethod authMethod = AuthMethod.NATIVE;

    public Authenticator(
        GreetingPacket greetingPacket,
        PacketChannel channel,
        String schema,
        String username,
        String password
    ) {
       this.greetingPacket = greetingPacket;
       this.scramble = greetingPacket.getScramble();
       this.channel = channel;
       this.schema = schema;
       this.username = username;
       this.password = password;
    }

    public void authenticate() throws IOException {
        logger.log(Level.FINE, "Begin auth for " + username);
        int collation = greetingPacket.getServerCollation();

        Command authenticateCommand;
        if ( SHA2_PASSWORD.equals(greetingPacket.getPluginProvidedData()) ) {
            authMethod = AuthMethod.CACHING_SHA2;
            authenticateCommand = new AuthenticateSHA2Command(schema, username, password, scramble, collation);
        } else {
            authMethod = AuthMethod.NATIVE;
            authenticateCommand = new AuthenticateSecurityPasswordCommand(schema, username, password, scramble, collation);
        }

        channel.write(authenticateCommand);
        readResult();
        logger.log(Level.FINE, "Auth complete " + username);
    }

    private void readResult() throws IOException {
        byte[] authenticationResult = channel.read();
        switch(authenticationResult[0]) {
            case (byte) 0x00:
                // success
                return;
            case (byte) 0xFF:
                // error
                byte[] bytes = Arrays.copyOfRange(authenticationResult, 1, authenticationResult.length);
                ErrorPacket errorPacket = new ErrorPacket(bytes);
                throw new AuthenticationException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                    errorPacket.getSqlState());
            case (byte) 0xFE:
                switchAuthentication(authenticationResult);
                return;
            default:
                if ( authMethod == AuthMethod.NATIVE )
                    throw new AuthenticationException("Unexpected authentication result (" + authenticationResult[0] + ")");
                else
                    processCachingSHA2Result(authenticationResult);
        }
    }

    private void processCachingSHA2Result(byte[] authenticationResult) throws IOException {
        if (authenticationResult.length < 2)
            throw new AuthenticationException("caching_sha2_password response too short!");

        ByteArrayInputStream stream = new ByteArrayInputStream(authenticationResult);
        stream.readPackedInteger(); // throw away length, always 1

        switch(stream.read()) {
            case 0x03:
                logger.log(Level.FINE, "cached sha2 auth successful");
                // successful fast authentication
                readResult();
                return;
            case 0x04:
                logger.log(Level.FINE, "cached sha2 auth not successful, moving to full auth path");
                continueCachingSHA2Authentication();
        }
    }

    private void continueCachingSHA2Authentication() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        if ( channel.isSSL() ) {
            // over SSL we simply send the password in cleartext.

            buffer.writeZeroTerminatedString(password);

            Command c = new ByteArrayCommand(buffer.toByteArray());
            channel.write(c);
            readResult();
        } else {
            // try to download an RSA key
            buffer.write(0x02);
            channel.write(new ByteArrayCommand(buffer.toByteArray()));

            ByteArrayInputStream stream = new ByteArrayInputStream(channel.read());
            int result = stream.read();
            switch(result) {
                case 0x01:
                    byte[] rsaKey = new byte[stream.available()];
                    stream.read(rsaKey);

                    logger.log(Level.FINE, "received RSA key: " + rsaKey);
                    Command c = new AuthenticateSHA2RSAPasswordCommand(new String(rsaKey), password, scramble);
                    channel.write(c);

                    readResult();
                    return;
                default:
                    throw new AuthenticationException("Unkown response fetching RSA key in caching_sha2_pasword auth: " + result);
            }
        }
    }

    private void switchAuthentication(byte[] authenticationResult) throws IOException {
        /*
            Azure-MySQL likes to tell us to switch authentication methods, even though
            we haven't advertised that we support any.  It uses this for some-odd
            reason to send the real password scramble.
        */
        ByteArrayInputStream buffer = new ByteArrayInputStream(authenticationResult);
        buffer.read(1);

        String authName = buffer.readZeroTerminatedString();
        if (MYSQL_NATIVE.equals(authName)) {
            authMethod = AuthMethod.NATIVE;

            this.scramble = buffer.readZeroTerminatedString();

            Command switchCommand = new AuthenticateNativePasswordCommand(scramble, password);
            channel.write(switchCommand);
        } else if ( SHA2_PASSWORD.equals(authName) ) {
            authMethod = AuthMethod.CACHING_SHA2;

            this.scramble = buffer.readZeroTerminatedString();
            Command authCommand = new AuthenticateSHA2Command(scramble, password);
            channel.write(authCommand);
        } else {
            throw new AuthenticationException("unsupported authentication method: " + authName);
        }

        readResult();
    }
}
