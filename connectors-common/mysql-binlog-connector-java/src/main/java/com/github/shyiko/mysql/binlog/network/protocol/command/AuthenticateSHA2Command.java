/*
 * Copyright 2018 dingxiaobo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog.network.protocol.command;

import com.github.shyiko.mysql.binlog.io.ByteArrayOutputStream;
import com.github.shyiko.mysql.binlog.network.ClientCapabilities;

import java.io.IOException;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author <a href="mailto:dxb1405@163.com">dingxiaobo</a>
 */
public class AuthenticateSHA2Command implements Command {

    private String schema;
    private String username;
    private String password;
    private String scramble;
    private int clientCapabilities;
    private int collation;
    private boolean rawPassword = false;

    public AuthenticateSHA2Command(String schema, String username, String password, String scramble, int collation) {
        this.schema = schema;
        this.username = username;
        this.password = password;
        this.scramble = scramble;
        this.collation = collation;
    }

    public AuthenticateSHA2Command(String scramble, String password) {
        this.rawPassword = true;
        this.password = password;
        this.scramble = scramble;
    }

    public void setClientCapabilities(int clientCapabilities) {
        this.clientCapabilities = clientCapabilities;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        if ( rawPassword ) {
            byte[] passwordSHA1 = encodePassword();
            buffer.write(passwordSHA1);
            return buffer.toByteArray();
        }

        int clientCapabilities = this.clientCapabilities;
        if (clientCapabilities == 0) {
            clientCapabilities |= ClientCapabilities.LONG_FLAG;
            clientCapabilities |= ClientCapabilities.PROTOCOL_41;
            clientCapabilities |= ClientCapabilities.SECURE_CONNECTION;
            clientCapabilities |= ClientCapabilities.PLUGIN_AUTH;
            clientCapabilities |= ClientCapabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA;

            if (schema != null) {
                clientCapabilities |= ClientCapabilities.CONNECT_WITH_DB;
            }
        }
        buffer.writeInteger(clientCapabilities, 4);
        buffer.writeInteger(0, 4); // maximum packet length
        buffer.writeInteger(collation, 1);
        for (int i = 0; i < 23; i++) {
            buffer.write(0);
        }
        buffer.writeZeroTerminatedString(username);
        byte[] passwordSHA1 = encodePassword();
        buffer.writeInteger(passwordSHA1.length, 1);
        buffer.write(passwordSHA1);
        if (schema != null) {
            buffer.writeZeroTerminatedString(schema);
        }
        buffer.writeZeroTerminatedString("caching_sha2_password");

        return buffer.toByteArray();
    }

    private byte[] encodePassword() {
        if (password == null || "".equals(password)) {
            return new byte[0];
        }
        // caching_sha2_password
        /*
         * Server does it in 4 steps (see sql/auth/sha2_password_common.cc Generate_scramble::scramble method):
         *
         * SHA2(src) => digest_stage1
         * SHA2(digest_stage1) => digest_stage2
         * SHA2(digest_stage2, m_rnd) => scramble_stage1
         * XOR(digest_stage1, scramble_stage1) => scramble
         */
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");

            int CACHING_SHA2_DIGEST_LENGTH = 32;
            byte[] dig1 = new byte[CACHING_SHA2_DIGEST_LENGTH];
            byte[] dig2 = new byte[CACHING_SHA2_DIGEST_LENGTH];
            byte[] scramble1 = new byte[CACHING_SHA2_DIGEST_LENGTH];

            // SHA2(src) => digest_stage1
            md.update(password.getBytes(), 0, password.getBytes().length);
            md.digest(dig1, 0, CACHING_SHA2_DIGEST_LENGTH);
            md.reset();

            // SHA2(digest_stage1) => digest_stage2
            md.update(dig1, 0, dig1.length);
            md.digest(dig2, 0, CACHING_SHA2_DIGEST_LENGTH);
            md.reset();

            // SHA2(digest_stage2, m_rnd) => scramble_stage1
            md.update(dig2, 0, dig1.length);
            md.update(scramble.getBytes(), 0, scramble.getBytes().length);
            md.digest(scramble1, 0, CACHING_SHA2_DIGEST_LENGTH);

            // XOR(digest_stage1, scramble_stage1) => scramble
            return CommandUtils.xor(dig1, scramble1);
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        } catch (DigestException e) {
            throw new RuntimeException(e);
        }
    }
}
