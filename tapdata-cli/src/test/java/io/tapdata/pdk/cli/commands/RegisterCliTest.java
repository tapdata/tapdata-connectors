package io.tapdata.pdk.cli.commands;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RegisterCliTest {

    @Test
    void validateAdministratorAcceptsAdminCredentials() {
        assertDoesNotThrow(() -> RegisterCli.validateAdministrator("admin@admin.com", "password"));
    }

    @Test
    void validateAdministratorRejectsNonAdminUser() {
        assertThrows(IllegalArgumentException.class,
                () -> RegisterCli.validateAdministrator("user@example.com", "password"));
    }

    @Test
    void validateAdministratorRejectsBlankPassword() {
        assertThrows(IllegalArgumentException.class,
                () -> RegisterCli.validateAdministrator("admin@admin.com", " "));
    }

    @Test
    void validateAdministratorTrimsUsername() {
        assertDoesNotThrow(() -> RegisterCli.validateAdministrator("  admin@admin.com  ", "password"));
    }

    @Test
    void validateAdministratorRejectsNullUser() {
        assertThrows(IllegalArgumentException.class,
                () -> RegisterCli.validateAdministrator(null, "password"));
    }

    @Test
    void legacyAccessCodeDoesNotRequirePasswordOption() {
        CommandLine commandLine = new CommandLine(new RegisterCli());

        assertDoesNotThrow(() -> commandLine.parseArgs(
                "-a", "legacy-access-code", "-t", "http://localhost:3000", "mysql.jar"));
    }

    @Test
    void cloudAccessKeyDoesNotRequirePasswordOption() {
        CommandLine commandLine = new CommandLine(new RegisterCli());

        assertDoesNotThrow(() -> commandLine.parseArgs(
                "-ak", "access-key", "-sk", "secret-key", "-t", "http://localhost:3000", "mysql.jar"));
    }

    @Test
    void validateAuthenticationAcceptsLegacyAccessCode() {
        assertDoesNotThrow(() -> RegisterCli.validateAuthentication(
                "legacy-access-code", null, null, null));
    }

    @Test
    void validateAuthenticationAcceptsCloudAccessKey() {
        assertDoesNotThrow(() -> RegisterCli.validateAuthentication(
                null, null, null, "access-key"));
    }

    @Test
    void validateAuthenticationRequiresPasswordForNewSelfHostedFlow() {
        assertThrows(IllegalArgumentException.class,
                () -> RegisterCli.validateAuthentication(null, "admin@admin.com", " ", null));
    }
}
