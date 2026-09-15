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
    void commandRejectsMissingPasswordOption() {
        CommandLine commandLine = new CommandLine(new RegisterCli());

        assertThrows(CommandLine.MissingParameterException.class,
                () -> commandLine.parseArgs("-t", "http://localhost:3000", "mysql.jar"));
    }
}
