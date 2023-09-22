package io.tapdata.tdd.cli;

import io.tapdata.tdd.cli.commands.JarHijackerCli;
import io.tapdata.tdd.cli.commands.MainCli;
import io.tapdata.tdd.cli.commands.TDDCli;
import io.tapdata.tdd.cli.commands.TapPDKRunnerCli;
import picocli.CommandLine;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class Main {
    //
    public static void main(String... args) {
        Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
    }

    public static CommandLine registerCommands() {
        CommandLine commandLine = new CommandLine(new MainCli());
        commandLine.addSubcommand("test", new TDDCli());
        commandLine.addSubcommand("jar", new JarHijackerCli());
        commandLine.addSubcommand("run", new TapPDKRunnerCli());
        return commandLine;
    }
}
