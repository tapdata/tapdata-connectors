package io.tapdata.tdd.cli.commands;

import io.tapdata.tdd.cli.CommonCli;
import picocli.CommandLine;

@CommandLine.Command(
        name = "subcommand",
        description = "Specify subcommand please"
)
public class MainCli extends CommonCli {

    @CommandLine.Option(names = { "-h", "--help" }, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;

    public Integer execute() throws Exception {
        throw new Exception("Please specify subcommand");
    }
}
