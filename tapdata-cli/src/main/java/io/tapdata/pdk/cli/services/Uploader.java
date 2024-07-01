package io.tapdata.pdk.cli.services;

import io.tapdata.pdk.cli.utils.PrintUtil;
import picocli.CommandLine;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public interface Uploader {
    public void upload(Map<String, InputStream> inputStreamMap, File file, List<String> jsons, String connectionType);

    public static void asyncWait(PrintUtil printUtil, PrintStream out, PrintStream newOut, AtomicBoolean over, String waitString) {
        new Thread(() -> {
            int a = 0;
            printUtil.print0("\n");
            while (!over.get()) {
                StringBuilder builder = new StringBuilder("\r").append(waitString);
                if (a > 3) {
                    a = 0;
                }
                switch (a) {
                    case 0: builder.append(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(28) / |@")); break;
                    case 1: builder.append(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(28) - |@")); break;
                    case 2: builder.append(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(28) \\ |@")); break;
                    case 3: builder.append(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(28) | |@")); break;
                }
                a++;
                builder.append("\r");
                synchronized (over) {
                    try {
                        System.setOut(out);
                        printUtil.print0(builder.toString());
                    } finally {
                        System.setOut(newOut);
                    }
                }
                try {
                    Thread.sleep(500);
                } catch (Exception e) {}
            }
        }).start();
    }

    public static void asyncWait(PrintUtil printUtil, AtomicBoolean over, String waitString, boolean inLeft) {
        new Thread(() -> {
            int a = 0;
            printUtil.print0("\n");
            while (!over.get()) {
                StringBuilder builder = new StringBuilder("\r ");
                if (!inLeft) {
                    builder.append(waitString);
                }
                if (a > 3) {
                    a = 0;
                }
                builder.append("[");
                switch (a) {
                    case 0: builder.append(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(28) /|@")); break;
                    case 1: builder.append(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(28) -|@")); break;
                    case 2: builder.append(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(28) \\|@")); break;
                    case 3: builder.append(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(28) ||@")); break;
                }
                a++;
                builder.append("]");
                if (inLeft) {
                    builder.append(waitString);
                }
                builder.append("\r");
                synchronized (over) {
                    printUtil.print0(builder.toString());
                }
                try {
                    Thread.sleep(500);
                } catch (Exception e) {}
            }
        }).start();
    }
}
