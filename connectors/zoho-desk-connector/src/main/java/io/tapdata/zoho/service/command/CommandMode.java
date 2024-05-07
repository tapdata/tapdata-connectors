package io.tapdata.zoho.service.command;

import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.zoho.utils.Checker;

public interface CommandMode {
    String DATA = "data";
    String SET_VALUE = "setValue";

    CommandResult command(TapConnectionContext connectionContext,CommandInfo commandInfo);
    /**
     * Strategy to realize Command,Different names implement different methods .please in sub class Override the Function :
     *      public CommandResult command(TapConnectionContext connectionContext,CommandInfo commandInfo);
     * And use this function[getInstanceByName] in your invoker addr.
     * */
    static CommandResult getInstanceByName(TapConnectionContext connectionContext,CommandInfo commandInfo){
        String command = commandInfo.getCommand();
        if (Checker.isEmpty(command)) {
            throw new CoreException("Command type can not be empty");
        }
        Class<CommandMode> clz = null;
        try {
            clz = (Class<CommandMode>)Class.forName("io.tapdata.zoho.service.command.impl." + command);
            return clz.newInstance().command(connectionContext,commandInfo);
        } catch (ClassNotFoundException e) {
            throw new CoreException("Not Fund command: {}", command);
        } catch (InstantiationException e1) {
            throw new CoreException("Can not get instance of command: {}", command);
        } catch (IllegalAccessException e2) {
            throw new CoreException("Can not access command, message: {}", e2.getMessage());
        }
    }
}
