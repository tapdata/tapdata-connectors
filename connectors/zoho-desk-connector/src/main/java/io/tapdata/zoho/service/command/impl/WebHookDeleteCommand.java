package io.tapdata.zoho.service.command.impl;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.zoho.entity.CommandResultV2;
import io.tapdata.zoho.service.command.CommandMode;
import io.tapdata.zoho.service.command.ConfigContextChecker;

import java.util.Map;
//command -> WebHookDelete
public class WebHookDeleteCommand extends ConfigContextChecker<Object> implements CommandMode {
    @Override
    public CommandResult command(TapConnectionContext connectionContext, CommandInfo commandInfo) {
        return null;
    }

    @Override
    protected boolean checkerConfig(Map<String, Object> context) {
        return false;
    }

    @Override
    protected CommandResultV2 commandResult(Object entity) {
        return null;
    }
}
