package io.tapdata.zoho.service.command.impl;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.zoho.annonation.LanguageEnum;
import io.tapdata.zoho.entity.CommandResultV2;
import io.tapdata.zoho.enums.FieldModelType;
import io.tapdata.zoho.service.command.CommandMode;
import io.tapdata.zoho.service.command.ConfigContextChecker;
import io.tapdata.zoho.service.zoho.loader.OrganizationFieldLoader;
import io.tapdata.zoho.utils.Checker;

import java.util.Map;


//command -> FieldsCommand
public class FieldsCommand extends ConfigContextChecker<Object> implements CommandMode {
    @Override
    public CommandResult command(TapConnectionContext connectionContext, CommandInfo commandInfo) {
        String language = commandInfo.getLocale();
        this.language(Checker.isEmpty(language)? LanguageEnum.EN.getLanguage():language);
        CommandResult commandResult = new CommandResult();
        return commandResult.result(
                        OrganizationFieldLoader.create(connectionContext)
                                .allOrganizationFields(FieldModelType.TICKETS)
                                .getResult(),
                        CommandResult.CODE_OK,
                        "succeed");
    }

    @Override
    protected boolean checkerConfig(Map<String, Object> context) {
        return true;
    }

    @Override
    protected CommandResultV2 commandResult(Object entity) {
        return null;
    }
}
