package io.tapdata.zoho.service.zoho.loader;

import java.util.HashMap;
import java.util.Map;

public interface ZoHoBase {
    String AUTHORIZATION_KEY = "Authorization";
    String ZO_HO_BASE_URL = "https://desk.zoho.com.cn%s";
    String ZO_HO_BASE_TOKEN_URL = "https://accounts.zoho.com.cn%s";
    String ZO_HO_ACCESS_TOKEN_PREFIX = "Zoho-oauthtoken ";
    String ZO_HO_BASE_SCOPE = "Desk.tickets.ALL,Desk.contacts.READ,Desk.contacts.WRITE,Desk.contacts.UPDATE,Desk.contacts.CREATE,Desk.tasks.ALL,Desk.basic.READ,Desk.basic.CREATE,Desk.settings.ALL,Desk.events.ALL,Desk.articles.READ,Desk.articles.CREATE,Desk.articles.UPDATE,Desk.articles.DELETE";

    static String builderAccessToken(String accessToken) {
        return accessToken.startsWith(ZoHoBase.ZO_HO_ACCESS_TOKEN_PREFIX) ? accessToken : ZoHoBase.ZO_HO_ACCESS_TOKEN_PREFIX + accessToken;
    }

    default Object getFromMap(Map<?, ?> obj, String key) {
        return null != obj ? obj.get(key) : null;
    }

    default Object getFromObj(Object obj, String key) {
        if (obj instanceof Map) {
            return getFromMap(((Map<?, ?>) obj), key);
        }
        return null;
    }

    default Number getNumberFromMapObj(Object obj, String key) {
        Object fromObj = getFromObj(obj, key);
        if (fromObj instanceof Number) {
            return (Number) fromObj;
        }
        return null;
    }

    default int getInt(Object obj, String key) {
        Number number = getNumberFromMapObj(obj, key);
        return null == number ? 0 : number.intValue();
    }

    default long getLong(Object obj, String key) {
        Number number = getNumberFromMapObj(obj, key);
        return null == number ? 0 : number.longValue();
    }

    default boolean getBoolean(Object obj, String key) {
        Object fromObj = getFromObj(obj, key);
        return Boolean.TRUE.equals(fromObj);
    }

    default Map<String, Object> parseMap(Object obj, Class<? extends Map> claz) {
        if (obj instanceof Map) {
            return (Map<String, Object>) obj;
        }
        if (null == claz) return null;
        try {
            return (Map<String, Object>) claz.newInstance();
        } catch (Exception e) {
            return new HashMap<>();
        }
    }

    default Map<String, Object> parseWithDefault(Object obj) {
        return parseMap(obj, HashMap.class);
    }

    default Map<String, Object> parseMap(Object obj) {
        return parseMap(obj, null);
    }
}
