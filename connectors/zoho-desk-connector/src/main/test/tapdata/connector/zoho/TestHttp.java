package tapdata.connector.zoho;

import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.Map;

public class TestHttp {
    public static void main(String[] args) {
        HttpEntity form = HttpEntity.create().build("","");
        HttpEntity body = HttpEntity.create().build("","");
        HttpNormalEntity header = HttpEntity.create().build("Authorization","Zoho-oauthtoken 1000.4c15607cdb92a91c3acc96e19c400021.cb2dd3413de77e2bf0e88b261e8ae6be");

        HttpNormalEntity resetFull1 = HttpEntity.create().build("ticketID","10504000000165033");
        ZoHoHttp hoHttp = ZoHoHttp.create(
                "https://desk.zoho.com.cn/api/v1/tickets/{ticketID}",
                HttpType.POST,
                header,
                body
        ).resetFull(resetFull1);
//        Map<String, Object> post = hoHttp.post();


        HttpNormalEntity resetFull = HttpEntity.create().build("ticketID","10504000000165033");
        ZoHoHttp get = ZoHoHttp.create(
                "https://desk.zoho.com.cn/api/v1/tickets/{ticketID}",
                HttpType.GET,
                header
        ).form(form).resetFull(resetFull);
//        Map<String, Object> stringObjectMap = get.get();

        System.out.println("");

    }
}
