import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class Main2 {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("127.0.0.1", 6379);
        jedis.auth("gj0628");
        Map<String, String> person = new HashMap<>();
        person.put("name", "李四");
        person.put("sex", "男");
        person.put("age", "28");
        jedis.hmset("person", person);
        jedis.close();
    }
}
