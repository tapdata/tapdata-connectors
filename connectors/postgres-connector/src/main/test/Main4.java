import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class Main4 {
    public static void main(String[] args) throws Exception {
        System.out.println(Boolean.FALSE.toString());
        Class.forName("org.postgresql.Driver");
        Properties properties = new Properties();
        properties.setProperty("user", "postgres");
        properties.setProperty("password", "gj0628");
        properties.setProperty("ssl", "true");
        // 配置根证书地址
        properties.setProperty("sslrootcert","/Users/jarad/Desktop/postgres-local/root.crt");
        // 配置客户端私钥地址
        properties.setProperty("sslkey", "/Users/jarad/Desktop/postgres-local/postgresql.unprotected.pk8");
        // 配置客户端证书地址
        properties.setProperty("sslcert", "/Users/jarad/Desktop/postgres-local/postgresql.crt");
        try (
                Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5433/postgres", properties);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("select count(1) from public.ppp")
        ) {
            if(rs.next()) {
                System.out.println(rs.getString(1));
            }
        }
    }
}
