package io.tapdata.connector.gauss;


import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class TestScript {
    MysqlJdbc mysqlJdbc = new MysqlJdbc("*.*.*.*", 3306, "test", "root", "*****");
    GaussJdbc gaussJdbc = new GaussJdbc("*.*.*.*", 8000, "postgres", "root", "*****");

    @Test
    public void mysqlInsert() {
        try(Connection conn = mysqlJdbc.getConn()) {
            int id = 100;
            while(true) {
                try (PreparedStatement statement = conn.prepareStatement("insert into test.full_table" +
                        "(id,s_smallint,s_integer,s_bigint,s_numeric,s_real," +
                        "s_double_precision,s_character,s_character_varying,s_text," +
                        "s_bytea,s_bit,s_bit_varying,s_boolean,s_date,s_interval," +
                        "s_timestamp_without_time_zone,s_timestamp_wit_time_zone," +
                        "s_time_without_time_zone,s_time_wit_time_zone)" +
                        " values" +
                        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);")) {

                    statement.setObject(1, id);
                    statement.setObject(2, 100);
                    statement.setObject(3, 100);
                    statement.setObject(4, 100);

                    statement.setObject(5, 100.0);
                    statement.setObject(6, null);
                    statement.setObject(7, 100.00);
                    statement.setObject(8, null);

                    statement.setObject(9, null);
                    statement.setObject(10, null);
                    statement.setObject(11, null);
                    statement.setObject(12, null);

                    statement.setObject(13, null);
                    statement.setObject(14, 1);
                    statement.setObject(15, null);//"2024-02-02 17:56:00");
                    statement.setObject(16, null);

                    statement.setObject(17, null);//"2024-02-02 17:56:00.666666");
                    statement.setObject(18, null);//"2024-02-02 17:56:00.333333");
                    statement.setObject(19, null);//"17:56:00");
                    statement.setObject(20, null);//"17:56:22");
                    statement.execute();
                    System.out.println("insert one record...");
                } finally {
                    Thread.sleep(200);
                    id++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void mysqlInsert2() {
        try(Connection conn = mysqlJdbc.getConn()) {
            int id = -100;
            while(true) {
                try (PreparedStatement statement = conn.prepareStatement("insert into test.full_table" +
                        "(id,s_smallint,s_integer,s_bigint,s_numeric,s_real," +
                        "s_double_precision,s_character,s_character_varying,s_text," +
                        "s_bytea,s_bit,s_bit_varying,s_boolean,s_date,s_interval," +
                        "s_timestamp_without_time_zone,s_timestamp_wit_time_zone," +
                        "s_time_without_time_zone,s_time_wit_time_zone)" +
                        " values" +
                        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);")) {

                    statement.setObject(1, id);
                    statement.setObject(2, 100);
                    statement.setObject(3, 100);
                    statement.setObject(4, 100);

                    statement.setObject(5, 100.0);
                    statement.setObject(6, null);
                    statement.setObject(7, 100.00);
                    statement.setObject(8, null);

                    statement.setObject(9, null);
                    statement.setObject(10, null);
                    statement.setObject(11, null);
                    statement.setObject(12, null);

                    statement.setObject(13, null);
                    statement.setObject(14, 1);
                    statement.setObject(15, null);//"2024-02-02 17:56:00");
                    statement.setObject(16, null);

                    statement.setObject(17, null);//"2024-02-02 17:56:00.666666");
                    statement.setObject(18, null);//"2024-02-02 17:56:00.333333");
                    statement.setObject(19, null);//"17:56:00");
                    statement.setObject(20, null);//"17:56:22");
                    statement.execute();
                    System.out.println("insert one record...");
                } finally {
                    Thread.sleep(10);
                    id--;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void mysqlCleanTable() {
        try(Connection conn = mysqlJdbc.getConn();
            Statement statement = conn.createStatement();) {
            statement.execute("delete from test.full_table where 1=1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void mysqlUpdate() {
        try(Connection conn = mysqlJdbc.getConn()) {
            int id = 100;
            while(true) {
                try (PreparedStatement statement = conn.prepareStatement("update test.full_table " +
                        "set s_smallint = ? , s_integer = ? , s_bigint = ? , s_numeric = ? , s_real = ? , " +
                        "s_double_precision = ? , s_character = ? , s_character_varying = ?, s_text = ? , " +
                        "s_bytea = ? , s_bit = ? , s_bit_varying = ? , s_boolean = ? , s_date = ? , s_interval = ? , " +
                        "s_timestamp_without_time_zone = ? , s_timestamp_wit_time_zone = ? , " +
                        "s_time_without_time_zone = ? , s_time_wit_time_zone = ? " +
                        " where id =?;")) {

                    statement.setObject(1, 100);
                    statement.setObject(2, 100);
                    statement.setObject(3, 100);

                    statement.setObject(4, 100.0);
                    statement.setObject(5, null);
                    statement.setObject(6, 100.00);
                    statement.setObject(7, null);

                    statement.setObject(8, null);
                    statement.setObject(9, null);
                    statement.setObject(10, null);
                    statement.setObject(11, null);

                    statement.setObject(12, null);
                    statement.setObject(13, 1);
                    statement.setObject(14, null);//"2024-02-02 17:56:00");
                    statement.setObject(15, null);

                    statement.setObject(16, null);//"2024-02-02 17:56:00.666666");
                    statement.setObject(17, null);//"2024-02-02 17:56:00.333333");
                    statement.setObject(18, null);//"17:56:00");
                    statement.setObject(19, null);//"17:56:22");
                    statement.setObject(20, id);
                    statement.execute();
                    System.out.println("update one record...");
                } finally {
                    Thread.sleep(400);
                    id++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void mysqlUpdate2() {
        try(Connection conn = mysqlJdbc.getConn()) {
            int id = 100;
            while(true) {
                try (PreparedStatement statement = conn.prepareStatement("update test.full_table " +
                        "set s_smallint = ? , s_integer = ? , s_bigint = ? , s_numeric = ? , s_real = ? , " +
                        "s_double_precision = ? , s_character = ? , s_character_varying = ?, s_text = ? , " +
                        "s_bytea = ? , s_bit = ? , s_bit_varying = ? , s_boolean = ? , s_date = ? , s_interval = ? , " +
                        "s_timestamp_without_time_zone = ? , s_timestamp_wit_time_zone = ? , " +
                        "s_time_without_time_zone = ? , s_time_wit_time_zone = ? " +
                        " where id =?;")) {

                    statement.setObject(1, 101);
                    statement.setObject(2, 99);
                    statement.setObject(3, 7);

                    statement.setObject(4, 66.6);
                    statement.setObject(5, null);
                    statement.setObject(6, 88.92);
                    statement.setObject(7, null);

                    statement.setObject(8, null);
                    statement.setObject(9, null);
                    statement.setObject(10, null);
                    statement.setObject(11, null);

                    statement.setObject(12, null);
                    statement.setObject(13, 1);
                    statement.setObject(14, null);//"2024-02-02 17:56:00");
                    statement.setObject(15, null);

                    statement.setObject(16, null);//"2024-02-02 17:56:00.666666");
                    statement.setObject(17, null);//"2024-02-02 17:56:00.333333");
                    statement.setObject(18, null);//"17:56:00");
                    statement.setObject(19, null);//"17:56:22");
                    statement.setObject(20, id);
                    statement.execute();
                    System.out.println("update one record...");
                } finally {
                    Thread.sleep(400);
                    id++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void mysqlUpdate3() {
        try(Connection conn = mysqlJdbc.getConn()) {
            int id = -100;
            while(true) {
                try (PreparedStatement statement = conn.prepareStatement("update test.full_table " +
                        "set s_smallint = ? , s_integer = ? , s_bigint = ? , s_numeric = ? , s_real = ? , " +
                        "s_double_precision = ? , s_character = ? , s_character_varying = ?, s_text = ? , " +
                        "s_bytea = ? , s_bit = ? , s_bit_varying = ? , s_boolean = ? , s_date = ? , s_interval = ? , " +
                        "s_timestamp_without_time_zone = ? , s_timestamp_wit_time_zone = ? , " +
                        "s_time_without_time_zone = ? , s_time_wit_time_zone = ? " +
                        " where id =?;")) {

                    statement.setObject(1, 101);
                    statement.setObject(2, 99);
                    statement.setObject(3, 7);

                    statement.setObject(4, 66.6);
                    statement.setObject(5, null);
                    statement.setObject(6, 88.92);
                    statement.setObject(7, null);

                    statement.setObject(8, null);
                    statement.setObject(9, null);
                    statement.setObject(10, null);
                    statement.setObject(11, null);

                    statement.setObject(12, null);
                    statement.setObject(13, 1);
                    statement.setObject(14, null);//"2024-02-02 17:56:00");
                    statement.setObject(15, null);

                    statement.setObject(16, null);//"2024-02-02 17:56:00.666666");
                    statement.setObject(17, null);//"2024-02-02 17:56:00.333333");
                    statement.setObject(18, null);//"17:56:00");
                    statement.setObject(19, null);//"17:56:22");
                    statement.setObject(20, id);
                    statement.execute();
                    System.out.println("update one record...");
                } finally {
                    Thread.sleep(20);
                    id--;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void mysqlUpdate4() {
        try(Connection conn = mysqlJdbc.getConn()) {
            int id = -100;
            while(true) {
                try (PreparedStatement statement = conn.prepareStatement("update test.full_table " +
                        "set s_smallint = ? , s_integer = ? , s_bigint = ? , s_numeric = ? , s_real = ? , " +
                        "s_double_precision = ? , s_character = ? , s_character_varying = ?, s_text = ? , " +
                        "s_bytea = ? , s_bit = ? , s_bit_varying = ? , s_boolean = ? , s_date = ? , s_interval = ? , " +
                        "s_timestamp_without_time_zone = ? , s_timestamp_wit_time_zone = ? , " +
                        "s_time_without_time_zone = ? , s_time_wit_time_zone = ? " +
                        " where id =?;")) {

                    statement.setObject(1, 76);
                    statement.setObject(2, 99);
                    statement.setObject(3, 7);

                    statement.setObject(4, 66.6);
                    statement.setObject(5, null);
                    statement.setObject(6, 88.92);
                    statement.setObject(7, null);

                    statement.setObject(8, null);
                    statement.setObject(9, null);
                    statement.setObject(10, null);
                    statement.setObject(11, null);

                    statement.setObject(12, null);
                    statement.setObject(13, 1);
                    statement.setObject(14, null);//"2024-02-02 17:56:00");
                    statement.setObject(15, null);

                    statement.setObject(16, null);//"2024-02-02 17:56:00.666666");
                    statement.setObject(17, null);//"2024-02-02 17:56:00.333333");
                    statement.setObject(18, null);//"17:56:00");
                    statement.setObject(19, null);//"17:56:22");
                    statement.setObject(20, id);
                    statement.execute();
                    System.out.println("update one record...");
                } finally {
                    Thread.sleep(30);
                    id--;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void gaussUpdate() {
        try(Connection conn = gaussJdbc.getConn()) {
            int id = 100;
            while(true) {
                try (PreparedStatement statement = conn.prepareStatement("update public.full_table " +
                        "set s_smallint = ? , s_integer = ? , s_bigint = ? , s_numeric = ? , s_real = ? , " +
                        "s_double_precision = ? , s_character = ? , s_character_varying = ?, s_text = ? , " +
                        "s_bytea = ? , s_bit = ? , s_bit_varying = ? , s_boolean = ? , s_date = ? , s_interval = ? , " +
                        "s_timestamp_without_time_zone = ? , s_timestamp_wit_time_zone = ? , " +
                        "s_time_without_time_zone = ? , s_time_wit_time_zone = ? " +
                        " where id =?;")) {

                    statement.setObject(1, 76);
                    statement.setObject(2, 99);
                    statement.setObject(3, 7);

                    statement.setObject(4, 66.6);
                    statement.setObject(5, null);
                    statement.setObject(6, 88.92);
                    statement.setObject(7, null);

                    statement.setObject(8, null);
                    statement.setObject(9, null);
                    statement.setObject(10, null);
                    statement.setObject(11, null);

                    statement.setObject(12, null);
                    statement.setObject(13, 1);
                    statement.setObject(14, "2024-02-02 17:56:00");
                    statement.setObject(15, null);

                    statement.setObject(16, null);
                    statement.setObject(17, null);
                    statement.setObject(18, null);
                    statement.setObject(19, null);
                    statement.setObject(20, id);
                    statement.execute();
                    System.out.println("update one record...");
                } finally {
                    Thread.sleep(200);
                    id++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void gaussUpdate2() {
        try(Connection conn = gaussJdbc.getConn()) {
            int id = -100;
            while(true) {
                try (PreparedStatement statement = conn.prepareStatement("update public.full_table " +
                        "set s_smallint = ? , s_integer = ? , s_bigint = ? , s_numeric = ? , s_real = ? , " +
                        "s_double_precision = ? , s_character = ? , s_character_varying = ?, s_text = ? , " +
                        "s_bytea = ? , s_bit = ? , s_bit_varying = ? , s_boolean = ? , s_date = ? , s_interval = ? , " +
                        "s_timestamp_without_time_zone = ? , s_timestamp_wit_time_zone = ? , " +
                        "s_time_without_time_zone = ? , s_time_wit_time_zone = ? " +
                        " where id =?;")) {

                    statement.setObject(1, 76);
                    statement.setObject(2, 99);
                    statement.setObject(3, 7);

                    statement.setObject(4, 66.6);
                    statement.setObject(5, null);
                    statement.setObject(6, 88.92);
                    statement.setObject(7, null);

                    statement.setObject(8, null);
                    statement.setObject(9, null);
                    statement.setObject(10, null);
                    statement.setObject(11, null);

                    statement.setObject(12, null);
                    statement.setObject(13, 1);
                    statement.setObject(14, "2024-02-02 17:56:00");
                    statement.setObject(15, null);

                    statement.setObject(16, null);
                    statement.setObject(17, null);
                    statement.setObject(18, null);
                    statement.setObject(19, null);
                    statement.setObject(20, id);
                    statement.execute();
                    System.out.println("update one record...");
                } finally {
                    Thread.sleep(20);
                    id--;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
