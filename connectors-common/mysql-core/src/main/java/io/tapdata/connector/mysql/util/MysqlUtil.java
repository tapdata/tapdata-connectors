package io.tapdata.connector.mysql.util;

import io.debezium.util.HexConverter;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.constant.DeployModeEnum;
import io.tapdata.kit.EmptyKit;
import io.tapdata.util.NetUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author samuel
 * @Description
 * @create 2022-05-06 20:31
 **/
public class MysqlUtil extends JdbcUtil {

	private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
	private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

	public static Integer getSubVersion(String version, int index) {
		if (StringUtils.isBlank(version)) {
			return null;
		}

		String[] split = version.split("\\.");
		if (split.length <= 1) {
			return null;
		}

		String str = split[index - 1];
		try {
			return Integer.valueOf(str);
		} catch (NumberFormatException e) {
			throw new RuntimeException("Version string: " + str + ", is not a number");
		}
	}

	public static int randomServerId() {
		int lowestServerId = 5400;
		int highestServerId = Integer.MAX_VALUE;
		return lowestServerId + new Random().nextInt(highestServerId - lowestServerId);
	}

	public static String fixDataType(String dataType, String version) {
		if (StringUtils.isBlank(dataType)) {
			return dataType;
		}
		// Fix datetime/timestamp when version<5.6
		dataType = fixDatetime(dataType, version);
		// Fix json when version<5.7
		dataType = fixJson(dataType, version);
		return dataType;
	}

	private static String fixJson(String dataType, String version) {
		if (StringUtils.isBlank(version)) {
			return dataType;
		}
		Integer firstVersion = getFirstVersion(version);
		if (null == firstVersion) {
			return dataType;
		}
		Integer secondVersion = getSecondVersion(version);
		if (null == secondVersion) {
			return dataType;
		}
		if (firstVersion.compareTo(5) <= 0 && secondVersion.compareTo(7) < 0) {
			if (StringUtils.equalsIgnoreCase(dataType, "json")) {
				dataType = "longtext";
			}
		}
		return dataType;
	}

	public static String fixDatetime(String dataType, String version) {
		if (StringUtils.isBlank(version)) {
			return dataType;
		}
		Integer firstVersion = getFirstVersion(version);
		if (null == firstVersion) {
			return dataType;
		}
		Integer secondVersion = getSecondVersion(version);
		if (null == secondVersion) {
			return dataType;
		}
		if (firstVersion.compareTo(5) <= 0 && secondVersion.compareTo(6) <= 0) {
			Pattern pattern = Pattern.compile("(datetime|timestamp)\\(\\d+\\)", Pattern.CASE_INSENSITIVE);
			if (pattern.matcher(dataType).matches()) {
				dataType = dataType.replaceAll("\\(\\d+\\)", "");
			}
		}
		return dataType;
	}

	private static Integer getSecondVersion(String version) {
		Integer secondVersion;
		try {
			secondVersion = getSubVersion(version, 2);
		} catch (Exception e) {
			throw new RuntimeException("Get second version number failed, version string: " + version + ", error: " + e.getMessage(), e);
		}
		return secondVersion;
	}

	private static Integer getFirstVersion(String version) {
		Integer firstVersion;
		try {
			firstVersion = getSubVersion(version, 1);
		} catch (Exception e) {
			throw new RuntimeException("Get first version number failed, version string: " + version + ", error: " + e.getMessage(), e);
		}
		return firstVersion;
	}

	public static long convertTimestamp(long timestamp, TimeZone fromTimeZone, TimeZone toTimeZone) {
		LocalDateTime dt = LocalDateTime.now();
		ZonedDateTime fromZonedDateTime = dt.atZone(fromTimeZone.toZoneId());
		ZonedDateTime toZonedDateTime = dt.atZone(toTimeZone.toZoneId());
		long diff = Duration.between(toZonedDateTime, fromZonedDateTime).toMillis();
		return timestamp + diff;
	}

	public static String convertTime(Object time){
		String str[] =((String)time).split(":");
		String timeTemp;
		if(str.length==3){
			int hour = Math.abs(Integer.parseInt(str[0]))%24;
			timeTemp = (hour < 10 ? ("0" + hour) : hour) + ":" +str[1] + ":"+str[2];
			return timeTemp;
		}
		return null;
	}

	public static String toHHmmss(long time) {
		String timeTemp;
		int hours = (int) (time % (1000 * 60 * 60 * 24) / (1000 * 60 * 60));
		int minutes = (int) (time % (1000 * 60 * 60) / (1000 * 60));
		int seconds = (int) (time % (1000 * 60) / 1000);
		timeTemp = (hours < 10 ? ("0" + hours) : hours) + ":" + (minutes < 10 ? ("0" + minutes) : minutes) + ":" + (seconds < 10 ? ("0" + seconds) : seconds);
		return timeTemp;
	}

	public static String object2String(Object obj) {
		String result;
		if (null == obj) {
			result = "null";
		} else if (obj instanceof String) {
			result = "'" + ((String) obj).replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'").replaceAll("\\(", "\\\\(").replaceAll("\\)", "\\\\)") + "'";
		} else if (obj instanceof Number) {
			result = obj.toString();
		} else if (obj instanceof Date) {
			result = "'" + dateFormat.format(obj) + "'";
		} else if (obj instanceof Instant) {
			result = "'" + LocalDateTime.ofInstant((Instant) obj, ZoneId.of("GMT")).format(dateTimeFormatter) + "'";
		} else if (obj instanceof byte[]) {
			String hexString = HexConverter.convertToHexString((byte[]) obj);
			return "X'" + hexString + "'";
		} else if (obj instanceof Boolean) {
			if ("true".equalsIgnoreCase(obj.toString())) {
				return "1";
			}
			return "0";
		} else {
			return "'" + obj + "'";
		}
		return result;
	}

	protected static void testHostPortForMasterSlave(MysqlConfig mysqlConfig) {
		ArrayList<LinkedHashMap<String, Integer>> masterSlaveAddress = mysqlConfig.getMasterSlaveAddress();
		ArrayList<LinkedHashMap<String, Integer>> availableMasterSlaveAddress = Optional.ofNullable(mysqlConfig.getAvailableMasterSlaveAddress()).orElse(new ArrayList<>());
		for (LinkedHashMap<String, Integer> hostPort : masterSlaveAddress) {
            try {
                NetUtil.validateHostPortWithSocket(String.valueOf(hostPort.get("host")), hostPort.get("port"));
				availableMasterSlaveAddress.add(hostPort);
            } catch (IOException e) {
				availableMasterSlaveAddress.remove(hostPort);
            }
		}
		mysqlConfig.setAvailableMasterSlaveAddress(availableMasterSlaveAddress);
	}
	public static void buildMasterNode(MysqlConfig mysqlConfig) {
		if (null == mysqlConfig) return;
		String deploymentMode = mysqlConfig.getDeploymentMode();
		DeployModeEnum deployModeEnum = DeployModeEnum.fromString(deploymentMode);
		if (deployModeEnum == null) {
			deployModeEnum = DeployModeEnum.STANDALONE;
		}
		if (deployModeEnum == DeployModeEnum.MASTER_SLAVE) {
			ArrayList<LinkedHashMap<String, Integer>> masterSlaveAddress = mysqlConfig.getMasterSlaveAddress();
			if (EmptyKit.isEmpty(masterSlaveAddress)) {
				throw new RuntimeException("host cannot be empty");
			}
			testHostPortForMasterSlave(mysqlConfig);
			ArrayList<LinkedHashMap<String, Integer>> availableMasterSlaveAddress = mysqlConfig.getAvailableMasterSlaveAddress();
			if (EmptyKit.isEmpty(availableMasterSlaveAddress)){
				throw new RuntimeException("there is no available node");
			}
			MysqlJdbcContextV2 mysqlJdbcContext;
			HashSet<LinkedHashMap<String, Integer>> masterNode = new HashSet<>(availableMasterSlaveAddress);
			Map<String, Object> masterHostPortAndStatus = null;
			int count = 1;
			boolean needQuerySlaveStatus = false;
			if (availableMasterSlaveAddress.size() == 1) {
				needQuerySlaveStatus = true;
			}
			while ((count < 3 && masterNode.size() != 1) || needQuerySlaveStatus) {
				Iterator<LinkedHashMap<String, Integer>> iterator = masterNode.iterator();
				while (iterator.hasNext()){
					LinkedHashMap<String, Integer> address = iterator.next();
					mysqlConfig.setHost(String.valueOf(address.get("host")));
					mysqlConfig.setPort(address.get("port"));
                    try {
						mysqlJdbcContext = new MysqlJdbcContextV2(mysqlConfig);
                        masterHostPortAndStatus = mysqlJdbcContext.querySlaveStatus();
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                    if (EmptyKit.isEmpty(masterHostPortAndStatus)) {
						continue;
					}
					String slaveIoRunning = (String) masterHostPortAndStatus.get("slaveIoRunning");
					String slaveSqlRunning = (String) masterHostPortAndStatus.get("slaveSqlRunning");
					if ("Yes".equalsIgnoreCase(slaveIoRunning) || "Yes".equalsIgnoreCase(slaveSqlRunning)) {
						iterator.remove();
					}
				}
				count++;
				needQuerySlaveStatus = false;
			}
			if (masterNode.size() < 1) {
				throw new RuntimeException(String.format("master node:%s is not available, please make sure host port is valid", masterSlaveAddress.get(0)));
			} else if (masterNode.size() > 1) {
				throw new RuntimeException("please make sure there is one master node at most");
			} else {
				LinkedHashMap<String, Integer> master = masterNode.stream().findFirst().get();
				mysqlConfig.setHost(String.valueOf(master.get("host")));
				mysqlConfig.setPort(master.get("port"));
				mysqlConfig.setMasterNode(master);
			}
		}
	}
}
