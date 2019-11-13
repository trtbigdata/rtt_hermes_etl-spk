package com.hupu.hermes.utils;

/**
 * @author yanjiajun
 * @create 2019-07-05 17:01
 */
public class Constants {

    /**
     * jdbc配置文件路径(src/main/resources)
     **/
    public static final String JDBC_CONF_FILE = "db.properties";

    /**
     * Mysql字段、条件及重命名字段
     */
    public static final String ID = "id";
    public static final String TOPIC_ID = "topic_id";
    public static final String INPUT_TOPIC_ID = "input_topic_id";
    public static final String OUTPUT_TOPIC_ID = "output_topic_id";
    public static final String INPUT_TOPIC = "input_topic";
    public static final String _TOPIC = "_topic";
    public static final String OUTPUT_TOPIC = "output_topic";
    public static final String INPUT_FIELD = "input_field";
    public static final String INPUT_FIELDS = "input_fields";
    public static final String INPUT_TYPE = "input_type";
    public static final String UDF = "udf";
    public static final String _UDF = "_udf";
    public static final String OUTPUT_FIELD_IDS = "output_field_ids";
    public static final String FIELD_NAME = "field_name";
    public static final String OUTPUT_FIELD = "output_field";
    public static final String FIELD_TYPE = "field_type";
    public static final String OUTPUT_TYPE = "output_type";
    public static final String PARSE_FIELD = "parse_field";
    public static final String FIELD_CONCAT = "field_concat";
    public static final String COUNT_1 = "count(1)";
    public static final String UDF_IS_NOT_NULL = "udf is not null";
    public static final String SPACE = " ";
    public static final String GET_FROM_MAP = "get_from_map";
    public static final String ALL_FIELDS = "all_fields";
    /**
     * spark表
     */
    public static final String TBL_FIELD_DETAIL = "tbl_field_detail";
    public static final String TBL_FILTERED_DATA = "tbl_filtered_data";
    public static final String TBL_TRANSFER_DATA = "tbl_transfer_data";
    public static final String ACCU_FROM_KAFKA = "fromKafka";
    public static final String ACCU_INTO_KAFKA = "intoKafka";

    /**
     * UDF
     */
    public static final String PARSE_PATH = "parse_path";
    public static final String PARSE_TIME = "parse_time";
    public static final String PARSE_GENDER = "parse_gender";
    public static final String PARSE_REGION = "parse_region";
    public static final String PARSE_AB_TEST = "parse_ab";

    /**
     * config
     */
    public static final String SWITCH = ConfigUtil.getProperty("switch");
    public static final String APP_NAME = ConfigUtil.getProperty("app.name");
    public static final String IN_KAFKA_TOPIC = ConfigUtil.getProperty("inKafka.topics");
    public static final String STREAMING_DURATION = ConfigUtil.getProperty("streaming.duration");
    public static final String OUT_KAFKA_SERVERS = ConfigUtil.getProperty("outKafka.bootstrap.servers");
    // "1"代表本地测试
    public static final String IF_LOCAL = ConfigUtil.getProperty("if.local");

}
