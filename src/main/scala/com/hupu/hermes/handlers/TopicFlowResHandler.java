package com.hupu.hermes.handlers;

import com.hupu.hermes.model.EtlFlowMonitor;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author yanjiajun
 * @create 2019-08-21 14:23
 */
public class TopicFlowResHandler implements ResultSetHandler {
    @Override
    public EtlFlowMonitor handle(ResultSet rs) throws SQLException {
        EtlFlowMonitor etlFlowMonitor = null;
        while (rs.next()) {
            String dt = rs.getString(1);
            String topic = rs.getString(2);
            Long numFromKafka = rs.getLong(3);
            Long numIntoKafka = rs.getLong(4);
            etlFlowMonitor = new EtlFlowMonitor();
            etlFlowMonitor.setDt(dt);
            etlFlowMonitor.setTopic(topic);
            etlFlowMonitor.setNumFromKafka(numFromKafka);
            etlFlowMonitor.setNumIntoKafka(numIntoKafka);
        }
        return etlFlowMonitor;
    }
}
