package com.hupu.hermes.model;

/**
 * @author yanjiajun
 * @create 2019-08-21 14:21
 */
public class EtlFlowMonitor {
    private String dt;
    private String topic;
    private Long numFromKafka;
    private Long numIntoKafka;

    public EtlFlowMonitor() {}

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Long getNumFromKafka() {
        return numFromKafka;
    }

    public void setNumFromKafka(Long numFromKafka) {
        this.numFromKafka = numFromKafka;
    }

    public Long getNumIntoKafka() {
        return numIntoKafka;
    }

    public void setNumIntoKafka(Long numIntoKafka) {
        this.numIntoKafka = numIntoKafka;
    }
}
