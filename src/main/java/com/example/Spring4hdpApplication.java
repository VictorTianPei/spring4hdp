package com.example;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.hadoop.config.annotation.EnableHadoop;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hive.HiveTemplate;

import java.util.List;


@ImportResource("hadoop-context.xml")
@SpringBootApplication
@EnableHadoop
public class Spring4hdpApplication {

  @Autowired
  HbaseTemplate hbaseTemplate;

  @Autowired
  HiveTemplate hiveTemplate;

  public static void main(String[] args) {
    SpringApplication.run(Spring4hdpApplication.class, args);
  }

  @Bean
  public CommandLineRunner commandLineRunner() {
    return (String... args) -> {

      System.out.println("===================");

      Scan scan = new Scan();
      scan.addColumn(Bytes.toBytes("ais"), Bytes.toBytes("LONG_LONGITUDE"));
      List<Object> list = hbaseTemplate.find("S_AIS_FLOW_MAP", scan, (result, rowNum) -> result);
      System.out.println(list);


      List<String> result = hiveTemplate.query("select * from tst");
      System.out.println(result);

      System.out.println("===================");

    };
  }
}
