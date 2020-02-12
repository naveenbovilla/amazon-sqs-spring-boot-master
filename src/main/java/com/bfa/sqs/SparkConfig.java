package com.bfa.sqs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {
 
//    @Value("${spark.app.name}")
//    private String appName;
//    @Value("${spark.master}")
//    private String masterUri;
 
    @Bean
    public SparkSession conf() {

        SparkConf conf = new SparkConf().setAppName("MongoSparkConnectorIntro")
                .setMaster("local")
                .set("spark.driver.memory", "1g")
                .set("spark.executor.memory","2g")
                .set("spark.network.timeout", "600s");

        SparkSession spark = SparkSession.builder().config(conf).config("spark.mongodb.input.uri", "mongodb://user:pass@x.x.x.x:27017/test.teams_snapshot_input")
                .config("spark.mongodb.output.uri", "mongodb://user:pass@x.x.x.x:27017/test.teams_snapshot_final_service")
                .getOrCreate();
//        SparkSession spark = SparkSession.builder()
////                .master("local")
////                .appName("MongoSparkConnectorIntro")
////                .config("spark.mongodb.input.uri", "mongodb://user:pass@x.x.x.x:27017/test.teams_snapshot_input")
////                .config("spark.mongodb.output.uri", "mongodb://user:pass@x.x.x.x:27017/test.teams_snapshot_final_service")
////                .getOrCreate();
        return spark;
    }
 
    @Bean
    public JavaSparkContext sc() {
        return new JavaSparkContext(conf().sparkContext());
    }
 
}