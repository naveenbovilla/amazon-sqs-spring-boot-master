package com.bfa.sqs.service;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class RankService {
 
    @Autowired
    JavaSparkContext sc;
 
//    public Map<String, Long> getCount(List<String> wordList) {
//        JavaRDD<String> words = sc.parallelize(wordList);
//        Map<String, Long> wordCounts = words.countByValue();
//        return wordCounts;
//    }


    public void rankAndInsert(){
        Instant start = Instant.now();
        JavaRDD<Document> rdd1 = MongoSpark.load(sc);
        JavaPairRDD<Double, Document> pairs =
                rdd1.mapToPair(s -> {
                    return new Tuple2<>((Double) s.get("teamScore"), s);
                });

        JavaPairRDD<Double, Iterable<Document>> tem = pairs.groupByKey();
        AtomicInteger rank = new AtomicInteger(0);

        List<Document> updateRankList = new ArrayList<>();

//          Working piece

        for (Tuple2<Double, Iterable<Document>> tuple : tem.collect()) {
            AtomicInteger tempRank = new AtomicInteger(0);
            //System.out.println("Team scores " + tuple._1.toString());
            Iterator<Document> it = tuple._2.iterator();
            Document doc;

            while (it.hasNext()) {
                doc = it.next();
                doc.put("teamRank", rank.get() + tempRank.get());
                tempRank.getAndIncrement();
                updateRankList.add(doc);
            }
            //System.out.println("No of team with the above score " + tempRank.get());
            rank.getAndIncrement();
        }

        System.out.println("-------------" + updateRankList.size());
        JavaRDD<Document> updateRDD = sc.parallelize(updateRankList);

        MongoSpark.save(updateRDD);
        updateRDD.collect();

        Instant finish = Instant.now();

        long timeElapsed = Duration.between(start, finish).toMillis();
        long seconds = TimeUnit.MILLISECONDS.toSeconds(timeElapsed);

        System.out.println("Total time to sort and rank ------------- :" + seconds);

    }

}
