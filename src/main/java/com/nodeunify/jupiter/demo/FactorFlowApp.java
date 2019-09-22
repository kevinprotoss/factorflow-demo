package com.nodeunify.jupiter.demo;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import com.nodeunify.jupiter.datastream.v1.Quote;
import com.nodeunify.jupiter.spark.io.KafkaIO;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.yaml.snakeyaml.Yaml;

@SuppressWarnings("unchecked")
public final class FactorFlowApp {
    private FactorFlowApp() {
    }

    /**
     * Says hello to the world.
     *
     * @param args The arguments of the program.
     * @throws InstantiationException
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws StreamingQueryException
     */
    public static void main(String[] args)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
            SecurityException, InstantiationException, StreamingQueryException {
        Yaml yaml = new Yaml();
        InputStream is = FactorFlowApp.class.getClassLoader().getResourceAsStream("application.yml");
        Map<String, Object> appProps = (Map<String, Object>) yaml.load(is);

        KafkaIO kafkaIO = KafkaIO.create(appProps);
        Dataset<Quote> quotes = kafkaIO.readData(Quote.class);
        Dataset<Quote> result = kafkaIO.readData("test.out", Quote.class);
        Dataset<String> codes = result.map((MapFunction<Quote, String>) v -> v.getCode(), Encoders.STRING());
        // @formatter:off
        StreamingQuery query = codes
            .writeStream()
            .outputMode("update")
            .format("console")
            .start();
        // @formatter:on
        kafkaIO.writeData(quotes);
        query.awaitTermination();
    }
}
