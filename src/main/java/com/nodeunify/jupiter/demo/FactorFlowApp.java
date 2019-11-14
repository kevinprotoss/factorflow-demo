package com.nodeunify.jupiter.demo;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import com.nodeunify.jupiter.datastream.v1.StockData;
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
        Map<String, Object> props = (Map<String, Object>) yaml.load(is);

        KafkaIO kafkaIO = KafkaIO.create(props);
        Dataset<StockData> stockData = kafkaIO.readData(StockData.class);
        Dataset<StockData> result = kafkaIO.readData("test.out", StockData.class);
        Dataset<String> codes = result.map((MapFunction<StockData, String>) v -> v.getCode(), Encoders.STRING());
        // @formatter:off
        StreamingQuery query = codes
            .writeStream()
            .outputMode("update")
            .format("console")
            .start();
        // @formatter:on
        kafkaIO.writeData(stockData);
        query.awaitTermination();
    }
}
