package com.linuxacademy.ccdak.connect;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public class FileStreamSourceTask extends SourceTask {

    private String filename;
    private BufferedReader reader;
    private String topic;

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            ArrayList<SourceRecord> records = new ArrayList<>();
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                if (line != null) {
                    Map sourcePartition = Collections.singletonMap("filename", filename);
                    Map sourceOffset = Collections.singletonMap("position", lineNumber);
                    records.add(new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line));
                }
            }
            lineNumber++;
            return records;
        } catch (IOException e) {
            // Underlying stream was killed, probably as a result of calling stop. Allow to return
            // null, and driving thread will handle any shutdown if necessary.
        }
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        filename = props.get(FileStreamSourceConnector.FILE_CONFIG);
        try {
            reader = new BufferedReader(new FileReader(new File(filename)));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        topic = props.get(FileStreamSourceConnector.TOPIC_CONFIG);
    }

    @Override
    public synchronized void stop() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String version() {
        return new FileStreamSourceConnector().version();
    }

}
