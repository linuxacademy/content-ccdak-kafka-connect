package com.linuxacademy.ccdak.connect;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public class FileSinkTask extends SinkTask {
    
    private String filename;
    private BufferedWriter writer;

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            try {
                writer.write(record.toString());
                writer.newLine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void start(Map<String, String> props) {
        filename = props.get(FileSourceConnector.FILE_CONFIG);
        try {
            writer = new BufferedWriter(new FileWriter(new File(filename), false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void stop() {
        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String version() {
        return new FileSourceConnector().version();
    }

}
