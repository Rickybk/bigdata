package com.lagartos;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class WC_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String fechaIniStr;
    private String fechaFinStr;

    public void configure(JobConf job) {
        // Obtiene las fechas de la configuración del trabajo
        fechaIniStr = job.get("fechaIni");
        fechaFinStr = job.get("fechaFin");
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
            Reporter reporter) throws IOException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, "\n");
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            String[] parts = token.split("_");
            if (parts.length == 2) {
                String title = parts[0];
                String date = parts[1];
                if (isDateInRange(date)) {
                    word.set(title);
                    output.collect(word, one);
                }
            }
        }
    }

    private boolean isDateInRange(String date) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
        LocalDate fechaIni = LocalDate.parse(fechaIniStr, formatter);
        LocalDate fechaFin = LocalDate.parse(fechaFinStr, formatter);
        LocalDate fecha = LocalDate.parse(date, formatter);

        return (fecha.isAfter(fechaIni) || fecha.isEqual(fechaIni)) && (fecha.isBefore(fechaFin) || fecha.isEqual(fechaFin));
    }
}