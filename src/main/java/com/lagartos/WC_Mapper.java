package com.lagartos;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import org.apache.hadoop.mapred.JobConf;
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

import org.apache.log4j.Logger;

public class WC_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private static final Logger LOG = Logger.getLogger(WC_Mapper.class);
    private Text word = new Text();
    private String fechaIniStr;
    private String fechaFinStr;
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList("a", "ante", "bajo", "cabe", "con", "contra", "de", "desde", "durante", 
    "en", "entre", "hacia", "hasta", "mediante", "para", "por", "según", 
    "sin", "so", "sobre", "tras", "versus", "vía", "el", "la", "los", 
    "las", "un", "una", "unos", "unas", "al", "del", "y", "o", "u", "pero", 
    "aunque", "ni", "sin", "sino", "si", "mas", "ya", "bien", "sea", "que", 
    "como", "cuál", "cuáles", "donde", "cuando", "cuyo", "cuya", "cuyos", 
    "cuyas", "quién", "quiénes", "qué", "cuánto", "cuánta", "cuántos", 
    "cuántas", "mi", "tu", "su", "nuestro", "nuestra", "nuestros", "nuestras", 
    "vuestro", "vuestra", "vuestros", "vuestras", "me", "te", "se", "nos", 
    "os", "les", "lo", "la", "le", "los", "las", "les", "uno", "dos", 
    "tres", "cuatro", "cinco", "seis", "siete", "ocho", "nueve", "diez", 
    "primer", "segundo", "tercer", "cuarto", "quinto", "sexto", "séptimo", 
    "octavo", "noveno", "décimo", "primero", "segundo", "tercero", "cuarto", 
    "quinto", "sexto", "séptimo", "octavo", "noveno", "décimo", "por", 
    "señor", "señora", "señores", "señoras", "don", "doña", "etc", "etcétera", 
    "ah", "ay", "eh", "hey", "hola", "oh", "sí", "sí", "no", "muy", 
    "más", "menos", "mucho", "muchos", "poco", "pocos", "demasiado", 
    "nada", "algo", "todo", "toda", "todos", "todas", "cualquier", 
    "cualquiera", "cualesquiera", "otro", "otra", "otros", "otras", 
    "mismo", "misma", "mismos", "mismas", "alguien", "nadie", "alguno", 
    "alguna", "algunos", "algunas", "ninguno", "ninguna", "ningunos", 
    "ningunas", "quien", "cuyo", "cuyos", "cuyas", "sobre", "tras", "más", 
    "menos", "ese", "esa", "esos", "esas", "este", "esta", "estos", 
    "estas", "aquel", "aquella", "aquellos", "aquellas"));

    public void configure(JobConf job) {
        fechaIniStr = job.get("fechaIni");
        fechaFinStr = job.get("fechaFin");
        LOG.info("Configurado con fechaIni=" + fechaIniStr + ", fechaFin=" + fechaFinStr);
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
    String line = value.toString();
    StringTokenizer tokenizer = new StringTokenizer(line, "\n");
    while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken();
        String[] parts = token.split("_");
        if (parts.length == 2) {
            String title = parts[0];
            String date = parts[1];
            if (isDateInRange(date, fechaIniStr, fechaFinStr)) {
                StringTokenizer wordTokenizer = new StringTokenizer(title);
                while (wordTokenizer.hasMoreTokens()) {
                    String wordStr = wordTokenizer.nextToken();
                    if (!STOP_WORDS.contains(wordStr.toLowerCase())) {
                        word.set(wordStr);
                        output.collect(word, one);
                    }
                }
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