package kafka.connect;

import kafka.connect.model.AvroReader;
import kafka.connect.model.AvroWriter;
import kafka.connect.model.avro.Employee;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public class KafkaWriter {
    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroWriter.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,1);
        props.put(ProducerConfig.LINGER_MS_CONFIG,100);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroReader.class.getName());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "emp33");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        ArrayList<Employee> employees = new ArrayList<Employee>();
        employees.add ( Employee.newBuilder().setAge(11).setName("AArush").setLastName("Marchareddy").setPhoneNumber("6094249761").build());
        employees.add ( Employee.newBuilder().setAge(11).setName("Arnav").setLastName("Marchareddy").setPhoneNumber("6094249761").build());
        employees.add ( Employee.newBuilder().setAge(33).setName("Leena").setLastName("Marchareddy").setPhoneNumber("6094249761").build());
        employees.add ( Employee.newBuilder().setAge(44).setName("Praveen").setLastName("Marchareddy").setPhoneNumber("6094249761").build());


        KafkaProducer producer = new KafkaProducer(props);
//
        try {

            employees.forEach(ec->{
                System.out.print("Sending " + ec.getName());
                producer.send(new ProducerRecord("newemployees", ec.getName(), ec), (x, e)->{
                     System.out.println("Sent  " + x +  " e " + e);
                });
                producer.flush();
            });


        } catch (Throwable e) {
            e.printStackTrace();
        }

        System.out.print("CONSUMER Starting");
        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
        ArrayList<String> arrayList = new ArrayList<String>();
        arrayList.add("newemployees");
        kafkaConsumer.subscribe(arrayList);
        ConsumerRecords records = kafkaConsumer.poll(Duration.ofMillis(1000));
        try {
            records.forEach(d -> {
                System.out.println( d.toString());

            });
            System.out.print("CONSUMER Started");
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
