import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "87.236.23.232:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random rand = new Random();
        String[] users = {"users1", "users2", "user3"};
        String[] actions = {"view_page", "click_button", "add_to_cart"};
        try {
            while (true){
                String userId = users[rand.nextInt(users.length)];
                String action = actions[rand.nextInt(actions.length)];
                long timestamp = System.currentTimeMillis();
                String event = String.format("{\"event_type\": \"%s\", \"user_id\": \"%s\", \"action\": \"%s\", \"timestamp\": \"%d\"}",
                        "action", userId, action, timestamp);
                ProducerRecord<String, String> record = new ProducerRecord<>("user_actions", null, event);
                producer.send(record);
                Thread.sleep(rand.nextInt(5000));
            }
        }catch (InterruptedException e){
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }
}
