package myflinktopn.kafka;

import com.alibaba.fastjson.JSON;
import myflinktopn.pojo.UserAction;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author huangqingshi
 * @Date 2019-12-07
 */
public class KafkaWriter {

    //本地的kafka机器列表
    public static final String BROKER_LIST = "localhost:9092";
    //kafka的topic
    public static final String TOPIC_USER_ACTION = "USER_ACTION";
    //key序列化的方式，采用字符串的形式
    public static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    //value的序列化的方式
    public static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    //用户的行为列表
    public static final List<String> userBehaviors = Arrays.asList("pv", "buy", "cart", "fav");

    public static void writeToKafka() throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", KEY_SERIALIZER);
        props.put("value.serializer", VALUE_SERIALIZER);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        UserAction userAction = new UserAction();
        userAction.setUserId(RandomUtils.nextLong(1, 100));
        userAction.setItemId(RandomUtils.nextLong(1, 1000));
        userAction.setCategoryId(RandomUtils.nextInt(1, 30));
        userAction.setBehavior(userBehaviors.get(RandomUtils.nextInt(0, 3)));
        userAction.setTimestamp(System.currentTimeMillis());

        //转换成JSON
        String userActionJson = JSON.toJSONString(userAction);

        //包装成kafka发送的记录
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_USER_ACTION, null,
                null, userActionJson);
        //发送到缓存
        producer.send(record);
        System.out.println("向kafka发送数据:" + userActionJson);
        //立即发送
        producer.flush();

    }

    public static void main(String[] args) {
        while(true) {
            try {
                //每1秒写一条数据
                TimeUnit.SECONDS.sleep(1);
                writeToKafka();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

}
