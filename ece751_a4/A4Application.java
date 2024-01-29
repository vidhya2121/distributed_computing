import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;


public class A4Application {

	public static void main(String[] args) throws Exception {
		// do not modify the structure of the command line
		String bootstrapServers = args[0];
		String appName = args[1];
		String studentTopic = args[2];
		String classroomTopic = args[3];
		String outputTopic = args[4];
		String stateStoreDir = args[5];

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);
		props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

		// add code here if you need any additional configuration options

		StreamsBuilder builder = new StreamsBuilder();



		// add code here
		// 
		// ... = builder.stream(studentTopic);
		// ... = builder.stream(classroomTopic);
		// ...
		// ...to(outputTopic);

		// studentTable -> (student_id, room_id)
		KTable<String, String> studentTable = builder.table(studentTopic);
		// classTable -> (room_id, room_capacity)
		KTable<String, String> classTable = builder.table(classroomTopic);

		// table -> (room_id, current_number_of_people_in_room)
		// converts student_id,room_num to room_num,student_id and calculates the number of students in each room
		KTable<String, Long> table = studentTable.groupBy((student_id, room_num) -> new KeyValue<>(room_num, student_id)).count();

		//uncomment this
		KTable<String, String> jointable = classTable.join(table, (room_capacity, current_number_of_people_in_room) -> room_capacity + "," + current_number_of_people_in_room);

		Aggregator<String, String, String> aggregator = ((key, curr, return_of_prev) -> {

			String[] split_array = curr.split(",");
			int room_capacity = Integer.parseInt(split_array[0]);
			int current_number_of_people_in_room = Integer.parseInt(split_array[1]);

			if(current_number_of_people_in_room > room_capacity) {
				return String.valueOf(current_number_of_people_in_room);
			}
			else if(return_of_prev != null  && !return_of_prev.equals("OK") && Integer.parseInt(return_of_prev) > current_number_of_people_in_room) {
				return "OK";
			}

			return null;
		});

		jointable.toStream().groupByKey().aggregate(()->null, aggregator).toStream().filter((key, value) -> value!=null).to(outputTopic);

		//printing all the stuff
		jointable.toStream().peek((room_capacity, current_number_of_people_in_room) -> System.out.println("joinTable: " + room_capacity + "," + current_number_of_people_in_room));
		classTable.toStream().peek((class_num, class_cap) -> System.out.println("CT " + class_num + "," + class_cap));
		table.toStream().peek((room_num, current_num_studs) -> System.out.println("RT " + room_num + "," + current_num_studs));
		//studentStream.groupBy((student_id, room_num) -> KeyValue.pair(room_num, student_id));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);

		// this line initiates processing
		streams.start();

		// shutdown hook for Ctrl+C
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
