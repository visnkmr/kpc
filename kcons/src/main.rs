// use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
// use kafka::error::Error as KafkaError;
// use std::str;
// /// This program demonstrates consuming messages through a `Consumer`.
// /// This is a convenient client that will fit most use cases.  Note
// /// that messages must be marked and committed as consumed to ensure
// /// only once delivery.
// fn main() {
//     // tracing_subscriber::fmt::init();

//     let broker = "localhost:9092".to_owned();
//     let topic = "my-topic".to_owned();
//     let group = "my-group".to_owned();

//     if let Err(e) = consume_messages(group, topic, vec![broker]) {
//         println!("Failed consuming messages: {}", e);
//     }
// }

// fn consume_messages(group: String, topic: String, brokers: Vec<String>) -> Result<(), KafkaError> {
//     let mut con = Consumer::from_hosts(brokers)
//         .with_topic(topic)
//         .with_group(group)
//         // .with_fallback_offset(FetchOffset::Earliest)
//         // .with_offset_storage(Some(GroupOffsetStorage::Kafka))
//         .create()?;

//     loop {
//         let mss = con.poll()?;
//         if mss.is_empty() {
//             println!("No messages available right now.");
//             return Ok(());
//         }
//         // print!("{:?}",mss.iter());
//         for ms in mss.iter() {
//             for m in ms.messages() {
//                 println!(
//                     "{}:{}@{}: {:?}",
//                     ms.topic(),
//                     ms.partition(),
//                     m.offset,
//                     str::from_utf8(m.value).unwrap()
//                 );
//             }
//             let _ = con.consume_messageset(ms);
//         }
//         con.commit_consumed()?;
//     }
// }

use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    message::Message,
    topic_partition_list::TopicPartitionList,
};
use std::{fs::File, io::Write, time::Duration};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), String> {
    let brokers = "localhost:9092";
    let group_id = "my-group";
    let topics = &["my-topic"]; // Replace with your topic(s)

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest") // Start from the earliest offset
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(topics)
        .expect("Can't subscribe to specified topics");

    println!("Consumer started. Waiting for messages...");

    loop {
        match consumer.recv().await {
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    Some(Ok(s)) => {
                   
                        s
                    },
                    Some(Err(e)) => {
                        println!("Error deserializing message payload: {:?}", e);
                        continue;
                    },
                    None => "",
                };
                println!("{}",payload);
                // let key = m.key_view::<str>().map_or("", |s| s);
                // let topic = m.topic();
                // let partition = m.partition();
                // let offset = m.offset();

                // println!(
                //     "key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}",
                //     key, payload, topic, partition, offset
                // );

                // // Save to disk
                // let filename = format!("message_{}_{}_{}.txt", topic, partition, offset);
                // let mut file = File::create(filename.clone()).expect("Error creating file");
                // file.write_all(payload.as_bytes())
                //     .expect("Error writing to file");
                // println!("Message saved to {}", filename);
            }
            Err(e) => {
                println!("Kafka error: {}", e);
                // Handle the error appropriately, e.g., reconnect, exit
                sleep(Duration::from_secs(1)).await; // Avoid rapid looping on error
                continue;
            }
        }
    }

    Ok(())
}