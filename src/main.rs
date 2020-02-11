use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::time::{Duration, Instant};
use rdkafka::Message;

use std::fs::File;
use std::io::{self, prelude::*, BufReader};

fn main() {

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("acks", "0")
        .set("linger.ms", "0")
        .set("retries", "0")
        .create()
        .expect("Producer creation error");

    let file = File::open("src/teste.txt").unwrap();
    let reader = BufReader::new(file);
    let lines: Vec<_> = reader.lines().collect();

    let producer_clone = producer.clone();
    std::thread::spawn(move ||{
        loop {
            producer_clone.flush(Duration::from_millis(1));
        }
    });

    for line in lines.iter().cycle() {
        std::thread::sleep(Duration::from_micros(100));
        let before = Instant::now();
        producer.send::<str, str>(
            BaseRecord::to("teste.json")
                .payload(line.as_ref().unwrap().as_str()),
        ).expect("Failed to enqueue");
    }

//    let consumer: BaseConsumer = ClientConfig::new()
//        .set("group.id", "kafka_kudu")
//        .set("bootstrap.servers", "localhost:2181")
//        .create()
//        .expect("Consumer creation failed");
//
//    let topics = ["sgn.taa"];
//    consumer.subscribe(&topics.to_vec())
//        .expect("Can't subscribe to specified topics");
//
//    for message in consumer.iter() {
//        println!("{}", message.unwrap().payload_view::<str>().unwrap().unwrap());
//    }
}
