var KafkaNode = require('./lib/index.js');

var consumer = new KafkaNode.KafkaConsumer(
  {
    'group.id': 'testing-44',
    'metadata.broker.list': 'localhost:9093',
    'enable.auto.commit': false,
  },
  {
    'auto.offset.reset': 'earliest',
  }
);

consumer.connect();

var topicName = 'consume-per-partition';

consumer.on('ready', function () {
  console.log('ready');

  consumer.subscribe([topicName]);

  consumeMessages(0, 100);
  consumeMessages(1, 1000);
});

function consumeMessages(partition, timeout) {
  function consume() {
    consumer.consume(10, topicName, partition, callback);
  }

  function callback(err, messages) {
    messages.forEach(function (message) {
      console.log('partition ' + message.partition + ' value: ' + message.value.toString());
      consumer.commitMessage(message);
    });

    // wait for the timeout and then trigger the next consume
    setTimeout(consume, timeout);
  }

  // kick-off recursive consume loop
  consume();
}
