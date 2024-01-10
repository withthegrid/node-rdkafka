var KafkaNode = require('./lib/index.js');

var consumer = new KafkaNode.KafkaConsumer(
  {
    'group.id': 'testing-67',
    'metadata.broker.list': 'localhost:9093',
    'enable.auto.commit': false,
    'rebalance_cb': true,
  },
  {
    'auto.offset.reset': 'earliest',
  }
);

consumer.on('rebalance', function(err, assignments) {
  console.log('rebalancing done: ', assignments);
});

consumer.on('subscribed', function(topics) {
  console.log('subscribed: ', topics, ' assignments: ', consumer.assignments());
});

consumer.connect();

var topicName = 'consume-per-partition';

consumer.on('ready', function () {
  console.log('ready');

  consumer.subscribe([topicName]);

  setInterval(function() {
    consumer.consume(1);
  }, 500);
}).on('data', function(message) {
  console.log('partition ' + message.partition + ' value: ' + message.value.toString());
  consumer.commitMessage(message);
});

process.on('SIGUSR2', function() {
  console.log('process received SIGUSR2. Shutting down consumer');
  consumer.disconnect(function () {
    console.log('Consumer disconnected. Shutting down...');
    process.exit(0);
  });
});

process.on('SIGINT', function() {
  console.log('process received SIGINT. Shutting down consumer');
  consumer.disconnect(function () {
    console.log('Consumer disconnected. Shutting down...');
    process.exit(0);
  });
});
