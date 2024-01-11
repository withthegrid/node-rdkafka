var KafkaNode = require('./lib/index.js');

var consumer = new KafkaNode.KafkaConsumer({
  'group.id': 'testing-80',
  'metadata.broker.list': 'localhost:9093',
  'enable.auto.commit': false,
  'rebalance_cb': true,
}, {
  'auto.offset.reset': 'earliest',
});

consumer.on('subscribed', function(topics) {
  console.log('subscribed: ', topics, ' assignments: ', consumer.assignments().map(function(a) {
    return a.partition;
  }));
});

consumer.connect();

var topicName = 'consume-per-partition';
var assignments = [];

consumer.on('ready', function() {
  console.log('ready: ', consumer.globalConfig['group.id']);

  consumer.subscribe([topicName]);

  // Remove the default timeout so that we won't wait on each consume
  consumer.setDefaultConsumeTimeout(0);

  // start a regular consume loop in flowing mode. This won't result in any
  // messages because will we start consuming from a partition directly.
  // This is required to serve the rebalancing events
  consumer.consume();
});


consumer.on('rebalance', function(err, updatedAssignments) {
  console.log('rebalancing done, got partitions assigned: ', updatedAssignments.map(function(a) {
    return a.partition;
  }));

  // find new assignments
  var newAssignments = updatedAssignments.filter(function(updatedAssignment) {
    return !assignments.some(function(assignment) {
      return assignment.partition === updatedAssignment.partition;
    });
  });

  // update global assignments array
  assignments = updatedAssignments;

  // then start consume loops for the new assignments
  newAssignments.forEach(function(assignment) {
    startConsumeMessages(assignment.partition);
  });
});

function startConsumeMessages(partition) {
  console.log('partition: ' + partition + ' starting to consume');

  function consume() {
    var isPartitionAssigned = assignments.some(function(assignment) {
      return assignment.partition === partition;
    });

    if (!isPartitionAssigned) {
      console.log('partition: ' + partition + ' stop consuming');
      return;
    }

    // consume per 5 messages
    consumer.consume(1, topicName, partition, callback);
  }

  function callback(err, messages) {
    messages.forEach(function(message) {
      // consume the message
      console.log('partition ' + message.partition + ' value ' + message.value.toString());
      consumer.commitMessage(message);
    });

    if (messages.length > 0) {
      consumer.commitMessage(messages.pop());
    }

    // simulate performance
    setTimeout(consume, partition === 0 ? 2000 : 500);
  }

  // kick-off recursive consume loop
  consume();
}


process.on('SIGUSR2', function() {
  console.log('process received SIGUSR2. Shutting down consumer');
  consumer.disconnect(function() {
    console.log('Consumer disconnected. Shutting down...');
    process.exit();
  });
});

process.on('SIGINT', function() {
  console.log('process received SIGINT. Shutting down consumer');
  consumer.disconnect(function() {
    console.log('Consumer disconnected. Shutting down...');
    process.exit(0);
  });
});
