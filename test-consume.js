var KafkaNode = require('./lib/index.js');

var consumer = new KafkaNode.KafkaConsumer(
  {
    'group.id': 'testing-78',
    'metadata.broker.list': 'localhost:9093',
    'enable.auto.commit': false,
    'rebalance_cb': true,
  },
  {
    'auto.offset.reset': 'earliest',
  }
);

consumer.on('subscribed', function(topics) {
  console.log('subscribed: ', topics, ' assignments: ', consumer.assignments().map(function(a) { return a.partition; }));
});

consumer.connect();

var topicName = 'consume-per-partition';
var isShuttingDown = false;
var assignments = [];

consumer.on('ready', function () {
  console.log('ready: ', consumer.globalConfig['group.id']);

  consumer.subscribe([topicName]);

  // do not wait on each consume
  consumer.setDefaultConsumeLoopTimeoutDelay(500);
  consumer.setDefaultConsumeTimeout(0);

  // Trigger a consume loop which serves the rebalancing events
  consumer.consume();
});


consumer.on('rebalance', function(err, updatedAssignments) {
  console.log('rebalancing done: ', updatedAssignments.map(function(a) { return a.partition; }));

  // find new assignments
  var newAssignments = updatedAssignments.filter(function (updatedAssignment) {
    return !assignments.some(function (assignment) {
      return assignment.partition === updatedAssignment.partition;
    });
  });

  // update global assignments array
  assignments = updatedAssignments;

  // then start consume loops for the new assignments
  newAssignments.forEach(function (assignment) {
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

    consumer.consume(1, topicName, partition, callback);
  }

  function callback(err, messages) {
    messages.forEach(function (message) {
      console.log('partition ' + message.partition + ' value ' + message.value.toString());
      consumer.commitMessage(message);
    });

    // simulate performance
    if(partition === 0) {
      setTimeout(consume, 2000);
    } else {
      // consume();
      setTimeout(consume, 500);
    }
  }

  // kick-off recursive consume loop
  consume();
}


process.on('SIGUSR2', function() {
  isShuttingDown = true;
  console.log('process received SIGUSR2. Shutting down consumer');
  consumer.disconnect(function () {
    console.log('Consumer disconnected. Shutting down...');
    process.exit();
  });
});

process.on('SIGINT', function() {
  isShuttingDown = true;
  console.log('process received SIGINT. Shutting down consumer');
  consumer.disconnect(function () {
    console.log('Consumer disconnected. Shutting down...');
    process.exit(0);
  });
});
