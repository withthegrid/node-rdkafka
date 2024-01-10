var KafkaNode = require('./lib/index.js');

var consumer = new KafkaNode.KafkaConsumer(
  {
    'group.id': 'testing-54',
    'metadata.broker.list': 'localhost:9093',
    'enable.auto.commit': false,
  },
  {
    'auto.offset.reset': 'earliest',
  }
);

consumer.on('subscribed', function(topics) {
  console.log('subscribed: ', topics, ' assignments: ', consumer.assignments());
});

consumer.connect();

var topicName = 'consume-per-partition';
var assignments = [];

consumer.on('ready', function () {
  console.log('ready');

  consumer.subscribe([topicName]);

  // check assigned partitions
  // and start consume for newly assigned partitions
  setInterval(updateAssignmentsAndStartConsumeLoops, 1000);
});

function updateAssignmentsAndStartConsumeLoops() {
  var updatedAssignments = consumer.assignments();

  // find new assignments
  var newAssignments = updatedAssignments.filter(function (a) {
    return !assignments.includes(a);
  });

  newAssignments.forEach(function (assignment) {
    startConsumeMessages(assignment.partition);
  });

  assignments = updatedAssignments;
}

function startConsumeMessages(partition) {
  // simulate different performance
  var timeout = partition === 0 ? 500 : 2000;

  function consume() {
    var isPartitionAssigned = assignments.includes(function(assignment) {
      return assignment.partition === partition;
    });

    if (!isPartitionAssigned) {
      // stop consuming
      return;
    }

    consumer.consume(10, topicName, partition, callback);
  }

  function callback(err, messages) {
    messages.forEach(function (message) {
      console.log('partition ' + message.partition + ' value: ' + message.value.toString());
      consumer.commitMessage(message);
    });

    // simulate performance
    setTimeout(consume, timeout);
  }

  // kick-off recursive consume loop
  consume();
}


process.on('SIGUSR2', function() {
  console.log('process received SIGUSR2. Shutting down consumer');
  consumer.disconnect(function () {
    console.log('Consumer disconnected. Shutting down...');
    process.exit();
  });
});
