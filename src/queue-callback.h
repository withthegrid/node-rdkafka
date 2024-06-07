/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_QUEUE_CALLBACKS_H_
#define SRC_QUEUE_CALLBACKS_H_

#include <uv.h>
#include <nan.h>

#include <vector>
#include <map>

#include "rdkafkacpp.h"
#include "src/common.h"

typedef Nan::Persistent<v8::Function,
  Nan::CopyablePersistentTraits<v8::Function> > PersistentCopyableFunction;
typedef std::vector<PersistentCopyableFunction> CopyableFunctionList;

namespace NodeKafka {

class KafkaConsumer;

namespace QueueCallbacks {

struct CompareTopicPartition
{
    bool operator()(RdKafka::TopicPartition *lhs, RdKafka::TopicPartition *rhs) const
    {
      int topic_compare = lhs->topic().compare(rhs->topic());
      if (topic_compare != 0) {
        return topic_compare < 0;
      }
      return lhs->partition() < rhs->partition();
    }
};

class QueueDispatcher {
 public:
  QueueDispatcher();
  ~QueueDispatcher();
  void Dispatch(RdKafka::TopicPartition * toppar);
  void AddCallback(RdKafka::TopicPartition * toppar, const v8::Local<v8::Function>&);
  void RemoveCallback(RdKafka::TopicPartition * toppar, const v8::Local<v8::Function>&);
  bool HasCallbacks(RdKafka::TopicPartition * toppar);
  void Execute();
  void Activate();
  void Deactivate();
  void Add(RdKafka::TopicPartition *);
  void Flush();

 protected:
  std::map<RdKafka::TopicPartition*, std::vector<v8::Persistent<v8::Function, v8::CopyablePersistentTraits<v8::Function> > >, CompareTopicPartition> queue_event_toppar_callbacks;
  std::vector<RdKafka::TopicPartition*> events;

  uv_mutex_t async_lock;

 private:
  NAN_INLINE static NAUV_WORK_CB(AsyncMessage_) {
     QueueDispatcher *dispatcher =
            static_cast<QueueDispatcher*>(async->data);
     dispatcher->Flush();
  }

  uv_async_t *async;
};


class QueueEventCallbackOpaque {
  public:
    QueueEventCallbackOpaque(QueueDispatcher *_dispatcher, RdKafka::TopicPartition *_toppar);
    ~QueueEventCallbackOpaque();
    QueueDispatcher *dispatcher;
    RdKafka::TopicPartition *toppar;
};

}  // namespace QueueCallbacks

}  // namespace NodeKafka

#endif  // SRC_QUEUE_CALLBACKS_H_
