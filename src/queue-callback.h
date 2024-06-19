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

class QueueDispatcher {
 public:
  QueueDispatcher();
  ~QueueDispatcher();
  void Dispatch(rd_kafka_queue_t * rkqu);
  void AddCallback(rd_kafka_queue_t * rkqu, const v8::Local<v8::Function>&);
  void RemoveCallback(rd_kafka_queue_t * rkqu, const v8::Local<v8::Function>&);
  bool HasCallbacks(rd_kafka_queue_t * rkqu);
  void Execute();
  void Activate();
  void Deactivate();
  void Add(rd_kafka_queue_t *);
  void Flush();

 protected:
  std::map<rd_kafka_queue_t*, std::vector<v8::Persistent<v8::Function, v8::CopyablePersistentTraits<v8::Function> > >> queue_event_rkqu_callbacks;
  std::vector<rd_kafka_queue_t*> events;

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
    QueueEventCallbackOpaque(QueueDispatcher *_dispatcher, rd_kafka_queue_t *_rkqu);
    ~QueueEventCallbackOpaque();
    QueueDispatcher *dispatcher;
    rd_kafka_queue_t *rkqu;
};

}  // namespace QueueCallbacks

}  // namespace NodeKafka

#endif  // SRC_QUEUE_CALLBACKS_H_
