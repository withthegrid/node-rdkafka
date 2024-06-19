/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#include "src/queue-callback.h"



namespace NodeKafka {
namespace QueueCallbacks {

QueueDispatcher::QueueDispatcher() {
  async = NULL;
  uv_mutex_init(&async_lock);
}

QueueDispatcher::~QueueDispatcher() {
  if (queue_event_rkqu_callbacks.size() < 1) return;

  std::map<rd_kafka_queue_t*, std::vector<v8::Persistent<v8::Function, v8::CopyablePersistentTraits<v8::Function> > >>::iterator it;
  for (it = queue_event_rkqu_callbacks.begin(); it != queue_event_rkqu_callbacks.end(); it++) {
    for (size_t i=0; i < it->second.size(); i++) {
      it->second[i].Reset();
    }
  }

  uv_mutex_destroy(&async_lock);
}

// Only run this if we aren't already listening
void QueueDispatcher::Activate() {
  if (!async) {
    async = new uv_async_t;
    uv_async_init(uv_default_loop(), async, AsyncMessage_);

    async->data = this;
  }
}

// Should be able to run this regardless of whether it is active or not
void QueueDispatcher::Deactivate() {
  if (async) {
    uv_close(reinterpret_cast<uv_handle_t*>(async), NULL);
    async = NULL;
  }
}

bool QueueDispatcher::HasCallbacks(rd_kafka_queue_t * rkqu) {
  std::map<rd_kafka_queue_t*, std::vector<v8::Persistent<v8::Function, v8::CopyablePersistentTraits<v8::Function> > >>::iterator it =
          queue_event_rkqu_callbacks.find(rkqu);
  if (it != queue_event_rkqu_callbacks.end()) {
    return it->second.size() > 0;
  }
  return false;
}

void QueueDispatcher::Execute() {
  if (async) {
    uv_async_send(async);
  }
}

void QueueDispatcher::Dispatch(rd_kafka_queue_t * rkqu) {
  std::map<rd_kafka_queue_t*, std::vector<v8::Persistent<v8::Function, v8::CopyablePersistentTraits<v8::Function> > >>::iterator it =
          queue_event_rkqu_callbacks.find(rkqu);

  if (it != queue_event_rkqu_callbacks.end()) {
    for (size_t i=0; i < it->second.size(); i++) {
      v8::Local<v8::Function> f = Nan::New<v8::Function>(it->second[i]);
      Nan::Callback cb(f);
      cb.Call(0, 0);
    }
  }
}

void QueueDispatcher::AddCallback(rd_kafka_queue_t * rkqu, const v8::Local<v8::Function> &cb) {
  Nan::Persistent<v8::Function,
                  Nan::CopyablePersistentTraits<v8::Function> > value(cb);
  // PersistentCopyableFunction value(func);
  queue_event_rkqu_callbacks[rkqu].push_back(value);
}

void QueueDispatcher::RemoveCallback(rd_kafka_queue_t * rkqu, const v8::Local<v8::Function> &cb) {
  std::map<rd_kafka_queue_t*, std::vector<v8::Persistent<v8::Function, v8::CopyablePersistentTraits<v8::Function> > >>::iterator it =
          queue_event_rkqu_callbacks.find(rkqu);

  if (it != queue_event_rkqu_callbacks.end()) {
    for (size_t i=0; i < it->second.size(); i++) {
      if (it->second[i] == cb) {
        it->second[i].Reset();
        it->second.erase(it->second.begin() + i);
        break;
      }
    }
    if (it->second.size() == 0) {
      queue_event_rkqu_callbacks.erase(rkqu);
    }
  }
}

void QueueDispatcher::Add(rd_kafka_queue_t * e) {
  scoped_mutex_lock lock(async_lock);
  events.push_back(e);
}

void QueueDispatcher::Flush() {
  Nan::HandleScope scope;
  // Iterate through each of the currently stored events
  // generate a callback object for each, setting to the members
  // then
  if (events.size() < 1) return;

  std::vector<rd_kafka_queue_t*> _events;
  {
    scoped_mutex_lock lock(async_lock);
    events.swap(_events);
  }

  for (size_t i=0; i < _events.size(); i++) {
    Dispatch(_events[i]);
  }
}

QueueEventCallbackOpaque::QueueEventCallbackOpaque(QueueDispatcher *_dispatcher, rd_kafka_queue_t *_rkqu) {
  dispatcher = _dispatcher;
  rkqu = _rkqu;
}

QueueEventCallbackOpaque::~QueueEventCallbackOpaque() {}



}  // end namespace QueueCallbacks

}  // End namespace NodeKafka
