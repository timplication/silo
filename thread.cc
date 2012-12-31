#include "macros.h"
#include "thread.h"

#include <iostream>

using namespace std;

void
ndb_thread::start()
{
  ALWAYS_ASSERT(pthread_create(&p, NULL, pthread_bootstrap, this) == 0);
}

void
ndb_thread::join()
{
  ALWAYS_ASSERT(pthread_join(p, NULL) == 0);
}

void
ndb_thread::run()
{
  assert(body);
  body();
}

bool
ndb_thread::register_completion_callback(callback_t callback)
{
  completion_callbacks().push_back(callback);
  return true;
}

vector<ndb_thread::callback_t>&
ndb_thread::completion_callbacks()
{
  static vector<callback_t> *callbacks = NULL;
  if (!callbacks)
    callbacks = new vector<callback_t>;
  return *callbacks;
}

void
ndb_thread::on_complete()
{
  for (vector<callback_t>::iterator it = completion_callbacks().begin();
       it != completion_callbacks().end(); ++it)
    (*it)(this);
}

void *
ndb_thread::pthread_bootstrap(void *p)
{
  ndb_thread *self = (ndb_thread *) p;
  try {
    self->run();
  } catch (...) {
    self->on_complete();
    throw;
  }
  self->on_complete();
  return NULL;
}