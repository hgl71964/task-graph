#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    assert(num_threads > 1);
    num_threads_ = num_threads;
    terminate_ = false;
    tid_ = 0;
    mutex_ = new std::mutex();
    threads_ = new std::thread[num_threads];
    cv_ = new std::condition_variable();

    // start thread pool
    for (auto i = 0; i < num_threads_ - 1; ++i) {
      threads_[i] = std::thread([this] {
          // get lock first
          std::unique_lock<std::mutex> lk(*(this->mutex_));

          // thread pool loop
          while (true) {
            if (!this->jobs_.empty()) {
              auto job = this->jobs_.front();
              this->jobs_.pop();

              // run
              lk.unlock();
              job();
              lk.lock();

              // update TODO how to know all job related to a task has all finishes
              continue;
            }

            this->cv_->wait(lk);
            if (this->terminate_) {
              break;
            }
          }

          // exit release lock
          lk.unlock();
          });
    }

    // background thread to bookkeeping
    threads_[num_threads_-1] = std::thread([this] {
          // just busy waiting for now
          while (true) {
            this->mutex_->lock();

            std::vector<std::tuple<TaskID, IRunnable*, int>> dispatchable_list{};
            for (const auto &record: records_) {
              // check if job dispatchable
              auto task_id = std::get<0>(record);
              auto deps = this->deps_books_[task_id];

              bool dispatchable = true;
              for (const auto &id: deps) {
                if (this->completed_task_ids_.find(id) == this->completed_task_ids_.end()) {
                  dispatchable = false;
                  break;
                }
              }

              if (dispatchable) {
                dispatchable_list.push_back(record);
              }
            }

            // build jobs and dispatch
            for (const auto &record: dispatchable_list) {
              // delete from submitted data structure
              this->records_.erase(std::remove(records_.begin(), records_.end(),
                                  record), records_.end());

              // build & dispatch
              IRunnable* runnable = std::get<1>(record);
              int num_total_tasks = std::get<2>(record);
              for (int i = 0; i < this->num_threads_ - 1; ++i) {
                // NOTE: task granularity: N threads per task
                // NOTE: must capture by copy
                // otherwise, num_total_tasks will change across Calls!!!!!
                // TODO how to make sure N-threads have all finished this task
                jobs_.push([=] () -> void {
                  for (auto j = i; j < num_total_tasks; j += num_threads_-1) {
                    runnable->runTask(j, num_total_tasks);
                  }
                });
              }
              this->cv_->notify_one();
            }

            // XXX should wake one up anyways?
            // in case all sleep but still jobs to run
            this->cv_->notify_one();

            this->mutex_->unlock();

            // exit
            if (this->terminate_) {
              break;
            }
          }

        });
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    terminate_ = true;

    // XXX IF a thread hasn't gone to sleep, the above call will not wake it up!!
    // HOW to know if all threads go to sleep
    // must linger a bit
    for (auto i = 0; i < 3; i++) {
      cv_->notify_all();
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    for (auto j = 0; j < num_threads_ - 1; ++j)
      threads_[j].join();

    // shut down background thread
    threads_[num_threads_-1].join();

    delete[] threads_;
    delete mutex_;
    delete cv_;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    //
    // CS149 students will implement this method in Part B.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    // dispatch
    auto id = tid_++;

    mutex_->lock();

    assert(deps_books_.find(id) == deps_books_.end());
    deps_books_[id] = deps;

    // build record; push to pending list
    records_.push_back(std::make_tuple(id, runnable, num_total_tasks));
    mutex_->unlock();

    // return immediately
    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // CS149 students will modify the implementation of this method in Part B.
    //
    mutex_->lock();
    while (!jobs_.empty() || !records_.empty()) {
    mutex_->unlock();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    mutex_->lock();
    }
    mutex_->unlock();
}
