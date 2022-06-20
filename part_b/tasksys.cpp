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
    m2_ = new std::mutex();
    m3_ = new std::mutex();
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

              // release lock & parse job & run
              lk.unlock();
              TaskID task_id = std::get<0>(job);
              IRunnable* runnable = std::get<1>(job);
              int start_index = std::get<2>(job);
              int num_total_tasks = std::get<3>(job);
              int num_worker = std::get<4>(job);
              for (auto j = start_index; j < num_total_tasks; j += num_worker) {
                runnable->runTask(j, num_total_tasks);
              }
              lk.lock();

              // sync jobs status, mark completed; each task is run by `num_worker`
              worker_cnt_[task_id]++;
              if (worker_cnt_[task_id] == num_worker) {
                // printf("task: %d; ", task_id);
                this->completed_task_ids_.insert(task_id);
                this->worker_cnt_.erase(task_id);
                // this->deps_books_.erase(task_id);
                // TODO clean-up other bookkeeping data structure?
              }
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
            this->m2_->lock();

            // check if job dispatchable
            std::vector<std::tuple<TaskID, IRunnable*, int>> dispatchable_list{};
            for (const auto &record: records_) {
              auto task_id = std::get<0>(record);
              auto deps = this->deps_books_[task_id];

              this->mutex_->lock();
              bool dispatchable = true;
              for (const auto &id: deps) {
                if (this->completed_task_ids_.find(id) == this->completed_task_ids_.end()) {
                  dispatchable = false;
                  break;
                }
              }
              this->mutex_->unlock();

              if (dispatchable) {
                dispatchable_list.push_back(record);
              }
            }
            // delete from submitted data structure
            for (const auto &record: dispatchable_list) {
              this->records_.erase(std::remove(records_.begin(), records_.end(),
                                  record), records_.end());
            }
            this->m2_->unlock();

            // build jobs and dispatch
            if (!dispatchable_list.empty()) {
              for (const auto &record: dispatchable_list) {
                // build & dispatch
                TaskID task_id = std::get<0>(record);
                IRunnable* runnable = std::get<1>(record);
                int num_total_tasks = std::get<2>(record);

                this->mutex_->lock();
                for (int i = 0; i < this->num_threads_ - 1; ++i) {

                  // NOTE: task granularity: N threads per task
                  // TODO how to make sure N-threads have all finished this task
                  jobs_.push(std::make_tuple(task_id, runnable, i,
                          num_total_tasks, this->num_threads_-1));
                }
                this->mutex_->unlock();

              }
              // XXX should wake up anyways? in case all sleep but still jobs to run
              this->cv_->notify_all();
            }


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
    // printf("destroy...\n");
    sync(); // ensure all jobs finish
    terminate_ = true;

    // XXX IF a thread hasn't gone to sleep, the notyfy call will not wake it up!!
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

    m2_->lock();

    // build record; push to pending list
    assert(deps_books_.find(id) == deps_books_.end());
    deps_books_[id] = deps;
    records_.push_back(std::make_tuple(id, runnable, num_total_tasks));

    m2_->unlock();

    // return immediately
    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // CS149 students will modify the implementation of this method in Part B.
    //
    int cnt = 0;
    m2_->lock();
    mutex_->lock();
    while (!jobs_.empty() || !records_.empty() || !worker_cnt_.empty()) {
      mutex_->unlock();
      m2_->unlock();

      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      m2_->lock();
      mutex_->lock();

      // FIXME
      cnt++;
      if (cnt > 10) {
        printf("jobs -> %ld - %ld\n", jobs_.size(), records_.size());
        // for (auto it = worker_cnt_.begin(); it != worker_cnt_.end(); ) {
        //   auto task_id = it->first;
        //   auto worker_cnt = it->second;
        //   printf("task %d - %d - %d\n", task_id, worker_cnt, num_threads_-1);
        //   ++it;
        // }
      }
    }
    mutex_->unlock();
    m2_->unlock();
}
