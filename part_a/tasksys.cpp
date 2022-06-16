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
    assert(false);
    return 0;
}

void TaskSystemSerial::sync() {
    assert(false);
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
    //
    // CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    assert(num_threads > 0);
    this->num_threads_ = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {

    //
    // CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // this shows sequential work assignment
    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    // THIS DOESN'T CONSIDER CACHE EFFICIENCY
    // std::thread threads[num_threads_-1];
    // int work_per_thread = num_total_tasks / num_threads_;

    // // launch
    // for (auto j = 1; j < num_threads_; ++j)
    //     threads[j-1] = std::thread([&, j]{
    //     for (auto i = j * work_per_thread;
    //           i < std::min((j * work_per_thread + (j+1) * work_per_thread), num_total_tasks); ++i) {
    //           runnable->runTask(i, num_total_tasks);
    //       }
    // });

    // // main thread
    // for (auto i = 0; i <  work_per_thread; ++i) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    // //stop
    // for (auto j = 0; j < num_threads_-1; ++j)
    //     threads[j].join();

    // bulk launch with data locality
    std::thread threads[num_threads_ - 1];

    // launch
    for (auto j = 0; j < num_threads_ - 1; ++j)
        threads[j] = std::thread([&, j]{
        for (auto i = j; i < num_total_tasks; i += num_threads_) {
              runnable->runTask(i, num_total_tasks);
          }
    });

    // main thread
    for (auto i = num_threads_ - 1; i <  num_total_tasks; i += num_threads_) {
        runnable->runTask(i, num_total_tasks);
    }

    //stop
    for (auto j = 0; j < num_threads_-1; ++j)
        threads[j].join();
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    assert(false);
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    assert(false);
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
    //
    // CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    assert(num_threads > 0);
    num_threads_ = num_threads;
    terminate_ = false;
    last_one_done_ = false;
    mutex_ = new std::mutex();
    threads_ = new std::thread[num_threads];

    // start thread pool
    for (auto i = 0; i < num_threads_; ++i) {
      // need to capture this by reference
      threads_[i] = std::thread([this] {
          while (true) {
            this->mutex_->lock();

            // run jobs
            if (!this->jobs_.empty()) {
              auto job = this->jobs_.front();
              this->jobs_.pop();

              bool last_one = false;
              if (this->jobs_.empty()) {
                last_one = true;
              }

              // unlock and run
              this->mutex_->unlock();
              job();
              this->mutex_->lock();

              // mark the last job has finished
              if (last_one) {
                this->last_one_done_ = true;
              }
            }

            this->mutex_->unlock();

            // terminate
            if (this->terminate_) {
              break;
            }
          }
          });
    }

    // chan_cv_ = new std::condition_variable();
    // chan_mutex_ = new std::mutex();
    // for (auto i = 0; i < num_threads_; ++i) {
    //   threads_[i] = std::thread([this] {
    //       while (true) {
    //         this->mutex_->lock();

    //         if (this->jobs_.empty()) {
    //           this->chan_cv_->notify_all();
    //         } else {
    //           auto job = this->jobs_.front();
    //           this->jobs_.pop();

    //           // unlock and run
    //           this->mutex_->unlock();
    //           job();
    //           this->mutex_->lock();
    //         }


    //         // terminate
    //         if (this->terminate_) {
    //           break;
    //         }
    //         this->mutex_->unlock();
    //       }
    //       this->mutex_->unlock();
    //       });
    // }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
	// std::cout << "close out\n" << std::flush;
	// std::this_thread::sleep_for(std::chrono::milliseconds(5000));

  // notify to close
  terminate_ = true;
  for (auto j = 0; j < num_threads_; ++j)
      threads_[j].join();
  delete[] threads_;
  delete mutex_;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    //
    // CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    // std::cout << "start assignment\n" << std::flush;
		// std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    // lock to assign jobs
    this->mutex_->lock();
    last_one_done_ = false;
    for (int i = 0; i < num_total_tasks; ++i) {
      // push jobs (copy by value for all closures)
      auto fn = [=] () -> void {
        runnable->runTask(i, num_total_tasks);
      };
      jobs_.push(fn);
    }
    this->mutex_->unlock();

    // while (!jobs_.empty()) {
    //   this->mutex_->unlock();
    //   std::this_thread::sleep_for(std::chrono::milliseconds(100));
    //   this->mutex_->lock();
    // }
    // this->mutex_->unlock();

    // MUST ensure all jobs done for this run
    while (!last_one_done_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // std::unique_lock<std::mutex> chan_lk(*chan_mutex_);
    // while (!jobs_.empty()) {
    //   this->mutex_->unlock();
    //   chan_cv_->wait_for(chan_lk, std::chrono::milliseconds(1000));
    //   this->mutex_->lock();
    // }
    // chan_lk.unlock();
    // this->mutex_->unlock();
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
		assert(false);
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
		assert(false);
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

    assert(num_threads > 0);
    num_threads_ = num_threads;
    terminate_ = false;
    mutex_ = new std::mutex();
    threads_ = new std::thread[num_threads];
    condition_variable_ = new std::condition_variable();

    // dedicated for channel sync for the `run` thread
    chan_cv_ = new std::condition_variable();
    chan_mutex_ = new std::mutex();

    // start thread pool
    // for (auto i = 0; i < num_threads_; ++i) {
    //   threads_[i] = std::thread([this] {
    //       // get lock first
    //       std::unique_lock<std::mutex> lk(*(this->mutex_));

    //       // thread pool loop
    //       while (true) {
    //         if (this->jobs_.empty()) {
    //           this->chan_cv_->notify_all();
    //           this->condition_variable_->wait(lk);
    //         } else {
    //           auto job = this->jobs_.front();
    //           this->jobs_.pop();
    //           lk.unlock();
    //           job();
    //           lk.lock();
    //         }

    //         // terminate
    //         if (this->terminate_) {
    //           break;
    //         }
    //       }

    //       // exit release lock
    //       lk.unlock();
    //       });
    // }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    // terminate_ = true;
    // condition_variable_->notify_all();
    // for (auto j = 0; j < num_threads_; ++j)
    //     threads_[j].join();

    // delete[] threads_;
    // delete mutex_;
    // delete condition_variable_;
    // delete chan_mutex_;
    // delete chan_cv_;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    //
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    // std::unique_lock<std::mutex> lk(*mutex_);
    // std::unique_lock<std::mutex> chan_lk(*chan_mutex_);
    // for (int i = 0; i < num_total_tasks; ++i) {
    //   auto fn = [=] () -> void {
    //     runnable->runTask(i, num_total_tasks);
    //   };
    //   jobs_.push(fn);
    // }

    // // if all jobs in this run done, return
    // while (!jobs_.empty()) {
    //   condition_variable_->notify_all();
    //   lk.unlock();

    //   // sleep on channel to wait
    //   chan_cv_->wait_for(chan_lk, std::chrono::milliseconds(1000));
    //   lk.lock();
    // }
    // chan_lk.unlock();
    // lk.unlock();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    assert(false);

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    assert(false);
    return;
}
