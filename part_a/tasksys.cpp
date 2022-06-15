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

    std::thread threads[num_total_tasks];
    auto fn = [] (IRunnable *runnable, int idx, int num_total_tasks) -> void {
      runnable->runTask(idx, num_total_tasks);
    };
    // we cannot run more than num_threads_ at a time
    for (int i = 0; i < num_total_tasks; i += num_threads_) {
      for (int j = i; j < i + num_threads_ && j < num_total_tasks; ++j)
          threads[j] = std::thread(fn, runnable, j, num_total_tasks);
      for (int j = i; j < i + num_threads_ && j < num_total_tasks; ++j)
          threads[j].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
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

              // unlock and run
              this->mutex_->unlock();
              job();
              this->mutex_->lock();
            }

            // terminate
            if (this->terminate_ && this->jobs_.empty()) {
              break;
            }
            this->mutex_->unlock();
          }
          this->mutex_->unlock();
          });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
	// std::cout << "close out\n" << std::flush;
	// std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  // for (auto j = 0; j < num_threads_; ++j)
  //     threads_[j].join();
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

    for (int i = 0; i < num_total_tasks; ++i) {
      // push jobs (copy by value for all closures)
      auto fn = [=] () -> void {
        runnable->runTask(i, num_total_tasks);
      };
      jobs_.push(fn);
    }
		// std::cout << "push all\n" << std::flush;
		// std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    // notify to close
    terminate_ = true;

    // main thread wait
    // NOTE: the test assume after run; all jobs are done; otherwise seg fault
    for (auto j = 0; j < num_threads_; ++j)
        threads_[j].join();
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
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
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
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
