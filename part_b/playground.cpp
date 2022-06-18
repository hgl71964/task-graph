#include <iostream>
#include <vector>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>

/*
 * This shows how to use function pointer vector to achieve book keeping
 */

using namespace std;
int main() {

  int tid = 0;
  const int num_threads_ = 4;
  std::vector<std::function<void()>*> submitted_jobs_{};
  std::unordered_map<std::function<void()>*, int> m;

  // record function pointer
  for (int i = 0; i < num_threads_; ++i) {

    // use the address of the function to hash
    std::function<void()>* fn = new std::function<void()>;
    // *fn = [i] () -> void { return i;};
    *fn = [i] () -> void { cout << i << endl;};

    submitted_jobs_.push_back(fn);
    m[fn] = tid++;
    cout << fn << " - " <<  tid << endl;
  }

  // mark delete based on the mapping
  cout << "iter" << endl;
  // std::unordered_set<std::function<void()>*> delete_list{};
  std::vector<std::function<void()>*> delete_list{};
	for (auto it = m.begin(); it != m.end(); ++it) {
    cout << it->second << endl;

    // if (it->second == 1 || it->second == 2) {
    if (it->second == 1) {
      cout << it->first << endl;
      // delete_list.insert(it->first);
      delete_list.push_back(it->first);
    }
	}
  cout << "delete list: " << delete_list.size() << endl;
  cout << "num element: " << submitted_jobs_.size() << endl;

  // iterate over vector and delete on-the-fly
  // std::vector<std::function<void()>*>::iterator itr = submitted_jobs_.begin();
  // while (itr != submitted_jobs_.end()) {
  //   cout << *itr << endl;
  //   if (delete_list.find(*itr) != delete_list.end()) {
  //     delete (*itr); // NOTE: delete to prevent mem leak
  //     itr = submitted_jobs_.erase(itr);
  //   } else {
  //     ++itr;
  //   }
  // }

  // delete by std::remove
  submitted_jobs_.erase(std::remove(submitted_jobs_.begin(), submitted_jobs_.end(), delete_list[0]),
                    submitted_jobs_.end());

  cout << "after delete list: " << submitted_jobs_.size() << endl;
  for (auto i = 0; i < submitted_jobs_.size(); ++i) {
    (*submitted_jobs_[i])();
  }
  cout << "done" << endl;
}
