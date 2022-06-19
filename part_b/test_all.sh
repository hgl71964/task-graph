#! /bin/bash

set -e # stop on any error

num_threads=4

#Declare a string array
tests=(
        # "super_light"
        # "super_super_light"
        # "mandelbrot_chunked"
        # "ping_pong_equal"
        # "ping_pong_unequal"
        # "recursive_fibonacci"
        # "math_operations_in_tight_for_loop"
        # "math_operations_in_tight_for_loop_fewer_tasks"
        # "math_operations_in_tight_for_loop_fan_in"
        # "math_operations_in_tight_for_loop_reduction_tree"
        # "spin_between_run_calls"
        "mandelbrot_chunked_async"
        "ping_pong_equal_async"
        "ping_pong_unequal_async"
        "super_light_async"
        "super_super_light_async"
        "recursive_fibonacci_async"
        # "simple_run_deps_test" # it takes a very long time
        "math_operations_in_tight_for_loop_async"
        "math_operations_in_tight_for_loop_fewer_tasks_async"
        "math_operations_in_tight_for_loop_fan_in_async"
        "math_operations_in_tight_for_loop_reduction_tree_async"
        "spin_between_run_calls_async"
        "strict_diamond_deps_async"
        "strict_graph_deps_small_async"
        "strict_graph_deps_med_async"
        "strict_graph_deps_large_async"
)

# Print array values in  lines
echo
for val1 in ${tests[*]}; do
        echo $val1":"
        echo "My impl:"
        ./runtasks -n $num_threads $val1
        echo "Ref impl:"
        ./runtasks_ref_linux -n $num_threads $val1
        echo
        echo
done
