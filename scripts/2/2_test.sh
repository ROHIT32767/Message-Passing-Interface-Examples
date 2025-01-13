#!/bin/bash

if ls ../../2/*.py &>/dev/null; then
    executable="python3 ../../2/*.py"
elif ls ../../2/*.cpp &>/dev/null; then
    mpic++ -o 2 ../../2/*.cpp 
    executable="./2"
elif ls ../../2/*.c &>/dev/null; then
    mpicc -o 2 ../../2/*.c
    executable="./2"
else
    echo "No Python, C, or C++ file found in ../../2/"
    exit 1
fi

normalize_spaces() {
    sed -e 's/[[:space:]]\+/ /g' -e 's/[[:space:]]*$//' -e '/^$/d' "$1" > "$2"
}

mkdir -p results

total_marks=0

num_test_cases=$(ls testcases/*.in | wc -l)

for i in $(seq 1 $num_test_cases); do
    test_file="testcases/${i}.in"
    expected_output="testcases/${i}.out"

    all_passed=true

    for np in {1..12}; do
        if [[ "$executable" == python* ]]; then
            mpiexec -np $np --use-hwthread-cpus --oversubscribe $executable < $test_file > results/2_${np}_${i}.txt
        else
            mpiexec -np $np --use-hwthread-cpus --oversubscribe $executable < $test_file > results/2_${np}_${i}.txt
        fi

        normalize_spaces results/2_${np}_${i}.txt results/2_${np}_${i}_normalized.txt
        normalize_spaces $expected_output results/expected_${i}_normalized.txt

        if ! diff -q results/2_${np}_${i}_normalized.txt results/expected_${i}_normalized.txt > /dev/null; then
            all_passed=false
            break
        fi
    done


    if [ "$all_passed" = true ]; then
        printf "Test case $i: \e[32mPASSED\e[0m\n"
        marks=$(grep "^${i} " marks.txt | cut -d ' ' -f 2)
        total_marks=$((total_marks + marks))

    else
        printf "Test case $i: \e[31mFAILED\e[0m\n"
    fi
done

echo -e "Final Score: $total_marks/100"

rm -rf 2 results/
