package cos418_hw1_1

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.

func sumWorker(nums chan int, out chan int) {
	sum := 0
	for num := range nums {
		sum += num
	}
	out <- sum
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	file, err := os.Open(fileName)
	checkError(err)
	int_array, err := readInts(file)
	checkError(err)
	chan_size := len(int_array) / num
	out := make(chan int)
	tracker := 0

	// divide the work among the routines
	for i := 0; i < num; i++ {
		nums := make(chan int, chan_size)
		slice := int_array[tracker : tracker+chan_size]
		go sumWorker(nums, out)
		// send the numbers
		for k := 0; k < len(slice); k++ {
			nums <- slice[k]
		}
		close(nums)
		tracker += chan_size
	}

	totalSum := 0
	// get the final sum
	for i := 0; i < num; i++ {
		sum := <-out
		totalSum += sum
	}
	return totalSum
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
