package cos418_hw1_1

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// Find the top K most common words in a text document.
//
//	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
//
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuation and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	data, err := os.ReadFile(path)
	checkError(err)
	word_array := []string(strings.Fields(string(data)))
	hashmap = make(map[string]WordCount)
	str := regexp.MustCompile(`[^0-9a-zA-Z]+`)
	for _, word := range word_array {
		//filter each word
		lowerword := strings.ToLower(word)
		nonalphanumeric := str.ReplaceAllLiteralString(lowerword, " ")
		filterword := strings.Replace(nonalphanumeric, " ", "", 50)
		//if the word meets threshold
		if len(filterword) >= charThreshold {
			word, in := hashmap[filterword]
			//if word is NOT in map
			if !in {
				hashmap[filterword] = WordCount{filterword, 1}
			} else {
				//if word IS in map
				current_count := word.Count
				new_count := current_count + 1
				hashmap[filterword] = WordCount{filterword, new_count}
			}
		}
	}
	//put all WordCounts from hashmap into array of WordCounts
	wordCounts := make([]WordCount, len(hashmap))
	for _, value := range hashmap {
		wordCounts = append(wordCounts, value)
	}
	sortWordCounts(wordCounts)
	return wordCounts[:numWords]
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

var hashmap map[string]WordCount

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
