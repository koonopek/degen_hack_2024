package main

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/cdipaolo/sentiment"
)

func main() {

	inputFile := os.Args[1]

	err := analyzeFileSentiment(inputFile)

	if err != nil {
		panic(err)
	}

	os.Exit(0)
}

func analyzeFileSentiment(filePath string) error {
	model, err := sentiment.Restore()
	if err != nil {
		panic(fmt.Sprintf("Could not restore model!\n\t%v\n", err))
	}

	result, err := os.ReadFile(filePath)

	if err != nil {
		return fmt.Errorf("Failed to read input file")
	}

	que := make(chan struct {
		string
		uint8
	})

	sentences := strings.Split(string(result), "\n")

	var wg sync.WaitGroup
	threads := 8
	partSize := len(sentences) / threads

	for i := 0; i < threads; i++ {
		start := i * partSize
		end := start + partSize
		if i == threads-1 {
			end = len(sentences)
		}

		wg.Add(1)
		go func(sentencesPart []string) {
			defer wg.Done()
			analyzeSentences(model, sentencesPart, que)
		}(sentences[start:end])
	}

	go func() {
		wg.Wait()
		close(que)
	}()

	// Collect results
	for res := range que {
		fmt.Printf("%s, %d\n", res.string, res.uint8)
	}

	return nil
}

func analyzeSentences(model sentiment.Models, sentences []string, que chan struct {
	string
	uint8
}) {
	for _, sentence := range sentences {
		score := analyzeSentence(model, sentence)
		que <- struct {
			string
			uint8
		}{sentence, score.Score}
	}
}

func analyzeSentence(model sentiment.Models, sentence string) *sentiment.Analysis {
	result := model.SentimentAnalysis(sentence, sentiment.English)
	return result
}
