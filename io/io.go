package io

import (
	"QuantCore/dataframe"
	"QuantCore/series"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
)

func ReadCSV(filename string) (*dataframe.DataFrame, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Error closing file: %v", err)
		}
	}()

	reader := csv.NewReader(file)
	reader.Comma = ','

	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return dataframe.NewDataFrame(), nil
	}

	headers := records[0]
	dataRows := records[1:]

	df := dataframe.NewDataFrame()
	numRows := len(dataRows)

	for colIdx, colName := range headers {
		rawColData := make([]string, numRows)

		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if colIdx < len(dataRows[rowIdx]) {
				rawColData[rowIdx] = dataRows[rowIdx][colIdx]
			} else {
				rawColData[rowIdx] = ""
			}
		}

		s := inferTypeAndCreateSeries(colName, rawColData)
		if err := df.AddColumn(s); err != nil {
			return nil, fmt.Errorf("failed to add column %s: %w", colName, err)
		}
	}

	return df, nil
}

func inferTypeAndCreateSeries(name string, rawData []string) series.Series {
	floatData, isFloat := tryParseFloat(rawData)
	if isFloat {
		return series.NewFloatSeries(name, floatData)
	}

	return series.NewStringSeries(name, rawData)
}

func tryParseFloat(rawData []string) ([]float64, bool) {
	floatData := make([]float64, len(rawData))

	for i, val := range rawData {
		if val == "" {
			floatData[i] = math.NaN()

			continue
		}

		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, false
		}

		floatData[i] = f
	}

	return floatData, true
}
