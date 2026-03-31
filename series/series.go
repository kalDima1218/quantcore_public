package series

import "fmt"

type Series interface {
	Name() string
	Type() string
	Len() int
	String() string

	Get(index int) any
	PushBack(obj any) error
	PushFront(obj any) error
	PopBack() error
	PopFront() error
	Subset(indices []int) Series
	Subseq(from int, to int) Series
	Head() string

	Add(other Series) (Series, error)
	Sub(other Series) (Series, error)
	Mul(other Series) (Series, error)
	Div(other Series) (Series, error)

	Copy() Series
}

var ErrSeriesLengthMismatch = fmt.Errorf("series length mismatch")
var ErrOperationNotSupported = fmt.Errorf("operation not supported for this type")
var ErrSeriesTypesMismatch = fmt.Errorf("types mismatch")

func Add(series ...Series) Series {
	if len(series) == 0 {
		panic(fmt.Errorf("at least one series required"))
	}

	result := series[0]
	for _, s := range series[1:] {
		if s.Type() != series[0].Type() {
			panic(ErrSeriesTypesMismatch)
		}

		if s.Len() != series[0].Len() {
			panic(ErrSeriesLengthMismatch)
		}

		addResult, err := result.Add(s)
		if err != nil {
			panic(err)
		}

		result = addResult
	}

	return result
}

func Sub(series ...Series) Series {
	if len(series) == 0 {
		panic(fmt.Errorf("at least one series required"))
	}

	result := series[0]
	for _, s := range series[1:] {
		if s.Type() != series[0].Type() {
			panic(ErrSeriesTypesMismatch)
		}

		if s.Len() != series[0].Len() {
			panic(ErrSeriesLengthMismatch)
		}

		subResult, err := result.Sub(s)
		if err != nil {
			panic(err)
		}

		result = subResult
	}

	return result
}

func Mul(series ...Series) Series {
	if len(series) == 0 {
		panic(fmt.Errorf("at least one series required"))
	}

	result := series[0]
	for _, s := range series[1:] {
		if s.Type() != series[0].Type() {
			panic(ErrSeriesTypesMismatch)
		}

		if s.Len() != series[0].Len() {
			panic(ErrSeriesLengthMismatch)
		}

		mulResult, err := result.Mul(s)
		if err != nil {
			panic(err)
		}

		result = mulResult
	}

	return result
}

func Div(series ...Series) Series {
	if len(series) == 0 {
		panic(fmt.Errorf("at least one series required"))
	}

	result := series[0]
	for _, s := range series[1:] {
		if s.Type() != series[0].Type() {
			panic(ErrSeriesTypesMismatch)
		}

		if s.Len() != series[0].Len() {
			panic(ErrSeriesLengthMismatch)
		}

		divResult, err := result.Div(s)
		if err != nil {
			panic(err)
		}

		result = divResult
	}

	return result
}
