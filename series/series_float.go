package series

import (
	"fmt"
	"log"
	"math"
	"strings"
)

type FloatSeries struct {
	name string
	data []float64
}

func NewFloatSeries(name string, data []float64) *FloatSeries {
	return &FloatSeries{name: name, data: data}
}

func NewConstFloatSeries(name string, f float64, length int) *FloatSeries {
	data := make([]float64, length)

	if f != 0 {
		for i := range data {
			data[i] = f
		}
	}

	return NewFloatSeries(name, data)
}

func (s *FloatSeries) Name() string { return s.name }
func (s *FloatSeries) Type() string { return "float64" }
func (s *FloatSeries) Len() int     { return len(s.data) }

func (s *FloatSeries) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Series: %s [float64]\n", s.name))

	for i, v := range s.data {
		sb.WriteString(fmt.Sprintf("%d: %.4f\n", i, v))
	}

	return sb.String()
}

func (s *FloatSeries) Get(i int) any {
	if i < 0 || i >= len(s.data) || math.IsNaN(s.data[i]) {
		return nil
	}

	return s.data[i]
}

func (s *FloatSeries) PushBack(obj any) error {
	objFloat, ok := obj.(float64)
	if obj == nil {
		s.data = append(s.data, math.NaN())
		return nil
	}
	if !ok {
		return fmt.Errorf("FloatSeries PushBack: obj not float64")
	}
	s.data = append(s.data, objFloat)
	return nil
}

func (s *FloatSeries) PushFront(obj any) error {
	objFloat, ok := obj.(float64)
	if obj == nil {
		objFloatSlice := make([]float64, 1)
		objFloatSlice[0] = math.NaN()
		s.data = append(objFloatSlice, s.data...)
		return nil
	}
	if !ok {
		return fmt.Errorf("FloatSeries PushFront: obj not float64")
	}
	objFloatSlice := make([]float64, 1)
	objFloatSlice[0] = objFloat
	s.data = append(objFloatSlice, s.data...)
	return nil
}

func (s *FloatSeries) PopBack() error {
	if len(s.data) == 0 {
		return fmt.Errorf("FloatSeries PopBack: empty data")
	}
	s.data = s.data[:s.Len()-1]
	return nil
}

func (s *FloatSeries) PopFront() error {
	if len(s.data) == 0 {
		return fmt.Errorf("FloatSeries PopFront: empty data")
	}
	s.data = s.data[1:]
	return nil
}

func (s *FloatSeries) Subset(indices []int) Series {
	newData := make([]float64, len(indices))
	for k, rowIdx := range indices {
		if rowIdx < len(s.data) {
			newData[k] = s.data[rowIdx]
		}
	}

	return NewFloatSeries(s.name, newData)
}

func (s *FloatSeries) Subseq(from int, to int) Series {
	return &FloatSeries{name: s.name, data: s.data[from:to]}
}

func (s *FloatSeries) Head() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Series: %s [float64]\n", s.name))

	for i := 0; i < min(10, s.Len()); i++ {
		sb.WriteString(fmt.Sprintf("%d: %.4f\n", i, s.data[i]))
	}

	return sb.String()
}

func (s *FloatSeries) Add(other Series) (Series, error) {
	if s.Len() != other.Len() {
		return nil, ErrSeriesLengthMismatch
	}

	otherFloat, ok := other.(*FloatSeries)
	if !ok {
		return nil, ErrOperationNotSupported
	}

	n := s.Len()

	result := make([]float64, n)
	for i := 0; i < n; i++ {
		result[i] = s.data[i] + otherFloat.data[i]
	}

	return NewFloatSeries("("+s.name+" + "+otherFloat.name+")", result), nil
}

func (s *FloatSeries) Sub(other Series) (Series, error) {
	if s.Len() != other.Len() {
		return nil, ErrSeriesLengthMismatch
	}

	otherFloat, ok := other.(*FloatSeries)
	if !ok {
		return nil, ErrOperationNotSupported
	}

	n := s.Len()

	result := make([]float64, n)
	for i := 0; i < n; i++ {
		result[i] = s.data[i] - otherFloat.data[i]
	}

	return NewFloatSeries("("+s.name+" - "+otherFloat.name+")", result), nil
}

func (s *FloatSeries) Mul(other Series) (Series, error) {
	if s.Len() != other.Len() {
		return nil, ErrSeriesLengthMismatch
	}

	otherFloat, ok := other.(*FloatSeries)
	if !ok {
		return nil, ErrOperationNotSupported
	}

	n := s.Len()

	result := make([]float64, n)
	for i := 0; i < n; i++ {
		result[i] = s.data[i] * otherFloat.data[i]
	}

	return NewFloatSeries("("+s.name+" * "+otherFloat.name+")", result), nil
}

func (s *FloatSeries) Div(other Series) (Series, error) {
	if s.Len() != other.Len() {
		return nil, ErrSeriesLengthMismatch
	}

	otherFloat, ok := other.(*FloatSeries)
	if !ok {
		return nil, ErrOperationNotSupported
	}

	n := s.Len()

	result := make([]float64, n)
	for i := 0; i < n; i++ {
		result[i] = s.data[i] / otherFloat.data[i]
	}

	return NewFloatSeries("("+s.name+" / "+otherFloat.name+")", result), nil
}

func (s *FloatSeries) Copy() Series {
	sCopy := *s
	sCopy.data = make([]float64, len(s.data))
	copy(sCopy.data, s.data)

	return &sCopy
}

func (s *FloatSeries) AddConst(val float64) *FloatSeries {
	n := s.Len()

	result := make([]float64, n)
	for i := 0; i < n; i++ {
		result[i] = s.data[i] + val
	}

	return NewFloatSeries("("+s.name+"_add_c)", result)
}

func (s *FloatSeries) SubConst(val float64) *FloatSeries {
	n := s.Len()

	result := make([]float64, n)
	for i := 0; i < n; i++ {
		result[i] = s.data[i] - val
	}

	return NewFloatSeries("("+s.name+"_sub_c)", result)
}

func (s *FloatSeries) MulConst(val float64) *FloatSeries {
	n := s.Len()

	result := make([]float64, n)
	for i := 0; i < n; i++ {
		result[i] = s.data[i] * val
	}

	return NewFloatSeries("("+s.name+"_mul_c)", result)
}

func (s *FloatSeries) DivConst(val float64) *FloatSeries {
	n := s.Len()

	result := make([]float64, n)
	for i := 0; i < n; i++ {
		result[i] = s.data[i] / val
	}

	return NewFloatSeries("("+s.name+"_div_c)", result)
}

func (s *FloatSeries) Abs() *FloatSeries {
	n := s.Len()

	result := make([]float64, n)
	for i := 0; i < n; i++ {
		result[i] = math.Abs(s.data[i])
	}

	return NewFloatSeries("("+s.name+"_abs)", result)
}

func (s *FloatSeries) Sign() *FloatSeries {
	n := s.Len()

	result := make([]float64, n)
	for i := 0; i < n; i++ {
		if s.data[i] >= 0 {
			result[i] = 1
		} else {
			result[i] = -1
		}
	}

	return NewFloatSeries("("+s.name+"_sign)", result)
}

func (s *FloatSeries) RollingMean(windowSize int) *FloatSeries {
	if windowSize <= 0 {
		log.Fatal("rolling mean window size must be greater than zero")
	}

	result := make([]float64, s.Len())

	rollingSum := 0.0
	rollingNanCount := 0

	for i := 0; i < windowSize-1 && i < s.Len(); i++ {
		val := s.data[i]
		if !math.IsNaN(val) {
			rollingSum += val
		} else {
			rollingNanCount++
		}

		result[i] = math.NaN()
	}

	if windowSize <= s.Len() {
		val := s.data[windowSize-1]
		if !math.IsNaN(val) {
			rollingSum += val
		} else {
			rollingNanCount++
		}

		if rollingNanCount == 0 {
			result[windowSize-1] = rollingSum / float64(windowSize)
		} else {
			result[windowSize-1] = math.NaN()
		}
	}

	for i := windowSize; i < s.Len(); i++ {
		newVal := s.data[i]
		oldVal := s.data[i-windowSize]

		if !math.IsNaN(newVal) {
			rollingSum += newVal
		} else {
			rollingNanCount++
		}

		if !math.IsNaN(oldVal) {
			rollingSum -= oldVal
		} else {
			rollingNanCount--
		}

		if rollingNanCount == 0 {
			result[i] = rollingSum / float64(windowSize)
		} else {
			result[i] = math.NaN()
		}
	}

	return NewFloatSeries("("+s.name+"_rolling_mean)", result)
}

func Max(series ...*FloatSeries) *FloatSeries {
	if len(series) == 0 {
		return nil
	}

	nRows := series[0].Len()
	for k := 1; k < len(series); k++ {
		if series[k].Len() != nRows {
			log.Fatal(ErrSeriesLengthMismatch)
		}
	}

	result := make([]float64, nRows)

	if len(series) == 1 {
		copy(result, series[0].data)
		return NewFloatSeries("max_result", result)
	}

	for i := 0; i < nRows; i++ {
		maxVal := series[0].data[i]
		hasNaN := math.IsNaN(maxVal)

		if !hasNaN {
			for k := 1; k < len(series); k++ {
				val := series[k].data[i]
				if math.IsNaN(val) {
					hasNaN = true
					break
				}

				if val > maxVal {
					maxVal = val
				}
			}
		}

		if hasNaN {
			result[i] = math.NaN()
		} else {
			result[i] = maxVal
		}
	}

	return NewFloatSeries("max_result", result)
}

func Min(series ...*FloatSeries) *FloatSeries {
	if len(series) == 0 {
		return nil
	}

	nRows := series[0].Len()
	for k := 1; k < len(series); k++ {
		if series[k].Len() != nRows {
			log.Fatal(ErrSeriesLengthMismatch)
		}
	}

	result := make([]float64, nRows)

	if len(series) == 1 {
		copy(result, series[0].data)
		return NewFloatSeries("min_result", result)
	}

	for i := 0; i < nRows; i++ {
		minVal := series[0].data[i]
		hasNaN := math.IsNaN(minVal)

		if !hasNaN {
			for k := 1; k < len(series); k++ {
				val := series[k].data[i]
				if math.IsNaN(val) {
					hasNaN = true
					break
				}

				if val < minVal {
					minVal = val
				}
			}
		}

		if hasNaN {
			result[i] = math.NaN()
		} else {
			result[i] = minVal
		}
	}

	return NewFloatSeries("min_result", result)
}
