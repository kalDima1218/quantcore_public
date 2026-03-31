package series

import (
	"fmt"
	"strings"
	"time"
)

type DurationSeries struct {
	name  string
	data  []time.Duration
	valid []bool
}

func NewDurationSeries(name string, data []time.Duration) *DurationSeries {
	valid := make([]bool, len(data))
	for i := range valid {
		valid[i] = true
	}

	return &DurationSeries{name: name, data: data, valid: valid}
}

func NewConstDurationSeries(name string, d time.Duration, length int) *DurationSeries {
	data := make([]time.Duration, length)

	if d != 0 {
		for i := range data {
			data[i] = d
		}
	}

	return NewDurationSeries(name, data)
}

func (s *DurationSeries) Name() string { return s.name }
func (s *DurationSeries) Type() string { return "duration" }
func (s *DurationSeries) Len() int     { return len(s.data) }

func (s *DurationSeries) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Series: %s [duration]\n", s.name))

	for i, v := range s.data {
		sb.WriteString(fmt.Sprintf("%d: %s\n", i, v.String()))
	}

	return sb.String()
}

func (s *DurationSeries) Get(i int) interface{} {
	if i < 0 || i >= len(s.data) || !s.valid[i] {
		return nil
	}

	return s.data[i]
}

func (s *DurationSeries) PushBack(obj any) error {
	objDuration, ok := obj.(time.Duration)
	if obj == nil {
		s.data = append(s.data, time.Duration(0))
		s.valid = append(s.valid, false)
		return nil
	}
	if !ok {
		return fmt.Errorf("DurationSeries PushBack: obj not time.Duration")
	}
	s.data = append(s.data, objDuration)
	s.valid = append(s.valid, true)
	return nil
}

func (s *DurationSeries) PushFront(obj any) error {
	objDuration, ok := obj.(time.Duration)
	if obj == nil {
		objDurationSlice := make([]time.Duration, 1)
		objDurationSlice[0] = time.Duration(0)
		s.data = append(objDurationSlice, s.data...)
		validSlice := make([]bool, 1)
		validSlice[0] = false
		s.valid = append(validSlice, s.valid...)
		return nil
	}
	if !ok {
		return fmt.Errorf("DurationSeries PushFront: obj not time.Duration")
	}
	objDurationSlice := make([]time.Duration, 1)
	objDurationSlice[0] = objDuration
	s.data = append(objDurationSlice, s.data...)
	validSlice := make([]bool, 1)
	validSlice[0] = true
	s.valid = append(validSlice, s.valid...)
	return nil
}

func (s *DurationSeries) PopBack() error {
	if len(s.data) == 0 {
		return fmt.Errorf("DurationSeries PopBack: empty data")
	}
	s.valid = s.valid[:s.Len()-1]
	s.data = s.data[:s.Len()-1]
	return nil
}

func (s *DurationSeries) PopFront() error {
	if len(s.data) == 0 {
		return fmt.Errorf("DurationSeries PopFront: empty data")
	}
	s.valid = s.valid[:s.Len()-1]
	s.data = s.data[1:]
	return nil
}

func (s *DurationSeries) Subset(indices []int) Series {
	newData := make([]time.Duration, len(indices))
	newValid := make([]bool, len(indices))
	for k, rowIdx := range indices {
		if rowIdx < len(s.data) {
			newData[k] = s.data[rowIdx]
			newValid[k] = s.valid[rowIdx]
		}
	}

	return &DurationSeries{name: s.name, data: newData, valid: newValid}
}

func (s *DurationSeries) Subseq(from int, to int) Series {
	return &DurationSeries{name: s.name, data: s.data[from:to], valid: s.valid[from:to]}
}

func (s *DurationSeries) Head() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Series: %s [duration]\n", s.name))

	for i := 0; i < min(10, s.Len()); i++ {
		sb.WriteString(fmt.Sprintf("%d: %s\n", i, s.data[i]))
	}

	return sb.String()
}

func (s *DurationSeries) Add(other Series) (Series, error) {
	if s.Len() != other.Len() {
		return nil, ErrSeriesLengthMismatch
	}

	o, ok := other.(*DurationSeries)
	if !ok {
		return nil, ErrSeriesTypesMismatch
	}

	n := s.Len()

	res := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		res[i] = s.data[i] + o.data[i]
	}

	return NewDurationSeries("("+s.name+" + "+o.name+")", res), nil
}

func (s *DurationSeries) Sub(other Series) (Series, error) {
	if s.Len() != other.Len() {
		return nil, ErrSeriesLengthMismatch
	}

	otherDuration, ok := other.(*DurationSeries)
	if !ok {
		return nil, ErrSeriesTypesMismatch
	}

	n := s.Len()

	res := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		res[i] = s.data[i] - otherDuration.data[i]
	}

	return NewDurationSeries("("+s.name+" - "+otherDuration.name+")", res), nil
}

func (s *DurationSeries) Mul(other Series) (Series, error) {
	if s.Len() != other.Len() {
		return nil, ErrSeriesLengthMismatch
	}

	otherFloat, ok := other.(*FloatSeries)
	if !ok {
		return nil, ErrSeriesTypesMismatch
	}

	n := s.Len()

	res := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		val := float64(s.data[i]) * otherFloat.data[i]
		res[i] = time.Duration(val)
	}

	return NewDurationSeries("("+s.name+" * "+otherFloat.name+")", res), nil
}

func (s *DurationSeries) Div(other Series) (Series, error) {
	if s.Len() != other.Len() {
		return nil, ErrSeriesLengthMismatch
	}

	otherFloat, ok := other.(*FloatSeries)
	if !ok {
		return nil, ErrSeriesTypesMismatch
	}

	n := s.Len()

	res := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		val := float64(s.data[i]) / otherFloat.data[i]
		res[i] = time.Duration(val)
	}

	return NewDurationSeries("("+s.name+" / "+otherFloat.name+")", res), nil
}

func (s *DurationSeries) Copy() Series {
	sCopy := *s
	sCopy.data = make([]time.Duration, len(s.data))
	copy(sCopy.data, s.data)
	sCopy.valid = make([]bool, len(s.valid))
	copy(sCopy.valid, s.valid)

	return &sCopy
}

func (s *DurationSeries) GetHours() *FloatSeries {
	hours := make([]float64, len(s.data))
	for i, t := range s.data {
		hours[i] = float64(t.Hours())
	}

	return NewFloatSeries(s.name+"_hours", hours)
}

func (s *DurationSeries) GetMinutes() *FloatSeries {
	minutes := make([]float64, len(s.data))
	for i, t := range s.data {
		minutes[i] = float64(t.Minutes())
	}

	return NewFloatSeries(s.name+"_minutes", minutes)
}

func (s *DurationSeries) GetSeconds() *FloatSeries {
	seconds := make([]float64, len(s.data))
	for i, t := range s.data {
		seconds[i] = float64(t.Seconds())
	}

	return NewFloatSeries(s.name+"_seconds", seconds)
}

func (s *DurationSeries) GetDays() *FloatSeries {
	days := make([]float64, len(s.data))
	for i, t := range s.data {
		days[i] = float64(t.Hours()) / 24
	}

	return NewFloatSeries(s.name+"_days", days)
}
