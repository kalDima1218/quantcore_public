package series

import (
	"fmt"
	"strings"
	"time"
)

type TimeSeries struct {
	name  string
	data  []time.Time
	valid []bool
}

func NewTimeSeries(name string, data []time.Time) *TimeSeries {
	valid := make([]bool, len(data))
	for i := range valid {
		valid[i] = true
	}

	return &TimeSeries{name: name, data: data, valid: valid}
}

func MakeTimeSeries(s *StringSeries, layout string) (*TimeSeries, error) {
	timeData := make([]time.Time, s.Len())

	for i, val := range s.data {
		t, err := time.Parse(layout, val)
		if err != nil {
			return nil, fmt.Errorf("failed to parse time at index %d ('%s'): %w", i, val, err)
		}

		timeData[i] = t
	}

	return NewTimeSeries(s.name, timeData), nil
}

func NewConstTimeSeries(name string, t time.Time, length int) *TimeSeries {
	data := make([]time.Time, length)
	for i := range data {
		data[i] = t
	}

	return NewTimeSeries(name, data)
}

func (s *TimeSeries) Name() string { return s.name }
func (s *TimeSeries) Type() string { return "time" }
func (s *TimeSeries) Len() int     { return len(s.data) }

func (s *TimeSeries) String() string {
	var sb strings.Builder

	_, err := sb.WriteString(fmt.Sprintf("Series: %s [time]\n", s.name))
	if err != nil {
		panic(err)
	}

	for i, v := range s.data {
		sb.WriteString(fmt.Sprintf("%d: %s\n", i, v.Format("2006-01-02 15:04:05")))
	}

	return sb.String()
}

func (s *TimeSeries) Get(i int) any {
	if i < 0 || i >= len(s.data) || !s.valid[i] {
		return nil
	}

	return s.data[i]
}

func (s *TimeSeries) PushBack(obj any) error {
	objTime, ok := obj.(time.Time)
	if obj == nil {
		s.data = append(s.data, time.Time{})
		s.valid = append(s.valid, false)
		return nil
	}
	if !ok {
		return fmt.Errorf("TimeSeries PushBack: obj not time.Time")
	}
	s.data = append(s.data, objTime)
	s.valid = append(s.valid, true)
	return nil
}

func (s *TimeSeries) PushFront(obj any) error {
	objTime, ok := obj.(time.Time)
	if obj == nil {
		objTimeSlice := make([]time.Time, 1)
		objTimeSlice[0] = time.Time{}
		s.data = append(objTimeSlice, s.data...)
		validSlice := make([]bool, 1)
		validSlice[0] = false
		s.valid = append(validSlice, s.valid...)
		return nil
	}
	if !ok {
		return fmt.Errorf("TimeSeries PushFront: obj not time.Time")
	}
	objTimeSlice := make([]time.Time, 1)
	objTimeSlice[0] = objTime
	s.data = append(objTimeSlice, s.data...)
	validSlice := make([]bool, 1)
	validSlice[0] = true
	s.valid = append(validSlice, s.valid...)
	return nil
}

func (s *TimeSeries) PopBack() error {
	if len(s.data) == 0 {
		return fmt.Errorf("TimeSeries PopBack: empty data")
	}
	s.valid = s.valid[:s.Len()-1]
	s.data = s.data[:s.Len()-1]
	return nil
}

func (s *TimeSeries) PopFront() error {
	if len(s.data) == 0 {
		return fmt.Errorf("TimeSeries PopFront: empty data")
	}
	s.valid = s.valid[:s.Len()-1]
	s.data = s.data[1:]
	return nil
}

func (s *TimeSeries) Subset(indices []int) Series {
	newData := make([]time.Time, len(indices))
	newValid := make([]bool, len(indices))
	for k, rowIdx := range indices {
		if rowIdx < len(s.data) {
			newData[k] = s.data[rowIdx]
			newValid[k] = s.valid[rowIdx]
		}
	}

	return &TimeSeries{name: s.name, data: newData, valid: newValid}
}

func (s *TimeSeries) Subseq(from int, to int) Series {
	return &TimeSeries{name: s.name, data: s.data[from:to], valid: s.valid[from:to]}
}

func (s *TimeSeries) Head() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Series: %s [time]\n", s.name))

	for i := 0; i < min(10, s.Len()); i++ {
		dateStr := s.data[i].Format("2006-01-02 15:04:05")
		sb.WriteString(fmt.Sprintf("%d: %s\n", i, dateStr))
	}

	return sb.String()
}

func (s *TimeSeries) Add(other Series) (Series, error) {
	if s.Len() != other.Len() {
		return nil, ErrSeriesLengthMismatch
	}

	otherDuration, ok := other.(*DurationSeries)
	if !ok {
		return nil, ErrOperationNotSupported
	}

	n := s.Len()

	result := make([]time.Time, n)
	for i := 0; i < n; i++ {
		result[i] = s.data[i].Add(otherDuration.data[i])
	}

	return NewTimeSeries("("+s.name+" + "+otherDuration.name+")", result), nil
}

func (s *TimeSeries) Sub(other Series) (Series, error) {
	if s.Len() != other.Len() {
		return nil, ErrSeriesLengthMismatch
	}

	if otherTime, ok := other.(*TimeSeries); ok {
		n := s.Len()

		result := make([]time.Duration, n)
		for i := 0; i < n; i++ {
			result[i] = s.data[i].Sub(otherTime.data[i])
		}

		return NewDurationSeries("("+s.name+" - "+otherTime.name+")", result), nil
	}

	if otherDuration, ok := other.(*DurationSeries); ok {
		n := s.Len()

		result := make([]time.Time, n)
		for i := 0; i < n; i++ {
			result[i] = s.data[i].Add(-otherDuration.data[i])
		}

		return NewTimeSeries("("+s.name+" - "+otherDuration.name+")", result), nil
	}

	return nil, ErrOperationNotSupported
}

func (s *TimeSeries) Mul(_ Series) (Series, error) {
	return nil, ErrOperationNotSupported
}

func (s *TimeSeries) Div(_ Series) (Series, error) {
	return nil, ErrOperationNotSupported
}

func (s *TimeSeries) Copy() Series {
	sCopy := *s
	sCopy.data = make([]time.Time, len(s.data))
	copy(sCopy.data, s.data)
	sCopy.valid = make([]bool, len(s.valid))
	copy(sCopy.valid, s.valid)

	return &sCopy
}
