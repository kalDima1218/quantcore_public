package series

import (
	"fmt"
	"strings"
)

type StringSeries struct {
	name  string
	data  []string
	valid []bool
}

func NewStringSeries(name string, data []string) *StringSeries {
	valid := make([]bool, len(data))
	for i := range valid {
		valid[i] = true
	}

	return &StringSeries{name: name, data: data, valid: valid}
}

func NewConstStringSeries(name string, s string, length int) *StringSeries {
	data := make([]string, length)
	for i := range data {
		data[i] = s
	}

	return NewStringSeries(name, data)
}

func (s *StringSeries) Name() string { return s.name }
func (s *StringSeries) Type() string { return "string" }
func (s *StringSeries) Len() int     { return len(s.data) }

func (s *StringSeries) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Series: %s [string]\n", s.name))

	for i, v := range s.data {
		sb.WriteString(fmt.Sprintf("%d: %s\n", i, v))
	}

	return sb.String()
}

func (s *StringSeries) Get(i int) any {
	if i < 0 || i >= len(s.data) || !s.valid[i] {
		return nil
	}

	return s.data[i]
}

func (s *StringSeries) PushBack(obj any) error {
	objString, ok := obj.(string)
	if obj == nil {
		s.data = append(s.data, "")
		s.valid = append(s.valid, false)
		return nil
	}
	if !ok {
		return fmt.Errorf("StringSeries PushBack: obj not string")
	}
	s.data = append(s.data, objString)
	s.valid = append(s.valid, true)
	return nil
}

func (s *StringSeries) PushFront(obj any) error {
	objString, ok := obj.(string)
	if obj == nil {
		objStringSlice := make([]string, 1)
		objStringSlice[0] = ""
		s.data = append(objStringSlice, s.data...)
		validSlice := make([]bool, 1)
		validSlice[0] = false
		s.valid = append(validSlice, s.valid...)
		return nil
	}
	if !ok {
		return fmt.Errorf("StringSeries PushFront: obj not string")
	}
	objStringSlice := make([]string, 1)
	objStringSlice[0] = objString
	s.data = append(objStringSlice, s.data...)
	validSlice := make([]bool, 1)
	validSlice[0] = true
	s.valid = append(validSlice, s.valid...)
	return nil
}

func (s *StringSeries) PopBack() error {
	if len(s.data) == 0 {
		return fmt.Errorf("StringSeries PopBack: empty data")
	}
	s.valid = s.valid[:s.Len()-1]
	s.data = s.data[:s.Len()-1]
	return nil
}

func (s *StringSeries) PopFront() error {
	if len(s.data) == 0 {
		return fmt.Errorf("StringSeries PopFront: empty data")
	}
	s.valid = s.valid[:s.Len()-1]
	s.data = s.data[1:]
	return nil
}

func (s *StringSeries) Subset(indices []int) Series {
	newData := make([]string, len(indices))
	newValid := make([]bool, len(indices))
	for k, rowIdx := range indices {
		if rowIdx < len(s.data) {
			newData[k] = s.data[rowIdx]
			newValid[k] = s.valid[rowIdx]
		}
	}

	return &StringSeries{name: s.name, data: newData, valid: newValid}
}

func (s *StringSeries) Subseq(from int, to int) Series {
	return &StringSeries{name: s.name, data: s.data[from:to], valid: s.valid[from:to]}
}

func (s *StringSeries) Head() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Series: %s [string]\n", s.name))

	for i := 0; i < min(10, s.Len()); i++ {
		sb.WriteString(fmt.Sprintf("%d: %s\n", i, s.data[i]))
	}

	return sb.String()
}

func (s *StringSeries) Add(other Series) (Series, error) {
	if s.Len() != other.Len() {
		return nil, ErrSeriesLengthMismatch
	}

	otherString, ok := other.(*StringSeries)
	if !ok {
		return nil, ErrOperationNotSupported
	}

	n := s.Len()

	result := make([]string, n)
	for i := 0; i < n; i++ {
		result[i] = s.data[i] + otherString.data[i]
	}

	return NewStringSeries("("+s.name+" + "+otherString.name+")", result), nil
}

func (s *StringSeries) Sub(_ Series) (Series, error) {
	return nil, ErrOperationNotSupported
}

func (s *StringSeries) Mul(_ Series) (Series, error) {
	return nil, ErrOperationNotSupported
}

func (s *StringSeries) Div(_ Series) (Series, error) {
	return nil, ErrOperationNotSupported
}

func (s *StringSeries) Copy() Series {
	sCopy := *s
	sCopy.data = make([]string, len(s.data))
	copy(sCopy.data, s.data)
	sCopy.valid = make([]bool, len(s.valid))
	copy(sCopy.valid, s.valid)

	return &sCopy
}
