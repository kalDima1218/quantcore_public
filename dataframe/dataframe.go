package dataframe

import (
	"QuantCore/series"
	"fmt"
	"log"
	"strings"
	"time"
)

type DataFrame struct {
	columns  map[string]series.Series
	colOrder []string
	nRows    int
}

func NewDataFrame() *DataFrame {
	return &DataFrame{
		columns:  make(map[string]series.Series),
		colOrder: make([]string, 0),
		nRows:    0,
	}
}

func (df *DataFrame) Shape() (int, int) {
	return df.nRows, len(df.colOrder)
}

func (df *DataFrame) GetColumnNames() []string {
	return df.colOrder
}

func (df *DataFrame) SetColumn(name string, s series.Series) error {
	if len(df.columns) == 0 {
		df.nRows = s.Len()
	} else {
		if s.Len() != df.nRows {
			return fmt.Errorf("column length %d does not match dataframe length %d", s.Len(), df.nRows)
		}
	}

	df.columns[name] = s
	for _, col := range df.colOrder {
		if col == name {
			return nil
		}
	}

	df.colOrder = append(df.colOrder, name)

	return nil
}

func (df *DataFrame) AddColumn(s series.Series) error {
	err := df.SetColumn(s.Name(), s)
	if err != nil {
		return err
	}
	return nil
}

func (df *DataFrame) DropColumn(name string) error {
	for i, col := range df.colOrder {
		if col == name {
			df.colOrder = append(df.colOrder[:i], df.colOrder[i+1:]...)
			delete(df.columns, name)

			return nil
		}
	}

	return fmt.Errorf("column %s not found", name)
}

func (df *DataFrame) DropColumns(names ...string) error {
	for _, col := range names {
		err := df.DropColumn(col)
		if err != nil {
			return err
		}
	}

	return nil
}

func (df *DataFrame) GetColumn(name string) series.Series {
	col, ok := df.columns[name]
	if !ok {
		log.Fatalln(fmt.Errorf("column %s not found", name))
	}

	return col
}

func (df *DataFrame) Show(rows int) {
	rows = min(rows, df.nRows)

	fmt.Printf("%-5s", "Idx")

	for _, name := range df.colOrder {
		fmt.Printf(" | %-10s", name)
	}

	fmt.Println("\n" + strings.Repeat("-", 5+13*len(df.colOrder)))

	for i := 0; i < rows; i++ {
		fmt.Printf("%-5d", i)

		for _, name := range df.colOrder {
			val := df.columns[name].Get(i)
			if val == nil {
				fmt.Printf(" | NaN")
			} else {
				switch v := val.(type) {
				case float64:
					fmt.Printf(" | %-10.2f", v)
				case time.Time:
					fmt.Print(v.Format("2006-01-02 15:04:05"))
				case time.Duration:
					fmt.Printf(" | %-10s", v)
				default:
					fmt.Printf(" | %-10v", v)
				}
			}
		}

		fmt.Println()
	}

	fmt.Println()
}

func (df *DataFrame) ShowHead() {
	df.Show(10)
}

func (df *DataFrame) Filter(predicate func(df *DataFrame, i int) bool) *DataFrame {
	keepIndices := make([]int, 0, df.nRows)

	for i := 0; i < df.nRows; i++ {
		if predicate(df, i) {
			keepIndices = append(keepIndices, i)
		}
	}

	newDf := NewDataFrame()

	for _, colName := range df.colOrder {
		originalCol := df.columns[colName]
		newCol := originalCol.Subset(keepIndices)

		if err := newDf.SetColumn(colName, newCol); err != nil {
			panic(err)
		}
	}

	return newDf
}

func (df *DataFrame) DropNa() {
	if df.nRows == 0 {
		return
	}

	cols := make([]series.Series, len(df.colOrder))
	for idx, colName := range df.colOrder {
		cols[idx] = df.columns[colName]
	}

	keepIndices := make([]int, 0, df.nRows)

	for i := 0; i < df.nRows; i++ {
		rowHasNa := false

		for _, col := range cols {
			if col.Get(i) == nil {
				rowHasNa = true
				break
			}
		}

		if !rowHasNa {
			keepIndices = append(keepIndices, i)
		}
	}

	if len(keepIndices) == df.nRows {
		return
	}

	df.nRows = len(keepIndices)

	for name, col := range df.columns {
		err := df.SetColumn(name, col.Subset(keepIndices))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (df *DataFrame) Copy() *DataFrame {
	dfCopy := &DataFrame{
		nRows: df.nRows,
	}

	dfCopy.colOrder = make([]string, len(df.colOrder))
	copy(dfCopy.colOrder, df.colOrder)

	dfCopy.columns = make(map[string]series.Series, len(df.columns))

	for key, ser := range df.columns {
		dfCopy.columns[key] = ser.Copy()
	}

	return dfCopy
}

func (df *DataFrame) GetRow(i int) map[string]any {
	if i >= df.nRows {
		log.Fatal("error GetRow: not enough rows")
	}
	result := make(map[string]any, len(df.colOrder))
	for _, col := range df.colOrder {
		result[col] = df.columns[col].Get(i)
	}

	return result
}

func (df *DataFrame) GetRowTo(i int, result map[string]any) {
	//clear(result)
	if i >= df.nRows {
		log.Fatal("error GetRow: not enough rows")
	}
	for _, col := range df.colOrder {
		result[col] = df.columns[col].Get(i)
	}
}

func (df *DataFrame) PushBack(row map[string]any) error {
	for _, col := range df.colOrder {
		err := df.columns[col].PushBack(row[col])
		if err != nil {
			return err
		}
	}
	return nil
}

func (df *DataFrame) PushFront(row map[string]any) error {
	for _, col := range df.colOrder {
		err := df.columns[col].PushFront(row[col])
		if err != nil {
			return err
		}
	}
	return nil
}

func (df *DataFrame) PopBack() error {
	for _, col := range df.colOrder {
		err := df.columns[col].PopBack()
		if err != nil {
			return err
		}
	}
	return nil
}

func (df *DataFrame) PopFront() error {
	for _, col := range df.colOrder {
		err := df.columns[col].PopFront()
		if err != nil {
			return err
		}
	}
	return nil
}

func (df *DataFrame) Subseq(from int, to int) *DataFrame {
	dfSubseq := NewDataFrame()
	for _, col := range df.colOrder {
		err := dfSubseq.AddColumn(df.GetColumn(col).Subseq(from, to))
		if err != nil {
			log.Fatal(err)
		}
	}
	return dfSubseq
}
