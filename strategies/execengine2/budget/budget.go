// Package budget хранит общий лимит отправки заявок для одного счёта.
// Время окна и опрос брокера находятся снаружи.
package budget

import (
	"errors"
	"sync/atomic"

	"QuantCore/strategies/execengine2/internal/model"
)

// Atomic — общий счётчик для всех движков одного счёта.
// reserve не меняется, remaining — единственное изменяемое поле.
type Atomic struct {
	remaining atomic.Int64
	reserve   int64
}

// New создаёт лимит с заданным числом попыток и запасом для хеджа.
func New(limit, reserve int64) (*Atomic, error) {
	if limit < 0 {
		return nil, errors.New("budget limit must not be negative")
	}
	if reserve < 0 {
		return nil, errors.New("budget reserve must not be negative")
	}
	b := &Atomic{reserve: reserve}
	b.remaining.Store(limit)
	return b, nil
}

// Take одной атомарной операцией проверяет и списывает попытки.
// Обязательный хедж может взять запас и увести счётчик ниже нуля.
func (b *Atomic) Take(ops int64, class model.LimitKind) bool {
	if ops <= 0 {
		return true
	}
	for {
		current := b.remaining.Load()
		next := current - ops
		if class != model.LimitMust && next < b.reserve {
			return false
		}
		if b.remaining.CompareAndSwap(current, next) {
			return true
		}
	}
}

// Reset начинает новое окно. Время окна хранится снаружи.
func (b *Atomic) Reset(limit int64) {
	b.remaining.Store(limit)
}

// SetIfLower применяет значение брокера только тогда, когда оно меньше локального.
// Старый ответ брокера не может отменить уже сделанный Take.
func (b *Atomic) SetIfLower(remaining int64) {
	for {
		current := b.remaining.Load()
		if remaining >= current {
			return
		}
		if b.remaining.CompareAndSwap(current, remaining) {
			return
		}
	}
}

// Remaining возвращает остаток. Минус означает, что обязательный хедж
// вышел за обычный лимит окна.
func (b *Atomic) Remaining() int64 {
	return b.remaining.Load()
}
