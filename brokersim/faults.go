package brokersim

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Fault — одно правило инъекции сбоя. Правила матчатся по имени метода
// (короткое имя RPC: "PlaceOrder", "GetOrders", "SubscribeTrades", … или "*")
// и срабатывают Count раз (-1 — пока не снимут).
//
// Действия (Action):
//
//	"error"            — вернуть gRPC-ошибку Code/Message, НЕ применяя запрос
//	                     (чистый отказ: брокер ответил, ничего не встало);
//	"drop_after_apply" — ПРИМЕНИТЬ запрос и вернуть ошибку транспортного класса
//	                     (потерянный ответ: ордер встал, клиент об этом не знает —
//	                     главный инцидент идемпотентной постановки). Для методов
//	                     без мутаций эквивалентно "error";
//	"delay"            — задержать ответ на Delay и продолжить нормально
//	                     (таймауты RPC на клиенте);
//	"kill_stream"      — оборвать стрим ошибкой при следующей отправке;
//	"silence"          — молча перестать слать события/heartbeat'ы, держа стрим
//	                     открытым (клиентский heartbeat-timeout / мёртвый фид);
//	"dup_events"       — доставлять каждое событие дважды (дубли при реплеях).
type Fault struct {
	ID          int64      `json:"id"`
	Method      string     `json:"method"`
	Action      string     `json:"action"`
	Code        codes.Code `json:"code,omitempty"`
	Message     string     `json:"message,omitempty"`
	Delay       Duration   `json:"delay,omitempty"`
	Count       int        `json:"count"`       // оставшиеся срабатывания; -1 — без лимита
	Probability float64    `json:"probability"` // 0 или 1 — всегда; иначе шанс срабатывания
}

// faultTable — активные правила. Отдельный мьютекс: gate зовётся и из-под
// s.mu (унарные хендлеры), и из стрим-горутин без s.mu.
type faultTable struct {
	mu     sync.Mutex
	nextID int64
	rules  []*Fault
	rng    *rand.Rand
}

// mangleID — как исказить order id В ОТВЕТЕ PlaceOrder, не трогая внутренний
// ордер (он всё равно исполняется/стоит под своим настоящим id, а его филлы
// стримятся под ним). Моделирует брокера, вернувшего битый ack: клиент видит
// пустой/переиспользованный id и уходит в unverified.
type mangleID int

const (
	mangleNone mangleID = iota
	mangleBlank
	mangleReuse
)

// directive — решение по одному вызову/событию.
type directive struct {
	err            error
	dropAfterApply bool
	delay          time.Duration
	silence        bool
	dup            bool
	mangle         mangleID
}

var validActions = map[string]bool{
	"error": true, "drop_after_apply": true, "delay": true,
	"kill_stream": true, "silence": true, "dup_events": true,
	"blank_order_id": true, "reuse_order_id": true,
}

// Add регистрирует правило, проставляя ему ID и дефолты.
func (ft *faultTable) Add(f Fault) (Fault, error) {
	if !validActions[f.Action] {
		return Fault{}, fmt.Errorf("unknown fault action %q", f.Action)
	}
	if f.Method == "" {
		return Fault{}, fmt.Errorf("fault method is required")
	}
	if f.Count == 0 {
		switch f.Action {
		case "silence", "dup_events":
			f.Count = -1 // режимы «пока не снимут»
		default:
			f.Count = 1
		}
	}
	if f.Code == codes.OK {
		// Дефолтный код — по смыслу действия. "error" — ЧИСТЫЙ деловой отказ:
		// код обязан быть вне maybeDelivered-класса клиента (InvalidArgument),
		// иначе placer ошибочно пойдёт искать «потерянный» ордер. Транспортные
		// действия (drop_after_apply, kill_stream) — Unavailable, ровно из
		// ambiguous-класса.
		if f.Action == "error" {
			f.Code = codes.InvalidArgument
		} else {
			f.Code = codes.Unavailable
		}
	}
	if f.Message == "" {
		f.Message = "brokersim injected fault: " + f.Action
	}
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.nextID++
	f.ID = ft.nextID
	if ft.rng == nil {
		ft.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	ft.rules = append(ft.rules, &f)
	return f, nil
}

// Remove снимает правило по ID; id<0 очищает всё. Возвращает число снятых.
func (ft *faultTable) Remove(id int64) int {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	if id < 0 {
		n := len(ft.rules)
		ft.rules = nil
		return n
	}
	for i, r := range ft.rules {
		if r.ID == id {
			ft.rules = append(ft.rules[:i], ft.rules[i+1:]...)
			return 1
		}
	}
	return 0
}

// List возвращает копию активных правил.
func (ft *faultTable) List() []Fault {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	out := make([]Fault, 0, len(ft.rules))
	for _, r := range ft.rules {
		out = append(out, *r)
	}
	return out
}

// gate оценивает правила для метода и возвращает директиву. Совпавшее правило
// расходует одно срабатывание. Директивы разных правил (например delay+error)
// складываются; первый err побеждает.
func (ft *faultTable) gate(method string) directive {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	var d directive
	kept := ft.rules[:0]
	for _, r := range ft.rules {
		if r.Method != "*" && r.Method != method {
			kept = append(kept, r)
			continue
		}
		if r.Probability > 0 && r.Probability < 1 && ft.rng.Float64() >= r.Probability {
			kept = append(kept, r)
			continue
		}
		if r.Count == 0 {
			continue // исчерпано — выкидываем
		}
		if r.Count > 0 {
			r.Count--
		}
		switch r.Action {
		case "error":
			if d.err == nil {
				d.err = status.Error(r.Code, r.Message)
			}
		case "drop_after_apply":
			if d.err == nil {
				d.err = status.Error(r.Code, r.Message)
				d.dropAfterApply = true
			}
		case "delay":
			d.delay += time.Duration(r.Delay)
		case "kill_stream":
			if d.err == nil {
				d.err = status.Error(r.Code, r.Message)
			}
		case "silence":
			d.silence = true
		case "dup_events":
			d.dup = true
		case "blank_order_id":
			if d.mangle == mangleNone {
				d.mangle = mangleBlank
			}
		case "reuse_order_id":
			if d.mangle == mangleNone {
				d.mangle = mangleReuse
			}
		}
		if r.Count != 0 {
			kept = append(kept, r)
		}
	}
	ft.rules = kept
	return d
}

// peek — как gate, но НЕ расходует срабатывания и учитывает только липкие
// стрим-режимы (silence/dup): стримы опрашивают его на каждом событии.
func (ft *faultTable) peek(method string) directive {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	var d directive
	for _, r := range ft.rules {
		if r.Method != "*" && r.Method != method || r.Count == 0 {
			continue
		}
		switch r.Action {
		case "silence":
			d.silence = true
		case "dup_events":
			d.dup = true
		}
	}
	return d
}

// gateKill расходует и возвращает только kill_stream-правило метода — стримы
// проверяют его на каждой отправке, не трогая унарные правила.
func (ft *faultTable) gateKill(method string) error {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	for i, r := range ft.rules {
		if r.Method != "*" && r.Method != method || r.Action != "kill_stream" || r.Count == 0 {
			continue
		}
		if r.Probability > 0 && r.Probability < 1 && ft.rng.Float64() >= r.Probability {
			continue
		}
		if r.Count > 0 {
			r.Count--
		}
		err := status.Error(r.Code, r.Message)
		if r.Count == 0 {
			ft.rules = append(ft.rules[:i], ft.rules[i+1:]...)
		}
		return err
	}
	return nil
}

// AddFault регистрирует правило сбоя (программный аналог POST /v1/faults).
func (s *Sim) AddFault(f Fault) (Fault, error) { return s.faults.Add(f) }

// RemoveFault снимает правило по ID; id<0 очищает все. Возвращает число снятых.
func (s *Sim) RemoveFault(id int64) int { return s.faults.Remove(id) }

// ListFaults возвращает активные правила.
func (s *Sim) ListFaults() []Fault { return s.faults.List() }

// gateReadOnly — вход хендлера без мутаций: применяет delay и возвращает
// инжектированную ошибку (drop_after_apply здесь эквивалентен error).
func (s *Sim) gateReadOnly(method string) error {
	d := s.faults.gate(method)
	if d.delay > 0 {
		time.Sleep(d.delay)
	}
	return d.err
}

// unaryGate — вход мутирующего унарного хендлера: применяет delay и решает,
// продолжать ли. Возвращённая функция after() вызывается ПОСЛЕ применения
// мутации; она отдаёт ошибку вместо результата для drop_after_apply. mangle
// сообщает хендлеру, как исказить order id в успешном ответе (blank/reuse).
func (s *Sim) unaryGate(method string) (abort error, after func() error, mangle mangleID) {
	d := s.faults.gate(method)
	if d.delay > 0 {
		time.Sleep(d.delay)
	}
	if d.err != nil && !d.dropAfterApply {
		return d.err, nil, mangleNone
	}
	if d.dropAfterApply {
		return nil, func() error { return d.err }, d.mangle
	}
	return nil, func() error { return nil }, d.mangle
}
