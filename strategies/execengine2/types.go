package execengine2

import (
	"QuantCore/strategies/execengine2/internal/model"
	"QuantCore/strategies/execengine2/internal/run"
)

type Side = model.Side

const (
	SideBuy  = model.SideBuy
	SideSell = model.SideSell
)

type Leg = model.Leg

const (
	LegNone = model.LegNone
	LegA    = model.LegA
	LegB    = model.LegB
)

type OrderKind = model.OrderKind

const (
	OrderLimit  = model.OrderLimit
	OrderMarket = model.OrderMarket
)

type OrderRole = model.OrderRole

const (
	RoleTrade    = model.RoleTrade
	RoleHedge    = model.RoleHedge
	RoleFix      = model.RoleFix
	RoleLateFill = model.RoleLateFill
)

type Mode = model.Mode

const (
	ModeDefault   = model.ModeDefault
	ModeTwoLimits = model.ModeTwoLimits
	ModeLimitA    = model.ModeLimitA
	ModeLimitB    = model.ModeLimitB
	ModeMarket    = model.ModeMarket
)

type Plan = model.Plan
type Signal = model.Signal
type Lot = model.Lot
type Result = model.Result
type Prices = model.Prices
type OrderRequest = model.OrderRequest
type Fill = model.Fill
type CancelResult = model.CancelResult
type OrderStatus = model.OrderStatus
type LimitKind = model.LimitKind

const (
	LimitNormal = model.LimitNormal
	LimitMust   = model.LimitMust
)

type PositionChange = model.PositionChange
type PriceChange = model.PriceChange

type RunState = run.Code

const (
	StateReady       = run.Ready
	StateFixing      = run.Fixing
	StateCheckNeeded = run.CheckNeeded
	StateStopped     = run.Stopped
)
