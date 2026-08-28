package execengine2

import "errors"

type placeKind uint8

const (
	placeUnknown placeKind = iota
	placeNoOrder
)

type placeError struct {
	err      error
	kind     placeKind
	clientID string
}

func (e *placeError) Error() string { return e.err.Error() }
func (e *placeError) Unwrap() error { return e.err }

// NotPlaced помечает ошибку, при которой заявка точно не была создана.
func NotPlaced(err error) error {
	if err == nil {
		return nil
	}
	return &placeError{err: err, kind: placeNoOrder}
}

// OrderUnknown помечает случай, когда заявка могла попасть к брокеру.
// Client ID сохраняется, чтобы не создать дубль.
func OrderUnknown(clientID string, err error) error {
	if err == nil {
		return nil
	}
	return &placeError{err: err, kind: placeUnknown, clientID: clientID}
}

// OrderMayExist считает любую непомеченную ошибку опасной.
func OrderMayExist(err error) bool {
	if err == nil {
		return false
	}
	var placeErr *placeError
	return !errors.As(err, &placeErr) || placeErr.kind != placeNoOrder
}

// ErrorClientID возвращает client ID из ошибки OrderUnknown.
func ErrorClientID(err error) (string, bool) {
	var placeErr *placeError
	if !errors.As(err, &placeErr) || placeErr.clientID == "" {
		return "", false
	}
	return placeErr.clientID, true
}
