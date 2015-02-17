package loges

import (
	"log"
)

// Errer handles errors and warnings.
type Errer interface {
	// Error handles a fatal error.
	Error(error)

	// Warning handles a non-fatal error.
	Warning(error)
}

// DefaultErrer implements Errer, logging with log.
type DefaultErrer struct{}

// Error handles a fatal error.
func (DefaultErrer) Error(err error) {
	log.Print(err)
}

// Warning handles a non-fatal error.
func (DefaultErrer) Warning(err error) {
	log.Print(err)
}

var _ Errer = DefaultErrer{}
