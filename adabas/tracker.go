package adabas

import (
	"time"

	"github.com/tknie/adabas-go-api/adatypes"
)

// Tracker tracker function containing the function to call
type Tracker struct {
	TrackFunc func(start time.Duration, adabas *Adabas)
}

var adabasTracker *Tracker

// RegisterTracker register tracker for analysis Adabas calls
func RegisterTracker(tracker *Tracker) {
	adabasTracker = tracker
}

// ClearTracker clear tracker for analysis Adabas calls
func ClearTracker() {
	adabasTracker = nil
}

// TrackAdabas track Adabas calls (internally)
func TrackAdabas(start time.Time, adabas *Adabas) {
	elapsed := time.Since(start)
	if adabasTracker != nil {
		if adatypes.Central.IsDebugLevel() {
			adatypes.Central.Log.Debugf("Tracking call")
		}
		adabasTracker.TrackFunc(elapsed, adabas)
	}
}
