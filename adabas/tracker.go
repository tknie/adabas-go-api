package adabas

import "time"

// Tracker tracker function containing the function to call
type Tracker struct {
	TrackFunc func(start time.Time, adabas *Adabas)
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
	if adabasTracker != nil {
		adabasTracker.TrackFunc(start, adabas)
	}
}
