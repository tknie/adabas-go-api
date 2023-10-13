/*
* Copyright © 2018-2022 Software AG, Darmstadt, Germany and/or its licensors
*
* SPDX-License-Identifier: Apache-2.0
*
*   Licensed under the Apache License, Version 2.0 (the "License");
*   you may not use this file except in compliance with the License.
*   You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
*
 */

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
