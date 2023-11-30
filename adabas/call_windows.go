//go:build adalnk && cgo && windows
// +build adalnk,cgo,windows

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
	"fmt"
	"os"
	"os/user"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/tknie/adabas-go-api/adatypes"
)

var (
	adaLibrary     = syscall.NewLazyDLL("adalnkx.dll")
	adabasCallFunc = adaLibrary.NewProc("adabasx")
	adabasIdFunc   = adaLibrary.NewProc("lnk_set_adabas_id")
	adabasPwdFunc  = adaLibrary.NewProc("lnk_set_uid_pw")
)

var disableInterface = false

// NewAdabasID create a new unique Adabas ID instance using static data. Instead
// using the current process id a generate unique time stamp and counter version
// of the pid is used.
func NewAdabasID() *ID {
	AdaID := AID{level: 3, size: adabasIDSize}
	aid := ID{AdaID: &AdaID, connectionMap: make(map[string]*Status)}
	//	C.lnk_get_adabas_id(adabasIDSize, (*C.uchar)(unsafe.Pointer(&AdaID)))
	curUser, err := user.Current()
	if err != nil {
		adatypes.Central.Log.Debugf("Error evaluing user")
		copy(AdaID.User[:], ([]byte("Unknown "))[:8])
	} else {
		adatypes.Central.Log.Debugf("Create new ID(local) with %s", curUser.Username)
		copy(AdaID.User[:], ([]byte(curUser.Username + "        "))[:8])
	}
	host, err := os.Hostname()
	if err != nil {
		adatypes.Central.Log.Debugf("Error evaluing host")
		copy(AdaID.Node[:], ([]byte("Unknown"))[:8])
	} else {
		adatypes.Central.Log.Debugf("Current host is %s", curUser)
		copy(AdaID.Node[:], ([]byte(host + "        "))[:8])
	}
	id := atomic.AddUint32(&idCounter, 1)
	adatypes.Central.Log.Debugf("Create new ID(local) with %v", AdaID.Node)
	AdaID.Timestamp = uint64(time.Now().UnixNano() / 1000)
	AdaID.Pid = uint32((AdaID.Timestamp - (AdaID.Timestamp % 100)) + uint64(id))
	adatypes.Central.Log.Debugf("Create Adabas ID: %d -> %s", AdaID.Pid, aid.String())
	return &aid
}

// setAdabasID set the Adabas ID Windows API Call to call
func setAdabasID(id *ID) error {
	ret, _, errno := adabasIdFunc.Call(
		uintptr(unsafe.Pointer(id.AdaID)))
	adatypes.Central.Log.Debugf("Adabas set ID returns %d", ret)
	if ret != 0 {
		return fmt.Errorf("Errno: (%d) %v", ret, errno)
	}
	return nil
}

// CallAdabas uses the Adabas Windows API Call to call
func (adabas *Adabas) CallAdabas(acbx *Acbx, abd []*Buffer) error {
	defer TimeTrack(time.Now(), "Call adabas(Windows)", adabas)
	defer TrackAdabas(time.Now(), adabas)
	for _, ab := range abd {
		if len(ab.buffer) > 0 {
			ab.abd.Abdaddr = uint64(uintptr(unsafe.Pointer(&ab.buffer[0])))
		}
	}
	nrAbd := len(abd)
	var abds uintptr
	if nrAbd > 0 {
		abds = uintptr(unsafe.Pointer(&abd[0]))
	}
	ret, _, errno := adabasCallFunc.Call(
		uintptr(unsafe.Pointer(acbx)),
		uintptr(nrAbd),
		abds,
	)
	adatypes.Central.Log.Debugf("Adabas call returns %d: %v", int(ret), errno)
	/*if ret == -1 {
		return fmt.Errorf("Error calling Adabas interface")
	}
	if ret != 0 {
		return fmt.Errorf("Error calling Adabas API")
	}*/
	return nil

}

type AdaIPC struct {
}

func NewAdaIPC(URL *URL, ID *ID) *AdaIPC {
	return &AdaIPC{}
}

// Send Send the TCP/IP request to remote Adabas database
func (ipc *AdaIPC) Send(adabas *Adabas) (err error) {
	if disableInterface {
		return fmt.Errorf("IPC interface not present")
	}

	adatypes.Central.Log.Debugf("Call Adabas using dynamic native link")
	err = adabasCallFunc.Find()
	if err != nil {
		disableInterface = true
		adatypes.Central.Log.Debugf("Disable interface because not available")
		return err
	}
	err = setAdabasID(adabas.ID)
	if err != nil {
		return err
	}
	/* For OP calls, initialize the security layer setting the password. The corresponding
	 * Security buffer (Z-Buffer) are generated inside the Adabas client layer.
	 * Under the hood the Z-Buffer will generate one time passwords send with the next call
	 * after OP. */
	if adabas.ID.pwd != "" && adabas.Acbx.Acbxcmd == op.code() {
		adatypes.Central.Log.Debugf("Set user %s password credentials", adabas.ID.user)
		ret, _, errno := adabasIdFunc.Call(uintptr(unsafe.Pointer(&adabas.Acbx.Acbxdbid)),
			uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(adabas.ID.user))),
			uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(adabas.ID.pwd))))
		adatypes.Central.Log.Debugf("Set user pwd for %s: %d %v", adabas.ID.user, ret, errno)
	}
	// Call Adabas call to database
	err = callAdabas(adabas.Acbx, adabas.AdabasBuffers)
	if err != nil {
		return err
	}
	adatypes.Central.Log.Debugf("Return call adabas")
	return nil
}

func (ipc *AdaIPC) Connect(adabas *Adabas) (err error) {
	return nil
}

// Disconnect disconnect remote TCP/IP Adabas nucleus
func (ipc *AdaIPC) Disconnect() (err error) {
	return nil
}
