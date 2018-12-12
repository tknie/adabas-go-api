/*
* Copyright © 2018 Software AG, Darmstadt, Germany and/or its licensors
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

package adatypes

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strconv"
)

type uint32Value struct {
	adaValue
	value uint32
}

func newUInt4Value(initType IAdaType) *uint32Value {
	value := uint32Value{adaValue: adaValue{adatype: initType}}
	return &value
}

func (value *uint32Value) ByteValue() byte {
	return byte(value.value)
}

func (value *uint32Value) String() string {
	return strconv.Itoa(int(value.value))
}

func (value *uint32Value) Value() interface{} {
	return value.value
}

func (value *uint32Value) Bytes() []byte {
	v := make([]byte, 4)
	value.adatype.Endian().PutUint32(v, value.value)
	return v
}

func (value *uint32Value) SetStringValue(stValue string) {
	iv, err := strconv.Atoi(stValue)
	if err == nil {
		value.value = uint32(iv)
	}
}

func (value *uint32Value) SetValue(v interface{}) error {
	x, err := value.commonUInt64Convert(v)
	if err != nil {
		return err
	}
	value.value = uint32(x)
	return nil
}

func (value *uint32Value) FormatBuffer(buffer *bytes.Buffer, option *BufferOption) uint32 {
	return value.commonFormatBuffer(buffer, option)
}

func (value *uint32Value) StoreBuffer(helper *BufferHelper) error {
	return helper.PutUInt32(value.value)
}

func (value *uint32Value) parseBuffer(helper *BufferHelper, option *BufferOption) (res TraverseResult, err error) {
	value.value, err = helper.ReceiveUInt32()
	Central.Log.Debugf("Buffer get uint4 offset=%d %s %d", helper.offset, value.Type().Name(), value.value)
	return
}

func (value *uint32Value) Int32() (int32, error) {
	return 0, errors.New("Cannot convert value to signed 32-bit integer")
}

func (value *uint32Value) UInt32() (uint32, error) {
	return 0, errors.New("Cannot convert value to unsigned 32-bit integer")
}
func (value *uint32Value) Int64() (int64, error) {
	return 0, errors.New("Cannot convert value to signed 64-bit integer")
}
func (value *uint32Value) UInt64() (uint64, error) {
	return 0, errors.New("Cannot convert value to unsigned 64-bit integer")
}
func (value *uint32Value) Float() (float64, error) {
	return 0, errors.New("Cannot convert value to 64-bit float")
}

type int32Value struct {
	adaValue
	value int32
}

func newInt4Value(initType IAdaType) *int32Value {
	value := int32Value{adaValue: adaValue{adatype: initType}}
	return &value
}

func (value *int32Value) ByteValue() byte {
	return byte(value.value)
}

func (value *int32Value) String() string {
	return strconv.Itoa(int(value.value))
}

func (value *int32Value) Value() interface{} {
	return value.value
}

func (value *int32Value) Bytes() []byte {
	v := make([]byte, 4)
	binary.PutVarint(v, int64(value.value))
	return v
}

func (value *int32Value) SetStringValue(stValue string) {
	iv, err := strconv.ParseInt(stValue, 10, 32)
	if err == nil {
		value.value = int32(iv)
	}
}

func (value *int32Value) SetValue(v interface{}) error {
	x, err := value.commonInt64Convert(v)
	if err != nil {
		return err
	}
	value.value = int32(x)
	return nil
}

func (value *int32Value) FormatBuffer(buffer *bytes.Buffer, option *BufferOption) uint32 {
	return value.commonFormatBuffer(buffer, option)
}

func (value *int32Value) StoreBuffer(helper *BufferHelper) error {
	return helper.PutInt32(value.value)
}

func (value *int32Value) parseBuffer(helper *BufferHelper, option *BufferOption) (res TraverseResult, err error) {
	value.value, err = helper.ReceiveInt32()
	Central.Log.Debugf("Buffer get int4 offset=%d %s", helper.offset, value.Type().Name())
	return
}

func (value *int32Value) Int32() (int32, error) {
	return 0, errors.New("Cannot convert value to signed 32-bit integer")
}

func (value *int32Value) UInt32() (uint32, error) {
	return 0, errors.New("Cannot convert value to unsigned 32-bit integer")
}
func (value *int32Value) Int64() (int64, error) {
	return 0, errors.New("Cannot convert value to signed 64-bit integer")
}
func (value *int32Value) UInt64() (uint64, error) {
	return 0, errors.New("Cannot convert value to unsigned 64-bit integer")
}
func (value *int32Value) Float() (float64, error) {
	return 0, errors.New("Cannot convert value to 64-bit float")
}