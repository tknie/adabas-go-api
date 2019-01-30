/*
* Copyright © 2018-2019 Software AG, Darmstadt, Germany and/or its licensors
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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"unsafe"

	"github.com/SoftwareAG/adabas-go-api/adatypes"
)

// BufferType type of buffer following
type BufferType uint32

const (
	adatcpHeaderEyecatcher = "ADATCP"

	adatcpHeaderVersion = "01"
	// ConnectRequest connect
	ConnectRequest = BufferType(1)
	// ConnectReply reply after first connect
	ConnectReply = BufferType(2)
	// ConnectError connection errror
	ConnectError = BufferType(3)
	// DisconnectRequest disconnect request
	DisconnectRequest = BufferType(4)
	// DisconnetReply disconnect reply
	DisconnetReply = BufferType(5)
	// DisconnectError disconnect error
	DisconnectError = BufferType(6)
	// DataRequest data request
	DataRequest = BufferType(7)
	// DataReply data reply
	DataReply = BufferType(8)
	// DataError data error
	DataError = BufferType(9)
)

type adaUUID [16]byte

// AdaTCPHeader Adabas TCP Header ADATCP
type AdaTCPHeader struct {
	Eyecatcher     [6]byte
	Version        [2]byte
	Length         uint32
	BufferType     BufferType
	Identification adaUUID
	ErrorCode      uint32
	Reserved       uint32
}

// AdaTCPHeaderLength length of AdaTCPHeader structure
const AdaTCPHeaderLength = 40

const (
	adatcpBigEndian    = byte(1)
	adatcpLittleEndian = byte(2)

	adatcpASCII8 = byte(1)
	adatcpEBCDIC = byte(2)

	adatcpFloatIEEE = byte(1)
)

// AdaTCPConnectPayload Adabas TCP connect payload
type AdaTCPConnectPayload struct {
	DatabaseVersion [16]byte
	DatabaseName    [16]byte
	Userid          [8]byte
	Nodeid          [8]byte
	ProcessID       uint32
	DatabaseID      uint32
	TimeStamp       uint64
	Endianness      byte
	Charset         byte
	Floatingpoint   byte
	Filler          [5]byte
}

// AdaTCPConnectPayloadLength ADATCP connect payload
const AdaTCPConnectPayloadLength = 72

type adatcpDisconnectPayload struct {
	Dummy uint64
}

// adatcp TCP connection handle (for internal use only)
type adatcp struct {
	connection          net.Conn
	url                 string
	order               binary.ByteOrder
	adauuid             adaUUID
	serverEndianness    byte
	serverCharset       byte
	serverFloatingpoint byte
	databaseVersion     [16]byte
	databaseName        [16]byte
	databaseID          uint32
}

const adatcpDataHeaderEyecatcher = "DATA"

const adatcpDataHeaderVersion = "0001"

const (
	adabasRequest = uint32(1)
	adabasReply   = uint32(2)
)

// AdaTCPDataHeader Adabas TCP header
type AdaTCPDataHeader struct {
	Eyecatcher      [4]byte
	Version         [4]byte
	Length          uint32
	DataType        uint32
	NumberOfBuffers uint32
	ErrorCode       uint32
}

// AdaTCPDataHeaderLength length of AdaTCPDataHeader structure
const AdaTCPDataHeaderLength = 24

func adatcpTCPClientHTON8(l uint64) uint64 {
	return uint64(
		((uint64(l) >> 56) & uint64(0x00000000000000ff)) | ((uint64(l) >> 40) & uint64(0x000000000000ff00)) | ((uint64(l) >> 24) & uint64(0x0000000000ff0000)) | ((uint64(l) >> 8) & uint64(0x00000000ff000000)) | ((uint64(l) << 8) & uint64(0x000000ff00000000)) | ((uint64(l) << 24) & uint64(0x0000ff0000000000)) | ((uint64(l) << 40) & uint64(0x00ff000000000000)) | ((uint64(l) << 56) & uint64(0xff00000000000000)))
}
func adatcpTCPClientHTON4(l uint32) uint32 {
	return uint32(
		((uint32(l) >> 24) & uint32(0x000000ff)) | ((uint32(l) >> 8) & uint32(0x0000ff00)) | ((uint32(l) << 8) & uint32(0x00ff0000)) | ((uint32(l) << 24) & uint32(0xff000000)))
}

// NewAdatcpHeader new Adabas TCP header
func NewAdatcpHeader(bufferType BufferType) AdaTCPHeader {
	header := AdaTCPHeader{BufferType: BufferType(uint32(bufferType))}
	copy(header.Eyecatcher[:], adatcpHeaderEyecatcher)
	copy(header.Version[:], adatcpHeaderVersion)
	return header
}

func newAdatcpDataHeader(dataType uint32) AdaTCPDataHeader {
	header := AdaTCPDataHeader{DataType: dataType}
	copy(header.Eyecatcher[:], adatcpDataHeaderEyecatcher)
	copy(header.Version[:], adatcpDataHeaderVersion)
	return header
}

func bigEndian() (ret bool) {
	i := 0x1
	bs := (*[4]byte)(unsafe.Pointer(&i))
	if bs[0] == 0 {
		return true
	}
	return false
}

// Endian current byte order of the client system
func Endian() binary.ByteOrder {
	if bigEndian() {
		return binary.BigEndian
	}
	return binary.LittleEndian
}

// Connect connect to remote TCP/IP Adabas nucleus
func connect(url string, order binary.ByteOrder, user [8]byte, node [8]byte,
	pid uint32, timestamp uint64) (connection *adatcp, err error) {
	connection = &adatcp{url: url, order: order}
	adatypes.Central.Log.Debugf("Open TCP connection to %s", connection.url)
	addr, _ := net.ResolveTCPAddr("tcp", connection.url)
	tcpConn, tcpErr := net.DialTCP("tcp", nil, addr)
	err = tcpErr
	if err != nil {
		adatypes.Central.Log.Debugf("Connect error : %v", err)
		return
	}
	adatypes.Central.Log.Debugf("Connect dial passed ...")
	connection.connection = tcpConn
	tcpConn.SetNoDelay(true)
	var buffer bytes.Buffer
	header := NewAdatcpHeader(ConnectRequest)
	payload := AdaTCPConnectPayload{Charset: adatcpASCII8, Floatingpoint: adatcpFloatIEEE}
	copy(payload.Userid[:], user[:])
	copy(payload.Nodeid[:], node[:])
	payload.ProcessID = pid
	payload.TimeStamp = timestamp

	header.Length = uint32(AdaTCPHeaderLength + unsafe.Sizeof(payload))
	err = binary.Write(&buffer, binary.BigEndian, header)
	if err != nil {
		adatypes.Central.Log.Debugf("Write TCP header in buffer error %s", err)
		return
	}
	if bigEndian() {
		payload.Endianness = adatcpBigEndian
	} else {
		payload.Endianness = adatcpLittleEndian
	}
	adatypes.Central.Log.Debugf("Buffer size after header=%d", buffer.Len())

	// Send payload in big endian needed until remote knows the endianess of the client
	err = binary.Write(&buffer, binary.BigEndian, payload)
	if err != nil {
		adatypes.Central.Log.Debugf("Write TCP connect payload in buffer error %s", err)
		return
	}
	adatypes.Central.Log.Debugf("Buffer size after payload=%d", buffer.Len())

	_, err = connection.connection.Write(buffer.Bytes())
	if err != nil {
		adatypes.Central.Log.Debugf("Error writing data %s", err)
		return
	}
	rcvBuffer := make([]byte, buffer.Len())
	_, err = io.ReadFull(connection.connection, rcvBuffer)
	//	_, err = connection.connection.Read(rcvBuffer)
	if err != nil {
		adatypes.Central.Log.Debugf("Error reading data %v", err)
		return
	}
	buf := bytes.NewBuffer(rcvBuffer)
	err = binary.Read(buf, binary.BigEndian, &header)
	if err != nil {
		adatypes.Central.Log.Debugf("Error parsing header %v", err)
		return
	}

	err = binary.Read(buf, binary.BigEndian, &payload)
	if err != nil {
		adatypes.Central.Log.Debugf("Error parsing payload %v", err)
		return
	}

	connection.adauuid = header.Identification
	connection.serverEndianness = payload.Endianness
	connection.serverCharset = payload.Charset
	connection.serverFloatingpoint = payload.Floatingpoint
	connection.databaseVersion = payload.DatabaseVersion
	connection.databaseName = payload.DatabaseName
	connection.databaseID = payload.DatabaseID

	return
}

// Disconnect disconnect remote TCP/IP Adabas nucleus
func (connection *adatcp) Disconnect() (err error) {
	adatypes.Central.Log.Debugf("Disconnect connection to %s", connection.url)
	var buffer bytes.Buffer
	header := NewAdatcpHeader(DisconnectRequest)
	header.Identification = connection.adauuid
	payload := adatcpDisconnectPayload{}
	header.Length = uint32(AdaTCPHeaderLength + unsafe.Sizeof(payload))

	// Write structures to buffer
	err = binary.Write(&buffer, binary.BigEndian, header)
	if err != nil {
		adatypes.Central.Log.Debugf("Write TCP header in buffer error %s", err)
		return
	}
	err = binary.Write(&buffer, binary.BigEndian, payload)
	if err != nil {
		adatypes.Central.Log.Debugf("Write TCP header in buffer error %s", err)
		return
	}

	// Send the data to network
	_, err = connection.connection.Write(buffer.Bytes())
	if err != nil {
		return
	}
	rcvBuffer := make([]byte, buffer.Len())
	_, err = io.ReadFull(connection.connection, rcvBuffer)
	if err != nil {
		return
	}
	// Parse buffer from network into structure
	buf := bytes.NewBuffer(rcvBuffer)
	err = binary.Read(buf, connection.order, &header)
	if err != nil {
		return
	}
	err = binary.Read(buf, connection.order, &payload)
	if err != nil {
		return
	}

	err = connection.connection.Close()

	return
}

// SendData send data to remote TCP/IP Adabas nucleus
func (connection *adatcp) SendData(buffer bytes.Buffer, nrAbdBuffers uint32) (err error) {
	header := NewAdatcpHeader(DataRequest)
	dataHeader := newAdatcpDataHeader(adabasRequest)
	dataHeader.NumberOfBuffers = nrAbdBuffers
	header.Identification = connection.adauuid
	header.Length = uint32(AdaTCPHeaderLength + AdaTCPDataHeaderLength + buffer.Len())
	dataHeader.Length = uint32(AdaTCPDataHeaderLength + buffer.Len())
	var headerBuffer bytes.Buffer
	err = binary.Write(&headerBuffer, binary.BigEndian, header)
	if err != nil {
		adatypes.Central.Log.Debugf("Write TCP header in buffer error %s", err)
		return
	}
	err = binary.Write(&headerBuffer, Endian(), dataHeader)
	if err != nil {
		adatypes.Central.Log.Debugf("Write TCP header in buffer error %s", err)
		return
	}
	headerBuffer.Write(buffer.Bytes())
	send := headerBuffer.Bytes()
	if adatypes.Central.IsDebugLevel() {
		adatypes.LogMultiLineString(adatypes.FormatBytesWithLength("SND:", send, len(send), 8, true))
	}
	var n int
	adatypes.Central.Log.Debugf("Write TCP data of length=%d capacity=%d netto bytes send=%d", headerBuffer.Len(), headerBuffer.Cap(), len(send))
	n, err = connection.connection.Write(send)
	if err != nil {
		return
	}

	adatypes.Central.Log.Debugf("Send data completed buffer send=%d really send=%d", buffer.Len(), n)
	return
}

// Generate error code specific error
func generateError(errorCode uint32) error {
	return fmt.Errorf("Unknown error code %03d", errorCode)
}

// ReceiveData receive data from remote TCP/IP Adabas nucleus
func (connection *adatcp) ReceiveData(buffer *bytes.Buffer) (nrAbdBuffers uint32, err error) {
	adatypes.Central.Log.Debugf("Receive data .... size=%d", buffer.Len())

	header := NewAdatcpHeader(DataReply)
	dataHeader := newAdatcpDataHeader(adabasRequest)
	header.Identification = connection.adauuid
	headerLength := uint32(AdaTCPHeaderLength)
	dataHeaderLength := uint32(AdaTCPDataHeaderLength)

	hl := int(headerLength + dataHeaderLength)
	rcvHeaderBuffer := make([]byte, headerLength+dataHeaderLength)
	var n int
	//	n, err = io.ReadFull(connection.connection, rcvHeaderBuffer)
	n, err = io.ReadAtLeast(connection.connection, rcvHeaderBuffer, hl)
	if err != nil {
		return
	}
	if adatypes.Central.IsDebugLevel() {
		adatypes.Central.Log.Debugf("Receive got header .... size=%d/%d", n, len(rcvHeaderBuffer))
		adatypes.LogMultiLineString(adatypes.FormatBytesWithLength("RCV Header BUFFER:", rcvHeaderBuffer, len(rcvHeaderBuffer), 8, true))
	}
	if n < hl {
		return 0, fmt.Errorf("Header not received")
		//	return 0, adatypes.NewGenericError( 36)

	}
	headerBuffer := bytes.NewBuffer(rcvHeaderBuffer)
	err = binary.Read(headerBuffer, binary.BigEndian, &header)
	if err != nil {
		return
	}

	//header.Length = header.Length
	adatypes.Central.Log.Debugf("Receive got header length .... size=%d error=%d", header.Length, header.ErrorCode)
	err = binary.Read(headerBuffer, Endian(), &dataHeader)
	if err != nil {
		return
	}
	adatypes.Central.Log.Debugf("Receive got data length .... size=%d nrBuffer=%d", dataHeader.Length, dataHeader.NumberOfBuffers)
	nrAbdBuffers = dataHeader.NumberOfBuffers
	if header.Length == headerLength+dataHeaderLength {
		return 0, generateError(header.ErrorCode)
	}
	if header.Length < headerLength+dataHeaderLength {
		return 0, fmt.Errorf("Received data length incorrect: %d", header.Length)
		//		return 0, adatypes.NewGenericError( 35, header.Length)
	}
	adatypes.Central.Log.Debugf("Current size of buffer=%d", buffer.Len())
	adatypes.Central.Log.Debugf("Receive %d number of bytes of %d", n, header.Length)
	_, err = buffer.Write(rcvHeaderBuffer[hl:])
	if err != nil {
		return
	}
	adatypes.Central.Log.Debugf("Received header size of buffer=%d", buffer.Len())
	if header.Length > uint32(n) {
		dataBytes := make([]byte, int(header.Length)-n)
		adatypes.Central.Log.Debugf("Create buffer of size %d to read rest of missingdata", len(dataBytes))
		n, err = io.ReadFull(connection.connection, dataBytes)
		// _, err = connection.connection.Read(dataBytes)
		if err != nil {
			return
		}
		adatypes.Central.Log.Debugf("Extra read receive %d number of bytes", n)
		buffer.Write(dataBytes)
		adatypes.Central.Log.Debugf("Current size of buffer=%d", buffer.Len())
	}
	if adatypes.Central.IsDebugLevel() {
		adatypes.LogMultiLineString(adatypes.FormatBytes("RCV DATA BUFFER:", buffer.Bytes(), buffer.Len(), 8))
	}

	return
}
