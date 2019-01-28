package adabas

import (
	"bytes"
	"fmt"

	"github.com/SoftwareAG/adabas-go-api/adatypes"
)

/*
* Internal constants providing various configurations for the Adabas buffer
* block.
 */
const abdEyecatcher = 'G' /*      G - EYECATCHER              */
const abdVersion = '2'    /*      2 - VERSION                 */
//const E_ABD_EYECATCHER = 0xc7 /* EBCDIC G - EYECATCHER            */
//const E_ABD_VERSION = 0xf2    /* EBCDIC 2 - VERSION               */
const (
	AbdAQFb  = ('F') /*      F-Format Buffer             */
	AbdAQRb  = ('R') /*      R-Record Buffer             */
	AbdAQSb  = ('S') /*      S-Search Buffer             */
	AbdAQVb  = ('V') /*      V-Value Buffer              */
	AbdAQIb  = ('I') /*      I-ISN Buffer                */
	AbdAQPb  = ('P') /*      Performance Buffer          */
	AbdAQMb  = ('M') /*      Multifetch  Buffer          */
	AbdAQUi  = ('U') /*      U-User Info                 */
	AbdAQOb  = ('O') /*      I/O Buffer (internal)       */
	AbdAQXb  = ('X') /*      CLEX Info Buffer (internal) */
	AbdAQZb  = ('Z') /*      Security Buffer (internal)  */
	AbdEQFb  = 0xc6  /* EBCDIC F-Format Buffer           */
	AbdEQRb  = 0xd9  /* EBCDIC R-Record Buffer           */
	AbdEQSb  = 0xe2  /* EBCDIC S-Search Buffer           */
	AbdEQVb  = 0xe5  /* EBCDIC V-Value Buffer            */
	AbdEQIb  = 0xc9  /* EBCDIC I-ISN Buffer              */
	AbdEQPb  = 0xd7  /* EBCDIC Performance Buffer        */
	AbdEQMb  = 0xd4  /* EBCDIC Multifetch  Buffer        */
	AbdEQUi  = 0xe4  /* EBCDIC User Info                 */
	AbdEQOb  = 0xd6  /* EBCDIC I/O Buffer (internal)     */
	ABdEQXb  = 0xe7  /* EBCDIC CLEX Info Buffer          */
	AbdEQZb  = 0xe9  /* EBCDIC Security Buffer           */
	abdQStd  = (' ') /*      ' ' -at end of ABD (std)    */
	abdQInd  = ('I') /*      I   -indirectly addressed   */
	eAbdQStd = 0x40  /* EBCDIC ' ' at end of ABD (std)   */
	eABdQInd = 0xc9  /* EBCDIC I  indirectly addressed   */
)
const abdLength = 48

// Abd Adabas Buffer definition. Representation of ABD structure in the GO environment.
type Abd struct {
	Abdlen  uint16  /* +00  ABD Length                  */
	Abdver  [2]byte /* +02  Version:                    */
	Abdid   byte    /* +04  Buffer ID:                  */
	Abdrsv1 byte    /* +05  Reserved - must be 0x00     */
	Abdloc  byte    /* +06  Buffer location flag:       */
	Abdrsv2 [9]byte /* +07  Reserved - must be 0x00     */
	Abdsize uint64  /* +10  Buffer Size                 */
	Abdsend uint64  /* +18  Len to send to database     */
	Abdrecv uint64  /* +20  Len received from database  */

	Abdaddr uint64 /* +28  8 byte aligned 64bit Ptr    */
	/*      indirectly to buffer        */
}

// Buffer Adabas Buffer overlay to combine the buffer itself with
// the Adabas buffer definition. It includes the current offset
// of the buffer.
type Buffer struct {
	abd    Abd
	offset int
	buffer []byte
}

// NewBuffer Create a new buffer with given id
func NewBuffer(id byte) *Buffer {
	return &Buffer{
		abd:    Abd{Abdver: [2]byte{abdEyecatcher, abdVersion}, Abdlen: abdLength, Abdid: id, Abdloc: abdQInd},
		offset: 0,
	}
}

// NewBufferWithSize Create a new buffer with given id and predefined size
func NewBufferWithSize(id byte, size uint32) *Buffer {
	b := &Buffer{
		abd:    Abd{Abdver: [2]byte{abdEyecatcher, abdVersion}, Abdlen: abdLength, Abdid: id, Abdloc: abdQInd},
		offset: 0,
	}
	b.Allocate(size)
	return b
}

// If needed, grow the buffer size to new size given
func (adabasBuffer *Buffer) grow(newSize int) {
	adatypes.Central.Log.Debugf("Current %c buffer to %d,%d", adabasBuffer.abd.Abdid, len(adabasBuffer.buffer), cap(adabasBuffer.buffer))
	adatypes.Central.Log.Debugf("Resize buffer to %d", newSize)
	newBuffer := make([]byte, newSize)
	copy(newBuffer, adabasBuffer.buffer)
	adabasBuffer.buffer = newBuffer
	adatypes.Central.Log.Debugf("Growed buffer len=%d cap=%d", len(adabasBuffer.buffer), cap(adabasBuffer.buffer))
	adabasBuffer.abd.Abdsize = uint64(len(adabasBuffer.buffer))
}

// WriteString write string intp buffer
func (adabasBuffer *Buffer) WriteString(content string) {
	adatypes.Central.Log.Debugf("Write string in adabas buffer")
	end := adabasBuffer.offset + len(content)
	if adabasBuffer.offset+len(content) > cap(adabasBuffer.buffer) {
		adabasBuffer.grow(adabasBuffer.offset + len(content))
		adabasBuffer.abd.Abdsize = uint64(adabasBuffer.offset + len(content))
	}
	copy(adabasBuffer.buffer[adabasBuffer.offset:end], content)
	adabasBuffer.offset += len(content)
	adabasBuffer.abd.Abdsend = uint64(adabasBuffer.offset)
}

// WriteBinary write binary slice into buffer
func (adabasBuffer *Buffer) WriteBinary(content []byte) {
	adatypes.Central.Log.Debugf("Write binary in adabas buffer")
	end := adabasBuffer.offset + len(content)
	if adabasBuffer.offset+len(content) > cap(adabasBuffer.buffer) {
		adabasBuffer.grow(end)
		adabasBuffer.abd.Abdsize = uint64(end)
	}

	// Copy content into buffer
	adatypes.Central.Log.Debugf("Copy to range", adabasBuffer.offset, end, len(adabasBuffer.buffer), cap(adabasBuffer.buffer))
	copy(adabasBuffer.buffer[adabasBuffer.offset:], content[:])
	adabasBuffer.offset += len(content)
	adabasBuffer.abd.Abdsend = uint64(adabasBuffer.offset)
}

// Allocate allocate buffer of specified size
func (adabasBuffer *Buffer) Allocate(size uint32) {
	if adabasBuffer.buffer == nil || size != uint32(len(adabasBuffer.buffer)) {
		adabasBuffer.buffer = make([]byte, size)
		adabasBuffer.abd.Abdsize = uint64(len(adabasBuffer.buffer))
	}
}

// Bytes receive buffer content
func (adabasBuffer *Buffer) Bytes() []byte {
	return adabasBuffer.buffer
}

// Position offset to another position in the buffer
func (adabasBuffer *Buffer) position(pos int) int {
	switch {
	case pos < 0:
		adabasBuffer.offset = 0
	case pos > len(adabasBuffer.buffer):
		adabasBuffer.offset = len(adabasBuffer.buffer)
	default:
		adabasBuffer.offset = pos
	}
	return adabasBuffer.offset
}

// Received Number of received bytes
func (adabasBuffer *Buffer) Received() uint64 {
	return adabasBuffer.abd.Abdrecv
}

// Clear buffer emptied
func (adabasBuffer *Buffer) Clear() {
	adabasBuffer.buffer = nil
	adabasBuffer.offset = 0
	adabasBuffer.abd.Abdsize = 0
	adabasBuffer.abd.Abdsend = 0
	adabasBuffer.abd.Abdrecv = 0
}

// String common string representation of the Adabas buffer
func (adabasBuffer *Buffer) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("ABD ID: %c\n  Size: %d\n", adabasBuffer.abd.Abdid, adabasBuffer.abd.Abdsize))
	buffer.WriteString(fmt.Sprintf(" Send: %d  Received: %d\n", adabasBuffer.abd.Abdsend, adabasBuffer.abd.Abdrecv))
	buffer.WriteString(adatypes.FormatByteBuffer("Buffer", adabasBuffer.buffer))
	return buffer.String()
}

// SearchAdabasBuffer returns search buffer of the search tree
func SearchAdabasBuffer(tree *adatypes.SearchTree) *Buffer {
	adabasBuffer := NewBuffer(AbdAQSb)
	sb := tree.SearchBuffer()
	adatypes.Central.Log.Debugf("Search buffer created: %s", sb)
	adabasBuffer.buffer = []byte(sb)
	adabasBuffer.abd.Abdsize = uint64(len(sb))
	adabasBuffer.abd.Abdsend = adabasBuffer.abd.Abdsize
	adatypes.Central.Log.Debugf("Send search buffer of size %d -> send=%d", adabasBuffer.abd.Abdsize,
		adabasBuffer.abd.Abdsend)
	return adabasBuffer
}

// ValueAdabasBuffer returns value buffer of the search tree
func ValueAdabasBuffer(tree *adatypes.SearchTree) *Buffer {
	adabasBuffer := NewBuffer(AbdAQVb)
	var buffer bytes.Buffer
	tree.ValueBuffer(&buffer)
	adabasBuffer.buffer = buffer.Bytes()
	adabasBuffer.abd.Abdsize = uint64(buffer.Len())
	adabasBuffer.abd.Abdsend = adabasBuffer.abd.Abdsize
	return adabasBuffer
}