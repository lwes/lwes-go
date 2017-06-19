// Package lwes provides lwes related functionalities.
//  http://www.lwes.org/ (Light Weight Event System)
//  https://github.com/lwes/lwes (Light Weight Event System C library)
//  https://github.com/lwes/lwes-erlang (Light Weight Event System Erlang library, currently the most complete implementation)
package lwes

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
)

// the original types are from https://github.com/lwes/lwes/blob/master/src/lwes_types.c;
// extended types are from https://github.com/lwes/lwes-erlang/blob/master/include/lwes.hrl
const (
	LWES_TYPE_U_INT_16  byte = 1   /*!< 2 byte unsigned integer type */
	LWES_TYPE_INT_16         = 2   /*!< 2 byte signed integer type type */
	LWES_TYPE_U_INT_32       = 3   /*!< 4 byte unsigned integer type */
	LWES_TYPE_INT_32         = 4   /*!< 4 byte signed integer type */
	LWES_TYPE_STRING         = 5   /*!< variable bytes string type */
	LWES_TYPE_IP_ADDR        = 6   /*!< 4 byte ipv4 address type, in the Little Endian order */
	LWES_TYPE_INT_64         = 7   /*!< 8 byte signed integer type */
	LWES_TYPE_U_INT_64       = 8   /*!< 8 byte unsigned integer type */
	LWES_TYPE_BOOLEAN        = 9   /*!< 1 byte boolean type */
	LWES_TYPE_UNDEFINED      = 255 /*!< undefined type */

	// Extended types
	LWES_TYPE_BYTE        = 10
	LWES_TYPE_FLOAT       = 11
	LWES_TYPE_DOUBLE      = 12
	LWES_TYPE_LONG_STRING = 13

	// the array and sparse array types are not supported yet
	LWES_TYPE_U_INT_16_ARRAY = 129
	LWES_TYPE_INT_16_ARRAY   = 130
	LWES_TYPE_U_INT_32_ARRAY = 131
	LWES_TYPE_INT_32_ARRAY   = 132
	LWES_TYPE_STRING_ARRAY   = 133
	LWES_TYPE_IP_ADDR_ARRAY  = 134
	LWES_TYPE_INT_64_ARRAY   = 135
	LWES_TYPE_U_INT_64_ARRAY = 136
	LWES_TYPE_BOOLEAN_ARRAY  = 137
	LWES_TYPE_BYTE_ARRAY     = 138
	LWES_TYPE_FLOAT_ARRAY    = 139
	LWES_TYPE_DOUBLE_ARRAY   = 140

	// the nullable array; can be very sparse
	LWES_TYPE_N_U_INT_16_ARRAY = 141
	LWES_TYPE_N_INT_16_ARRAY   = 142
	LWES_TYPE_N_U_INT_32_ARRAY = 143
	LWES_TYPE_N_INT_32_ARRAY   = 144
	LWES_TYPE_N_STRING_ARRAY   = 145
	// there is no sparse IP_ADDR_ARRAY
	LWES_TYPE_N_INT_64_ARRAY   = 147
	LWES_TYPE_N_U_INT_64_ARRAY = 148
	LWES_TYPE_N_BOOLEAN_ARRAY  = 149
	LWES_TYPE_N_BYTE_ARRAY     = 150
	LWES_TYPE_N_FLOAT_ARRAY    = 151
	LWES_TYPE_N_DOUBLE_ARRAY   = 152
)

func writeLengthStr(buf []byte, min, max int, str string) ([]byte, error) {
	l := len(str)
	if !(min <= l && l <= max) {
		return nil, fmt.Errorf("write str not in length range (%d,%d)", min, max)
	}
	buf = append(buf, byte(l))
	buf = append(buf, str...)
	return buf, nil
}

// from https://github.com/lwes/lwes-erlang/blob/master/src/lwes_event.erl#L587
// the name should not be longer than 127
func writeName(buf []byte, name string) ([]byte, error) {
	return writeLengthStr(buf, 1, 127, name)
}

func writeKey(buf []byte, key string) ([]byte, error) {
	return writeLengthStr(buf, 1, 255, key)
}

type LwesEvent struct {
	Name  string                 // the event name
	Attrs map[string]interface{} // the attrs in a map

	attr_keys []string // save the order of keys, for internal use for debugging
}

// for emitting lwes event, start with NewLwesEvent
// and following by multiple .Set key value pairs
// then .MarshalBinary to get the bytes
func NewLwesEvent(name string) *LwesEvent {
	return &LwesEvent{
		Name:  name,
		Attrs: make(map[string]interface{}),
	}
}

// use NewLwesEvent and Set for events to be encoded
func (lwe *LwesEvent) Set(key string, value interface{}) {
	lwe.attr_keys = append(lwe.attr_keys, key)
	lwe.Attrs[key] = value
}

// the helper to marshal a BinaryMarshaler to bytes (should this be in "encoding" ?)
func Marshal(v encoding.BinaryMarshaler) ([]byte, error) {
	return v.MarshalBinary()
}

var (
	errNameTooLong         = errors.New("name too long")
	errInvalidIPAddr       = errors.New("invalid net.IPv4 address")
	errUnsupportedDataType = errors.New("unsupported data type")
)

// the helper to unmarshal bytes into a BinaryMarshaler (should this be in "encoding" ?)
func Unmarshal(data []byte, v encoding.BinaryUnmarshaler) error {
	return v.UnmarshalBinary(data)

	// v is a pointer to LwesEvent
	// use indirect to set its dereference value to the decoded
	// reflect.Indirect(reflect.ValueOf(v)).Set(reflect.ValueOf(lwe).Elem())
}

// calculate the bytes size needed for constructing a new lwes event
// it's used in MarshalBinary for how many bytes need to allocate
func (lwe *LwesEvent) Size() int {
	// 1. a byte length prefixed string as the message name (<=255 bytes)
	s := 1 + len(lwe.Name)
	// 2. Uint16BE Number of Attrs
	s += 2
	// 3. num of key, value pairs
	//  each key is a byte length prefixed string (<=255 bytes)
	//  each value is a byte tag prefix as value type, followed by the value
	for key, value := range lwe.Attrs {
		s += 1 + len(key)
		switch v := value.(type) {
		case uint16:
			s += 1 + 2
		case int16:
			s += 1 + 2
		case uint32:
			s += 1 + 4
		case int32:
			s += 1 + 4
		case uint64:
			s += 1 + 8
		case int64:
			s += 1 + 8
		case string:
			l := len(v)
			if l <= 65535 {
				s += 1 + 2 + l // short string
			} else {
				s += 1 + 4 + l // long string
			}
		case net.IP:
			s += 1 + 4
		case bool:
			s += 1 + 1
		case byte:
			s += 1 + 1
		case float32:
			s += 1 + 4
		case float64:
			s += 1 + 8
		default:
			// unknown data type
		}
	}

	return s
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (lwe *LwesEvent) MarshalBinary() (buf []byte, err error) {
	buf = make([]byte, 0, lwe.Size())

	if buf, err = writeName(buf, lwe.Name); err != nil {
		return nil, err
	}

	buf = buf[0 : len(buf)+2]
	binary.BigEndian.PutUint16(buf[len(buf)-2:], uint16(len(lwe.attr_keys)))

	for _, key := range lwe.attr_keys {
		if buf, err = writeKey(buf, key); err != nil {
			return nil, err
		}

		value := lwe.Attrs[key]
		switch v := value.(type) {
		case uint16:
			buf = append(buf, LWES_TYPE_U_INT_16)
			buf = buf[0 : len(buf)+2]
			binary.BigEndian.PutUint16(buf[len(buf)-2:], v)
		case int16:
			buf = append(buf, LWES_TYPE_INT_16)
			buf = buf[0 : len(buf)+2]
			binary.BigEndian.PutUint16(buf[len(buf)-2:], uint16(v))
		case uint32:
			buf = append(buf, LWES_TYPE_U_INT_32)
			buf = buf[0 : len(buf)+4]
			binary.BigEndian.PutUint32(buf[len(buf)-4:], v)
		case int32:
			buf = append(buf, LWES_TYPE_INT_32)
			buf = buf[0 : len(buf)+4]
			binary.BigEndian.PutUint32(buf[len(buf)-4:], uint32(v))
		case uint64:
			// binary.BigEndian.PutUint64(bs[:8], v)
			buf = append(buf, LWES_TYPE_U_INT_64)
			buf = buf[0 : len(buf)+8]
			binary.BigEndian.PutUint64(buf[len(buf)-8:], v)
		case int64:
			buf = append(buf, LWES_TYPE_INT_64)
			buf = buf[0 : len(buf)+8]
			binary.BigEndian.PutUint64(buf[len(buf)-8:], uint64(v))
		case string:
			l := len(v)
			if l <= 65535 /* 0xffff, or max of uint16 */ {
				buf = append(buf, LWES_TYPE_STRING)
				buf = buf[0 : len(buf)+2]
				binary.BigEndian.PutUint16(buf[len(buf)-2:], uint16(l))
			} else if l <= 4294967295 /* 0xffffffff, or max of uint32 */ {
				buf = append(buf, LWES_TYPE_LONG_STRING)
				buf = buf[0 : len(buf)+2]
				binary.BigEndian.PutUint32(buf[len(buf)-4:], uint32(l))
			} else {
				return nil, errNameTooLong
			}
			buf = append(buf, v...)

		case net.IP:
			if len(v) != net.IPv4len {
				return nil, errInvalidIPAddr
			}
			// the network bytes are in the reverse order
			buf = append(buf, LWES_TYPE_IP_ADDR, v[3], v[2], v[1], v[0])

		case bool:
			var b byte = 0
			if v { // if the boolean is true
				b = 1
			}
			buf = append(buf, LWES_TYPE_BOOLEAN, b)
		case byte:
			buf = append(buf, LWES_TYPE_BYTE, v)

		case float32:
			buf = append(buf, LWES_TYPE_FLOAT)
			buf = buf[0 : len(buf)+4]
			binary.BigEndian.PutUint32(buf[len(buf)-4:], math.Float32bits(v))
		case float64:
			buf = append(buf, LWES_TYPE_DOUBLE)
			buf = buf[0 : len(buf)+8]
			binary.BigEndian.PutUint64(buf[len(buf)-8:], math.Float64bits(v))

		default:
			return nil, errUnsupportedDataType
		}
	}

	return
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (lwe *LwesEvent) UnmarshalBinary(data []byte) error {
	return parse(data, lwe)
}

// Decode a bytes buffer into a LwesEvent
func parse(buf []byte, lwe *LwesEvent) error {
	// off, tlen := 0, len(buf)
	r := bytes.NewBuffer(buf)

	// 1. a byte length prefixed string as the message name (<=255 bytes)
	b, err := r.ReadByte()
	if err != nil {
		// log.Printf("unable to read byte: 1: %v %#v\n", err, err)
		return err
	}
	// off++

	if r.Len() < int(b) {
		return fmt.Errorf("Unexpected end of msg with remaining: %d bytes from total len: %d", r.Len(), len(buf))
	}
	// lwe := &LwesEvent{Name: name}

	lwe.Name = string(r.Next(int(b)))

	// bstr = make([]byte, blen)
	// off += int(b)

	// 2. Uint16BE Number of Attrs
	if r.Len() < 2 {
		return fmt.Errorf("Unexpected end of msg with remaining: %d bytes from total len: %d", r.Len(), len(buf))
	}
	num := binary.BigEndian.Uint16(r.Next(2))
	// off += int(binary.Size(num))

	// it seems always carried 3 extra fields
	//  for ReceiptTime, SenderIP, and SenderPort
	num += 3
	lwe.Attrs = make(map[string]interface{}, num)
	lwe.attr_keys = make([]string, 0, num)

	// num of key, value pairs
	//  each key is a byte length prefixed string (<=255 bytes)
	//  each value is a byte tag prefix as value type, followed by the value
	for {
		b, err = r.ReadByte()

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		// off++

		if r.Len() < int(b) {
			return fmt.Errorf("Unexpected end of msg with remaining: %d bytes from total len: %d", r.Len(), len(buf))
		}

		key := string(r.Next(int(b)))
		// off += int(b)

		var (
			tag   byte
			value interface{}
		)

		tag, err = r.ReadByte()

		if err != nil {
			// log.Printf("unable to read binary: 2: %v %#v\n", err, err)
			return err
		}
		// off++

		// https://github.com/lwes/lwes/blob/master/src/lwes_types.c
		switch tag {
		case LWES_TYPE_UNDEFINED: // LWES_UNDEFINED_TOKEN
			fallthrough

		default:
			// log.Printf("unknown tag: %d\n", tag)
			return fmt.Errorf("unknown tag: %d, (off:%d, len:%d, err: %#v)\n", tag, len(buf)-r.Len()-1, len(buf), err)

		case LWES_TYPE_U_INT_16: // case 1: // type uint16
			const readLen = 2
			if r.Len() < readLen {
				return fmt.Errorf("Unexpected end of msg with remaining: %d bytes from total len: %d", r.Len(), len(buf))
			}
			value = binary.BigEndian.Uint16(r.Next(readLen))
			// off += binary.Size(value)

		case LWES_TYPE_INT_16: // case 2: // type: int16
			const readLen = 2
			if r.Len() < readLen {
				return fmt.Errorf("Unexpected end of msg with remaining: %d bytes from total len: %d", r.Len(), len(buf))
			}
			value = int16(binary.BigEndian.Uint16(r.Next(readLen)))
			// off += binary.Size(value)

		case LWES_TYPE_U_INT_32: // case 3: // LWES_U_INT_32_TOKEN
			const readLen = 4
			if r.Len() < readLen {
				return fmt.Errorf("Unexpected end of msg with remaining: %d bytes from total len: %d", r.Len(), len(buf))
			}
			value = binary.BigEndian.Uint32(r.Next(readLen))
			// off += binary.Size(value)

		case LWES_TYPE_INT_32: // case 4: // LWES_INT_32_TOKEN
			const readLen = 4
			if r.Len() < readLen {
				return fmt.Errorf("Unexpected end of msg with remaining: %d bytes from total len: %d", r.Len(), len(buf))
			}
			value = int32(binary.BigEndian.Uint32(r.Next(readLen)))
			// off += binary.Size(value)

		case LWES_TYPE_STRING: // case 5: type: long string
			const readLen = 2
			if r.Len() < readLen {
				return fmt.Errorf("Unexpected end of msg with remaining: %d bytes from total len: %d", r.Len(), len(buf))
			}
			blen := binary.BigEndian.Uint16(r.Next(readLen))
			if r.Len() < int(blen) {
				return fmt.Errorf("Unexpected end of msg with remaining: %d bytes from total len: %d", r.Len(), len(buf))
			}
			value = string(r.Next(int(blen)))
			// off += binary.Size(blen) + int(blen)

		case LWES_TYPE_IP_ADDR: // case 6: IP Addr
			const readLen = 4
			if r.Len() < readLen {
				return fmt.Errorf("Unexpected end of msg with remaining: %d bytes from total len: %d", r.Len(), len(buf))
			}

			bval := r.Next(readLen)
			// off += readLen

			// return a 4 bytes slice; net.IP is same as []byte
			// saves memory and better than net.IPv4(...) which always returns 16bytes IPv4

			// this address is in the Little Endian order
			value = net.IP{bval[3], bval[2], bval[1], bval[0]}

		case LWES_TYPE_INT_64: // case 7: type int64
			const readLen = 8
			if r.Len() < readLen {
				return fmt.Errorf("Unexpected end of msg with remaining: %d bytes from total len: %d", r.Len(), len(buf))
			}

			value = int64(binary.BigEndian.Uint64(r.Next(readLen)))
			// off += readLen

		case LWES_TYPE_U_INT_64: // case 8, LWES_U_INT_64_TOKEN
			const readLen = 8
			if r.Len() < readLen {
				return fmt.Errorf("Unexpected end of msg with remaining: %d bytes from total len: %d", r.Len(), len(buf))
			}

			value = binary.BigEndian.Uint64(r.Next(readLen))
			// off += readLen

		case LWES_TYPE_BOOLEAN: // case 9: // LWES_BOOLEAN_TOKEN
			b, err = r.ReadByte()
			if err != nil {
				// log.Printf("unable to read binary: 9: %v %#v\n", err, err)
				return err
			}
			// off++

			// convert to bool: 0 is false; otherwise true
			value = (b != 0x00)
		}

		lwe.attr_keys = append(lwe.attr_keys, key)
		lwe.Attrs[key] = value
	}

	// should be nothing remained
	if r.Len() != 0 {
		return fmt.Errorf("extra bytes %v", r.Bytes())
	}

	// should be reading exactly to the end
	if err == io.EOF {
		return nil
	}

	return err
}

// this print all key/value pairs in the original order
// mainly for debug printing
func (lwe *LwesEvent) FPrint(w io.Writer) {
	fmt.Fprintf(w, "%s[%d]\n", lwe.Name, len(lwe.Attrs))
	fmt.Fprintln(w, "{")

	for _, k := range lwe.attr_keys {
		fmt.Fprintf(w, "\t%s = %v;\n", k, lwe.Attrs[k])
	}

	fmt.Fprintln(w, "}")
}

// Enumerate all key/value pairs in the original order
func (lwe *LwesEvent) Enumerate(callback func(key string, value interface{}) bool) {
	for _, k := range lwe.attr_keys {
		if !callback(k, lwe.Attrs[k]) {
			break
		}
	}
}
