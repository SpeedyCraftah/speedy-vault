package handlers

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net"
	"reflect"
	"strconv"
	"strings"
)

// Converts an ASCII number represented inside of a []byte into an unsigned integer.
func (MiscHandler) Btoui64(value []byte) (uint64, error) {
	if len(value) == 0 {
		return 0, errors.New("Empty digit value");
	}
	
	var count uint64 = 0;
	for _, c := range value {
		if c < '0' || c > '9' {
			return 0, errors.New("Invalid digits");
		}

		count = (count * 10) + uint64(c - '0');
	}

	return count, nil;
}


type ParsedRangeHeader struct {
	Start uint64;
	Length uint64;
	ContentRangeHeader string;
}

// TODO: support multiple ranges?
var ParseRangeUnsatisfiableError error = errors.New("Range cannot be satisfied");

// Parses a "Range" header from the client and transforms it into a starting index and length of what should be read from the file.
// If the header is invalid, an error will be returned which means the header should be ignored.
// If the header's range cannot be satisfied, an error of type ParseRangeUnsatisfiableError will be returned, which can be relayed to the client as a 416 status code.
// The struct also contains a "ContentRangeHeader" value which contains the "Content-Range" header that should be set; this is zero-initialized on error EXCEPT for an error of type ParseRangeUnsatisfiableError.
func (MiscHandler) ParseRangeHeader(header []byte, size uint64) (ParsedRangeHeader, error) {
	GenerateUnsatisfiableRangedHeader := func(size uint64) string {
		var b strings.Builder;
		b.Grow(28);
		b.WriteString("bytes */");
		b.WriteString(strconv.FormatUint(size, 10));

		return b.String();
	};

	GenerateRangedHeader := func(size uint64, startByte uint64, length uint64) string {
		var b strings.Builder;
		b.Grow(40);
		b.WriteString("bytes ");
		b.WriteString(strconv.FormatUint(startByte, 10));
		b.WriteRune('-');
		b.WriteString(strconv.FormatUint(startByte + length - 1, 10));
		b.WriteRune('/');
		b.WriteString(strconv.FormatUint(size, 10));

		return b.String();
	};
	
	if !bytes.HasPrefix(header, []byte("bytes=")) {
		return ParsedRangeHeader{}, errors.New("Unexpected prefix");
	}

	// Header is guaranteed to at least be 6 bytes from the above prefix check, hence won't panic.
	values := bytes.Split(header[6:], []byte("-"));
	if len(values) != 2 {
		return ParsedRangeHeader{}, errors.New("Invalid range format");
	}

	startIncluded := len(values[0]) != 0;
	endIncluded := len(values[1]) != 0;

	// If both the start and end was included, this wants us to return the bytes in that range.
	if startIncluded && endIncluded {
		startIndex, err := Misc.Btoui64(values[0]);
		if err != nil {
			return ParsedRangeHeader{}, err;
		}

		endIndex, err := Misc.Btoui64(values[1]);
		if err != nil {
			return ParsedRangeHeader{}, err;
		}

		// If this range is impossible to fulfil.
		if startIndex > endIndex || startIndex >= size || endIndex >= size {
			return ParsedRangeHeader{ ContentRangeHeader: GenerateUnsatisfiableRangedHeader(size) }, ParseRangeUnsatisfiableError;
		}

		length := 1 + endIndex - startIndex;
		return ParsedRangeHeader{ Start: startIndex, Length: length, ContentRangeHeader: GenerateRangedHeader(size, startIndex, length) }, nil;
	}

	// If only the start is included, this means we need to read all bytes starting at that index to the end.
	if startIncluded {
		startIndex, err := Misc.Btoui64(values[0]);
		if err != nil {
			return ParsedRangeHeader{}, err;
		}

		// If this range is impossible to fulfil.
		if startIndex >= size {
			return ParsedRangeHeader{ ContentRangeHeader: GenerateUnsatisfiableRangedHeader(size) }, ParseRangeUnsatisfiableError;
		}

		length := size - startIndex;
		return ParsedRangeHeader{ Start: startIndex, Length: length, ContentRangeHeader: GenerateRangedHeader(size, startIndex, length) }, nil;
	}

	// If only the end is included, this means we need to read the last X bytes.
	if endIncluded {
		endIndex, err := Misc.Btoui64(values[1]);
		if err != nil {
			return ParsedRangeHeader{}, err;
		}

		// If this range is impossible to fulfil.
		if endIndex == 0 || endIndex > size {
			return ParsedRangeHeader{ ContentRangeHeader: GenerateUnsatisfiableRangedHeader(size) }, ParseRangeUnsatisfiableError;
		}

		startByte := size - endIndex;
		return ParsedRangeHeader{ Start: startByte, Length: endIndex, ContentRangeHeader: GenerateRangedHeader(size, startByte, endIndex) }, nil;
	}

	// Both the start and the end weren't specified, which isn't allowed. 
	return ParsedRangeHeader{}, errors.New("Invalid range format");
}

// Inside of a hijacked request, FastHTTP returns an annoying interface wrapper around the actual connection.
// This retrieves the underlying connection, which is useful for low-level access to the socket.
func (MiscHandler) ExtractNetTCPFromFastHTTPWrapper(conn net.Conn) *net.TCPConn {
	v := reflect.ValueOf(conn).Elem();
    embedded := v.Field(0);
	
    c, ok := embedded.Interface().(*net.TCPConn)
    if !ok {
        panic("Could not convert FastHTTP connection wrapper to net.Conn");
    }

	return c;
}


// A thread safe method to generate a unique identifier for a file.
// This uses a secure PRNG which is slower, but performs much better in concurrent scenarios.
func (MiscHandler) NewRandomUID() string {
	buffer := make([]byte, 16);
	rand.Read(buffer);
	
	return base64.RawURLEncoding.EncodeToString(buffer);
}

type MiscHandler struct{};
var Misc = MiscHandler{};