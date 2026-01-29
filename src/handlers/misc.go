package handlers

import (
	"crypto/rand"
	"encoding/base64"
)

// A thread safe method to generate a unique identifier for a file.
// This uses a secure PRNG which is slower, but performs much better in concurrent scenarios.
func (MiscHandler) NewRandomUID() string {
	buffer := make([]byte, 16);
	rand.Read(buffer);
	
	return base64.RawURLEncoding.EncodeToString(buffer);
}

type MiscHandler struct{};
var Misc = MiscHandler{};