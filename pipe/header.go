package pipe

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

//Header represent file metadata in the beginning of the file
type Header struct {
	Format    string
	Filters   []string `json:",omitempty"`
	Schema    []byte   `json:",omitempty"`
	Delimited bool     `json:",omitempty"`
	HMAC      string   `json:"HMAC-SHA256,omitempty"`
	IV        string   `json:"AES256-CFB-IV,omitempty"`
}

func writeHeader(header *Header, hash []byte, f io.Writer) error {
	if len(hash) != 0 {
		header.HMAC = fmt.Sprintf("%x", hash)
	}

	h, err := json.Marshal(header)
	if err != nil {
		return err
	}

	h = append(h, delimiter)

	_, err = f.Write(h)

	return err
}

func readHeader(r *bufio.Reader) (*Header, error) {
	h, err := r.ReadBytes(delimiter)
	if err != nil {
		return nil, err
	}

	u := &Header{}

	err = json.Unmarshal(h, u)
	if err != nil {
		return nil, err
	}

	return u, nil
}
