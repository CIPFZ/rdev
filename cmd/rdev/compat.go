package main

import (
	"encoding/json"
	"os"

	"github.com/CIPFZ/rdev/internal/compat"
)

func cmdCompat() error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(compat.Current())
}
