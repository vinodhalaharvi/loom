package loom

import "fmt"

// errMissing reports a required Options field that was left empty.
func errMissing(field string) error {
	return fmt.Errorf("loom: %s is required", field)
}
