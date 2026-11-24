package config

import "fmt"

// RecreateLostSlotsVar names the environment variable that lets the service
// create a slot Postgres invalidated again. Off by default: revocations in
// the WAL the old slot lost are never read, so a revoked session's token
// could stream from the new slot until it expires.
const RecreateLostSlotsVar = "PCS_RECREATE_LOST_SLOTS"

// RecreateLostSlots reads RecreateLostSlotsVar through lookup, such as
// os.LookupEnv: unset is false, and anything but true or false is an error.
func RecreateLostSlots(lookup func(string) (string, bool)) (bool, error) {
	v, ok := lookup(RecreateLostSlotsVar)
	if !ok {
		return false, nil
	}
	switch v {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, fmt.Errorf("%s must be true or false", RecreateLostSlotsVar)
}
