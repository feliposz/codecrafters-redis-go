package main

import (
	"fmt"
	"strconv"
)

const longitudeMax = 180
const longitudeMin = -180
const latitudeMax = 85.05112878
const latitudeMin = -85.05112878

func (srv *serverState) GeoAdd(key string, longStr string, latStr string, member string) (response string) {
	longitude, err := strconv.ParseFloat(longStr, 64)
	if err != nil {
		response = encodeError(fmt.Errorf("invalid longitude argument"))
		return
	}
	latitude, err := strconv.ParseFloat(latStr, 64)
	if err != nil {
		response = encodeError(fmt.Errorf("invalid latitude argument"))
		return
	}

	if longitude < longitudeMin || longitude > longitudeMax {
		response = encodeError(fmt.Errorf("invalid longitude value"))
		return
	}
	if latitude < latitudeMin || latitude > latitudeMax {
		response = encodeError(fmt.Errorf("invalid latitude value"))
		return
	}
	var score float64
	set := srv.getSortedSet(key, true)
	count := set.Put(score, member)
	response = encodeInt(count)
	return
}
