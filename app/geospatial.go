package main

import (
	"fmt"
	"math"
	"strconv"
)

const longitudeMax = 180
const longitudeMin = -180
const latitudeMax = 85.05112878
const latitudeMin = -85.05112878
const longitudeRange = longitudeMax - longitudeMin
const latitudeRange = latitudeMax - latitudeMin

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
	score := GeoHash(longitude, latitude)
	set := srv.getSortedSet(key, true)
	count := set.Put(score, member)
	response = encodeInt(count)
	return
}

const hashBits = 26

func GeoHash(longitude, latitude float64) float64 {
	normRange := math.Pow(2, hashBits)
	normLong := normRange * (longitude - longitudeMin) / longitudeRange
	normLat := normRange * (latitude - latitudeMin) / latitudeRange
	truncLong := uint32(normLong)
	truncLat := uint32(normLat)
	interleaved := interleaveBits(truncLat, truncLong)
	return float64(interleaved)
}

func interleaveBits(x, y uint32) (result uint64) {
	for i := 0; i < hashBits+1; i++ {
		result |= uint64(x>>i&1) << (i * 2)
		result |= uint64(y>>i&1) << (i*2 + 1)
	}
	return
}

func (srv *serverState) GeoPos(key string, members []string) string {
	set := srv.getSortedSet(key, true)
	var result []any
	for _, member := range members {
		rank := set.GetRank(member)
		if rank != -1 {
			result = append(result, []string{"0", "0"})
		} else {
			result = append(result, nil)
		}
	}
	return encodeArray(result)
}
