package main

import (
	"fmt"
	"math"
	"strconv"
)

const longitudeMax = 180.0
const longitudeMin = -180.0
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

const hashExponent = 26

func GeoHash(longitude, latitude float64) float64 {
	normRange := math.Pow(2, hashExponent)
	normLong := normRange * (longitude - longitudeMin) / longitudeRange
	normLat := normRange * (latitude - latitudeMin) / latitudeRange
	truncLong := uint32(normLong)
	truncLat := uint32(normLat)
	interleaved := interleaveBits(truncLat, truncLong)
	return float64(interleaved)
}

func GeoDecode(encoded float64) (longitude, latitude float64) {
	truncLat, truncLong := deinterleaveBits(uint64(encoded))
	normLong := float64(truncLong)
	normLat := float64(truncLat)
	normRange := math.Pow(2, hashExponent)
	gridLongMin := normLong/normRange*longitudeRange + longitudeMin
	gridLatMin := normLat/normRange*latitudeRange + latitudeMin
	gridLongMax := (normLong+1)/normRange*longitudeRange + longitudeMin
	gridLatMax := (normLat+1)/normRange*latitudeRange + latitudeMin
	longitude = (gridLongMin + gridLongMax) / 2
	latitude = (gridLatMin + gridLatMax) / 2
	return
}

func interleaveBits(x, y uint32) (result uint64) {
	for i := 0; i < 32; i++ {
		result |= uint64(x>>i&1) << (i * 2)
		result |= uint64(y>>i&1) << (i*2 + 1)
	}
	return
}

func deinterleaveBits(x uint64) (a, b uint32) {
	for i := 0; i < 32; i++ {
		a |= uint32(x>>(i*2)&1) << i
		b |= uint32(x>>(i*2+1)&1) << i
	}
	return
}

func (srv *serverState) GeoPos(key string, members []string) string {
	set := srv.getSortedSet(key, true)
	var result []any
	for _, member := range members {
		score := set.GetScore(member)
		if score != -1 {
			longitude, latitude := GeoDecode(score)
			latStr := fmt.Sprintf("%.10f", latitude)
			longStr := fmt.Sprintf("%.10f", longitude)
			result = append(result, []string{longStr, latStr})
		} else {
			result = append(result, nil)
		}
	}
	return encodeArray(result)
}
