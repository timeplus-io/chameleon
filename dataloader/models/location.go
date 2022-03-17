package models

import (
	"math/rand"
	"time"
)

// North-of-China
// (40, 112) -> (40, 118)
// (38, 112) -> (38, 118)

// East-of-China
// (32, 118) -> (32, 122)
// (28, 118) -> (28, 122)

// Middle-of-China
// (32, 110) -> (32, 117)
// (28, 110) -> (28, 117)

// South-of-China
// (24, 106) -> (24, 116)
// (22, 106) -> (22, 116)

type LatLon struct {
	Lat  float32 `json:"lat"`
	Lon  float32 `json:"lon"`
	City string  `json:"city"`
}

type region struct {
	leftUp    LatLon
	rightUp   LatLon
	leftDown  LatLon
	rightDown LatLon
}

var regionMap map[string][]region

func init() {
	regionMap = make(map[string][]region)

	bj := "BeiJing"
	ts := "TangShan"
	sjz := "ShiJiaZhuang"
	zjk := "ZhangJiaKou"
	bd := "BaoDing"

	regionMap["north-of-china"] = []region{
		{
			// BeiJin
			LatLon{40.014126, 116.241441, bj}, LatLon{40.014126, 116.527459, bj},
			LatLon{39.835775, 116.241441, bj}, LatLon{39.835775, 116.527459, bj},
		},
		{
			// TangShan
			LatLon{39.691410, 118.111948, ts}, LatLon{39.691410, 118.391853, ts},
			LatLon{39.524439, 118.111948, ts}, LatLon{39.524439, 118.391853, ts},
		},
		{
			// ShiJiaZhuang
			LatLon{38.086184, 114.429500, sjz}, LatLon{38.086184, 114.605516, sjz},
			LatLon{37.914092, 114.429500, sjz}, LatLon{37.914092, 114.605516, sjz},
		},
		{
			// ZhangJiaKou
			LatLon{40.785688, 114.782518, zjk}, LatLon{40.785688, 115.064669, zjk},
			LatLon{40.612707, 114.782518, zjk}, LatLon{40.612707, 115.064669, zjk},
		},
		{
			// BaoDing
			LatLon{38.928030, 115.339878, bd}, LatLon{38.928030, 115.617934, bd},
			LatLon{38.754821, 115.339878, bd}, LatLon{38.754821, 115.617934, bd},
		},
	}

	sh := "Shanghai"
	sz := "SuZhou"
	hz := "HangZhou"
	nb := "NingBo"
	nj := "NanJing"

	regionMap["east-of-china"] = []region{
		{
			// ShangHai
			LatLon{31.274066, 121.338275, sh}, LatLon{31.274066, 121.614325, sh},
			LatLon{31.103254, 121.338275, sh}, LatLon{31.103254, 121.614325, sh},
		},
		{
			// SuZhou
			LatLon{31.315470, 120.517825, sz}, LatLon{31.315470, 120.794325, sz},
			LatLon{31.143254, 120.517825, sz}, LatLon{31.143254, 120.794325, sz},
		},
		{
			// HangZhou
			LatLon{30.318831, 120.104833, hz}, LatLon{30.318831, 120.384325, hz},
			LatLon{30.143254, 120.104833, hz}, LatLon{30.143254, 120.384325, hz},
		},
		{
			// NingBo
			LatLon{29.896547, 121.499437, nb}, LatLon{29.896547, 121.774325, nb},
			LatLon{29.723254, 121.499437, nb}, LatLon{30.143254, 121.774325, nb},
		},
		{
			// Nanjing
			LatLon{32.061658, 118.740244, nj}, LatLon{32.061658, 119.020244, nj},
			LatLon{31.891658, 118.740244, nj}, LatLon{31.891658, 119.024325, nj},
		},
	}

	cd := "ChengDu"
	cq := "ChongQing"
	cs := "ChangSha"
	wh := "WuHan"
	nc := "NanChang"

	regionMap["middle-of-china"] = []region{
		{
			// ChengDu
			LatLon{30.700643, 103.965655, cd}, LatLon{30.700643, 104.245655, cd},
			LatLon{30.630643, 103.965655, cd}, LatLon{30.630643, 104.245655, cd},
		},
		{
			// ChongQing
			LatLon{29.489695, 106.843546, cq}, LatLon{29.489695, 107.123546, cq},
			LatLon{29.319695, 106.843546, cq}, LatLon{29.319695, 107.123546, cq},
		},
		{
			// ChangSha
			LatLon{28.231280, 112.897382, cs}, LatLon{28.231280, 113.177382, cs},
			LatLon{28.061280, 112.897382, cs}, LatLon{28.061280, 113.177382, cs},
		},
		{
			// WuHan
			LatLon{30.639306, 114.074692, wh}, LatLon{30.639306, 114.074692, wh},
			LatLon{30.469306, 114.074692, wh}, LatLon{30.469306, 114.074692, wh},
		},
		{
			// NanChang
			LatLon{28.704473, 115.775266, nc}, LatLon{28.704473, 116.055266, nc},
			LatLon{28.534473, 115.775266, nc}, LatLon{28.534473, 116.055266, nc},
		},
	}

	gz := "GuangZhou"
	dg := "DongGuan"
	nn := "NanNing"
	sg := "ShaoGuan"

	// south-of-china
	regionMap["south-of-china"] = []region{
		{
			// Guangzhou
			LatLon{23.140674, 113.232065, gz}, LatLon{23.140674, 113.512065, gz},
			LatLon{22.970674, 113.232065, gz}, LatLon{22.970674, 113.512065, gz},
		},
		{
			// Dongguan
			LatLon{23.072614, 113.634543, dg}, LatLon{23.072614, 113.914543, dg},
			LatLon{22.900674, 113.634543, dg}, LatLon{22.900674, 113.914543, dg},
		},
		{
			// Nanning
			LatLon{22.855866, 108.249357, nn}, LatLon{22.855866, 108.529357, nn},
			LatLon{22.685866, 108.249357, nn}, LatLon{22.685866, 108.529357, nn},
		},
		{
			// Shaoguan
			LatLon{24.832381, 113.514673, sg}, LatLon{24.832381, 113.794673, sg},
			LatLon{24.662381, 113.514673, sg}, LatLon{24.662381, 113.794673, sg},
		},
	}
}

func generateLocation(regions []region, centerized bool) LatLon {
	rand.Seed(time.Now().UTC().UnixNano())
	r := regions[rand.Uint32()%uint32(len(regions))]

	latDelta := r.leftUp.Lat - r.leftDown.Lat
	lonDelta := r.rightUp.Lon - r.leftUp.Lon

	low := float32(0.0)
	high := float32(1.0)
	if centerized {
		low = 0.45
		high = 0.55
	}

	lat := r.leftDown.Lat + latDelta*randRangeFloat32(low, high)
	lon := r.leftDown.Lon + lonDelta*randRangeFloat32(low, high)

	return LatLon{lat, lon, r.leftUp.City}
}

func GenerateLocations(totalLocations uint32, centerized bool) map[string][]LatLon {
	allLocations := make(map[string][]LatLon)
	n := int(totalLocations) / len(regionMap)
	if n == 0 {
		n = 1
	}

	for k, v := range regionMap {
		var locations []LatLon
		for i := 0; i < n; i++ {
			locations = append(locations, generateLocation(v, centerized))
		}
		allLocations[k] = locations
	}
	return allLocations
}

func randRangeFloat32(low float32, high float32) float32 {
	for {
		f := rand.Float32()
		if f >= low && f <= high {
			return f
		}
	}
}
