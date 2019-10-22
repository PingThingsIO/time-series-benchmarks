package iface

type MaterializePMUParams struct {
	//How many streams to materialize
	NumStreams int
	//How much time to materialize (nanoseconds)
	Timespan uint64
	//Zero is equivalent to 1 (no subsample). Final frequency will be 120/SubSample
	SubSample int
	//Should timestamps contain jitter
	TSJitter bool
	//Should values be truncated to 32 bits
	TruncateValue bool
	//How many points should be delivered in each array over the channel
	BatchSize int
}

type DataSource interface {
	MaterializePMU(p *MaterializePMUParams) []chan []Point
	StartTime() int64
	EndTime() int64
}
