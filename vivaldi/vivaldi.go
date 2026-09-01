// Package vivaldi implements the Vivaldi/Newton update logic for virtual coordinates.
package vivaldi

import (
	"fmt"
	"math"
	"math/rand"
)

// Coord represents a 2D Euclidean coordinate with a height component.
type Coord struct {
	X float64
	Y float64
	H float64 // Height
}

// VivaldiState holds the local coordinate and error estimate.
type VivaldiState struct {
	Coord Coord
	Error float64
}

// VivaldiUpdateParams holds the parameters for a single update step.
type VivaldiUpdateParams struct {
	Local            VivaldiState // Our node's state
	Remote           VivaldiState // State from other peer
	RTT              float64      // Measured RTT (ms)
	Ce               float64      // Error correction constant
	Cc               float64      // Coordinate correction constant
	Newton           bool         // If true, apply Newton-Vivaldi security checks
	OutlierThreshold float64      // Optional outlier threshold on force magnitude (ms)
}

// UpdateResult holds the outcome of a single Vivaldi coordinate update,
// including the new coordinate and error plus diagnostic force and prediction
// values.
type UpdateResult struct {
	NewCoord       Coord
	NewError       float64
	Weight         float64
	SampleRelError float64
	ForceVector    Coord
	ForceMagnitude float64
	PredictedRTTMS float64
}

// UpdateVivaldi performs the Vivaldi Algorithm 1 update with height-vector operations.
func UpdateVivaldi(params VivaldiUpdateParams) (UpdateResult, error) {
	if params.RTT <= 0 {
		return UpdateResult{}, fmt.Errorf("vivaldi: non-positive RTT %.4fms", params.RTT)
	}
	if params.Ce <= 0 || params.Cc <= 0 {
		return UpdateResult{}, fmt.Errorf("vivaldi: invalid constants ce=%.4f cc=%.4f", params.Ce, params.Cc)
	}

	// Height-vector distance prediction: ||[xi - xj, hi + hj]||
	diff := hvSub(params.Local.Coord, params.Remote.Coord)
	predDist := hvNorm(diff)
	forceMag := params.RTT - predDist

	// Newton outlier guard if requested.
	if params.Newton && params.OutlierThreshold > 0 && math.Abs(forceMag) > params.OutlierThreshold {
		return UpdateResult{}, fmt.Errorf(
			"newton: outlier detected (|force|=%.4f > threshold=%.4f)",
			math.Abs(forceMag), params.OutlierThreshold,
		)
	}

	// Algorithm 1: w = ei / (ei + ej)
	totalErr := params.Local.Error + params.Remote.Error
	if totalErr <= 0 {
		totalErr = 1e-6
	}
	w := params.Local.Error / totalErr
	if w < 0 {
		w = 0
	}
	if w > 1 {
		w = 1
	}

	// e_s = |pred-rtt| / rtt
	es := math.Abs(predDist-params.RTT) / params.RTT

	// alpha = ce * w
	alpha := params.Ce * w
	if alpha < 0 {
		alpha = 0
	}
	if alpha > 1 {
		alpha = 1
	}

	// e_i = alpha*e_s + (1-alpha)*e_i
	newErr := alpha*es + (1-alpha)*params.Local.Error
	if newErr < 1e-6 {
		newErr = 1e-6
	}

	// delta = cc * w
	delta := params.Cc * w

	// x_i = x_i + delta * (rtt - ||xi-xj||) * u(xi-xj)
	u := hvUnit(diff)
	forceVec := hvScale(u, forceMag)
	step := hvScale(u, delta*forceMag)
	newCoord := hvAdd(params.Local.Coord, step)
	if newCoord.H < 0 {
		newCoord.H = 0
	}

	return UpdateResult{
		NewCoord:       newCoord,
		NewError:       newErr,
		Weight:         w,
		SampleRelError: es,
		ForceVector:    forceVec,
		ForceMagnitude: math.Abs(forceMag),
		PredictedRTTMS: predDist,
	}, nil
}

func euclideanDist(a, b Coord) float64 {
	dx := a.X - b.X
	dy := a.Y - b.Y
	return math.Sqrt(dx*dx + dy*dy)
}

func hvSub(a, b Coord) Coord {
	return Coord{
		X: a.X - b.X,
		Y: a.Y - b.Y,
		H: a.H + b.H,
	}
}

func hvAdd(a, b Coord) Coord {
	return Coord{
		X: a.X + b.X,
		Y: a.Y + b.Y,
		H: a.H + b.H,
	}
}

func hvScale(a Coord, s float64) Coord {
	return Coord{
		X: a.X * s,
		Y: a.Y * s,
		H: a.H * s,
	}
}

func hvNorm(a Coord) float64 {
	return math.Sqrt(a.X*a.X+a.Y*a.Y) + a.H
}

func hvUnit(a Coord) Coord {
	n := hvNorm(a)
	if n == 0 {
		// As in Vivaldi, break ties randomly if colocated.
		theta := rand.Float64() * 2 * math.Pi
		return Coord{X: math.Cos(theta), Y: math.Sin(theta), H: 0}
	}
	return hvScale(a, 1.0/n)
}

func hvDot(a, b Coord) float64 {
	return a.X*b.X + a.Y*b.Y + a.H*b.H
}

func hvProjection(a, unitDir Coord) Coord {
	return hvScale(unitDir, hvDot(a, unitDir))
}

func hvDiff(a, b Coord) Coord {
	return Coord{X: a.X - b.X, Y: a.Y - b.Y, H: a.H - b.H}
}

// Distance returns the Vivaldi height-vector distance between two coordinates:
// the Euclidean distance between their planar components plus both heights.
func Distance(a, b Coord) float64 {
	return euclideanDist(a, b) + a.H + b.H
}

func median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	tmp := append([]float64(nil), vals...)
	sortFloat64s(tmp)
	n := len(tmp)
	if n%2 == 1 {
		return tmp[n/2]
	}
	return (tmp[n/2-1] + tmp[n/2]) / 2
}

func mad(vals []float64, med float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	dev := make([]float64, len(vals))
	for i, v := range vals {
		dev[i] = math.Abs(v - med)
	}
	return median(dev)
}

func sortFloat64s(v []float64) {
	for i := 1; i < len(v); i++ {
		x := v[i]
		j := i - 1
		for ; j >= 0 && v[j] > x; j-- {
			v[j+1] = v[j]
		}
		v[j+1] = x
	}
}
