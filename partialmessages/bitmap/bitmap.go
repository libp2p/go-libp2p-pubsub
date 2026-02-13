package bitmap

import (
	"math/bits"
)

type Bitmap []byte

func NewBitmapWithOnesCount(n int) Bitmap {
	b := make(Bitmap, (n+7)/8)
	for i := range n {
		b.Set(i)
	}
	return b
}

func Merge(left, right Bitmap) Bitmap {
	out := make(Bitmap, max(len(left), len(right)))
	out.Or(left)
	out.Or(right)
	return out
}

func (b Bitmap) IsZero() bool {
	for i := range b {
		if b[i] != 0 {
			return false
		}
	}
	return true
}

func (b Bitmap) OnesCount() int {
	var count int
	for i := range b {
		count += bits.OnesCount8(b[i])
	}
	return count
}

func (b Bitmap) Set(index int) {
	for len(b)*8 <= index {
		b = append(b, 0)
	}
	b[index/8] |= 1 << (uint(index) % 8)
}

func (b Bitmap) Get(index int) bool {
	if index >= len(b)*8 {
		return false
	}
	return b[index/8]&(1<<(uint(index%8))) != 0
}

func (b Bitmap) Clear(index int) {
	if index >= len(b)*8 {
		return
	}
	b[index/8] &^= 1 << (uint(index) % 8)
}

func (b Bitmap) And(other Bitmap) {
	for i := range min(len(b), len(other)) {
		b[i] &= other[i]
	}
}

func (b Bitmap) Or(other Bitmap) {
	for i := range min(len(b), len(other)) {
		b[i] |= other[i]
	}
}

func (b Bitmap) Xor(other Bitmap) {
	for i := range min(len(b), len(other)) {
		b[i] ^= other[i]
	}
}

func (b Bitmap) Flip() {
	for i := range b {
		b[i] ^= 0xff
	}
}
