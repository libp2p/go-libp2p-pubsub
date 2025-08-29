// merkle is a minimal merkle tree commitment scheme to serve as an example and
// test for partial messages.
package merkle

import (
	"bytes"
	"crypto/sha256"
)

func hash(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

// buildMerkleTree computes the Merkle root and build layers for proof generation
func buildMerkleTree(leaves [][]byte) [][][]byte {
	tree := [][][]byte{leaves}
	for len(tree[len(tree)-1]) > 1 {
		level := tree[len(tree)-1]
		var nextLevel [][]byte
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				nextLevel = append(nextLevel, level[i])
			} else {
				combined := append(level[i], level[i+1]...)
				nextLevel = append(nextLevel, hash(combined))
			}
		}
		tree = append(tree, nextLevel)
	}
	return tree
}

func MerkleRoot(leaves [][]byte) []byte {
	tree := buildMerkleTree(leaves)
	return tree[len(tree)-1][0]
}

// Merkle proof element with sibling hash and direction flag (true if sibling is on left)
type ProofStep struct {
	Hash   []byte
	IsLeft bool
}

// Generate Merkle proof for leaf at index
func MerkleProof(leaves [][]byte, index int) []ProofStep {
	tree := buildMerkleTree(leaves)
	proof := []ProofStep{}
	for level := 0; level < len(tree)-1; level++ {
		siblingIndex := 0
		if index%2 == 0 {
			siblingIndex = index + 1
			if siblingIndex >= len(tree[level]) {
				// No sibling, skip
				index /= 2
				continue
			}
			proof = append(proof, ProofStep{Hash: tree[level][siblingIndex], IsLeft: false})
		} else {
			siblingIndex = index - 1
			proof = append(proof, ProofStep{Hash: tree[level][siblingIndex], IsLeft: true})
		}
		index /= 2
	}
	return proof
}

// Verify Merkle proof for leaf, root, and proof steps
func VerifyProof(leaf, root []byte, proof []ProofStep) bool {
	computedHash := leaf
	for _, step := range proof {
		if step.IsLeft {
			computedHash = hash(append(step.Hash, computedHash...))
		} else {
			computedHash = hash(append(computedHash, step.Hash...))
		}
	}
	return bytes.Equal(computedHash, root)
}
