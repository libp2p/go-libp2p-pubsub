package pubsub

import (
	"testing"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
)

func TestSigning(t *testing.T) {
	privk, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		t.Fatal(err)
	}
	testSignVerify(t, privk)

	privk, _, err = crypto.GenerateKeyPair(crypto.Ed25519, 0)
	if err != nil {
		t.Fatal(err)
	}
	testSignVerify(t, privk)
}

func testSignVerify(t *testing.T, privk crypto.PrivKey) {
	id, err := peer.IDFromPublicKey(privk.GetPublic())
	if err != nil {
		t.Fatal(err)
	}
	topic := "foo"
	m := pb.Message{
		Data:  []byte("abc"),
		Topic: &topic,
		From:  []byte(id),
		Seqno: []byte("123"),
	}
	signMessage(id, privk, &m)
	err = verifyMessageSignature(&m)
	if err != nil {
		t.Fatal(err)
	}
}
