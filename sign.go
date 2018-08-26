package floodsub

import (
	"fmt"

	pb "github.com/libp2p/go-floodsub/pb"

	crypto "github.com/libp2p/go-libp2p-crypto"
	peer "github.com/libp2p/go-libp2p-peer"
)

func verifyMessageSignature(m *pb.Message) error {
	var pubk crypto.PubKey

	pid, err := peer.IDFromBytes(m.From)
	if err != nil {
		return err
	}

	if m.Key == nil {
		// no attached key, it must be extractable from the source ID
		pubk, err = pid.ExtractPublicKey()
		if err != nil {
			return err
		}
	} else {
		pubk, err = crypto.UnmarshalPublicKey(m.Key)
		if err != nil {
			return err
		}

		// verify that the source ID matches the attached key
		xpid, err := peer.IDFromPublicKey(pubk)
		if err != nil {
			return err
		}

		if pid != xpid {
			return fmt.Errorf("bad signing key; source ID/key mismatch: %s %s", pid, xpid)
		}
	}

	xm := pb.Message{
		Data:     m.Data,
		TopicIDs: m.TopicIDs,
		From:     m.From,
		Seqno:    m.Seqno,
	}
	bytes, err := xm.Marshal()
	if err != nil {
		return err
	}

	valid, err := pubk.Verify(bytes, m.Signature)
	if err != nil {
		return err
	}

	if !valid {
		return fmt.Errorf("invalid signature")
	}

	return nil
}

func signMessage(key crypto.PrivKey, m *pb.Message) error {
	bytes, err := m.Marshal()
	if err != nil {
		return err
	}

	sig, err := key.Sign(bytes)
	if err != nil {
		return err
	}

	m.Signature = sig
	switch key.Type() {
	case crypto.RSA:
		pubk, err := key.GetPublic().Bytes()
		if err != nil {
			return err
		}
		m.Key = pubk
	}
	return nil
}
