package floodsub

import (
	pb "github.com/libp2p/go-floodsub/pb"

	crypto "github.com/libp2p/go-libp2p-crypto"
)

func verifyMessageSignature(m *pb.Message) error {
	return nil
}

func signMessage(key crypto.PrivKey, m *pb.Message) {

}
