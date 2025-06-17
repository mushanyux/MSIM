package msutil

import (
	"crypto/rand"
	"io"

	"golang.org/x/crypto/curve25519"
)

// GetCurve25519KeyPair GetCurve25519KeyPair
func GetCurve25519KeyPair() (Aprivate, Apublic [32]byte) {
	//产生随机数
	if _, err := io.ReadFull(rand.Reader, Aprivate[:]); err != nil {
		panic(err)
	}
	curve25519.ScalarBaseMult(&Apublic, &Aprivate)
	return
}

// GetCurve25519Key GetCurve25519Key
func GetCurve25519Key(private, public [32]byte) (Key [32]byte) {
	//产生随机数
	curve25519.ScalarMult(&Key, &private, &public)
	return
}
