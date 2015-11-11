package pg

import "testing"

func BenchmarkLUintSwitch(b *testing.B) {
	bs := []byte{0x21, 0x43, 0x65, 0x87}
	expected := uint64(0x87654321)

	for i := 0; i < b.N; i++ {
		if actual := luintSwitch(bs); actual != expected {
			b.Fatalf("expected %.8X but parsed %.8X", expected, actual)
		}
	}
}

func BenchmarkLUintLoop(b *testing.B) {
	bs := []byte{0x21, 0x43, 0x65, 0x87}
	expected := uint64(0x87654321)

	for i := 0; i < b.N; i++ {
		if actual := luintLoop(bs); actual != expected {
			b.Fatalf("expected %.8X but parsed %.8X", expected, actual)
		}
	}
}
