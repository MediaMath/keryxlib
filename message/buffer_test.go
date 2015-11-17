package message

import "testing"

func TestSmallSizeItemIsPadded(t *testing.T) {
	testAddWithSize(t, 8)
}

func TestNormalSizeItem(t *testing.T) {
	testAddWithSize(t, 10)
}

func TestLargeSizeItemIsTruncated(t *testing.T) {
	testAddWithSize(t, 12)
}

func TestZeroMemoryLimit(t *testing.T) {
	testAddWithLimit(t, 0)
}

func TestInsufficientMemoryLimit(t *testing.T) {
	testAddWithLimit(t, 555)
}

func TestSufficientMemoryLimit(t *testing.T) {
	testAddWithLimit(t, 1000)
}

func TestGenerousMemoryLimit(t *testing.T) {
	testAddWithLimit(t, 2000)
}

func testAddWithSize(t *testing.T, originalSize uint64) {
	const (
		memoryLimit          = 100
		expectedSize         = 10
		itemKey              = 1
		repeatedNonZeroValue = 1
	)

	b := NewBuffer(".", memoryLimit, expectedSize)
	defer b.initialize()

	var bs []byte
	for i := uint64(0); i < originalSize; i++ {
		bs = append(bs, repeatedNonZeroValue)
	}

	b.Add(itemKey, bs)
	out := b.Remove(itemKey)

	if len(out) != 1 {
		t.Fatal("expected exactly one value")
	}

	actualSize := len(out[0])
	if actualSize != expectedSize {
		t.Fatal("expected ", expectedSize, " but got ", actualSize)
	}

	actualNonZeroByteCount := uint64(0)
	for _, b := range out[0] {
		if b != 0 {
			actualNonZeroByteCount++
		}
	}

	expectedNonZeroByteCount := originalSize
	if originalSize > expectedSize {
		expectedNonZeroByteCount = expectedSize
	}
	if actualNonZeroByteCount != expectedNonZeroByteCount {
		t.Fatal("expected ", expectedNonZeroByteCount, " bytes but found ", actualNonZeroByteCount)
	}
}

func testAddWithLimit(t *testing.T, memoryLimit uint64) {
	const (
		keyCount  = 10
		itemCount = 10
		itemSize  = 10
	)

	b := NewBuffer(".", memoryLimit, itemSize)
	defer b.initialize()

	for key := uint32(1); key <= keyCount; key++ {
		for itemNumber := byte(0); itemNumber < itemCount; itemNumber++ {
			item := make([]byte, itemSize)
			for i := range item {
				item[i] = itemNumber
			}
			b.Add(key, item)
		}
	}

	for key := uint32(1); key <= keyCount; key++ {
		items := b.Remove(key)
		if len(items) < itemCount {
			t.Error("insufficient items for key", key, len(items))
		}
		for itemNumber, item := range items {
			for _, b := range item {
				if b != byte(itemNumber) {
					t.Error("incorrect contents of item", key, itemNumber, b)
				}
			}
		}
	}
}
