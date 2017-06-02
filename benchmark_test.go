package lwes_test

import (
	"testing"

	"go.openx.org/lwes"
)

func BenchmarkLwesEncode(b *testing.B) {
	lwe := lwes.NewLwesEvent("MonDemand::PerfMsg")
	lwe.Set("id", "0db302ef-4ba1-4d6b-86e3-92793d4b0c9e")
	lwe.Set("caller_label", "broker")
	lwe.Set("ctxt_num", uint16(3))
	lwe.Set("ctxt_k0", "platform_hash")
	lwe.Set("ctxt_v0", "7e319737-a81c-4817-bdc6-8f596e5caa46")
	lwe.Set("ctxt_k1", "bidder_count")
	lwe.Set("ctxt_v1", "28")
	lwe.Set("ctxt_k2", "total_count")
	lwe.Set("ctxt_v2", "28")
	lwe.Set("num", uint16(1))
	lwe.Set("label0", "adunit:538494050:call:1:ssrtb")
	lwe.Set("start0", int64(1494880081332))
	lwe.Set("end0", int64(1494880081487))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// lwe.MarshalBinary()
		bs, err := lwes.Marshal(lwe)
		if err != nil || len(bs) != 315 {
			b.Fatalf("for lwes-event<%v> got encoded err: %v, or length %d not 315", lwe, err, len(bs))
		}
	}
}
