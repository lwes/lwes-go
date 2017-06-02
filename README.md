Light Weight Event System (LWES)
================================
Click [here](http://lwes.github.io) for more information about lwes.
For more information about using lwes from erlang read on.

Example Usage:

```go
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

    bs, err := lwes.Marshal(lwe)
    // check if no error then bs is the bytes of encoded binary
```