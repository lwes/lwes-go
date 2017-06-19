package lwes_test

import (
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"

	"go.openx.org/lwes"
)

// Example printing the lwes event
func ExampleDecode() {
	// find out all 2 consecutive hexdigit

	data := `
00000000                    12 4d  6f 6e 44 65 6d 61 6e 64  |.......MonDemand|
00000020  3a 3a 50 65 72 66 4d 73  67 00 0d 07 63 74 78 74  |::PerfMsg...ctxt|
00000030  5f 76 32 05 00 02 32 38  07 63 74 78 74 5f 6b 32  |_v2...28.ctxt_k2|
00000040  05 00 0b 74 6f 74 61 6c  5f 63 6f 75 6e 74 07 63  |...total_count.c|
00000050  74 78 74 5f 76 31 05 00  02 32 38 07 63 74 78 74  |txt_v1...28.ctxt|
00000060  5f 6b 31 05 00 0c 62 69  64 64 65 72 5f 63 6f 75  |_k1...bidder_cou|
00000070  6e 74 07 63 74 78 74 5f  76 30 05 00 24 37 65 33  |nt.ctxt_v0..$7e3|
00000080  31 39 37 33 37 2d 61 38  31 63 2d 34 38 31 37 2d  |19737-a81c-4817-|
00000090  62 64 63 36 2d 38 66 35  39 36 65 35 63 61 61 34  |bdc6-8f596e5caa4|
000000a0  36 07 63 74 78 74 5f 6b  30 05 00 0d 70 6c 61 74  |6.ctxt_k0...plat|
000000b0  66 6f 72 6d 5f 68 61 73  68 08 63 74 78 74 5f 6e  |form_hash.ctxt_n|
000000c0  75 6d 01 00 03 04 65 6e  64 30 07 00 00 01 5c 0d  |um....end0....\.|
000000d0  cb d6 4f 06 73 74 61 72  74 30 07 00 00 01 5c 0d  |..O.start0....\.|
000000e0  cb d5 b4 06 6c 61 62 65  6c 30 05 00 1d 61 64 75  |....label0...adu|
000000f0  6e 69 74 3a 35 33 38 34  39 34 30 35 30 3a 63 61  |nit:538494050:ca|
00000100  6c 6c 3a 31 3a 73 73 72  74 62 03 6e 75 6d 01 00  |ll:1:ssrtb.num..|
00000110  01 0c 63 61 6c 6c 65 72  5f 6c 61 62 65 6c 05 00  |..caller_label..|
00000120  06 62 72 6f 6b 65 72 02  69 64 05 00 24 30 64 62  |.broker.id..$0db|
00000130  33 30 32 65 66 2d 34 62  61 31 2d 34 64 36 62 2d  |302ef-4ba1-4d6b-|
00000140  38 36 65 33 2d 39 32 37  39 33 64 34 62 30 63 39  |86e3-92793d4b0c9|
00000150  65 0b 52 65 63 65 69 70  74 54 69 6d 65 07 00 00  |e.ReceiptTime...|
00000160  01 5c 0d cb d6 71 08 53  65 6e 64 65 72 49 50 06  |.\...q.SenderIP.|
00000170  46 7f 01 0a 0a 53 65 6e  64 65 72 50 6f 72 74 01  |F....SenderPort.|
00000180  b7 50`

	allhexin := strings.Replace(strings.Join(regexp.MustCompile(` \b[[:xdigit:]]{2}\b`).FindAllString(data, -1), ""), " ", "", -1)
	raw, _ := hex.DecodeString(allhexin)

	// raw, _ := hex.DecodeString(strings.Replace(strings.Join(regexp.MustCompile(`\b(\s+[[:xdigit:]]{2}){2,16}\b`).FindAllString(data, -1), ""), " ", "", -1))
	// fmt.Println(raw, regexp.MustCompile(` \b[[:xdigit:]]{2}\b`).FindAllString(data, -1))

	// raw, _ := hex.DecodeString(strings.Join(regexp.MustCompile(`\b[[:xdigit:]]{2}\b`).FindAllString(regexp.MustCompile(`\|.{16}\|`).ReplaceAllString(data, ""), -1), ""))

	// fmt.Println(raw)

	lwe := new(lwes.LwesEvent)
	// lwe, _ := lwes.Decode(raw)
	lwes.Unmarshal(raw, lwe)

	lwe.FPrint(os.Stdout)

	// Output:
	// MonDemand::PerfMsg[16]
	// {
	// 	ctxt_v2 = 28;
	// 	ctxt_k2 = total_count;
	// 	ctxt_v1 = 28;
	// 	ctxt_k1 = bidder_count;
	// 	ctxt_v0 = 7e319737-a81c-4817-bdc6-8f596e5caa46;
	// 	ctxt_k0 = platform_hash;
	// 	ctxt_num = 3;
	// 	end0 = 1494880081487;
	// 	start0 = 1494880081332;
	// 	label0 = adunit:538494050:call:1:ssrtb;
	// 	num = 1;
	// 	caller_label = broker;
	// 	id = 0db302ef-4ba1-4d6b-86e3-92793d4b0c9e;
	// 	ReceiptTime = 1494880081521;
	// 	SenderIP = 10.1.127.70;
	// 	SenderPort = 46928;
	// }
}

// Example printing the lwes event
func ExampleLwesEvent_Enumerate() {
	// find out all 2 consecutive hexdigit from the hexdump output

	data := `
00000000                    12 4d  6f 6e 44 65 6d 61 6e 64  |.......MonDemand|
00000020  3a 3a 50 65 72 66 4d 73  67 00 0d 07 63 74 78 74  |::PerfMsg...ctxt|
00000030  5f 76 32 05 00 02 32 38  07 63 74 78 74 5f 6b 32  |_v2...28.ctxt_k2|
00000040  05 00 0b 74 6f 74 61 6c  5f 63 6f 75 6e 74 07 63  |...total_count.c|
00000050  74 78 74 5f 76 31 05 00  02 32 38 07 63 74 78 74  |txt_v1...28.ctxt|
00000060  5f 6b 31 05 00 0c 62 69  64 64 65 72 5f 63 6f 75  |_k1...bidder_cou|
00000070  6e 74 07 63 74 78 74 5f  76 30 05 00 24 37 65 33  |nt.ctxt_v0..$7e3|
00000080  31 39 37 33 37 2d 61 38  31 63 2d 34 38 31 37 2d  |19737-a81c-4817-|
00000090  62 64 63 36 2d 38 66 35  39 36 65 35 63 61 61 34  |bdc6-8f596e5caa4|
000000a0  36 07 63 74 78 74 5f 6b  30 05 00 0d 70 6c 61 74  |6.ctxt_k0...plat|
000000b0  66 6f 72 6d 5f 68 61 73  68 08 63 74 78 74 5f 6e  |form_hash.ctxt_n|
000000c0  75 6d 01 00 03 04 65 6e  64 30 07 00 00 01 5c 0d  |um....end0....\.|
000000d0  cb d6 4f 06 73 74 61 72  74 30 07 00 00 01 5c 0d  |..O.start0....\.|
000000e0  cb d5 b4 06 6c 61 62 65  6c 30 05 00 1d 61 64 75  |....label0...adu|
000000f0  6e 69 74 3a 35 33 38 34  39 34 30 35 30 3a 63 61  |nit:538494050:ca|
00000100  6c 6c 3a 31 3a 73 73 72  74 62 03 6e 75 6d 01 00  |ll:1:ssrtb.num..|
00000110  01 0c 63 61 6c 6c 65 72  5f 6c 61 62 65 6c 05 00  |..caller_label..|
00000120  06 62 72 6f 6b 65 72 02  69 64 05 00 24 30 64 62  |.broker.id..$0db|
00000130  33 30 32 65 66 2d 34 62  61 31 2d 34 64 36 62 2d  |302ef-4ba1-4d6b-|
00000140  38 36 65 33 2d 39 32 37  39 33 64 34 62 30 63 39  |86e3-92793d4b0c9|
00000150  65 0b 52 65 63 65 69 70  74 54 69 6d 65 07 00 00  |e.ReceiptTime...|
00000160  01 5c 0d cb d6 71 08 53  65 6e 64 65 72 49 50 06  |.\...q.SenderIP.|
00000170  46 7f 01 0a 0a 53 65 6e  64 65 72 50 6f 72 74 01  |F....SenderPort.|
00000180  b7 50`

	allhexin := strings.Replace(strings.Join(regexp.MustCompile(` \b[[:xdigit:]]{2}\b`).FindAllString(data, -1), ""), " ", "", -1)
	raw, _ := hex.DecodeString(allhexin)

	lwe := new(lwes.LwesEvent)
	// lwe, _ := lwes.Decode(raw)
	lwes.Unmarshal(raw, lwe)

	fmt.Printf("%s[%d]\n", lwe.Name, len(lwe.Attrs))
	fmt.Println("{")
	lwe.Enumerate(func(key string, value interface{}) bool {
		switch key {
		// stop enumerating on no interest fields
		case "ReceiptTime", "SenderIP", "SenderPort":
			return false
		default:
			fmt.Printf("\t%s = %v;\n", key, value)
			return true
		}
	})
	fmt.Println("}")

	// Output:
	// MonDemand::PerfMsg[16]
	// {
	// 	ctxt_v2 = 28;
	// 	ctxt_k2 = total_count;
	// 	ctxt_v1 = 28;
	// 	ctxt_k1 = bidder_count;
	// 	ctxt_v0 = 7e319737-a81c-4817-bdc6-8f596e5caa46;
	// 	ctxt_k0 = platform_hash;
	// 	ctxt_num = 3;
	// 	end0 = 1494880081487;
	// 	start0 = 1494880081332;
	// 	label0 = adunit:538494050:call:1:ssrtb;
	// 	num = 1;
	// 	caller_label = broker;
	// 	id = 0db302ef-4ba1-4d6b-86e3-92793d4b0c9e;
	// }
}

func ExampleNewLwesEvent() {
	lwe := lwes.NewLwesEvent("MonDemand::PerfMsg")
	lwe.Set("id", "0db302ef-4ba1-4d6b-86e3-92793d4b0c9e")
	lwe.Set("caller_label", "broker")

	timelines := []struct {
		label      string
		start, end int64
	}{
		{"adunit:538494050:call:1:ssrtb", 1494880081332, 1494880081487},
	}
	lwe.Set("num", uint16(len(timelines)))
	for idx, tl := range timelines {
		lwe.Set(fmt.Sprint("label", idx), tl.label)
		lwe.Set(fmt.Sprint("start", idx), tl.start)
		lwe.Set(fmt.Sprint("end", idx), tl.end)
	}

	context := map[string]string{
		"platform_hash": "7e319737-a81c-4817-bdc6-8f596e5caa46",
		"bidder_count":  "28",
		"total_count":   "28",
	}
	if len(context) != 0 {
		lwe.Set("ctxt_num", uint16(len(context)))

		// if need stable order over the keys, need extra string slice
		// otherwise just range over the map is ok;
		for idx, key := range []string{"platform_hash", "bidder_count", "total_count"} {
			value := context[key]
			lwe.Set(fmt.Sprint("ctxt_k", idx), key)
			lwe.Set(fmt.Sprint("ctxt_v", idx), value)
		}
	} // omit ctxt if no context at all

	buf, _ := lwes.Marshal(lwe)
	if lwe.Size() != len(buf) || len(buf) != 0x13b {
		fmt.Fprintf(os.Stderr, "length not matching: %d:%d\n", lwe.Size(), len(buf))
	}

	// fmt.Println(hex.Dump(buf))

	lwe1 := new(lwes.LwesEvent)
	lwes.Unmarshal(buf, lwe1)
	lwe1.FPrint(os.Stdout)

	// Output:
	// MonDemand::PerfMsg[13]
	// {
	// 	id = 0db302ef-4ba1-4d6b-86e3-92793d4b0c9e;
	// 	caller_label = broker;
	// 	num = 1;
	// 	label0 = adunit:538494050:call:1:ssrtb;
	// 	start0 = 1494880081332;
	// 	end0 = 1494880081487;
	// 	ctxt_num = 3;
	// 	ctxt_k0 = platform_hash;
	// 	ctxt_v0 = 7e319737-a81c-4817-bdc6-8f596e5caa46;
	// 	ctxt_k1 = bidder_count;
	// 	ctxt_v1 = 28;
	// 	ctxt_k2 = total_count;
	// 	ctxt_v2 = 28;
	// }
}
