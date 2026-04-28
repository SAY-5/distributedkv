// kvctl is the operator CLI: get/put/del/cas/info.
//
//	kvctl -ep :8001,:8002,:8003 put hello world
//	kvctl -ep :8001 get hello
//	kvctl -ep :8001 info
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/SAY-5/distributedkv/internal/client"
)

func main() {
	ep := flag.String("ep", ":8001", "comma-separated endpoint list")
	stale := flag.Bool("stale", false, "for `get`: serve from local follower (non-linearizable)")
	expected := flag.Uint64("expected", 0, "for `cas`: expected version")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
	}

	c := client.New(strings.Split(*ep, ",")...)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch args[0] {
	case "put":
		if len(args) < 3 {
			usage()
		}
		v, err := c.Put(ctx, args[1], readValue(args[2]))
		fail(err)
		fmt.Printf("ok version=%d\n", v)

	case "get":
		if len(args) < 2 {
			usage()
		}
		v, ver, err := c.Get(ctx, args[1], *stale)
		if err == client.ErrNotFound {
			fmt.Println("(not found)")
			os.Exit(1)
		}
		fail(err)
		fmt.Printf("version=%d value=%s\n", ver, string(v))

	case "del":
		if len(args) < 2 {
			usage()
		}
		fail(c.Delete(ctx, args[1]))
		fmt.Println("ok")

	case "cas":
		if len(args) < 3 {
			usage()
		}
		v, conflict, err := c.CAS(ctx, args[1], *expected, readValue(args[2]))
		fail(err)
		if conflict != 0 {
			fmt.Printf("conflict actual_version=%d\n", conflict)
			os.Exit(2)
		}
		fmt.Printf("ok version=%d\n", v)

	case "info":
		info, err := c.ClusterInfo(ctx)
		fail(err)
		out, _ := json.MarshalIndent(info, "", "  ")
		fmt.Println(string(out))

	default:
		usage()
	}
}

// readValue: a leading '@' means "read from file" (or stdin if '@-').
func readValue(spec string) []byte {
	if !strings.HasPrefix(spec, "@") {
		return []byte(spec)
	}
	if spec == "@-" {
		b, err := io.ReadAll(os.Stdin)
		fail(err)
		return b
	}
	b, err := os.ReadFile(spec[1:])
	fail(err)
	return b
}

func usage() {
	fmt.Fprintf(os.Stderr, `kvctl — DistributedKV operator client
usage:
  kvctl [-ep <eps>] put <key> <value|@file|@->
  kvctl [-ep <eps>] get <key> [-stale]
  kvctl [-ep <eps>] del <key>
  kvctl [-ep <eps>] cas <key> <value> -expected <ver>
  kvctl [-ep <eps>] info
`)
	os.Exit(2)
}

func fail(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

// Suppress unused-import warning if strconv ever falls out of use.
var _ = strconv.Itoa
