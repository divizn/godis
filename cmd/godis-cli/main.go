package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/divizn/godis/client"
)

func main() {
	log.SetFlags(0)
	addr := flag.String("addr", "localhost:8080", "server addresses, comma separated")
	flag.Parse()

	c, err := client.Dial(strings.Split(*addr, ",")...)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	if flag.NArg() > 0 {
		out, err := run(c, flag.Args())
		if err != nil {
			fmt.Println("(error)", err)
			os.Exit(1)
		}
		fmt.Println(out)
		return
	}

	prompt := ""
	if fi, err := os.Stdin.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
		prompt = "godis> "
	}
	if err := repl(c, os.Stdin, os.Stdout, prompt); err != nil {
		log.Fatal(err)
	}
}

func repl(c *client.Client, in io.Reader, out io.Writer, prompt string) error {
	sc := bufio.NewScanner(in)
	sc.Buffer(nil, 64<<20)
	for {
		fmt.Fprint(out, prompt)
		if !sc.Scan() {
			break
		}
		args, err := splitArgs(sc.Text())
		if len(args) == 0 && err == nil {
			continue
		}
		var res string
		if err == nil {
			res, err = run(c, args)
		}
		if err != nil {
			res = "(error) " + err.Error()
		}
		fmt.Fprintln(out, res)
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read input: %w", err)
	}
	return nil
}

// arity is the least and most arguments of each command, its name included
var arity = map[string][2]int{"PING": {1, 1}, "GET": {2, 2}, "SET": {3, 5}, "DEL": {2, 2}, "CAS": {4, 4}, "INCR": {2, 3}, "PEXPIRE": {3, 3}, "PTTL": {2, 2}}

// absent as CAS's expected value means "only if the key does not exist"
const absent = "-"

func run(c *client.Client, args []string) (string, error) {
	cmd := strings.ToUpper(args[0])
	n, ok := arity[cmd]
	if !ok {
		return "", fmt.Errorf("unknown command %q", args[0])
	}
	if len(args) < n[0] || len(args) > n[1] {
		return "", fmt.Errorf("wrong number of arguments for '%s'", strings.ToLower(cmd))
	}

	ctx := context.Background()
	var err error
	switch cmd {
	case "PING":
		if err = c.Ping(ctx); err == nil {
			return "PONG", nil
		}
	case "GET":
		var v []byte
		var found bool
		if v, found, err = c.Get(ctx, args[1]); err == nil {
			if !found {
				return "(nil)", nil
			}
			return strconv.Quote(string(v)), nil
		}
	case "SET":
		if len(args) == 3 {
			err = c.Set(ctx, args[1], []byte(args[2]))
			break
		}
		if len(args) != 5 || !strings.EqualFold(args[3], "PX") {
			return "", errors.New("syntax error, expected SET key value [PX milliseconds]")
		}
		var ttl time.Duration
		if ttl, err = parseMillis(args[4]); err != nil {
			return "", err
		}
		err = c.SetTTL(ctx, args[1], []byte(args[2]), ttl)
	case "PEXPIRE":
		var ttl time.Duration
		if ttl, err = parseMillis(args[2]); err != nil {
			return "", err
		}
		var found bool
		if found, err = c.Expire(ctx, args[1], ttl); err == nil {
			if found {
				return "(integer) 1", nil
			}
			return "(integer) 0", nil
		}
	case "PTTL":
		var remaining time.Duration
		var found bool
		if remaining, found, err = c.TTL(ctx, args[1]); err == nil {
			switch {
			case !found:
				return "(integer) -2", nil
			case remaining == 0:
				return "(integer) -1", nil
			}
			return fmt.Sprint("(integer) ", remaining.Milliseconds()), nil
		}
	case "DEL":
		err = c.Del(ctx, args[1])
	case "CAS":
		var swapped bool
		if swapped, err = c.CAS(ctx, args[1], []byte(args[2]), args[2] != absent, []byte(args[3])); err == nil {
			if swapped {
				return "(integer) 1", nil
			}
			return "(integer) 0", nil
		}
	case "INCR":
		delta := int64(1)
		if len(args) == 3 {
			if delta, err = strconv.ParseInt(args[2], 10, 64); err != nil {
				return "", fmt.Errorf("delta %q is not an integer", args[2])
			}
		}
		var v int64
		if v, err = c.Incr(ctx, args[1], delta); err == nil {
			return fmt.Sprint("(integer) ", v), nil
		}
	}
	if err != nil {
		return "", err
	}
	return "OK", nil
}

// parseMillis reads a whole number of milliseconds, refusing one too large for a time.Duration
func parseMillis(s string) (time.Duration, error) {
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil || ms > math.MaxInt64/int64(time.Millisecond) || ms < math.MinInt64/int64(time.Millisecond) {
		return 0, fmt.Errorf("milliseconds %q is not a usable integer", s)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

var errBadQuote = errors.New("invalid quoted argument")

// splitArgs splits line on whitespace; an argument starting with " is a Go quoted string.
func splitArgs(line string) ([]string, error) {
	var args []string
	for {
		line = strings.TrimLeftFunc(line, unicode.IsSpace)
		if line == "" {
			return args, nil
		}
		if line[0] != '"' {
			end := strings.IndexFunc(line, unicode.IsSpace)
			if end < 0 {
				end = len(line)
			}
			args = append(args, line[:end])
			line = line[end:]
			continue
		}

		q, err := strconv.QuotedPrefix(line)
		if err != nil {
			return nil, errBadQuote
		}
		line = line[len(q):]
		if line != "" && !unicode.IsSpace(rune(line[0])) {
			return nil, errBadQuote
		}
		arg, err := strconv.Unquote(q)
		if err != nil {
			return nil, errBadQuote
		}
		args = append(args, arg)
	}
}
