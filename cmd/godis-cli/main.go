package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/divizn/godis/internal/network"
)

func main() {
	log.SetFlags(0)
	addr := flag.String("addr", "localhost:8080", "server address")
	flag.Parse()

	c, err := network.Dial(*addr)
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

func repl(c *network.Client, in io.Reader, out io.Writer, prompt string) error {
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

var arity = map[string]int{"PING": 1, "GET": 2, "SET": 3, "DEL": 2}

func run(c *network.Client, args []string) (string, error) {
	cmd := strings.ToUpper(args[0])
	n, ok := arity[cmd]
	if !ok {
		return "", fmt.Errorf("unknown command %q", args[0])
	}
	if len(args) != n {
		return "", fmt.Errorf("wrong number of arguments for '%s'", strings.ToLower(cmd))
	}

	var err error
	switch cmd {
	case "PING":
		if err = c.Ping(); err == nil {
			return "PONG", nil
		}
	case "GET":
		var v []byte
		var found bool
		if v, found, err = c.Get(args[1]); err == nil {
			if !found {
				return "(nil)", nil
			}
			return strconv.Quote(string(v)), nil
		}
	case "SET":
		err = c.Set(args[1], []byte(args[2]))
	case "DEL":
		err = c.Del(args[1])
	}
	if err != nil {
		return "", err
	}
	return "OK", nil
}

var errBadQuote = errors.New("invalid quoted argument")

// splitArgs splits line on whitespace; an argument starting with a double
// quote is a Go quoted string.
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
