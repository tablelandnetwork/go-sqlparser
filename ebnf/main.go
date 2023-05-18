package main

import (
	"fmt"
	"log"
	"os"
)

type scanner struct {
	input []byte
	ch    byte
	pos   int

	insideBraces int
}

func (s *scanner) scan() {
	s.skipUntilStart()
	s.readByte() // consume %
	s.readByte() // consume %

	for {
		if s.ch == 0 { // end
			break
		}

		if s.ch == '{' {
			s.insideBraces++
			s.readByte()
			continue
		}

		if s.ch == '}' {
			s.insideBraces--
			s.readByte()
			continue
		}

		if s.insideBraces > 0 || s.ch == '%' || (s.ch == ';' && s.peekByte() != '\'') {
			s.readByte()
			continue
		}

		if s.ch == ':' {
			fmt.Print(" ::= ")
		} else {
			fmt.Print(string(s.ch))
		}

		s.readByte()
	}
}

func (s *scanner) readByte() {
	if s.pos >= len(s.input) {
		s.ch = 0
	} else {
		s.ch = s.input[s.pos]
	}

	s.pos++
}

func (s *scanner) peekByte() byte {
	if s.pos >= len(s.input) {
		return 0
	}
	return s.input[s.pos]
}

func (s *scanner) skipUntilStart() {
	for {
		if s.ch == '%' && s.peekByte() == '%' {
			break
		}
		s.readByte()
	}
}

func main() {
	if len(os.Args) <= 1 {
		log.Fatalf("grammar filepath as argument expected")
	}

	dat, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	s := &scanner{}
	s.input = dat
	s.readByte()

	s.scan()
}
