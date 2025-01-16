package utils

const (
	Prompt = "goDB> "
)

type Call int

const (
	NewLine Call = iota
	Exit
	NoSuchCommand = 99
)