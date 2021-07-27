package gluon

import (
	"reflect"
	"strings"
	"unicode"
)

// GenerateMessageKey creates a message key ready to be used as topic
// or part of a topic using the name of the given value.
// A word to be trimmed may be specified to remove unwanted part of the
// name.
// (e.g. trimmed word: 'Command', PaySalaryCommand -> PaySalary)
//
// In addition, an 'isAction' flag is used to indicate if the given name is
// an action or a fact. (e.g. Action -> StartPayment, Fact -> PaymentStarted)
// Note: An action starts with the infinitive verb, while a fact ends with
// the verb written in past tense. Gluon takes this as rule in order to
// generate message keys.
func GenerateMessageKey(trimmingWord string, isAction bool, v interface{}) string {
	valueType := reflect.TypeOf(v)
	valueName := strings.Title(valueType.Name())
	if trimmingWord != "" {
		keySplit := strings.Split(valueName, trimmingWord)
		valueName = strings.Join(keySplit[:len(keySplit)-1], "")
	}
	return setFormatting(isAction, separateCamelCase(valueName))
}

func separateCamelCase(str string) []string {
	buffer := strings.Builder{}
	slice := make([]string, 0)
	for i, s := range str {
		if unicode.IsUpper(s) && i != 0 {
			slice = append(slice, strings.ToLower(buffer.String()))
			buffer.Reset()
		}
		_, _ = buffer.WriteRune(s)
		if len(str) == (i + 1) {
			slice = append(slice, strings.ToLower(buffer.String()))
		}
	}
	return slice
}

func setFormatting(isAction bool, str []string) string {
	if !isAction {
		return formatKeyWithoutAction(str)
	}
	return formatKeyWithAction(str)
}

func formatKeyWithoutAction(str []string) string {
	buffer := strings.Builder{}
	for i, s := range str {
		if len(str) == (i + 1) {
			buffer.WriteString("." + s)
			break
		}
		buffer.WriteString(s)
		if (i + 2) < len(str) {
			buffer.WriteString("_")
		}
	}
	return buffer.String()
}

func formatKeyWithAction(str []string) string {
	buffer := strings.Builder{}
	suffix := ""
	for i, s := range str {
		if i == 0 {
			suffix = s
			continue
		}
		buffer.WriteString(s)
		if !(i == (len(str) - 1)) {
			buffer.WriteString("_")
		}
	}
	buffer.WriteString("." + suffix)
	return buffer.String()
}
