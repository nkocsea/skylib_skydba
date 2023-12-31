package skydba

import (
	"strings"

	"github.com/nkocsea/skylib_skyutl/skyutl"
)

func BuildDocument(data []string) string {
	docArr := []string{}
	for _, item := range data {
		docItem := strings.TrimSpace(skyutl.RemoveAccents(item))
		if len(docItem) > 0 {
			docArr = append(docArr, docItem)
		}
	}
	if len(docArr) > 0 {
		return strings.ToUpper(strings.Join(docArr, " "))
	}
	return ""
}
