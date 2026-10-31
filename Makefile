D2_SOURCES := $(wildcard docs/*.d2)
D2_SVGS := $(D2_SOURCES:.d2=.svg)

.PHONY: all docs
.DELETE_ON_ERROR:

all: docs

docs: $(D2_SVGS)

docs/%.svg: docs/%.d2
	d2 "$<" "$@"
