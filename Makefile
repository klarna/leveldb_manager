suite=$(if $(SUITE), suite=$(SUITE), )

DIALYZER_OPTIONS := --fullpath --no_native -Wunderspecs
DIALYZER_PLT     := ./dialyzer.plt

.PHONY: default all get-deps compile deps test dialyze docs clean

default: compile

all: compile get-deps test dialyze docs

get-deps:
	./rebar get-deps

compile: get-deps
	./rebar compile

test: compile
	./rebar eunit $(suite) skip_deps=true

dialyze: build-plt
	./rebar dialyze

build-plt:
	./rebar build-plt

docs:
	./rebar doc

conf_clean:
	@:

clean:
	./rebar clean
	$(RM) doc/*.html doc/*edoc-info doc/erlang.png doc/stylesheet.css
