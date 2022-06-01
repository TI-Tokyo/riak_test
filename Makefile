.PHONY: compile clean distclean

REBAR=./rebar3

all: compile
	@$(REBAR) escriptize
	@mkdir -p ./ebin
	@cp ./_build/test/lib/riak_test/tests/*.beam ./ebin

docsclean:
	@rm -rf doc/*.png doc/*.html doc/*.css edoc-info

compile:
	@$(REBAR) as test compile

clean:
	@$(REBAR) clean
	@rm riak_test

distclean: clean
	@rm -rf riak_test _build

quickbuild:
	$(REBAR) compile
	$(REBAR) escriptize
