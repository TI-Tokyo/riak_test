# -------------------------------------------------------------------
#
# Copyright (c) 2012-2016 Basho Technologies, Inc.
# Copyright (c) 2017-2022 Workday, Inc.
#
# This file is provided to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------------
#
# Currently stripped down to irrelevance.
# Just use rebar3 from the command line.
#

REBAR ?= ./rebar3

all: compile

docsclean:
	@rm -rf doc/*.png doc/*.html doc/*.css edoc-info

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

distclean: clean
	rm -rf riak_test smoke_test _build

quickbuild: compile
