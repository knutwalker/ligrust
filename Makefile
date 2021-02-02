# https://tech.davis-hansson.com/p/make/
SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

ifeq ($(origin .RECIPEPREFIX), undefined)
  $(error This Make does not support .RECIPEPREFIX. Please use GNU Make 4.0 or later)
endif
.RECIPEPREFIX = >

APP := ligrust

DESTDIR ?=
PREFIX  ?= /usr/local


# generate release build
all: build
build: target/release/$(APP)

# install release build to local cargo bin directory
install: $(DESTDIR)$(PREFIX)/bin/$(APP)

# Remove installed binary
uninstall:
> -rm -- "$(DESTDIR)$(PREFIX)/bin/$(APP)"

# development builds
check: target/debug/$(APP)
test:
> cargo test --all --all-targets --all-features

# clean build output
clean:
> cargo clean

.PHONY: all build clean install uninstall check test

### build targets

target/debug/$(APP): Cargo.toml Cargo.lock $(shell find src -type f)
> cargo build --bin $(APP)

target/release/$(APP): Cargo.toml Cargo.lock $(shell find src -type f)
> RUSTFLAGS="-C link-arg=-s -C opt-level=2 -C target-cpu=native --emit=asm" cargo build --bin $(APP) --release

$(DESTDIR)$(PREFIX)/bin/$(APP): target/release/$(APP)
> install -m755 -- target/release/$(APP) "$(DESTDIR)$(PREFIX)/bin/"
