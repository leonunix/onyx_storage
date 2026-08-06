FIO_SOURCE_DIR ?=
TARGET := onyx_fio_engine.so

.PHONY: all clean

all: $(TARGET)

$(TARGET): Cargo.toml build.rs src/lib.rs src/fio_bridge.c
	@test -n "$(FIO_SOURCE_DIR)" || { echo "FIO_SOURCE_DIR must point to the fio source tree" >&2; exit 2; }
	@test -f "$(FIO_SOURCE_DIR)/fio.h" || { echo "$(FIO_SOURCE_DIR)/fio.h not found" >&2; exit 2; }
	@test -f "$(FIO_SOURCE_DIR)/config-host.h" || { echo "run ./configure in $(FIO_SOURCE_DIR) first" >&2; exit 2; }
	FIO_SOURCE_DIR="$(FIO_SOURCE_DIR)" cargo build --release --offline
	$(CC) -O2 -fPIC -shared -std=gnu11 -Wno-unused-parameter \
		-D_GNU_SOURCE -DFIO_EXTERNAL_ENGINE \
		-include "$(FIO_SOURCE_DIR)/config-host.h" -I"$(FIO_SOURCE_DIR)" \
		src/fio_bridge.c -Wl,--whole-archive target/release/libonyx_fio_engine.a \
		-Wl,--no-whole-archive -ldl -lpthread -lm -o $@

clean:
	rm -f $(TARGET)
	cargo clean
