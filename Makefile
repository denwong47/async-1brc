ramdisk_macos:
	diskutil apfs create $$(hdiutil attach -nomount ram://27262976) RAMDisk && touch /Volumes/RAMDisk/.metadata_never_index
	cp ../1brc/measurements.txt /Volumes/RAMDisk/

define cargo
cargo ${ACTION} --release --bin main --features=${FEATURES} -- ${ARGS}
endef

cargo:
	$(cargo)

run_ramdisk: ACTION:=run
run_ramdisk: FEATURES:=bench,assert
run_ramdisk: ARGS:=--file=/Volumes/RAMDisk/measurements.txt
run_ramdisk: cargo

run_local: ACTION:=run
run_local: FEATURES:=bench,assert
run_local: ARGS:=--file=../1brc/measurements.txt
run_local: cargo

run: run_local

test: ACTION:=test
test: FEATURES:=bench,debug,nohash
test: cargo

clippy: ACTION:=clippy
clippy: FEATURES:=bench,assert,timed,debug
clippy: cargo
