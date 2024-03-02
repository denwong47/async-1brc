ramdisk_macos:
	diskutil apfs create $$(hdiutil attach -nomount ram://27262976) RAMDisk && touch /Volumes/RAMDisk/.metadata_never_index
	cp ../1brc/measurements.txt /Volumes/RAMDisk/

define cargo
cargo ${ACTION} --release --features=${FEATURES}
endef

cargo:
	$(cargo)

run: ACTION:=run
run: FEATURES:=bench,assert
run: cargo

test: ACTION:=test
test: FEATURES:=bench,debug
test: cargo

clippy: ACTION:=clippy
clippy: FEATURES:=bench,assert,timed,debug
clippy: cargo