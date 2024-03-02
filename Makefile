ramdisk_macos:
	diskutil apfs create $$(hdiutil attach -nomount ram://27262976) RAMDisk && touch /Volumes/RAMDisk/.metadata_never_index
	cp ../1brc/measurements.txt /Volumes/RAMDisk/

run:
	RUSTFLAGS="-C target-cpu=native" cargo run --release --features=bench,assert
