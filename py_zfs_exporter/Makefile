all:
	mkdir -p _stage/usr/local/bin
	install -m 755 -o0 -g0 -S zfs_exporter.py _stage/usr/local/bin/zfs_exporter
	pkg create -p py311-prometheus-zfs-exporter.plist -M py311-prometheus-zfs-exporter.ucl -r _stage -o /tmp/
	@echo 'You may now install the package using "pkg install", which is available in /tmp/'

clean:
	rm -r _stage



