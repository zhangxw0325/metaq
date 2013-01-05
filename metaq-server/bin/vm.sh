sudo sysctl vm.overcommit_memory=1
sudo sysctl vm.min_free_kbytes=10000000
sudo sysctl vm.drop_caches=1
sudo sysctl vm.zone_reclaim_mode=0
sudo sysctl vm.max_map_count=655360
sudo sysctl vm.dirty_background_ratio=50
sudo sysctl vm.dirty_ratio=50
sudo sysctl vm.page-cluster=3
sudo sysctl vm.dirty_writeback_centisecs=360000
sudo sysctl vm.swappiness=10

# maybe root to do
ulimit -n 655360