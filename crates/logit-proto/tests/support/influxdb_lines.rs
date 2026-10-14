//! The line-protocol body `influxdb_fixed_point.rs`, `robustness.rs`, `logit-bench`'s fixtures,
//! and the fuzz seed generator all read, so the four can't drift apart.

/// One Telegraf `outputs.influxdb_v2` flush, 20 lines from the `cpu`, `mem`, `system`, `disk`,
/// `diskio`, `net`, `processes`, and `smart` inputs over two collection intervals.
///
/// Provenance: hand-built in the shape Telegraf 1.x writes those inputs (alphabetical tags and
/// fields, `host` on every line, integers as `Ni`, nanosecond timestamps), not recorded. It adds
/// what a default Telegraf doesn't send, so the codec's paths all run: a `u` field, a boolean, an
/// escaped space in a tag value, and `system`'s string field and `uptime` on lines of their own
/// at the series and timestamp of its numeric line, which the decoder merges: 18 events.
pub const INFLUX_LINES: &str = concat!(
    "cpu,cpu=cpu0,host=web-1 usage_guest=0,usage_idle=97.4,usage_iowait=0.2,usage_system=1.1,usage_user=1.3 1700000000000000000\n",
    "cpu,cpu=cpu1,host=web-1 usage_guest=0,usage_idle=95.8,usage_iowait=0.1,usage_system=1.9,usage_user=2.2 1700000000000000000\n",
    "cpu,cpu=cpu-total,host=web-1 usage_guest=0,usage_idle=96.6,usage_iowait=0.15,usage_system=1.5,usage_user=1.75 1700000000000000000\n",
    "mem,host=web-1 active=3221225472i,available=6442450944i,available_percent=75.01,buffered=268435456i,cached=2147483648i,free=4294967296i,total=8589934592i,used=2147483648i,used_percent=24.99 1700000000000000000\n",
    "system,host=web-1 load1=0.42,load15=0.31,load5=0.38,n_cpus=2i,n_users=1i 1700000000000000000\n",
    "system,host=web-1 uptime_format=\" 3 days,  4:05\" 1700000000000000000\n",
    "system,host=web-1 uptime=273900u 1700000000000000000\n",
    "disk,device=nvme0n1p1,fstype=ext4,host=web-1,mode=rw,path=/ free=40802189312i,inodes_free=2490000i,inodes_total=2621440i,inodes_used=131440i,total=42141548544i,used=1339359232i,used_percent=3.18 1700000000000000000\n",
    "disk,device=nvme1n1,fstype=xfs,host=web-1,mode=rw,path=/var/lib/data\\ dir free=96636764160i,inodes_free=52428000i,inodes_total=52428800i,inodes_used=800i,total=107374182400i,used=10737418240i,used_percent=10 1700000000000000000\n",
    "diskio,host=web-1,name=nvme0n1 io_time=81234i,iops_in_progress=0i,read_bytes=1048576000i,read_time=4521i,reads=25600i,weighted_io_time=9876i,write_bytes=2097152000i,write_time=5355i,writes=51200i 1700000000000000000\n",
    "diskio,host=web-1,name=nvme1n1 io_time=1234i,iops_in_progress=1i,read_bytes=524288i,read_time=12i,reads=128i,weighted_io_time=40i,write_bytes=8388608i,write_time=28i,writes=2048i 1700000000000000000\n",
    "net,host=web-1,interface=eth0 bytes_recv=987654321i,bytes_sent=123456789i,drop_in=0i,drop_out=0i,err_in=0i,err_out=0i,packets_recv=876543i,packets_sent=654321i 1700000000000000000\n",
    "net,host=web-1,interface=lo bytes_recv=1048576i,bytes_sent=1048576i,drop_in=0i,drop_out=0i,err_in=0i,err_out=0i,packets_recv=8192i,packets_sent=8192i 1700000000000000000\n",
    "processes,host=web-1 blocked=0i,dead=0i,idle=48i,paging=0i,running=1i,sleeping=97i,stopped=0i,total=146i,total_threads=312i,unknown=0i,zombies=0i 1700000000000000000\n",
    "smart_device,device=nvme0,host=web-1,model=Samsung\\ SSD\\ 980 exit_status=0i,health_ok=true,temp_c=41i 1700000000000000000\n",
    "smart_device,device=nvme1,host=web-1,model=Samsung\\ SSD\\ 980 exit_status=0i,health_ok=false,temp_c=47i 1700000000000000000\n",
    "cpu,cpu=cpu0,host=web-1 usage_guest=0,usage_idle=98.1,usage_iowait=0,usage_system=0.9,usage_user=1 1700000010000000000\n",
    "cpu,cpu=cpu1,host=web-1 usage_guest=0,usage_idle=96.3,usage_iowait=0.3,usage_system=1.6,usage_user=1.8 1700000010000000000\n",
    "mem,host=web-1 active=3221225472i,available=6442450944i,available_percent=75.01,buffered=268435456i,cached=2147483648i,free=4294967296i,total=8589934592i,used=2147483648i,used_percent=24.99 1700000010000000000\n",
    "system,host=web-1 load1=0.4,load15=0.31,load5=0.37,n_cpus=2i,n_users=1i 1700000010000000000\n",
);
